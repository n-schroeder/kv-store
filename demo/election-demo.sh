#!/usr/bin/env bash
#
# Leader election demo for kv-store.
#
# Starts a 5-node cluster in Docker, runs it through a series of failure
# scenarios, and for each one shows the election-related log lines from every
# node and checks that the cluster ended up in the expected state.
#
# Requires Docker with the Compose plugin (Docker Desktop, or Docker Engine on
# Linux). Runs on macOS and Linux, and on Windows from WSL2 or Git Bash.
# Compatible with bash 3.2, the version macOS ships with.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/compose.yaml"
NETWORK="kv-election-demo"
NODES="1 2 3 4 5"
TOTAL_SCENARIOS=9
WAIT_TIMEOUT=45       # seconds to wait for the cluster to reach an expected state
OBSERVE_SECONDS=8     # how long to watch a cluster that shouldn't elect anyone

# Log lines that aren't about elections. The first heartbeat a node receives is
# sometimes shown anyway, when it's the evidence a scenario is looking for.
BASE_NOISE='Received heartbeat for term|New client connected|disconnected gracefully|listening on port|Role: FOLLOWER|Peers: |Node ID: |The client (wants|is asking)'

# Replication lines are hidden in the election scenarios, where they're only
# churn from each new leader's no-op entry. The two data scenarios shadow NOISE
# with BASE_NOISE, because there they're the whole point.
REPLICATION_NOISE='Commit index advanced to|Received [0-9]+ entries for term|Log restored|Database booted|Restored Raft state|rejected entries at index|Truncating diverged log'
NOISE="$BASE_NOISE|$REPLICATION_NOISE"

PAUSE=1
KEEP=0
CLUSTER_STARTED=0
PASSED=0

usage() {
    cat <<EOF
Usage: $0 [--no-pause] [--keep]

  --no-pause   run every scenario back to back instead of waiting for Enter
  --keep       leave the cluster running when the demo ends
  -h, --help   show this help
EOF
}

for arg in "$@"; do
    case "$arg" in
        --no-pause) PAUSE=0 ;;
        --keep) KEEP=1 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; usage >&2; exit 2 ;;
    esac
done
[ -t 0 ] || PAUSE=0

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    USE_COLOR=1
    BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'; CYAN=$'\033[36m'; RESET=$'\033[0m'
else
    USE_COLOR=0
    BOLD=""; DIM=""; RED=""; GREEN=""; CYAN=""; RESET=""
fi

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

die() {
    printf '%s\n' "${RED}error:${RESET} $*" >&2
    exit 1
}

say() {
    printf '%s\n' "$*"
}

expect() {
    printf '%s\n' "${BOLD}What should happen:${RESET} $*"
}

scenario() {
    printf '\n%s\n' "${BOLD}${CYAN}━━━ Scenario $1 of $TOTAL_SCENARIOS: $2 ━━━${RESET}"
}

pause() {
    [ "$PAUSE" = 1 ] || return 0
    printf '%s' "${DIM}${1:-Press Enter to run it...}${RESET}"
    read -r _ || true
}

pass() {
    printf '\n%s\n' "${GREEN}✓ PASS${RESET}  $*"
    PASSED=$((PASSED + 1))
}

fail() {
    printf '\n%s\n' "${RED}✗ FAIL${RESET}  $*"
    say "Stopping here, since the later scenarios build on this one."
    say "Re-run with --keep to leave the cluster up for inspection."
    exit 1
}

cleanup() {
    [ "$CLUSTER_STARTED" = 1 ] || return 0
    echo
    if [ "$KEEP" = 1 ]; then
        say "Cluster left running (--keep). Tear it down with:"
        say "  docker compose -f \"$COMPOSE_FILE\" down -t 0"
    else
        say "${DIM}Removing the cluster...${RESET}"
        dc down -t 0 --remove-orphans >/dev/null 2>&1
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

dc() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

check_prereqs() {
    command -v docker >/dev/null 2>&1 \
        || die "Docker isn't installed. Install Docker Desktop (macOS/Windows) or Docker Engine (Linux)."
    docker info >/dev/null 2>&1 \
        || die "Docker is installed but not reachable. Make sure Docker Desktop (or the Docker daemon) is running."
    docker compose version >/dev/null 2>&1 \
        || die "The Docker Compose plugin ('docker compose') isn't available."
}

load_container_ids() {
    local n
    for n in $NODES; do
        CID[$n]=$(dc ps -a -q "node$n")
        [ -n "${CID[$n]}" ] || die "Couldn't find the container for node$n."
    done
}

is_running() {
    [ "$(docker inspect -f '{{.State.Running}}' "${CID[$1]}" 2>/dev/null)" = "true" ]
}

kill_node() {
    docker kill "${CID[$1]}" >/dev/null || die "Couldn't kill node$1."
}

start_node() {
    docker start "${CID[$1]}" >/dev/null || die "Couldn't start node$1."
}

# Logs from the node's current process only. A restarted container keeps the
# logs of its previous run, and those must not count toward its current role.
current_run_logs() {
    local started
    started=$(docker inspect -f '{{.State.StartedAt}}' "${CID[$1]}")
    docker logs --since "$started" "${CID[$1]}" 2>&1
}

# Records how many log lines each node has, so later checks and log output
# only look at what happened after this point.
mark_logs() {
    local n
    for n in $NODES; do
        MARK[$n]=$(docker logs "${CID[$n]}" 2>&1 | wc -l | tr -d ' ')
    done
}

logs_since_mark() {
    docker logs --timestamps "${CID[$1]}" 2>&1 | tail -n +"$((MARK[$1] + 1))"
}

# ---------------------------------------------------------------------------
# Reading cluster state from the logs
# ---------------------------------------------------------------------------

# Prints "leader <term>" if the node's latest role change in its current run
# was winning an election, and "other" otherwise.
node_state() {
    local line
    line=$(current_run_logs "$1" | grep -E 'Becoming LEADER|Becoming CANDIDATE|to FOLLOWER' | tail -n 1)
    case "$line" in
        *"Becoming LEADER"*)
            printf 'leader %s\n' "$(printf '%s' "$line" | sed -E 's/.*for term ([0-9]+).*/\1/')" ;;
        *)
            printf 'other\n' ;;
    esac
}

# Sets LEADERS to a space-separated list of "node:term" for every running node
# that believes it is the leader, skipping the node given as $1 (if any).
find_leaders() {
    local exclude=${1:-} n state
    LEADERS=""
    for n in $NODES; do
        [ "$n" = "$exclude" ] && continue
        is_running "$n" || continue
        state=$(node_state "$n")
        case "$state" in
            leader\ *) LEADERS="$LEADERS $n:${state#leader }" ;;
        esac
    done
    LEADERS=${LEADERS# }
}

# Waits until exactly one running node (other than $1, if given) is the
# leader, and the same node stays leader across three consecutive checks.
# Sets LEADER and LEADER_TERM.
wait_for_leader() {
    local exclude=${1:-} deadline last="" streak=0
    deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        find_leaders "$exclude"
        case "$LEADERS" in
            ""|*" "*)
                last=""; streak=0 ;;
            *)
                if [ "$LEADERS" = "$last" ]; then
                    streak=$((streak + 1))
                else
                    last=$LEADERS; streak=1
                fi
                if [ "$streak" -ge 3 ]; then
                    LEADER=${LEADERS%%:*}
                    LEADER_TERM=${LEADERS#*:}
                    return 0
                fi ;;
        esac
        sleep 0.5
    done
    return 1
}

# Waits until node $1 has logged a line matching the regex $2 since the mark.
wait_for_log() {
    local deadline
    deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        logs_since_mark "$1" | grep -qE "$2" && return 0
        sleep 0.5
    done
    return 1
}

# Waits until every running node except the leader ($1) and an optional
# excluded node ($3) has received a heartbeat for term $2 since the mark.
wait_for_followers() {
    local leader=$1 term=$2 exclude=${3:-} n
    for n in $NODES; do
        [ "$n" = "$leader" ] && continue
        [ "$n" = "$exclude" ] && continue
        is_running "$n" || continue
        wait_for_log "$n" "Received heartbeat for term $term\." || return 1
    done
    return 0
}

# Runs the bundled client inside node $1's container, against that node's own
# listener. Using 127.0.0.1 rather than the service name means this still works
# for a node that's been cut off from the cluster network. Redirects the client
# follows still use service names, which resolve fine for connected nodes.
kv() {
    local n=$1
    shift
    dc exec -T "node$n" client 127.0.0.1:7878 "$@" 2>&1
}

# Prints the highest commit index node $1 has logged in its current run, or 0.
commit_index() {
    local line
    line=$(current_run_logs "$1" | grep -E 'Commit index advanced to [0-9]+\.' | tail -n 1)
    if [ -n "$line" ]; then
        printf '%s\n' "$line" | sed -E 's/.*advanced to ([0-9]+)\..*/\1/'
    else
        printf '0\n'
    fi
}

# Waits until node $1's commit index has reached at least $2.
wait_for_commit() {
    local deadline
    deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        [ "$(commit_index "$1")" -ge "$2" ] && return 0
        sleep 0.5
    done
    return 1
}

running_nodes_except() {
    local n out=""
    for n in $NODES; do
        [ "$n" = "$1" ] && continue
        is_running "$n" && out="$out $n"
    done
    printf '%s\n' "${out# }"
}

# Prints "node1, node2 and node3" for the node numbers given as arguments.
join_nodes() {
    local sorted count i=0 n out=""
    sorted=$(printf '%s\n' "$@" | sort -n)
    count=$(printf '%s\n' "$sorted" | grep -c '^')
    for n in $sorted; do
        i=$((i + 1))
        if [ "$i" -eq 1 ]; then
            out="node$n"
        elif [ "$i" -eq "$count" ]; then
            out="$out and node$n"
        else
            out="$out, node$n"
        fi
    done
    printf '%s\n' "$out"
}

# Prints the non-heartbeat log lines from every node since the mark, merged
# into one timeline. $1 caps how many lines are printed, $2 is a list of nodes
# whose first received heartbeat should be shown, and $3 is an extra regex of
# lines to hide.
# Prints node $1's log lines since the mark, prefixed with its name, minus
# lines matching the regex $3. If $2 is 1, its first heartbeat is kept.
# (Kept out of show_logs because bash 3.2 can't parse `case` inside `$(...)`.)
filtered_node_logs() {
    logs_since_mark "$1" | awk -v node="node$1" -v want="$2" -v noise="$3" '
        {
            if ($0 ~ /Received heartbeat for term/) {
                keep = (want && !shown)
                if (keep) shown = 1
            } else {
                keep = ($0 !~ noise)
            }
            if (keep) print node " " $0
        }'
}

show_logs() {
    local max=${1:-40} first_hb_nodes=${2:-} extra_noise=${3:-} noise=$NOISE n lines total
    [ -z "$extra_noise" ] || noise="$noise|$extra_noise"
    for n in $NODES; do
        WANT_FIRST_HB[$n]=0
        case " $first_hb_nodes " in *" $n "*) WANT_FIRST_HB[$n]=1 ;; esac
    done
    lines=$(
        for n in $NODES; do
            filtered_node_logs "$n" "${WANT_FIRST_HB[$n]}" "$noise"
        done | awk '
            # Pad fractional seconds to 9 digits so the lines sort by time.
            {
                ts = $2
                sub(/Z$/, "", ts)
                split(ts, dt, "T")
                split(dt[2], hms, ".")
                frac = substr(hms[2] "000000000", 1, 9)
                msg = $0
                sub(/^[^ ]+ [^ ]+ /, "", msg)
                printf "%sT%s.%s %s %s\n", dt[1], hms[1], frac, $1, msg
            }' | LC_ALL=C sort
    )
    total=$(printf '%s' "$lines" | grep -c '^')

    echo
    if [ "$total" -eq 0 ]; then
        say "  ${DIM}(nothing logged besides heartbeats)${RESET}"
        return
    fi

    printf '%s\n' "$lines" | head -n "$max" | awk -v color="$USE_COLOR" '
        BEGIN { split("36 35 33 34 32", palette, " ") }
        {
            split($1, dt, "T")
            t = substr(dt[2], 1, 12)
            node = $2
            msg = $0
            sub(/^[^ ]+ [^ ]+ /, "", msg)
            if (color) {
                t = "\033[2m" t "\033[0m"
                node = "\033[" palette[substr(node, 5) + 0] "m" node "\033[0m"
                if (msg ~ /Becoming LEADER|Stepping down/) msg = "\033[1m" msg "\033[0m"
            }
            printf "  %s  %s  %s\n", t, node, msg
        }'
    if [ "$total" -gt "$max" ]; then
        say "  ${DIM}... $((total - max)) more lines${RESET}"
    fi
}

# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

scenario_startup() {
    scenario 1 "The cluster starts up"
    say "Five nodes boot at the same time. Every one starts as a follower, and no leader is configured."
    expect "Nobody is sending heartbeats, so each node's random election timeout (500-1000ms) runs out. The node whose timeout runs out first becomes a candidate, collects a majority (3 of 5), and becomes leader, usually in term 1. The other four start following it."
    pause

    local up_output
    up_output=$(dc up -d 2>&1) || { say "$up_output"; die "Couldn't start the cluster."; }
    load_container_ids
    local n
    for n in $NODES; do MARK[$n]=0; done

    wait_for_leader || { show_logs; fail "No single, stable leader emerged within ${WAIT_TIMEOUT}s."; }
    wait_for_followers "$LEADER" "$LEADER_TERM" \
        || { show_logs 40 "$NODES"; fail "Not every node started following node$LEADER."; }

    show_logs 40 "$NODES"
    pass "node$LEADER is the leader for term $LEADER_TERM, and the other four nodes are following it."
}

scenario_kill_leader() {
    local old=$LEADER old_term=$LEADER_TERM
    scenario 2 "Kill the leader"
    say "node$old is the leader (term $old_term). It's about to be killed without warning."
    expect "Heartbeats stop. Within about a second one of the four survivors times out, becomes a candidate in a higher term, wins a majority, and takes over."
    pause

    mark_logs
    kill_node "$old"
    DEAD="$old"

    wait_for_leader || { show_logs; fail "No new leader was elected within ${WAIT_TIMEOUT}s."; }
    [ "$LEADER_TERM" -gt "$old_term" ] \
        || { show_logs; fail "node$LEADER is leader, but its term ($LEADER_TERM) isn't higher than $old_term."; }
    wait_for_followers "$LEADER" "$LEADER_TERM" \
        || { show_logs; fail "Not every survivor started following node$LEADER."; }

    show_logs
    pass "node$LEADER took over as leader for term $LEADER_TERM after node$old died."
}

scenario_restart_old_leader() {
    local old=$DEAD leader=$LEADER term=$LEADER_TERM
    scenario 3 "Bring the old leader back"
    say "node$old is restarted. Terms aren't saved to disk, so it comes back as a follower in term 0."
    expect "Within a heartbeat or two it hears from node$leader, adopts term $term, and settles in as a follower. node$leader stays the leader."
    pause

    mark_logs
    start_node "$old"
    DEAD=""

    wait_for_log "$old" "Received heartbeat for term $term\." \
        || { show_logs 40 "$old"; fail "node$old never received a heartbeat for term $term."; }
    wait_for_leader || { show_logs 40 "$old"; fail "The cluster lost its leader after node$old rejoined."; }
    [ "$LEADER" = "$leader" ] \
        || { show_logs 40 "$old"; fail "Leadership moved from node$leader to node$LEADER when node$old rejoined."; }

    show_logs 40 "$old"
    pass "node$old rejoined as a follower in term $term, and node$leader is still the leader."
}

scenario_lose_two() {
    local leader=$LEADER term=$LEADER_TERM follower
    follower=$(running_nodes_except "$leader" | awk '{ print $1 }')
    scenario 4 "Kill the leader and a follower"
    say "node$leader (the leader) and node$follower are killed together, leaving three nodes running."
    expect "Three out of five is still a majority, so the survivors can still elect a leader. Every election now needs all three of their votes, so you might see a split vote and a retry before one node wins."
    pause

    mark_logs
    kill_node "$leader"
    kill_node "$follower"
    DEAD="$leader $follower"

    wait_for_leader || { show_logs; fail "The three survivors didn't elect a leader within ${WAIT_TIMEOUT}s."; }
    [ "$LEADER_TERM" -gt "$term" ] \
        || { show_logs; fail "node$LEADER is leader, but its term ($LEADER_TERM) isn't higher than $term."; }
    wait_for_followers "$LEADER" "$LEADER_TERM" \
        || { show_logs; fail "The other survivors didn't start following node$LEADER."; }

    show_logs
    pass "With 3 of 5 nodes up, node$LEADER was elected leader for term $LEADER_TERM."
}

scenario_lose_majority() {
    local leader=$LEADER
    scenario 5 "Lose the majority"
    say "node$leader, the newest leader, is killed as well. Only two nodes are left."
    say "${DIM}(The leader is the one killed on purpose: a leader that loses its followers doesn't step down on its own, so killing a follower here wouldn't trigger any elections.)${RESET}"
    expect "Two votes can never reach the majority of 3. The survivors keep timing out, holding elections that fail, dropping back to follower, and retrying with fresh random timeouts. Every attempt uses a new, higher term, so the term keeps climbing, and no leader is elected."
    pause

    mark_logs
    kill_node "$leader"
    DEAD="$DEAD $leader"

    local deadline
    deadline=$(( $(date +%s) + OBSERVE_SECONDS ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        find_leaders
        [ -z "$LEADERS" ] || { show_logs 30; fail "node${LEADERS%%:*} became leader without a majority."; }
        sleep 0.5
    done

    local failures=0 count n
    for n in $(running_nodes_except ""); do
        count=$(logs_since_mark "$n" | grep -c 'failed to reach majority')
        failures=$((failures + count))
    done
    HIGHEST_TERM=$(
        for n in $NODES; do logs_since_mark "$n"; done \
            | grep -oE 'Starting election for term [0-9]+' \
            | sed -E 's/.* ([0-9]+)$/\1/' | sort -n | tail -n 1
    )
    [ "$failures" -ge 3 ] \
        || { show_logs 30; fail "Expected repeated failed elections, but only saw $failures in ${OBSERVE_SECONDS}s."; }

    # The "peer unreachable" lines repeat three times per election; hiding them
    # fits more rounds of the retry loop on screen.
    show_logs 30 "" "unreachable or unresponsive"
    pass "No leader for ${OBSERVE_SECONDS}s: $failures failed elections, and the term climbed to $HIGHEST_TERM."
}

scenario_regain_majority() {
    local dead=$DEAD highest=$HIGHEST_TERM n
    scenario 6 "Restart the dead nodes"
    # shellcheck disable=SC2086
    say "$(join_nodes $dead) come back, each starting from term 0."
    expect "With all five nodes up, a majority is possible again. The two survivors pushed their terms up to about $highest in the last scenario and will reject anything with a lower term. Because terms aren't saved to disk, the three restarted nodes can sometimes elect one of themselves in a low term (three votes is a majority). That leader steps down the moment a survivor replies with its higher term. Either way, the cluster settles on one leader in a term of at least $highest, and everyone follows it."
    pause

    mark_logs
    for n in $dead; do start_node "$n"; done
    DEAD=""

    wait_for_leader || { show_logs 40 "$dead"; fail "No stable leader emerged within ${WAIT_TIMEOUT}s of restarting the nodes."; }
    [ "$LEADER_TERM" -ge "$highest" ] \
        || { show_logs 40 "$dead"; fail "node$LEADER leads term $LEADER_TERM, which is lower than the $highest the survivors had reached."; }
    wait_for_followers "$LEADER" "$LEADER_TERM" \
        || { show_logs 40 "$dead"; fail "Not all nodes started following node$LEADER."; }

    show_logs 40 "$dead"
    pass "All five nodes are back, and node$LEADER is the leader for term $LEADER_TERM."
}

scenario_partition() {
    local old=$LEADER old_term=$LEADER_TERM
    scenario 7 "Cut the leader off from the network, then reconnect it"
    say "node$old is disconnected from the cluster's network. Unlike a crash, its process keeps running and still believes it's the leader."
    expect "The other four stop hearing heartbeats and elect a new leader in a higher term, so for a while two nodes think they're the leader. When node$old reconnects, its heartbeats carry the old term. The other nodes ignore them and reply with the newer term, and node$old steps down."
    pause

    mark_logs
    docker network disconnect "$NETWORK" "${CID[$old]}" >/dev/null \
        || die "Couldn't disconnect node$old from the network."

    wait_for_leader "$old" \
        || { show_logs; fail "The connected nodes didn't elect a new leader within ${WAIT_TIMEOUT}s."; }
    local new=$LEADER new_term=$LEADER_TERM
    [ "$new_term" -gt "$old_term" ] \
        || { show_logs; fail "node$new is leader, but its term ($new_term) isn't higher than $old_term."; }
    [ "$(node_state "$old")" = "leader $old_term" ] \
        || { show_logs; fail "node$old stopped believing it was leader while cut off, which shouldn't be possible."; }
    wait_for_followers "$new" "$new_term" "$old" \
        || { show_logs; fail "The connected nodes didn't all start following node$new."; }

    show_logs
    echo
    say "${BOLD}Right now node$new leads term $new_term, while node$old is cut off and still thinks it leads term $old_term.${RESET}"
    pause "Press Enter to reconnect node$old..."

    mark_logs
    docker network connect --alias "node$old" "$NETWORK" "${CID[$old]}" >/dev/null \
        || die "Couldn't reconnect node$old to the network."

    wait_for_log "$old" "Stepping down to FOLLOWER" \
        || { show_logs 40 "$old"; fail "node$old never stepped down after reconnecting."; }
    wait_for_leader || { show_logs 40 "$old"; fail "No single, stable leader after node$old reconnected."; }
    wait_for_followers "$LEADER" "$LEADER_TERM" \
        || { show_logs 40 "$old"; fail "Not every node is following node$LEADER after the reconnect."; }

    show_logs 40 "$old"
    if [ "$LEADER" = "$new" ]; then
        pass "node$old stepped down after reconnecting, and node$new is the only leader (term $new_term)."
    else
        pass "node$old stepped down after reconnecting. It then timed out before hearing from node$new and won a new election, so node$LEADER now leads term $LEADER_TERM as the only leader."
    fi
}

scenario_catch_up() {
    # Replication lines are the evidence here, so don't filter them out.
    local NOISE="$BASE_NOISE"
    local leader=$LEADER victim target got i

    victim=$(running_nodes_except "$leader" | awk '{print $1}')
    [ -n "$victim" ] || die "No follower available to knock over."

    scenario 8 "A node misses writes while it's down, then catches up"
    say "node$victim is killed. Ten keys are then written to node$leader, which still has the four-node majority it needs to commit them. node$victim comes back with a log ten entries short of everyone else's."
    expect "node$leader backs its replication cursor up until it finds the last entry it and node$victim agree on, ships everything after it, and node$victim applies the lot. Its commit index catches up on its own -- no client replays anything, and nothing had to be written twice."
    pause

    mark_logs
    kill_node "$victim"

    for i in 1 2 3 4 5 6 7 8 9 10; do
        kv "$leader" set "catchup_$i" "value_$i" >/dev/null \
            || { show_logs 50; fail "Writing catchup_$i to node$leader failed while $((5 - 1)) nodes were up."; }
    done

    target=$(commit_index "$leader")
    [ "$target" -ge 10 ] \
        || { show_logs 50; fail "node$leader committed only up to index $target after ten writes."; }

    say "node$leader has committed through index $target while node$victim was down. Restarting node$victim..."

    # Re-mark so the timeline below shows only the reconciliation. The ten
    # writes themselves are ~80 lines of routine replication that would bury it.
    mark_logs
    start_node "$victim"

    wait_for_commit "$victim" "$target" \
        || { show_logs 50; fail "node$victim never caught up to commit index $target (it reached $(commit_index "$victim"))."; }

    got=$(kv "$victim" get catchup_7)
    printf '%s\n' "$got" | grep -q '^value_7$' \
        || { show_logs 50; fail "Reading catchup_7 back gave: $got"; }

    show_logs 50
    pass "node$victim rejoined ten entries behind and caught up to commit index $target on its own."
}

scenario_divergent_log() {
    local NOISE="$BASE_NOISE"
    local old old_term new new_term target ghost_write ghost_read

    wait_for_leader || { show_logs; fail "No stable leader to start from."; }
    old=$LEADER
    old_term=$LEADER_TERM

    scenario 9 "A partitioned leader's uncommitted write is rolled back"
    say "node$old is cut off from the network while it still believes it leads term $old_term, and a write is sent to it. It appends the entry to its own log, but with no majority to replicate to it can never commit -- so the client is told the write did not succeed."
    expect "The other four elect a new leader and commit writes of their own. When node$old reconnects, its log holds an entry the cluster never accepted. It steps down, the new leader backs up to the last index they agree on, and node$old truncates the diverged entry. The write that was never acknowledged stays gone."
    pause

    mark_logs
    docker network disconnect "$NETWORK" "${CID[$old]}" >/dev/null \
        || die "Couldn't disconnect node$old from the network."

    ghost_write=$(kv "$old" set ghost never-committed)
    case "$ghost_write" in
        *OK*)
            show_logs 50
            fail "node$old acknowledged a write it had no majority to commit." ;;
    esac
    say "The write to the cut-off node$old was refused, as it should be:"
    say "  ${DIM}${ghost_write}${RESET}"

    wait_for_leader "$old" \
        || { show_logs 50; fail "The connected nodes didn't elect a new leader within ${WAIT_TIMEOUT}s."; }
    new=$LEADER
    new_term=$LEADER_TERM
    [ "$new_term" -gt "$old_term" ] \
        || { show_logs 50; fail "node$new leads term $new_term, which isn't higher than $old_term."; }

    kv "$new" set survivor committed >/dev/null \
        || { show_logs 50; fail "The new leader node$new couldn't commit a write."; }
    target=$(commit_index "$new")

    say "node$new now leads term $new_term and has committed through index $target. Reconnecting node$old..."
    pause "Press Enter to reconnect node$old..."

    docker network connect --alias "node$old" "$NETWORK" "${CID[$old]}" >/dev/null \
        || die "Couldn't reconnect node$old to the network."

    wait_for_log "$old" "Truncating diverged log from index" \
        || { show_logs 50; fail "node$old never truncated its diverged entry after reconnecting."; }
    wait_for_commit "$old" "$target" \
        || { show_logs 50; fail "node$old never caught up to commit index $target."; }

    ghost_read=$(kv "$old" get ghost)
    case "$ghost_read" in
        *never-committed*)
            show_logs 50
            fail "The rolled-back write came back out of the cluster: $ghost_read" ;;
    esac

    show_logs 50
    pass "node$old truncated the entry it never committed and caught up to node$new's log at index $target. The unacknowledged write stayed gone."
}

# ---------------------------------------------------------------------------

main() {
    check_prereqs

    say "${BOLD}kv-store: leader election demo${RESET}"
    cat <<EOF

This starts a 5-node kv-store cluster in Docker and runs it through $TOTAL_SCENARIOS
scenarios: killing leaders, losing and regaining a majority, cutting a leader
off from the network, and then two that follow the data -- a node catching up
on writes it missed while it was down, and a partitioned leader rolling back a
write it could never commit. For each one it shows what the nodes logged (the
constant heartbeat traffic is filtered out) and checks that the cluster ended
up where it should.

Timestamps are in UTC. No ports are opened on this machine, and the cluster is
removed when the demo exits.

EOF
    say "Building the server image. The first build compiles the Rust dependencies"
    say "and can take a few minutes; later runs use Docker's cache."
    echo
    dc build || die "The image build failed."

    CLUSTER_STARTED=1
    dc down -t 0 --remove-orphans >/dev/null 2>&1

    scenario_startup
    scenario_kill_leader
    scenario_restart_old_leader
    scenario_lose_two
    scenario_lose_majority
    scenario_regain_majority
    scenario_partition
    scenario_catch_up
    scenario_divergent_log

    echo
    say "${GREEN}${BOLD}All $PASSED of $TOTAL_SCENARIOS scenarios passed.${RESET}"
}

main
