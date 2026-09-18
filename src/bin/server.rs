use kv_store::{AppendOutcome, Command, KvStore, RaftLog, RaftState, RaftStateStore, Response};
use rand::Rng;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Duration};
use std::collections::{HashMap, HashSet};
use std::env;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const WAL_PATH: &str = "wal.log";
const RAFT_STATE_PATH: &str = "raft-state.bin";

/// How long a client write waits for its entry to commit before we give up on
/// telling the client anything useful.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(2);

/// Entries per `AppendEntries`. Caps how much a badly lagging follower can pull
/// in one round trip, so catching up is incremental rather than one huge frame.
const MAX_ENTRIES_PER_APPEND: usize = 64;

/// Bound on every peer exchange: a peer that accepts a connection and then goes
/// quiet must not be able to stall an election or a heartbeat round.
const PEER_TIMEOUT: Duration = Duration::from_millis(150);

#[derive(PartialEq, Clone, Copy, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Everything one node is and knows.
///
/// Held in an `Arc` and shared by the listener, the replication loop, and the
/// election watchdog. The `std::sync::Mutex` fields are all synchronous
/// compare-and-set state, never held across an `.await`; the log is behind a
/// `tokio` mutex because touching it means disk I/O.
struct Node {
    node_id: String,
    peers: Vec<String>,

    log: tokio::sync::Mutex<RaftLog>,
    store: KvStore,

    /// Term, votedFor and commitIndex — the state that survives a crash.
    state: Mutex<RaftState>,
    state_store: RaftStateStore,
    /// Serializes state-file writers. A saver takes this *before* snapshotting
    /// the state, so two concurrent saves can't persist terms out of order and
    /// walk the stored term backwards.
    state_io: tokio::sync::Mutex<()>,

    role: Mutex<Role>,
    last_heartbeat: Mutex<Instant>,
    /// Who we last accepted an `AppendEntries` from, so a follower can redirect
    /// a client instead of just refusing it.
    leader_id: Mutex<Option<String>>,
    /// Highest log index handed to the state machine.
    last_applied: Mutex<u64>,

    /// Leader-only, rebuilt on every election win.
    next_index: Mutex<HashMap<String, u64>>,
    match_index: Mutex<HashMap<String, u64>>,
    /// Peers with an `AppendEntries` in flight, so a slow peer doesn't collect a
    /// pile-up of overlapping requests carrying stale `next_index` values.
    replicating: Mutex<HashSet<String>>,
    /// Peers we've already reported as unreachable. A leader retries every
    /// 150ms forever, so without this a single dead peer buries the log.
    unreachable: Mutex<HashSet<String>>,

    /// Wakes up client writes waiting for their entry to commit.
    commit_tx: watch::Sender<u64>,
}

/// Clears a peer's in-flight marker however `replicate_to` exits.
struct ReplicationGuard {
    node: Arc<Node>,
    peer: String,
}

impl Drop for ReplicationGuard {
    fn drop(&mut self) {
        self.node.replicating.lock().unwrap().remove(&self.peer);
    }
}

impl Node {
    fn term(&self) -> u64 {
        self.state.lock().unwrap().current_term
    }

    fn commit_index(&self) -> u64 {
        self.state.lock().unwrap().commit_index
    }

    fn role(&self) -> Role {
        *self.role.lock().unwrap()
    }

    fn is_leader(&self) -> bool {
        self.role() == Role::Leader
    }

    fn touch_heartbeat(&self) {
        *self.last_heartbeat.lock().unwrap() = Instant::now();
    }

    /// How many votes (or replicas) make a majority of the whole cluster.
    fn majority(&self) -> usize {
        (1 + self.peers.len()) / 2 + 1
    }

    /// Flushes the current in-memory state to disk. Must complete *before* the
    /// node acts on a term bump or a vote.
    async fn save(&self) -> io::Result<()> {
        let _guard = self.state_io.lock().await;
        let snapshot = self.state.lock().unwrap().clone();
        self.state_store.save(&snapshot).await
    }

    /// Logs a failed save without taking the node down. A node that can't
    /// persist its vote isn't safe to keep voting, but it can still follow a
    /// leader, so we surface it loudly rather than panic.
    async fn save_or_warn(&self, what: &str) {
        if let Err(e) = self.save().await {
            eprintln!("FATAL-ish: could not persist Raft state after {}: {}", what, e);
        }
    }

    /// Adopts `incoming_term` and steps down to `Follower` if it's newer than the locally known
    /// term. Returns whether it was adopted. This is the only way a stale `Candidate`/`Leader`
    /// learns it's behind, whether via a `RequestVote`, an `AppendEntries`, or an ack.
    ///
    /// Adopting also clears `voted_for`, because the vote is per-term. Callers
    /// must `save()` when this returns true, before acting on the change.
    fn adopt_term_if_newer(&self, incoming_term: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if incoming_term > state.current_term {
            state.current_term = incoming_term;
            state.voted_for = None;
            let mut role = self.role.lock().unwrap();
            if *role != Role::Follower {
                println!("Stepping down to FOLLOWER: saw higher term {}.", incoming_term);
                *role = Role::Follower;
            }
            true
        } else {
            false
        }
    }

    /// Decides a `RequestVote` and records the vote in memory. Returns
    /// `(vote_granted, needs_save)`; the caller must persist before replying.
    ///
    /// Grant when the candidate's term is current, we haven't already promised
    /// this term to someone else, and the candidate's log is at least as
    /// complete as ours. That last check is the election restriction: without
    /// it a node missing committed entries could win and overwrite them, which
    /// would make an acknowledged write a lie.
    fn decide_vote(
        &self,
        candidate_id: &str,
        candidate_term: u64,
        candidate_last_term: u64,
        candidate_last_index: u64,
        our_last_term: u64,
        our_last_index: u64,
    ) -> (bool, bool) {
        let adopted = self.adopt_term_if_newer(candidate_term);

        let mut state = self.state.lock().unwrap();
        if candidate_term != state.current_term {
            return (false, adopted);
        }

        let log_ok = match candidate_last_term.cmp(&our_last_term) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => candidate_last_index >= our_last_index,
        };

        if !log_ok {
            println!(
                "Rejecting {} for term {}: its log ends at ({}, {}), ours at ({}, {}).",
                candidate_id, candidate_term,
                candidate_last_term, candidate_last_index,
                our_last_term, our_last_index
            );
            return (false, adopted);
        }

        match state.voted_for {
            None => {
                state.voted_for = Some(candidate_id.to_string());
                (true, true)
            }
            // A retried RPC from the same candidate must not lose the vote.
            Some(ref already) if already == candidate_id => (true, adopted),
            Some(_) => (false, adopted),
        }
    }

    /// Hands every newly committed entry to the state machine, in order.
    fn apply_committed(&self, log: &RaftLog) {
        let commit = self.commit_index();
        let mut last_applied = self.last_applied.lock().unwrap();
        while *last_applied < commit {
            let next = *last_applied + 1;
            match log.entry_at(next) {
                Some(entry) => {
                    self.store.apply(&entry.command);
                    *last_applied = next;
                }
                None => break,
            }
        }
    }

    /// Moves the commit index forward, applies what that newly commits, and
    /// wakes any client waiting on it.
    async fn advance_commit_to(&self, new_commit: u64, log: &RaftLog) {
        let target = new_commit.min(log.last_index());

        {
            let mut state = self.state.lock().unwrap();
            if target <= state.commit_index {
                return;
            }
            state.commit_index = target;
        }

        self.apply_committed(log);
        println!("Commit index advanced to {}.", target);
        // commitIndex is only a lower-bound hint, but persisting it is what
        // lets a node that restarts alone serve its committed data instead of
        // waiting for a leader to tell it what it already knows.
        self.save_or_warn("advancing the commit index").await;
        let _ = self.commit_tx.send(target);
    }

    /// The leader's commit rule: the highest index stored on a majority. The
    /// term check is Raft's, and it is not optional — an entry from an earlier
    /// term can be present on a majority and still not be committed, so
    /// counting replicas alone can commit something a future leader overwrites.
    async fn advance_leader_commit(&self) {
        let log = self.log.lock().await;
        let current_term = self.term();

        let mut replicated: Vec<u64> = {
            let match_index = self.match_index.lock().unwrap();
            self.peers.iter().map(|p| match_index.get(p).copied().unwrap_or(0)).collect()
        };
        replicated.push(log.last_index()); // the leader holds its own entries
        replicated.sort_unstable_by(|a, b| b.cmp(a));

        let candidate = replicated[self.majority() - 1];
        if candidate > self.commit_index() && log.term_at(candidate) == Some(current_term) {
            self.advance_commit_to(candidate, &log).await;
        }
    }

    /// Takes leadership for `term`, resetting the per-peer replication cursors.
    async fn become_leader(&self, term: u64) {
        let last_index = {
            let log = self.log.lock().await;
            log.last_index()
        };

        {
            let mut next_index = self.next_index.lock().unwrap();
            let mut match_index = self.match_index.lock().unwrap();
            next_index.clear();
            match_index.clear();
            for peer in &self.peers {
                // Optimistically assume peers match us, and back up on refusal.
                next_index.insert(peer.clone(), last_index + 1);
                match_index.insert(peer.clone(), 0);
            }
        }

        *self.role.lock().unwrap() = Role::Leader;
        *self.leader_id.lock().unwrap() = Some(self.node_id.clone());

        // A fresh leader may hold entries from earlier terms that it cannot
        // commit by replica count alone. Appending one entry of its own term
        // gives the commit rule something to latch onto, so those older entries
        // commit along with it instead of waiting for the next client write.
        {
            let mut log = self.log.lock().await;
            if let Err(e) = log.append_command(term, Command::NoOp).await {
                eprintln!("Could not append the leader's no-op entry: {}", e);
            }
        }

        // In a single-node cluster the leader *is* the majority, so nothing
        // would ever commit if we only recounted on a peer's ack.
        self.advance_leader_commit().await;
    }
}

fn random_election_timeout() -> Duration {
    Duration::from_millis(rand::thread_rng().gen_range(500..1000))
}

/// Sends one framed `Command` and reads one framed `Response`, all inside a
/// single timeout so a peer that connects and then goes silent can't stall us.
///
/// The response always gets read even when the caller doesn't need it: a
/// fire-and-forget write races the peer's own reply and kills the peer's
/// connection task with `BrokenPipe`.
async fn send_to_peer(peer_addr: &str, cmd: &Command) -> Option<Response> {
    let result = timeout(PEER_TIMEOUT, async {
        let mut stream = TcpStream::connect(peer_addr).await.ok()?;

        let payload = bincode::serialize(cmd).ok()?;
        let len_bytes = (payload.len() as u32).to_be_bytes();
        stream.write_all(&len_bytes).await.ok()?;
        stream.write_all(&payload).await.ok()?;

        let mut resp_len_buf = [0u8; 4];
        stream.read_exact(&mut resp_len_buf).await.ok()?;
        let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
        let mut resp_payload = vec![0u8; resp_len];
        stream.read_exact(&mut resp_payload).await.ok()?;

        bincode::deserialize::<Response>(&resp_payload).ok()
    })
    .await;

    result.unwrap_or(None)
}

/// One round of replication to one peer: ship whatever it's missing, or an
/// empty `AppendEntries` as a heartbeat if it's caught up.
async fn replicate_to(node: Arc<Node>, peer: String) {
    if !node.replicating.lock().unwrap().insert(peer.clone()) {
        return; // a previous round is still in flight
    }
    let _guard = ReplicationGuard { node: node.clone(), peer: peer.clone() };

    let term = node.term();
    if !node.is_leader() {
        return;
    }

    let next = node.next_index.lock().unwrap().get(&peer).copied().unwrap_or(1);

    let (prev_log_index, prev_log_term, entries) = {
        let log = node.log.lock().await;
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = log.term_at(prev_log_index).unwrap_or(0);
        (prev_log_index, prev_log_term, log.entries_from(next, MAX_ENTRIES_PER_APPEND))
    };


    let cmd = Command::AppendEntries {
        term,
        leader_id: node.node_id.clone(),
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit: node.commit_index(),
    };

    let response = match send_to_peer(&peer, &cmd).await {
        Some(response) => response,
        None => {
            // Report the transition, not every retry.
            if node.unreachable.lock().unwrap().insert(peer.clone()) {
                println!("Replication failed: Node {} is unreachable.", peer);
            }
            return;
        }
    };

    if node.unreachable.lock().unwrap().remove(&peer) {
        println!("Node {} is reachable again.", peer);
    }

    let Response::AppendEntriesAck { term: ack_term, success, match_index, conflict_index } = response
    else {
        return;
    };

    // A peer that has seen a newer term means someone else won an election we
    // don't know about yet; step down. Reset the election timer too, or it fires
    // at once and disrupts the new leader.
    if node.adopt_term_if_newer(ack_term) {
        node.touch_heartbeat();
        node.save_or_warn("stepping down on a newer append-entries ack").await;
        return;
    }

    // Anything learned under an older term is stale by now.
    if !node.is_leader() || node.term() != term {
        return;
    }

    if success {
        {
            let mut next_index = node.next_index.lock().unwrap();
            let mut match_map = node.match_index.lock().unwrap();
            // Never move a cursor backwards on a reordered or duplicated ack.
            let current = match_map.get(&peer).copied().unwrap_or(0);
            if match_index > current {
                match_map.insert(peer.clone(), match_index);
                next_index.insert(peer.clone(), match_index + 1);
            }
        }
        node.advance_leader_commit().await;
    } else {
        let retry_from = conflict_index.max(1);
        println!("Node {} rejected entries at index {}; backing up to {}.", peer, prev_log_index, retry_from);
        node.next_index.lock().unwrap().insert(peer.clone(), retry_from);
    }
}

/// This node's dialable address, used as its identity in `votedFor` and as the
/// redirect a follower hands a client. Defaults to the container hostname,
/// which is what the demo's compose service names resolve to.
fn resolve_node_id() -> String {
    let raw = env::var("NODE_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| env::var("HOSTNAME").ok().filter(|s| !s.is_empty()))
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "unknown-node".to_string());

    if raw.contains(':') { raw } else { format!("{}:7878", raw) }
}

#[tokio::main]
async fn main() {
    let peers_env = env::var("PEERS").unwrap_or_default();
    let peers: Vec<String> = peers_env
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let node_id = resolve_node_id();

    let listener = TcpListener::bind("0.0.0.0:7878").await.unwrap();
    println!("Async Server with WAL listening on port 7878");

    let log = match RaftLog::open(WAL_PATH).await {
        Ok(log) => log,
        Err(e) => {
            eprintln!("Could not open the write-ahead log: {}", e);
            std::process::exit(1);
        }
    };

    let (state_store, persisted) = match RaftStateStore::open(RAFT_STATE_PATH).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("Could not open the Raft state file: {}", e);
            std::process::exit(1);
        }
    };

    println!("Server Booted.");
    println!("Role: FOLLOWER");
    println!("Node ID: {}", node_id);
    println!("Peers: {:?}", peers);
    println!(
        "Restored Raft state: term {}, voted for {:?}, commit index {}.",
        persisted.current_term, persisted.voted_for, persisted.commit_index
    );

    let (commit_tx, _commit_rx) = watch::channel(persisted.commit_index);

    let node = Arc::new(Node {
        node_id,
        peers: peers.clone(),
        log: tokio::sync::Mutex::new(log),
        store: KvStore::new(),
        state: Mutex::new(persisted),
        state_store,
        state_io: tokio::sync::Mutex::new(()),
        role: Mutex::new(Role::Follower),
        last_heartbeat: Mutex::new(Instant::now()),
        leader_id: Mutex::new(None),
        last_applied: Mutex::new(0),
        next_index: Mutex::new(HashMap::new()),
        match_index: Mutex::new(HashMap::new()),
        replicating: Mutex::new(HashSet::new()),
        unreachable: Mutex::new(HashSet::new()),
        commit_tx,
    });

    // Replay the committed prefix into the state machine. Committed entries are
    // never truncated, so this is always a valid view of the data — and the
    // uncommitted tail stays in the log, unapplied, safe to discard if it turns
    // out to have diverged.
    {
        let log = node.log.lock().await;

        // commitIndex can never exceed what's actually in the log. A crash
        // can't normally produce that (the entry is fsynced before the commit
        // index that covers it), but corruption or a hand-edited log can, and
        // "committed entries I don't have" is a state nothing downstream is
        // prepared to reason about.
        let last_index = log.last_index();
        if node.commit_index() > last_index {
            println!(
                "Commit index {} is past the last log entry ({}); clamping.                  The log is missing entries it had recorded as committed.",
                node.commit_index(), last_index
            );
            node.state.lock().unwrap().commit_index = last_index;
            node.save_or_warn("clamping the commit index at startup").await;
        }

        node.apply_committed(&log);
        println!(
            "Database booted. Restored {} keys by replaying {} committed entries.",
            node.store.len(),
            node.commit_index()
        );
    }

    // Replication / heartbeat loop.
    {
        let node = node.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(150)).await;

                if !node.is_leader() {
                    continue;
                }

                for peer in &node.peers {
                    tokio::spawn(replicate_to(node.clone(), peer.clone()));
                }
            }
        });
    }

    // Follower watchdog: start an election when the leader goes quiet.
    {
        let node = node.clone();
        tokio::spawn(async move {
            let mut election_timeout = random_election_timeout();

            loop {
                sleep(Duration::from_millis(100)).await;

                if node.role() != Role::Follower {
                    continue;
                }

                let elapsed = node.last_heartbeat.lock().unwrap().elapsed();
                if elapsed <= election_timeout {
                    continue;
                }

                // Re-roll now so the next wait window (whether this election wins, loses,
                // or gets preempted by another node's heartbeat/vote request) uses a fresh
                // randomized timeout, keeping repeat candidates from splitting votes forever.
                election_timeout = random_election_timeout();

                *node.role.lock().unwrap() = Role::Candidate;
                println!("Timeout: no heartbeat in {}ms. Becoming CANDIDATE.", elapsed.as_millis());

                tokio::spawn(run_election(node.clone()));
            }
        });
    }

    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        println!("New client connected: {}", addr);
        tokio::spawn(handle_connection(node.clone(), stream, addr));
    }
}

async fn run_election(node: Arc<Node>) {
    let current_term = {
        let mut state = node.state.lock().unwrap();
        state.current_term += 1;
        // A candidate votes for itself, and that vote is as binding as any
        // other: if we crash and come back, we must not hand this term to
        // someone else.
        state.voted_for = Some(node.node_id.clone());
        state.current_term
    };

    // Persist before asking anyone else for a vote.
    node.save_or_warn("starting an election").await;

    let (last_log_index, last_log_term) = {
        let log = node.log.lock().await;
        (log.last_index(), log.last_term())
    };

    println!(
        "Starting election for term {}. Log ends at ({}, {}).",
        current_term, last_log_term, last_log_index
    );

    let needed = node.majority();
    let mut votes = 1; // self-vote

    let mut set = tokio::task::JoinSet::new();
    for peer in node.peers.clone() {
        let cmd = Command::RequestVote {
            term: current_term,
            candidate_id: node.node_id.clone(),
            last_log_index,
            last_log_term,
        };
        set.spawn(async move {
            match send_to_peer(&peer, &cmd).await {
                Some(response) => Some(response),
                None => {
                    println!(
                        "Election: peer {} unreachable or unresponsive within timeout, no vote counted.",
                        peer
                    );
                    None
                }
            }
        });
    }

    while let Some(result) = set.join_next().await {
        if let Ok(Some(Response::VoteResponse { vote_granted: true, .. })) = result {
            votes += 1;
        }
    }

    println!(
        "Election for term {}: received {} of {} votes needed.",
        current_term, votes, needed
    );

    if votes >= needed {
        // A win that arrives after the term has moved on is stale: acting on it
        // would silently clobber whatever this node moved on to. If the term did
        // change, adopt_term_if_newer already put us back to Follower.
        if node.term() == current_term {
            node.become_leader(current_term).await;
            println!("Won election for term {} with {} votes. Becoming LEADER.", current_term, votes);
        } else {
            println!("Election for term {} won but term has since advanced; discarding stale result.", current_term);
        }
    } else {
        println!("Election for term {} failed to reach majority. Reverting to FOLLOWER to retry after next timeout.", current_term);
        *node.role.lock().unwrap() = Role::Follower;
        node.touch_heartbeat();
    }
}

/// Appends a client write and waits for it to commit on a majority before
/// answering. This is what makes an `Ok` mean "this survives any single node
/// dying" rather than "one disk has it".
async fn replicate_client_write(node: &Arc<Node>, command: Command) -> Response {
    let term = node.term();

    let index = {
        let mut log = node.log.lock().await;
        match log.append_command(term, command).await {
            Ok(index) => index,
            Err(e) => {
                eprintln!("Could not append the client write: {}", e);
                return Response::Error(format!("write failed: {}", e));
            }
        }
    };

    // Kick replication immediately rather than waiting for the next tick.
    for peer in &node.peers {
        tokio::spawn(replicate_to(node.clone(), peer.clone()));
    }

    // Recount now as well as on every ack: the leader's own copy is one of the
    // replicas, and in a single-node cluster it is the whole majority, so
    // without this there is no ack to ever trigger the count.
    node.advance_leader_commit().await;

    let mut commit_rx = node.commit_tx.subscribe();
    let wait = timeout(COMMIT_TIMEOUT, async {
        loop {
            if *commit_rx.borrow() >= index {
                return true;
            }
            // Losing leadership means this entry may yet be truncated, so we
            // must not report success for it.
            if !node.is_leader() || node.term() != term {
                return false;
            }
            if commit_rx.changed().await.is_err() {
                return false;
            }
        }
    })
    .await;

    match wait {
        Ok(true) => Response::Ok,
        Ok(false) => Response::Error(format!(
            "lost leadership for term {} before index {} committed; the write may or may not survive",
            term, index
        )),
        Err(_) => Response::Error(format!(
            "index {} not committed within {:?}; this is unknown, not failed — it may still commit",
            index, COMMIT_TIMEOUT
        )),
    }
}

async fn handle_append_entries(
    node: &Arc<Node>,
    term: u64,
    leader_id: String,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<kv_store::LogEntry>,
    leader_commit: u64,
) -> Response {
    let adopted = node.adopt_term_if_newer(term);
    if adopted {
        node.save_or_warn("adopting a newer term from an append-entries").await;
    }
    let local_term = node.term();

    if term < local_term {
        // Stale leader from an old term; ignore for timeout purposes so it
        // can't suppress a legitimate election.
        println!("Ignoring stale heartbeat for term {} (local term is {}).", term, local_term);
        return Response::AppendEntriesAck {
            term: local_term,
            success: false,
            match_index: 0,
            conflict_index: 0,
        };
    }

    if !adopted {
        // Term was already current; if we were still Candidate for it, someone
        // else's election won it first.
        let mut role = node.role.lock().unwrap();
        if *role == Role::Candidate {
            println!("Stepping down to FOLLOWER: heartbeat for current term {} from elected leader.", term);
            *role = Role::Follower;
        }
    }

    node.touch_heartbeat();
    *node.leader_id.lock().unwrap() = Some(leader_id);

    if entries.is_empty() {
        println!("Received heartbeat for term {}.", term);
    } else {
        println!(
            "Received {} entries for term {} (after index {}).",
            entries.len(), term, prev_log_index
        );
    }

    let mut log = node.log.lock().await;
    match log.try_append(prev_log_index, prev_log_term, &entries).await {
        Ok(AppendOutcome::Accepted { match_index }) => {
            if leader_commit > node.commit_index() {
                node.advance_commit_to(leader_commit, &log).await;
            }
            Response::AppendEntriesAck {
                term: local_term,
                success: true,
                match_index,
                conflict_index: 0,
            }
        }
        Ok(AppendOutcome::Conflict { conflict_index }) => {
            println!(
                "Log mismatch at index {} (leader's term {}); asking to resend from {}.",
                prev_log_index, prev_log_term, conflict_index
            );
            Response::AppendEntriesAck {
                term: local_term,
                success: false,
                match_index: 0,
                conflict_index,
            }
        }
        Err(e) => {
            eprintln!("Could not append replicated entries: {}", e);
            Response::AppendEntriesAck {
                term: local_term,
                success: false,
                match_index: 0,
                conflict_index: log.last_index() + 1,
            }
        }
    }
}

async fn handle_connection(node: Arc<Node>, mut stream: TcpStream, addr: std::net::SocketAddr) {
    loop {
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            println!("Client {} disconnected gracefully.", addr);
            return;
        }

        let payload_len = u32::from_be_bytes(len_buf) as usize;
        let mut payload_buf = vec![0u8; payload_len];
        if stream.read_exact(&mut payload_buf).await.is_err() {
            println!("Client {} disconnected mid-message.", addr);
            return;
        }

        let cmd: Command = match bincode::deserialize(&payload_buf) {
            Ok(cmd) => cmd,
            Err(e) => {
                eprintln!("Undecodable message from {}: {}", addr, e);
                return;
            }
        };

        let response = match cmd {
            Command::Set { key, value } => {
                if !node.is_leader() {
                    let leader = node.leader_id.lock().unwrap().clone();
                    println!("Refusing a write: not the leader (leader is {:?}).", leader);
                    Response::NotLeader { leader }
                } else {
                    println!("The client wants to store {} bytes under the key '{}'", value.len(), key);
                    replicate_client_write(&node, Command::Set { key, value }).await
                }
            }

            Command::Get { key } => {
                // Reads go through the leader so a partitioned-off follower
                // can't serve data the cluster has already moved past.
                if !node.is_leader() {
                    let leader = node.leader_id.lock().unwrap().clone();
                    Response::NotLeader { leader }
                } else {
                    println!("The client is asking for the key '{}'", key);
                    match node.store.get(&key) {
                        Some(val) => Response::Value(Some(val)),
                        None => Response::Error("Key not found".to_string()),
                    }
                }
            }

            Command::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                handle_append_entries(
                    &node, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit,
                )
                .await
            }

            Command::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                let (our_last_index, our_last_term) = {
                    let log = node.log.lock().await;
                    (log.last_index(), log.last_term())
                };

                let (vote_granted, needs_save) = node.decide_vote(
                    &candidate_id,
                    term,
                    last_log_term,
                    last_log_index,
                    our_last_term,
                    our_last_index,
                );

                // The vote has to be on disk before the candidate can count it,
                // or a crash lets us vote twice in one term.
                if needs_save {
                    node.save_or_warn("recording a vote").await;
                }

                let current_term = node.term();
                if vote_granted {
                    node.touch_heartbeat();
                    println!("Granted vote for term {}.", term);
                } else {
                    println!("Rejected vote request for term {} (local term is {}).", term, current_term);
                }

                Response::VoteResponse { term: current_term, vote_granted }
            }

            Command::NoOp => Response::Ok,
        };

        let payload = match bincode::serialize(&response) {
            Ok(payload) => payload,
            Err(e) => {
                eprintln!("Could not serialize a response for {}: {}", addr, e);
                return;
            }
        };
        let len_bytes = (payload.len() as u32).to_be_bytes();

        if stream.write_all(&len_bytes).await.is_err() || stream.write_all(&payload).await.is_err() {
            println!("Client {} went away before the response was sent.", addr);
            return;
        }
    }
}
