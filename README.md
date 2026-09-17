# kv-store

A small async key-value store written in Rust on top of tokio. Writes go into a
checksummed write-ahead log that doubles as a Raft replicated log, and the nodes
in a cluster elect a leader and replicate to a majority before acknowledging
anything. Once a client sees `Ok`, that write survives any single node dying.
I'm building it as a learning project for distributed-systems fundamentals, so
the code favors being easy to read over being feature-complete.

## See leader election in action

The fastest way to see what this project does is the election demo. It starts
a 5-node cluster in Docker, breaks it in nine different ways, and shows you
what the nodes do about it:

```bash
./demo/election-demo.sh
```

All you need is Docker with the Compose plugin; Rust doesn't have to be
installed. The script runs on macOS and Linux. On Windows, run it from WSL2
(which Docker Desktop already uses) or Git Bash. The first run compiles the
server inside Docker, which can take a few minutes. Later runs start almost
immediately.

The demo pauses before each scenario so you can read what's about to happen.
For each one it:

1. explains what's about to happen and what the cluster should do,
2. makes it happen,
3. prints the election-related log lines from all five nodes as one timeline,
   with the constant heartbeat traffic filtered out, and
4. checks that the cluster ended up in the right state, then prints PASS or
   FAIL.

| # | Scenario | What it shows |
|---|---|---|
| 1 | The cluster starts up | Five followers with no configured leader elect one on their own |
| 2 | Kill the leader | A survivor times out and takes over in a higher term |
| 3 | Bring the old leader back | It rejoins as a follower and adopts the current term |
| 4 | Kill the leader and a follower | Three of five nodes is still a majority, so a new leader is elected |
| 5 | Lose the majority | Two nodes can't win an election, so they keep retrying as the term climbs |
| 6 | Restart the dead nodes | The cluster gets its majority back and settles on one leader |
| 7 | Cut the leader off from the network, then reconnect it | The rest of the cluster moves on without it; once reconnected, it sees the newer term and steps down |

Here's part of scenario 7 from a real run:

```
  18:40:58.079  node2  Timeout: no heartbeat in 614ms. Becoming CANDIDATE.
  18:40:58.079  node2  Starting election for term 16.
  18:40:58.080  node5  Granted vote for term 16.
  18:40:58.080  node1  Granted vote for term 16.
  18:40:58.080  node4  Granted vote for term 16.
  18:40:58.231  node2  Election: peer node3:7878 unreachable or unresponsive within timeout, no vote counted.
  18:40:58.231  node2  Election for term 16: received 4 of 3 votes needed.
  18:40:58.231  node2  Won election for term 16 with 4 votes. Becoming LEADER.

Right now node2 leads term 16, while node3 is cut off and still thinks it leads term 15.

  18:40:59.840  node3  Stepping down to FOLLOWER: saw higher term 16.
  18:40:59.840  node3  Received heartbeat for term 16.

✓ PASS  node3 stepped down after reconnecting, and node2 is the only leader (term 16).
```

Options:

- `--no-pause` runs every scenario back to back without waiting for Enter.
- `--keep` leaves the cluster running afterward so you can poke at it.
  Otherwise it's removed when the demo exits, including if you press Ctrl-C.

The demo doesn't publish any ports on your machine. It leaves behind one
Docker image, `kv-election-demo`, which you can remove with
`docker image rm kv-election-demo`.

Which node wins each election is random, so the node names and terms will be
different on every run. That's expected.

## Quick start

```bash
cargo build --release --bin server   # build the server binary (what the Dockerfile builds)
cargo run --bin server               # run a node locally (listens on 0.0.0.0:7878, writes ./wal.log)
cargo run --bin client               # load test: 100 concurrent SETs, then 1 GET, against localhost:7878
cargo test                           # run the tests (currently just test_serialization in src/lib.rs)
```

A node finds the rest of the cluster through the `PEERS` environment variable,
a comma-separated list of `host:port` addresses that doesn't include the node
itself:

```bash
PEERS=node2:7878,node3:7878 cargo run --bin server
```

The port (`7878`) and the server's WAL path (`wal.log`) are hardcoded, so you can only run
one node per machine. To run a multi-node cluster on one machine, put each
node in its own Docker container. `demo/compose.yaml` does exactly that for
five nodes.

There's no lint config and no `tests/` directory. All tests live inline in
`src/lib.rs` under `#[cfg(test)]`.

## Project layout

| File | What it does |
|---|---|
| `src/lib.rs` | `Wal` (checksummed append-only log), `KvStore` (in-memory map + WAL), `RaftStateStore` (durable term/vote), and the `Command` / `Response` message types |
| `src/bin/server.rs` | Networking, leader election, heartbeats, and replication |
| `src/bin/client.rs` | A load-testing tool, not a client library |
| `demo/election-demo.sh` | Runs the leader election demo described above |
| `demo/compose.yaml` | The 5-node Docker Compose cluster the demo uses |

## Messages and framing

Everything a node sends or stores is one of two enums:

- `Command`: `Set { key, value }`, `Get { key }`, `NoOp`,
  `AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit }`,
  `RequestVote { term, candidate_id, last_log_index, last_log_term }`
- `Response`: `Ok`, `Value(Option<Vec<u8>>)`, `Error(String)`,
  `VoteResponse { term, vote_granted }`,
  `AppendEntriesAck { term, success, match_index, conflict_index }`,
  `NotLeader { leader }`

A WAL record's payload is a `LogEntry { term, index, command }` rather than a
bare `Command`. Term and index live inside the payload, not the record frame:
recovery decodes every record anyway, and the log keeps a byte offset per index
in memory, so nothing needs those fields before decoding.

Both are serialized with bincode. The framing around that payload differs by
destination:

- **On the network** (client traffic and node-to-node traffic): a 4-byte
  big-endian `u32` length prefix, then the payload.
- **On disk** (`wal.log`): an 8-byte file header `KVWALOG\x02`, then records of
  a 4-byte big-endian length, a 4-byte big-endian CRC32 of the payload, then
  the payload.

The disk format carries a checksum because a file outlives the process that
wrote it and has to be defended against half-written records; a TCP stream
doesn't. The header's trailing byte is a format version, so a log written by
an incompatible build is rejected at startup instead of misread.

One bincode encoding still means the enums are shared: change either one and
the network code in `server.rs` / `client.rs` and the WAL code in `lib.rs` have
to change with it, and every node in a cluster has to run the same build, since
old and new binaries can't decode each other's messages.

## Storage: `Wal`, `RaftLog` and `KvStore`

Three layers, each with one job:

- **`Wal`** owns the file and all of the framing. It appends a record and
  `fsync`s it before returning — one `fsync` per write, deliberately: a write
  that returned has reached the disk, not just the page cache. It knows nothing
  about what a payload means.
- **`RaftLog`** reads those records as `LogEntry`s, and is the thing Raft
  operates on: `last_index`, `term_at`, `entries_from`, `try_append`,
  `truncate_from`. It keeps every entry and its byte offset in memory, so
  turning an index into a file position is free.
- **`KvStore`** is the state machine: a `HashMap<String, Vec<u8>>` behind an
  `Arc<RwLock<_>>` and nothing else. It is deliberately *not* durable. The log
  is the durable thing, and the map is only ever a replay of the log's
  committed prefix.

On startup a node replays its log and applies entries up to the commit index it
had persisted. Committed entries are never truncated, so that prefix is always
valid; the uncommitted tail stays in the log unapplied, ready to be discarded if
it turns out to have diverged from the leader's.

Nothing ever compacts the log, so it grows forever and is fully replayed on
every boot. I/O errors are returned rather than unwrapped, so a failed write
becomes an error to the client instead of a dead connection task.

### Surviving a crash mid-write

A process killed partway through an append leaves a record with a truncated
payload, or a length prefix with nothing behind it. Recovery reads records until
one is short, absurdly long, or fails its CRC, then truncates the file at the
end of the last intact record and carries on. A half-written record costs you
that one write; it doesn't stop the node from booting.

The checksum catches bit-rot and torn payloads. It cannot catch a *stale*
record — one that was logically truncated but whose bytes survived. Those
records are perfectly intact and checksum fine; what gives them away is their
index. So recovery also checks that indexes run 1, 2, 3, … with no gaps and that
terms never decrease, and truncates at the first entry that breaks either rule.
That is what a crash partway through a log truncation looks like.

### Raft state that outlives the process: `raft-state.bin`

`currentTerm` and `votedFor` are written to `raft-state.bin` next to the WAL,
and flushed **before** the node acts on them: before it answers a `RequestVote`,
and before it asks for votes in a new term. Without that ordering, a node that
crashed after voting would come back at term 0 and could vote a second time in a
term it had already promised away — which elects two leaders at once.

The file is written by writing a temp file, `fsync`ing it, renaming it over the
real one, and `fsync`ing the directory, so a torn state file is never
observable. It also carries `commitIndex`. Raft treats that as volatile, but
persisting it is what lets a node that restarts alone serve the data it already
has instead of waiting to be told what it already knows. It is only ever a
lower bound, and it is clamped to the log's length on boot.

A node's identity in `votedFor` comes from `NODE_ID`, its own dialable address
(`node3:7878`), defaulting to the container hostname.

## Leader election

Every node starts as a `Follower`. There's no configured leader. The cluster
picks one on its own and picks a new one when the current leader goes away.

Each node tracks three pieces of in-memory state:

- **`role`**: `Follower`, `Candidate`, or `Leader`
- **`term`**: a number that goes up by one with every election. Terms let nodes
  tell current information from stale information.
- **`last_heartbeat`**: when this node last heard from a legitimate leader

None of these are saved to disk, so a restarted node comes back as a
`Follower` in term 0. All three use `std::sync::Mutex`, because each access is a
quick read or write that's never held across an `.await`.

### Heartbeats

Every 150ms, the leader sends each peer an `AppendEntries` carrying whatever
entries that peer is missing. When a peer is caught up there are no entries to
send and the message is empty — that empty `AppendEntries` *is* the heartbeat,
so replication and liveness ride on the same message. When a node gets one:

- **The heartbeat's term is newer than its own.** The node adopts that term,
  steps down to `Follower` if it wasn't one already, and resets its heartbeat
  timer.
- **The heartbeat's term matches its own.** The heartbeat is from the current
  leader, so the node resets its heartbeat timer. If the node was a `Candidate`
  in this term, someone else already won, so it steps down to `Follower`.
- **The heartbeat's term is older than its own.** The heartbeat comes from a
  leader that has already been replaced. The node ignores it and doesn't reset
  its timer, so a stale leader can't keep the rest of the cluster from holding
  an election.

Either way, the node replies with an `AppendEntriesAck` carrying its own term.
If that term is higher than the leader's, the leader learns it's been
replaced and steps down. This is how a leader that was cut off from the
cluster, or just fell behind, finds out an election happened without it.

The leader always waits for the ack before closing the connection. Closing
without reading the reply used to race the peer's response and crash the
peer's connection task with `BrokenPipe`.

### Starting an election

A watchdog on each node checks every 100ms. If the node is a `Follower` and
hasn't heard a heartbeat within its **election timeout**, it becomes a
`Candidate` and starts an election:

1. Increment `term` and vote for itself.
2. Send `RequestVote { term }` to every peer at the same time.
3. Count the votes. A majority of the whole cluster wins, counting the node
   itself: 2 of 3 nodes, 3 of 5.
4. If it has a majority *and* its term hasn't changed since the election
   started, become `Leader` and start sending heartbeats.

The term check in step 4 prevents a real bug. While votes are still coming
in, the node may already have moved to a newer term. If a late win were still
applied, it would overwrite that newer state. Before the check was added, a
3-node test cluster briefly had two leaders at once.

### Voting

A node grants a vote when three things hold:

1. **The candidate's term is current.** An older term is refused; a newer one is
   adopted first, which also steps the node down and clears its stored vote.
2. **It hasn't already voted this term** — or it's the same candidate asking
   again, so a retried request doesn't lose a vote it already granted. The vote
   is written to `raft-state.bin` and `fsync`ed *before* the reply goes out, so
   a crash can't let the node vote twice in one term.
3. **The candidate's log is at least as complete as its own** — a later last
   term wins, and on a tie the longer log wins.

That third rule is the election restriction, and it's what keeps
acknowledged writes safe. A write is acknowledged once a majority holds it, so
any majority — and therefore any winning candidate — must contain at least one
node that has it. Without the check, a node missing committed entries could win
and overwrite them, and the `Ok` the client already saw would have been a lie.

A node that grants a vote also resets its heartbeat timer.

### When an election fails

If a candidate doesn't get a majority, because too many nodes are down or the
vote split, it goes back to being a `Follower` and resets its heartbeat timer.
Once its election timeout runs out again, the watchdog starts a new election
in the next term. The node keeps retrying until one succeeds or a leader shows
up.

The election timeout is picked at random between **500ms and 1000ms**, and a
new value is picked every time it fires. Without the randomness, nodes that
lost their leader at the same moment would all time out together, split the
vote, and repeat that forever. With it, one node usually times out first and
wins before the others try.

### Stepping down

The rule "if you see a higher term, adopt it and become a `Follower`" lives in
a single helper, `adopt_term_if_newer`, which runs in three places:

- when a node receives a `RequestVote`
- when a node receives an `AppendEntries`
- when the leader reads back an `AppendEntriesAck`

### Timeouts at a glance

| Setting | Value | Why |
|---|---|---|
| Heartbeat interval | 150ms | Comfortably shorter than the smallest election timeout |
| Watchdog check interval | 100ms | How often a follower checks whether its leader has gone quiet |
| Election timeout | random 500–1000ms | Randomized so candidates don't keep splitting the vote |
| Peer connect timeout | 150ms | Makes an unreachable peer fail fast |
| `RequestVote` round trip | 150ms total | Covers connect, send, and reply, so one slow peer can't stall an election |

The connect timeout exists for a reason. Without it, connecting to a dead peer
could hang longer than the election timeout. In a 3-node test with one node
killed, the two survivors kept starting overlapping elections and churned
through 10 terms in about 5 seconds without ever settling on a leader.

### What it looks like

To watch all of this on your own machine, run the
[election demo](#see-leader-election-in-action). Here's the start of a 5-node
cluster, with heartbeat lines filtered out:

```
node1  | Granted vote for term 1.
node5  | Granted vote for term 1.
node4  | Granted vote for term 1.
node2  | Granted vote for term 1.
node3  | Starting election for term 1.
node3  | Election for term 1: received 5 of 3 votes needed.
node3  | Won election for term 1 with 5 votes. Becoming LEADER.
```

And here's a node that can't reach any of its peers, retrying with a new
timeout each round:

```
Timeout: no heartbeat in 1015ms. Becoming CANDIDATE.
Starting election for term 1.
Election for term 1: received 1 of 2 votes needed.
Election for term 1 failed to reach majority. Reverting to FOLLOWER to retry after next timeout.
Timeout: no heartbeat in 761ms. Becoming CANDIDATE.
Starting election for term 2.
...
```

## Replication

The WAL is the Raft log: every record is a `LogEntry { term, index, command }`,
indexed from 1. Writes and reads both go through the leader; a follower asked
for either replies `NotLeader { leader }` with the leader's address, and the
bundled client follows that redirect.

### Committing a write

1. The leader appends the entry to its own log and `fsync`s it.
2. It sends the entry to every peer in an `AppendEntries`, alongside the index
   and term of the entry immediately before it.
3. A peer appends only if its own log matches at that preceding entry. That
   check, applied inductively, means a matching entry implies matching history.
4. Once a majority (the leader included) holds the entry, it is **committed**:
   the leader applies it to the map and only then replies `Ok`.

If the entry doesn't commit within two seconds, the client gets an error saying
so — worded as *unknown*, not *failed*, because the entry may still commit
afterwards. That is the honest answer; claiming either outcome would be a guess.

### Catching a node up

The leader keeps a `nextIndex` per peer, guessed optimistically at its own last
index plus one. A peer that disagrees replies with the first index of the term
they disagree on, so the leader backs up a whole term per round trip rather than
one entry at a time, then ships everything from there. A node that was down for
a thousand writes catches up on its own, in batches of 64, with nothing to
replay by hand.

If a peer holds entries the cluster never committed — a partitioned leader that
accepted writes it could never replicate — those entries conflict with the real
leader's, and the follower truncates them before appending the real ones. They
were never acknowledged to any client, so nothing that was promised is lost.

### Why the leader writes a no-op when it's elected

Raft only lets a leader commit an entry from its *own* term by counting
replicas; an older entry can sit on a majority and still not be committed. So a
new leader appends one empty entry of its own term immediately. Committing that
commits everything before it, which would otherwise wait for the next client
write that might never come.

## Client

`client.rs` is a small command-line client, and the demo uses it to read and
write inside the cluster network:

```bash
client <addr> set <key> <value>
client <addr> get <key>
client                          # load test: 100 concurrent Sets, then one Get
```

It follows a `NotLeader` redirect once, so pointing it at any node works. It is
a testing tool, not a client library: no connection reuse, no retry policy, no
batching.

## Deployment

Deployment is manual:

- **`./deploy_local.sh`** rebuilds the Docker image and runs it on this machine
  as `local_node` with `PEERS=192.168.1.120:7878`.
- **`./deploy_pi.sh`** cross-compiles for `linux/arm64` with `docker buildx`,
  copies the image to a Raspberry Pi (`node0` in SSH config) over `scp`, and
  restarts the `rpi-server` container there.

Both scripts bind-mount a host directory to `/app`, the container's working
directory, which is what keeps `wal.log` around across restarts and redeploys.

## Known limitations

An acknowledged write now survives any single node dying, and a node that was
down rejoins and converges on its own. What's left is mostly about scale and
operations rather than correctness.

- **The log is never compacted.** `wal.log` grows without bound and is replayed
  in full on every boot. No snapshotting, so catching up a node that was down
  for a long time means shipping every entry it missed.
- **Cluster membership is fixed.** `PEERS` is read once at startup. Adding or
  removing a node means restarting the cluster, and there's no joint-consensus
  configuration change.
- **Reads are leader-local, not linearizable.** A leader that has just been
  partitioned off doesn't know it yet and will keep answering reads from its own
  map for up to an election timeout. Fixing it properly needs a read index or a
  leader lease.
- **One `fsync` per write, no group commit.** Concurrent writes each pay their
  own flush instead of sharing one.
- **A write that times out is reported as unknown.** That's honest, but there's
  no request ID or dedup, so a client that retries can apply the same write
  twice. Writes are idempotent per key, so this is survivable, not correct.
- **In the 2-node laptop+Pi deployment, nothing commits while either node is
  down.** A majority of two is two. That's correct Raft, not a bug, but it means
  that topology has no fault tolerance for writes — it needs a third node.
- **`deploy_pi.sh` sets no `PEERS`,** so the Pi believes it's a single-node
  cluster and elects itself leader of its own term, independently of the laptop.
  Pre-existing, and worth fixing before trusting that deployment.

Older gaps, now closed: writes acknowledged before replication, log entries
without a term or index, no election restriction, nodes that never caught up,
replicated writes arriving out of order, followers accepting client writes,
non-durable terms, and a torn WAL record crashing startup.

<details>
<summary>The former list, for reference</summary>

- **Writes are acknowledged before they're replicated.** If the leader dies
  right after replying `Ok`, the write may exist only on the old leader.
- **Log entries have no term or index.** Nodes can't compare logs, figure out
  what's committed, or fix a log that has diverged.
- **Votes don't check how complete the candidate's log is.** Raft only votes
  for a candidate whose log is at least as up to date. This doesn't matter
  yet, but it will once real replication exists.
- **Nodes that fall behind never catch up.** A node that was down misses those
  writes permanently.
- **Replicated writes can arrive out of order.** Each `Set` goes out over its
  own connection, so two quick writes to the same key can reach a follower in
  the wrong order.
- **Followers accept writes from clients** and apply them locally without
  forwarding them to the leader.
- **Reads can be stale,** because any node answers `Get` from its own data.
- **A half-written WAL record crashes startup.**
- **`flush()` isn't `fsync`,** so a power loss can drop recent writes.

</details>
- **The WAL is never compacted.** It grows forever and is fully replayed on
  every boot.
