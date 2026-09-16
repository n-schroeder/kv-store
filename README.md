# kv-store

A small async key-value store written in Rust on top of tokio. Writes go
through a write-ahead log (WAL) so data survives restarts, and the nodes in a
cluster elect a leader using a stripped-down version of Raft's election
protocol. I'm building it as a learning project for distributed-systems
fundamentals, so the code favors being easy to read over being feature-complete.

## See leader election in action

The fastest way to see what this project does is the election demo. It starts
a 5-node cluster in Docker, breaks it in seven different ways, and shows you
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

The port (`7878`) and WAL path (`wal.log`) are hardcoded, so you can only run
one node per machine. To run a multi-node cluster on one machine, put each
node in its own Docker container. `demo/compose.yaml` does exactly that for
five nodes.

There's no lint config and no `tests/` directory. All tests live inline in
`src/lib.rs` under `#[cfg(test)]`.

## Project layout

| File | What it does |
|---|---|
| `src/lib.rs` | `KvStore` (in-memory map + WAL) and the `Command` / `Response` message types |
| `src/bin/server.rs` | Networking, leader election, heartbeats, and replication |
| `src/bin/client.rs` | A load-testing tool, not a client library |
| `demo/election-demo.sh` | Runs the leader election demo described above |
| `demo/compose.yaml` | The 5-node Docker Compose cluster the demo uses |

## Messages and framing

Everything a node sends or stores is one of two enums:

- `Command`: `Set { key, value }`, `Get { key }`, `Heartbeat { term }`,
  `RequestVote { term }`
- `Response`: `Ok`, `Value(Option<Vec<u8>>)`, `Error(String)`,
  `VoteResponse { term, vote_granted }`, `HeartbeatAck { term }`

Each message is serialized with bincode and framed the same way everywhere:
a 4-byte big-endian `u32` length prefix, then the payload. That goes for
client traffic, node-to-node traffic, and the records in `wal.log`.

Sharing one encoding keeps the serialization code in one place, but it couples
things together. If you change either enum, the network code in `server.rs` and
`client.rs` and the WAL code in `lib.rs` have to change with it. Existing
`wal.log` files stop being readable, and every node in a cluster has to run
the same build, since old and new binaries can't decode each other's messages.

## Storage: `KvStore`

`KvStore` keeps the data in a `HashMap<String, Vec<u8>>` behind an
`Arc<RwLock<_>>`, with the WAL file behind an `Arc<Mutex<_>>`.

- **Startup (`KvStore::open`)** reads `wal.log` from the start and replays every
  `Set` record into the map, so the last write to a key wins. It then reopens
  the file in append mode. Replay ignores every other record type. Nothing ever
  compacts the log, so it grows forever and gets fully replayed on every boot.
- **Writes (`KvStore::set`)** append the record to the WAL and call `flush()`,
  and only then update the map. The WAL lock is released before the map lock
  is taken, so the two are never held at the same time. `flush()` isn't an
  `fsync`, though, so a write can still be lost if the machine loses power
  right after it.
- **Reads (`KvStore::get`)** only look at the in-memory map.

Most I/O calls `.unwrap()`, so a disk error crashes the task instead of being
handled. That includes WAL replay at startup.

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

Every 150ms, the leader sends `Heartbeat { term }` to each peer. When a node
gets a heartbeat:

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

Either way, the node replies with `HeartbeatAck { term }` carrying its own
term. If that term is higher than the leader's, the leader learns it's been
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

A node grants a vote only if the candidate's term is **strictly higher** than
its own. Granting the vote also bumps its term to match, so a second candidate
in the same term gets turned down. That's what limits each node to one vote
per term, with no separate "voted" flag. A node that grants a vote also steps
down to `Follower` if it wasn't one, and resets its heartbeat timer.

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
- when a node receives a `Heartbeat`
- when the leader reads back a `HeartbeatAck`

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

When the leader handles a `Set`, it writes the value locally and then sends
the same `Set` to every peer, each over its own connection in a background
task. It replies `Ok` to the client right away, without waiting for any peer.
If a peer can't be reached, the failure is logged and never retried.

This is best-effort replication, not Raft's log replication. See the
limitations below for what that means in practice.

## Client

`client.rs` connects to `0.0.0.0:7878`, sends 100 concurrent `Set`s followed by
one `Get`, and prints the responses. It's for putting load on a server, not
for building on as a client library.

## Deployment

Deployment is manual:

- **`./deploy_local.sh`** rebuilds the Docker image and runs it on this machine
  as `laptop-leader` with `PEERS=192.168.1.120:7878`. Despite the name, the
  container starts as a follower like every other node. The script also still
  sets `IS_LEADER=true`, which the server no longer reads.
- **`./deploy_pi.sh`** cross-compiles for `linux/arm64` with `docker buildx`,
  copies the image to a Raspberry Pi (`node0` in SSH config) over `scp`, and
  restarts the `rpi-server` container there.

Both scripts bind-mount a host directory to `/app`, the container's working
directory, which is what keeps `wal.log` around across restarts and redeploys.

## Known limitations

Leader election is in reasonable shape. The data path isn't Raft yet, and
most of the gaps below come from that.

- **Terms aren't saved to disk.** A restarted node goes back to term 0, so it
  can vote again in a term it already voted in before the crash. That can
  produce two leaders in the same term.
- **A node that steps down can immediately start an election of its own.**
  When a leader or candidate learns about a higher term from a reply rather
  than from the new leader's heartbeat, it becomes a follower without
  resetting its election timer. If that timer has already run out, it starts
  an election right away and can take leadership from a perfectly healthy
  leader. Scenario 7 of the demo shows this in roughly half of runs: the
  reconnected node steps down and then wins the next term.
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
- **A half-written WAL record crashes startup.** The replay loop unwraps every
  read, so a record cut off by a crash stops the node from booting.
- **`flush()` isn't `fsync`,** so a power loss can drop recent writes.
- **The WAL is never compacted.** It grows forever and is fully replayed on
  every boot.
