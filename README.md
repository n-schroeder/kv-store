# kv-store

A small async key-value store in Rust (tokio), with a write-ahead log for
durability and a Raft-lite leader-election layerr. Built
as a learning project for distributed-systems fundamentals.

## Quick start

```bash
cargo build --release --bin server   # build the server binary (what the Dockerfile builds)
cargo run --bin server               # run the server locally (listens on 0.0.0.0:7878, writes ./wal.log)
cargo run --bin client               # load-test client (100 concurrent SETs + 1 GET against localhost:7878)
cargo test                           # run tests (currently just src/lib.rs's test_serialization)
```

There's no lint config (no clippy/rustfmt CI step) and no `tests/`
integration directory — all tests live inline in `src/lib.rs` under
`#[cfg(test)]`.

## Deployment

Deployment is manual, not CI-driven:

- `./deploy_local.sh` — rebuilds the Docker image and runs it locally
  (`laptop-leader`, `PEERS=192.168.1.120:7878`).
- `./deploy_pi.sh` — cross-compiles for `linux/arm64` via `docker buildx`,
  ships the tarball to a Raspberry Pi host (`node0` in SSH config) over
  `scp`, and restarts the `rpi-server` container there as a follower.

Both deployments bind-mount a host directory to `/app` (the container's
`WORKDIR`), which is what makes `wal.log` persist across container
restarts/redeploys. The WAL path itself is a hardcoded relative literal
(`"wal.log"`), not configurable via env var or flag.

## Architecture

The codebase is a library (`src/lib.rs`) plus two binaries
(`src/bin/server.rs`, `src/bin/client.rs`).

### One wire format for everything

`Command` (`Set { key, value }`, `Get { key }`, `Heartbeat`,
`RequestVote { term }`) and `Response` (`Ok`, `Value(Option<Vec<u8>>)`,
`Error(String)`, `VoteResponse { term, vote_granted }`) are bincode-serialized
and framed identically wherever they show up: over the network
(client-server, and leader-follower replication/heartbeats/elections) and
on disk in `wal.log` — a 4-byte big-endian `u32` length prefix followed by
the bincode payload.

Reusing one encoding for both the wire protocol and the on-disk log keeps
the serialization code in one place. The cost is coupling: changing either
enum means keeping the network handling in `server.rs`/`client.rs` and the
WAL read/write logic in `lib.rs` in sync, and it breaks compatibility with
any existing `wal.log` file. `KvStore::open()`'s replay loop only ever acts
on `Command::Set` (via `if let`), so the other variants — including
`RequestVote`, which is never written to the WAL — are safely ignored
during recovery.

### `KvStore`: the storage engine (`src/lib.rs`)

Holds an in-memory `HashMap<String, Vec<u8>>` behind `Arc<RwLock<_>>`, plus
an `Arc<Mutex<tokio::fs::File>>` for the WAL.

- **`KvStore::open()`** does recovery: reads `wal.log` sequentially,
  replaying every `Command::Set` record into the map (last write for a key
  wins), then reopens the file in append mode for subsequent writes. There
  is no checkpointing or compaction — the file grows unboundedly, and a
  full replay happens on every startup.
- **`KvStore::set()`** writes the record to the WAL and calls `flush()`
  (not `fsync`/`sync_all`) *before* mutating the in-memory map, then drops
  the WAL lock before taking the map's write lock — so the two locks are
  never held simultaneously. This ordering means a crash between the flush
  and the map mutation can't lose an acknowledged write, but `flush()`
  alone doesn't guarantee the data survived a power loss (only `fsync`
  does) — a deliberate durability/throughput tradeoff.
- **`KvStore::get()`** only touches the in-memory map; it never reads or
  writes the WAL.
- Nearly all I/O uses `.unwrap()`. I/O errors panic the task rather than
  propagating — including in the recovery loop in `open()`. Simple, and
  appropriate for a single-purpose service where a broken disk should be
  loud, not swallowed.

### `server.rs`: networking, replication, and Raft-lite (`src/bin/server.rs`)

Every node boots as `Follower`; there's no `IS_LEADER` env var — the only
thing read from the environment is `PEERS` (comma-separated `host:port`).
Role, a `last_heartbeat: Arc<Mutex<Instant>>` timestamp, and a
`term: Arc<Mutex<u64>>` counter are shared, process-local state — not
persisted, not part of `KvStore` — all using `std::sync::Mutex` since every
access is a synchronous compare-or-set that's never held across an
`.await`.

Three background mechanisms drive the `Role` state machine
(`Follower` / `Candidate` / `Leader`), all running unconditionally on every
node and gating their behavior on the current role:

1. **Heartbeat sender** — ticks every 150ms; while `Role::Leader`, fires
   `Command::Heartbeat` at every peer, reading back the `Response` before
   dropping the connection. That read-back is required: a fire-and-forget
   write races the peer's own reply and previously crashed the peer's
   connection task with `BrokenPipe`.
2. **Follower watchdog** — ticks every 100ms; while `Role::Follower`, if
   `last_heartbeat.elapsed()` exceeds 500ms it flips to `Role::Candidate`
   (logging once — the role-guard at the top of the loop means it won't
   refire) and spawns an election task: increments `term`, self-votes,
   fires `Command::RequestVote { term }` at every peer concurrently via a
   `tokio::task::JoinSet`, and if it collects a strict majority of votes
   (self included), sets `Role::Leader` — but *only if* `term` still
   equals the term the election started with. That re-check matters: a
   peer connect can stall well past when the election "should" have
   resolved, and without it a late-arriving stale win would silently
   clobber whatever the node had correctly moved on to in the meantime.
   This was verified live — a 3-node cluster with one node killed briefly
   ran two simultaneous leaders before the guard was added. If an election
   doesn't reach majority (or its win turns out stale), the node just
   stays `Candidate` forever — there's no retry/backoff loop.
3. **Per-connection handler's `RequestVote` arm** — grants a vote only if
   the incoming term is strictly higher than the locally known term. That
   single `u64` comparison *is* the "already voted this term" check:
   bumping the term and granting the vote happen in the same atomic
   critical section, so no separate voted-flag is needed. Granting a vote
   also steps a stale `Candidate`/`Leader` down to `Follower` and resets
   `last_heartbeat` — this is the *only* way a stale `Leader` learns it's
   behind, since `Heartbeat` carries no term. A `Leader` that's still
   successfully heartbeating followers has no way to learn a newer term
   won an election elsewhere unless that candidate's `RequestVote` reaches
   it directly. **This is an accepted split-brain gap in the current
   minimal wire format, not yet patched.**

`Set` replication is push-based and best-effort: gated on
`*role.lock().unwrap() == Role::Leader`, the leader fires an independent
connection to each peer with the same command (reading back the response
for the same `BrokenPipe`-avoidance reason as heartbeats). Replication
failures/timeouts beyond a failed connect are only logged, never retried.

**Every `TcpStream::connect` call** (`request_vote`, the heartbeat sender,
and `Set`-replication forwarding) is wrapped in a 150ms
`tokio::time::timeout`. Without this, a connect to a dead/unreachable peer
stalls significantly longer than the 500ms follower timeout — verified
live: with a 3-node cluster and one node killed, elections routinely took
longer than 500ms to resolve because they were still waiting on the dead
peer's connect, so the two live nodes kept leapfrogging each other into
ever-higher terms (10 terms churned in ~5 seconds) instead of converging on
a leader. The 150ms timeout makes an unreachable peer fail fast enough
that elections reliably resolve within a single 500ms window.

### `client.rs`: a load-testing tool, not a client library

Hardcodes `0.0.0.0:7878`, fires 100 concurrent `Set`s then one `Get`, and
prints the responses. Useful for exercising the server under concurrent
load, not meant to be depended on as an SDK.

## Known limitations

- No log compaction — `wal.log` grows forever, full replay on every
  restart.
- `flush()`-not-`fsync()` on writes trades a small durability window for
  throughput.
- Split-brain gap: a stale `Leader` that's still heartbeating successfully
  has no way to learn a newer term won an election elsewhere, since
  `Heartbeat` carries no term.
- No election retry/backoff — a `Candidate` that fails to reach majority
  stays `Candidate` forever rather than re-triggering a new round.
