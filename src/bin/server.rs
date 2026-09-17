use kv_store::{Command, Response, KvStore, RaftState, RaftStateStore};
use rand::Rng;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, timeout, Duration};
use std::env;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const WAL_PATH: &str = "wal.log";
const RAFT_STATE_PATH: &str = "raft-state.bin";

#[derive(PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// The node's crash-surviving Raft state, plus the machinery to persist it.
///
/// The in-memory copy sits behind a `std::sync::Mutex` so the synchronous
/// compare-and-set paths stay unchanged, and it is never held across an
/// `.await`. Writes to disk go through `io_lock`, a `tokio` mutex that
/// serializes savers: each one snapshots *after* taking the lock, so two
/// concurrent saves can never persist the terms out of order and walk the
/// stored term backwards.
struct Persistent {
    state: Mutex<RaftState>,
    store: RaftStateStore,
    io_lock: tokio::sync::Mutex<()>,
}

impl Persistent {
    fn new(store: RaftStateStore, state: RaftState) -> Self {
        Persistent {
            state: Mutex::new(state),
            store,
            io_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn term(&self) -> u64 {
        self.state.lock().unwrap().current_term
    }

    /// Flushes the current in-memory state to disk. Must complete *before* the
    /// node acts on a term bump or a vote — that ordering is the entire reason
    /// this file exists.
    async fn save(&self) -> io::Result<()> {
        let _guard = self.io_lock.lock().await;
        let snapshot = self.state.lock().unwrap().clone();
        self.store.save(&snapshot).await
    }

    /// Logs a failed save without taking the node down. A node that can't
    /// persist its vote is not safe to keep voting, but it can still serve
    /// reads and follow a leader, so we surface it loudly rather than panic.
    async fn save_or_warn(&self, what: &str) {
        if let Err(e) = self.save().await {
            eprintln!("FATAL-ish: could not persist Raft state after {}: {}", what, e);
        }
    }
}

fn random_election_timeout() -> Duration {
    Duration::from_millis(rand::thread_rng().gen_range(500..1000))
}

/// Adopts `incoming_term` and steps down to `Follower` if it's newer than the locally known
/// term. Returns whether it was adopted. This is the only way a stale `Candidate`/`Leader`
/// learns it's behind, whether via a `RequestVote`, a `Heartbeat`, or a `HeartbeatAck`.
///
/// Adopting a new term also clears `voted_for`: the vote is per-term, so a fresh
/// term means the node may vote again. Callers must `save()` when this returns
/// true, before acting on the change.
fn adopt_term_if_newer(persistent: &Persistent, role: &Mutex<Role>, incoming_term: u64) -> bool {
    let mut state = persistent.state.lock().unwrap();
    if incoming_term > state.current_term {
        state.current_term = incoming_term;
        state.voted_for = None;
        let mut current_role = role.lock().unwrap();
        if *current_role != Role::Follower {
            println!("Stepping down to FOLLOWER: saw higher term {}.", incoming_term);
            *current_role = Role::Follower;
        }
        true
    } else {
        false
    }
}

/// Decides a `RequestVote` and records the vote in memory.
///
/// Raft's rule, which the old "grant iff the term was strictly higher" shortcut
/// only approximated: grant when the candidate's term is current *and* we
/// haven't already promised this term to someone else. Re-asking from the same
/// candidate is idempotent, so a retried RPC doesn't lose a vote.
///
/// Returns `(vote_granted, needs_save)`. The caller must persist before replying.
fn decide_vote(
    persistent: &Persistent,
    role: &Mutex<Role>,
    candidate_id: &str,
    candidate_term: u64,
) -> (bool, bool) {
    let adopted = adopt_term_if_newer(persistent, role, candidate_term);

    let mut state = persistent.state.lock().unwrap();
    if candidate_term != state.current_term {
        return (false, adopted);
    }

    match state.voted_for {
        None => {
            state.voted_for = Some(candidate_id.to_string());
            (true, true)
        }
        Some(ref already) if already == candidate_id => (true, adopted),
        Some(_) => (false, adopted),
    }
}

async fn request_vote(peer_addr: String, term: u64, candidate_id: String) -> Option<Response> {
    let result = timeout(Duration::from_millis(150), async {
        let mut stream = TcpStream::connect(&peer_addr).await.ok()?;

        let cmd = Command::RequestVote { term, candidate_id };
        let payload = bincode::serialize(&cmd).ok()?;
        let len_bytes = (payload.len() as u32).to_be_bytes();

        stream.write_all(&len_bytes).await.ok()?;
        stream.write_all(&payload).await.ok()?;

        let mut resp_len_buf = [0u8; 4];
        stream.read_exact(&mut resp_len_buf).await.ok()?;
        let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
        let mut resp_payload = vec![0u8; resp_len];
        stream.read_exact(&mut resp_payload).await.ok()?;

        bincode::deserialize::<Response>(&resp_payload).ok()
    }).await;

    match result {
        Ok(response) => response,
        Err(_) => {
            println!("Election: peer {} unreachable or unresponsive within timeout, no vote counted.", peer_addr);
            None
        }
    }
}

/// This node's dialable address, used as its identity in `votedFor` and (later)
/// as the redirect target a follower hands a client. Defaults to the container
/// hostname, which is what the demo's compose service names resolve to.
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

    if raw.contains(':') {
        raw
    } else {
        format!("{}:7878", raw)
    }
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

    let store = match KvStore::open(WAL_PATH).await {
        Ok(store) => store,
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
        "Restored Raft state: term {}, voted for {:?}.",
        persisted.current_term, persisted.voted_for
    );

    let role = Arc::new(Mutex::new(Role::Follower));
    let last_heartbeat = Arc::new(Mutex::new(Instant::now()));
    let persistent = Arc::new(Persistent::new(state_store, persisted));

    {
        let heartbeat_peers = peers.clone();
        let role_for_sender = role.clone();
        let persistent_for_sender = persistent.clone();
        let last_heartbeat_for_sender = last_heartbeat.clone();

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(150)).await;

                if *role_for_sender.lock().unwrap() != Role::Leader {
                    continue;
                }

                let current_term = persistent_for_sender.term();

                for peer in &heartbeat_peers {
                    let peer_addr = peer.clone();
                    let role_for_hb = role_for_sender.clone();
                    let persistent_for_hb = persistent_for_sender.clone();
                    let last_heartbeat_for_hb = last_heartbeat_for_sender.clone();

                    tokio::spawn(async move {
                        if let Ok(Ok(mut stream)) = timeout(Duration::from_millis(150), TcpStream::connect(&peer_addr)).await {
                            let hb = Command::Heartbeat { term: current_term };
                            let payload = bincode::serialize(&hb).unwrap();
                            let len_bytes = (payload.len() as u32).to_be_bytes();

                            let _ = stream.write_all(&len_bytes).await;
                            let _ = stream.write_all(&payload).await;

                            let mut resp_len_buf = [0u8; 4];
                            if stream.read_exact(&mut resp_len_buf).await.is_ok() {
                                let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
                                let mut resp_payload = vec![0u8; resp_len];
                                if stream.read_exact(&mut resp_payload).await.is_ok() {
                                    if let Ok(Response::HeartbeatAck { term: ack_term }) = bincode::deserialize::<Response>(&resp_payload) {
                                        // A follower that has seen a newer term means someone else
                                        // won an election we don't know about yet; step down. Reset the
                                        // election timer too, or it fires at once and disrupts the new leader.
                                        if adopt_term_if_newer(&persistent_for_hb, &role_for_hb, ack_term) {
                                            *last_heartbeat_for_hb.lock().unwrap() = Instant::now();
                                            persistent_for_hb.save_or_warn("stepping down on a newer heartbeat ack").await;
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }
        });
    }

    {
        let follower_timer = last_heartbeat.clone();
        let role_for_watchdog = role.clone();
        let persistent_for_election = persistent.clone();
        let election_peers = peers.clone();
        let node_id_for_election = node_id.clone();

        tokio::spawn(async move {
            let mut election_timeout = random_election_timeout();

            loop {
                sleep(Duration::from_millis(100)).await;

                if *role_for_watchdog.lock().unwrap() != Role::Follower {
                    continue;
                }

                let elapsed = follower_timer.lock().unwrap().elapsed();

                if elapsed > election_timeout {
                    // Re-roll now so the next wait window (whether this election wins, loses,
                    // or gets preempted by another node's heartbeat/vote request) uses a fresh
                    // randomized timeout, keeping repeat candidates from splitting votes forever.
                    election_timeout = random_election_timeout();

                    *role_for_watchdog.lock().unwrap() = Role::Candidate;
                    println!(
                        "Timeout: no heartbeat in {}ms. Becoming CANDIDATE.",
                        elapsed.as_millis()
                    );

                    let persistent_for_election = persistent_for_election.clone();
                    let role_for_election = role_for_watchdog.clone();
                    let last_heartbeat_for_election = follower_timer.clone();
                    let election_peers = election_peers.clone();
                    let node_id = node_id_for_election.clone();

                    tokio::spawn(async move {
                        let current_term = {
                            let mut state = persistent_for_election.state.lock().unwrap();
                            state.current_term += 1;
                            // A candidate votes for itself, and that vote is as
                            // binding as any other: if we crash and come back,
                            // we must not hand this term to someone else.
                            state.voted_for = Some(node_id.clone());
                            state.current_term
                        };

                        // Persist *before* asking anyone else for a vote.
                        persistent_for_election.save_or_warn("starting an election").await;

                        println!("Starting election for term {}.", current_term);

                        let needed = (1 + election_peers.len()) / 2 + 1;
                        let mut votes = 1; // self-vote

                        let mut set = tokio::task::JoinSet::new();
                        for peer in election_peers {
                            let candidate_id = node_id.clone();
                            set.spawn(async move { request_vote(peer, current_term, candidate_id).await });
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
                            if persistent_for_election.term() == current_term {
                                *role_for_election.lock().unwrap() = Role::Leader;
                                println!("Won election for term {} with {} votes. Becoming LEADER.", current_term, votes);
                            } else {
                                println!("Election for term {} won but term has since advanced; discarding stale result.", current_term);
                            }
                        } else {
                            println!("Election for term {} failed to reach majority. Reverting to FOLLOWER to retry after next timeout.", current_term);
                            *role_for_election.lock().unwrap() = Role::Follower;
                            *last_heartbeat_for_election.lock().unwrap() = Instant::now();
                        }
                    });
                }
            }
        });
    }

    loop {
        let (mut stream, addr) = listener.accept().await.unwrap();
        println!("New client connected: {}", addr);

        let store_clone = store.clone();
        let peers_clone = peers.clone();
        let last_heartbeat_clone = last_heartbeat.clone();
        let role_clone = role.clone();
        let persistent_clone = persistent.clone();

        tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];

                match stream.read_exact(&mut len_buf).await {
                    Ok(_) => {
                        let payload_len = u32::from_be_bytes(len_buf) as usize;
                        let mut payload_buf = vec![0u8; payload_len];

                        stream.read_exact(&mut payload_buf).await.unwrap();
                        let cmd: Command = bincode::deserialize(&payload_buf).unwrap();

                        let response = match cmd {
                            Command::Set { key: k, value: v } => {
                                println!("The client wants to store {} bytes under the key '{}'", v.len(), k);

                                match store_clone.set(k.clone(), v.clone()).await {
                                    Err(e) => {
                                        eprintln!("Write to '{}' failed: {}", k, e);
                                        Response::Error(format!("write failed: {}", e))
                                    }
                                    Ok(()) => {
                                        if *role_clone.lock().unwrap() == Role::Leader {
                                            for peer in &peers_clone {
                                                let peer_addr = peer.clone();
                                                let cmd_clone = Command::Set {
                                                    key: k.clone(),
                                                    value: v.clone()
                                                };

                                                tokio::spawn(async move {
                                                    if let Ok(Ok(mut peer_stream)) = timeout(Duration::from_millis(150), TcpStream::connect(&peer_addr)).await {
                                                        let payload = bincode::serialize(&cmd_clone).unwrap();
                                                        let len_bytes = (payload.len() as u32).to_be_bytes();

                                                        let _ = peer_stream.write_all(&len_bytes).await;
                                                        let _ = peer_stream.write_all(&payload).await;

                                                        let mut resp_len_buf = [0u8; 4];
                                                        if peer_stream.read_exact(&mut resp_len_buf).await.is_ok() {
                                                            let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
                                                            let mut resp_payload = vec![0u8; resp_len];
                                                            let _ = peer_stream.read_exact(&mut resp_payload).await;
                                                        }
                                                    } else {
                                                        println!("Replication failed: Node {} is unreachable.", peer_addr);
                                                    }
                                                });
                                            }
                                        }
                                        Response::Ok
                                    }
                                }
                            }

                            Command::Get { key: k } => {
                                println!("The client is asking for the key '{}'", k);
                                match store_clone.get(&k) {
                                    Some(val) => Response::Value(Some(val)),
                                    None => Response::Error("Key not found".to_string()),
                                }
                            }

                            Command::Heartbeat { term: incoming_term } => {
                                let adopted = adopt_term_if_newer(&persistent_clone, &role_clone, incoming_term);
                                if adopted {
                                    persistent_clone.save_or_warn("adopting a newer term from a heartbeat").await;
                                }
                                let local_term = persistent_clone.term();

                                if incoming_term < local_term {
                                    // Stale leader from an old term; ignore for timeout purposes
                                    // so it can't suppress a legitimate election.
                                    println!("Ignoring stale heartbeat for term {} (local term is {}).", incoming_term, local_term);
                                } else {
                                    if !adopted {
                                        // Term was already current; if we were still Candidate for
                                        // it, someone else's election won it first.
                                        let mut current_role = role_clone.lock().unwrap();
                                        if *current_role == Role::Candidate {
                                            println!("Stepping down to FOLLOWER: heartbeat for current term {} from elected leader.", incoming_term);
                                            *current_role = Role::Follower;
                                        }
                                    }
                                    *last_heartbeat_clone.lock().unwrap() = Instant::now();
                                    println!("Received heartbeat for term {}.", incoming_term);
                                }

                                Response::HeartbeatAck { term: local_term }
                            }

                            Command::RequestVote { term: incoming_term, candidate_id } => {
                                let (vote_granted, needs_save) =
                                    decide_vote(&persistent_clone, &role_clone, &candidate_id, incoming_term);

                                // The vote has to be on disk before the candidate
                                // can count it, or a crash lets us vote twice.
                                if needs_save {
                                    persistent_clone.save_or_warn("recording a vote").await;
                                }

                                let current_term = persistent_clone.term();

                                if vote_granted {
                                    *last_heartbeat_clone.lock().unwrap() = Instant::now();
                                    println!("Granted vote for term {}.", incoming_term);
                                } else {
                                    println!("Rejected vote request for term {} (local term is {}).", incoming_term, current_term);
                                }

                                Response::VoteResponse { term: current_term, vote_granted }
                            }
                        };

                        let payload = bincode::serialize(&response).unwrap();
                        let len_bytes = (payload.len() as u32).to_be_bytes();

                        stream.write_all(&len_bytes).await.unwrap();
                        stream.write_all(&payload).await.unwrap();
                    }
                    Err(_) => {
                        println!("Client {} disconnected gracefully.", addr);
                        break;
                    }
                }
            }
        });
    }
}
