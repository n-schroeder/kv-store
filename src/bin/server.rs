use kv_store::{Command, Response, KvStore};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, timeout, Duration};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

async fn request_vote(peer_addr: String, term: u64) -> Option<Response> {
    let mut stream = match timeout(Duration::from_millis(150), TcpStream::connect(&peer_addr)).await {
        Ok(Ok(s)) => s,
        _ => {
            println!("Election: peer {} unreachable, no vote counted.", peer_addr);
            return None;
        }
    };

    let cmd = Command::RequestVote { term };
    let payload = bincode::serialize(&cmd).ok()?;
    let len_bytes = (payload.len() as u32).to_be_bytes();

    stream.write_all(&len_bytes).await.ok()?;
    stream.write_all(&payload).await.ok()?;

    let mut resp_len_buf = [0u8; 4];
    stream.read_exact(&mut resp_len_buf).await.ok()?;
    let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
    let mut resp_payload = vec![0u8; resp_len];
    stream.read_exact(&mut resp_payload).await.ok()?;

    bincode::deserialize(&resp_payload).ok()
}

#[tokio::main]
async fn main() {
    let peers_env = env::var("PEERS").unwrap_or_default();
    let peers: Vec<String> = peers_env
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let listener = TcpListener::bind("0.0.0.0:7878").await.unwrap();
    println!("Async Server with WAL listening on port 7878");
    let store = KvStore::open().await;

    println!("Server Booted.");
    println!("Role: FOLLOWER");
    println!("Peers: {:?}", peers);

    let role = Arc::new(Mutex::new(Role::Follower));
    let last_heartbeat = Arc::new(Mutex::new(Instant::now()));
    let term = Arc::new(Mutex::new(0u64));

    {
        let heartbeat_peers = peers.clone();
        let role_for_sender = role.clone();

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(150)).await;

                if *role_for_sender.lock().unwrap() != Role::Leader {
                    continue;
                }

                for peer in &heartbeat_peers {
                    let peer_addr = peer.clone();

                    tokio::spawn(async move {
                        if let Ok(Ok(mut stream)) = timeout(Duration::from_millis(150), TcpStream::connect(&peer_addr)).await {
                            let hb = Command::Heartbeat;
                            let payload = bincode::serialize(&hb).unwrap();
                            let len_bytes = (payload.len() as u32).to_be_bytes();

                            let _ = stream.write_all(&len_bytes).await;
                            let _ = stream.write_all(&payload).await;

                            let mut resp_len_buf = [0u8; 4];
                            if stream.read_exact(&mut resp_len_buf).await.is_ok() {
                                let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
                                let mut resp_payload = vec![0u8; resp_len];
                                let _ = stream.read_exact(&mut resp_payload).await;
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
        let term_for_election = term.clone();
        let election_peers = peers.clone();

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(100)).await;

                if *role_for_watchdog.lock().unwrap() != Role::Follower {
                    continue;
                }

                let elapsed = follower_timer.lock().unwrap().elapsed();

                if elapsed > Duration::from_millis(500) {
                    *role_for_watchdog.lock().unwrap() = Role::Candidate;
                    println!(
                        "Timeout: no heartbeat in {}ms. Becoming CANDIDATE (election not yet implemented).",
                        elapsed.as_millis()
                    );

                    let term_for_election = term_for_election.clone();
                    let role_for_election = role_for_watchdog.clone();
                    let election_peers = election_peers.clone();

                    tokio::spawn(async move {
                        let current_term = {
                            let mut t = term_for_election.lock().unwrap();
                            *t += 1;
                            *t
                        };

                        println!("Starting election for term {}.", current_term);

                        let needed = (1 + election_peers.len()) / 2 + 1;
                        let mut votes = 1; // self-vote

                        let mut set = tokio::task::JoinSet::new();
                        for peer in election_peers {
                            set.spawn(async move { request_vote(peer, current_term).await });
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
                            if *term_for_election.lock().unwrap() == current_term {
                                *role_for_election.lock().unwrap() = Role::Leader;
                                println!("Won election for term {} with {} votes. Becoming LEADER.", current_term, votes);
                            } else {
                                println!("Election for term {} won but term has since advanced; discarding stale result.", current_term);
                            }
                        } else {
                            println!("Election for term {} failed to reach majority. Remaining CANDIDATE (no retry).", current_term);
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
        let term_clone = term.clone();

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
                                store_clone.set(k.clone(), v.clone()).await;

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
                    
                            Command::Get { key: k } => {
                                println!("The client is asking for the key '{}'", k);
                                match store_clone.get(&k) {
                                    Some(val) => Response::Value(Some(val)),
                                    None => Response::Error("Key not found".to_string()),
                                }
                            }

                            Command::Heartbeat => {
                                *last_heartbeat_clone.lock().unwrap() = Instant::now();
                                println!("Received heartbeat");
                                Response::Ok
                            }

                            Command::RequestVote { term: incoming_term } => {
                                let mut local_term = term_clone.lock().unwrap();
                                let vote_granted = incoming_term > *local_term;

                                if vote_granted {
                                    *local_term = incoming_term;
                                }
                                let current_term = *local_term;
                                drop(local_term);

                                if vote_granted {
                                    let mut current_role = role_clone.lock().unwrap();
                                    if *current_role != Role::Follower {
                                        println!("Stepping down to FOLLOWER: saw higher term {} in RequestVote.", incoming_term);
                                        *current_role = Role::Follower;
                                    }
                                    drop(current_role);
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