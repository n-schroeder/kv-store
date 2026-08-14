use kv_store::{Command, Response, KvStore};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, Duration};
use std::env;

#[tokio::main]
async fn main() {
    let is_leader = env::var("IS_LEADER").unwrap_or_else(|_| "false".to_string()) == "true";

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
    println!("Role: {}", if is_leader { "LEADER" } else { "FOLLOWER" });
    println!("Peers: {:?}", peers);

    if is_leader {
        let heartbeat_peers = peers.clone();
    
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(150)).await;
                
                for peer in &heartbeat_peers {
                    let peer_addr = peer.clone();
                    
                    tokio::spawn(async move {
                        if let Ok(mut stream) = TcpStream::connect(&peer_addr).await {
                            let hb = Command::Heartbeat;
                            let payload = bincode::serialize(&hb).unwrap();
                            let len_bytes = (payload.len() as u32).to_be_bytes();
                            
                            let _ = stream.write_all(&len_bytes).await;
                            let _ = stream.write_all(&payload).await;
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

                                if is_leader {
                                    for peer in &peers_clone {
                                        let peer_addr = peer.clone();
                                        let cmd_clone = Command::Set { 
                                            key: k.clone(), 
                                            value: v.clone()
                                        };
                                        
                                        tokio::spawn(async move {
                                            if let Ok(mut peer_stream) = TcpStream::connect(&peer_addr).await {
                                                let payload = bincode::serialize(&cmd_clone).unwrap();
                                                let len_bytes = (payload.len() as u32).to_be_bytes();
                                                
                                                let _ = peer_stream.write_all(&len_bytes).await;
                                                let _ = peer_stream.write_all(&payload).await;
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
                                Response::Ok
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