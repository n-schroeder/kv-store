use kv_store::{Command, Response, KvStore};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:7878").await.unwrap();
    println!("Async Server with WAL listening on port 7878");

    let store = KvStore::open().await;

    loop {
        let (mut stream, addr) = listener.accept().await.unwrap();
        println!("New client connected: {}", addr);

        let store_clone = store.clone();

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
                            store_clone.set(k, v).await;
                            Response::Ok
                            }
                    
                            Command::Get { key: k } => {
                            println!("The client is asking for the key '{}'", k);
                            let value = store_clone.get(&k);
                            Response::Value(value)
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