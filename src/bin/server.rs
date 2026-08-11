use std::{io::Write, net::TcpListener};
use std::io::Read;
use kv_store::{Command, KvStore, Response};

fn main() {
    let store = KvStore::new();
    let listner = TcpListener::bind("127.0.0.1:7878").unwrap();
    println!("KV Server listening on 127.0.0.1:7878...");

    for stream_result in listner.incoming() {
        let mut stream = stream_result.unwrap();
        println!("Client connected.");

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();

        let len_payload = u32::from_be_bytes(len_buf) as usize;
        let mut payload_buf = vec![0u8, len_payload as u8];
        stream.read_exact(&mut payload_buf).unwrap();

        let cmd: Command = bincode::deserialize(&payload_buf).unwrap();

        let response = match cmd {
            Command::Set { key: k, value: v } => {
            println!("The client wants to store {} bytes under the key '{}'", v.len(), k);
            store.set(k, v);
            Response::Ok
            }
    
            Command::Get { key: k } => {
            println!("The client is asking for the key '{}'", k);
            let value = store.get(&k);
            Response::Value(value)
            }
        };

    let payload = bincode::serialize(&response).unwrap();
    let len_bytes = (payload.len() as u32).to_be_bytes();

    stream.write_all(&len_bytes).unwrap();
    stream.write_all(&payload).unwrap();
    }
}