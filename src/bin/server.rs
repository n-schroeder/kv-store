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

        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).unwrap();
        let cmd: Command = bincode::deserialize(&buffer).unwrap();

        match cmd {
            Command::Set { key: k, value: v } => {
            println!("The client wants to store {} bytes under the key '{}'", v.len(), k);
            store.set(k, v);
            let response = Response::Ok;
            let response_bytes = bincode::serialize(&response).unwrap();
            stream.write_all(&response_bytes).unwrap();
            }
    
            Command::Get { key: k } => {
            println!("The client is asking for the key '{}'", k);
            let value = store.get(&k);
            let response = Response::Value((value));
            let response_bytes = bincode::serialize(&response).unwrap();
            stream.write_all(&response_bytes).unwrap();
            }
        }
    }
}