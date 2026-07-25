use std::{net::TcpListener, print};
use kv_store::{KvStore, Command};

fn main() {
    let store = KvStore::new();
    let listner = TcpListener::bind("127.0.0.1:7878").unwrap();
    println!("KV Server listening on 127.0.0.1:7878...");
}