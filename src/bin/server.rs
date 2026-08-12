use kv_store::{Command, Response};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1.7878").await.unwrap();
    println!("Async Server listening on port 7878");

    let store = Arc::new(RwLock::new(HashMap::new()));


}