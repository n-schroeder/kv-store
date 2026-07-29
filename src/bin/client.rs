use std::net::TcpStream;
use std::io::Write;
use kv_store::Command;

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
    println!("Connected to the server!");

    let cmd = Command::Set {
        key: "test_key".to_string(),
        value: vec![1, 2, 3, 4],
    };

    let bytes = bincode::serialize(&cmd).unwrap();
    stream.write_all(&bytes).unwrap();
}