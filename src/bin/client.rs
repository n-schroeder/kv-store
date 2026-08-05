use std::net::TcpStream;
use std::io::{Read, Write};
use std::println;
use kv_store::{Command, Response};

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
    println!("Connected to the server!");

    let cmd = Command::Set {
        key: "test_key".to_string(),
        value: vec![1, 2, 3, 4],
    };

    let bytes = bincode::serialize(&cmd).unwrap();
    stream.write_all(&bytes).unwrap();

    let mut buffer= Vec::new();
    stream.read_to_end(&mut buffer).unwrap();
    let server_response: Response = bincode::deserialize(&buffer).unwrap();
    println!("Server response: {:?}", server_response);
}