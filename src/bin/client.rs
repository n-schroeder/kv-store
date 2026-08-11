use std::net::{TcpStream, Shutdown};
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

    let payload = bincode::serialize(&cmd).unwrap();
    let len_bytes = (payload.len() as u32).to_be_bytes();

    stream.write_all(&len_bytes).unwrap();
    stream.write_all(&payload).unwrap();

    stream.shutdown(Shutdown::Write).unwrap();

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();

    let len_payload = u32::from_be_bytes(len_buf) as usize;
    let mut payload_buf = vec![0u8, len_payload as u8];
    stream.read_exact(&mut payload_buf).unwrap();

    let server_response: Response = bincode::deserialize(&payload_buf).unwrap();
    println!("Server response: {:?}", server_response);
}