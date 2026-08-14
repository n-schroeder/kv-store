use kv_store::{Command, Response};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    println!("Starting load test...");
    
    let mut tasks = vec![];

    for i in 0..100 {
        let task = tokio::spawn(async move {
            let mut stream = TcpStream::connect("192.168.1.120:7878").await.unwrap();
            
            let cmd = Command::Set {
                key: format!("key_{}", i),
                value: vec![i as u8, 0, 0, 0],
            };
            
            let payload = bincode::serialize(&cmd).unwrap();
            let len_bytes = (payload.len() as u32).to_be_bytes();
            
            stream.write_all(&len_bytes).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            
            let mut resp_len_buf = [0u8; 4];
            stream.read_exact(&mut resp_len_buf).await.unwrap();
            
            let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
            let mut resp_payload = vec![0u8; resp_len];
            stream.read_exact(&mut resp_payload).await.unwrap();
            
            let response: Response = bincode::deserialize(&resp_payload).unwrap();
            println!("Client {} got: {:?}", i, response);
        });
        
        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }
    
    println!("Load test complete");
}