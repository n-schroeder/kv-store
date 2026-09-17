use kv_store::{Command, Response};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DEFAULT_ADDR: &str = "0.0.0.0:7878";

/// Sends one command and reads one response.
async fn send(addr: &str, cmd: &Command) -> std::io::Result<Response> {
    let mut stream = TcpStream::connect(addr).await?;

    let payload = bincode::serialize(cmd)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let len_bytes = (payload.len() as u32).to_be_bytes();

    stream.write_all(&len_bytes).await?;
    stream.write_all(&payload).await?;

    let mut resp_len_buf = [0u8; 4];
    stream.read_exact(&mut resp_len_buf).await?;
    let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
    let mut resp_payload = vec![0u8; resp_len];
    stream.read_exact(&mut resp_payload).await?;

    bincode::deserialize(&resp_payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Sends a command, following a leader redirect if the node we asked isn't the
/// leader. Reads and writes both go through the leader now, so hitting the
/// wrong node is routine rather than an error.
async fn send_following_redirect(addr: &str, cmd: &Command) -> std::io::Result<Response> {
    match send(addr, cmd).await? {
        Response::NotLeader { leader: Some(leader) } => {
            if leader == addr {
                return Ok(Response::NotLeader { leader: Some(leader) });
            }
            eprintln!("{} is not the leader; retrying against {}.", addr, leader);
            send(&leader, cmd).await
        }
        other => Ok(other),
    }
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  \
         client                          run the load test against {addr}\n  \
         client <addr> set <key> <value>\n  \
         client <addr> get <key>",
        addr = DEFAULT_ADDR
    );
    std::process::exit(2);
}

/// Fires 100 concurrent writes and then reads one key back. This is for putting
/// load on a server, not a client library.
async fn load_test() {
    println!("Starting load test...");

    let mut tasks = vec![];

    for i in 0..100 {
        let task = tokio::spawn(async move {
            let cmd = Command::Set {
                key: format!("key_{}", i),
                value: vec![i as u8, 0, 0, 0],
            };

            match send_following_redirect(DEFAULT_ADDR, &cmd).await {
                Ok(response) => println!("Client {} got: {:?}", i, response),
                Err(e) => println!("Client {} failed: {}", i, e),
            }
        });

        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    let cmd = Command::Get { key: "key_42".to_string() };
    match send_following_redirect(DEFAULT_ADDR, &cmd).await {
        Ok(response) => println!("Read back key_42: {:?}", response),
        Err(e) => println!("Read back key_42 failed: {}", e),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        load_test().await;
        return;
    }

    if args.len() < 2 {
        usage();
    }

    let addr = args[0].clone();
    let cmd = match args[1].as_str() {
        "set" if args.len() == 4 => Command::Set {
            key: args[2].clone(),
            value: args[3].clone().into_bytes(),
        },
        "get" if args.len() == 3 => Command::Get { key: args[2].clone() },
        _ => usage(),
    };

    match send_following_redirect(&addr, &cmd).await {
        Ok(Response::Value(Some(bytes))) => {
            // Values are arbitrary bytes; print them as text when they are text.
            match String::from_utf8(bytes.clone()) {
                Ok(text) => println!("{}", text),
                Err(_) => println!("{:?}", bytes),
            }
        }
        Ok(Response::Ok) => println!("OK"),
        Ok(Response::NotLeader { leader }) => {
            eprintln!("not the leader, and no leader known yet (last seen: {:?})", leader);
            std::process::exit(1);
        }
        Ok(Response::Error(msg)) => {
            eprintln!("error: {}", msg);
            std::process::exit(1);
        }
        Ok(other) => {
            eprintln!("unexpected response: {:?}", other);
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("{}: {}", addr, e);
            std::process::exit(1);
        }
    }
}
