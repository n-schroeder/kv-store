use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use tokio::fs::{OpenOptions, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt}; 
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct KvStore {
    db: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    wal: Arc<Mutex<File>>,
}

impl KvStore {
    pub async fn open() -> Self {
        let mut store = HashMap::new();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("wal.log")
            .await
            .unwrap();

        loop {
            let mut len_buf = [0u8; 4];

            match file.read_exact(&mut len_buf).await {
                Ok(_) => {
                    let len = u32::from_be_bytes(len_buf) as usize;
                    let mut payload = vec![0u8; len];
                    file.read_exact(&mut payload).await.unwrap();
                    
                    let cmd: Command = bincode::deserialize(&payload).unwrap();
                    
                    if let Command::Set { key, value } = cmd {
                        store.insert(key, value);
                    }
                }
                Err(_) => break,
            }
        }

        println!("Database booted. Restored {} keys from WAL.", store.len());

        let append_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("wal.log")
            .await
            .unwrap();

        KvStore {
            db: Arc::new(RwLock::new(store)),
            wal: Arc::new(Mutex::new(append_file)),
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>) {
        let mut lock = self.db.write().unwrap();
        lock.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let lock = self.db.read().unwrap();
        let value = lock.get(key);
        value.cloned()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Command {
    Set { key: String, value: Vec<u8> },
    Get { key: String },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{assert_eq, thread};

    #[test]
    fn test_concurrent_inserts() {
        let store = KvStore::new();
        let mut handles = vec![];

        for i in 0..100 {
            let store_clone = store.clone(); 
            let handle = thread::spawn(move || {
                let key = format!("user_{}", i);
                let value = vec![i as u8];

                store_clone.set(key, value);
            });
            
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..100 {
            let key = format!("user_{}", i);
            let expected_value = vec![i as u8];
            
            let actual_value = store.get(&key).expect("Key should exist");
            assert_eq!(actual_value, expected_value, "Data mismatch for key: {}", key);
        }
    }

    #[test]
    fn test_serialization() {
        let original_cmd = Command::Set {
            key: "some_data".to_string(),
            value: vec![99, 100, 101],
        };

        let network_bytes = bincode::serialize(&original_cmd).unwrap();
        let rebuilt_cmd: Command = bincode::deserialize(&network_bytes).unwrap();

        assert_eq!(original_cmd, rebuilt_cmd);
    }
}