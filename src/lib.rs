use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct KvStore {
    db: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl KvStore {
    pub fn new() -> Self {
        let empty_map = HashMap::new();
        let locked_map = RwLock::new(empty_map);
        let thread_safe_db = Arc::new(locked_map);

        KvStore { db: thread_safe_db }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

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
}