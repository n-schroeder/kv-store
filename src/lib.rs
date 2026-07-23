use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
        return value.cloned()
    }
}