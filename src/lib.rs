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
}