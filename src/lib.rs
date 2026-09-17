use std::collections::HashMap;
use std::io::{self, ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use tokio::fs::{OpenOptions, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Magic bytes plus format version at the head of every WAL file. The trailing
/// byte is the version. Pre-checksum logs (v1) had no header at all, so their
/// first bytes are a record length prefix — which is how we tell them apart and
/// refuse to misread them.
pub const WAL_MAGIC: &[u8; 8] = b"KVWALOG\x02";

/// Upper bound on one record's payload. A garbage length prefix from a torn
/// write would otherwise have us allocate gigabytes before the CRC ever gets a
/// chance to reject it.
const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;

/// Bytes of framing ahead of each payload: `len: u32 | crc32: u32`.
const RECORD_HEADER_LEN: u64 = 8;

/// One recovered record. `offset` is where its length prefix starts, which is
/// what a caller needs to truncate the log back to this point.
#[derive(Debug, Clone, PartialEq)]
pub struct WalRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
}

/// An append-only log of length-prefixed, CRC-checked records.
///
/// Every append is `fsync`ed before it returns, and a torn or corrupt trailing
/// record is truncated away at open rather than crashing recovery. The frame is
/// a pure integrity envelope: it knows nothing about what the payload means, so
/// term/index checks belong to the caller that decodes them.
#[derive(Debug)]
pub struct Wal {
    file: File,
    path: PathBuf,
    end: u64,
}

impl Wal {
    /// Opens (or creates) the log at `path` and replays it, returning every
    /// intact record in order.
    ///
    /// Recovery stops at the first record that is short, over-long, or fails
    /// its checksum, and physically truncates the file there — a crash partway
    /// through an append leaves exactly that shape.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<(Self, Vec<WalRecord>)> {
        let path = path.as_ref().to_path_buf();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;

        let file_len = file.metadata().await?.len();

        if file_len == 0 {
            file.write_all(WAL_MAGIC).await?;
            file.sync_all().await?;
            return Ok((Wal { file, path, end: WAL_MAGIC.len() as u64 }, Vec::new()));
        }

        Self::check_header(&mut file, &path, file_len).await?;

        let mut records = Vec::new();
        let mut good_end = WAL_MAGIC.len() as u64;
        file.seek(SeekFrom::Start(good_end)).await?;

        loop {
            match Self::read_record(&mut file).await? {
                Some(payload) => {
                    records.push(WalRecord { offset: good_end, payload: payload.clone() });
                    good_end += RECORD_HEADER_LEN + payload.len() as u64;
                }
                None => break,
            }
        }

        if good_end < file_len {
            println!(
                "WAL: truncating {} trailing bytes (torn record at offset {}).",
                file_len - good_end,
                good_end
            );
            file.set_len(good_end).await?;
            file.sync_all().await?;
        }

        file.seek(SeekFrom::Start(good_end)).await?;

        Ok((Wal { file, path, end: good_end }, records))
    }

    /// Rejects anything that isn't a v2 log before a single record is read. A
    /// v1 file is left untouched on disk: the operator decides what happens to
    /// it, because there is no safe way to reinterpret it as v2.
    async fn check_header(file: &mut File, path: &Path, file_len: u64) -> io::Result<()> {
        let mut header = vec![0u8; WAL_MAGIC.len().min(file_len as usize)];
        file.seek(SeekFrom::Start(0)).await?;
        file.read_exact(&mut header).await?;

        if header == WAL_MAGIC {
            return Ok(());
        }

        // A file shorter than the header that is still a prefix of it means we
        // crashed between creating the file and writing the magic. Nothing can
        // have been appended yet, so finishing the header is safe.
        if header.len() < WAL_MAGIC.len() && WAL_MAGIC.starts_with(&header[..]) {
            file.seek(SeekFrom::Start(0)).await?;
            file.write_all(WAL_MAGIC).await?;
            file.set_len(WAL_MAGIC.len() as u64).await?;
            file.sync_all().await?;
            return Ok(());
        }

        if header.len() == WAL_MAGIC.len() && header[..7] == WAL_MAGIC[..7] {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "{}: WAL format version {} is not supported by this build (expected {}). \
                     Every node in a cluster must run the same build.",
                    path.display(),
                    header[7],
                    WAL_MAGIC[7]
                ),
            ));
        }

        Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "{}: not a v2 WAL — this looks like a pre-checksum (v1) log, whose records \
                 carry no term, index, or CRC and cannot be read as Raft log entries. \
                 Move it aside (e.g. `mv {} {}.v1.bak`) and restart to begin a fresh log.",
                path.display(),
                path.display(),
                path.display()
            ),
        ))
    }

    /// Reads one record at the current position. `Ok(None)` means the log ends
    /// here — cleanly or otherwise; either way the caller truncates to this
    /// point. Only genuine I/O failures propagate as errors.
    async fn read_record(file: &mut File) -> io::Result<Option<Vec<u8>>> {
        let mut header = [0u8; RECORD_HEADER_LEN as usize];
        match file.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let expected_crc = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

        if len > MAX_RECORD_LEN {
            return Ok(None);
        }

        let mut payload = vec![0u8; len as usize];
        match file.read_exact(&mut payload).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        if crc32fast::hash(&payload) != expected_crc {
            return Ok(None);
        }

        Ok(Some(payload))
    }

    /// Appends one record and `fsync`s it, returning the offset it starts at.
    pub async fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        let offset = self.end;
        let len = u32::try_from(payload.len()).map_err(|_| {
            io::Error::new(ErrorKind::InvalidInput, "WAL record exceeds u32 length prefix")
        })?;

        if len > MAX_RECORD_LEN {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("WAL record of {} bytes exceeds the {} byte limit", len, MAX_RECORD_LEN),
            ));
        }

        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(&len.to_be_bytes()).await?;
        self.file.write_all(&crc32fast::hash(payload).to_be_bytes()).await?;
        self.file.write_all(payload).await?;
        self.file.sync_all().await?;

        self.end = offset + RECORD_HEADER_LEN + payload.len() as u64;
        Ok(offset)
    }

    /// Drops everything from `offset` on. Used to remove a diverged suffix once
    /// a leader's log disagrees with ours.
    pub async fn truncate_from(&mut self, offset: u64) -> io::Result<()> {
        self.file.set_len(offset).await?;
        self.file.sync_all().await?;
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.end = offset;
        Ok(())
    }

    /// Offset one past the last record — where the next append will land.
    pub fn end_offset(&self) -> u64 {
        self.end
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// The Raft state that has to survive a crash.
///
/// `current_term` and `voted_for` are what stop a restarted node from voting
/// twice in one term (which can elect two leaders in the same term).
/// `commit_index` is a lower-bound hint: committed entries are never truncated,
/// so replaying up to it on boot is always safe and saves a node that restarts
/// alone from serving empty reads.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub commit_index: u64,
}

/// Crash-safe storage for [`RaftState`], written by write-temp-then-rename so a
/// torn state file is never observable.
pub struct RaftStateStore {
    path: PathBuf,
    tmp_path: PathBuf,
}

impl RaftStateStore {
    /// Opens the state file, returning the persisted state or a fresh default
    /// if the node has never voted.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<(Self, RaftState)> {
        let path = path.as_ref().to_path_buf();
        let tmp_path = path.with_extension("tmp");
        let store = RaftStateStore { path, tmp_path };

        let state = match tokio::fs::read(&store.path).await {
            Ok(bytes) => bincode::deserialize(&bytes).map_err(|e| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("{}: unreadable Raft state file: {}", store.path.display(), e),
                )
            })?,
            Err(e) if e.kind() == ErrorKind::NotFound => RaftState::default(),
            Err(e) => return Err(e),
        };

        Ok((store, state))
    }

    /// Persists `state` durably. Must complete *before* the node acts on the
    /// change — before answering a `RequestVote`, and before sending one.
    pub async fn save(&self, state: &RaftState) -> io::Result<()> {
        let bytes = bincode::serialize(state).map_err(|e| {
            io::Error::new(ErrorKind::InvalidData, format!("cannot serialize Raft state: {}", e))
        })?;

        let mut tmp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.tmp_path)
            .await?;
        tmp.write_all(&bytes).await?;
        tmp.sync_all().await?;
        drop(tmp);

        tokio::fs::rename(&self.tmp_path, &self.path).await?;

        // The rename itself needs flushing, or a power loss can leave the
        // directory entry pointing at the old file.
        if let Some(dir) = self.path.parent() {
            let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
            File::open(dir).await?.sync_all().await?;
        }

        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone)]
pub struct KvStore {
    db: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    wal: Arc<Mutex<Wal>>,
}

impl KvStore {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let (wal, records) = Wal::open(path).await?;

        let mut store = HashMap::new();
        for record in &records {
            let cmd: Command = bincode::deserialize(&record.payload).map_err(|e| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "{}: record at offset {} passed its checksum but could not be decoded: {}",
                        wal.path().display(),
                        record.offset,
                        e
                    ),
                )
            })?;

            if let Command::Set { key, value } = cmd {
                store.insert(key, value);
            }
        }

        println!("Database booted. Restored {} keys from WAL.", store.len());

        Ok(KvStore {
            db: Arc::new(RwLock::new(store)),
            wal: Arc::new(Mutex::new(wal)),
        })
    }

    pub async fn set(&self, key: String, value: Vec<u8>) -> io::Result<()> {
        let cmd = Command::Set { key: key.clone(), value: value.clone() };

        let payload = bincode::serialize(&cmd).map_err(|e| {
            io::Error::new(ErrorKind::InvalidData, format!("cannot serialize Set: {}", e))
        })?;

        let mut wal = self.wal.lock().await;
        wal.append(&payload).await?;
        drop(wal);

        let mut store = self.db.write().unwrap();
        store.insert(key, value);

        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let lock = self.db.read().unwrap();
        let value = lock.get(key);
        value.cloned()
    }

    pub fn len(&self) -> usize {
        self.db.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Command {
    Set { key: String, value: Vec<u8> },
    Get { key: String },
    Heartbeat { term: u64 },
    RequestVote { term: u64, candidate_id: String },
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Error(String),
    VoteResponse { term: u64, vote_granted: bool },
    HeartbeatAck { term: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique path under the system temp dir that cleans itself up, so tests
    /// can run in parallel without fighting over one hardcoded `wal.log`.
    struct TempPath(PathBuf);

    impl TempPath {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir()
                .join(format!("kv-store-test-{}-{}-{}", std::process::id(), tag, n));
            let _ = std::fs::remove_file(&path);
            TempPath(path)
        }

        fn as_path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(self.0.with_extension("tmp"));
        }
    }

    fn payloads(records: &[WalRecord]) -> Vec<Vec<u8>> {
        records.iter().map(|r| r.payload.clone()).collect()
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

    #[tokio::test]
    async fn wal_round_trips_records() {
        let path = TempPath::new("roundtrip");

        let (mut wal, records) = Wal::open(path.as_path()).await.unwrap();
        assert!(records.is_empty(), "a fresh log has no records");

        for i in 0..5u8 {
            wal.append(&[i, i, i]).await.unwrap();
        }
        drop(wal);

        let (_wal, records) = Wal::open(path.as_path()).await.unwrap();
        assert_eq!(
            payloads(&records),
            (0..5u8).map(|i| vec![i, i, i]).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn wal_recovers_from_a_torn_trailing_record() {
        let path = TempPath::new("torn");

        let (mut wal, _) = Wal::open(path.as_path()).await.unwrap();
        for i in 0..3u8 {
            wal.append(&[i; 16]).await.unwrap();
        }
        let good_end = wal.end_offset();
        drop(wal);

        // Simulate a crash partway through a fourth append: a length prefix and
        // some payload bytes, but not all of them.
        let mut file = std::fs::OpenOptions::new().append(true).open(path.as_path()).unwrap();
        use std::io::Write;
        file.write_all(&32u32.to_be_bytes()).unwrap();
        file.write_all(&0u32.to_be_bytes()).unwrap();
        file.write_all(&[9u8; 10]).unwrap();
        drop(file);

        let (wal, records) = Wal::open(path.as_path()).await.unwrap();
        assert_eq!(records.len(), 3, "the three complete records survive");
        assert_eq!(wal.end_offset(), good_end, "the log resumes at the last good record");
        assert_eq!(
            std::fs::metadata(path.as_path()).unwrap().len(),
            good_end,
            "the torn bytes are physically gone, not just skipped"
        );
    }

    #[tokio::test]
    async fn wal_stops_at_a_corrupted_checksum() {
        let path = TempPath::new("crc");

        let (mut wal, _) = Wal::open(path.as_path()).await.unwrap();
        wal.append(b"first").await.unwrap();
        let second_offset = wal.append(b"second").await.unwrap();
        wal.append(b"third").await.unwrap();
        drop(wal);

        // Flip a byte inside the second record's payload. Its length still
        // parses, so only the CRC can catch this.
        let mut bytes = std::fs::read(path.as_path()).unwrap();
        let payload_start = (second_offset + RECORD_HEADER_LEN) as usize;
        bytes[payload_start] ^= 0xff;
        std::fs::write(path.as_path(), &bytes).unwrap();

        let (_wal, records) = Wal::open(path.as_path()).await.unwrap();
        assert_eq!(
            payloads(&records),
            vec![b"first".to_vec()],
            "recovery stops at the corrupt record and keeps what came before"
        );
    }

    #[tokio::test]
    async fn wal_truncate_from_drops_the_suffix() {
        let path = TempPath::new("truncate");

        let (mut wal, _) = Wal::open(path.as_path()).await.unwrap();
        wal.append(b"keep").await.unwrap();
        let cut = wal.append(b"diverged").await.unwrap();
        wal.append(b"also diverged").await.unwrap();

        wal.truncate_from(cut).await.unwrap();
        wal.append(b"replacement").await.unwrap();
        drop(wal);

        let (_wal, records) = Wal::open(path.as_path()).await.unwrap();
        assert_eq!(payloads(&records), vec![b"keep".to_vec(), b"replacement".to_vec()]);
    }

    #[tokio::test]
    async fn wal_refuses_a_v1_log_without_touching_it() {
        let path = TempPath::new("v1");

        // A v1 log: bare `len | payload` framing, no header.
        let payload = bincode::serialize(&Command::Set {
            key: "legacy".to_string(),
            value: vec![1, 2, 3],
        })
        .unwrap();
        let mut bytes = (payload.len() as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&payload);
        std::fs::write(path.as_path(), &bytes).unwrap();

        let err = Wal::open(path.as_path()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("v1"),
            "the error should name the problem: {}",
            err
        );
        assert_eq!(
            std::fs::read(path.as_path()).unwrap(),
            bytes,
            "the old log must be left exactly as it was"
        );
    }

    #[tokio::test]
    async fn wal_completes_a_torn_header() {
        let path = TempPath::new("torn-header");

        // Crashed between creating the file and writing the magic.
        std::fs::write(path.as_path(), &WAL_MAGIC[..3]).unwrap();

        let (mut wal, records) = Wal::open(path.as_path()).await.unwrap();
        assert!(records.is_empty());
        wal.append(b"after recovery").await.unwrap();
        drop(wal);

        let (_wal, records) = Wal::open(path.as_path()).await.unwrap();
        assert_eq!(payloads(&records), vec![b"after recovery".to_vec()]);
    }

    #[tokio::test]
    async fn kv_store_restores_keys_across_a_reopen() {
        let path = TempPath::new("kvstore");

        let store = KvStore::open(path.as_path()).await.unwrap();
        store.set("a".to_string(), vec![1]).await.unwrap();
        store.set("b".to_string(), vec![2]).await.unwrap();
        store.set("a".to_string(), vec![3]).await.unwrap();
        drop(store);

        let store = KvStore::open(path.as_path()).await.unwrap();
        assert_eq!(store.get("a"), Some(vec![3]), "last write for a key wins");
        assert_eq!(store.get("b"), Some(vec![2]));
        assert_eq!(store.get("missing"), None);
    }

    #[tokio::test]
    async fn raft_state_round_trips() {
        let path = TempPath::new("state");

        let (store, state) = RaftStateStore::open(path.as_path()).await.unwrap();
        assert_eq!(state, RaftState::default(), "a node that never voted starts clean");

        let saved = RaftState {
            current_term: 7,
            voted_for: Some("node3:7878".to_string()),
            commit_index: 42,
        };
        store.save(&saved).await.unwrap();
        drop(store);

        let (_store, reloaded) = RaftStateStore::open(path.as_path()).await.unwrap();
        assert_eq!(reloaded, saved);
    }

    #[tokio::test]
    async fn raft_state_ignores_a_leftover_temp_file() {
        let path = TempPath::new("state-tmp");

        let (store, _) = RaftStateStore::open(path.as_path()).await.unwrap();
        let saved = RaftState { current_term: 3, voted_for: None, commit_index: 0 };
        store.save(&saved).await.unwrap();

        // A crash mid-save leaves a half-written temp file behind; the real
        // state file must still be the one that's read.
        std::fs::write(path.as_path().with_extension("tmp"), b"garbage").unwrap();

        let (_store, reloaded) = RaftStateStore::open(path.as_path()).await.unwrap();
        assert_eq!(reloaded, saved);
    }

    /// The bug this whole branch exists to kill: before `voted_for` was
    /// persisted, a node that crashed after voting came back at term 0 and
    /// could vote again in the same term, electing two leaders at once.
    #[tokio::test]
    async fn a_restarted_node_remembers_who_it_voted_for() {
        let path = TempPath::new("double-vote");

        let (store, mut state) = RaftStateStore::open(path.as_path()).await.unwrap();
        state.current_term = 5;
        state.voted_for = Some("node1:7878".to_string());
        store.save(&state).await.unwrap();
        drop(store);

        // Restart.
        let (_store, state) = RaftStateStore::open(path.as_path()).await.unwrap();
        assert_eq!(state.current_term, 5, "the term survived the crash");
        assert_eq!(state.voted_for.as_deref(), Some("node1:7878"));
    }
}
