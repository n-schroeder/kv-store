use std::collections::HashMap;
use std::io::{self, ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use tokio::fs::{OpenOptions, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

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

/// One entry in the replicated log. `index` is 1-based; index 0 is the "empty
/// log" sentinel that `prev_log_index` uses to mean "nothing before this".
///
/// `term` and `index` live here, inside the WAL record's payload, rather than in
/// the record frame: recovery decodes every record anyway, and [`RaftLog`] keeps
/// an in-memory offset per index, so nothing needs these fields before decoding.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
}

/// What a follower did with an `AppendEntries`.
#[derive(Debug, Clone, PartialEq)]
pub enum AppendOutcome {
    /// The entries are in the log; `match_index` is the last index held.
    Accepted { match_index: u64 },
    /// The log doesn't match at `prev_log_index`. `conflict_index` is where the
    /// leader should try again, so it can back up by a whole term at a time
    /// instead of one index per round trip.
    Conflict { conflict_index: u64 },
}

/// The replicated log: the WAL, read as a sequence of [`LogEntry`]s.
pub struct RaftLog {
    wal: Wal,
    entries: Vec<LogEntry>,
    /// Byte offset of each entry's record, parallel to `entries`. This is what
    /// lets a truncation turn an index back into a file position.
    offsets: Vec<u64>,
}

impl RaftLog {
    /// Opens the log at `path`, replaying it into memory.
    ///
    /// On top of the frame-level CRC check, this validates that indexes run
    /// 1, 2, 3, … with no gaps and that terms never decrease, truncating at the
    /// first entry that breaks either rule. That check is not redundant with the
    /// checksum: if we truncate a diverged suffix and crash before the
    /// replacement entry is written — or write a shorter one that doesn't cover
    /// all the old bytes — the leftover records are *intact*, correct CRC and
    /// all. Only their indexes give them away.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let (mut wal, records) = Wal::open(path).await?;

        let mut entries: Vec<LogEntry> = Vec::with_capacity(records.len());
        let mut offsets: Vec<u64> = Vec::with_capacity(records.len());
        let mut truncate_at: Option<(u64, String)> = None;

        for record in &records {
            let entry: LogEntry = match bincode::deserialize(&record.payload) {
                Ok(entry) => entry,
                Err(e) => {
                    truncate_at = Some((record.offset, format!("undecodable record ({})", e)));
                    break;
                }
            };

            let expected_index = entries.len() as u64 + 1;
            if entry.index != expected_index {
                truncate_at = Some((
                    record.offset,
                    format!("expected index {}, found {}", expected_index, entry.index),
                ));
                break;
            }

            if let Some(previous) = entries.last() {
                if entry.term < previous.term {
                    truncate_at = Some((
                        record.offset,
                        format!("term went backwards ({} after {})", entry.term, previous.term),
                    ));
                    break;
                }
            }

            entries.push(entry);
            offsets.push(record.offset);
        }

        if let Some((offset, reason)) = truncate_at {
            println!(
                "WAL: dropping entries from index {} on — {}. This is what a crash \
                 during a log truncation looks like.",
                entries.len() as u64 + 1,
                reason
            );
            wal.truncate_from(offset).await?;
        }

        println!("Log restored: {} entries, last term {}.",
            entries.len(),
            entries.last().map(|e| e.term).unwrap_or(0));

        Ok(RaftLog { wal, entries, offsets })
    }

    pub fn last_index(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// The term of the entry at `index`, or `Some(0)` for the index-0 sentinel.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.entries.get((index - 1) as usize).map(|e| e.term)
    }

    pub fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    /// Up to `max` entries starting at `index`, for a leader to ship to a peer.
    pub fn entries_from(&self, index: u64, max: usize) -> Vec<LogEntry> {
        if index == 0 || index > self.last_index() {
            return Vec::new();
        }
        let start = (index - 1) as usize;
        let end = (start + max).min(self.entries.len());
        self.entries[start..end].to_vec()
    }

    /// Is `other` at least as up to date as this log? Raft's election
    /// restriction: a later last term wins, and on a tie the longer log wins.
    /// Without it, a node missing committed entries could win an election and
    /// overwrite them, which would make an acknowledged write a lie.
    pub fn is_up_to_date(&self, other_last_term: u64, other_last_index: u64) -> bool {
        match other_last_term.cmp(&self.last_term()) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => other_last_index >= self.last_index(),
        }
    }

    /// Appends a new command as the leader, returning the index it landed at.
    pub async fn append_command(&mut self, term: u64, command: Command) -> io::Result<u64> {
        let index = self.last_index() + 1;
        let entry = LogEntry { term, index, command };
        self.append_one(entry).await?;
        Ok(index)
    }

    async fn append_one(&mut self, entry: LogEntry) -> io::Result<()> {
        let payload = bincode::serialize(&entry).map_err(|e| {
            io::Error::new(ErrorKind::InvalidData, format!("cannot serialize log entry: {}", e))
        })?;
        let offset = self.wal.append(&payload).await?;
        self.entries.push(entry);
        self.offsets.push(offset);
        Ok(())
    }

    /// The follower side of `AppendEntries`: check that our log matches the
    /// leader's at `prev_log_index`, then take the entries.
    pub async fn try_append(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[LogEntry],
    ) -> io::Result<AppendOutcome> {
        // We're simply missing entries the leader assumes we have.
        if prev_log_index > self.last_index() {
            return Ok(AppendOutcome::Conflict { conflict_index: self.last_index() + 1 });
        }

        if let Some(our_term) = self.term_at(prev_log_index) {
            if our_term != prev_log_term {
                // Back the leader up past every entry of the term we disagree
                // on, rather than one index per round trip.
                let mut conflict_index = prev_log_index;
                while conflict_index > 1 && self.term_at(conflict_index - 1) == Some(our_term) {
                    conflict_index -= 1;
                }
                return Ok(AppendOutcome::Conflict { conflict_index });
            }
        }

        // Skip entries we already hold, so a duplicate or delayed AppendEntries
        // can't truncate a suffix that is actually fine — possibly a committed
        // one. Only a genuine term mismatch truncates.
        let mut first_new = 0;
        while first_new < entries.len() {
            let entry = &entries[first_new];
            match self.term_at(entry.index) {
                Some(existing_term) if existing_term == entry.term => first_new += 1,
                Some(existing_term) => {
                    println!(
                        "Truncating diverged log from index {} (ours term {}, leader's term {}).",
                        entry.index, existing_term, entry.term
                    );
                    self.truncate_from(entry.index).await?;
                    break;
                }
                None => break,
            }
        }

        for entry in &entries[first_new..] {
            self.append_one(entry.clone()).await?;
        }

        Ok(AppendOutcome::Accepted {
            match_index: prev_log_index + entries.len() as u64,
        })
    }

    /// Drops every entry from `index` on, both in memory and on disk.
    pub async fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        if index == 0 || index > self.last_index() {
            return Ok(());
        }
        let position = (index - 1) as usize;
        let offset = self.offsets[position];

        self.wal.truncate_from(offset).await?;
        self.entries.truncate(position);
        self.offsets.truncate(position);
        Ok(())
    }
}

/// The state machine the log is applied into: an in-memory map, and nothing
/// else. It is deliberately *not* durable — the log is the durable thing, and
/// this is only ever a replay of the log's committed prefix.
#[derive(Clone, Default)]
pub struct KvStore {
    db: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore { db: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Applies a committed command. Only `Set` changes anything; reads and Raft
    /// RPCs are ignored, so a log that carries them replays harmlessly.
    pub fn apply(&self, command: &Command) {
        if let Command::Set { key, value } = command {
            self.db.write().unwrap().insert(key.clone(), value.clone());
        }
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
    /// A leader's first entry in a new term. It carries no data; it exists so
    /// the commit rule has an entry of the current term to latch onto, which is
    /// what lets entries inherited from earlier terms commit.
    NoOp,
    /// Replication and heartbeat in one. An empty `entries` is a heartbeat.
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Response {
    Ok,
    Value(Option<Vec<u8>>),
    Error(String),
    VoteResponse { term: u64, vote_granted: bool },
    AppendEntriesAck {
        term: u64,
        success: bool,
        /// On success, the last index the follower now holds.
        match_index: u64,
        /// On failure, the index the leader should retry from.
        conflict_index: u64,
    },
    /// This node isn't the leader. `leader` is its dialable address if known.
    NotLeader { leader: Option<String> },
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

    fn set(key: &str, value: u8) -> Command {
        Command::Set { key: key.to_string(), value: vec![value] }
    }

    #[tokio::test]
    async fn raft_log_restores_entries_across_a_reopen() {
        let path = TempPath::new("raftlog");

        let mut log = RaftLog::open(path.as_path()).await.unwrap();
        assert_eq!(log.last_index(), 0, "an empty log is at index 0");
        assert_eq!(log.append_command(1, set("a", 1)).await.unwrap(), 1);
        assert_eq!(log.append_command(1, set("b", 2)).await.unwrap(), 2);
        assert_eq!(log.append_command(2, set("a", 3)).await.unwrap(), 3);
        drop(log);

        let log = RaftLog::open(path.as_path()).await.unwrap();
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(0), Some(0), "index 0 is the empty-log sentinel");

        // Replaying the log into the state machine is how a node recovers.
        let store = KvStore::new();
        for i in 1..=log.last_index() {
            store.apply(&log.entry_at(i).unwrap().command);
        }
        assert_eq!(store.get("a"), Some(vec![3]), "last write for a key wins");
        assert_eq!(store.get("b"), Some(vec![2]));
        assert_eq!(store.get("missing"), None);
    }

    /// The failure a checksum cannot see. We truncate a diverged suffix and
    /// crash before writing the replacement, so records that were logically
    /// removed are still physically present and still checksum perfectly. Only
    /// the index sequence gives them away.
    #[tokio::test]
    async fn raft_log_drops_a_stale_suffix_that_still_checksums() {
        let path = TempPath::new("stale-suffix");

        let mut log = RaftLog::open(path.as_path()).await.unwrap();
        for i in 1..=10u8 {
            log.append_command(1, set(&format!("key{}", i), i)).await.unwrap();
        }
        drop(log);

        let intact = std::fs::read(path.as_path()).unwrap();

        // Truncate back to 5 entries, then simulate the crash: put the original
        // bytes for entries 6..10 back, exactly as they were on disk.
        let mut log = RaftLog::open(path.as_path()).await.unwrap();
        log.truncate_from(6).await.unwrap();
        drop(log);

        let truncated_len = std::fs::metadata(path.as_path()).unwrap().len() as usize;
        std::fs::write(path.as_path(), &intact).unwrap();
        assert!(intact.len() > truncated_len, "the stale bytes are really there");

        // Every one of those records passes its CRC, so recovery has to catch
        // this some other way: write a *new* entry 6 and check the leftovers go.
        let mut log = RaftLog::open(path.as_path()).await.unwrap();
        log.truncate_from(6).await.unwrap();
        log.append_command(2, set("replacement", 99)).await.unwrap();
        // Now paste the stale tail back on, as a crash mid-truncate would leave it.
        let mut bytes = std::fs::read(path.as_path()).unwrap();
        bytes.extend_from_slice(&intact[truncated_len..]);
        std::fs::write(path.as_path(), &bytes).unwrap();
        drop(log);

        let log = RaftLog::open(path.as_path()).await.unwrap();
        assert_eq!(log.last_index(), 6, "the resurrected entries are gone");
        assert_eq!(log.last_term(), 2);
        assert_eq!(
            log.entry_at(6).unwrap().command,
            set("replacement", 99),
            "the replacement entry survived, the stale ones did not"
        );
    }

    #[tokio::test]
    async fn try_append_truncates_a_conflicting_suffix() {
        let path = TempPath::new("conflict");

        let mut log = RaftLog::open(path.as_path()).await.unwrap();
        log.append_command(1, set("a", 1)).await.unwrap();
        log.append_command(1, set("b", 2)).await.unwrap();
        log.append_command(1, set("c", 3)).await.unwrap();

        // A new leader in term 2 disagrees about index 2 onward.
        let replacement = vec![
            LogEntry { term: 2, index: 2, command: set("b", 99) },
            LogEntry { term: 2, index: 3, command: set("c", 98) },
        ];
        let outcome = log.try_append(1, 1, &replacement).await.unwrap();

        assert_eq!(outcome, AppendOutcome::Accepted { match_index: 3 });
        assert_eq!(log.entry_at(2).unwrap().command, set("b", 99));
        assert_eq!(log.last_term(), 2);
    }

    #[tokio::test]
    async fn try_append_is_idempotent_for_entries_we_already_have() {
        let path = TempPath::new("duplicate");

        let mut log = RaftLog::open(path.as_path()).await.unwrap();
        log.append_command(1, set("a", 1)).await.unwrap();
        log.append_command(1, set("b", 2)).await.unwrap();
        log.append_command(1, set("c", 3)).await.unwrap();

        // A delayed retransmission of entries we already hold must not truncate
        // the entries after them — those may already be committed.
        let already_have = vec![LogEntry { term: 1, index: 2, command: set("b", 2) }];
        let outcome = log.try_append(1, 1, &already_have).await.unwrap();

        assert_eq!(outcome, AppendOutcome::Accepted { match_index: 2 });
        assert_eq!(log.last_index(), 3, "entry 3 was not truncated away");
        assert_eq!(log.entry_at(3).unwrap().command, set("c", 3));
    }

    #[tokio::test]
    async fn try_append_reports_where_to_back_up_to() {
        let path = TempPath::new("backup");

        let mut log = RaftLog::open(path.as_path()).await.unwrap();

        // A follower that is simply behind asks for the next index it needs.
        let outcome = log.try_append(5, 3, &[]).await.unwrap();
        assert_eq!(outcome, AppendOutcome::Conflict { conflict_index: 1 });

        // A follower holding a whole term the leader doesn't have should send
        // the leader back past all of it at once, not one index per round trip.
        for _ in 0..4 {
            log.append_command(4, set("x", 0)).await.unwrap();
        }
        let outcome = log.try_append(4, 9, &[]).await.unwrap();
        assert_eq!(
            outcome,
            AppendOutcome::Conflict { conflict_index: 1 },
            "back up past every entry of the disputed term"
        );
    }

    #[tokio::test]
    async fn election_restriction_compares_logs() {
        let path = TempPath::new("uptodate");

        let mut log = RaftLog::open(path.as_path()).await.unwrap();
        log.append_command(1, set("a", 1)).await.unwrap();
        log.append_command(2, set("b", 2)).await.unwrap();
        // Our log: last term 2, last index 2.

        assert!(log.is_up_to_date(2, 2), "an identical log qualifies");
        assert!(log.is_up_to_date(2, 5), "same term, longer log qualifies");
        assert!(log.is_up_to_date(3, 1), "a later term wins even if shorter");
        assert!(!log.is_up_to_date(2, 1), "same term, shorter log is behind");
        assert!(!log.is_up_to_date(1, 9), "an earlier last term loses at any length");
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
