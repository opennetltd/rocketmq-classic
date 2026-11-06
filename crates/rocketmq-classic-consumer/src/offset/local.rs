use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{
        LazyLock, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use rocketmq_classic_common::message::MessageQueue;

use super::{
    OffsetRead, OffsetStore, OffsetStoreError, OffsetStoreFuture, OffsetTable, ReadOffsetType,
};

const STORE_FILE_NAME: &str = "offsets.json";
const STORE_ROOT_PROPERTY: &str = "rocketmq.client.localOffsetStoreDir";

struct StoreRootSnapshot {
    java_value: String,
    path: PathBuf,
}

static STORE_ROOT_SNAPSHOT: LazyLock<StoreRootSnapshot> = LazyLock::new(read_store_root_snapshot);

static ACTIVE_STORE_PATHS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn read_store_root_snapshot() -> StoreRootSnapshot {
    if let Some(root) = std::env::var_os(STORE_ROOT_PROPERTY) {
        let java_value = root.to_string_lossy().into_owned();
        let path = if root.is_empty() {
            PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
        } else {
            PathBuf::from(root)
        };
        return StoreRootSnapshot { java_value, path };
    }

    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    let Some(home) = home else {
        let java_value = format!("null{}{}", std::path::MAIN_SEPARATOR, ".rocketmq_offsets");
        return StoreRootSnapshot {
            path: PathBuf::from(&java_value),
            java_value,
        };
    };

    let mut java_value = home.to_string_lossy().into_owned();
    java_value.push(std::path::MAIN_SEPARATOR);
    java_value.push_str(".rocketmq_offsets");
    let mut path = if home.is_empty() {
        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
    } else {
        PathBuf::from(home)
    };
    path.push(".rocketmq_offsets");
    StoreRootSnapshot { java_value, path }
}

/// Stores offsets in Java 5.5.1 `OffsetSerializeWrapper` JSON format.
///
/// One writer may own a given path in this process at a time. The storage root
/// honors Java's `rocketmq.client.localOffsetStoreDir` property through the
/// same-named process environment variable, then falls back to the user's home
/// directory plus `.rocketmq_offsets`.
///
/// Internal file/group inspection helpers are not public Java `OffsetStore`
/// API.
///
/// ```compile_fail
/// use rocketmq_classic_consumer::offset::LocalFileOffsetStore;
/// fn inspect_group(store: &LocalFileOffsetStore) {
///     let _ = store.group_name();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_consumer::offset::LocalFileOffsetStore;
/// fn inspect_path(store: &LocalFileOffsetStore) {
///     let _ = store.storage_path();
/// }
/// ```
pub struct LocalFileOffsetStore {
    path: PathBuf,
    offsets: OffsetTable,
    _writer_lease: StoreWriterLease,
}

struct StoreWriterLease {
    path: PathBuf,
    #[cfg(any(unix, windows))]
    lock_file: Option<File>,
}

impl Drop for StoreWriterLease {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(file) = self.lock_file.take() {
            release_process_lock(&file);
            drop(file);
        }
        #[cfg(windows)]
        drop(self.lock_file.take());
        let paths = ACTIVE_STORE_PATHS.get_or_init(Default::default);
        paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.path);
    }
}

struct TempFile(PathBuf);

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistStep {
    TempCreated,
    TempSynced,
    BackupTempSynced,
    BackupInstalled,
    PrimaryInstalled,
    DirectorySynced,
}

impl LocalFileOffsetStore {
    /// Returns Java's public `LOCAL_OFFSET_STORE_DIR` value.
    ///
    /// The value is captured on first access, matching Java's static field
    /// initialization. Rust reads the same-named process environment variable
    /// as its system-property adapter and falls back to the home directory.
    pub fn local_offset_store_dir() -> &'static str {
        &STORE_ROOT_SNAPSHOT.java_value
    }

    /// Uses the configured/default offset root and Java-compatible file layout.
    pub fn new(
        client_id: impl AsRef<str>,
        group_name: impl AsRef<str>,
    ) -> Result<Self, OffsetStoreError> {
        let root = STORE_ROOT_SNAPSHOT.path.clone();
        Self::with_root(root, client_id, group_name)
    }

    fn with_root(
        root: impl AsRef<Path>,
        client_id: impl AsRef<str>,
        group_name: impl AsRef<str>,
    ) -> Result<Self, OffsetStoreError> {
        let client_component = encode_path_component(client_id.as_ref())?;
        let group_component = encode_path_component(group_name.as_ref())?;
        let root = absolute_normalized(root.as_ref())?;
        let path = root
            .join(client_component)
            .join(group_component)
            .join(STORE_FILE_NAME);
        let normalized_path = absolute_normalized(&path)?;
        let parent = normalized_path.parent().ok_or_else(|| {
            OffsetStoreError::Runtime("offset path has no parent directory".into())
        })?;
        fs::create_dir_all(parent)?;
        let owned_path = fs::canonicalize(parent)?.join(STORE_FILE_NAME);

        let active_paths = ACTIVE_STORE_PATHS.get_or_init(Default::default);
        let mut active_paths = active_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !active_paths.insert(owned_path.clone()) {
            return Err(OffsetStoreError::AlreadyOpen);
        }
        drop(active_paths);

        #[cfg(any(unix, windows))]
        let lock_file = match acquire_process_lock(&owned_path) {
            Ok(file) => file,
            Err(error) => {
                ACTIVE_STORE_PATHS
                    .get_or_init(Default::default)
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&owned_path);
                return Err(error);
            }
        };

        Ok(Self {
            path: owned_path.clone(),
            offsets: OffsetTable::default(),
            _writer_lease: StoreWriterLease {
                path: owned_path,
                #[cfg(any(unix, windows))]
                lock_file: Some(lock_file),
            },
        })
    }

    fn read_store(&self) -> Result<HashMap<MessageQueue, i64>, OffsetStoreError> {
        read_best_snapshot(&self.path)
    }

    fn persist_values(&self, updates: HashMap<MessageQueue, i64>) -> Result<(), OffsetStoreError> {
        let mut merged = self.read_store()?;
        merged.extend(updates);
        persist_snapshot(&self.path, &merged, |_| Ok(()))
    }
}

impl OffsetStore for LocalFileOffsetStore {
    fn load(&self) -> OffsetStoreFuture<'_, ()> {
        Box::pin(async move {
            let offsets = self.read_store()?;
            self.offsets.replace(offsets);
            Ok(())
        })
    }

    fn update_offset(&self, queue: &MessageQueue, offset: i64, increase_only: bool) {
        self.offsets.update(queue, offset, increase_only);
    }

    fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
        self.offsets.update_and_freeze(queue, offset);
    }

    fn read_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        mode: ReadOffsetType,
    ) -> OffsetStoreFuture<'a, OffsetRead> {
        Box::pin(async move {
            match mode {
                ReadOffsetType::ReadFromMemory => Ok(self
                    .offsets
                    .read(queue)
                    .map_or(OffsetRead::NotFound, OffsetRead::Found)),
                ReadOffsetType::ReadFromStore => {
                    let stored = self.read_store()?.get(queue).copied();
                    if let Some(offset) = stored {
                        self.offsets.update(queue, offset, false);
                        Ok(OffsetRead::Found(offset))
                    } else {
                        Ok(OffsetRead::NotFound)
                    }
                }
                ReadOffsetType::MemoryFirstThenStore => {
                    if let Some(offset) = self.offsets.read(queue) {
                        return Ok(OffsetRead::Found(offset));
                    }
                    let stored = self.read_store()?.get(queue).copied();
                    if let Some(offset) = stored {
                        self.offsets.update(queue, offset, false);
                        Ok(OffsetRead::Found(offset))
                    } else {
                        Ok(OffsetRead::NotFound)
                    }
                }
            }
        })
    }

    fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            if let Some(offset) = self.offsets.read(queue) {
                self.persist_values(HashMap::from([(queue.clone(), offset)]))?;
            }
            Ok(())
        })
    }

    fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            if queues.is_empty() {
                return Ok(());
            }
            let selected = queues.iter().cloned().collect::<HashSet<_>>();
            let updates = self
                .offsets
                .snapshot(None)
                .into_iter()
                .filter(|(queue, _)| selected.contains(queue))
                .collect();
            self.persist_values(updates)
        })
    }

    fn remove_offset(&self, queue: &MessageQueue) {
        self.offsets.remove(queue);
    }

    fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
        self.offsets.snapshot(topic)
    }

    fn update_consume_offset_to_broker<'a>(
        &'a self,
        _queue: &'a MessageQueue,
        _offset: i64,
        _oneway: bool,
    ) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn encode_path_component(value: &str) -> Result<String, OffsetStoreError> {
    if value.is_empty() || value == "." || value == ".." {
        return Err(OffsetStoreError::InvalidPathComponent(value.to_owned()));
    }
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@' | b'#') {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write as _;
            write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail");
        }
    }
    Ok(encoded)
}

fn absolute_normalized(path: &Path) -> Result<PathBuf, OffsetStoreError> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

#[cfg(unix)]
fn acquire_process_lock(path: &Path) -> Result<File, OffsetStoreError> {
    use std::os::fd::AsRawFd;

    let lock_path = PathBuf::from(format!("{}.lock", path.display()));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    // POSIX record locks are not inherited across fork, unlike flock locks.
    // Keep the lock file in place after unlock so another process cannot lock
    // a replacement inode while an existing owner still holds this inode.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) };
    if result == 0 {
        return Ok(file);
    }
    let error = std::io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::EACCES) | Some(libc::EAGAIN)
    ) {
        Err(OffsetStoreError::AlreadyOpen)
    } else {
        Err(error.into())
    }
}

#[cfg(unix)]
fn release_process_lock(file: &File) {
    use std::os::fd::AsRawFd;

    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_UNLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    let _ = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) };
}

#[cfg(windows)]
fn acquire_process_lock(path: &Path) -> Result<File, OffsetStoreError> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::{
        Storage::FileSystem::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx},
        System::IO::OVERLAPPED,
    };

    let lock_path = PathBuf::from(format!("{}.lock", path.display()));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    let mut overlapped = OVERLAPPED::default();
    let result = unsafe {
        LockFileEx(
            file.as_raw_handle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if result != 0 {
        return Ok(file);
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(33) {
        Err(OffsetStoreError::AlreadyOpen)
    } else {
        Err(error.into())
    }
}

fn read_best_snapshot(path: &Path) -> Result<HashMap<MessageQueue, i64>, OffsetStoreError> {
    let primary = read_optional_file(path)?;
    if let Some(bytes) = primary.as_deref() {
        if let Some(parsed) = parse_offset_store(bytes) {
            return Ok(parsed);
        }
    }

    let backup_path = backup_path(path);
    let backup = read_optional_file(&backup_path)?;
    if let Some(bytes) = backup.as_deref() {
        if let Some(parsed) = parse_offset_store(bytes) {
            return Ok(parsed);
        }
    }

    if primary.is_none() && backup.is_none() {
        return Ok(HashMap::new());
    }
    Err(OffsetStoreError::CorruptStore)
}

fn read_optional_file(path: &Path) -> Result<Option<Vec<u8>>, OffsetStoreError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;
    Ok(Some(contents))
}

fn parse_offset_store(bytes: &[u8]) -> Option<HashMap<MessageQueue, i64>> {
    let source = std::str::from_utf8(bytes).ok()?;
    let mut cursor = JsonCursor { source, offset: 0 };
    cursor.skip_whitespace();
    cursor.expect(b'{').ok()?;
    let mut result = HashMap::new();
    loop {
        cursor.skip_whitespace();
        if cursor.consume(b'}') {
            break;
        }
        let field = cursor.read_string().ok()?;
        cursor.skip_whitespace();
        cursor.expect(b':').ok()?;
        cursor.skip_whitespace();
        if field == "offsetTable" {
            result = cursor.read_offset_table().ok()?;
        } else {
            cursor.skip_json_value().ok()?;
        }
        cursor.skip_whitespace();
        if cursor.consume(b'}') {
            break;
        }
        cursor.expect(b',').ok()?;
    }
    cursor.skip_whitespace();
    (cursor.offset == source.len()).then_some(result)
}

struct JsonCursor<'a> {
    source: &'a str,
    offset: usize,
}

impl JsonCursor<'_> {
    fn skip_whitespace(&mut self) {
        while self
            .source
            .as_bytes()
            .get(self.offset)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.source.as_bytes().get(self.offset) == Some(&expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: u8) -> serde_json::Result<()> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(invalid_json())
        }
    }

    fn read_string(&mut self) -> serde_json::Result<String> {
        let start = self.offset;
        self.expect(b'"')?;
        let mut escaped = false;
        while let Some(&byte) = self.source.as_bytes().get(self.offset) {
            self.offset += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                return serde_json::from_str(&self.source[start..self.offset]);
            }
        }
        Err(invalid_json())
    }

    fn read_object_key(&mut self) -> serde_json::Result<MessageQueue> {
        let start = self.offset;
        if !self.consume(b'{') {
            return Err(invalid_json());
        }
        let mut object_depth = 1usize;
        let mut array_depth = 0usize;
        let mut in_string = false;
        let mut escaped = false;
        while let Some(&byte) = self.source.as_bytes().get(self.offset) {
            self.offset += 1;
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => in_string = true,
                b'{' => object_depth += 1,
                b'}' => {
                    object_depth -= 1;
                    if object_depth == 0 && array_depth == 0 {
                        return serde_json::from_str(&self.source[start..self.offset]);
                    }
                }
                b'[' => array_depth += 1,
                b']' => array_depth = array_depth.checked_sub(1).ok_or_else(invalid_json)?,
                _ => {}
            }
        }
        Err(invalid_json())
    }

    fn read_offset_table(&mut self) -> serde_json::Result<HashMap<MessageQueue, i64>> {
        if self.source.as_bytes().get(self.offset..self.offset + 4) == Some(b"null") {
            self.offset += 4;
            return Ok(HashMap::new());
        }
        self.expect(b'{')?;
        let mut result = HashMap::new();
        loop {
            self.skip_whitespace();
            if self.consume(b'}') {
                return Ok(result);
            }
            let queue = self.read_object_key()?;
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            let value_start = self.offset;
            while self
                .source
                .as_bytes()
                .get(self.offset)
                .is_some_and(|byte| !matches!(byte, b',' | b'}'))
            {
                self.offset += 1;
            }
            let value = self.source[value_start..self.offset].trim();
            let offset = value.parse::<i64>().map_err(|_| invalid_json())?;
            result.insert(queue, offset);
            self.skip_whitespace();
            if self.consume(b'}') {
                return Ok(result);
            }
            self.expect(b',')?;
        }
    }

    fn skip_json_value(&mut self) -> serde_json::Result<()> {
        let remaining = &self.source[self.offset..];
        let mut stream =
            serde_json::Deserializer::from_str(remaining).into_iter::<serde_json::Value>();
        if stream.next().transpose()?.is_none() {
            return Err(invalid_json());
        }
        self.offset += stream.byte_offset();
        Ok(())
    }
}

fn invalid_json() -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid Java offset JSON",
    ))
}

fn backup_path(path: &Path) -> PathBuf {
    let mut backup = path.as_os_str().to_owned();
    backup.push(".bak");
    PathBuf::from(backup)
}

fn next_temp_path(path: &Path, purpose: &str) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{purpose}.{}.{}", std::process::id(), sequence));
    PathBuf::from(name)
}

fn persist_snapshot(
    path: &Path,
    values: &HashMap<MessageQueue, i64>,
    mut after_step: impl FnMut(PersistStep) -> std::io::Result<()>,
) -> Result<(), OffsetStoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| OffsetStoreError::Runtime("offset path has no parent directory".into()))?;
    fs::create_dir_all(parent)?;
    let bytes = encode_offset_store(values)?;
    let temp_path = next_temp_path(path, "tmp");
    let temp_guard = TempFile(temp_path.clone());
    let mut temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)?;
    after_step(PersistStep::TempCreated)?;
    temp.write_all(&bytes)?;
    temp.sync_all()?;
    drop(temp);
    after_step(PersistStep::TempSynced)?;

    if let Some(old_primary) = read_optional_file(path)? {
        if parse_offset_store(&old_primary).is_some() {
            let backup = backup_path(path);
            let backup_temp_path = next_temp_path(&backup, "tmp");
            let backup_guard = TempFile(backup_temp_path.clone());
            let mut backup_temp = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&backup_temp_path)?;
            backup_temp.write_all(&old_primary)?;
            backup_temp.sync_all()?;
            drop(backup_temp);
            after_step(PersistStep::BackupTempSynced)?;
            replace_file(&backup_temp_path, &backup)?;
            std::mem::forget(backup_guard);
            sync_directory(parent)?;
            after_step(PersistStep::BackupInstalled)?;
        }
    }

    replace_file(&temp_path, path)?;
    std::mem::forget(temp_guard);
    after_step(PersistStep::PrimaryInstalled)?;
    sync_directory(parent)?;
    after_step(PersistStep::DirectorySynced)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), OffsetStoreError> {
    #[cfg(windows)]
    {
        let _ = path;
        Ok(())
    }
    #[cfg(not(windows))]
    match File::open(path).and_then(|directory| directory.sync_all()) {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::Unsupported | std::io::ErrorKind::InvalidInput
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn replace_file(source: &Path, destination: &Path) -> Result<(), OffsetStoreError> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        };

        let source = source
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let destination = destination
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        fs::rename(source, destination)?;
        Ok(())
    }
}

fn encode_offset_store(values: &HashMap<MessageQueue, i64>) -> Result<Vec<u8>, OffsetStoreError> {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| {
        left.topic()
            .cmp(&right.topic())
            .then_with(|| left.broker_name().cmp(&right.broker_name()))
            .then_with(|| left.queue_id().cmp(&right.queue_id()))
    });
    let mut output = String::from("{\n\t\"offsetTable\":{");
    for (index, (queue, offset)) in entries.into_iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str("\n\t\t{");
        if let Some(broker) = queue.broker_name() {
            output.push_str("\n\t\t\t\"brokerName\":");
            output.push_str(&serde_json::to_string(broker)?);
            output.push(',');
        }
        output.push_str(&format!("\n\t\t\t\"queueId\":{}", queue.queue_id()));
        if let Some(topic) = queue.topic() {
            output.push_str(",\n\t\t\t\"topic\":");
            output.push_str(&serde_json::to_string(topic)?);
        }
        output.push_str("\n\t\t}:");
        output.push_str(&offset.to_string());
    }
    if !values.is_empty() {
        output.push('\n');
    }
    output.push_str("\t}\n}");
    Ok(output.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn clone_topic_filter_matches_pinned_java_for_every_bmp_scalar() {
        let root = temporary_root();
        let store = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        super::super::assert_clone_topic_filter_matches_java(&store);
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    fn temporary_root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "rocketmq-offset-local-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn run_store_root_child(mode: &str, property: Option<&str>, home: Option<&str>) {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "offset::local::tests::local_offset_store_dir_snapshots_like_java",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("ROCKETMQ_CLASSIC_OFFSET_ROOT_CHILD", mode)
            .env_remove(STORE_ROOT_PROPERTY)
            .env_remove("HOME")
            .env_remove("USERPROFILE");
        if let Some(property) = property {
            command.env(STORE_ROOT_PROPERTY, property);
        }
        if let Some(home) = home {
            command.env("HOME", home);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "offset root child {mode} failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run_store_lock_child(root: &Path, should_open: bool) {
        let mut command = Command::new(std::env::current_exe().unwrap());
        let output = command
            .args([
                "--exact",
                "offset::local::tests::process_lock_child_probe",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("ROCKETMQ_CLASSIC_OFFSET_LOCK_CHILD_ROOT", root)
            .env(
                "ROCKETMQ_CLASSIC_OFFSET_LOCK_CHILD_OPEN",
                should_open.to_string(),
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "offset lock child (should_open={should_open}) failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn process_lock_child_probe() {
        let Some(root) = std::env::var_os("ROCKETMQ_CLASSIC_OFFSET_LOCK_CHILD_ROOT") else {
            return;
        };
        let should_open = std::env::var("ROCKETMQ_CLASSIC_OFFSET_LOCK_CHILD_OPEN").unwrap();
        let result = LocalFileOffsetStore::with_root(root, "client", "group");
        match (should_open.as_str(), result) {
            ("true", Ok(store)) => drop(store),
            ("false", Err(OffsetStoreError::AlreadyOpen)) => {}
            (_, Ok(store)) => {
                drop(store);
                panic!("unexpectedly acquired the cross-process offset lock");
            }
            (_, Err(error)) => panic!("cross-process offset lock failed: {error:?}"),
        }
    }

    #[test]
    fn process_lock_excludes_another_process_until_the_owner_drops() {
        let root = temporary_root();
        let store = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        run_store_lock_child(&root, false);
        drop(store);
        run_store_lock_child(&root, true);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn local_offset_store_dir_snapshots_like_java() {
        if let Some(mode) = std::env::var_os("ROCKETMQ_CLASSIC_OFFSET_ROOT_CHILD") {
            match mode.to_string_lossy().as_ref() {
                "override" => {
                    let original = std::env::var(STORE_ROOT_PROPERTY).unwrap();
                    assert_eq!(LocalFileOffsetStore::local_offset_store_dir(), original);

                    // SAFETY: this subprocess runs only this exact test with one
                    // test thread and has no other code reading environment variables.
                    unsafe {
                        std::env::set_var(STORE_ROOT_PROPERTY, "changed-after-class-init");
                    }
                    assert_eq!(LocalFileOffsetStore::local_offset_store_dir(), original);

                    let store = LocalFileOffsetStore::new("client", "group").unwrap();
                    let expected_parent =
                        fs::canonicalize(Path::new(&original).join("client").join("group"))
                            .unwrap();
                    assert_eq!(store.path, expected_parent.join(STORE_FILE_NAME));
                    drop(store);
                    fs::remove_dir_all(original).unwrap();
                }
                "home" => {
                    let home = std::env::var("HOME").unwrap();
                    let expected = format!("{home}{}.rocketmq_offsets", std::path::MAIN_SEPARATOR);
                    assert_eq!(LocalFileOffsetStore::local_offset_store_dir(), expected);
                    assert_eq!(
                        STORE_ROOT_SNAPSHOT.path,
                        PathBuf::from(home).join(".rocketmq_offsets")
                    );
                }
                "empty" => {
                    assert_eq!(LocalFileOffsetStore::local_offset_store_dir(), "");
                    assert_eq!(
                        STORE_ROOT_SNAPSHOT.path,
                        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
                    );
                }
                "empty-home" => {
                    let expected = format!("{}.rocketmq_offsets", std::path::MAIN_SEPARATOR);
                    assert_eq!(LocalFileOffsetStore::local_offset_store_dir(), expected);
                    assert_eq!(
                        STORE_ROOT_SNAPSHOT.path,
                        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
                            .join(".rocketmq_offsets")
                    );
                }
                "no-home" => {
                    let expected =
                        format!("null{}{}", std::path::MAIN_SEPARATOR, ".rocketmq_offsets");
                    assert_eq!(LocalFileOffsetStore::local_offset_store_dir(), expected);
                    assert_eq!(STORE_ROOT_SNAPSHOT.path, PathBuf::from(expected));
                }
                other => panic!("unexpected offset-root child mode: {other}"),
            }
            return;
        }

        let override_root = temporary_root();
        let override_root = override_root.to_str().unwrap();
        let java_case: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/offsets.json"))
                .unwrap();
        let java_property_case = &java_case["localOffsetStoreDirProperty"];
        assert_eq!(
            java_property_case["valueAtInitialization"],
            java_property_case["valueAfterHomeChange"]
        );
        assert_eq!(
            java_property_case["valueAtInitialization"],
            "java-home-before-store-class-init/.rocketmq_offsets"
        );

        run_store_root_child("override", Some(override_root), None);
        run_store_root_child("home", None, Some("java-home-before-store-class-init"));
        run_store_root_child("empty", Some(""), None);
        run_store_root_child("empty-home", None, Some(""));
        run_store_root_child("no-home", None, None);
    }

    fn fixture() -> Vec<u8> {
        include_bytes!("../../../../fixtures/offset/local-offset-store.json").to_vec()
    }

    #[test]
    fn reads_java_fastjson_object_keys_and_exact_long_offsets() {
        let parsed = parse_offset_store(&fixture()).unwrap();
        assert_eq!(
            parsed.get(&MessageQueue::new("orders", "broker-a", 2)),
            Some(&41)
        );
        assert_eq!(
            parsed.get(&MessageQueue::new("events", "broker-b", 0)),
            Some(&9_007_199_254_740_993)
        );
        let round_trip = parse_offset_store(&encode_offset_store(&parsed).unwrap()).unwrap();
        assert_eq!(round_trip, parsed);
    }

    #[tokio::test]
    async fn fixture_load_read_modes_and_restart_persist_match_java_layout() {
        let root = temporary_root();
        let store = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        fs::create_dir_all(store.path.parent().unwrap()).unwrap();
        fs::write(&store.path, fixture()).unwrap();
        store.load().await.unwrap();

        let orders = MessageQueue::new("orders", "broker-a", 2);
        assert_eq!(
            store
                .read_offset(&orders, ReadOffsetType::ReadFromMemory)
                .await
                .unwrap(),
            OffsetRead::Found(41)
        );
        store.update_offset(&orders, 42, false);
        store.persist(&orders).await.unwrap();
        let persisted_path = store.path.clone();
        drop(store);
        let restarted = LocalFileOffsetStore::with_root(&root, "client", "group")
            .unwrap_or_else(|error| {
                let active = ACTIVE_STORE_PATHS
                    .get()
                    .and_then(|paths| paths.lock().ok())
                    .is_some_and(|paths| paths.contains(&persisted_path));
                panic!(
                    "failed to reopen {:?} after dropping its owner (same-process owner still registered: {active}): {error:?}",
                    persisted_path,
                );
            });
        assert_eq!(
            restarted
                .read_offset(&orders, ReadOffsetType::ReadFromStore)
                .await
                .unwrap(),
            OffsetRead::Found(42)
        );
        drop(restarted);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn persist_all_writes_only_selected_queues_and_restores_after_restart() {
        let root = temporary_root();
        let orders = MessageQueue::new("orders", "broker", 0);
        let events = MessageQueue::new("events", "broker", 1);
        let store = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        store.update_offset(&orders, 51, false);
        store.update_offset(&events, 92, false);
        store
            .persist_all(std::slice::from_ref(&orders))
            .await
            .unwrap();

        assert_eq!(
            store
                .read_offset(&events, ReadOffsetType::ReadFromMemory)
                .await
                .unwrap(),
            OffsetRead::Found(92)
        );
        drop(store);

        let restarted = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        restarted.load().await.unwrap();
        assert_eq!(
            restarted
                .read_offset(&orders, ReadOffsetType::ReadFromMemory)
                .await
                .unwrap(),
            OffsetRead::Found(51)
        );
        assert_eq!(
            restarted
                .read_offset(&events, ReadOffsetType::ReadFromMemory)
                .await
                .unwrap(),
            OffsetRead::NotFound
        );
        drop(restarted);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn repeated_offset_persistence_keeps_long_running_snapshot_restorable() {
        const PERSIST_ROUNDS: i64 = 128;
        const QUEUE_COUNT: usize = 8;

        let root = temporary_root();
        let queues = (0..QUEUE_COUNT)
            .map(|queue_id| MessageQueue::new("persistent", "broker", queue_id as i32))
            .collect::<Vec<_>>();
        let store = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();

        for round in 0..PERSIST_ROUNDS {
            let mut expected = HashMap::new();
            for (queue_index, queue) in queues.iter().enumerate() {
                let offset = round * QUEUE_COUNT as i64 + queue_index as i64;
                store.update_offset(queue, offset, false);
                expected.insert(queue.clone(), offset);
            }
            store.persist_all(&queues).await.unwrap();
            assert_eq!(store.read_store().unwrap(), expected);
        }

        drop(store);
        let restarted = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        restarted.load().await.unwrap();
        for (queue_index, queue) in queues.iter().enumerate() {
            let expected = (PERSIST_ROUNDS - 1) * QUEUE_COUNT as i64 + queue_index as i64;
            assert_eq!(
                restarted
                    .read_offset(queue, ReadOffsetType::ReadFromMemory)
                    .await
                    .unwrap(),
                OffsetRead::Found(expected)
            );
        }
        drop(restarted);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_primary_recovers_backup_and_both_corrupt_files_error() {
        let root = temporary_root();
        let path = root.join("offsets.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(&path, b"not json").unwrap();
        fs::write(backup_path(&path), fixture()).unwrap();
        assert_eq!(read_best_snapshot(&path).unwrap().len(), 2);

        fs::write(backup_path(&path), b"also not json").unwrap();
        assert!(matches!(
            read_best_snapshot(&path),
            Err(OffsetStoreError::CorruptStore)
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_empty_or_traversing_components_and_encodes_separators() {
        assert!(matches!(
            encode_path_component(""),
            Err(OffsetStoreError::InvalidPathComponent(_))
        ));
        assert!(matches!(
            encode_path_component(".."),
            Err(OffsetStoreError::InvalidPathComponent(_))
        ));
        assert_eq!(
            encode_path_component("worker/group").unwrap(),
            "worker%2Fgroup"
        );
        assert_eq!(encode_path_component("../worker").unwrap(), "..%2Fworker");
    }

    #[test]
    fn only_one_store_in_process_can_own_a_file_path() {
        let root = temporary_root();
        let first = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        assert!(matches!(
            LocalFileOffsetStore::with_root(&root, "client", "group"),
            Err(OffsetStoreError::AlreadyOpen)
        ));
        drop(first);
        let reopened = LocalFileOffsetStore::with_root(&root, "client", "group").unwrap();
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn injected_crashes_always_leave_a_recoverable_old_or_new_snapshot() {
        for failed_step in [
            PersistStep::TempCreated,
            PersistStep::TempSynced,
            PersistStep::BackupTempSynced,
            PersistStep::BackupInstalled,
            PersistStep::PrimaryInstalled,
            PersistStep::DirectorySynced,
        ] {
            let root = temporary_root();
            let path = root.join("offsets.json");
            let queue = MessageQueue::new("orders", "broker-a", 2);
            let old = HashMap::from([(queue.clone(), 41)]);
            let new = HashMap::from([(queue.clone(), 42)]);
            persist_snapshot(&path, &old, |_| Ok(())).unwrap();
            let result = persist_snapshot(&path, &new, |step| {
                if step == failed_step {
                    Err(std::io::Error::other("injected interruption"))
                } else {
                    Ok(())
                }
            });
            assert!(
                result.is_err(),
                "fault point {failed_step:?} was not reached"
            );
            let recovered = read_best_snapshot(&path).unwrap();
            assert!(
                recovered == old || recovered == new,
                "fault point {failed_step:?} lost both generations"
            );
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn malformed_primary_with_valid_backup_is_replaced_from_recovered_snapshot() {
        let root = temporary_root();
        let path = root.join("offsets.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(&path, b"broken").unwrap();
        fs::write(backup_path(&path), fixture()).unwrap();
        let recovered = read_best_snapshot(&path).unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 2);
        persist_snapshot(&path, &recovered, |_| Ok(())).unwrap();
        assert_eq!(read_best_snapshot(&path).unwrap(), recovered);
        assert_eq!(read_best_snapshot(&path).unwrap().get(&queue), Some(&41));
        fs::remove_dir_all(root).unwrap();
    }
}
