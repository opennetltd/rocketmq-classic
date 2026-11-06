use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

mod local;
mod remote;

pub use local::LocalFileOffsetStore;
pub use remote::RemoteBrokerOffsetStore;

#[cfg(test)]
fn assert_clone_topic_filter_matches_java(store: &dyn OffsetStore) {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../../fixtures/runtime/offsets.json")).unwrap();
    let blank: HashSet<u32> = fixture["cloneOffsetTableBlankTopicCodeUnits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_u64().unwrap() as u32)
        .collect();
    let expected = HashMap::from([
        (MessageQueue::new("orders", "broker", 0), 42),
        (MessageQueue::new("events", "broker", 1), -7),
    ]);
    for (queue, offset) in &expected {
        store.update_offset(queue, *offset, false);
    }
    for code in 0..=u16::MAX as u32 {
        let Some(character) = char::from_u32(code) else {
            continue;
        };
        let topic = character.to_string();
        let actual = store.clone_offset_table(Some(&topic));
        if blank.contains(&code) {
            assert_eq!(actual, expected, "Java blank topic U+{code:04X}");
        } else {
            assert!(actual.is_empty(), "Java literal topic U+{code:04X}");
        }
    }
    for topic in [None, Some(""), Some(" \t\n")] {
        assert_eq!(store.clone_offset_table(topic), expected);
    }
    assert_eq!(
        store.clone_offset_table(Some("orders")),
        HashMap::from([(MessageQueue::new("orders", "broker", 0), 42)])
    );
    assert!(store.clone_offset_table(Some(" orders ")).is_empty());
    let mut detached = store.clone_offset_table(None);
    detached.clear();
    assert_eq!(store.clone_offset_table(None), expected);
}

use rocketmq_classic_common::message::MessageQueue;
use thiserror::Error;

/// How `OffsetStore::read_offset` chooses between memory and persistent state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOffsetType {
    ReadFromMemory,
    ReadFromStore,
    MemoryFirstThenStore,
}

/// A successful offset lookup. Absence and a failed lookup are distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetRead {
    Found(i64),
    NotFound,
}

impl OffsetRead {
    /// Java `OffsetStore.readOffset` uses `-1` only for a missing offset.
    /// Rust callers should retain `NotFound` and errors until reaching a
    /// Java-compatible API boundary.
    pub const fn java_value(self) -> i64 {
        match self {
            Self::Found(offset) => offset,
            Self::NotFound => -1,
        }
    }
}

pub type OffsetStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, OffsetStoreError>> + Send + 'a>>;

#[derive(Debug, Error)]
pub enum OffsetStoreError {
    #[error("offset store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("offset store JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("offset store RPC failed: {0}")]
    Rpc(#[from] rocketmq_classic_core::error::RpcError),
    #[error("client runtime failed: {0}")]
    Runtime(String),
    #[error("broker `{0}` was not found in the current route")]
    BrokerNotFound(String),
    #[error("offset store path component `{0}` is empty or unsafe")]
    InvalidPathComponent(String),
    #[error("local offset file and its backup are both unreadable")]
    CorruptStore,
    #[error("an offset file for this client/group is already open for writing")]
    AlreadyOpen,
}

/// Offset state shared by the remote and local stores. The update value and
/// freeze bit are protected by the same per-queue lock so reset/freeze and
/// concurrent updates have a single atomic order.
#[derive(Default)]
struct OffsetTable {
    entries: RwLock<HashMap<MessageQueue, Arc<Mutex<ControllableOffset>>>>,
}

#[derive(Debug)]
struct ControllableOffset {
    value: i64,
    allow_update: bool,
}

impl OffsetTable {
    fn read_entries(
        &self,
    ) -> RwLockReadGuard<'_, HashMap<MessageQueue, Arc<Mutex<ControllableOffset>>>> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_entries(
        &self,
    ) -> RwLockWriteGuard<'_, HashMap<MessageQueue, Arc<Mutex<ControllableOffset>>>> {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_offset(offset: &Mutex<ControllableOffset>) -> MutexGuard<'_, ControllableOffset> {
        offset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn entry(&self, queue: &MessageQueue, initial: i64) -> Arc<Mutex<ControllableOffset>> {
        let mut entries = self.write_entries();
        entries
            .entry(queue.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(ControllableOffset {
                    value: initial,
                    allow_update: true,
                }))
            })
            .clone()
    }

    fn update(&self, queue: &MessageQueue, offset: i64, increase_only: bool) {
        let entry = self.entry(queue, offset);
        let mut current = Self::lock_offset(&entry);
        if current.allow_update {
            current.value = if increase_only {
                current.value.max(offset)
            } else {
                offset
            };
        }
    }

    fn update_and_freeze(&self, queue: &MessageQueue, offset: i64) {
        let entry = self.entry(queue, offset);
        let mut current = Self::lock_offset(&entry);
        current.allow_update = false;
        current.value = offset;
    }

    fn read(&self, queue: &MessageQueue) -> Option<i64> {
        let entry = self.read_entries().get(queue).cloned()?;
        let offset = Self::lock_offset(&entry).value;
        Some(offset)
    }

    fn remove(&self, queue: &MessageQueue) -> bool {
        self.write_entries().remove(queue).is_some()
    }

    fn snapshot(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
        // Java stores use UtilAll.isBlank -> StringUtils.isBlank, whose
        // Character.isWhitespace set differs from Rust's Unicode White_Space.
        let topic = topic.filter(|topic| {
            !topic.chars().all(|character| {
                matches!(character,
                    '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}'
                    | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}'
                    | '\u{2028}'..='\u{2029}' | '\u{205f}' | '\u{3000}')
            })
        });
        let entries = self.read_entries();
        entries
            .iter()
            .filter(|(queue, _)| topic.is_none_or(|topic| queue.topic() == Some(topic)))
            .map(|(queue, offset)| (queue.clone(), Self::lock_offset(offset).value))
            .collect()
    }

    fn replace(&self, values: HashMap<MessageQueue, i64>) {
        let mut entries = self.write_entries();
        *entries = values
            .into_iter()
            .map(|(queue, value)| {
                (
                    queue,
                    Arc::new(Mutex::new(ControllableOffset {
                        value,
                        allow_update: true,
                    })),
                )
            })
            .collect();
    }

    fn remove_unselected(&self, selected: &HashSet<MessageQueue>) {
        self.write_entries()
            .retain(|queue, _| selected.contains(queue));
    }
}

/// A pluggable offset store. The boxed futures keep this API object-safe so a
/// pull consumer can accept either built-in store or a user implementation.
pub trait OffsetStore: Send + Sync {
    fn load(&self) -> OffsetStoreFuture<'_, ()>;
    fn update_offset(&self, queue: &MessageQueue, offset: i64, increase_only: bool);
    fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64);
    fn read_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        mode: ReadOffsetType,
    ) -> OffsetStoreFuture<'a, OffsetRead>;
    fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()>;
    fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()>;
    fn remove_offset(&self, queue: &MessageQueue);
    fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64>;
    fn update_consume_offset_to_broker<'a>(
        &'a self,
        queue: &'a MessageQueue,
        offset: i64,
        oneway: bool,
    ) -> OffsetStoreFuture<'a, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(topic: &str, queue_id: i32) -> MessageQueue {
        MessageQueue::new(topic, "broker-a", queue_id)
    }

    #[test]
    fn offset_table_distinguishes_missing_values_and_filters_snapshots_by_topic() {
        let table = OffsetTable::default();
        let a = queue("orders", 0);
        let b = queue("payments", 1);

        assert_eq!(table.read(&a), None);
        table.update(&a, 10, false);
        table.update(&b, 20, false);

        assert_eq!(table.read(&a), Some(10));
        assert_eq!(
            table.snapshot(Some("orders")),
            HashMap::from([(a.clone(), 10)])
        );
        assert_eq!(table.snapshot(None).len(), 2);
    }

    #[test]
    fn increase_only_updates_ignore_stale_offsets_but_unconditional_updates_can_reset() {
        let table = OffsetTable::default();
        let queue = queue("orders", 0);

        table.update(&queue, 50, false);
        table.update(&queue, 30, true);
        assert_eq!(table.read(&queue), Some(50));

        table.update(&queue, 12, false);
        assert_eq!(table.read(&queue), Some(12));
    }

    #[test]
    fn freeze_and_value_change_are_one_atomic_transition() {
        let table = OffsetTable::default();
        let queue = queue("orders", 0);

        table.update(&queue, 10, false);
        table.update_and_freeze(&queue, 40);
        table.update(&queue, 80, false);
        table.update(&queue, 90, true);

        assert_eq!(table.read(&queue), Some(40));

        // Java permits another explicit freeze to establish a newer reset.
        table.update_and_freeze(&queue, 25);
        assert_eq!(table.read(&queue), Some(25));
    }

    #[test]
    fn removing_a_frozen_queue_allows_a_fresh_offset_lifecycle() {
        let table = OffsetTable::default();
        let queue = queue("orders", 0);

        table.update_and_freeze(&queue, 15);
        assert!(table.remove(&queue));
        assert!(!table.remove(&queue));
        table.update(&queue, 22, false);

        assert_eq!(table.read(&queue), Some(22));
    }

    #[test]
    fn loading_replaces_existing_offsets_and_reopens_updates() {
        let table = OffsetTable::default();
        let old = queue("orders", 0);
        let loaded = queue("orders", 1);
        table.update(&old, 10, false);

        table.replace(HashMap::from([(loaded.clone(), 60)]));
        assert_eq!(table.read(&old), None);
        assert_eq!(table.read(&loaded), Some(60));
        table.update(&loaded, 30, true);
        assert_eq!(table.read(&loaded), Some(60));
    }

    #[test]
    fn concurrent_updates_and_freeze_leave_the_frozen_value_unchanged() {
        let table = Arc::new(OffsetTable::default());
        let queue = queue("orders", 0);
        let writer = {
            let table = table.clone();
            let queue = queue.clone();
            std::thread::spawn(move || {
                for value in 0..1_000 {
                    table.update(&queue, value, true);
                }
            })
        };
        table.update_and_freeze(&queue, 2_000);
        writer.join().unwrap();

        assert_eq!(table.read(&queue), Some(2_000));
    }
}
