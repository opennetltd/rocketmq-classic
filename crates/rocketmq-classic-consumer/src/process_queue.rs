// P10-06 attaches these queue operations to bounded consume requests. Keep
// this owner private until the callback lifecycle is complete.
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) const PULL_MAX_IDLE_TIME_PROPERTY: &str = "rocketmq.client.pull.pullMaxIdleTime";
pub(crate) const PULL_MAX_IDLE_TIME_MILLIS: i64 = 120_000;
pub(crate) const REBALANCE_LOCK_MAX_LIVE_TIME_PROPERTY: &str =
    "rocketmq.client.rebalance.lockMaxLiveTime";
pub(crate) const REBALANCE_LOCK_MAX_LIVE_TIME_MILLIS: i64 = 30_000;
pub(crate) const REBALANCE_LOCK_INTERVAL_PROPERTY: &str = "rocketmq.client.rebalance.lockInterval";
pub(crate) const REBALANCE_LOCK_INTERVAL_MILLIS: i64 = 20_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct JavaRebalanceLockSettings {
    max_live_time_millis: i64,
    interval_millis: i64,
}

pub(crate) fn pull_idle_time_expired(
    now_millis: i64,
    last_pull_millis: i64,
    max_idle_time_millis: i64,
) -> bool {
    now_millis.wrapping_sub(last_pull_millis) > max_idle_time_millis
}

pub(crate) fn pull_max_idle_time_from_environment() -> Result<i64, String> {
    match std::env::var(PULL_MAX_IDLE_TIME_PROPERTY) {
        Ok(value) => parse_pull_max_idle_time(Some(&value)),
        Err(std::env::VarError::NotPresent) => parse_pull_max_idle_time(None),
        Err(std::env::VarError::NotUnicode(value)) => Err(format!(
            "non-Unicode Java-compatible long property {PULL_MAX_IDLE_TIME_PROPERTY}={value:?}"
        )),
    }
}

pub(crate) fn cached_java_property<T: Clone, E: Clone>(
    cache: &OnceLock<Result<T, E>>,
    read_property: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    cache.get_or_init(read_property).clone()
}

pub(crate) fn process_queue_pull_max_idle_time() -> Result<i64, String> {
    static JAVA_PULL_MAX_IDLE_TIME: OnceLock<Result<i64, String>> = OnceLock::new();
    cached_java_property(
        &JAVA_PULL_MAX_IDLE_TIME,
        pull_max_idle_time_from_environment,
    )
}

fn java_rebalance_lock_settings() -> Result<JavaRebalanceLockSettings, String> {
    static SETTINGS: OnceLock<Result<JavaRebalanceLockSettings, String>> = OnceLock::new();
    cached_java_property(&SETTINGS, || {
        Ok(JavaRebalanceLockSettings {
            max_live_time_millis: java_long_property(
                REBALANCE_LOCK_MAX_LIVE_TIME_PROPERTY,
                REBALANCE_LOCK_MAX_LIVE_TIME_MILLIS,
            )?,
            interval_millis: java_long_property(
                REBALANCE_LOCK_INTERVAL_PROPERTY,
                REBALANCE_LOCK_INTERVAL_MILLIS,
            )?,
        })
    })
}

fn java_long_property(name: &str, default: i64) -> Result<i64, String> {
    match std::env::var(name) {
        Ok(value) => parse_java_long(&value)
            .ok_or_else(|| format!("invalid Java-compatible long property {name}={value:?}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(value)) => Err(format!(
            "non-Unicode Java-compatible long property {name}={value:?}"
        )),
    }
}

fn parse_pull_max_idle_time(value: Option<&str>) -> Result<i64, String> {
    match value {
        Some(value) => parse_java_long(value).ok_or_else(|| {
            format!("invalid Java-compatible long property {PULL_MAX_IDLE_TIME_PROPERTY}={value:?}")
        }),
        None => Ok(PULL_MAX_IDLE_TIME_MILLIS),
    }
}

pub(crate) fn parse_java_long(value: &str) -> Option<i64> {
    let mut units = value.encode_utf16().peekable();
    let negative = match units.peek().copied()? {
        0x002d => {
            units.next();
            true
        }
        0x002b => {
            units.next();
            false
        }
        _ => false,
    };
    units.peek()?;
    let limit = if negative { i64::MIN } else { -i64::MAX };
    let multiply_limit = limit / 10;
    let mut result = 0_i64;
    for unit in units {
        let digit = i64::from(java_decimal_digit(unit)?);
        if result < multiply_limit {
            return None;
        }
        result *= 10;
        if result < limit + digit {
            return None;
        }
        result -= digit;
    }
    Some(if negative { result } else { -result })
}

fn java_decimal_digit(unit: u16) -> Option<u16> {
    const ZEROS: &[u16] = &[
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
        0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
        0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];
    ZEROS
        .iter()
        .find_map(|zero| unit.checked_sub(*zero).filter(|digit| *digit < 10))
}

use rocketmq_classic_common::message::{
    MessageExt, MessageQueue,
    constants::{PROPERTY_CONSUME_START_TIMESTAMP, PROPERTY_MAX_OFFSET},
};
use tokio::time::Instant;

/// A single queue assignment's retained messages and in-flight consume work.
/// Every operation that can change ownership state is scoped to its generation.
pub struct ProcessQueue {
    queue: MessageQueue,
    generation: u64,
    state: Mutex<ProcessQueueState>,
    orderly_consume_gate: Arc<tokio::sync::RwLock<()>>,
}

struct ProcessQueueState {
    messages: BTreeMap<i64, QueueMessage>,
    ready_duplicates: BTreeMap<i64, QueueMessage>,
    in_flight_batches: HashMap<u64, Vec<i64>>,
    cached_count: i64,
    cached_bytes: i64,
    highest_seen_offset: Option<i64>,
    queue_offset_max: i64,
    msg_acc_cnt: i64,
    dropped: bool,
    consuming: bool,
    next_batch_id: u64,
    last_pull_timestamp_millis: i64,
    last_consume_timestamp_millis: i64,
    locked: bool,
    last_lock_timestamp_millis: i64,
    last_lock_instant: Option<Instant>,
    lock_attempt_id: u64,
    try_unlock_times: i64,
}

struct QueueMessage {
    message: Arc<MessageExt>,
    body_bytes: u64,
    state: MessageState,
    consume_start_timestamp_millis: ConsumeStartTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConsumeStartTimestamp {
    Missing,
    Parsed(i64),
    Invalid(String),
}

fn consume_start_timestamp(message: &MessageExt) -> ConsumeStartTimestamp {
    match message.get_property(PROPERTY_CONSUME_START_TIMESTAMP) {
        None => ConsumeStartTimestamp::Missing,
        Some(value) if value.is_empty() => ConsumeStartTimestamp::Missing,
        Some(value) => match parse_java_long(&value) {
            Some(timestamp) => ConsumeStartTimestamp::Parsed(timestamp),
            None => ConsumeStartTimestamp::Invalid(value),
        },
    }
}

#[derive(Clone, Copy)]
enum ProcessQueueCounterKind {
    Count,
    Size,
}

/// Mutable Java `AtomicLong` counterpart for the cached count or byte size.
pub struct ProcessQueueAtomicLong<'a> {
    state: &'a Mutex<ProcessQueueState>,
    kind: ProcessQueueCounterKind,
}

impl ProcessQueueAtomicLong<'_> {
    pub fn get(&self) -> i64 {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.value(&state)
    }

    pub fn set(&self, value: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.set_value(&mut state, value);
    }

    pub fn get_and_add(&self, delta: i64) -> i64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = self.value(&state);
        self.set_value(&mut state, previous.wrapping_add(delta));
        previous
    }

    pub fn add_and_get(&self, delta: i64) -> i64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = self.value(&state).wrapping_add(delta);
        self.set_value(&mut state, value);
        value
    }

    pub fn get_and_set(&self, value: i64) -> i64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = self.value(&state);
        self.set_value(&mut state, value);
        previous
    }

    pub fn increment_and_get(&self) -> i64 {
        self.add_and_get(1)
    }

    pub fn get_and_increment(&self) -> i64 {
        self.get_and_add(1)
    }

    pub fn decrement_and_get(&self) -> i64 {
        self.add_and_get(-1)
    }

    pub fn get_and_decrement(&self) -> i64 {
        self.get_and_add(-1)
    }

    pub fn compare_and_set(&self, expected: i64, value: i64) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.value(&state) != expected {
            return false;
        }
        self.set_value(&mut state, value);
        true
    }

    fn value(&self, state: &ProcessQueueState) -> i64 {
        match self.kind {
            ProcessQueueCounterKind::Count => state.cached_count,
            ProcessQueueCounterKind::Size => state.cached_bytes,
        }
    }

    fn set_value(&self, state: &mut ProcessQueueState, value: i64) {
        match self.kind {
            ProcessQueueCounterKind::Count => state.cached_count = value,
            ProcessQueueCounterKind::Size => state.cached_bytes = value,
        }
    }
}

/// Safe Rust view of Java's directly exposed mutable `msgTreeMap`.
///
/// The read and write guards hold the queue lock for their full lifetime.
/// Direct map edits intentionally do not change the separate message count or
/// byte count, matching Java's independent `AtomicLong` fields.
pub struct ProcessQueueMessageTreeMap<'a> {
    state: &'a Mutex<ProcessQueueState>,
}

pub struct ProcessQueueMessageTreeReadGuard<'a> {
    _state: MutexGuard<'a, ProcessQueueState>,
    view: BTreeMap<i64, Arc<MessageExt>>,
}

pub struct ProcessQueueMessageTreeWriteGuard<'a> {
    state: MutexGuard<'a, ProcessQueueState>,
    view: BTreeMap<i64, Arc<MessageExt>>,
}

impl ProcessQueueMessageTreeMap<'_> {
    pub fn read(&self) -> ProcessQueueMessageTreeReadGuard<'_> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let view = ready_message_view(&state);
        ProcessQueueMessageTreeReadGuard {
            _state: state,
            view,
        }
    }

    pub fn write(&self) -> ProcessQueueMessageTreeWriteGuard<'_> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let view = ready_message_view(&state);
        ProcessQueueMessageTreeWriteGuard { state, view }
    }
}

impl Deref for ProcessQueueMessageTreeReadGuard<'_> {
    type Target = BTreeMap<i64, Arc<MessageExt>>;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl Deref for ProcessQueueMessageTreeWriteGuard<'_> {
    type Target = BTreeMap<i64, Arc<MessageExt>>;

    fn deref(&self) -> &Self::Target {
        &self.view
    }
}

impl DerefMut for ProcessQueueMessageTreeWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.view
    }
}

impl Drop for ProcessQueueMessageTreeWriteGuard<'_> {
    fn drop(&mut self) {
        reconcile_ready_message_view(&mut self.state, &self.view);
    }
}

fn ready_message_view(state: &ProcessQueueState) -> BTreeMap<i64, Arc<MessageExt>> {
    let mut view = state
        .messages
        .iter()
        .filter(|(_, message)| message.state == MessageState::Ready)
        .map(|(offset, message)| (*offset, message.message.clone()))
        .collect::<BTreeMap<_, _>>();
    view.extend(
        state
            .ready_duplicates
            .iter()
            .map(|(offset, message)| (*offset, message.message.clone())),
    );
    view
}

fn ready_message_by_offset(state: &ProcessQueueState, offset: i64) -> Option<&QueueMessage> {
    state
        .messages
        .get(&offset)
        .filter(|message| message.state == MessageState::Ready)
        .or_else(|| state.ready_duplicates.get(&offset))
}

fn ready_offsets(state: &ProcessQueueState) -> Vec<i64> {
    state
        .messages
        .iter()
        .filter(|(_, message)| message.state == MessageState::Ready)
        .map(|(offset, _)| *offset)
        .chain(state.ready_duplicates.keys().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn has_ready_messages(state: &ProcessQueueState) -> bool {
    state
        .messages
        .values()
        .any(|message| message.state == MessageState::Ready)
        || !state.ready_duplicates.is_empty()
}

fn first_ready_offset(state: &ProcessQueueState) -> Option<i64> {
    ready_offsets(state).into_iter().next()
}

fn reconcile_ready_message_view(
    state: &mut ProcessQueueState,
    view: &BTreeMap<i64, Arc<MessageExt>>,
) {
    for offset in ready_offsets(state) {
        if !view.contains_key(&offset)
            && state.ready_duplicates.remove(&offset).is_none()
            && state
                .messages
                .get(&offset)
                .is_some_and(|message| message.state == MessageState::Ready)
        {
            state.messages.remove(&offset);
        }
    }
    for (offset, message) in view {
        let body_bytes = message.body().len() as u64;
        let consume_start_timestamp_millis = consume_start_timestamp(message);
        match state.messages.get_mut(offset) {
            Some(existing) if existing.state == MessageState::Ready => {
                existing.message = message.clone();
                existing.body_bytes = body_bytes;
                existing.consume_start_timestamp_millis = consume_start_timestamp_millis;
            }
            Some(_) => {
                state.ready_duplicates.insert(
                    *offset,
                    QueueMessage {
                        message: message.clone(),
                        body_bytes,
                        state: MessageState::Ready,
                        consume_start_timestamp_millis,
                    },
                );
            }
            None => match state.ready_duplicates.get_mut(offset) {
                Some(existing) => {
                    existing.message = message.clone();
                    existing.body_bytes = body_bytes;
                    existing.consume_start_timestamp_millis = consume_start_timestamp_millis;
                }
                None => {
                    state.messages.insert(
                        *offset,
                        QueueMessage {
                            message: message.clone(),
                            body_bytes,
                            state: MessageState::Ready,
                            consume_start_timestamp_millis,
                        },
                    );
                }
            },
        }
    }
    let in_flight_offsets = state
        .messages
        .iter()
        .filter(|(_, entry)| matches!(entry.state, MessageState::InFlight(_)))
        .map(|(offset, _)| *offset)
        .collect::<BTreeSet<_>>();
    state.in_flight_batches.retain(|_, offsets| {
        offsets.retain(|offset| in_flight_offsets.contains(offset));
        !offsets.is_empty()
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageState {
    Ready,
    InFlight(u64),
    Held,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderlyCompletion {
    HoldCurrent,
    CommitAll,
    RollbackAll,
    RequeueCurrent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockAttemptResult {
    Applied,
    Superseded,
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProcessQueueError {
    #[error("work belongs to an older queue assignment generation")]
    StaleGeneration,
    #[error("java.lang.NullPointerException")]
    NullMessageList,
    #[error("java.lang.NullPointerException")]
    NullMessageElement,
    #[error("process queue assignment has been dropped")]
    Dropped,
    #[error("consume batch does not belong to this process queue")]
    UnknownBatch,
    #[error("java.lang.NoSuchElementException: no orderly messages are available to commit")]
    EmptyOrderedConsumeSet,
    #[error("java.lang.IllegalArgumentException: Illegal Capacity: {batch_size}")]
    InvalidBatchSize { batch_size: i32 },
    #[error("consume batch no longer owns all of its messages")]
    BatchOwnershipLost,
    #[error("java.lang.NumberFormatException parsing MAX_OFFSET property: {value:?}")]
    InvalidMaxOffset { value: String },
    #[error("invalid consume-start timestamp for message at offset {offset}: {value:?}")]
    InvalidConsumeStartTimestamp { offset: i64, value: String },
}

/// Failure raised when an expired-message timestamp cannot be parsed like Java's `Long.parseLong`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExpiredMessageCleanupError {
    #[error("java.lang.NullPointerException")]
    NullPushConsumer,
    #[error("invalid consume-start timestamp for message at offset {offset}: {value:?}")]
    InvalidConsumeStartTimestamp { offset: i64, value: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InsertOutcome {
    pub(crate) inserted_count: usize,
    pub(crate) dispatch_to_consume: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumeBatch {
    id: u64,
    generation: u64,
    queue: MessageQueue,
    offsets: Vec<i64>,
    messages: Vec<Arc<MessageExt>>,
}

impl ConsumeBatch {
    pub(crate) const fn id(&self) -> u64 {
        self.id
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn queue(&self) -> &MessageQueue {
        &self.queue
    }

    pub(crate) fn offsets(&self) -> &[i64] {
        &self.offsets
    }

    pub(crate) fn messages(&self) -> &[Arc<MessageExt>] {
        &self.messages
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletionOutcome {
    pub(crate) completed_offsets: Vec<i64>,
    pub(crate) requeued_offsets: Vec<i64>,
    pub(crate) next_commit_offset: Option<i64>,
    pub(crate) has_ready_messages: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessQueueSnapshot {
    pub(crate) queue: MessageQueue,
    pub(crate) generation: u64,
    pub(crate) cached_count: i64,
    pub(crate) cached_bytes: i64,
    pub(crate) cached_map_count: usize,
    pub(crate) minimum_offset: Option<i64>,
    pub(crate) maximum_offset: Option<i64>,
    pub(crate) transaction_minimum_offset: Option<i64>,
    pub(crate) transaction_maximum_offset: Option<i64>,
    pub(crate) transaction_count: usize,
    pub(crate) max_span: i64,
    pub(crate) next_commit_offset: Option<i64>,
    pub(crate) in_flight_count: usize,
    pub(crate) dropped: bool,
    pub(crate) consuming: bool,
    pub(crate) last_pull_timestamp_millis: i64,
    pub(crate) last_consume_timestamp_millis: i64,
    pub(crate) locked: bool,
    pub(crate) last_lock_timestamp_millis: i64,
    pub(crate) try_unlock_times: i64,
}

impl ProcessQueue {
    /// Returns Java's `REBALANCE_LOCK_MAX_LIVE_TIME` field value.
    ///
    /// The paired Java `ProcessQueue` lock properties are captured on first
    /// access, matching its class-initialized static fields. Invalid property
    /// values are returned as errors instead of Java class-initialization
    /// exceptions.
    pub fn rebalance_lock_max_live_time_millis() -> Result<i64, String> {
        java_rebalance_lock_settings().map(|settings| settings.max_live_time_millis)
    }

    /// Returns Java's `REBALANCE_LOCK_INTERVAL` field value.
    ///
    /// The paired Java `ProcessQueue` lock properties are captured on first
    /// access, matching its class-initialized static fields. Invalid property
    /// values are returned as errors instead of Java class-initialization
    /// exceptions.
    pub fn rebalance_lock_interval_millis() -> Result<i64, String> {
        java_rebalance_lock_settings().map(|settings| settings.interval_millis)
    }

    /// Creates a public process queue, matching Java's no-argument constructor.
    pub fn new() -> Self {
        Self::new_for_assignment(MessageQueue::default(), 0)
    }

    pub(crate) fn new_for_assignment(queue: MessageQueue, generation: u64) -> Self {
        java_rebalance_lock_settings().unwrap_or_else(|error| panic!("{error}"));
        process_queue_pull_max_idle_time().unwrap_or_else(|error| panic!("{error}"));
        let now = epoch_millis();
        Self {
            queue,
            generation,
            state: Mutex::new(ProcessQueueState {
                messages: BTreeMap::new(),
                ready_duplicates: BTreeMap::new(),
                in_flight_batches: HashMap::new(),
                cached_count: 0,
                cached_bytes: 0,
                highest_seen_offset: None,
                queue_offset_max: 0,
                msg_acc_cnt: 0,
                dropped: false,
                consuming: false,
                next_batch_id: 1,
                last_pull_timestamp_millis: now,
                last_consume_timestamp_millis: now,
                locked: false,
                last_lock_timestamp_millis: now,
                last_lock_instant: Some(Instant::now()),
                lock_attempt_id: 0,
                try_unlock_times: 0,
            }),
            orderly_consume_gate: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    /// Returns whether this queue has been dropped from its assignment.
    pub fn is_dropped(&self) -> bool {
        self.lock_state().dropped
    }

    pub fn set_dropped(&self, dropped: bool) {
        self.lock_state().dropped = dropped;
    }

    pub fn is_locked(&self) -> bool {
        self.lock_state().locked
    }

    pub fn set_locked(&self, locked: bool) {
        self.lock_state().locked = locked;
    }

    pub fn is_lock_expired(&self) -> bool {
        let state = self.lock_state();
        epoch_millis().wrapping_sub(state.last_lock_timestamp_millis)
            > Self::rebalance_lock_max_live_time_millis().unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn is_pull_expired(&self) -> bool {
        self.is_pull_expired_with_timeout(
            process_queue_pull_max_idle_time().unwrap_or_else(|error| panic!("{error}")),
        )
    }

    pub fn get_last_lock_timestamp(&self) -> i64 {
        self.lock_state().last_lock_timestamp_millis
    }

    pub fn set_last_lock_timestamp(&self, timestamp_millis: i64) {
        let mut state = self.lock_state();
        state.last_lock_timestamp_millis = timestamp_millis;
        state.last_lock_instant = None;
    }

    pub fn get_last_pull_timestamp(&self) -> i64 {
        self.lock_state().last_pull_timestamp_millis
    }

    pub fn set_last_pull_timestamp(&self, timestamp_millis: i64) {
        self.lock_state().last_pull_timestamp_millis = timestamp_millis;
    }

    pub fn get_last_consume_timestamp(&self) -> i64 {
        self.lock_state().last_consume_timestamp_millis
    }

    pub fn set_last_consume_timestamp(&self, timestamp_millis: i64) {
        self.lock_state().last_consume_timestamp_millis = timestamp_millis;
    }

    pub fn get_msg_acc_cnt(&self) -> i64 {
        self.lock_state().msg_acc_cnt
    }

    pub fn set_msg_acc_cnt(&self, count: i64) {
        self.lock_state().msg_acc_cnt = count;
    }

    pub fn get_try_unlock_times(&self) -> i64 {
        self.lock_state().try_unlock_times
    }

    pub fn inc_try_unlock_times(&self) {
        let mut state = self.lock_state();
        state.try_unlock_times = state.try_unlock_times.wrapping_add(1);
    }

    /// Returns the current ready messages in queue-offset order.
    pub fn get_msg_tree_map(&self) -> ProcessQueueMessageTreeMap<'_> {
        ProcessQueueMessageTreeMap { state: &self.state }
    }

    pub fn get_msg_count(&self) -> ProcessQueueAtomicLong<'_> {
        ProcessQueueAtomicLong {
            state: &self.state,
            kind: ProcessQueueCounterKind::Count,
        }
    }

    pub fn get_msg_size(&self) -> ProcessQueueAtomicLong<'_> {
        ProcessQueueAtomicLong {
            state: &self.state,
            kind: ProcessQueueCounterKind::Size,
        }
    }

    pub fn get_consume_lock(&self) -> Arc<tokio::sync::RwLock<()>> {
        self.orderly_consume_gate()
    }

    pub fn get_max_span(&self) -> i64 {
        let state = self.lock_state();
        let offsets = ready_offsets(&state);
        let first = offsets.first().copied();
        let last = offsets.last().copied();
        first
            .zip(last)
            .map_or(0, |(min, max)| max.wrapping_sub(min))
    }

    pub fn contains_message(&self, message: &MessageExt) -> bool {
        ready_message_by_offset(&self.lock_state(), message.queue_offset()).is_some()
    }

    /// Matches Java's `containsMessage`, which returns `false` for a null message.
    pub fn contains_message_nullable(&self, message: Option<&MessageExt>) -> bool {
        message.is_some_and(|message| self.contains_message(message))
    }

    pub fn has_temp_message(&self) -> bool {
        has_ready_messages(&self.lock_state())
    }

    pub fn put_message(&self, messages: &[MessageExt]) -> Result<bool, ProcessQueueError> {
        self.insert_messages_inner(self.generation, messages.to_vec(), false)
            .map(|outcome| outcome.dispatch_to_consume)
    }

    /// Maps Java's `putMessage(null)` failure while preserving the typed slice API.
    pub fn put_message_nullable(
        &self,
        messages: Option<&[MessageExt]>,
    ) -> Result<bool, ProcessQueueError> {
        let Some(messages) = messages else {
            return Err(ProcessQueueError::NullMessageList);
        };
        self.put_message(messages)
    }

    /// Maps Java `putMessage` lists containing null elements, including prefix mutations.
    pub fn put_message_items_nullable(
        &self,
        messages: Option<&[Option<MessageExt>]>,
    ) -> Result<bool, ProcessQueueError> {
        let Some(messages) = messages else {
            return Err(ProcessQueueError::NullMessageList);
        };
        let Some(null_index) = messages.iter().position(Option::is_none) else {
            let messages = messages
                .iter()
                .map(|message| message.as_ref().unwrap().clone())
                .collect::<Vec<_>>();
            return self.put_message(&messages);
        };

        let mut state = self.lock_state();
        self.ensure_generation(self.generation)?;
        for message in messages[..null_index].iter().flatten() {
            let offset = message.queue_offset();
            let body_bytes = message.body().len() as u64;
            state.highest_seen_offset = Some(
                state
                    .highest_seen_offset
                    .map_or(offset, |highest| highest.max(offset)),
            );
            match state.messages.get(&offset).map(|entry| entry.state) {
                Some(MessageState::Ready) => {
                    if let Some(existing) = state.messages.get_mut(&offset) {
                        existing.message = Arc::new(message.clone());
                        existing.body_bytes = body_bytes;
                        existing.consume_start_timestamp_millis = consume_start_timestamp(message);
                    }
                }
                Some(MessageState::InFlight(_) | MessageState::Held) => {
                    if let Some(existing) = state.ready_duplicates.get_mut(&offset) {
                        existing.message = Arc::new(message.clone());
                        existing.body_bytes = body_bytes;
                        existing.consume_start_timestamp_millis = consume_start_timestamp(message);
                    } else {
                        state.ready_duplicates.insert(
                            offset,
                            QueueMessage {
                                message: Arc::new(message.clone()),
                                body_bytes,
                                state: MessageState::Ready,
                                consume_start_timestamp_millis: consume_start_timestamp(message),
                            },
                        );
                        state.queue_offset_max = offset;
                        state.cached_bytes = state.cached_bytes.wrapping_add(body_bytes as i64);
                    }
                }
                None => {
                    if let Some(existing) = state.ready_duplicates.get_mut(&offset) {
                        existing.message = Arc::new(message.clone());
                        existing.body_bytes = body_bytes;
                        existing.consume_start_timestamp_millis = consume_start_timestamp(message);
                    } else {
                        state.messages.insert(
                            offset,
                            QueueMessage {
                                message: Arc::new(message.clone()),
                                body_bytes,
                                state: MessageState::Ready,
                                consume_start_timestamp_millis: consume_start_timestamp(message),
                            },
                        );
                        state.queue_offset_max = offset;
                        state.cached_bytes = state.cached_bytes.wrapping_add(body_bytes as i64);
                    }
                }
            }
        }
        Err(ProcessQueueError::NullMessageElement)
    }

    pub fn take_messages(
        &self,
        batch_size: i32,
    ) -> Result<Vec<Arc<MessageExt>>, ProcessQueueError> {
        if batch_size < 0 {
            return Err(ProcessQueueError::InvalidBatchSize { batch_size });
        }
        let mut state = self.lock_state();
        state.last_consume_timestamp_millis = epoch_millis();
        let offsets = ready_offsets(&state)
            .into_iter()
            .take(batch_size as usize)
            .collect::<Vec<_>>();
        if offsets.is_empty() {
            state.consuming = false;
            return Ok(Vec::new());
        }
        let batch_id = Self::allocate_batch_id(&mut state);
        let mut messages = Vec::with_capacity(offsets.len());
        for offset in &offsets {
            if let Some(mut duplicate) = state.ready_duplicates.remove(offset) {
                duplicate.state = MessageState::InFlight(batch_id);
                messages.push(duplicate.message.clone());
                state.messages.insert(*offset, duplicate);
            } else if let Some(message) = state.messages.get_mut(offset) {
                message.state = MessageState::InFlight(batch_id);
                messages.push(message.message.clone());
            }
        }
        state.in_flight_batches.insert(batch_id, offsets);
        Ok(messages)
    }

    pub fn remove_message(&self, messages: &[Arc<MessageExt>]) -> i64 {
        let now = epoch_millis();
        let mut state = self.lock_state();
        state.last_consume_timestamp_millis = now;
        if !has_ready_messages(&state) {
            return -1;
        }
        let mut result = state.queue_offset_max.wrapping_add(1);
        for message in messages {
            let offset = message.queue_offset();
            let removed_ready_duplicate = state.ready_duplicates.remove(&offset).is_some();
            let removed_ready_message = if removed_ready_duplicate {
                true
            } else if state
                .messages
                .get(&offset)
                .is_some_and(|entry| entry.state == MessageState::Ready)
            {
                state.messages.remove(&offset);
                true
            } else {
                false
            };
            if removed_ready_message {
                state.cached_count = state.cached_count.wrapping_sub(1);
                let body_bytes = message.body().len();
                state.cached_bytes = state.cached_bytes.wrapping_sub(body_bytes as i64);
            }
        }
        if let Some(offset) = first_ready_offset(&state) {
            result = offset;
        }
        if state.cached_count == 0 {
            state.cached_bytes = 0;
        }
        result
    }

    /// Matches Java's caught null-list failure after it updates the consume timestamp.
    pub fn remove_message_nullable(&self, messages: Option<&[Arc<MessageExt>]>) -> i64 {
        let Some(messages) = messages else {
            let now = epoch_millis();
            let mut state = self.lock_state();
            state.last_consume_timestamp_millis = now;
            return if has_ready_messages(&state) {
                state.queue_offset_max.wrapping_add(1)
            } else {
                -1
            };
        };
        self.remove_message(messages)
    }

    /// Preserves Java's caught removal failure and partial effects for null list elements.
    pub fn remove_message_items_nullable(
        &self,
        messages: Option<&[Option<Arc<MessageExt>>]>,
    ) -> i64 {
        let Some(messages) = messages else {
            return self.remove_message_nullable(None);
        };
        let Some(null_index) = messages.iter().position(Option::is_none) else {
            let messages = messages
                .iter()
                .map(|message| message.as_ref().unwrap().clone())
                .collect::<Vec<_>>();
            return self.remove_message(&messages);
        };

        let now = epoch_millis();
        let mut state = self.lock_state();
        state.last_consume_timestamp_millis = now;
        if !has_ready_messages(&state) {
            return -1;
        }
        let result = state.queue_offset_max.wrapping_add(1);
        for message in messages[..null_index].iter().flatten() {
            let offset = message.queue_offset();
            let removed_ready_duplicate = state.ready_duplicates.remove(&offset).is_some();
            let removed_ready_message = if removed_ready_duplicate {
                true
            } else if state
                .messages
                .get(&offset)
                .is_some_and(|entry| entry.state == MessageState::Ready)
            {
                state.messages.remove(&offset);
                true
            } else {
                false
            };
            if removed_ready_message {
                state.cached_bytes = state.cached_bytes.wrapping_sub(message.body().len() as i64);
            }
        }
        result
    }

    pub fn rollback(&self) {
        let mut state = self.lock_state();
        let in_flight_offsets = state
            .messages
            .iter()
            .filter(|(_, message)| {
                matches!(
                    message.state,
                    MessageState::InFlight(_) | MessageState::Held
                )
            })
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>();
        for offset in in_flight_offsets {
            state.ready_duplicates.remove(&offset);
            if let Some(message) = state.messages.get_mut(&offset) {
                message.state = MessageState::Ready;
            }
        }
        state.in_flight_batches.clear();
    }

    pub fn commit(&self) -> Result<i64, ProcessQueueError> {
        let mut state = self.lock_state();
        let consuming_offsets = state
            .messages
            .iter()
            .filter(|(_, message)| {
                matches!(
                    message.state,
                    MessageState::InFlight(_) | MessageState::Held
                )
            })
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>();
        let Some(last_offset) = consuming_offsets.last().copied() else {
            return Err(ProcessQueueError::EmptyOrderedConsumeSet);
        };
        let mut consumed_messages = Vec::with_capacity(consuming_offsets.len());
        for offset in &consuming_offsets {
            if let Some(message) = state.messages.remove(offset) {
                consumed_messages.push((*offset, message));
            }
        }
        for _ in &consumed_messages {
            state.cached_count = state.cached_count.wrapping_sub(1);
        }
        if state.cached_count == 0 {
            state.cached_bytes = 0;
        } else {
            for (_, message) in &consumed_messages {
                state.cached_bytes = state.cached_bytes.wrapping_sub(message.body_bytes as i64);
            }
        }
        for (offset, _) in consumed_messages {
            if let Some(ready_message) = state.ready_duplicates.remove(&offset) {
                state.messages.insert(offset, ready_message);
            }
        }
        state.in_flight_batches.clear();
        Ok(last_offset.wrapping_add(1))
    }

    pub fn make_message_to_consume_again(&self, messages: &[MessageExt]) {
        let mut state = self.lock_state();
        for message in messages {
            let offset = message.queue_offset();
            let replacement = QueueMessage {
                message: Arc::new(message.clone()),
                body_bytes: message.body().len() as u64,
                state: MessageState::Ready,
                consume_start_timestamp_millis: consume_start_timestamp(message),
            };
            state.ready_duplicates.remove(&offset);
            if let Some(entry) = state.messages.get_mut(&offset) {
                entry.message = Arc::new(message.clone());
                entry.body_bytes = message.body().len() as u64;
                entry.state = MessageState::Ready;
                entry.consume_start_timestamp_millis = consume_start_timestamp(message);
            } else if let Some(entry) = state.ready_duplicates.get_mut(&offset) {
                *entry = replacement;
            } else {
                state.messages.insert(offset, replacement);
            }
        }
        let in_flight_offsets = state
            .messages
            .iter()
            .filter(|(_, entry)| matches!(entry.state, MessageState::InFlight(_)))
            .map(|(offset, _)| *offset)
            .collect::<BTreeSet<_>>();
        state.in_flight_batches.retain(|_, offsets| {
            offsets.retain(|offset| in_flight_offsets.contains(offset));
            !offsets.is_empty()
        });
    }

    /// Maps Java's uncaught null-list failure for `makeMessageToConsumeAgain`.
    pub fn make_message_to_consume_again_nullable(
        &self,
        messages: Option<&[MessageExt]>,
    ) -> Result<(), ProcessQueueError> {
        let Some(messages) = messages else {
            return Err(ProcessQueueError::NullMessageList);
        };
        self.make_message_to_consume_again(messages);
        Ok(())
    }

    /// Preserves Java's prefix requeue when a message list contains a null element.
    pub fn make_message_to_consume_again_items_nullable(
        &self,
        messages: Option<&[Option<MessageExt>]>,
    ) -> Result<(), ProcessQueueError> {
        let Some(messages) = messages else {
            return Err(ProcessQueueError::NullMessageList);
        };
        let Some(null_index) = messages.iter().position(Option::is_none) else {
            let messages = messages
                .iter()
                .map(|message| message.as_ref().unwrap().clone())
                .collect::<Vec<_>>();
            self.make_message_to_consume_again(&messages);
            return Ok(());
        };
        let prefix = messages[..null_index]
            .iter()
            .filter_map(Option::as_ref)
            .cloned()
            .collect::<Vec<_>>();
        self.make_message_to_consume_again(&prefix);
        Err(ProcessQueueError::NullMessageElement)
    }

    pub fn fill_process_queue_info(
        &self,
        info: &mut rocketmq_classic_protocol::body::ConsumerProcessQueueInfo,
    ) {
        let state = self.lock_state();
        let ready = ready_offsets(&state);
        if let (Some(minimum), Some(maximum)) = (ready.first(), ready.last()) {
            info.set_cached_msg_min_offset(*minimum);
            info.set_cached_msg_max_offset(*maximum);
            info.set_cached_msg_count(i32::try_from(ready.len()).unwrap_or(i32::MAX));
        }
        info.set_cached_msg_size_in_mib((state.cached_bytes / (1024 * 1024)) as i32);
        let consuming = state
            .messages
            .iter()
            .filter(|(_, message)| {
                matches!(
                    message.state,
                    MessageState::InFlight(_) | MessageState::Held
                )
            })
            .collect::<Vec<_>>();
        if let (Some((minimum, _)), Some((maximum, _))) = (consuming.first(), consuming.last()) {
            info.set_transaction_msg_min_offset(**minimum);
            info.set_transaction_msg_max_offset(**maximum);
            info.set_transaction_msg_count(i32::try_from(consuming.len()).unwrap_or(i32::MAX));
        }
        info.set_locked(state.locked);
        info.set_try_unlock_times(state.try_unlock_times);
        info.set_last_lock_timestamp(state.last_lock_timestamp_millis);
        info.set_droped(state.dropped);
        info.set_last_pull_timestamp(state.last_pull_timestamp_millis);
        info.set_last_consume_timestamp(state.last_consume_timestamp_millis);
    }

    /// Matches Java's `fillProcessQueueInfo`, which swallows exceptions for a null info value.
    pub fn fill_process_queue_info_nullable(
        &self,
        info: Option<&mut rocketmq_classic_protocol::body::ConsumerProcessQueueInfo>,
    ) {
        if let Some(info) = info {
            self.fill_process_queue_info(info);
        }
    }

    pub async fn clean_expired_msg(
        &self,
        push_consumer: &crate::push::PushConsumer,
    ) -> Result<(), ExpiredMessageCleanupError> {
        if push_consumer.is_consume_orderly() {
            return Ok(());
        }
        let timeout_millis = push_consumer
            .config()
            .consume_timeout_minutes()
            .wrapping_mul(60_000);
        let tree_map = self.get_msg_tree_map();
        let iterations = tree_map.read().len().min(16);
        for _ in 0..iterations {
            let (offset, message) =
                match self.expired_head_message(self.generation, epoch_millis(), timeout_millis) {
                    Ok(Some(expired)) => expired,
                    Ok(None) | Err(ProcessQueueError::StaleGeneration) => break,
                    Err(ProcessQueueError::InvalidConsumeStartTimestamp { offset, value }) => {
                        return Err(ExpiredMessageCleanupError::InvalidConsumeStartTimestamp {
                            offset,
                            value,
                        });
                    }
                    Err(_) => break,
                };
            if push_consumer.send_message_back(&message, 3).await.is_err() {
                continue;
            }
            let _ = self.remove_head_message(self.generation, offset, epoch_millis());
        }
        Ok(())
    }

    /// Preserves Java's null-consumer failure before the expiry scan starts.
    pub async fn clean_expired_msg_nullable(
        &self,
        push_consumer: Option<&crate::push::PushConsumer>,
    ) -> Result<(), ExpiredMessageCleanupError> {
        let Some(push_consumer) = push_consumer else {
            return Err(ExpiredMessageCleanupError::NullPushConsumer);
        };
        self.clean_expired_msg(push_consumer).await
    }

    pub(crate) fn queue(&self) -> &MessageQueue {
        &self.queue
    }

    pub(crate) fn is_pull_expired_with_timeout(&self, max_idle_time_millis: i64) -> bool {
        let last_pull_timestamp_millis = self.lock_state().last_pull_timestamp_millis;
        pull_idle_time_expired(
            epoch_millis(),
            last_pull_timestamp_millis,
            max_idle_time_millis,
        )
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn orderly_consume_gate(&self) -> Arc<tokio::sync::RwLock<()>> {
        self.orderly_consume_gate.clone()
    }

    /// Retain pulled messages by queue offset. Repeated offsets update a ready
    /// cached message but never double-count it; an in-flight message remains
    /// owned by the listener batch that already received it.
    pub(crate) fn insert_messages(
        &self,
        generation: u64,
        messages: Vec<MessageExt>,
    ) -> Result<InsertOutcome, ProcessQueueError> {
        self.insert_messages_inner(generation, messages, true)
    }

    fn insert_messages_inner(
        &self,
        generation: u64,
        messages: Vec<MessageExt>,
        require_active: bool,
    ) -> Result<InsertOutcome, ProcessQueueError> {
        let mut state = self.lock_state();
        if require_active {
            self.ensure_active(generation, &state)?;
        } else {
            self.ensure_generation(generation)?;
        }

        let mut inserted_count = 0usize;
        let max_offset = messages.last().and_then(|message| {
            Some((
                message.queue_offset(),
                message.get_property(PROPERTY_MAX_OFFSET)?,
            ))
        });
        for message in messages {
            let offset = message.queue_offset();
            let body_bytes = message.body().len() as u64;
            state.highest_seen_offset = Some(
                state
                    .highest_seen_offset
                    .map_or(offset, |highest| highest.max(offset)),
            );
            match state
                .messages
                .get(&offset)
                .map(|entry| (entry.state, entry.body_bytes))
            {
                Some((MessageState::Ready, _previous_body_bytes)) => {
                    let consume_start_timestamp_millis = consume_start_timestamp(&message);
                    if let Some(existing) = state.messages.get_mut(&offset) {
                        existing.message = Arc::new(message);
                        existing.body_bytes = body_bytes;
                        existing.consume_start_timestamp_millis = consume_start_timestamp_millis;
                    }
                }
                Some((MessageState::InFlight(_) | MessageState::Held, _)) => {
                    let consume_start_timestamp_millis = consume_start_timestamp(&message);
                    if let Some(existing) = state.ready_duplicates.get_mut(&offset) {
                        existing.message = Arc::new(message);
                        existing.body_bytes = body_bytes;
                        existing.consume_start_timestamp_millis = consume_start_timestamp_millis;
                    } else {
                        state.ready_duplicates.insert(
                            offset,
                            QueueMessage {
                                message: Arc::new(message),
                                body_bytes,
                                state: MessageState::Ready,
                                consume_start_timestamp_millis,
                            },
                        );
                        state.queue_offset_max = offset;
                        state.cached_count = state.cached_count.wrapping_add(1);
                        state.cached_bytes = state.cached_bytes.wrapping_add(body_bytes as i64);
                        inserted_count = inserted_count.saturating_add(1);
                    }
                }
                None => {
                    state.queue_offset_max = offset;
                    state.messages.insert(
                        offset,
                        QueueMessage {
                            consume_start_timestamp_millis: consume_start_timestamp(&message),
                            message: Arc::new(message),
                            body_bytes,
                            state: MessageState::Ready,
                        },
                    );
                    state.cached_count = state.cached_count.wrapping_add(1);
                    state.cached_bytes = state.cached_bytes.wrapping_add(body_bytes as i64);
                    inserted_count = inserted_count.saturating_add(1);
                }
            }
        }

        let mut invalid_max_offset = None;
        if let Some((message_offset, max_offset)) = max_offset {
            if let Some(max_offset) = parse_java_long(&max_offset) {
                let accumulated = max_offset.wrapping_sub(message_offset);
                if accumulated > 0 {
                    state.msg_acc_cnt = accumulated;
                }
            } else {
                invalid_max_offset = Some(max_offset);
            }
        }

        let has_ready_messages = has_ready_messages(&state);
        let dispatch_to_consume = has_ready_messages && !state.consuming;
        if dispatch_to_consume {
            state.consuming = true;
        }
        if let Some(value) = invalid_max_offset {
            return Err(ProcessQueueError::InvalidMaxOffset { value });
        }
        Ok(InsertOutcome {
            inserted_count,
            dispatch_to_consume,
        })
    }

    pub(crate) fn take_batch(
        &self,
        generation: u64,
        batch_size: usize,
    ) -> Result<Option<ConsumeBatch>, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        if batch_size == 0 {
            return Ok(None);
        }

        let offsets = ready_offsets(&state)
            .into_iter()
            .take(batch_size)
            .collect::<Vec<_>>();
        if offsets.is_empty() {
            if state.in_flight_batches.is_empty() {
                state.consuming = false;
            }
            return Ok(None);
        }

        let batch_id = Self::allocate_batch_id(&mut state);
        let mut messages = Vec::with_capacity(offsets.len());
        for offset in &offsets {
            if let Some(mut duplicate) = state.ready_duplicates.remove(offset) {
                duplicate.state = MessageState::InFlight(batch_id);
                messages.push(duplicate.message.clone());
                state.messages.insert(*offset, duplicate);
            } else if let Some(message) = state.messages.get_mut(offset) {
                message.state = MessageState::InFlight(batch_id);
                messages.push(message.message.clone());
            }
        }
        state.in_flight_batches.insert(batch_id, offsets.clone());
        state.consuming = true;
        state.last_consume_timestamp_millis = epoch_millis();
        Ok(Some(ConsumeBatch {
            id: batch_id,
            generation,
            queue: self.queue.clone(),
            offsets,
            messages,
        }))
    }

    pub(crate) fn mark_batch_started(
        &self,
        generation: u64,
        batch_id: u64,
        timestamp_millis: i64,
    ) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        let Some(offsets) = state.in_flight_batches.get(&batch_id).cloned() else {
            return Err(ProcessQueueError::UnknownBatch);
        };
        for offset in offsets {
            let Some(message) = state.messages.get_mut(&offset) else {
                return Err(ProcessQueueError::BatchOwnershipLost);
            };
            if message.state != MessageState::InFlight(batch_id) {
                return Err(ProcessQueueError::BatchOwnershipLost);
            }
            message.consume_start_timestamp_millis =
                ConsumeStartTimestamp::Parsed(timestamp_millis);
        }
        Ok(())
    }

    pub(crate) fn expired_head_message(
        &self,
        generation: u64,
        now_millis: i64,
        timeout_millis: i64,
    ) -> Result<Option<(i64, Arc<MessageExt>)>, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let state = self.lock_state();
        let Some(offset) = first_ready_offset(&state)
            .or_else(|| state.messages.first_key_value().map(|(offset, _)| *offset))
        else {
            return Ok(None);
        };
        let Some(message) =
            ready_message_by_offset(&state, offset).or_else(|| state.messages.get(&offset))
        else {
            return Ok(None);
        };
        let started_at = match &message.consume_start_timestamp_millis {
            ConsumeStartTimestamp::Missing => return Ok(None),
            ConsumeStartTimestamp::Parsed(timestamp) => *timestamp,
            ConsumeStartTimestamp::Invalid(value) => {
                return Err(ProcessQueueError::InvalidConsumeStartTimestamp {
                    offset,
                    value: value.clone(),
                });
            }
        };
        if now_millis.wrapping_sub(started_at) > timeout_millis {
            Ok(Some((offset, message.message.clone())))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn remove_head_message(
        &self,
        generation: u64,
        offset: i64,
        now_millis: i64,
    ) -> Result<bool, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let mut state = self.lock_state();
        let oldest_offset = first_ready_offset(&state)
            .or_else(|| state.messages.first_key_value().map(|(offset, _)| *offset));
        if oldest_offset != Some(offset) {
            return Ok(false);
        }
        let removed = if let Some(duplicate) = state.ready_duplicates.remove(&offset) {
            duplicate
        } else if state
            .messages
            .get(&offset)
            .is_some_and(|message| message.state == MessageState::Ready)
        {
            let Some(message) = state.messages.remove(&offset) else {
                return Ok(false);
            };
            message
        } else if let Some(message) = state.messages.remove(&offset) {
            message
        } else {
            return Ok(false);
        };
        state.cached_count = state.cached_count.wrapping_sub(1);
        state.cached_bytes = state.cached_bytes.wrapping_sub(removed.body_bytes as i64);
        if state.cached_count == 0 {
            state.cached_bytes = 0;
        }
        if !state
            .messages
            .get(&offset)
            .is_some_and(|message| matches!(message.state, MessageState::InFlight(_)))
        {
            for offsets in state.in_flight_batches.values_mut() {
                offsets.retain(|in_flight_offset| *in_flight_offset != offset);
            }
        }
        state.last_consume_timestamp_millis = now_millis;
        Ok(true)
    }

    /// Remove only terminally handled offsets. Every other message in the
    /// batch returns to the ready map, so retries stay accounted until success.
    pub(crate) fn complete_batch(
        &self,
        generation: u64,
        batch_id: u64,
        terminal_offsets: &[i64],
    ) -> Result<CompletionOutcome, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        let Some(offsets) = state.in_flight_batches.get(&batch_id).cloned() else {
            return Err(ProcessQueueError::UnknownBatch);
        };
        if offsets.iter().any(|offset| {
            state
                .messages
                .get(offset)
                .is_none_or(|message| message.state != MessageState::InFlight(batch_id))
        }) {
            return Err(ProcessQueueError::BatchOwnershipLost);
        }

        let had_owned_messages = !offsets.is_empty();
        let terminal_offsets = terminal_offsets.iter().copied().collect::<BTreeSet<_>>();
        state.in_flight_batches.remove(&batch_id);
        let mut completed_offsets = Vec::new();
        let mut requeued_offsets = Vec::new();
        for offset in offsets {
            if terminal_offsets.contains(&offset) {
                if let Some(message) = state.messages.remove(&offset) {
                    state.cached_count = state.cached_count.wrapping_sub(1);
                    state.cached_bytes = state.cached_bytes.wrapping_sub(message.body_bytes as i64);
                    if let Some(ready_message) = state.ready_duplicates.remove(&offset) {
                        state.messages.insert(offset, ready_message);
                    }
                    completed_offsets.push(offset);
                }
            } else {
                state.ready_duplicates.remove(&offset);
                if let Some(message) = state.messages.get_mut(&offset) {
                    message.state = MessageState::Ready;
                    requeued_offsets.push(offset);
                }
            }
        }
        if state.cached_count == 0 {
            state.cached_bytes = 0;
        }
        let has_ready_messages = has_ready_messages(&state);
        state.consuming = !state.messages.is_empty() || !state.ready_duplicates.is_empty();
        state.last_consume_timestamp_millis = epoch_millis();
        let next_commit_offset = if had_owned_messages {
            next_commit_offset(&state)
        } else {
            first_ready_offset(&state).or_else(|| state.messages.first_key_value().map(|(o, _)| *o))
        };
        Ok(CompletionOutcome {
            completed_offsets,
            requeued_offsets,
            next_commit_offset,
            has_ready_messages,
        })
    }

    /// Applies Java orderly semantics to the current batch and any prior
    /// autoCommit=false batches retained in `Held` state.
    pub(crate) fn complete_orderly_batch(
        &self,
        generation: u64,
        batch_id: u64,
        completion: OrderlyCompletion,
    ) -> Result<CompletionOutcome, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        let Some(offsets) = state.in_flight_batches.get(&batch_id).cloned() else {
            return Err(ProcessQueueError::UnknownBatch);
        };
        if offsets.iter().any(|offset| {
            state
                .messages
                .get(offset)
                .is_none_or(|message| message.state != MessageState::InFlight(batch_id))
        }) {
            return Err(ProcessQueueError::BatchOwnershipLost);
        }

        state.in_flight_batches.remove(&batch_id);
        let mut completed_offsets = Vec::new();
        let mut requeued_offsets = Vec::new();
        let transition_offsets = match completion {
            OrderlyCompletion::CommitAll => state
                .messages
                .iter()
                .filter(|(_, message)| {
                    message.state == MessageState::Held
                        || message.state == MessageState::InFlight(batch_id)
                })
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            OrderlyCompletion::RollbackAll => state
                .messages
                .iter()
                .filter(|(_, message)| {
                    message.state == MessageState::Held
                        || message.state == MessageState::InFlight(batch_id)
                })
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            OrderlyCompletion::HoldCurrent | OrderlyCompletion::RequeueCurrent => offsets,
        };

        for offset in transition_offsets {
            let current_batch = state
                .messages
                .get(&offset)
                .is_some_and(|message| message.state == MessageState::InFlight(batch_id));
            let transition_held = completion == OrderlyCompletion::RollbackAll
                && state
                    .messages
                    .get(&offset)
                    .is_some_and(|message| message.state == MessageState::Held);
            if completion == OrderlyCompletion::CommitAll {
                if let Some(message) = state.messages.remove(&offset) {
                    state.cached_count = state.cached_count.wrapping_sub(1);
                    state.cached_bytes = state.cached_bytes.wrapping_sub(message.body_bytes as i64);
                    if let Some(ready_message) = state.ready_duplicates.remove(&offset) {
                        state.messages.insert(offset, ready_message);
                    }
                    completed_offsets.push(offset);
                }
            } else if current_batch || transition_held {
                state.ready_duplicates.remove(&offset);
                if let Some(message) = state.messages.get_mut(&offset) {
                    match completion {
                        OrderlyCompletion::HoldCurrent => message.state = MessageState::Held,
                        OrderlyCompletion::RollbackAll | OrderlyCompletion::RequeueCurrent => {
                            message.state = MessageState::Ready;
                            requeued_offsets.push(offset);
                        }
                        OrderlyCompletion::CommitAll => unreachable!(),
                    }
                }
            }
        }

        if state.cached_count == 0 {
            state.cached_bytes = 0;
        }

        let has_ready_messages = has_ready_messages(&state);
        // Held messages are committed or rolled back only by a later listener
        // result. They must not keep dispatch latched when no ready message is
        // available, otherwise a future pull would never schedule a callback.
        state.consuming = has_ready_messages;
        state.last_consume_timestamp_millis = epoch_millis();
        let next_commit_offset = next_commit_offset(&state);
        Ok(CompletionOutcome {
            completed_offsets,
            requeued_offsets,
            next_commit_offset,
            has_ready_messages,
        })
    }

    pub(crate) fn increment_reconsume_times(
        &self,
        generation: u64,
        offset: i64,
    ) -> Result<i32, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        let Some(entry) = state.messages.get_mut(&offset) else {
            return Err(ProcessQueueError::UnknownBatch);
        };
        let mut message = entry.message.as_ref().clone();
        let reconsume_times = message.reconsume_times().saturating_add(1);
        message.set_reconsume_times(reconsume_times);
        entry.message = Arc::new(message);
        Ok(reconsume_times)
    }

    pub(crate) fn drop_assignment(&self, generation: u64) -> Result<bool, ProcessQueueError> {
        if generation != self.generation {
            return Err(ProcessQueueError::StaleGeneration);
        }
        let mut state = self.lock_state();
        if state.dropped {
            return Ok(false);
        }
        state.dropped = true;
        state.consuming = false;
        state.lock_attempt_id = state.lock_attempt_id.wrapping_add(1);
        Ok(true)
    }

    pub fn clear(&self) {
        let mut state = self.lock_state();
        state.messages.clear();
        state.ready_duplicates.clear();
        state.in_flight_batches.clear();
        state.cached_count = 0;
        state.cached_bytes = 0;
        state.highest_seen_offset = None;
        state.queue_offset_max = 0;
    }

    pub(crate) fn release_consume_dispatch(
        &self,
        generation: u64,
    ) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        state.consuming = false;
        Ok(())
    }

    pub(crate) fn snapshot(
        &self,
        generation: u64,
    ) -> Result<ProcessQueueSnapshot, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let state = self.lock_state();
        let cached_offsets = ready_offsets(&state);
        let transaction_offsets = state
            .messages
            .iter()
            .filter(|(_, message)| {
                matches!(
                    message.state,
                    MessageState::InFlight(_) | MessageState::Held
                )
            })
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>();
        let minimum_offset = cached_offsets.first().copied();
        let maximum_offset = cached_offsets.last().copied();
        let max_span = minimum_offset
            .zip(maximum_offset)
            .map_or(0, |(min, max)| max.wrapping_sub(min));
        let in_flight_count = state
            .messages
            .values()
            .filter(|message| matches!(message.state, MessageState::InFlight(_)))
            .count();
        Ok(ProcessQueueSnapshot {
            queue: self.queue.clone(),
            generation: self.generation,
            cached_count: state.cached_count,
            cached_bytes: state.cached_bytes,
            cached_map_count: cached_offsets.len(),
            minimum_offset,
            maximum_offset,
            transaction_minimum_offset: transaction_offsets.first().copied(),
            transaction_maximum_offset: transaction_offsets.last().copied(),
            transaction_count: transaction_offsets.len(),
            max_span,
            next_commit_offset: next_commit_offset(&state),
            in_flight_count,
            dropped: state.dropped,
            consuming: state.consuming,
            last_pull_timestamp_millis: state.last_pull_timestamp_millis,
            last_consume_timestamp_millis: state.last_consume_timestamp_millis,
            locked: state.locked,
            last_lock_timestamp_millis: state.last_lock_timestamp_millis,
            try_unlock_times: state.try_unlock_times,
        })
    }

    pub(crate) fn set_last_pull_timestamp_for_assignment(
        &self,
        generation: u64,
        timestamp_millis: i64,
    ) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        state.last_pull_timestamp_millis = timestamp_millis;
        Ok(())
    }

    pub(crate) fn set_lock_state(
        &self,
        generation: u64,
        locked: bool,
        timestamp_millis: i64,
    ) -> Result<(), ProcessQueueError> {
        self.set_lock_state_at(
            generation,
            locked,
            timestamp_millis,
            locked.then(Instant::now),
        )
    }

    /// Sets local lock state using the instant the Broker response was
    /// received, which may precede initial-offset lookup during assignment.
    pub(crate) fn set_lock_state_at(
        &self,
        generation: u64,
        locked: bool,
        timestamp_millis: i64,
        lock_instant: Option<Instant>,
    ) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        state.locked = locked;
        if locked {
            state.last_lock_timestamp_millis = timestamp_millis;
            state.last_lock_instant = lock_instant;
        }
        Ok(())
    }

    /// Starts a lock request tied to this queue generation. The returned token
    /// prevents an older response from overwriting a newer lock decision.
    pub(crate) fn begin_lock_attempt(&self, generation: u64) -> Result<u64, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_active(generation, &state)?;
        state.lock_attempt_id = state.lock_attempt_id.wrapping_add(1).max(1);
        Ok(state.lock_attempt_id)
    }

    /// Applies a Broker lock result only while the request still owns the
    /// current, active assignment generation.
    pub(crate) fn complete_lock_attempt(
        &self,
        generation: u64,
        attempt_id: u64,
        locked: bool,
    ) -> Result<LockAttemptResult, ProcessQueueError> {
        self.complete_lock_attempt_at(
            generation,
            attempt_id,
            locked,
            epoch_millis(),
            Instant::now(),
        )
    }

    /// Applies a Broker lock result using the instant its response arrived.
    /// Batch requests may continue across other brokers before local state is
    /// reconciled, so recording the later batch-completion time would extend
    /// the earlier Broker lock's effective lifetime.
    pub(crate) fn complete_lock_attempt_at(
        &self,
        generation: u64,
        attempt_id: u64,
        locked: bool,
        timestamp_millis: i64,
        lock_instant: Instant,
    ) -> Result<LockAttemptResult, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        if state.dropped {
            return Ok(LockAttemptResult::Dropped);
        }
        if state.lock_attempt_id != attempt_id {
            return Ok(LockAttemptResult::Superseded);
        }
        state.locked = locked;
        if locked {
            state.last_lock_timestamp_millis = timestamp_millis;
            state.last_lock_instant = Some(lock_instant);
        }
        Ok(LockAttemptResult::Applied)
    }

    /// Records a confirmed unlock. This remains legal after drop so cleanup
    /// can finish without making the old assignment active again.
    pub(crate) fn record_unlocked(&self, generation: u64) -> Result<(), ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        state.locked = false;
        state.lock_attempt_id = state.lock_attempt_id.wrapping_add(1).max(1);
        Ok(())
    }

    pub(crate) fn is_lock_fresh(
        &self,
        generation: u64,
        max_live_time_millis: i64,
    ) -> Result<bool, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let state = self.lock_state();
        if state.dropped || !state.locked {
            return Ok(false);
        }
        let Some(last_lock_instant) = state.last_lock_instant else {
            return Ok(false);
        };
        let elapsed_millis = Instant::now()
            .saturating_duration_since(last_lock_instant)
            .as_millis();
        Ok(i128::try_from(elapsed_millis).unwrap_or(i128::MAX) <= i128::from(max_live_time_millis))
    }

    /// Reports whether the most recent confirmed lock is older than Java's
    /// dropped-queue unlock delay. The timestamp remains available after an
    /// unlock response, matching ProcessQueue's retained lastLockTimestamp.
    pub(crate) fn is_lock_older_than(
        &self,
        generation: u64,
        unlock_delay_millis: i64,
    ) -> Result<bool, ProcessQueueError> {
        self.ensure_generation(generation)?;
        let state = self.lock_state();
        let Some(last_lock_instant) = state.last_lock_instant else {
            return Ok(false);
        };
        let elapsed_millis = Instant::now()
            .saturating_duration_since(last_lock_instant)
            .as_millis();
        Ok(i128::try_from(elapsed_millis).unwrap_or(i128::MAX) > i128::from(unlock_delay_millis))
    }

    pub(crate) fn increment_try_unlock_times(
        &self,
        generation: u64,
    ) -> Result<i64, ProcessQueueError> {
        let mut state = self.lock_state();
        self.ensure_generation(generation)?;
        state.try_unlock_times = state.try_unlock_times.wrapping_add(1);
        Ok(state.try_unlock_times)
    }

    fn allocate_batch_id(state: &mut ProcessQueueState) -> u64 {
        loop {
            let batch_id = state.next_batch_id.max(1);
            state.next_batch_id = batch_id.wrapping_add(1).max(1);
            if !state.in_flight_batches.contains_key(&batch_id) {
                return batch_id;
            }
        }
    }

    fn ensure_active(
        &self,
        generation: u64,
        state: &ProcessQueueState,
    ) -> Result<(), ProcessQueueError> {
        self.ensure_generation(generation)?;
        if state.dropped {
            return Err(ProcessQueueError::Dropped);
        }
        Ok(())
    }

    fn ensure_generation(&self, generation: u64) -> Result<(), ProcessQueueError> {
        if generation == self.generation {
            Ok(())
        } else {
            Err(ProcessQueueError::StaleGeneration)
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ProcessQueueState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for ProcessQueue {
    fn default() -> Self {
        Self::new()
    }
}

fn next_commit_offset(state: &ProcessQueueState) -> Option<i64> {
    state
        .messages
        .first_key_value()
        .map(|(offset, _)| *offset)
        .or_else(|| {
            state
                .highest_seen_offset
                .map(|offset| offset.wrapping_add(1))
        })
}

fn epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::{Message, MessageExt, MessageQueue};

    // Oracle: ProcessQueueTest.java at the pinned Java 5.5.1 commit
    // efa1be59d6fc747ad220f036c86030f05f0419f9, SHA-256
    // 67ff9bd9913b76944c3e7a75d6f219525799b135049f7c86a1b6cace9111c762.
    // Java cases cover cached count/size and ordered take/commit; generation
    // rejection and gap-safe commit checks enforce the Phase 10 ownership contract.

    fn process_queue(generation: u64) -> ProcessQueue {
        ProcessQueue::new_for_assignment(MessageQueue::new("orders", "broker-a", 0), generation)
    }

    #[test]
    fn try_unlock_counter_matches_java_signed_atomic_long_wrap() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/consumer/facade-api.json"))
                .expect("valid pinned Java facade fixture");
        let java_wrapped_value =
            fixture["implementationBehaviors"]["processQueueTryUnlockOverflow"]
                .as_i64()
                .expect("Java fixture records the signed overflow result");
        let process_queue = process_queue(31);
        process_queue.lock_state().try_unlock_times = i64::MAX;

        process_queue.inc_try_unlock_times();
        assert_eq!(process_queue.get_try_unlock_times(), java_wrapped_value);
        let mut info = rocketmq_classic_protocol::body::ConsumerProcessQueueInfo::default();
        process_queue.fill_process_queue_info(&mut info);
        assert_eq!(info.get_try_unlock_times(), java_wrapped_value);
        process_queue.lock_state().try_unlock_times = i64::MAX;
        assert_eq!(
            process_queue.increment_try_unlock_times(31),
            Ok(java_wrapped_value)
        );
        assert_eq!(
            process_queue.snapshot(31).unwrap().try_unlock_times,
            java_wrapped_value
        );
    }

    fn message(offset: i64, body_size: usize) -> MessageExt {
        let mut message =
            crate::test_support::message_ext_from(Message::new("orders", vec![b'x'; body_size]));
        message.set_queue_offset(offset);
        message
    }

    #[test]
    fn remove_message_matches_java_for_in_flight_entries() {
        let process_queue = process_queue(16);
        let first = message(10, 2);
        process_queue
            .put_message(&[first.clone(), message(11, 3)])
            .unwrap();
        let batch = process_queue.take_messages(1).unwrap();
        assert_eq!(batch[0].queue_offset(), 10);
        assert_eq!(process_queue.snapshot(16).unwrap().in_flight_count, 1);

        let next_offset = process_queue.remove_message(&batch);

        assert_eq!(next_offset, 11);
        let snapshot = process_queue.snapshot(16).unwrap();
        assert_eq!(snapshot.cached_count, 2);
        assert_eq!(snapshot.cached_bytes, 5);
        assert_eq!(snapshot.in_flight_count, 1);
        assert_eq!(
            process_queue
                .get_msg_tree_map()
                .read()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [11]
        );
        assert_eq!(process_queue.commit(), Ok(11));
        assert_eq!(process_queue.get_msg_count().get(), 1);
        assert_eq!(process_queue.get_msg_size().get(), 3);
    }

    #[test]
    fn remove_message_preserves_dispatch_state_until_empty_take() {
        let process_queue = process_queue(17);
        let first = message(10, 2);
        assert!(process_queue.put_message(&[first.clone()]).unwrap());
        assert_eq!(process_queue.remove_message(&[Arc::new(first)]), 11);

        assert!(!process_queue.put_message(&[message(11, 3)]).unwrap());
        assert_eq!(process_queue.get_msg_count().get(), 1);
        assert_eq!(process_queue.get_msg_size().get(), 3);
        assert_eq!(
            process_queue
                .get_msg_tree_map()
                .read()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [11]
        );
    }

    #[test]
    fn pull_idle_timeout_property_keeps_java_long_defaults_and_values() {
        assert_eq!(parse_pull_max_idle_time(None).unwrap(), 120_000);
        assert_eq!(parse_pull_max_idle_time(Some("+2500")).unwrap(), 2_500);
        assert_eq!(parse_pull_max_idle_time(Some("-1")).unwrap(), -1);
        assert_eq!(parse_pull_max_idle_time(Some("٢٠٢٤")).unwrap(), 2_024);
        assert!(parse_pull_max_idle_time(Some(" 120000")).is_err());
        assert!(parse_pull_max_idle_time(Some("9223372036854775808")).is_err());
    }

    #[test]
    fn process_queue_pull_idle_property_keeps_its_first_java_class_value() {
        let property = OnceLock::new();
        assert_eq!(
            cached_java_property(&property, || Ok::<_, String>(120_000)),
            Ok(120_000)
        );
        assert_eq!(
            cached_java_property(&property, || Ok::<_, String>(5)),
            Ok(120_000)
        );
    }

    #[test]
    fn pull_idle_expiry_matches_java_long_subtraction_and_strict_boundary() {
        assert!(!pull_idle_time_expired(120_000, 0, 120_000));
        assert!(pull_idle_time_expired(120_001, 0, 120_000));
        assert!(pull_idle_time_expired(0, 1, -2));
        assert!(pull_idle_time_expired(i64::MIN, 1, 0));
        assert!(!pull_idle_time_expired(i64::MIN, 1, i64::MAX));
    }

    #[test]
    fn expired_head_uses_java_timestamp_boundary_and_fences_late_batch_completion() {
        let process_queue = process_queue(29);
        process_queue
            .insert_messages(29, vec![message(10, 2), message(11, 3)])
            .unwrap();
        let batch = process_queue.take_batch(29, 2).unwrap().unwrap();
        process_queue
            .mark_batch_started(29, batch.id(), 1_000)
            .unwrap();

        assert!(
            process_queue
                .expired_head_message(29, 61_000, 60_000)
                .unwrap()
                .is_none()
        );
        let expired = process_queue
            .expired_head_message(29, 61_001, 60_000)
            .unwrap()
            .unwrap();
        assert_eq!(expired.0, 10);
        assert!(process_queue.remove_head_message(29, 10, 61_001).unwrap());
        assert_eq!(process_queue.snapshot(29).unwrap().cached_count, 1);

        let completed = process_queue
            .complete_batch(29, batch.id(), &[10, 11])
            .unwrap();
        assert_eq!(completed.completed_offsets, [11]);
        assert_eq!(completed.next_commit_offset, Some(12));
        assert_eq!(process_queue.snapshot(29).unwrap().cached_count, 0);
    }

    #[test]
    fn expired_only_in_flight_batch_does_not_persist_a_second_offset() {
        let process_queue = process_queue(30);
        process_queue
            .insert_messages(30, vec![message(12, 2)])
            .unwrap();
        let batch = process_queue.take_batch(30, 1).unwrap().unwrap();
        process_queue
            .mark_batch_started(30, batch.id(), 2_000)
            .unwrap();
        assert!(process_queue.remove_head_message(30, 12, 62_001).unwrap());

        let completed = process_queue.complete_batch(30, batch.id(), &[12]).unwrap();
        assert!(completed.completed_offsets.is_empty());
        assert_eq!(completed.next_commit_offset, None);
    }

    #[test]
    fn deduplicates_offsets_and_keeps_cached_count_bytes_and_batch_order_exact() {
        let process_queue = process_queue(17);
        let inserted = process_queue
            .insert_messages(17, vec![message(12, 1), message(10, 3), message(11, 2)])
            .unwrap();
        assert_eq!(inserted.inserted_count, 3);
        assert!(inserted.dispatch_to_consume);

        let duplicate = process_queue
            .insert_messages(17, vec![message(10, 7)])
            .unwrap();
        assert_eq!(duplicate.inserted_count, 0);
        assert!(!duplicate.dispatch_to_consume);

        let snapshot = process_queue.snapshot(17).unwrap();
        assert_eq!(snapshot.cached_count, 3);
        assert_eq!(snapshot.cached_bytes, 6);
        assert_eq!(snapshot.max_span, 2);
        assert_eq!(snapshot.next_commit_offset, Some(10));

        let batch = process_queue.take_batch(17, 8).unwrap().unwrap();
        assert_eq!(batch.offsets(), &[10, 11, 12]);
        assert_eq!(batch.messages()[0].body().len(), 7);
        let in_flight = process_queue.snapshot(17).unwrap();
        assert_eq!(in_flight.cached_count, 3);
        assert_eq!(in_flight.cached_bytes, 6);
        assert_eq!(in_flight.in_flight_count, 3);

        let completed = process_queue
            .complete_batch(17, batch.id(), &[10, 11, 12])
            .unwrap();
        assert_eq!(completed.next_commit_offset, Some(13));
        assert_eq!(process_queue.snapshot(17).unwrap().cached_count, 0);
        assert_eq!(process_queue.snapshot(17).unwrap().cached_bytes, 0);
    }

    #[test]
    fn duplicate_repull_stays_ready_when_the_original_batch_completes() {
        let process_queue = process_queue(31);
        process_queue
            .insert_messages(31, vec![message(20, 2)])
            .unwrap();
        let original_batch = process_queue.take_batch(31, 1).unwrap().unwrap();

        let duplicate = message(20, 4);
        let inserted = process_queue.put_message(&[duplicate]).unwrap();
        assert!(!inserted);
        let during_flight = process_queue.snapshot(31).unwrap();
        assert_eq!(during_flight.cached_count, 2);
        assert_eq!(during_flight.cached_bytes, 6);
        assert_eq!(during_flight.cached_map_count, 1);
        assert_eq!(during_flight.in_flight_count, 1);
        assert_eq!(process_queue.get_msg_tree_map().read()[&20].body().len(), 4);

        let completed = process_queue
            .complete_batch(31, original_batch.id(), &[20])
            .unwrap();
        assert_eq!(completed.completed_offsets, [20]);
        let after_completion = process_queue.snapshot(31).unwrap();
        assert_eq!(after_completion.cached_count, 1);
        assert_eq!(after_completion.cached_bytes, 4);
        assert_eq!(after_completion.cached_map_count, 1);
        assert_eq!(after_completion.in_flight_count, 0);
        assert_eq!(process_queue.get_msg_tree_map().read()[&20].body().len(), 4);
    }

    #[test]
    fn gaps_and_out_of_order_batches_never_commit_past_unresolved_messages() {
        let process_queue = process_queue(18);
        process_queue
            .insert_messages(18, vec![message(5, 1), message(7, 1), message(8, 1)])
            .unwrap();

        let first = process_queue.take_batch(18, 1).unwrap().unwrap();
        let later = process_queue.take_batch(18, 2).unwrap().unwrap();
        assert_eq!(first.offsets(), &[5]);
        assert_eq!(later.offsets(), &[7, 8]);

        let later_completed = process_queue
            .complete_batch(18, later.id(), &[7, 8])
            .unwrap();
        assert_eq!(later_completed.next_commit_offset, Some(5));
        assert_eq!(process_queue.snapshot(18).unwrap().cached_count, 1);

        let first_completed = process_queue.complete_batch(18, first.id(), &[5]).unwrap();
        assert_eq!(first_completed.next_commit_offset, Some(9));
        assert_eq!(process_queue.snapshot(18).unwrap().cached_count, 0);
    }

    #[test]
    fn partial_completion_requeues_only_unresolved_messages() {
        let process_queue = process_queue(19);
        process_queue
            .insert_messages(19, vec![message(20, 2), message(21, 3), message(22, 4)])
            .unwrap();
        let batch = process_queue.take_batch(19, 3).unwrap().unwrap();

        let partially_completed = process_queue
            .complete_batch(19, batch.id(), &[20, 22])
            .unwrap();
        assert_eq!(partially_completed.next_commit_offset, Some(21));
        assert_eq!(partially_completed.requeued_offsets, vec![21]);
        let snapshot = process_queue.snapshot(19).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.cached_bytes, 3);

        let retry = process_queue.take_batch(19, 3).unwrap().unwrap();
        assert_eq!(retry.offsets(), &[21]);
        let completed = process_queue.complete_batch(19, retry.id(), &[21]).unwrap();
        assert_eq!(completed.next_commit_offset, Some(23));
    }

    #[test]
    fn orderly_success_holds_offsets_until_a_later_commit() {
        let process_queue = process_queue(23);
        process_queue
            .insert_messages(23, vec![message(10, 1), message(11, 1)])
            .unwrap();
        let first = process_queue.take_batch(23, 1).unwrap().unwrap();
        let held = process_queue
            .complete_orderly_batch(23, first.id(), OrderlyCompletion::HoldCurrent)
            .unwrap();
        assert_eq!(held.next_commit_offset, Some(10));
        assert_eq!(process_queue.snapshot(23).unwrap().cached_count, 2);

        let second = process_queue.take_batch(23, 1).unwrap().unwrap();
        assert_eq!(second.offsets(), &[11]);
        let committed = process_queue
            .complete_orderly_batch(23, second.id(), OrderlyCompletion::CommitAll)
            .unwrap();
        assert_eq!(committed.completed_offsets, vec![10, 11]);
        assert_eq!(committed.next_commit_offset, Some(12));
        assert_eq!(process_queue.snapshot(23).unwrap().cached_count, 0);
    }

    #[test]
    fn orderly_rollback_restores_held_and_current_messages_in_offset_order() {
        let process_queue = process_queue(24);
        process_queue
            .insert_messages(24, vec![message(10, 1), message(11, 1), message(12, 1)])
            .unwrap();
        let first = process_queue.take_batch(24, 1).unwrap().unwrap();
        process_queue
            .complete_orderly_batch(24, first.id(), OrderlyCompletion::HoldCurrent)
            .unwrap();
        let second = process_queue.take_batch(24, 1).unwrap().unwrap();
        let rolled_back = process_queue
            .complete_orderly_batch(24, second.id(), OrderlyCompletion::RollbackAll)
            .unwrap();
        assert_eq!(rolled_back.requeued_offsets, vec![10, 11]);

        let replay = process_queue.take_batch(24, 3).unwrap().unwrap();
        assert_eq!(replay.offsets(), &[10, 11, 12]);
    }

    #[test]
    fn stale_and_dropped_lock_responses_cannot_restore_queue_ownership() {
        let process_queue = process_queue(25);
        let first = process_queue.begin_lock_attempt(25).unwrap();
        let current = process_queue.begin_lock_attempt(25).unwrap();
        assert_eq!(
            process_queue.complete_lock_attempt(25, first, true),
            Ok(LockAttemptResult::Superseded)
        );
        assert_eq!(
            process_queue.complete_lock_attempt(25, current, true),
            Ok(LockAttemptResult::Applied)
        );
        assert!(process_queue.snapshot(25).unwrap().locked);

        let late = process_queue.begin_lock_attempt(25).unwrap();
        process_queue.drop_assignment(25).unwrap();
        assert_eq!(
            process_queue.complete_lock_attempt(25, late, true),
            Ok(LockAttemptResult::Dropped)
        );
        assert!(process_queue.snapshot(25).unwrap().dropped);
    }

    #[test]
    fn reset_clear_discards_cached_and_in_flight_queue_messages() {
        let process_queue = process_queue(28);
        process_queue
            .insert_messages(28, vec![message(30, 1), message(31, 2)])
            .unwrap();
        let batch = process_queue.take_batch(28, 2).unwrap().unwrap();
        assert_eq!(process_queue.snapshot(28).unwrap().in_flight_count, 2);

        process_queue.drop_assignment(28).unwrap();
        process_queue.clear();

        let snapshot = process_queue.snapshot(28).unwrap();
        assert_eq!(snapshot.cached_count, 0);
        assert_eq!(snapshot.cached_bytes, 0);
        assert_eq!(snapshot.in_flight_count, 0);
        assert_eq!(snapshot.next_commit_offset, None);
        assert!(matches!(
            process_queue.complete_batch(28, batch.id(), &[30, 31]),
            Err(ProcessQueueError::Dropped)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_lock_expires_only_after_the_java_max_live_boundary() {
        let process_queue = process_queue(26);
        process_queue.set_lock_state(26, true, 1_000).unwrap();
        assert!(process_queue.is_lock_fresh(26, 30_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(30_000)).await;
        assert!(process_queue.is_lock_fresh(26, 30_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert!(!process_queue.is_lock_fresh(26, 30_000).unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_queue_unlock_delay_uses_the_java_strict_boundary() {
        let process_queue = process_queue(27);
        process_queue.set_lock_state(27, true, 1_000).unwrap();
        process_queue.record_unlocked(27).unwrap();
        assert!(!process_queue.is_lock_older_than(27, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(20_000)).await;
        assert!(!process_queue.is_lock_older_than(27, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert!(process_queue.is_lock_older_than(27, 20_000).unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn never_locked_queue_unlock_delay_starts_at_process_queue_creation() {
        let process_queue = process_queue(28);
        assert!(!process_queue.is_lock_older_than(28, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(20_000)).await;
        assert!(!process_queue.is_lock_older_than(28, 20_000).unwrap());
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert!(process_queue.is_lock_older_than(28, 20_000).unwrap());
    }

    #[test]
    fn dropped_generation_rejects_late_work_without_affecting_replacement_queue() {
        let old_queue = process_queue(20);
        old_queue.insert_messages(20, vec![message(4, 4)]).unwrap();
        let batch = old_queue.take_batch(20, 1).unwrap().unwrap();
        assert!(old_queue.drop_assignment(20).unwrap());

        assert_eq!(
            old_queue.complete_batch(20, batch.id(), &[4]),
            Err(ProcessQueueError::Dropped)
        );
        assert_eq!(
            old_queue.insert_messages(21, vec![message(100, 1)]),
            Err(ProcessQueueError::StaleGeneration)
        );
        let dropped = old_queue.snapshot(20).unwrap();
        assert!(dropped.dropped);
        assert_eq!(dropped.cached_count, 1);
        assert_eq!(dropped.next_commit_offset, Some(4));

        let replacement = process_queue(21);
        replacement
            .insert_messages(21, vec![message(100, 1)])
            .unwrap();
        assert_eq!(
            replacement.snapshot(21).unwrap().next_commit_offset,
            Some(100)
        );
        assert_eq!(old_queue.snapshot(20).unwrap().cached_count, 1);
    }
}
