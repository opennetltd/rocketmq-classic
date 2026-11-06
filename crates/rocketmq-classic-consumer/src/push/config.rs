use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_classic_core::config::ClientConfig;
use rocketmq_classic_protocol::heartbeat::{ConsumeFromWhere, MessageModel};
use rocketmq_classic_remoting::client::TlsConfig;

use crate::{
    config::validate_group,
    error::PushConsumerError,
    push::listener::MessageQueueListener,
    rebalance::{AllocateMessageQueueAveragely, AllocateMessageQueueStrategy},
};

pub(crate) const DEFAULT_PUSH_CONSUMER_GROUP: &str = "DEFAULT_CONSUMER";
pub(crate) const DEFAULT_CONSUME_THREAD_MIN: i32 = 20;
pub(crate) const DEFAULT_CONSUME_THREAD_MAX: i32 = 20;
pub(crate) const DEFAULT_ADJUST_THREAD_POOL_NUMS_THRESHOLD: i64 = 100_000;
pub(crate) const DEFAULT_CONSUME_CONCURRENTLY_MAX_SPAN: i32 = 2_000;
pub(crate) const DEFAULT_PULL_THRESHOLD_FOR_QUEUE: i32 = 1_000;
pub(crate) const DEFAULT_PULL_THRESHOLD_SIZE_FOR_QUEUE_MIB: i32 = 100;
pub(crate) const DEFAULT_PULL_THRESHOLD_FOR_TOPIC: i32 = -1;
pub(crate) const DEFAULT_PULL_THRESHOLD_SIZE_FOR_TOPIC_MIB: i32 = -1;
pub(crate) const DEFAULT_PULL_INTERVAL_MILLIS: i64 = 0;
pub(crate) const DEFAULT_CONSUME_MESSAGE_BATCH_MAX_SIZE: i32 = 1;
pub(crate) const DEFAULT_PULL_BATCH_SIZE: i32 = 32;
pub(crate) const DEFAULT_PULL_BATCH_SIZE_IN_BYTES: i32 = 256 * 1024;
pub(crate) const DEFAULT_MAX_RECONSUME_TIMES: i32 = -1;
pub(crate) const DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MILLIS: i64 = 1_000;
pub(crate) const DEFAULT_CONSUME_TIMEOUT_MINUTES: i64 = 15;
pub(crate) const DEFAULT_AWAIT_TERMINATION_MILLIS_WHEN_SHUTDOWN: i64 = 0;
pub(crate) const DEFAULT_POP_THRESHOLD_FOR_QUEUE: i32 = 96;
pub(crate) const DEFAULT_POP_INVISIBLE_TIME_MILLIS: i64 = 60_000;
pub(crate) const DEFAULT_POP_BATCH_NUMS: i32 = 32;

/// Java 5.5.1 `DefaultMQPushConsumer` settings used by the pull and POP paths.
///
/// The consumer group accessor and setter preserve Java's nullable `String`
/// through `Option<String>`.
///
/// ```compile_fail
/// use rocketmq_classic_consumer::PushConsumerConfig;
///
/// fn removed_nullable_alias(config: &PushConsumerConfig) {
///     config.consumer_group_nullable();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_consumer::PushConsumerConfig;
///
/// fn removed_nullable_setter(config: &PushConsumerConfig) {
///     config.set_consumer_group_nullable(None);
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_consumer::push::config::{
///     DEFAULT_ADJUST_THREAD_POOL_NUMS_THRESHOLD,
///     DEFAULT_AWAIT_TERMINATION_MILLIS_WHEN_SHUTDOWN,
///     DEFAULT_CONSUME_CONCURRENTLY_MAX_SPAN,
///     DEFAULT_CONSUME_MESSAGE_BATCH_MAX_SIZE,
///     DEFAULT_CONSUME_THREAD_MAX,
///     DEFAULT_CONSUME_THREAD_MIN,
///     DEFAULT_CONSUME_TIMEOUT_MINUTES,
///     DEFAULT_MAX_RECONSUME_TIMES,
///     DEFAULT_POP_BATCH_NUMS,
///     DEFAULT_POP_INVISIBLE_TIME_MILLIS,
///     DEFAULT_POP_THRESHOLD_FOR_QUEUE,
///     DEFAULT_PULL_BATCH_SIZE,
///     DEFAULT_PULL_BATCH_SIZE_IN_BYTES,
///     DEFAULT_PULL_INTERVAL_MILLIS,
///     DEFAULT_PULL_THRESHOLD_FOR_QUEUE,
///     DEFAULT_PULL_THRESHOLD_FOR_TOPIC,
///     DEFAULT_PULL_THRESHOLD_SIZE_FOR_QUEUE_MIB,
///     DEFAULT_PULL_THRESHOLD_SIZE_FOR_TOPIC_MIB,
///     DEFAULT_PUSH_CONSUMER_GROUP,
///     DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MILLIS,
/// };
/// ```
pub struct PushConsumerConfig {
    client: ClientConfig,
    tls_config: Option<TlsConfig>,
    consumer_group: RwLock<Option<String>>,
    message_model: AtomicI32,
    consume_from_where: AtomicI32,
    consume_timestamp: RwLock<String>,
    consume_thread_min: i32,
    consume_thread_max: AtomicI32,
    adjust_thread_pool_nums_threshold: AtomicI64,
    consume_concurrently_max_span: AtomicI32,
    pull_threshold_for_queue: AtomicI32,
    pull_threshold_size_for_queue_mib: AtomicI32,
    pull_threshold_for_topic: AtomicI32,
    pull_threshold_size_for_topic_mib: AtomicI32,
    pull_interval_millis: AtomicI64,
    consume_message_batch_max_size: AtomicI32,
    pull_batch_size: AtomicI32,
    pull_batch_size_in_bytes: AtomicI32,
    post_subscription_when_pull: AtomicBool,
    unit_mode: AtomicBool,
    max_reconsume_times: AtomicI32,
    suspend_current_queue_time_millis: AtomicI64,
    consume_timeout_minutes: AtomicI64,
    await_termination_millis_when_shutdown: AtomicI64,
    pop_threshold_for_queue: AtomicI32,
    pop_invisible_time_millis: AtomicI64,
    pop_batch_nums: AtomicI32,
    allocate_message_queue_strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    client_rebalance: AtomicBool,
    message_queue_listener: RwLock<Option<MessageQueueListener>>,
}

impl PushConsumerConfig {
    pub fn new(consumer_group: impl Into<String>) -> Result<Self, PushConsumerError> {
        Ok(Self {
            client: ClientConfig::new_auto()?,
            tls_config: None,
            consumer_group: RwLock::new(Some(consumer_group.into())),
            message_model: AtomicI32::new(message_model_to_ordinal(MessageModel::Clustering)),
            consume_from_where: AtomicI32::new(consume_from_where_to_ordinal(
                ConsumeFromWhere::ConsumeFromLastOffset,
            )),
            consume_timestamp: RwLock::new(default_consume_timestamp()),
            consume_thread_min: DEFAULT_CONSUME_THREAD_MIN,
            consume_thread_max: AtomicI32::new(DEFAULT_CONSUME_THREAD_MAX),
            adjust_thread_pool_nums_threshold: AtomicI64::new(
                DEFAULT_ADJUST_THREAD_POOL_NUMS_THRESHOLD,
            ),
            consume_concurrently_max_span: AtomicI32::new(DEFAULT_CONSUME_CONCURRENTLY_MAX_SPAN),
            pull_threshold_for_queue: AtomicI32::new(DEFAULT_PULL_THRESHOLD_FOR_QUEUE),
            pull_threshold_size_for_queue_mib: AtomicI32::new(
                DEFAULT_PULL_THRESHOLD_SIZE_FOR_QUEUE_MIB,
            ),
            pull_threshold_for_topic: AtomicI32::new(DEFAULT_PULL_THRESHOLD_FOR_TOPIC),
            pull_threshold_size_for_topic_mib: AtomicI32::new(
                DEFAULT_PULL_THRESHOLD_SIZE_FOR_TOPIC_MIB,
            ),
            pull_interval_millis: AtomicI64::new(DEFAULT_PULL_INTERVAL_MILLIS),
            consume_message_batch_max_size: AtomicI32::new(DEFAULT_CONSUME_MESSAGE_BATCH_MAX_SIZE),
            pull_batch_size: AtomicI32::new(DEFAULT_PULL_BATCH_SIZE),
            pull_batch_size_in_bytes: AtomicI32::new(DEFAULT_PULL_BATCH_SIZE_IN_BYTES),
            post_subscription_when_pull: AtomicBool::new(false),
            unit_mode: AtomicBool::new(false),
            max_reconsume_times: AtomicI32::new(DEFAULT_MAX_RECONSUME_TIMES),
            suspend_current_queue_time_millis: AtomicI64::new(
                DEFAULT_SUSPEND_CURRENT_QUEUE_TIME_MILLIS,
            ),
            consume_timeout_minutes: AtomicI64::new(DEFAULT_CONSUME_TIMEOUT_MINUTES),
            await_termination_millis_when_shutdown: AtomicI64::new(
                DEFAULT_AWAIT_TERMINATION_MILLIS_WHEN_SHUTDOWN,
            ),
            pop_threshold_for_queue: AtomicI32::new(DEFAULT_POP_THRESHOLD_FOR_QUEUE),
            pop_invisible_time_millis: AtomicI64::new(DEFAULT_POP_INVISIBLE_TIME_MILLIS),
            pop_batch_nums: AtomicI32::new(DEFAULT_POP_BATCH_NUMS),
            allocate_message_queue_strategy: RwLock::new(Arc::new(AllocateMessageQueueAveragely)),
            client_rebalance: AtomicBool::new(true),
            message_queue_listener: RwLock::new(None),
        })
    }

    /// Creates the Java-default Push configuration. Java's default group is
    /// `DEFAULT_CONSUMER`; startup rejects it until the caller changes it.
    pub fn new_default() -> Result<Self, PushConsumerError> {
        Self::new(DEFAULT_PUSH_CONSUMER_GROUP)
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        &mut self.client
    }

    pub fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }

    pub fn set_tls_config(&mut self, value: Option<TlsConfig>) {
        if value.is_some() {
            self.client.set_use_tls(true);
        }
        self.tls_config = value;
    }

    /// Returns the current Java-compatible consumer group, including null.
    pub fn consumer_group(&self) -> Option<String> {
        read_lock(&self.consumer_group).clone()
    }

    /// Updates the consumer group. The retained Push consumer configuration
    /// remains live after construction. `None` represents Java's null value.
    pub fn set_consumer_group(&self, value: Option<String>) {
        *write_lock(&self.consumer_group) = value;
    }

    pub fn message_model(&self) -> MessageModel {
        message_model_from_ordinal(self.message_model.load(Ordering::Acquire))
    }

    pub fn set_message_model(&self, value: MessageModel) {
        self.message_model
            .store(message_model_to_ordinal(value), Ordering::Release);
    }

    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        read_lock(&self.allocate_message_queue_strategy).clone()
    }

    pub fn set_allocate_message_queue_strategy(
        &self,
        value: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        *write_lock(&self.allocate_message_queue_strategy) = value;
    }

    pub fn client_rebalance(&self) -> bool {
        self.client_rebalance.load(Ordering::Acquire)
    }

    pub fn set_client_rebalance(&self, value: bool) {
        self.client_rebalance.store(value, Ordering::Release);
    }

    pub fn message_queue_listener(&self) -> Option<MessageQueueListener> {
        read_lock(&self.message_queue_listener).clone()
    }

    pub fn set_message_queue_listener(&self, value: Option<MessageQueueListener>) {
        *write_lock(&self.message_queue_listener) = value;
    }

    pub fn consume_from_where(&self) -> ConsumeFromWhere {
        consume_from_where_from_ordinal(self.consume_from_where.load(Ordering::Acquire))
    }

    pub fn set_consume_from_where(&self, value: ConsumeFromWhere) {
        self.consume_from_where
            .store(consume_from_where_to_ordinal(value), Ordering::Release);
    }

    pub fn consume_timestamp(&self) -> String {
        read_lock(&self.consume_timestamp).clone()
    }

    pub fn set_consume_timestamp(&self, value: impl Into<String>) {
        *write_lock(&self.consume_timestamp) = value.into();
    }

    pub const fn consume_thread_min(&self) -> i32 {
        self.consume_thread_min
    }

    pub fn set_consume_thread_min(&mut self, value: i32) {
        self.consume_thread_min = value;
    }

    pub fn consume_thread_max(&self) -> i32 {
        self.consume_thread_max.load(Ordering::Acquire)
    }

    pub fn set_consume_thread_max(&self, value: i32) {
        self.consume_thread_max.store(value, Ordering::Release);
    }

    pub fn adjust_thread_pool_nums_threshold(&self) -> i64 {
        self.adjust_thread_pool_nums_threshold
            .load(Ordering::Acquire)
    }

    pub fn set_adjust_thread_pool_nums_threshold(&self, value: i64) {
        self.adjust_thread_pool_nums_threshold
            .store(value, Ordering::Release);
    }

    pub fn consume_concurrently_max_span(&self) -> i32 {
        self.consume_concurrently_max_span.load(Ordering::Acquire)
    }

    pub fn set_consume_concurrently_max_span(&self, value: i32) {
        self.consume_concurrently_max_span
            .store(value, Ordering::Release);
    }

    pub fn pull_threshold_for_queue(&self) -> i32 {
        self.pull_threshold_for_queue.load(Ordering::Acquire)
    }

    pub fn set_pull_threshold_for_queue(&self, value: i32) {
        self.pull_threshold_for_queue
            .store(value, Ordering::Release);
    }

    pub fn pull_threshold_size_for_queue_mib(&self) -> i32 {
        self.pull_threshold_size_for_queue_mib
            .load(Ordering::Acquire)
    }

    pub fn set_pull_threshold_size_for_queue_mib(&self, value: i32) {
        self.pull_threshold_size_for_queue_mib
            .store(value, Ordering::Release);
    }

    pub fn pull_threshold_for_topic(&self) -> i32 {
        self.pull_threshold_for_topic.load(Ordering::Acquire)
    }

    pub fn set_pull_threshold_for_topic(&self, value: i32) {
        self.pull_threshold_for_topic
            .store(value, Ordering::Release);
    }

    pub fn pull_threshold_size_for_topic_mib(&self) -> i32 {
        self.pull_threshold_size_for_topic_mib
            .load(Ordering::Acquire)
    }

    pub fn set_pull_threshold_size_for_topic_mib(&self, value: i32) {
        self.pull_threshold_size_for_topic_mib
            .store(value, Ordering::Release);
    }

    pub fn pull_interval_millis(&self) -> i64 {
        self.pull_interval_millis.load(Ordering::Acquire)
    }

    pub fn set_pull_interval_millis(&self, value: i64) {
        self.pull_interval_millis.store(value, Ordering::Release);
    }

    pub fn consume_message_batch_max_size(&self) -> i32 {
        self.consume_message_batch_max_size.load(Ordering::Acquire)
    }

    pub fn set_consume_message_batch_max_size(&self, value: i32) {
        self.consume_message_batch_max_size
            .store(value, Ordering::Release);
    }

    pub fn pull_batch_size(&self) -> i32 {
        self.pull_batch_size.load(Ordering::Acquire)
    }

    pub fn set_pull_batch_size(&self, value: i32) {
        self.pull_batch_size.store(value, Ordering::Release);
    }

    pub fn pull_batch_size_in_bytes(&self) -> i32 {
        self.pull_batch_size_in_bytes.load(Ordering::Acquire)
    }

    pub fn set_pull_batch_size_in_bytes(&self, value: i32) {
        self.pull_batch_size_in_bytes
            .store(value, Ordering::Release);
    }

    pub fn post_subscription_when_pull(&self) -> bool {
        self.post_subscription_when_pull.load(Ordering::Acquire)
    }

    pub fn set_post_subscription_when_pull(&self, value: bool) {
        self.post_subscription_when_pull
            .store(value, Ordering::Release);
    }

    pub fn unit_mode(&self) -> bool {
        self.unit_mode.load(Ordering::Acquire)
    }

    pub fn set_unit_mode(&self, value: bool) {
        self.unit_mode.store(value, Ordering::Release);
    }

    pub fn max_reconsume_times(&self) -> i32 {
        self.max_reconsume_times.load(Ordering::Acquire)
    }

    /// Java `DefaultMQPushConsumerImpl.getMaxReconsumeTimes()` resolves only
    /// the `-1` default to 16 in concurrent mode.
    pub fn effective_max_reconsume_times(&self) -> i32 {
        if self.max_reconsume_times() == -1 {
            16
        } else {
            self.max_reconsume_times()
        }
    }

    /// Java orderly consumers interpret the `-1` default as unbounded retries.
    pub(crate) fn effective_orderly_max_reconsume_times(&self) -> i32 {
        if self.max_reconsume_times() == -1 {
            i32::MAX
        } else {
            self.max_reconsume_times()
        }
    }

    pub fn set_max_reconsume_times(&self, value: i32) {
        self.max_reconsume_times.store(value, Ordering::Release);
    }

    pub fn suspend_current_queue_time_millis(&self) -> i64 {
        self.suspend_current_queue_time_millis
            .load(Ordering::Acquire)
    }

    pub fn set_suspend_current_queue_time_millis(&self, value: i64) {
        self.suspend_current_queue_time_millis
            .store(value, Ordering::Release);
    }

    pub fn consume_timeout_minutes(&self) -> i64 {
        self.consume_timeout_minutes.load(Ordering::Acquire)
    }

    pub fn set_consume_timeout_minutes(&self, value: i64) {
        self.consume_timeout_minutes.store(value, Ordering::Release);
    }

    pub fn await_termination_millis_when_shutdown(&self) -> i64 {
        self.await_termination_millis_when_shutdown
            .load(Ordering::Acquire)
    }

    pub fn set_await_termination_millis_when_shutdown(&self, value: i64) {
        self.await_termination_millis_when_shutdown
            .store(value, Ordering::Release);
    }

    pub fn pop_threshold_for_queue(&self) -> i32 {
        self.pop_threshold_for_queue.load(Ordering::Acquire)
    }

    pub fn set_pop_threshold_for_queue(&self, value: i32) {
        self.pop_threshold_for_queue.store(value, Ordering::Release);
    }

    pub fn pop_invisible_time_millis(&self) -> i64 {
        self.pop_invisible_time_millis.load(Ordering::Acquire)
    }

    pub fn set_pop_invisible_time_millis(&self, value: i64) {
        self.pop_invisible_time_millis
            .store(value, Ordering::Release);
    }

    pub fn pop_batch_nums(&self) -> i32 {
        self.pop_batch_nums.load(Ordering::Acquire)
    }

    pub fn set_pop_batch_nums(&self, value: i32) {
        self.pop_batch_nums.store(value, Ordering::Release);
    }

    pub(crate) fn validate(&self, effective_consumer_group: &str) -> Result<(), PushConsumerError> {
        validate_group(effective_consumer_group)
            .map_err(|_| PushConsumerError::InvalidConsumerGroup)?;
        if effective_consumer_group == DEFAULT_PUSH_CONSUMER_GROUP {
            return Err(PushConsumerError::DefaultConsumerGroup);
        }
        if parse_java_timestamp_millis(&self.consume_timestamp()).is_none() {
            return Err(PushConsumerError::InvalidConsumeTimestamp);
        }
        for (name, value) in [
            ("consumeThreadMin", self.consume_thread_min),
            ("consumeThreadMax", self.consume_thread_max()),
        ] {
            validate_range(name, i64::from(value), 1, 1_000)?;
        }
        if self.consume_thread_min > self.consume_thread_max() {
            return Err(PushConsumerError::InvalidThreadBounds {
                minimum: self.consume_thread_min,
                maximum: self.consume_thread_max(),
            });
        }
        validate_range(
            "consumeConcurrentlyMaxSpan",
            i64::from(self.consume_concurrently_max_span()),
            1,
            65_535,
        )?;
        validate_range(
            "pullThresholdForQueue",
            i64::from(self.pull_threshold_for_queue()),
            1,
            65_535,
        )?;
        validate_optional_range(
            "pullThresholdForTopic",
            self.pull_threshold_for_topic(),
            6_553_500,
        )?;
        validate_range(
            "pullThresholdSizeForQueue",
            i64::from(self.pull_threshold_size_for_queue_mib()),
            1,
            1_024,
        )?;
        validate_optional_range(
            "pullThresholdSizeForTopic",
            self.pull_threshold_size_for_topic_mib(),
            102_400,
        )?;
        validate_range("pullInterval", self.pull_interval_millis(), 0, 65_535)?;
        validate_range(
            "consumeMessageBatchMaxSize",
            i64::from(self.consume_message_batch_max_size()),
            1,
            1_024,
        )?;
        validate_range("pullBatchSize", i64::from(self.pull_batch_size()), 1, 1_024)?;
        validate_range(
            "popInvisibleTime",
            self.pop_invisible_time_millis(),
            5_000,
            300_000,
        )?;
        validate_range("popBatchNums", i64::from(self.pop_batch_nums()), 1, 32)?;
        Ok(())
    }
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn message_model_to_ordinal(value: MessageModel) -> i32 {
    match value {
        MessageModel::Broadcasting => 0,
        MessageModel::Clustering => 1,
        MessageModel::LiteSelective => 2,
    }
}

fn message_model_from_ordinal(value: i32) -> MessageModel {
    match value {
        0 => MessageModel::Broadcasting,
        2 => MessageModel::LiteSelective,
        _ => MessageModel::Clustering,
    }
}

fn consume_from_where_to_ordinal(value: ConsumeFromWhere) -> i32 {
    match value {
        ConsumeFromWhere::ConsumeFromLastOffset => 0,
        ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => 1,
        ConsumeFromWhere::ConsumeFromMinOffset => 2,
        ConsumeFromWhere::ConsumeFromMaxOffset => 3,
        ConsumeFromWhere::ConsumeFromFirstOffset => 4,
        ConsumeFromWhere::ConsumeFromTimestamp => 5,
    }
}

fn consume_from_where_from_ordinal(value: i32) -> ConsumeFromWhere {
    match value {
        1 => ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
        2 => ConsumeFromWhere::ConsumeFromMinOffset,
        3 => ConsumeFromWhere::ConsumeFromMaxOffset,
        4 => ConsumeFromWhere::ConsumeFromFirstOffset,
        5 => ConsumeFromWhere::ConsumeFromTimestamp,
        _ => ConsumeFromWhere::ConsumeFromLastOffset,
    }
}

fn validate_range(
    name: &'static str,
    value: i64,
    minimum: i64,
    maximum: i64,
) -> Result<(), PushConsumerError> {
    if !(minimum..=maximum).contains(&value) {
        return Err(PushConsumerError::InvalidSetting {
            name,
            value,
            minimum,
            maximum,
        });
    }
    Ok(())
}

fn validate_optional_range(
    name: &'static str,
    value: i32,
    maximum: i64,
) -> Result<(), PushConsumerError> {
    if value == -1 {
        return Ok(());
    }
    validate_range(name, i64::from(value), 1, maximum)
}

pub(crate) fn parse_java_timestamp_millis(value: &str) -> Option<i64> {
    let units = value.encode_utf16().collect::<Vec<_>>();
    let mut cursor = 0usize;
    let year = parse_fixed_java_number(&units, &mut cursor, 4)?;
    let month = parse_fixed_java_number(&units, &mut cursor, 2)?;
    let day = parse_fixed_java_number(&units, &mut cursor, 2)?;
    let hour = parse_fixed_java_number(&units, &mut cursor, 2)?;
    let minute = parse_fixed_java_number(&units, &mut cursor, 2)?;
    let second = parse_java_number_prefix(units.get(cursor..)?)?;

    #[cfg(unix)]
    {
        let mut local = std::mem::MaybeUninit::<libc::tm>::zeroed();
        // SAFETY: all tm fields used below are assigned. `mktime` normalizes
        // overflowing calendar fields like Java SimpleDateFormat's lenient
        // Calendar and uses the process-local timezone like Java's default.
        let local = unsafe {
            let local = local.assume_init_mut();
            local.tm_year = year.checked_sub(1900)?;
            local.tm_mon = month.checked_sub(1)?;
            local.tm_mday = day;
            local.tm_hour = hour;
            local.tm_min = minute;
            local.tm_sec = second;
            local.tm_isdst = -1;
            libc::mktime(local as *mut libc::tm)
        };
        (local as i64).checked_mul(1_000)
    }

    #[cfg(not(unix))]
    {
        let month_zero = i64::from(month).checked_sub(1)?;
        let normalized_year = i64::from(year).checked_add(month_zero.div_euclid(12))?;
        let normalized_month = month_zero.rem_euclid(12) + 1;
        let adjusted_year = normalized_year - i64::from(normalized_month <= 2);
        let era = adjusted_year.div_euclid(400);
        let year_of_era = adjusted_year - era * 400;
        let day_of_year =
            (153 * (normalized_month + if normalized_month > 2 { -3 } else { 9 }) + 2) / 5
                + i64::from(day)
                - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        let days = era * 146_097 + day_of_era - 719_468;
        let seconds = days
            .checked_mul(86_400)?
            .checked_add(i64::from(hour) * 3_600)?
            .checked_add(i64::from(minute) * 60)?
            .checked_add(i64::from(second))?;
        seconds.checked_mul(1_000)
    }
}

fn parse_fixed_java_number(units: &[u16], cursor: &mut usize, width: usize) -> Option<i32> {
    let end = cursor.checked_add(width)?;
    let value = parse_java_number_prefix(units.get(*cursor..end)?)?;
    *cursor = end;
    Some(value)
}

fn parse_java_number_prefix(units: &[u16]) -> Option<i32> {
    let mut cursor = 0;
    while units
        .get(cursor)
        .is_some_and(|unit| char::from_u32(u32::from(*unit)).is_some_and(char::is_whitespace))
    {
        cursor += 1;
    }
    let negative = units.get(cursor) == Some(&u16::from(b'-'));
    if negative {
        cursor += 1;
    }
    let mut value = 0i64;
    let mut digits = 0;
    for unit in &units[cursor..] {
        let Some(digit) = java_decimal_digit(*unit) else {
            break;
        };
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
        digits += 1;
    }
    if digits == 0 {
        return None;
    }
    let value = if negative { -value } else { value };
    i32::try_from(value).ok()
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

pub(crate) fn default_consume_timestamp() -> String {
    let target = SystemTime::now()
        .checked_sub(Duration::from_secs(30 * 60))
        .unwrap_or(UNIX_EPOCH);
    let seconds = target
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());

    #[cfg(unix)]
    if let Ok(seconds) = libc::time_t::try_from(seconds) {
        let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
        // `localtime_r` writes the result into the supplied, uniquely owned buffer.
        let result = unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) };
        if !result.is_null() {
            // SAFETY: a non-null return means `localtime_r` initialized `local`.
            let local = unsafe { local.assume_init() };
            return format!(
                "{:04}{:02}{:02}{:02}{:02}{:02}",
                local.tm_year + 1900,
                local.tm_mon + 1,
                local.tm_mday,
                local.tm_hour,
                local.tm_min,
                local.tm_sec,
            );
        }
    }

    format_utc_timestamp(seconds)
}

#[cfg_attr(unix, allow(dead_code))]
fn format_utc_timestamp(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let seconds_in_day = seconds % 86_400;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    let hour = seconds_in_day / 3_600;
    let minute = seconds_in_day % 3_600 / 60;
    let second = seconds_in_day % 60;
    format!("{year:04}{month:02}{day:02}{hour:02}{minute:02}{second:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_tls_config_enables_client_transport() {
        let mut config = PushConsumerConfig::new("tls-consumer").unwrap();
        let mut tls = TlsConfig::default();
        tls.test_mode = false;
        config.set_tls_config(Some(tls));
        assert!(config.client_config().use_tls());
        assert!(!config.tls_config().unwrap().test_mode);
    }

    #[test]
    fn defaults_match_java_push_consumer() {
        let config = PushConsumerConfig::new("orders").unwrap();
        assert_eq!(config.message_model(), MessageModel::Clustering);
        assert_eq!(
            config.consume_from_where(),
            ConsumeFromWhere::ConsumeFromLastOffset
        );
        assert_eq!(config.consume_thread_min(), 20);
        assert_eq!(config.consume_thread_max(), 20);
        assert_eq!(config.adjust_thread_pool_nums_threshold(), 100_000);
        assert_eq!(config.consume_concurrently_max_span(), 2_000);
        assert!(!config.post_subscription_when_pull());
        assert!(!config.unit_mode());
        assert_eq!(config.pull_threshold_for_queue(), 1_000);
        assert_eq!(config.pull_threshold_size_for_queue_mib(), 100);
        assert_eq!(config.pull_threshold_for_topic(), -1);
        assert_eq!(config.pull_threshold_size_for_topic_mib(), -1);
        assert_eq!(config.pull_interval_millis(), 0);
        assert_eq!(config.consume_message_batch_max_size(), 1);
        assert_eq!(config.pull_batch_size(), 32);
        assert_eq!(config.pull_batch_size_in_bytes(), 256 * 1024);
        assert_eq!(config.max_reconsume_times(), -1);
        assert_eq!(config.effective_max_reconsume_times(), 16);
        assert_eq!(config.effective_orderly_max_reconsume_times(), i32::MAX);
        assert_eq!(config.suspend_current_queue_time_millis(), 1_000);
        let config = config;
        config.set_max_reconsume_times(-2);
        assert_eq!(config.effective_max_reconsume_times(), -2);
        assert_eq!(config.effective_orderly_max_reconsume_times(), -2);
        config.set_max_reconsume_times(5);
        assert_eq!(config.effective_orderly_max_reconsume_times(), 5);
        config.set_suspend_current_queue_time_millis(4_321);
        assert_eq!(config.suspend_current_queue_time_millis(), 4_321);
        assert_eq!(config.consume_timeout_minutes(), 15);
        assert_eq!(config.await_termination_millis_when_shutdown(), 0);
        assert_eq!(config.pop_threshold_for_queue(), 96);
        assert_eq!(config.pop_invisible_time_millis(), 60_000);
        assert_eq!(config.pop_batch_nums(), 32);
        assert!(parse_java_timestamp_millis(&config.consume_timestamp()).is_some());
    }

    #[test]
    fn live_runtime_settings_match_shared_java_enum_values() {
        let config = PushConsumerConfig::new("live-settings").unwrap();
        for model in [
            MessageModel::Broadcasting,
            MessageModel::Clustering,
            MessageModel::LiteSelective,
        ] {
            config.set_message_model(model);
            assert_eq!(config.message_model(), model);
        }
        for policy in [
            ConsumeFromWhere::ConsumeFromLastOffset,
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
            ConsumeFromWhere::ConsumeFromMinOffset,
            ConsumeFromWhere::ConsumeFromMaxOffset,
            ConsumeFromWhere::ConsumeFromFirstOffset,
            ConsumeFromWhere::ConsumeFromTimestamp,
        ] {
            config.set_consume_from_where(policy);
            assert_eq!(config.consume_from_where(), policy);
        }

        config.set_consume_thread_max(17);
        config.set_adjust_thread_pool_nums_threshold(16_000);
        config.set_consume_concurrently_max_span(18);
        config.set_pull_threshold_for_queue(19);
        config.set_pull_threshold_size_for_queue_mib(20);
        config.set_pull_threshold_for_topic(21);
        config.set_pull_threshold_size_for_topic_mib(22);
        config.set_pull_interval_millis(23);
        config.set_consume_message_batch_max_size(24);
        config.set_post_subscription_when_pull(true);
        config.set_unit_mode(true);
        config.set_max_reconsume_times(25);
        config.set_consume_timestamp("20240229010203");
        config.set_suspend_current_queue_time_millis(26);
        config.set_consume_timeout_minutes(27);
        config.set_await_termination_millis_when_shutdown(28);
        config.set_pop_threshold_for_queue(29);
        config.set_pop_invisible_time_millis(30_000);
        config.set_pop_batch_nums(31);

        assert_eq!(config.consume_thread_max(), 17);
        assert_eq!(config.adjust_thread_pool_nums_threshold(), 16_000);
        assert_eq!(config.consume_concurrently_max_span(), 18);
        assert_eq!(config.pull_threshold_for_queue(), 19);
        assert_eq!(config.pull_threshold_size_for_queue_mib(), 20);
        assert_eq!(config.pull_threshold_for_topic(), 21);
        assert_eq!(config.pull_threshold_size_for_topic_mib(), 22);
        assert_eq!(config.pull_interval_millis(), 23);
        assert_eq!(config.consume_message_batch_max_size(), 24);
        assert!(config.post_subscription_when_pull());
        assert!(config.unit_mode());
        assert_eq!(config.max_reconsume_times(), 25);
        assert_eq!(config.consume_timestamp(), "20240229010203");
        assert_eq!(config.suspend_current_queue_time_millis(), 26);
        assert_eq!(config.consume_timeout_minutes(), 27);
        assert_eq!(config.await_termination_millis_when_shutdown(), 28);
        assert_eq!(config.pop_threshold_for_queue(), 29);
        assert_eq!(config.pop_invisible_time_millis(), 30_000);
        assert_eq!(config.pop_batch_nums(), 31);
    }

    #[test]
    fn validation_matches_java_push_ranges_and_topic_unlimited_sentinel() {
        let mut config = PushConsumerConfig::new("orders").unwrap();
        assert!(config.validate("orders").is_ok());

        config.set_consume_thread_min(0);
        assert!(config.validate("orders").is_err());
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        assert!(config.validate("orders").is_ok());
        config.set_consume_thread_min(1_000);
        config.set_consume_thread_max(1_000);
        assert!(config.validate("orders").is_ok());
        config.set_consume_thread_min(1_001);
        assert!(config.validate("orders").is_err());
        config.set_consume_thread_min(20);
        config.set_consume_thread_max(1_001);
        assert!(config.validate("orders").is_err());
        config.set_consume_thread_max(19);
        assert!(matches!(
            config.validate("orders"),
            Err(PushConsumerError::InvalidThreadBounds { .. })
        ));
        config.set_consume_thread_max(20);

        config.set_pop_invisible_time_millis(4_999);
        assert!(config.validate("orders").is_err());
        config.set_pop_invisible_time_millis(5_000);
        config.set_pop_batch_nums(33);
        assert!(config.validate("orders").is_err());
        config.set_pop_batch_nums(32);
        assert!(config.validate("orders").is_ok());

        config.set_consume_concurrently_max_span(0);
        assert!(config.validate("orders").is_err());
        config.set_consume_concurrently_max_span(65_536);
        assert!(config.validate("orders").is_err());
        config.set_consume_concurrently_max_span(65_535);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_for_queue(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_queue(65_536);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_queue(65_535);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_for_topic(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_topic(6_553_500);
        assert!(config.validate("orders").is_ok());
        config.set_pull_threshold_for_topic(6_553_501);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_topic(-1);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_size_for_queue_mib(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_queue_mib(1_025);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_queue_mib(1_024);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_size_for_topic_mib(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_topic_mib(102_400);
        assert!(config.validate("orders").is_ok());
        config.set_pull_threshold_size_for_topic_mib(102_401);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_topic_mib(-1);

        config.set_pull_interval_millis(-1);
        assert!(config.validate("orders").is_err());
        config.set_pull_interval_millis(65_535);
        assert!(config.validate("orders").is_ok());
        config.set_pull_interval_millis(65_536);
        assert!(config.validate("orders").is_err());
        config.set_pull_interval_millis(0);

        config.set_consume_message_batch_max_size(0);
        assert!(config.validate("orders").is_err());
        config.set_consume_message_batch_max_size(1_024);
        assert!(config.validate("orders").is_ok());
        config.set_consume_message_batch_max_size(1_025);
        assert!(config.validate("orders").is_err());
        config.set_consume_message_batch_max_size(1);

        config.set_pull_batch_size(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_batch_size(1_024);
        assert!(config.validate("orders").is_ok());
        config.set_pull_batch_size(1_025);
        assert!(config.validate("orders").is_err());

        // Java checkConfig intentionally does not range-check this byte limit.
        config.set_pull_batch_size(32);
        config.set_pull_batch_size_in_bytes(-1);
        assert!(config.validate("orders").is_ok());
    }

    #[test]
    fn group_and_timestamp_validation_fail_before_startup() {
        let config = PushConsumerConfig::new("orders").unwrap();
        assert!(matches!(
            config.validate(DEFAULT_PUSH_CONSUMER_GROUP),
            Err(PushConsumerError::DefaultConsumerGroup)
        ));
        assert!(matches!(
            config.validate("bad/group"),
            Err(PushConsumerError::InvalidConsumerGroup)
        ));

        let config = PushConsumerConfig::new("orders").unwrap();
        config.set_consume_timestamp("not-a-date");
        assert!(matches!(
            config.validate("orders"),
            Err(PushConsumerError::InvalidConsumeTimestamp)
        ));
    }

    #[test]
    fn timestamp_validation_matches_java_lenient_date_parser() {
        assert!(parse_java_timestamp_millis("20240229010203").is_some());
        assert!(parse_java_timestamp_millis("20230229010203").is_some());
        assert!(parse_java_timestamp_millis("20241301010203").is_some());
        assert!(parse_java_timestamp_millis("20240229010203xyz").is_some());
        assert!(parse_java_timestamp_millis("2024022901020x").is_some());
        assert!(parse_java_timestamp_millis("2026010112000").is_some());
        assert!(parse_java_timestamp_millis("٢٠٢٤٠٢٢٩٠١٠٢٠٣").is_some());
        assert!(parse_java_timestamp_millis("２０２４０２２９０１０２０３").is_some());
        assert!(parse_java_timestamp_millis("2024022901").is_none());
        assert!(parse_java_timestamp_millis("+20240229010203").is_none());
        assert!(parse_java_timestamp_millis("not-a-date").is_none());
    }

    #[test]
    fn timestamp_acceptance_matches_java_551_fixture_cases() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/rebalance/initial-offsets.json"
        ))
        .unwrap();
        let cases = fixture["timestampParsing"].as_array().unwrap();
        assert_eq!(cases.len(), 12);
        for case in cases {
            let input = case["input"].as_str().unwrap();
            assert_eq!(
                parse_java_timestamp_millis(input).is_some(),
                case["accepted"].as_bool().unwrap(),
                "Java parseDate acceptance for {input:?}"
            );
        }
    }

    #[test]
    fn utc_fallback_formatter_matches_java_timestamp_shape() {
        assert_eq!(format_utc_timestamp(0), "19700101000000");
        assert_eq!(format_utc_timestamp(1_738_368_000), "20250201000000");
    }
}
