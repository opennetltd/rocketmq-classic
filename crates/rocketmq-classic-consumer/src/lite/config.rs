use std::sync::Arc;

use rocketmq_classic_core::config::{ClientConfig, ConfigError};
use rocketmq_classic_protocol::heartbeat::{ConsumeFromWhere, MessageModel};
use rocketmq_classic_remoting::client::TlsConfig;

use crate::{
    config::DEFAULT_CONSUMER_GROUP,
    error::LitePullConsumerError,
    offset::OffsetStore,
    push::{MessageQueueListener, default_consume_timestamp},
    rebalance::{AllocateMessageQueueAveragely, AllocateMessageQueueStrategy},
};

pub(crate) const DEFAULT_LITE_PULL_THREAD_NUMS: i32 = 20;
pub(crate) const DEFAULT_LITE_AUTO_COMMIT_INTERVAL_MILLIS: i64 = 5_000;
pub(crate) const DEFAULT_LITE_PULL_BATCH_SIZE: i32 = 10;
pub(crate) const DEFAULT_LITE_PULL_THRESHOLD_FOR_ALL: i64 = 10_000;
pub(crate) const DEFAULT_LITE_PULL_THRESHOLD_FOR_QUEUE: i32 = 1_000;
pub(crate) const DEFAULT_LITE_PULL_THRESHOLD_SIZE_FOR_QUEUE: i32 = 100;
pub(crate) const DEFAULT_LITE_CONSUME_MAX_SPAN: i32 = 2_000;
pub(crate) const DEFAULT_LITE_POLL_TIMEOUT_MILLIS: i64 = 5_000;
pub(crate) const DEFAULT_LITE_TOPIC_METADATA_CHECK_INTERVAL_MILLIS: i64 = 30_000;
pub(crate) const DEFAULT_LITE_BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
pub(crate) const DEFAULT_LITE_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;
pub(crate) const DEFAULT_LITE_CONSUMER_PULL_TIMEOUT_MILLIS: i64 = 10_000;

/// Java 5.5.1 `DefaultLitePullConsumer` configuration.
///
/// The constants that define these defaults are implementation details; Java
/// exposes the corresponding values through getters.
///
/// ```compile_fail
/// use rocketmq_classic_consumer::lite::config::{
///     DEFAULT_LITE_AUTO_COMMIT_INTERVAL_MILLIS,
///     DEFAULT_LITE_BROKER_SUSPEND_MAX_TIME_MILLIS,
///     DEFAULT_LITE_CONSUME_MAX_SPAN,
///     DEFAULT_LITE_CONSUMER_PULL_TIMEOUT_MILLIS,
///     DEFAULT_LITE_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
///     DEFAULT_LITE_POLL_TIMEOUT_MILLIS,
///     DEFAULT_LITE_PULL_BATCH_SIZE,
///     DEFAULT_LITE_PULL_THREAD_NUMS,
///     DEFAULT_LITE_PULL_THRESHOLD_FOR_ALL,
///     DEFAULT_LITE_PULL_THRESHOLD_FOR_QUEUE,
///     DEFAULT_LITE_PULL_THRESHOLD_SIZE_FOR_QUEUE,
///     DEFAULT_LITE_TOPIC_METADATA_CHECK_INTERVAL_MILLIS,
/// };
/// ```
pub struct LitePullConsumerConfig {
    client: ClientConfig,
    tls_config: Option<TlsConfig>,
    consumer_group: Option<String>,
    consumer_timeout_millis_when_suspend: i64,
    consumer_pull_timeout_millis: i64,
    message_model: MessageModel,
    message_queue_listener: Option<MessageQueueListener>,
    offset_store: Option<Arc<dyn OffsetStore>>,
    allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    auto_commit: bool,
    pull_thread_nums: i32,
    auto_commit_interval_millis: i64,
    pull_batch_size: i32,
    pull_threshold_for_all: i64,
    consume_max_span: i32,
    pull_threshold_for_queue: i32,
    pull_threshold_size_for_queue: i32,
    poll_timeout_millis: i64,
    topic_metadata_check_interval_millis: i64,
    consume_from_where: ConsumeFromWhere,
    consume_timestamp: String,
    connect_broker_by_user: bool,
    default_broker_id: i64,
}

impl LitePullConsumerConfig {
    /// Creates the Rust equivalent of Java's no-argument constructor.
    /// The `DEFAULT_CONSUMER` group is rejected when the consumer starts, as in Java.
    pub fn new_default() -> Result<Self, ConfigError> {
        Self::new(DEFAULT_CONSUMER_GROUP)
    }

    pub fn new(consumer_group: impl Into<String>) -> Result<Self, ConfigError> {
        Self::new_nullable(Some(consumer_group.into()))
    }

    /// Creates Lite configuration while preserving a Java `null` group until
    /// the consumer's startup validation boundary.
    pub fn new_nullable(consumer_group: Option<String>) -> Result<Self, ConfigError> {
        let mut client = ClientConfig::new_auto()?;
        // Java's DefaultLitePullConsumer constructors enable stream request
        // type before creating the underlying client runtime.
        client.set_enable_stream_request_type(true);
        Ok(Self {
            client,
            tls_config: None,
            consumer_group,
            consumer_timeout_millis_when_suspend: DEFAULT_LITE_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
            consumer_pull_timeout_millis: DEFAULT_LITE_CONSUMER_PULL_TIMEOUT_MILLIS,
            message_model: MessageModel::Clustering,
            message_queue_listener: None,
            offset_store: None,
            allocate_message_queue_strategy: Arc::new(AllocateMessageQueueAveragely),
            auto_commit: true,
            pull_thread_nums: DEFAULT_LITE_PULL_THREAD_NUMS,
            auto_commit_interval_millis: DEFAULT_LITE_AUTO_COMMIT_INTERVAL_MILLIS,
            pull_batch_size: DEFAULT_LITE_PULL_BATCH_SIZE,
            pull_threshold_for_all: DEFAULT_LITE_PULL_THRESHOLD_FOR_ALL,
            consume_max_span: DEFAULT_LITE_CONSUME_MAX_SPAN,
            pull_threshold_for_queue: DEFAULT_LITE_PULL_THRESHOLD_FOR_QUEUE,
            pull_threshold_size_for_queue: DEFAULT_LITE_PULL_THRESHOLD_SIZE_FOR_QUEUE,
            poll_timeout_millis: DEFAULT_LITE_POLL_TIMEOUT_MILLIS,
            topic_metadata_check_interval_millis: DEFAULT_LITE_TOPIC_METADATA_CHECK_INTERVAL_MILLIS,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            consume_timestamp: default_consume_timestamp(),
            connect_broker_by_user: false,
            default_broker_id: 0,
        })
    }

    /// Java `ClientConfig` settings inherited by Lite are configured through
    /// this value and copied into the underlying pull consumer at startup.
    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    /// Mutable access to Lite's inherited Java `ClientConfig` settings.
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

    /// Returns the convenience string view, or `""` when the Java group is
    /// null. Use `consumer_group_option` to distinguish null from empty.
    pub fn consumer_group(&self) -> &str {
        self.consumer_group.as_deref().unwrap_or_default()
    }

    pub fn consumer_group_option(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn get_consumer_group(&self) -> Option<String> {
        self.consumer_group.clone()
    }

    pub fn set_consumer_group(&mut self, value: impl Into<String>) {
        self.consumer_group = Some(value.into());
    }

    pub fn set_consumer_group_nullable(&mut self, value: Option<String>) {
        self.consumer_group = value;
    }

    pub const fn broker_suspend_max_time_millis(&self) -> i64 {
        DEFAULT_LITE_BROKER_SUSPEND_MAX_TIME_MILLIS
    }

    pub const fn consumer_timeout_millis_when_suspend(&self) -> i64 {
        self.consumer_timeout_millis_when_suspend
    }

    pub fn set_consumer_timeout_millis_when_suspend(&mut self, value: i64) {
        self.consumer_timeout_millis_when_suspend = value;
    }

    pub const fn consumer_pull_timeout_millis(&self) -> i64 {
        self.consumer_pull_timeout_millis
    }

    pub fn set_consumer_pull_timeout_millis(&mut self, value: i64) {
        self.consumer_pull_timeout_millis = value;
    }

    pub const fn message_model(&self) -> MessageModel {
        self.message_model
    }

    pub fn set_message_model(&mut self, value: MessageModel) {
        self.message_model = value;
    }

    pub fn message_queue_listener(&self) -> Option<MessageQueueListener> {
        self.message_queue_listener.clone()
    }

    pub fn set_message_queue_listener(&mut self, value: Option<MessageQueueListener>) {
        self.message_queue_listener = value;
    }

    pub fn offset_store(&self) -> Option<Arc<dyn OffsetStore>> {
        self.offset_store.clone()
    }

    pub fn set_offset_store(&mut self, value: Option<Arc<dyn OffsetStore>>) {
        self.offset_store = value;
    }

    pub fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy> {
        self.allocate_message_queue_strategy.clone()
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        value: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        self.allocate_message_queue_strategy = value;
    }

    pub fn unit_mode(&self) -> bool {
        self.client.unit_mode()
    }

    pub fn set_unit_mode(&mut self, value: bool) {
        self.client.set_unit_mode(value);
    }

    pub const fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn set_auto_commit(&mut self, value: bool) {
        self.auto_commit = value;
    }

    pub const fn pull_thread_nums(&self) -> i32 {
        self.pull_thread_nums
    }

    pub fn set_pull_thread_nums(&mut self, value: i32) {
        self.pull_thread_nums = value;
    }

    pub const fn auto_commit_interval_millis(&self) -> i64 {
        self.auto_commit_interval_millis
    }

    pub fn set_auto_commit_interval_millis(&mut self, value: i64) {
        if value >= 1_000 {
            self.auto_commit_interval_millis = value;
        }
    }

    pub const fn pull_batch_size(&self) -> i32 {
        self.pull_batch_size
    }

    pub fn set_pull_batch_size(&mut self, value: i32) {
        self.pull_batch_size = value;
    }

    pub const fn pull_threshold_for_all(&self) -> i64 {
        self.pull_threshold_for_all
    }

    pub fn set_pull_threshold_for_all(&mut self, value: i64) {
        self.pull_threshold_for_all = value;
    }

    pub const fn consume_max_span(&self) -> i32 {
        self.consume_max_span
    }

    pub fn set_consume_max_span(&mut self, value: i32) {
        self.consume_max_span = value;
    }

    pub const fn pull_threshold_for_queue(&self) -> i32 {
        self.pull_threshold_for_queue
    }

    pub fn set_pull_threshold_for_queue(&mut self, value: i32) {
        self.pull_threshold_for_queue = value;
    }

    pub const fn pull_threshold_size_for_queue(&self) -> i32 {
        self.pull_threshold_size_for_queue
    }

    pub fn set_pull_threshold_size_for_queue(&mut self, value: i32) {
        self.pull_threshold_size_for_queue = value;
    }

    pub const fn poll_timeout_millis(&self) -> i64 {
        self.poll_timeout_millis
    }

    pub fn set_poll_timeout_millis(&mut self, value: i64) {
        self.poll_timeout_millis = value;
    }

    pub const fn topic_metadata_check_interval_millis(&self) -> i64 {
        self.topic_metadata_check_interval_millis
    }

    pub fn set_topic_metadata_check_interval_millis(&mut self, value: i64) {
        self.topic_metadata_check_interval_millis = value;
    }

    pub const fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consume_from_where
    }

    pub fn set_consume_from_where(
        &mut self,
        value: ConsumeFromWhere,
    ) -> Result<(), LitePullConsumerError> {
        if !matches!(
            value,
            ConsumeFromWhere::ConsumeFromFirstOffset
                | ConsumeFromWhere::ConsumeFromLastOffset
                | ConsumeFromWhere::ConsumeFromTimestamp
        ) {
            return Err(LitePullConsumerError::InvalidConsumeFromWhere(value));
        }
        self.consume_from_where = value;
        Ok(())
    }

    pub fn consume_timestamp(&self) -> &str {
        &self.consume_timestamp
    }

    pub fn set_consume_timestamp(&mut self, value: impl Into<String>) {
        self.consume_timestamp = value.into();
    }

    pub const fn connect_broker_by_user(&self) -> bool {
        self.connect_broker_by_user
    }

    pub fn set_connect_broker_by_user(&mut self, value: bool) {
        self.connect_broker_by_user = value;
    }

    pub const fn default_broker_id(&self) -> i64 {
        self.default_broker_id
    }

    pub fn set_default_broker_id(&mut self, value: i64) {
        self.default_broker_id = value;
    }

    pub fn enable_msg_trace(&self) -> bool {
        self.client.enable_trace()
    }

    pub fn set_enable_msg_trace(&mut self, value: bool) {
        self.client.set_enable_trace(value);
    }

    pub fn customized_trace_topic(&self) -> Option<&str> {
        self.client.trace_topic()
    }

    pub fn set_customized_trace_topic(&mut self, value: Option<String>) {
        self.client.set_trace_topic(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_tls_config_enables_client_transport() {
        let mut config = LitePullConsumerConfig::new("tls-lite-consumer").unwrap();
        let mut tls = TlsConfig::default();
        tls.test_mode = false;
        config.set_tls_config(Some(tls));
        assert!(config.client_config().use_tls());
        assert!(!config.tls_config().unwrap().test_mode);
    }

    #[test]
    fn nullable_consumer_group_is_distinct_from_empty_group() {
        let mut config = LitePullConsumerConfig::new_nullable(None).unwrap();
        assert_eq!(config.get_consumer_group(), None);
        config.set_consumer_group_nullable(Some(String::new()));
        assert_eq!(config.get_consumer_group().as_deref(), Some(""));
        config.set_consumer_group("orders");
        assert_eq!(config.get_consumer_group().as_deref(), Some("orders"));
    }
}
