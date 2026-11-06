//! Settings of pinned Java 5.5.1 `org.apache.rocketmq.client.ClientConfig`.
//! Durations retain Java signed widths and milliseconds. Validation happens
//! when the owning runtime/service uses a setting, not in Java's plain setters.
//! Use namespace/NameServer setters to preserve Java's lazy-cache transitions.

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::codes::LanguageCode;
use std::{
    collections::BTreeSet,
    sync::{Arc, OnceLock, RwLock, RwLockReadGuard},
};

mod local_address;
pub(crate) mod namespace;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("instance endpoint path precedes its instance boundary")]
    InvalidInstanceEndpoint,
    #[error("instance name is null")]
    NullInstanceName,
    #[error("queue collection is null")]
    NullQueueCollection,
    #[error("message queue is null")]
    NullMessageQueue,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AccessChannel {
    #[default]
    Local,
    Cloud,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AccessChannelLookupError {
    /// Java's generated `valueOf(String)` rejects a null argument.
    #[error("Name is null")]
    NullName,
    /// Java's generated `valueOf(String)` rejects names outside the enum.
    #[error("No enum constant org.apache.rocketmq.client.AccessChannel.{0}")]
    UnknownName(String),
}

impl AccessChannel {
    /// Returns the Java enum declaration name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Local => "LOCAL",
            Self::Cloud => "CLOUD",
        }
    }

    /// Returns the Java enum declaration ordinal.
    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Local => 0,
            Self::Cloud => 1,
        }
    }

    /// Returns a fresh value array in Java declaration order.
    pub const fn values() -> [Self; 2] {
        [Self::Local, Self::Cloud]
    }

    /// Applies Java's exact, case-sensitive generated `valueOf(String)` lookup.
    pub fn from_java_name(name: Option<&str>) -> Result<Self, AccessChannelLookupError> {
        match name {
            None => Err(AccessChannelLookupError::NullName),
            Some("LOCAL") => Ok(Self::Local),
            Some("CLOUD") => Ok(Self::Cloud),
            Some(other) => Err(AccessChannelLookupError::UnknownName(other.to_owned())),
        }
    }

    /// Returns Java enum `compareTo`'s ordinal difference.
    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for AccessChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Settings corresponding to Java 5.5.1 `ClientConfig`.
///
/// Java keeps these values behind methods, so Rust callers use the matching
/// accessors instead of mutating implementation fields directly.
///
/// ```compile_fail
/// use rocketmq_classic_core::config::ClientConfig;
///
/// let mut config = ClientConfig::default();
/// config.client_ip = Some("192.0.2.1".to_owned());
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub(crate) namesrv_addr: Option<String>,
    pub(crate) client_ip: Option<String>,
    pub(crate) instance_name: Option<String>,
    pub(crate) client_callback_executor_threads: i32,
    pub(crate) namespace: Option<String>,
    namespace_initialized: bool,
    pub(crate) namespace_v2: Option<String>,
    pub(crate) access_channel: Option<AccessChannel>,
    pub(crate) poll_name_server_interval: i32,
    pub(crate) heartbeat_broker_interval: i32,
    pub(crate) persist_consumer_offset_interval: i32,
    pub(crate) pull_time_delay_millis_when_exception: i64,
    pub(crate) trace_msg_batch_num: i32,
    pub(crate) unit_mode: bool,
    pub(crate) unit_name: Option<String>,
    pub(crate) decode_read_body: bool,
    pub(crate) decode_decompress_body: bool,
    pub(crate) vip_channel_enabled: bool,
    pub(crate) use_heartbeat_v2: bool,
    pub(crate) use_tls: bool,
    pub(crate) socks_proxy_config: Option<String>,
    pub(crate) mq_client_api_timeout: i32,
    pub(crate) detect_timeout: i32,
    pub(crate) detect_interval: i32,
    pub(crate) language: Option<LanguageCode>,
    pub(crate) enable_stream_request_type: bool,
    pub(crate) send_latency_enable: bool,
    pub(crate) start_detector_enable: bool,
    pub(crate) enable_heartbeat_channel_event_listener: bool,
    pub(crate) enable_concurrent_heartbeat: bool,
    pub(crate) concurrent_heartbeat_thread_pool_size: i32,
    pub(crate) enable_trace: bool,
    pub(crate) trace_topic: Option<String>,
    pub(crate) max_page_size_in_get_metadata: i32,
}

/// Shared mutable configuration exposed by a constructed client runtime.
///
/// Java's `MQClientInstance.getClientConfig()` returns the live mutable
/// `ClientConfig` retained by that instance. Rust exposes the same ownership
/// through a lock-protected handle.
pub type SharedClientConfig = Arc<RwLock<ClientConfig>>;

// Java ClientConfig.useTLS reads the class-initialized TlsSystemConfig.tlsEnable
// value, so later system-property changes do not affect new ClientConfig objects.
static JAVA_TLS_ENABLE: OnceLock<bool> = OnceLock::new();

fn java_boolean_property(value: Option<String>, default: bool) -> bool {
    value.map_or(default, |value| value.eq_ignore_ascii_case("true"))
}

pub(crate) fn read_shared_client_config(
    config: &SharedClientConfig,
) -> RwLockReadGuard<'_, ClientConfig> {
    config
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("client_id", &self.build_client_id())
            .field("use_tls", &self.use_tls)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let access_channel = match self.access_channel {
            Some(AccessChannel::Local) => "LOCAL",
            Some(AccessChannel::Cloud) => "CLOUD",
            None => "null",
        };
        let language = self.language.map(LanguageCode::name).unwrap_or("null");
        write!(
            f,
            "ClientConfig{{namesrvAddr='{}', clientIP='{}', instanceName='{}', clientCallbackExecutorThreads={}, namespace='{}', namespaceInitialized={}, namespaceV2='{}', accessChannel={}, pollNameServerInterval={}, heartbeatBrokerInterval={}, persistConsumerOffsetInterval={}, pullTimeDelayMillsWhenException={}, unitMode={}, unitName='{}', decodeReadBody={}, decodeDecompressBody={}, vipChannelEnabled={}, useHeartbeatV2={}, useTLS={}, socksProxyConfig='{}', mqClientApiTimeout={}, detectTimeout={}, detectInterval={}, language={}, enableStreamRequestType={}, sendLatencyEnable={}, startDetectorEnable={}, enableHeartbeatChannelEventListener={}, enableTrace={}, traceTopic='{}', enableConcurrentHeartbeat={}, concurrentHeartbeatThreadPoolSize={}}}",
            self.namesrv_addr.as_deref().unwrap_or("null"),
            self.client_ip.as_deref().unwrap_or("null"),
            self.instance_name.as_deref().unwrap_or("null"),
            self.client_callback_executor_threads,
            self.namespace.as_deref().unwrap_or("null"),
            self.namespace_initialized,
            self.namespace_v2.as_deref().unwrap_or("null"),
            access_channel,
            self.poll_name_server_interval,
            self.heartbeat_broker_interval,
            self.persist_consumer_offset_interval,
            self.pull_time_delay_millis_when_exception,
            self.unit_mode,
            self.unit_name.as_deref().unwrap_or("null"),
            self.decode_read_body,
            self.decode_decompress_body,
            self.vip_channel_enabled,
            self.use_heartbeat_v2,
            self.use_tls,
            self.socks_proxy_config.as_deref().unwrap_or("null"),
            self.mq_client_api_timeout,
            self.detect_timeout,
            self.detect_interval,
            language,
            self.enable_stream_request_type,
            self.send_latency_enable,
            self.start_detector_enable,
            self.enable_heartbeat_channel_event_listener,
            self.enable_trace,
            self.trace_topic.as_deref().unwrap_or("null"),
            self.enable_concurrent_heartbeat,
            self.concurrent_heartbeat_thread_pool_size,
        )
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new_auto().expect("automatic client-IP initialization failed")
    }
}

#[cfg(test)]
fn java_client_property(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

#[cfg(not(test))]
fn java_client_property(key: &str) -> Option<String> {
    rocketmq_classic_common::utils::system_properties::get_property(key)
}

impl ClientConfig {
    pub const SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY: &'static str =
        "com.rocketmq.sendMessageWithVIPChannel";
    pub const SOCKS_PROXY_CONFIG: &'static str = "com.rocketmq.socks.proxy.config";
    pub const DECODE_READ_BODY: &'static str = "com.rocketmq.read.body";
    pub const DECODE_DECOMPRESS_BODY: &'static str = "com.rocketmq.decompress.body";
    pub const SEND_LATENCY_ENABLE: &'static str = "com.rocketmq.sendLatencyEnable";
    pub const START_DETECTOR_ENABLE: &'static str = "com.rocketmq.startDetectorEnable";
    pub const HEART_BEAT_V2: &'static str = "com.rocketmq.heartbeat.v2";

    /// Automatic IP initialization corresponding to Java's no-argument constructor.
    pub fn new_auto() -> Result<Self, ConfigError> {
        Ok(Self::new_auto_with_address_provider(local_address::get))
    }

    fn new_auto_with_address_provider(
        address_provider: impl FnOnce() -> std::io::Result<String>,
    ) -> Self {
        Self::from_auto_client_ip_result(address_provider())
    }

    fn from_auto_client_ip_result(address: std::io::Result<String>) -> Self {
        let tls_enable = *JAVA_TLS_ENABLE
            .get_or_init(|| java_boolean_property(std::env::var("tls.enable").ok(), false));
        // NetworkUtil.getLocalAddress catches interface/DNS failures and returns
        // null; ClientConfig stores that null rather than failing construction.
        Self::with_properties_and_tls_enable(address.ok(), java_client_property, tls_enable)
    }

    /// Explicit client-IP selection, corresponding to Java `setClientIP`.
    /// Known Java system-property keys are read from the shared property
    /// overlay before the process environment; NAMESRV_ADDR remains the Java
    /// environment fallback.
    pub fn new(client_ip: impl Into<String>) -> Self {
        let tls_enable = *JAVA_TLS_ENABLE
            .get_or_init(|| java_boolean_property(std::env::var("tls.enable").ok(), false));
        Self::with_properties_and_tls_enable(
            Some(client_ip.into()),
            java_client_property,
            tls_enable,
        )
    }

    #[cfg(test)]
    fn with_properties(client_ip: String, property: impl Fn(&str) -> Option<String>) -> Self {
        let boolean = |key: &str, default: bool| {
            property(key).map_or(default, |value| value.eq_ignore_ascii_case("true"))
        };
        let tls_enable = boolean("tls.enable", false);
        Self::with_properties_and_tls_enable(Some(client_ip), property, tls_enable)
    }

    fn with_properties_and_tls_enable(
        client_ip: Option<String>,
        property: impl Fn(&str) -> Option<String>,
        tls_enable: bool,
    ) -> Self {
        let boolean = |key: &str, default: bool| {
            property(key).map_or(default, |value| value.eq_ignore_ascii_case("true"))
        };
        let processors = std::thread::available_parallelism()
            .ok()
            .and_then(|count| i32::try_from(count.get()).ok())
            .unwrap_or(1);
        Self {
            namesrv_addr: property("rocketmq.namesrv.addr").or_else(|| property("NAMESRV_ADDR")),
            client_ip,
            instance_name: Some(
                property("rocketmq.client.name").unwrap_or_else(|| "DEFAULT".into()),
            ),
            client_callback_executor_threads: processors,
            namespace: None,
            namespace_initialized: false,
            namespace_v2: None,
            access_channel: Some(AccessChannel::Local),
            poll_name_server_interval: 30_000,
            heartbeat_broker_interval: 30_000,
            persist_consumer_offset_interval: 5_000,
            pull_time_delay_millis_when_exception: 1_000,
            trace_msg_batch_num: 10,
            unit_mode: false,
            unit_name: None,
            decode_read_body: boolean(Self::DECODE_READ_BODY, true),
            decode_decompress_body: boolean(Self::DECODE_DECOMPRESS_BODY, true),
            vip_channel_enabled: boolean(Self::SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, false),
            use_heartbeat_v2: boolean(Self::HEART_BEAT_V2, false),
            use_tls: tls_enable,
            socks_proxy_config: Some(
                property(Self::SOCKS_PROXY_CONFIG).unwrap_or_else(|| "{}".into()),
            ),
            mq_client_api_timeout: 3_000,
            detect_timeout: 200,
            detect_interval: 2_000,
            language: Some(LanguageCode::Java),
            enable_stream_request_type: false,
            send_latency_enable: boolean(Self::SEND_LATENCY_ENABLE, false),
            start_detector_enable: boolean(Self::START_DETECTOR_ENABLE, false),
            enable_heartbeat_channel_event_listener: true,
            enable_concurrent_heartbeat: false,
            concurrent_heartbeat_thread_pool_size: processors,
            enable_trace: false,
            trace_topic: None,
            max_page_size_in_get_metadata: 2_000,
        }
    }

    pub fn build_client_id(&self) -> String {
        let mut id = format!(
            "{}@{}",
            self.client_ip.as_deref().unwrap_or("null"),
            self.instance_name.as_deref().unwrap_or("null")
        );
        if let Some(unit) = &self.unit_name {
            if !unit.chars().all(java_whitespace) {
                id.push('@');
                id.push_str(unit);
            }
        }
        if self.enable_stream_request_type {
            id.push_str("@STREAM");
        }
        id
    }

    pub fn trace_msg_batch_num(&self) -> i32 {
        self.trace_msg_batch_num
    }
    pub fn set_trace_msg_batch_num(&mut self, value: i32) {
        self.trace_msg_batch_num = value;
    }
    pub fn client_ip(&self) -> Option<&str> {
        self.client_ip.as_deref()
    }
    pub fn set_client_ip(&mut self, value: Option<String>) {
        self.client_ip = value;
    }
    pub fn instance_name(&self) -> Option<&str> {
        self.instance_name.as_deref()
    }
    pub fn set_instance_name(&mut self, value: Option<String>) {
        self.instance_name = value;
    }
    pub fn client_callback_executor_threads(&self) -> i32 {
        self.client_callback_executor_threads
    }
    pub fn set_client_callback_executor_threads(&mut self, value: i32) {
        self.client_callback_executor_threads = value;
    }
    pub fn poll_name_server_interval(&self) -> i32 {
        self.poll_name_server_interval
    }
    pub fn set_poll_name_server_interval(&mut self, value: i32) {
        self.poll_name_server_interval = value;
    }
    pub fn heartbeat_broker_interval(&self) -> i32 {
        self.heartbeat_broker_interval
    }
    pub fn set_heartbeat_broker_interval(&mut self, value: i32) {
        self.heartbeat_broker_interval = value;
    }
    pub fn persist_consumer_offset_interval(&self) -> i32 {
        self.persist_consumer_offset_interval
    }
    pub fn set_persist_consumer_offset_interval(&mut self, value: i32) {
        self.persist_consumer_offset_interval = value;
    }
    pub fn pull_time_delay_millis_when_exception(&self) -> i64 {
        self.pull_time_delay_millis_when_exception
    }
    pub fn set_pull_time_delay_millis_when_exception(&mut self, value: i64) {
        self.pull_time_delay_millis_when_exception = value;
    }
    pub fn unit_name(&self) -> Option<&str> {
        self.unit_name.as_deref()
    }
    pub fn set_unit_name(&mut self, value: Option<String>) {
        self.unit_name = value;
    }
    pub fn unit_mode(&self) -> bool {
        self.unit_mode
    }
    pub fn set_unit_mode(&mut self, value: bool) {
        self.unit_mode = value;
    }
    pub fn vip_channel_enabled(&self) -> bool {
        self.vip_channel_enabled
    }
    pub fn set_vip_channel_enabled(&mut self, value: bool) {
        self.vip_channel_enabled = value;
    }
    pub fn use_tls(&self) -> bool {
        self.use_tls
    }
    pub fn set_use_tls(&mut self, value: bool) {
        self.use_tls = value;
    }
    pub fn socks_proxy_config(&self) -> Option<&str> {
        self.socks_proxy_config.as_deref()
    }
    pub fn set_socks_proxy_config(&mut self, value: Option<String>) {
        self.socks_proxy_config = value;
    }
    pub fn language(&self) -> Option<LanguageCode> {
        self.language
    }
    pub fn set_language(&mut self, value: Option<LanguageCode>) {
        self.language = value;
    }
    pub fn decode_read_body(&self) -> bool {
        self.decode_read_body
    }
    pub fn set_decode_read_body(&mut self, value: bool) {
        self.decode_read_body = value;
    }
    pub fn decode_decompress_body(&self) -> bool {
        self.decode_decompress_body
    }
    pub fn set_decode_decompress_body(&mut self, value: bool) {
        self.decode_decompress_body = value;
    }
    pub fn namespace_v2(&self) -> Option<&str> {
        self.namespace_v2.as_deref()
    }
    pub fn set_namespace_v2(&mut self, value: Option<String>) {
        self.namespace_v2 = value;
    }
    pub fn access_channel(&self) -> Option<AccessChannel> {
        self.access_channel
    }
    pub fn set_access_channel(&mut self, value: Option<AccessChannel>) {
        self.access_channel = value;
    }
    pub fn mq_client_api_timeout(&self) -> i32 {
        self.mq_client_api_timeout
    }
    pub fn set_mq_client_api_timeout(&mut self, value: i32) {
        self.mq_client_api_timeout = value;
    }
    pub fn enable_stream_request_type(&self) -> bool {
        self.enable_stream_request_type
    }
    pub fn set_enable_stream_request_type(&mut self, value: bool) {
        self.enable_stream_request_type = value;
    }
    pub fn send_latency_enable(&self) -> bool {
        self.send_latency_enable
    }
    pub fn set_send_latency_enable(&mut self, value: bool) {
        self.send_latency_enable = value;
    }
    pub fn start_detector_enable(&self) -> bool {
        self.start_detector_enable
    }
    pub fn set_start_detector_enable(&mut self, value: bool) {
        self.start_detector_enable = value;
    }
    pub fn enable_heartbeat_channel_event_listener(&self) -> bool {
        self.enable_heartbeat_channel_event_listener
    }
    pub fn set_enable_heartbeat_channel_event_listener(&mut self, value: bool) {
        self.enable_heartbeat_channel_event_listener = value;
    }
    pub fn detect_timeout(&self) -> i32 {
        self.detect_timeout
    }
    pub fn set_detect_timeout(&mut self, value: i32) {
        self.detect_timeout = value;
    }
    pub fn detect_interval(&self) -> i32 {
        self.detect_interval
    }
    pub fn set_detect_interval(&mut self, value: i32) {
        self.detect_interval = value;
    }
    pub fn use_heartbeat_v2(&self) -> bool {
        self.use_heartbeat_v2
    }
    pub fn set_use_heartbeat_v2(&mut self, value: bool) {
        self.use_heartbeat_v2 = value;
    }
    pub fn enable_trace(&self) -> bool {
        self.enable_trace
    }
    pub fn set_enable_trace(&mut self, value: bool) {
        self.enable_trace = value;
    }
    pub fn trace_topic(&self) -> Option<&str> {
        self.trace_topic.as_deref()
    }
    pub fn set_trace_topic(&mut self, value: Option<String>) {
        self.trace_topic = value;
    }
    pub fn max_page_size_in_get_metadata(&self) -> i32 {
        self.max_page_size_in_get_metadata
    }
    pub fn set_max_page_size_in_get_metadata(&mut self, value: i32) {
        self.max_page_size_in_get_metadata = value;
    }
    pub fn enable_concurrent_heartbeat(&self) -> bool {
        self.enable_concurrent_heartbeat
    }
    pub fn set_enable_concurrent_heartbeat(&mut self, value: bool) {
        self.enable_concurrent_heartbeat = value;
    }
    pub fn concurrent_heartbeat_thread_pool_size(&self) -> i32 {
        self.concurrent_heartbeat_thread_pool_size
    }
    pub fn set_concurrent_heartbeat_thread_pool_size(&mut self, value: i32) {
        self.concurrent_heartbeat_thread_pool_size = value;
    }

    /// Returns the Java-compatible non-null field snapshot used by
    /// `MixAll.object2Properties` when building consumer running information.
    ///
    /// This is an internal bridge for the separately packaged Consumer crate;
    /// it does not expose mutable configuration storage.
    #[doc(hidden)]
    pub fn to_java_properties(&self) -> std::collections::BTreeMap<String, String> {
        let mut properties = std::collections::BTreeMap::new();
        macro_rules! insert_value {
            ($key:literal, $field:ident) => {
                properties.insert($key.to_owned(), self.$field.to_string());
            };
        }
        macro_rules! insert_optional {
            ($key:literal, $field:ident) => {
                if let Some(value) = &self.$field {
                    properties.insert($key.to_owned(), value.clone());
                }
            };
        }

        insert_optional!("namesrvAddr", namesrv_addr);
        insert_optional!("clientIP", client_ip);
        insert_optional!("instanceName", instance_name);
        insert_value!(
            "clientCallbackExecutorThreads",
            client_callback_executor_threads
        );
        insert_optional!("namespace", namespace);
        insert_optional!("namespaceV2", namespace_v2);
        if let Some(access_channel) = self.access_channel {
            properties.insert("accessChannel".to_owned(), access_channel.name().to_owned());
        }
        insert_value!("pollNameServerInterval", poll_name_server_interval);
        insert_value!("heartbeatBrokerInterval", heartbeat_broker_interval);
        insert_value!(
            "persistConsumerOffsetInterval",
            persist_consumer_offset_interval
        );
        insert_value!(
            "pullTimeDelayMillsWhenException",
            pull_time_delay_millis_when_exception
        );
        insert_value!("traceMsgBatchNum", trace_msg_batch_num);
        insert_value!("unitMode", unit_mode);
        insert_optional!("unitName", unit_name);
        insert_value!("decodeReadBody", decode_read_body);
        insert_value!("decodeDecompressBody", decode_decompress_body);
        insert_value!("vipChannelEnabled", vip_channel_enabled);
        insert_value!("useHeartbeatV2", use_heartbeat_v2);
        insert_value!("useTLS", use_tls);
        insert_optional!("socksProxyConfig", socks_proxy_config);
        insert_value!("mqClientApiTimeout", mq_client_api_timeout);
        insert_value!("detectTimeout", detect_timeout);
        insert_value!("detectInterval", detect_interval);
        if let Some(language) = self.language {
            properties.insert("language".to_owned(), language.name().to_owned());
        }
        insert_value!("enableStreamRequestType", enable_stream_request_type);
        insert_value!("sendLatencyEnable", send_latency_enable);
        insert_value!("startDetectorEnable", start_detector_enable);
        insert_value!(
            "enableHeartbeatChannelEventListener",
            enable_heartbeat_channel_event_listener
        );
        insert_value!("enableConcurrentHeartbeat", enable_concurrent_heartbeat);
        insert_value!(
            "concurrentHeartbeatThreadPoolSize",
            concurrent_heartbeat_thread_pool_size
        );
        insert_value!("enableTrace", enable_trace);
        insert_optional!("traceTopic", trace_topic);
        insert_value!("maxPageSizeInGetMetadata", max_page_size_in_get_metadata);
        properties
    }

    pub fn set_namesrv_addr(&mut self, address: Option<String>) {
        self.namesrv_addr = address;
        self.namespace_initialized = false;
    }

    pub fn get_namesrv_addr(&self) -> Option<String> {
        self.namesrv_addr.as_deref().map(namespace::address)
    }

    pub fn set_namespace(&mut self, namespace: Option<String>) {
        self.namespace = namespace;
        self.namespace_initialized = true;
    }

    /// Mutable getter represents Java's lazy cached endpoint namespace.
    pub fn get_namespace(&mut self) -> Result<Option<&str>, ConfigError> {
        if !self.namespace_initialized && self.namespace.as_deref().is_none_or(str::is_empty) {
            if let Some(raw) = self
                .namesrv_addr
                .as_deref()
                .filter(|raw| namespace::instance_endpoint(raw))
            {
                self.namespace = Some(namespace::instance(raw)?);
            }
            self.namespace_initialized = true;
        }
        Ok(self.namespace.as_deref())
    }

    pub fn with_namespace(
        &mut self,
        resource: Option<&str>,
    ) -> Result<Option<String>, ConfigError> {
        Ok(namespace::wrap(self.get_namespace()?, resource))
    }

    pub fn without_namespace(
        &mut self,
        resource: Option<&str>,
    ) -> Result<Option<String>, ConfigError> {
        Ok(namespace::unwrap(self.get_namespace()?, resource))
    }

    pub fn with_namespace_set(
        &mut self,
        resources: &BTreeSet<Option<String>>,
    ) -> Result<BTreeSet<Option<String>>, ConfigError> {
        resources
            .iter()
            .map(|resource| self.with_namespace(resource.as_deref()))
            .collect()
    }

    pub fn without_namespace_set(
        &mut self,
        resources: &BTreeSet<Option<String>>,
    ) -> Result<BTreeSet<Option<String>>, ConfigError> {
        resources
            .iter()
            .map(|resource| self.without_namespace(resource.as_deref()))
            .collect()
    }

    pub fn queue_with_namespace(
        &mut self,
        queue: &MessageQueue,
    ) -> Result<MessageQueue, ConfigError> {
        let topic = self.with_namespace(queue.topic())?;
        Ok(MessageQueue::new_nullable(
            topic,
            queue.broker_name().map(str::to_owned),
            queue.queue_id(),
        ))
    }

    /// Mutates the supplied queues and returns the same borrowed collection.
    /// A null collection or element follows the Java failure point only when a
    /// non-empty namespace requires iteration.
    pub fn queues_with_namespace<'a>(
        &mut self,
        queues: Option<&'a mut [Option<MessageQueue>]>,
    ) -> Result<Option<&'a mut [Option<MessageQueue>]>, ConfigError> {
        let namespace = self.get_namespace()?.map(str::to_owned);
        let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
            return Ok(queues);
        };
        let Some(queues) = queues else {
            return Err(ConfigError::NullQueueCollection);
        };
        for item in queues.iter_mut() {
            let queue = item.as_mut().ok_or(ConfigError::NullMessageQueue)?;
            queue.set_topic(namespace::wrap(Some(&namespace), queue.topic()));
        }
        Ok(Some(queues))
    }

    pub fn change_instance_name_to_pid(&mut self) -> Result<(), ConfigError> {
        let name = self
            .instance_name
            .as_deref()
            .ok_or(ConfigError::NullInstanceName)?;
        if name == "DEFAULT" {
            static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
            let nanos = ORIGIN
                .get_or_init(std::time::Instant::now)
                .elapsed()
                .as_nanos() as i64;
            self.instance_name = Some(format!("{}#{nanos}", std::process::id() as i32));
        }
        Ok(())
    }

    /// Unlike Rust `Clone`, Java's cloneClientConfig omits three settings.
    pub fn clone_client_config(&self) -> Self {
        let mut copy = Self::new(self.client_ip.clone().unwrap_or_default());
        copy.reset_client_config(self);
        copy
    }

    /// Preserve the receiver's fields omitted by Java resetClientConfig.
    pub fn reset_client_config(&mut self, source: &Self) {
        let (trace_batch, page_size, channel, namespace_initialized) = (
            self.trace_msg_batch_num,
            self.max_page_size_in_get_metadata,
            self.access_channel,
            self.namespace_initialized,
        );
        *self = source.clone();
        self.trace_msg_batch_num = trace_batch;
        self.max_page_size_in_get_metadata = page_size;
        self.access_channel = channel;
        self.namespace_initialized = namespace_initialized;
    }
}

// Character.isWhitespace, used by Apache StringUtils.isBlank. Rust Unicode
// White_Space also includes NBSP/NEL; Java deliberately excludes those.
pub(crate) fn java_whitespace(value: char) -> bool {
    matches!(value, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}'
        | '\u{1680}' | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}'
        | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::process::Command;

    #[test]
    fn every_constructor_field_matches_pinned_client_config_defaults() {
        let processors = std::thread::available_parallelism().unwrap().get() as i32;
        assert_eq!(
            ClientConfig::with_properties("explicit-ip".into(), |_| None),
            ClientConfig {
                namesrv_addr: None,
                client_ip: Some("explicit-ip".into()),
                instance_name: Some("DEFAULT".into()),
                client_callback_executor_threads: processors,
                namespace: None,
                namespace_initialized: false,
                namespace_v2: None,
                access_channel: Some(AccessChannel::Local),
                poll_name_server_interval: 30_000,
                heartbeat_broker_interval: 30_000,
                persist_consumer_offset_interval: 5_000,
                pull_time_delay_millis_when_exception: 1_000,
                trace_msg_batch_num: 10,
                unit_mode: false,
                unit_name: None,
                decode_read_body: true,
                decode_decompress_body: true,
                vip_channel_enabled: false,
                use_heartbeat_v2: false,
                use_tls: false,
                socks_proxy_config: Some("{}".into()),
                mq_client_api_timeout: 3_000,
                detect_timeout: 200,
                detect_interval: 2_000,
                language: Some(LanguageCode::Java),
                enable_stream_request_type: false,
                send_latency_enable: false,
                start_detector_enable: false,
                enable_heartbeat_channel_event_listener: true,
                enable_concurrent_heartbeat: false,
                concurrent_heartbeat_thread_pool_size: processors,
                enable_trace: false,
                trace_topic: None,
                max_page_size_in_get_metadata: 2_000,
            }
        );
    }

    #[test]
    fn namespace_cache_setter_clone_reset_and_empty_collection_state_matches_java() {
        let mut config = ClientConfig::new("ip");
        config.set_namesrv_addr(Some("MQ_INST_a_b.example".into()));
        assert_eq!(config.get_namespace().unwrap(), Some("MQ_INST_a_b"));
        config.set_namesrv_addr(Some("MQ_INST_c_d.example".into()));
        assert_eq!(config.get_namespace().unwrap(), Some("MQ_INST_a_b"));
        config.set_namespace(None);
        assert_eq!(config.get_namespace().unwrap(), None);
        let mut copy = config.clone_client_config();
        assert_eq!(copy.get_namespace().unwrap(), Some("MQ_INST_c_d"));
        let mut initialized = ClientConfig::new("ip");
        initialized.set_namespace(None);
        initialized.reset_client_config(&config);
        assert_eq!(initialized.get_namespace().unwrap(), None);
        config.set_namesrv_addr(Some("MQ_INST_c_d.example".into()));
        assert_eq!(config.get_namespace().unwrap(), Some("MQ_INST_c_d"));
        let mut invalid = ClientConfig::new("ip");
        invalid.set_namesrv_addr(Some("http://MQ_INST_a_b.example/path".into()));
        assert!(
            invalid
                .with_namespace_set(&BTreeSet::new())
                .unwrap()
                .is_empty()
        );
        assert!(!invalid.namespace_initialized);
        assert!(invalid.queues_with_namespace(Some(&mut [])).is_err());
    }

    #[test]
    fn resource_sets_deduplicate_and_queue_collection_is_updated_in_place() {
        let mut config = ClientConfig::new("ip");
        config.set_namespace(Some("ns".into()));
        let resources = BTreeSet::from([
            None,
            Some("topic".into()),
            Some("ns%topic".into()),
            Some("%RETRY%group".into()),
        ]);
        let wrapped = config.with_namespace_set(&resources).unwrap();
        assert_eq!(
            wrapped,
            BTreeSet::from([
                None,
                Some("ns%topic".into()),
                Some("%RETRY%ns%group".into())
            ])
        );
        assert_eq!(
            config.without_namespace_set(&wrapped).unwrap(),
            BTreeSet::from([None, Some("topic".into()), Some("%RETRY%group".into())])
        );
        let queue = MessageQueue::new("topic", "broker", -1);
        assert_eq!(
            config.queue_with_namespace(&queue).unwrap(),
            MessageQueue::new("ns%topic", "broker", -1)
        );
        assert_eq!(queue.topic(), Some("topic"));
        let mut queues = [
            Some(queue),
            Some(MessageQueue::new("%DLQ%group", "broker", 2)),
        ];
        let address = queues.as_mut_ptr();
        let returned = config.queues_with_namespace(Some(&mut queues)).unwrap();
        assert_eq!(returned.unwrap().as_mut_ptr(), address);
        assert_eq!(
            queues[1].as_ref().unwrap(),
            &MessageQueue::new("%DLQ%ns%group", "broker", 2)
        );
        config.queues_with_namespace(Some(&mut queues)).unwrap();
        assert_eq!(queues[0].as_ref().unwrap().topic(), Some("ns%topic"));
    }

    #[test]
    fn pid_rename_only_changes_default_once_and_null_is_a_typed_error() {
        let mut config = ClientConfig::new("ip");
        config.instance_name = Some("DEFAULT".into());
        config.change_instance_name_to_pid().unwrap();
        let name = config.instance_name.clone().unwrap();
        let (pid, nanos) = name.split_once('#').unwrap();
        assert_eq!(pid, (std::process::id() as i32).to_string());
        nanos.parse::<i64>().unwrap();
        config.change_instance_name_to_pid().unwrap();
        assert_eq!(config.instance_name.as_deref(), Some(name.as_str()));
        for custom in ["custom", "default", ""] {
            config.instance_name = Some(custom.into());
            config.change_instance_name_to_pid().unwrap();
            assert_eq!(config.instance_name.as_deref(), Some(custom));
        }
        config.instance_name = None;
        assert_eq!(
            config.change_instance_name_to_pid(),
            Err(ConfigError::NullInstanceName)
        );
    }

    #[test]
    fn java_properties_have_exact_precedence_and_parse_boolean_rules() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let name_server = &fixture["nameServerAddressCases"];
        assert_eq!(name_server["systemPropertyKey"], "rocketmq.namesrv.addr");
        assert_eq!(name_server["environmentKey"], "NAMESRV_ADDR");
        assert_eq!(name_server["systemPropertyValue"], "system:9876");
        assert_eq!(name_server["explicitBlankPropertyValue"], "");

        let values = HashMap::from([
            ("rocketmq.namesrv.addr", ""),
            ("NAMESRV_ADDR", "fallback:9876"),
            ("ROCKETMQ_NAMESRV_ADDR", "wrong-fallback:9876"),
            ("rocketmq.client.name", "custom"),
            ("com.rocketmq.heartbeat.v2", "TrUe"),
            ("com.rocketmq.read.body", " true "),
            ("com.rocketmq.decompress.body", "invalid"),
            (
                "com.rocketmq.socks.proxy.config",
                "sensitive-proxy-credentials",
            ),
        ]);
        let config = ClientConfig::with_properties("ip".into(), |key| {
            values.get(key).map(|value| (*value).to_owned())
        });
        assert_eq!(config.namesrv_addr.as_deref(), Some(""));
        assert_eq!(config.build_client_id(), "ip@custom");
        assert!(config.use_heartbeat_v2);
        assert!(!config.decode_read_body);
        assert!(!config.decode_decompress_body);
        assert!(!format!("{config:?}").contains("credentials"));

        let fallback = ClientConfig::with_properties("ip".into(), |key| {
            (key == name_server["environmentKey"].as_str().unwrap())
                .then(|| "fallback:9876".to_owned())
        });
        assert_eq!(fallback.namesrv_addr.as_deref(), Some("fallback:9876"));
        let rejected_alias = ClientConfig::with_properties("ip".into(), |key| {
            (key == "ROCKETMQ_NAMESRV_ADDR").then(|| "wrong-fallback:9876".to_owned())
        });
        assert_eq!(rejected_alias.namesrv_addr, None);
    }

    #[test]
    fn client_tls_enable_uses_global_enable_not_server_mode() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/runtime/client-config-tls.json"
        ))
        .unwrap();
        assert_eq!(fixture["systemProperties"]["tls.server.mode"], "enforcing");
        assert_eq!(fixture["tlsEnable"], false);
        assert_eq!(fixture["tlsMode"], "ENFORCING");
        assert_eq!(fixture["clientConfigUseTLS"], false);
        assert_eq!(fixture["nettyClientConfigDefaultUseTLS"], true);
        assert_eq!(fixture["runtimeCopiedUseTLS"], false);

        let config = ClientConfig::with_properties("ip".into(), |key| {
            (key == "tls.server.mode").then(|| "enforcing".into())
        });
        assert_eq!(config.use_tls, fixture["clientConfigUseTLS"]);
        let config = ClientConfig::with_properties("ip".into(), |key| match key {
            "tls.server.mode" => Some("enforcing".into()),
            "tls.enable" => Some("false".into()),
            _ => None,
        });
        assert!(!config.use_tls);
        let config = ClientConfig::with_properties("ip".into(), |key| match key {
            "tls.server.mode" => Some("disabled".into()),
            "tls.enable" => Some("true".into()),
            _ => None,
        });
        assert!(config.use_tls);
    }

    #[test]
    fn tls_enable_class_initialization_child() {
        if std::env::var_os("ROCKETMQ_TLS_INIT_PROBE_CHILD").is_none() {
            return;
        }
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/runtime/client-config-tls.json"
        ))
        .unwrap();
        let first = ClientConfig::new("first");
        assert_eq!(first.use_tls, fixture["clientConfigUseTLS"]);

        // The parent starts this exact one-test child process with tls.enable=false.
        // SAFETY: no other test runs in this child process or reads the environment.
        unsafe { std::env::set_var("tls.enable", "true") };
        let second = ClientConfig::new("second");
        assert_eq!(
            second.use_tls,
            fixture["clientConfigUseTLSAfterPropertyMutation"]
        );
    }

    #[test]
    fn client_tls_property_snapshot_matches_invoked_java_class_initialization() {
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "config::tests::tls_enable_class_initialization_child",
            ])
            .env("ROCKETMQ_TLS_INIT_PROBE_CHILD", "1")
            .env("tls.enable", "false")
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn java_property_backed_client_fields_match_pinned_constructor_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/runtime/client-config-properties.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        let defaults = ClientConfig::with_properties("ip".into(), |_| None);
        assert_eq!(client_property_snapshot(&defaults), fixture["defaults"]);

        let properties = fixture["systemProperties"].as_object().unwrap();
        let configured = ClientConfig::with_properties("ip".into(), |key| {
            properties
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
        assert_eq!(client_property_snapshot(&configured), fixture["configured"]);
    }

    #[test]
    fn automatic_client_ip_constructor_uses_the_resolved_address() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        assert_eq!(fixture["clientConfigAutoIPUsesNetworkUtil"], true);

        let expected = "192.0.2.111";
        let config = ClientConfig::new_auto_with_address_provider(|| Ok(expected.to_owned()));
        assert_eq!(config.client_ip.as_deref(), Some(expected));

        // Exercise the real host enumeration path without assuming that this
        // machine has the same interfaces as the machine that generated the
        // Java fixture.
        let config = ClientConfig::new_auto().expect("automatic client-IP initialization");
        assert!(
            config
                .client_ip
                .as_deref()
                .is_none_or(|address| !address.is_empty())
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "run only in a Linux environment with IPv6 and no non-loopback IPv4 address"]
    fn automatic_client_ip_constructor_supports_ipv6_only_interfaces() {
        let config = ClientConfig::new_auto().expect("automatic client-IP initialization");
        let address = config
            .client_ip
            .as_deref()
            .expect("IPv6-only interface should provide a client IP");
        let bracketed_address = address
            .strip_prefix('[')
            .and_then(|address| address.strip_suffix(']'))
            .expect("Java-compatible IPv6 client IP should be bracketed");
        let unscoped_address = bracketed_address
            .split('%')
            .next()
            .unwrap_or(bracketed_address);
        let parsed: std::net::IpAddr = unscoped_address
            .parse()
            .expect("automatic client IP should be a valid IP address");

        assert!(parsed.is_ipv6(), "resolved non-IPv4 address: {address}");
        assert!(
            !parsed.is_loopback(),
            "resolved loopback address: {address}"
        );
    }

    #[test]
    fn automatic_client_ip_failure_preserves_java_null_default() {
        let config = ClientConfig::from_auto_client_ip_result(Err(std::io::Error::from(
            std::io::ErrorKind::AddrNotAvailable,
        )));
        assert_eq!(config.client_ip, None);
        assert_eq!(config.build_client_id(), "null@DEFAULT");
    }

    fn client_property_snapshot(config: &ClientConfig) -> serde_json::Value {
        serde_json::json!({
            "instanceName": config.instance_name,
            "decodeReadBody": config.decode_read_body,
            "decodeDecompressBody": config.decode_decompress_body,
            "vipChannelEnabled": config.vip_channel_enabled,
            "useHeartbeatV2": config.use_heartbeat_v2,
            "socksProxyConfig": config.socks_proxy_config,
            "sendLatencyEnable": config.send_latency_enable,
            "startDetectorEnable": config.start_detector_enable,
        })
    }
}
