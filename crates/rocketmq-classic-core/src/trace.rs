//! Java 5.5.1 message-trace record model and wire encoder.
//!
//! Trace delimiters correspond to the public `TraceConstants` fields. Buffer
//! capacity and chunk limits remain implementation details, as in Java.
//!
//! ```compile_fail
//! use rocketmq_classic_core::trace::{
//!     TRACE_MAX_BATCH_SIZE,
//!     TRACE_MAX_MESSAGE_SIZE,
//!     TRACE_QUEUE_CAPACITY,
//! };
//! ```

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    fmt,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::config::AccessChannel;
use crate::unique_id::UniqueIdGenerator;
use rocketmq_classic_protocol::codes::JavaEnumLookupError;

pub const TRACE_FIELD_SEPARATOR: char = '\u{1}';
pub const TRACE_RECORD_SEPARATOR: char = '\u{2}';

static TRACE_CONTEXT_IDS: OnceLock<UniqueIdGenerator> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceType {
    Pub,
    Recall,
    SubBefore,
    SubAfter,
    EndTransaction,
}

impl TraceType {
    pub const fn values() -> [Self; 5] {
        [
            Self::Pub,
            Self::Recall,
            Self::SubBefore,
            Self::SubAfter,
            Self::EndTransaction,
        ]
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Pub => 0,
            Self::Recall => 1,
            Self::SubBefore => 2,
            Self::SubAfter => 3,
            Self::EndTransaction => 4,
        }
    }

    pub const fn java_name(self) -> &'static str {
        match self {
            Self::Pub => "Pub",
            Self::Recall => "Recall",
            Self::SubBefore => "SubBefore",
            Self::SubAfter => "SubAfter",
            Self::EndTransaction => "EndTransaction",
        }
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        Self::values()
            .into_iter()
            .find(|trace_type| Some(trace_type.java_name()) == name)
            .ok_or_else(|| match name {
                None => JavaEnumLookupError::NullName,
                Some(name) => JavaEnumLookupError::UnknownName {
                    enum_class: "org.apache.rocketmq.client.trace.TraceType",
                    name: name.to_owned(),
                },
            })
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

/// Java client `MessageType` values used by producer hooks and trace records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageType {
    Normal,
    TransactionHalf,
    TransactionCommit,
    Delay,
    Order,
}

impl MessageType {
    pub const fn values() -> [Self; 5] {
        [
            Self::Normal,
            Self::TransactionHalf,
            Self::TransactionCommit,
            Self::Delay,
            Self::Order,
        ]
    }

    pub const fn java_name(self) -> &'static str {
        match self {
            Self::Normal => "Normal_Msg",
            Self::TransactionHalf => "Trans_Msg_Half",
            Self::TransactionCommit => "Trans_msg_Commit",
            Self::Delay => "Delay_Msg",
            Self::Order => "Order_Msg",
        }
    }

    pub const fn short_name(self) -> &'static str {
        match self {
            Self::Normal => "Normal",
            Self::TransactionHalf => "Trans",
            Self::TransactionCommit => "TransCommit",
            Self::Delay => "Delay",
            Self::Order => "Order",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Normal => 0,
            Self::TransactionHalf => 1,
            Self::TransactionCommit => 2,
            Self::Delay => 3,
            Self::Order => 4,
        }
    }

    pub fn get_by_short_name(short_name: Option<&str>) -> Self {
        Self::values()
            .into_iter()
            .find(|message_type| Some(message_type.short_name()) == short_name)
            .unwrap_or(Self::Normal)
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        Self::values()
            .into_iter()
            .find(|message_type| Some(message_type.java_name()) == name)
            .ok_or_else(|| match name {
                None => JavaEnumLookupError::NullName,
                Some(name) => JavaEnumLookupError::UnknownName {
                    enum_class: "org.apache.rocketmq.common.message.MessageType",
                    name: name.to_owned(),
                },
            })
    }
}

/// Local transaction outcome used by Java trace beans and transaction sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTransactionState {
    Commit,
    Rollback,
    Unknown,
}

impl LocalTransactionState {
    pub const fn values() -> [Self; 3] {
        [Self::Commit, Self::Rollback, Self::Unknown]
    }

    pub const fn java_name(self) -> &'static str {
        match self {
            Self::Commit => "COMMIT_MESSAGE",
            Self::Rollback => "ROLLBACK_MESSAGE",
            Self::Unknown => "UNKNOW",
        }
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        Self::values()
            .into_iter()
            .find(|state| Some(state.java_name()) == name)
            .ok_or_else(|| match name {
                None => JavaEnumLookupError::NullName,
                Some(name) => JavaEnumLookupError::UnknownName {
                    enum_class: "org.apache.rocketmq.client.producer.LocalTransactionState",
                    name: name.to_owned(),
                },
            })
    }
}

/// One Java `TraceBean` value. Java inherits identity-based `Object.equals` and
/// `Object.hashCode`, so this Rust value intentionally has no structural
/// equality implementation.
///
/// ```compile_fail
/// use rocketmq_classic_core::trace::TraceBean;
/// fn requires_equality<T: PartialEq>() {}
/// requires_equality::<TraceBean>();
/// ```
#[derive(Clone, Debug)]
pub struct TraceBean {
    pub topic: Option<String>,
    pub message_id: Option<String>,
    pub offset_message_id: Option<String>,
    pub tags: Option<String>,
    pub keys: Option<String>,
    pub store_host: Option<String>,
    /// Java `TraceBean.clientHost`, initialized from the local address.
    pub client_host: Option<String>,
    /// Java `TraceBean.storeTime`; retained even though the 5.5.1 wire encoder
    /// does not serialize it.
    pub store_time_millis: i64,
    pub body_length: i32,
    pub retry_times: i32,
    pub message_type: Option<MessageType>,
    pub transaction_id: Option<String>,
    pub transaction_state: Option<LocalTransactionState>,
    pub from_transaction_check: bool,
}

impl Default for TraceBean {
    fn default() -> Self {
        let local_address = crate::config::ClientConfig::new_auto()
            .ok()
            .and_then(|config| config.client_ip().map(str::to_owned))
            .unwrap_or_default();
        Self {
            topic: Some(String::new()),
            message_id: Some(String::new()),
            offset_message_id: Some(String::new()),
            tags: Some(String::new()),
            keys: Some(String::new()),
            store_host: Some(local_address.clone()),
            client_host: Some(local_address),
            store_time_millis: 0,
            body_length: 0,
            retry_times: 0,
            message_type: None,
            transaction_id: None,
            transaction_state: None,
            from_transaction_check: false,
        }
    }
}

/// Java `TraceContext` value with its mutable trace fields. Java implements
/// `Comparable` by timestamp but inherits identity-based `equals` and
/// `hashCode`; this Rust value therefore does not expose structural equality.
///
/// ```compile_fail
/// use rocketmq_classic_core::trace::TraceContext;
/// fn requires_equality<T: PartialEq>() {}
/// requires_equality::<TraceContext>();
/// ```
#[derive(Clone, Debug)]
pub struct TraceContext {
    /// Java `TraceContext.traceType` defaults to null and remains mutable.
    pub trace_type: Option<TraceType>,
    pub timestamp_millis: i64,
    pub region_id: Option<String>,
    /// Java `TraceContext.regionName`; the 5.5.1 wire encoder uses `regionId`.
    pub region_name: Option<String>,
    pub group_name: Option<String>,
    pub cost_time_millis: i32,
    pub success: bool,
    pub request_id: Option<String>,
    pub context_code: i32,
    pub access_channel: Option<AccessChannel>,
    /// Java leaves `traceBeans` null in its no-argument constructor.
    pub trace_beans: Option<Vec<TraceBean>>,
}

impl Default for TraceContext {
    fn default() -> Self {
        let timestamp_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or_default();
        let client_ip = crate::config::ClientConfig::new_auto()
            .ok()
            .and_then(|config| config.client_ip().map(str::to_owned));
        let ids = TRACE_CONTEXT_IDS.get_or_init(|| UniqueIdGenerator::new(client_ip.as_deref()));
        Self {
            trace_type: None,
            timestamp_millis,
            region_id: Some(String::new()),
            region_name: Some(String::new()),
            group_name: Some(String::new()),
            cost_time_millis: 0,
            success: true,
            request_id: Some(ids.create()),
            context_code: 0,
            access_channel: None,
            trace_beans: None,
        }
    }
}

impl TraceContext {
    /// Creates a context with a trace type and the remaining Java defaults.
    pub fn new(trace_type: TraceType) -> Self {
        Self {
            trace_type: Some(trace_type),
            ..Self::default()
        }
    }

    /// Java `TraceContext.compareTo`, ordered by event timestamp.
    pub fn compare_to(&self, other: &Self) -> Ordering {
        self.timestamp_millis.cmp(&other.timestamp_millis)
    }

    /// Compares with an optional value and maps Java's null-argument failure.
    pub fn java_compare_to_nullable(
        &self,
        other: Option<&Self>,
    ) -> Result<Ordering, TraceContextCompareError> {
        other
            .map(|other| self.compare_to(other))
            .ok_or(TraceContextCompareError::NullOther)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TraceContextCompareError {
    #[error("Java NullPointerException: null TraceContext compareTo argument")]
    NullOther,
}

impl fmt::Display for TraceContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "TraceContext{{{}_{}_{}_{}_",
            self.trace_type.map(TraceType::java_name).unwrap_or("null"),
            java_nullable(self.group_name.as_deref()),
            java_nullable(self.region_id.as_deref()),
            self.success
        )?;
        if let Some(trace_beans) = &self.trace_beans {
            for bean in trace_beans {
                write!(
                    formatter,
                    "{}_{}_",
                    java_nullable(bean.message_id.as_deref()),
                    java_nullable(bean.topic.as_deref())
                )?;
            }
        }
        formatter.write_str("}")
    }
}

impl TraceContext {
    /// Serializes records in the order consumed by Java `TraceDataEncoder`.
    pub fn encode(&self) -> String {
        let trace_type = self
            .trace_type
            .expect("Java TraceDataEncoder fails when traceType is null");
        let trace_beans = self
            .trace_beans
            .as_ref()
            .expect("Java TraceDataEncoder fails when traceBeans is null");
        if trace_beans.is_empty()
            && matches!(
                trace_type,
                TraceType::Pub | TraceType::EndTransaction | TraceType::Recall
            )
        {
            panic!("Java TraceDataEncoder reads the first trace bean for this trace type");
        }
        let beans_to_encode = match trace_type {
            TraceType::Pub | TraceType::EndTransaction | TraceType::Recall => &trace_beans[..1],
            TraceType::SubBefore | TraceType::SubAfter => trace_beans.as_slice(),
        };
        let mut output = String::new();
        for bean in beans_to_encode {
            let fields = match trace_type {
                TraceType::Pub => {
                    let message_type = bean
                        .message_type
                        .expect("Java TraceDataEncoder reads the Pub bean message type");
                    vec![
                        trace_type.java_name().to_owned(),
                        self.timestamp_millis.to_string(),
                        java_nullable(self.region_id.as_deref()),
                        java_nullable(self.group_name.as_deref()),
                        java_nullable(bean.topic.as_deref()),
                        java_nullable(bean.message_id.as_deref()),
                        java_nullable(bean.tags.as_deref()),
                        java_nullable(bean.keys.as_deref()),
                        java_nullable(bean.store_host.as_deref()),
                        bean.body_length.to_string(),
                        self.cost_time_millis.to_string(),
                        message_type.ordinal().to_string(),
                        java_nullable(bean.offset_message_id.as_deref()),
                        self.success.to_string(),
                    ]
                }
                TraceType::SubBefore => vec![
                    trace_type.java_name().to_owned(),
                    self.timestamp_millis.to_string(),
                    java_nullable(self.region_id.as_deref()),
                    java_nullable(self.group_name.as_deref()),
                    java_nullable(self.request_id.as_deref()),
                    java_nullable(bean.message_id.as_deref()),
                    bean.retry_times.to_string(),
                    java_nullable(bean.keys.as_deref()),
                ],
                TraceType::SubAfter => {
                    let access_channel = self
                        .access_channel
                        .expect("Java TraceDataEncoder dereferences SubAfter accessChannel");
                    let mut fields = vec![
                        trace_type.java_name().to_owned(),
                        java_nullable(self.request_id.as_deref()),
                        java_nullable(bean.message_id.as_deref()),
                        self.cost_time_millis.to_string(),
                        self.success.to_string(),
                        java_nullable(bean.keys.as_deref()),
                        self.context_code.to_string(),
                    ];
                    if access_channel != AccessChannel::Cloud {
                        fields.push(self.timestamp_millis.to_string());
                        fields.push(java_nullable(self.group_name.as_deref()));
                    } else {
                        // Java appends the field separator before omitting Cloud-only fields.
                        fields.push(String::new());
                    }
                    fields
                }
                TraceType::EndTransaction => {
                    let message_type = bean
                        .message_type
                        .expect("Java TraceDataEncoder reads the transaction bean message type");
                    let transaction_state = bean
                        .transaction_state
                        .expect("Java TraceDataEncoder reads the transaction state name");
                    vec![
                        trace_type.java_name().to_owned(),
                        self.timestamp_millis.to_string(),
                        java_nullable(self.region_id.as_deref()),
                        java_nullable(self.group_name.as_deref()),
                        java_nullable(bean.topic.as_deref()),
                        java_nullable(bean.message_id.as_deref()),
                        java_nullable(bean.tags.as_deref()),
                        java_nullable(bean.keys.as_deref()),
                        java_nullable(bean.store_host.as_deref()),
                        message_type.ordinal().to_string(),
                        java_nullable(bean.transaction_id.as_deref()),
                        transaction_state.java_name().to_owned(),
                        bean.from_transaction_check.to_string(),
                    ]
                }
                TraceType::Recall => vec![
                    trace_type.java_name().to_owned(),
                    self.timestamp_millis.to_string(),
                    java_nullable(self.region_id.as_deref()),
                    java_nullable(self.group_name.as_deref()),
                    java_nullable(bean.topic.as_deref()),
                    java_nullable(bean.message_id.as_deref()),
                    self.success.to_string(),
                ],
            };
            append_record(&mut output, fields);
        }
        output
    }

    /// Returns Java trace transfer keys: message IDs and each nonempty message key.
    #[doc(hidden)]
    pub fn transfer_keys(&self) -> BTreeSet<String> {
        let mut keys = BTreeSet::new();
        for bean in self.trace_beans.iter().flatten() {
            if let Some(message_id) = &bean.message_id {
                keys.insert(message_id.clone());
            }
            if let Some(message_keys) = bean.keys.as_deref().filter(|keys| !keys.is_empty()) {
                keys.extend(
                    message_keys
                        .split(' ')
                        .filter(|key| !key.is_empty())
                        .map(str::to_owned),
                );
            }
        }
        keys
    }
}

fn java_nullable(value: Option<&str>) -> String {
    value.unwrap_or("null").to_owned()
}

fn append_record(output: &mut String, fields: impl IntoIterator<Item = String>) {
    let mut first = true;
    for field in fields {
        if !first {
            output.push(TRACE_FIELD_SEPARATOR);
        }
        output.push_str(&field);
        first = false;
    }
    output.push(TRACE_RECORD_SEPARATOR);
}

#[cfg(test)]
fn message_type_from_ordinal(ordinal: i64) -> Option<MessageType> {
    MessageType::values()
        .into_iter()
        .find(|message_type| i64::from(message_type.ordinal()) == ordinal)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    fn fixtures() -> Value {
        serde_json::from_str(include_str!("../../../fixtures/tracing/trace-records.json")).unwrap()
    }

    #[test]
    fn every_trace_record_matches_invoked_java_551_encoder() {
        let fixtures = fixtures();
        for case in fixtures["cases"].as_array().unwrap() {
            let context = context_from_fixture(case);
            assert_eq!(
                context.encode(),
                case["encoded"].as_str().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                context.to_string(),
                case["context"]["text"].as_str().unwrap(),
                "{}",
                case["name"]
            );
            let keys = context.transfer_keys().into_iter().collect::<Vec<_>>();
            let expected_keys = case["transferKeys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(keys, expected_keys, "{}", case["name"]);
        }
    }

    #[test]
    fn trace_types_select_java_serialization_beans_and_keep_all_transfer_keys() {
        let fixtures = fixtures();
        let cases = fixtures["selectionCases"].as_array().unwrap();
        assert_eq!(cases.len(), 20);

        for case in cases {
            let context = context_from_fixture(case);
            let encoded = std::panic::catch_unwind(|| context.encode());
            if case["error"].is_null() {
                let encoded = encoded
                    .unwrap_or_else(|_| panic!("{} unexpectedly failed encoding", case["name"]));
                assert_eq!(
                    encoded,
                    case["encoded"].as_str().unwrap(),
                    "{}",
                    case["name"]
                );
                let row_count = encoded
                    .split(TRACE_RECORD_SEPARATOR)
                    .filter(|row| !row.is_empty())
                    .count();
                assert_eq!(
                    row_count,
                    case["rowCount"].as_u64().unwrap() as usize,
                    "{}",
                    case["name"]
                );
                let keys = context.transfer_keys().into_iter().collect::<Vec<_>>();
                let expected_keys = case["transferKeys"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|key| key.as_str().unwrap().to_owned())
                    .collect::<Vec<_>>();
                assert_eq!(keys, expected_keys, "{}", case["name"]);
            } else {
                assert!(encoded.is_err(), "{} should fail like Java", case["name"]);
            }
        }
    }

    #[test]
    fn trace_public_model_fields_and_enum_surfaces_match_java_551() {
        let fixtures = fixtures();
        let first = &fixtures["cases"][0];
        let context = context_from_fixture(first);
        let bean = &context.trace_beans.as_ref().unwrap()[0];
        assert_eq!(context.region_name.as_deref(), Some(""));
        assert_eq!(bean.store_time_millis, 0);
        assert_eq!(
            bean.client_host.as_deref(),
            first["context"]["traceBeans"][0]["clientHost"].as_str()
        );

        let default_bean = TraceBean::default();
        assert_eq!(default_bean.topic.as_deref(), Some(""));
        assert_eq!(default_bean.message_id.as_deref(), Some(""));
        assert_eq!(default_bean.offset_message_id.as_deref(), Some(""));
        assert_eq!(default_bean.tags.as_deref(), Some(""));
        assert_eq!(default_bean.keys.as_deref(), Some(""));
        assert_eq!(default_bean.store_time_millis, 0);
        assert_eq!(default_bean.store_host, default_bean.client_host);
        assert!(default_bean.client_host.as_deref().is_some_and(|host| {
            let host = host.trim_start_matches('[').trim_end_matches(']');
            host.parse::<std::net::IpAddr>().is_ok()
        }));
        assert!(bean.client_host.as_deref().is_some_and(|host| {
            let host = host.trim_start_matches('[').trim_end_matches(']');
            host.parse::<std::net::IpAddr>().is_ok()
        }));

        assert_eq!(
            TraceType::values().map(TraceType::java_name),
            ["Pub", "Recall", "SubBefore", "SubAfter", "EndTransaction"]
        );
        assert_eq!(
            MessageType::values().map(MessageType::java_name),
            [
                "Normal_Msg",
                "Trans_Msg_Half",
                "Trans_msg_Commit",
                "Delay_Msg",
                "Order_Msg"
            ]
        );
        assert_eq!(
            LocalTransactionState::values().map(LocalTransactionState::java_name),
            ["COMMIT_MESSAGE", "ROLLBACK_MESSAGE", "UNKNOW"]
        );
        assert_eq!(
            TraceType::value_of_name(Some("SubAfter")),
            Ok(TraceType::SubAfter)
        );
        assert_eq!(
            MessageType::value_of_name(Some("Trans_msg_Commit")),
            Ok(MessageType::TransactionCommit)
        );
        assert_eq!(
            LocalTransactionState::value_of_name(Some("UNKNOW")),
            Ok(LocalTransactionState::Unknown)
        );
    }

    #[test]
    fn trace_record_equality_follows_java_identity_contract() {
        let fixture = fixtures();
        let identity = &fixture["identityEquality"];
        assert!(!identity["equalValueBeansCompareEqual"].as_bool().unwrap());
        assert!(
            identity["sameBeanReferenceComparesEqual"]
                .as_bool()
                .unwrap()
        );
        assert!(
            !identity["equalValueContextsCompareEqual"]
                .as_bool()
                .unwrap()
        );
        assert!(
            identity["sameContextReferenceComparesEqual"]
                .as_bool()
                .unwrap()
        );
        assert!(
            identity["equalTimestampContextsCompareAsZero"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            identity["compareNullOtherException"],
            "NullPointerException"
        );
    }

    #[test]
    fn trace_context_default_constructor_matches_java_nullable_state() {
        let defaults = &fixtures()["defaults"];
        let context = TraceContext::default();
        let bean = TraceBean::default();

        assert_eq!(context.trace_type, None);
        assert!(context.trace_beans.is_none());
        assert!(context.timestamp_millis > 0);
        assert_eq!(context.region_id.as_deref(), defaults["regionId"].as_str());
        assert_eq!(
            context.region_name.as_deref(),
            defaults["regionName"].as_str()
        );
        assert_eq!(
            context.group_name.as_deref(),
            defaults["groupName"].as_str()
        );
        assert_eq!(
            context.cost_time_millis,
            defaults["costTime"].as_i64().unwrap() as i32
        );
        assert_eq!(context.success, defaults["success"].as_bool().unwrap());
        assert!(
            context
                .request_id
                .as_deref()
                .is_some_and(|id| !id.is_empty())
        );
        assert_eq!(
            context.context_code,
            defaults["contextCode"].as_i64().unwrap() as i32
        );
        assert_eq!(context.access_channel, None);
        assert_eq!(context.to_string(), defaults["text"].as_str().unwrap());
        assert!(defaults["requestIdPresent"].as_bool().unwrap());

        assert_eq!(bean.topic.as_deref(), defaults["beanTopic"].as_str());
        assert_eq!(
            bean.message_id.as_deref(),
            defaults["beanMessageId"].as_str()
        );
        assert_eq!(
            bean.offset_message_id.as_deref(),
            defaults["beanOffsetMessageId"].as_str()
        );
        assert_eq!(bean.tags.as_deref(), defaults["beanTags"].as_str());
        assert_eq!(bean.keys.as_deref(), defaults["beanKeys"].as_str());
        assert_eq!(bean.store_host, bean.client_host);
        assert!(defaults["beanStoreHostEqualsClientHost"].as_bool().unwrap());
        assert!(defaults["beanStoreHostIsAddress"].as_bool().unwrap());
        assert_eq!(
            bean.store_time_millis,
            defaults["beanStoreTime"].as_i64().unwrap()
        );
        assert_eq!(
            bean.retry_times,
            defaults["beanRetryTimes"].as_i64().unwrap() as i32
        );
        assert_eq!(
            bean.body_length,
            defaults["beanBodyLength"].as_i64().unwrap() as i32
        );
        assert_eq!(bean.message_type, None);
        assert_eq!(bean.transaction_state, None);
        assert_eq!(bean.transaction_id, None);
        assert!(!bean.from_transaction_check);
    }

    #[test]
    fn trace_encoder_preserves_java_failures_for_invalid_nullable_fields() {
        let invalid = &fixtures()["invalidInputThrows"];
        let throws =
            |context: &TraceContext| std::panic::catch_unwind(|| context.encode()).is_err();

        let default_context = TraceContext::default();
        assert_eq!(
            throws(&default_context),
            invalid["defaultContext"].as_bool().unwrap()
        );

        let pub_without_beans = TraceContext::new(TraceType::Pub);
        assert_eq!(
            throws(&pub_without_beans),
            invalid["pubWithoutBeans"].as_bool().unwrap()
        );

        let mut pub_without_message_type = TraceContext::new(TraceType::Pub);
        pub_without_message_type.trace_beans = Some(vec![TraceBean::default()]);
        assert_eq!(
            throws(&pub_without_message_type),
            invalid["pubWithoutMessageType"].as_bool().unwrap()
        );

        let sub_before_without_beans = TraceContext::new(TraceType::SubBefore);
        assert_eq!(
            throws(&sub_before_without_beans),
            invalid["subBeforeWithoutBeans"].as_bool().unwrap()
        );
        let mut sub_before_with_empty_beans = TraceContext::new(TraceType::SubBefore);
        sub_before_with_empty_beans.trace_beans = Some(Vec::new());
        assert_eq!(
            throws(&sub_before_with_empty_beans),
            invalid["subBeforeWithEmptyBeans"].as_bool().unwrap()
        );

        let mut sub_after_without_access_channel = TraceContext::new(TraceType::SubAfter);
        sub_after_without_access_channel.trace_beans = Some(vec![TraceBean::default()]);
        assert_eq!(
            throws(&sub_after_without_access_channel),
            invalid["subAfterWithoutAccessChannel"].as_bool().unwrap()
        );

        let mut transaction_without_state = TraceContext::new(TraceType::EndTransaction);
        transaction_without_state.trace_beans = Some(vec![TraceBean {
            message_type: Some(MessageType::Normal),
            ..TraceBean::default()
        }]);
        assert_eq!(
            throws(&transaction_without_state),
            invalid["transactionWithoutState"].as_bool().unwrap()
        );
    }

    #[test]
    fn trace_context_constructor_applies_java_defaults_and_unique_request_ids() {
        let mut first = TraceContext::new(TraceType::Pub);
        let second = TraceContext::new(TraceType::SubBefore);
        assert_eq!(first.region_id.as_deref(), Some(""));
        assert_eq!(first.region_name.as_deref(), Some(""));
        assert_eq!(first.group_name.as_deref(), Some(""));
        assert_eq!(first.cost_time_millis, 0);
        assert!(first.success);
        assert_eq!(first.context_code, 0);
        assert_eq!(first.access_channel, None);
        assert_eq!(first.trace_type, Some(TraceType::Pub));
        assert!(first.trace_beans.is_none());
        assert!(first.timestamp_millis > 0);
        let first_id = first.request_id.as_deref().unwrap();
        let second_id = second.request_id.as_deref().unwrap();
        assert_ne!(first_id, second_id);
        assert!(first_id.len() == 32 || first_id.len() == 56);
        assert!(first_id.bytes().all(|byte| byte.is_ascii_hexdigit()));

        first.timestamp_millis = 1;
        let mut later = second;
        later.timestamp_millis = 2;
        assert_eq!(first.compare_to(&later), Ordering::Less);
        assert_eq!(later.compare_to(&first), Ordering::Greater);
        assert_eq!(
            first.java_compare_to_nullable(None),
            Err(TraceContextCompareError::NullOther)
        );
    }

    fn context_from_fixture(case: &Value) -> TraceContext {
        let value = &case["context"];
        let trace_type = match value["traceType"].as_str().unwrap() {
            "Pub" => TraceType::Pub,
            "Recall" => TraceType::Recall,
            "SubBefore" => TraceType::SubBefore,
            "SubAfter" => TraceType::SubAfter,
            "EndTransaction" => TraceType::EndTransaction,
            other => panic!("unknown trace type {other}"),
        };
        let beans = value["traceBeans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bean| TraceBean {
                topic: bean["topic"].as_str().map(str::to_owned),
                message_id: bean["messageId"].as_str().map(str::to_owned),
                offset_message_id: bean["offsetMessageId"].as_str().map(str::to_owned),
                tags: bean["tags"].as_str().map(str::to_owned),
                keys: bean["keys"].as_str().map(str::to_owned),
                store_host: bean["storeHost"].as_str().map(str::to_owned),
                client_host: bean["clientHost"].as_str().map(str::to_owned),
                store_time_millis: bean["storeTime"].as_i64().unwrap_or_default(),
                body_length: bean["bodyLength"].as_i64().unwrap_or_default() as i32,
                retry_times: bean["retryTimes"].as_i64().unwrap_or_default() as i32,
                message_type: bean["messageTypeOrdinal"]
                    .as_i64()
                    .and_then(message_type_from_ordinal),
                transaction_id: bean["transactionId"].as_str().map(str::to_owned),
                transaction_state: bean["transactionState"]
                    .as_str()
                    .and_then(|name| LocalTransactionState::value_of_name(Some(name)).ok()),
                from_transaction_check: bean["fromTransactionCheck"].as_bool().unwrap_or_default(),
            })
            .collect();
        TraceContext {
            trace_type: Some(trace_type),
            timestamp_millis: value["timestampMillis"].as_i64().unwrap(),
            region_id: value["regionId"].as_str().map(str::to_owned),
            region_name: value["regionName"].as_str().map(str::to_owned),
            group_name: value["groupName"].as_str().map(str::to_owned),
            cost_time_millis: value["costTimeMillis"].as_i64().unwrap_or_default() as i32,
            success: value["success"].as_bool().unwrap_or_default(),
            request_id: value["requestId"].as_str().map(str::to_owned),
            context_code: value["contextCode"].as_i64().unwrap_or_default() as i32,
            access_channel: match value["accessChannel"].as_str() {
                Some("LOCAL") => Some(AccessChannel::Local),
                Some("CLOUD") => Some(AccessChannel::Cloud),
                _ => None,
            },
            trace_beans: Some(beans),
        }
    }
}
