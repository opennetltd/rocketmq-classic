use std::hash::{Hash, Hasher};

use rocketmq_classic_common::message::TopicFilterType;

use super::{
    CommandHeader, HeaderMap, ProtocolError, java_boolean, optional_i32, optional_i32_with_mode,
    optional_i64, optional_i64_with_mode, optional_java_boolean, optional_string, required_i32,
    required_i64, required_string,
};
use crate::codes::JavaEnumLookupError;

macro_rules! impl_rpc_request_header_semantics {
    ($($header:ty),+ $(,)?) => {
        $(
            impl PartialEq for $header {
                fn eq(&self, other: &Self) -> bool {
                    self.namespace == other.namespace
                        && self.namespaced == other.namespaced
                        && self.broker_name == other.broker_name
                        && self.oneway == other.oneway
                }
            }

            impl Eq for $header {}

            impl Hash for $header {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    self.namespace.hash(state);
                    self.namespaced.hash(state);
                    self.broker_name.hash(state);
                    self.oneway.hash(state);
                }
            }
        )+
    };
}

macro_rules! impl_rpc_request_header_accessors {
    ($($header:ty),+ $(,)?) => {
        $(
            impl $header {
                pub fn get_namespace(&self) -> Option<&str> {
                    self.namespace.as_deref()
                }

                pub fn set_namespace(&mut self, value: Option<String>) {
                    self.namespace = value;
                }

                pub fn get_namespaced(&self) -> Option<bool> {
                    self.namespaced
                }

                pub fn set_namespaced(&mut self, value: Option<bool>) {
                    self.namespaced = value;
                }

                pub fn get_broker_name(&self) -> Option<&str> {
                    self.broker_name.as_deref()
                }

                pub fn set_broker_name(&mut self, value: Option<String>) {
                    self.broker_name = value;
                }

                #[deprecated(note = "use get_broker_name")]
                pub fn get_bname(&self) -> Option<&str> {
                    self.get_broker_name()
                }

                #[deprecated(note = "use set_broker_name")]
                pub fn set_bname(&mut self, value: Option<String>) {
                    self.set_broker_name(value);
                }

                pub fn get_oneway(&self) -> Option<bool> {
                    self.oneway
                }

                pub fn set_oneway(&mut self, value: Option<bool>) {
                    self.oneway = value;
                }
            }
        )+
    };
}

macro_rules! impl_topic_request_header_accessors {
    ($($header:ty),+ $(,)?) => {
        impl_rpc_request_header_accessors!($($header),+);
        $(
            impl $header {
                pub fn get_lo(&self) -> Option<bool> {
                    self.logical
                }

                pub fn set_lo(&mut self, value: Option<bool>) {
                    self.logical = value;
                }
            }
        )+
    };
}

macro_rules! impl_header_field_accessors {
    ($header:ty, optional_string, $field:ident, [$($getter:ident),* $(,)?], [$($setter:ident),* $(,)?]) => {
        impl $header {
            $(
                pub fn $getter(&self) -> Option<&str> {
                    self.$field.as_deref()
                }
            )*
            $(
                pub fn $setter(&mut self, value: Option<String>) {
                    self.$field = value;
                }
            )*
        }
    };
    ($header:ty, string, $field:ident, [$($getter:ident),* $(,)?], [$($setter:ident),* $(,)?]) => {
        impl $header {
            $(
                pub fn $getter(&self) -> &str {
                    &self.$field
                }
            )*
            $(
                pub fn $setter(&mut self, value: String) {
                    self.$field = value;
                }
            )*
        }
    };
    ($header:ty, optional_copy($value:ty), $field:ident, [$($getter:ident),* $(,)?], [$($setter:ident),* $(,)?]) => {
        impl $header {
            $(
                pub fn $getter(&self) -> Option<$value> {
                    self.$field
                }
            )*
            $(
                pub fn $setter(&mut self, value: Option<$value>) {
                    self.$field = value;
                }
            )*
        }
    };
    ($header:ty, copy($value:ty), $field:ident, [$($getter:ident),* $(,)?], [$($setter:ident),* $(,)?]) => {
        impl $header {
            $(
                pub fn $getter(&self) -> $value {
                    self.$field
                }
            )*
            $(
                pub fn $setter(&mut self, value: $value) {
                    self.$field = value;
                }
            )*
        }
    };
}

fn put_optional<T: ToString>(fields: &mut HeaderMap, name: &str, value: Option<T>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value.to_string());
    }
}

fn java_pop_timeout_too_much(now_millis: i64, born_time: i64, poll_time: i64) -> bool {
    now_millis.wrapping_sub(born_time).wrapping_sub(poll_time) > 500
}

/// Pinned Java `org.apache.rocketmq.common.BoundaryType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryType {
    Lower,
    Upper,
}

impl BoundaryType {
    pub const fn values() -> [Self; 2] {
        [Self::Lower, Self::Upper]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("LOWER") => Ok(Self::Lower),
            Some("UPPER") => Ok(Self::Upper),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.common.BoundaryType",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
        }
    }

    /// Java `BoundaryType.getName()` uses lowercase values for broker semantics.
    pub const fn get_name(self) -> &'static str {
        match self {
            Self::Lower => "lower",
            Self::Upper => "upper",
        }
    }

    /// Java `BoundaryType.getType` recognizes the case-insensitive `upper`
    /// name and falls back to LOWER for null or any other input.
    pub fn get_type(name: Option<&str>) -> Self {
        if name.is_some_and(|name| name.eq_ignore_ascii_case("upper")) {
            Self::Upper
        } else {
            Self::Lower
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Lower => 0,
            Self::Upper => 1,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl Default for BoundaryType {
    fn default() -> Self {
        Self::Lower
    }
}

impl std::fmt::Display for BoundaryType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

/// The three configuration families accepted by Java's
/// `ExportRocksDBConfigToJsonRequestHeader.ConfigType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExportRocksDBConfigType {
    Topics,
    SubscriptionGroups,
    ConsumerOffsets,
}

impl ExportRocksDBConfigType {
    pub const fn values() -> &'static [Self] {
        &[
            Self::Topics,
            Self::SubscriptionGroups,
            Self::ConsumerOffsets,
        ]
    }

    pub fn value_of(name: &str) -> Result<Self, ProtocolError> {
        match name {
            "TOPICS" => Ok(Self::Topics),
            "SUBSCRIPTION_GROUPS" => Ok(Self::SubscriptionGroups),
            "CONSUMER_OFFSETS" => Ok(Self::ConsumerOffsets),
            _ => Err(ProtocolError::InvalidField {
                field: "configType",
                reason: format!("Unknown enum name: {name}"),
            }),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Topics => "TOPICS",
            Self::SubscriptionGroups => "SUBSCRIPTION_GROUPS",
            Self::ConsumerOffsets => "CONSUMER_OFFSETS",
        }
    }

    pub const fn ordinal(self) -> usize {
        match self {
            Self::Topics => 0,
            Self::SubscriptionGroups => 1,
            Self::ConsumerOffsets => 2,
        }
    }

    /// Java `Enum.toString()` returns the enum constant name.
    pub fn java_to_string(self) -> &'static str {
        self.name()
    }

    pub fn get_config_type_by_name(name: &str) -> Result<Self, ProtocolError> {
        match name.trim().to_ascii_lowercase().as_str() {
            "topics" => Ok(Self::Topics),
            "subscriptiongroups" => Ok(Self::SubscriptionGroups),
            "consumeroffsets" => Ok(Self::ConsumerOffsets),
            _ => Err(ProtocolError::InvalidField {
                field: "configType",
                reason: format!("Unknown config type: {name}"),
            }),
        }
    }

    pub fn from_string(value: &str) -> Result<Vec<Self>, ProtocolError> {
        value
            .split(';')
            .filter(|name| !name.is_empty())
            .map(Self::get_config_type_by_name)
            .collect()
    }

    /// Formats Java's semicolon-separated list, including its trailing
    /// separator after every enum value.
    pub fn to_string(config_types: &[Self]) -> String {
        let mut result = String::new();
        for config_type in config_types {
            result.push_str(config_type.get_type_name());
            result.push(';');
        }
        result
    }

    pub fn get_type_name(self) -> &'static str {
        match self {
            Self::Topics => "topics",
            Self::SubscriptionGroups => "subscriptionGroups",
            Self::ConsumerOffsets => "consumerOffsets",
        }
    }
}

fn java_nullable_string(value: Option<&str>) -> String {
    value.unwrap_or("null").to_owned()
}

fn java_nullable_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "null",
    }
}

fn java_boolean_hash_code(value: bool) -> i32 {
    if value { 1231 } else { 1237 }
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

// Java 5.5.1 typed-header public field accessors.

// Keep private identity state and deterministic timeout helpers in this scope.
/// Java `GetTopicConfigRequestHeader`; annotated `topic` remains nullable
/// because `checkFields()` is a no-op, and `lo` is inherited from
/// `TopicRequestHeader`.
#[derive(Debug, Clone, Default)]
pub struct GetTopicConfigRequestHeader {
    topic: Option<String>,
    namespace: Option<String>,
    namespaced: Option<bool>,
    broker_name: Option<String>,
    oneway: Option<bool>,
    logical: Option<bool>,
}

/// Java 5.5.1's Lite subscription control RPC header. It declares no payload
/// fields and inherits namespace and routing fields from `RpcRequestHeader`.
#[derive(Debug, Clone, Default)]
pub struct LiteSubscriptionCtlRequestHeader {
    namespace: Option<String>,
    namespaced: Option<bool>,
    broker_name: Option<String>,
    oneway: Option<bool>,
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::NotifyUnsubscribeLiteRequestHeader;
/// let _ = NotifyUnsubscribeLiteRequestHeader::default().lite_topic;
/// ```
///
/// Java equality for this header compares the inherited RPC routing fields;
/// the payload fields do not participate in equality or hashing.
#[derive(Debug, Clone, Default)]
pub struct NotifyUnsubscribeLiteRequestHeader {
    pub(crate) lite_topic: String,
    pub(crate) consumer_group: String,
    pub(crate) client_id: String,
    namespace: Option<String>,
    namespaced: Option<bool>,
    broker_name: Option<String>,
    oneway: Option<bool>,
}

/// Java `QueryConsumeQueueRequestHeader`. Its primitive pagination values keep
/// their zero defaults, while topic/group and inherited RPC fields stay nullable.
#[derive(Debug, Clone, Default)]
pub struct QueryConsumeQueueRequestHeader {
    topic: Option<String>,
    queue_id: i32,
    index: i64,
    count: i32,
    consumer_group: Option<String>,
    broker_name: Option<String>,
    namespace: Option<String>,
    namespaced: Option<bool>,
    logical: Option<bool>,
    oneway: Option<bool>,
}

/// Pinned Java `QueryCorrectionOffsetHeader` for `QUERY_CORRECTION_OFFSET`.
/// Java's `checkFields()` is a no-op, so the compared group, topic, filter
/// groups, and inherited request fields all remain nullable.
#[derive(Debug, Clone, Default)]
pub struct QueryCorrectionOffsetRequestHeader {
    namespace: Option<String>,
    namespaced: Option<bool>,
    broker_name: Option<String>,
    oneway: Option<bool>,
    topic: Option<String>,
    logical: Option<bool>,
    filter_groups: Option<String>,
    compare_group: Option<String>,
}

/// Pinned Java `UpdateGroupForbiddenRequestHeader`. Its `checkFields()` is a
/// no-op despite non-null annotations, so group and topic remain nullable at
/// this request boundary.
#[derive(Debug, Clone, Default)]
pub struct UpdateGroupForbiddenRequestHeader {
    namespace: Option<String>,
    namespaced: Option<bool>,
    broker_name: Option<String>,
    oneway: Option<bool>,
    topic: Option<String>,
    logical: Option<bool>,
    group: Option<String>,
    readable: Option<bool>,
}

impl PopMessageRequestHeader {
    fn is_timeout_too_much_at(&self, now_millis: i64) -> bool {
        java_pop_timeout_too_much(now_millis, self.born_time, self.poll_time)
    }
}

impl PopLiteMessageRequestHeader {
    fn is_timeout_too_much_at(&self, now_millis: i64) -> bool {
        java_pop_timeout_too_much(now_millis, self.born_time, self.poll_time)
    }
}

mod requests;
mod responses;
pub use requests::*;
pub use responses::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_header_identity_matches_invoked_java() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/protocol/header-response-identity.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["clientArtifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(fixture["serverContacted"], false);
        assert_eq!(
            fixture["recallMessageIdNullability"]["defaultMsgIdIsNull"],
            true
        );
        assert_eq!(
            fixture["recallMessageIdNullability"]["msgIdAfterNullSetterIsNull"],
            true
        );
        assert_eq!(
            fixture["recallMessageIdNullability"]["nullExtFieldsDecodeReturnsNullMsgId"],
            true
        );
        assert_eq!(
            fixture["recallMessageIdNullability"]["emptyExtFieldsMissingMsgIdRejected"],
            false
        );
        let cases = fixture["identityCases"].as_object().unwrap();
        assert_eq!(cases.len(), 22);
        let api_inventories = fixture["apiInventories"].as_object().unwrap();
        assert_eq!(api_inventories.len(), 22);
        for (rust_name, inventory) in api_inventories {
            assert_eq!(
                inventory["publicFields"],
                serde_json::json!([]),
                "{rust_name}"
            );
        }
        let recall_methods =
            api_inventories["RecallMessageResponseHeader"]["callablePublicMethods"]
                .as_array()
                .unwrap();
        assert!(recall_methods.iter().any(|method| {
            method
                .as_str()
                .unwrap()
                .ends_with("RecallMessageResponseHeader.getMsgId()")
        }));
        assert!(recall_methods.iter().any(|method| {
            method
                .as_str()
                .unwrap()
                .ends_with("RecallMessageResponseHeader.setMsgId(java.lang.String)")
        }));
        for (rust_name, identity) in cases {
            let expected_java_class = match rust_name.as_str() {
                "AddWritePermOfBrokerResponseHeader" => {
                    format!("org.apache.rocketmq.remoting.protocol.header.namesrv.{rust_name}")
                }
                "GetKvConfigResponseHeader" => {
                    "org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader"
                        .to_owned()
                }
                "WipeWritePermOfBrokerResponseHeader" => {
                    format!("org.apache.rocketmq.remoting.protocol.header.namesrv.{rust_name}")
                }
                _ => format!("org.apache.rocketmq.remoting.protocol.header.{rust_name}"),
            };
            assert_eq!(identity["javaClass"], expected_java_class);
            assert_eq!(
                identity["equalsDeclaredBy"], "java.lang.Object",
                "{rust_name}"
            );
            assert_eq!(
                identity["hashCodeDeclaredBy"], "java.lang.Object",
                "{rust_name}"
            );
            assert_eq!(identity["sameObjectEqualsItself"], true, "{rust_name}");
            assert_eq!(
                identity["distinctDefaultObjectsEquals"], false,
                "{rust_name}"
            );
        }
    }

    fn request_header_hash<T: std::hash::Hash>(value: &T) -> u64 {
        use std::hash::Hasher;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    macro_rules! check_rpc_request_header {
        ($header:ty, $broker_field:ident, $prepare:expr, $payload_change:expr, $decode_fast:expr) => {{
            let mut header = <$header>::default();
            header.namespace = Some("namespace".into());
            header.namespaced = Some(true);
            header.$broker_field = Some("broker".into());
            header.oneway = Some(false);
            ($prepare)(&mut header);

            let mut payload_change = header.clone();
            ($payload_change)(&mut payload_change);
            assert_eq!(
                header,
                payload_change,
                "{} payload equality",
                stringify!($header)
            );
            assert_eq!(
                request_header_hash(&header),
                request_header_hash(&payload_change),
                "{} payload hash",
                stringify!($header)
            );

            let mut fields = HeaderMap::new();
            header.encode_fields(&mut fields);
            assert_eq!(fields.get("ns").map(String::as_str), Some("namespace"));
            assert_eq!(fields.get("nsd").map(String::as_str), Some("true"));
            assert_eq!(fields.get("bname").map(String::as_str), Some("broker"));
            assert_eq!(fields.get("oway").map(String::as_str), Some("false"));
            let decoded = <$header>::decode_fields_with_mode(&fields, $decode_fast).unwrap();
            assert_eq!(
                header,
                decoded,
                "{} base-field round trip",
                stringify!($header)
            );

            let mut changed_namespace = header.clone();
            changed_namespace.namespace = Some("other-namespace".into());
            assert_ne!(header, changed_namespace, "{} ns", stringify!($header));
            let mut changed_namespaced = header.clone();
            changed_namespaced.namespaced = Some(false);
            assert_ne!(header, changed_namespaced, "{} nsd", stringify!($header));
            let mut changed_broker = header.clone();
            changed_broker.$broker_field = Some("other-broker".into());
            assert_ne!(header, changed_broker, "{} bname", stringify!($header));
            let mut changed_oneway = header.clone();
            changed_oneway.oneway = Some(true);
            assert_ne!(header, changed_oneway, "{} oway", stringify!($header));
        }};
        ($header:ty, $broker_field:ident, $prepare:expr, $payload_change:expr) => {
            check_rpc_request_header!($header, $broker_field, $prepare, $payload_change, true);
        };
    }

    #[test]
    fn request_header_equality_and_hash_match_rpc_base_fields() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/protocol/request-header-identity.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["clientArtifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(fixture["serverContacted"], false);
        let cases = fixture["identityCases"].as_object().unwrap();
        assert_eq!(cases.len(), 50);
        let rpc_semantics_types: std::collections::BTreeSet<_> = include_str!("basic/requests.rs")
            .split_once("impl_rpc_request_header_semantics!(")
            .unwrap()
            .1
            .split_once(");")
            .unwrap()
            .0
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        let fixture_rpc_types: std::collections::BTreeSet<_> = cases
            .keys()
            .filter(|name| name.as_str() != "ConsumeMessageDirectlyResultRequestHeader")
            .cloned()
            .collect();
        assert_eq!(
            fixture_rpc_types, rpc_semantics_types,
            "Java identity/API inventory must cover every Rust RpcRequestHeader equality mapping"
        );
        for (rust_name, identity) in cases {
            let java_class = if rust_name == "GetRouteInfoRequestHeader" {
                "org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader"
                    .to_owned()
            } else if rust_name == "DeleteTopicFromNamesrvRequestHeader" {
                "org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader"
                .to_owned()
            } else if rust_name == "SendMessageRequestHeaderV1" {
                "org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader".to_owned()
            } else if rust_name == "QueryCorrectionOffsetRequestHeader" {
                "org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader"
                    .to_owned()
            } else {
                format!("org.apache.rocketmq.remoting.protocol.header.{rust_name}")
            };
            assert_eq!(identity["javaClass"], java_class, "{rust_name}");
            assert_eq!(
                identity["equalsDeclaredBy"], "org.apache.rocketmq.remoting.rpc.RpcRequestHeader",
                "{rust_name}"
            );
            assert_eq!(
                identity["hashCodeDeclaredBy"], "org.apache.rocketmq.remoting.rpc.RpcRequestHeader",
                "{rust_name}"
            );
            assert_eq!(identity["sameObjectEqualsItself"], true, "{rust_name}");
            assert_eq!(
                identity["distinctDefaultObjectsEquals"], true,
                "{rust_name}"
            );
            assert_eq!(identity["equalBaseFieldsEquals"], true, "{rust_name}");
            assert_eq!(identity["equalBaseFieldsHashEqual"], true, "{rust_name}");
            assert_eq!(identity["payloadDifferenceIgnored"], true, "{rust_name}");
            assert_eq!(
                identity["payloadDifferenceTested"],
                !matches!(
                    rust_name.as_str(),
                    "LockBatchMqRequestHeader"
                        | "LiteSubscriptionCtlRequestHeader"
                        | "UnlockBatchMqRequestHeader"
                        | "HeartbeatRequestHeader"
                ),
                "{rust_name}"
            );
            for field in ["ns", "nsd", "bname", "oway"] {
                assert_eq!(
                    identity["baseFieldDifferenceEquals"][field], false,
                    "{rust_name}.{field}"
                );
            }
        }

        check_rpc_request_header!(
            QueryConsumerOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryConsumerOffsetRequestHeader| {
                h.consumer_group = Some("payload".into())
            }
        );
        check_rpc_request_header!(
            HeartbeatRequestHeader,
            broker_name,
            |_| {},
            |_: &mut HeartbeatRequestHeader| {}
        );
        check_rpc_request_header!(
            LiteSubscriptionCtlRequestHeader,
            broker_name,
            |_| {},
            |_: &mut LiteSubscriptionCtlRequestHeader| {}
        );
        check_rpc_request_header!(
            NotifyUnsubscribeLiteRequestHeader,
            broker_name,
            |_| {},
            |header: &mut NotifyUnsubscribeLiteRequestHeader| {
                header.lite_topic = "payload".into()
            }
        );
        check_rpc_request_header!(
            CloneGroupOffsetRequestHeader,
            broker_name,
            |_| {},
            |header: &mut CloneGroupOffsetRequestHeader| {
                header.src_group = Some("payload".into())
            }
        );
        check_rpc_request_header!(
            GetSubscriptionGroupConfigRequestHeader,
            broker_name,
            |_| {},
            |header: &mut GetSubscriptionGroupConfigRequestHeader| {
                header.group = Some("payload".into())
            }
        );
        check_rpc_request_header!(
            GetTopicConfigRequestHeader,
            broker_name,
            |_| {},
            |header: &mut GetTopicConfigRequestHeader| header.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            QueryConsumeQueueRequestHeader,
            broker_name,
            |_| {},
            |header: &mut QueryConsumeQueueRequestHeader| { header.topic = Some("payload".into()) }
        );
        check_rpc_request_header!(
            QueryCorrectionOffsetRequestHeader,
            broker_name,
            |_| {},
            |header: &mut QueryCorrectionOffsetRequestHeader| {
                header.topic = Some("payload".into())
            }
        );
        check_rpc_request_header!(
            UpdateGroupForbiddenRequestHeader,
            broker_name,
            |_| {},
            |header: &mut UpdateGroupForbiddenRequestHeader| {
                header.group = Some("payload".into())
            }
        );
        check_rpc_request_header!(
            UpdateConsumerOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut UpdateConsumerOffsetRequestHeader| h.commit_offset = Some(17)
        );
        check_rpc_request_header!(
            QueryConsumeTimeSpanRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryConsumeTimeSpanRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            SearchOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut SearchOffsetRequestHeader| h.timestamp = Some(17)
        );
        check_rpc_request_header!(
            GetMaxOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetMaxOffsetRequestHeader| h.committed = false
        );
        check_rpc_request_header!(
            GetMinOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetMinOffsetRequestHeader| h.queue_id = Some(17)
        );
        check_rpc_request_header!(
            GetEarliestMsgStoretimeRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetEarliestMsgStoretimeRequestHeader| h.queue_id = Some(17)
        );
        check_rpc_request_header!(
            PullMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PullMessageRequestHeader| h.consumer_group = Some("payload".into())
        );
        check_rpc_request_header!(
            UnregisterClientRequestHeader,
            broker_name,
            |_| {},
            |h: &mut UnregisterClientRequestHeader| h.client_id = "payload".into()
        );
        check_rpc_request_header!(
            GetRouteInfoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetRouteInfoRequestHeader| h.topic = "payload".into()
        );
        check_rpc_request_header!(
            CreateTopicRequestHeader,
            broker_name,
            |h: &mut CreateTopicRequestHeader| h.topic_filter_type = Some("SINGLE_TAG".into()),
            |h: &mut CreateTopicRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            DeleteTopicRequestHeader,
            broker_name,
            |_| {},
            |h: &mut DeleteTopicRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            DeleteTopicFromNamesrvRequestHeader,
            broker_name,
            |_| {},
            |h: &mut DeleteTopicFromNamesrvRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            DeleteSubscriptionGroupRequestHeader,
            broker_name,
            |_| {},
            |h: &mut DeleteSubscriptionGroupRequestHeader| h.group_name = Some("payload".into())
        );
        check_rpc_request_header!(
            QueryMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryMessageRequestHeader| h.key = Some("payload".into())
        );
        check_rpc_request_header!(
            QueryTopicsByConsumerRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryTopicsByConsumerRequestHeader| h.group = Some("payload".into())
        );
        check_rpc_request_header!(
            QuerySubscriptionByConsumerRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QuerySubscriptionByConsumerRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            QueryTopicConsumeByWhoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryTopicConsumeByWhoRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            GetTopicStatsInfoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetTopicStatsInfoRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            GetConsumerListByGroupRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumerListByGroupRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            LockBatchMqRequestHeader,
            broker_name,
            |_| {},
            |_: &mut LockBatchMqRequestHeader| {}
        );
        check_rpc_request_header!(
            UnlockBatchMqRequestHeader,
            broker_name,
            |_| {},
            |_: &mut UnlockBatchMqRequestHeader| {}
        );
        check_rpc_request_header!(
            ConsumerSendMsgBackRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ConsumerSendMsgBackRequestHeader| h.group = "payload".into()
        );
        check_rpc_request_header!(
            RecallMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut RecallMessageRequestHeader| h.recall_handle = "payload".into()
        );
        check_rpc_request_header!(
            EndTransactionRequestHeader,
            broker_name,
            |h: &mut EndTransactionRequestHeader| h.commit_or_rollback = Some(8),
            |h: &mut EndTransactionRequestHeader| h.transaction_id = Some("payload".into())
        );
        check_rpc_request_header!(
            CheckTransactionStateRequestHeader,
            broker_name,
            |_| {},
            |h: &mut CheckTransactionStateRequestHeader| h.transaction_id = Some("payload".into())
        );
        check_rpc_request_header!(
            ReplyMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ReplyMessageRequestHeader| h.producer_group = "payload".into()
        );
        check_rpc_request_header!(
            NotificationRequestHeader,
            broker_name,
            |_| {},
            |h: &mut NotificationRequestHeader| h.attempt_id = Some("payload".into())
        );
        check_rpc_request_header!(
            ResetOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ResetOffsetRequestHeader| h.group = "payload".into()
        );
        check_rpc_request_header!(
            GetConsumerStatusRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumerStatusRequestHeader| h.topic = "payload".into()
        );
        check_rpc_request_header!(
            GetConsumerConnectionListRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumerConnectionListRequestHeader| {
                h.consumer_group = Some("payload".into())
            }
        );
        check_rpc_request_header!(
            GetConsumeStatsRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumeStatsRequestHeader| { h.consumer_group = Some("payload".into()) }
        );
        check_rpc_request_header!(
            GetConsumerRunningInfoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumerRunningInfoRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            ConsumeMessageDirectlyResultRequestHeader,
            rpc_broker_name,
            |_| {},
            |h: &mut ConsumeMessageDirectlyResultRequestHeader| h.broker_name =
                Some("payload".into())
        );
        check_rpc_request_header!(
            PopMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PopMessageRequestHeader| h.consumer_group = Some("payload".into())
        );
        check_rpc_request_header!(
            PopLiteMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PopLiteMessageRequestHeader| h.client_id = Some("payload".into())
        );
        check_rpc_request_header!(
            PopLiteMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PopLiteMessageRequestHeader| h.client_id = Some("payload".into())
        );
        check_rpc_request_header!(
            AckMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut AckMessageRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            ChangeInvisibleTimeRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ChangeInvisibleTimeRequestHeader| h.consumer_group = Some("payload".into())
        );
        check_rpc_request_header!(
            PollingInfoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PollingInfoRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            SendMessageRequestHeaderV1,
            broker_name,
            |_| {},
            |h: &mut SendMessageRequestHeaderV1| h.topic = "payload".into()
        );
        check_rpc_request_header!(
            SendMessageRequestHeaderV2,
            broker_name,
            |_| {},
            |h: &mut SendMessageRequestHeaderV2| h.b = Some("payload".into()),
            false
        );
    }

    #[test]
    fn pop_lite_timeout_method_matches_java_arithmetic_and_threshold() {
        let header = PopLiteMessageRequestHeader {
            born_time: 100,
            poll_time: 200,
            ..PopLiteMessageRequestHeader::default()
        };
        assert!(!header.is_timeout_too_much_at(800));
        assert!(header.is_timeout_too_much_at(801));

        let wrapped = PopLiteMessageRequestHeader {
            born_time: 1,
            ..PopLiteMessageRequestHeader::default()
        };
        assert!(wrapped.is_timeout_too_much_at(i64::MIN));

        let regular = PopMessageRequestHeader {
            born_time: 100,
            poll_time: 200,
            ..PopMessageRequestHeader::default()
        };
        assert!(!regular.is_timeout_too_much_at(800));
        assert!(regular.is_timeout_too_much_at(801));
    }

    #[test]
    fn query_offset_preserves_unknown_fields_and_optional_false() {
        let raw = HeaderMap::from([
            ("consumerGroup".into(), "workers".into()),
            ("topic".into(), "orders".into()),
            ("queueId".into(), "0".into()),
            ("setZeroIfNotFound".into(), "false".into()),
            ("futureField".into(), "keep".into()),
        ]);
        let mut updated = raw.clone();
        let mut typed = QueryConsumerOffsetRequestHeader::decode_fields(&raw).unwrap();
        typed.set_zero_if_not_found = Some(true);
        typed.encode_fields(&mut updated);

        assert_eq!(updated["setZeroIfNotFound"], "true");
        assert_eq!(updated["futureField"], "keep");
        assert_eq!(raw["setZeroIfNotFound"], "false");
    }

    #[test]
    fn send_v1_and_v2_round_trip_compact_fields() {
        let v1 = SendMessageRequestHeaderV1 {
            producer_group: Some("producer".into()),
            topic: "orders".into(),
            default_topic: "TBW102".into(),
            default_topic_queue_nums: 4,
            queue_id: 2,
            sys_flag: 1,
            born_timestamp: i64::MAX,
            flag: 7,
            properties: Some("TAGS\u{1}blue".into()),
            reconsume_times: Some(0),
            unit_mode: Some(true),
            max_reconsume_times: Some(16),
            batch: Some(false),
            ..SendMessageRequestHeaderV1::default()
        };
        let v2 = SendMessageRequestHeaderV2::from_v1(&v1);
        let mut v2_fields = HeaderMap::new();
        v2.encode_fields(&mut v2_fields);
        let decoded = SendMessageRequestHeaderV2::decode_fields(&v2_fields).unwrap();
        assert_eq!(decoded, v2);
        assert_eq!(v2_fields["a"], "producer");
        assert!(!v2_fields.contains_key("n"));
    }

    #[test]
    fn nullable_producer_group_is_omitted_from_send_headers() {
        let header = SendMessageRequestHeaderV1 {
            producer_group: None,
            broker_name: Some("short-broker".into()),
            namespace: Some("tenant".into()),
            namespaced: Some(true),
            logical: Some(true),
            oneway: Some(false),
            ..SendMessageRequestHeaderV1::default()
        };
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        assert!(!fields.contains_key("producerGroup"));
        assert_eq!(
            SendMessageRequestHeaderV1::decode_fields(&fields),
            Ok(header.clone())
        );

        let compact = SendMessageRequestHeaderV2::from_v1(&header);
        assert_eq!(compact.n.as_deref(), Some("short-broker"));
        assert_eq!(compact.namespace, None);
        assert_eq!(compact.namespaced, None);
        assert_eq!(compact.logical, None);
        assert_eq!(compact.broker_name, None);
        assert_eq!(compact.oneway, None);
        let mut compact_fields = HeaderMap::new();
        compact.encode_fields(&mut compact_fields);
        assert!(!compact_fields.contains_key("a"));
        assert_eq!(
            compact_fields.get("n").map(String::as_str),
            Some("short-broker")
        );
        assert!(!compact_fields.contains_key("bname"));
        assert!(!compact_fields.contains_key("ns"));
        assert!(!compact_fields.contains_key("nsd"));
        assert!(!compact_fields.contains_key("lo"));
        assert!(!compact_fields.contains_key("oway"));
        assert_eq!(
            SendMessageRequestHeaderV2::decode_fields(&compact_fields).unwrap(),
            compact
        );
    }

    #[test]
    fn pull_headers_round_trip_required_and_optional_fields() {
        let header = PullMessageRequestHeader {
            consumer_group: Some("workers".into()),
            topic: Some("orders".into()),
            queue_id: Some(-1),
            queue_offset: Some(i64::MIN),
            max_msg_nums: Some(32),
            sys_flag: Some(0),
            commit_offset: Some(0),
            suspend_timeout_millis: Some(15_000),
            sub_version: Some(i64::MAX),
            proxy_forward_client_id: Some("proxy".into()),
            broker_name: Some("broker-a".into()),
            ..PullMessageRequestHeader::default()
        };
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        assert_eq!(PullMessageRequestHeader::decode_fields(&fields), Ok(header));
        assert!(!fields.contains_key("subscription"));
        assert_eq!(fields["proxyFrowardClientId"], "proxy");
        assert_eq!(fields["bname"], "broker-a");
    }

    #[test]
    fn pull_request_header_round_trips_java_lite_topic_field() {
        let mut fields = HeaderMap::new();
        fields.insert("consumerGroup".into(), "workers".into());
        fields.insert("topic".into(), "orders".into());
        fields.insert("queueId".into(), "1".into());
        fields.insert("queueOffset".into(), "9".into());
        fields.insert("maxMsgNums".into(), "32".into());
        fields.insert("sysFlag".into(), "0".into());
        fields.insert("commitOffset".into(), "8".into());
        fields.insert("suspendTimeoutMillis".into(), "15000".into());
        fields.insert("subVersion".into(), "1".into());
        fields.insert("liteTopic".into(), "lite-orders".into());

        let decoded = PullMessageRequestHeader::decode_fields(&fields).unwrap();
        assert_eq!(decoded.get_lite_topic(), Some("lite-orders"));
        let mut encoded = HeaderMap::new();
        decoded.encode_fields(&mut encoded);
        assert_eq!(encoded.get("liteTopic"), Some(&"lite-orders".to_owned()));
    }

    #[test]
    fn missing_and_invalid_response_header_values_name_the_field() {
        assert!(matches!(
            QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::new()),
            Err(ProtocolError::MissingField("offset"))
        ));
        let decoded = QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::from([(
            "offset".into(),
            "12".into(),
        )]))
        .unwrap();
        assert_eq!(decoded.offset, Some(12));
        assert!(matches!(
            QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::from([(
                "offset".into(),
                "9223372036854775808".into()
            )])),
            Err(ProtocolError::InvalidField {
                field: "offset",
                ..
            })
        ));
    }

    #[test]
    fn recall_headers_match_java_field_names_and_nullable_response_msg_id() {
        let request = RecallMessageRequestHeader {
            producer_group: Some("recall-group".into()),
            topic: "orders".into(),
            recall_handle: "handle".into(),
            broker_name: Some("broker-a".into()),
            ..RecallMessageRequestHeader::default()
        };
        let mut fields = HeaderMap::new();
        request.encode_fields(&mut fields);
        assert_eq!(
            fields,
            HeaderMap::from([
                ("producerGroup".into(), "recall-group".into()),
                ("topic".into(), "orders".into()),
                ("recallHandle".into(), "handle".into()),
                ("bname".into(), "broker-a".into()),
            ])
        );
        assert_eq!(
            RecallMessageRequestHeader::decode_fields(&fields),
            Ok(request)
        );
        assert_eq!(
            RecallMessageResponseHeader::decode_fields(&HeaderMap::new())
                .unwrap()
                .get_msg_id(),
            None
        );
        let decoded = RecallMessageResponseHeader::decode_fields(&HeaderMap::from([(
            "msgId".into(),
            "recalled-id".into(),
        )]))
        .unwrap();
        assert_eq!(decoded.get_msg_id(), Some("recalled-id"));

        let mut response = RecallMessageResponseHeader::default();
        response.set_msg_id(Some("recalled-id".into()));
        assert_eq!(response.get_msg_id(), Some("recalled-id"));
        let mut encoded = HeaderMap::new();
        response.encode_fields(&mut encoded);
        assert_eq!(
            encoded.get("msgId").map(String::as_str),
            Some("recalled-id")
        );
        response.set_msg_id(None);
        assert_eq!(response.get_msg_id(), None);
    }
}
