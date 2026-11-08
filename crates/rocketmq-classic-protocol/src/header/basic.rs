use std::hash::{Hash, Hasher};

use rocketmq_classic_common::message::TopicFilterType;

use super::{
    CommandHeader, HeaderMap, ProtocolError, java_boolean, optional_i32, optional_i32_with_mode,
    optional_i64, optional_i64_with_mode, optional_java_boolean, optional_string, parse_java_i32,
    parse_java_i64, required_i32, required_i64, required_string,
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
mod tests;
