//! Client-side topic classification matching RocketMQ Java 5.5.1.
//!
//! ```compile_fail
//! use rocketmq_classic_common::topic::{
//!     AUTO_CREATE_TOPIC_KEY_TOPIC, RMQ_SYS_BENCHMARK_TOPIC, RMQ_SYS_OFFSET_MOVED_EVENT,
//!     RMQ_SYS_ROCKSDB_OFFSET_TOPIC, RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
//!     RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC, RMQ_SYS_SCHEDULE_TOPIC,
//!     RMQ_SYS_SELF_TEST_TOPIC, RMQ_SYS_TRACE_TOPIC, RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
//!     RMQ_SYS_TRANS_HALF_TOPIC, RMQ_SYS_TRANS_OP_HALF_TOPIC,
//!     SYNC_BROKER_MEMBER_GROUP_PREFIX, SYSTEM_TOPIC_PREFIX,
//! };
//! # fn main() {}
//! ```

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{OnceLock, RwLock};

use thiserror::Error;

const AUTO_CREATE_TOPIC_KEY_TOPIC: &str = "TBW102";
const RMQ_SYS_SCHEDULE_TOPIC: &str = "SCHEDULE_TOPIC_XXXX";
const RMQ_SYS_BENCHMARK_TOPIC: &str = "BenchmarkTest";
const RMQ_SYS_TRANS_HALF_TOPIC: &str = "RMQ_SYS_TRANS_HALF_TOPIC";
const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC";
const RMQ_SYS_TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
const RMQ_SYS_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC: &str = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC";
const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &str = "TRANS_CHECK_MAX_TIME_TOPIC";
const RMQ_SYS_SELF_TEST_TOPIC: &str = "SELF_TEST_TOPIC";
const RMQ_SYS_OFFSET_MOVED_EVENT: &str = "OFFSET_MOVED_EVENT";
const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &str = "CHECKPOINT_TOPIC";
const SYSTEM_TOPIC_PREFIX: &str = "rmq_sys_";
const SYNC_BROKER_MEMBER_GROUP_PREFIX: &str = "rmq_sys_SYNC_BROKER_MEMBER_";
pub const TOPIC_MESSAGE_TYPE_ATTRIBUTE: &str = "message.type";
pub const LITE_EXPIRATION_ATTRIBUTE: &str = "lite.topic.expiration";
pub type TopicMessagePropertyMap = HashMap<String, Option<String>>;

/// Java `org.apache.rocketmq.common.attribute.TopicMessageType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicMessageType {
    Unspecified,
    Normal,
    Fifo,
    Delay,
    Transaction,
    Priority,
    Lite,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TopicMessageTypeError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.common.attribute.TopicMessageType.{0}")]
    UnknownName(String),
    #[error("java.lang.NullPointerException")]
    NullMessageProperties,
}

impl TopicMessageType {
    pub const fn values() -> [Self; 8] {
        [
            Self::Unspecified,
            Self::Normal,
            Self::Fifo,
            Self::Delay,
            Self::Transaction,
            Self::Priority,
            Self::Lite,
            Self::Mixed,
        ]
    }

    pub const fn get_value(self) -> &'static str {
        self.name()
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Normal => "NORMAL",
            Self::Fifo => "FIFO",
            Self::Delay => "DELAY",
            Self::Transaction => "TRANSACTION",
            Self::Priority => "PRIORITY",
            Self::Lite => "LITE",
            Self::Mixed => "MIXED",
        }
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, TopicMessageTypeError> {
        match name {
            None => Err(TopicMessageTypeError::NullName),
            Some("UNSPECIFIED") => Ok(Self::Unspecified),
            Some("NORMAL") => Ok(Self::Normal),
            Some("FIFO") => Ok(Self::Fifo),
            Some("DELAY") => Ok(Self::Delay),
            Some("TRANSACTION") => Ok(Self::Transaction),
            Some("PRIORITY") => Ok(Self::Priority),
            Some("LITE") => Ok(Self::Lite),
            Some("MIXED") => Ok(Self::Mixed),
            Some(other) => Err(TopicMessageTypeError::UnknownName(other.to_owned())),
        }
    }

    pub fn topic_message_type_set() -> HashSet<String> {
        Self::values()
            .into_iter()
            .map(|value| value.get_value().to_owned())
            .collect()
    }

    pub fn parse_from_message_property(
        message_property: Option<&TopicMessagePropertyMap>,
    ) -> Result<Self, TopicMessageTypeError> {
        let message_property =
            message_property.ok_or(TopicMessageTypeError::NullMessageProperties)?;
        if message_property
            .get(crate::message::constants::PROPERTY_TRANSACTION_PREPARED)
            .and_then(Option::as_deref)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            Ok(Self::Transaction)
        } else if [
            crate::message::constants::PROPERTY_DELAY_TIME_LEVEL,
            crate::message::constants::PROPERTY_TIMER_DELIVER_MS,
            crate::message::constants::PROPERTY_TIMER_DELAY_SEC,
            crate::message::constants::PROPERTY_TIMER_DELAY_MS,
        ]
        .iter()
        .any(|key| message_property.get(*key).is_some_and(Option::is_some))
        {
            Ok(Self::Delay)
        } else if message_property
            .get(crate::message::constants::PROPERTY_SHARDING_KEY)
            .is_some_and(Option::is_some)
        {
            Ok(Self::Fifo)
        } else if message_property
            .get(crate::message::constants::PROPERTY_PRIORITY)
            .is_some_and(Option::is_some)
        {
            Ok(Self::Priority)
        } else if message_property
            .get(crate::message::constants::PROPERTY_LITE_TOPIC)
            .is_some_and(Option::is_some)
        {
            Ok(Self::Lite)
        } else {
            Ok(Self::Normal)
        }
    }

    pub fn metrics_value(self) -> String {
        let language = java_default_locale_language();
        self.name()
            .chars()
            .flat_map(|character| match (language.as_deref(), character) {
                (Some("tr" | "az"), 'I') => 'ı'.to_lowercase(),
                (_, character) => character.to_lowercase(),
            })
            .collect()
    }
}

pub(crate) fn java_default_locale_language() -> Option<String> {
    ["LC_ALL", "LC_CTYPE", "LANG"]
        .into_iter()
        .filter_map(std::env::var_os)
        .map(|locale| locale.to_string_lossy().into_owned())
        .find(|locale| !locale.is_empty())
        .map(|locale| {
            locale
                .split(['_', '-', '.', '@'])
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
        })
}

pub(crate) fn java_default_locale_uppercase(value: &str) -> String {
    if matches!(java_default_locale_language().as_deref(), Some("tr" | "az")) {
        value
            .chars()
            .flat_map(|character| match character {
                'i' => 'İ'.to_uppercase(),
                'ı' => 'I'.to_uppercase(),
                _ => character.to_uppercase(),
            })
            .collect()
    } else {
        value.to_uppercase()
    }
}

impl fmt::Display for TopicMessageType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

const INITIAL_SYSTEM_TOPICS: &[&str] = &[
    AUTO_CREATE_TOPIC_KEY_TOPIC,
    RMQ_SYS_SCHEDULE_TOPIC,
    RMQ_SYS_BENCHMARK_TOPIC,
    RMQ_SYS_TRANS_HALF_TOPIC,
    RMQ_SYS_TRACE_TOPIC,
    RMQ_SYS_TRANS_OP_HALF_TOPIC,
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
    RMQ_SYS_SELF_TEST_TOPIC,
    RMQ_SYS_OFFSET_MOVED_EVENT,
    RMQ_SYS_ROCKSDB_OFFSET_TOPIC,
    RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
    RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
];

const INITIAL_NOT_ALLOWED_SEND_TOPICS: &[&str] = &[
    RMQ_SYS_SCHEDULE_TOPIC,
    RMQ_SYS_TRANS_HALF_TOPIC,
    RMQ_SYS_TRANS_OP_HALF_TOPIC,
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
    RMQ_SYS_SELF_TEST_TOPIC,
    RMQ_SYS_OFFSET_MOVED_EVENT,
    RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
    RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
];

fn system_topics() -> &'static RwLock<HashSet<Option<String>>> {
    static SYSTEM_TOPICS: OnceLock<RwLock<HashSet<Option<String>>>> = OnceLock::new();
    SYSTEM_TOPICS.get_or_init(|| {
        RwLock::new(
            INITIAL_SYSTEM_TOPICS
                .iter()
                .map(|topic| Some((*topic).to_owned()))
                .collect(),
        )
    })
}

fn not_allowed_send_topics() -> &'static RwLock<HashSet<Option<String>>> {
    static NOT_ALLOWED_SEND_TOPICS: OnceLock<RwLock<HashSet<Option<String>>>> = OnceLock::new();
    NOT_ALLOWED_SEND_TOPICS.get_or_init(|| {
        RwLock::new(
            INITIAL_NOT_ALLOWED_SEND_TOPICS
                .iter()
                .map(|topic| Some((*topic).to_owned()))
                .collect(),
        )
    })
}

fn valid_char_bit_map() -> &'static RwLock<[bool; 128]> {
    static VALID_CHAR_BIT_MAP: OnceLock<RwLock<[bool; 128]>> = OnceLock::new();
    VALID_CHAR_BIT_MAP.get_or_init(|| {
        let mut valid = [false; 128];
        for character in b"%|-_" {
            valid[*character as usize] = true;
        }
        for character in b'0'..=b'9' {
            valid[character as usize] = true;
        }
        for character in b'A'..=b'Z' {
            valid[character as usize] = true;
        }
        for character in b'a'..=b'z' {
            valid[character as usize] = true;
        }
        RwLock::new(valid)
    })
}

fn java_length(value: &str) -> usize {
    value.encode_utf16().count()
}

fn java_blank(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        value.is_empty()
            || value.chars().all(|character| {
                (character.is_whitespace() || matches!(character, '\u{001c}'..='\u{001f}'))
                    && !matches!(character, '\u{00a0}' | '\u{2007}' | '\u{202f}')
            })
    })
}

/// Result returned by Java-compatible topic and group validation.
///
/// Java `TopicValidator.ValidateResult` inherits identity-based `Object.equals`
/// and `Object.hashCode`; it does not define value equality. This type therefore
/// intentionally has no `PartialEq` or `Eq` implementation.
///
/// ```compile_fail
/// use rocketmq_classic_common::topic::ValidateResult;
///
/// fn requires_eq<T: Eq>() {}
/// requires_eq::<ValidateResult>();
/// ```
#[derive(Debug, Clone)]
pub struct ValidateResult {
    valid: bool,
    remark: Option<String>,
}

/// Rust error corresponding to a Java `NullPointerException` from a
/// `TopicValidator` method that dereferences a nullable string argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicValidatorError {
    NullString,
}

impl fmt::Display for TopicValidatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NullString => formatter.write_str("null string argument"),
        }
    }
}

impl std::error::Error for TopicValidatorError {}

impl ValidateResult {
    /// Creates a validation result. `None` matches Java's nullable remark.
    pub fn new(valid: bool, remark: Option<String>) -> Self {
        Self { valid, remark }
    }

    fn valid() -> Self {
        Self::new(true, Some(String::new()))
    }

    fn invalid(remark: String) -> Self {
        Self::new(false, Some(remark))
    }

    pub const fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn remark(&self) -> Option<&str> {
        self.remark.as_deref()
    }
}

/// Java's `TopicValidator` operations used by namespace and producer validation.
pub struct TopicValidator;

impl TopicValidator {
    pub const AUTO_CREATE_TOPIC_KEY_TOPIC: &'static str = crate::topic::AUTO_CREATE_TOPIC_KEY_TOPIC;
    pub const RMQ_SYS_SCHEDULE_TOPIC: &'static str = crate::topic::RMQ_SYS_SCHEDULE_TOPIC;
    pub const RMQ_SYS_BENCHMARK_TOPIC: &'static str = crate::topic::RMQ_SYS_BENCHMARK_TOPIC;
    pub const RMQ_SYS_TRANS_HALF_TOPIC: &'static str = crate::topic::RMQ_SYS_TRANS_HALF_TOPIC;
    pub const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC: &'static str =
        crate::topic::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC;
    pub const RMQ_SYS_TRACE_TOPIC: &'static str = crate::topic::RMQ_SYS_TRACE_TOPIC;
    pub const RMQ_SYS_TRANS_OP_HALF_TOPIC: &'static str = crate::topic::RMQ_SYS_TRANS_OP_HALF_TOPIC;
    pub const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC: &'static str =
        crate::topic::RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC;
    pub const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: &'static str =
        crate::topic::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC;
    pub const RMQ_SYS_SELF_TEST_TOPIC: &'static str = crate::topic::RMQ_SYS_SELF_TEST_TOPIC;
    pub const RMQ_SYS_OFFSET_MOVED_EVENT: &'static str = crate::topic::RMQ_SYS_OFFSET_MOVED_EVENT;
    pub const RMQ_SYS_ROCKSDB_OFFSET_TOPIC: &'static str =
        crate::topic::RMQ_SYS_ROCKSDB_OFFSET_TOPIC;
    pub const SYSTEM_TOPIC_PREFIX: &'static str = crate::topic::SYSTEM_TOPIC_PREFIX;
    pub const SYNC_BROKER_MEMBER_GROUP_PREFIX: &'static str =
        crate::topic::SYNC_BROKER_MEMBER_GROUP_PREFIX;

    /// Returns the mutable Java-style character table behind validation.
    /// Values outside ASCII are always rejected, as in Java's 128-element array.
    pub fn valid_char_bit_map() -> &'static RwLock<[bool; 128]> {
        valid_char_bit_map()
    }

    pub fn is_topic_or_group_illegal(value: &str) -> bool {
        let map = valid_char_bit_map()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        value
            .encode_utf16()
            .any(|unit| usize::from(unit) >= map.len() || !map[usize::from(unit)])
    }

    /// Handles Java's nullable input boundary; `None` maps to its NPE branch.
    pub fn is_topic_or_group_illegal_nullable(
        value: Option<&str>,
    ) -> Result<bool, TopicValidatorError> {
        value
            .map(Self::is_topic_or_group_illegal)
            .ok_or(TopicValidatorError::NullString)
    }

    pub fn validate_topic(topic: Option<&str>) -> ValidateResult {
        let Some(topic) = topic.filter(|topic| !java_blank(Some(topic))) else {
            return ValidateResult::invalid("The specified topic is blank.".to_owned());
        };
        if Self::is_topic_or_group_illegal(topic) {
            return ValidateResult::invalid(format!(
                "The specified topic: {topic}, contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$"
            ));
        }
        let is_retry_or_dlq = topic.starts_with("%RETRY%") || topic.starts_with("%DLQ%");
        let maximum = if is_retry_or_dlq { 255 } else { 127 };
        if java_length(topic) > maximum {
            if is_retry_or_dlq {
                return ValidateResult::invalid(format!(
                    "The specified topic is DLQ or Retry topic: {topic}, and it's longer than topic max length: {maximum}"
                ));
            }
            return ValidateResult::invalid(format!(
                "The specified topic: {topic}, is longer than topic max length: {maximum}"
            ));
        }
        ValidateResult::valid()
    }

    pub fn validate_group(group: Option<&str>) -> ValidateResult {
        let Some(group) = group.filter(|group| !java_blank(Some(group))) else {
            return ValidateResult::invalid("The specified group is blank.".to_owned());
        };
        if Self::is_topic_or_group_illegal(group) {
            return ValidateResult::invalid(format!(
                "The specified group: {group}, contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$"
            ));
        }
        if java_length(group) > 120 {
            return ValidateResult::invalid(format!(
                "The specified group: {group}, is longer than group max length: 120"
            ));
        }
        ValidateResult::valid()
    }

    pub fn is_system_topic(topic: &str) -> bool {
        topic.starts_with(SYSTEM_TOPIC_PREFIX)
            || system_topics()
                .read()
                .expect("system topic set lock poisoned")
                .contains(&Some(topic.to_owned()))
    }

    /// Handles Java's nullable input and mutable set behavior. If the live
    /// system set contains null, Java returns true before dereferencing it.
    pub fn is_system_topic_nullable(topic: Option<&str>) -> Result<bool, TopicValidatorError> {
        let topics = system_topics()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if topics.contains(&topic.map(str::to_owned)) {
            return Ok(true);
        }
        let Some(topic) = topic else {
            return Err(TopicValidatorError::NullString);
        };
        Ok(topic.starts_with(SYSTEM_TOPIC_PREFIX))
    }

    pub fn is_not_allowed_send_topic(topic: &str) -> bool {
        not_allowed_send_topics()
            .read()
            .expect("send topic set lock poisoned")
            .contains(&Some(topic.to_owned()))
    }

    /// Tests membership for either a Java string or Java null.
    pub fn is_not_allowed_send_topic_nullable(topic: Option<&str>) -> bool {
        not_allowed_send_topics()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&topic.map(str::to_owned))
    }

    pub fn add_system_topic(topic: impl Into<String>) {
        system_topics()
            .write()
            .expect("system topic set lock poisoned")
            .insert(Some(topic.into()));
    }

    /// Adds a Java string or null to the live system-topic set.
    pub fn add_system_topic_nullable(topic: Option<&str>) {
        system_topics()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic.map(str::to_owned));
    }

    /// Returns the live Java-style mutable set behind a lock.
    pub fn system_topic_set() -> &'static RwLock<HashSet<Option<String>>> {
        system_topics()
    }

    /// Returns the live Java-style mutable set behind a lock.
    pub fn not_allowed_send_topic_set() -> &'static RwLock<HashSet<Option<String>>> {
        not_allowed_send_topics()
    }
}

#[cfg(test)]
mod tests {
    use super::TopicValidator;

    #[test]
    fn added_system_topics_are_visible_to_all_classifiers() {
        let topic = "phase16-custom-system-topic";
        TopicValidator::add_system_topic(topic);
        assert!(TopicValidator::is_system_topic(topic));
        assert!(
            TopicValidator::system_topic_set()
                .read()
                .unwrap()
                .contains(&Some(topic.to_owned()))
        );
    }
}
