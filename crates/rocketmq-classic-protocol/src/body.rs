use crate::route::{BrokerData, SharedBrokerDataHandle, TopicQueueMappingInfo};
use crate::{
    JavaJsonWriterFeature, SharedValue,
    codes::{JavaEnumLookupError, LanguageCode},
    heartbeat::{ConsumeFromWhere, ConsumeType, MessageModel, SubscriptionData},
    serde_fastjson,
    subscription::SubscriptionGroupConfig,
};
use indexmap::IndexMap;
use rocketmq_classic_common::{
    message::MessageQueue,
    permission::PermName,
    topic::{
        LITE_EXPIRATION_ATTRIBUTE, TOPIC_MESSAGE_TYPE_ATTRIBUTE, TopicMessageType,
        TopicMessageTypeError,
    },
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    ser::{SerializeMap, SerializeStruct},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicI32, Ordering},
};

macro_rules! impl_java_enum_api {
    ($enum_type:ident, $java_class:literal, $count:literal, $($variant:ident => $name:literal = $ordinal:literal),+ $(,)?) => {
        impl $enum_type {
            pub const fn values() -> [Self; $count] {
                [$(Self::$variant),+]
            }

            pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
                match name {
                    None => Err(JavaEnumLookupError::NullName),
                    $(Some($name) => Ok(Self::$variant),)+
                    Some(other) => Err(JavaEnumLookupError::UnknownName {
                        enum_class: $java_class,
                        name: other.to_owned(),
                    }),
                }
            }

            pub const fn name(&self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }

            pub const fn ordinal(&self) -> i32 {
                match self {
                    $(Self::$variant => $ordinal,)+
                }
            }

            pub const fn java_compare_to(&self, other: &Self) -> i32 {
                self.ordinal() - other.ordinal()
            }
        }

        impl std::fmt::Display for $enum_type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.name())
            }
        }
    };
}

macro_rules! impl_java_static_remoting_serializable_adapters {
    ($($dto:ty),+ $(,)?) => {
        $(
            impl $dto {
                pub fn java_encode_optional(
                    value: Option<&Self>,
                ) -> Result<Option<Vec<u8>>, serde_json::Error> {
                    value.map(Self::java_encode).transpose()
                }

                pub fn java_to_json_optional(
                    value: Option<&Self>,
                    pretty_format: bool,
                ) -> Result<String, serde_json::Error> {
                    value.map_or_else(
                        || Ok("null".to_owned()),
                        |value| value.java_to_json(pretty_format),
                    )
                }

                pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
                    crate::route::java_fastjson_json(self, pretty_format)
                }

                pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
                    Ok(self.java_to_json(false)?.into_bytes())
                }

                pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
                    serde_fastjson::from_fastjson_slice(data)
                }

                pub fn java_decode_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    serde_fastjson::from_fastjson_slice_nullable(data)
                }

                pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
                    serde_fastjson::from_fastjson_slice(data)
                }

                pub fn java_decode_list_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
                    serde_fastjson::from_fastjson_list_nullable(data)
                }

                pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
                    serde_fastjson::from_fastjson_str(json)
                }

                pub fn java_from_json_nullable(
                    json: Option<&str>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    serde_fastjson::from_fastjson_str_nullable(json)
                }
            }
        )+
    };
}

macro_rules! impl_java_remoting_serializable_writer_features {
    ($($dto:ty),+ $(,)?) => {
        $(
            impl $dto {
                /// Encodes with the supported inherited Fastjson2 writer features.
                pub fn java_encode_with_features(
                    &self,
                    features: &[JavaJsonWriterFeature],
                ) -> Result<Vec<u8>, serde_json::Error> {
                    Ok(crate::route::java_fastjson_json_with_features(
                        self, false, features,
                    )?.into_bytes())
                }
            }
        )+
    };
}

macro_rules! impl_java_remoting_serializable_adapters {
    ($($dto:ty),+ $(,)?) => {
        impl_java_static_remoting_serializable_adapters!($($dto),+);
        impl_java_remoting_serializable_writer_features!($($dto),+);
    };
}

macro_rules! impl_java_static_topic_serializable_adapters {
    ($($dto:ty),+ $(,)?) => {
        $(
            impl $dto {
                pub fn java_encode_optional(
                    value: Option<&Self>,
                ) -> Result<Option<Vec<u8>>, serde_json::Error> {
                    value.map(Self::java_encode).transpose()
                }

                pub fn java_to_json_optional(
                    value: Option<&Self>,
                    pretty_format: bool,
                ) -> Result<String, serde_json::Error> {
                    value.map_or_else(
                        || Ok("null".to_owned()),
                        |value| value.java_to_json(pretty_format),
                    )
                }

                pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
                    crate::route::java_fastjson_json(self, pretty_format)
                }

                pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
                    Ok(self.java_to_json(false)?.into_bytes())
                }

                pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
                    decode_static_topic_json(data)
                }

                pub fn java_decode_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    let Some(data) = data else {
                        return Ok(None);
                    };
                    decode_static_topic_json_nullable(data)
                }

                pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
                    decode_static_topic_json(data)
                }

                pub fn java_decode_list_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
                    let Some(data) = data else {
                        return Ok(None);
                    };
                    decode_static_topic_json_list_nullable(data)
                }

                pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
                    decode_static_topic_json(json.as_bytes())
                }

                pub fn java_from_json_nullable(
                    json: Option<&str>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    let Some(json) = json else {
                        return Ok(None);
                    };
                    decode_static_topic_json_nullable(json.as_bytes())
                }
            }
        )+
    };
}

fn decode_static_topic_json<T: serde::de::DeserializeOwned>(
    input: &[u8],
) -> Result<T, serde_json::Error> {
    let normalized = normalize_static_topic_json(input);
    serde_json::from_str(&normalized)
}

fn decode_static_topic_json_nullable<T: serde::de::DeserializeOwned>(
    input: &[u8],
) -> Result<Option<T>, serde_json::Error> {
    let normalized = normalize_static_topic_json(input);
    serde_json::from_str(&normalized)
}

fn decode_static_topic_json_list_nullable<T: serde::de::DeserializeOwned>(
    input: &[u8],
) -> Result<Option<Vec<Option<T>>>, serde_json::Error> {
    let normalized = normalize_static_topic_json(input);
    serde_json::from_str(&normalized)
}

fn normalize_static_topic_json(input: &[u8]) -> String {
    let input = String::from_utf8_lossy(input);
    let normalized = serde_fastjson::normalize_json_input(&input);
    serde_fastjson::normalize_fastjson_integer_map_keys(&normalized)
}

/// Status names and integer values of Java
/// `CheckRocksdbCqWriteResult.CheckStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckRocksdbCqWriteStatus {
    CheckOk,
    CheckNotOk,
    CheckInProgress,
    CheckError,
}

impl_java_enum_api!(
    CheckRocksdbCqWriteStatus,
    "org.apache.rocketmq.common.CheckRocksdbCqWriteResult$CheckStatus",
    4,
    CheckOk => "CHECK_OK" = 0,
    CheckNotOk => "CHECK_NOT_OK" = 1,
    CheckInProgress => "CHECK_IN_PROGRESS" = 2,
    CheckError => "CHECK_ERROR" = 3,
);

impl CheckRocksdbCqWriteStatus {
    pub const fn get_value(&self) -> i32 {
        self.ordinal()
    }
}

/// Java `CheckRocksdbCqWriteResult`. The DTO has identity-based equality and
/// exposes its nullable result text and primitive status through accessors.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::CheckRocksdbCqWriteResult;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<CheckRocksdbCqWriteResult>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::CheckRocksdbCqWriteResult;
/// let _ = CheckRocksdbCqWriteResult::default().check_status;
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::CheckRocksdbCqWriteResult;
/// let _ = CheckRocksdbCqWriteResult::default().java_encode_with_features(&[]);
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CheckRocksdbCqWriteResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    check_result: Option<String>,
    check_status: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckRocksdbCqWriteResultFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    check_result: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    check_status: i32,
}

impl<'de> Deserialize<'de> for CheckRocksdbCqWriteResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: CheckRocksdbCqWriteResultFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            check_result: fields.check_result,
            check_status: fields.check_status,
        })
    }
}

impl CheckRocksdbCqWriteResult {
    pub fn get_check_result(&self) -> Option<&str> {
        self.check_result.as_deref()
    }

    pub fn set_check_result(&mut self, value: Option<String>) {
        self.check_result = value;
    }

    pub fn get_check_status(&self) -> i32 {
        self.check_status
    }

    pub fn set_check_status(&mut self, value: i32) {
        self.check_status = value;
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::CheckClientRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<CheckClientRequestBody>(); }
/// ```
///
/// Java keeps these properties private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::CheckClientRequestBody;
/// let _ = CheckClientRequestBody::default().client_id;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CheckClientRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_data: Option<SharedValue<super::heartbeat::SubscriptionData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckClientRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group: Option<String>,
    subscription_data: Option<super::heartbeat::SubscriptionData>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    namespace: Option<String>,
}

impl<'de> Deserialize<'de> for CheckClientRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: CheckClientRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            client_id: fields.client_id,
            group: fields.group,
            subscription_data: fields.subscription_data.map(SharedValue::new),
            namespace: fields.namespace,
        })
    }
}

impl CheckClientRequestBody {
    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: Option<String>) {
        self.group = group;
    }

    pub fn get_subscription_data(&self) -> Option<SharedValue<super::heartbeat::SubscriptionData>> {
        self.subscription_data.clone()
    }

    /// Returns the same shared object used by the Java DTO setter/getter pair.
    pub fn get_subscription_data_mut(
        &self,
    ) -> Option<SharedValue<super::heartbeat::SubscriptionData>> {
        self.subscription_data.clone()
    }

    pub fn set_subscription_data(
        &mut self,
        subscription_data: Option<SharedValue<super::heartbeat::SubscriptionData>>,
    ) {
        self.subscription_data = subscription_data;
    }

    pub fn get_namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn set_namespace(&mut self, namespace: Option<String>) {
        self.namespace = namespace;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageRequestMode {
    #[default]
    Pull,
    Pop,
}

impl_java_enum_api!(
    MessageRequestMode,
    "org.apache.rocketmq.common.message.MessageRequestMode",
    2,
    Pull => "PULL" = 0,
    Pop => "POP" = 1,
);

impl super::heartbeat::JavaOrdinalEnum for MessageRequestMode {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Pull),
            1 => Some(Self::Pop),
            _ => None,
        }
    }
}

fn default_assignment_mode() -> Option<MessageRequestMode> {
    Some(MessageRequestMode::Pull)
}

pub type SharedQueryAssignment = SharedValue<MessageQueueAssignment>;
pub type QueryAssignmentSet = std::collections::HashSet<Option<SharedQueryAssignment>>;
pub type SharedQueryAssignmentSet = SharedValue<QueryAssignmentSet>;
/// Map used by Java `MessageQueueAssignment.attachments`.
///
/// `None` keys represent Java's nullable `String` keys. Java Fastjson writes
/// those keys as the JSON property name `"null"` and omits null-valued entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct AssignmentAttachments(BTreeMap<Option<String>, Option<String>>);

impl Deref for AssignmentAttachments {
    type Target = BTreeMap<Option<String>, Option<String>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AssignmentAttachments {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeMap<Option<String>, Option<String>>> for AssignmentAttachments {
    fn from(entries: BTreeMap<Option<String>, Option<String>>) -> Self {
        Self(entries)
    }
}

impl Serialize for AssignmentAttachments {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key.as_deref().unwrap_or("null"), value)?;
        }
        map.end()
    }
}

struct NonNullAssignmentAttachments<'a>(&'a AssignmentAttachments);

impl Serialize for NonNullAssignmentAttachments<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let non_null_count = self.0.values().filter(|value| value.is_some()).count();
        let mut map = serializer.serialize_map(Some(non_null_count))?;
        for (key, value) in &self.0.0 {
            if let Some(value) = value {
                map.serialize_entry(key.as_deref().unwrap_or("null"), value)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for AssignmentAttachments {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::<String, Option<String>>::deserialize(deserializer).map(|entries| {
            Self(
                entries
                    .into_iter()
                    .map(|(key, value)| (Some(key), value))
                    .collect(),
            )
        })
    }
}

pub type SharedAssignmentAttachments = SharedValue<AssignmentAttachments>;
pub type SharedAssignmentMessageQueue = SharedValue<MessageQueue>;
pub type MessageQueueSet = std::collections::HashSet<Option<SharedAssignmentMessageQueue>>;
pub type SharedMessageQueueSet = SharedValue<MessageQueueSet>;

// Java MessageQueue implements value equality/hash and remains mutable when it
// is stored as a HashSet key. This handle keeps those Set semantics while
// retaining the exact nested MessageQueue object.
impl PartialEq for SharedValue<MessageQueue> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other) || self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<MessageQueue> {}

impl std::hash::Hash for SharedValue<MessageQueue> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.with(|message_queue| message_queue.hash(state));
    }
}

fn deserialize_optional_assignment_set<'de, D>(
    deserializer: D,
) -> Result<Option<SharedQueryAssignmentSet>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }
    let values = if raw.starts_with('[') {
        serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?
    } else if raw.starts_with('{') {
        vec![
            serde_json::from_str::<Box<serde_json::value::RawValue>>(raw)
                .map_err(de::Error::custom)?,
        ]
    } else {
        return Err(de::Error::custom(
            "expected a JSON assignment array, object, or null",
        ));
    };
    // Java stores these mutable DTO keys in a HashSet; changing a nested queue or
    // attachment can likewise leave the Java entry in its original bucket.
    #[allow(clippy::mutable_key_type)]
    let mut assignments = QueryAssignmentSet::with_capacity(values.len());
    let mut inserted: HashMap<(i32, Option<MessageRequestMode>, i32), Vec<SharedQueryAssignment>> =
        HashMap::with_capacity(values.len());
    let mut inserted_null = false;
    for value in values {
        let assignment: Option<SharedQueryAssignment> = if value.get().trim() == "null" {
            None
        } else {
            Some(SharedValue::new(
                serde_json::from_str(value.get()).map_err(de::Error::custom)?,
            ))
        };
        let Some(assignment) = assignment else {
            if !inserted_null {
                assignments.insert(None);
                inserted_null = true;
            }
            continue;
        };
        let bucket_key = assignment.with(MessageQueueAssignment::stable_java_hash_bucket_key);
        let bucket = inserted.entry(bucket_key).or_default();
        let duplicate = bucket.iter().try_fold(false, |duplicate, existing| {
            if duplicate {
                return Ok(true);
            }
            assignment.with(|assignment| {
                existing.with(|existing| {
                    assignment
                        .java_equals(Some(existing as &dyn std::any::Any))
                        .map_err(|error| match error {
                            MessageQueueAssignmentEqualsError::NullMessageQueue => {
                                de::Error::custom("NullPointerException")
                            }
                        })
                })
            })
        })?;
        if !duplicate {
            assignments.insert(Some(assignment.clone()));
            bucket.push(assignment);
        }
    }
    Ok(Some(SharedValue::new(assignments)))
}

fn default_message_queue_set() -> Option<MessageQueueSet> {
    Some(std::collections::HashSet::new())
}

fn deserialize_optional_message_queue_set<'de, D>(
    deserializer: D,
) -> Result<Option<MessageQueueSet>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" || raw == "\"\"" {
        return Ok(None);
    }
    let values = if raw.starts_with('[') {
        serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?
    } else if raw.starts_with('{') {
        vec![
            serde_json::from_str::<Box<serde_json::value::RawValue>>(raw)
                .map_err(de::Error::custom)?,
        ]
    } else {
        return Err(de::Error::custom(
            "expected a JSON message queue array, object, empty string, or null",
        ));
    };

    #[allow(clippy::mutable_key_type)]
    let queues = values
        .into_iter()
        .map(|value| {
            if value.get().trim() == "null" {
                Ok(None)
            } else {
                serde_json::from_str::<MessageQueue>(value.get())
                    .map(|message_queue| Some(SharedValue::new(message_queue)))
                    .map_err(de::Error::custom)
            }
        })
        .collect::<Result<std::collections::HashSet<_>, _>>()?;
    Ok(Some(queues))
}

/// Java's default JSON writer omits null-valued entries in this map.
fn serialize_assignment_attachments<S>(
    attachments: &Option<SharedAssignmentAttachments>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(attachments) = attachments else {
        return serializer.serialize_none();
    };
    if crate::shared::shared_value_reference_markers_enabled() {
        return attachments.with(|entries| {
            crate::shared::serialize_shared_value_identity(
                attachments.reference_identity(),
                &NonNullAssignmentAttachments(entries),
                serializer,
            )
        });
    }
    NonNullAssignmentAttachments(&attachments.snapshot()).serialize(serializer)
}

struct FastjsonI32(i32);

impl<'de> Deserialize<'de> for FastjsonI32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_fastjson::deserialize_i32(deserializer).map(Self)
    }
}

fn deserialize_assignment_mode<'de, D>(
    deserializer: D,
) -> Result<Option<MessageRequestMode>, D::Error>
where
    D: Deserializer<'de>,
{
    fn from_ordinal<E: de::Error>(ordinal: i64) -> Result<Option<MessageRequestMode>, E> {
        match ordinal {
            0 => Ok(Some(MessageRequestMode::Pull)),
            1 => Ok(Some(MessageRequestMode::Pop)),
            _ => Err(E::custom(format!(
                "Fastjson MessageRequestMode ordinal {ordinal} is out of range"
            ))),
        }
    }

    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }
    if raw.starts_with('"') {
        let value = serde_json::from_str::<String>(raw).map_err(de::Error::custom)?;
        return match value.as_str() {
            value if value.eq_ignore_ascii_case("PULL") => Ok(Some(MessageRequestMode::Pull)),
            value if value.eq_ignore_ascii_case("POP") => Ok(Some(MessageRequestMode::Pop)),
            _ => match value.parse::<i64>() {
                Ok(ordinal) => from_ordinal::<D::Error>(ordinal),
                Err(_) => Ok(None),
            },
        };
    }
    if raw == "true" || raw == "false" || raw.starts_with('{') || raw.starts_with('[') {
        return Err(de::Error::custom(
            "Fastjson cannot coerce this value to MessageRequestMode",
        ));
    }
    let FastjsonI32(ordinal) = serde_json::from_str(raw).map_err(de::Error::custom)?;
    from_ordinal::<D::Error>(ordinal as i64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiteSubscriptionAction {
    PartialAdd,
    PartialRemove,
    CompleteAdd,
    CompleteRemove,
}

impl_java_enum_api!(
    LiteSubscriptionAction,
    "org.apache.rocketmq.common.lite.LiteSubscriptionAction",
    4,
    PartialAdd => "PARTIAL_ADD" = 0,
    PartialRemove => "PARTIAL_REMOVE" = 1,
    CompleteAdd => "COMPLETE_ADD" = 2,
    CompleteRemove => "COMPLETE_REMOVE" = 3,
);

impl super::heartbeat::JavaOrdinalEnum for LiteSubscriptionAction {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::PartialAdd),
            1 => Some(Self::PartialRemove),
            2 => Some(Self::CompleteAdd),
            3 => Some(Self::CompleteRemove),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OffsetOptionType {
    Policy,
    Offset,
    TailN,
    Timestamp,
}

impl_java_enum_api!(
    OffsetOptionType,
    "org.apache.rocketmq.common.lite.OffsetOption.Type",
    4,
    Policy => "POLICY" = 0,
    Offset => "OFFSET" = 1,
    TailN => "TAIL_N" = 2,
    Timestamp => "TIMESTAMP" = 3,
);

impl super::heartbeat::JavaOrdinalEnum for OffsetOptionType {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Policy),
            1 => Some(Self::Offset),
            2 => Some(Self::TailN),
            3 => Some(Self::Timestamp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OffsetOption {
    #[serde(
        rename = "type",
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub option_type: Option<OffsetOptionType>,
    #[serde(default)]
    pub value: i64,
}

/// Shared handle for a Java `LiteSubscriptionDTO` lite topic set.
pub type SharedLiteSubscriptionTopicSet = SharedValue<std::collections::BTreeSet<Option<String>>>;

/// Shared handle for the mutable `OffsetOption` nested in a lite subscription DTO.
pub type SharedOffsetOption = SharedValue<OffsetOption>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OffsetOptionFields {
    #[serde(
        rename = "type",
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    option_type: Option<OffsetOptionType>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    value: i64,
}

impl<'de> Deserialize<'de> for OffsetOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: OffsetOptionFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            option_type: fields.option_type,
            value: fields.value,
        })
    }
}

impl OffsetOption {
    pub const POLICY_LAST_VALUE: i64 = 0;
    pub const POLICY_MIN_VALUE: i64 = 1;
    pub const POLICY_MAX_VALUE: i64 = 2;

    /// Mirrors Java `equals(Object)`, including null and wrong-type inputs.
    pub fn java_equals(&self, other: Option<&dyn std::any::Any>) -> bool {
        other
            .and_then(|other| other.downcast_ref::<Self>())
            .is_some_and(|other| self == other)
    }

    pub fn new(option_type: Option<OffsetOptionType>, value: i64) -> Self {
        Self { option_type, value }
    }

    pub fn option_type(&self) -> Option<OffsetOptionType> {
        self.option_type
    }

    pub fn set_option_type(&mut self, option_type: Option<OffsetOptionType>) {
        self.option_type = option_type;
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn set_value(&mut self, value: i64) {
        self.value = value;
    }
}

impl std::fmt::Display for OffsetOption {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let option_type = self
            .option_type
            .map(|option_type| option_type.to_string())
            .unwrap_or_else(|| "null".to_owned());
        write!(
            formatter,
            "OffsetOption{{type={option_type}, value={}}}",
            self.value
        )
    }
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
/// The Java DTO inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// fn require_value_equality<T: PartialEq + Eq>() {}
/// fn main() {
///     require_value_equality::<rocketmq_classic_protocol::body::LiteSubscriptionDto>();
/// }
/// ```
pub struct LiteSubscriptionDto {
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub action: Option<LiteSubscriptionAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lite_topic_set: Option<SharedLiteSubscriptionTopicSet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_option: Option<SharedOffsetOption>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    pub version: i64,
}

#[allow(non_camel_case_types)]
pub type LiteSubscriptionDTO = LiteSubscriptionDto;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiteSubscriptionDtoFields {
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    action: Option<LiteSubscriptionAction>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_fastjson_string_set")]
    lite_topic_set: Option<std::collections::BTreeSet<Option<String>>>,
    #[serde(default)]
    offset_option: Option<OffsetOption>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    version: i64,
}

fn deserialize_optional_fastjson_string_set<'de, D>(
    deserializer: D,
) -> Result<Option<std::collections::BTreeSet<Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }

    fn parse_value<E>(raw: &str) -> Result<Option<String>, E>
    where
        E: de::Error,
    {
        let mut deserializer = serde_json::Deserializer::from_str(raw);
        let value =
            serde_fastjson::deserialize_nullable_string(&mut deserializer).map_err(E::custom)?;
        deserializer.end().map_err(E::custom)?;
        Ok(value)
    }

    if raw.starts_with('"') {
        let value = serde_json::from_str::<String>(raw).map_err(de::Error::custom)?;
        let mut values = std::collections::BTreeSet::new();
        if !value.is_empty() {
            values.insert(Some(value));
        }
        return Ok(Some(values));
    }
    if raw.starts_with('{') {
        return Ok(Some(std::collections::BTreeSet::from([parse_value(raw)?])));
    }
    if raw.starts_with('[') {
        let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?;
        return values
            .iter()
            .map(|value| parse_value(value.get()))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()
            .map(Some);
    }
    Err(de::Error::custom(
        "expected a Fastjson string, object, array, or null for a Java Set field",
    ))
}

impl<'de> Deserialize<'de> for LiteSubscriptionDto {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LiteSubscriptionDtoFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            action: fields.action,
            client_id: fields.client_id,
            group: fields.group,
            topic: fields.topic,
            lite_topic_set: fields.lite_topic_set.map(SharedValue::new),
            offset_option: fields.offset_option.map(SharedValue::new),
            version: fields.version,
        })
    }
}

impl LiteSubscriptionDto {
    pub fn action(&self) -> Option<LiteSubscriptionAction> {
        self.action
    }

    pub fn set_action(&mut self, action: Option<LiteSubscriptionAction>) -> &mut Self {
        self.action = action;
        self
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) -> &mut Self {
        self.client_id = client_id;
        self
    }

    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: Option<String>) -> &mut Self {
        self.group = group;
        self
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) -> &mut Self {
        self.topic = topic;
        self
    }

    pub fn lite_topic_set(&self) -> Option<SharedLiteSubscriptionTopicSet> {
        self.lite_topic_set.clone()
    }

    pub fn lite_topic_set_mut(&mut self) -> Option<&mut SharedLiteSubscriptionTopicSet> {
        self.lite_topic_set.as_mut()
    }

    pub fn set_lite_topic_set(
        &mut self,
        lite_topic_set: Option<SharedLiteSubscriptionTopicSet>,
    ) -> &mut Self {
        self.lite_topic_set = lite_topic_set;
        self
    }

    pub fn offset_option(&self) -> Option<SharedOffsetOption> {
        self.offset_option.clone()
    }

    pub fn offset_option_mut(&mut self) -> Option<&mut SharedOffsetOption> {
        self.offset_option.as_mut()
    }

    pub fn set_offset_option(&mut self, offset_option: Option<SharedOffsetOption>) {
        self.offset_option = offset_option;
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn set_version(&mut self, version: i64) -> &mut Self {
        self.version = version;
        self
    }
}

impl std::fmt::Display for LiteSubscriptionDto {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let action = self
            .action
            .map(|action| action.to_string())
            .unwrap_or_else(|| "null".to_owned());
        let client_id = self.client_id.as_deref().unwrap_or("null");
        let group = self.group.as_deref().unwrap_or("null");
        let topic = self.topic.as_deref().unwrap_or("null");
        let lite_topic_set = self
            .lite_topic_set
            .as_ref()
            .map(|topics| {
                topics.with(|topics| {
                    format!(
                        "[{}]",
                        topics
                            .iter()
                            .map(|topic| topic.as_deref().unwrap_or("null"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
            })
            .unwrap_or_else(|| "null".to_owned());
        let offset_option = self
            .offset_option
            .as_ref()
            .map(|option| option.with(ToString::to_string))
            .unwrap_or_else(|| "null".to_owned());
        write!(
            formatter,
            "LiteSubscriptionDTO{{action={action}, clientId='{client_id}', group='{group}', topic='{topic}', liteTopicSet={lite_topic_set}, offsetOption={offset_option}, version={}}}",
            self.version
        )
    }
}

/// Nullable DTO elements in Java's identity-based subscription Set.
pub type LiteSubscriptionDtoSet =
    std::collections::HashSet<Option<SharedValue<LiteSubscriptionDto>>>;

/// Shared handle for the subscription Set stored by `LiteSubscriptionCtlRequestBody`.
pub type SharedLiteSubscriptionSet = SharedValue<LiteSubscriptionDtoSet>;

/// Shared mutable topic names returned by `LiteSubscription.getLiteTopicSet`.
pub type SharedLiteSubscriptionTopicNames = SharedValue<std::collections::BTreeSet<String>>;

impl PartialEq for SharedValue<LiteSubscriptionDto> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}

impl Eq for SharedValue<LiteSubscriptionDto> {}

impl std::hash::Hash for SharedValue<LiteSubscriptionDto> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash_identity(state);
    }
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
/// Java `LiteSubscriptionCtlRequestBody` inherits identity-based equality.
///
/// ```compile_fail
/// fn require_value_equality<T: PartialEq + Eq>() {}
/// fn main() {
///     require_value_equality::<rocketmq_classic_protocol::body::LiteSubscriptionCtlRequestBody>();
/// }
/// ```
pub struct LiteSubscriptionCtlRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_set: Option<SharedLiteSubscriptionSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiteSubscriptionCtlRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single"
    )]
    subscription_set: Option<Vec<Option<LiteSubscriptionDto>>>,
}

impl<'de> Deserialize<'de> for LiteSubscriptionCtlRequestBody {
    // The Set key hash uses the SharedValue allocation identity, which remains
    // stable when the Java DTO's mutable fields change.
    #[allow(clippy::mutable_key_type)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LiteSubscriptionCtlRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        let mut null_seen = false;
        let subscription_set = fields.subscription_set.map(|items| {
            let items = items
                .into_iter()
                .filter(|item| {
                    if item.is_some() {
                        true
                    } else if null_seen {
                        false
                    } else {
                        null_seen = true;
                        true
                    }
                })
                .map(|item| item.map(SharedValue::new))
                .collect::<LiteSubscriptionDtoSet>();
            SharedValue::new(items)
        });
        Ok(Self { subscription_set })
    }
}

impl LiteSubscriptionCtlRequestBody {
    pub fn subscription_set(&self) -> Option<SharedLiteSubscriptionSet> {
        self.subscription_set.clone()
    }

    pub fn subscription_set_mut(&mut self) -> Option<&mut SharedLiteSubscriptionSet> {
        self.subscription_set.as_mut()
    }

    pub fn set_subscription_set(&mut self, subscription_set: Option<SharedLiteSubscriptionSet>) {
        self.subscription_set = subscription_set;
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// Java `LiteSubscription` inherits identity-based equality.
///
/// ```compile_fail
/// fn require_value_equality<T: PartialEq + Eq>() {}
/// fn main() {
///     require_value_equality::<rocketmq_classic_protocol::body::LiteSubscription>();
/// }
/// ```
pub struct LiteSubscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub lite_topic_set: SharedLiteSubscriptionTopicNames,
    pub update_time: i64,
}

fn current_lite_subscription_time_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64),
    }
}

impl Default for LiteSubscription {
    fn default() -> Self {
        Self {
            group: None,
            topic: None,
            lite_topic_set: SharedValue::new(std::collections::BTreeSet::new()),
            update_time: current_lite_subscription_time_millis(),
        }
    }
}

impl LiteSubscription {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: Option<String>) -> &mut Self {
        self.group = group;
        self
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) -> &mut Self {
        self.topic = topic;
        self
    }

    pub fn lite_topic_set(&self) -> SharedLiteSubscriptionTopicNames {
        self.lite_topic_set.clone()
    }

    pub fn lite_topic_set_mut(&mut self) -> &mut SharedLiteSubscriptionTopicNames {
        &mut self.lite_topic_set
    }

    pub fn set_lite_topic_set<I, S>(&mut self, topics: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.lite_topic_set.with_mut(|topic_set| {
            topic_set.extend(topics.into_iter().map(|topic| topic.as_ref().to_owned()));
        });
        self
    }

    pub fn update_time(&self) -> i64 {
        self.update_time
    }

    pub fn set_update_time(&mut self, update_time: i64) {
        self.update_time = update_time;
    }

    pub fn add_lite_topic(&mut self, topic: impl Into<String>) -> bool {
        self.update_time = current_lite_subscription_time_millis();
        self.lite_topic_set
            .with_mut(|topic_set| topic_set.insert(topic.into()))
    }

    pub fn add_lite_topics<I, S>(&mut self, topics: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.update_time = current_lite_subscription_time_millis();
        self.lite_topic_set.with_mut(|topic_set| {
            topic_set.extend(topics.into_iter().map(|topic| topic.as_ref().to_owned()));
        });
    }

    pub fn remove_lite_topic(&mut self, topic: impl AsRef<str>) -> bool {
        self.update_time = current_lite_subscription_time_millis();
        self.lite_topic_set
            .with_mut(|topic_set| topic_set.remove(topic.as_ref()))
    }

    pub fn remove_lite_topics<I, S>(&mut self, topics: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.update_time = current_lite_subscription_time_millis();
        for topic in topics {
            self.lite_topic_set
                .with_mut(|topic_set| topic_set.remove(topic.as_ref()));
        }
    }
}

impl std::fmt::Display for LiteSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let group = self.group.as_deref().unwrap_or("null");
        let topic = self.topic.as_deref().unwrap_or("null");
        let lite_topic_set = self.lite_topic_set.with(|topics| {
            topics
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        });
        write!(
            formatter,
            "LiteSubscription{{group='{group}', topic='{topic}', liteTopicSet=[{lite_topic_set}], updateTime={}}}",
            self.update_time
        )
    }
}

struct LiteSubscriptionRawEntries(Vec<(String, Box<serde_json::value::RawValue>)>);

impl<'de> Deserialize<'de> for LiteSubscriptionRawEntries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntriesVisitor;

        impl<'de> de::Visitor<'de> for EntriesVisitor {
            type Value = LiteSubscriptionRawEntries;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object containing LiteSubscription fields")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut entries = Vec::new();
                while let Some((name, value)) = map.next_entry()? {
                    entries.push((name, value));
                }
                Ok(LiteSubscriptionRawEntries(entries))
            }
        }

        deserializer.deserialize_map(EntriesVisitor)
    }
}

fn parse_lite_subscription_nullable_string(raw: &str) -> Result<Option<String>, String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let value = serde_fastjson::deserialize_nullable_string(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok(value)
}

fn parse_lite_subscription_update_time(raw: &str) -> Result<i64, String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let value = serde_fastjson::deserialize_i64_reject_object(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok(value)
}

fn parse_lite_subscription_topic_set(
    raw: &str,
) -> Result<std::collections::BTreeSet<String>, String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let values = deserialize_optional_fastjson_string_set(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    let values =
        values.ok_or_else(|| "LiteSubscription cannot add a null lite topic set".to_owned())?;
    values
        .into_iter()
        .map(|value| {
            value.ok_or_else(|| "LiteSubscription cannot add a null lite topic".to_owned())
        })
        .collect()
}

impl<'de> Deserialize<'de> for LiteSubscription {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let LiteSubscriptionRawEntries(entries) =
            LiteSubscriptionRawEntries::deserialize(deserializer)?;
        let mut subscription = Self::default();
        for (name, raw) in entries {
            match name.as_str() {
                "group" => {
                    subscription.group = parse_lite_subscription_nullable_string(raw.get())
                        .map_err(de::Error::custom)?;
                }
                "topic" => {
                    subscription.topic = parse_lite_subscription_nullable_string(raw.get())
                        .map_err(de::Error::custom)?;
                }
                "liteTopicSet" => {
                    let topics =
                        parse_lite_subscription_topic_set(raw.get()).map_err(de::Error::custom)?;
                    subscription
                        .lite_topic_set
                        .with_mut(|topic_set| topic_set.extend(topics));
                }
                "updateTime" => {
                    subscription.update_time = parse_lite_subscription_update_time(raw.get())
                        .map_err(de::Error::custom)?;
                }
                _ => {}
            }
        }
        Ok(subscription)
    }
}

/// Java `QueryAssignmentRequestBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueryAssignmentRequestBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<QueryAssignmentRequestBody>();
/// ```
///
/// Java keeps these properties private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueryAssignmentRequestBody;
/// let _ = QueryAssignmentRequestBody::default().topic;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strategy_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    message_model: Option<super::heartbeat::MessageModel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryAssignmentRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    strategy_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    message_model: Option<super::heartbeat::MessageModel>,
}

impl<'de> Deserialize<'de> for QueryAssignmentRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueryAssignmentRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            consumer_group: fields.consumer_group,
            client_id: fields.client_id,
            strategy_name: fields.strategy_name,
            message_model: fields.message_model,
        })
    }
}

impl QueryAssignmentRequestBody {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn get_strategy_name(&self) -> Option<&str> {
        self.strategy_name.as_deref()
    }

    pub fn set_strategy_name(&mut self, strategy_name: Option<String>) {
        self.strategy_name = strategy_name;
    }

    pub fn get_message_model(&self) -> Option<super::heartbeat::MessageModel> {
        self.message_model
    }

    pub fn set_message_model(&mut self, message_model: Option<super::heartbeat::MessageModel>) {
        self.message_model = message_model;
    }
}

/// Java stores all three properties privately and exposes getters and setters.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::MessageQueueAssignment;
/// let _ = MessageQueueAssignment::default().message_queue;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueueAssignment {
    #[serde(skip_serializing_if = "Option::is_none")]
    message_queue: Option<SharedAssignmentMessageQueue>,
    /// Java defaults this field to PULL but Fastjson may set it to null.
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "deserialize_assignment_mode",
        skip_serializing_if = "Option::is_none"
    )]
    mode: Option<MessageRequestMode>,
    /// Java retains null values on decode and omits them during default JSON encoding.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_assignment_attachments",
        serialize_with = "serialize_assignment_attachments",
        skip_serializing_if = "Option::is_none"
    )]
    attachments: Option<SharedAssignmentAttachments>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageQueueAssignmentFields {
    message_queue: Option<MessageQueue>,
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "deserialize_assignment_mode"
    )]
    mode: Option<MessageRequestMode>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_assignment_attachments"
    )]
    attachments: Option<AssignmentAttachments>,
}

fn deserialize_optional_assignment_attachments<'de, D>(
    deserializer: D,
) -> Result<Option<AssignmentAttachments>, D::Error>
where
    D: Deserializer<'de>,
{
    serde_fastjson::deserialize_optional_nullable_key_string_map(deserializer)
        .map(|attachments| attachments.map(AssignmentAttachments::from))
}

impl<'de> Deserialize<'de> for MessageQueueAssignment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: MessageQueueAssignmentFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue: fields.message_queue.map(SharedValue::new),
            mode: fields.mode,
            attachments: fields.attachments.map(SharedValue::new),
        })
    }
}

impl PartialEq for MessageQueueAssignment {
    fn eq(&self, other: &Self) -> bool {
        self.message_queue.as_ref().map(SharedValue::snapshot)
            == other.message_queue.as_ref().map(SharedValue::snapshot)
            && self.mode == other.mode
            && self.attachments.as_ref().map(SharedValue::snapshot)
                == other.attachments.as_ref().map(SharedValue::snapshot)
    }
}

impl Eq for MessageQueueAssignment {}

impl std::hash::Hash for MessageQueueAssignment {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.message_queue
            .as_ref()
            .map(SharedValue::snapshot)
            .hash(state);
        self.mode.hash(state);
        self.attachments
            .as_ref()
            .map(SharedValue::snapshot)
            .hash(state);
    }
}

// Keep Java DTO object aliases while retaining the existing value-based Set
// membership used by this Rust response DTO.
impl PartialEq for SharedValue<MessageQueueAssignment> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other) || self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<MessageQueueAssignment> {}

impl std::hash::Hash for SharedValue<MessageQueueAssignment> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.snapshot().hash(state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MessageQueueAssignmentEqualsError {
    #[error("Java MessageQueueAssignment.equals dereferences a null messageQueue")]
    NullMessageQueue,
}

impl MessageQueueAssignment {
    pub fn message_queue(&self) -> Option<SharedAssignmentMessageQueue> {
        self.message_queue.clone()
    }

    /// Provides direct access to the assigned queue handle for replacement.
    pub fn message_queue_mut(&mut self) -> Option<&mut SharedAssignmentMessageQueue> {
        self.message_queue.as_mut()
    }

    pub fn set_message_queue(&mut self, message_queue: Option<SharedAssignmentMessageQueue>) {
        self.message_queue = message_queue;
    }

    pub fn mode(&self) -> Option<MessageRequestMode> {
        self.mode
    }

    pub fn set_mode(&mut self, mode: Option<MessageRequestMode>) {
        self.mode = mode;
    }

    pub fn attachments(&self) -> Option<SharedAssignmentAttachments> {
        self.attachments.clone()
    }

    pub fn attachments_mut(&mut self) -> Option<&mut SharedAssignmentAttachments> {
        self.attachments.as_mut()
    }

    pub fn set_attachments(&mut self, attachments: Option<SharedAssignmentAttachments>) {
        self.attachments = attachments;
    }

    /// Matches Java's public `equals` method, including its null-queue failure.
    pub fn java_equals(
        &self,
        other: Option<&dyn std::any::Any>,
    ) -> Result<bool, MessageQueueAssignmentEqualsError> {
        let Some(other) = other else {
            return Ok(false);
        };
        let Some(other) = other.downcast_ref::<Self>() else {
            return Ok(false);
        };
        if std::ptr::eq(self, other) {
            return Ok(true);
        }
        let Some(message_queue) = self.message_queue.as_ref() else {
            return Err(MessageQueueAssignmentEqualsError::NullMessageQueue);
        };
        Ok(other.message_queue.as_ref().is_some_and(|other_queue| {
            message_queue.ptr_eq(other_queue) || message_queue.snapshot() == other_queue.snapshot()
        }))
    }

    fn stable_java_hash_bucket_key(&self) -> (i32, Option<MessageRequestMode>, i32) {
        // The enum identity hash is process-local. Keeping the mode value as a
        // separate key component preserves exact bucket equivalence for equal
        // modes without inventing hashes for distinct Java enum objects.
        (
            self.java_queue_hash_code(),
            self.mode,
            self.java_attachments_hash_code(),
        )
    }

    fn java_queue_hash_code(&self) -> i32 {
        self.message_queue
            .as_ref()
            .map(|queue| queue.with(MessageQueue::java_hash_code))
            .unwrap_or(0)
    }

    fn java_attachments_hash_code(&self) -> i32 {
        self.attachments
            .as_ref()
            .map(|attachments| {
                attachments.with(|attachments| {
                    attachments.iter().fold(0_i32, |hash, (key, value)| {
                        let key_hash = key.as_deref().map_or(0, java_assignment_string_hash);
                        let value_hash = value.as_deref().map_or(0, java_assignment_string_hash);
                        hash.wrapping_add(key_hash ^ value_hash)
                    })
                })
            })
            .unwrap_or(0)
    }

    pub fn java_to_string(&self) -> String {
        self.to_string()
    }
}

fn java_assignment_string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

impl std::fmt::Display for MessageQueueAssignment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message_queue = self
            .message_queue
            .as_ref()
            .map(|message_queue| message_queue.with(ToString::to_string))
            .unwrap_or_else(|| "null".into());
        let mode = match self.mode {
            Some(MessageRequestMode::Pull) => "PULL",
            Some(MessageRequestMode::Pop) => "POP",
            None => "null",
        };
        write!(
            formatter,
            "MessageQueueAssignment [MessageQueue={message_queue}, Mode={mode}]"
        )
    }
}

impl Default for MessageQueueAssignment {
    fn default() -> Self {
        Self {
            message_queue: None,
            mode: default_assignment_mode(),
            attachments: None,
        }
    }
}

/// Java inherits identity-based equality and hashing for this response DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueryAssignmentResponseBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<QueryAssignmentResponseBody>(); }
/// ```
///
/// Java keeps this property private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueryAssignmentResponseBody;
/// let _ = QueryAssignmentResponseBody::default().message_queue_assignments;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentResponseBody {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_assignment_set",
        skip_serializing_if = "Option::is_none"
    )]
    message_queue_assignments: Option<SharedQueryAssignmentSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryAssignmentResponseBodyFields {
    #[serde(default, deserialize_with = "deserialize_optional_assignment_set")]
    message_queue_assignments: Option<SharedQueryAssignmentSet>,
}

impl<'de> Deserialize<'de> for QueryAssignmentResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueryAssignmentResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue_assignments: fields.message_queue_assignments,
        })
    }
}

impl QueryAssignmentResponseBody {
    pub fn get_message_queue_assignments(&self) -> Option<SharedQueryAssignmentSet> {
        self.message_queue_assignments.clone()
    }

    pub fn set_message_queue_assignments(
        &mut self,
        message_queue_assignments: Option<SharedQueryAssignmentSet>,
    ) {
        self.message_queue_assignments = message_queue_assignments;
    }
}

/// One queue's broker and consumer timestamp range, matching Java
/// `org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan`. Its fields stay
/// private like Java's; the message-queue accessor returns a shared handle so
/// getter mutations remain visible through the assigned object.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueueTimeSpan;
/// let _ = QueueTimeSpan::default().min_time_stamp;
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueueTimeSpan {
    message_queue: Option<SharedValue<MessageQueue>>,
    min_time_stamp: i64,
    max_time_stamp: i64,
    consume_time_stamp: i64,
    delay_time: i64,
}

impl Serialize for QueueTimeSpan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = serializer.serialize_struct(
            "QueueTimeSpan",
            7 + usize::from(self.message_queue.is_some()),
        )?;
        fields.serialize_field("consumeTimeStamp", &self.consume_time_stamp)?;
        fields.serialize_field("consumeTimeStampStr", &self.get_consume_time_stamp_str())?;
        fields.serialize_field("delayTime", &self.delay_time)?;
        fields.serialize_field("maxTimeStamp", &self.max_time_stamp)?;
        fields.serialize_field("maxTimeStampStr", &self.get_max_time_stamp_str())?;
        if let Some(message_queue) = &self.message_queue {
            fields.serialize_field("messageQueue", message_queue)?;
        }
        fields.serialize_field("minTimeStamp", &self.min_time_stamp)?;
        fields.serialize_field("minTimeStampStr", &self.get_min_time_stamp_str())?;
        fields.end()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueTimeSpanFields {
    #[serde(default)]
    message_queue: Option<MessageQueue>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    min_time_stamp: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    max_time_stamp: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    consume_time_stamp: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    delay_time: i64,
}

impl<'de> Deserialize<'de> for QueueTimeSpan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueueTimeSpanFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue: fields.message_queue.map(SharedValue::new),
            min_time_stamp: fields.min_time_stamp,
            max_time_stamp: fields.max_time_stamp,
            consume_time_stamp: fields.consume_time_stamp,
            delay_time: fields.delay_time,
        })
    }
}

impl QueueTimeSpan {
    pub fn get_min_time_stamp_str(&self) -> String {
        java_format_timestamp_millis(self.min_time_stamp)
    }

    pub fn get_max_time_stamp_str(&self) -> String {
        java_format_timestamp_millis(self.max_time_stamp)
    }

    pub fn get_consume_time_stamp_str(&self) -> String {
        java_format_timestamp_millis(self.consume_time_stamp)
    }

    pub fn get_message_queue(&self) -> Option<SharedValue<MessageQueue>> {
        self.message_queue.clone()
    }

    pub fn set_message_queue(&mut self, message_queue: Option<SharedValue<MessageQueue>>) {
        self.message_queue = message_queue;
    }

    pub fn get_min_time_stamp(&self) -> i64 {
        self.min_time_stamp
    }

    pub fn set_min_time_stamp(&mut self, min_time_stamp: i64) {
        self.min_time_stamp = min_time_stamp;
    }

    pub fn get_max_time_stamp(&self) -> i64 {
        self.max_time_stamp
    }

    pub fn set_max_time_stamp(&mut self, max_time_stamp: i64) {
        self.max_time_stamp = max_time_stamp;
    }

    pub fn get_consume_time_stamp(&self) -> i64 {
        self.consume_time_stamp
    }

    pub fn set_consume_time_stamp(&mut self, consume_time_stamp: i64) {
        self.consume_time_stamp = consume_time_stamp;
    }

    pub fn get_delay_time(&self) -> i64 {
        self.delay_time
    }

    pub fn set_delay_time(&mut self, delay_time: i64) {
        self.delay_time = delay_time;
    }
}

fn java_format_timestamp_millis(timestamp_millis: i64) -> String {
    const MILLIS_PER_DAY: i128 = 86_400_000;
    let timestamp_millis = i128::from(timestamp_millis);
    let utc_days = timestamp_millis.div_euclid(MILLIS_PER_DAY);
    let millis_in_day = timestamp_millis.rem_euclid(MILLIS_PER_DAY);
    let (utc_year, utc_month, utc_day) = java_calendar_date_from_epoch_day(utc_days);
    let hour = millis_in_day / 3_600_000;
    let minute = (millis_in_day / 60_000) % 60;
    let second = (millis_in_day / 1_000) % 60;
    let millisecond = millis_in_day % 1_000;
    let offset_seconds = java_local_offset_seconds(
        timestamp_millis,
        utc_year,
        utc_month,
        utc_day,
        hour,
        minute,
        second,
    );
    let local_millis = timestamp_millis + i128::from(offset_seconds) * 1_000;
    let local_days = local_millis.div_euclid(MILLIS_PER_DAY);
    let millis_in_day = local_millis.rem_euclid(MILLIS_PER_DAY);
    let (year, month, day) = java_calendar_date_from_epoch_day(local_days);
    let year_of_era = if year > 0 { year } else { 1 - year };
    format!(
        "{year_of_era:04}-{month:02}-{day:02}#{:02}:{:02}:{:02}:{millisecond:03}",
        millis_in_day / 3_600_000,
        (millis_in_day / 60_000) % 60,
        (millis_in_day / 1_000) % 60,
    )
}

fn java_time_millis_to_human_string(timestamp_millis: i64) -> String {
    java_format_timestamp_millis(timestamp_millis)
        .chars()
        .filter(|character| character.is_ascii_digit())
        .collect()
}

fn java_local_offset_seconds(
    timestamp_millis: i128,
    year: i128,
    month: i128,
    day: i128,
    hour: i128,
    minute: i128,
    second: i128,
) -> i32 {
    let exact_datetime =
        time::OffsetDateTime::from_unix_timestamp_nanos(timestamp_millis * 1_000_000);
    if let Ok(datetime) = exact_datetime {
        if let Ok(offset) = time::UtcOffset::local_offset_at(datetime) {
            return offset.whole_seconds();
        }
    }

    if year < -9_999 {
        let winter = local_offset_for_gregorian_date(2_000, 1, 15, 12, 0, 0);
        let summer = local_offset_for_gregorian_date(2_000, 7, 15, 12, 0, 0);
        return winter.min(summer);
    }

    let representative_year = if year > 9_999 {
        2_000 + (year - 2_000).rem_euclid(400)
    } else {
        year
    };
    let day = day.min(days_in_gregorian_month(representative_year, month));
    local_offset_for_gregorian_date(representative_year, month, day, hour, minute, second)
}

fn local_offset_for_gregorian_date(
    year: i128,
    month: i128,
    day: i128,
    hour: i128,
    minute: i128,
    second: i128,
) -> i32 {
    let representative_days = gregorian_days_from_civil(year, month, day);
    let representative_seconds = representative_days * 86_400 + hour * 3_600 + minute * 60 + second;
    time::OffsetDateTime::from_unix_timestamp(representative_seconds as i64)
        .ok()
        .and_then(|datetime| time::UtcOffset::local_offset_at(datetime).ok())
        .unwrap_or(time::UtcOffset::UTC)
        .whole_seconds()
}

fn java_calendar_date_from_epoch_day(days: i128) -> (i128, i128, i128) {
    // GregorianCalendar switches from the Julian calendar after 1582-10-04.
    if days >= -141_427 {
        let shifted = days + 719_468;
        let era = shifted.div_euclid(146_097);
        let day_of_era = shifted - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let mut year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_prime = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
        let month = month_prime + if month_prime < 10 { 3 } else { -9 };
        year += i128::from(month <= 2);
        (year, month, day)
    } else {
        let mut days_in_cycle = days + 719_530;
        let cycle = days_in_cycle.div_euclid(1_461);
        days_in_cycle = days_in_cycle.rem_euclid(1_461);
        let mut year = cycle * 4;
        loop {
            let year_length = if year.rem_euclid(4) == 0 { 366 } else { 365 };
            if days_in_cycle < year_length {
                break;
            }
            days_in_cycle -= year_length;
            year += 1;
        }
        let mut month = 1;
        while days_in_cycle >= julian_month_length(year, month) {
            days_in_cycle -= julian_month_length(year, month);
            month += 1;
        }
        (year, month, days_in_cycle + 1)
    }
}

fn julian_month_length(year: i128, month: i128) -> i128 {
    match month {
        2 if year.rem_euclid(4) == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn gregorian_days_from_civil(year: i128, month: i128, day: i128) -> i128 {
    let year = year - i128::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn days_in_gregorian_month(year: i128, month: i128) -> i128 {
    match month {
        2 if year.rem_euclid(4) == 0
            && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0) =>
        {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Java response wrapper returned by `QUERY_CONSUME_TIME_SPAN`.
///
/// The list and each non-null row retain Java reference identity through
/// shared handles. The default list is non-null and empty; explicit null stays
/// null. The Java field itself is package-private, so the Rust field is private
/// and callers use the getter/setter methods.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueryConsumeTimeSpanBody;
/// let _ = QueryConsumeTimeSpanBody::default().consume_time_span_set;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumeTimeSpanBody {
    #[serde(
        default = "default_consume_time_span_set",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single",
        skip_serializing_if = "Option::is_none"
    )]
    consume_time_span_set: Option<SharedValue<Vec<Option<SharedValue<QueueTimeSpan>>>>>,
}

fn default_consume_time_span_set() -> Option<Vec<Option<QueueTimeSpan>>> {
    Some(Vec::new())
}

fn shared_consume_time_span_set(
    values: Option<Vec<Option<QueueTimeSpan>>>,
) -> Option<SharedValue<Vec<Option<SharedValue<QueueTimeSpan>>>>> {
    values.map(|values| {
        SharedValue::new(
            values
                .into_iter()
                .map(|value| value.map(SharedValue::new))
                .collect(),
        )
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryConsumeTimeSpanBodyFields {
    #[serde(
        default = "default_consume_time_span_set",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single"
    )]
    consume_time_span_set: Option<Vec<Option<QueueTimeSpan>>>,
}

impl<'de> Deserialize<'de> for QueryConsumeTimeSpanBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueryConsumeTimeSpanBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consume_time_span_set: shared_consume_time_span_set(fields.consume_time_span_set),
        })
    }
}

impl Default for QueryConsumeTimeSpanBody {
    fn default() -> Self {
        Self {
            consume_time_span_set: shared_consume_time_span_set(default_consume_time_span_set()),
        }
    }
}

/// Java `QueryCorrectionOffsetBody`, carrying queue IDs to corrected offsets.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryCorrectionOffsetBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    correction_offsets: Option<SharedValue<HashMap<i32, Option<i64>>>>,
}

fn default_correction_offsets() -> Option<HashMap<i32, Option<i64>>> {
    Some(HashMap::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct QueryCorrectionOffsetBodyFields {
    #[serde(default = "default_correction_offsets")]
    correction_offsets: Option<HashMap<i32, Option<i64>>>,
}

impl Default for QueryCorrectionOffsetBodyFields {
    fn default() -> Self {
        Self {
            correction_offsets: default_correction_offsets(),
        }
    }
}

impl<'de> Deserialize<'de> for QueryCorrectionOffsetBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueryCorrectionOffsetBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            correction_offsets: fields.correction_offsets.map(SharedValue::new),
        })
    }
}

impl Default for QueryCorrectionOffsetBody {
    fn default() -> Self {
        Self {
            correction_offsets: default_correction_offsets().map(SharedValue::new),
        }
    }
}

impl QueryCorrectionOffsetBody {
    pub fn get_correction_offsets(&self) -> Option<SharedValue<HashMap<i32, Option<i64>>>> {
        self.correction_offsets.clone()
    }

    pub fn set_correction_offsets(
        &mut self,
        value: Option<SharedValue<HashMap<i32, Option<i64>>>>,
    ) {
        self.correction_offsets = value;
    }
}

/// Java `HARuntimeInfo` returned by `GET_BROKER_HA_STATUS`.
///
/// The default connection list and client status match the Java constructor.
/// Shared handles preserve the mutable list and nested object references
/// returned by Java accessors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HaRuntimeInfo {
    master: bool,
    master_commit_log_max_offset: i64,
    in_sync_slave_nums: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    ha_connection_info: Option<SharedValue<Vec<Option<SharedValue<HaConnectionRuntimeInfo>>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ha_client_runtime_info: Option<SharedValue<HaClientRuntimeInfo>>,
}

impl Default for HaRuntimeInfo {
    fn default() -> Self {
        Self {
            master: false,
            master_commit_log_max_offset: 0,
            in_sync_slave_nums: 0,
            ha_connection_info: Some(SharedValue::new(Vec::new())),
            ha_client_runtime_info: Some(SharedValue::new(HaClientRuntimeInfo::default())),
        }
    }
}

impl HaRuntimeInfo {
    pub fn is_master(&self) -> bool {
        self.master
    }

    pub fn set_master(&mut self, value: bool) {
        self.master = value;
    }

    pub fn get_master_commit_log_max_offset(&self) -> i64 {
        self.master_commit_log_max_offset
    }

    pub fn set_master_commit_log_max_offset(&mut self, value: i64) {
        self.master_commit_log_max_offset = value;
    }

    pub fn get_in_sync_slave_nums(&self) -> i32 {
        self.in_sync_slave_nums
    }

    pub fn set_in_sync_slave_nums(&mut self, value: i32) {
        self.in_sync_slave_nums = value;
    }

    pub fn get_ha_connection_info(
        &self,
    ) -> Option<SharedValue<Vec<Option<SharedValue<HaConnectionRuntimeInfo>>>>> {
        self.ha_connection_info.clone()
    }

    pub fn set_ha_connection_info(
        &mut self,
        value: Option<SharedValue<Vec<Option<SharedValue<HaConnectionRuntimeInfo>>>>>,
    ) {
        self.ha_connection_info = value;
    }

    pub fn get_ha_client_runtime_info(&self) -> Option<SharedValue<HaClientRuntimeInfo>> {
        self.ha_client_runtime_info.clone()
    }

    pub fn set_ha_client_runtime_info(&mut self, value: Option<SharedValue<HaClientRuntimeInfo>>) {
        self.ha_client_runtime_info = value;
    }
}

/// Java `HARuntimeInfo.HAConnectionRuntimeInfo` nested DTO.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct HaConnectionRuntimeInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    addr: Option<String>,
    slave_ack_offset: i64,
    diff: i64,
    in_sync: bool,
    transferred_byte_in_second: i64,
    transfer_from_where: i64,
}

impl HaConnectionRuntimeInfo {
    pub fn get_addr(&self) -> Option<&str> {
        self.addr.as_deref()
    }

    pub fn set_addr(&mut self, value: Option<String>) {
        self.addr = value;
    }

    pub fn get_slave_ack_offset(&self) -> i64 {
        self.slave_ack_offset
    }

    pub fn set_slave_ack_offset(&mut self, value: i64) {
        self.slave_ack_offset = value;
    }

    pub fn get_diff(&self) -> i64 {
        self.diff
    }

    pub fn set_diff(&mut self, value: i64) {
        self.diff = value;
    }

    pub fn is_in_sync(&self) -> bool {
        self.in_sync
    }

    pub fn set_in_sync(&mut self, value: bool) {
        self.in_sync = value;
    }

    pub fn get_transferred_byte_in_second(&self) -> i64 {
        self.transferred_byte_in_second
    }

    pub fn set_transferred_byte_in_second(&mut self, value: i64) {
        self.transferred_byte_in_second = value;
    }

    pub fn get_transfer_from_where(&self) -> i64 {
        self.transfer_from_where
    }

    pub fn set_transfer_from_where(&mut self, value: i64) {
        self.transfer_from_where = value;
    }
}

/// Java `HARuntimeInfo.HAClientRuntimeInfo` nested DTO.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct HaClientRuntimeInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    master_addr: Option<String>,
    transferred_byte_in_second: i64,
    max_offset: i64,
    last_read_timestamp: i64,
    last_write_timestamp: i64,
    master_flush_offset: i64,
}

impl HaClientRuntimeInfo {
    pub fn get_master_addr(&self) -> Option<&str> {
        self.master_addr.as_deref()
    }

    pub fn set_master_addr(&mut self, value: Option<String>) {
        self.master_addr = value;
    }

    pub fn get_transferred_byte_in_second(&self) -> i64 {
        self.transferred_byte_in_second
    }

    pub fn set_transferred_byte_in_second(&mut self, value: i64) {
        self.transferred_byte_in_second = value;
    }

    pub fn get_max_offset(&self) -> i64 {
        self.max_offset
    }

    pub fn set_max_offset(&mut self, value: i64) {
        self.max_offset = value;
    }

    pub fn get_last_read_timestamp(&self) -> i64 {
        self.last_read_timestamp
    }

    pub fn set_last_read_timestamp(&mut self, value: i64) {
        self.last_read_timestamp = value;
    }

    pub fn get_last_write_timestamp(&self) -> i64 {
        self.last_write_timestamp
    }

    pub fn set_last_write_timestamp(&mut self, value: i64) {
        self.last_write_timestamp = value;
    }

    pub fn get_master_flush_offset(&self) -> i64 {
        self.master_flush_offset
    }

    pub fn set_master_flush_offset(&mut self, value: i64) {
        self.master_flush_offset = value;
    }
}

impl_java_remoting_serializable_adapters!(
    HaRuntimeInfo,
    HaConnectionRuntimeInfo,
    HaClientRuntimeInfo,
);

impl QueryConsumeTimeSpanBody {
    pub fn get_consume_time_span_set(
        &self,
    ) -> Option<SharedValue<Vec<Option<SharedValue<QueueTimeSpan>>>>> {
        self.consume_time_span_set.clone()
    }

    pub fn set_consume_time_span_set(
        &mut self,
        consume_time_span_set: Option<SharedValue<Vec<Option<SharedValue<QueueTimeSpan>>>>>,
    ) {
        self.consume_time_span_set = consume_time_span_set;
    }
}

/// Java response body returned by `QUERY_TOPICS_BY_CONSUMER`.
///
/// The topic set is mutable and the Java getter returns the stored set object,
/// so the Rust getter returns a shared handle. Java initializes the field to an
/// empty set, while a decoded explicit `null` remains null.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicList;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<TopicList>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicList;
/// let _ = TopicList::default().topic_list;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicList {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_list: Option<SharedValue<std::collections::HashSet<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    broker_addr: Option<String>,
}

/// Java `SubscriptionGroupList`, the request body for batched group creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionGroupList {
    group_config_list: Vec<SubscriptionGroupConfig>,
}

/// Java `TopicConfig`, encoded in `CreateTopicListRequestBody` for batch topic
/// creation. The field order matches Fastjson's alphabetic bean-property order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicConfig {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_topic_config_attributes",
        deserialize_with = "deserialize_topic_config_attributes"
    )]
    attributes: Option<SharedValue<TopicConfigAttributeMap>>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_bool")]
    order: bool,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    perm: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    read_queue_nums: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_filter_type: Option<TopicConfigFilterType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_name: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    topic_sys_flag: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    write_queue_nums: i32,
}

/// Java `TopicConfig.attributes`, including nullable keys and values.
/// `None` keys represent Java's null key; JSON object keys decode as `Some`.
pub type TopicConfigAttributeMap = IndexMap<Option<String>, Option<String>>;

fn java_topic_attribute_bucket(key: Option<&str>, capacity: usize) -> usize {
    let hash = key.map_or(0, |key| {
        key.encode_utf16().fold(0_u32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(u32::from(unit))
        })
    });
    ((hash ^ (hash >> 16)) as usize) & (capacity - 1)
}

fn java_topic_attribute_entries(
    attributes: &TopicConfigAttributeMap,
) -> Vec<(&Option<String>, &Option<String>)> {
    let mut capacity = 16_usize;
    while attributes.len() > (capacity * 3) / 4 {
        capacity *= 2;
    }
    let mut entries = attributes.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| java_topic_attribute_bucket(key.as_deref(), capacity));
    entries
}

struct JavaTopicAttributes<'a>(&'a TopicConfigAttributeMap);

impl Serialize for JavaTopicAttributes<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let entries = java_topic_attribute_entries(self.0)
            .into_iter()
            .filter(|(_, value)| value.is_some())
            .collect::<Vec<_>>();
        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, value) in entries {
            map.serialize_entry(key.as_deref().unwrap_or("null"), value)?;
        }
        map.end()
    }
}

fn deserialize_optional_topic_config_attribute_map<'de, D>(
    deserializer: D,
) -> Result<Option<TopicConfigAttributeMap>, D::Error>
where
    D: Deserializer<'de>,
{
    serde_fastjson::deserialize_optional_nullable_string_index_map(deserializer).map(|attributes| {
        attributes.map(|attributes| {
            attributes
                .into_iter()
                .map(|(key, value)| (Some(key), value))
                .collect()
        })
    })
}

fn serialize_topic_config_attributes<S>(
    attributes: &Option<SharedValue<TopicConfigAttributeMap>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match attributes {
        Some(attributes) => JavaTopicAttributes(&attributes.snapshot()).serialize(serializer),
        None => serializer.serialize_none(),
    }
}

static DEFAULT_READ_QUEUE_NUMS: AtomicI32 = AtomicI32::new(16);
static DEFAULT_WRITE_QUEUE_NUMS: AtomicI32 = AtomicI32::new(16);

fn deserialize_topic_config_attributes<'de, D>(
    deserializer: D,
) -> Result<Option<SharedValue<TopicConfigAttributeMap>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_topic_config_attribute_map(deserializer)
        .map(|attributes| attributes.map(SharedValue::new))
}

#[derive(Debug, thiserror::Error)]
pub enum TopicConfigError {
    #[error("java.lang.NullPointerException")]
    NullInput,
    #[error("java.lang.NullPointerException")]
    NullAttributes,
    #[error("java.lang.NullPointerException")]
    NullTopicMessageType,
    #[error("java.lang.NumberFormatException: For input string: \"{input}\"")]
    InvalidInteger { input: String },
    #[error(
        "java.lang.IllegalArgumentException: No enum constant org.apache.rocketmq.common.TopicFilterType.{0}"
    )]
    InvalidFilterType(String),
    #[error(transparent)]
    InvalidTopicMessageType(#[from] TopicMessageTypeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TopicConfigFilterType {
    SingleTag,
    MultiTag,
}

impl_java_enum_api!(
    TopicConfigFilterType,
    "org.apache.rocketmq.common.TopicFilterType",
    2,
    SingleTag => "SINGLE_TAG" = 0,
    MultiTag => "MULTI_TAG" = 1,
);

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            attributes: Some(SharedValue::new(IndexMap::new())),
            order: false,
            perm: 6,
            read_queue_nums: DEFAULT_READ_QUEUE_NUMS.load(Ordering::Acquire),
            topic_filter_type: Some(TopicConfigFilterType::SingleTag),
            topic_name: None,
            topic_sys_flag: 0,
            write_queue_nums: DEFAULT_WRITE_QUEUE_NUMS.load(Ordering::Acquire),
        }
    }
}

impl TopicConfig {
    pub fn default_read_queue_nums() -> i32 {
        DEFAULT_READ_QUEUE_NUMS.load(Ordering::Acquire)
    }

    pub fn set_default_read_queue_nums(value: i32) {
        DEFAULT_READ_QUEUE_NUMS.store(value, Ordering::Release);
    }

    pub fn default_write_queue_nums() -> i32 {
        DEFAULT_WRITE_QUEUE_NUMS.load(Ordering::Acquire)
    }

    pub fn set_default_write_queue_nums(value: i32) {
        DEFAULT_WRITE_QUEUE_NUMS.store(value, Ordering::Release);
    }

    pub fn new(topic_name: impl Into<String>) -> Self {
        Self {
            topic_name: Some(topic_name.into()),
            ..Self::default()
        }
    }

    /// Java's `(String, int, int)` constructor.
    pub fn with_queue_counts(
        topic_name: impl Into<String>,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> Self {
        Self {
            topic_name: Some(topic_name.into()),
            read_queue_nums,
            write_queue_nums,
            ..Self::default()
        }
    }

    /// Java's `(String, int, int, int)` constructor.
    pub fn with_perm(
        topic_name: impl Into<String>,
        read_queue_nums: i32,
        write_queue_nums: i32,
        perm: i32,
    ) -> Self {
        Self {
            perm,
            ..Self::with_queue_counts(topic_name, read_queue_nums, write_queue_nums)
        }
    }

    /// Java's `(String, int, int, int, int)` constructor.
    pub fn with_topic_sys_flag(
        topic_name: impl Into<String>,
        read_queue_nums: i32,
        write_queue_nums: i32,
        perm: i32,
        topic_sys_flag: i32,
    ) -> Self {
        Self {
            topic_sys_flag,
            ..Self::with_perm(topic_name, read_queue_nums, write_queue_nums, perm)
        }
    }

    /// Java's copy constructor retains the same mutable attributes map.
    pub fn copy_from(other: &Self) -> Self {
        other.clone()
    }

    pub fn topic_name(&self) -> Option<&str> {
        self.topic_name.as_deref()
    }

    pub fn set_topic_name(&mut self, topic_name: Option<String>) {
        self.topic_name = topic_name;
    }

    pub fn read_queue_nums(&self) -> i32 {
        self.read_queue_nums
    }

    pub fn set_read_queue_nums(&mut self, read_queue_nums: i32) {
        self.read_queue_nums = read_queue_nums;
    }

    pub fn write_queue_nums(&self) -> i32 {
        self.write_queue_nums
    }

    pub fn set_write_queue_nums(&mut self, write_queue_nums: i32) {
        self.write_queue_nums = write_queue_nums;
    }

    pub fn perm(&self) -> i32 {
        self.perm
    }

    pub fn set_perm(&mut self, perm: i32) {
        self.perm = perm;
    }

    pub fn topic_filter_type(&self) -> Option<TopicConfigFilterType> {
        self.topic_filter_type
    }

    pub fn set_topic_filter_type(&mut self, topic_filter_type: Option<TopicConfigFilterType>) {
        self.topic_filter_type = topic_filter_type;
    }

    pub fn topic_sys_flag(&self) -> i32 {
        self.topic_sys_flag
    }

    pub fn set_topic_sys_flag(&mut self, topic_sys_flag: i32) {
        self.topic_sys_flag = topic_sys_flag;
    }

    pub fn is_order(&self) -> bool {
        self.order
    }

    pub fn set_order(&mut self, order: bool) {
        self.order = order;
    }

    pub fn attributes(&self) -> Option<SharedValue<TopicConfigAttributeMap>> {
        self.attributes.clone()
    }

    pub fn set_attributes(&mut self, attributes: Option<TopicConfigAttributeMap>) {
        self.attributes = attributes.map(SharedValue::new);
    }

    /// Sets the same live attributes map handle that Java's setter retains.
    pub fn set_attributes_shared(
        &mut self,
        attributes: Option<SharedValue<TopicConfigAttributeMap>>,
    ) {
        self.attributes = attributes;
    }

    pub fn topic_message_type(&self) -> Result<TopicMessageType, TopicConfigError> {
        let Some(attributes) = &self.attributes else {
            return Ok(TopicMessageType::Normal);
        };
        attributes
            .with(|values| {
                values
                    .get(&Some(TOPIC_MESSAGE_TYPE_ATTRIBUTE.to_owned()))
                    .cloned()
            })
            .flatten()
            .map_or(Ok(TopicMessageType::Normal), |value| {
                TopicMessageType::value_of_name(Some(&value))
                    .map_err(TopicConfigError::InvalidTopicMessageType)
            })
    }

    pub fn set_topic_message_type(
        &mut self,
        topic_message_type: Option<TopicMessageType>,
    ) -> Result<(), TopicConfigError> {
        let topic_message_type =
            topic_message_type.ok_or(TopicConfigError::NullTopicMessageType)?;
        let attributes = self
            .attributes
            .as_ref()
            .ok_or(TopicConfigError::NullAttributes)?;
        attributes.with_mut(|values| {
            values.insert(
                Some(TOPIC_MESSAGE_TYPE_ATTRIBUTE.to_owned()),
                Some(topic_message_type.get_value().to_owned()),
            );
        });
        Ok(())
    }

    pub fn set_lite_topic_expiration(
        &mut self,
        lite_topic_expiration: i32,
    ) -> Result<(), TopicConfigError> {
        if self.topic_message_type()? != TopicMessageType::Lite {
            return Ok(());
        }
        let attributes = self
            .attributes
            .as_ref()
            .ok_or(TopicConfigError::NullAttributes)?;
        attributes.with_mut(|values| {
            values.insert(
                Some(LITE_EXPIRATION_ATTRIBUTE.to_owned()),
                Some(lite_topic_expiration.to_string()),
            );
        });
        Ok(())
    }

    pub fn lite_topic_expiration(&self) -> Result<i32, TopicConfigError> {
        if self.topic_message_type()? != TopicMessageType::Lite {
            return Ok(-1);
        }
        let attributes = self
            .attributes
            .as_ref()
            .ok_or(TopicConfigError::NullAttributes)?;
        Ok(attributes
            .with(|values| {
                values
                    .get(&Some(LITE_EXPIRATION_ATTRIBUTE.to_owned()))
                    .cloned()
            })
            .flatten()
            .as_deref()
            .and_then(crate::header::parse_java_i32)
            .unwrap_or(-1))
    }

    /// Encodes the whitespace-separated Java `TopicConfig` form.
    pub fn encode(&self) -> Result<String, TopicConfigError> {
        let attributes = match &self.attributes {
            Some(attributes) => {
                serde_json::to_string(&JavaTopicAttributes(&attributes.snapshot()))?
            }
            None => String::new(),
        };
        Ok(format!(
            "{} {} {} {} {} {}",
            self.topic_name.as_deref().unwrap_or("null"),
            self.read_queue_nums,
            self.write_queue_nums,
            self.perm,
            self.topic_filter_type
                .map_or("null", |topic_filter_type| topic_filter_type.name()),
            attributes,
        ))
    }

    /// Java's mutating decode operation; fields are applied in source order.
    pub fn decode(&mut self, input: &str) -> Result<bool, TopicConfigError> {
        let mut fields = input.split(' ').collect::<Vec<_>>();
        while fields.last() == Some(&"") {
            fields.pop();
        }
        if fields.len() < 5 {
            return Ok(false);
        }
        self.topic_name = Some(fields[0].to_owned());
        self.read_queue_nums = crate::header::parse_java_i32(fields[1]).ok_or_else(|| {
            TopicConfigError::InvalidInteger {
                input: fields[1].to_owned(),
            }
        })?;
        self.write_queue_nums = crate::header::parse_java_i32(fields[2]).ok_or_else(|| {
            TopicConfigError::InvalidInteger {
                input: fields[2].to_owned(),
            }
        })?;
        self.perm = crate::header::parse_java_i32(fields[3]).ok_or_else(|| {
            TopicConfigError::InvalidInteger {
                input: fields[3].to_owned(),
            }
        })?;
        self.topic_filter_type = Some(match fields[4] {
            "SINGLE_TAG" => TopicConfigFilterType::SingleTag,
            "MULTI_TAG" => TopicConfigFilterType::MultiTag,
            other => return Err(TopicConfigError::InvalidFilterType(other.to_owned())),
        });
        if fields.len() >= 6 {
            if let Ok(attributes) =
                serde_fastjson::from_fastjson_str::<IndexMap<String, Option<String>>>(fields[5])
            {
                self.attributes = Some(SharedValue::new(
                    attributes
                        .into_iter()
                        .map(|(key, value)| (Some(key), value))
                        .collect(),
                ));
            }
        }
        Ok(true)
    }

    pub fn decode_nullable(input: Option<&str>) -> Result<bool, TopicConfigError> {
        let input = input.ok_or(TopicConfigError::NullInput)?;
        let mut config = Self::default();
        config.decode(input)
    }

    pub fn java_to_string(&self) -> String {
        let attributes = self.attributes.as_ref().map_or_else(
            || "null".to_owned(),
            |attributes| {
                let values = attributes.snapshot();
                format!(
                    "{{{}}}",
                    java_topic_attribute_entries(&values)
                        .iter()
                        .map(|(key, value)| {
                            format!(
                                "{}={}",
                                key.as_deref().unwrap_or("null"),
                                value.as_deref().unwrap_or("null")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            },
        );
        format!(
            "TopicConfig [topicName={}, readQueueNums={}, writeQueueNums={}, perm={}, topicFilterType={}, topicSysFlag={}, order={}, attributes={}]",
            self.topic_name.as_deref().unwrap_or("null"),
            self.read_queue_nums,
            self.write_queue_nums,
            PermName::perm_to_string(self.perm),
            self.topic_filter_type
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
            self.topic_sys_flag,
            self.order,
            attributes,
        )
    }
}

impl PartialEq for TopicConfig {
    fn eq(&self, other: &Self) -> bool {
        self.attributes.as_ref().map(SharedValue::snapshot)
            == other.attributes.as_ref().map(SharedValue::snapshot)
            && self.order == other.order
            && self.perm == other.perm
            && self.read_queue_nums == other.read_queue_nums
            && self.topic_filter_type == other.topic_filter_type
            && self.topic_name == other.topic_name
            && self.topic_sys_flag == other.topic_sys_flag
            && self.write_queue_nums == other.write_queue_nums
    }
}

impl Eq for TopicConfig {}

impl std::hash::Hash for TopicConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let attribute_entry_hashes = self.attributes.as_ref().map(|attributes| {
            let mut hashes = attributes
                .snapshot()
                .iter()
                .map(|(key, value)| {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    key.hash(&mut hasher);
                    value.hash(&mut hasher);
                    hasher.finish()
                })
                .collect::<Vec<_>>();
            hashes.sort_unstable();
            hashes
        });
        attribute_entry_hashes.hash(state);
        self.order.hash(state);
        self.perm.hash(state);
        self.read_queue_nums.hash(state);
        self.topic_filter_type.hash(state);
        self.topic_name.hash(state);
        self.topic_sys_flag.hash(state);
        self.write_queue_nums.hash(state);
    }
}

/// Java's `DataVersion`, the version token carried by paginated metadata RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DataVersion {
    state_version: i64,
    timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    counter: Option<i64>,
}

impl Default for DataVersion {
    fn default() -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| {
                duration.as_millis().min(i64::MAX as u128) as i64
            });
        Self {
            state_version: 0,
            timestamp,
            counter: Some(0),
        }
    }
}

impl DataVersion {
    pub fn new(state_version: i64, timestamp: i64, counter: Option<i64>) -> Self {
        Self {
            state_version,
            timestamp,
            counter,
        }
    }

    pub fn state_version(&self) -> i64 {
        self.state_version
    }

    pub fn get_state_version(&self) -> i64 {
        self.state_version
    }

    pub fn set_state_version(&mut self, value: i64) {
        self.state_version = value;
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    pub fn set_timestamp(&mut self, value: i64) {
        self.timestamp = value;
    }

    /// Returns the current counter value, matching `AtomicLong.get()`.
    pub fn counter(&self) -> Option<i64> {
        self.counter
    }

    pub fn get_counter(&self) -> Option<i64> {
        self.counter
    }

    pub fn set_counter(&mut self, value: Option<i64>) {
        self.counter = value;
    }

    pub fn assign_new_one(&mut self, other: &Self) {
        self.timestamp = other.timestamp;
        self.state_version = other.state_version;
        self.counter = other.counter;
    }

    pub fn next_version(&mut self) -> bool {
        self.next_version_with_state(0)
    }

    /// Updates the timestamp and state version and increments the counter.
    /// Returns `false` when the Java `AtomicLong` field has been set to null.
    pub fn next_version_with_state(&mut self, state_version: i64) -> bool {
        let Some(counter) = self.counter.as_mut() else {
            return false;
        };
        self.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| {
                duration.as_millis().min(i64::MAX as u128) as i64
            });
        self.state_version = state_version;
        *counter = counter.wrapping_add(1);
        true
    }

    /// Compares state version, counter, then timestamp as Java does.
    /// Returns `None` if either counter is null when the comparison reaches it.
    pub fn compare(&self, other: &Self) -> Option<i32> {
        let state_order = self.state_version.cmp(&other.state_version);
        if state_order != std::cmp::Ordering::Equal {
            return Some(ordering_to_i32(state_order));
        }
        let counter_order = self.counter?.cmp(&other.counter?);
        if counter_order != std::cmp::Ordering::Equal {
            return Some(ordering_to_i32(counter_order));
        }
        Some(ordering_to_i32(self.timestamp.cmp(&other.timestamp)))
    }

    pub fn java_hash_code(&self) -> i32 {
        let mut result = fold_i64_hash(self.state_version);
        result = result
            .wrapping_mul(31)
            .wrapping_add(fold_i64_hash(self.timestamp));
        if let Some(counter) = self.counter {
            result = result.wrapping_mul(31).wrapping_add(fold_i64_hash(counter));
        }
        result
    }

    pub fn java_to_string(&self) -> String {
        let counter = self
            .counter
            .map_or_else(|| "null".to_owned(), |value| value.to_string());
        format!(
            "DataVersion[timestamp={}, counter={counter}]",
            self.timestamp
        )
    }
}

fn ordering_to_i32(ordering: std::cmp::Ordering) -> i32 {
    match ordering {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn fold_i64_hash(value: i64) -> i32 {
    (value ^ (value >> 32)) as i32
}

/// Java's `TopicConfigSerializeWrapper` response model for Broker metadata.
/// Java inherits identity equality and hashing from `Object`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicConfigSerializeWrapper;
/// let first = TopicConfigSerializeWrapper::default();
/// let second = TopicConfigSerializeWrapper::default();
/// assert_eq!(first, second);
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigSerializeWrapper {
    #[serde(skip_serializing_if = "Option::is_none")]
    data_version: Option<DataVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_config_table: Option<BTreeMap<String, TopicConfig>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct TopicConfigSerializeWrapperFields {
    data_version: Option<DataVersion>,
    topic_config_table: Option<BTreeMap<String, TopicConfig>>,
}

impl Default for TopicConfigSerializeWrapperFields {
    fn default() -> Self {
        Self {
            data_version: Some(DataVersion::default()),
            topic_config_table: Some(BTreeMap::new()),
        }
    }
}

impl<'de> Deserialize<'de> for TopicConfigSerializeWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = TopicConfigSerializeWrapperFields::deserialize(deserializer)?;
        Ok(Self {
            data_version: fields.data_version,
            topic_config_table: fields.topic_config_table,
        })
    }
}

impl Default for TopicConfigSerializeWrapper {
    fn default() -> Self {
        Self {
            data_version: Some(DataVersion::default()),
            topic_config_table: Some(BTreeMap::new()),
        }
    }
}

impl TopicConfigSerializeWrapper {
    pub fn new(
        data_version: Option<DataVersion>,
        topic_config_table: Option<BTreeMap<String, TopicConfig>>,
    ) -> Self {
        Self {
            data_version,
            topic_config_table,
        }
    }

    pub fn data_version(&self) -> Option<&DataVersion> {
        self.data_version.as_ref()
    }

    pub fn get_data_version(&self) -> Option<&DataVersion> {
        self.data_version()
    }

    pub fn set_data_version(&mut self, value: Option<DataVersion>) {
        self.data_version = value;
    }

    pub fn topic_config_table(&self) -> Option<&BTreeMap<String, TopicConfig>> {
        self.topic_config_table.as_ref()
    }

    pub fn get_topic_config_table(&self) -> Option<&BTreeMap<String, TopicConfig>> {
        self.topic_config_table()
    }

    pub fn set_topic_config_table(&mut self, value: Option<BTreeMap<String, TopicConfig>>) {
        self.topic_config_table = value;
    }
}

/// Java `SubscriptionGroupWrapper`, returned by the paginated Broker snapshot
/// RPC. The nested maps use ordered Rust maps while preserving nullable table
/// fields and Java constructor defaults. Java inherits identity equality from
/// `Object`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::SubscriptionGroupWrapper;
/// let first = SubscriptionGroupWrapper::default();
/// let second = SubscriptionGroupWrapper::default();
/// assert_eq!(first, second);
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionGroupWrapper {
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_group_table: Option<BTreeMap<String, SubscriptionGroupConfig>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    forbidden_table: Option<BTreeMap<String, BTreeMap<String, i32>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_version: Option<DataVersion>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SubscriptionGroupWrapperFields {
    subscription_group_table: Option<BTreeMap<String, SubscriptionGroupConfig>>,
    forbidden_table: Option<BTreeMap<String, BTreeMap<String, i32>>>,
    data_version: Option<DataVersion>,
}

impl Default for SubscriptionGroupWrapperFields {
    fn default() -> Self {
        Self {
            subscription_group_table: Some(BTreeMap::new()),
            forbidden_table: Some(BTreeMap::new()),
            data_version: Some(DataVersion::default()),
        }
    }
}

impl<'de> Deserialize<'de> for SubscriptionGroupWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = SubscriptionGroupWrapperFields::deserialize(deserializer)?;
        Ok(Self {
            subscription_group_table: fields.subscription_group_table,
            forbidden_table: fields.forbidden_table,
            data_version: fields.data_version,
        })
    }
}

impl Default for SubscriptionGroupWrapper {
    fn default() -> Self {
        Self {
            subscription_group_table: Some(BTreeMap::new()),
            forbidden_table: Some(BTreeMap::new()),
            data_version: Some(DataVersion::default()),
        }
    }
}

impl SubscriptionGroupWrapper {
    pub fn new(
        data_version: Option<DataVersion>,
        subscription_group_table: Option<BTreeMap<String, SubscriptionGroupConfig>>,
        forbidden_table: Option<BTreeMap<String, BTreeMap<String, i32>>>,
    ) -> Self {
        Self {
            subscription_group_table,
            forbidden_table,
            data_version,
        }
    }

    pub fn subscription_group_table(&self) -> Option<&BTreeMap<String, SubscriptionGroupConfig>> {
        self.subscription_group_table.as_ref()
    }

    pub fn get_subscription_group_table(
        &self,
    ) -> Option<&BTreeMap<String, SubscriptionGroupConfig>> {
        self.subscription_group_table()
    }

    pub fn set_subscription_group_table(
        &mut self,
        value: Option<BTreeMap<String, SubscriptionGroupConfig>>,
    ) {
        self.subscription_group_table = value;
    }

    pub fn forbidden_table(&self) -> Option<&BTreeMap<String, BTreeMap<String, i32>>> {
        self.forbidden_table.as_ref()
    }

    pub fn get_forbidden_table(&self) -> Option<&BTreeMap<String, BTreeMap<String, i32>>> {
        self.forbidden_table()
    }

    pub fn set_forbidden_table(&mut self, value: Option<BTreeMap<String, BTreeMap<String, i32>>>) {
        self.forbidden_table = value;
    }

    pub fn data_version(&self) -> Option<&DataVersion> {
        self.data_version.as_ref()
    }

    pub fn get_data_version(&self) -> Option<&DataVersion> {
        self.data_version()
    }

    pub fn set_data_version(&mut self, value: Option<DataVersion>) {
        self.data_version = value;
    }
}

/// One physical generation in Java's `LogicQueueMappingItem` static-topic
/// metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LogicQueueMappingItem {
    #[serde(rename = "gen", deserialize_with = "serde_fastjson::deserialize_i32")]
    generation: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    queue_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    bname: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    logic_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    start_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    end_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    time_of_start: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    time_of_end: i64,
}

impl Default for LogicQueueMappingItem {
    fn default() -> Self {
        Self {
            generation: 0,
            queue_id: 0,
            bname: None,
            logic_offset: 0,
            start_offset: 0,
            end_offset: -1,
            time_of_start: -1,
            time_of_end: -1,
        }
    }
}

impl LogicQueueMappingItem {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        generation: i32,
        queue_id: i32,
        bname: Option<String>,
        logic_offset: i64,
        start_offset: i64,
        end_offset: i64,
        time_of_start: i64,
        time_of_end: i64,
    ) -> Self {
        Self {
            generation,
            queue_id,
            bname,
            logic_offset,
            start_offset,
            end_offset,
            time_of_start,
            time_of_end,
        }
    }

    pub fn generation(&self) -> i32 {
        self.generation
    }

    pub fn get_gen(&self) -> i32 {
        self.generation
    }

    pub fn set_generation(&mut self, value: i32) {
        self.generation = value;
    }

    pub fn set_gen(&mut self, value: i32) {
        self.generation = value;
    }

    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: i32) {
        self.queue_id = value;
    }

    pub fn bname(&self) -> Option<&str> {
        self.bname.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.bname = value;
    }

    pub fn logic_offset(&self) -> i64 {
        self.logic_offset
    }

    pub fn set_logic_offset(&mut self, value: i64) {
        self.logic_offset = value;
    }

    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    pub fn set_start_offset(&mut self, value: i64) {
        self.start_offset = value;
    }

    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }

    pub fn set_end_offset(&mut self, value: i64) {
        self.end_offset = value;
    }

    pub fn time_of_start(&self) -> i64 {
        self.time_of_start
    }

    pub fn set_time_of_start(&mut self, value: i64) {
        self.time_of_start = value;
    }

    pub fn time_of_end(&self) -> i64 {
        self.time_of_end
    }

    pub fn set_time_of_end(&mut self, value: i64) {
        self.time_of_end = value;
    }

    pub fn compute_static_queue_offset_loosely(&self, physical_queue_offset: i64) -> i64 {
        if self.logic_offset < 0 {
            return -1;
        }
        if physical_queue_offset < self.start_offset {
            return self.logic_offset;
        }
        if self.end_offset >= self.start_offset && self.end_offset < physical_queue_offset {
            return self
                .logic_offset
                .wrapping_add(self.end_offset.wrapping_sub(self.start_offset));
        }
        self.logic_offset
            .wrapping_add(physical_queue_offset.wrapping_sub(self.start_offset))
    }

    pub fn compute_static_queue_offset_strictly(&self, physical_queue_offset: i64) -> i64 {
        if physical_queue_offset < self.start_offset {
            return self.logic_offset;
        }
        self.logic_offset
            .wrapping_add(physical_queue_offset.wrapping_sub(self.start_offset))
    }

    pub fn compute_physical_queue_offset(&self, static_queue_offset: i64) -> i64 {
        static_queue_offset
            .wrapping_sub(self.logic_offset)
            .wrapping_add(self.start_offset)
    }

    pub fn compute_max_static_queue_offset(&self) -> i64 {
        if self.end_offset >= self.start_offset {
            self.logic_offset
                .wrapping_add(self.end_offset.wrapping_sub(self.start_offset))
        } else {
            self.logic_offset
        }
    }

    pub fn check_if_end_offset_decided(&self) -> bool {
        self.end_offset > self.start_offset
    }

    pub fn check_if_logic_offset_decided(&self) -> bool {
        self.logic_offset >= 0
    }

    pub fn compute_offset_delta(&self) -> i64 {
        self.logic_offset.wrapping_sub(self.start_offset)
    }

    /// Returns the pinned Java 5.5.1 `LogicQueueMappingItem.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = 17i32;
        result = result.wrapping_mul(37).wrapping_add(self.generation);
        result = result.wrapping_mul(37).wrapping_add(self.queue_id);
        result = result.wrapping_mul(37).wrapping_add(
            self.bname
                .as_deref()
                .map(cluster_info_java_string_hash_code)
                .unwrap_or(0),
        );
        result = result
            .wrapping_mul(37)
            .wrapping_add(fold_i64_hash(self.logic_offset));
        result = result
            .wrapping_mul(37)
            .wrapping_add(fold_i64_hash(self.start_offset));
        result = result
            .wrapping_mul(37)
            .wrapping_add(fold_i64_hash(self.end_offset));
        result = result
            .wrapping_mul(37)
            .wrapping_add(fold_i64_hash(self.time_of_start));
        result = result
            .wrapping_mul(37)
            .wrapping_add(fold_i64_hash(self.time_of_end));
        result
    }

    /// Returns the pinned Java 5.5.1 `LogicQueueMappingItem.toString` text.
    pub fn java_to_string(&self) -> String {
        format!(
            "LogicQueueMappingItem{{gen={}, queueId={}, bname='{}', logicOffset={}, startOffset={}, endOffset={}, timeOfStart={}, timeOfEnd={}}}",
            self.generation,
            self.queue_id,
            self.bname.as_deref().unwrap_or("null"),
            self.logic_offset,
            self.start_offset,
            self.end_offset,
            self.time_of_start,
            self.time_of_end
        )
    }
}

pub type HostedQueueMappingItems = SharedValue<Vec<Option<LogicQueueMappingItem>>>;
pub type HostedQueues = BTreeMap<i32, Option<HostedQueueMappingItems>>;
pub type SharedHostedQueues = SharedValue<HostedQueues>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopicQueueMappingDetailError {
    #[error("java.lang.NullPointerException: null TopicQueueMappingDetail")]
    NullMappingDetail,
    #[error("java.lang.NullPointerException: null hostedQueues")]
    NullHostedQueues,
    #[error("java.lang.NullPointerException: null global queue id")]
    NullGlobalId,
    #[error("java.lang.NullPointerException: null mapping item list")]
    NullMappingItems,
    #[error("java.lang.NullPointerException: null mapping item")]
    NullMappingItem,
    #[error("java.lang.NullPointerException: null broker name")]
    NullBrokerName,
}

fn hosted_queues_snapshot(
    value: &SharedHostedQueues,
) -> BTreeMap<i32, Option<Vec<Option<LogicQueueMappingItem>>>> {
    value.with(|queues| {
        queues
            .iter()
            .map(|(queue_id, items)| (*queue_id, items.as_ref().map(SharedValue::snapshot)))
            .collect()
    })
}

fn default_hosted_queues() -> Option<SharedHostedQueues> {
    Some(SharedValue::new(BTreeMap::new()))
}

fn deserialize_hosted_queues<'de, D>(
    deserializer: D,
) -> Result<Option<SharedHostedQueues>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let normalized = serde_fastjson::normalize_fastjson_integer_map_keys(raw.get());
    let queues = serde_json::from_str::<HostedQueues>(&normalized).map_err(de::Error::custom)?;
    Ok(Some(SharedValue::new(queues)))
}

/// Java `TopicQueueMappingDetail`, including its inherited mapping fields and
/// broker-local hosted queue generations.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueMappingDetail {
    #[serde(flatten)]
    mapping_info: TopicQueueMappingInfo,
    #[serde(
        default = "default_hosted_queues",
        deserialize_with = "deserialize_hosted_queues",
        skip_serializing_if = "Option::is_none"
    )]
    hosted_queues: Option<SharedHostedQueues>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct TopicQueueMappingDetailFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default = "default_topic_mapping_scope",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    scope: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    total_queues: i32,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    bname: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    epoch: i64,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    dirty: bool,
    #[serde(
        default = "default_topic_mapping_ids",
        deserialize_with = "serde_fastjson::deserialize_optional_int_map"
    )]
    curr_id_map: Option<BTreeMap<i32, i32>>,
    #[serde(
        default = "default_hosted_queues",
        deserialize_with = "deserialize_hosted_queues"
    )]
    hosted_queues: Option<SharedHostedQueues>,
}

impl Default for TopicQueueMappingDetailFields {
    fn default() -> Self {
        Self {
            topic: None,
            scope: default_topic_mapping_scope(),
            total_queues: 0,
            bname: None,
            epoch: 0,
            dirty: false,
            curr_id_map: default_topic_mapping_ids(),
            hosted_queues: default_hosted_queues(),
        }
    }
}

fn default_topic_mapping_scope() -> Option<String> {
    Some("__global__".to_owned())
}

fn default_topic_mapping_ids() -> Option<BTreeMap<i32, i32>> {
    Some(BTreeMap::new())
}

impl<'de> Deserialize<'de> for TopicQueueMappingDetail {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        let normalized = serde_fastjson::normalize_fastjson_integer_map_keys(raw.get());
        let fields = serde_json::from_str::<TopicQueueMappingDetailFields>(&normalized)
            .map_err(de::Error::custom)?;
        Ok(Self {
            mapping_info: TopicQueueMappingInfo {
                topic: fields.topic,
                total_queues: fields.total_queues,
                bname: fields.bname,
                epoch: fields.epoch,
                dirty: fields.dirty,
                scope: fields.scope,
                curr_id_map: fields.curr_id_map.map(SharedValue::new),
            },
            hosted_queues: fields.hosted_queues,
        })
    }
}

impl Default for TopicQueueMappingDetail {
    fn default() -> Self {
        Self {
            mapping_info: TopicQueueMappingInfo::default(),
            hosted_queues: default_hosted_queues(),
        }
    }
}

impl TopicQueueMappingDetail {
    /// Inherited pinned Java `TopicQueueMappingInfo.LEVEL_0` constant.
    pub const LEVEL_0: i32 = TopicQueueMappingInfo::LEVEL_0;

    /// Constructs a detail using the pinned Java constructor defaults.
    pub fn new(
        topic: Option<String>,
        total_queues: i32,
        bname: Option<String>,
        epoch: i64,
    ) -> Self {
        Self {
            mapping_info: TopicQueueMappingInfo::new(topic, total_queues, bname, epoch),
            hosted_queues: default_hosted_queues(),
        }
    }

    pub fn topic_queue_mapping_info(&self) -> &TopicQueueMappingInfo {
        &self.mapping_info
    }

    pub fn topic_queue_mapping_info_mut(&mut self) -> &mut TopicQueueMappingInfo {
        &mut self.mapping_info
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.mapping_info.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.mapping_info.topic = value;
    }

    pub fn get_scope(&self) -> Option<&str> {
        self.mapping_info.scope.as_deref()
    }

    pub fn set_scope(&mut self, value: Option<String>) {
        self.mapping_info.scope = value;
    }

    pub fn get_total_queues(&self) -> i32 {
        self.mapping_info.total_queues
    }

    pub fn set_total_queues(&mut self, value: i32) {
        self.mapping_info.total_queues = value;
    }

    pub fn get_bname(&self) -> Option<&str> {
        self.mapping_info.bname.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.mapping_info.bname = value;
    }

    pub fn get_epoch(&self) -> i64 {
        self.mapping_info.epoch
    }

    pub fn set_epoch(&mut self, value: i64) {
        self.mapping_info.epoch = value;
    }

    pub fn is_dirty(&self) -> bool {
        self.mapping_info.dirty
    }

    pub fn set_dirty(&mut self, value: bool) {
        self.mapping_info.dirty = value;
    }

    pub fn get_curr_id_map(&self) -> Option<SharedValue<BTreeMap<i32, i32>>> {
        self.mapping_info.get_curr_id_map()
    }

    pub fn set_curr_id_map(&mut self, value: Option<SharedValue<BTreeMap<i32, i32>>>) {
        self.mapping_info.set_curr_id_map(value);
    }

    pub fn set_curr_id_map_owned(&mut self, value: Option<BTreeMap<i32, i32>>) {
        self.mapping_info.set_curr_id_map_owned(value);
    }

    pub fn get_hosted_queues(&self) -> Option<SharedHostedQueues> {
        self.hosted_queues.clone()
    }

    pub fn set_hosted_queues(&mut self, value: Option<SharedHostedQueues>) {
        self.hosted_queues = value;
    }

    pub fn set_hosted_queues_owned(&mut self, value: Option<HostedQueues>) {
        self.hosted_queues = value.map(SharedValue::new);
    }

    /// Stores the caller's list handle under a global queue ID, matching
    /// Java's `putMappingInfo`; an empty list is accepted without mutation.
    pub fn put_mapping_info(
        mapping_detail: Option<&Self>,
        global_id: Option<i32>,
        mapping_info: Option<HostedQueueMappingItems>,
    ) -> Result<bool, TopicQueueMappingDetailError> {
        let mapping_info = mapping_info.ok_or(TopicQueueMappingDetailError::NullMappingItems)?;
        if mapping_info.with(Vec::is_empty) {
            return Ok(true);
        }
        let mapping_detail =
            mapping_detail.ok_or(TopicQueueMappingDetailError::NullMappingDetail)?;
        let hosted_queues = mapping_detail
            .hosted_queues
            .as_ref()
            .ok_or(TopicQueueMappingDetailError::NullHostedQueues)?;
        let global_id = global_id.ok_or(TopicQueueMappingDetailError::NullGlobalId)?;
        hosted_queues.with_mut(|queues| {
            queues.insert(global_id, Some(mapping_info));
        });
        Ok(true)
    }

    /// Returns the stored list handle for a global queue ID.
    pub fn get_mapping_info(
        mapping_detail: Option<&Self>,
        global_id: Option<i32>,
    ) -> Result<Option<HostedQueueMappingItems>, TopicQueueMappingDetailError> {
        let mapping_detail =
            mapping_detail.ok_or(TopicQueueMappingDetailError::NullMappingDetail)?;
        let hosted_queues = mapping_detail
            .hosted_queues
            .as_ref()
            .ok_or(TopicQueueMappingDetailError::NullHostedQueues)?;
        let global_id = global_id.ok_or(TopicQueueMappingDetailError::NullGlobalId)?;
        Ok(hosted_queues.with(|queues| queues.get(&global_id).cloned().flatten()))
    }

    /// Builds the current physical queue ID map. Java's disabled-by-default
    /// assertion leaves nonzero levels with an empty result.
    pub fn build_id_map(
        mapping_detail: Option<&Self>,
        level: i32,
    ) -> Result<BTreeMap<i32, i32>, TopicQueueMappingDetailError> {
        let mapping_detail =
            mapping_detail.ok_or(TopicQueueMappingDetailError::NullMappingDetail)?;
        let Some(hosted_queues) = mapping_detail.hosted_queues.as_ref() else {
            return Ok(BTreeMap::new());
        };
        hosted_queues.with(|queues| {
            let mut id_map = BTreeMap::new();
            for (global_id, items) in queues {
                if level != 0 {
                    continue;
                }
                let items = items
                    .as_ref()
                    .ok_or(TopicQueueMappingDetailError::NullMappingItems)?;
                let items = items.snapshot();
                let Some(item) = items.last() else {
                    continue;
                };
                let item = item
                    .as_ref()
                    .ok_or(TopicQueueMappingDetailError::NullMappingItem)?;
                let broker_name = mapping_detail
                    .mapping_info
                    .bname
                    .as_deref()
                    .ok_or(TopicQueueMappingDetailError::NullBrokerName)?;
                if item.bname.as_deref() == Some(broker_name) {
                    id_map.insert(*global_id, item.queue_id);
                }
            }
            Ok(id_map)
        })
    }

    /// Returns the highest static queue offset in the selected generation, or
    /// `-1` when the global queue has no mapping items.
    pub fn compute_max_offset_from_mapping(
        mapping_detail: Option<&Self>,
        global_id: Option<i32>,
    ) -> Result<i64, TopicQueueMappingDetailError> {
        let Some(items) = Self::get_mapping_info(mapping_detail, global_id)? else {
            return Ok(-1);
        };
        let items = items.snapshot();
        let Some(item) = items.last() else {
            return Ok(-1);
        };
        let item = item
            .as_ref()
            .ok_or(TopicQueueMappingDetailError::NullMappingItem)?;
        Ok(item.compute_max_static_queue_offset())
    }

    /// Copies the current mapping fields into a `TopicQueueMappingInfo` and
    /// derives its current physical ID map from hosted queue generations.
    pub fn clone_as_mapping_info(
        mapping_detail: Option<&Self>,
    ) -> Result<TopicQueueMappingInfo, TopicQueueMappingDetailError> {
        let mapping_detail =
            mapping_detail.ok_or(TopicQueueMappingDetailError::NullMappingDetail)?;
        let id_map = Self::build_id_map(Some(mapping_detail), 0)?;
        let mut info = TopicQueueMappingInfo::new(
            mapping_detail.mapping_info.topic.clone(),
            mapping_detail.mapping_info.total_queues,
            mapping_detail.mapping_info.bname.clone(),
            mapping_detail.mapping_info.epoch,
        );
        info.set_curr_id_map_owned(Some(id_map));
        Ok(info)
    }

    /// Reports whether the selected queue is physical: absent mappings and a
    /// single mapping beginning at logical offset zero both count as physical.
    pub fn check_if_as_physical(
        mapping_detail: Option<&Self>,
        global_id: Option<i32>,
    ) -> Result<bool, TopicQueueMappingDetailError> {
        let Some(items) = Self::get_mapping_info(mapping_detail, global_id)? else {
            return Ok(true);
        };
        let item = items.with(|items| (items.len() == 1).then(|| items[0].clone()));
        let Some(item) = item else {
            return Ok(false);
        };
        let item = item.ok_or(TopicQueueMappingDetailError::NullMappingItem)?;
        Ok(item.logic_offset == 0)
    }

    /// Returns the pinned Java 5.5.1 `TopicQueueMappingDetail.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let hosted_queues_hash = self
            .hosted_queues
            .as_ref()
            .map(|queues| queues.with(hosted_queues_java_hash_code))
            .unwrap_or(0);
        17i32.wrapping_mul(37).wrapping_add(hosted_queues_hash)
    }

    /// Returns the pinned Java 5.5.1 `TopicQueueMappingDetail.toString` text.
    /// Concurrent map entries use deterministic Rust key order.
    pub fn java_to_string(&self) -> String {
        let hosted_queues = self.hosted_queues.as_ref().map_or_else(
            || "null".to_owned(),
            |queues| queues.with(hosted_queues_java_to_string),
        );
        let curr_id_map = self.mapping_info.curr_id_map.as_ref().map_or_else(
            || "null".to_owned(),
            |ids| {
                ids.with(|ids| {
                    let entries = ids
                        .iter()
                        .map(|(logical, physical)| format!("{logical}={physical}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{{{entries}}}")
                })
            },
        );
        format!(
            "TopicQueueMappingDetail{{hostedQueues={}, topic='{}', totalQueues={}, bname='{}', epoch={}, dirty={}, currIdMap={}}}",
            hosted_queues,
            self.mapping_info.topic.as_deref().unwrap_or("null"),
            self.mapping_info.total_queues,
            self.mapping_info.bname.as_deref().unwrap_or("null"),
            self.mapping_info.epoch,
            self.mapping_info.dirty,
            curr_id_map
        )
    }
}

fn hosted_queues_java_hash_code(queues: &HostedQueues) -> i32 {
    queues.iter().fold(0i32, |hash, (global_id, items)| {
        let value_hash = items
            .as_ref()
            .map(|items| {
                items.with(|items| {
                    items.iter().fold(1i32, |list_hash, item| {
                        list_hash.wrapping_mul(31).wrapping_add(
                            item.as_ref()
                                .map(LogicQueueMappingItem::java_hash_code)
                                .unwrap_or(0),
                        )
                    })
                })
            })
            .unwrap_or(0);
        hash.wrapping_add(*global_id ^ value_hash)
    })
}

fn hosted_queues_java_to_string(queues: &HostedQueues) -> String {
    let entries = queues
        .iter()
        .map(|(global_id, items)| {
            let items = items.as_ref().map_or_else(
                || "null".to_owned(),
                |items| {
                    items.with(|items| {
                        let values = items
                            .iter()
                            .map(|item| {
                                item.as_ref().map_or_else(
                                    || "null".to_owned(),
                                    LogicQueueMappingItem::java_to_string,
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("[{values}]")
                    })
                },
            );
            format!("{global_id}={items}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{entries}}}")
}

impl PartialEq for TopicQueueMappingDetail {
    fn eq(&self, other: &Self) -> bool {
        self.hosted_queues.as_ref().map(hosted_queues_snapshot)
            == other.hosted_queues.as_ref().map(hosted_queues_snapshot)
    }
}

impl Eq for TopicQueueMappingDetail {}

impl Hash for TopicQueueMappingDetail {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.java_hash_code().hash(state);
    }
}

/// Java `TopicConfigAndQueueMapping`, a `TopicConfig` with optional static
/// queue mapping metadata.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicConfigAndQueueMapping;
/// let _ = TopicConfigAndQueueMapping::default().java_encode_with_features(&[]);
/// ```
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigAndQueueMapping {
    #[serde(flatten)]
    topic_config: TopicConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    mapping_detail: Option<SharedValue<TopicQueueMappingDetail>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct TopicConfigAndQueueMappingFields {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_topic_config_attribute_map"
    )]
    attributes: Option<TopicConfigAttributeMap>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_bool")]
    order: bool,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    perm: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    read_queue_nums: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_filter_type: Option<TopicConfigFilterType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_name: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    topic_sys_flag: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    write_queue_nums: i32,
    mapping_detail: Option<SharedValue<TopicQueueMappingDetail>>,
}

impl Default for TopicConfigAndQueueMappingFields {
    fn default() -> Self {
        let config = TopicConfig::default();
        Self {
            attributes: Some(IndexMap::new()),
            order: config.order,
            perm: config.perm,
            read_queue_nums: config.read_queue_nums,
            topic_filter_type: config.topic_filter_type,
            topic_name: config.topic_name,
            topic_sys_flag: config.topic_sys_flag,
            write_queue_nums: config.write_queue_nums,
            mapping_detail: None,
        }
    }
}

impl<'de> Deserialize<'de> for TopicConfigAndQueueMapping {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        let normalized = serde_fastjson::normalize_fastjson_integer_map_keys(raw.get());
        let fields = serde_json::from_str::<TopicConfigAndQueueMappingFields>(&normalized)
            .map_err(de::Error::custom)?;
        Ok(Self {
            topic_config: TopicConfig {
                attributes: fields.attributes.map(SharedValue::new),
                order: fields.order,
                perm: fields.perm,
                read_queue_nums: fields.read_queue_nums,
                topic_filter_type: fields.topic_filter_type,
                topic_name: fields.topic_name,
                topic_sys_flag: fields.topic_sys_flag,
                write_queue_nums: fields.write_queue_nums,
            },
            mapping_detail: fields.mapping_detail,
        })
    }
}

impl TopicConfigAndQueueMapping {
    /// Copies the supplied Java `TopicConfig` fields while retaining its
    /// mutable attributes handle, then retains the mapping-detail handle.
    pub fn new(
        topic_config: &TopicConfig,
        mapping_detail: Option<SharedValue<TopicQueueMappingDetail>>,
    ) -> Self {
        Self {
            topic_config: TopicConfig::copy_from(topic_config),
            mapping_detail,
        }
    }

    /// Nullable Java-constructor adapter; Java dereferences the config in its
    /// copy constructor and throws `NullPointerException` for a null input.
    pub fn new_nullable(
        topic_config: Option<&TopicConfig>,
        mapping_detail: Option<SharedValue<TopicQueueMappingDetail>>,
    ) -> Result<Self, TopicConfigError> {
        let topic_config = topic_config.ok_or(TopicConfigError::NullInput)?;
        Ok(Self::new(topic_config, mapping_detail))
    }

    pub fn topic_config(&self) -> &TopicConfig {
        &self.topic_config
    }

    pub fn topic_config_mut(&mut self) -> &mut TopicConfig {
        &mut self.topic_config
    }

    pub fn get_mapping_detail(&self) -> Option<SharedValue<TopicQueueMappingDetail>> {
        self.mapping_detail.clone()
    }

    pub fn set_mapping_detail(&mut self, value: Option<SharedValue<TopicQueueMappingDetail>>) {
        self.mapping_detail = value;
    }

    pub fn set_mapping_detail_owned(&mut self, value: Option<TopicQueueMappingDetail>) {
        self.mapping_detail = value.map(SharedValue::new);
    }

    /// Returns the Java `TopicConfigAndQueueMapping.toString` text.
    pub fn java_to_string(&self) -> String {
        let mut text = self.topic_config.java_to_string();
        if !text.trim().is_empty() {
            text.pop();
            text.push_str(", mappingDetail=");
            match &self.mapping_detail {
                Some(mapping_detail) => {
                    text.push_str(&mapping_detail.with(TopicQueueMappingDetail::java_to_string));
                }
                None => text.push_str("null"),
            }
            text.push(']');
        }
        text
    }
}

impl PartialEq for TopicConfigAndQueueMapping {
    fn eq(&self, other: &Self) -> bool {
        self.topic_config == other.topic_config
            && self.mapping_detail.as_ref().map(SharedValue::snapshot)
                == other.mapping_detail.as_ref().map(SharedValue::snapshot)
    }
}

impl Eq for TopicConfigAndQueueMapping {}

impl Hash for TopicConfigAndQueueMapping {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic_config.hash(state);
        self.mapping_detail
            .as_ref()
            .map(SharedValue::snapshot)
            .hash(state);
    }
}

impl std::ops::Deref for TopicConfigAndQueueMapping {
    type Target = TopicConfig;

    fn deref(&self) -> &Self::Target {
        &self.topic_config
    }
}

impl std::ops::DerefMut for TopicConfigAndQueueMapping {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.topic_config
    }
}

/// Java `CreateTopicListRequestBody`; a null list is omitted by Fastjson.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CreateTopicListRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_config_list: Option<Vec<Option<TopicConfig>>>,
}

impl CreateTopicListRequestBody {
    pub fn new(topic_config_list: Vec<TopicConfig>) -> Self {
        Self {
            topic_config_list: Some(topic_config_list.into_iter().map(Some).collect()),
        }
    }

    pub fn new_nullable(topic_config_list: Option<Vec<Option<TopicConfig>>>) -> Self {
        Self { topic_config_list }
    }

    pub fn topic_config_list(&self) -> Option<&[Option<TopicConfig>]> {
        self.topic_config_list.as_deref()
    }
}

/// Java `DeleteTopicListRequestBody`; a null list is omitted by Fastjson.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicListRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_list: Option<Vec<Option<String>>>,
}

impl DeleteTopicListRequestBody {
    pub fn new(topic_list: Vec<String>) -> Self {
        Self {
            topic_list: Some(topic_list.into_iter().map(Some).collect()),
        }
    }

    pub fn new_nullable(topic_list: Option<Vec<Option<String>>>) -> Self {
        Self { topic_list }
    }

    pub fn topic_list(&self) -> Option<&[Option<String>]> {
        self.topic_list.as_deref()
    }
}

/// Java `DeleteSubscriptionGroupListRequestBody`; a null group list is omitted.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeleteSubscriptionGroupListRequestBody {
    clean_offset: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_name_list: Option<Vec<Option<String>>>,
}

impl DeleteSubscriptionGroupListRequestBody {
    pub fn new(group_name_list: Vec<String>, clean_offset: bool) -> Self {
        Self {
            clean_offset,
            group_name_list: Some(group_name_list.into_iter().map(Some).collect()),
        }
    }

    pub fn new_nullable(group_name_list: Option<Vec<Option<String>>>, clean_offset: bool) -> Self {
        Self {
            clean_offset,
            group_name_list,
        }
    }

    pub fn group_name_list(&self) -> Option<&[Option<String>]> {
        self.group_name_list.as_deref()
    }

    pub fn clean_offset(&self) -> bool {
        self.clean_offset
    }
}

impl SubscriptionGroupList {
    pub fn new(group_config_list: Vec<SubscriptionGroupConfig>) -> Self {
        Self { group_config_list }
    }

    pub fn get_group_config_list(&self) -> &[SubscriptionGroupConfig] {
        &self.group_config_list
    }
}

fn default_topic_list() -> Option<std::collections::HashSet<String>> {
    Some(std::collections::HashSet::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TopicListFields {
    #[serde(default = "default_topic_list")]
    topic_list: Option<std::collections::HashSet<String>>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    broker_addr: Option<String>,
}

impl<'de> Deserialize<'de> for TopicList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: TopicListFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic_list: fields.topic_list.map(SharedValue::new),
            broker_addr: fields.broker_addr,
        })
    }
}

impl Default for TopicList {
    fn default() -> Self {
        Self {
            topic_list: default_topic_list().map(SharedValue::new),
            broker_addr: None,
        }
    }
}

impl TopicList {
    pub fn get_topic_list(&self) -> Option<SharedValue<std::collections::HashSet<String>>> {
        self.topic_list.clone()
    }

    pub fn set_topic_list(
        &mut self,
        topic_list: Option<SharedValue<std::collections::HashSet<String>>>,
    ) {
        self.topic_list = topic_list;
    }

    pub fn get_broker_addr(&self) -> Option<&str> {
        self.broker_addr.as_deref()
    }

    pub fn set_broker_addr(&mut self, broker_addr: Option<String>) {
        self.broker_addr = broker_addr;
    }
}

/// Java response wrapper returned by `QUERY_SUBSCRIPTION_BY_CONSUMER`.
///
/// The nested `SubscriptionData` object is nullable and the Java getter returns
/// the same mutable object assigned through the setter.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QuerySubscriptionResponseBody;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<QuerySubscriptionResponseBody>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QuerySubscriptionResponseBody;
/// let _ = QuerySubscriptionResponseBody::default().group;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QuerySubscriptionResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_data: Option<SharedValue<SubscriptionData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuerySubscriptionResponseBodyFields {
    #[serde(default)]
    subscription_data: Option<SubscriptionData>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
}

impl<'de> Deserialize<'de> for QuerySubscriptionResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QuerySubscriptionResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            subscription_data: fields.subscription_data.map(SharedValue::new),
            group: fields.group,
            topic: fields.topic,
        })
    }
}

impl QuerySubscriptionResponseBody {
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    pub fn get_subscription_data(&self) -> Option<SharedValue<SubscriptionData>> {
        self.subscription_data.clone()
    }

    pub fn set_subscription_data(
        &mut self,
        subscription_data: Option<SharedValue<SubscriptionData>>,
    ) {
        self.subscription_data = subscription_data;
    }

    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: Option<String>) {
        self.group = group;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }
}

/// Java `ConsumeQueueData` row returned by a `QUERY_CONSUME_QUEUE` request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ConsumeQueueData {
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    physic_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    physic_size: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    tags_code: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    extend_data_json: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    bit_map: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_bool")]
    eval: bool,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    msg: Option<String>,
}

impl ConsumeQueueData {
    pub fn get_physic_offset(&self) -> i64 {
        self.physic_offset
    }

    pub fn set_physic_offset(&mut self, value: i64) {
        self.physic_offset = value;
    }

    pub fn get_physic_size(&self) -> i32 {
        self.physic_size
    }

    pub fn set_physic_size(&mut self, value: i32) {
        self.physic_size = value;
    }

    pub fn get_tags_code(&self) -> i64 {
        self.tags_code
    }

    pub fn set_tags_code(&mut self, value: i64) {
        self.tags_code = value;
    }

    pub fn get_extend_data_json(&self) -> Option<&str> {
        self.extend_data_json.as_deref()
    }

    pub fn set_extend_data_json(&mut self, value: Option<String>) {
        self.extend_data_json = value;
    }

    pub fn get_bit_map(&self) -> Option<&str> {
        self.bit_map.as_deref()
    }

    pub fn set_bit_map(&mut self, value: Option<String>) {
        self.bit_map = value;
    }

    pub fn is_eval(&self) -> bool {
        self.eval
    }

    pub fn set_eval(&mut self, value: bool) {
        self.eval = value;
    }

    pub fn get_msg(&self) -> Option<&str> {
        self.msg.as_deref()
    }

    pub fn set_msg(&mut self, value: Option<String>) {
        self.msg = value;
    }

    pub fn java_to_string(&self) -> String {
        format!(
            "ConsumeQueueData{{physicOffset={}, physicSize={}, tagsCode={}, extendDataJson='{}', bitMap='{}', eval={}, msg='{}'}}",
            self.physic_offset,
            self.physic_size,
            self.tags_code,
            self.extend_data_json.as_deref().unwrap_or("null"),
            self.bit_map.as_deref().unwrap_or("null"),
            self.eval,
            self.msg.as_deref().unwrap_or("null"),
        )
    }
}

/// Java `QueryConsumeQueueResponseBody`, including its nullable nested DTOs and
/// mutable list/object aliases.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumeQueueResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_data: Option<SharedValue<SubscriptionData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_data: Option<SharedValue<Vec<Option<SharedValue<ConsumeQueueData>>>>>,
    max_queue_index: i64,
    min_queue_index: i64,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct QueryConsumeQueueResponseBodyFields {
    subscription_data: Option<SubscriptionData>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    filter_data: Option<String>,
    queue_data: Option<Vec<Option<ConsumeQueueData>>>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    max_queue_index: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    min_queue_index: i64,
}

impl<'de> Deserialize<'de> for QueryConsumeQueueResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        let fields: QueryConsumeQueueResponseBodyFields = fields;
        Ok(Self {
            subscription_data: fields.subscription_data.map(SharedValue::new),
            filter_data: fields.filter_data,
            queue_data: fields.queue_data.map(|items| {
                SharedValue::new(
                    items
                        .into_iter()
                        .map(|item| item.map(SharedValue::new))
                        .collect(),
                )
            }),
            max_queue_index: fields.max_queue_index,
            min_queue_index: fields.min_queue_index,
        })
    }
}

impl QueryConsumeQueueResponseBody {
    pub fn get_subscription_data(&self) -> Option<SharedValue<SubscriptionData>> {
        self.subscription_data.clone()
    }

    pub fn set_subscription_data(&mut self, value: Option<SharedValue<SubscriptionData>>) {
        self.subscription_data = value;
    }

    pub fn get_filter_data(&self) -> Option<&str> {
        self.filter_data.as_deref()
    }

    pub fn set_filter_data(&mut self, value: Option<String>) {
        self.filter_data = value;
    }

    pub fn get_queue_data(
        &self,
    ) -> Option<SharedValue<Vec<Option<SharedValue<ConsumeQueueData>>>>> {
        self.queue_data.clone()
    }

    pub fn set_queue_data(
        &mut self,
        value: Option<SharedValue<Vec<Option<SharedValue<ConsumeQueueData>>>>>,
    ) {
        self.queue_data = value;
    }

    pub fn get_max_queue_index(&self) -> i64 {
        self.max_queue_index
    }

    pub fn set_max_queue_index(&mut self, value: i64) {
        self.max_queue_index = value;
    }

    pub fn get_min_queue_index(&self) -> i64 {
        self.min_queue_index
    }

    pub fn set_min_queue_index(&mut self, value: i64) {
        self.min_queue_index = value;
    }
}

/// Java `GroupList` body returned by `QUERY_TOPIC_CONSUME_BY_WHO`.
///
/// The Java DTO defaults to an empty mutable Set, permits a null Set and null
/// Set elements, and returns the stored Set object from its getter.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GroupList;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<GroupList>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GroupList;
/// let _ = GroupList::default().group_list;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupList {
    #[serde(skip_serializing_if = "Option::is_none")]
    group_list: Option<SharedValue<std::collections::HashSet<Option<String>>>>,
}

fn default_group_list() -> Option<std::collections::HashSet<Option<String>>> {
    Some(std::collections::HashSet::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupListFields {
    #[serde(
        default = "default_group_list",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_string_set"
    )]
    group_list: Option<std::collections::HashSet<Option<String>>>,
}

impl<'de> Deserialize<'de> for GroupList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: GroupListFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            group_list: fields.group_list.map(SharedValue::new),
        })
    }
}

impl Default for GroupList {
    fn default() -> Self {
        Self {
            group_list: default_group_list().map(SharedValue::new),
        }
    }
}

impl GroupList {
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    pub fn get_group_list(&self) -> Option<SharedValue<std::collections::HashSet<Option<String>>>> {
        self.group_list.clone()
    }

    pub fn set_group_list(
        &mut self,
        group_list: Option<SharedValue<std::collections::HashSet<Option<String>>>>,
    ) {
        self.group_list = group_list;
    }
}

/// A Java `ClusterInfo.brokerAddrTable` reference, including nullable Java
/// string keys and nullable `BrokerData` values.
pub type SharedClusterBrokerAddrTable =
    SharedValue<HashMap<Option<String>, Option<SharedBrokerDataHandle>>>;
/// A Java `ClusterInfo.clusterAddrTable` reference, including nullable Java
/// string keys and nullable broker-name sets.
pub type SharedClusterAddrTable =
    SharedValue<HashMap<Option<String>, Option<SharedClusterBrokerNames>>>;
/// A Java cluster's mutable broker-name set.
pub type SharedClusterBrokerNames = SharedValue<HashSet<Option<String>>>;

fn serialize_cluster_broker_addr_table<S>(
    value: &Option<SharedClusterBrokerAddrTable>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(table) = value else {
        return serializer.serialize_none();
    };
    table.with(|entries| {
        let mut output = serializer.serialize_map(Some(entries.len()))?;
        for (key, broker) in entries {
            output.serialize_entry(key.as_deref().unwrap_or("null"), broker)?;
        }
        output.end()
    })
}

fn serialize_cluster_addr_table<S>(
    value: &Option<SharedClusterAddrTable>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(table) = value else {
        return serializer.serialize_none();
    };
    table.with(|entries| {
        let mut output = serializer.serialize_map(Some(entries.len()))?;
        for (key, names) in entries {
            output.serialize_entry(key.as_deref().unwrap_or("null"), names)?;
        }
        output.end()
    })
}

/// Java `ClusterInfo` response DTO returned by `GET_BROKER_CLUSTER_INFO`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    #[serde(
        serialize_with = "serialize_cluster_broker_addr_table",
        skip_serializing_if = "Option::is_none"
    )]
    broker_addr_table: Option<SharedClusterBrokerAddrTable>,
    #[serde(
        serialize_with = "serialize_cluster_addr_table",
        skip_serializing_if = "Option::is_none"
    )]
    cluster_addr_table: Option<SharedClusterAddrTable>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ClusterInfoFields {
    broker_addr_table: Option<HashMap<String, Option<BrokerData>>>,
    cluster_addr_table: Option<HashMap<String, Option<HashSet<Option<String>>>>>,
}

impl<'de> Deserialize<'de> for ClusterInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ClusterInfoFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            broker_addr_table: fields.broker_addr_table.map(|table| {
                SharedValue::new(
                    table
                        .into_iter()
                        .map(|(name, broker)| (Some(name), broker.map(SharedValue::new)))
                        .collect(),
                )
            }),
            cluster_addr_table: fields.cluster_addr_table.map(|table| {
                SharedValue::new(
                    table
                        .into_iter()
                        .map(|(name, brokers)| (Some(name), brokers.map(SharedValue::new)))
                        .collect(),
                )
            }),
        })
    }
}

impl PartialEq for ClusterInfo {
    fn eq(&self, other: &Self) -> bool {
        let broker_tables_equal = match (&self.broker_addr_table, &other.broker_addr_table) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                cluster_broker_tables_equal(&left.snapshot(), &right.snapshot())
            }
            _ => false,
        };
        broker_tables_equal
            && match (&self.cluster_addr_table, &other.cluster_addr_table) {
                (None, None) => true,
                (Some(left), Some(right)) => {
                    cluster_name_tables_equal(&left.snapshot(), &right.snapshot())
                }
                _ => false,
            }
    }
}

impl Eq for ClusterInfo {}

fn cluster_broker_tables_equal(
    left: &HashMap<Option<String>, Option<SharedBrokerDataHandle>>,
    right: &HashMap<Option<String>, Option<SharedBrokerDataHandle>>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(name, left_broker)| {
            let Some(right_broker) = right.get(name) else {
                return false;
            };
            match (left_broker, right_broker) {
                (None, None) => true,
                (Some(left), Some(right)) => left.snapshot() == right.snapshot(),
                _ => false,
            }
        })
}

fn cluster_name_tables_equal(
    left: &HashMap<Option<String>, Option<SharedClusterBrokerNames>>,
    right: &HashMap<Option<String>, Option<SharedClusterBrokerNames>>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(name, left_brokers)| {
            let Some(right_brokers) = right.get(name) else {
                return false;
            };
            match (left_brokers, right_brokers) {
                (None, None) => true,
                (Some(left), Some(right)) => left.snapshot() == right.snapshot(),
                _ => false,
            }
        })
}

fn cluster_info_java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn cluster_broker_table_hash(
    table: &HashMap<Option<String>, Option<SharedBrokerDataHandle>>,
) -> i32 {
    table.iter().fold(0i32, |hash, (name, broker)| {
        let key_hash = name
            .as_deref()
            .map(cluster_info_java_string_hash_code)
            .unwrap_or(0);
        let value_hash = broker
            .as_ref()
            .map(|broker| broker.with(BrokerData::java_hash_code))
            .unwrap_or(0);
        hash.wrapping_add(key_hash ^ value_hash)
    })
}

fn cluster_name_table_hash(
    table: &HashMap<Option<String>, Option<SharedClusterBrokerNames>>,
) -> i32 {
    table.iter().fold(0i32, |hash, (name, brokers)| {
        let key_hash = name
            .as_deref()
            .map(cluster_info_java_string_hash_code)
            .unwrap_or(0);
        let value_hash = brokers
            .as_ref()
            .map(|brokers| {
                brokers.with(|names| {
                    names.iter().fold(0i32, |hash, name| {
                        hash.wrapping_add(
                            name.as_deref()
                                .map(cluster_info_java_string_hash_code)
                                .unwrap_or(0),
                        )
                    })
                })
            })
            .unwrap_or(0);
        hash.wrapping_add(key_hash ^ value_hash)
    })
}

/// Java `NullPointerException` branches from `ClusterInfo` query methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClusterInfoLookupError {
    #[error("ClusterInfo.clusterAddrTable is null (Java NullPointerException)")]
    NullClusterAddrTable,
    #[error("ClusterInfo cluster broker-name set is null (Java NullPointerException)")]
    NullClusterBrokerNames,
    #[error("ClusterInfo.brokerAddrTable is null (Java NullPointerException)")]
    NullBrokerAddrTable,
    #[error("BrokerData.brokerAddrs is null (Java NullPointerException)")]
    NullBrokerAddressMap,
}

impl ClusterInfo {
    pub fn get_broker_addr_table(&self) -> Option<SharedClusterBrokerAddrTable> {
        self.broker_addr_table.clone()
    }

    pub fn set_broker_addr_table(&mut self, table: Option<SharedClusterBrokerAddrTable>) {
        self.broker_addr_table = table;
    }

    pub fn get_cluster_addr_table(&self) -> Option<SharedClusterAddrTable> {
        self.cluster_addr_table.clone()
    }

    pub fn set_cluster_addr_table(&mut self, table: Option<SharedClusterAddrTable>) {
        self.cluster_addr_table = table;
    }

    pub fn retrieve_all_addr_by_cluster(
        &self,
        cluster: Option<&str>,
    ) -> Result<Vec<Option<String>>, ClusterInfoLookupError> {
        let table = self
            .cluster_addr_table
            .as_ref()
            .ok_or(ClusterInfoLookupError::NullClusterAddrTable)?;
        let cluster = cluster.map(str::to_owned);
        let Some(names) = table.with(|entries| entries.get(&cluster).cloned()) else {
            return Ok(Vec::new());
        };
        let names = names.ok_or(ClusterInfoLookupError::NullClusterBrokerNames)?;
        let broker_names = names.snapshot();
        let mut addresses = Vec::new();
        for broker_name in broker_names {
            let table = self
                .broker_addr_table
                .as_ref()
                .ok_or(ClusterInfoLookupError::NullBrokerAddrTable)?;
            let Some(broker) = table.with(|entries| entries.get(&broker_name).cloned().flatten())
            else {
                continue;
            };
            let broker_addresses = broker
                .with(|broker| broker.broker_addrs.clone())
                .ok_or(ClusterInfoLookupError::NullBrokerAddressMap)?;
            broker_addresses.with(|entries| {
                addresses.extend(entries.values().cloned());
            });
        }
        Ok(addresses)
    }

    pub fn retrieve_all_cluster_names(
        &self,
    ) -> Result<Vec<Option<String>>, ClusterInfoLookupError> {
        let table = self
            .cluster_addr_table
            .as_ref()
            .ok_or(ClusterInfoLookupError::NullClusterAddrTable)?;
        Ok(table.with(|entries| entries.keys().cloned().collect()))
    }

    /// Returns the pinned Java 5.5.1 `ClusterInfo.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let broker_table_hash = self
            .broker_addr_table
            .as_ref()
            .map(|table| table.with(cluster_broker_table_hash))
            .unwrap_or(0);
        let cluster_table_hash = self
            .cluster_addr_table
            .as_ref()
            .map(|table| table.with(cluster_name_table_hash))
            .unwrap_or(0);
        31i32
            .wrapping_mul(31i32.wrapping_add(broker_table_hash))
            .wrapping_add(cluster_table_hash)
    }

    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(crate::route::java_fastjson_json(self, false)?.into_bytes())
    }

    pub fn java_encode_optional(
        value: Option<&Self>,
    ) -> Result<Option<Vec<u8>>, serde_json::Error> {
        value.map(Self::java_encode).transpose()
    }

    pub fn java_encode_with_features(
        &self,
        features: &[JavaJsonWriterFeature],
    ) -> Result<Vec<u8>, serde_json::Error> {
        Ok(crate::route::java_fastjson_json_with_features(self, false, features)?.into_bytes())
    }

    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        crate::route::java_fastjson_json(self, pretty_format)
    }

    pub fn java_to_json_optional(
        value: Option<&Self>,
        pretty_format: bool,
    ) -> Result<String, serde_json::Error> {
        value.map_or_else(
            || Ok("null".to_owned()),
            |value| value.java_to_json(pretty_format),
        )
    }

    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        crate::route::parse_java_route_json(&String::from_utf8_lossy(data))
    }

    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        let Some(data) = data else {
            return Ok(None);
        };
        crate::route::parse_java_route_json(&String::from_utf8_lossy(data))
    }

    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        crate::route::parse_java_route_json(&String::from_utf8_lossy(data))
    }

    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        let Some(data) = data else {
            return Ok(None);
        };
        crate::route::parse_java_route_json(&String::from_utf8_lossy(data))
    }

    pub fn java_from_json(input: &str) -> Result<Self, serde_json::Error> {
        crate::route::parse_java_route_json(input)
    }

    pub fn java_from_json_nullable(input: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        let Some(input) = input else {
            return Ok(None);
        };
        crate::route::parse_java_route_json(input)
    }
}

/// Java `Connection` values returned inside a `ConsumerConnection` body.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<LanguageCode>,
    version: i32,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ConnectionFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_addr: Option<String>,
    language: Option<LanguageCode>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    version: i32,
}

impl<'de> Deserialize<'de> for Connection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ConnectionFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            client_id: fields.client_id,
            client_addr: fields.client_addr,
            language: fields.language,
            version: fields.version,
        })
    }
}

impl Connection {
    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, value: Option<String>) {
        self.client_id = value;
    }

    pub fn get_client_addr(&self) -> Option<&str> {
        self.client_addr.as_deref()
    }

    pub fn set_client_addr(&mut self, value: Option<String>) {
        self.client_addr = value;
    }

    pub fn get_language(&self) -> Option<LanguageCode> {
        self.language
    }

    pub fn set_language(&mut self, value: Option<LanguageCode>) {
        self.language = value;
    }

    pub fn get_version(&self) -> i32 {
        self.version
    }

    pub fn set_version(&mut self, value: i32) {
        self.version = value;
    }
}

/// Java `HashSet<Connection>` membership uses `Connection` object identity.
/// `SharedValue` clones represent the same Java object; independently created
/// values remain distinct even when their fields match.
type ConnectionSet = HashSet<Option<SharedValue<Connection>>>;
type SharedConnectionSet = SharedValue<ConnectionSet>;

fn empty_shared_connection_set() -> Option<SharedConnectionSet> {
    Some(SharedValue::default())
}

impl PartialEq for SharedValue<Connection> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}

impl Eq for SharedValue<Connection> {}

impl Hash for SharedValue<Connection> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_identity(state);
    }
}

/// Java `ProducerConnection` response DTO returned by
/// `GET_PRODUCER_CONNECTION_LIST`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducerConnection {
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_set: Option<SharedConnectionSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ProducerConnectionFields {
    #[serde(
        default = "empty_shared_connection_set",
        deserialize_with = "crate::heartbeat::deserialize_identity_shared_hash_set_with_null"
    )]
    connection_set: Option<SharedConnectionSet>,
}

impl Default for ProducerConnectionFields {
    fn default() -> Self {
        Self {
            connection_set: empty_shared_connection_set(),
        }
    }
}

impl<'de> Deserialize<'de> for ProducerConnection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ProducerConnectionFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            connection_set: fields.connection_set,
        })
    }
}

impl Default for ProducerConnection {
    fn default() -> Self {
        Self {
            connection_set: empty_shared_connection_set(),
        }
    }
}

impl ProducerConnection {
    pub fn get_connection_set(
        &self,
    ) -> Option<SharedValue<HashSet<Option<SharedValue<Connection>>>>> {
        self.connection_set.clone()
    }

    pub fn set_connection_set(
        &mut self,
        value: Option<SharedValue<HashSet<Option<SharedValue<Connection>>>>>,
    ) {
        self.connection_set = value;
    }
}

/// Java `UserInfo` body DTO used by the Broker auth user operations.
/// Java inherits identity equality from `Object` for this model.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::UserInfo;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<UserInfo>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::UserInfo;
/// let user = UserInfo::default();
/// user.java_encode_with_features(&[]).unwrap();
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    #[serde(skip)]
    java_list_raw_value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
}

impl Serialize for UserInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(raw_value) = &self.java_list_raw_value {
            return raw_value.serialize(serializer);
        }

        let field_count = [
            self.password.is_some(),
            self.user_status.is_some(),
            self.user_type.is_some(),
            self.username.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        let mut state = serializer.serialize_struct("UserInfo", field_count)?;
        if let Some(value) = &self.password {
            state.serialize_field("password", value)?;
        }
        if let Some(value) = &self.user_status {
            state.serialize_field("userStatus", value)?;
        }
        if let Some(value) = &self.user_type {
            state.serialize_field("userType", value)?;
        }
        if let Some(value) = &self.username {
            state.serialize_field("username", value)?;
        }
        state.end()
    }
}

impl UserInfo {
    pub fn of(
        username: Option<String>,
        password: Option<String>,
        user_type: Option<String>,
    ) -> Self {
        Self {
            java_list_raw_value: None,
            username,
            password,
            user_type,
            user_status: None,
        }
    }

    pub fn of_with_status(
        username: Option<String>,
        password: Option<String>,
        user_type: Option<String>,
        user_status: Option<String>,
    ) -> Self {
        Self {
            java_list_raw_value: None,
            username,
            password,
            user_type,
            user_status,
        }
    }

    pub fn get_username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn set_username(&mut self, value: Option<String>) {
        self.username = value;
    }

    pub fn get_password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn set_password(&mut self, value: Option<String>) {
        self.password = value;
    }

    pub fn get_user_type(&self) -> Option<&str> {
        self.user_type.as_deref()
    }

    pub fn set_user_type(&mut self, value: Option<String>) {
        self.user_type = value;
    }

    pub fn get_user_status(&self) -> Option<&str> {
        self.user_status.as_deref()
    }

    pub fn set_user_status(&mut self, value: Option<String>) {
        self.user_status = value;
    }

    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        crate::route::java_fastjson_json(self, pretty_format)
    }

    pub fn java_to_json_optional(
        value: Option<&Self>,
        pretty_format: bool,
    ) -> Result<String, serde_json::Error> {
        value.map_or_else(
            || Ok("null".to_owned()),
            |value| value.java_to_json(pretty_format),
        )
    }

    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    pub fn java_encode_optional(
        value: Option<&Self>,
    ) -> Result<Option<Vec<u8>>, serde_json::Error> {
        value.map(Self::java_encode).transpose()
    }

    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice_nullable(data)
    }

    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes Java `RemotingSerializable.decodeList` results, including the
    /// Fastjson object-root case that returns the original JSON text as a
    /// String element despite the method's declared `List<UserInfo>` type.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        let Some(data) = data else {
            return Ok(None);
        };
        let root: serde_json::Value = serde_fastjson::from_fastjson_slice(data)?;
        if root.is_object() {
            let raw_text = String::from_utf8_lossy(data).into_owned();
            return Ok(Some(vec![Some(Self {
                java_list_raw_value: Some(serde_json::Value::String(raw_text)),
                ..Self::default()
            })]));
        }
        serde_fastjson::from_fastjson_list_nullable(Some(data))
    }

    pub fn java_from_json(input: &str) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_str(input)
    }

    pub fn java_from_json_nullable(input: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_str_nullable(input)
    }
}

/// Java `AclInfo` body DTO used by Broker ACL operations. The Java class uses
/// identity equality and keeps its policy list nullable.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::AclInfo;
/// let _ = AclInfo::default().java_encode_with_features(&[]);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AclInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    policies: Option<Vec<Option<AclPolicyInfo>>>,
}

/// Java `AclInfo.PolicyInfo` nested body DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::AclPolicyInfo;
/// let _ = AclPolicyInfo::default().java_encode_with_features(&[]);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AclPolicyInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    policy_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entries: Option<Vec<Option<AclPolicyEntryInfo>>>,
}

/// Java `AclInfo.PolicyEntryInfo` nested body DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::AclPolicyEntryInfo;
/// let _ = AclPolicyEntryInfo::default().java_encode_with_features(&[]);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AclPolicyEntryInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actions: Option<Vec<Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_ips: Option<Vec<Option<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decision: Option<String>,
}

impl AclInfo {
    pub fn of(
        subject: Option<String>,
        resources: Vec<Option<String>>,
        actions: Option<Vec<Option<String>>>,
        source_ips: Option<Vec<Option<String>>>,
        decision: Option<String>,
    ) -> Self {
        Self {
            subject,
            policies: Some(vec![Some(AclPolicyInfo::of(
                resources, actions, source_ips, decision,
            ))]),
        }
    }

    pub fn get_subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub fn set_subject(&mut self, value: Option<String>) {
        self.subject = value;
    }

    pub fn get_policies(&self) -> Option<&[Option<AclPolicyInfo>]> {
        self.policies.as_deref()
    }

    pub fn set_policies(&mut self, value: Option<Vec<Option<AclPolicyInfo>>>) {
        self.policies = value;
    }
}

impl AclPolicyInfo {
    pub fn of(
        resources: Vec<Option<String>>,
        actions: Option<Vec<Option<String>>>,
        source_ips: Option<Vec<Option<String>>>,
        decision: Option<String>,
    ) -> Self {
        let entries = resources
            .into_iter()
            .map(|resource| {
                Some(AclPolicyEntryInfo::of(
                    resource,
                    actions.clone(),
                    source_ips.clone(),
                    decision.clone(),
                ))
            })
            .collect();
        Self {
            policy_type: None,
            entries: Some(entries),
        }
    }

    pub fn get_policy_type(&self) -> Option<&str> {
        self.policy_type.as_deref()
    }

    pub fn set_policy_type(&mut self, value: Option<String>) {
        self.policy_type = value;
    }

    pub fn get_entries(&self) -> Option<&[Option<AclPolicyEntryInfo>]> {
        self.entries.as_deref()
    }

    pub fn set_entries(&mut self, value: Option<Vec<Option<AclPolicyEntryInfo>>>) {
        self.entries = value;
    }
}

impl AclPolicyEntryInfo {
    pub fn of(
        resource: Option<String>,
        actions: Option<Vec<Option<String>>>,
        source_ips: Option<Vec<Option<String>>>,
        decision: Option<String>,
    ) -> Self {
        Self {
            resource,
            actions,
            source_ips,
            decision,
        }
    }

    pub fn get_resource(&self) -> Option<&str> {
        self.resource.as_deref()
    }

    pub fn set_resource(&mut self, value: Option<String>) {
        self.resource = value;
    }

    pub fn get_actions(&self) -> Option<&[Option<String>]> {
        self.actions.as_deref()
    }

    pub fn set_actions(&mut self, value: Option<Vec<Option<String>>>) {
        self.actions = value;
    }

    pub fn get_source_ips(&self) -> Option<&[Option<String>]> {
        self.source_ips.as_deref()
    }

    pub fn set_source_ips(&mut self, value: Option<Vec<Option<String>>>) {
        self.source_ips = value;
    }

    pub fn get_decision(&self) -> Option<&str> {
        self.decision.as_deref()
    }

    pub fn set_decision(&mut self, value: Option<String>) {
        self.decision = value;
    }
}

/// Java `KVTable` response DTO returned by Broker runtime and query RPCs.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KvTable {
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<SharedValue<HashMap<String, Option<String>>>>,
}

fn default_kv_table() -> Option<HashMap<String, Option<String>>> {
    Some(HashMap::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct KvTableFields {
    #[serde(default = "default_kv_table")]
    table: Option<HashMap<String, Option<String>>>,
}

impl Default for KvTableFields {
    fn default() -> Self {
        Self {
            table: default_kv_table(),
        }
    }
}

impl<'de> Deserialize<'de> for KvTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: KvTableFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            table: fields.table.map(SharedValue::new),
        })
    }
}

impl Default for KvTable {
    fn default() -> Self {
        Self {
            table: Some(SharedValue::new(HashMap::new())),
        }
    }
}

impl KvTable {
    pub fn get_table(&self) -> Option<SharedValue<HashMap<String, Option<String>>>> {
        self.table.clone()
    }

    pub fn set_table(&mut self, table: Option<SharedValue<HashMap<String, Option<String>>>>) {
        self.table = table;
    }
}

/// Java `ProducerInfo` record in a `ProducerTableInfo` response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducerInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(rename = "remoteIP", skip_serializing_if = "Option::is_none")]
    remote_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<LanguageCode>,
    version: i32,
    last_update_timestamp: i64,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ProducerInfoFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        rename = "remoteIP",
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    remote_ip: Option<String>,
    language: Option<LanguageCode>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    version: i32,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    last_update_timestamp: i64,
}

impl<'de> Deserialize<'de> for ProducerInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ProducerInfoFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            client_id: fields.client_id,
            remote_ip: fields.remote_ip,
            language: fields.language,
            version: fields.version,
            last_update_timestamp: fields.last_update_timestamp,
        })
    }
}

impl ProducerInfo {
    pub fn new(
        client_id: Option<String>,
        remote_ip: Option<String>,
        language: Option<LanguageCode>,
        version: i32,
        last_update_timestamp: i64,
    ) -> Self {
        Self {
            client_id,
            remote_ip,
            language,
            version,
            last_update_timestamp,
        }
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, value: Option<String>) {
        self.client_id = value;
    }

    pub fn get_remote_ip(&self) -> Option<&str> {
        self.remote_ip.as_deref()
    }

    pub fn set_remote_ip(&mut self, value: Option<String>) {
        self.remote_ip = value;
    }

    pub fn get_language(&self) -> Option<LanguageCode> {
        self.language
    }

    pub fn set_language(&mut self, value: Option<LanguageCode>) {
        self.language = value;
    }

    pub fn get_version(&self) -> i32 {
        self.version
    }

    pub fn set_version(&mut self, value: i32) {
        self.version = value;
    }

    pub fn get_last_update_timestamp(&self) -> i64 {
        self.last_update_timestamp
    }

    pub fn set_last_update_timestamp(&mut self, value: i64) {
        self.last_update_timestamp = value;
    }

    /// Formats the pinned Java `ProducerInfo.toString()` output. Java
    /// dereferences `language` while formatting and throws for a null value.
    pub fn java_to_string(&self) -> Result<String, ProducerInfoToStringError> {
        let language = self
            .language
            .ok_or(ProducerInfoToStringError::NullLanguage)?;
        Ok(format!(
            "clientId={},remoteIP={}, language={}, version={}, lastUpdateTimestamp={}",
            self.client_id.as_deref().unwrap_or("null"),
            self.remote_ip.as_deref().unwrap_or("null"),
            language.name(),
            self.version,
            self.last_update_timestamp
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProducerInfoToStringError {
    #[error("Java ProducerInfo.toString dereferences a null language")]
    NullLanguage,
}

/// Java `ProducerTableInfo` Broker response DTO.
/// The Java producer map keyed by group, with nullable lists and nullable rows.
pub type ProducerInfoTable = HashMap<String, Option<Vec<Option<ProducerInfo>>>>;
/// A shared mutable Java `ProducerTableInfo.data` map.
pub type SharedProducerInfoTable = SharedValue<ProducerInfoTable>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducerTableInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<SharedProducerInfoTable>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ProducerTableInfoFields {
    data: Option<ProducerInfoTable>,
}

impl<'de> Deserialize<'de> for ProducerTableInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ProducerTableInfoFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            data: fields.data.map(SharedValue::new),
        })
    }
}

impl ProducerTableInfo {
    pub fn new(data: Option<SharedProducerInfoTable>) -> Self {
        Self { data }
    }

    pub fn get_data(&self) -> Option<SharedProducerInfoTable> {
        self.data.clone()
    }

    pub fn set_data(&mut self, data: Option<SharedProducerInfoTable>) {
        self.data = data;
    }
}

/// Java `ConsumerConnection` response DTO returned by
/// `GET_CONSUMER_CONNECTION_LIST`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConnection {
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_set: Option<SharedConnectionSet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_table: Option<SharedValue<HashMap<String, Option<SharedValue<SubscriptionData>>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consume_type: Option<ConsumeType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_model: Option<MessageModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consume_from_where: Option<ConsumeFromWhere>,
}

fn default_consumer_connection_subscription_table()
-> Option<HashMap<String, Option<SubscriptionData>>> {
    Some(HashMap::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ConsumerConnectionFields {
    #[serde(
        default = "empty_shared_connection_set",
        deserialize_with = "crate::heartbeat::deserialize_identity_shared_hash_set_with_null"
    )]
    connection_set: Option<SharedConnectionSet>,
    #[serde(default = "default_consumer_connection_subscription_table")]
    subscription_table: Option<HashMap<String, Option<SubscriptionData>>>,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    consume_type: Option<ConsumeType>,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    message_model: Option<MessageModel>,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    consume_from_where: Option<ConsumeFromWhere>,
}

impl Default for ConsumerConnectionFields {
    fn default() -> Self {
        Self {
            connection_set: empty_shared_connection_set(),
            subscription_table: default_consumer_connection_subscription_table(),
            consume_type: None,
            message_model: None,
            consume_from_where: None,
        }
    }
}

impl<'de> Deserialize<'de> for ConsumerConnection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ConsumerConnectionFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            connection_set: fields.connection_set,
            subscription_table: fields.subscription_table.map(|values| {
                SharedValue::new(
                    values
                        .into_iter()
                        .map(|(topic, value)| (topic, value.map(SharedValue::new)))
                        .collect(),
                )
            }),
            consume_type: fields.consume_type,
            message_model: fields.message_model,
            consume_from_where: fields.consume_from_where,
        })
    }
}

impl Default for ConsumerConnection {
    fn default() -> Self {
        let fields = ConsumerConnectionFields::default();
        Self {
            connection_set: fields.connection_set,
            subscription_table: fields.subscription_table.map(|values| {
                SharedValue::new(
                    values
                        .into_iter()
                        .map(|(topic, value)| (topic, value.map(SharedValue::new)))
                        .collect(),
                )
            }),
            consume_type: None,
            message_model: None,
            consume_from_where: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConsumerConnectionComputeMinVersionError {
    #[error("Java ConsumerConnection.computeMinVersion dereferences a null connectionSet")]
    NullConnectionSet,
    #[error("Java ConsumerConnection.computeMinVersion dereferences a null Connection")]
    NullConnection,
}

impl ConsumerConnection {
    pub fn get_connection_set(
        &self,
    ) -> Option<SharedValue<HashSet<Option<SharedValue<Connection>>>>> {
        self.connection_set.clone()
    }

    pub fn set_connection_set(
        &mut self,
        value: Option<SharedValue<HashSet<Option<SharedValue<Connection>>>>>,
    ) {
        self.connection_set = value;
    }

    pub fn get_subscription_table(
        &self,
    ) -> Option<SharedValue<HashMap<String, Option<SharedValue<SubscriptionData>>>>> {
        self.subscription_table.clone()
    }

    pub fn set_subscription_table(
        &mut self,
        value: Option<SharedValue<HashMap<String, Option<SharedValue<SubscriptionData>>>>>,
    ) {
        self.subscription_table = value;
    }

    pub fn get_consume_type(&self) -> Option<ConsumeType> {
        self.consume_type
    }

    pub fn set_consume_type(&mut self, value: Option<ConsumeType>) {
        self.consume_type = value;
    }

    pub fn get_message_model(&self) -> Option<MessageModel> {
        self.message_model
    }

    pub fn set_message_model(&mut self, value: Option<MessageModel>) {
        self.message_model = value;
    }

    pub fn get_consume_from_where(&self) -> Option<ConsumeFromWhere> {
        self.consume_from_where
    }

    pub fn set_consume_from_where(&mut self, value: Option<ConsumeFromWhere>) {
        self.consume_from_where = value;
    }

    /// Computes the minimum Java `Connection.version`, returning `i32::MAX`
    /// for an empty set. Java throws `NullPointerException` when the set or an
    /// element is null; Rust reports those cases as typed errors.
    pub fn compute_min_version(&self) -> Result<i32, ConsumerConnectionComputeMinVersionError> {
        let connections = self
            .connection_set
            .as_ref()
            .ok_or(ConsumerConnectionComputeMinVersionError::NullConnectionSet)?;
        connections.with(|connections| {
            connections
                .iter()
                .try_fold(i32::MAX, |minimum, connection| {
                    let connection = connection
                        .as_ref()
                        .ok_or(ConsumerConnectionComputeMinVersionError::NullConnection)?;
                    Ok(connection.with(|connection| minimum.min(connection.version)))
                })
        })
    }
}

/// Java `OffsetWrapper` returned by `ConsumeStats`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::OffsetWrapper;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { requires_java_identity::<OffsetWrapper>(); }
/// ```
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OffsetWrapper {
    broker_offset: i64,
    consumer_offset: i64,
    pull_offset: i64,
    last_timestamp: i64,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct OffsetWrapperFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    broker_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    consumer_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    pull_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    last_timestamp: i64,
}

impl<'de> Deserialize<'de> for OffsetWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: OffsetWrapperFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            broker_offset: fields.broker_offset,
            consumer_offset: fields.consumer_offset,
            pull_offset: fields.pull_offset,
            last_timestamp: fields.last_timestamp,
        })
    }
}

impl OffsetWrapper {
    pub fn get_broker_offset(&self) -> i64 {
        self.broker_offset
    }

    pub fn set_broker_offset(&mut self, value: i64) {
        self.broker_offset = value;
    }

    pub fn get_consumer_offset(&self) -> i64 {
        self.consumer_offset
    }

    pub fn set_consumer_offset(&mut self, value: i64) {
        self.consumer_offset = value;
    }

    pub fn get_pull_offset(&self) -> i64 {
        self.pull_offset
    }

    pub fn set_pull_offset(&mut self, value: i64) {
        self.pull_offset = value;
    }

    pub fn get_last_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    pub fn set_last_timestamp(&mut self, value: i64) {
        self.last_timestamp = value;
    }
}

/// Java `TopicOffset`; its boxed-looking accessors actually expose primitive
/// `long` fields with zero defaults.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicOffset;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<TopicOffset>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicOffset;
/// let _ = TopicOffset::default().min_offset;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicOffset {
    min_offset: i64,
    max_offset: i64,
    last_update_timestamp: i64,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct TopicOffsetFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    min_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    max_offset: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    last_update_timestamp: i64,
}

impl<'de> Deserialize<'de> for TopicOffset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: TopicOffsetFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            min_offset: fields.min_offset,
            max_offset: fields.max_offset,
            last_update_timestamp: fields.last_update_timestamp,
        })
    }
}

impl TopicOffset {
    pub fn get_min_offset(&self) -> i64 {
        self.min_offset
    }

    pub fn set_min_offset(&mut self, value: i64) {
        self.min_offset = value;
    }

    pub fn get_max_offset(&self) -> i64 {
        self.max_offset
    }

    pub fn set_max_offset(&mut self, value: i64) {
        self.max_offset = value;
    }

    pub fn get_last_update_timestamp(&self) -> i64 {
        self.last_update_timestamp
    }

    pub fn set_last_update_timestamp(&mut self, value: i64) {
        self.last_update_timestamp = value;
    }
}

impl std::fmt::Display for TopicOffset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "TopicOffset{{minOffset={}, maxOffset={}, lastUpdateTimestamp={}}}",
            self.min_offset, self.max_offset, self.last_update_timestamp
        )
    }
}

/// Java `ClientGroup`, whose equality and hash keys are its two nullable
/// public final fields. The Rust names are snake case while the serialized
/// fields retain Java's camel case. Read-only accessors preserve the Java
/// field boundary without allowing Rust callers to replace either value.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ClientGroup;
/// let group = ClientGroup::new(Some("client".into()), Some("workers".into()));
/// let _ = group.client_id;
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ClientGroup;
/// let mut group = ClientGroup::new(Some("client".into()), Some("workers".into()));
/// group.group = Some("other".into());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientGroup {
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ClientGroupFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group: Option<String>,
}

impl<'de> Deserialize<'de> for ClientGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ClientGroupFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            client_id: fields.client_id,
            group: fields.group,
        })
    }
}

impl ClientGroup {
    pub fn new(client_id: Option<String>, group: Option<String>) -> Self {
        Self { client_id, group }
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}

impl std::fmt::Display for ClientGroup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "ClientGroup{{clientId='{}', group='{}'}}",
            self.client_id.as_deref().unwrap_or("null"),
            self.group.as_deref().unwrap_or("null")
        )
    }
}

/// Java `GetLiteTopicInfoResponseBody`. Its subscriber Set and nested
/// `TopicOffset` are retained mutable objects; null body responses remain
/// distinct from an empty default instance.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GetLiteTopicInfoResponseBody;
/// let _ = GetLiteTopicInfoResponseBody::default().parent_topic;
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GetLiteTopicInfoResponseBody;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<GetLiteTopicInfoResponseBody>();
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteTopicInfoResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lite_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscriber: Option<SharedValue<HashSet<Option<ClientGroup>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_offset: Option<SharedValue<TopicOffset>>,
    sharding_to_broker: bool,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GetLiteTopicInfoResponseBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    parent_topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    lite_topic: Option<String>,
    #[serde(default)]
    subscriber: Option<HashSet<Option<ClientGroup>>>,
    #[serde(default)]
    topic_offset: Option<TopicOffset>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    sharding_to_broker: bool,
}

impl<'de> Deserialize<'de> for GetLiteTopicInfoResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: GetLiteTopicInfoResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            parent_topic: fields.parent_topic,
            lite_topic: fields.lite_topic,
            subscriber: fields.subscriber.map(SharedValue::new),
            topic_offset: fields.topic_offset.map(SharedValue::new),
            sharding_to_broker: fields.sharding_to_broker,
        })
    }
}

impl GetLiteTopicInfoResponseBody {
    pub fn get_parent_topic(&self) -> Option<&str> {
        self.parent_topic.as_deref()
    }

    pub fn set_parent_topic(&mut self, value: Option<String>) {
        self.parent_topic = value;
    }

    pub fn get_lite_topic(&self) -> Option<&str> {
        self.lite_topic.as_deref()
    }

    pub fn set_lite_topic(&mut self, value: Option<String>) {
        self.lite_topic = value;
    }

    pub fn get_subscriber(&self) -> Option<SharedValue<HashSet<Option<ClientGroup>>>> {
        self.subscriber.clone()
    }

    pub fn set_subscriber(&mut self, value: Option<SharedValue<HashSet<Option<ClientGroup>>>>) {
        self.subscriber = value;
    }

    pub fn get_topic_offset(&self) -> Option<SharedValue<TopicOffset>> {
        self.topic_offset.clone()
    }

    pub fn set_topic_offset(&mut self, value: Option<SharedValue<TopicOffset>>) {
        self.topic_offset = value;
    }

    pub fn is_sharding_to_broker(&self) -> bool {
        self.sharding_to_broker
    }

    pub fn set_sharding_to_broker(&mut self, value: bool) {
        self.sharding_to_broker = value;
    }

    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        java_fastjson_remoting_body_json(self, false).map(String::into_bytes)
    }

    fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        serde_json::from_str(&normalized)
    }
}

/// Java `GetBrokerLiteInfoResponseBody`, preserving nullable metadata maps and
/// mutable getter aliases alongside primitive counter defaults.
pub type BrokerLiteTopicMeta = SharedValue<BTreeMap<String, Option<i32>>>;
pub type BrokerLiteGroupMeta = SharedValue<BTreeMap<String, Option<HashSet<Option<String>>>>>;

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetBrokerLiteInfoResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    store_type: Option<String>,
    max_lmq_num: i32,
    current_lmq_num: i32,
    lite_subscription_count: i32,
    order_info_count: i32,
    cq_table_size: i32,
    offset_table_size: i32,
    event_map_size: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_meta: Option<BrokerLiteTopicMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_meta: Option<BrokerLiteGroupMeta>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct GetBrokerLiteInfoResponseBodyFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    store_type: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    max_lmq_num: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    current_lmq_num: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    lite_subscription_count: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    order_info_count: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    cq_table_size: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    offset_table_size: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    event_map_size: i32,
    topic_meta: Option<BTreeMap<String, Option<i32>>>,
    group_meta: Option<BTreeMap<String, Option<HashSet<Option<String>>>>>,
}

impl<'de> Deserialize<'de> for GetBrokerLiteInfoResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: GetBrokerLiteInfoResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            store_type: fields.store_type,
            max_lmq_num: fields.max_lmq_num,
            current_lmq_num: fields.current_lmq_num,
            lite_subscription_count: fields.lite_subscription_count,
            order_info_count: fields.order_info_count,
            cq_table_size: fields.cq_table_size,
            offset_table_size: fields.offset_table_size,
            event_map_size: fields.event_map_size,
            topic_meta: fields.topic_meta.map(SharedValue::new),
            group_meta: fields.group_meta.map(SharedValue::new),
        })
    }
}

impl GetBrokerLiteInfoResponseBody {
    pub fn get_store_type(&self) -> Option<&str> {
        self.store_type.as_deref()
    }

    pub fn set_store_type(&mut self, value: Option<String>) {
        self.store_type = value;
    }

    pub fn get_max_lmq_num(&self) -> i32 {
        self.max_lmq_num
    }

    pub fn set_max_lmq_num(&mut self, value: i32) {
        self.max_lmq_num = value;
    }

    pub fn get_current_lmq_num(&self) -> i32 {
        self.current_lmq_num
    }

    pub fn set_current_lmq_num(&mut self, value: i32) {
        self.current_lmq_num = value;
    }

    pub fn get_lite_subscription_count(&self) -> i32 {
        self.lite_subscription_count
    }

    pub fn set_lite_subscription_count(&mut self, value: i32) {
        self.lite_subscription_count = value;
    }

    pub fn get_order_info_count(&self) -> i32 {
        self.order_info_count
    }

    pub fn set_order_info_count(&mut self, value: i32) {
        self.order_info_count = value;
    }

    pub fn get_cq_table_size(&self) -> i32 {
        self.cq_table_size
    }

    pub fn set_cq_table_size(&mut self, value: i32) {
        self.cq_table_size = value;
    }

    pub fn get_offset_table_size(&self) -> i32 {
        self.offset_table_size
    }

    pub fn set_offset_table_size(&mut self, value: i32) {
        self.offset_table_size = value;
    }

    pub fn get_event_map_size(&self) -> i32 {
        self.event_map_size
    }

    pub fn set_event_map_size(&mut self, value: i32) {
        self.event_map_size = value;
    }

    pub fn get_topic_meta(&self) -> Option<BrokerLiteTopicMeta> {
        self.topic_meta.clone()
    }

    pub fn set_topic_meta(&mut self, value: Option<BrokerLiteTopicMeta>) {
        self.topic_meta = value;
    }

    pub fn get_group_meta(&self) -> Option<BrokerLiteGroupMeta> {
        self.group_meta.clone()
    }

    pub fn set_group_meta(&mut self, value: Option<BrokerLiteGroupMeta>) {
        self.group_meta = value;
    }

    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        java_fastjson_remoting_body_json(self, false).map(String::into_bytes)
    }

    fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        serde_json::from_str(&normalized)
    }
}

/// Java `GetParentTopicInfoResponseBody`, preserving nullable topic/group
/// values, primitive counter defaults and the mutable group Set getter alias.
pub type ParentTopicInfoGroups = SharedValue<HashSet<Option<String>>>;

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetParentTopicInfoResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    ttl: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    groups: Option<ParentTopicInfoGroups>,
    lmq_num: i32,
    lite_topic_count: i32,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct GetParentTopicInfoResponseBodyFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    topic: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    ttl: i32,
    groups: Option<HashSet<Option<String>>>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    lmq_num: i32,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    lite_topic_count: i32,
}

impl<'de> Deserialize<'de> for GetParentTopicInfoResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: GetParentTopicInfoResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            ttl: fields.ttl,
            groups: fields.groups.map(SharedValue::new),
            lmq_num: fields.lmq_num,
            lite_topic_count: fields.lite_topic_count,
        })
    }
}

impl GetParentTopicInfoResponseBody {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_ttl(&self) -> i32 {
        self.ttl
    }

    pub fn set_ttl(&mut self, value: i32) {
        self.ttl = value;
    }

    pub fn get_groups(&self) -> Option<ParentTopicInfoGroups> {
        self.groups.clone()
    }

    pub fn set_groups(&mut self, value: Option<ParentTopicInfoGroups>) {
        self.groups = value;
    }

    pub fn get_lmq_num(&self) -> i32 {
        self.lmq_num
    }

    pub fn set_lmq_num(&mut self, value: i32) {
        self.lmq_num = value;
    }

    pub fn get_lite_topic_count(&self) -> i32 {
        self.lite_topic_count
    }

    pub fn set_lite_topic_count(&mut self, value: i32) {
        self.lite_topic_count = value;
    }

    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        java_fastjson_remoting_body_json(self, false).map(String::into_bytes)
    }

    fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        serde_json::from_str(&normalized)
    }
}

/// Java `GetLiteClientInfoResponseBody`, preserving the nullable topic Set
/// and the mutable collection alias returned by its getter.
pub type LiteClientInfoTopicSet = SharedValue<HashSet<Option<String>>>;

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteClientInfoResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    last_access_time: i64,
    last_consume_time: i64,
    lite_topic_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    lite_topic_set: Option<LiteClientInfoTopicSet>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct GetLiteClientInfoResponseBodyFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    parent_topic: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    group: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    client_id: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    last_access_time: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    last_consume_time: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    lite_topic_count: i32,
    lite_topic_set: Option<HashSet<Option<String>>>,
}

impl<'de> Deserialize<'de> for GetLiteClientInfoResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: GetLiteClientInfoResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            parent_topic: fields.parent_topic,
            group: fields.group,
            client_id: fields.client_id,
            last_access_time: fields.last_access_time,
            last_consume_time: fields.last_consume_time,
            lite_topic_count: fields.lite_topic_count,
            lite_topic_set: fields.lite_topic_set.map(SharedValue::new),
        })
    }
}

impl GetLiteClientInfoResponseBody {
    pub fn get_parent_topic(&self) -> Option<&str> {
        self.parent_topic.as_deref()
    }

    pub fn set_parent_topic(&mut self, value: Option<String>) {
        self.parent_topic = value;
    }

    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, value: Option<String>) {
        self.group = value;
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, value: Option<String>) {
        self.client_id = value;
    }

    pub fn get_last_access_time(&self) -> i64 {
        self.last_access_time
    }

    pub fn set_last_access_time(&mut self, value: i64) {
        self.last_access_time = value;
    }

    pub fn get_last_consume_time(&self) -> i64 {
        self.last_consume_time
    }

    pub fn set_last_consume_time(&mut self, value: i64) {
        self.last_consume_time = value;
    }

    pub fn get_lite_topic_count(&self) -> i32 {
        self.lite_topic_count
    }

    pub fn set_lite_topic_count(&mut self, value: i32) {
        self.lite_topic_count = value;
    }

    pub fn get_lite_topic_set(&self) -> Option<LiteClientInfoTopicSet> {
        self.lite_topic_set.clone()
    }

    pub fn set_lite_topic_set(&mut self, value: Option<LiteClientInfoTopicSet>) {
        self.lite_topic_set = value;
    }

    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        java_fastjson_remoting_body_json(self, false).map(String::into_bytes)
    }

    fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        serde_json::from_str(&normalized)
    }
}

/// Java `LiteLagInfo`; `consumerOffset` remains readable but Fastjson omits it
/// from JSON output.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::LiteLagInfo;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<LiteLagInfo>();
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiteLagInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    lite_topic: Option<String>,
    lag_count: i64,
    earliest_unconsumed_timestamp: i64,
    #[serde(skip_serializing)]
    consumer_offset: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct LiteLagInfoFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    lite_topic: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    lag_count: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    earliest_unconsumed_timestamp: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    consumer_offset: i64,
}

impl Default for LiteLagInfoFields {
    fn default() -> Self {
        Self {
            lite_topic: None,
            lag_count: 0,
            earliest_unconsumed_timestamp: -1,
            consumer_offset: -1,
        }
    }
}

impl<'de> Deserialize<'de> for LiteLagInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LiteLagInfoFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            lite_topic: fields.lite_topic,
            lag_count: fields.lag_count,
            earliest_unconsumed_timestamp: fields.earliest_unconsumed_timestamp,
            consumer_offset: fields.consumer_offset,
        })
    }
}

impl Default for LiteLagInfo {
    fn default() -> Self {
        Self {
            lite_topic: None,
            lag_count: 0,
            earliest_unconsumed_timestamp: -1,
            consumer_offset: -1,
        }
    }
}

impl LiteLagInfo {
    pub fn get_lite_topic(&self) -> Option<&str> {
        self.lite_topic.as_deref()
    }

    pub fn set_lite_topic(&mut self, value: Option<String>) {
        self.lite_topic = value;
    }

    pub fn get_lag_count(&self) -> i64 {
        self.lag_count
    }

    pub fn set_lag_count(&mut self, value: i64) {
        self.lag_count = value;
    }

    pub fn get_earliest_unconsumed_timestamp(&self) -> i64 {
        self.earliest_unconsumed_timestamp
    }

    pub fn set_earliest_unconsumed_timestamp(&mut self, value: i64) {
        self.earliest_unconsumed_timestamp = value;
    }

    pub fn get_consumer_offset(&self) -> i64 {
        self.consumer_offset
    }

    pub fn set_consumer_offset(&mut self, value: i64) {
        self.consumer_offset = value;
    }
}

/// A nullable Java List with nullable elements and shared handles for its
/// mutable `LiteLagInfo` entries.
pub type LiteLagInfoList = SharedValue<Vec<Option<SharedValue<LiteLagInfo>>>>;

/// Java `GetLiteGroupInfoResponseBody`, including the mutable nested DTO and
/// list aliases exposed by its getters.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GetLiteGroupInfoResponseBody;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<GetLiteGroupInfoResponseBody>();
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteGroupInfoResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lite_topic: Option<String>,
    earliest_unconsumed_timestamp: i64,
    total_lag_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    lite_topic_offset_wrapper: Option<SharedValue<OffsetWrapper>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag_count_top_k: Option<LiteLagInfoList>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag_timestamp_top_k: Option<LiteLagInfoList>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct GetLiteGroupInfoResponseBodyFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    group: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    parent_topic: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    lite_topic: Option<String>,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    earliest_unconsumed_timestamp: i64,
    #[serde(deserialize_with = "serde_fastjson::deserialize_i64_reject_object")]
    total_lag_count: i64,
    lite_topic_offset_wrapper: Option<OffsetWrapper>,
    lag_count_top_k: Option<Vec<Option<LiteLagInfo>>>,
    lag_timestamp_top_k: Option<Vec<Option<LiteLagInfo>>>,
}

impl Default for GetLiteGroupInfoResponseBodyFields {
    fn default() -> Self {
        Self {
            group: None,
            parent_topic: None,
            lite_topic: None,
            earliest_unconsumed_timestamp: -1,
            total_lag_count: 0,
            lite_topic_offset_wrapper: None,
            lag_count_top_k: None,
            lag_timestamp_top_k: None,
        }
    }
}

impl Default for GetLiteGroupInfoResponseBody {
    fn default() -> Self {
        Self {
            group: None,
            parent_topic: None,
            lite_topic: None,
            earliest_unconsumed_timestamp: -1,
            total_lag_count: 0,
            lite_topic_offset_wrapper: None,
            lag_count_top_k: None,
            lag_timestamp_top_k: None,
        }
    }
}

impl<'de> Deserialize<'de> for GetLiteGroupInfoResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: GetLiteGroupInfoResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        let map_list = |value: Option<Vec<Option<LiteLagInfo>>>| {
            value.map(|items| {
                SharedValue::new(
                    items
                        .into_iter()
                        .map(|item| item.map(SharedValue::new))
                        .collect(),
                )
            })
        };
        Ok(Self {
            group: fields.group,
            parent_topic: fields.parent_topic,
            lite_topic: fields.lite_topic,
            earliest_unconsumed_timestamp: fields.earliest_unconsumed_timestamp,
            total_lag_count: fields.total_lag_count,
            lite_topic_offset_wrapper: fields.lite_topic_offset_wrapper.map(SharedValue::new),
            lag_count_top_k: map_list(fields.lag_count_top_k),
            lag_timestamp_top_k: map_list(fields.lag_timestamp_top_k),
        })
    }
}

impl GetLiteGroupInfoResponseBody {
    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, value: Option<String>) {
        self.group = value;
    }

    pub fn get_parent_topic(&self) -> Option<&str> {
        self.parent_topic.as_deref()
    }

    pub fn set_parent_topic(&mut self, value: Option<String>) {
        self.parent_topic = value;
    }

    pub fn get_lite_topic(&self) -> Option<&str> {
        self.lite_topic.as_deref()
    }

    pub fn set_lite_topic(&mut self, value: Option<String>) {
        self.lite_topic = value;
    }

    pub fn get_earliest_unconsumed_timestamp(&self) -> i64 {
        self.earliest_unconsumed_timestamp
    }

    pub fn set_earliest_unconsumed_timestamp(&mut self, value: i64) {
        self.earliest_unconsumed_timestamp = value;
    }

    pub fn get_total_lag_count(&self) -> i64 {
        self.total_lag_count
    }

    pub fn set_total_lag_count(&mut self, value: i64) {
        self.total_lag_count = value;
    }

    pub fn get_lite_topic_offset_wrapper(&self) -> Option<SharedValue<OffsetWrapper>> {
        self.lite_topic_offset_wrapper.clone()
    }

    pub fn set_lite_topic_offset_wrapper(&mut self, value: Option<SharedValue<OffsetWrapper>>) {
        self.lite_topic_offset_wrapper = value;
    }

    pub fn get_lag_count_top_k(&self) -> Option<LiteLagInfoList> {
        self.lag_count_top_k.clone()
    }

    pub fn set_lag_count_top_k(&mut self, value: Option<LiteLagInfoList>) {
        self.lag_count_top_k = value;
    }

    pub fn get_lag_timestamp_top_k(&self) -> Option<LiteLagInfoList> {
        self.lag_timestamp_top_k.clone()
    }

    pub fn set_lag_timestamp_top_k(&mut self, value: Option<LiteLagInfoList>) {
        self.lag_timestamp_top_k = value;
    }

    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        java_fastjson_remoting_body_json(self, false).map(String::into_bytes)
    }

    fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        serde_json::from_str(&normalized)
    }
}

/// Java `TopicStatsTable` response DTO. Its map and nested TopicOffset values
/// retain Java nullability and the map handle returned by the getter.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicStatsTable;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// requires_java_identity::<TopicStatsTable>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::TopicStatsTable;
/// let _ = TopicStatsTable::default().topic_put_tps;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatsTable {
    topic_put_tps: f64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "topic_stats_offset_table"
    )]
    offset_table: Option<SharedTopicStatsOffsetTable>,
}

pub type TopicStatsOffsetTable =
    std::collections::HashMap<MessageQueue, Option<SharedValue<TopicOffset>>>;
pub type SharedTopicStatsOffsetTable = SharedValue<TopicStatsOffsetTable>;

fn default_topic_stats_offset_table() -> Option<SharedTopicStatsOffsetTable> {
    Some(SharedValue::new(std::collections::HashMap::new()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct TopicStatsTableFields {
    #[serde(deserialize_with = "deserialize_fastjson_f64")]
    topic_put_tps: f64,
    #[serde(
        default = "default_topic_stats_offset_table",
        with = "topic_stats_offset_table"
    )]
    offset_table: Option<SharedTopicStatsOffsetTable>,
}

impl Default for TopicStatsTableFields {
    fn default() -> Self {
        Self {
            topic_put_tps: 0.0,
            offset_table: default_topic_stats_offset_table(),
        }
    }
}

impl<'de> Deserialize<'de> for TopicStatsTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: TopicStatsTableFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic_put_tps: fields.topic_put_tps,
            offset_table: fields.offset_table,
        })
    }
}

fn deserialize_fastjson_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    match raw {
        "null" | "false" => Ok(0.0),
        "true" => Ok(1.0),
        _ if raw.starts_with('"') => {
            let value = serde_json::from_str::<String>(raw).map_err(de::Error::custom)?;
            value.parse::<f64>().map_err(de::Error::custom)
        }
        _ => raw.parse::<f64>().map_err(de::Error::custom),
    }
}

impl Default for TopicStatsTable {
    fn default() -> Self {
        Self {
            topic_put_tps: 0.0,
            offset_table: default_topic_stats_offset_table(),
        }
    }
}

impl TopicStatsTable {
    pub fn get_offset_table(&self) -> Option<SharedTopicStatsOffsetTable> {
        self.offset_table.clone()
    }

    pub fn set_offset_table(&mut self, value: Option<SharedTopicStatsOffsetTable>) {
        self.offset_table = value;
    }

    pub fn get_topic_put_tps(&self) -> f64 {
        self.topic_put_tps
    }

    pub fn set_topic_put_tps(&mut self, value: f64) {
        self.topic_put_tps = value;
    }

    /// Encodes using Fastjson's MessageQueue object-key representation.
    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut fields = Vec::with_capacity(2);
        if let Some(offset_table) = &self.offset_table {
            fields.push(format!(
                "\"offsetTable\":{}",
                offset_table.with(topic_stats_offset_table::encode_java_json)?
            ));
        }
        fields.push(format!(
            "\"topicPutTps\":{}",
            serde_json::to_string(&self.topic_put_tps)?
        ));
        Ok(format!("{{{}}}", fields.join(",")).into_bytes())
    }

    fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized =
            normalize_java_object_map_keys(&serde_fastjson::normalize_json_input(input))?;
        serde_json::from_str(&normalized)
    }
}

/// Java `ConsumeStats` response DTO, including the mutable MessageQueue-keyed
/// offset table.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumeStats;
/// fn requires_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { requires_java_identity::<ConsumeStats>(); }
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeStats {
    consume_tps: f64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "consume_stats_offset_table"
    )]
    offset_table: Option<SharedConsumeStatsOffsetTable>,
}

pub type ConsumeStatsOffsetTable =
    std::collections::HashMap<MessageQueue, Option<SharedValue<OffsetWrapper>>>;
pub type SharedConsumeStatsOffsetTable = SharedValue<ConsumeStatsOffsetTable>;

fn default_consume_stats_offset_table() -> Option<SharedConsumeStatsOffsetTable> {
    Some(SharedValue::new(ConsumeStatsOffsetTable::new()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ConsumeStatsFields {
    #[serde(deserialize_with = "deserialize_fastjson_f64")]
    consume_tps: f64,
    #[serde(
        default = "default_consume_stats_offset_table",
        with = "consume_stats_offset_table"
    )]
    offset_table: Option<SharedConsumeStatsOffsetTable>,
}

impl Default for ConsumeStatsFields {
    fn default() -> Self {
        Self {
            consume_tps: 0.0,
            offset_table: default_consume_stats_offset_table(),
        }
    }
}

impl<'de> Deserialize<'de> for ConsumeStats {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ConsumeStatsFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consume_tps: fields.consume_tps,
            offset_table: fields.offset_table,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConsumeStatsComputationError {
    #[error("offsetTable is null")]
    NullOffsetTable,
    #[error("offsetTable contains a null OffsetWrapper")]
    NullOffsetWrapper,
}

impl Default for ConsumeStats {
    fn default() -> Self {
        Self {
            consume_tps: 0.0,
            offset_table: default_consume_stats_offset_table(),
        }
    }
}

impl ConsumeStats {
    pub fn get_offset_table(&self) -> Option<SharedConsumeStatsOffsetTable> {
        self.offset_table.clone()
    }

    pub fn set_offset_table(&mut self, value: Option<SharedConsumeStatsOffsetTable>) {
        self.offset_table = value;
    }

    pub fn get_consume_tps(&self) -> f64 {
        self.consume_tps
    }

    pub fn set_consume_tps(&mut self, value: f64) {
        self.consume_tps = value;
    }

    /// Computes Java's broker-minus-consumer offset total with signed-long
    /// wrapping arithmetic. Null tables or entries are returned as errors.
    pub fn compute_total_diff(&self) -> Result<i64, ConsumeStatsComputationError> {
        let table = self
            .offset_table
            .as_ref()
            .ok_or(ConsumeStatsComputationError::NullOffsetTable)?;
        table.with(|entries| {
            entries.values().try_fold(0_i64, |total, offset| {
                let offset = offset
                    .as_ref()
                    .ok_or(ConsumeStatsComputationError::NullOffsetWrapper)?;
                Ok(total.wrapping_add(
                    offset.with(|offset| offset.broker_offset.wrapping_sub(offset.consumer_offset)),
                ))
            })
        })
    }

    /// Computes Java's pull-minus-consumer in-flight total with signed-long
    /// wrapping arithmetic. Null tables or entries are returned as errors.
    pub fn compute_inflight_total_diff(&self) -> Result<i64, ConsumeStatsComputationError> {
        let table = self
            .offset_table
            .as_ref()
            .ok_or(ConsumeStatsComputationError::NullOffsetTable)?;
        table.with(|entries| {
            entries.values().try_fold(0_i64, |total, offset| {
                let offset = offset
                    .as_ref()
                    .ok_or(ConsumeStatsComputationError::NullOffsetWrapper)?;
                Ok(total.wrapping_add(
                    offset.with(|offset| offset.pull_offset.wrapping_sub(offset.consumer_offset)),
                ))
            })
        })
    }

    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut fields = Vec::with_capacity(2);
        fields.push(format!(
            "\"consumeTps\":{}",
            serde_json::to_string(&self.consume_tps)?
        ));
        if let Some(offset_table) = &self.offset_table {
            fields.push(format!(
                "\"offsetTable\":{}",
                offset_table.with(consume_stats_offset_table::encode_java_json)?
            ));
        }
        Ok(format!("{{{}}}", fields.join(",")).into_bytes())
    }

    fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized =
            normalize_java_object_map_keys(&serde_fastjson::normalize_json_input(input))?;
        serde_json::from_str(&normalized)
    }
}

pub type BrokerConsumeStatsGroup =
    HashMap<String, Option<SharedValue<Vec<Option<SharedValue<ConsumeStats>>>>>>;
pub type BrokerConsumeStatsGroups = Vec<Option<SharedValue<BrokerConsumeStatsGroup>>>;

fn encode_broker_consume_stats_group(
    group: &BrokerConsumeStatsGroup,
) -> Result<String, serde_json::Error> {
    let mut names = group.keys().collect::<Vec<_>>();
    names.sort();
    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        let stats = match group.get(name).expect("group key came from map") {
            None => "null".to_owned(),
            Some(stats) => stats.with(|stats| {
                let mut rows = Vec::with_capacity(stats.len());
                for stat in stats {
                    rows.push(match stat {
                        None => "null".to_owned(),
                        Some(stat) => {
                            let encoded = stat.with(|stat| stat.encode_java_json())?;
                            String::from_utf8_lossy(&encoded).into_owned()
                        }
                    });
                }
                Ok::<_, serde_json::Error>(format!("[{}]", rows.join(",")))
            })?,
        };
        entries.push(format!("{}:{stats}", serde_json::to_string(name)?));
    }
    Ok(format!("{{{}}}", entries.join(",")))
}

fn encode_broker_consume_stats_groups(
    groups: &BrokerConsumeStatsGroups,
) -> Result<String, serde_json::Error> {
    let mut encoded_groups = Vec::with_capacity(groups.len());
    for group in groups {
        encoded_groups.push(match group {
            None => "null".to_owned(),
            Some(group) => group.with(encode_broker_consume_stats_group)?,
        });
    }
    Ok(format!("[{}]", encoded_groups.join(",")))
}

/// Java `ConsumeStatsList` response body returned by request 317.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeStatsList {
    #[serde(skip_serializing_if = "Option::is_none")]
    consume_stats_list: Option<SharedValue<BrokerConsumeStatsGroups>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    broker_addr: Option<String>,
    total_diff: i64,
    total_inflight_diff: i64,
}

fn default_broker_consume_stats_groups() -> Option<Vec<Option<BrokerConsumeStatsGroup>>> {
    Some(Vec::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsumeStatsListFields {
    #[serde(default = "default_broker_consume_stats_groups")]
    consume_stats_list: Option<Vec<Option<BrokerConsumeStatsGroup>>>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    broker_addr: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    total_diff: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    total_inflight_diff: i64,
}

impl Default for ConsumeStatsListFields {
    fn default() -> Self {
        Self {
            consume_stats_list: default_broker_consume_stats_groups(),
            broker_addr: None,
            total_diff: 0,
            total_inflight_diff: 0,
        }
    }
}

impl<'de> Deserialize<'de> for ConsumeStatsList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ConsumeStatsListFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consume_stats_list: fields.consume_stats_list.map(|groups| {
                SharedValue::new(
                    groups
                        .into_iter()
                        .map(|group| group.map(SharedValue::new))
                        .collect(),
                )
            }),
            broker_addr: fields.broker_addr,
            total_diff: fields.total_diff,
            total_inflight_diff: fields.total_inflight_diff,
        })
    }
}

impl Default for ConsumeStatsList {
    fn default() -> Self {
        Self {
            consume_stats_list: default_broker_consume_stats_groups().map(|groups| {
                SharedValue::new(
                    groups
                        .into_iter()
                        .map(|group| group.map(SharedValue::new))
                        .collect(),
                )
            }),
            broker_addr: None,
            total_diff: 0,
            total_inflight_diff: 0,
        }
    }
}

impl ConsumeStatsList {
    /// Decodes the Java Fastjson body syntax, including nested MessageQueue
    /// objects used as keys in each consumer's `offsetTable`.
    pub fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        let normalized = normalize_java_object_map_keys(&normalized)?;
        serde_json::from_str(&normalized)
    }

    fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut fields = Vec::with_capacity(4);
        if let Some(broker_addr) = &self.broker_addr {
            fields.push(format!(
                "\"brokerAddr\":{}",
                serde_json::to_string(broker_addr)?
            ));
        }
        if let Some(consume_stats_list) = &self.consume_stats_list {
            let encoded = consume_stats_list.with(encode_broker_consume_stats_groups)?;
            fields.push(format!("\"consumeStatsList\":{encoded}"));
        }
        fields.push(format!("\"totalDiff\":{}", self.total_diff));
        fields.push(format!(
            "\"totalInflightDiff\":{}",
            self.total_inflight_diff
        ));
        Ok(format!("{{{}}}", fields.join(",")).into_bytes())
    }

    pub fn get_consume_stats_list(&self) -> Option<SharedValue<BrokerConsumeStatsGroups>> {
        self.consume_stats_list.clone()
    }

    pub fn set_consume_stats_list(&mut self, value: Option<SharedValue<BrokerConsumeStatsGroups>>) {
        self.consume_stats_list = value;
    }

    pub fn get_broker_addr(&self) -> Option<&str> {
        self.broker_addr.as_deref()
    }

    pub fn set_broker_addr(&mut self, value: Option<String>) {
        self.broker_addr = value;
    }

    pub fn get_total_diff(&self) -> i64 {
        self.total_diff
    }

    pub fn set_total_diff(&mut self, value: i64) {
        self.total_diff = value;
    }

    pub fn get_total_inflight_diff(&self) -> i64 {
        self.total_inflight_diff
    }

    pub fn set_total_inflight_diff(&mut self, value: i64) {
        self.total_inflight_diff = value;
    }
}

/// Java `BrokerStatsItem`, one minute/hour/day aggregate in `BrokerStatsData`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerStatsItem {
    sum: i64,
    tps: f64,
    avgpt: f64,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct BrokerStatsItemFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    sum: i64,
    #[serde(default, deserialize_with = "deserialize_fastjson_f64")]
    tps: f64,
    #[serde(default, deserialize_with = "deserialize_fastjson_f64")]
    avgpt: f64,
}

impl<'de> Deserialize<'de> for BrokerStatsItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: BrokerStatsItemFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            sum: fields.sum,
            tps: fields.tps,
            avgpt: fields.avgpt,
        })
    }
}

impl Default for BrokerStatsItem {
    fn default() -> Self {
        Self {
            sum: 0,
            tps: 0.0,
            avgpt: 0.0,
        }
    }
}

impl BrokerStatsItem {
    pub fn get_sum(&self) -> i64 {
        self.sum
    }

    pub fn set_sum(&mut self, value: i64) {
        self.sum = value;
    }

    pub fn get_tps(&self) -> f64 {
        self.tps
    }

    pub fn set_tps(&mut self, value: f64) {
        self.tps = value;
    }

    pub fn get_avgpt(&self) -> f64 {
        self.avgpt
    }

    pub fn set_avgpt(&mut self, value: f64) {
        self.avgpt = value;
    }
}

/// Java `BrokerStatsData`, returned by `VIEW_BROKER_STATS_DATA`.
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerStatsData {
    #[serde(skip_serializing_if = "Option::is_none")]
    stats_minute: Option<SharedValue<BrokerStatsItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats_hour: Option<SharedValue<BrokerStatsItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats_day: Option<SharedValue<BrokerStatsItem>>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct BrokerStatsDataFields {
    stats_minute: Option<BrokerStatsItem>,
    stats_hour: Option<BrokerStatsItem>,
    stats_day: Option<BrokerStatsItem>,
}

impl<'de> Deserialize<'de> for BrokerStatsData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: BrokerStatsDataFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            stats_minute: fields.stats_minute.map(SharedValue::new),
            stats_hour: fields.stats_hour.map(SharedValue::new),
            stats_day: fields.stats_day.map(SharedValue::new),
        })
    }
}

impl BrokerStatsData {
    pub fn get_stats_minute(&self) -> Option<SharedValue<BrokerStatsItem>> {
        self.stats_minute.clone()
    }

    pub fn set_stats_minute(&mut self, value: Option<SharedValue<BrokerStatsItem>>) {
        self.stats_minute = value;
    }

    pub fn get_stats_hour(&self) -> Option<SharedValue<BrokerStatsItem>> {
        self.stats_hour.clone()
    }

    pub fn set_stats_hour(&mut self, value: Option<SharedValue<BrokerStatsItem>>) {
        self.stats_hour = value;
    }

    pub fn get_stats_day(&self) -> Option<SharedValue<BrokerStatsItem>> {
        self.stats_day.clone()
    }

    pub fn set_stats_day(&mut self, value: Option<SharedValue<BrokerStatsItem>>) {
        self.stats_day = value;
    }
}

impl_java_remoting_serializable_adapters!(BrokerStatsData);

/// Java `SetMessageRequestModeRequestBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::SetMessageRequestModeRequestBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<SetMessageRequestModeRequestBody>();
/// ```
///
/// Java keeps these properties private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::SetMessageRequestModeRequestBody;
/// let _ = SetMessageRequestModeRequestBody::default().topic;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetMessageRequestModeRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_group: Option<String>,
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    mode: Option<MessageRequestMode>,
    #[serde(default)]
    pop_share_queue_num: i32,
}

impl SetMessageRequestModeRequestBody {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_mode(&self) -> Option<MessageRequestMode> {
        self.mode
    }

    pub fn set_mode(&mut self, mode: Option<MessageRequestMode>) {
        self.mode = mode;
    }

    pub fn get_pop_share_queue_num(&self) -> i32 {
        self.pop_share_queue_num
    }

    pub fn set_pop_share_queue_num(&mut self, pop_share_queue_num: i32) {
        self.pop_share_queue_num = pop_share_queue_num;
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetMessageRequestModeRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    mode: Option<MessageRequestMode>,
    #[serde(default, deserialize_with = "deserialize_fastjson_i32_object_as_zero")]
    pop_share_queue_num: i32,
}

fn deserialize_fastjson_i32_object_as_zero<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim_start().starts_with('{') {
        return Ok(0);
    }

    #[derive(Deserialize)]
    struct ValueField {
        #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
        value: i32,
    }

    let wrapped = format!("{{\"value\":{}}}", raw.get());
    serde_json::from_str::<ValueField>(&wrapped)
        .map(|field| field.value)
        .map_err(de::Error::custom)
}

impl<'de> Deserialize<'de> for SetMessageRequestModeRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: SetMessageRequestModeRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            consumer_group: fields.consumer_group,
            mode: fields.mode,
            pop_share_queue_num: fields.pop_share_queue_num,
        })
    }
}

impl Default for SetMessageRequestModeRequestBody {
    fn default() -> Self {
        Self {
            topic: None,
            consumer_group: None,
            mode: default_assignment_mode(),
            pop_share_queue_num: 0,
        }
    }
}

/// A compact, Java-compatible bit set used by [`BatchAck`].
///
/// Bit indexes use the same little-endian byte ordering as
/// `java.util.BitSet.toByteArray()`.
#[derive(Debug, Clone)]
pub struct BitSet {
    words: Vec<u64>,
    java_capacity_words: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitSetError {
    NegativeIndex(i32),
    NegativeCapacity(i32),
    InvalidRange { from: i32, to: i32 },
    IndexOverflow(usize),
    AllocationFailed(i32),
}

impl std::fmt::Display for BitSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeIndex(index) => {
                write!(formatter, "bit index must be nonnegative: {index}")
            }
            Self::NegativeCapacity(capacity) => {
                write!(
                    formatter,
                    "bit-set capacity must be nonnegative: {capacity}"
                )
            }
            Self::InvalidRange { from, to } => {
                write!(formatter, "bit range must have from <= to: {from}..{to}")
            }
            Self::IndexOverflow(index) => {
                write!(formatter, "bit index does not fit Java int: {index}")
            }
            Self::AllocationFailed(index) => {
                write!(formatter, "could not allocate bit index: {index}")
            }
        }
    }
}

impl std::error::Error for BitSetError {}

impl Default for BitSet {
    fn default() -> Self {
        Self {
            words: Vec::new(),
            java_capacity_words: 1,
        }
    }
}

impl PartialEq for BitSet {
    fn eq(&self, other: &Self) -> bool {
        self.words == other.words
    }
}

impl Eq for BitSet {}

impl BitSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a bit set with Java's requested initial capacity in bits.
    pub fn try_with_capacity_bits(capacity: i32) -> Result<Self, BitSetError> {
        let capacity =
            usize::try_from(capacity).map_err(|_| BitSetError::NegativeCapacity(capacity))?;
        let capacity_words = capacity.div_ceil(64);
        let mut words = Vec::new();
        words.try_reserve_exact(capacity_words).map_err(|_| {
            BitSetError::AllocationFailed(capacity.saturating_sub(1).min(i32::MAX as usize) as i32)
        })?;
        Ok(Self {
            words,
            java_capacity_words: capacity_words,
        })
    }

    /// Creates a bit set from Java's little-endian `BitSet.toByteArray()` form.
    pub fn from_byte_array(bytes: &[u8]) -> Self {
        let mut words = bytes
            .chunks(8)
            .map(|chunk| {
                let mut word = [0; 8];
                word[..chunk.len()].copy_from_slice(chunk);
                u64::from_le_bytes(word)
            })
            .collect::<Vec<_>>();
        while words.last() == Some(&0) {
            words.pop();
        }
        let java_capacity_words = words.len();
        Self {
            words,
            java_capacity_words,
        }
    }

    /// Creates a bit set from Java's native signed-long `BitSet.valueOf(long[])` form.
    pub fn try_from_long_array(values: &[i64]) -> Result<Self, BitSetError> {
        let mut words = Vec::new();
        words
            .try_reserve_exact(values.len())
            .map_err(|_| BitSetError::AllocationFailed(i32::MAX))?;
        words.extend(values.iter().map(|value| *value as u64));
        while words.last() == Some(&0) {
            words.pop();
        }
        let java_capacity_words = words.len();
        Ok(Self {
            words,
            java_capacity_words,
        })
    }

    /// Returns the Java `BitSet.size()` capacity in bits.
    pub fn size(&self) -> usize {
        self.java_capacity_words.saturating_mul(64)
    }

    /// Returns the minimal little-endian byte array used by Java's `toByteArray()`.
    pub fn to_byte_array(&self) -> Result<Vec<u8>, BitSetError> {
        let bit_length = self.length();
        let byte_length = bit_length.div_ceil(8);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(byte_length)
            .map_err(|_| BitSetError::AllocationFailed(bit_length.saturating_sub(1) as i32))?;
        bytes.resize(byte_length, 0);
        for (word_index, word) in self.words.iter().copied().enumerate() {
            let start = word_index * 8;
            if start >= byte_length {
                break;
            }
            let encoded = word.to_le_bytes();
            let count = (byte_length - start).min(encoded.len());
            bytes[start..start + count].copy_from_slice(&encoded[..count]);
        }
        Ok(bytes)
    }

    pub fn get(&self, index: i32) -> Result<bool, BitSetError> {
        let index = Self::checked_index(index)?;
        let word = index / 64;
        Ok(self
            .words
            .get(word)
            .is_some_and(|value| value & (1 << (index % 64)) != 0))
    }

    pub fn set(&mut self, index: i32) -> Result<(), BitSetError> {
        let index = Self::checked_index(index)?;
        let word_index = index / 64;
        self.ensure_words_len(word_index + 1, index as i32)?;
        self.words[word_index] |= 1 << (index % 64);
        Ok(())
    }

    /// Sets or clears one bit, matching Java's `set(index, value)` overload.
    pub fn set_with_value(&mut self, index: i32, value: bool) -> Result<(), BitSetError> {
        if value {
            self.set(index)
        } else {
            self.clear(index)
        }
    }

    pub fn clear(&mut self, index: i32) -> Result<(), BitSetError> {
        let index = Self::checked_index(index)?;
        let word_index = index / 64;
        if let Some(word) = self.words.get_mut(word_index) {
            *word &= !(1 << (index % 64));
            self.trim_trailing_zero_words();
        }
        Ok(())
    }

    pub fn flip(&mut self, index: i32) -> Result<(), BitSetError> {
        let index = Self::checked_index(index)?;
        if self.get(index as i32)? {
            self.clear(index as i32)
        } else {
            self.set(index as i32)
        }
    }

    /// Sets every bit in the half-open range `from..to`.
    pub fn set_range(&mut self, from: i32, to: i32) -> Result<(), BitSetError> {
        self.mutate_range(from, to, true, |word, mask| word | mask)
    }

    /// Clears every bit in the half-open range `from..to`.
    pub fn clear_range(&mut self, from: i32, to: i32) -> Result<(), BitSetError> {
        self.mutate_range(from, to, false, |word, mask| word & !mask)
    }

    /// Flips every bit in the half-open range `from..to`.
    pub fn flip_range(&mut self, from: i32, to: i32) -> Result<(), BitSetError> {
        self.mutate_range(from, to, true, |word, mask| word ^ mask)
    }

    /// Sets or clears every bit in the half-open range, matching Java's boolean overload.
    pub fn set_range_with_value(
        &mut self,
        from: i32,
        to: i32,
        value: bool,
    ) -> Result<(), BitSetError> {
        if value {
            self.set_range(from, to)
        } else {
            self.clear_range(from, to)
        }
    }

    /// Clears all bits while keeping this owned value reusable.
    pub fn clear_all(&mut self) {
        self.words.clear();
    }

    /// Returns a copy of the half-open range, shifted so `from` becomes bit zero.
    pub fn get_range(&self, from: i32, to: i32) -> Result<Self, BitSetError> {
        let (from, to) = Self::checked_range(from, to)?;
        let bit_count = to - from;
        if bit_count == 0 {
            return Ok(Self {
                words: Vec::new(),
                java_capacity_words: 0,
            });
        }

        let output_word_count = bit_count.div_ceil(64);
        let mut words = Vec::new();
        words
            .try_reserve_exact(output_word_count)
            .map_err(|_| BitSetError::AllocationFailed((to - 1) as i32))?;
        for output_word_index in 0..output_word_count {
            let source_bit_index = from + output_word_index * 64;
            let source_word_index = source_bit_index / 64;
            let shift = source_bit_index % 64;
            let lower = self.words.get(source_word_index).copied().unwrap_or(0) >> shift;
            let upper = if shift == 0 {
                0
            } else {
                self.words.get(source_word_index + 1).copied().unwrap_or(0) << (64 - shift)
            };
            let mut word = lower | upper;
            let remaining_bits = bit_count - output_word_index * 64;
            if remaining_bits < 64 {
                word &= (1_u64 << remaining_bits) - 1;
            }
            words.push(word);
        }
        while words.last() == Some(&0) {
            words.pop();
        }
        Ok(Self {
            words,
            java_capacity_words: output_word_count,
        })
    }

    /// Returns the next set bit at or after `from`, or `-1` when none exists.
    pub fn next_set_bit(&self, from: i32) -> Result<i32, BitSetError> {
        let from = Self::checked_index(from)?;
        let mut word_index = from / 64;
        let Some(first_word) = self.words.get(word_index).copied() else {
            return Ok(-1);
        };
        let first_candidate = first_word & (u64::MAX << (from % 64));
        if first_candidate != 0 {
            return Self::java_index(word_index * 64 + first_candidate.trailing_zeros() as usize);
        }
        word_index += 1;
        while let Some(word) = self.words.get(word_index).copied() {
            if word != 0 {
                return Self::java_index(word_index * 64 + word.trailing_zeros() as usize);
            }
            word_index += 1;
        }
        Ok(-1)
    }

    /// Returns the next clear bit at or after `from`.
    pub fn next_clear_bit(&self, from: i32) -> Result<i32, BitSetError> {
        let from = Self::checked_index(from)?;
        let mut word_index = from / 64;
        let Some(first_word) = self.words.get(word_index).copied() else {
            return Self::java_index(from);
        };
        let first_candidate = !first_word & (u64::MAX << (from % 64));
        if first_candidate != 0 {
            return Self::java_index(word_index * 64 + first_candidate.trailing_zeros() as usize);
        }
        word_index += 1;
        while let Some(word) = self.words.get(word_index).copied() {
            if word != u64::MAX {
                let candidate = !word;
                return Self::java_index(word_index * 64 + candidate.trailing_zeros() as usize);
            }
            word_index += 1;
        }
        Self::java_index(self.words.len() * 64)
    }

    /// Returns the previous set bit at or before `from`, or `-1` when none exists.
    /// Java defines `previousSetBit(-1)` as `-1` as a special case.
    pub fn previous_set_bit(&self, from: i32) -> Result<i32, BitSetError> {
        if from == -1 {
            return Ok(-1);
        }
        if from < -1 {
            return Err(BitSetError::NegativeIndex(from));
        }
        let from = from as usize;
        if self.words.is_empty() {
            return Ok(-1);
        }
        let mut word_index = (from / 64).min(self.words.len() - 1);
        let bit_offset = from.min(word_index * 64 + 63) % 64;
        let mask = if bit_offset == 63 {
            u64::MAX
        } else {
            (1_u64 << (bit_offset + 1)) - 1
        };
        let candidate = self.words[word_index] & mask;
        if candidate != 0 {
            return Self::java_index(word_index * 64 + (63 - candidate.leading_zeros() as usize));
        }
        while word_index > 0 {
            word_index -= 1;
            let candidate = self.words[word_index];
            if candidate != 0 {
                return Self::java_index(
                    word_index * 64 + (63 - candidate.leading_zeros() as usize),
                );
            }
        }
        Ok(-1)
    }

    /// Returns the previous clear bit at or before `from`, or `-1` when none exists.
    /// Java defines `previousClearBit(-1)` as `-1` as a special case.
    pub fn previous_clear_bit(&self, from: i32) -> Result<i32, BitSetError> {
        if from == -1 {
            return Ok(-1);
        }
        if from < -1 {
            return Err(BitSetError::NegativeIndex(from));
        }
        let from = from as usize;
        if from / 64 >= self.words.len() {
            return Self::java_index(from);
        }
        let mut word_index = from / 64;
        let bit_offset = from % 64;
        let mask = if bit_offset == 63 {
            u64::MAX
        } else {
            (1_u64 << (bit_offset + 1)) - 1
        };
        let candidate = !self.words[word_index] & mask;
        if candidate != 0 {
            return Self::java_index(word_index * 64 + (63 - candidate.leading_zeros() as usize));
        }
        while word_index > 0 {
            word_index -= 1;
            let candidate = !self.words[word_index];
            if candidate != 0 {
                return Self::java_index(
                    word_index * 64 + (63 - candidate.leading_zeros() as usize),
                );
            }
        }
        Ok(-1)
    }

    /// Returns true if this bit set and `other` share at least one set bit.
    pub fn intersects(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(&other.words)
            .any(|(left, right)| left & right != 0)
    }

    /// Replaces this value with its bitwise intersection with `other`.
    pub fn and(&mut self, other: &Self) {
        for (index, word) in self.words.iter_mut().enumerate() {
            *word &= other.words.get(index).copied().unwrap_or(0);
        }
        self.trim_trailing_zero_words();
    }

    /// Adds every set bit from `other` to this value.
    pub fn or(&mut self, other: &Self) -> Result<(), BitSetError> {
        self.ensure_words_len(
            other.words.len(),
            other.length().saturating_sub(1).min(i32::MAX as usize) as i32,
        )?;
        for (word, other_word) in self.words.iter_mut().zip(&other.words) {
            *word |= other_word;
        }
        Ok(())
    }

    /// Replaces this value with its bitwise exclusive-or with `other`.
    pub fn xor(&mut self, other: &Self) -> Result<(), BitSetError> {
        self.ensure_words_len(
            other.words.len(),
            other.length().saturating_sub(1).min(i32::MAX as usize) as i32,
        )?;
        for (word, other_word) in self.words.iter_mut().zip(&other.words) {
            *word ^= other_word;
        }
        self.trim_trailing_zero_words();
        Ok(())
    }

    /// Clears every bit present in `other` from this value.
    pub fn and_not(&mut self, other: &Self) {
        for (word, other_word) in self.words.iter_mut().zip(&other.words) {
            *word &= !other_word;
        }
        self.trim_trailing_zero_words();
    }

    /// Returns the used 64-bit words in Java's `toLongArray()` order.
    pub fn to_long_array(&self) -> Result<Vec<i64>, BitSetError> {
        let mut words = Vec::new();
        words
            .try_reserve_exact(self.words.len())
            .map_err(|_| BitSetError::AllocationFailed(self.length().saturating_sub(1) as i32))?;
        words.extend(self.words.iter().map(|word| *word as i64));
        Ok(words)
    }

    /// Returns the Java `BitSet.hashCode()` value.
    pub fn java_hash_code(&self) -> i32 {
        let hash = self
            .words
            .iter()
            .enumerate()
            .fold(1234_u64, |hash, (index, word)| {
                hash ^ word.wrapping_mul(index as u64 + 1)
            });
        ((hash >> 32) ^ hash) as i32
    }

    /// Iterates over set-bit indexes in ascending order.
    pub fn iter_set_bits(&self) -> impl Iterator<Item = usize> + '_ {
        let mut next_word_index = 0;
        let mut active_word_index = 0;
        let mut remaining = 0_u64;
        std::iter::from_fn(move || {
            loop {
                if remaining != 0 {
                    let bit = remaining.trailing_zeros() as usize;
                    remaining &= remaining - 1;
                    return Some(active_word_index * 64 + bit);
                }
                let word = self.words.get(next_word_index).copied()?;
                active_word_index = next_word_index;
                next_word_index += 1;
                remaining = word;
            }
        })
    }

    pub fn cardinality(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    /// Returns one plus the highest set-bit index, or zero for an empty set.
    pub fn length(&self) -> usize {
        self.words.last().map_or(0, |word| {
            (self.words.len() - 1) * 64 + (64 - word.leading_zeros() as usize)
        })
    }

    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    fn checked_index(index: i32) -> Result<usize, BitSetError> {
        usize::try_from(index).map_err(|_| BitSetError::NegativeIndex(index))
    }

    fn checked_range(from: i32, to: i32) -> Result<(usize, usize), BitSetError> {
        let from_index = Self::checked_index(from)?;
        if to < 0 {
            return Err(BitSetError::NegativeIndex(to));
        }
        if from > to {
            return Err(BitSetError::InvalidRange { from, to });
        }
        Ok((from_index, to as usize))
    }

    fn mutate_range(
        &mut self,
        from: i32,
        to: i32,
        may_grow: bool,
        operation: fn(u64, u64) -> u64,
    ) -> Result<(), BitSetError> {
        let (from, to) = Self::checked_range(from, to)?;
        if from == to {
            return Ok(());
        }
        let first_word = from / 64;
        let mut last_word = (to - 1) / 64;
        if may_grow {
            self.ensure_words_len(last_word + 1, (to - 1) as i32)?;
        } else if first_word >= self.words.len() {
            return Ok(());
        } else {
            last_word = last_word.min(self.words.len() - 1);
        }
        for word_index in first_word..=last_word {
            let word_start = word_index * 64;
            let start_bit = from.saturating_sub(word_start).min(64);
            let end_bit = (to - word_start).min(64);
            let lower_mask = u64::MAX << start_bit;
            let upper_mask = if end_bit == 64 {
                u64::MAX
            } else {
                (1_u64 << end_bit) - 1
            };
            self.words[word_index] = operation(self.words[word_index], lower_mask & upper_mask);
        }
        self.trim_trailing_zero_words();
        Ok(())
    }

    fn java_index(index: usize) -> Result<i32, BitSetError> {
        i32::try_from(index).map_err(|_| BitSetError::IndexOverflow(index))
    }

    fn ensure_words_len(
        &mut self,
        words_len: usize,
        allocation_index: i32,
    ) -> Result<(), BitSetError> {
        let java_capacity_words = if words_len > self.java_capacity_words {
            self.java_capacity_words.saturating_mul(2).max(words_len)
        } else {
            self.java_capacity_words
        };
        if words_len > self.words.len() {
            self.words
                .try_reserve(words_len - self.words.len())
                .map_err(|_| BitSetError::AllocationFailed(allocation_index))?;
            self.words.resize(words_len, 0);
        }
        self.java_capacity_words = java_capacity_words;
        Ok(())
    }

    fn trim_trailing_zero_words(&mut self) {
        while self.words.last() == Some(&0) {
            self.words.pop();
        }
    }

    pub fn java_to_string(&self) -> String {
        let mut output = String::from("{");
        let mut first = true;
        for (word_index, word) in self.words.iter().copied().enumerate() {
            let mut remaining = word;
            while remaining != 0 {
                let bit = remaining.trailing_zeros() as usize;
                if !first {
                    output.push_str(", ");
                }
                first = false;
                output.push_str(&(word_index * 64 + bit).to_string());
                remaining &= remaining - 1;
            }
        }
        output.push('}');
        output
    }
}

mod bitset_base64 {
    use super::BitSet;
    use crate::SharedValue;
    use crate::serde_fastjson;
    use base64::{
        Engine as _, alphabet, engine::general_purpose::GeneralPurpose,
        engine::general_purpose::GeneralPurposeConfig, engine::general_purpose::NO_PAD,
        engine::general_purpose::STANDARD,
    };
    use serde::{Deserializer, Serializer};

    // Java's Basic decoder ignores unused low bits in the final Base64 symbol.
    const JAVA_STANDARD: GeneralPurpose = GeneralPurpose::new(
        &alphabet::STANDARD,
        GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
    );
    const JAVA_STANDARD_NO_PAD: GeneralPurpose = GeneralPurpose::new(
        &alphabet::STANDARD,
        NO_PAD.with_decode_allow_trailing_bits(true),
    );

    pub fn serialize<S>(
        bit_set: &Option<SharedValue<BitSet>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bit_set {
            Some(bit_set) => {
                let bytes = bit_set
                    .with(BitSet::to_byte_array)
                    .map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(&STANDARD.encode(bytes))
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<BitSet>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_fastjson::deserialize_nullable_string(deserializer)?;
        value
            .map(|text| {
                JAVA_STANDARD
                    .decode(&text)
                    .or_else(|_| JAVA_STANDARD_NO_PAD.decode(&text))
                    .map(|bytes| BitSet::from_byte_array(&bytes))
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
    }
}

fn is_empty_bitset(value: &Option<SharedValue<BitSet>>) -> bool {
    value
        .as_ref()
        .is_none_or(|bit_set| bit_set.with(BitSet::is_empty))
}

fn deserialize_batch_ack_list<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<Option<BatchAck>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    match raw.get().trim() {
        // Fastjson's List converter treats a Boolean token as a singleton null row.
        "true" | "false" => Ok(Some(vec![None])),
        raw => {
            let mut deserializer = serde_json::Deserializer::from_str(raw);
            let rows =
                serde_fastjson::deserialize_optional_nullable_vec_or_single(&mut deserializer)
                    .map_err(de::Error::custom)?;
            deserializer.end().map_err(de::Error::custom)?;
            Ok(rows)
        }
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::BatchAckMessageRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<BatchAckMessageRequestBody>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::BatchAckMessageRequestBody;
/// let _ = BatchAckMessageRequestBody::default().broker_name;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BatchAckMessageRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    broker_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acks: Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchAckMessageRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    broker_name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_batch_ack_list")]
    acks: Option<Vec<Option<BatchAck>>>,
}

impl<'de> Deserialize<'de> for BatchAckMessageRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: BatchAckMessageRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            broker_name: fields.broker_name,
            acks: fields.acks.map(|acks| {
                SharedValue::new(
                    acks.into_iter()
                        .map(|ack| ack.map(SharedValue::new))
                        .collect(),
                )
            }),
        })
    }
}

impl BatchAckMessageRequestBody {
    /// Encodes the inherited Java static encode(Object) form, preserving its null result.
    pub fn java_encode_optional(
        value: Option<&Self>,
    ) -> Result<Option<Vec<u8>>, serde_json::Error> {
        value.map(Self::java_encode).transpose()
    }

    /// Encodes the inherited Java static toJson(Object, boolean) form.
    pub fn java_to_json_optional(
        value: Option<&Self>,
        pretty_format: bool,
    ) -> Result<String, serde_json::Error> {
        value.map_or_else(
            || Ok("null".to_owned()),
            |value| value.java_to_json(pretty_format),
        )
    }

    /// Encodes the inherited Java RemotingSerializable.toJson(boolean) form.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        crate::route::java_fastjson_json(self, pretty_format)
    }

    /// Encodes the inherited Java RemotingSerializable.encode() UTF-8 bytes.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Encodes with the supported inherited Fastjson2 writer features.
    pub fn java_encode_with_features(
        &self,
        features: &[JavaJsonWriterFeature],
    ) -> Result<Vec<u8>, serde_json::Error> {
        Ok(crate::route::java_fastjson_json_with_features(self, false, features)?.into_bytes())
    }

    /// Decodes the inherited Java RemotingSerializable.decode form.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes the nullable inherited Java utility, including null bytes and a JSON null root.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice_nullable(data)
    }

    /// Decodes the inherited Java RemotingSerializable.decodeList form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes a nullable Java list, preserving null roots and members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        serde_fastjson::from_fastjson_list_nullable(data)
    }

    /// Decodes the inherited Java RemotingSerializable.fromJson form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_str(json)
    }

    /// Decodes the nullable inherited Java fromJson utility form.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_str_nullable(json)
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, broker_name: Option<String>) {
        self.broker_name = broker_name;
    }

    /// Returns a handle to the same mutable List object assigned to the Java DTO.
    pub fn get_acks(&self) -> Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>> {
        self.acks.clone()
    }

    pub fn set_acks(&mut self, acks: Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>) {
        self.acks = acks;
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::BatchAck;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<BatchAck>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::BatchAck;
/// let _ = BatchAck::default().consumer_group;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
pub struct BatchAck {
    #[serde(
        rename = "c",
        alias = "consumerGroup",
        skip_serializing_if = "Option::is_none"
    )]
    consumer_group: Option<String>,
    #[serde(rename = "t", alias = "topic", skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(rename = "r", alias = "retry", skip_serializing_if = "Option::is_none")]
    retry: Option<String>,
    #[serde(rename = "so", alias = "startOffset")]
    start_offset: i64,
    #[serde(rename = "q", alias = "queueId")]
    queue_id: i32,
    #[serde(rename = "rq", alias = "reviveQueueId")]
    revive_queue_id: i32,
    #[serde(rename = "pt", alias = "popTime")]
    pop_time: i64,
    #[serde(rename = "it", alias = "invisibleTime")]
    invisible_time: i64,
    #[serde(
        rename = "b",
        alias = "bitSet",
        with = "bitset_base64",
        skip_serializing_if = "is_empty_bitset"
    )]
    bit_set: Option<SharedValue<BitSet>>,
}

#[derive(Deserialize)]
struct BatchAckFields {
    #[serde(
        default,
        rename = "c",
        alias = "consumerGroup",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        rename = "t",
        alias = "topic",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default,
        rename = "r",
        alias = "retry",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    retry: Option<String>,
    #[serde(
        default,
        rename = "so",
        alias = "startOffset",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    start_offset: i64,
    #[serde(
        default,
        rename = "q",
        alias = "queueId",
        deserialize_with = "deserialize_fastjson_i32_object_as_zero"
    )]
    queue_id: i32,
    #[serde(
        default,
        rename = "rq",
        alias = "reviveQueueId",
        deserialize_with = "deserialize_fastjson_i32_object_as_zero"
    )]
    revive_queue_id: i32,
    #[serde(
        default,
        rename = "pt",
        alias = "popTime",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    pop_time: i64,
    #[serde(
        default,
        rename = "it",
        alias = "invisibleTime",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    invisible_time: i64,
    #[serde(default, rename = "b", alias = "bitSet", with = "bitset_base64")]
    bit_set: Option<BitSet>,
}

impl<'de> Deserialize<'de> for BatchAck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: BatchAckFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consumer_group: fields.consumer_group,
            topic: fields.topic,
            retry: fields.retry,
            start_offset: fields.start_offset,
            queue_id: fields.queue_id,
            revive_queue_id: fields.revive_queue_id,
            pop_time: fields.pop_time,
            invisible_time: fields.invisible_time,
            bit_set: fields.bit_set.map(SharedValue::new),
        })
    }
}

impl BatchAck {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn get_retry(&self) -> Option<&str> {
        self.retry.as_deref()
    }

    pub fn set_retry(&mut self, retry: Option<String>) {
        self.retry = retry;
    }

    pub fn get_start_offset(&self) -> i64 {
        self.start_offset
    }

    pub fn set_start_offset(&mut self, start_offset: i64) {
        self.start_offset = start_offset;
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    pub fn get_revive_queue_id(&self) -> i32 {
        self.revive_queue_id
    }

    pub fn set_revive_queue_id(&mut self, revive_queue_id: i32) {
        self.revive_queue_id = revive_queue_id;
    }

    pub fn get_pop_time(&self) -> i64 {
        self.pop_time
    }

    pub fn set_pop_time(&mut self, pop_time: i64) {
        self.pop_time = pop_time;
    }

    pub fn get_invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub fn set_invisible_time(&mut self, invisible_time: i64) {
        self.invisible_time = invisible_time;
    }

    pub fn get_bit_set(&self) -> Option<SharedValue<BitSet>> {
        self.bit_set.clone()
    }

    pub fn set_bit_set(&mut self, bit_set: Option<SharedValue<BitSet>>) {
        self.bit_set = bit_set;
    }
}

impl std::fmt::Display for BatchAck {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consumer_group = self.consumer_group.as_deref().unwrap_or("null");
        let topic = self.topic.as_deref().unwrap_or("null");
        let retry = self.retry.as_deref().unwrap_or("null");
        let bit_set = self.bit_set.as_ref().map_or_else(
            || "null".to_owned(),
            |bit_set| bit_set.with(BitSet::java_to_string),
        );
        write!(
            formatter,
            "BatchAck{{consumerGroup='{consumer_group}', topic='{topic}', retry='{retry}', startOffset={}, queueId={}, reviveQueueId={}, popTime={}, invisibleTime={}, bitSet={bit_set}}}",
            self.start_offset,
            self.queue_id,
            self.revive_queue_id,
            self.pop_time,
            self.invisible_time
        )
    }
}

pub type MessageRequestModeGroup =
    std::collections::BTreeMap<String, SharedValue<SetMessageRequestModeRequestBody>>;
pub type SharedMessageRequestModeGroup = SharedValue<MessageRequestModeGroup>;
pub type MessageRequestModeMap = std::collections::BTreeMap<String, SharedMessageRequestModeGroup>;
pub type SharedMessageRequestModeMap = SharedValue<MessageRequestModeMap>;

#[derive(Debug, Clone, Serialize)]
/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::MessageRequestModeSerializeWrapper;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<MessageRequestModeSerializeWrapper>(); }
/// ```
///
/// Java keeps this property private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::MessageRequestModeSerializeWrapper;
/// let _ = MessageRequestModeSerializeWrapper::default().message_request_mode_map;
/// ```
#[serde(rename_all = "camelCase")]
pub struct MessageRequestModeSerializeWrapper {
    #[serde(skip_serializing_if = "Option::is_none")]
    message_request_mode_map: Option<SharedMessageRequestModeMap>,
}

impl Default for MessageRequestModeSerializeWrapper {
    fn default() -> Self {
        Self {
            message_request_mode_map: Some(SharedValue::new(std::collections::BTreeMap::new())),
        }
    }
}

impl MessageRequestModeSerializeWrapper {
    pub fn get_message_request_mode_map(&self) -> Option<SharedMessageRequestModeMap> {
        self.message_request_mode_map.clone()
    }

    pub fn set_message_request_mode_map(
        &mut self,
        message_request_mode_map: Option<SharedMessageRequestModeMap>,
    ) {
        self.message_request_mode_map = message_request_mode_map;
    }
}

fn default_message_request_mode_map() -> Option<
    std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, SetMessageRequestModeRequestBody>,
    >,
> {
    Some(std::collections::BTreeMap::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageRequestModeSerializeWrapperFields {
    #[serde(default = "default_message_request_mode_map")]
    message_request_mode_map: Option<
        std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, SetMessageRequestModeRequestBody>,
        >,
    >,
}

impl<'de> Deserialize<'de> for MessageRequestModeSerializeWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: MessageRequestModeSerializeWrapperFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        let message_request_mode_map = fields.message_request_mode_map.map(|topics| {
            SharedValue::new(
                topics
                    .into_iter()
                    .map(|(topic, groups)| {
                        let groups = SharedValue::new(
                            groups
                                .into_iter()
                                .map(|(group, body)| (group, SharedValue::new(body)))
                                .collect(),
                        );
                        (topic, groups)
                    })
                    .collect(),
            )
        });
        Ok(Self {
            message_request_mode_map,
        })
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::LockBatchRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<LockBatchRequestBody>(); }
/// ```
///
/// Java keeps these properties private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::LockBatchRequestBody;
/// let _ = LockBatchRequestBody::default().consumer_group;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchRequestBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    consumer_group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(default)]
    only_this_broker: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    mq_set: Option<SharedMessageQueueSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LockBatchRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    only_this_broker: bool,
    #[serde(
        default = "default_message_queue_set",
        deserialize_with = "deserialize_optional_message_queue_set"
    )]
    mq_set: Option<MessageQueueSet>,
}

impl<'de> Deserialize<'de> for LockBatchRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LockBatchRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consumer_group: fields.consumer_group,
            client_id: fields.client_id,
            only_this_broker: fields.only_this_broker,
            mq_set: fields.mq_set.map(SharedValue::new),
        })
    }
}

impl Default for LockBatchRequestBody {
    fn default() -> Self {
        Self {
            consumer_group: None,
            client_id: None,
            only_this_broker: false,
            mq_set: default_message_queue_set().map(SharedValue::new),
        }
    }
}

impl LockBatchRequestBody {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn is_only_this_broker(&self) -> bool {
        self.only_this_broker
    }

    pub fn set_only_this_broker(&mut self, only_this_broker: bool) {
        self.only_this_broker = only_this_broker;
    }

    /// Returns a handle to the same mutable Set object assigned to the Java DTO.
    pub fn get_mq_set(&self) -> Option<SharedMessageQueueSet> {
        self.mq_set.clone()
    }

    pub fn set_mq_set(&mut self, mq_set: Option<SharedMessageQueueSet>) {
        self.mq_set = mq_set;
    }
}

impl std::fmt::Display for LockBatchRequestBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "LockBatchRequestBody{{consumerGroup={}, clientId={}, onlyThisBroker={}, mqSet={}}}",
            self.consumer_group.as_deref().unwrap_or("null"),
            self.client_id.as_deref().unwrap_or("null"),
            self.only_this_broker,
            java_message_queue_set_text(&self.mq_set)
        )
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::UnlockBatchRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<UnlockBatchRequestBody>(); }
/// ```
///
/// Java keeps these properties private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::UnlockBatchRequestBody;
/// let _ = UnlockBatchRequestBody::default().consumer_group;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnlockBatchRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    only_this_broker: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    mq_set: Option<SharedMessageQueueSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UnlockBatchRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    only_this_broker: bool,
    #[serde(
        default = "default_message_queue_set",
        deserialize_with = "deserialize_optional_message_queue_set"
    )]
    mq_set: Option<MessageQueueSet>,
}

impl<'de> Deserialize<'de> for UnlockBatchRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: UnlockBatchRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consumer_group: fields.consumer_group,
            client_id: fields.client_id,
            only_this_broker: fields.only_this_broker,
            mq_set: fields.mq_set.map(SharedValue::new),
        })
    }
}

impl Default for UnlockBatchRequestBody {
    fn default() -> Self {
        Self {
            consumer_group: None,
            client_id: None,
            only_this_broker: false,
            mq_set: default_message_queue_set().map(SharedValue::new),
        }
    }
}

impl UnlockBatchRequestBody {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn is_only_this_broker(&self) -> bool {
        self.only_this_broker
    }

    pub fn set_only_this_broker(&mut self, only_this_broker: bool) {
        self.only_this_broker = only_this_broker;
    }

    /// Returns a handle to the same mutable Set object assigned to the Java DTO.
    pub fn get_mq_set(&self) -> Option<SharedMessageQueueSet> {
        self.mq_set.clone()
    }

    pub fn set_mq_set(&mut self, mq_set: Option<SharedMessageQueueSet>) {
        self.mq_set = mq_set;
    }
}

impl std::fmt::Display for UnlockBatchRequestBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "UnlockBatchRequestBody{{consumerGroup={}, clientId={}, onlyThisBroker={}, mqSet={}}}",
            self.consumer_group.as_deref().unwrap_or("null"),
            self.client_id.as_deref().unwrap_or("null"),
            self.only_this_broker,
            java_message_queue_set_text(&self.mq_set)
        )
    }
}

fn java_message_queue_set_text(queues: &Option<SharedMessageQueueSet>) -> String {
    match queues {
        None => "null".to_owned(),
        Some(queues) => queues.with(|queues| {
            format!(
                "[{}]",
                queues
                    .iter()
                    .map(|queue| queue
                        .as_ref()
                        .map(|message_queue| message_queue.with(ToString::to_string))
                        .unwrap_or_else(|| "null".to_owned()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }),
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::LockBatchResponseBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<LockBatchResponseBody>(); }
/// ```
///
/// Java exposes the Set through accessors rather than a public field.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::LockBatchResponseBody;
/// let _ = LockBatchResponseBody::default().lock_okmq_set;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchResponseBody {
    #[serde(rename = "lockOKMQSet", skip_serializing_if = "Option::is_none")]
    lock_okmq_set: Option<SharedMessageQueueSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LockBatchResponseBodyFields {
    #[serde(
        default = "default_message_queue_set",
        rename = "lockOKMQSet",
        deserialize_with = "deserialize_optional_message_queue_set"
    )]
    lock_okmq_set: Option<MessageQueueSet>,
}

impl<'de> Deserialize<'de> for LockBatchResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LockBatchResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            lock_okmq_set: fields.lock_okmq_set.map(SharedValue::new),
        })
    }
}

impl Default for LockBatchResponseBody {
    fn default() -> Self {
        Self {
            lock_okmq_set: default_message_queue_set().map(SharedValue::new),
        }
    }
}

impl LockBatchResponseBody {
    /// Returns a handle to the same mutable Set object assigned to the Java DTO.
    pub fn get_lock_okmq_set(&self) -> Option<SharedMessageQueueSet> {
        self.lock_okmq_set.clone()
    }

    pub fn set_lock_okmq_set(&mut self, lock_okmq_set: Option<SharedMessageQueueSet>) {
        self.lock_okmq_set = lock_okmq_set;
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerListByGroupResponseBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<ConsumerListByGroupResponseBody>(); }
/// ```
///
/// Java exposes this DTO's list through accessors rather than a public field.
/// The field must stay private so Rust callers cannot bypass those methods.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerListByGroupResponseBody;
/// let _ = ConsumerListByGroupResponseBody::default().consumer_id_list;
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerListByGroupResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer_id_list: Option<SharedValue<Vec<Option<String>>>>,
}

impl<'de> Deserialize<'de> for ConsumerListByGroupResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConsumerListVisitor;

        impl<'de> de::Visitor<'de> for ConsumerListVisitor {
            type Value = ConsumerListByGroupResponseBody;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a consumer-list response object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut raw_consumer_id_list = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "consumerIdList" {
                        raw_consumer_id_list =
                            Some(map.next_value::<Box<serde_json::value::RawValue>>()?);
                    } else {
                        map.next_value::<de::IgnoredAny>()?;
                    }
                }

                let consumer_id_list = match raw_consumer_id_list {
                    Some(raw) => {
                        // Fastjson retains the last duplicate property before coercing its value.
                        let mut raw_deserializer = serde_json::Deserializer::from_str(raw.get());
                        let value = serde_fastjson::deserialize_optional_nullable_string_list(
                            &mut raw_deserializer,
                        )
                        .map_err(de::Error::custom)?;
                        raw_deserializer.end().map_err(de::Error::custom)?;
                        value
                    }
                    None => None,
                };

                Ok(ConsumerListByGroupResponseBody {
                    consumer_id_list: consumer_id_list.map(SharedValue::new),
                })
            }
        }

        deserializer.deserialize_map(ConsumerListVisitor)
    }
}

impl ConsumerListByGroupResponseBody {
    /// Returns a handle to the same mutable List object assigned to the Java DTO.
    pub fn get_consumer_id_list(&self) -> Option<SharedValue<Vec<Option<String>>>> {
        self.consumer_id_list.clone()
    }

    pub fn set_consumer_id_list(
        &mut self,
        consumer_id_list: Option<SharedValue<Vec<Option<String>>>>,
    ) {
        self.consumer_id_list = consumer_id_list;
    }
}

/// Java `GetConsumerStatusBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GetConsumerStatusBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<GetConsumerStatusBody>();
/// ```
///
/// Java keeps these properties private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GetConsumerStatusBody;
/// let _ = GetConsumerStatusBody::default().message_queue_table;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusBody {
    #[serde(with = "message_queue_offset_table")]
    message_queue_table: Option<SharedQueueOffsetEntries>,
    #[serde(with = "nested_message_queue_offset_table")]
    consumer_table: Option<SharedConsumerQueueOffsets>,
}

impl GetConsumerStatusBody {
    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut consumer_entries = Vec::new();
        if let Some(consumer_table) = &self.consumer_table {
            consumer_table.with(|consumer_table| {
                for (consumer, entries) in consumer_table {
                    if let Some(entries) = entries {
                        let encoded =
                            entries.with(|entries| encode_java_queue_table_text(entries))?;
                        consumer_entries.push(format!(
                            "{}:{}",
                            serde_json::to_string(consumer)?,
                            encoded
                        ));
                    }
                }
                Ok::<_, serde_json::Error>(())
            })?;
        }
        let mut fields = Vec::new();
        if self.consumer_table.is_some() {
            fields.push(format!(
                "\"consumerTable\":{{{}}}",
                consumer_entries.join(",")
            ));
        }
        if let Some(entries) = &self.message_queue_table {
            fields.push(format!(
                "\"messageQueueTable\":{}",
                entries.with(|entries| encode_java_queue_table_text(entries))?
            ));
        }
        Ok(format!("{{{}}}", fields.join(",")).into_bytes())
    }

    /// Decode the Java Fastjson body syntax, including MessageQueue objects used as map keys.
    pub fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        let normalized = normalize_java_object_map_keys(&normalized)?;
        serde_json::from_str(&normalized)
    }

    pub fn get_message_queue_table(&self) -> Option<SharedQueueOffsetEntries> {
        self.message_queue_table.clone()
    }

    pub fn set_message_queue_table(&mut self, value: Option<SharedQueueOffsetEntries>) {
        self.message_queue_table = value;
    }

    pub fn get_consumer_table(&self) -> Option<SharedConsumerQueueOffsets> {
        self.consumer_table.clone()
    }

    pub fn set_consumer_table(&mut self, value: Option<SharedConsumerQueueOffsets>) {
        self.consumer_table = value;
    }
}

impl Default for GetConsumerStatusBody {
    fn default() -> Self {
        Self {
            message_queue_table: Some(SharedValue::new(Vec::new())),
            consumer_table: Some(SharedValue::new(std::collections::BTreeMap::new())),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
struct GetConsumerStatusBodyFields {
    #[serde(with = "message_queue_offset_table")]
    message_queue_table: Option<SharedQueueOffsetEntries>,
    #[serde(with = "nested_message_queue_offset_table")]
    consumer_table: Option<SharedConsumerQueueOffsets>,
}

impl Default for GetConsumerStatusBodyFields {
    fn default() -> Self {
        Self {
            message_queue_table: Some(SharedValue::new(Vec::new())),
            consumer_table: Some(SharedValue::new(std::collections::BTreeMap::new())),
        }
    }
}

impl<'de> Deserialize<'de> for GetConsumerStatusBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields: GetConsumerStatusBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue_table: fields.message_queue_table,
            consumer_table: fields.consumer_table,
        })
    }
}

/// Java `ProcessQueueInfo` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerProcessQueueInfo;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerProcessQueueInfo>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerProcessQueueInfo;
/// let _ = ConsumerProcessQueueInfo::default().commit_offset;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ConsumerProcessQueueInfo {
    commit_offset: i64,
    cached_msg_min_offset: i64,
    cached_msg_max_offset: i64,
    cached_msg_count: i32,
    #[serde(rename = "cachedMsgSizeInMiB")]
    cached_msg_size_in_mi_b: i32,
    transaction_msg_min_offset: i64,
    transaction_msg_max_offset: i64,
    transaction_msg_count: i32,
    locked: bool,
    try_unlock_times: i64,
    last_lock_timestamp: i64,
    droped: bool,
    last_pull_timestamp: i64,
    last_consume_timestamp: i64,
}

impl ConsumerProcessQueueInfo {
    pub fn get_commit_offset(&self) -> i64 {
        self.commit_offset
    }

    pub fn set_commit_offset(&mut self, value: i64) {
        self.commit_offset = value;
    }

    pub fn get_cached_msg_min_offset(&self) -> i64 {
        self.cached_msg_min_offset
    }

    pub fn set_cached_msg_min_offset(&mut self, value: i64) {
        self.cached_msg_min_offset = value;
    }

    pub fn get_cached_msg_max_offset(&self) -> i64 {
        self.cached_msg_max_offset
    }

    pub fn set_cached_msg_max_offset(&mut self, value: i64) {
        self.cached_msg_max_offset = value;
    }

    pub fn get_cached_msg_count(&self) -> i32 {
        self.cached_msg_count
    }

    pub fn set_cached_msg_count(&mut self, value: i32) {
        self.cached_msg_count = value;
    }

    pub fn get_cached_msg_size_in_mib(&self) -> i32 {
        self.cached_msg_size_in_mi_b
    }

    pub fn set_cached_msg_size_in_mib(&mut self, value: i32) {
        self.cached_msg_size_in_mi_b = value;
    }

    pub fn get_transaction_msg_min_offset(&self) -> i64 {
        self.transaction_msg_min_offset
    }

    pub fn set_transaction_msg_min_offset(&mut self, value: i64) {
        self.transaction_msg_min_offset = value;
    }

    pub fn get_transaction_msg_max_offset(&self) -> i64 {
        self.transaction_msg_max_offset
    }

    pub fn set_transaction_msg_max_offset(&mut self, value: i64) {
        self.transaction_msg_max_offset = value;
    }

    pub fn get_transaction_msg_count(&self) -> i32 {
        self.transaction_msg_count
    }

    pub fn set_transaction_msg_count(&mut self, value: i32) {
        self.transaction_msg_count = value;
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    pub fn set_locked(&mut self, value: bool) {
        self.locked = value;
    }

    pub fn get_try_unlock_times(&self) -> i64 {
        self.try_unlock_times
    }

    pub fn set_try_unlock_times(&mut self, value: i64) {
        self.try_unlock_times = value;
    }

    pub fn get_last_lock_timestamp(&self) -> i64 {
        self.last_lock_timestamp
    }

    pub fn set_last_lock_timestamp(&mut self, value: i64) {
        self.last_lock_timestamp = value;
    }

    pub fn is_droped(&self) -> bool {
        self.droped
    }

    pub fn set_droped(&mut self, value: bool) {
        self.droped = value;
    }

    pub fn get_last_pull_timestamp(&self) -> i64 {
        self.last_pull_timestamp
    }

    pub fn set_last_pull_timestamp(&mut self, value: i64) {
        self.last_pull_timestamp = value;
    }

    pub fn get_last_consume_timestamp(&self) -> i64 {
        self.last_consume_timestamp
    }

    pub fn set_last_consume_timestamp(&mut self, value: i64) {
        self.last_consume_timestamp = value;
    }
}

impl std::fmt::Display for ConsumerProcessQueueInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "ProcessQueueInfo [commitOffset={}, cachedMsgMinOffset={}, cachedMsgMaxOffset={}, cachedMsgCount={}, cachedMsgSizeInMiB={}, transactionMsgMinOffset={}, transactionMsgMaxOffset={}, transactionMsgCount={}, locked={}, tryUnlockTimes={}, lastLockTimestamp={}, droped={}, lastPullTimestamp={}, lastConsumeTimestamp={}]",
            self.commit_offset,
            self.cached_msg_min_offset,
            self.cached_msg_max_offset,
            self.cached_msg_count,
            self.cached_msg_size_in_mi_b,
            self.transaction_msg_min_offset,
            self.transaction_msg_max_offset,
            self.transaction_msg_count,
            self.locked,
            self.try_unlock_times,
            java_time_millis_to_human_string(self.last_lock_timestamp),
            self.droped,
            java_time_millis_to_human_string(self.last_pull_timestamp),
            java_time_millis_to_human_string(self.last_consume_timestamp),
        )
    }
}

/// Java `PopProcessQueueInfo` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerPopProcessQueueInfo;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerPopProcessQueueInfo>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerPopProcessQueueInfo;
/// let _ = ConsumerPopProcessQueueInfo::default().wait_ack_count;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ConsumerPopProcessQueueInfo {
    wait_ack_count: i32,
    droped: bool,
    last_pop_timestamp: i64,
}

impl ConsumerPopProcessQueueInfo {
    pub fn get_wait_ack_count(&self) -> i32 {
        self.wait_ack_count
    }

    pub fn set_wait_ack_count(&mut self, value: i32) {
        self.wait_ack_count = value;
    }

    pub fn is_droped(&self) -> bool {
        self.droped
    }

    pub fn set_droped(&mut self, value: bool) {
        self.droped = value;
    }

    pub fn get_last_pop_timestamp(&self) -> i64 {
        self.last_pop_timestamp
    }

    pub fn set_last_pop_timestamp(&mut self, value: i64) {
        self.last_pop_timestamp = value;
    }
}

impl std::fmt::Display for ConsumerPopProcessQueueInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "PopProcessQueueInfo [waitAckCount:{}, droped:{}, lastPopTimestamp:{}]",
            self.wait_ack_count, self.droped, self.last_pop_timestamp
        )
    }
}

/// Java `ConsumeStatus` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerConsumeStatus;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerConsumeStatus>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerConsumeStatus;
/// let _ = ConsumerConsumeStatus::default().pull_rt;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ConsumerConsumeStatus {
    #[serde(rename = "pullRT")]
    pull_rt: f64,
    #[serde(rename = "pullTPS")]
    pull_tps: f64,
    #[serde(rename = "consumeRT")]
    consume_rt: f64,
    #[serde(rename = "consumeOKTPS")]
    consume_ok_tps: f64,
    #[serde(rename = "consumeFailedTPS")]
    consume_failed_tps: f64,
    consume_failed_msgs: i64,
}

impl ConsumerConsumeStatus {
    pub fn get_pull_rt(&self) -> f64 {
        self.pull_rt
    }

    pub fn set_pull_rt(&mut self, value: f64) {
        self.pull_rt = value;
    }

    pub fn get_pull_tps(&self) -> f64 {
        self.pull_tps
    }

    pub fn set_pull_tps(&mut self, value: f64) {
        self.pull_tps = value;
    }

    pub fn get_consume_rt(&self) -> f64 {
        self.consume_rt
    }

    pub fn set_consume_rt(&mut self, value: f64) {
        self.consume_rt = value;
    }

    pub fn get_consume_ok_tps(&self) -> f64 {
        self.consume_ok_tps
    }

    pub fn set_consume_ok_tps(&mut self, value: f64) {
        self.consume_ok_tps = value;
    }

    pub fn get_consume_failed_tps(&self) -> f64 {
        self.consume_failed_tps
    }

    pub fn set_consume_failed_tps(&mut self, value: f64) {
        self.consume_failed_tps = value;
    }

    pub fn get_consume_failed_msgs(&self) -> i64 {
        self.consume_failed_msgs
    }

    pub fn set_consume_failed_msgs(&mut self, value: i64) {
        self.consume_failed_msgs = value;
    }
}

/// Pinned Java `ConsumerRunningInfo` JSON body. Fastjson encodes the
/// `MessageQueue` map keys as object values rather than quoted strings. The
/// corresponding Java DTO inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerRunningInfoBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerRunningInfoBody>();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerRunningInfoBody;
/// let _ = ConsumerRunningInfoBody::default().properties;
/// ```
#[derive(Debug, Clone)]
pub struct ConsumerRunningInfoBody {
    properties: Option<SharedValue<BTreeMap<String, String>>>,
    subscription_set: Option<SharedValue<Vec<SubscriptionData>>>,
    mq_table: Option<SharedValue<Vec<(MessageQueue, ConsumerProcessQueueInfo)>>>,
    mq_pop_table: Option<SharedValue<Vec<(MessageQueue, ConsumerPopProcessQueueInfo)>>>,
    status_table: Option<SharedValue<BTreeMap<String, ConsumerConsumeStatus>>>,
    user_consumer_info: Option<SharedValue<BTreeMap<String, String>>>,
    jstack: Option<String>,
}

impl Default for ConsumerRunningInfoBody {
    fn default() -> Self {
        Self {
            properties: Some(SharedValue::new(BTreeMap::new())),
            subscription_set: Some(SharedValue::new(Vec::new())),
            mq_table: Some(SharedValue::new(Vec::new())),
            mq_pop_table: Some(SharedValue::new(Vec::new())),
            status_table: Some(SharedValue::new(BTreeMap::new())),
            user_consumer_info: Some(SharedValue::new(BTreeMap::new())),
            jstack: None,
        }
    }
}

impl ConsumerRunningInfoBody {
    /// Java `ConsumerRunningInfo.PROP_NAMESERVER_ADDR` constant.
    pub const PROP_NAMESERVER_ADDR: &str = "PROP_NAMESERVER_ADDR";
    /// Java `ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE` constant.
    pub const PROP_THREADPOOL_CORE_SIZE: &str = "PROP_THREADPOOL_CORE_SIZE";
    /// Java `ConsumerRunningInfo.PROP_CONSUME_ORDERLY` constant. Its value
    /// intentionally omits the underscore before `ORDERLY`.
    pub const PROP_CONSUME_ORDERLY: &str = "PROP_CONSUMEORDERLY";
    /// Java `ConsumerRunningInfo.PROP_CONSUME_TYPE` constant.
    pub const PROP_CONSUME_TYPE: &str = "PROP_CONSUME_TYPE";
    /// Java `ConsumerRunningInfo.PROP_CLIENT_VERSION` constant.
    pub const PROP_CLIENT_VERSION: &str = "PROP_CLIENT_VERSION";
    /// Java `ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP` constant.
    pub const PROP_CONSUMER_START_TIMESTAMP: &str = "PROP_CONSUMER_START_TIMESTAMP";

    pub fn get_properties(&self) -> Option<SharedValue<BTreeMap<String, String>>> {
        self.properties.clone()
    }

    pub fn set_properties(&mut self, value: Option<SharedValue<BTreeMap<String, String>>>) {
        self.properties = value;
    }

    pub fn get_subscription_set(&self) -> Option<SharedValue<Vec<SubscriptionData>>> {
        self.subscription_set.clone()
    }

    pub fn set_subscription_set(&mut self, value: Option<SharedValue<Vec<SubscriptionData>>>) {
        self.subscription_set = value;
    }

    pub fn get_mq_table(
        &self,
    ) -> Option<SharedValue<Vec<(MessageQueue, ConsumerProcessQueueInfo)>>> {
        self.mq_table.clone()
    }

    pub fn set_mq_table(
        &mut self,
        value: Option<SharedValue<Vec<(MessageQueue, ConsumerProcessQueueInfo)>>>,
    ) {
        self.mq_table = value;
    }

    pub fn get_mq_pop_table(
        &self,
    ) -> Option<SharedValue<Vec<(MessageQueue, ConsumerPopProcessQueueInfo)>>> {
        self.mq_pop_table.clone()
    }

    pub fn set_mq_pop_table(
        &mut self,
        value: Option<SharedValue<Vec<(MessageQueue, ConsumerPopProcessQueueInfo)>>>,
    ) {
        self.mq_pop_table = value;
    }

    pub fn get_status_table(&self) -> Option<SharedValue<BTreeMap<String, ConsumerConsumeStatus>>> {
        self.status_table.clone()
    }

    pub fn set_status_table(
        &mut self,
        value: Option<SharedValue<BTreeMap<String, ConsumerConsumeStatus>>>,
    ) {
        self.status_table = value;
    }

    pub fn get_user_consumer_info(&self) -> Option<SharedValue<BTreeMap<String, String>>> {
        self.user_consumer_info.clone()
    }

    pub fn get_jstack(&self) -> Option<&str> {
        self.jstack.as_deref()
    }

    pub fn set_jstack(&mut self, value: Option<String>) {
        self.jstack = value;
    }

    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut subscription_set = self
            .subscription_set
            .as_ref()
            .map(SharedValue::snapshot)
            .unwrap_or_default();
        subscription_set.sort_by(|left, right| left.java_compare_to(right).cmp(&0));
        let subscription_set = subscription_set
            .iter()
            .map(encode_java_subscription_data)
            .collect::<Result<Vec<_>, _>>()?;

        let mut fields = Vec::with_capacity(7);
        if let Some(jstack) = &self.jstack {
            fields.push(format!("\"jstack\":{}", serde_json::to_string(jstack)?));
        }
        fields.push(format!(
            "\"mqPopTable\":{}",
            encode_running_info_queue_table(self.mq_pop_table.as_ref().map(SharedValue::snapshot))?
        ));
        fields.push(format!(
            "\"mqTable\":{}",
            encode_running_info_queue_table(self.mq_table.as_ref().map(SharedValue::snapshot))?
        ));
        fields.push(format!(
            "\"properties\":{}",
            serde_json::to_string(&self.properties.as_ref().map(SharedValue::snapshot))?
        ));
        fields.push(format!(
            "\"statusTable\":{}",
            serde_json::to_string(&self.status_table.as_ref().map(SharedValue::snapshot))?
        ));
        fields.push(format!(
            "\"subscriptionSet\":{}",
            serde_json::to_string(&self.subscription_set.as_ref().map(|_| subscription_set))?
        ));
        fields.push(format!(
            "\"userConsumerInfo\":{}",
            serde_json::to_string(&self.user_consumer_info.as_ref().map(SharedValue::snapshot))?
        ));
        Ok(format!("{{{}}}", fields.join(",")).into_bytes())
    }

    pub fn decode_java_json(input: &str) -> Result<serde_json::Value, serde_json::Error> {
        let fastjson_normalized = serde_fastjson::normalize_json_input(input);
        let normalized = normalize_java_object_map_keys(&fastjson_normalized)?;
        serde_json::from_str(&normalized)
    }

    /// Decodes a Java `ConsumerRunningInfo` body into a typed Rust snapshot.
    /// Fastjson's object-valued `MessageQueue` map keys are normalized and
    /// parsed back into their typed queue and process-info pairs. Input syntax,
    /// including Fastjson's raw EOF marker, is normalized before Serde parsing.
    pub fn decode_java_json_body(input: &str) -> Result<Self, serde_json::Error> {
        let value = Self::decode_java_json(input)?;
        let mut fields: BTreeMap<String, serde_json::Value> = serde_json::from_value(value)?;
        Ok(Self {
            properties: take_nullable_running_info_field(&mut fields, "properties")?,
            subscription_set: take_nullable_running_info_field(&mut fields, "subscriptionSet")?,
            mq_table: decode_running_info_queue_table(fields.remove("mqTable"))?,
            mq_pop_table: decode_running_info_queue_table(fields.remove("mqPopTable"))?,
            status_table: take_nullable_running_info_field(&mut fields, "statusTable")?,
            user_consumer_info: take_nullable_running_info_field(&mut fields, "userConsumerInfo")?,
            jstack: fields
                .remove("jstack")
                .filter(|value| !value.is_null())
                .map(serde_json::from_value)
                .transpose()?,
        })
    }
}

fn take_nullable_running_info_field<T>(
    fields: &mut BTreeMap<String, serde_json::Value>,
    name: &str,
) -> Result<Option<SharedValue<T>>, serde_json::Error>
where
    T: de::DeserializeOwned + Default,
{
    match fields.remove(name) {
        None => Ok(Some(SharedValue::new(T::default()))),
        Some(serde_json::Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value).map(|value| Some(SharedValue::new(value))),
    }
}

type RunningInfoQueueTable<T> = Option<SharedValue<Vec<(MessageQueue, T)>>>;

fn decode_running_info_queue_table<T>(
    value: Option<serde_json::Value>,
) -> Result<RunningInfoQueueTable<T>, serde_json::Error>
where
    T: de::DeserializeOwned + Default,
{
    let Some(value) = value else {
        return Ok(Some(SharedValue::new(Vec::new())));
    };
    if value.is_null() {
        return Ok(None);
    };
    let fields: BTreeMap<String, serde_json::Value> = serde_json::from_value(value)?;
    let entries = fields
        .into_iter()
        .map(|(encoded_queue, info)| {
            let queue = serde_json::from_str(&encoded_queue)?;
            let info = serde_json::from_value(info)?;
            Ok((queue, info))
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    Ok(Some(SharedValue::new(entries)))
}

fn encode_running_info_queue_table<T: Serialize>(
    table: Option<Vec<(MessageQueue, T)>>,
) -> Result<String, serde_json::Error> {
    let Some(table) = table else {
        return Ok("null".to_owned());
    };
    let mut queues = table.iter().collect::<Vec<_>>();
    queues.sort_by(|left, right| left.0.cmp(&right.0));
    let mut entries = Vec::with_capacity(queues.len());
    for (queue, info) in queues {
        entries.push((queue.clone(), serde_json::to_string(info)?));
    }
    encode_java_object_key_map(&entries)
}

fn encode_java_subscription_data(
    subscription: &SubscriptionData,
) -> Result<serde_json::Value, serde_json::Error> {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "classFilterMode".into(),
        serde_json::Value::Bool(subscription.class_filter_mode),
    );
    if let Some(topic) = &subscription.topic {
        fields.insert("topic".into(), serde_json::Value::String(topic.clone()));
    }
    if let Some(sub_string) = &subscription.sub_string {
        fields.insert(
            "subString".into(),
            serde_json::Value::String(sub_string.clone()),
        );
    }
    if let Some(tags) = &subscription.tags_set {
        let mut tags = tags.snapshot().into_iter().collect::<Vec<_>>();
        tags.sort_by(java_optional_string_cmp);
        fields.insert("tagsSet".into(), serde_json::to_value(tags)?);
    }
    if let Some(codes) = &subscription.code_set {
        let mut codes = codes.snapshot().into_iter().collect::<Vec<_>>();
        codes.sort();
        fields.insert("codeSet".into(), serde_json::to_value(codes)?);
    }
    fields.insert(
        "subVersion".into(),
        serde_json::Value::Number(subscription.sub_version.into()),
    );
    if let Some(expression_type) = &subscription.expression_type {
        fields.insert(
            "expressionType".into(),
            serde_json::Value::String(expression_type.clone()),
        );
    }
    Ok(serde_json::Value::Object(fields))
}

fn java_optional_string_cmp(left: &Option<String>, right: &Option<String>) -> std::cmp::Ordering {
    match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => left.encode_utf16().cmp(right.encode_utf16()),
    }
}

fn encode_java_object_key_map(
    entries: &[(MessageQueue, String)],
) -> Result<String, serde_json::Error> {
    let parts = entries
        .iter()
        .map(|(queue, value)| Ok(format!("{}:{value}", encode_java_message_queue_key(queue)?)))
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    Ok(format!("{{{}}}", parts.join(",")))
}

fn encode_java_message_queue_key(queue: &MessageQueue) -> Result<String, serde_json::Error> {
    let mut queue_fields = Vec::with_capacity(3);
    if let Some(broker_name) = queue.broker_name() {
        queue_fields.push(format!(
            "\"brokerName\":{}",
            serde_json::to_string(broker_name)?
        ));
    }
    queue_fields.push(format!(
        "\"queueId\":{}",
        serde_json::to_string(&queue.queue_id())?
    ));
    if let Some(topic) = queue.topic() {
        queue_fields.push(format!("\"topic\":{}", serde_json::to_string(topic)?));
    }
    Ok(format!("{{{}}}", queue_fields.join(",")))
}

fn normalize_java_object_map_keys(input: &str) -> Result<String, serde_json::Error> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ObjectKind {
        Root,
        ConsumerTable,
        QueueTable,
        Other,
    }

    struct ObjectFrame {
        kind: ObjectKind,
        last_key: Option<String>,
    }

    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut objects = Vec::<ObjectFrame>::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            let end = skip_json_string(bytes, index).unwrap_or(bytes.len());
            let is_key = bytes.get(skip_json_whitespace(bytes, end)) == Some(&b':');
            if is_key {
                if objects
                    .last()
                    .is_some_and(|object| object.kind == ObjectKind::QueueTable)
                {
                    return Err(java_queue_key_error());
                }
                if let Some(object) = objects.last_mut() {
                    object.last_key = serde_json::from_str(&input[index..end]).ok();
                }
            }
            output.push_str(&input[index..end]);
            index = end;
            continue;
        }

        if bytes[index] == b'{'
            && objects
                .last()
                .is_some_and(|object| object.kind == ObjectKind::QueueTable)
            && matches!(previous_json_byte(bytes, index), Some(b'{' | b','))
        {
            if let Some(end) = find_json_object_end(bytes, index) {
                if bytes.get(skip_json_whitespace(bytes, end)) == Some(&b':') {
                    let encoded_key = serde_json::to_string(&input[index..end])?;
                    output.push_str(&encoded_key);
                    index = end;
                    continue;
                }
            }
        }

        if bytes[index] == b'{' {
            let kind = match objects.last() {
                None => ObjectKind::Root,
                Some(parent) if previous_json_byte(bytes, index) == Some(b':') => {
                    match (parent.kind, parent.last_key.as_deref()) {
                        (ObjectKind::ConsumerTable, _) => ObjectKind::QueueTable,
                        (
                            _,
                            Some("offsetTable" | "messageQueueTable" | "mqTable" | "mqPopTable"),
                        ) => ObjectKind::QueueTable,
                        (ObjectKind::Root, Some("consumerTable")) => ObjectKind::ConsumerTable,
                        _ => ObjectKind::Other,
                    }
                }
                Some(_) => ObjectKind::Other,
            };
            output.push('{');
            objects.push(ObjectFrame {
                kind,
                last_key: None,
            });
            index += 1;
            continue;
        }

        if bytes[index] == b'}' {
            output.push('}');
            objects.pop();
            index += 1;
            continue;
        }

        if bytes[index] == b',' {
            if let Some(object) = objects.last_mut() {
                object.last_key = None;
            }
            output.push(',');
            index += 1;
            continue;
        }

        let character = input[index..]
            .chars()
            .next()
            .expect("byte index is within a UTF-8 string");
        output.push(character);
        index += character.len_utf8();
    }
    Ok(output)
}

fn java_queue_key_error() -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Fastjson MessageQueue map keys must be object values",
    ))
}

fn skip_json_string(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = index.checked_add(2)?,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn find_json_object_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut index = start;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => index = skip_json_string(bytes, index)?,
            b'{' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                depth = depth.checked_sub(1)?;
                index += 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => index += 1,
        }
    }
    None
}

fn skip_json_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
    {
        index += 1;
    }
    index
}

fn previous_json_byte(bytes: &[u8], index: usize) -> Option<u8> {
    let mut previous = index.checked_sub(1)?;
    while bytes
        .get(previous)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
    {
        previous = previous.checked_sub(1)?;
    }
    bytes.get(previous).copied()
}

/// Java `ResetOffsetBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ResetOffsetBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ResetOffsetBody>();
/// ```
///
/// Java keeps this property private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ResetOffsetBody;
/// let _ = ResetOffsetBody::default().offset_table;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetBody {
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "optional_message_queue_offset_table"
    )]
    offset_table: Option<SharedQueueOffsetEntries>,
}

impl ResetOffsetBody {
    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let Some(entries) = &self.offset_table else {
            return Ok(b"{}".to_vec());
        };
        let table = entries.with(|entries| encode_java_queue_table_text(entries))?;
        Ok(format!("{{\"offsetTable\":{table}}}").into_bytes())
    }

    pub fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        let normalized = normalize_java_object_map_keys(&normalized)?;
        serde_json::from_str(&normalized)
    }

    pub fn get_offset_table(&self) -> Option<SharedQueueOffsetEntries> {
        self.offset_table.clone()
    }

    pub fn set_offset_table(&mut self, value: Option<SharedQueueOffsetEntries>) {
        self.offset_table = value;
    }
}

impl Default for ResetOffsetBody {
    fn default() -> Self {
        Self {
            offset_table: Some(SharedValue::new(Vec::new())),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ResetOffsetBodyFields {
    #[serde(with = "optional_message_queue_offset_table")]
    offset_table: Option<SharedQueueOffsetEntries>,
}

impl Default for ResetOffsetBodyFields {
    fn default() -> Self {
        Self {
            offset_table: Some(SharedValue::new(Vec::new())),
        }
    }
}

impl<'de> Deserialize<'de> for ResetOffsetBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields: ResetOffsetBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            offset_table: fields.offset_table,
        })
    }
}

fn encode_java_queue_table_text(
    entries: &[(MessageQueue, Option<i64>)],
) -> Result<String, serde_json::Error> {
    let mut table = std::collections::BTreeMap::new();
    for (queue, offset) in entries {
        table.insert(queue.clone(), *offset);
    }
    let mut parts = Vec::with_capacity(entries.len());
    for (queue, offset) in table {
        let Some(offset) = offset else {
            continue;
        };
        let mut queue_fields = Vec::with_capacity(3);
        if let Some(broker_name) = queue.broker_name() {
            queue_fields.push(format!(
                "\"brokerName\":{}",
                serde_json::to_string(broker_name)?
            ));
        }
        queue_fields.push(format!(
            "\"queueId\":{}",
            serde_json::to_string(&queue.queue_id())?
        ));
        if let Some(topic) = queue.topic() {
            queue_fields.push(format!("\"topic\":{}", serde_json::to_string(topic)?));
        }
        let queue_key = format!("{{{}}}", queue_fields.join(","));
        parts.push(format!("{queue_key}:{}", serde_json::to_string(&offset)?));
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

pub type QueueOffsetEntries = Vec<(MessageQueue, Option<i64>)>;
pub type NullableQueueOffsetEntries = QueueOffsetEntries;
pub type SharedQueueOffsetEntries = SharedValue<QueueOffsetEntries>;
pub type ConsumerQueueOffsets =
    std::collections::BTreeMap<String, Option<SharedQueueOffsetEntries>>;
pub type SharedConsumerQueueOffsets = SharedValue<ConsumerQueueOffsets>;

impl PartialEq for SharedValue<QueueOffsetEntries> {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<QueueOffsetEntries> {}

impl PartialEq for SharedValue<ConsumerQueueOffsets> {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<ConsumerQueueOffsets> {}

#[derive(Debug, Clone, Copy)]
struct FastjsonLong(Option<i64>);

impl FastjsonLong {
    fn from_raw<E>(raw: &str) -> Result<Self, E>
    where
        E: de::Error,
    {
        if raw == "null" {
            return Ok(Self(None));
        }
        if raw == "true" {
            return Ok(Self(Some(1)));
        }
        if raw == "false" {
            return Ok(Self(Some(0)));
        }
        if raw.starts_with('"') {
            let text: String = serde_json::from_str(raw).map_err(E::custom)?;
            return Self::from_string::<E>(&text);
        }
        if raw.contains('.') && !raw.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            return decimal_text_to_java_long::<E>(raw).map(|value| Self(Some(value)));
        }
        if raw.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            let value = raw.parse::<f64>().map_err(E::custom)?;
            return Ok(Self(Some(value as i64)));
        }
        raw.parse::<i64>()
            .map(|value| Self(Some(value)))
            .map_err(E::custom)
    }

    fn from_string<E>(text: &str) -> Result<Self, E>
    where
        E: de::Error,
    {
        if text.is_empty() {
            return Ok(Self(None));
        }
        if let Ok(value) = text.parse::<i64>() {
            return Ok(Self(Some(value)));
        }
        if text.contains('.') && !text.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            return decimal_text_to_java_long::<E>(text).map(|value| Self(Some(value)));
        }
        if text.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            let value = text.parse::<f64>().map_err(E::custom)?;
            return Ok(Self(Some(value as i64)));
        }
        Err(E::custom("string is not a Java Long value"))
    }
}

fn decimal_text_to_java_long<E>(text: &str) -> Result<i64, E>
where
    E: de::Error,
{
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let mut pieces = unsigned.split('.');
    let integer = pieces.next().unwrap_or_default();
    let fraction = pieces
        .next()
        .ok_or_else(|| E::custom("invalid decimal value"))?;
    if pieces.next().is_some()
        || (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(E::custom("invalid decimal value"));
    }
    let mut magnitude = 0u64;
    for digit in integer.bytes() {
        magnitude = magnitude
            .wrapping_mul(10)
            .wrapping_add(u64::from(digit - b'0'));
    }
    let value = if negative {
        0u64.wrapping_sub(magnitude)
    } else {
        magnitude
    };
    Ok(value as i64)
}

mod message_queue_offset_table {
    use super::encode_table;
    use super::{MessageQueue, QueueOffsetEntries, SharedQueueOffsetEntries};
    use crate::SharedValue;
    use serde::de::MapAccess;
    use serde::de::Visitor;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedQueueOffsetEntries>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match entries {
            Some(entries) => entries
                .with(|entries| encode_table(entries))
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedQueueOffsetEntries>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(encoded) = Option::<RawQueueOffsetEntries>::deserialize(deserializer)? else {
            return Ok(None);
        };
        decode_entries(encoded).map(SharedValue::new).map(Some)
    }

    pub(super) struct RawQueueOffsetEntries(Vec<(String, Option<i64>)>);

    pub(super) fn decode_entries<E>(encoded: RawQueueOffsetEntries) -> Result<QueueOffsetEntries, E>
    where
        E: serde::de::Error,
    {
        let mut table = std::collections::BTreeMap::new();
        for (key, offset) in encoded.0 {
            let queue: MessageQueue =
                serde_json::from_str(&key).map_err(serde::de::Error::custom)?;
            table.insert(queue, offset);
        }
        Ok(table.into_iter().collect())
    }

    impl<'de> Deserialize<'de> for RawQueueOffsetEntries {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TableVisitor;

            impl<'de> Visitor<'de> for TableVisitor {
                type Value = RawQueueOffsetEntries;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a MessageQueue-keyed offset object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let raw = map.next_value::<Box<serde_json::value::RawValue>>()?;
                        let offset = super::FastjsonLong::from_raw::<A::Error>(raw.get())?.0;
                        entries.push((key, offset));
                    }
                    Ok(RawQueueOffsetEntries(entries))
                }
            }

            deserializer.deserialize_map(TableVisitor)
        }
    }
}

fn encode_table(
    entries: &[(MessageQueue, Option<i64>)],
) -> Result<std::collections::BTreeMap<String, Option<i64>>, serde_json::Error> {
    entries
        .iter()
        .map(|(queue, offset)| Ok((serde_json::to_string(queue)?, *offset)))
        .collect()
}

mod nested_message_queue_offset_table {
    use super::SharedConsumerQueueOffsets;
    use super::encode_table;
    use crate::SharedValue;
    use serde::de::MapAccess;
    use serde::de::Visitor;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedConsumerQueueOffsets>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match entries {
            Some(entries) => entries.with(|entries| {
                let mut encoded = std::collections::BTreeMap::new();
                for (key, values) in entries {
                    let values = values
                        .as_ref()
                        .map(|values| values.with(|values| encode_table(values)))
                        .transpose()
                        .map_err(serde::ser::Error::custom)?;
                    encoded.insert(key, values);
                }
                encoded.serialize(serializer)
            }),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedConsumerQueueOffsets>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(raw) = Option::<RawConsumerQueueOffsets>::deserialize(deserializer)? else {
            return Ok(None);
        };
        let mut entries = std::collections::BTreeMap::new();
        for (consumer, table) in raw.0 {
            let table = table
                .map(super::message_queue_offset_table::decode_entries::<D::Error>)
                .map(|result| result.map(SharedValue::new))
                .transpose()?;
            entries.insert(consumer, table);
        }
        Ok(Some(SharedValue::new(entries)))
    }

    struct RawConsumerQueueOffsets(
        Vec<(
            String,
            Option<super::message_queue_offset_table::RawQueueOffsetEntries>,
        )>,
    );

    impl<'de> Deserialize<'de> for RawConsumerQueueOffsets {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct ConsumerTableVisitor;

            impl<'de> Visitor<'de> for ConsumerTableVisitor {
                type Value = RawConsumerQueueOffsets;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a consumer-keyed MessageQueue offset object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some(consumer) = map.next_key::<String>()? {
                        entries.push(
                            (
                                consumer,
                                map.next_value::<Option<
                                    super::message_queue_offset_table::RawQueueOffsetEntries,
                                >>()?,
                            ),
                        );
                    }
                    Ok(RawConsumerQueueOffsets(entries))
                }
            }

            deserializer.deserialize_map(ConsumerTableVisitor)
        }
    }
}

mod optional_message_queue_offset_table {
    use super::{MessageQueue, SharedQueueOffsetEntries};
    use crate::SharedValue;
    use serde::de::{MapAccess, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedQueueOffsetEntries>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match entries {
            Some(entries) => entries.with(|entries| {
                let mut table = std::collections::BTreeMap::new();
                for (queue, offset) in entries {
                    table.insert(
                        serde_json::to_string(queue).map_err(serde::ser::Error::custom)?,
                        offset,
                    );
                }
                table.serialize(serializer)
            }),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedQueueOffsetEntries>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(encoded) = Option::<RawQueueOffsetTable>::deserialize(deserializer)? else {
            return Ok(None);
        };
        let mut table = std::collections::BTreeMap::new();
        for (key, offset) in encoded.0 {
            let queue: MessageQueue =
                serde_json::from_str(&key).map_err(serde::de::Error::custom)?;
            table.insert(queue, offset);
        }
        Ok(Some(SharedValue::new(table.into_iter().collect())))
    }

    struct RawQueueOffsetTable(Vec<(String, Option<i64>)>);

    impl<'de> Deserialize<'de> for RawQueueOffsetTable {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TableVisitor;

            impl<'de> Visitor<'de> for TableVisitor {
                type Value = RawQueueOffsetTable;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a MessageQueue-keyed nullable offset object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let raw = map.next_value::<Box<serde_json::value::RawValue>>()?;
                        let offset = super::FastjsonLong::from_raw::<A::Error>(raw.get())?.0;
                        entries.push((key, offset));
                    }
                    Ok(RawQueueOffsetTable(entries))
                }
            }

            deserializer.deserialize_map(TableVisitor)
        }
    }
}

mod topic_stats_offset_table {
    use super::{
        MessageQueue, SharedTopicStatsOffsetTable, TopicOffset, TopicStatsOffsetTable,
        encode_java_message_queue_key,
    };
    use crate::{SharedValue, serde_fastjson};
    use serde::de::{MapAccess, Visitor};
    use serde::ser::SerializeMap;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedTopicStatsOffsetTable>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(entries) = entries else {
            return serializer.serialize_none();
        };
        let mut encoded = entries
            .with(|entries| {
                entries
                    .iter()
                    .filter_map(|(queue, offset)| {
                        offset.as_ref().map(|offset| {
                            Ok((encode_java_message_queue_key(queue)?, Some(offset.clone())))
                        })
                    })
                    .collect::<Result<Vec<_>, serde_json::Error>>()
            })
            .map_err(serde::ser::Error::custom)?;
        encoded.sort_by(|left, right| left.0.cmp(&right.0));
        let mut map = serializer.serialize_map(Some(encoded.len()))?;
        for (key, offset) in encoded {
            map.serialize_entry(&key, &offset)?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedTopicStatsOffsetTable>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(encoded) = Option::<RawTopicStatsOffsetTable>::deserialize(deserializer)? else {
            return Ok(None);
        };
        let mut table = TopicStatsOffsetTable::new();
        for (key, raw_offset) in encoded.0 {
            let queue: MessageQueue =
                serde_fastjson::from_fastjson_str(&key).map_err(serde::de::Error::custom)?;
            let offset =
                serde_fastjson::from_fastjson_str_nullable::<TopicOffset>(Some(raw_offset.get()))
                    .map_err(serde::de::Error::custom)?
                    .map(SharedValue::new);
            table.insert(queue, offset);
        }
        Ok(Some(SharedValue::new(table)))
    }

    pub(super) fn encode_java_json(
        entries: &TopicStatsOffsetTable,
    ) -> Result<String, serde_json::Error> {
        let mut rows = Vec::new();
        for (queue, offset) in entries {
            let Some(offset) = offset else {
                continue;
            };
            rows.push((
                encode_java_message_queue_key(queue)?,
                serde_json::to_value(offset)?.to_string(),
            ));
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(format!(
            "{{{}}}",
            rows.into_iter()
                .map(|(key, value)| format!("{key}:{value}"))
                .collect::<Vec<_>>()
                .join(",")
        ))
    }

    struct RawTopicStatsOffsetTable(Vec<(String, Box<serde_json::value::RawValue>)>);

    impl<'de> Deserialize<'de> for RawTopicStatsOffsetTable {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TableVisitor;

            impl<'de> Visitor<'de> for TableVisitor {
                type Value = RawTopicStatsOffsetTable;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a MessageQueue-keyed TopicOffset object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some((key, value)) = map.next_entry()? {
                        entries.push((key, value));
                    }
                    Ok(RawTopicStatsOffsetTable(entries))
                }
            }

            deserializer.deserialize_map(TableVisitor)
        }
    }
}

mod consume_stats_offset_table {
    use super::{
        ConsumeStatsOffsetTable, MessageQueue, OffsetWrapper, SharedConsumeStatsOffsetTable,
        encode_java_message_queue_key,
    };
    use crate::{SharedValue, serde_fastjson};
    use serde::de::{MapAccess, Visitor};
    use serde::ser::SerializeMap;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedConsumeStatsOffsetTable>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let Some(entries) = entries else {
            return serializer.serialize_none();
        };
        let mut encoded = entries
            .with(|entries| {
                entries
                    .iter()
                    .filter_map(|(queue, offset)| {
                        offset.as_ref().map(|offset| {
                            Ok((encode_java_message_queue_key(queue)?, Some(offset.clone())))
                        })
                    })
                    .collect::<Result<Vec<_>, serde_json::Error>>()
            })
            .map_err(serde::ser::Error::custom)?;
        encoded.sort_by(|left, right| left.0.cmp(&right.0));
        let mut map = serializer.serialize_map(Some(encoded.len()))?;
        for (key, offset) in encoded {
            map.serialize_entry(&key, &offset)?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedConsumeStatsOffsetTable>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(encoded) = Option::<RawConsumeStatsOffsetTable>::deserialize(deserializer)? else {
            return Ok(None);
        };
        let mut table = ConsumeStatsOffsetTable::new();
        for (key, raw_offset) in encoded.0 {
            let queue: MessageQueue =
                serde_fastjson::from_fastjson_str(&key).map_err(serde::de::Error::custom)?;
            let offset =
                serde_fastjson::from_fastjson_str_nullable::<OffsetWrapper>(Some(raw_offset.get()))
                    .map_err(serde::de::Error::custom)?
                    .map(SharedValue::new);
            table.insert(queue, offset);
        }
        Ok(Some(SharedValue::new(table)))
    }

    pub(super) fn encode_java_json(
        entries: &ConsumeStatsOffsetTable,
    ) -> Result<String, serde_json::Error> {
        let mut rows = Vec::new();
        for (queue, offset) in entries {
            let Some(offset) = offset else {
                continue;
            };
            rows.push((
                encode_java_message_queue_key(queue)?,
                serde_json::to_value(offset)?.to_string(),
            ));
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(format!(
            "{{{}}}",
            rows.into_iter()
                .map(|(key, value)| format!("{key}:{value}"))
                .collect::<Vec<_>>()
                .join(",")
        ))
    }

    struct RawConsumeStatsOffsetTable(Vec<(String, Box<serde_json::value::RawValue>)>);

    impl<'de> Deserialize<'de> for RawConsumeStatsOffsetTable {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TableVisitor;

            impl<'de> Visitor<'de> for TableVisitor {
                type Value = RawConsumeStatsOffsetTable;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a MessageQueue-keyed OffsetWrapper object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some((key, value)) = map.next_entry()? {
                        entries.push((key, value));
                    }
                    Ok(RawConsumeStatsOffsetTable(entries))
                }
            }

            deserializer.deserialize_map(TableVisitor)
        }
    }
}

/// Java `ConsumeMessageDirectlyResult` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumeMessageDirectlyResult;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumeMessageDirectlyResult>();
/// ```
///
/// Java keeps these properties private and exposes accessors.
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumeMessageDirectlyResult;
/// let _ = ConsumeMessageDirectlyResult::default().order;
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeMessageDirectlyResult {
    order: bool,
    auto_commit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    consume_result: Option<ConsumeMessageResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remark: Option<String>,
    spent_time_mills: i64,
}

impl Default for ConsumeMessageDirectlyResult {
    fn default() -> Self {
        Self {
            order: false,
            auto_commit: default_auto_commit(),
            consume_result: None,
            remark: None,
            spent_time_mills: 0,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsumeMessageDirectlyResultFields {
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    order: bool,
    #[serde(
        default = "default_auto_commit",
        deserialize_with = "serde_fastjson::deserialize_bool"
    )]
    auto_commit: bool,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    consume_result: Option<ConsumeMessageResult>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    remark: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    spent_time_mills: i64,
}

impl<'de> Deserialize<'de> for ConsumeMessageDirectlyResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ConsumeMessageDirectlyResultFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            order: fields.order,
            auto_commit: fields.auto_commit,
            consume_result: fields.consume_result,
            remark: fields.remark,
            spent_time_mills: fields.spent_time_mills,
        })
    }
}

impl ConsumeMessageDirectlyResult {
    pub fn is_order(&self) -> bool {
        self.order
    }

    pub fn set_order(&mut self, order: bool) {
        self.order = order;
    }

    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }

    pub fn get_consume_result(&self) -> Option<ConsumeMessageResult> {
        self.consume_result
    }

    pub fn set_consume_result(&mut self, consume_result: Option<ConsumeMessageResult>) {
        self.consume_result = consume_result;
    }

    pub fn get_remark(&self) -> Option<&str> {
        self.remark.as_deref()
    }

    pub fn set_remark(&mut self, remark: Option<String>) {
        self.remark = remark;
    }

    pub fn get_spent_time_mills(&self) -> i64 {
        self.spent_time_mills
    }

    pub fn set_spent_time_mills(&mut self, spent_time_mills: i64) {
        self.spent_time_mills = spent_time_mills;
    }
}

impl std::fmt::Display for ConsumeMessageDirectlyResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consume_result = self
            .consume_result
            .map(|result| result.name())
            .unwrap_or("null");
        write!(
            formatter,
            "ConsumeMessageDirectlyResult [order={}, autoCommit={}, consumeResult={}, remark={}, spentTimeMills={}]",
            self.order,
            self.auto_commit,
            consume_result,
            self.remark.as_deref().unwrap_or("null"),
            self.spent_time_mills
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumeMessageResult {
    CrSuccess,
    CrLater,
    CrRollback,
    CrCommit,
    CrThrowException,
    CrReturnNull,
}

impl_java_enum_api!(
    ConsumeMessageResult,
    "org.apache.rocketmq.remoting.protocol.body.CMResult",
    6,
    CrSuccess => "CR_SUCCESS" = 0,
    CrLater => "CR_LATER" = 1,
    CrRollback => "CR_ROLLBACK" = 2,
    CrCommit => "CR_COMMIT" = 3,
    CrThrowException => "CR_THROW_EXCEPTION" = 4,
    CrReturnNull => "CR_RETURN_NULL" = 5,
);

impl super::heartbeat::JavaOrdinalEnum for ConsumeMessageResult {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::CrSuccess),
            1 => Some(Self::CrLater),
            2 => Some(Self::CrRollback),
            3 => Some(Self::CrCommit),
            4 => Some(Self::CrThrowException),
            5 => Some(Self::CrReturnNull),
            _ => None,
        }
    }
}

fn default_auto_commit() -> bool {
    true
}

impl_java_remoting_serializable_adapters!(
    DataVersion,
    SubscriptionGroupWrapper,
    CheckClientRequestBody,
    LogicQueueMappingItem,
    LiteSubscriptionCtlRequestBody,
    QueryAssignmentRequestBody,
    QueryAssignmentResponseBody,
    QueryConsumeQueueResponseBody,
    QueryConsumeTimeSpanBody,
    QueryCorrectionOffsetBody,
    SetMessageRequestModeRequestBody,
    MessageRequestModeSerializeWrapper,
    LockBatchRequestBody,
    UnlockBatchRequestBody,
    LockBatchResponseBody,
    ConsumerListByGroupResponseBody,
    ProducerConnection,
    ProducerTableInfo,
    KvTable,
    ConsumerConnection,
    ConsumeMessageDirectlyResult,
    CreateTopicListRequestBody,
    DeleteTopicListRequestBody,
    DeleteSubscriptionGroupListRequestBody,
    TopicList,
    SubscriptionGroupList,
    TopicConfigSerializeWrapper,
);

impl_java_static_remoting_serializable_adapters!(
    CheckRocksdbCqWriteResult,
    AclInfo,
    AclPolicyInfo,
    AclPolicyEntryInfo,
);

impl_java_static_topic_serializable_adapters!(TopicConfigAndQueueMapping, TopicQueueMappingDetail);
impl_java_remoting_serializable_writer_features!(TopicQueueMappingDetail);

macro_rules! impl_java_remoting_serializable_body_adapters {
    ($($dto:ty),+ $(,)?) => {
        $(
            impl $dto {
                pub fn java_encode_optional(
                    value: Option<&Self>,
                ) -> Result<Option<Vec<u8>>, serde_json::Error> {
                    value.map(Self::java_encode).transpose()
                }

                pub fn java_to_json_optional(
                    value: Option<&Self>,
                    pretty_format: bool,
                ) -> Result<String, serde_json::Error> {
                    value.map_or_else(
                        || Ok("null".to_owned()),
                        |value| value.java_to_json(pretty_format),
                    )
                }

                pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
                    if pretty_format {
                        java_fastjson_remoting_body_json(self, true)
                    } else {
                        Ok(String::from_utf8_lossy(&self.encode_java_json()?).into_owned())
                    }
                }

                pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
                    self.encode_java_json()
                }

                /// Encodes with the supported inherited Fastjson2 writer features.
                pub fn java_encode_with_features(
                    &self,
                    features: &[JavaJsonWriterFeature],
                ) -> Result<Vec<u8>, serde_json::Error> {
                    Ok(java_fastjson_remoting_body_json_with_features(
                        self, false, features,
                    )?.into_bytes())
                }

                pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
                    Self::decode_java_json(&String::from_utf8_lossy(data))
                }

                pub fn java_decode_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    let Some(data) = data else {
                        return Ok(None);
                    };
                    let input = String::from_utf8_lossy(data);
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(&input),
                    )?;
                    if serde_json::from_str::<serde_json::Value>(&normalized)?.is_null() {
                        return Ok(None);
                    }
                    Self::decode_java_json(&input).map(Some)
                }

                pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
                    let input = String::from_utf8_lossy(data);
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(&input),
                    )?;
                    serde_json::from_str(&normalized)
                }

                pub fn java_decode_list_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
                    let Some(data) = data else {
                        return Ok(None);
                    };
                    let input = String::from_utf8_lossy(data);
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(&input),
                    )?;
                    if serde_json::from_str::<serde_json::Value>(&normalized)?.is_null() {
                        return Ok(None);
                    }
                    serde_json::from_str(&normalized).map(Some)
                }

                pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
                    Self::decode_java_json(json)
                }

                pub fn java_from_json_nullable(
                    json: Option<&str>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    let Some(json) = json else {
                        return Ok(None);
                    };
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(json),
                    )?;
                    if serde_json::from_str::<serde_json::Value>(&normalized)?.is_null() {
                        return Ok(None);
                    }
                    Self::decode_java_json(json).map(Some)
                }
            }
        )+
    };
}

impl_java_remoting_serializable_body_adapters!(
    GetBrokerLiteInfoResponseBody,
    GetParentTopicInfoResponseBody,
    GetLiteClientInfoResponseBody,
    GetLiteGroupInfoResponseBody,
    GetLiteTopicInfoResponseBody,
    TopicStatsTable,
    ConsumeStats,
    ConsumeStatsList,
    GetConsumerStatusBody,
    ResetOffsetBody,
);

fn java_fastjson_remoting_body_json<T: Serialize>(
    value: &T,
    pretty_format: bool,
) -> Result<String, serde_json::Error> {
    java_fastjson_remoting_body_json_with_features(value, pretty_format, &[])
}

fn java_fastjson_remoting_body_json_with_features<T: Serialize>(
    value: &T,
    pretty_format: bool,
    features: &[JavaJsonWriterFeature],
) -> Result<String, serde_json::Error> {
    crate::route::validate_java_writer_features(features)?;
    if let Some(output) = crate::route::java_non_serializable_writer_output(features)? {
        return Ok(output);
    }
    let indent = crate::route::java_json_indent(pretty_format, features);
    let mut value = crate::route::java_serialize_value_with_features(value, features)?;
    let value_features = JavaJsonWriterValueFeatures {
        write_non_string_values_as_strings: features
            .contains(&JavaJsonWriterFeature::WriteNonStringValueAsString),
        write_boolean_as_number: features.contains(&JavaJsonWriterFeature::WriteBooleanAsNumber),
        write_enum_using_ordinal: features.contains(&JavaJsonWriterFeature::WriteEnumUsingOrdinal),
        browser_compatible: features.contains(&JavaJsonWriterFeature::BrowserCompatible),
        browser_secure: features.contains(&JavaJsonWriterFeature::BrowserSecure),
        bean_to_array: features.contains(&JavaJsonWriterFeature::BeanToArray),
        write_class_name: features.contains(&JavaJsonWriterFeature::WriteClassName),
        not_write_set_class_name: features.contains(&JavaJsonWriterFeature::NotWriteSetClassName),
        use_enum_names: features.contains(&JavaJsonWriterFeature::WriteEnumsUsingName)
            || features.contains(&JavaJsonWriterFeature::WriteEnumUsingToString),
        ..JavaJsonWriterValueFeatures::default()
    };
    crate::route::apply_java_json_writer_value_features(
        &mut value,
        value_features.write_non_string_values_as_strings,
        value_features.write_boolean_as_number,
        value_features.write_enum_using_ordinal,
        value_features.browser_compatible,
    );
    if features.contains(&JavaJsonWriterFeature::NotWriteDefaultValue) {
        crate::route::omit_default_writer_values(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::NotWriteEmptyArray) {
        crate::route::omit_empty_writer_values(&mut value, false);
    }
    if features.contains(&JavaJsonWriterFeature::IgnoreEmpty) {
        crate::route::omit_empty_writer_values(&mut value, true);
    }
    if features.contains(&JavaJsonWriterFeature::WriteLongAsString) {
        crate::route::apply_java_json_long_as_string(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::IgnoreNonFieldGetter) {
        crate::route::omit_java_non_field_getter_properties(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::WriteNulls)
        || features.contains(&JavaJsonWriterFeature::WriteMapNullValue)
    {
        crate::route::apply_java_write_nulls::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::WriteNullListAsEmpty) {
        crate::route::apply_java_null_lists::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::WriteNullStringAsEmpty) {
        crate::route::apply_java_null_strings::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::NullAsDefaultValue) {
        crate::route::apply_java_null_defaults::<T>(&mut value)?;
    }
    if value_features.write_class_name {
        crate::route::apply_java_class_names::<T>(
            &mut value,
            features.contains(&JavaJsonWriterFeature::NotWriteRootClassName),
            features.contains(&JavaJsonWriterFeature::NotWriteHashMapArrayListClassName),
        )?;
    }
    let value_features = JavaJsonWriterValueFeatures {
        use_single_quotes: features.contains(&JavaJsonWriterFeature::UseSingleQuotes),
        unquote_field_names: features.contains(&JavaJsonWriterFeature::UnquoteFieldName),
        ignore_non_field_getter: features.contains(&JavaJsonWriterFeature::IgnoreNonFieldGetter),
        ..value_features
    };
    let mut bean_to_array_maps = std::collections::HashMap::new();
    if value_features.bean_to_array {
        crate::route::apply_java_bean_to_array::<T>(
            &mut value,
            &mut bean_to_array_maps,
            value_features.use_enum_names,
        )?;
    }
    crate::route::apply_java_reference_output_markers(&mut value);
    let mut output = String::new();
    let mut writer = JavaRemotingBodyWriter {
        indent,
        value_features,
        map_fields: &bean_to_array_maps,
        path: Vec::new(),
    };
    write_java_remoting_body_json(&value, None, 0, false, &mut writer, &mut output)?;
    if features.contains(&JavaJsonWriterFeature::EscapeNoneAscii) {
        Ok(crate::route::escape_none_ascii_json(&output))
    } else {
        Ok(output)
    }
}

#[derive(Clone, Copy, Default)]
struct JavaJsonWriterValueFeatures {
    write_non_string_values_as_strings: bool,
    write_boolean_as_number: bool,
    write_enum_using_ordinal: bool,
    browser_compatible: bool,
    browser_secure: bool,
    use_single_quotes: bool,
    unquote_field_names: bool,
    ignore_non_field_getter: bool,
    bean_to_array: bool,
    write_class_name: bool,
    not_write_set_class_name: bool,
    use_enum_names: bool,
}

struct JavaRemotingBodyWriter<'a> {
    indent: Option<&'a str>,
    value_features: JavaJsonWriterValueFeatures,
    map_fields: &'a crate::route::BeanToArrayMapFields,
    path: Vec<crate::route::JavaJsonPathSegment>,
}

fn write_java_remoting_body_json(
    value: &serde_json::Value,
    field_name: Option<&str>,
    depth: usize,
    queue_table: bool,
    writer: &mut JavaRemotingBodyWriter<'_>,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    let indent = writer.indent;
    let value_features = writer.value_features;
    let field_name = writer.map_fields.get(&writer.path).copied().or(field_name);
    let queue_table = queue_table
        || matches!(
            field_name,
            Some("consumerTable" | "messageQueueTable" | "offsetTable")
        );
    match value {
        serde_json::Value::Object(fields)
            if fields.is_empty() && (indent.is_none() || field_name.is_some()) =>
        {
            output.push_str("{}")
        }
        serde_json::Value::Object(fields) if fields.is_empty() => {
            output.push_str("{\n");
            write_java_remoting_body_indent(output, depth + 1, indent);
            output.push('\n');
            write_java_remoting_body_indent(output, depth, indent);
            output.push('}');
        }
        serde_json::Value::Object(fields) => {
            output.push('{');
            for (index, (key, value)) in fields.iter().enumerate() {
                if indent.is_some() {
                    output.push('\n');
                    write_java_remoting_body_indent(output, depth + 1, indent);
                }
                let mut queue_key = if queue_table {
                    serde_json::from_str::<serde_json::Value>(key)
                        .ok()
                        .filter(serde_json::Value::is_object)
                } else {
                    None
                };
                if let Some(queue_key) = &mut queue_key {
                    crate::route::apply_java_json_writer_value_features(
                        queue_key,
                        value_features.write_non_string_values_as_strings,
                        value_features.write_boolean_as_number,
                        value_features.write_enum_using_ordinal,
                        value_features.browser_compatible,
                    );
                    if value_features.bean_to_array {
                        crate::route::apply_java_message_queue_bean_to_array(
                            queue_key,
                            value_features.use_enum_names,
                        );
                    }
                    if value_features.write_class_name {
                        crate::route::apply_java_message_queue_class_name(queue_key);
                    }
                }
                if let Some(queue_key) = queue_key {
                    if value_features.ignore_non_field_getter {
                        let queue_key = serde_json::to_string(&queue_key)?;
                        crate::route::write_java_json_string_with_browser_secure(
                            &queue_key,
                            value_features.use_single_quotes,
                            value_features.browser_secure,
                            output,
                        )?;
                    } else {
                        let mut queue_key_writer = JavaRemotingBodyWriter {
                            indent: writer.indent,
                            value_features: JavaJsonWriterValueFeatures {
                                write_class_name: value_features.write_class_name,
                                not_write_set_class_name: value_features.not_write_set_class_name,
                                use_single_quotes: value_features.use_single_quotes,
                                unquote_field_names: value_features.unquote_field_names,
                                browser_secure: value_features.browser_secure,
                                bean_to_array: value_features.bean_to_array,
                                use_enum_names: value_features.use_enum_names,
                                ..JavaJsonWriterValueFeatures::default()
                            },
                            map_fields: writer.map_fields,
                            path: writer.path.clone(),
                        };
                        write_java_remoting_body_json(
                            &queue_key,
                            None,
                            depth + 1,
                            false,
                            &mut queue_key_writer,
                            output,
                        )?;
                    }
                } else {
                    crate::route::write_java_json_field_name(
                        key,
                        value,
                        field_name.is_some_and(crate::route::is_java_map_property),
                        value_features.use_single_quotes,
                        value_features.unquote_field_names,
                        value_features.browser_secure,
                        output,
                    )?;
                }
                output.push(':');
                let child_is_queue_table = queue_table
                    || matches!(key.as_str(), "messageQueueTable" | "offsetTable")
                    || field_name == Some("consumerTable");
                writer
                    .path
                    .push(crate::route::JavaJsonPathSegment::ObjectKey(key.clone()));
                let result = write_java_remoting_body_json(
                    value,
                    Some(key),
                    depth + 1,
                    child_is_queue_table,
                    writer,
                    output,
                );
                writer.path.pop();
                result?;
                if index + 1 != fields.len() {
                    output.push(',');
                }
            }
            if indent.is_some() {
                output.push('\n');
                write_java_remoting_body_indent(output, depth, indent);
            }
            output.push('}');
        }
        serde_json::Value::Array(values) if values.is_empty() && indent.is_none() => {
            if value_features.write_class_name
                && !value_features.not_write_set_class_name
                && crate::route::is_java_set_property(field_name)
            {
                output.push_str("Set");
            }
            output.push_str("[]")
        }
        serde_json::Value::Array(values) if values.is_empty() => {
            if value_features.write_class_name
                && !value_features.not_write_set_class_name
                && crate::route::is_java_set_property(field_name)
            {
                output.push_str("Set");
            }
            output.push_str("[\n");
            write_java_remoting_body_indent(output, depth + 1, indent);
            output.push('\n');
            write_java_remoting_body_indent(output, depth, indent);
            output.push(']');
        }
        serde_json::Value::Array(values) => {
            if value_features.write_class_name
                && !value_features.not_write_set_class_name
                && crate::route::is_java_set_property(field_name)
            {
                output.push_str("Set");
            }
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if indent.is_some() {
                    output.push('\n');
                    write_java_remoting_body_indent(output, depth + 1, indent);
                }
                writer
                    .path
                    .push(crate::route::JavaJsonPathSegment::ArrayIndex(index));
                let result =
                    write_java_remoting_body_json(value, None, depth + 1, false, writer, output);
                writer.path.pop();
                result?;
                if index + 1 != values.len() {
                    output.push(',');
                }
            }
            if indent.is_some() {
                output.push('\n');
                write_java_remoting_body_indent(output, depth, indent);
            }
            output.push(']');
        }
        serde_json::Value::String(value) => {
            let use_single_quotes = value_features.use_single_quotes
                && field_name.is_none_or(|name| !crate::route::is_java_enum_field(name));
            crate::route::write_java_json_string_with_browser_secure(
                value,
                use_single_quotes,
                value_features.browser_secure,
                output,
            )?;
        }
        serde_json::Value::Number(number) => {
            output.push_str(&serde_json::to_string(value)?);
            if value_features.write_class_name
                && (queue_table || crate::route::is_java_long_field(field_name))
                && crate::route::is_small_java_long(number)
            {
                output.push('L');
            }
        }
        _ => output.push_str(&serde_json::to_string(value)?),
    }
    Ok(())
}

fn write_java_remoting_body_indent(output: &mut String, depth: usize, indent: Option<&str>) {
    let Some(indent) = indent else {
        return;
    };
    for _ in 0..depth {
        output.push_str(indent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn consumer_running_info_raw_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/consumer/running-info.json"))
                .unwrap();
        let cases = fixture["decodeInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 4);
        for case in cases {
            let decoded =
                ConsumerRunningInfoBody::decode_java_json_body(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(decoded.is_ok(), java_accepts, "{}: {decoded:?}", case["id"]);
            if let Ok(decoded) = decoded {
                let encoded = String::from_utf8(decoded.encode_java_json().unwrap()).unwrap();
                assert_eq!(encoded, case["encodedBody"], "{}", case["id"]);
            }
        }

        let java_body = fixture["body"].as_str().unwrap();
        let decoded = ConsumerRunningInfoBody::decode_java_json_body(java_body).unwrap();
        let encoded = String::from_utf8(decoded.encode_java_json().unwrap()).unwrap();
        fn normalize_java_sets(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Object(fields) => {
                    for (name, value) in fields {
                        normalize_java_sets(value);
                        if matches!(name.as_str(), "codeSet" | "tagsSet") {
                            if let serde_json::Value::Array(values) = value {
                                values.sort_by_key(serde_json::Value::to_string);
                            }
                        }
                    }
                }
                serde_json::Value::Array(values) => {
                    for value in values {
                        normalize_java_sets(value);
                    }
                }
                _ => {}
            }
        }
        let mut expected = ConsumerRunningInfoBody::decode_java_json(java_body).unwrap();
        let mut actual = ConsumerRunningInfoBody::decode_java_json(&encoded).unwrap();
        normalize_java_sets(&mut expected);
        normalize_java_sets(&mut actual);
        assert_eq!(
            actual, expected,
            "populated Java body with object-valued MessageQueue map keys"
        );
    }

    #[test]
    fn data_version_matches_java_comparison_and_copy_rules() {
        let mut version = DataVersion::new(1, 10, Some(4));
        assert_eq!(
            version.java_to_string(),
            "DataVersion[timestamp=10, counter=4]"
        );
        assert_eq!(DataVersion::new(1, 2, Some(3)).java_hash_code(), 1_026);
        assert_eq!(version.compare(&DataVersion::new(2, 0, Some(0))), Some(-1));
        assert_eq!(version.compare(&DataVersion::new(1, 0, Some(5))), Some(-1));
        assert_eq!(version.compare(&DataVersion::new(1, 11, Some(4))), Some(-1));
        assert_eq!(version.compare(&DataVersion::new(1, 10, Some(4))), Some(0));

        let copied = DataVersion::new(7, 20, Some(8));
        version.assign_new_one(&copied);
        assert_eq!(version, copied);
        assert!(version.next_version_with_state(9));
        assert_eq!(version.state_version(), 9);
        assert_eq!(version.counter(), Some(9));
        assert!(version.timestamp() >= 20);

        version.set_counter(None);
        assert!(!version.next_version());
        assert_eq!(version.compare(&DataVersion::new(9, 0, Some(1))), None);
    }

    #[test]
    fn logic_queue_mapping_item_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["logicQueueMappingItemMethods"].as_array().unwrap() {
            let item: LogicQueueMappingItem =
                serde_json::from_value(case["value"].clone()).unwrap();
            let getters = &case["getters"];
            assert_eq!(item.get_gen(), getters["gen"].as_i64().unwrap() as i32);
            assert_eq!(item.queue_id(), getters["queueId"].as_i64().unwrap() as i32);
            assert_eq!(item.bname(), getters["bname"].as_str());
            assert_eq!(
                item.logic_offset(),
                getters["logicOffset"].as_i64().unwrap()
            );
            assert_eq!(
                item.start_offset(),
                getters["startOffset"].as_i64().unwrap()
            );
            assert_eq!(item.end_offset(), getters["endOffset"].as_i64().unwrap());
            assert_eq!(
                item.time_of_start(),
                getters["timeOfStart"].as_i64().unwrap()
            );
            assert_eq!(item.time_of_end(), getters["timeOfEnd"].as_i64().unwrap());

            for conversion in case["conversions"].as_array().unwrap() {
                if let Some(physical_offset) = conversion["physicalOffset"].as_i64() {
                    if let Some(loose) = conversion["looseStaticOffset"].as_i64() {
                        assert_eq!(
                            item.compute_static_queue_offset_loosely(physical_offset),
                            loose,
                            "{} loose offset at {physical_offset}",
                            case["case"]
                        );
                    }
                    if let Some(strict) = conversion["strictStaticOffset"].as_i64() {
                        assert_eq!(
                            item.compute_static_queue_offset_strictly(physical_offset),
                            strict,
                            "{} strict offset at {physical_offset}",
                            case["case"]
                        );
                    }
                    if let Some(static_offset) = conversion["staticOffset"].as_i64() {
                        assert_eq!(
                            item.compute_physical_queue_offset(static_offset),
                            physical_offset,
                            "{} physical offset at {static_offset}",
                            case["case"]
                        );
                    }
                }
            }
            assert_eq!(
                item.compute_max_static_queue_offset(),
                case["maxStaticQueueOffset"].as_i64().unwrap()
            );
            assert_eq!(item.check_if_end_offset_decided(), case["endOffsetDecided"]);
            assert_eq!(
                item.check_if_logic_offset_decided(),
                case["logicOffsetDecided"]
            );
            assert_eq!(
                item.compute_offset_delta(),
                case["offsetDelta"].as_i64().unwrap()
            );
            assert_eq!(
                item.java_hash_code(),
                case["hashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(item.java_to_string(), case["toString"]);
        }

        for case in fixture["logicQueueMappingItemEquality"].as_array().unwrap() {
            let left: LogicQueueMappingItem = serde_json::from_value(case["left"].clone()).unwrap();
            let right: LogicQueueMappingItem =
                serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"], "{}", case["case"]);
        }

        let setters = &fixture["logicQueueMappingItemSetters"];
        let mut item = LogicQueueMappingItem::default();
        item.set_gen(setters["generation"].as_i64().unwrap() as i32);
        item.set_queue_id(setters["queueId"].as_i64().unwrap() as i32);
        item.set_bname(setters["bname"].as_str().map(str::to_owned));
        item.set_logic_offset(setters["logicOffset"].as_i64().unwrap());
        item.set_start_offset(setters["startOffset"].as_i64().unwrap());
        item.set_end_offset(setters["endOffset"].as_i64().unwrap());
        item.set_time_of_start(setters["timeOfStart"].as_i64().unwrap());
        item.set_time_of_end(setters["timeOfEnd"].as_i64().unwrap());
        assert_eq!(
            item.get_gen(),
            setters["generation"].as_i64().unwrap() as i32
        );
        assert_eq!(item.queue_id(), setters["queueId"].as_i64().unwrap() as i32);
        assert_eq!(item.bname(), setters["bname"].as_str());
        assert_eq!(
            item.logic_offset(),
            setters["logicOffset"].as_i64().unwrap()
        );
        assert_eq!(
            item.start_offset(),
            setters["startOffset"].as_i64().unwrap()
        );
        assert_eq!(item.end_offset(), setters["endOffset"].as_i64().unwrap());
        assert_eq!(
            item.time_of_start(),
            setters["timeOfStart"].as_i64().unwrap()
        );
        assert_eq!(item.time_of_end(), setters["timeOfEnd"].as_i64().unwrap());
    }

    #[test]
    fn topic_queue_mapping_detail_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicQueueMappingDetailMethods"]
            .as_array()
            .unwrap()
        {
            let left: TopicQueueMappingDetail =
                serde_json::from_value(case["left"].clone()).unwrap();
            let right: TopicQueueMappingDetail =
                serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"], "{} equality", case["case"]);
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32,
                "{} left hashCode",
                case["case"]
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32,
                "{} right hashCode",
                case["case"]
            );
            assert_eq!(
                left.java_to_string(),
                case["leftToString"],
                "{} left toString",
                case["case"]
            );
            assert_eq!(
                right.java_to_string(),
                case["rightToString"],
                "{} right toString",
                case["case"]
            );
        }
    }

    #[test]
    fn topic_queue_mapping_detail_operations_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
        let expected = &fixture["topicQueueMappingDetailOperations"];
        let mut detail =
            TopicQueueMappingDetail::new(Some("topic".into()), 10, Some("broker".into()), 4);
        let items = SharedValue::new(vec![Some(LogicQueueMappingItem::new(
            1,
            4,
            Some("broker".into()),
            0,
            0,
            6,
            -1,
            -1,
        ))]);
        assert_eq!(
            TopicQueueMappingDetail::put_mapping_info(Some(&detail), Some(7), Some(items.clone()))
                .unwrap(),
            expected["inserted"]
        );
        assert_eq!(
            TopicQueueMappingDetail::put_mapping_info(None, None, Some(SharedValue::new(vec![])))
                .unwrap(),
            expected["emptyPutWithNullDetail"]
        );
        let returned = TopicQueueMappingDetail::get_mapping_info(Some(&detail), Some(7))
            .unwrap()
            .unwrap();
        assert_eq!(returned.ptr_eq(&items), expected["returnedSameList"]);
        items.with_mut(|items| {
            items.push(Some(LogicQueueMappingItem::new(
                2,
                5,
                Some("broker".into()),
                6,
                0,
                -1,
                -1,
                -1,
            )));
        });
        assert_eq!(
            returned.with(Vec::len),
            expected["returnedSizeAfterExternalAdd"].as_u64().unwrap() as usize
        );
        detail.set_hosted_queues_owned(Some(BTreeMap::from([
            (7, Some(items)),
            (
                8,
                Some(SharedValue::new(vec![Some(LogicQueueMappingItem::new(
                    1,
                    2,
                    Some("broker".into()),
                    0,
                    0,
                    5,
                    -1,
                    -1,
                ))])),
            ),
        ])));
        assert_eq!(
            TopicQueueMappingDetail::get_mapping_info(Some(&detail), Some(99))
                .unwrap()
                .is_none(),
            expected["missingMapping"]
        );
        assert_eq!(
            serde_json::to_value(TopicQueueMappingDetail::build_id_map(Some(&detail), 0).unwrap())
                .unwrap(),
            expected["currentIdMap"]
        );
        assert_eq!(
            serde_json::to_value(TopicQueueMappingDetail::build_id_map(Some(&detail), 1).unwrap())
                .unwrap(),
            expected["otherLevelIdMap"]
        );
        assert_eq!(
            TopicQueueMappingDetail::compute_max_offset_from_mapping(Some(&detail), Some(7))
                .unwrap(),
            expected["maxOffset"].as_i64().unwrap()
        );
        assert_eq!(
            TopicQueueMappingDetail::compute_max_offset_from_mapping(Some(&detail), Some(99))
                .unwrap(),
            expected["emptyMaxOffset"].as_i64().unwrap()
        );
        assert_eq!(
            TopicQueueMappingDetail::check_if_as_physical(Some(&detail), Some(8)).unwrap(),
            expected["physicalSingle"]
        );
        assert_eq!(
            TopicQueueMappingDetail::check_if_as_physical(Some(&detail), Some(7)).unwrap(),
            expected["physicalMultiple"]
        );
        assert_eq!(
            TopicQueueMappingDetail::check_if_as_physical(Some(&detail), Some(99)).unwrap(),
            expected["physicalMissing"]
        );
        assert_eq!(
            serde_json::to_value(
                TopicQueueMappingDetail::clone_as_mapping_info(Some(&detail)).unwrap()
            )
            .unwrap(),
            expected["clone"]
        );

        let cases = fixture["topicQueueMappingDetailNullOperations"]
            .as_array()
            .unwrap();
        let valid = TopicQueueMappingDetail::default();
        let null_hosted = serde_json::from_value::<TopicQueueMappingDetail>(
            serde_json::json!({"hostedQueues": null}),
        )
        .unwrap();
        let null_item = serde_json::from_value::<TopicQueueMappingDetail>(
            serde_json::json!({"hostedQueues": {"0": [null]}}),
        )
        .unwrap();
        let nonempty_items = SharedValue::new(vec![Some(LogicQueueMappingItem::default())]);
        for case in cases {
            let result = match case["operation"].as_str().unwrap() {
                "put-null-list" => {
                    TopicQueueMappingDetail::put_mapping_info(Some(&valid), Some(0), None)
                        .map(|_| ())
                }
                "put-null-detail" => TopicQueueMappingDetail::put_mapping_info(
                    None,
                    Some(0),
                    Some(nonempty_items.clone()),
                )
                .map(|_| ()),
                "put-null-id" => TopicQueueMappingDetail::put_mapping_info(
                    Some(&valid),
                    None,
                    Some(nonempty_items.clone()),
                )
                .map(|_| ()),
                "get-null-detail" => {
                    TopicQueueMappingDetail::get_mapping_info(None, Some(0)).map(|_| ())
                }
                "get-null-hosted-queues" => {
                    TopicQueueMappingDetail::get_mapping_info(Some(&null_hosted), Some(0))
                        .map(|_| ())
                }
                "get-null-id" => {
                    TopicQueueMappingDetail::get_mapping_info(Some(&valid), None).map(|_| ())
                }
                "build-null-detail" => TopicQueueMappingDetail::build_id_map(None, 0).map(|_| ()),
                "build-null-item" => {
                    TopicQueueMappingDetail::build_id_map(Some(&null_item), 0).map(|_| ())
                }
                "compute-null-hosted-queues" => {
                    TopicQueueMappingDetail::compute_max_offset_from_mapping(
                        Some(&null_hosted),
                        Some(0),
                    )
                    .map(|_| ())
                }
                "compute-null-item" => TopicQueueMappingDetail::compute_max_offset_from_mapping(
                    Some(&null_item),
                    Some(0),
                )
                .map(|_| ()),
                "check-null-item" => {
                    TopicQueueMappingDetail::check_if_as_physical(Some(&null_item), Some(0))
                        .map(|_| ())
                }
                "clone-null-detail" => {
                    TopicQueueMappingDetail::clone_as_mapping_info(None).map(|_| ())
                }
                operation => panic!("unhandled Java operation case: {operation}"),
            };
            assert!(
                result.is_err(),
                "{} should match Java {}",
                case["operation"],
                case["outcome"]
            );
            assert_eq!(case["outcome"], "NullPointerException");
        }
    }

    #[test]
    fn acl_info_preserves_nullable_nested_java_policy_fields() {
        let defaults = AclInfo::java_decode(b"{}").unwrap();
        assert_eq!(defaults.get_subject(), None);
        assert!(defaults.get_policies().is_none());

        let decoded = AclInfo::java_decode(
            br#"{"subject":"subject","policies":[null,{"policyType":"CUSTOM","entries":[null,{"resource":"topic-a","actions":["PUB",null],"sourceIps":[],"decision":"ALLOW"}]}]}"#,
        )
        .unwrap();
        assert_eq!(decoded.get_subject(), Some("subject"));
        let policies = decoded.get_policies().unwrap();
        assert!(policies[0].is_none());
        let policy = policies[1].as_ref().unwrap();
        assert_eq!(policy.get_policy_type(), Some("CUSTOM"));
        let entries = policy.get_entries().unwrap();
        assert!(entries[0].is_none());
        let entry = entries[1].as_ref().unwrap();
        assert_eq!(entry.get_resource(), Some("topic-a"));
        assert_eq!(entry.get_actions().unwrap(), &[Some("PUB".into()), None]);
        assert!(entry.get_source_ips().unwrap().is_empty());
        assert_eq!(entry.get_decision(), Some("ALLOW"));

        let encoded: serde_json::Value =
            serde_json::from_slice(&decoded.java_encode().unwrap()).unwrap();
        assert_eq!(encoded["policies"][0], serde_json::Value::Null);
        assert_eq!(
            encoded["policies"][1]["entries"][0],
            serde_json::Value::Null
        );
        assert_eq!(encoded["policies"][1]["entries"][1]["sourceIps"], json!([]));

        let generated = AclInfo::of(
            Some("subject".into()),
            vec![Some("topic-b".into())],
            Some(vec![Some("SUB".into())]),
            Some(vec![Some("127.0.0.1".into())]),
            Some("ALLOW".into()),
        );
        let policy = generated.get_policies().unwrap()[0].as_ref().unwrap();
        assert_eq!(
            policy.get_entries().unwrap()[0]
                .as_ref()
                .unwrap()
                .get_resource(),
            Some("topic-b")
        );
    }

    #[test]
    fn topic_config_serialize_wrapper_preserves_java_constructor_defaults() {
        let defaults = TopicConfigSerializeWrapper::java_decode(b"{}").unwrap();
        assert!(defaults.data_version().is_some());
        assert!(defaults.data_version().unwrap().timestamp() > 0);
        assert_eq!(defaults.data_version().unwrap().counter(), Some(0));
        assert!(defaults.topic_config_table().unwrap().is_empty());

        let explicit_nulls = TopicConfigSerializeWrapper::java_decode(
            br#"{"dataVersion":null,"topicConfigTable":null}"#,
        )
        .unwrap();
        assert!(explicit_nulls.data_version().is_none());
        assert!(explicit_nulls.topic_config_table().is_none());
    }

    #[test]
    fn subscription_group_wrapper_preserves_java_defaults_and_null_tables() {
        let defaults = SubscriptionGroupWrapper::java_decode(b"{}").unwrap();
        assert!(defaults.subscription_group_table().unwrap().is_empty());
        assert!(defaults.forbidden_table().unwrap().is_empty());
        assert_eq!(defaults.data_version().unwrap().counter(), Some(0));
        assert!(defaults.data_version().unwrap().timestamp() > 0);

        let explicit_nulls = SubscriptionGroupWrapper::java_decode(
            br#"{"subscriptionGroupTable":null,"forbiddenTable":null,"dataVersion":null}"#,
        )
        .unwrap();
        assert!(explicit_nulls.subscription_group_table().is_none());
        assert!(explicit_nulls.forbidden_table().is_none());
        assert!(explicit_nulls.data_version().is_none());

        let populated = SubscriptionGroupWrapper::java_decode(
            br#"{"subscriptionGroupTable":{"workers":{"groupName":"workers"}},"forbiddenTable":{"workers":{"orders":1}},"dataVersion":{"stateVersion":3,"timestamp":7,"counter":2}}"#,
        )
        .unwrap();
        assert_eq!(
            populated.subscription_group_table().unwrap()["workers"].get_group_name(),
            Some("workers")
        );
        assert_eq!(populated.forbidden_table().unwrap()["workers"]["orders"], 1);
        assert_eq!(populated.data_version().unwrap().state_version(), 3);
    }

    #[test]
    fn metadata_snapshot_wrappers_match_java_identity_equality() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let identity = &fixture["metadataWrapperIdentity"];

        for prefix in ["topicConfigWrapper", "subscriptionGroupWrapper"] {
            assert_eq!(identity[format!("{prefix}EqualsOwner")], "java.lang.Object");
            assert_eq!(
                identity[format!("{prefix}HashCodeOwner")],
                "java.lang.Object"
            );
            assert_eq!(identity[format!("{prefix}SameStateEncodedEqual")], true);
            assert_eq!(identity[format!("{prefix}DistinctSameStateEquals")], false);
            assert_eq!(identity[format!("{prefix}SameReferenceEquals")], true);
        }
    }

    #[test]
    fn query_subscription_response_body_matches_java_null_and_alias_semantics() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/admin/query-subscription-response-body.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["artifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody()"])
        );
        assert_eq!(fixture["publicFields"], json!([]));
        assert_eq!(
            fixture["publicMethods"],
            json!([
                "java.lang.String getGroup()",
                "java.lang.String getTopic()",
                "org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData getSubscriptionData()",
                "void setGroup(java.lang.String)",
                "void setSubscriptionData(org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData)",
                "void setTopic(java.lang.String)"
            ])
        );

        let default = QuerySubscriptionResponseBody::default();
        assert!(default.get_subscription_data().is_none());
        assert!(default.get_group().is_none());
        assert!(default.get_topic().is_none());
        assert_eq!(
            serde_json::to_value(&default).unwrap(),
            serde_json::from_str::<serde_json::Value>(fixture["defaultJson"].as_str().unwrap())
                .unwrap()
        );

        let populated_json: serde_json::Value =
            serde_json::from_str(fixture["populatedJson"].as_str().unwrap()).unwrap();
        let decoded: QuerySubscriptionResponseBody =
            serde_json::from_value(populated_json.clone()).unwrap();
        assert_eq!(decoded.get_group(), Some("workers"));
        assert_eq!(decoded.get_topic(), Some("orders"));
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            populated_json,
            "the Java response body JSON must round-trip"
        );

        let assigned = SharedValue::new(SubscriptionData::default());
        let mut value = QuerySubscriptionResponseBody::default();
        value.set_subscription_data(Some(assigned.clone()));
        let getter = value.get_subscription_data().unwrap();
        assert_eq!(
            getter.ptr_eq(&assigned),
            fixture["getterReturnsStoredSubscriptionDataIdentity"]
        );
        getter.with_mut(|subscription| {
            subscription.set_sub_string(Some("tag-b".into()));
        });
        assert_eq!(
            assigned.with(|subscription| subscription.get_sub_string().map(str::to_owned)),
            Some("tag-b".to_owned()),
            "mutating through the getter must update the assigned object"
        );
        assert_eq!(fixture["getterMutationVisibleToAssignedObject"], true);

        value.set_group(Some("workers".to_owned()));
        value.set_topic(Some("orders".to_owned()));
        assert_eq!(value.get_group(), Some("workers"));
        assert_eq!(value.get_topic(), Some("orders"));

        value.set_subscription_data(None);
        value.set_group(None);
        value.set_topic(None);
        assert!(value.get_subscription_data().is_none());
        assert!(value.get_group().is_none());
        assert!(value.get_topic().is_none());
        assert_eq!(
            serde_json::to_value(&value).unwrap(),
            serde_json::from_str::<serde_json::Value>(
                fixture["explicitNullJson"].as_str().unwrap()
            )
            .unwrap()
        );

        let explicit_null: QuerySubscriptionResponseBody =
            serde_json::from_str(r#"{"subscriptionData":null,"group":null,"topic":null}"#).unwrap();
        assert!(explicit_null.get_subscription_data().is_none());
        assert_eq!(
            serde_json::to_value(explicit_null).unwrap(),
            serde_json::from_str::<serde_json::Value>(
                fixture["explicitNullJson"].as_str().unwrap()
            )
            .unwrap()
        );
    }

    #[test]
    fn group_list_matches_java_default_null_element_and_alias_semantics() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/admin/group-list.json")).unwrap();
        assert_eq!(
            fixture["artifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.GroupList()"])
        );
        assert_eq!(fixture["publicFields"], json!([]));
        assert_eq!(
            fixture["publicMethods"],
            json!([
                "java.util.HashSet getGroupList()",
                "void setGroupList(java.util.HashSet)"
            ])
        );

        let default = GroupList::default();
        let default_set = default.get_group_list().unwrap();
        assert!(default_set.with(|set| set.is_empty()));
        assert_eq!(fixture["defaultGroupListNull"], false);
        assert_eq!(fixture["defaultGroupListEmpty"], true);
        assert_eq!(
            serde_json::to_value(&default).unwrap(),
            serde_json::from_str::<serde_json::Value>(fixture["defaultJson"].as_str().unwrap())
                .unwrap()
        );

        let populated_json: serde_json::Value =
            serde_json::from_str(fixture["populatedJson"].as_str().unwrap()).unwrap();
        let decoded: GroupList = serde_json::from_value(populated_json.clone()).unwrap();
        let groups = decoded.get_group_list().unwrap();
        assert!(groups.with(|set| set.contains(&Some("workers".into()))));
        assert!(groups.with(|set| set.contains(&None)));

        let assigned = SharedValue::new(std::collections::HashSet::from([
            Some("workers".to_owned()),
            None,
        ]));
        let mut value = GroupList::default();
        value.set_group_list(Some(assigned.clone()));
        let getter = value.get_group_list().unwrap();
        assert_eq!(
            getter.ptr_eq(&assigned),
            fixture["getterReturnsStoredSetIdentity"]
        );
        getter.with_mut(|groups| {
            groups.insert(Some("billing".into()));
        });
        assert!(assigned.with(|groups| groups.contains(&Some("billing".into()))));
        assert_eq!(fixture["getterMutationVisibleToAssignedSet"], true);

        let explicit_null: GroupList = serde_json::from_str(r#"{"groupList":null}"#).unwrap();
        assert!(explicit_null.get_group_list().is_none());
        assert_eq!(fixture["setterPreservesNull"], true);
        assert_eq!(
            serde_json::to_value(explicit_null).unwrap(),
            serde_json::from_str::<serde_json::Value>(
                fixture["explicitNullJson"].as_str().unwrap()
            )
            .unwrap()
        );
    }

    #[test]
    fn get_lite_topic_info_response_body_matches_java_api_and_alias_semantics() {
        use base64::Engine as _;

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/rpc-invoked-ext-more/lite-topic-info-success.json"
        ))
        .unwrap();
        let api = &fixture["liteTopicInfoApi"];
        assert_eq!(api["requestHeaderPublicFields"], json!([]));
        assert_eq!(api["responseBodyPublicFields"], json!([]));
        assert_eq!(api["topicOffsetPublicFields"], json!([]));
        assert_eq!(
            api["responseBodyPublicMethods"],
            json!([
                "public boolean org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.isShardingToBroker()",
                "public java.lang.String org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.getLiteTopic()",
                "public java.lang.String org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.getParentTopic()",
                "public java.util.Set<org.apache.rocketmq.common.entity.ClientGroup> org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.getSubscriber()",
                "public org.apache.rocketmq.remoting.protocol.admin.TopicOffset org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.getTopicOffset()",
                "public void org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.setLiteTopic(java.lang.String)",
                "public void org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.setParentTopic(java.lang.String)",
                "public void org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.setShardingToBroker(boolean)",
                "public void org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.setSubscriber(java.util.Set<org.apache.rocketmq.common.entity.ClientGroup>)",
                "public void org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody.setTopicOffset(org.apache.rocketmq.remoting.protocol.admin.TopicOffset)"
            ])
        );
        assert_eq!(
            api["clientGroupPublicFields"],
            json!(["final String clientId", "final String group"])
        );
        assert_eq!(api["clientGroupPublicMethods"].as_array().unwrap().len(), 3);
        assert_eq!(api["topicOffsetPublicMethods"].as_array().unwrap().len(), 7);

        let defaults = GetLiteTopicInfoResponseBody::default();
        assert_eq!(defaults.get_parent_topic(), None);
        assert_eq!(defaults.get_lite_topic(), None);
        assert!(defaults.get_subscriber().is_none());
        assert!(defaults.get_topic_offset().is_none());
        assert!(!defaults.is_sharding_to_broker());
        assert_eq!(api["responseBodyDefaults"]["parentTopic"], json!(null));
        assert_eq!(api["responseBodyDefaults"]["liteTopic"], json!(null));
        assert_eq!(api["responseBodyDefaults"]["subscriberNull"], true);
        assert_eq!(api["responseBodyDefaults"]["topicOffsetNull"], true);
        assert_eq!(api["responseBodyDefaults"]["shardingToBroker"], false);
        assert_eq!(
            serde_json::to_value(&defaults).unwrap(),
            json!({"shardingToBroker": false})
        );
        let java_default: serde_json::Value = serde_json::from_str(
            std::str::from_utf8(
                &base64::engine::general_purpose::STANDARD
                    .decode(fixture["response"]["bodyBase64"].as_str().unwrap())
                    .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let default_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/rpc-invoked-ext-more/lite-topic-info-defaults.json"
        ))
        .unwrap();
        let default_body = base64::engine::general_purpose::STANDARD
            .decode(default_fixture["response"]["bodyBase64"].as_str().unwrap())
            .unwrap();
        let default_body: serde_json::Value = serde_json::from_slice(&default_body).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&defaults.java_encode().unwrap()).unwrap(),
            default_body
        );

        let encoded = base64::engine::general_purpose::STANDARD
            .decode(fixture["response"]["bodyBase64"].as_str().unwrap())
            .unwrap();
        let decoded = GetLiteTopicInfoResponseBody::java_decode(&encoded).unwrap();
        assert_eq!(decoded.get_parent_topic(), Some("lite-parent"));
        assert_eq!(decoded.get_lite_topic(), Some("%LMQ%lite-parent%orders"));
        assert!(decoded.is_sharding_to_broker());
        let subscribers = decoded.get_subscriber().unwrap();
        assert_eq!(subscribers.with(HashSet::len), 3);
        assert!(subscribers.with(|set| set.contains(&None)));
        assert!(subscribers.with(|set| {
            set.contains(&Some(ClientGroup::new(
                Some("client-1".into()),
                Some("workers".into()),
            )))
        }));
        assert_eq!(
            ClientGroup::new(None, None).to_string(),
            api["clientGroupNullToString"].as_str().unwrap()
        );
        let client_group = ClientGroup::new(Some("client-1".into()), Some("workers".into()));
        assert_eq!(client_group.client_id(), Some("client-1"));
        assert_eq!(client_group.group(), Some("workers"));
        assert_eq!(api["clientGroupEqualValuesCompareEqual"], true);
        assert_eq!(api["clientGroupDifferentValuesCompareUnequal"], true);
        let offset = decoded.get_topic_offset().unwrap();
        assert_eq!(offset.with(TopicOffset::get_min_offset), -2);
        assert_eq!(offset.with(TopicOffset::get_max_offset), 17);
        assert_eq!(offset.with(TopicOffset::get_last_update_timestamp), 1234);
        let encoded: serde_json::Value =
            serde_json::from_slice(&decoded.java_encode().unwrap()).unwrap();
        assert_eq!(encoded["parentTopic"], java_default["parentTopic"]);
        assert_eq!(encoded["liteTopic"], java_default["liteTopic"]);
        assert_eq!(
            encoded["shardingToBroker"],
            java_default["shardingToBroker"]
        );
        assert_eq!(encoded["topicOffset"], java_default["topicOffset"]);
        let encoded_subscribers: HashSet<Option<ClientGroup>> =
            serde_json::from_value(encoded["subscriber"].clone()).unwrap();
        assert_eq!(
            encoded_subscribers,
            subscribers.with(std::clone::Clone::clone)
        );

        let assigned_subscribers = SharedValue::new(HashSet::new());
        let assigned_offset = SharedValue::new(TopicOffset::default());
        let mut value = GetLiteTopicInfoResponseBody::default();
        value.set_subscriber(Some(assigned_subscribers.clone()));
        value.set_topic_offset(Some(assigned_offset.clone()));
        assert!(
            value
                .get_subscriber()
                .unwrap()
                .ptr_eq(&assigned_subscribers)
        );
        assert!(value.get_topic_offset().unwrap().ptr_eq(&assigned_offset));
        value
            .get_subscriber()
            .unwrap()
            .with_mut(|set| set.insert(Some(ClientGroup::new(Some("c".into()), Some("g".into())))));
        value
            .get_topic_offset()
            .unwrap()
            .with_mut(|offset| offset.set_min_offset(8));
        assert_eq!(assigned_subscribers.with(HashSet::len), 1);
        assert_eq!(assigned_offset.with(TopicOffset::get_min_offset), 8);
        assert_eq!(api["subscriberGetterReturnsAssignedSet"], true);
        assert_eq!(api["subscriberGetterMutationVisible"], true);
        assert_eq!(api["topicOffsetGetterReturnsAssignedObject"], true);
        assert_eq!(api["topicOffsetMutationVisible"], true);
    }

    #[test]
    fn get_broker_lite_info_response_body_preserves_defaults_and_metadata_aliases() {
        let defaults = GetBrokerLiteInfoResponseBody::default();
        assert_eq!(defaults.get_store_type(), None);
        assert_eq!(defaults.get_max_lmq_num(), 0);
        assert_eq!(defaults.get_current_lmq_num(), 0);
        assert_eq!(defaults.get_lite_subscription_count(), 0);
        assert_eq!(defaults.get_order_info_count(), 0);
        assert_eq!(defaults.get_cq_table_size(), 0);
        assert_eq!(defaults.get_offset_table_size(), 0);
        assert_eq!(defaults.get_event_map_size(), 0);
        assert!(defaults.get_topic_meta().is_none());
        assert!(defaults.get_group_meta().is_none());

        let decoded = GetBrokerLiteInfoResponseBody::java_decode(
            br#"{
            "storeType":"rocksdb",
            "maxLmqNum":20,
            "currentLmqNum":3,
            "liteSubscriptionCount":2,
            "orderInfoCount":4,
            "cqTableSize":5,
            "offsetTableSize":6,
            "eventMapSize":7,
            "topicMeta":{"orders":8,"payments":9},
            "groupMeta":{"workers":["orders","payments"]}
        }"#,
        )
        .unwrap();
        assert_eq!(decoded.get_store_type(), Some("rocksdb"));
        assert_eq!(decoded.get_max_lmq_num(), 20);
        assert_eq!(decoded.get_current_lmq_num(), 3);
        assert_eq!(decoded.get_lite_subscription_count(), 2);
        assert_eq!(decoded.get_order_info_count(), 4);
        assert_eq!(decoded.get_cq_table_size(), 5);
        assert_eq!(decoded.get_offset_table_size(), 6);
        assert_eq!(decoded.get_event_map_size(), 7);

        let topic_meta = decoded.get_topic_meta().unwrap();
        assert_eq!(topic_meta.with(|map| map["orders"]), Some(8));
        let group_meta = decoded.get_group_meta().unwrap();
        assert_eq!(
            group_meta.with(|map| map["workers"].as_ref().unwrap().len()),
            2
        );
        let encoded: serde_json::Value =
            serde_json::from_slice(&decoded.java_encode().unwrap()).unwrap();
        assert_eq!(encoded["topicMeta"]["payments"], 9);
        let encoded_groups: HashSet<Option<String>> =
            serde_json::from_value(encoded["groupMeta"]["workers"].clone()).unwrap();
        assert_eq!(
            encoded_groups,
            HashSet::from([Some("orders".into()), Some("payments".into())])
        );

        let assigned = SharedValue::new(BTreeMap::new());
        let mut value = GetBrokerLiteInfoResponseBody::default();
        value.set_topic_meta(Some(assigned.clone()));
        let getter = value.get_topic_meta().unwrap();
        assert!(getter.ptr_eq(&assigned));
        getter.with_mut(|map| {
            map.insert("billing".into(), Some(1));
        });
        assert_eq!(assigned.with(|map| map["billing"]), Some(1));
    }

    #[test]
    fn get_parent_topic_info_response_body_preserves_defaults_and_group_aliases() {
        let defaults = GetParentTopicInfoResponseBody::default();
        assert_eq!(defaults.get_topic(), None);
        assert_eq!(defaults.get_ttl(), 0);
        assert!(defaults.get_groups().is_none());
        assert_eq!(defaults.get_lmq_num(), 0);
        assert_eq!(defaults.get_lite_topic_count(), 0);

        let decoded = GetParentTopicInfoResponseBody::java_decode(
            br#"{"topic":"lite-parent","ttl":3600,"groups":["billing","workers",null],"lmqNum":3,"liteTopicCount":12}"#,
        )
        .unwrap();
        assert_eq!(decoded.get_topic(), Some("lite-parent"));
        assert_eq!(decoded.get_ttl(), 3600);
        assert_eq!(decoded.get_lmq_num(), 3);
        assert_eq!(decoded.get_lite_topic_count(), 12);
        let groups = decoded.get_groups().unwrap();
        assert_eq!(groups.with(HashSet::len), 3);
        assert!(groups.with(|set| set.contains(&None)));
        assert!(groups.with(|set| set.contains(&Some("workers".into()))));

        let assigned = SharedValue::new(HashSet::new());
        let mut value = GetParentTopicInfoResponseBody::default();
        value.set_groups(Some(assigned.clone()));
        assert!(value.get_groups().unwrap().ptr_eq(&assigned));
        assigned.with_mut(|set| {
            set.insert(Some("workers".into()));
        });
        assert_eq!(value.get_groups().unwrap().with(HashSet::len), 1);
    }

    #[test]
    fn get_lite_client_info_response_body_preserves_defaults_and_topic_set_aliases() {
        let defaults = GetLiteClientInfoResponseBody::default();
        assert_eq!(defaults.get_parent_topic(), None);
        assert_eq!(defaults.get_group(), None);
        assert_eq!(defaults.get_client_id(), None);
        assert_eq!(defaults.get_last_access_time(), 0);
        assert_eq!(defaults.get_last_consume_time(), 0);
        assert_eq!(defaults.get_lite_topic_count(), 0);
        assert!(defaults.get_lite_topic_set().is_none());

        let decoded = GetLiteClientInfoResponseBody::java_decode(
            br#"{"parentTopic":"lite-parent","group":"workers","clientId":"client-a","lastAccessTime":1000,"lastConsumeTime":500,"liteTopicCount":2,"liteTopicSet":["%LMQ%lite-parent%orders",null]}"#,
        )
        .unwrap();
        assert_eq!(decoded.get_parent_topic(), Some("lite-parent"));
        assert_eq!(decoded.get_group(), Some("workers"));
        assert_eq!(decoded.get_client_id(), Some("client-a"));
        assert_eq!(decoded.get_last_access_time(), 1000);
        assert_eq!(decoded.get_last_consume_time(), 500);
        assert_eq!(decoded.get_lite_topic_count(), 2);
        let topics = decoded.get_lite_topic_set().unwrap();
        assert_eq!(topics.with(HashSet::len), 2);
        assert!(topics.with(|set| set.contains(&None)));
        assert!(topics.with(|set| set.contains(&Some("%LMQ%lite-parent%orders".into()))));

        let assigned = SharedValue::new(HashSet::new());
        let mut value = GetLiteClientInfoResponseBody::default();
        value.set_lite_topic_set(Some(assigned.clone()));
        assert!(value.get_lite_topic_set().unwrap().ptr_eq(&assigned));
        assigned.with_mut(|set| {
            set.insert(Some("%LMQ%lite-parent%orders".into()));
        });
        assert_eq!(value.get_lite_topic_set().unwrap().with(HashSet::len), 1);
    }

    #[test]
    fn get_lite_group_info_body_preserves_nested_defaults_lists_and_aliases() {
        let defaults = GetLiteGroupInfoResponseBody::default();
        assert_eq!(defaults.get_group(), None);
        assert_eq!(defaults.get_parent_topic(), None);
        assert_eq!(defaults.get_lite_topic(), None);
        assert_eq!(defaults.get_earliest_unconsumed_timestamp(), -1);
        assert_eq!(defaults.get_total_lag_count(), 0);
        assert!(defaults.get_lite_topic_offset_wrapper().is_none());
        assert!(defaults.get_lag_count_top_k().is_none());
        assert!(defaults.get_lag_timestamp_top_k().is_none());

        let lag_defaults = LiteLagInfo::default();
        assert_eq!(lag_defaults.get_lite_topic(), None);
        assert_eq!(lag_defaults.get_lag_count(), 0);
        assert_eq!(lag_defaults.get_earliest_unconsumed_timestamp(), -1);
        assert_eq!(lag_defaults.get_consumer_offset(), -1);
        let lag_json = serde_json::to_value(&lag_defaults).unwrap();
        assert_eq!(lag_json["lagCount"], 0);
        assert_eq!(lag_json["earliestUnconsumedTimestamp"], -1);
        assert!(lag_json.get("consumerOffset").is_none());

        let decoded = GetLiteGroupInfoResponseBody::java_decode(
            br#"{"group":"workers","parentTopic":"lite-parent","liteTopic":"%LMQ%lite-parent%orders","earliestUnconsumedTimestamp":1400,"totalLagCount":40,"liteTopicOffsetWrapper":{"brokerOffset":100,"consumerOffset":80,"pullOffset":85,"lastTimestamp":1700},"lagCountTopK":[{"liteTopic":"%LMQ%lite-parent%orders","lagCount":30,"earliestUnconsumedTimestamp":1600,"consumerOffset":70},null],"lagTimestampTopK":[{"liteTopic":"%LMQ%lite-parent%payments","lagCount":10,"earliestUnconsumedTimestamp":1500,"consumerOffset":90}]}"#,
        )
        .unwrap();
        assert_eq!(decoded.get_group(), Some("workers"));
        assert_eq!(decoded.get_parent_topic(), Some("lite-parent"));
        assert_eq!(decoded.get_lite_topic(), Some("%LMQ%lite-parent%orders"));
        assert_eq!(decoded.get_earliest_unconsumed_timestamp(), 1400);
        assert_eq!(decoded.get_total_lag_count(), 40);
        let wrapper = decoded.get_lite_topic_offset_wrapper().unwrap();
        assert_eq!(wrapper.with(OffsetWrapper::get_broker_offset), 100);
        assert_eq!(wrapper.with(OffsetWrapper::get_consumer_offset), 80);
        assert_eq!(wrapper.with(OffsetWrapper::get_pull_offset), 85);
        assert_eq!(wrapper.with(OffsetWrapper::get_last_timestamp), 1700);

        let count_top_k = decoded.get_lag_count_top_k().unwrap();
        assert_eq!(count_top_k.with(Vec::len), 2);
        let first = count_top_k.with(|items| items[0].clone().unwrap());
        assert_eq!(first.with(LiteLagInfo::get_lag_count), 30);
        assert_eq!(
            first.with(LiteLagInfo::get_earliest_unconsumed_timestamp),
            1600
        );
        assert_eq!(first.with(LiteLagInfo::get_consumer_offset), 70);
        assert!(count_top_k.with(|items| items[1].is_none()));
        let timestamp_top_k = decoded.get_lag_timestamp_top_k().unwrap();
        assert_eq!(timestamp_top_k.with(Vec::len), 1);
        assert_eq!(
            timestamp_top_k.with(|items| {
                items[0]
                    .as_ref()
                    .unwrap()
                    .with(|lag| lag.get_lite_topic().map(str::to_owned))
            }),
            Some("%LMQ%lite-parent%payments".to_owned())
        );

        let encoded: serde_json::Value =
            serde_json::from_slice(&decoded.java_encode().unwrap()).unwrap();
        assert_eq!(encoded["earliestUnconsumedTimestamp"], 1400);
        assert_eq!(encoded["liteTopicOffsetWrapper"]["brokerOffset"], 100);
        assert!(encoded["lagCountTopK"][0].get("consumerOffset").is_none());
        assert!(encoded["lagCountTopK"][1].is_null());

        let mut value = GetLiteGroupInfoResponseBody::default();
        let assigned_wrapper = SharedValue::new(OffsetWrapper::default());
        value.set_lite_topic_offset_wrapper(Some(assigned_wrapper.clone()));
        assert!(
            value
                .get_lite_topic_offset_wrapper()
                .unwrap()
                .ptr_eq(&assigned_wrapper)
        );
        assigned_wrapper.with_mut(|offset| offset.set_broker_offset(12));
        assert_eq!(
            value
                .get_lite_topic_offset_wrapper()
                .unwrap()
                .with(OffsetWrapper::get_broker_offset),
            12
        );

        let assigned_lag = SharedValue::new(LiteLagInfo::default());
        let assigned_list = SharedValue::new(vec![Some(assigned_lag.clone()), None]);
        value.set_lag_count_top_k(Some(assigned_list.clone()));
        assert!(value.get_lag_count_top_k().unwrap().ptr_eq(&assigned_list));
        assigned_lag.with_mut(|lag| lag.set_lag_count(7));
        assigned_list.with_mut(|items| items.push(None));
        assert_eq!(value.get_lag_count_top_k().unwrap().with(Vec::len), 3);
        assert_eq!(
            assigned_list.with(|items| items[0].as_ref().unwrap().with(LiteLagInfo::get_lag_count)),
            7
        );
    }

    #[test]
    fn topic_stats_table_matches_java_defaults_nulls_keys_and_aliases() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/admin/topic-stats-table.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["artifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["topicStatsTablePublicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable()"])
        );
        assert_eq!(
            fixture["topicOffsetPublicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.admin.TopicOffset()"])
        );
        assert_eq!(
            fixture["topicStatsTablePublicMethods"],
            json!([
                "double getTopicPutTps()",
                "java.util.Map getOffsetTable()",
                "void setOffsetTable(java.util.Map)",
                "void setTopicPutTps(double)"
            ])
        );
        assert_eq!(fixture["topicStatsTablePublicFields"], json!([]));
        assert_eq!(
            fixture["topicOffsetPublicMethods"],
            json!([
                "java.lang.String toString()",
                "long getLastUpdateTimestamp()",
                "long getMaxOffset()",
                "long getMinOffset()",
                "void setLastUpdateTimestamp(long)",
                "void setMaxOffset(long)",
                "void setMinOffset(long)"
            ])
        );
        assert_eq!(fixture["topicOffsetPublicFields"], json!([]));

        let default = TopicStatsTable::default();
        let default_table = default.get_offset_table().unwrap();
        assert!(default_table.with(|table| table.is_empty()));
        assert_eq!(default.get_topic_put_tps(), 0.0);
        assert_eq!(fixture["defaultOffsetTableNonNull"], true);
        assert_eq!(fixture["defaultOffsetTableEmpty"], true);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&default.java_to_json(false).unwrap())
                .unwrap(),
            serde_json::from_str::<serde_json::Value>(fixture["defaultJson"].as_str().unwrap())
                .unwrap()
        );

        let default_offset = TopicOffset::default();
        assert_eq!(
            [
                default_offset.get_min_offset(),
                default_offset.get_max_offset(),
                default_offset.get_last_update_timestamp(),
            ],
            [0, 0, 0]
        );
        assert_eq!(
            default_offset.to_string(),
            fixture["defaultOffsetToString"].as_str().unwrap()
        );
        assert_eq!(fixture["defaultOffsetValues"], json!([0, 0, 0]));
        assert_eq!(
            fixture["offsetSetterValues"],
            json!([i64::MIN, i64::MAX, -9])
        );
        let mut setter_probe = TopicOffset::default();
        setter_probe.set_min_offset(i64::MIN);
        setter_probe.set_max_offset(i64::MAX);
        setter_probe.set_last_update_timestamp(-9);
        assert_eq!(
            [
                setter_probe.get_min_offset(),
                setter_probe.get_max_offset(),
                setter_probe.get_last_update_timestamp()
            ],
            [i64::MIN, i64::MAX, -9]
        );

        let populated_json = fixture["populatedJson"].as_str().unwrap();
        let decoded = TopicStatsTable::java_from_json(populated_json).unwrap();
        assert_eq!(decoded.get_topic_put_tps(), 1.25);
        let table = decoded.get_offset_table().unwrap();
        assert_eq!(table.with(|table| table.len()), 1);
        let first_queue = MessageQueue::new("orders", "broker-a", 1);
        let first_offset = table.with(|table| table[&first_queue].as_ref().unwrap().clone());
        assert_eq!(first_offset.with(|offset| offset.get_min_offset()), 10);
        assert_eq!(first_offset.with(|offset| offset.get_max_offset()), 16);
        assert_eq!(
            first_offset.with(|offset| offset.get_last_update_timestamp()),
            100
        );
        let encoded = decoded.java_to_json(false).unwrap();
        assert_eq!(encoded, populated_json);
        assert_eq!(decoded.java_encode().unwrap(), populated_json.as_bytes());
        let with_null_values =
            TopicStatsTable::java_from_json(fixture["writeNullMapValuesJson"].as_str().unwrap())
                .unwrap();
        let table_with_nulls = with_null_values.get_offset_table().unwrap();
        assert_eq!(table_with_nulls.with(|table| table.len()), 3);
        assert!(
            table_with_nulls
                .with(|table| { table[&MessageQueue::new("orders", "broker-b", 2)].is_none() })
        );
        assert!(
            table_with_nulls
                .with(|table| { table[&MessageQueue::new("orders", "broker-c", 3)].is_none() })
        );

        let assigned_offset = SharedValue::new(TopicOffset::default());
        let assigned_table = SharedValue::new(std::collections::HashMap::from([(
            MessageQueue::new("orders", "broker-a", 1),
            Some(assigned_offset.clone()),
        )]));
        let mut assigned = TopicStatsTable::default();
        assigned.set_offset_table(Some(assigned_table.clone()));
        assert!(assigned.get_offset_table().unwrap().ptr_eq(&assigned_table));
        let returned_offset = assigned.get_offset_table().unwrap().with(|table| {
            table[&MessageQueue::new("orders", "broker-a", 1)]
                .clone()
                .unwrap()
        });
        assert!(returned_offset.ptr_eq(&assigned_offset));
        returned_offset.with_mut(|offset| offset.set_max_offset(29));
        assert_eq!(assigned_offset.with(|offset| offset.get_max_offset()), 29);
        assert_eq!(fixture["setterRetainsMapIdentity"], true);
        assert_eq!(fixture["getterReturnsStoredMapIdentity"], true);
        assert_eq!(fixture["getterReturnsStoredOffsetIdentity"], true);
        assert_eq!(fixture["getterMutationVisibleToAssignedOffset"], true);
        assert_eq!(fixture["getterMutationVisibleToAssignedMap"], true);

        let missing: TopicStatsTable = serde_json::from_str("{}").unwrap();
        assert_eq!(missing.get_topic_put_tps(), 0.0);
        assert!(
            missing
                .get_offset_table()
                .unwrap()
                .with(|table| table.is_empty())
        );
        let explicit_null: TopicStatsTable =
            serde_json::from_str(r#"{"topicPutTps":1.25,"offsetTable":null}"#).unwrap();
        assert!(explicit_null.get_offset_table().is_none());
        assert_eq!(fixture["setterPreservesNull"], true);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&explicit_null.java_to_json(false).unwrap())
                .unwrap(),
            serde_json::from_str::<serde_json::Value>(
                fixture["explicitNullJson"].as_str().unwrap()
            )
            .unwrap()
        );
        assert!(
            TopicStatsTable::java_decode_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicStatsTable::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn consume_stats_matches_invoked_java_admin_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/admin/query-consume-time-span.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["artifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        let consume_stats_fixture = &fixture["getConsumeStats"];
        assert_eq!(
            consume_stats_fixture["method"],
            "public abstract java.util.concurrent.CompletableFuture<org.apache.rocketmq.remoting.protocol.admin.ConsumeStats> org.apache.rocketmq.client.MqClientAdmin.getConsumeStats(java.lang.String,org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader,long)"
        );
        assert_eq!(
            consume_stats_fixture["consumeStatsPublicMethods"],
            json!([
                "public double org.apache.rocketmq.remoting.protocol.admin.ConsumeStats.getConsumeTps()",
                "public java.util.Map<org.apache.rocketmq.common.message.MessageQueue, org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper> org.apache.rocketmq.remoting.protocol.admin.ConsumeStats.getOffsetTable()",
                "public long org.apache.rocketmq.remoting.protocol.admin.ConsumeStats.computeInflightTotalDiff()",
                "public long org.apache.rocketmq.remoting.protocol.admin.ConsumeStats.computeTotalDiff()",
                "public void org.apache.rocketmq.remoting.protocol.admin.ConsumeStats.setConsumeTps(double)",
                "public void org.apache.rocketmq.remoting.protocol.admin.ConsumeStats.setOffsetTable(java.util.Map<org.apache.rocketmq.common.message.MessageQueue, org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper>)"
            ])
        );
        assert_eq!(
            consume_stats_fixture["offsetWrapperPublicMethods"],
            json!([
                "public long org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.getBrokerOffset()",
                "public long org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.getConsumerOffset()",
                "public long org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.getLastTimestamp()",
                "public long org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.getPullOffset()",
                "public void org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.setBrokerOffset(long)",
                "public void org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.setConsumerOffset(long)",
                "public void org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.setLastTimestamp(long)",
                "public void org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper.setPullOffset(long)"
            ])
        );
        let dto_semantics = &consume_stats_fixture["dtoSemantics"];
        assert_eq!(dto_semantics["defaultConsumeTps"], 0.0);
        assert_eq!(dto_semantics["defaultOffsetTableNonNull"], true);
        assert_eq!(dto_semantics["defaultOffsetTableEmpty"], true);
        assert_eq!(dto_semantics["defaultOffsetValues"], json!([0, 0, 0, 0]));
        assert_eq!(dto_semantics["setterRetainsMapIdentity"], true);
        assert_eq!(dto_semantics["getterReturnsStoredOffsetIdentity"], true);
        assert_eq!(dto_semantics["getterMutationVisibleToAssignedOffset"], true);
        assert_eq!(dto_semantics["getterMutationVisibleToAssignedMap"], true);
        assert_eq!(dto_semantics["setterPreservesNull"], true);
        let java_stats = &fixture["getConsumeStats"]["success"];
        assert_eq!(java_stats["requestCode"], 208);
        assert_eq!(java_stats["result"]["computeTotalDiff"], 13);
        assert_eq!(java_stats["result"]["computeInflightTotalDiff"], 8);

        let body = java_stats["responseBody"].as_str().unwrap();
        let decoded = ConsumeStats::java_from_json(body).unwrap();
        assert_eq!(decoded.get_consume_tps(), 1.25);
        let table = decoded.get_offset_table().unwrap();
        assert_eq!(table.with(|table| table.len()), 2);
        let orders = table.with(|table| {
            table[&MessageQueue::new("orders", "broker-a", 1)]
                .as_ref()
                .unwrap()
                .clone()
        });
        assert_eq!(orders.with(|offset| offset.get_broker_offset()), 29);
        assert_eq!(orders.with(|offset| offset.get_consumer_offset()), 20);
        assert_eq!(orders.with(|offset| offset.get_pull_offset()), 25);
        assert_eq!(orders.with(|offset| offset.get_last_timestamp()), 1000);
        assert_eq!(decoded.compute_total_diff().unwrap(), 13);
        assert_eq!(decoded.compute_inflight_total_diff().unwrap(), 8);
        assert_eq!(
            decoded.java_to_json(false).unwrap(),
            body,
            "the MessageQueue-keyed Fastjson body must round-trip byte-for-byte"
        );
        assert_eq!(decoded.java_encode().unwrap(), body.as_bytes());
        assert!(ConsumeStats::java_decode_nullable(None).unwrap().is_none());

        let default = ConsumeStats::default();
        assert_eq!(default.get_consume_tps(), 0.0);
        assert!(
            default
                .get_offset_table()
                .unwrap()
                .with(|table| table.is_empty())
        );
        assert_eq!(
            default.java_to_json(false).unwrap(),
            dto_semantics["defaultJson"]
        );
        let missing: ConsumeStats = serde_json::from_str("{}").unwrap();
        assert_eq!(missing.get_consume_tps(), 0.0);
        assert!(
            missing
                .get_offset_table()
                .unwrap()
                .with(|table| table.is_empty())
        );

        let explicit_null: ConsumeStats =
            serde_json::from_str(r#"{"consumeTps":1.25,"offsetTable":null}"#).unwrap();
        assert!(explicit_null.get_offset_table().is_none());
        assert_eq!(
            explicit_null.compute_total_diff(),
            Err(ConsumeStatsComputationError::NullOffsetTable)
        );
        let mut null_default = ConsumeStats::default();
        null_default.set_offset_table(None);
        assert_eq!(
            null_default.java_to_json(false).unwrap(),
            dto_semantics["explicitNullJson"]
        );

        let queue = MessageQueue::new("orders", "broker-a", 1);
        let assigned_offset = SharedValue::new(OffsetWrapper::default());
        let assigned_table = SharedValue::new(std::collections::HashMap::from([(
            queue.clone(),
            Some(assigned_offset.clone()),
        )]));
        let mut assigned = ConsumeStats::default();
        assigned.set_offset_table(Some(assigned_table.clone()));
        assert!(assigned.get_offset_table().unwrap().ptr_eq(&assigned_table));
        let returned_offset = assigned
            .get_offset_table()
            .unwrap()
            .with(|table| table[&queue].as_ref().unwrap().clone());
        assert!(returned_offset.ptr_eq(&assigned_offset));
        returned_offset.with_mut(|offset| offset.set_broker_offset(9));
        assert_eq!(assigned_offset.with(|offset| offset.get_broker_offset()), 9);
        assert_eq!(assigned.compute_total_diff().unwrap(), 9);

        let null_offset_table = SharedValue::new(std::collections::HashMap::from([(queue, None)]));
        assigned.set_offset_table(Some(null_offset_table));
        assert_eq!(
            assigned.compute_inflight_total_diff(),
            Err(ConsumeStatsComputationError::NullOffsetWrapper)
        );
    }

    #[test]
    // The Java Set key is mutable, but this Rust key hashes and compares only
    // the stable SharedValue pointer identity, matching Object.equals/hashCode.
    #[allow(clippy::mutable_key_type)]
    fn consumer_connection_min_version_matches_invoked_java_api() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/admin/query-consume-time-span.json"
        ))
        .unwrap();
        let api = &fixture["getConsumerConnectionList"]["success"]["consumerConnectionApi"];
        let set_semantics = &api["connectionSetSemantics"];
        assert_eq!(
            api["connectionPublicConstructors"],
            json!(["public org.apache.rocketmq.remoting.protocol.body.Connection()"])
        );
        assert_eq!(
            api["connectionPublicMethods"],
            json!([
                "public int org.apache.rocketmq.remoting.protocol.body.Connection.getVersion()",
                "public java.lang.String org.apache.rocketmq.remoting.protocol.body.Connection.getClientAddr()",
                "public java.lang.String org.apache.rocketmq.remoting.protocol.body.Connection.getClientId()",
                "public org.apache.rocketmq.remoting.protocol.LanguageCode org.apache.rocketmq.remoting.protocol.body.Connection.getLanguage()",
                "public void org.apache.rocketmq.remoting.protocol.body.Connection.setClientAddr(java.lang.String)",
                "public void org.apache.rocketmq.remoting.protocol.body.Connection.setClientId(java.lang.String)",
                "public void org.apache.rocketmq.remoting.protocol.body.Connection.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode)",
                "public void org.apache.rocketmq.remoting.protocol.body.Connection.setVersion(int)"
            ])
        );
        assert_eq!(
            api["consumerConnectionPublicConstructors"],
            json!(["public org.apache.rocketmq.remoting.protocol.body.ConsumerConnection()"])
        );
        assert_eq!(
            api["consumerConnectionPublicMethods"],
            json!([
                "public int org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.computeMinVersion()",
                "public java.util.HashSet<org.apache.rocketmq.remoting.protocol.body.Connection> org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.getConnectionSet()",
                "public java.util.concurrent.ConcurrentMap<java.lang.String, org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData> org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.getSubscriptionTable()",
                "public org.apache.rocketmq.common.consumer.ConsumeFromWhere org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.getConsumeFromWhere()",
                "public org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.getConsumeType()",
                "public org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.getMessageModel()",
                "public void org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.setConnectionSet(java.util.HashSet<org.apache.rocketmq.remoting.protocol.body.Connection>)",
                "public void org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.setConsumeFromWhere(org.apache.rocketmq.common.consumer.ConsumeFromWhere)",
                "public void org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.setConsumeType(org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType)",
                "public void org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel)",
                "public void org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.setSubscriptionTable(java.util.concurrent.ConcurrentHashMap<java.lang.String, org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData>)"
            ])
        );

        let default_connection = Connection::default();
        assert!(api["connectionDefaults"]["clientIdNull"].as_bool().unwrap());
        assert!(
            api["connectionDefaults"]["clientAddrNull"]
                .as_bool()
                .unwrap()
        );
        assert!(api["connectionDefaults"]["languageNull"].as_bool().unwrap());
        assert_eq!(default_connection.get_client_id(), None);
        assert_eq!(default_connection.get_client_addr(), None);
        assert_eq!(default_connection.get_language(), None);
        assert_eq!(
            default_connection.get_version(),
            api["connectionDefaults"]["version"].as_i64().unwrap() as i32
        );

        assert_eq!(set_semantics["firstSameReferenceAdd"], true);
        assert_eq!(set_semantics["sameReferencePresentAfterMutation"], true);
        assert_eq!(set_semantics["secondSameReferenceAdd"], false);
        assert_eq!(set_semantics["differentReferenceAdd"], true);
        assert_eq!(set_semantics["differentReferencesCompareEqual"], false);
        assert_eq!(set_semantics["sizeBeforeNull"], 2);
        assert_eq!(set_semantics["firstNullAdd"], true);
        assert_eq!(set_semantics["secondNullAdd"], false);
        assert_eq!(set_semantics["sizeAfterNull"], 3);
        assert_eq!(set_semantics["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(set_semantics["decodedNullAndDistinctObjectsSize"], 3);

        let mut connection_set = ConnectionSet::new();
        let same_connection = SharedValue::new(Connection::default());
        assert!(connection_set.insert(Some(same_connection.clone())));
        same_connection.with_mut(|connection| connection.set_version(55));
        assert!(connection_set.contains(&Some(same_connection.clone())));
        assert!(!connection_set.insert(Some(same_connection.clone())));
        assert!(connection_set.insert(Some(SharedValue::new(Connection::default()))));
        assert!(connection_set.insert(None));
        assert!(!connection_set.insert(None));
        assert_eq!(connection_set.len(), 3);
        assert!(connection_set.contains(&None));
        assert!(connection_set.remove(&None));
        assert!(!connection_set.contains(&None));

        let decoded_duplicate_members = ConsumerConnection::java_decode(
            br#"{"connectionSet":[null,null,{"version":7},{"version":7}]}"#,
        )
        .unwrap();
        let decoded_connection_set = decoded_duplicate_members.get_connection_set().unwrap();
        assert_eq!(decoded_connection_set.with(HashSet::len), 3);
        assert_eq!(
            decoded_connection_set
                .with(|values| values.iter().filter(|value| value.is_none()).count()),
            1
        );
        assert_eq!(
            decoded_connection_set
                .with(|values| values.iter().filter(|value| value.is_some()).count()),
            2
        );

        let mut assigned_connection = Connection::default();
        assigned_connection.set_client_id(Some("client-api".to_owned()));
        assigned_connection.set_client_addr(Some("192.0.2.20:10911".to_owned()));
        assigned_connection.set_language(Some(LanguageCode::Rust));
        assigned_connection.set_version(55);
        assert_eq!(
            assigned_connection.get_client_id(),
            api["connectionAccessors"]["clientId"].as_str()
        );
        assert_eq!(
            assigned_connection.get_client_addr(),
            api["connectionAccessors"]["clientAddr"].as_str()
        );
        assert_eq!(assigned_connection.get_language(), Some(LanguageCode::Rust));
        assert_eq!(api["connectionAccessors"]["language"], "RUST");
        assert_eq!(
            assigned_connection.get_version(),
            api["connectionAccessors"]["version"].as_i64().unwrap() as i32
        );

        let default_consumer_connection = ConsumerConnection::default();
        let default_set = default_consumer_connection.get_connection_set().unwrap();
        let default_table = default_consumer_connection
            .get_subscription_table()
            .unwrap();
        assert!(
            api["consumerConnectionDefaults"]["connectionSetNonNull"]
                .as_bool()
                .unwrap()
        );
        assert!(
            api["consumerConnectionDefaults"]["connectionSetEmpty"]
                .as_bool()
                .unwrap()
        );
        assert!(
            api["consumerConnectionDefaults"]["subscriptionTableNonNull"]
                .as_bool()
                .unwrap()
        );
        assert!(
            api["consumerConnectionDefaults"]["subscriptionTableEmpty"]
                .as_bool()
                .unwrap()
        );
        assert!(default_set.snapshot().is_empty());
        assert!(default_table.snapshot().is_empty());
        assert!(default_consumer_connection.get_consume_type().is_none());
        assert!(default_consumer_connection.get_message_model().is_none());
        assert!(
            default_consumer_connection
                .get_consume_from_where()
                .is_none()
        );

        let assigned_connection_value = SharedValue::new(Connection::default());
        let assigned_connection_set =
            SharedValue::new(HashSet::from([Some(assigned_connection_value.clone())]));
        let mut assigned_consumer_connection = ConsumerConnection::default();
        assigned_consumer_connection.set_connection_set(Some(assigned_connection_set.clone()));
        let returned_connection_set = assigned_consumer_connection.get_connection_set().unwrap();
        assert!(
            api["connectionSetGetterReturnsAssignedSet"]
                .as_bool()
                .unwrap()
        );
        assert!(
            api["connectionSetGetterReturnsAssignedElement"]
                .as_bool()
                .unwrap()
        );
        assert!(returned_connection_set.ptr_eq(&assigned_connection_set));
        assert!(returned_connection_set.with(|values| {
            values
                .iter()
                .next()
                .unwrap()
                .as_ref()
                .is_some_and(|value| value.ptr_eq(&assigned_connection_value))
        }));
        returned_connection_set.with_mut(|values| {
            values.insert(Some(SharedValue::new(Connection::default())));
        });
        assert!(
            api["connectionSetGetterMutationVisibleToAssignedSet"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(assigned_connection_set.with(HashSet::len), 2);

        let assigned_subscription = SharedValue::new(SubscriptionData::default());
        let assigned_subscription_table = SharedValue::new(HashMap::from([(
            "orders".to_owned(),
            Some(assigned_subscription.clone()),
        )]));
        assigned_consumer_connection
            .set_subscription_table(Some(assigned_subscription_table.clone()));
        let returned_subscription_table = assigned_consumer_connection
            .get_subscription_table()
            .unwrap();
        assert!(
            api["subscriptionTableGetterReturnsAssignedMap"]
                .as_bool()
                .unwrap()
        );
        assert!(
            api["subscriptionTableGetterReturnsAssignedValue"]
                .as_bool()
                .unwrap()
        );
        assert!(returned_subscription_table.ptr_eq(&assigned_subscription_table));
        assert!(returned_subscription_table.with(|values| {
            values["orders"]
                .as_ref()
                .is_some_and(|value| value.ptr_eq(&assigned_subscription))
        }));
        returned_subscription_table.with_mut(|values| {
            values.insert(
                "billing".to_owned(),
                Some(SharedValue::new(SubscriptionData::default())),
            );
        });
        assert!(
            api["subscriptionTableGetterMutationVisibleToAssignedMap"]
                .as_bool()
                .unwrap()
        );
        assert!(assigned_subscription_table.with(|values| values.contains_key("billing")));

        assigned_consumer_connection.set_consume_type(Some(ConsumeType::ConsumePassively));
        assigned_consumer_connection.set_message_model(Some(MessageModel::Broadcasting));
        assigned_consumer_connection
            .set_consume_from_where(Some(ConsumeFromWhere::ConsumeFromFirstOffset));
        assert_eq!(
            assigned_consumer_connection.get_consume_type(),
            Some(ConsumeType::ConsumePassively)
        );
        assert_eq!(
            api["consumerConnectionAccessors"]["consumeType"],
            "CONSUME_PASSIVELY"
        );
        assert_eq!(
            assigned_consumer_connection.get_message_model(),
            Some(MessageModel::Broadcasting)
        );
        assert_eq!(
            api["consumerConnectionAccessors"]["messageModel"],
            "BROADCASTING"
        );
        assert_eq!(
            assigned_consumer_connection.get_consume_from_where(),
            Some(ConsumeFromWhere::ConsumeFromFirstOffset)
        );
        assert_eq!(
            api["consumerConnectionAccessors"]["consumeFromWhere"],
            "CONSUME_FROM_FIRST_OFFSET"
        );
        assigned_consumer_connection.set_subscription_table(None);
        assert!(
            api["subscriptionTableSetterPreservesNull"]
                .as_bool()
                .unwrap()
        );
        assert!(
            assigned_consumer_connection
                .get_subscription_table()
                .is_none()
        );

        assert_eq!(
            api["computeMinVersionMethod"],
            "public int org.apache.rocketmq.remoting.protocol.body.ConsumerConnection.computeMinVersion()"
        );

        let connection = |version| {
            let mut connection = Connection::default();
            connection.set_version(version);
            Some(SharedValue::new(connection))
        };

        let default = ConsumerConnection::default();
        assert_eq!(
            default.compute_min_version().unwrap(),
            api["defaultEmpty"]["value"].as_i64().unwrap() as i32
        );

        let mut explicit_empty = ConsumerConnection::default();
        explicit_empty.set_connection_set(Some(SharedValue::new(HashSet::new())));
        assert_eq!(
            explicit_empty.compute_min_version().unwrap(),
            api["explicitEmpty"]["value"].as_i64().unwrap() as i32
        );

        let mut default_connection = ConsumerConnection::default();
        default_connection
            .set_connection_set(Some(SharedValue::new(HashSet::from([connection(0)]))));
        assert_eq!(
            default_connection.compute_min_version().unwrap(),
            api["defaultConnection"]["value"].as_i64().unwrap() as i32
        );

        let mut mixed_versions = ConsumerConnection::default();
        mixed_versions.set_connection_set(Some(SharedValue::new(HashSet::from([
            connection(9),
            connection(-3),
            connection(4),
        ]))));
        assert_eq!(
            mixed_versions.compute_min_version().unwrap(),
            api["mixedVersions"]["value"].as_i64().unwrap() as i32
        );

        let mut integer_bounds = ConsumerConnection::default();
        integer_bounds.set_connection_set(Some(SharedValue::new(HashSet::from([
            connection(i32::MAX),
            connection(i32::MIN),
        ]))));
        assert_eq!(
            integer_bounds.compute_min_version().unwrap(),
            api["integerBounds"]["value"].as_i64().unwrap() as i32
        );

        let mut null_set = ConsumerConnection::default();
        null_set.set_connection_set(None);
        assert_eq!(
            api["nullSet"]["exceptionClass"],
            "java.lang.NullPointerException"
        );
        assert_eq!(
            null_set.compute_min_version(),
            Err(ConsumerConnectionComputeMinVersionError::NullConnectionSet)
        );

        let mut null_element = ConsumerConnection::default();
        null_element.set_connection_set(Some(SharedValue::new(HashSet::from([None]))));
        assert_eq!(
            api["nullElement"]["exceptionClass"],
            "java.lang.NullPointerException"
        );
        assert_eq!(
            null_element.compute_min_version(),
            Err(ConsumerConnectionComputeMinVersionError::NullConnection)
        );
    }

    #[test]
    fn producer_diagnostic_dtos_match_java_api_and_response_bodies() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert_eq!(
            fixture["producerDiagnosticsApi"],
            json!([
                "org.apache.rocketmq.remoting.protocol.body.KVTable org.apache.rocketmq.client.impl.MQClientAPIImpl.getBrokerRuntimeInfo(java.lang.String,long)",
                "org.apache.rocketmq.remoting.protocol.body.ProducerConnection org.apache.rocketmq.client.impl.MQClientAPIImpl.getProducerConnectionList(java.lang.String,java.lang.String,long)",
                "org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo org.apache.rocketmq.client.impl.MQClientAPIImpl.getAllProducerInfo(java.lang.String,long)"
            ])
        );

        let connection_api = &fixture["producerConnectionDtoApi"];
        assert_eq!(
            connection_api["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.ProducerConnection()"])
        );
        assert_eq!(
            connection_api["declaredPublicMethods"],
            json!([
                "java.util.HashSet org.apache.rocketmq.remoting.protocol.body.ProducerConnection.getConnectionSet()",
                "void org.apache.rocketmq.remoting.protocol.body.ProducerConnection.setConnectionSet(java.util.HashSet)"
            ])
        );
        let producer_info_api = &fixture["producerInfoDtoApi"];
        assert_eq!(
            producer_info_api["publicConstructors"],
            json!([
                "org.apache.rocketmq.remoting.protocol.body.ProducerInfo(java.lang.String,java.lang.String,org.apache.rocketmq.remoting.protocol.LanguageCode,int,long)"
            ])
        );
        assert_eq!(
            producer_info_api["declaredPublicMethods"],
            json!([
                "int org.apache.rocketmq.remoting.protocol.body.ProducerInfo.getVersion()",
                "java.lang.String org.apache.rocketmq.remoting.protocol.body.ProducerInfo.getClientId()",
                "java.lang.String org.apache.rocketmq.remoting.protocol.body.ProducerInfo.getRemoteIP()",
                "java.lang.String org.apache.rocketmq.remoting.protocol.body.ProducerInfo.toString()",
                "long org.apache.rocketmq.remoting.protocol.body.ProducerInfo.getLastUpdateTimestamp()",
                "org.apache.rocketmq.remoting.protocol.LanguageCode org.apache.rocketmq.remoting.protocol.body.ProducerInfo.getLanguage()",
                "void org.apache.rocketmq.remoting.protocol.body.ProducerInfo.setClientId(java.lang.String)",
                "void org.apache.rocketmq.remoting.protocol.body.ProducerInfo.setLanguage(org.apache.rocketmq.remoting.protocol.LanguageCode)",
                "void org.apache.rocketmq.remoting.protocol.body.ProducerInfo.setLastUpdateTimestamp(long)",
                "void org.apache.rocketmq.remoting.protocol.body.ProducerInfo.setRemoteIP(java.lang.String)",
                "void org.apache.rocketmq.remoting.protocol.body.ProducerInfo.setVersion(int)"
            ])
        );
        assert_eq!(
            fixture["producerTableInfoDtoApi"]["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo(java.util.Map)"])
        );
        assert_eq!(
            fixture["producerTableInfoDtoApi"]["declaredPublicMethods"],
            json!([
                "java.util.Map org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo.getData()",
                "void org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo.setData(java.util.Map)"
            ])
        );
        assert_eq!(
            fixture["kvTableDtoApi"]["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.KVTable()"])
        );
        assert_eq!(
            fixture["kvTableDtoApi"]["declaredPublicMethods"],
            json!([
                "java.util.HashMap org.apache.rocketmq.remoting.protocol.body.KVTable.getTable()",
                "void org.apache.rocketmq.remoting.protocol.body.KVTable.setTable(java.util.HashMap)"
            ])
        );

        let operations = &fixture["producerDiagnosticsOperations"];
        let producer_set_semantics = &operations["producerConnectionSetSemantics"];
        assert_eq!(producer_set_semantics["dtoGetterReturnsAssignedSet"], true);
        assert_eq!(
            producer_set_semantics["dtoGetterMutationVisibleInAssignedSet"],
            true
        );
        assert_eq!(producer_set_semantics["firstSameReferenceAdd"], true);
        assert_eq!(producer_set_semantics["secondSameReferenceAdd"], false);
        assert_eq!(producer_set_semantics["differentReferenceAdd"], true);
        assert_eq!(producer_set_semantics["sizeBeforeNull"], 2);
        assert_eq!(producer_set_semantics["firstNullAdd"], true);
        assert_eq!(producer_set_semantics["secondNullAdd"], false);
        assert_eq!(producer_set_semantics["sizeAfterNull"], 3);
        let decoded_producer_connections = ProducerConnection::java_decode(
            br#"{"connectionSet":[null,null,{"version":7},{"version":7}]}"#,
        )
        .unwrap();
        let decoded_producer_set = decoded_producer_connections.get_connection_set().unwrap();
        assert_eq!(decoded_producer_set.with(HashSet::len), 3);
        assert_eq!(
            decoded_producer_set
                .with(|values| values.iter().filter(|value| value.is_none()).count()),
            1
        );
        assert_eq!(
            decoded_producer_set
                .with(|values| values.iter().filter(|value| value.is_some()).count()),
            2
        );
        let assigned_connection = SharedValue::new(Connection::default());
        let assigned_connection_set =
            SharedValue::new(HashSet::from([Some(assigned_connection.clone())]));
        let mut assigned_producer_connection = ProducerConnection::default();
        assigned_producer_connection.set_connection_set(Some(assigned_connection_set.clone()));
        let returned_connection_set = assigned_producer_connection.get_connection_set().unwrap();
        assert!(returned_connection_set.ptr_eq(&assigned_connection_set));
        let added_connection = SharedValue::new(Connection::default());
        assert!(
            returned_connection_set
                .with_mut(|values| values.insert(Some(added_connection.clone())))
        );
        assert!(
            assigned_connection_set.with(|values| { values.contains(&Some(added_connection)) })
        );
        let default_connection = ProducerConnection::default();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &default_connection.java_to_json(false).unwrap()
            )
            .unwrap(),
            serde_json::from_str::<serde_json::Value>(
                operations["producerConnectionDefaultJson"]
                    .as_str()
                    .unwrap()
            )
            .unwrap()
        );
        let default_table = KvTable::default();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&default_table.java_to_json(false).unwrap())
                .unwrap(),
            serde_json::from_str::<serde_json::Value>(
                operations["kvTableDefaultJson"].as_str().unwrap()
            )
            .unwrap()
        );
        let null_semantics = &operations["dtoNullSemantics"];
        assert!(
            null_semantics["producerConnectionSetIsNull"]
                .as_bool()
                .unwrap()
        );
        let mut null_connections = ProducerConnection::default();
        null_connections.set_connection_set(None);
        assert!(null_connections.get_connection_set().is_none());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &null_connections.java_to_json(false).unwrap()
            )
            .unwrap(),
            serde_json::from_str::<serde_json::Value>(
                null_semantics["producerConnectionJson"].as_str().unwrap()
            )
            .unwrap()
        );
        assert!(
            serde_json::from_str::<ProducerConnection>("{}")
                .unwrap()
                .get_connection_set()
                .unwrap()
                .with(|connections| connections.is_empty())
        );
        assert!(
            null_semantics["producerTableInfoDataIsNull"]
                .as_bool()
                .unwrap()
        );
        let null_producer_table: ProducerTableInfo =
            serde_json::from_str(null_semantics["producerTableInfoJson"].as_str().unwrap())
                .unwrap();
        assert!(null_producer_table.get_data().is_none());
        assert!(null_semantics["kvTableIsNull"].as_bool().unwrap());
        let mut null_table = KvTable::default();
        null_table.set_table(None);
        assert!(null_table.get_table().is_none());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&null_table.java_to_json(false).unwrap())
                .unwrap(),
            serde_json::from_str::<serde_json::Value>(
                null_semantics["kvTableJson"].as_str().unwrap()
            )
            .unwrap()
        );
        assert!(
            serde_json::from_str::<KvTable>("{}")
                .unwrap()
                .get_table()
                .unwrap()
                .with(|table| table.is_empty())
        );
        let connection_case = &operations["getProducerConnectionList"]["cases"][0];
        let connection_body = connection_case["responseBody"].as_str().unwrap().as_bytes();
        let producer_connection = ProducerConnection::java_decode(connection_body).unwrap();
        assert_eq!(
            producer_connection
                .get_connection_set()
                .unwrap()
                .with(HashSet::len),
            1
        );
        let connection_set = producer_connection.get_connection_set().unwrap();
        let connection =
            connection_set.with(|values| values.iter().next().unwrap().as_ref().unwrap().clone());
        assert_eq!(
            connection.with(|value| value.get_client_id().map(str::to_owned)),
            Some("producer-client".to_owned())
        );
        assert_eq!(
            connection.with(|value| value.get_client_addr().map(str::to_owned)),
            Some("192.0.2.9:1234".to_owned())
        );
        assert_eq!(
            connection.with(|value| value.get_language()),
            Some(LanguageCode::Java)
        );
        assert_eq!(connection.with(|value| value.get_version()), 1);
        assert!(
            default_connection
                .get_connection_set()
                .unwrap()
                .with(|values| values.is_empty())
        );
        assert!(
            serde_json::from_str::<ProducerConnection>(r#"{"connectionSet":null}"#)
                .unwrap()
                .get_connection_set()
                .is_none()
        );

        let producer_table_case = &operations["getAllProducerInfo"]["cases"][0];
        let producer_table = ProducerTableInfo::java_decode(
            producer_table_case["responseBody"]
                .as_str()
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
        let data = producer_table.get_data().unwrap();
        data.with(|data| {
            let producer = data["workers"].as_ref().unwrap()[0].as_ref().unwrap();
            assert_eq!(producer.get_client_id(), Some("producer-client"));
            assert_eq!(producer.get_remote_ip(), Some("192.0.2.9"));
            assert_eq!(producer.get_language(), Some(LanguageCode::Java));
            assert_eq!(producer.get_version(), 1);
            assert_eq!(producer.get_last_update_timestamp(), 1_700_000_000_123);
            assert_eq!(
                producer.java_to_string().unwrap(),
                operations["producerInfoToString"]["value"]
            );
        });
        assert_eq!(
            operations["producerInfoToString"]["nullLanguageOutcome"],
            "NullPointerException"
        );
        assert_eq!(
            ProducerInfo::new(
                Some("producer-client".into()),
                Some("192.0.2.9".into()),
                None,
                1,
                0,
            )
            .java_to_string(),
            Err(ProducerInfoToStringError::NullLanguage)
        );

        let runtime_case = &operations["getBrokerRuntimeInfo"]["cases"][0];
        let runtime =
            KvTable::java_decode(runtime_case["responseBody"].as_str().unwrap().as_bytes())
                .unwrap();
        let table = runtime.get_table().unwrap();
        assert_eq!(
            table.with(|table| table["brokerName"].clone()),
            Some("broker-a".to_owned())
        );
        assert_eq!(
            table.with(|table| table["brokerVersion"].clone()),
            Some("5.5.1".to_owned())
        );
        assert!(
            default_table
                .get_table()
                .unwrap()
                .with(|table| table.is_empty())
        );

        let assigned_connection = SharedValue::new(Connection::default());
        let assigned_connections =
            SharedValue::new(HashSet::from([Some(assigned_connection.clone())]));
        let mut connection_owner = ProducerConnection::default();
        connection_owner.set_connection_set(Some(assigned_connections.clone()));
        let returned_connections = connection_owner.get_connection_set().unwrap();
        assert!(returned_connections.ptr_eq(&assigned_connections));
        assert!(
            !returned_connections
                .with_mut(|values| { values.insert(Some(assigned_connection.clone())) })
        );
        returned_connections.with_mut(|values| {
            values
                .iter()
                .next()
                .unwrap()
                .as_ref()
                .unwrap()
                .with_mut(|value| value.set_client_id(Some("updated-client".into())))
        });
        assert_eq!(
            assigned_connection.with(|value| value.get_client_id().map(str::to_owned)),
            Some("updated-client".to_owned())
        );

        let assigned_table = SharedValue::new(HashMap::from([(
            "brokerName".to_owned(),
            Some("broker-a".to_owned()),
        )]));
        let mut table_owner = KvTable::default();
        table_owner.set_table(Some(assigned_table.clone()));
        assert!(table_owner.get_table().unwrap().ptr_eq(&assigned_table));

        let assigned_data = SharedValue::new(HashMap::from([(
            "workers".to_owned(),
            Some(vec![Some(ProducerInfo::new(
                Some("producer-client".into()),
                Some("192.0.2.9".into()),
                Some(LanguageCode::Java),
                1,
                1_700_000_000_123,
            ))]),
        )]));
        let mut producer_owner = ProducerTableInfo::new(Some(assigned_data.clone()));
        assert!(producer_owner.get_data().unwrap().ptr_eq(&assigned_data));
        producer_owner.set_data(Some(SharedValue::new(HashMap::from([(
            "workers".to_owned(),
            Some(vec![Some(ProducerInfo::new(
                Some("updated-client".into()),
                Some("192.0.2.10".into()),
                Some(LanguageCode::Java),
                2,
                2_000,
            ))]),
        )]))));
        assert!(!producer_owner.get_data().unwrap().ptr_eq(&assigned_data));
    }

    #[test]
    fn topic_list_matches_java_default_null_and_alias_semantics() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/admin/topic-list.json")).unwrap();
        assert_eq!(
            fixture["artifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.TopicList()"])
        );
        assert_eq!(fixture["publicFields"], json!([]));
        assert_eq!(
            fixture["publicMethods"],
            json!([
                "java.lang.String getBrokerAddr()",
                "java.util.Set getTopicList()",
                "void setBrokerAddr(java.lang.String)",
                "void setTopicList(java.util.Set)"
            ])
        );

        let default = TopicList::default();
        assert!(default.get_broker_addr().is_none());
        assert!(default.get_topic_list().unwrap().snapshot().is_empty());
        assert_eq!(
            serde_json::to_value(&default).unwrap(),
            serde_json::from_str::<serde_json::Value>(fixture["defaultJson"].as_str().unwrap())
                .unwrap()
        );

        let missing: TopicList = serde_json::from_str("{}").unwrap();
        assert!(missing.get_topic_list().unwrap().snapshot().is_empty());
        let explicit_null: TopicList = serde_json::from_str(r#"{"topicList":null}"#).unwrap();
        assert!(explicit_null.get_topic_list().is_none());
        assert_eq!(
            serde_json::to_value(&explicit_null).unwrap(),
            serde_json::from_str::<serde_json::Value>(
                fixture["explicitNullJson"].as_str().unwrap()
            )
            .unwrap()
        );

        let decoded: TopicList = serde_json::from_str(
            r#"{"topicList":["orders","orders","billing"],"brokerAddr":"broker-a"}"#,
        )
        .unwrap();
        assert_eq!(decoded.get_broker_addr(), Some("broker-a"));
        let java_populated: serde_json::Value =
            serde_json::from_str(fixture["populatedJson"].as_str().unwrap()).unwrap();
        let java_topics = java_populated["topicList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|topic| topic.as_str().unwrap().to_owned())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(decoded.get_topic_list().unwrap().snapshot(), java_topics);

        let assigned = SharedValue::new(std::collections::HashSet::from(["orders".to_owned()]));
        let mut value = TopicList::default();
        value.set_topic_list(Some(assigned.clone()));
        value.set_broker_addr(Some("broker-a".to_owned()));
        assert_eq!(
            value.get_broker_addr(),
            Some(fixture["brokerAddrGetter"].as_str().unwrap())
        );
        let getter = value.get_topic_list().unwrap();
        assert_eq!(
            assigned.ptr_eq(&getter),
            fixture["getterReturnsStoredSetIdentity"]
        );
        getter.with_mut(|topics| {
            topics.insert("billing".to_owned());
        });
        assert_eq!(assigned.snapshot().len(), 2);
        assert_eq!(
            assigned.snapshot().contains("billing"),
            fixture["getterMutationVisibleToAssignedSet"]
        );
    }

    #[allow(clippy::mutable_key_type)]
    fn message_queue_set_element_aliases_match(
        case: &serde_json::Value,
        assigned_set: &SharedMessageQueueSet,
        getter_set: &SharedMessageQueueSet,
    ) {
        let assigned_queue = assigned_set.with(|queues| {
            queues
                .iter()
                .find_map(Clone::clone)
                .expect("fixture Set contains a non-null MessageQueue")
        });
        let getter_queue = getter_set.with(|queues| {
            queues
                .iter()
                .find_map(Clone::clone)
                .expect("fixture Set contains a non-null MessageQueue")
        });
        assert_eq!(
            assigned_queue.ptr_eq(&getter_queue),
            case["getterReturnsAssignedElement"]
        );

        let original_queue_id = assigned_queue.with(MessageQueue::queue_id);
        assigned_queue.with_mut(|queue| queue.set_queue_id(original_queue_id + 5));
        assert_eq!(
            getter_set.with(|queues| {
                queues.iter().find_map(|queue| {
                    queue
                        .as_ref()
                        .map(|queue| queue.with(MessageQueue::queue_id))
                }) == Some(original_queue_id + 5)
            }),
            case["setterElementMutationVisibleToGetter"]
        );
        assigned_queue.with_mut(|queue| queue.set_queue_id(original_queue_id));

        getter_queue.with_mut(|queue| queue.set_queue_id(original_queue_id + 6));
        assert_eq!(
            assigned_queue.with(|queue| queue.queue_id() == original_queue_id + 6),
            case["getterElementMutationVisibleToSetterObject"]
        );
        getter_queue.with_mut(|queue| queue.set_queue_id(original_queue_id));
    }

    fn normalize_batch_body_value(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(values) => {
                for value in values.iter_mut() {
                    normalize_batch_body_value(value);
                }
                values.sort_by_key(|value| serde_json::to_string(value).unwrap());
            }
            serde_json::Value::Object(fields) => {
                for value in fields.values_mut() {
                    normalize_batch_body_value(value);
                }
            }
            _ => {}
        }
    }

    fn batch_body_cases_match_java<T>(cases: &[serde_json::Value], expected_len: usize)
    where
        T: serde::de::DeserializeOwned + Serialize + std::fmt::Debug,
    {
        assert_eq!(cases.len(), expected_len);
        let mut mismatches = Vec::new();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let decoded = serde_json::from_str::<Option<T>>(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "ACCEPTED";
            match (decoded, java_accepts) {
                (Ok(decoded), true) => {
                    let mut actual = serde_json::to_value(decoded).unwrap();
                    let mut expected = case["encoded"].clone();
                    normalize_batch_body_value(&mut actual);
                    normalize_batch_body_value(&mut expected);
                    if actual != expected {
                        mismatches.push(format!("{name}: encoded {actual} != Java {expected}"));
                    }
                }
                (Ok(_), false) => mismatches.push(format!("{name}: Rust accepted, Java rejected")),
                (Err(error), true) => {
                    mismatches.push(format!("{name}: Rust rejected ({error}), Java accepted"))
                }
                (Err(_), false) => {}
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn lock_batch_request_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        batch_body_cases_match_java::<LockBatchRequestBody>(
            fixture["lockBatchRequestBodyCases"].as_array().unwrap(),
            46,
        );
    }

    #[test]
    fn unlock_batch_request_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        batch_body_cases_match_java::<UnlockBatchRequestBody>(
            fixture["unlockBatchRequestBodyCases"].as_array().unwrap(),
            46,
        );
    }

    #[test]
    fn lock_batch_response_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        batch_body_cases_match_java::<LockBatchResponseBody>(
            fixture["lockBatchResponseBodyCases"].as_array().unwrap(),
            25,
        );
    }

    #[test]
    fn batch_ack_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["batchAckBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 111);
        for case in cases {
            let decoded = BatchAckMessageRequestBody::java_decode_nullable(Some(
                case["rawInput"].as_str().unwrap().as_bytes(),
            ));
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if case["name"] == "duplicate-null-queue-assignments" {
                assert_eq!(case["error"], "NullPointerException");
                assert!(
                    decoded
                        .as_ref()
                        .unwrap_err()
                        .to_string()
                        .contains("NullPointerException")
                );
            }
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn batch_ack_public_methods_match_invoked_java() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let case = &fixture["batchAckApiCase"];

        let mut ack = BatchAck::default();
        assert_eq!(
            json!({
                "consumerGroup": ack.get_consumer_group(),
                "topic": ack.get_topic(),
                "retry": ack.get_retry(),
                "startOffset": ack.get_start_offset(),
                "queueId": ack.get_queue_id(),
                "reviveQueueId": ack.get_revive_queue_id(),
                "popTime": ack.get_pop_time(),
                "invisibleTime": ack.get_invisible_time(),
                "bitSet": ack
                    .get_bit_set()
                    .map(|bit_set| bit_set.with(BitSet::to_byte_array))
                    .transpose()
                    .unwrap(),
            }),
            case["defaults"]
        );

        ack.set_consumer_group(Some("workers".into()));
        ack.set_topic(Some("orders".into()));
        ack.set_retry(Some("0".into()));
        ack.set_start_offset(100);
        ack.set_queue_id(1);
        ack.set_revive_queue_id(2);
        ack.set_pop_time(10);
        ack.set_invisible_time(20);
        let assigned_bits = SharedValue::new(BitSet::from_byte_array(&[0b0000_0101, 0, 0]));
        ack.set_bit_set(Some(assigned_bits.clone()));
        let bit_set_base64 = serde_json::to_value(&ack).unwrap()["b"].clone();
        let bits = ack.get_bit_set().unwrap();
        assert!(bits.ptr_eq(&assigned_bits));
        assert_eq!(
            json!({
                "consumerGroup": ack.get_consumer_group(),
                "topic": ack.get_topic(),
                "retry": ack.get_retry(),
                "startOffset": ack.get_start_offset(),
                "queueId": ack.get_queue_id(),
                "reviveQueueId": ack.get_revive_queue_id(),
                "popTime": ack.get_pop_time(),
                "invisibleTime": ack.get_invisible_time(),
                "bitSetBase64": bit_set_base64,
                "bitSetCardinality": bits.with(BitSet::cardinality),
                "bitSetLength": bits.with(BitSet::length),
                "bitSetQueries": bits.with(|bits| [
                    bits.get(0).unwrap(), bits.get(1).unwrap(), bits.get(2).unwrap(), bits.get(5).unwrap(),
                ]),
            }),
            case["values"]
        );
        assert_eq!(ack.to_string(), case["toString"].as_str().unwrap());
        assert_eq!(case["bitSetGetterReturnsAssignedObject"], true);
        assert_eq!(case["bitSetMutationVisibleThroughGetter"], true);
        assert_eq!(case["bitSetMutationVisibleThroughSetterObject"], true);
        bits.with_mut(|bits| bits.set(5).unwrap());
        assert!(assigned_bits.with(|bits| bits.get(5).unwrap()));
        assert_eq!(
            serde_json::to_value(&ack).unwrap()["b"],
            case["mutatedBitSetBase64"]
        );
        let mut cloned_bits = ack.get_bit_set().unwrap().snapshot();
        cloned_bits.clear(0).unwrap();
        cloned_bits.set(7).unwrap();
        let mut cloned_ack = BatchAck::default();
        cloned_ack.set_bit_set(Some(SharedValue::new(cloned_bits)));
        assert_eq!(
            json!({
                "cloneIsDistinctObject": true,
                "sourceBase64": serde_json::to_value(&ack).unwrap()["b"],
                "cloneBase64": serde_json::to_value(&cloned_ack).unwrap()["b"],
                "sourceRetainsClearedBit": bits.with(|bits| bits.get(0).unwrap()),
                "sourceExcludesCloneBit": !bits.with(|bits| bits.get(7).unwrap()),
            }),
            case["bitSetClone"]
        );
        let preallocated_source = BitSet::try_with_capacity_bits(130).unwrap();
        let mut preallocated_clone = preallocated_source.clone();
        preallocated_clone.set(7).unwrap();
        assert_eq!(
            json!({
                "cloneIsDistinctObject": !std::ptr::eq(&preallocated_source, &preallocated_clone),
                "sourceSize": preallocated_source.size(),
                "cloneSize": preallocated_clone.size(),
                "sourceBase64": STANDARD.encode(preallocated_source.to_byte_array().unwrap()),
                "cloneBase64": STANDARD.encode(preallocated_clone.to_byte_array().unwrap()),
                "sourceExcludesCloneBit": !preallocated_source.get(7).unwrap(),
            }),
            case["bitSetPreallocatedClone"]
        );
        let mut operations = bits.snapshot();
        operations.clear(2).unwrap();
        operations.flip(7).unwrap();
        let mut operations_ack = BatchAck::default();
        operations_ack.set_bit_set(Some(SharedValue::new(operations.clone())));
        assert_eq!(
            json!({
                "base64": serde_json::to_value(operations_ack).unwrap()["b"],
                "cardinality": operations.cardinality(),
                "length": operations.length(),
                "isEmpty": operations.is_empty(),
                "get": [operations.get(0).unwrap(), operations.get(2).unwrap(), operations.get(5).unwrap(), operations.get(7).unwrap()],
            }),
            case["bitSetOperations"]
        );
        assert_eq!(
            json!({
                "get": "IndexOutOfBoundsException",
                "set": "IndexOutOfBoundsException",
                "clear": "IndexOutOfBoundsException",
                "flip": "IndexOutOfBoundsException",
            }),
            case["bitSetNegativeIndexErrors"]
        );
        assert_eq!(
            bits.with(|bits| bits.get(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            bits.with_mut(|bits| bits.set(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            bits.with_mut(|bits| bits.clear(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            bits.with_mut(|bits| bits.flip(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        let mut word_boundary_bits = BitSet::new();
        for index in [7, 8, 63, 64, 127] {
            word_boundary_bits.set(index).unwrap();
        }
        assert_eq!(word_boundary_bits.cardinality(), 5);
        assert_eq!(word_boundary_bits.length(), 128);
        let mut word_boundary_ack = BatchAck::default();
        word_boundary_ack.set_bit_set(Some(SharedValue::new(word_boundary_bits)));
        assert_eq!(
            json!({
                "base64": serde_json::to_value(word_boundary_ack).unwrap()["b"],
                "cardinality": 5,
                "length": 128,
            }),
            case["bitSetWordBoundary"]
        );

        let bit_set_base64 = |bits: &BitSet| {
            let mut encoded_ack = BatchAck::default();
            encoded_ack.set_bit_set(Some(SharedValue::new(bits.clone())));
            serde_json::to_value(encoded_ack).unwrap()["b"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let mut ranged = BitSet::new();
        ranged.set_range(3, 12).unwrap();
        ranged.clear_range(5, 7).unwrap();
        ranged.flip_range(8, 14).unwrap();
        let slice = ranged.get_range(2, 14).unwrap();
        let mut intersects = BitSet::new();
        intersects.set(4).unwrap();
        let mut disjoint = BitSet::new();
        disjoint.set(30).unwrap();
        assert_eq!(
            json!({
                "base64": bit_set_base64(&ranged),
                "cardinality": ranged.cardinality(),
                "length": ranged.length(),
                "sliceBase64": bit_set_base64(&slice),
                "nextSetBit": [
                    ranged.next_set_bit(0).unwrap(),
                    ranged.next_set_bit(5).unwrap(),
                    ranged.next_set_bit(8).unwrap(),
                    ranged.next_set_bit(14).unwrap(),
                ],
                "nextClearBit": [
                    ranged.next_clear_bit(3).unwrap(),
                    ranged.next_clear_bit(8).unwrap(),
                    ranged.next_clear_bit(14).unwrap(),
                ],
                "previousSetBit": [
                    ranged.previous_set_bit(13).unwrap(),
                    ranged.previous_set_bit(11).unwrap(),
                    ranged.previous_set_bit(2).unwrap(),
                    ranged.previous_set_bit(-1).unwrap(),
                ],
                "previousClearBit": [
                    ranged.previous_clear_bit(13).unwrap(),
                    ranged.previous_clear_bit(3).unwrap(),
                    ranged.previous_clear_bit(-1).unwrap(),
                ],
                "intersects": ranged.intersects(&intersects),
                "disjoint": ranged.intersects(&disjoint),
            }),
            case["bitSetRanges"]
        );
        assert_eq!(ranged.next_set_bit(-1), Err(BitSetError::NegativeIndex(-1)));
        assert_eq!(
            ranged.next_clear_bit(-1),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            ranged.previous_set_bit(-2),
            Err(BitSetError::NegativeIndex(-2))
        );
        assert_eq!(
            ranged.previous_clear_bit(-2),
            Err(BitSetError::NegativeIndex(-2))
        );
        assert_eq!(ranged.set_range(-1, 2), Err(BitSetError::NegativeIndex(-1)));
        assert_eq!(
            ranged.set_range(3, 2),
            Err(BitSetError::InvalidRange { from: 3, to: 2 })
        );
        assert_eq!(
            ranged.clear_range(-1, 2),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            ranged.flip_range(3, 2),
            Err(BitSetError::InvalidRange { from: 3, to: 2 })
        );
        assert_eq!(ranged.get_range(-1, 2), Err(BitSetError::NegativeIndex(-1)));
        assert_eq!(
            ranged.get_range(3, 2),
            Err(BitSetError::InvalidRange { from: 3, to: 2 })
        );
        assert!(
            case["bitSetRangeErrors"]
                .as_object()
                .unwrap()
                .values()
                .all(|error| error == "IndexOutOfBoundsException")
        );

        let mut range_word_boundary = BitSet::new();
        range_word_boundary.set_range(63, 66).unwrap();
        range_word_boundary.clear_range(64, 65).unwrap();
        range_word_boundary.flip_range(62, 64).unwrap();
        let range_word_boundary_slice = range_word_boundary.get_range(61, 66).unwrap();
        assert_eq!(
            json!({
                "base64": bit_set_base64(&range_word_boundary),
                "sliceBase64": bit_set_base64(&range_word_boundary_slice),
                "cardinality": range_word_boundary.cardinality(),
                "length": range_word_boundary.length(),
            }),
            case["bitSetRangeWordBoundary"]
        );

        let mut boolean_operations = BitSet::new();
        boolean_operations.set_range(2, 6).unwrap();
        boolean_operations.set_with_value(4, false).unwrap();
        boolean_operations.set_with_value(8, true).unwrap();
        boolean_operations.set_range_with_value(1, 3, true).unwrap();
        boolean_operations
            .set_range_with_value(2, 5, false)
            .unwrap();
        let mut cleared = boolean_operations.clone();
        cleared.clear_all();
        assert_eq!(
            json!({
                "base64": bit_set_base64(&boolean_operations),
                "longs": boolean_operations.to_long_array().unwrap(),
                "cardinality": boolean_operations.cardinality(),
                "length": boolean_operations.length(),
                "hashCode": boolean_operations.java_hash_code(),
                "toString": boolean_operations.java_to_string(),
                "stream": boolean_operations.iter_set_bits().collect::<Vec<_>>(),
                "clear": cleared.is_empty() && cleared.cardinality() == 0 && cleared.length() == 0,
            }),
            case["bitSetBooleanOperations"]
        );

        let mut bulk_left = BitSet::new();
        for index in [0, 2, 5, 64, 127] {
            bulk_left.set(index).unwrap();
        }
        let mut bulk_right = BitSet::new();
        for index in [1, 2, 5, 64, 126] {
            bulk_right.set(index).unwrap();
        }
        let mut and_result = bulk_left.clone();
        and_result.and(&bulk_right);
        let mut or_result = bulk_left.clone();
        or_result.or(&bulk_right).unwrap();
        let mut xor_result = bulk_left.clone();
        xor_result.xor(&bulk_right).unwrap();
        let mut and_not_result = bulk_left.clone();
        and_not_result.and_not(&bulk_right);
        assert_eq!(
            json!({
                "and": bit_set_base64(&and_result),
                "or": bit_set_base64(&or_result),
                "xor": bit_set_base64(&xor_result),
                "andNot": bit_set_base64(&and_not_result),
                "intersects": bulk_left.intersects(&bulk_right),
                "equalClone": bulk_left == bulk_left.clone(),
                "leftLongs": bulk_left.to_long_array().unwrap(),
                "leftHashCode": bulk_left.java_hash_code(),
                "leftToString": bulk_left.java_to_string(),
                "leftStream": bulk_left.iter_set_bits().collect::<Vec<_>>(),
            }),
            case["bitSetBulkOperations"]
        );

        let capacity_case = &fixture["batchAckBitSetCapacityCase"];
        let mut grown = BitSet::new();
        grown.set(63).unwrap();
        let after_bit_63 = grown.size();
        grown.set(64).unwrap();
        let after_bit_64 = grown.size();
        grown.set(128).unwrap();
        let after_bit_128 = grown.size();
        grown.clear(128).unwrap();
        let mut capacity_source = BitSet::new();
        capacity_source.set(0).unwrap();
        capacity_source.set(64).unwrap();
        let mut narrow_capacity = BitSet::try_with_capacity_bits(1).unwrap();
        let mut wide_capacity = BitSet::try_with_capacity_bits(128).unwrap();
        narrow_capacity.set(4).unwrap();
        wide_capacity.set(4).unwrap();
        assert_eq!(
            json!({
                "constructorSizes": [
                    BitSet::new().size(),
                    BitSet::try_with_capacity_bits(0).unwrap().size(),
                    BitSet::try_with_capacity_bits(1).unwrap().size(),
                    BitSet::try_with_capacity_bits(64).unwrap().size(),
                    BitSet::try_with_capacity_bits(65).unwrap().size(),
                ],
                "growthAndClearSizes": [after_bit_63, after_bit_64, after_bit_128, grown.size()],
                "valueOfSizes": [
                    BitSet::from_byte_array(&[1, 0, 0]).size(),
                    BitSet::try_from_long_array(&[1, 0]).unwrap().size(),
                    BitSet::try_from_long_array(&[]).unwrap().size(),
                ],
                "negativeCapacityError": capacity_case["negativeCapacityError"],
                "sliceSize": capacity_source.get_range(0, 65).unwrap().size(),
                "emptySliceSize": BitSet::new().get_range(3, 3).unwrap().size(),
                "equalWithDifferentCapacities": narrow_capacity.size() != wide_capacity.size()
                    && narrow_capacity == wide_capacity,
            }),
            capacity_case.clone()
        );
        assert_eq!(
            BitSet::try_with_capacity_bits(-1),
            Err(BitSetError::NegativeCapacity(-1))
        );
        assert_eq!(
            capacity_case["negativeCapacityError"],
            "NegativeArraySizeException"
        );

        let mut body = BatchAckMessageRequestBody::default();
        assert_eq!(
            json!([body.get_broker_name(), body.get_acks()]),
            case["bodyDefaults"]
        );
        body.set_broker_name(Some("broker-a".into()));
        let caller_ack = SharedValue::new(ack.clone());
        let caller_acks = SharedValue::new(vec![Some(caller_ack.clone())]);
        let assigned_acks = caller_acks.clone();
        body.set_acks(Some(caller_acks.clone()));
        let getter_acks = body.get_acks().unwrap();
        assert_eq!(
            assigned_acks.ptr_eq(&getter_acks),
            case["bodyReturnsAssignedList"]
        );
        assert_eq!(body.get_broker_name(), case["bodyBrokerName"].as_str());
        assert_eq!(getter_acks.with(|acks| acks.len()), case["bodyAckCount"]);
        assert!(getter_acks.with(|acks| acks[0].as_ref().unwrap().ptr_eq(&caller_ack)));
        assert_eq!(
            json!(
                getter_acks
                    .snapshot()
                    .iter()
                    .map(|ack| ack.as_ref().map(|ack| ack.with(BatchAck::get_queue_id)))
                    .collect::<Vec<_>>()
            ),
            case["bodyAssignedQueueIds"]
        );
        caller_ack.with_mut(|ack| ack.set_queue_id(5));
        assert_eq!(
            getter_acks
                .with(|acks| { acks[0].as_ref().unwrap().with(BatchAck::get_queue_id) == 5 }),
            case["externalAckObjectMutationVisible"]
        );
        getter_acks.with(|acks| {
            acks[0]
                .as_ref()
                .unwrap()
                .with_mut(|ack| ack.set_queue_id(6))
        });
        assert_eq!(
            caller_ack.with(|ack| ack.get_queue_id() == 6),
            case["ackObjectMutationVisibleThroughBody"]
        );
        caller_acks.with_mut(|acks| acks.clear());
        let mut external_ack = BatchAck::default();
        external_ack.set_queue_id(3);
        caller_acks.with_mut(|acks| acks.push(Some(SharedValue::new(external_ack))));
        assert_eq!(
            getter_acks.with(|acks| {
                acks.iter()
                    .flatten()
                    .any(|ack| ack.with(BatchAck::get_queue_id) == 3)
            }),
            case["externalMutationVisible"]
        );
        let mut getter_ack = BatchAck::default();
        getter_ack.set_queue_id(4);
        getter_acks.with_mut(|acks| {
            acks.clear();
            acks.push(Some(SharedValue::new(getter_ack)));
        });
        assert_eq!(
            assigned_acks.with(|acks| {
                acks.iter()
                    .flatten()
                    .any(|ack| ack.with(BatchAck::get_queue_id) == 4)
            }),
            case["getterMutationVisibleToAssignedList"]
        );
        let mutated_queue_ids = getter_acks
            .snapshot()
            .iter()
            .map(|ack| ack.as_ref().map(|ack| ack.with(BatchAck::get_queue_id)))
            .collect::<Vec<_>>();
        assert_eq!(json!(mutated_queue_ids), case["bodyMutatedQueueIds"]);
        let ack_identity = &case["ackObjectIdentity"];
        assert_eq!(ack_identity["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(ack_identity["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(ack_identity["sameInstanceEquals"], true);
        assert_eq!(ack_identity["equalValuesDistinctInstanceEquals"], false);
        assert_eq!(case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(case["sameInstanceEquals"], true);
        assert_eq!(case["equalValuesDistinctInstanceEquals"], false);
        body.set_acks(None);
        assert!(body.get_acks().is_none());
        assert_eq!(case["bodyNullAcks"], true);
    }

    #[test]
    fn batch_ack_inherited_remoting_serialization_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let api = &fixture["batchAckApiCase"];
        assert_eq!(
            api["batchAckPublicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.BatchAck()"])
        );
        assert_eq!(api["batchAckPublicFields"], json!([]));
        assert_eq!(
            api["batchAckDeclaredPublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            19
        );
        assert_eq!(
            api["batchAckDeclaredPublicMethods"],
            api["batchAckCallablePublicMethods"]
        );
        assert_eq!(
            api["bodyPublicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody()"])
        );
        assert_eq!(api["bodyPublicFields"], json!([]));
        assert_eq!(
            api["bodyDeclaredPublicMethods"].as_array().unwrap().len(),
            4
        );
        assert_eq!(
            api["bodyCallablePublicMethods"].as_array().unwrap().len(),
            13
        );

        let method_names = |key: &str| {
            let mut names = api[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|signature| {
                    signature
                        .as_str()
                        .unwrap()
                        .split_once('(')
                        .unwrap()
                        .0
                        .rsplit_once('.')
                        .unwrap()
                        .1
                        .to_owned()
                })
                .collect::<Vec<_>>();
            names.sort();
            names.dedup();
            names
        };
        assert_eq!(
            method_names("batchAckDeclaredPublicMethods"),
            [
                "getBitSet",
                "getConsumerGroup",
                "getInvisibleTime",
                "getPopTime",
                "getQueueId",
                "getRetry",
                "getReviveQueueId",
                "getStartOffset",
                "getTopic",
                "setBitSet",
                "setConsumerGroup",
                "setInvisibleTime",
                "setPopTime",
                "setQueueId",
                "setRetry",
                "setReviveQueueId",
                "setStartOffset",
                "setTopic",
                "toString",
            ]
        );
        assert_eq!(
            method_names("bodyDeclaredPublicMethods"),
            ["getAcks", "getBrokerName", "setAcks", "setBrokerName"]
        );
        assert_eq!(
            method_names("bodyCallablePublicMethods"),
            [
                "decode",
                "decodeList",
                "encode",
                "fromJson",
                "getAcks",
                "getBrokerName",
                "setAcks",
                "setBrokerName",
                "toJson",
            ]
        );

        for case in fixture["batchAckInheritedSerializationCases"]
            .as_array()
            .unwrap()
        {
            let raw_input = case["rawInput"].as_str().unwrap();
            let value = BatchAckMessageRequestBody::java_decode(raw_input.as_bytes()).unwrap();
            let compact = value.java_to_json(false).unwrap();
            let encoded = value.java_encode().unwrap();
            assert_eq!(
                value.java_encode_with_features(&[]).unwrap(),
                encoded,
                "{} empty writer-feature array",
                case["id"]
            );
            let browser_compatible = value
                .java_encode_with_features(&[JavaJsonWriterFeature::BrowserCompatible])
                .unwrap();
            let expected_browser_compatible = case["writerFeatureOverrides"]["BrowserCompatible"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                browser_compatible,
                expected_browser_compatible.as_bytes(),
                "{} BrowserCompatible",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::OptimizedForAscii])
                    .unwrap(),
                case["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                "{} OptimizedForAscii",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::WriteNameAsSymbol,
                    "WriteNameAsSymbol",
                ),
                (
                    JavaJsonWriterFeature::NotWriteRootClassName,
                    "NotWriteRootClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteHashMapArrayListClassName,
                    "NotWriteHashMapArrayListClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteNumberClassName,
                    "NotWriteNumberClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteSetClassName,
                    "NotWriteSetClassName",
                ),
                (
                    JavaJsonWriterFeature::WriteByteArrayAsBase64,
                    "WriteByteArrayAsBase64",
                ),
                (
                    JavaJsonWriterFeature::WritePairAsJavaBean,
                    "WritePairAsJavaBean",
                ),
                (
                    JavaJsonWriterFeature::WriteThrowableClassName,
                    "WriteThrowableClassName",
                ),
                (
                    JavaJsonWriterFeature::WriterUtilDateAsMillis,
                    "WriterUtilDateAsMillis",
                ),
                (
                    JavaJsonWriterFeature::WriteBigDecimalAsPlain,
                    "WriteBigDecimalAsPlain",
                ),
                (
                    JavaJsonWriterFeature::WriteFloatSpecialAsString,
                    "WriteFloatSpecialAsString",
                ),
                (JavaJsonWriterFeature::BrowserSecure, "BrowserSecure"),
                (
                    JavaJsonWriterFeature::WriteEnumsUsingName,
                    "WriteEnumsUsingName",
                ),
                (
                    JavaJsonWriterFeature::WriteEnumUsingToString,
                    "WriteEnumUsingToString",
                ),
                (
                    JavaJsonWriterFeature::IgnoreErrorGetter,
                    "IgnoreErrorGetter",
                ),
                (
                    JavaJsonWriterFeature::IgnoreNonFieldGetter,
                    "IgnoreNonFieldGetter",
                ),
                (JavaJsonWriterFeature::WriteNulls, "WriteNulls"),
                (
                    JavaJsonWriterFeature::WriteMapNullValue,
                    "WriteMapNullValue",
                ),
                (
                    JavaJsonWriterFeature::WriteNullListAsEmpty,
                    "WriteNullListAsEmpty",
                ),
                (
                    JavaJsonWriterFeature::WriteNullStringAsEmpty,
                    "WriteNullStringAsEmpty",
                ),
                (
                    JavaJsonWriterFeature::WriteNullNumberAsZero,
                    "WriteNullNumberAsZero",
                ),
                (
                    JavaJsonWriterFeature::NullAsDefaultValue,
                    "NullAsDefaultValue",
                ),
                (JavaJsonWriterFeature::WriteClassName, "WriteClassName"),
                (
                    JavaJsonWriterFeature::ReferenceDetection,
                    "ReferenceDetection",
                ),
                (JavaJsonWriterFeature::BeanToArray, "BeanToArray"),
                (JavaJsonWriterFeature::FieldBased, "FieldBased"),
                (
                    JavaJsonWriterFeature::WriteNullBooleanAsFalse,
                    "WriteNullBooleanAsFalse",
                ),
                (JavaJsonWriterFeature::MapSortField, "MapSortField"),
                (
                    JavaJsonWriterFeature::SortMapEntriesByKeys,
                    "SortMapEntriesByKeys",
                ),
                (JavaJsonWriterFeature::LargeObject, "LargeObject"),
            ] {
                let featured = value.java_encode_with_features(&[feature]).unwrap();
                let expected = case["writerFeatureOverrides"][feature_name]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
                assert_eq!(
                    featured,
                    expected.as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }
            assert_eq!(
                BatchAckMessageRequestBody::java_to_json_optional(Some(&value), false).unwrap(),
                compact
            );
            assert_eq!(
                BatchAckMessageRequestBody::java_encode_optional(Some(&value)).unwrap(),
                Some(encoded.clone())
            );
            assert_eq!(
                compact, case["instanceJson"],
                "{} instance JSON",
                case["id"]
            );
            assert_eq!(compact, case["staticJson"], "{} static JSON", case["id"]);
            assert_eq!(
                value.java_to_json(true).unwrap(),
                case["instancePrettyJson"],
                "{} pretty JSON",
                case["id"]
            );
            assert_eq!(
                value.java_to_json(true).unwrap(),
                case["staticPrettyJson"],
                "{} static pretty JSON",
                case["id"]
            );
            assert_eq!(
                encoded,
                case["instanceEncodedJson"].as_str().unwrap().as_bytes()
            );
            assert_eq!(
                encoded,
                case["staticEncodedJson"].as_str().unwrap().as_bytes()
            );
            assert_eq!(
                encoded,
                case["featureEncodedJson"].as_str().unwrap().as_bytes()
            );
            let featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteNonStringValueAsString])
                .unwrap();
            let expected_featured = case["writerFeatureOverrides"]["WriteNonStringValueAsString"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                featured,
                expected_featured.as_bytes(),
                "{} WriteNonStringValueAsString",
                case["id"]
            );
            let boolean_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteBooleanAsNumber])
                .unwrap();
            let expected_boolean_featured = case["writerFeatureOverrides"]["WriteBooleanAsNumber"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                boolean_featured,
                expected_boolean_featured.as_bytes(),
                "{} WriteBooleanAsNumber",
                case["id"]
            );
            let combined_featured = value
                .java_encode_with_features(&[
                    JavaJsonWriterFeature::WriteBooleanAsNumber,
                    JavaJsonWriterFeature::WriteNonStringValueAsString,
                ])
                .unwrap();
            let expected_combined_featured =
                case["writerFeatureOverrides"]["WriteBooleanAsNumber+WriteNonStringValueAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                combined_featured,
                expected_combined_featured.as_bytes(),
                "{} combined boolean writer features",
                case["id"]
            );
            let escaped_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                .unwrap();
            let expected_escaped_featured = case["writerFeatureOverrides"]["EscapeNoneAscii"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                escaped_featured,
                expected_escaped_featured.as_bytes(),
                "{} EscapeNoneAscii",
                case["id"]
            );
            let pretty_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::PrettyFormat])
                .unwrap();
            let expected_pretty_featured = case["writerFeatureOverrides"]["PrettyFormat"]
                .as_str()
                .unwrap_or_else(|| case["instancePrettyJson"].as_str().unwrap());
            assert_eq!(
                pretty_featured,
                expected_pretty_featured.as_bytes(),
                "{} PrettyFormat",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::WriteLongAsString])
                    .unwrap(),
                case["writerFeatureOverrides"]["WriteLongAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap())
                    .as_bytes(),
                "{} WriteLongAsString",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::PrettyFormatWith2Space,
                    "PrettyFormatWith2Space",
                ),
                (
                    JavaJsonWriterFeature::PrettyFormatWith4Space,
                    "PrettyFormatWith4Space",
                ),
            ] {
                assert_eq!(
                    value.java_encode_with_features(&[feature]).unwrap(),
                    case["writerFeatureOverrides"][feature_name]
                        .as_str()
                        .unwrap()
                        .as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }

            let decoded = BatchAckMessageRequestBody::java_decode(&encoded).unwrap();
            let from_json = BatchAckMessageRequestBody::java_from_json(&compact).unwrap();
            let list =
                BatchAckMessageRequestBody::java_decode_list(format!("[{compact}]").as_bytes())
                    .unwrap();
            assert_eq!(decoded.java_to_json(false).unwrap(), case["decodedJson"]);
            assert_eq!(from_json.java_to_json(false).unwrap(), case["fromJsonJson"]);
            assert_eq!(
                list.len(),
                case["decodedListCount"].as_u64().unwrap() as usize
            );
            assert_eq!(
                list[0].java_to_json(false).unwrap(),
                case["decodedListJson"],
                "{} list decode",
                case["id"]
            );
        }

        let nulls = &fixture["batchAckInheritedSerializationNullUtilities"];
        assert_eq!(nulls["staticEncodeNullReturnsNull"], true);
        assert_eq!(nulls["staticToJsonNull"], "null");
        assert_eq!(
            BatchAckMessageRequestBody::java_encode_optional(None).unwrap(),
            None
        );
        assert_eq!(
            BatchAckMessageRequestBody::java_to_json_optional(None, false).unwrap(),
            nulls["staticToJsonNull"]
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        let null_members =
            BatchAckMessageRequestBody::java_decode_list_nullable(Some(b"[null,{}]")).unwrap();
        assert_eq!(null_members.as_ref().map(Vec::len), Some(2));
        assert!(null_members.unwrap()[0].is_none());
        assert_eq!(nulls["decodeNullBytesReturnsNull"], true);
        assert_eq!(nulls["decodeJsonNullReturnsNull"], true);
        assert_eq!(nulls["fromJsonNullInputReturnsNull"], true);
        assert_eq!(nulls["fromJsonJsonNullReturnsNull"], true);
        assert_eq!(nulls["decodeListNullBytesReturnsNull"], true);
        assert_eq!(nulls["decodeListJsonNullReturnsNull"], true);
        assert_eq!(nulls["decodeListNullMemberCount"], 2);
        assert_eq!(nulls["decodeListPreservesNullMember"], true);
    }

    #[test]
    fn assignment_body_public_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();

        let request_case = &fixture["queryAssignmentRequestApiCase"];
        assert_eq!(request_case["bodyPublicFields"], json!([]));
        let mut request = QueryAssignmentRequestBody::default();
        assert_eq!(
            json!({
                "topic": request.get_topic(),
                "consumerGroup": request.get_consumer_group(),
                "clientId": request.get_client_id(),
                "strategyName": request.get_strategy_name(),
                "messageModel": request.get_message_model(),
            }),
            request_case["defaults"]
        );
        request.set_topic(Some("orders".into()));
        request.set_consumer_group(Some("workers".into()));
        request.set_client_id(Some("client-a".into()));
        request.set_strategy_name(Some("AllocateByCircle".into()));
        request.set_message_model(Some(super::super::heartbeat::MessageModel::Clustering));
        assert_eq!(
            json!({
                "topic": request.get_topic(),
                "consumerGroup": request.get_consumer_group(),
                "clientId": request.get_client_id(),
                "strategyName": request.get_strategy_name(),
                "messageModel": request.get_message_model(),
            }),
            request_case["values"]
        );
        request.set_topic(None);
        request.set_consumer_group(None);
        request.set_client_id(None);
        request.set_strategy_name(None);
        request.set_message_model(None);
        assert_eq!(request_case["nullSettersRemainNull"], true);
        assert_eq!(
            json!({
                "topic": request.get_topic(),
                "consumerGroup": request.get_consumer_group(),
                "clientId": request.get_client_id(),
                "strategyName": request.get_strategy_name(),
                "messageModel": request.get_message_model(),
            }),
            json!({
                "topic": null,
                "consumerGroup": null,
                "clientId": null,
                "strategyName": null,
                "messageModel": null,
            })
        );

        let response_case = &fixture["queryAssignmentResponseApiCase"];
        assert_eq!(response_case["bodyPublicFields"], json!([]));
        assert_eq!(
            response_case["messageQueueAssignmentPublicFields"],
            json!([])
        );
        assert_eq!(
            response_case["messageQueueAssignmentPublicConstructors"],
            json!(["org.apache.rocketmq.common.message.MessageQueueAssignment()"])
        );
        assert_eq!(
            response_case["messageQueueAssignmentDeclaredPublicMethods"],
            json!([
                "boolean org.apache.rocketmq.common.message.MessageQueueAssignment.equals(java.lang.Object)",
                "int org.apache.rocketmq.common.message.MessageQueueAssignment.hashCode()",
                "java.lang.String org.apache.rocketmq.common.message.MessageQueueAssignment.toString()",
                "java.util.Map org.apache.rocketmq.common.message.MessageQueueAssignment.getAttachments()",
                "org.apache.rocketmq.common.message.MessageQueue org.apache.rocketmq.common.message.MessageQueueAssignment.getMessageQueue()",
                "org.apache.rocketmq.common.message.MessageRequestMode org.apache.rocketmq.common.message.MessageQueueAssignment.getMode()",
                "void org.apache.rocketmq.common.message.MessageQueueAssignment.setAttachments(java.util.Map)",
                "void org.apache.rocketmq.common.message.MessageQueueAssignment.setMessageQueue(org.apache.rocketmq.common.message.MessageQueue)",
                "void org.apache.rocketmq.common.message.MessageQueueAssignment.setMode(org.apache.rocketmq.common.message.MessageRequestMode)"
            ])
        );
        assert_eq!(
            response_case["messageQueueAssignmentCallablePublicMethods"],
            response_case["messageQueueAssignmentDeclaredPublicMethods"]
        );
        assert_eq!(response_case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(response_case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(response_case["sameInstanceEquals"], true);
        assert_eq!(response_case["equalValuesDistinctInstanceEquals"], false);
        let mut response = QueryAssignmentResponseBody::default();
        assert_eq!(
            serde_json::to_value(response.get_message_queue_assignments()).unwrap(),
            response_case["defaultAssignments"]
        );
        // This fixture intentionally exercises Java's mutable Set element objects.
        #[allow(clippy::mutable_key_type)]
        let assignments: QueryAssignmentSet =
            serde_json::from_value(response_case["assignedValues"].clone()).unwrap();
        let assigned_element = assignments
            .iter()
            .next()
            .and_then(Option::as_ref)
            .unwrap()
            .clone();
        let assigned_alias = SharedValue::new(assignments);
        let caller_alias = assigned_alias.clone();
        response.set_message_queue_assignments(Some(assigned_alias.clone()));
        let getter_alias = response.get_message_queue_assignments().unwrap();
        assert!(assigned_alias.ptr_eq(&getter_alias));
        assert!(caller_alias.ptr_eq(&getter_alias));
        let getter_element = getter_alias.with(|assignments| {
            assignments
                .iter()
                .next()
                .and_then(Option::as_ref)
                .unwrap()
                .clone()
        });
        assert!(assigned_element.ptr_eq(&getter_element));
        assert_eq!(response_case["getterReturnsAssignedElement"], true);
        assigned_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            getter_element.with(MessageQueueAssignment::mode),
            Some(MessageRequestMode::Pull)
        );
        assigned_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pop)));
        getter_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            assigned_element.with(MessageQueueAssignment::mode),
            Some(MessageRequestMode::Pull)
        );
        getter_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pop)));
        assert_eq!(response_case["setterElementMutationVisibleToGetter"], true);
        assert_eq!(
            response_case["getterElementMutationVisibleToSetterObject"],
            true
        );
        assert_eq!(
            getter_alias.with(std::collections::HashSet::len),
            response_case["valueCount"]
        );
        caller_alias.with_mut(|assignments_mut| {
            assignments_mut.clear();
            let mut assignment = MessageQueueAssignment::default();
            assignment.set_message_queue(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
            assignment.set_mode(Some(MessageRequestMode::Pull));
            assignments_mut.insert(Some(SharedValue::new(assignment)));
        });
        assert_eq!(
            getter_alias.with(|assignments| assignments.iter().any(|assignment| assignment
                .as_ref()
                .is_some_and(|assignment| assignment.with(|assignment| assignment
                    .message_queue()
                    .is_some_and(|queue| queue.with(|queue| queue.queue_id() == 3)))))),
            response_case["externalMutationVisible"]
        );
        getter_alias.with_mut(|assignments_mut| {
            assignments_mut.clear();
            let mut assignment = MessageQueueAssignment::default();
            assignment.set_message_queue(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 4,
            ))));
            assignment.set_mode(Some(MessageRequestMode::Pop));
            assignments_mut.insert(Some(SharedValue::new(assignment)));
        });
        assert_eq!(
            caller_alias.with(|assignments| assignments.iter().any(|assignment| assignment
                .as_ref()
                .is_some_and(|assignment| assignment.with(|assignment| assignment
                    .message_queue()
                    .is_some_and(|queue| queue.with(|queue| queue.queue_id() == 4)))))),
            response_case["getterMutationVisibleToAssignedSet"]
        );
        let mutated_values = getter_alias.with(|assignments| {
            assignments
                .iter()
                .map(|assignment| {
                    assignment.as_ref().map(|assignment| {
                        assignment.with(|assignment| {
                            json!({
                                "messageQueue": assignment.message_queue(),
                                "mode": assignment.mode(),
                                "attachments": assignment.attachments(),
                            })
                        })
                    })
                })
                .collect::<Vec<_>>()
        });
        assert_eq!(json!(mutated_values), response_case["mutatedValues"]);
        // Java returns the same mutable Set; the shared handle keeps its identity on both sides.
        assert_eq!(response_case["setterRetainsSetIdentity"], true);
        response.set_message_queue_assignments(None);
        assert!(response.get_message_queue_assignments().is_none());
        assert_eq!(response_case["nullSetterRemainsNull"], true);

        let mode_case = &fixture["setRequestModeRequestApiCase"];
        assert_eq!(mode_case["bodyPublicFields"], json!([]));
        let mut mode_body = SetMessageRequestModeRequestBody::default();
        assert_eq!(
            json!({
                "topic": mode_body.get_topic(),
                "consumerGroup": mode_body.get_consumer_group(),
                "mode": mode_body.get_mode(),
                "popShareQueueNum": mode_body.get_pop_share_queue_num(),
            }),
            mode_case["defaults"]
        );
        mode_body.set_topic(Some("orders".into()));
        mode_body.set_consumer_group(Some("workers".into()));
        mode_body.set_mode(Some(MessageRequestMode::Pop));
        mode_body.set_pop_share_queue_num(2);
        assert_eq!(
            json!({
                "topic": mode_body.get_topic(),
                "consumerGroup": mode_body.get_consumer_group(),
                "mode": mode_body.get_mode(),
                "popShareQueueNum": mode_body.get_pop_share_queue_num(),
            }),
            mode_case["values"]
        );
        mode_body.set_topic(None);
        mode_body.set_consumer_group(None);
        mode_body.set_mode(None);
        mode_body.set_pop_share_queue_num(0);
        assert_eq!(mode_case["nullSettersRemainNull"], true);
        assert_eq!(
            mode_body.get_pop_share_queue_num(),
            mode_case["primitiveAfterReset"]
        );
    }

    #[test]
    fn protocol_dto_identity_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/protocol/dto-identity.json"))
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

        for dto in [
            "QueryAssignmentRequestBody",
            "SetMessageRequestModeRequestBody",
            "GetConsumerStatusBody",
            "ResetOffsetBody",
            "ConsumeMessageDirectlyResult",
            "TopicOffset",
            "TopicStatsTable",
        ] {
            let identity = &fixture["identityCases"][dto];
            assert_eq!(identity["equalsDeclaredBy"], "java.lang.Object", "{dto}");
            assert_eq!(identity["hashCodeDeclaredBy"], "java.lang.Object", "{dto}");
            assert_eq!(identity["sameObjectEqualsItself"], true, "{dto}");
            assert_eq!(identity["distinctDefaultObjectsEquals"], false, "{dto}");
        }
    }

    #[test]
    fn remaining_runtime_body_public_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let assert_java_object_identity = |case: &serde_json::Value| {
            assert_eq!(case["equalsDeclaringClass"], "java.lang.Object");
            assert_eq!(case["hashCodeDeclaringClass"], "java.lang.Object");
            assert_eq!(case["sameInstanceEquals"], true);
            assert_eq!(case["equalValuesDistinctInstanceEquals"], false);
        };

        let check_case = &fixture["checkClientBodyApiCase"];
        let mut check = CheckClientRequestBody::default();
        assert_eq!(check_case["equalsSameInstance"], true);
        assert_eq!(check_case["equalsEqualFieldsDistinctInstance"], false);
        assert_eq!(check_case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(check_case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(check_case["bodyPublicFields"], json!([]));
        assert_eq!(
            json!({
                "clientId": check.get_client_id(),
                "group": check.get_group(),
                "subscriptionData": check.get_subscription_data(),
                "namespace": check.get_namespace(),
            }),
            check_case["defaults"]
        );
        let subscription = SharedValue::new(
            serde_json::from_value(check_case["values"]["subscriptionData"].clone()).unwrap(),
        );
        check.set_client_id(Some("client-a".into()));
        check.set_group(Some("workers".into()));
        check.set_subscription_data(Some(subscription.clone()));
        check.set_namespace(Some("tenant".into()));
        let subscription_values = check.get_subscription_data().map(|value| {
            value.with(|value| {
                json!({
                    "topic": value.topic.as_deref(),
                    "subString": value.sub_string.as_deref(),
                    "subVersion": value.sub_version,
                    "classFilterMode": value.class_filter_mode,
                    "expressionType": value.expression_type.as_deref(),
                    "tagsSet": value.tags_set.as_ref(),
                })
            })
        });
        assert_eq!(
            json!({
                "clientId": check.get_client_id(),
                "group": check.get_group(),
                "subscriptionData": subscription_values,
                "namespace": check.get_namespace(),
            }),
            check_case["values"]
        );
        let subscription_mut = check.get_subscription_data_mut().unwrap();
        assert_eq!(
            subscription_mut.ptr_eq(&subscription),
            check_case["getterReturnsAssignedSubscription"]
        );
        // Mutations through the original setter handle are visible through the body getter.
        subscription.with_mut(|value| value.set_sub_version(43));
        assert_eq!(
            subscription_mut.with(|value| value.get_sub_version() == 43),
            check_case["externalMutationVisible"]
        );
        // Mutations through the getter handle are visible through the original setter handle.
        subscription_mut.with_mut(|value| value.set_sub_string(Some("orders*".into())));
        assert_eq!(
            subscription.with(|value| value.get_sub_string() == Some("orders*")),
            check_case["getterMutationVisibleToAssignedSubscription"]
        );
        assert_eq!(
            subscription_mut.with(|value| json!({
                "topic": value.get_topic(),
                "subString": value.get_sub_string(),
                "subVersion": value.get_sub_version(),
                "classFilterMode": value.is_class_filter_mode(),
                "expressionType": value.get_expression_type(),
                "tagsSet": value.get_tags_set(),
            })),
            check_case["mutatedSubscriptionData"]
        );
        check.set_client_id(None);
        check.set_group(None);
        check.set_subscription_data(None);
        check.set_namespace(None);
        assert!(check.get_client_id().is_none());
        assert!(check.get_group().is_none());
        assert!(check.get_subscription_data().is_none());
        assert!(check.get_namespace().is_none());
        assert_eq!(check_case["nullSettersRemainNull"], true);

        let list_case = &fixture["consumerListApiCase"];
        assert_eq!(
            list_case["publicConstructors"],
            json!([
                "org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody()"
            ])
        );
        assert_eq!(
            list_case["declaredPublicMethods"],
            json!([
                "java.util.List org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.getConsumerIdList()",
                "void org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody.setConsumerIdList(java.util.List)"
            ])
        );
        assert_eq!(list_case["publicFields"], json!([]));
        let mut consumer_list = ConsumerListByGroupResponseBody::default();
        assert_eq!(
            serde_json::to_value(consumer_list.get_consumer_id_list()).unwrap(),
            list_case["defaultConsumerIds"]
        );
        let consumer_ids: Vec<Option<String>> =
            serde_json::from_value(list_case["assignedValues"].clone()).unwrap();
        let caller_alias = SharedValue::new(consumer_ids);
        let assigned_alias = caller_alias.clone();
        consumer_list.set_consumer_id_list(Some(caller_alias.clone()));
        let getter_alias = consumer_list.get_consumer_id_list().unwrap();
        assert_eq!(
            assigned_alias.ptr_eq(&getter_alias),
            list_case["getterReturnsAssignedList"]
        );
        caller_alias.with_mut(|ids| ids.push(Some("client-c".into())));
        assert_eq!(
            getter_alias.with(|ids| ids.contains(&Some("client-c".into()))),
            list_case["externalMutationVisible"]
        );
        getter_alias.with_mut(|ids| ids.push(Some("client-d".into())));
        assert_eq!(
            assigned_alias.with(|ids| ids.contains(&Some("client-d".into()))),
            list_case["getterMutationVisibleToAssignedList"]
        );
        assert_eq!(
            serde_json::to_value(getter_alias.snapshot()).unwrap(),
            list_case["mutatedValues"]
        );
        consumer_list.set_consumer_id_list(None);
        assert!(consumer_list.get_consumer_id_list().is_none());
        assert_eq!(list_case["nullSetterRemainsNull"], true);
        assert_eq!(list_case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(list_case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(list_case["sameInstanceEquals"], true);
        assert_eq!(list_case["equalValuesDistinctInstanceEquals"], false);

        let lock_case = &fixture["lockBatchRequestApiCase"];
        assert_eq!(lock_case["bodyPublicFields"], json!([]));
        let mut lock = LockBatchRequestBody::default();
        assert_eq!(
            json!({
                "consumerGroup": lock.get_consumer_group(),
                "clientId": lock.get_client_id(),
                "onlyThisBroker": lock.is_only_this_broker(),
                "mqSet": lock.get_mq_set(),
            }),
            lock_case["defaults"]
        );
        assert_eq!(lock.to_string(), lock_case["defaultText"].as_str().unwrap());
        #[allow(clippy::mutable_key_type)]
        let lock_queues: MessageQueueSet =
            serde_json::from_value(lock_case["assignedValues"].clone()).unwrap();
        let lock_caller = SharedValue::new(lock_queues);
        let lock_assigned = lock_caller.clone();
        lock.set_consumer_group(Some("workers".into()));
        lock.set_client_id(Some("client-a".into()));
        lock.set_only_this_broker(true);
        lock.set_mq_set(Some(lock_caller.clone()));
        let lock_getter = lock.get_mq_set().unwrap();
        assert_eq!(
            lock_assigned.ptr_eq(&lock_getter),
            lock_case["getterReturnsAssignedSet"]
        );
        message_queue_set_element_aliases_match(lock_case, &lock_caller, &lock_getter);
        lock_caller.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 2,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
        });
        assert_eq!(
            lock_getter.with(
                |queues| queues.contains(&Some(SharedValue::new(MessageQueue::new(
                    "orders", "broker", 3
                ))))
            ),
            lock_case["externalMutationVisible"]
        );
        lock_getter.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 4,
            ))));
        });
        assert_eq!(
            lock_assigned.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 4)
            )))),
            lock_case["getterMutationVisibleToAssignedSet"]
        );
        assert_eq!(
            json!({
                "consumerGroup": lock.get_consumer_group(),
                "clientId": lock.get_client_id(),
                "onlyThisBroker": lock.is_only_this_broker(),
                "mqSet": lock_getter.snapshot(),
            }),
            lock_case["mutatedValues"]
        );
        assert_eq!(lock.to_string(), lock_case["text"].as_str().unwrap());
        assert_java_object_identity(lock_case);
        lock.set_consumer_group(None);
        lock.set_client_id(None);
        lock.set_only_this_broker(false);
        lock.set_mq_set(None);
        assert!(lock.get_consumer_group().is_none());
        assert!(lock.get_client_id().is_none());
        assert!(!lock.is_only_this_broker());
        assert!(lock.get_mq_set().is_none());
        assert_eq!(lock_case["nullSettersRemainNull"], true);

        let unlock_case = &fixture["unlockBatchRequestApiCase"];
        assert_eq!(unlock_case["bodyPublicFields"], json!([]));
        let mut unlock = UnlockBatchRequestBody::default();
        assert_eq!(
            json!({
                "consumerGroup": unlock.get_consumer_group(),
                "clientId": unlock.get_client_id(),
                "onlyThisBroker": unlock.is_only_this_broker(),
                "mqSet": unlock.get_mq_set(),
            }),
            unlock_case["defaults"]
        );
        assert_eq!(
            unlock.to_string(),
            unlock_case["defaultText"].as_str().unwrap()
        );
        #[allow(clippy::mutable_key_type)]
        let unlock_queues: MessageQueueSet =
            serde_json::from_value(unlock_case["assignedValues"].clone()).unwrap();
        let unlock_caller = SharedValue::new(unlock_queues);
        let unlock_assigned = unlock_caller.clone();
        unlock.set_consumer_group(Some("workers".into()));
        unlock.set_client_id(Some("client-a".into()));
        unlock.set_only_this_broker(true);
        unlock.set_mq_set(Some(unlock_caller.clone()));
        let unlock_getter = unlock.get_mq_set().unwrap();
        assert_eq!(
            unlock_assigned.ptr_eq(&unlock_getter),
            unlock_case["getterReturnsAssignedSet"]
        );
        message_queue_set_element_aliases_match(unlock_case, &unlock_caller, &unlock_getter);
        unlock_caller.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 2,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
        });
        assert_eq!(
            unlock_getter.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 3)
            )))),
            unlock_case["externalMutationVisible"]
        );
        unlock_getter.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 4,
            ))));
        });
        assert_eq!(
            unlock_assigned.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 4)
            )))),
            unlock_case["getterMutationVisibleToAssignedSet"]
        );
        assert_eq!(
            json!({
                "consumerGroup": unlock.get_consumer_group(),
                "clientId": unlock.get_client_id(),
                "onlyThisBroker": unlock.is_only_this_broker(),
                "mqSet": unlock_getter.snapshot(),
            }),
            unlock_case["mutatedValues"]
        );
        assert_eq!(unlock.to_string(), unlock_case["text"].as_str().unwrap());
        assert_java_object_identity(unlock_case);
        unlock.set_consumer_group(None);
        unlock.set_client_id(None);
        unlock.set_only_this_broker(false);
        unlock.set_mq_set(None);
        assert!(unlock.get_consumer_group().is_none());
        assert!(unlock.get_client_id().is_none());
        assert!(!unlock.is_only_this_broker());
        assert!(unlock.get_mq_set().is_none());
        assert_eq!(unlock_case["nullSettersRemainNull"], true);

        let lock_response_case = &fixture["lockBatchResponseApiCase"];
        assert_eq!(
            lock_response_case["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody()"])
        );
        assert_eq!(
            lock_response_case["declaredPublicMethods"],
            json!([
                "java.util.Set org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody.getLockOKMQSet()",
                "void org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody.setLockOKMQSet(java.util.Set)"
            ])
        );
        assert_eq!(lock_response_case["publicFields"], json!([]));
        let mut lock_response = LockBatchResponseBody::default();
        assert_eq!(
            serde_json::to_value(lock_response.get_lock_okmq_set()).unwrap(),
            lock_response_case["defaultLockOKMQSet"]
        );
        #[allow(clippy::mutable_key_type)]
        let lock_response_queues: MessageQueueSet =
            serde_json::from_value(lock_response_case["assignedValues"].clone()).unwrap();
        let lock_response_caller = SharedValue::new(lock_response_queues);
        let lock_response_assigned = lock_response_caller.clone();
        lock_response.set_lock_okmq_set(Some(lock_response_caller.clone()));
        let lock_response_getter = lock_response.get_lock_okmq_set().unwrap();
        assert_eq!(
            lock_response_assigned.ptr_eq(&lock_response_getter),
            lock_response_case["getterReturnsAssignedSet"]
        );
        message_queue_set_element_aliases_match(
            lock_response_case,
            &lock_response_caller,
            &lock_response_getter,
        );
        lock_response_caller.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 2,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
        });
        assert_eq!(
            lock_response_getter.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 3)
            )))),
            lock_response_case["externalMutationVisible"]
        );
        lock_response_getter.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 4,
            ))));
        });
        assert_eq!(
            lock_response_assigned.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 4)
            )))),
            lock_response_case["getterMutationVisibleToAssignedSet"]
        );
        assert_eq!(
            serde_json::to_value(lock_response_getter.snapshot()).unwrap(),
            lock_response_case["mutatedValues"]
        );
        assert_java_object_identity(lock_response_case);
        lock_response.set_lock_okmq_set(None);
        assert!(lock_response.get_lock_okmq_set().is_none());
        assert_eq!(lock_response_case["nullSetterRemainsNull"], true);

        let direct_case = &fixture["consumeDirectResultApiCase"];
        assert_eq!(direct_case["bodyPublicFields"], json!([]));
        let mut direct = ConsumeMessageDirectlyResult::default();
        assert_eq!(
            json!({
                "order": direct.is_order(),
                "autoCommit": direct.is_auto_commit(),
                "consumeResult": direct.get_consume_result(),
                "remark": direct.get_remark(),
                "spentTimeMills": direct.get_spent_time_mills(),
            }),
            direct_case["defaults"]
        );
        assert_eq!(
            direct.to_string(),
            direct_case["defaultText"].as_str().unwrap()
        );
        direct.set_order(true);
        direct.set_auto_commit(false);
        direct.set_consume_result(Some(ConsumeMessageResult::CrSuccess));
        direct.set_remark(Some("consumed".into()));
        direct.set_spent_time_mills(42);
        assert_eq!(
            json!({
                "order": direct.is_order(),
                "autoCommit": direct.is_auto_commit(),
                "consumeResult": direct.get_consume_result(),
                "remark": direct.get_remark(),
                "spentTimeMills": direct.get_spent_time_mills(),
            }),
            direct_case["values"]
        );
        assert_eq!(direct.to_string(), direct_case["text"].as_str().unwrap());
        direct.set_consume_result(None);
        direct.set_remark(None);
        assert!(direct.get_consume_result().is_none());
        assert!(direct.get_remark().is_none());
        assert_eq!(direct_case["nullSettersRemainNull"], true);
    }

    #[test]
    fn consumer_status_and_reset_offset_public_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();

        let status_case = &fixture["consumerStatusBodyApiCase"];
        assert_eq!(status_case["bodyPublicFields"], json!([]));
        let mut status = GetConsumerStatusBody::default();
        assert_eq!(
            serde_json::to_value(status.get_message_queue_table()).unwrap(),
            status_case["defaults"]["messageQueueTable"]
        );
        assert_eq!(
            serde_json::to_value(status.get_consumer_table()).unwrap(),
            status_case["defaults"]["consumerTable"]
        );
        let message_queue_table = SharedValue::new(
            serde_json::from_value::<QueueOffsetEntries>(
                status_case["values"]["messageQueueTable"].clone(),
            )
            .unwrap(),
        );
        let consumer_table = SharedValue::new(
            serde_json::from_value::<ConsumerQueueOffsets>(
                status_case["values"]["consumerTable"].clone(),
            )
            .unwrap(),
        );
        let workers_table = consumer_table.with(|table| table["workers"].as_ref().unwrap().clone());
        status.set_message_queue_table(Some(message_queue_table.clone()));
        status.set_consumer_table(Some(consumer_table.clone()));
        assert!(
            status
                .get_message_queue_table()
                .unwrap()
                .ptr_eq(&message_queue_table)
        );
        assert!(status.get_consumer_table().unwrap().ptr_eq(&consumer_table));
        let nested_workers_table =
            consumer_table.with(|table| table["workers"].as_ref().unwrap().clone());
        assert!(workers_table.ptr_eq(&nested_workers_table));
        assert_eq!(
            serde_json::to_value(status.get_message_queue_table()).unwrap(),
            status_case["values"]["messageQueueTable"]
        );
        assert_eq!(
            serde_json::to_value(status.get_consumer_table()).unwrap(),
            status_case["values"]["consumerTable"]
        );
        message_queue_table.with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 4), Some(99)));
        });
        assert_eq!(
            status.get_message_queue_table().unwrap().with(|table| table
                .iter()
                .any(|(queue, offset)| queue.queue_id() == 4 && *offset == Some(99))),
            status_case["messageQueueTableExternalMutationVisible"]
        );
        status.get_consumer_table().unwrap().with_mut(|table| {
            table.insert(
                "getter-added".to_owned(),
                Some(SharedValue::new(vec![(
                    MessageQueue::new("orders", "broker", 5),
                    Some(100),
                )])),
            );
        });
        assert_eq!(
            consumer_table.with(|table| table.contains_key("getter-added")),
            status_case["consumerTableGetterMutationVisibleToAssignedMap"]
        );
        workers_table.with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 6), Some(101)));
        });
        assert_eq!(
            consumer_table.with(
                |table| table["workers"].as_ref().unwrap().with(|entries| entries
                    .iter()
                    .any(|(queue, offset)| queue.queue_id() == 6 && *offset == Some(101)))
            ),
            status_case["nestedGetterMutationVisibleToAssignedTable"]
        );
        assert_eq!(
            json!({
                "messageQueueTable": status.get_message_queue_table(),
                "consumerTable": status.get_consumer_table(),
            }),
            status_case["mutatedValues"]
        );
        // Java returns the same mutable outer Maps and nested Map assigned by setters.
        assert_eq!(status_case["getterReturnsAssignedMessageQueueTable"], true);
        assert_eq!(status_case["getterReturnsAssignedConsumerTable"], true);
        assert_eq!(status_case["nestedGetterReturnsAssignedTable"], true);
        status.set_message_queue_table(None);
        status.set_consumer_table(None);
        assert!(status.get_message_queue_table().is_none());
        assert!(status.get_consumer_table().is_none());
        assert_eq!(status_case["nullSettersRemainNull"], true);

        let reset_case = &fixture["resetOffsetBodyApiCase"];
        assert_eq!(reset_case["bodyPublicFields"], json!([]));
        let mut reset = ResetOffsetBody::default();
        assert_eq!(
            serde_json::to_value(reset.get_offset_table()).unwrap(),
            reset_case["defaults"]
        );
        let offset_table = SharedValue::new(
            serde_json::from_value::<NullableQueueOffsetEntries>(reset_case["values"].clone())
                .unwrap(),
        );
        reset.set_offset_table(Some(offset_table.clone()));
        assert!(reset.get_offset_table().unwrap().ptr_eq(&offset_table));
        assert_eq!(
            serde_json::to_value(reset.get_offset_table()).unwrap(),
            reset_case["values"]
        );
        assert_eq!(reset_case["getterReturnsAssignedMap"], true);
        assert_eq!(reset_case["externalMutationVisible"], true);
        assert_eq!(reset_case["getterMutationVisibleToAssignedMap"], true);
        offset_table.with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 4), Some(99)));
        });
        reset.get_offset_table().unwrap().with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 5), Some(100)));
        });
        assert_eq!(
            serde_json::to_value(reset.get_offset_table()).unwrap(),
            reset_case["mutatedValues"]
        );
        reset.set_offset_table(None);
        assert!(reset.get_offset_table().is_none());
        assert_eq!(reset_case["nullSetterRemainsNull"], true);
    }

    #[test]
    fn check_client_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-check.json"))
                .unwrap();
        let cases = fixture["checkClientBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 43);
        for required in [
            "shape-clientId-array",
            "shape-group-fraction",
            "shape-subscriptionData-object",
            "shape-namespace-boolean",
            "root-null",
            "root-empty-object",
            "root-empty-array",
            "root-string",
            "root-integer",
            "root-fraction",
            "root-boolean",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            let decoded = serde_fastjson::from_fastjson_str::<CheckClientRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(decoded.is_ok(), java_accepts, "{}: {decoded:?}", case["id"]);
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["fields"],
                    "{}",
                    case["id"]
                );
            }
        }
    }

    #[test]
    fn check_client_inherited_remoting_serialization_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let api = &fixture["checkClientBodyApiCase"];
        let inherited = api["bodyInheritedSerializableMethods"].as_array().unwrap();
        for method in [
            "encode()",
            "encode([Lcom.alibaba.fastjson2.JSONWriter$Feature;)",
            "toJson()",
            "toJson(boolean)",
            "encode(java.lang.Object)",
            "toJson(java.lang.Object,boolean)",
            "decode([B,java.lang.Class)",
            "decodeList([B,java.lang.Class)",
            "fromJson(java.lang.String,java.lang.Class)",
        ] {
            assert!(
                inherited.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(method))
                }),
                "Java inherited method missing from fixture: {method}"
            );
        }

        for case in fixture["checkClientInheritedSerializationCases"]
            .as_array()
            .unwrap()
        {
            let value =
                CheckClientRequestBody::java_decode(case["rawInput"].as_str().unwrap().as_bytes())
                    .unwrap();
            let encoded = value.java_encode().unwrap();
            assert_eq!(
                value.java_encode_with_features(&[]).unwrap(),
                encoded,
                "{} empty writer-feature array",
                case["id"]
            );
            assert_eq!(
                String::from_utf8(encoded.clone()).unwrap(),
                case["instanceEncodedJson"],
                "{} encode",
                case["id"]
            );
            let browser_compatible = value
                .java_encode_with_features(&[JavaJsonWriterFeature::BrowserCompatible])
                .unwrap();
            let expected_browser_compatible = case["writerFeatureOverrides"]["BrowserCompatible"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                browser_compatible,
                expected_browser_compatible.as_bytes(),
                "{} BrowserCompatible",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::OptimizedForAscii])
                    .unwrap(),
                case["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                "{} OptimizedForAscii",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::WriteNameAsSymbol,
                    "WriteNameAsSymbol",
                ),
                (
                    JavaJsonWriterFeature::NotWriteRootClassName,
                    "NotWriteRootClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteHashMapArrayListClassName,
                    "NotWriteHashMapArrayListClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteNumberClassName,
                    "NotWriteNumberClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteSetClassName,
                    "NotWriteSetClassName",
                ),
                (
                    JavaJsonWriterFeature::WriteByteArrayAsBase64,
                    "WriteByteArrayAsBase64",
                ),
                (
                    JavaJsonWriterFeature::WritePairAsJavaBean,
                    "WritePairAsJavaBean",
                ),
                (
                    JavaJsonWriterFeature::WriteThrowableClassName,
                    "WriteThrowableClassName",
                ),
                (
                    JavaJsonWriterFeature::WriterUtilDateAsMillis,
                    "WriterUtilDateAsMillis",
                ),
                (
                    JavaJsonWriterFeature::WriteBigDecimalAsPlain,
                    "WriteBigDecimalAsPlain",
                ),
                (
                    JavaJsonWriterFeature::WriteFloatSpecialAsString,
                    "WriteFloatSpecialAsString",
                ),
                (JavaJsonWriterFeature::BrowserSecure, "BrowserSecure"),
                (
                    JavaJsonWriterFeature::WriteEnumsUsingName,
                    "WriteEnumsUsingName",
                ),
                (
                    JavaJsonWriterFeature::WriteEnumUsingToString,
                    "WriteEnumUsingToString",
                ),
                (
                    JavaJsonWriterFeature::IgnoreErrorGetter,
                    "IgnoreErrorGetter",
                ),
                (
                    JavaJsonWriterFeature::IgnoreNonFieldGetter,
                    "IgnoreNonFieldGetter",
                ),
                (JavaJsonWriterFeature::WriteNulls, "WriteNulls"),
                (
                    JavaJsonWriterFeature::WriteMapNullValue,
                    "WriteMapNullValue",
                ),
                (
                    JavaJsonWriterFeature::WriteNullListAsEmpty,
                    "WriteNullListAsEmpty",
                ),
                (
                    JavaJsonWriterFeature::WriteNullStringAsEmpty,
                    "WriteNullStringAsEmpty",
                ),
                (
                    JavaJsonWriterFeature::WriteNullNumberAsZero,
                    "WriteNullNumberAsZero",
                ),
                (
                    JavaJsonWriterFeature::NullAsDefaultValue,
                    "NullAsDefaultValue",
                ),
                (JavaJsonWriterFeature::WriteClassName, "WriteClassName"),
                (
                    JavaJsonWriterFeature::ReferenceDetection,
                    "ReferenceDetection",
                ),
                (JavaJsonWriterFeature::BeanToArray, "BeanToArray"),
                (JavaJsonWriterFeature::FieldBased, "FieldBased"),
                (
                    JavaJsonWriterFeature::WriteNullBooleanAsFalse,
                    "WriteNullBooleanAsFalse",
                ),
                (JavaJsonWriterFeature::MapSortField, "MapSortField"),
                (
                    JavaJsonWriterFeature::SortMapEntriesByKeys,
                    "SortMapEntriesByKeys",
                ),
                (JavaJsonWriterFeature::LargeObject, "LargeObject"),
            ] {
                let featured = value.java_encode_with_features(&[feature]).unwrap();
                let expected = case["writerFeatureOverrides"][feature_name]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
                assert_eq!(
                    featured,
                    expected.as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }
            assert_eq!(
                value
                    .java_encode_with_features(&[
                        JavaJsonWriterFeature::BrowserSecure,
                        JavaJsonWriterFeature::UnquoteFieldName,
                    ])
                    .unwrap(),
                case["writerFeatureOverrides"]["BrowserSecure+UnquoteFieldName"]
                    .as_str()
                    .unwrap()
                    .as_bytes(),
                "{} BrowserSecure+UnquoteFieldName",
                case["id"]
            );
            let featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteNonStringValueAsString])
                .unwrap();
            let expected_featured = case["writerFeatureOverrides"]["WriteNonStringValueAsString"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                featured,
                expected_featured.as_bytes(),
                "{} WriteNonStringValueAsString",
                case["id"]
            );
            let boolean_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteBooleanAsNumber])
                .unwrap();
            let expected_boolean_featured = case["writerFeatureOverrides"]["WriteBooleanAsNumber"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                boolean_featured,
                expected_boolean_featured.as_bytes(),
                "{} WriteBooleanAsNumber",
                case["id"]
            );
            let combined_featured = value
                .java_encode_with_features(&[
                    JavaJsonWriterFeature::WriteBooleanAsNumber,
                    JavaJsonWriterFeature::WriteNonStringValueAsString,
                ])
                .unwrap();
            let expected_combined_featured =
                case["writerFeatureOverrides"]["WriteBooleanAsNumber+WriteNonStringValueAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                combined_featured,
                expected_combined_featured.as_bytes(),
                "{} combined boolean writer features",
                case["id"]
            );
            let escaped_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                .unwrap();
            let expected_escaped_featured = case["writerFeatureOverrides"]["EscapeNoneAscii"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                escaped_featured,
                expected_escaped_featured.as_bytes(),
                "{} EscapeNoneAscii",
                case["id"]
            );
            let pretty_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::PrettyFormat])
                .unwrap();
            let expected_pretty_featured = case["writerFeatureOverrides"]["PrettyFormat"]
                .as_str()
                .unwrap_or_else(|| case["instancePrettyJson"].as_str().unwrap());
            assert_eq!(
                pretty_featured,
                expected_pretty_featured.as_bytes(),
                "{} PrettyFormat",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::WriteLongAsString])
                    .unwrap(),
                case["writerFeatureOverrides"]["WriteLongAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap())
                    .as_bytes(),
                "{} WriteLongAsString",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::PrettyFormatWith2Space,
                    "PrettyFormatWith2Space",
                ),
                (
                    JavaJsonWriterFeature::PrettyFormatWith4Space,
                    "PrettyFormatWith4Space",
                ),
            ] {
                assert_eq!(
                    value.java_encode_with_features(&[feature]).unwrap(),
                    case["writerFeatureOverrides"][feature_name]
                        .as_str()
                        .unwrap()
                        .as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }
            assert_eq!(
                String::from_utf8(
                    CheckClientRequestBody::java_encode_optional(Some(&value))
                        .unwrap()
                        .unwrap()
                )
                .unwrap(),
                case["staticEncodedJson"],
                "{} static encode",
                case["id"]
            );
            assert_eq!(
                value.java_to_json(false).unwrap(),
                case["instanceJson"],
                "{} compact JSON",
                case["id"]
            );
            assert_eq!(
                value.java_to_json(true).unwrap(),
                case["instancePrettyJson"],
                "{} pretty JSON",
                case["id"]
            );
            assert_eq!(
                CheckClientRequestBody::java_to_json_optional(Some(&value), false).unwrap(),
                case["staticJson"],
                "{} static JSON",
                case["id"]
            );
            assert_eq!(
                CheckClientRequestBody::java_decode(&encoded)
                    .unwrap()
                    .java_to_json(false)
                    .unwrap(),
                case["decodedJson"],
                "{} decode",
                case["id"]
            );
            assert_eq!(
                CheckClientRequestBody::java_from_json(case["instanceJson"].as_str().unwrap())
                    .unwrap()
                    .java_to_json(false)
                    .unwrap(),
                case["fromJsonJson"],
                "{} fromJson",
                case["id"]
            );
            let list = format!("[{}]", case["instanceJson"].as_str().unwrap());
            assert_eq!(
                CheckClientRequestBody::java_decode_list(list.as_bytes()).unwrap()[0]
                    .java_to_json(false)
                    .unwrap(),
                case["decodedListJson"],
                "{} decodeList",
                case["id"]
            );
        }

        let nulls = &fixture["checkClientInheritedSerializationNullUtilities"];
        assert_eq!(
            CheckClientRequestBody::java_encode_optional(None).unwrap(),
            None
        );
        assert_eq!(
            CheckClientRequestBody::java_to_json_optional(None, false).unwrap(),
            nulls["staticToJsonNull"]
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_nullable(None)
                .unwrap()
                .is_none(),
            nulls["decodeNullBytesReturnsNull"]
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none(),
            nulls["decodeJsonNullReturnsNull"]
        );
        assert!(
            CheckClientRequestBody::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            CheckClientRequestBody::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_list_nullable(None)
                .unwrap()
                .is_none(),
            nulls["decodeListNullBytesReturnsNull"]
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none(),
            nulls["decodeListJsonNullReturnsNull"]
        );
        let list = CheckClientRequestBody::java_decode_list_nullable(Some(b"[null,{}]"))
            .unwrap()
            .unwrap();
        assert_eq!(list.len(), nulls["decodeListNullMemberCount"]);
        assert_eq!(list[0].is_none(), nulls["decodeListPreservesNullMember"]);
    }

    #[test]
    fn escape_none_ascii_matches_invoked_java_for_inherited_body_serializers() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let case = &fixture["remotingSerializableWriterEscapingCase"];
        let raw_input = case["rawInput"].as_str().unwrap();
        let value = CheckClientRequestBody::java_decode(raw_input.as_bytes()).unwrap();
        assert_eq!(
            String::from_utf8(value.java_encode().unwrap()).unwrap(),
            case["instanceEncodedJson"]
        );
        assert_eq!(
            String::from_utf8(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                    .unwrap()
            )
            .unwrap(),
            case["escapeNoneAsciiJson"]
        );

        let generic_body: serde_json::Value = serde_json::from_str(raw_input).unwrap();
        assert_eq!(
            java_fastjson_remoting_body_json_with_features(
                &generic_body,
                false,
                &[JavaJsonWriterFeature::EscapeNoneAscii],
            )
            .unwrap(),
            case["escapeNoneAsciiJson"]
        );
    }

    #[test]
    fn remoting_serializable_body_adapters_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let timezone = match std::env::var("TZ").as_deref() {
            Ok("Asia/Taipei") => "Asia/Taipei",
            Ok("UTC" | "Etc/UTC" | "GMT" | "Etc/GMT") | Err(_) => "UTC",
            Ok(other) => panic!(
                "runtime lookup fixture has timestamp strings for UTC and Asia/Taipei; TZ={other}"
            ),
        };

        macro_rules! assert_body_serialization {
            ($dto:ty, $name:literal) => {{
                let dto_case = &fixture["remotingSerializableBodyCases"][$name];
                let methods = dto_case["inheritedMethods"].as_array().unwrap();
                assert_eq!(methods.len(), 9, "{} inherited method inventory", $name);
                for case in dto_case["cases"].as_array().unwrap() {
                    let expected = case["byTimeZone"].get(timezone).unwrap_or(case);
                    let value =
                        <$dto>::java_decode(case["rawInput"].as_str().unwrap().as_bytes()).unwrap();
                    let encoded = value.java_encode().unwrap();
                    assert_eq!(
                        value.java_encode_with_features(&[]).unwrap(),
                        encoded,
                        "{} {} empty writer-feature array",
                        $name,
                        case["id"]
                    );
                    let browser_compatible = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::BrowserCompatible])
                        .unwrap();
                    let expected_browser_compatible = expected["writerFeatureOverrides"]
                        ["BrowserCompatible"]
                        .as_str()
                        .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        browser_compatible,
                        expected_browser_compatible.as_bytes(),
                        "{} {} BrowserCompatible",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::OptimizedForAscii,
                            ])
                            .unwrap(),
                        expected["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                        "{} {} OptimizedForAscii",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        String::from_utf8(encoded.clone()).unwrap(),
                        expected["instanceEncodedJson"],
                        "{} {} instance encode",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        String::from_utf8(
                            <$dto>::java_encode_optional(Some(&value)).unwrap().unwrap()
                        )
                        .unwrap(),
                        expected["staticEncodedJson"],
                        "{} {} static encode",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value.java_to_json(false).unwrap(),
                        expected["instanceJson"],
                        "{} {} instance JSON",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value.java_to_json(true).unwrap(),
                        expected["instancePrettyJson"],
                        "{} {} pretty JSON",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        <$dto>::java_to_json_optional(Some(&value), false).unwrap(),
                        expected["staticJson"],
                        "{} {} static JSON",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        <$dto>::java_decode(&encoded)
                            .unwrap()
                            .java_to_json(false)
                            .unwrap(),
                        expected["decodedJson"],
                        "{} {} decode",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        <$dto>::java_from_json(case["instanceJson"].as_str().unwrap())
                            .unwrap()
                            .java_to_json(false)
                            .unwrap(),
                        expected["fromJsonJson"],
                        "{} {} fromJson",
                        $name,
                        case["id"]
                    );
                    let encoded_list = format!("[{}]", case["instanceJson"].as_str().unwrap());
                    assert_eq!(
                        <$dto>::java_decode_list(encoded_list.as_bytes()).unwrap()[0]
                            .java_to_json(false)
                            .unwrap(),
                        expected["decodedListJson"],
                        "{} {} decodeList",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        expected["featureEncodedJson"], expected["instanceEncodedJson"],
                        "{} {} empty writer features",
                        $name, case["id"]
                    );
                    let featured = value
                        .java_encode_with_features(&[
                            JavaJsonWriterFeature::WriteNonStringValueAsString,
                        ])
                        .unwrap();
                    let expected_featured =
                        expected["writerFeatureOverrides"]["WriteNonStringValueAsString"]
                            .as_str()
                            .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        featured,
                        expected_featured.as_bytes(),
                        "{} {} WriteNonStringValueAsString",
                        $name,
                        case["id"]
                    );
                    let boolean_featured = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::WriteBooleanAsNumber])
                        .unwrap();
                    let expected_boolean_featured =
                        expected["writerFeatureOverrides"]["WriteBooleanAsNumber"]
                            .as_str()
                            .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        boolean_featured,
                        expected_boolean_featured.as_bytes(),
                        "{} {} WriteBooleanAsNumber",
                        $name,
                        case["id"]
                    );
                    let combined_featured = value
                        .java_encode_with_features(&[
                            JavaJsonWriterFeature::WriteBooleanAsNumber,
                            JavaJsonWriterFeature::WriteNonStringValueAsString,
                        ])
                        .unwrap();
                    let expected_combined_featured = expected[
                        "writerFeatureOverrides"
                    ]["WriteBooleanAsNumber+WriteNonStringValueAsString"]
                        .as_str()
                        .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        combined_featured,
                        expected_combined_featured.as_bytes(),
                        "{} {} combined boolean writer features",
                        $name,
                        case["id"]
                    );
                    let escaped_featured = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                        .unwrap();
                    let expected_escaped_featured = expected["writerFeatureOverrides"][
                        "EscapeNoneAscii"
                    ]
                        .as_str()
                        .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        escaped_featured,
                        expected_escaped_featured.as_bytes(),
                        "{} {} EscapeNoneAscii",
                        $name,
                        case["id"]
                    );
                    let pretty_featured = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::PrettyFormat])
                        .unwrap();
                    let expected_pretty_featured =
                        expected["writerFeatureOverrides"]["PrettyFormat"]
                            .as_str()
                            .unwrap_or_else(|| expected["instancePrettyJson"].as_str().unwrap());
                    assert_eq!(
                        pretty_featured,
                        expected_pretty_featured.as_bytes(),
                        "{} {} PrettyFormat",
                        $name,
                        case["id"]
                    );
                    for (feature, feature_name) in [
                        (
                            JavaJsonWriterFeature::NotWriteEmptyArray,
                            "NotWriteEmptyArray",
                        ),
                        (JavaJsonWriterFeature::IgnoreEmpty, "IgnoreEmpty"),
                        (
                            JavaJsonWriterFeature::WriteNonStringKeyAsString,
                            "WriteNonStringKeyAsString",
                        ),
                        (JavaJsonWriterFeature::WriteLongAsString, "WriteLongAsString"),
                        (
                            JavaJsonWriterFeature::WriteEnumUsingOrdinal,
                            "WriteEnumUsingOrdinal",
                        ),
                        (
                            JavaJsonWriterFeature::WriteEnumsUsingName,
                            "WriteEnumsUsingName",
                        ),
                        (
                            JavaJsonWriterFeature::WriteEnumUsingToString,
                            "WriteEnumUsingToString",
                        ),
                        (
                            JavaJsonWriterFeature::IgnoreErrorGetter,
                            "IgnoreErrorGetter",
                        ),
                        (
                            JavaJsonWriterFeature::IgnoreNonFieldGetter,
                            "IgnoreNonFieldGetter",
                        ),
                        (JavaJsonWriterFeature::WriteNulls, "WriteNulls"),
                        (
                            JavaJsonWriterFeature::WriteMapNullValue,
                            "WriteMapNullValue",
                        ),
                        (
                            JavaJsonWriterFeature::WriteNullListAsEmpty,
                            "WriteNullListAsEmpty",
                        ),
                        (
                            JavaJsonWriterFeature::WriteNullStringAsEmpty,
                            "WriteNullStringAsEmpty",
                        ),
                        (
                            JavaJsonWriterFeature::WriteNullNumberAsZero,
                            "WriteNullNumberAsZero",
                        ),
                        (
                            JavaJsonWriterFeature::NullAsDefaultValue,
                            "NullAsDefaultValue",
                        ),
                        (JavaJsonWriterFeature::WriteClassName, "WriteClassName"),
                        (
                            JavaJsonWriterFeature::ReferenceDetection,
                            "ReferenceDetection",
                        ),
                        (JavaJsonWriterFeature::BeanToArray, "BeanToArray"),
                        (JavaJsonWriterFeature::FieldBased, "FieldBased"),
                        (
                            JavaJsonWriterFeature::WriteNullBooleanAsFalse,
                            "WriteNullBooleanAsFalse",
                        ),
                        (JavaJsonWriterFeature::MapSortField, "MapSortField"),
                        (
                            JavaJsonWriterFeature::SortMapEntriesByKeys,
                            "SortMapEntriesByKeys",
                        ),
                        (JavaJsonWriterFeature::LargeObject, "LargeObject"),
                        (
                            JavaJsonWriterFeature::NotWriteDefaultValue,
                            "NotWriteDefaultValue",
                        ),
                        (
                            JavaJsonWriterFeature::PrettyFormatWith2Space,
                            "PrettyFormatWith2Space",
                        ),
                        (
                            JavaJsonWriterFeature::PrettyFormatWith4Space,
                            "PrettyFormatWith4Space",
                        ),
                        (JavaJsonWriterFeature::UseSingleQuotes, "UseSingleQuotes"),
                        (
                            JavaJsonWriterFeature::WriteNameAsSymbol,
                            "WriteNameAsSymbol",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteRootClassName,
                            "NotWriteRootClassName",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteHashMapArrayListClassName,
                            "NotWriteHashMapArrayListClassName",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteNumberClassName,
                            "NotWriteNumberClassName",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteSetClassName,
                            "NotWriteSetClassName",
                        ),
                        (
                            JavaJsonWriterFeature::WriteByteArrayAsBase64,
                            "WriteByteArrayAsBase64",
                        ),
                        (
                            JavaJsonWriterFeature::WritePairAsJavaBean,
                            "WritePairAsJavaBean",
                        ),
                        (
                            JavaJsonWriterFeature::WriteThrowableClassName,
                            "WriteThrowableClassName",
                        ),
                        (
                            JavaJsonWriterFeature::WriterUtilDateAsMillis,
                            "WriterUtilDateAsMillis",
                        ),
                        (
                            JavaJsonWriterFeature::WriteBigDecimalAsPlain,
                            "WriteBigDecimalAsPlain",
                        ),
                        (
                            JavaJsonWriterFeature::WriteFloatSpecialAsString,
                            "WriteFloatSpecialAsString",
                        ),
                        (JavaJsonWriterFeature::BrowserSecure, "BrowserSecure"),
                        (JavaJsonWriterFeature::UnquoteFieldName, "UnquoteFieldName"),
                        (
                            JavaJsonWriterFeature::IgnoreNoneSerializable,
                            "IgnoreNoneSerializable",
                        ),
                    ] {
                        assert_eq!(
                            value.java_encode_with_features(&[feature]).unwrap(),
                            expected["writerFeatureOverrides"][feature_name]
                                .as_str()
                                .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap())
                                .as_bytes(),
                            "{} {} {feature_name}",
                            $name,
                            case["id"]
                        );
                    }
                    assert_eq!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::BrowserSecure,
                                JavaJsonWriterFeature::UnquoteFieldName,
                            ])
                            .unwrap(),
                        expected["writerFeatureOverrides"]["BrowserSecure+UnquoteFieldName"]
                            .as_str()
                            .unwrap()
                            .as_bytes(),
                        "{} {} BrowserSecure+UnquoteFieldName",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        expected["writerFeatureOverrides"]["ErrorOnNoneSerializable"]["error"],
                        "JSONException",
                        "{} {} Java error feature result",
                        $name,
                        case["id"]
                    );
                    assert!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::ErrorOnNoneSerializable,
                            ])
                            .is_err(),
                        "{} {} ErrorOnNoneSerializable",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::ErrorOnNoneSerializable,
                                JavaJsonWriterFeature::IgnoreNoneSerializable,
                            ])
                            .unwrap(),
                        b"null",
                        "{} {} IgnoreNoneSerializable takes precedence",
                        $name,
                        case["id"]
                    );
                }

                let nulls = &fixture["remotingSerializableBodyNullUtilities"][$name];
                assert!(<$dto>::java_encode_optional(None).unwrap().is_none());
                assert_eq!(
                    <$dto>::java_to_json_optional(None, false).unwrap(),
                    nulls["staticToJsonNull"]
                );
                assert_eq!(
                    <$dto>::java_decode_nullable(None).unwrap().is_none(),
                    nulls["decodeNullBytesReturnsNull"]
                );
                assert_eq!(
                    <$dto>::java_decode_nullable(Some(b"null"))
                        .unwrap()
                        .is_none(),
                    nulls["decodeJsonNullReturnsNull"]
                );
                assert!(<$dto>::java_from_json_nullable(None).unwrap().is_none());
                assert!(
                    <$dto>::java_from_json_nullable(Some("null"))
                        .unwrap()
                        .is_none()
                );
                assert_eq!(
                    <$dto>::java_decode_list_nullable(None).unwrap().is_none(),
                    nulls["decodeListNullBytesReturnsNull"]
                );
                assert_eq!(
                    <$dto>::java_decode_list_nullable(Some(b"null"))
                        .unwrap()
                        .is_none(),
                    nulls["decodeListJsonNullReturnsNull"]
                );
                let list = <$dto>::java_decode_list_nullable(Some(b"[null,{}]"))
                    .unwrap()
                    .unwrap();
                assert_eq!(list.len(), nulls["decodeListNullMemberCount"]);
                assert_eq!(list[0].is_none(), nulls["decodeListPreservesNullMember"]);
            }};
        }

        assert_body_serialization!(
            LiteSubscriptionCtlRequestBody,
            "LiteSubscriptionCtlRequestBody"
        );
        assert_body_serialization!(QueryAssignmentRequestBody, "QueryAssignmentRequestBody");
        assert_body_serialization!(QueryAssignmentResponseBody, "QueryAssignmentResponseBody");
        assert_body_serialization!(QueryConsumeTimeSpanBody, "QueryConsumeTimeSpanBody");
        assert_body_serialization!(
            SetMessageRequestModeRequestBody,
            "SetMessageRequestModeRequestBody"
        );
        assert_body_serialization!(
            MessageRequestModeSerializeWrapper,
            "MessageRequestModeSerializeWrapper"
        );
        assert_body_serialization!(LockBatchRequestBody, "LockBatchRequestBody");
        assert_body_serialization!(UnlockBatchRequestBody, "UnlockBatchRequestBody");
        assert_body_serialization!(LockBatchResponseBody, "LockBatchResponseBody");
        assert_body_serialization!(
            ConsumerListByGroupResponseBody,
            "GetConsumerListByGroupResponseBody"
        );
        assert_body_serialization!(ConsumeMessageDirectlyResult, "ConsumeMessageDirectlyResult");
        assert_body_serialization!(GetConsumerStatusBody, "GetConsumerStatusBody");
        assert_body_serialization!(ResetOffsetBody, "ResetOffsetBody");

        macro_rules! assert_decode_cases {
            ($dto:ty, $name:literal) => {{
                let dto_case = &fixture["remotingSerializableBodyCases"][$name];
                for case in dto_case["cases"].as_array().unwrap() {
                    let value =
                        <$dto>::java_decode(case["rawInput"].as_str().unwrap().as_bytes()).unwrap();
                    let actual = serde_json::to_value(value).unwrap();
                    let expected = serde_json::from_str::<serde_json::Value>(
                        case["decodedJson"].as_str().unwrap(),
                    )
                    .unwrap();
                    assert_eq!(actual, expected, "{} {}", $name, case["id"]);
                }
            }};
        }

        assert_decode_cases!(GroupList, "GroupList");
        assert_decode_cases!(TopicList, "TopicList");
        assert_decode_cases!(
            QuerySubscriptionResponseBody,
            "QuerySubscriptionResponseBody"
        );

        let queue_api = &fixture["queueTimeSpanApiCase"];
        assert_eq!(
            queue_api["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan()"])
        );
        assert_eq!(
            queue_api["declaredPublicMethods"],
            json!([
                "java.lang.String org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getConsumeTimeStampStr()",
                "java.lang.String org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getMaxTimeStampStr()",
                "java.lang.String org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getMinTimeStampStr()",
                "long org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getConsumeTimeStamp()",
                "long org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getDelayTime()",
                "long org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getMaxTimeStamp()",
                "long org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getMinTimeStamp()",
                "org.apache.rocketmq.common.message.MessageQueue org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.getMessageQueue()",
                "void org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.setConsumeTimeStamp(long)",
                "void org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.setDelayTime(long)",
                "void org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.setMaxTimeStamp(long)",
                "void org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.setMessageQueue(org.apache.rocketmq.common.message.MessageQueue)",
                "void org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan.setMinTimeStamp(long)"
            ])
        );
        let default_expected = &queue_api["defaultsByTimeZone"][timezone];
        let default_span = QueueTimeSpan::default();
        assert!(
            queue_api["defaultAccessors"]["messageQueueNull"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(default_span.get_message_queue(), None);
        for (rust_value, key) in [
            (default_span.get_min_time_stamp(), "minTimeStamp"),
            (default_span.get_max_time_stamp(), "maxTimeStamp"),
            (default_span.get_consume_time_stamp(), "consumeTimeStamp"),
            (default_span.get_delay_time(), "delayTime"),
        ] {
            assert_eq!(
                rust_value,
                queue_api["defaultAccessors"][key].as_i64().unwrap()
            );
        }
        assert_eq!(
            default_span.get_min_time_stamp_str(),
            default_expected["minTimeStampStr"]
        );
        assert_eq!(
            default_span.get_max_time_stamp_str(),
            default_expected["maxTimeStampStr"]
        );
        assert_eq!(
            default_span.get_consume_time_stamp_str(),
            default_expected["consumeTimeStampStr"]
        );
        let expected = &queue_api["valuesByTimeZone"][timezone];
        let mut span = QueueTimeSpan::default();
        let assigned_queue = SharedValue::new(MessageQueue::new("orders", "broker-a", 1));
        span.set_message_queue(Some(assigned_queue.clone()));
        span.set_min_time_stamp(1_000);
        span.set_max_time_stamp(9_000);
        span.set_consume_time_stamp(5_000);
        span.set_delay_time(4_000);
        let returned_queue = span.get_message_queue().unwrap();
        assert!(
            queue_api["accessorValues"]["messageQueueReturnsAssignedObject"]
                .as_bool()
                .unwrap()
        );
        assert!(returned_queue.ptr_eq(&assigned_queue));
        assert!(
            returned_queue
                .with(|queue| { queue.topic() == queue_api["accessorValues"]["topic"].as_str() })
        );
        assert!(returned_queue.with(|queue| {
            queue.broker_name() == queue_api["accessorValues"]["brokerName"].as_str()
        }));
        assert_eq!(
            returned_queue.with(|queue| queue.queue_id()),
            queue_api["accessorValues"]["queueId"].as_i64().unwrap() as i32
        );
        assert_eq!(span.get_min_time_stamp_str(), expected["minTimeStampStr"]);
        assert_eq!(span.get_max_time_stamp_str(), expected["maxTimeStampStr"]);
        assert_eq!(
            span.get_consume_time_stamp_str(),
            expected["consumeTimeStampStr"]
        );
        let serialized = serde_json::to_value(&span).unwrap();
        assert_eq!(serialized["minTimeStampStr"], expected["minTimeStampStr"]);
        assert_eq!(serialized["maxTimeStampStr"], expected["maxTimeStampStr"]);
        assert_eq!(
            serialized["consumeTimeStampStr"],
            expected["consumeTimeStampStr"]
        );
        for (rust_value, key) in [
            (span.get_min_time_stamp(), "minTimeStamp"),
            (span.get_max_time_stamp(), "maxTimeStamp"),
            (span.get_consume_time_stamp(), "consumeTimeStamp"),
            (span.get_delay_time(), "delayTime"),
        ] {
            assert_eq!(
                rust_value,
                queue_api["accessorValues"][key].as_i64().unwrap()
            );
        }
        returned_queue.with_mut(|queue| queue.set_queue_id(7));
        assert!(
            queue_api["messageQueueGetterMutationVisibleToAssignedObject"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(assigned_queue.with(|queue| queue.queue_id()), 7);
        span.set_message_queue(None);
        assert!(
            queue_api["nullMessageQueueSetterPreservesNull"]
                .as_bool()
                .unwrap()
        );
        assert!(span.get_message_queue().is_none());

        let body_api = &queue_api["queryConsumeTimeSpanBodyApi"];
        assert_eq!(
            body_api["publicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody()"])
        );
        assert_eq!(
            body_api["declaredPublicMethods"],
            json!([
                "java.util.List org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody.getConsumeTimeSpanSet()",
                "void org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody.setConsumeTimeSpanSet(java.util.List)"
            ])
        );
        assert!(body_api["defaultListNonNull"].as_bool().unwrap());
        assert!(body_api["defaultListEmpty"].as_bool().unwrap());
        assert!(
            body_api["defaultGetterReturnsStoredList"]
                .as_bool()
                .unwrap()
        );
        let default_body = QueryConsumeTimeSpanBody::default();
        let default_list = default_body.get_consume_time_span_set().unwrap();
        assert!(default_list.snapshot().is_empty());
        assert!(default_list.ptr_eq(&default_body.get_consume_time_span_set().unwrap()));

        let assigned_span = SharedValue::new(QueueTimeSpan::default());
        let assigned_list = SharedValue::new(vec![Some(assigned_span.clone())]);
        let mut body = QueryConsumeTimeSpanBody::default();
        body.set_consume_time_span_set(Some(assigned_list.clone()));
        let returned_list = body.get_consume_time_span_set().unwrap();
        assert!(body_api["setterRetainsAssignedList"].as_bool().unwrap());
        assert!(body_api["getterReturnsAssignedList"].as_bool().unwrap());
        assert!(body_api["getterReturnsAssignedElement"].as_bool().unwrap());
        assert!(returned_list.ptr_eq(&assigned_list));
        assert!(returned_list.with(|values| {
            values[0]
                .as_ref()
                .is_some_and(|value| value.ptr_eq(&assigned_span))
        }));
        returned_list
            .with_mut(|values| values.push(Some(SharedValue::new(QueueTimeSpan::default()))));
        assert!(
            body_api["getterMutationVisibleToAssignedList"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(assigned_list.with(Vec::len), 2);
        assigned_list.with_mut(|values| values.push(None));
        assert!(
            body_api["assignedListMutationVisibleToGetter"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(returned_list.with(Vec::len), 3);
        assert!(returned_list.with(|values| values[2].is_none()));
        body.set_consume_time_span_set(None);
        assert!(body_api["nullSetterPreservesNull"].as_bool().unwrap());
        assert!(body.get_consume_time_span_set().is_none());

        let extreme_expected = &queue_api["extremeValuesByTimeZone"][timezone];
        let mut extremes = QueueTimeSpan::default();
        extremes.set_min_time_stamp(i64::MIN);
        extremes.set_max_time_stamp(i64::MAX);
        extremes.set_consume_time_stamp(i64::MIN);
        assert_eq!(
            extremes.get_min_time_stamp_str(),
            extreme_expected["minTimeStampStr"]
        );
        assert_eq!(
            extremes.get_max_time_stamp_str(),
            extreme_expected["maxTimeStampStr"]
        );
        assert_eq!(
            extremes.get_consume_time_stamp_str(),
            extreme_expected["consumeTimeStampStr"]
        );
        for method in [
            "getMinTimeStampStr",
            "getMaxTimeStampStr",
            "getConsumeTimeStampStr",
        ] {
            assert!(
                queue_api["declaredPublicMethods"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|signature| signature.as_str().unwrap().contains(method))
            );
        }
    }

    #[test]
    fn lock_and_unlock_bodies_preserve_collection_and_boolean_semantics() {
        let lock = LockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            only_this_broker: false,
            mq_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(MessageQueue::new("orders", "broker-a", 0)),
            )]))),
        };
        let unlock = UnlockBatchRequestBody {
            consumer_group: lock.consumer_group.clone(),
            client_id: lock.client_id.clone(),
            mq_set: lock.mq_set.clone(),
            only_this_broker: true,
        };
        let decoded =
            serde_json::from_value::<LockBatchRequestBody>(serde_json::to_value(&lock).unwrap())
                .unwrap();
        assert_eq!(
            decoded.mq_set.as_ref().map(SharedValue::snapshot),
            lock.mq_set.as_ref().map(SharedValue::snapshot)
        );
        let value = serde_json::to_value(unlock).unwrap();
        assert_eq!(value["onlyThisBroker"], true);
        assert_eq!(value["mqSet"][0]["queueId"], 0);
    }

    #[test]
    fn consumer_list_body_keeps_empty_and_nonempty_lists_distinct() {
        assert_eq!(
            serde_json::to_value(ConsumerListByGroupResponseBody::default()).unwrap(),
            json!({})
        );
        let populated = serde_json::from_value::<ConsumerListByGroupResponseBody>(json!({
            "consumerIdList": ["client"]
        }))
        .unwrap();
        assert_eq!(
            populated.get_consumer_id_list().unwrap().snapshot(),
            vec![Some("client".into())]
        );
    }

    #[test]
    fn lock_request_uses_java_message_queue_field_names() {
        let body = LockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            mq_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(MessageQueue::new("orders", "broker-a", 0)),
            )]))),
            ..LockBatchRequestBody::default()
        };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["mqSet"][0]["brokerName"], "broker-a");
        assert_eq!(value["mqSet"][0]["queueId"], 0);
    }

    #[test]
    fn runtime_inbound_bodies_keep_map_and_optional_shapes() {
        let queue = MessageQueue::new("orders", "broker-a", 1);
        let reset = ResetOffsetBody {
            offset_table: Some(SharedValue::new(vec![(queue.clone(), Some(7))])),
        };
        let value = serde_json::to_value(reset).unwrap();
        let offset_table = value["offsetTable"].as_object().unwrap();
        assert_eq!(offset_table.values().next().unwrap(), 7);
        let key: serde_json::Value =
            serde_json::from_str(offset_table.keys().next().unwrap()).unwrap();
        assert_eq!(key["queueId"], 1);

        let status = GetConsumerStatusBody {
            message_queue_table: Some(SharedValue::new(vec![(queue.clone(), Some(7))])),
            consumer_table: Some(SharedValue::new(std::collections::BTreeMap::from([(
                "consumer-a".into(),
                Some(SharedValue::new(vec![(queue, Some(8))])),
            )]))),
        };
        let value = serde_json::to_value(status).unwrap();
        let consumer_table = value["consumerTable"]["consumer-a"].as_object().unwrap();
        assert_eq!(consumer_table.values().next().unwrap(), 8);
        let queue_key: serde_json::Value =
            serde_json::from_str(consumer_table.keys().next().unwrap()).unwrap();
        assert_eq!(queue_key["queueId"], 1);

        let direct: ConsumeMessageDirectlyResult =
            serde_json::from_value(serde_json::json!({"order":false, "spentTimeMills":0})).unwrap();
        let value = serde_json::to_value(direct).unwrap();
        assert_eq!(value["autoCommit"], true);
        assert!(value.get("consumeResult").is_none());
        let direct = ConsumeMessageDirectlyResult {
            consume_result: Some(ConsumeMessageResult::CrSuccess),
            ..ConsumeMessageDirectlyResult::default()
        };
        assert_eq!(
            serde_json::to_value(direct).unwrap()["consumeResult"],
            "CR_SUCCESS"
        );
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn assignment_request_mode_lite_and_batch_ack_match_java_json_shapes() {
        let mut assignment = MessageQueueAssignment::default();
        assignment.set_message_queue(Some(SharedValue::new(MessageQueue::new(
            "orders", "broker-a", 1,
        ))));
        assignment.set_mode(Some(MessageRequestMode::Pop));
        assignment.set_attachments(Some(SharedValue::new(AssignmentAttachments::from(
            std::collections::BTreeMap::from([(Some("zone".into()), Some("z1".into()))]),
        ))));
        let value = serde_json::to_value(assignment).unwrap();
        assert_eq!(value["mode"], "POP");
        assert_eq!(value["messageQueue"]["brokerName"], "broker-a");

        let lite = LiteSubscriptionDto {
            action: Some(LiteSubscriptionAction::CompleteAdd),
            client_id: Some("client".into()),
            group: Some("workers".into()),
            topic: Some("orders".into()),
            lite_topic_set: Some(SharedValue::new(std::collections::BTreeSet::from([Some(
                "lite-orders".into(),
            )]))),
            offset_option: Some(SharedValue::new(OffsetOption {
                option_type: Some(OffsetOptionType::Timestamp),
                value: 99,
            })),
            version: 2,
        };
        let value = serde_json::to_value(lite).unwrap();
        assert_eq!(value["action"], "COMPLETE_ADD");
        assert_eq!(value["offsetOption"]["type"], "TIMESTAMP");

        let body = BatchAckMessageRequestBody {
            broker_name: Some("broker-a".into()),
            acks: Some(SharedValue::new(vec![Some(SharedValue::new(BatchAck {
                consumer_group: Some("workers".into()),
                topic: Some("orders".into()),
                retry: Some("0".into()),
                start_offset: 100,
                queue_id: 1,
                revive_queue_id: 0,
                pop_time: 10,
                invisible_time: 20,
                bit_set: Some(SharedValue::new(BitSet::from_byte_array(&[0b0000_0101]))),
            }))])),
        };
        let value = serde_json::to_value(&body).unwrap();
        let ack = &value["acks"][0];
        assert_eq!(ack["c"], "workers");
        assert_eq!(ack["t"], "orders");
        assert_eq!(ack["b"], "BQ==");
        let round_trip: BatchAckMessageRequestBody = serde_json::from_value(value).unwrap();
        assert_eq!(
            round_trip.acks.unwrap().snapshot()[0]
                .as_ref()
                .unwrap()
                .with(|ack| ack.bit_set.as_ref().unwrap().snapshot()),
            BitSet::from_byte_array(&[0b0000_0101])
        );
    }

    #[test]
    fn assignment_mode_inputs_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["assignmentModeCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            20,
            "Java oracle includes decimal ordinal boundaries"
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "fractional-ordinal-precision-boundary")
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "decimal-ordinal-wraps-to-zero")
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "integer-ordinal-wrap-boundary")
        );
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let assignment = decoded
                    .message_queue_assignments
                    .as_ref()
                    .and_then(|assignments| assignments.snapshot().into_iter().next())
                    .flatten()
                    .unwrap()
                    .snapshot();
                let mode = assignment
                    .mode()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or(serde_json::Value::Null);
                assert_eq!(mode, case["mode"], "{}", case["name"]);

                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
        assert_eq!(
            MessageQueueAssignment::default().mode(),
            Some(MessageRequestMode::Pull)
        );
    }

    #[test]
    fn assignment_attachment_inputs_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["assignmentAttachmentCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            7,
            "Java oracle includes numeric string coercions"
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "numeric-string-values")
        );
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let assignment = decoded
                    .message_queue_assignments
                    .as_ref()
                    .and_then(|assignments| assignments.snapshot().into_iter().next())
                    .flatten()
                    .unwrap()
                    .snapshot();
                assert_eq!(
                    serde_json::to_value(assignment.attachments()).unwrap(),
                    case["attachments"],
                    "{}",
                    case["name"]
                );

                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn consumer_list_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumerListCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            18,
            "Java oracle includes high-precision numeric strings, duplicate fields, and EOF-marker suffixes"
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "high-precision-numeric-strings")
        );
        for case in cases {
            let decoded = ConsumerListByGroupResponseBody::java_decode(
                case["rawInput"].as_str().unwrap().as_bytes(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded.get_consumer_id_list()).unwrap(),
                    case["ids"],
                    "{}",
                    case["name"]
                );
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn consumer_list_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumerListRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<ConsumerListByGroupResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded.get_consumer_id_list()).unwrap(),
                    case["ids"],
                    "{}",
                    case["name"]
                );
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn consumer_status_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumerStatusBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 143);
        let rows = |entries: Option<&[(MessageQueue, Option<i64>)]>| {
            let Some(entries) = entries else {
                return serde_json::Value::Null;
            };
            let mut rows = entries
                .iter()
                .map(|(queue, offset)| {
                    serde_json::json!({
                        "topic": queue.topic(),
                        "brokerName": queue.broker_name(),
                        "queueId": queue.queue_id(),
                        "offset": offset
                    })
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(serde_json::Value::to_string);
            serde_json::Value::Array(rows)
        };
        let mut mismatches = Vec::new();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let decoded =
                GetConsumerStatusBody::decode_java_json(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "ACCEPTED";
            match (decoded, java_accepts) {
                (Ok(decoded), true) => {
                    let encoded = String::from_utf8(decoded.encode_java_json().unwrap()).unwrap();
                    if encoded != case["encodedBody"] {
                        mismatches.push(format!(
                            "{name}: encoded body {encoded} != Java {}",
                            case["encodedBody"]
                        ));
                    }
                    let actual_message_queue_table = decoded
                        .message_queue_table
                        .as_ref()
                        .map(|table| table.with(|table| rows(Some(table))))
                        .unwrap_or(serde_json::Value::Null);
                    if actual_message_queue_table != case["messageQueueTable"] {
                        mismatches.push(format!(
                            "{name}: messageQueueTable {} != Java {}",
                            actual_message_queue_table, case["messageQueueTable"]
                        ));
                    }
                    let actual_consumer_table = decoded.consumer_table.as_ref().map(|table| {
                        table.with(|table| {
                            table
                                .iter()
                                .map(|(consumer, entries)| {
                                    (
                                        consumer.clone(),
                                        entries
                                            .as_ref()
                                            .map(|entries| {
                                                entries.with(|entries| rows(Some(entries)))
                                            })
                                            .unwrap_or(serde_json::Value::Null),
                                    )
                                })
                                .collect::<std::collections::BTreeMap<_, _>>()
                        })
                    });
                    if serde_json::to_value(&actual_consumer_table).unwrap()
                        != case["consumerTable"]
                    {
                        mismatches.push(format!(
                            "{name}: consumerTable {} != Java {}",
                            serde_json::to_value(actual_consumer_table).unwrap(),
                            case["consumerTable"]
                        ));
                    }
                }
                (Ok(_), false) => mismatches.push(format!("{name}: Rust accepted, Java rejected")),
                (Err(error), true) => {
                    mismatches.push(format!("{name}: Rust rejected ({error}), Java accepted"))
                }
                (Err(_), false) => {}
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn consumer_status_body_accessors_keep_java_null_and_empty_states() {
        let mut body = GetConsumerStatusBody::default();
        assert!(body.get_message_queue_table().unwrap().with(Vec::is_empty));
        assert!(
            body.get_consumer_table()
                .unwrap()
                .with(std::collections::BTreeMap::is_empty)
        );

        let queue = MessageQueue::new("orders", "broker-a", 1);
        let message_queue_table = SharedValue::new(vec![(queue.clone(), Some(9))]);
        let consumer_queue_table = SharedValue::new(vec![(queue.clone(), Some(8))]);
        let consumer_table = SharedValue::new(std::collections::BTreeMap::from([(
            "consumer-a".to_owned(),
            Some(consumer_queue_table.clone()),
        )]));
        body.set_message_queue_table(Some(message_queue_table.clone()));
        body.set_consumer_table(Some(consumer_table.clone()));
        assert!(
            body.get_message_queue_table()
                .unwrap()
                .ptr_eq(&message_queue_table)
        );
        assert!(body.get_consumer_table().unwrap().ptr_eq(&consumer_table));
        assert!(consumer_table.with(|table| {
            table["consumer-a"]
                .as_ref()
                .unwrap()
                .ptr_eq(&consumer_queue_table)
        }));
        assert_eq!(
            body.get_message_queue_table().unwrap().snapshot(),
            vec![(queue.clone(), Some(9))]
        );
        assert_eq!(
            body.get_consumer_table()
                .unwrap()
                .with(|table| table["consumer-a"].as_ref().unwrap().snapshot()),
            vec![(queue, Some(8))]
        );

        let queue = MessageQueue::new("orders", "broker-a", 1);
        body.set_message_queue_table(Some(SharedValue::new(vec![(queue.clone(), None)])));
        body.set_consumer_table(Some(SharedValue::new(std::collections::BTreeMap::from([
            (
                "consumer-a".to_owned(),
                Some(SharedValue::new(vec![(queue.clone(), None)])),
            ),
        ]))));
        assert_eq!(
            body.get_message_queue_table().unwrap().snapshot(),
            vec![(queue.clone(), None)]
        );
        assert_eq!(
            body.get_consumer_table()
                .unwrap()
                .with(|table| table["consumer-a"].as_ref().unwrap().snapshot()),
            vec![(queue, None)]
        );

        body.set_message_queue_table(None);
        body.set_consumer_table(None);
        assert!(body.get_message_queue_table().is_none());
        assert!(body.get_consumer_table().is_none());
    }

    #[test]
    fn reset_offset_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["resetOffsetBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 77);
        let rows = |entries: Option<&[(MessageQueue, Option<i64>)]>| {
            let Some(entries) = entries else {
                return serde_json::Value::Null;
            };
            let mut rows = entries
                .iter()
                .map(|(queue, offset)| {
                    serde_json::json!({
                        "topic": queue.topic(),
                        "brokerName": queue.broker_name(),
                        "queueId": queue.queue_id(),
                        "offset": offset
                    })
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(serde_json::Value::to_string);
            serde_json::Value::Array(rows)
        };
        let mut mismatches = Vec::new();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let decoded = ResetOffsetBody::decode_java_json(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "ACCEPTED";
            match (decoded, java_accepts) {
                (Ok(decoded), true) => {
                    let actual_offset_table = decoded
                        .offset_table
                        .as_ref()
                        .map(|table| table.with(|table| rows(Some(table))))
                        .unwrap_or(serde_json::Value::Null);
                    if actual_offset_table != case["offsetTable"] {
                        mismatches.push(format!(
                            "{name}: offsetTable {} != Java {}",
                            actual_offset_table, case["offsetTable"]
                        ));
                    }
                    let encoded = String::from_utf8(decoded.encode_java_json().unwrap()).unwrap();
                    if encoded != case["encodedBody"] {
                        mismatches.push(format!(
                            "{name}: encoded body {encoded} != Java {}",
                            case["encodedBody"]
                        ));
                    }
                }
                (Ok(_), false) => mismatches.push(format!("{name}: Rust accepted, Java rejected")),
                (Err(error), true) => {
                    mismatches.push(format!("{name}: Rust rejected ({error}), Java accepted"))
                }
                (Err(_), false) => {}
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn assignment_and_request_mode_json_preserve_optional_fields_and_defaults() {
        let query: QueryAssignmentRequestBody = serde_json::from_value(serde_json::json!({
            "topic":"orders", "consumerGroup":"workers", "clientId":"client",
            "strategyName":"AllocateMessageQueueAveragely", "messageModel":"CLUSTERING"
        }))
        .unwrap();
        assert_eq!(query.topic.as_deref(), Some("orders"));

        let mode: SetMessageRequestModeRequestBody = serde_json::from_value(serde_json::json!({
            "topic":"orders", "consumerGroup":"workers", "popShareQueueNum":0
        }))
        .unwrap();
        assert_eq!(mode.mode, Some(MessageRequestMode::Pull));
        assert_eq!(serde_json::to_value(mode).unwrap()["mode"], "PULL");

        let absent: LiteSubscriptionCtlRequestBody =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(serde_json::to_value(absent).unwrap(), serde_json::json!({}));
    }

    #[test]
    fn assignment_body_inputs_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["assignmentBodyCases"].as_array().unwrap();
        assert_eq!(cases.len(), 33);
        for required in [
            "fraction-field",
            "string-element",
            "number-element",
            "boolean-element",
            "array-element",
            "nested-array-object-element",
            "mixed-null-object-elements",
            "mixed-scalar-object-elements",
            "message-queue-null",
            "message-queue-string",
            "message-queue-number",
            "message-queue-boolean",
            "message-queue-array",
            "message-queue-empty-object",
            "message-queue-object",
            "same-mode-attachment-hash-collision",
            "same-mode-different-attachment-hash",
            "duplicate-null-queue-assignments",
        ] {
            assert!(
                cases.iter().any(|case| case["name"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let actual_assignments = decoded.message_queue_assignments.as_ref().map(|items| {
                    items
                        .snapshot()
                        .iter()
                        .map(|item| match item {
                            Some(item) => {
                                let item = item.snapshot();
                                serde_json::json!({
                                    "attachments": item.attachments().map(|value| value.snapshot()),
                                    "messageQueue": item.message_queue().map(|queue| queue.with(|queue| {
                                        serde_json::json!({
                                            "topic": queue.topic(),
                                            "brokerName": queue.broker_name(),
                                            "queueId": queue.queue_id(),
                                        })
                                    })),
                                    "mode": item.mode(),
                                })
                            },
                            None => serde_json::Value::Null,
                        })
                        .collect::<Vec<_>>()
                });
                let mut actual_assignments = serde_json::to_value(actual_assignments).unwrap();
                let mut expected_assignments = case["assignments"].clone();
                normalize(&mut actual_assignments);
                normalize(&mut expected_assignments);
                assert_eq!(actual_assignments, expected_assignments, "{}", case["name"]);

                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn assignment_body_root_shapes_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["assignmentBodyRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let actual_assignments = decoded.message_queue_assignments.as_ref().map(|items| {
                    items
                        .snapshot()
                        .iter()
                        .map(|item| match item {
                            Some(item) => {
                                let item = item.snapshot();
                                serde_json::json!({
                                    "attachments": item.attachments().map(|value| value.snapshot()),
                                    "messageQueue": item.message_queue().map(|queue| queue.with(|queue| {
                                        serde_json::json!({
                                            "topic": queue.topic(),
                                            "brokerName": queue.broker_name(),
                                            "queueId": queue.queue_id(),
                                        })
                                    })),
                                    "mode": item.mode(),
                                })
                            },
                            None => serde_json::Value::Null,
                        })
                        .collect::<Vec<_>>()
                });
                let mut actual_assignments = serde_json::to_value(actual_assignments).unwrap();
                let mut expected_assignments = case["assignments"].clone();
                normalize(&mut actual_assignments);
                normalize(&mut expected_assignments);
                assert_eq!(actual_assignments, expected_assignments, "{}", case["name"]);

                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn message_request_mode_wrapper_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["messageRequestModeWrapperCases"]
            .as_array()
            .unwrap();
        batch_body_cases_match_java::<MessageRequestModeSerializeWrapper>(cases, 33);
        let default_encoded =
            serde_json::to_value(MessageRequestModeSerializeWrapper::default()).unwrap();
        assert_eq!(
            default_encoded,
            cases
                .iter()
                .find(|case| case["name"] == "default-empty-map")
                .unwrap()["encoded"],
            "Rust Default should match Java's no-argument wrapper values"
        );

        let api = &fixture["messageRequestModeWrapperApiCase"];
        assert_eq!(api["bodyPublicFields"], json!([]));
        let mut wrapper = MessageRequestModeSerializeWrapper::default();
        assert_eq!(api["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(api["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(api["sameInstanceEquals"], true);
        assert_eq!(api["equalValuesDistinctInstanceEquals"], false);
        assert_eq!(api["defaultMapNonNull"], true);
        assert_eq!(api["defaultMapSize"], 0);
        assert_eq!(
            wrapper
                .get_message_request_mode_map()
                .unwrap()
                .with(std::collections::BTreeMap::len),
            0
        );
        wrapper.set_message_request_mode_map(None);
        assert_eq!(api["nullSetterRemainsNull"], true);
        assert!(wrapper.get_message_request_mode_map().is_none());
        let assigned_body = SharedValue::new(SetMessageRequestModeRequestBody {
            topic: Some("orders".into()),
            consumer_group: Some("workers".into()),
            mode: Some(MessageRequestMode::Pop),
            pop_share_queue_num: 2,
        });
        let assigned_group = SharedValue::new(std::collections::BTreeMap::from([(
            "workers".into(),
            assigned_body.clone(),
        )]));
        let assigned_map = SharedValue::new(std::collections::BTreeMap::from([(
            "orders".into(),
            assigned_group.clone(),
        )]));
        wrapper.set_message_request_mode_map(Some(assigned_map.clone()));
        let getter_map = wrapper.get_message_request_mode_map().unwrap();
        let getter_group = getter_map.with(|map| map.get("orders").unwrap().clone());
        let getter_body = getter_group.with(|groups| groups.get("workers").unwrap().clone());
        assert!(assigned_map.ptr_eq(&getter_map));
        assert!(assigned_group.ptr_eq(&getter_group));
        assert!(assigned_body.ptr_eq(&getter_body));
        assert_eq!(api["setterRetainsMapIdentity"], true);
        assert_eq!(api["setterRetainsGroupMapIdentity"], true);
        assert_eq!(api["getterReturnsAssignedBody"], true);

        assigned_map.with_mut(|map| {
            map.insert(
                "caller-topic".into(),
                SharedValue::new(std::collections::BTreeMap::new()),
            );
        });
        assert!(getter_map.with(|map| map.contains_key("caller-topic")));
        assigned_map.with_mut(|map| {
            map.remove("caller-topic");
        });
        assigned_group.with_mut(|groups| {
            groups.insert("caller-group".into(), assigned_body.clone());
        });
        assert!(getter_group.with(|groups| groups.contains_key("caller-group")));
        assigned_group.with_mut(|groups| {
            groups.remove("caller-group");
        });
        assigned_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            getter_body.with(SetMessageRequestModeRequestBody::get_mode),
            Some(MessageRequestMode::Pull)
        );
        assigned_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pop)));
        getter_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            assigned_body.with(SetMessageRequestModeRequestBody::get_mode),
            Some(MessageRequestMode::Pull)
        );
        getter_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pop)));
        assert_eq!(api["callerOuterMutationVisible"], true);
        assert_eq!(api["callerGroupMutationVisible"], true);
        assert_eq!(api["callerBodyMutationVisible"], true);
        assert_eq!(api["getterBodyMutationVisibleToSetterObject"], true);
        assert_eq!(
            serde_json::to_value(&wrapper).unwrap()["messageRequestModeMap"],
            api["populatedMap"]
        );
        assert_eq!(api["getterMutationVisible"], true);
        let map = wrapper.get_message_request_mode_map().unwrap();
        map.with_mut(|map| {
            map.entry("archive".into())
                .or_insert_with(|| SharedValue::new(std::collections::BTreeMap::new()));
            let readers = map
                .get("orders")
                .unwrap()
                .with(|groups| groups.get("workers").unwrap().clone());
            map.get("orders")
                .unwrap()
                .with_mut(|groups| groups.insert("readers".into(), readers));
        });
        assert_eq!(
            serde_json::to_value(wrapper).unwrap()["messageRequestModeMap"],
            api["getterMutatedMap"]
        );
    }

    #[test]
    fn lite_subscription_enum_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionEnumCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            33,
            "Java oracle includes lite enum values, duplicate fields, and list shapes"
        );
        for required in [
            "action-duplicate-last-wins",
            "client-id-duplicate-last-wins",
            "group-duplicate-last-wins",
            "topic-duplicate-last-wins",
            "lite-topic-set-duplicate-last-wins",
            "offset-option-duplicate-last-wins",
            "version-duplicate-last-wins",
            "subscription-set-duplicate-last-wins",
            "offset-type-duplicate-last-wins",
            "offset-value-duplicate-last-wins",
            "subscription-set-null",
            "subscription-set-empty",
            "subscription-set-single-object",
            "subscription-set-null-element",
            "subscription-set-string-element",
            "subscription-set-scalar-number",
        ] {
            assert!(
                cases.iter().any(|case| case["name"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_field_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionFieldCases"].as_array().unwrap();
        assert_eq!(cases.len(), 49);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_identity_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionIdentityCases"].as_array().unwrap();
        assert_eq!(cases.len(), 3);
        for (case, expected_class) in cases.iter().zip([
            "LiteSubscriptionDTO",
            "LiteSubscription",
            "LiteSubscriptionCtlRequestBody",
        ]) {
            assert_eq!(case["class"], expected_class);
            assert_eq!(case["sameObjectEquals"], true, "{}", case["class"]);
            assert_eq!(
                case["equalFieldDistinctObjectsEquals"], false,
                "{}",
                case["class"]
            );
            assert_eq!(case["equalFieldHashSetSize"], 2, "{}", case["class"]);
        }
    }

    #[test]
    fn lite_subscription_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let case = &fixture["liteSubscriptionApiCases"][0];
        let mut subscription = LiteSubscription::new();
        let subscription_ptr = std::ptr::addr_of_mut!(subscription);
        assert_eq!(
            subscription.set_group(Some("workers".into())) as *mut LiteSubscription,
            subscription_ptr
        );
        assert_eq!(
            subscription.set_topic(Some("orders".into())) as *mut LiteSubscription,
            subscription_ptr
        );
        assert_eq!(subscription.group(), Some("workers"));
        assert_eq!(subscription.topic(), Some("orders"));
        subscription.set_update_time(case["initialTime"].as_i64().unwrap());

        let mut setter_input = std::collections::BTreeSet::from(["lite-a".to_owned()]);
        assert_eq!(
            subscription.set_lite_topic_set(&setter_input) as *mut LiteSubscription,
            subscription_ptr
        );
        assert_eq!(subscription.update_time(), 7);
        setter_input.insert("setter-only".into());
        assert!(
            subscription
                .lite_topic_set()
                .with(|topics| topics.contains("lite-a"))
        );
        assert!(
            !subscription
                .lite_topic_set()
                .with(|topics| topics.contains("setter-only"))
        );
        let live_topics = subscription.lite_topic_set();
        assert!(live_topics.ptr_eq(&subscription.lite_topic_set()));
        assert!(subscription.add_lite_topic("lite-b"));
        assert!(!subscription.add_lite_topic("lite-b"));
        assert!(subscription.update_time() > 7);
        subscription.add_lite_topics(["lite-c", "lite-d"]);
        assert!(subscription.update_time() > 7);
        assert!(live_topics.with(|topics| topics.contains("lite-c")));
        assert!(subscription.remove_lite_topic("lite-b"));
        subscription.remove_lite_topics(["lite-c", "lite-d"]);
        live_topics.with_mut(|topics| {
            topics.insert("getter-added".into());
        });
        assert!(
            subscription
                .lite_topic_set()
                .with(|topics| topics.contains("getter-added"))
        );
        live_topics.with_mut(|topics| {
            topics.remove("getter-added");
        });

        let before_set_lite_topic_set = subscription.update_time();
        subscription.set_lite_topic_set(["lite-e"]);
        assert!(live_topics.with(|topics| topics.contains("lite-a")));
        assert!(live_topics.with(|topics| topics.contains("lite-e")));
        assert_eq!(subscription.update_time(), before_set_lite_topic_set);
        subscription.remove_lite_topic("lite-e");
        subscription.set_update_time(7);

        assert!(case["groupSetterReturnsThis"].as_bool().unwrap());
        assert!(case["topicSetterReturnsThis"].as_bool().unwrap());
        assert!(case["setLiteTopicSetReturnsThis"].as_bool().unwrap());
        assert!(!case["setLiteTopicSetRetainsInputSet"].as_bool().unwrap());
        assert!(!case["setterInputMutationVisible"].as_bool().unwrap());
        assert!(case["setLiteTopicSetKeepsExisting"].as_bool().unwrap());
        assert!(case["setLiteTopicSetLeavesUpdateTime"].as_bool().unwrap());
        assert!(case["getterReturnsSameSetObject"].as_bool().unwrap());
        assert!(
            case["getterMutationVisibleToSubscription"]
                .as_bool()
                .unwrap()
        );
        assert!(case["singleAddResult"].as_bool().unwrap());
        assert!(!case["duplicateAddResult"].as_bool().unwrap());
        assert!(case["singleAddUpdatesTime"].as_bool().unwrap());
        assert!(case["collectionAddUpdatesTime"].as_bool().unwrap());
        assert!(case["getterReturnsLiveSet"].as_bool().unwrap());
        assert!(case["singleRemoveResult"].as_bool().unwrap());
        assert!(case["collectionRemoveClearsMembers"].as_bool().unwrap());
        assert!(
            case["setLiteTopicSetAddsWithoutClearing"]
                .as_bool()
                .unwrap()
        );
        assert!(case["setLiteTopicSetDoesNotUpdateTime"].as_bool().unwrap());
        assert_eq!(subscription.to_string(), case["toString"].as_str().unwrap());

        let encoded = serde_json::to_value(&subscription).unwrap();
        assert_eq!(encoded["group"], "workers");
        assert_eq!(encoded["topic"], "orders");
        assert_eq!(encoded["liteTopicSet"], serde_json::json!(["lite-a"]));
        assert_eq!(encoded["updateTime"], 7);
        assert!(encoded.get("clientId").is_none());
    }

    #[test]
    fn lite_subscription_fastjson_inputs_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 16);
        for case in cases {
            let decoded =
                serde_json::from_str::<LiteSubscription>(case["rawInput"].as_str().unwrap());
            if case["outcome"] == "REJECTED" {
                assert!(decoded.is_err(), "{}: {decoded:?}", case["name"]);
                continue;
            }

            let decoded = decoded.unwrap_or_else(|error| {
                panic!("{}: {error}", case["name"]);
            });
            assert_eq!(
                serde_json::to_value(decoded.group).unwrap(),
                case["group"],
                "{}",
                case["name"]
            );
            assert_eq!(
                serde_json::to_value(decoded.topic).unwrap(),
                case["topic"],
                "{}",
                case["name"]
            );
            let lite_topics = decoded
                .lite_topic_set
                .snapshot()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(
                serde_json::to_value(lite_topics).unwrap(),
                case["liteTopicSet"],
                "{}",
                case["name"]
            );
            if let Some(update_time) = case["updateTime"].as_i64() {
                assert_eq!(decoded.update_time, update_time, "{}", case["name"]);
            }
            if case["updateTimeWasInitialized"] == true {
                assert!(decoded.update_time > 0, "{}", case["name"]);
            }
        }
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn lite_subscription_related_dto_apis_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();

        let dto_case = &fixture["liteSubscriptionDtoApiCase"];
        let mut dto = LiteSubscriptionDto::default();
        let dto_ptr = std::ptr::addr_of_mut!(dto);
        assert_eq!(
            dto.set_action(Some(LiteSubscriptionAction::CompleteAdd)) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        assert_eq!(
            dto.set_client_id(Some("client".into())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        assert_eq!(
            dto.set_group(Some("workers".into())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        assert_eq!(
            dto.set_topic(Some("orders".into())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        let lite_topics =
            SharedValue::new(std::collections::BTreeSet::from([Some("lite-a".into())]));
        assert_eq!(
            dto.set_lite_topic_set(Some(lite_topics.clone())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        let assigned_option =
            SharedValue::new(OffsetOption::new(Some(OffsetOptionType::Policy), 11));
        dto.set_offset_option(Some(assigned_option.clone()));
        assert_eq!(dto.set_version(9) as *mut LiteSubscriptionDto, dto_ptr);
        assert_eq!(dto.action(), Some(LiteSubscriptionAction::CompleteAdd));
        assert_eq!(dto.client_id(), Some("client"));
        assert_eq!(dto.group(), Some("workers"));
        assert_eq!(dto.topic(), Some("orders"));
        let getter_topics = dto.lite_topic_set().unwrap();
        let getter_option = dto.offset_option().unwrap();
        assert!(lite_topics.ptr_eq(&getter_topics));
        assert!(assigned_option.ptr_eq(&getter_option));
        assert_eq!(dto.version(), 9);
        assert!(dto_case["actionSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["clientIdSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["groupSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["topicSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["liteTopicSetSetterReturnsThis"].as_bool().unwrap());
        assert!(
            dto_case["liteTopicSetGetterReturnsSameSet"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(dto_case["liteTopicSetExternalMutationVisible"], true);
        assert_eq!(
            dto_case["liteTopicSetGetterMutationVisibleToAssignedSet"],
            true
        );
        lite_topics.with_mut(|topics| {
            topics.insert(Some("setter-added".into()));
        });
        getter_topics.with_mut(|topics| {
            topics.insert(Some("getter-added".into()));
        });
        assert_eq!(
            serde_json::to_value(getter_topics.snapshot()).unwrap(),
            dto_case["mutatedLiteTopicSet"]
        );
        assert!(
            dto_case["offsetOptionGetterReturnsSameObject"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(dto_case["offsetOptionExternalMutationVisible"], true);
        assigned_option.with_mut(|option| option.set_value(12));
        assert_eq!(getter_option.with(|option| option.value()), 12);
        assert_eq!(
            dto_case["offsetOptionGetterMutationVisibleToAssignedObject"],
            true
        );
        getter_option.with_mut(|option| option.set_value(13));
        assert_eq!(
            getter_option.with(|option| option.value()),
            dto_case["mutatedOffsetOptionValue"].as_i64().unwrap()
        );
        assert_eq!(
            assigned_option.with(|option| option.value()),
            dto_case["mutatedOffsetOptionValue"].as_i64().unwrap()
        );
        assert_eq!(dto.to_string(), dto_case["toString"].as_str().unwrap());
        assert!(dto_case["versionSetterReturnsThis"].as_bool().unwrap());
        assert_eq!(dto_case["action"], "COMPLETE_ADD");
        assert_eq!(dto_case["clientId"], "client");
        assert_eq!(dto_case["group"], "workers");
        assert_eq!(dto_case["topic"], "orders");
        assert_eq!(dto_case["liteTopicSet"], serde_json::json!(["lite-a"]));
        assert_eq!(dto_case["offsetOptionType"], "POLICY");
        assert_eq!(dto_case["offsetOptionValue"], 11);
        assert_eq!(dto_case["version"], 9);

        let mut option = OffsetOption::default();
        option.set_option_type(Some(OffsetOptionType::TailN));
        option.set_value(23);
        let option_case = &fixture["offsetOptionApiCase"];
        assert_eq!(option.option_type(), Some(OffsetOptionType::TailN));
        assert_eq!(option.value(), 23);
        assert_eq!(
            option.to_string(),
            option_case["toString"].as_str().unwrap()
        );
        assert_eq!(option_case["defaultType"], serde_json::Value::Null);
        assert_eq!(option_case["defaultValue"], 0);
        assert_eq!(
            option_case["policyLastValue"],
            OffsetOption::POLICY_LAST_VALUE
        );
        assert_eq!(
            option_case["policyMinValue"],
            OffsetOption::POLICY_MIN_VALUE
        );
        assert_eq!(
            option_case["policyMaxValue"],
            OffsetOption::POLICY_MAX_VALUE
        );
        assert_eq!(option_case["type"], "TAIL_N");
        assert_eq!(option_case["value"], 23);
        assert!(option_case["equalValue"].as_bool().unwrap());
        assert!(!option_case["differentValue"].as_bool().unwrap());
        assert!(!option_case["differentType"].as_bool().unwrap());
        assert!(option_case["nullEqualsFalse"].as_bool().unwrap());
        assert!(option_case["wrongTypeEqualsFalse"].as_bool().unwrap());
        assert!(option_case["hashCodeStableOnSameObject"].as_bool().unwrap());
        assert!(
            option_case["equalInstancesHaveSameHashCode"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(option, OffsetOption::new(Some(OffsetOptionType::TailN), 23));
        assert!(option.java_equals(Some(&option as &dyn std::any::Any)));
        assert!(!option.java_equals(None));
        let wrong_type = "not an OffsetOption";
        assert!(!option.java_equals(Some(&wrong_type as &dyn std::any::Any)));
        let equal_option = OffsetOption::new(Some(OffsetOptionType::TailN), 23);
        assert!(option.java_equals(Some(&equal_option as &dyn std::any::Any)));
        let different_type = OffsetOption::new(Some(OffsetOptionType::Policy), 23);
        assert!(!option.java_equals(Some(&different_type as &dyn std::any::Any)));
        let equal_options = std::collections::HashSet::from([option.clone(), equal_option]);
        assert_eq!(
            equal_options.len(),
            option_case["equalHashSetSize"].as_u64().unwrap() as usize
        );

        let control_case = &fixture["liteSubscriptionControlApiCase"];
        let mut control = LiteSubscriptionCtlRequestBody::default();
        assert!(control.subscription_set().is_none());
        let worker = SharedValue::new(dto);
        let assigned_subscriptions =
            SharedValue::new(std::collections::HashSet::from([Some(worker.clone())]));
        let repeated_reference_set =
            std::collections::HashSet::from([Some(worker.clone()), Some(worker.clone())]);
        assert_eq!(repeated_reference_set.len(), 1);
        let equal_content_distinct = SharedValue::new(worker.snapshot());
        let identity_distinct_set =
            std::collections::HashSet::from([Some(worker.clone()), Some(equal_content_distinct)]);
        assert_eq!(identity_distinct_set.len(), 2);
        control.set_subscription_set(Some(assigned_subscriptions.clone()));
        let getter_subscriptions = control.subscription_set().unwrap();
        assert!(assigned_subscriptions.ptr_eq(&getter_subscriptions));
        assert_eq!(getter_subscriptions.with(std::collections::HashSet::len), 1);
        assert!(
            getter_subscriptions
                .with(|subscriptions| subscriptions.contains(&Some(worker.clone())))
        );
        assert!(
            control_case["defaultSubscriptionSetIsNull"]
                .as_bool()
                .unwrap()
        );
        assert!(control_case["setterStoresSameSet"].as_bool().unwrap());
        assert!(control_case["setterElementIdentity"].as_bool().unwrap());
        assert_eq!(control_case["size"], 1);
        assert_eq!(control_case["externalMutationVisible"], true);
        assert_eq!(control_case["getterMutationVisibleToAssignedSet"], true);
        assert!(control_case["externalElementIdentity"].as_bool().unwrap());
        assert!(control_case["getterElementIdentity"].as_bool().unwrap());
        let consumer = SharedValue::new(LiteSubscriptionDto::default());
        assigned_subscriptions.with_mut(|subscriptions| {
            subscriptions.insert(Some(consumer.clone()));
        });
        assert!(
            getter_subscriptions
                .with(|subscriptions| subscriptions.contains(&Some(consumer.clone())))
        );
        let admin = SharedValue::new(LiteSubscriptionDto::default());
        getter_subscriptions.with_mut(|subscriptions| {
            subscriptions.insert(Some(admin.clone()));
        });
        assert!(
            assigned_subscriptions
                .with(|subscriptions| subscriptions.contains(&Some(admin.clone())))
        );
        assert_eq!(
            getter_subscriptions.with(std::collections::HashSet::len),
            control_case["mutatedSize"].as_u64().unwrap() as usize
        );
    }

    #[test]
    fn lite_subscription_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_control_field_shapes_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionControlFieldCases"]
            .as_array()
            .unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn lite_subscription_set_element_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionElementCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_offset_value_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionOffsetValueCases"]
            .as_array()
            .unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_set_duplicates_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionSetCases"].as_array().unwrap();
        assert_eq!(cases.len(), 4);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn query_assignment_request_enum_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["queryAssignmentEnumCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            10,
            "Java oracle includes assignment request enum and duplicate-field inputs"
        );
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<QueryAssignmentRequestBody>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn query_assignment_request_field_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["queryAssignmentFieldCases"].as_array().unwrap();
        assert_eq!(cases.len(), 35);
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn query_assignment_request_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["queryAssignmentRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<Option<QueryAssignmentRequestBody>>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn set_request_mode_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["setRequestModeCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            14,
            "Java oracle includes request mode defaults, coercions, and duplicate fields"
        );
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<SetMessageRequestModeRequestBody>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn set_request_mode_field_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["setRequestModeFieldCases"].as_array().unwrap();
        assert_eq!(cases.len(), 28);
        for case in cases {
            let decoded = serde_json::from_str::<SetMessageRequestModeRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn set_request_mode_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["setRequestModeRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<Option<SetMessageRequestModeRequestBody>>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn consume_direct_result_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumeDirectResultCases"].as_array().unwrap();
        batch_body_cases_match_java::<ConsumeMessageDirectlyResult>(cases, 66);
        let default_encoded =
            serde_json::to_value(ConsumeMessageDirectlyResult::default()).unwrap();
        assert_eq!(
            default_encoded,
            cases
                .iter()
                .find(|case| case["name"] == "root-empty-object")
                .unwrap()["encoded"],
            "Rust Default should match Java's no-argument DTO values"
        );
    }

    #[test]
    fn assignment_set_hash_collision_oracle_records_the_java_boundary() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let collision = &fixture["assignmentHashCollisionCases"];

        // This cross-mode oracle cancels the JVM-local enum identity-hash delta.
        // Stable same-mode collisions are covered by assignmentBodyCases and the
        // response decoder, which leaves the DTO's lawful Rust Eq/Hash unchanged.
        assert_eq!(collision["sameQueueEquals"], true);
        assert_eq!(collision["sameMode"], false);
        assert_eq!(collision["equalHash"], true);
        assert_eq!(collision["setSize"], 1);
        assert_eq!(collision["collisionDeduplicates"], true);
    }

    #[test]
    fn assignment_methods_match_invoked_java_public_api() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        for case in fixture["assignmentMethodCases"].as_array().unwrap() {
            let mut assignment = MessageQueueAssignment::default();
            let queue_alias =
                serde_json::from_value::<Option<MessageQueue>>(case["messageQueue"].clone())
                    .unwrap()
                    .map(SharedValue::new);
            let caller_queue_alias = queue_alias.clone();
            assignment.set_message_queue(queue_alias);
            assignment.set_mode(
                serde_json::from_value::<Option<MessageRequestMode>>(case["mode"].clone()).unwrap(),
            );
            let attachment_alias = serde_json::from_value::<Option<AssignmentAttachments>>(
                case["attachments"].clone(),
            )
            .unwrap()
            .map(SharedValue::new);
            let caller_attachment_alias = attachment_alias.clone();
            assignment.set_attachments(attachment_alias);

            assert_eq!(
                assignment.message_queue().map(|queue| queue.snapshot()),
                serde_json::from_value::<Option<MessageQueue>>(case["messageQueue"].clone())
                    .unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                assignment.mode(),
                serde_json::from_value::<Option<MessageRequestMode>>(case["mode"].clone()).unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                assignment
                    .attachments()
                    .map(|attachments| attachments.snapshot()),
                serde_json::from_value::<Option<AssignmentAttachments>>(
                    case["attachments"].clone(),
                )
                .unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(
                assignment.java_to_string(),
                case["text"].as_str().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(assignment.to_string(), case["text"].as_str().unwrap());
            assert_eq!(assignment.java_equals(None), Ok(false));
            assert_eq!(assignment.java_equals(Some(&assignment)), Ok(true));

            if case["name"] == "default" {
                assert_eq!(case["equalsSelf"], true);
                assert_eq!(case["equalsNull"], false);
                assert_eq!(case["equalsDistinctNullQueue"], "NullPointerException");
                assert_eq!(
                    assignment.java_equals(Some(&"not-an-assignment" as &dyn std::any::Any)),
                    Ok(case["equalsWrongType"].as_bool().unwrap())
                );
                assert_eq!(
                    assignment.java_equals(Some(&MessageQueueAssignment::default())),
                    Err(MessageQueueAssignmentEqualsError::NullMessageQueue)
                );
            } else {
                let mut same_queue = assignment.clone();
                same_queue.set_mode(Some(MessageRequestMode::Pull));
                same_queue.set_attachments(None);
                assert_eq!(
                    assignment.java_equals(Some(&same_queue)),
                    Ok(case["equalsSameQueueDifferentMetadata"].as_bool().unwrap())
                );

                let mut different_queue = same_queue.clone();
                different_queue.set_message_queue(Some(SharedValue::new(MessageQueue::new(
                    "orders", "broker", 3,
                ))));
                assert_eq!(
                    assignment.java_equals(Some(&different_queue)),
                    Ok(case["equalsDifferentQueue"].as_bool().unwrap())
                );
                if case["name"] == "populated" {
                    assert_eq!(case["attachmentsGetterReturnsSetterMap"], true);
                    let caller_attachments = caller_attachment_alias.as_ref().unwrap();
                    let getter_attachments = assignment.attachments().unwrap();
                    assert!(caller_attachments.ptr_eq(&getter_attachments));
                    assert_eq!(case["attachmentsSetterMutationVisibleToGetter"], true);
                    caller_attachments.with_mut(|attachments| {
                        attachments.insert(Some("caller".into()), Some("visible".into()));
                    });
                    assert_eq!(
                        getter_attachments
                            .with(|attachments| attachments.get(&Some("caller".into())).cloned()),
                        Some(Some("visible".into()))
                    );
                    getter_attachments.with_mut(|attachments| {
                        attachments.insert(Some("getter".into()), Some("visible".into()));
                    });
                    assert_eq!(case["attachmentsGetterMutationVisibleToSetterMap"], true);
                    assert_eq!(
                        serde_json::to_value(getter_attachments.snapshot()).unwrap(),
                        case["mutatedAttachments"]
                    );
                    assert_eq!(case["messageQueueGetterReturnsSetterObject"], true);
                    let caller_queue = caller_queue_alias.as_ref().unwrap();
                    let getter_queue = assignment.message_queue().unwrap();
                    assert!(caller_queue.ptr_eq(&getter_queue));
                    caller_queue.with_mut(|queue| queue.set_queue_id(3));
                    assert_eq!(getter_queue.with(|queue| queue.queue_id()), 3);
                    assert_eq!(case["messageQueueSetterMutationVisibleToGetter"], true);
                    caller_queue.with_mut(|queue| queue.set_queue_id(2));
                    getter_queue.with_mut(|queue| queue.set_queue_id(4));
                    assert_eq!(
                        case["messageQueueGetterMutationVisibleToSetterObject"],
                        caller_queue.with(|queue| queue.queue_id() == 4)
                    );
                    assert_eq!(
                        serde_json::to_value(getter_queue.snapshot()).unwrap(),
                        case["mutatedMessageQueue"]
                    );
                }
            }
        }
    }

    #[test]
    fn assignment_attachments_preserve_java_null_key_and_fastjson_shape() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let oracle = &fixture["assignmentNullAttachmentApiCase"];
        assert_eq!(oracle["setterAcceptedNullKey"], true);
        assert_eq!(oracle["getterReturnsSetterMap"], true);
        assert_eq!(oracle["setterMutationVisibleToGetter"], true);
        assert_eq!(oracle["getterMutationVisibleToSetter"], true);

        let mut attachments = AssignmentAttachments::default();
        attachments.insert(None, Some("null-key".into()));
        attachments.insert(Some("ordinary".into()), Some("value".into()));
        attachments.insert(Some("null".into()), Some("literal-null-key".into()));
        let caller_map = SharedValue::new(attachments);
        let mut assignment = MessageQueueAssignment::default();
        assignment.set_attachments(Some(caller_map.clone()));

        let getter_map = assignment.attachments().unwrap();
        assert!(caller_map.ptr_eq(&getter_map));
        caller_map.with_mut(|attachments| {
            attachments.insert(Some("caller".into()), Some("visible".into()));
        });
        getter_map.with_mut(|attachments| {
            attachments.insert(Some("getter".into()), Some("visible".into()));
        });
        assert_eq!(
            getter_map.with(|attachments| attachments.get(&None).cloned()),
            Some(Some("null-key".into()))
        );
        assert_eq!(
            serde_json::to_value(getter_map.snapshot()).unwrap(),
            serde_json::from_str::<serde_json::Value>(
                oracle["serializedAttachments"].as_str().unwrap()
            )
            .unwrap()
        );
        let serialized_assignment = serde_json::to_string(&assignment).unwrap();
        let java_serialized_assignment: serde_json::Value =
            serde_json::from_str(oracle["serializedAssignment"].as_str().unwrap()).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&serialized_assignment).unwrap(),
            java_serialized_assignment
        );
        assert_eq!(serialized_assignment.matches("\"null\":").count(), 2);

        let decoded: MessageQueueAssignment =
            serde_json::from_str(r#"{"attachments":{"null":"string-key"}}"#).unwrap();
        let decoded_attachments = decoded.attachments().unwrap();
        assert_eq!(
            decoded_attachments.with(|attachments| attachments.get(&Some("null".into())).cloned()),
            Some(Some("string-key".into()))
        );
        assert_eq!(
            decoded_attachments.with(|attachments| attachments.get(&None).cloned()),
            None
        );
    }

    #[test]
    fn message_request_mode_name_matches_invoked_java_public_api() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        for case in fixture["messageRequestModeMethodCases"].as_array().unwrap() {
            let mode: MessageRequestMode =
                serde_json::from_value(case["constant"].clone()).unwrap();
            assert_eq!(mode.name(), case["getName"].as_str().unwrap());
        }
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn lite_subscription_control_and_wrapper_preserve_nested_wire_shape() {
        let lite = LiteSubscriptionDto {
            action: Some(LiteSubscriptionAction::CompleteAdd),
            client_id: Some("client".into()),
            group: Some("workers".into()),
            topic: Some("orders".into()),
            lite_topic_set: Some(SharedValue::new(std::collections::BTreeSet::from([Some(
                "shard-a".into(),
            )]))),
            offset_option: Some(SharedValue::new(OffsetOption {
                option_type: Some(OffsetOptionType::Policy),
                value: 1,
            })),
            version: 99,
        };
        let ctl = LiteSubscriptionCtlRequestBody {
            subscription_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(lite),
            )]))),
        };
        let value = serde_json::to_value(&ctl).unwrap();
        assert_eq!(value["subscriptionSet"][0]["action"], "COMPLETE_ADD");
        assert_eq!(
            value["subscriptionSet"][0]["offsetOption"]["type"],
            "POLICY"
        );
        let round_tripped =
            serde_json::from_value::<LiteSubscriptionCtlRequestBody>(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(round_tripped).unwrap(), value);

        let wrapper = MessageRequestModeSerializeWrapper {
            message_request_mode_map: Some(SharedValue::new(std::collections::BTreeMap::from([(
                "orders".into(),
                SharedValue::new(std::collections::BTreeMap::from([(
                    "workers".into(),
                    SharedValue::new(SetMessageRequestModeRequestBody {
                        topic: Some("orders".into()),
                        consumer_group: Some("workers".into()),
                        mode: Some(MessageRequestMode::Pop),
                        pop_share_queue_num: 2,
                    }),
                )])),
            )]))),
        };
        let value = serde_json::to_value(wrapper).unwrap();
        assert_eq!(
            value["messageRequestModeMap"]["orders"]["workers"]["mode"],
            "POP"
        );
    }
}
