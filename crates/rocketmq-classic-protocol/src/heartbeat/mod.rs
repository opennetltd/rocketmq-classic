use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::marker::PhantomData;

use crate::SharedValue;
use crate::codes::JavaEnumLookupError;
use crate::serde_fastjson;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, de};
use std::fmt;

pub type SubscriptionTagSet = HashSet<Option<String>>;
pub type SubscriptionCodeSet = HashSet<Option<i32>>;
pub type SharedSubscriptionTagSet = SharedValue<SubscriptionTagSet>;
pub type SharedSubscriptionCodeSet = SharedValue<SubscriptionCodeSet>;
pub type ConsumerSubscriptionDataSet = HashSet<Option<SharedValue<SubscriptionData>>>;
pub type SharedConsumerSubscriptionDataSet = SharedValue<ConsumerSubscriptionDataSet>;
pub type ProducerDataSet = HashSet<Option<SharedValue<ProducerData>>>;
pub type SharedProducerDataSet = SharedValue<ProducerDataSet>;
pub type HeartbeatConsumerDataSet = HashSet<Option<SharedValue<ConsumerData>>>;
pub type SharedHeartbeatConsumerDataSet = SharedValue<HeartbeatConsumerDataSet>;
pub(crate) type SharedIdentityNullableSet<T> = SharedValue<HashSet<Option<SharedValue<T>>>>;

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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionData {
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub class_filter_mode: bool,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub sub_string: Option<String>,
    #[serde(
        default = "empty_shared_tags",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_string_set",
        skip_serializing_if = "Option::is_none"
    )]
    pub tags_set: Option<SharedSubscriptionTagSet>,
    #[serde(
        default = "empty_shared_codes",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_int_set",
        skip_serializing_if = "Option::is_none"
    )]
    pub code_set: Option<SharedSubscriptionCodeSet>,
    #[serde(
        default = "current_subscription_version",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    pub sub_version: i64,
    #[serde(
        default = "default_expression_type",
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub expression_type: Option<String>,
    #[serde(
        rename = "filterClassSource",
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing
    )]
    pub filter_class_source: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionDataFields {
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    class_filter_mode: bool,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    sub_string: Option<String>,
    #[serde(
        default = "empty_tags",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_string_set"
    )]
    tags_set: Option<HashSet<Option<String>>>,
    #[serde(
        default = "empty_codes",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_int_set"
    )]
    code_set: Option<HashSet<Option<i32>>>,
    #[serde(
        default = "current_subscription_version",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    sub_version: i64,
    #[serde(
        default = "default_expression_type",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    expression_type: Option<String>,
    #[serde(
        rename = "filterClassSource",
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    filter_class_source: Option<String>,
}

impl<'de> Deserialize<'de> for SubscriptionData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: SubscriptionDataFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            class_filter_mode: fields.class_filter_mode,
            topic: fields.topic,
            sub_string: fields.sub_string,
            tags_set: fields.tags_set.map(SharedValue::from),
            code_set: fields.code_set.map(SharedValue::from),
            sub_version: fields.sub_version,
            expression_type: fields.expression_type,
            filter_class_source: fields.filter_class_source,
        })
    }
}

impl PartialEq for SubscriptionData {
    fn eq(&self, other: &Self) -> bool {
        self.class_filter_mode == other.class_filter_mode
            && self.topic == other.topic
            && self.sub_string == other.sub_string
            && self.tags_set == other.tags_set
            && self.code_set == other.code_set
            && self.sub_version == other.sub_version
            && self.expression_type == other.expression_type
    }
}

impl Clone for SubscriptionData {
    fn clone(&self) -> Self {
        Self {
            class_filter_mode: self.class_filter_mode,
            topic: self.topic.clone(),
            sub_string: self.sub_string.clone(),
            tags_set: self
                .tags_set
                .as_ref()
                .map(|set| SharedValue::from(set.snapshot())),
            code_set: self
                .code_set
                .as_ref()
                .map(|set| SharedValue::from(set.snapshot())),
            sub_version: self.sub_version,
            expression_type: self.expression_type.clone(),
            filter_class_source: self.filter_class_source.clone(),
        }
    }
}

impl Eq for SubscriptionData {}

impl Default for SubscriptionData {
    fn default() -> Self {
        Self {
            class_filter_mode: false,
            topic: None,
            sub_string: None,
            tags_set: empty_shared_tags(),
            code_set: empty_shared_codes(),
            sub_version: current_subscription_version(),
            expression_type: default_expression_type(),
            filter_class_source: None,
        }
    }
}

impl Hash for SubscriptionData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.class_filter_mode.hash(state);
        self.topic.hash(state);
        self.sub_string.hash(state);
        hash_optional_shared_set(&self.tags_set, state);
        hash_optional_shared_set(&self.code_set, state);
        self.sub_version.hash(state);
        self.expression_type.hash(state);
    }
}

/// Failure cases from the pinned Java `SubscriptionData.compareTo` method.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubscriptionDataCompareError {
    #[error("Java NullPointerException: null SubscriptionData compareTo argument")]
    NullOther,
}

impl<T> PartialEq for SharedValue<HashSet<T>>
where
    T: Clone + Eq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl<T> Eq for SharedValue<HashSet<T>> where T: Clone + Eq + Hash {}

impl<T> Hash for SharedValue<HashSet<T>>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.with(|values| hash_set(values, state));
    }
}

impl PartialEq for SharedValue<SubscriptionData> {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<SubscriptionData> {}

impl Hash for SharedValue<SubscriptionData> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.snapshot().hash(state);
    }
}

impl SubscriptionData {
    pub const SUB_ALL: &str = "*";

    pub fn is_class_filter_mode(&self) -> bool {
        self.class_filter_mode
    }

    pub fn set_class_filter_mode(&mut self, value: bool) {
        self.class_filter_mode = value;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_sub_string(&self) -> Option<&str> {
        self.sub_string.as_deref()
    }

    pub fn set_sub_string(&mut self, value: Option<String>) {
        self.sub_string = value;
    }

    pub fn get_tags_set(&self) -> Option<SharedSubscriptionTagSet> {
        self.tags_set.clone()
    }

    /// Returns a shared tag Set handle for Java-compatible getter mutations.
    pub fn get_tags_set_mut(&mut self) -> Option<SharedSubscriptionTagSet> {
        self.tags_set.clone()
    }

    pub fn set_tags_set(&mut self, value: Option<SharedSubscriptionTagSet>) {
        self.tags_set = value;
    }

    pub fn set_tags_set_owned(&mut self, value: Option<SubscriptionTagSet>) {
        self.tags_set = value.map(SharedValue::from);
    }

    pub fn get_sub_version(&self) -> i64 {
        self.sub_version
    }

    pub fn set_sub_version(&mut self, value: i64) {
        self.sub_version = value;
    }

    pub fn get_code_set(&self) -> Option<SharedSubscriptionCodeSet> {
        self.code_set.clone()
    }

    /// Returns a shared code Set handle for Java-compatible getter mutations.
    pub fn get_code_set_mut(&mut self) -> Option<SharedSubscriptionCodeSet> {
        self.code_set.clone()
    }

    pub fn set_code_set(&mut self, value: Option<SharedSubscriptionCodeSet>) {
        self.code_set = value;
    }

    pub fn set_code_set_owned(&mut self, value: Option<SubscriptionCodeSet>) {
        self.code_set = value.map(SharedValue::from);
    }

    pub fn get_expression_type(&self) -> Option<&str> {
        self.expression_type.as_deref()
    }

    pub fn set_expression_type(&mut self, value: Option<String>) {
        self.expression_type = value;
    }

    pub fn get_filter_class_source(&self) -> Option<&str> {
        self.filter_class_source.as_deref()
    }

    pub fn set_filter_class_source(&mut self, value: Option<String>) {
        self.filter_class_source = value;
    }

    /// Mirrors the Java `(topic, subString)` constructor while retaining
    /// Rust's explicit representation for nullable Java strings.
    pub fn new(topic: Option<String>, sub_string: Option<String>) -> Self {
        Self {
            topic,
            sub_string,
            ..Self::default()
        }
    }

    /// Java `SubscriptionData.hashCode`; subVersion and filterClassSource do
    /// not participate even though subVersion is checked by `equals`.
    pub fn java_hash_code(&self) -> i32 {
        let mut hash = 1i32;
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(if self.class_filter_mode { 1231 } else { 1237 });
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(java_optional_shared_set_hash(&self.code_set, |value| {
                value.unwrap_or(0)
            }));
        hash = hash.wrapping_mul(31).wrapping_add(
            self.sub_string
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(java_optional_shared_set_hash(&self.tags_set, |value| {
                value.as_deref().map(java_string_hash_code).unwrap_or(0)
            }));
        hash = hash.wrapping_mul(31).wrapping_add(
            self.topic
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        hash.wrapping_mul(31).wrapping_add(
            self.expression_type
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        )
    }

    /// Java `SubscriptionData.toString` field order and labels. Set contents
    /// are sorted in Java string order because Java HashSet iteration order is
    /// unspecified and Rust HashSet iteration is randomized.
    pub fn java_to_string(&self) -> String {
        format!(
            "SubscriptionData [classFilterMode={}, topic={}, subString={}, tagsSet={}, codeSet={}, subVersion={}, expressionType={}]",
            self.class_filter_mode,
            java_nullable_string(&self.topic),
            java_nullable_string(&self.sub_string),
            java_string_set_to_string(&self.tags_set),
            java_integer_set_to_string(&self.code_set),
            self.sub_version,
            java_nullable_string(&self.expression_type),
        )
    }

    /// Java compares the concatenated `topic@subString` keys as UTF-16. A
    /// null component is rendered as the literal `null` by StringBuilder.
    pub fn java_compare_to(&self, other: &Self) -> i32 {
        let left = format!(
            "{}@{}",
            java_nullable_string(&self.topic),
            java_nullable_string(&self.sub_string)
        );
        let right = format!(
            "{}@{}",
            java_nullable_string(&other.topic),
            java_nullable_string(&other.sub_string)
        );
        java_string_compare(&left, &right)
    }

    /// Compares with an optional value and maps Java's null-argument failure.
    pub fn java_compare_to_nullable(
        &self,
        other: Option<&Self>,
    ) -> Result<i32, SubscriptionDataCompareError> {
        other
            .map(|other| self.java_compare_to(other))
            .ok_or(SubscriptionDataCompareError::NullOther)
    }
}

fn java_optional_shared_set_hash<T>(
    values: &Option<SharedValue<HashSet<T>>>,
    mut value_hash: impl FnMut(&T) -> i32,
) -> i32 {
    values.as_ref().map_or(0, |values| {
        values.with(|values| {
            values
                .iter()
                .fold(0i32, |hash, value| hash.wrapping_add(value_hash(value)))
        })
    })
}

fn java_nullable_string(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("null")
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn java_string_compare(left: &str, right: &str) -> i32 {
    let mut left = left.encode_utf16();
    let mut right = right.encode_utf16();
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right)) if left != right => {
                return i32::from(left) - i32::from(right);
            }
            (Some(_), Some(_)) => {}
            (None, Some(_)) => return -(1 + right.count() as i32),
            (Some(_), None) => return 1 + left.count() as i32,
            (None, None) => return 0,
        }
    }
}

fn java_string_set_to_string(values: &Option<SharedSubscriptionTagSet>) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    let snapshot = values.snapshot();
    let mut values = snapshot
        .iter()
        .map(|value| value.as_deref())
        .collect::<Vec<_>>();
    values.sort_by(|left, right| match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => java_string_compare(left, right).cmp(&0),
    });
    format!(
        "[{}]",
        values
            .into_iter()
            .map(|value| value.unwrap_or("null"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn java_integer_set_to_string(values: &Option<SharedSubscriptionCodeSet>) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    let snapshot = values.snapshot();
    let mut values = snapshot.iter().copied().collect::<Vec<_>>();
    values.sort_unstable();
    format!(
        "[{}]",
        values
            .into_iter()
            .map(|value| value.map_or_else(|| "null".to_owned(), |value| value.to_string()))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn java_hash_set_to_string<T>(
    values: &Option<HashSet<Option<T>>>,
    mut item_to_string: impl FnMut(&T) -> String,
) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    let mut values = values
        .iter()
        .map(|value| {
            value.as_ref().map_or_else(
                || (0, "null".to_owned()),
                |value| (1, item_to_string(value)),
            )
        })
        .collect::<Vec<_>>();
    values.sort();
    format!(
        "[{}]",
        values
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn java_shared_hash_set_to_string<T: Clone>(
    values: &Option<SharedValue<HashSet<Option<T>>>>,
    item_to_string: impl FnMut(&T) -> String,
) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    java_hash_set_to_string(&Some(values.snapshot()), item_to_string)
}

fn hash_set<T: Hash, H: Hasher>(values: &HashSet<T>, state: &mut H) {
    let mut hashes = values
        .iter()
        .map(|value| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        })
        .collect::<Vec<_>>();
    hashes.sort_unstable();
    hashes.hash(state);
}

fn hash_optional_shared_set<T: Hash, H: Hasher>(
    values: &Option<SharedValue<HashSet<T>>>,
    state: &mut H,
) {
    match values {
        Some(values) => {
            true.hash(state);
            values.with(|values| hash_set(values, state));
        }
        None => false.hash(state),
    }
}

fn empty_tags() -> Option<HashSet<Option<String>>> {
    Some(HashSet::new())
}

fn empty_codes() -> Option<HashSet<Option<i32>>> {
    Some(HashSet::new())
}

fn empty_shared_tags() -> Option<SharedSubscriptionTagSet> {
    Some(SharedValue::default())
}

fn empty_shared_codes() -> Option<SharedSubscriptionCodeSet> {
    Some(SharedValue::default())
}

fn current_subscription_version() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Deserialize)]
struct FastjsonEnumOrdinalInput {
    #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
    ordinal: i32,
}

pub(crate) trait JavaOrdinalEnum: Sized {
    fn from_java_ordinal(ordinal: i64) -> Option<Self>;
}

pub(crate) fn deserialize_fastjson_optional_enum<'de, D, T>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + JavaOrdinalEnum,
{
    match Option::<Box<serde_json::value::RawValue>>::deserialize(deserializer)? {
        None => Ok(None),
        Some(raw) if raw.get().starts_with('"') => {
            let name = serde_json::from_str::<String>(raw.get())
                .map_err(<D::Error as de::Error>::custom)?;
            if let Ok(value) = T::deserialize(
                serde::de::value::StringDeserializer::<D::Error>::new(name.to_ascii_uppercase()),
            ) {
                return Ok(Some(value));
            }
            if is_ascii_signed_integer(&name) {
                let ordinal = name.parse::<i32>().map_err(|error| {
                    <D::Error as serde::de::Error>::custom(format!("enum ordinal: {error}"))
                })?;
                return Ok(T::from_java_ordinal(i64::from(ordinal)));
            }
            Ok(None)
        }
        Some(raw) if raw.get() == "null" => Ok(None),
        Some(raw) => {
            let token = raw.get();
            let ordinal = if token.contains(['e', 'E']) {
                let value = token
                    .parse::<f64>()
                    .map_err(<D::Error as de::Error>::custom)?;
                if !value.is_finite() {
                    return Err(<D::Error as de::Error>::custom(
                        "enum ordinal is not finite",
                    ));
                }
                value as i32
            } else if token.contains('.') {
                let wrapped = format!("{{\"ordinal\":{token}}}");
                serde_json::from_str::<FastjsonEnumOrdinalInput>(&wrapped)
                    .map_err(<D::Error as de::Error>::custom)?
                    .ordinal
            } else {
                token
                    .parse::<i64>()
                    .map_err(|error| {
                        <D::Error as de::Error>::custom(format!("enum ordinal: {error}"))
                    })?
                    .try_into()
                    .map_err(|error| {
                        <D::Error as de::Error>::custom(format!("enum ordinal: {error}"))
                    })?
            };
            T::from_java_ordinal(i64::from(ordinal))
                .map(Some)
                .ok_or_else(|| <D::Error as de::Error>::custom("enum ordinal out of range"))
        }
    }
}

fn is_ascii_signed_integer(value: &str) -> bool {
    let digits = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn default_expression_type() -> Option<String> {
    Some("TAG".to_owned())
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumeType {
    #[default]
    ConsumeActively,
    ConsumePassively,
    ConsumePop,
}

impl_java_enum_api!(
    ConsumeType,
    "org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType",
    3,
    ConsumeActively => "CONSUME_ACTIVELY" = 0,
    ConsumePassively => "CONSUME_PASSIVELY" = 1,
    ConsumePop => "CONSUME_POP" = 2,
);

impl ConsumeType {
    pub const fn type_cn(self) -> &'static str {
        match self {
            Self::ConsumeActively => "PULL",
            Self::ConsumePassively => "PUSH",
            Self::ConsumePop => "POP",
        }
    }
}

impl JavaOrdinalEnum for ConsumeType {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::ConsumeActively),
            1 => Some(Self::ConsumePassively),
            2 => Some(Self::ConsumePop),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageModel {
    #[default]
    Clustering,
    Broadcasting,
    LiteSelective,
}

impl_java_enum_api!(
    MessageModel,
    "org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel",
    3,
    Broadcasting => "BROADCASTING" = 0,
    Clustering => "CLUSTERING" = 1,
    LiteSelective => "LITE_SELECTIVE" = 2,
);

impl PartialOrd for MessageModel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MessageModel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ordinal().cmp(&other.ordinal())
    }
}

impl MessageModel {
    pub const fn mode_cn(self) -> &'static str {
        match self {
            Self::Broadcasting => "BROADCASTING",
            Self::Clustering => "CLUSTERING",
            Self::LiteSelective => "LITE_SELECTIVE",
        }
    }
}

impl JavaOrdinalEnum for MessageModel {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Broadcasting),
            1 => Some(Self::Clustering),
            2 => Some(Self::LiteSelective),
            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumeFromWhere {
    #[default]
    ConsumeFromLastOffset,
    #[serde(rename = "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST")]
    ConsumeFromLastOffsetAndFromMinWhenBootFirst,
    ConsumeFromMinOffset,
    ConsumeFromMaxOffset,
    ConsumeFromFirstOffset,
    ConsumeFromTimestamp,
}

impl_java_enum_api!(
    ConsumeFromWhere,
    "org.apache.rocketmq.common.consumer.ConsumeFromWhere",
    6,
    ConsumeFromLastOffset => "CONSUME_FROM_LAST_OFFSET" = 0,
    ConsumeFromLastOffsetAndFromMinWhenBootFirst => "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" = 1,
    ConsumeFromMinOffset => "CONSUME_FROM_MIN_OFFSET" = 2,
    ConsumeFromMaxOffset => "CONSUME_FROM_MAX_OFFSET" = 3,
    ConsumeFromFirstOffset => "CONSUME_FROM_FIRST_OFFSET" = 4,
    ConsumeFromTimestamp => "CONSUME_FROM_TIMESTAMP" = 5,
);

impl JavaOrdinalEnum for ConsumeFromWhere {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::ConsumeFromLastOffset),
            1 => Some(Self::ConsumeFromLastOffsetAndFromMinWhenBootFirst),
            2 => Some(Self::ConsumeFromMinOffset),
            3 => Some(Self::ConsumeFromMaxOffset),
            4 => Some(Self::ConsumeFromFirstOffset),
            5 => Some(Self::ConsumeFromTimestamp),
            _ => None,
        }
    }
}

/// Java inherits identity based `Object.equals`/`hashCode` for this DTO, so
/// the native type intentionally has no structural equality or hash traits.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::heartbeat::ProducerData;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<ProducerData>(); }
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProducerData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub group_name: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProducerDataFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group_name: Option<String>,
}

impl<'de> Deserialize<'de> for ProducerData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ProducerDataFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            group_name: fields.group_name,
        })
    }
}

impl PartialEq for SharedValue<ProducerData> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}

impl Eq for SharedValue<ProducerData> {}

impl Hash for SharedValue<ProducerData> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_identity(state);
    }
}

impl ProducerData {
    pub fn get_group_name(&self) -> Option<&str> {
        self.group_name.as_deref()
    }

    pub fn set_group_name(&mut self, value: Option<String>) {
        self.group_name = value;
    }

    pub fn java_to_string(&self) -> String {
        format!(
            "ProducerData [groupName={}]",
            java_nullable_string(&self.group_name)
        )
    }
}

/// Java inherits identity based `Object.equals`/`hashCode` for this DTO, so
/// the native type intentionally has no structural equality or hash traits.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::heartbeat::ConsumerData;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<ConsumerData>(); }
/// ```
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub group_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub consume_type: Option<ConsumeType>,
    #[serde(
        default,
        deserialize_with = "deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_model: Option<MessageModel>,
    #[serde(
        default,
        deserialize_with = "deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub consume_from_where: Option<ConsumeFromWhere>,
    #[serde(
        default = "empty_subscriptions",
        deserialize_with = "deserialize_nullable_hash_set_with_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub subscription_data_set: Option<SharedConsumerSubscriptionDataSet>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub unit_mode: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsumerDataFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group_name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_fastjson_optional_enum")]
    consume_type: Option<ConsumeType>,
    #[serde(default, deserialize_with = "deserialize_fastjson_optional_enum")]
    message_model: Option<MessageModel>,
    #[serde(default, deserialize_with = "deserialize_fastjson_optional_enum")]
    consume_from_where: Option<ConsumeFromWhere>,
    #[serde(
        default = "empty_subscriptions",
        deserialize_with = "deserialize_nullable_hash_set_with_null"
    )]
    subscription_data_set: Option<HashSet<Option<SubscriptionData>>>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    unit_mode: bool,
}

impl<'de> Deserialize<'de> for ConsumerData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ConsumerDataFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            group_name: fields.group_name,
            consume_type: fields.consume_type,
            message_model: fields.message_model,
            consume_from_where: fields.consume_from_where,
            subscription_data_set: fields.subscription_data_set.map(|set| {
                SharedValue::from(
                    set.into_iter()
                        .map(|subscription| subscription.map(SharedValue::from))
                        .collect::<ConsumerSubscriptionDataSet>(),
                )
            }),
            unit_mode: fields.unit_mode,
        })
    }
}

impl PartialEq for SharedValue<ConsumerData> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}

impl Eq for SharedValue<ConsumerData> {}

impl Hash for SharedValue<ConsumerData> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_identity(state);
    }
}

impl Clone for ConsumerData {
    fn clone(&self) -> Self {
        let subscription_data_set = self.subscription_data_set.as_ref().map(|set| {
            SharedValue::from(
                set.snapshot()
                    .into_iter()
                    .map(|subscription| {
                        subscription.map(|subscription| SharedValue::from(subscription.snapshot()))
                    })
                    .collect::<ConsumerSubscriptionDataSet>(),
            )
        });
        Self {
            group_name: self.group_name.clone(),
            consume_type: self.consume_type,
            message_model: self.message_model,
            consume_from_where: self.consume_from_where,
            subscription_data_set,
            unit_mode: self.unit_mode,
        }
    }
}

impl Default for ConsumerData {
    fn default() -> Self {
        Self {
            group_name: None,
            consume_type: None,
            message_model: None,
            consume_from_where: None,
            subscription_data_set: empty_shared_subscriptions(),
            unit_mode: false,
        }
    }
}

impl ConsumerData {
    pub fn get_group_name(&self) -> Option<&str> {
        self.group_name.as_deref()
    }

    pub fn set_group_name(&mut self, value: Option<String>) {
        self.group_name = value;
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

    pub fn get_subscription_data_set(&self) -> Option<SharedConsumerSubscriptionDataSet> {
        self.subscription_data_set.clone()
    }

    /// Returns a shared subscription Set handle for Java-compatible getter mutations.
    pub fn get_subscription_data_set_mut(&mut self) -> Option<SharedConsumerSubscriptionDataSet> {
        self.subscription_data_set.clone()
    }

    pub fn set_subscription_data_set(&mut self, value: Option<SharedConsumerSubscriptionDataSet>) {
        self.subscription_data_set = value;
    }

    pub fn set_subscription_data_set_owned(
        &mut self,
        value: Option<HashSet<Option<SubscriptionData>>>,
    ) {
        self.subscription_data_set = value.map(|set| {
            SharedValue::from(
                set.into_iter()
                    .map(|subscription| subscription.map(SharedValue::from))
                    .collect::<ConsumerSubscriptionDataSet>(),
            )
        });
    }

    pub fn is_unit_mode(&self) -> bool {
        self.unit_mode
    }

    pub fn set_unit_mode(&mut self, value: bool) {
        self.unit_mode = value;
    }

    pub fn java_to_string(&self) -> String {
        format!(
            "ConsumerData [groupName={}, consumeType={}, messageModel={}, consumeFromWhere={}, unitMode={}, subscriptionDataSet={}]",
            java_nullable_string(&self.group_name),
            self.consume_type
                .map(consume_type_java_name)
                .unwrap_or("null"),
            self.message_model
                .map(message_model_java_name)
                .unwrap_or("null"),
            self.consume_from_where
                .map(consume_from_where_java_name)
                .unwrap_or("null"),
            self.unit_mode,
            java_shared_hash_set_to_string(&self.subscription_data_set, |subscription| {
                subscription.with(SubscriptionData::java_to_string)
            }),
        )
    }
}

fn consume_type_java_name(value: ConsumeType) -> &'static str {
    match value {
        ConsumeType::ConsumeActively => "CONSUME_ACTIVELY",
        ConsumeType::ConsumePassively => "CONSUME_PASSIVELY",
        ConsumeType::ConsumePop => "CONSUME_POP",
    }
}

fn message_model_java_name(value: MessageModel) -> &'static str {
    match value {
        MessageModel::Clustering => "CLUSTERING",
        MessageModel::Broadcasting => "BROADCASTING",
        MessageModel::LiteSelective => "LITE_SELECTIVE",
    }
}

fn consume_from_where_java_name(value: ConsumeFromWhere) -> &'static str {
    match value {
        ConsumeFromWhere::ConsumeFromLastOffset => "CONSUME_FROM_LAST_OFFSET",
        ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => {
            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"
        }
        ConsumeFromWhere::ConsumeFromMinOffset => "CONSUME_FROM_MIN_OFFSET",
        ConsumeFromWhere::ConsumeFromMaxOffset => "CONSUME_FROM_MAX_OFFSET",
        ConsumeFromWhere::ConsumeFromFirstOffset => "CONSUME_FROM_FIRST_OFFSET",
        ConsumeFromWhere::ConsumeFromTimestamp => "CONSUME_FROM_TIMESTAMP",
    }
}

fn empty_subscriptions() -> Option<HashSet<Option<SubscriptionData>>> {
    Some(HashSet::new())
}

fn empty_shared_subscriptions() -> Option<SharedConsumerSubscriptionDataSet> {
    Some(SharedValue::default())
}

/// Java inherits identity based `Object.equals`/`hashCode` for this DTO, so
/// the native type intentionally has no structural equality or hash traits.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::heartbeat::HeartbeatData;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<HeartbeatData>(); }
/// ```
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatData {
    #[serde(
        rename = "clientID",
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub client_id: Option<String>,
    #[serde(default = "empty_producers", skip_serializing_if = "Option::is_none")]
    /// Java ProducerData inherits identity equality, so repeated equal wire
    /// rows must remain distinct after decoding.
    #[serde(deserialize_with = "deserialize_identity_shared_hash_set_with_null")]
    pub producer_data_set: Option<SharedProducerDataSet>,
    #[serde(default = "empty_consumers", skip_serializing_if = "Option::is_none")]
    /// Java ConsumerData has object identity equality. Preserve its wire
    /// multiplicity rather than deduplicating structurally equal metadata.
    #[serde(deserialize_with = "deserialize_identity_shared_hash_set_with_null")]
    pub consumer_data_set: Option<SharedHeartbeatConsumerDataSet>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub heartbeat_fingerprint: i32,
    #[serde(
        default,
        rename = "withoutSub",
        deserialize_with = "serde_fastjson::deserialize_bool"
    )]
    pub is_without_sub: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeartbeatDataFields {
    #[serde(
        rename = "clientID",
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default = "empty_producers",
        deserialize_with = "deserialize_identity_shared_hash_set_with_null"
    )]
    producer_data_set: Option<SharedProducerDataSet>,
    #[serde(
        default = "empty_consumers",
        deserialize_with = "deserialize_identity_shared_hash_set_with_null"
    )]
    consumer_data_set: Option<SharedHeartbeatConsumerDataSet>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    heartbeat_fingerprint: i32,
    #[serde(
        default,
        rename = "withoutSub",
        deserialize_with = "serde_fastjson::deserialize_bool"
    )]
    is_without_sub: bool,
}

impl<'de> Deserialize<'de> for HeartbeatData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: HeartbeatDataFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            client_id: fields.client_id,
            producer_data_set: fields.producer_data_set,
            consumer_data_set: fields.consumer_data_set,
            heartbeat_fingerprint: fields.heartbeat_fingerprint,
            is_without_sub: fields.is_without_sub,
        })
    }
}

#[cfg(test)]
mod fastjson_dto_input_tests {
    use super::{ConsumerData, HeartbeatData, ProducerData, SubscriptionData};
    use crate::serde_fastjson;

    fn normalize_json(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(values) => {
                for value in values.iter_mut() {
                    normalize_json(value);
                }
                values.sort_by_key(|value| serde_json::to_string(value).unwrap());
            }
            serde_json::Value::Object(fields) => {
                fields.retain(|_, value| !value.is_null());
                for value in fields.values_mut() {
                    normalize_json(value);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn heartbeat_dto_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = fixture["heartbeatDtoInputCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            1221,
            "Java oracle includes DTO root shapes, scalar, numeric-boundary, collection-shape, syntax-grammar, numeric suffix and decimal-point forms, per-field token-kind, systematic whitespace-position/string probes, and exhaustive EOF-marker separators"
        );
        for dto in ["subscription", "producer", "consumer", "heartbeat"] {
            for shape in [
                "empty-object",
                "null",
                "empty-array",
                "singleton-array",
                "number",
                "boolean",
                "string",
            ] {
                let id = format!("root-{dto}-{shape}");
                assert!(
                    cases
                        .iter()
                        .any(|case| case["id"].as_str() == Some(id.as_str())),
                    "missing {id}"
                );
            }
        }
        for required in [
            "subscription-duplicate-class-filter-mode-last-wins",
            "subscription-duplicate-topic-last-wins",
            "subscription-duplicate-sub-string-last-wins",
            "subscription-duplicate-tags-set-last-wins",
            "subscription-duplicate-code-set-last-wins",
            "subscription-duplicate-sub-version-last-wins",
            "subscription-duplicate-expression-type-last-wins",
            "producer-duplicate-group-name-last-wins",
            "consumer-duplicate-group-name-last-wins",
            "consumer-duplicate-consume-type-last-wins",
            "consumer-duplicate-message-model-last-wins",
            "consumer-duplicate-consume-from-last-wins",
            "consumer-duplicate-subscription-set-last-wins",
            "consumer-duplicate-unit-mode-last-wins",
            "heartbeat-duplicate-client-id-last-wins",
            "heartbeat-duplicate-producer-set-last-wins",
            "heartbeat-duplicate-consumer-set-last-wins",
            "heartbeat-duplicate-fingerprint-last-wins",
            "heartbeat-duplicate-without-sub-last-wins",
            "subscription-high-precision-number-to-string",
            "subscription-exponent-number-to-string",
            "subscription-decimal-exponent-number-to-string",
            "subscription-precision-exponent-number-to-string",
            "subscription-tags-high-precision-number-to-string",
            "subscription-leading-plus-zero-version",
            "subscription-number-long-suffix",
            "subscription-number-double-suffix",
            "subscription-number-float-suffix",
            "subscription-number-byte-suffix",
            "subscription-number-short-suffix",
            "subscription-float-suffix-high-precision-to-string",
            "subscription-float-suffix-exponent-to-string",
            "subscription-double-suffix-exponent-to-string",
            "subscription-long-suffix-to-string",
            "subscription-lowercase-float-suffix-rejected",
            "subscription-leading-dot-negative-version",
            "subscription-trailing-dot-version",
            "subscription-trailing-dot-exponent-double-suffix",
            "subscription-leading-dot-positive-version",
            "subscription-leading-dot-positive-float-version",
            "subscription-leading-dot-positive-topic-rejected",
            "subscription-leading-dot-positive-float-topic-rejected",
            "subscription-leading-dot-explicit-positive-topic",
            "subscription-incomplete-exponent-version",
            "subscription-incomplete-exponent-topic",
            "subscription-incomplete-exponent-float-topic",
            "subscription-incomplete-exponent-trailing-dot-topic",
            "heartbeat-leading-plus-zero-fingerprint",
            "heartbeat-exponent-float-suffix-fingerprint",
            "consumer-form-feed-and-leading-zero",
            "heartbeat-bom-prefix",
            "heartbeat-eof-marker-repeated",
            "heartbeat-eof-marker-separated",
            "heartbeat-eof-marker-before-trailing-text",
            "heartbeat-eof-marker-before-trailing-json",
            "heartbeat-eof-marker-between-0001",
            "heartbeat-eof-marker-after-0001",
            "heartbeat-eof-marker-between-00a0",
            "heartbeat-eof-marker-after-00a0",
            "subscription-form-feed-before-version",
            "consumer-vertical-tab-prefix",
            "heartbeat-embedded-bom",
            "subscription-single-quoted-strings",
            "subscription-single-quoted-escapes",
            "subscription-comment-markers-in-string",
            "subscription-unquoted-key-rejected",
            "subscription-trailing-object-comma",
            "subscription-trailing-array-comma",
            "subscription-block-comment",
            "subscription-line-comment",
            "subscription-single-quoted-key",
            "subscription-short-control-escapes",
            "subscription-char1-escape-table",
            "subscription-hex-escapes",
            "subscription-identity-escapes",
            "subscription-double-quoted-apostrophe-escape",
            "subscription-single-quote-escaped-delimiters",
            "subscription-comments-between-tokens",
            "subscription-comment-after-object-open",
            "subscription-comment-after-object-comma",
            "subscription-comment-after-array-open",
            "subscription-comment-after-array-comma",
            "subscription-comment-after-colon",
            "subscription-comment-before-object-close",
            "subscription-comment-before-array-comma",
            "subscription-comment-after-trailing-array-comma",
            "subscription-comment-after-nested-value",
            "subscription-comment-after-trailing-comma",
            "subscription-null-array-trailing-comma",
            "subscription-eof-line-comment",
            "subscription-single-quoted-comment-markers",
            "subscription-nested-trailing-commas",
            "subscription-leading-and-trailing-comments",
            "subscription-unknown-escape-rejected",
            "subscription-nonhex-x-lookup-zero",
            "subscription-hex-escape-above-lookup-range-rejected",
            "subscription-char1-escape-8-rejected",
            "subscription-object-to-string",
            "subscription-array-to-string",
            "producer-object-to-string",
            "heartbeat-object-to-string",
            "subscription-tags-singleton-string",
            "subscription-tags-singleton-number",
            "subscription-tags-singleton-boolean",
            "subscription-tags-singleton-object",
            "subscription-tags-object-member",
            "subscription-codes-singleton-number",
            "subscription-codes-singleton-string",
            "subscription-codes-singleton-boolean",
            "subscription-codes-singleton-object",
            "subscription-codes-object-member",
            "subscription-long-object-to-zero",
            "subscription-long-empty-object-to-zero",
            "subscription-long-empty-array",
            "subscription-code-set-boundaries",
            "subscription-code-set-overflow-positive",
            "subscription-code-set-overflow-negative",
            "subscription-code-set-numeric-string-exponent",
            "subscription-code-set-wide-decimal",
            "subscription-code-set-high-precision-decimal",
            "subscription-code-set-decimal-long-maximum",
            "subscription-code-set-decimal-long-overflow-positive",
            "subscription-code-set-decimal-long-overflow-negative",
            "subscription-code-set-null",
            "subscription-tags-coercion-deduplicates",
            "consumer-subscriptions-singleton-object",
            "consumer-subscription-set-scalar-string",
            "heartbeat-producers-singleton-object",
            "heartbeat-consumers-singleton-object",
            "shape-subscription-subVersion-object",
            "shape-consumer-consumeType-array",
            "shape-heartbeat-producerDataSet-object",
            "heartbeat-whitespace-0000-root-prefix",
            "heartbeat-whitespace-000c-colon-value",
            "heartbeat-whitespace-000b-object-open",
            "heartbeat-whitespace-00a0-after-root",
            "heartbeat-whitespace-00a0-object-comma",
            "heartbeat-whitespace-00a0-array-comma",
            "heartbeat-whitespace-00a0-nested-object-close",
            "heartbeat-whitespace-0000-string-key",
            "heartbeat-whitespace-0000-string-value",
            "heartbeat-whitespace-00a0-string-value",
            "heartbeat-whitespace-2007-key-colon",
            "heartbeat-whitespace-feff-root-prefix",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        let mut mismatches = Vec::new();
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let case_id = case["id"].as_str().unwrap();
            let decoded = match case["type"].as_str().unwrap() {
                "subscription" => {
                    serde_fastjson::from_fastjson_str::<Option<SubscriptionData>>(raw_input)
                        .and_then(serde_json::to_value)
                }
                "producer" => serde_fastjson::from_fastjson_str::<Option<ProducerData>>(raw_input)
                    .and_then(serde_json::to_value),
                "consumer" => serde_fastjson::from_fastjson_str::<Option<ConsumerData>>(raw_input)
                    .and_then(serde_json::to_value),
                "heartbeat" => HeartbeatData::java_from_json_nullable(Some(raw_input))
                    .and_then(serde_json::to_value),
                other => panic!("unknown heartbeat DTO case type {other}"),
            };
            let java_accepts = case["outcome"] == "ACCEPTED";
            if decoded.is_ok() != java_accepts {
                mismatches.push(format!(
                    "{case_id}: Java accepted={java_accepts}, Rust={decoded:?}, input={raw_input:?}"
                ));
                continue;
            }
            if let Ok(mut decoded) = decoded {
                let mut expected = case["fields"].clone();
                if case_id == "root-subscription-empty-object" {
                    decoded.as_object_mut().unwrap().remove("subVersion");
                    expected.as_object_mut().unwrap().remove("subVersion");
                }
                normalize_json(&mut decoded);
                normalize_json(&mut expected);
                if decoded != expected {
                    mismatches.push(format!(
                        "{case_id}: Rust fields={decoded}, Java fields={expected}, input={raw_input:?}"
                    ));
                }
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }
}

impl Default for HeartbeatData {
    fn default() -> Self {
        Self {
            client_id: None,
            producer_data_set: empty_producers(),
            consumer_data_set: empty_consumers(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        }
    }
}

impl Clone for HeartbeatData {
    fn clone(&self) -> Self {
        let producer_data_set = self.producer_data_set.as_ref().map(|set| {
            SharedValue::from(
                set.snapshot()
                    .into_iter()
                    .map(|producer| producer.map(|producer| SharedValue::from(producer.snapshot())))
                    .collect::<ProducerDataSet>(),
            )
        });
        let consumer_data_set = self.consumer_data_set.as_ref().map(|set| {
            SharedValue::from(
                set.snapshot()
                    .into_iter()
                    .map(|consumer| consumer.map(|consumer| SharedValue::from(consumer.snapshot())))
                    .collect::<HeartbeatConsumerDataSet>(),
            )
        });
        Self {
            client_id: self.client_id.clone(),
            producer_data_set,
            consumer_data_set,
            heartbeat_fingerprint: self.heartbeat_fingerprint,
            is_without_sub: self.is_without_sub,
        }
    }
}

impl HeartbeatData {
    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, value: Option<String>) {
        self.client_id = value;
    }

    pub fn get_producer_data_set(&self) -> Option<SharedProducerDataSet> {
        self.producer_data_set.clone()
    }

    /// Returns the shared producer Set handle for Java-compatible getter mutations.
    pub fn get_producer_data_set_mut(&mut self) -> Option<SharedProducerDataSet> {
        self.producer_data_set.clone()
    }

    pub fn set_producer_data_set(&mut self, value: Option<SharedProducerDataSet>) {
        self.producer_data_set = value;
    }

    pub fn set_producer_data_set_owned(&mut self, value: Option<Vec<Option<ProducerData>>>) {
        self.producer_data_set = value.map(|values| {
            SharedValue::from(
                values
                    .into_iter()
                    .map(|producer| producer.map(SharedValue::from))
                    .collect::<ProducerDataSet>(),
            )
        });
    }

    pub fn get_consumer_data_set(&self) -> Option<SharedHeartbeatConsumerDataSet> {
        self.consumer_data_set.clone()
    }

    /// Returns the shared consumer Set handle for Java-compatible getter mutations.
    pub fn get_consumer_data_set_mut(&mut self) -> Option<SharedHeartbeatConsumerDataSet> {
        self.consumer_data_set.clone()
    }

    pub fn set_consumer_data_set(&mut self, value: Option<SharedHeartbeatConsumerDataSet>) {
        self.consumer_data_set = value;
    }

    pub fn set_consumer_data_set_owned(&mut self, value: Option<Vec<Option<ConsumerData>>>) {
        self.consumer_data_set = value.map(|values| {
            SharedValue::from(
                values
                    .into_iter()
                    .map(|consumer| consumer.map(SharedValue::from))
                    .collect::<HeartbeatConsumerDataSet>(),
            )
        });
    }

    pub fn get_heartbeat_fingerprint(&self) -> i32 {
        self.heartbeat_fingerprint
    }

    pub fn set_heartbeat_fingerprint(&mut self, value: i32) {
        self.heartbeat_fingerprint = value;
    }

    pub fn is_without_sub(&self) -> bool {
        self.is_without_sub
    }

    pub fn set_without_sub(&mut self, value: bool) {
        self.is_without_sub = value;
    }

    /// Decodes the inherited Java `RemotingSerializable.decode` form.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes the nullable inherited Java utility, including null bytes and
    /// a JSON `null` root.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.decodeList` form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes a nullable Java list, preserving null roots and members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        serde_fastjson::from_fastjson_list_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.fromJson` form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_str(json)
    }

    /// Decodes the nullable Java `fromJson` utility, including JSON `null`.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_str_nullable(json)
    }

    /// Java `RemotingSerializable.toJson(Object, prettyFormat)` and the
    /// inherited instance `toJson(boolean)` serialize Fastjson's sorted bean
    /// fields. `serde_json::Value` supplies the same deterministic key order.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        let value = serde_json::to_value(self)?;
        if pretty_format {
            serde_json::to_string_pretty(&value)
        } else {
            serde_json::to_string(&value)
        }
    }

    /// Java `RemotingSerializable.encode(Object)` and inherited `encode()`
    /// return the compact UTF-8 representation produced by `java_to_json`.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Encodes with the supported inherited Fastjson2 writer features.
    pub fn java_encode_with_features(
        &self,
        features: &[crate::JavaJsonWriterFeature],
    ) -> Result<Vec<u8>, serde_json::Error> {
        if features.is_empty() {
            return self.java_encode();
        }
        Ok(crate::route::java_fastjson_json_with_features(self, false, features)?.into_bytes())
    }

    /// Java `HeartbeatData.toString`; set ordering is normalized because the
    /// source uses HashSet iteration with unspecified order.
    pub fn java_to_string(&self) -> String {
        format!(
            "HeartbeatData [clientID={}, producerDataSet={}, consumerDataSet={}]",
            java_nullable_string(&self.client_id),
            java_shared_hash_set_to_string(&self.producer_data_set, |producer| {
                producer.with(ProducerData::java_to_string)
            }),
            java_shared_hash_set_to_string(&self.consumer_data_set, |consumer| {
                consumer.with(ConsumerData::java_to_string)
            }),
        )
    }

    /// Computes the client-local heartbeat fingerprint using Java 5.5.1's
    /// JSON-copy defaults, normalized fields and Java UTF-16 string hash.
    pub fn compute_heartbeat_fingerprint(&self) -> Result<i32, HeartbeatFingerprintError> {
        let mut value = serde_json::to_value(self)?;
        // Java serializes and reparses a copy first. Omitted nullable
        // collections regain their constructor-empty values during parsing.
        if !value["producerDataSet"].is_array() {
            value["producerDataSet"] = serde_json::json!([]);
        }
        if !value["consumerDataSet"].is_array() {
            value["consumerDataSet"] = serde_json::json!([]);
        }
        value["clientID"] = "".into();
        value["heartbeatFingerprint"] = 0.into();
        value["withoutSub"] = false.into();
        if let Some(consumers) = value["consumerDataSet"].as_array_mut() {
            for consumer in consumers {
                if consumer.is_null() {
                    return Err(HeartbeatFingerprintError::NullConsumerData);
                }
                if !consumer["subscriptionDataSet"].is_array() {
                    consumer["subscriptionDataSet"] = serde_json::json!([]);
                }
                if let Some(subscriptions) = consumer["subscriptionDataSet"].as_array_mut() {
                    for subscription in subscriptions {
                        if subscription.is_null() {
                            return Err(HeartbeatFingerprintError::NullSubscriptionData);
                        }
                        subscription["subVersion"] = 0.into();
                    }
                }
            }
        }
        fn canonicalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        canonicalize(value);
                    }
                    values.sort_unstable_by_key(serde_json::Value::to_string);
                }
                serde_json::Value::Object(values) => {
                    for value in values.values_mut() {
                        canonicalize(value);
                    }
                }
                _ => {}
            }
        }
        canonicalize(&mut value);
        Ok(value.to_string().encode_utf16().fold(0i32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeartbeatFingerprintError {
    #[error("heartbeat fingerprint JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Java NullPointerException: null ConsumerData in consumerDataSet")]
    NullConsumerData,
    #[error("Java NullPointerException: null SubscriptionData in subscriptionDataSet")]
    NullSubscriptionData,
}

struct IdentitySetVisitor<T>(PhantomData<T>);

impl<'de, T> Visitor<'de> for IdentitySetVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = Option<Vec<Option<T>>>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON array, object, or null for a Java Set field")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<Option<T>>()? {
            values.push(value);
        }
        Ok(Some(deduplicate_null_set_member(values)))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let value = T::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
        Ok(Some(vec![Some(value)]))
    }
}

fn deduplicate_null_set_member<T>(mut values: Vec<Option<T>>) -> Vec<Option<T>> {
    let mut retained_null = false;
    values.retain(|value| {
        if value.is_some() {
            true
        } else if retained_null {
            false
        } else {
            retained_null = true;
            true
        }
    });
    values
}

fn deserialize_identity_set_with_null<'de, T, D>(
    deserializer: D,
) -> Result<Option<Vec<Option<T>>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(IdentitySetVisitor(PhantomData))
}

pub(crate) fn deserialize_identity_shared_hash_set_with_null<'de, T, D>(
    deserializer: D,
) -> Result<Option<SharedIdentityNullableSet<T>>, D::Error>
where
    T: Deserialize<'de>,
    SharedValue<T>: Eq + Hash,
    D: Deserializer<'de>,
{
    deserialize_identity_set_with_null::<T, D>(deserializer).map(|values| {
        values.map(|values| {
            SharedValue::from(
                values
                    .into_iter()
                    .map(|value| value.map(SharedValue::from))
                    .collect::<HashSet<Option<SharedValue<T>>>>(),
            )
        })
    })
}

fn deserialize_nullable_hash_set_with_null<'de, T, D>(
    deserializer: D,
) -> Result<Option<HashSet<Option<T>>>, D::Error>
where
    T: Deserialize<'de> + Eq + std::hash::Hash,
    D: Deserializer<'de>,
{
    deserialize_identity_set_with_null(deserializer)
        .map(|values| values.map(|values| values.into_iter().collect()))
}

fn empty_producers() -> Option<SharedProducerDataSet> {
    Some(SharedValue::default())
}

fn empty_consumers() -> Option<SharedHeartbeatConsumerDataSet> {
    Some(SharedValue::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn subscription_fields_match_invoked_java_nulls_and_defaults() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["subscriptionFieldCases"].as_array().unwrap() {
            let raw = case["rawInput"].as_str().unwrap();
            let before = current_millis();
            let value: SubscriptionData = serde_json::from_str(raw).unwrap();
            let after = current_millis();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.topic.is_none(), case["topicNull"]);
            assert_eq!(value.sub_string.is_none(), case["subStringNull"]);
            assert_eq!(value.tags_set.is_none(), case["tagsNull"]);
            assert_eq!(value.code_set.is_none(), case["codeNull"]);
            assert_eq!(value.expression_type.is_none(), case["expressionNull"]);
            assert_eq!(
                value.filter_class_source,
                case["filterClassSource"].as_str().map(str::to_owned)
            );
            assert_eq!(
                value.filter_class_source.is_none(),
                case["filterClassSourceNull"]
            );
            assert_eq!(
                value
                    .tags_set
                    .as_ref()
                    .is_some_and(|values| values.with(HashSet::is_empty)),
                case["tagsEmpty"]
            );
            assert_eq!(
                value
                    .code_set
                    .as_ref()
                    .is_some_and(|values| values.with(HashSet::is_empty)),
                case["codeEmpty"]
            );
            assert_eq!(
                value
                    .tags_set
                    .as_ref()
                    .map(|values| values.with(HashSet::len)),
                case["tagsCount"].as_u64().map(|count| count as usize)
            );
            assert_eq!(
                value
                    .code_set
                    .as_ref()
                    .map(|values| values.with(HashSet::len)),
                case["codeCount"].as_u64().map(|count| count as usize)
            );
            assert_eq!(
                value
                    .tags_set
                    .as_ref()
                    .is_some_and(|values| values.with(|values| values.contains(&None))),
                case["tagsHasNull"]
            );
            assert_eq!(
                value
                    .code_set
                    .as_ref()
                    .is_some_and(|values| values.with(|values| values.contains(&None))),
                case["codeHasNull"]
            );
            if case["versionCurrent"] == true {
                assert!((before..=after).contains(&value.sub_version));
            }
            assert_eq!(value.sub_version == 0, case["versionZero"]);
            assert_eq!(value.sub_version == 42, case["versionFortyTwo"]);
            for (field, marker) in [
                ("topic", "encodedHasTopic"),
                ("subString", "encodedHasSubString"),
                ("tagsSet", "encodedHasTags"),
                ("codeSet", "encodedHasCode"),
                ("expressionType", "encodedHasExpression"),
                ("filterClassSource", "encodedHasFilterClassSource"),
            ] {
                assert_eq!(encoded.get(field).is_some(), case[marker], "{field}");
                if field == "filterClassSource" {
                    assert_eq!(
                        encoded
                            .get(field)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                        case["encodedFilterClassSource"]
                    );
                }
            }
            for (field, marker) in [("tagsSet", "tagsHasNull"), ("codeSet", "codeHasNull")] {
                assert_eq!(
                    encoded
                        .get(field)
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|values| values.contains(&serde_json::Value::Null)),
                    case[marker],
                    "{field} null member"
                );
            }
            for (field, expected_field) in [("tagsSet", "encodedTags"), ("codeSet", "encodedCode")]
            {
                if let Some(expected) = case[expected_field].as_array() {
                    let mut expected = expected.clone();
                    let mut actual = encoded[field].as_array().unwrap().clone();
                    expected.sort_by_key(serde_json::Value::to_string);
                    actual.sort_by_key(serde_json::Value::to_string);
                    assert_eq!(actual, expected, "{field} JSON values");
                }
            }
        }
    }

    #[test]
    fn subscription_constant_and_constructors_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        assert_eq!(
            SubscriptionData::SUB_ALL,
            fixture["subscriptionMethods"]["subAll"]
        );
        for case in fixture["subscriptionConstructorCases"].as_array().unwrap() {
            let value = match case["case"].as_str().unwrap() {
                "no-argument" => SubscriptionData::default(),
                "two-argument" => SubscriptionData::new(Some("topic".into()), Some("tag".into())),
                "two-null-arguments" => SubscriptionData::new(None, None),
                name => panic!("unknown SubscriptionData constructor case: {name}"),
            };
            assert_eq!(value.topic, case["topic"].as_str().map(str::to_owned));
            assert_eq!(
                value.sub_string,
                case["subString"].as_str().map(str::to_owned)
            );
            assert_eq!(
                value
                    .tags_set
                    .as_ref()
                    .is_some_and(|values| values.with(HashSet::is_empty)),
                case["tagsEmpty"]
            );
            assert_eq!(
                value
                    .code_set
                    .as_ref()
                    .is_some_and(|values| values.with(HashSet::is_empty)),
                case["codeEmpty"]
            );
            assert_eq!(
                value.expression_type,
                case["expressionType"].as_str().map(str::to_owned)
            );
            assert_eq!(value.sub_version > 0, case["versionCurrent"]);
        }
    }

    #[test]
    fn subscription_equality_matches_invoked_java_method() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["subscriptionMethods"]["equalCases"]
            .as_array()
            .unwrap()
        {
            let mut left: SubscriptionData =
                serde_json::from_str(case["leftInput"].as_str().unwrap()).unwrap();
            let mut right: SubscriptionData =
                serde_json::from_str(case["rightInput"].as_str().unwrap()).unwrap();
            left.filter_class_source = case["leftFilterClassSource"].as_str().map(str::to_owned);
            right.filter_class_source = case["rightFilterClassSource"].as_str().map(str::to_owned);
            assert_eq!(left == right, case["equal"], "{}", case["case"]);
        }
    }

    #[test]
    fn subscription_methods_match_invoked_java_hash_text_and_comparison() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let methods = &fixture["subscriptionMethods"];
        for case in methods["values"].as_array().unwrap() {
            let mut value: SubscriptionData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            value.filter_class_source = Some("ignored-in-text-and-equality".to_owned());
            assert_eq!(
                value.java_hash_code(),
                case["hashCode"].as_i64().unwrap() as i32,
                "{} hashCode",
                case["case"]
            );
            assert_eq!(
                value.java_to_string(),
                case["toString"],
                "{} toString",
                case["case"]
            );
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(
                encoded.get("filterClassSource").is_some(),
                case["encodedHasFilterClassSource"],
                "{} JSON field",
                case["case"]
            );
            assert_eq!(
                encoded
                    .get("filterClassSource")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                case["encodedFilterClassSource"],
                "{} JSON value",
                case["case"]
            );
        }
        for case in methods["equalCases"].as_array().unwrap() {
            let left: SubscriptionData =
                serde_json::from_str(case["leftInput"].as_str().unwrap()).unwrap();
            let right: SubscriptionData =
                serde_json::from_str(case["rightInput"].as_str().unwrap()).unwrap();
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
        }
        for case in methods["compareCases"].as_array().unwrap() {
            let left: SubscriptionData =
                serde_json::from_str(case["leftInput"].as_str().unwrap()).unwrap();
            let right: SubscriptionData =
                serde_json::from_str(case["rightInput"].as_str().unwrap()).unwrap();
            assert_eq!(
                left.java_compare_to(&right),
                case["comparison"].as_i64().unwrap() as i32,
                "{} compareTo",
                case["case"]
            );
        }
        assert_eq!(methods["compareNullOtherException"], "NullPointerException");
        assert_eq!(
            SubscriptionData::default().java_compare_to_nullable(None),
            Err(SubscriptionDataCompareError::NullOther)
        );
    }

    #[test]
    // Java HashSet stores mutable DTO references, so this test intentionally
    // exercises interior-mutable structural keys after insertion.
    #[allow(clippy::mutable_key_type)]
    fn heartbeat_dto_public_accessors_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();

        let subscription_case = &fixture["subscriptionDataApiCase"];
        let mut subscription = SubscriptionData::default();
        assert!(!subscription.is_class_filter_mode());
        assert!(subscription.get_topic().is_none());
        assert!(subscription.get_sub_string().is_none());
        assert_eq!(
            serde_json::to_value(subscription.get_tags_set()).unwrap(),
            subscription_case["defaults"]["tagsSet"]
        );
        assert_eq!(
            serde_json::to_value(subscription.get_code_set()).unwrap(),
            subscription_case["defaults"]["codeSet"]
        );
        assert_eq!(
            subscription.get_sub_version() > 0,
            subscription_case["defaults"]["subVersionCurrent"]
        );
        assert_eq!(subscription.get_expression_type(), Some("TAG"));
        assert!(subscription.get_filter_class_source().is_none());

        subscription.set_class_filter_mode(true);
        subscription.set_topic(Some("orders".to_owned()));
        subscription.set_sub_string(Some("*".to_owned()));
        let mut tags = HashSet::new();
        tags.insert(Some("gold".to_owned()));
        let tags = SharedValue::from(tags);
        subscription.set_tags_set(Some(tags.clone()));
        let mut codes = HashSet::new();
        codes.insert(Some(7));
        let codes = SharedValue::from(codes);
        subscription.set_code_set(Some(codes.clone()));
        subscription.set_sub_version(42);
        subscription.set_expression_type(Some("SQL92".to_owned()));
        subscription.set_filter_class_source(Some("source-a".to_owned()));
        assert_eq!(
            json!({
                "classFilterMode": subscription.is_class_filter_mode(),
                "topic": subscription.get_topic(),
                "subString": subscription.get_sub_string(),
                "tagsSet": subscription.get_tags_set(),
                "codeSet": subscription.get_code_set(),
                "subVersion": subscription.get_sub_version(),
                "expressionType": subscription.get_expression_type(),
                "filterClassSource": subscription.get_filter_class_source(),
            }),
            subscription_case["values"]
        );
        // The Rust handles preserve the same caller/getter Set alias Java exposes.
        assert_eq!(subscription_case["getterReturnsAssignedTagsSet"], true);
        assert_eq!(subscription_case["getterReturnsAssignedCodeSet"], true);
        assert!(tags.ptr_eq(&subscription.get_tags_set().unwrap()));
        assert!(codes.ptr_eq(&subscription.get_code_set().unwrap()));
        assert_eq!(subscription_case["externalTagMutationVisible"], true);
        assert_eq!(
            subscription_case["tagGetterMutationVisibleToAssignedSet"],
            true
        );
        assert_eq!(subscription_case["externalCodeMutationVisible"], true);
        assert_eq!(
            subscription_case["codeGetterMutationVisibleToAssignedSet"],
            true
        );
        tags.with_mut(|tags| {
            tags.insert(Some("silver".to_owned()));
        });
        subscription.get_tags_set_mut().unwrap().with_mut(|tags| {
            tags.insert(Some("bronze".to_owned()));
        });
        let mut mutated_tags = tags.snapshot().into_iter().collect::<Vec<_>>();
        mutated_tags.sort();
        assert_eq!(json!(mutated_tags), subscription_case["mutatedTags"]);
        codes.with_mut(|codes| {
            codes.insert(Some(9));
        });
        subscription.get_code_set_mut().unwrap().with_mut(|codes| {
            codes.insert(Some(11));
        });
        let mut mutated_codes = codes.snapshot().into_iter().collect::<Vec<_>>();
        mutated_codes.sort();
        assert_eq!(json!(mutated_codes), subscription_case["mutatedCodes"]);
        subscription.set_class_filter_mode(false);
        subscription.set_topic(None);
        subscription.set_sub_string(None);
        subscription.set_tags_set(None);
        subscription.set_code_set(None);
        subscription.set_sub_version(0);
        subscription.set_expression_type(None);
        subscription.set_filter_class_source(None);
        assert!(subscription.get_topic().is_none());
        assert!(subscription.get_sub_string().is_none());
        assert!(subscription.get_tags_set().is_none());
        assert!(subscription.get_code_set().is_none());
        assert!(subscription.get_expression_type().is_none());
        assert!(subscription.get_filter_class_source().is_none());
        assert_eq!(subscription_case["nullSettersRemainNull"], true);
        assert_eq!(
            json!({
                "classFilterMode": subscription.is_class_filter_mode(),
                "subVersion": subscription.get_sub_version(),
            }),
            subscription_case["primitiveAfterReset"]
        );

        let producer_case = &fixture["producerDataApiCase"];
        let mut producer = ProducerData::default();
        assert!(producer.get_group_name().is_none());
        producer.set_group_name(Some("producers".to_owned()));
        assert_eq!(
            json!({"groupName": producer.get_group_name()}),
            producer_case["values"]
        );
        producer.set_group_name(None);
        assert!(producer.get_group_name().is_none());
        assert_eq!(producer_case["nullSetterRemainsNull"], true);

        let consumer_case = &fixture["consumerDataApiCase"];
        let mut consumer = ConsumerData::default();
        assert!(consumer.get_group_name().is_none());
        assert!(consumer.get_consume_type().is_none());
        assert!(consumer.get_message_model().is_none());
        assert!(consumer.get_consume_from_where().is_none());
        assert!(
            consumer
                .get_subscription_data_set()
                .is_some_and(|set| set.with(HashSet::is_empty))
        );
        assert!(!consumer.is_unit_mode());
        consumer.set_group_name(Some("workers".to_owned()));
        consumer.set_consume_type(Some(ConsumeType::ConsumeActively));
        consumer.set_message_model(Some(MessageModel::Clustering));
        consumer.set_consume_from_where(Some(ConsumeFromWhere::ConsumeFromLastOffset));
        let mut subscriptions = HashSet::new();
        let mut subscription =
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned()));
        subscription.set_sub_version(42);
        let subscription = SharedValue::from(subscription);
        subscriptions.insert(Some(subscription.clone()));
        let subscriptions = SharedValue::from(subscriptions);
        consumer.set_subscription_data_set(Some(subscriptions.clone()));
        consumer.set_unit_mode(true);
        assert_eq!(
            json!({
                "groupName": consumer.get_group_name(),
                "consumeType": consumer.get_consume_type(),
                "messageModel": consumer.get_message_model(),
                "consumeFromWhere": consumer.get_consume_from_where(),
                "subscriptionDataSet": consumer.get_subscription_data_set(),
                "unitMode": consumer.is_unit_mode(),
            }),
            consumer_case["values"]
        );
        // Caller/getter Sets and nested DTO handles retain their Java aliases.
        assert_eq!(consumer_case["getterReturnsAssignedSubscriptionSet"], true);
        assert_eq!(
            consumer_case["getterReturnsAssignedSubscriptionElement"],
            true
        );
        assert!(subscriptions.ptr_eq(&consumer.get_subscription_data_set().unwrap()));
        let retained_subscription = consumer
            .get_subscription_data_set()
            .unwrap()
            .with(|set| set.iter().flatten().next().unwrap().clone());
        assert!(subscription.ptr_eq(&retained_subscription));
        subscription.with_mut(|value| value.set_sub_version(43));
        assert_eq!(
            consumer_case["callerSubscriptionMutationVisibleThroughGetter"],
            true
        );
        assert_eq!(
            retained_subscription.with(|value| value.get_sub_version()),
            43
        );
        retained_subscription.with_mut(|value| value.set_sub_version(44));
        assert_eq!(
            consumer_case["getterSubscriptionMutationVisibleToCaller"],
            true
        );
        assert_eq!(subscription.with(|value| value.get_sub_version()), 44);
        assert_eq!(consumer_case["externalSubscriptionMutationVisible"], true);
        assert_eq!(
            consumer_case["subscriptionGetterMutationVisibleToAssignedSet"],
            true
        );
        subscriptions.with_mut(|subscriptions| {
            let mut payment =
                SubscriptionData::new(Some("payments".to_owned()), Some("*".to_owned()));
            payment.set_sub_version(43);
            subscriptions.insert(Some(SharedValue::from(payment)));
        });
        consumer
            .get_subscription_data_set_mut()
            .unwrap()
            .with_mut(|subscriptions| {
                let mut refund =
                    SubscriptionData::new(Some("refunds".to_owned()), Some("*".to_owned()));
                refund.set_sub_version(44);
                subscriptions.insert(Some(SharedValue::from(refund)));
            });
        assert_eq!(
            subscriptions.with(HashSet::len),
            consumer_case["mutatedSubscriptionDataSetSize"]
                .as_u64()
                .unwrap() as usize
        );
        consumer.set_group_name(None);
        consumer.set_consume_type(None);
        consumer.set_message_model(None);
        consumer.set_consume_from_where(None);
        consumer.set_subscription_data_set(None);
        consumer.set_unit_mode(false);
        assert!(consumer.get_group_name().is_none());
        assert!(consumer.get_consume_type().is_none());
        assert!(consumer.get_message_model().is_none());
        assert!(consumer.get_consume_from_where().is_none());
        assert!(consumer.get_subscription_data_set().is_none());
        assert!(!consumer.is_unit_mode());
        assert_eq!(consumer_case["nullSettersRemainNull"], true);

        let heartbeat_case = &fixture["heartbeatDataApiCase"];
        let mut heartbeat = HeartbeatData::default();
        assert!(heartbeat.get_client_id().is_none());
        assert!(
            heartbeat
                .get_producer_data_set()
                .is_some_and(|set| set.with(HashSet::is_empty))
        );
        assert!(
            heartbeat
                .get_consumer_data_set()
                .is_some_and(|set| set.with(HashSet::is_empty))
        );
        assert_eq!(heartbeat.get_heartbeat_fingerprint(), 0);
        assert!(!heartbeat.is_without_sub());
        heartbeat.set_client_id(Some("client-a".to_owned()));
        let mut producer = ProducerData::default();
        producer.set_group_name(Some("producers".to_owned()));
        let producer = SharedValue::from(producer);
        let producers = SharedValue::from(HashSet::from([Some(producer.clone())]));
        heartbeat.set_producer_data_set(Some(producers.clone()));
        let mut consumer = ConsumerData::default();
        consumer.set_group_name(Some("workers".to_owned()));
        let consumer = SharedValue::from(consumer);
        let consumers = SharedValue::from(HashSet::from([Some(consumer.clone())]));
        heartbeat.set_consumer_data_set(Some(consumers.clone()));
        heartbeat.set_heartbeat_fingerprint(7);
        heartbeat.set_without_sub(true);
        assert_eq!(
            json!({
                "clientID": heartbeat.get_client_id(),
                "producerDataSet": heartbeat.get_producer_data_set(),
                "consumerDataSet": heartbeat.get_consumer_data_set(),
                "heartbeatFingerprint": heartbeat.get_heartbeat_fingerprint(),
                "withoutSub": heartbeat.is_without_sub(),
            }),
            heartbeat_case["values"]
        );
        // Caller/getter Sets and their identity-based elements retain aliases.
        assert_eq!(heartbeat_case["getterReturnsAssignedProducerSet"], true);
        assert_eq!(heartbeat_case["getterReturnsAssignedConsumerSet"], true);
        assert_eq!(heartbeat_case["getterReturnsAssignedProducer"], true);
        assert_eq!(heartbeat_case["getterReturnsAssignedConsumer"], true);
        assert!(producers.ptr_eq(&heartbeat.get_producer_data_set().unwrap()));
        assert!(consumers.ptr_eq(&heartbeat.get_consumer_data_set().unwrap()));
        assert!(
            producer.ptr_eq(&producers.with(|set| set.iter().flatten().next().unwrap().clone()))
        );
        assert!(
            consumer.ptr_eq(&consumers.with(|set| set.iter().flatten().next().unwrap().clone()))
        );
        producer.with_mut(|value| value.set_group_name(Some("caller-producer-update".into())));
        assert_eq!(
            heartbeat_case["callerProducerMutationVisibleThroughGetter"],
            true
        );
        let retained_producer = producers.with(|set| set.iter().flatten().next().unwrap().clone());
        assert_eq!(
            retained_producer.with(|value| value.get_group_name().map(str::to_owned)),
            Some("caller-producer-update".to_owned())
        );
        retained_producer
            .with_mut(|value| value.set_group_name(Some("getter-producer-update".into())));
        assert_eq!(
            heartbeat_case["getterProducerMutationVisibleToCaller"],
            true
        );
        assert_eq!(
            producer.with(|value| value.get_group_name().map(str::to_owned)),
            Some("getter-producer-update".to_owned())
        );
        consumer.with_mut(|value| value.set_group_name(Some("caller-consumer-update".into())));
        assert_eq!(
            heartbeat_case["callerConsumerMutationVisibleThroughGetter"],
            true
        );
        let retained_consumer = consumers.with(|set| set.iter().flatten().next().unwrap().clone());
        assert_eq!(
            retained_consumer.with(|value| value.get_group_name().map(str::to_owned)),
            Some("caller-consumer-update".to_owned())
        );
        retained_consumer
            .with_mut(|value| value.set_group_name(Some("getter-consumer-update".into())));
        assert_eq!(
            heartbeat_case["getterConsumerMutationVisibleToCaller"],
            true
        );
        assert_eq!(
            consumer.with(|value| value.get_group_name().map(str::to_owned)),
            Some("getter-consumer-update".to_owned())
        );
        assert_eq!(heartbeat_case["externalProducerMutationVisible"], true);
        assert_eq!(
            heartbeat_case["producerGetterMutationVisibleToAssignedSet"],
            true
        );
        assert_eq!(heartbeat_case["externalConsumerMutationVisible"], true);
        assert_eq!(
            heartbeat_case["consumerGetterMutationVisibleToAssignedSet"],
            true
        );
        let producer_data_set_len = {
            let producer_data_set_mut = heartbeat.get_producer_data_set_mut().unwrap();
            for group in ["producers-b", "producers-c"] {
                let mut extra = ProducerData::default();
                extra.set_group_name(Some(group.to_owned()));
                producer_data_set_mut.with_mut(|set| {
                    set.insert(Some(SharedValue::from(extra)));
                });
            }
            producer_data_set_mut.with(HashSet::len)
        };
        let consumer_data_set_len = {
            let consumer_data_set_mut = heartbeat.get_consumer_data_set_mut().unwrap();
            for group in ["consumers", "admins"] {
                let mut extra = ConsumerData::default();
                extra.set_group_name(Some(group.to_owned()));
                consumer_data_set_mut.with_mut(|set| {
                    set.insert(Some(SharedValue::from(extra)));
                });
            }
            consumer_data_set_mut.with(HashSet::len)
        };
        assert_eq!(
            json!({
                "producerDataSet": producer_data_set_len,
                "consumerDataSet": consumer_data_set_len,
            }),
            heartbeat_case["mutatedSetSizes"]
        );
        heartbeat.set_client_id(None);
        heartbeat.set_producer_data_set(None);
        heartbeat.set_consumer_data_set(None);
        heartbeat.set_heartbeat_fingerprint(0);
        heartbeat.set_without_sub(false);
        assert!(heartbeat.get_client_id().is_none());
        assert!(heartbeat.get_producer_data_set().is_none());
        assert!(heartbeat.get_consumer_data_set().is_none());
        assert_eq!(heartbeat_case["nullSettersRemainNull"], true);
        assert_eq!(
            json!({
                "heartbeatFingerprint": heartbeat.get_heartbeat_fingerprint(),
                "withoutSub": heartbeat.is_without_sub(),
            }),
            heartbeat_case["primitiveAfterReset"]
        );
    }

    #[test]
    fn consumer_enum_inputs_match_invoked_java_names_and_rejections() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = fixture["consumerEnumCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            65,
            "Java oracle includes enum name and ordinal coercions"
        );
        for case in cases {
            let input = case["rawInput"].as_str().unwrap();
            match case["outcome"].as_str().unwrap() {
                "accepted" | "null" => {
                    let value: ConsumerData = serde_json::from_str(input)
                        .unwrap_or_else(|error| panic!("{}: {error}", case["rawInput"]));
                    let encoded = serde_json::to_value(value).unwrap();
                    assert_eq!(
                        encoded
                            .get(case["field"].as_str().unwrap())
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                        case["encodedName"],
                        "{} encode",
                        case["rawInput"]
                    );
                }
                "rejected" => assert!(
                    serde_json::from_str::<ConsumerData>(input).is_err(),
                    "{} should reject like Java ({})",
                    case["rawInput"],
                    case["error"]
                ),
                outcome => panic!("unknown Java enum outcome: {outcome}"),
            }
        }
    }

    #[test]
    fn consumer_enum_public_methods_match_invoked_java_values() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["consumerEnumMethodCases"].as_array().unwrap() {
            let actual = match case["enum"].as_str().unwrap() {
                "consumeType" => {
                    let value: ConsumeType =
                        serde_json::from_value(case["constant"].clone()).unwrap();
                    value.type_cn()
                }
                "messageModel" => {
                    let value: MessageModel =
                        serde_json::from_value(case["constant"].clone()).unwrap();
                    value.mode_cn()
                }
                kind => panic!("unknown enum kind: {kind}"),
            };
            assert_eq!(actual, case["methodValue"].as_str().unwrap(), "{case}");
        }
    }

    #[test]
    fn heartbeat_dto_identity_methods_match_invoked_java_object_behavior() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = fixture["identityMethodCases"].as_array().unwrap();
        assert_eq!(cases.len(), 3);
        for case in cases {
            assert_eq!(case["equalsDeclaringClass"], "java.lang.Object", "{case}");
            assert_eq!(case["hashCodeDeclaringClass"], "java.lang.Object", "{case}");
            assert_eq!(case["sameInstanceEquals"], true, "{case}");
            assert_eq!(case["equalFieldsDistinctInstanceEquals"], false, "{case}");
            assert_eq!(case["equalFieldsSetSize"], 2, "{case}");
        }
    }

    #[test]
    fn heartbeat_inherited_serialization_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let methods = &fixture["heartbeatSerializationMethodCases"];
        assert_eq!(methods["staticEncodeNullIsNull"], true);
        assert_eq!(methods["staticToJsonNull"], "null");
        assert_eq!(methods["decodeNullIsNull"], true);
        assert_eq!(methods["decodeListNullIsNull"], true);
        assert_eq!(methods["fromJsonNullIsNull"], true);
        assert_eq!(methods["decodeJsonNullIsNull"], true);
        assert_eq!(methods["decodeListJsonNullIsNull"], true);
        assert_eq!(methods["fromJsonJsonNullIsNull"], true);
        assert_eq!(methods["decodeListPreservesNullMember"], true);
        assert_eq!(
            methods["featureEncodeParameterType"],
            "com.alibaba.fastjson2.JSONWriter$Feature"
        );

        for case in methods["cases"].as_array().unwrap() {
            let raw_input = case["rawInput"].as_str().unwrap();
            let value = HeartbeatData::java_from_json(raw_input).unwrap();
            let native_json = value.java_to_json(false).unwrap();
            assert_eq!(
                value.java_encode_with_features(&[]).unwrap(),
                native_json.as_bytes(),
                "{} empty writer-feature array",
                case["case"]
            );
            let browser_compatible = value
                .java_encode_with_features(&[crate::JavaJsonWriterFeature::BrowserCompatible])
                .unwrap();
            let expected_browser_compatible = case["writerFeatureOverrides"]["BrowserCompatible"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                browser_compatible,
                expected_browser_compatible.as_bytes(),
                "{} BrowserCompatible",
                case["case"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[crate::JavaJsonWriterFeature::OptimizedForAscii,])
                    .unwrap(),
                case["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                "{} OptimizedForAscii",
                case["case"]
            );
            assert_eq!(
                native_json, case["instanceJson"],
                "{} instance encode",
                case["case"]
            );
            assert_eq!(
                native_json, case["staticJson"],
                "{} static toJson",
                case["case"]
            );
            assert_eq!(
                native_json, case["instanceEncodedJson"],
                "{} encode bytes",
                case["case"]
            );
            assert_eq!(
                native_json, case["staticEncodedJson"],
                "{} static encode",
                case["case"]
            );
            assert_eq!(
                native_json, case["featureEncodedJson"],
                "{} feature encode",
                case["case"]
            );
            let featured = value
                .java_encode_with_features(&[
                    crate::JavaJsonWriterFeature::WriteNonStringValueAsString,
                ])
                .unwrap();
            let expected_featured = case["writerFeatureOverrides"]["WriteNonStringValueAsString"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                featured,
                expected_featured.as_bytes(),
                "{} WriteNonStringValueAsString",
                case["case"]
            );
            let boolean_featured = value
                .java_encode_with_features(&[crate::JavaJsonWriterFeature::WriteBooleanAsNumber])
                .unwrap();
            let expected_boolean_featured = case["writerFeatureOverrides"]["WriteBooleanAsNumber"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                boolean_featured,
                expected_boolean_featured.as_bytes(),
                "{} WriteBooleanAsNumber",
                case["case"]
            );
            let combined_featured = value
                .java_encode_with_features(&[
                    crate::JavaJsonWriterFeature::WriteBooleanAsNumber,
                    crate::JavaJsonWriterFeature::WriteNonStringValueAsString,
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
                case["case"]
            );
            let escaped_featured = value
                .java_encode_with_features(&[crate::JavaJsonWriterFeature::EscapeNoneAscii])
                .unwrap();
            let expected_escaped_featured = case["writerFeatureOverrides"]["EscapeNoneAscii"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                escaped_featured,
                expected_escaped_featured.as_bytes(),
                "{} EscapeNoneAscii",
                case["case"]
            );
            let pretty_featured = value
                .java_encode_with_features(&[crate::JavaJsonWriterFeature::PrettyFormat])
                .unwrap();
            let expected_pretty_featured = case["writerFeatureOverrides"]["PrettyFormat"]
                .as_str()
                .unwrap_or_else(|| case["instancePrettyJson"].as_str().unwrap());
            assert_eq!(
                pretty_featured,
                expected_pretty_featured.as_bytes(),
                "{} PrettyFormat",
                case["case"]
            );
            for (feature, feature_name) in [
                (
                    crate::JavaJsonWriterFeature::NotWriteEmptyArray,
                    "NotWriteEmptyArray",
                ),
                (crate::JavaJsonWriterFeature::IgnoreEmpty, "IgnoreEmpty"),
                (
                    crate::JavaJsonWriterFeature::WriteNonStringKeyAsString,
                    "WriteNonStringKeyAsString",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteLongAsString,
                    "WriteLongAsString",
                ),
                (
                    crate::JavaJsonWriterFeature::NotWriteDefaultValue,
                    "NotWriteDefaultValue",
                ),
                (
                    crate::JavaJsonWriterFeature::PrettyFormatWith2Space,
                    "PrettyFormatWith2Space",
                ),
                (
                    crate::JavaJsonWriterFeature::PrettyFormatWith4Space,
                    "PrettyFormatWith4Space",
                ),
                (
                    crate::JavaJsonWriterFeature::UseSingleQuotes,
                    "UseSingleQuotes",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteNameAsSymbol,
                    "WriteNameAsSymbol",
                ),
                (
                    crate::JavaJsonWriterFeature::NotWriteRootClassName,
                    "NotWriteRootClassName",
                ),
                (
                    crate::JavaJsonWriterFeature::NotWriteHashMapArrayListClassName,
                    "NotWriteHashMapArrayListClassName",
                ),
                (
                    crate::JavaJsonWriterFeature::NotWriteNumberClassName,
                    "NotWriteNumberClassName",
                ),
                (
                    crate::JavaJsonWriterFeature::NotWriteSetClassName,
                    "NotWriteSetClassName",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteByteArrayAsBase64,
                    "WriteByteArrayAsBase64",
                ),
                (
                    crate::JavaJsonWriterFeature::WritePairAsJavaBean,
                    "WritePairAsJavaBean",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteThrowableClassName,
                    "WriteThrowableClassName",
                ),
                (
                    crate::JavaJsonWriterFeature::WriterUtilDateAsMillis,
                    "WriterUtilDateAsMillis",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteBigDecimalAsPlain,
                    "WriteBigDecimalAsPlain",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteFloatSpecialAsString,
                    "WriteFloatSpecialAsString",
                ),
                (crate::JavaJsonWriterFeature::BrowserSecure, "BrowserSecure"),
                (
                    crate::JavaJsonWriterFeature::WriteEnumsUsingName,
                    "WriteEnumsUsingName",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteEnumUsingToString,
                    "WriteEnumUsingToString",
                ),
                (
                    crate::JavaJsonWriterFeature::IgnoreErrorGetter,
                    "IgnoreErrorGetter",
                ),
                (
                    crate::JavaJsonWriterFeature::IgnoreNonFieldGetter,
                    "IgnoreNonFieldGetter",
                ),
                (crate::JavaJsonWriterFeature::WriteNulls, "WriteNulls"),
                (
                    crate::JavaJsonWriterFeature::WriteMapNullValue,
                    "WriteMapNullValue",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteNullListAsEmpty,
                    "WriteNullListAsEmpty",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteNullStringAsEmpty,
                    "WriteNullStringAsEmpty",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteNullNumberAsZero,
                    "WriteNullNumberAsZero",
                ),
                (
                    crate::JavaJsonWriterFeature::NullAsDefaultValue,
                    "NullAsDefaultValue",
                ),
                (
                    crate::JavaJsonWriterFeature::WriteClassName,
                    "WriteClassName",
                ),
                (
                    crate::JavaJsonWriterFeature::ReferenceDetection,
                    "ReferenceDetection",
                ),
                (crate::JavaJsonWriterFeature::BeanToArray, "BeanToArray"),
                (crate::JavaJsonWriterFeature::FieldBased, "FieldBased"),
                (
                    crate::JavaJsonWriterFeature::WriteNullBooleanAsFalse,
                    "WriteNullBooleanAsFalse",
                ),
                (crate::JavaJsonWriterFeature::MapSortField, "MapSortField"),
                (
                    crate::JavaJsonWriterFeature::SortMapEntriesByKeys,
                    "SortMapEntriesByKeys",
                ),
                (crate::JavaJsonWriterFeature::LargeObject, "LargeObject"),
                (
                    crate::JavaJsonWriterFeature::UnquoteFieldName,
                    "UnquoteFieldName",
                ),
                (
                    crate::JavaJsonWriterFeature::IgnoreNoneSerializable,
                    "IgnoreNoneSerializable",
                ),
            ] {
                assert_eq!(
                    value.java_encode_with_features(&[feature]).unwrap(),
                    case["writerFeatureOverrides"][feature_name]
                        .as_str()
                        .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap())
                        .as_bytes(),
                    "{} {feature_name}",
                    case["case"]
                );
            }
            assert_eq!(
                value
                    .java_encode_with_features(&[
                        crate::JavaJsonWriterFeature::BrowserSecure,
                        crate::JavaJsonWriterFeature::UnquoteFieldName,
                    ])
                    .unwrap(),
                case["writerFeatureOverrides"]["BrowserSecure+UnquoteFieldName"]
                    .as_str()
                    .unwrap()
                    .as_bytes(),
                "{} BrowserSecure+UnquoteFieldName",
                case["case"]
            );
            assert_eq!(
                case["writerFeatureOverrides"]["ErrorOnNoneSerializable"]["error"], "JSONException",
                "{} Java error feature result",
                case["case"]
            );
            assert!(
                value
                    .java_encode_with_features(&[
                        crate::JavaJsonWriterFeature::ErrorOnNoneSerializable,
                    ])
                    .is_err(),
                "{} ErrorOnNoneSerializable",
                case["case"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[
                        crate::JavaJsonWriterFeature::ErrorOnNoneSerializable,
                        crate::JavaJsonWriterFeature::IgnoreNoneSerializable,
                    ])
                    .unwrap(),
                b"null",
                "{} IgnoreNoneSerializable takes precedence",
                case["case"]
            );
            assert_eq!(
                value.java_encode().unwrap(),
                native_json.as_bytes(),
                "{} UTF-8 encode",
                case["case"]
            );

            let pretty_json = value.java_to_json(true).unwrap();
            let actual_pretty: serde_json::Value = serde_json::from_str(&pretty_json).unwrap();
            for field in ["instancePrettyJson", "staticPrettyJson"] {
                let expected: serde_json::Value =
                    serde_json::from_str(case[field].as_str().unwrap()).unwrap();
                assert_eq!(actual_pretty, expected, "{} {field}", case["case"]);
            }

            let decoded_from_bytes = HeartbeatData::java_decode(native_json.as_bytes()).unwrap();
            let decoded_from_text = HeartbeatData::java_from_json(&native_json).unwrap();
            let decoded_list =
                HeartbeatData::java_decode_list(format!("[{native_json}]").as_bytes()).unwrap();
            for decoded in [
                decoded_from_bytes,
                decoded_from_text,
                decoded_list.into_iter().next().unwrap(),
            ] {
                let actual = serde_json::to_value(decoded).unwrap();
                for field in ["decodedJson", "fromJsonJson", "decodedListJson"] {
                    let expected: serde_json::Value =
                        serde_json::from_str(case[field].as_str().unwrap()).unwrap();
                    assert_eq!(actual, expected, "{} {field}", case["case"]);
                }
            }
            assert_eq!(case["decodedListCount"], 1, "{} decodeList", case["case"]);
        }

        assert_eq!(
            serde_json::to_string(&Option::<HeartbeatData>::None).unwrap(),
            methods["staticToJsonNull"]
        );
        assert!(
            serde_json::from_str::<Option<HeartbeatData>>("null")
                .unwrap()
                .is_none()
        );
        assert!(HeartbeatData::java_decode_nullable(None).unwrap().is_none());
        assert!(
            HeartbeatData::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        let decoded_null_members = HeartbeatData::java_decode_list_nullable(Some(b"[null,{}]"))
            .unwrap()
            .unwrap();
        assert_eq!(decoded_null_members.len(), 2);
        assert!(decoded_null_members[0].is_none());
        assert!(decoded_null_members[1].is_some());
    }

    #[test]
    fn heartbeat_writer_enum_ordinal_uses_java_consumer_enum_ordinals() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["heartbeatSerializationMethodCases"]["cases"]
            .as_array()
            .unwrap()
        {
            let value = HeartbeatData::java_from_json(case["rawInput"].as_str().unwrap()).unwrap();
            let mut expected: serde_json::Value =
                serde_json::from_str(case["instanceEncodedJson"].as_str().unwrap()).unwrap();
            if case["case"] == "populated" {
                expected["consumerDataSet"][0]["consumeType"] = serde_json::json!(0);
                expected["consumerDataSet"][0]["messageModel"] = serde_json::json!(1);
                expected["consumerDataSet"][0]["consumeFromWhere"] = serde_json::json!(0);
            }
            let expected = serde_json::to_string(&expected).unwrap();
            assert_eq!(
                value
                    .java_encode_with_features(&[
                        crate::JavaJsonWriterFeature::WriteEnumUsingOrdinal,
                    ])
                    .unwrap(),
                expected.as_bytes(),
                "{} WriteEnumUsingOrdinal",
                case["case"]
            );
        }
    }

    #[test]
    fn heartbeat_owner_fields_match_invoked_java_nulls_and_defaults() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = &fixture["ownerFieldCases"];
        for case in cases["producers"].as_array().unwrap() {
            let value: ProducerData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.group_name.is_none(), case["groupNull"]);
            assert_eq!(value.java_to_string(), case["toString"]);
            assert_eq!(encoded.get("groupName").is_some(), case["encodedHasGroup"]);
        }
        for case in cases["consumers"].as_array().unwrap() {
            let value: ConsumerData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.group_name.is_none(), case["groupNull"]);
            assert_eq!(value.java_to_string(), case["toString"]);
            assert_eq!(value.consume_type.is_none(), case["consumeTypeNull"]);
            assert_eq!(value.message_model.is_none(), case["messageModelNull"]);
            assert_eq!(
                value.consume_from_where.is_none(),
                case["consumeFromWhereNull"]
            );
            assert_eq!(
                value.subscription_data_set.is_none(),
                case["subscriptionsNull"]
            );
            assert_eq!(
                value
                    .subscription_data_set
                    .as_ref()
                    .is_some_and(|set| set.with(HashSet::is_empty)),
                case["subscriptionsEmpty"]
            );
            assert_eq!(value.unit_mode, case["unitMode"]);
            for (field, marker) in [
                ("groupName", "encodedHasGroup"),
                ("consumeType", "encodedHasConsumeType"),
                ("messageModel", "encodedHasMessageModel"),
                ("consumeFromWhere", "encodedHasConsumeFromWhere"),
                ("subscriptionDataSet", "encodedHasSubscriptions"),
            ] {
                assert_eq!(encoded.get(field).is_some(), case[marker], "{field}");
            }
        }
    }

    #[test]
    fn heartbeat_collections_match_invoked_java_nulls_and_defaults() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["heartbeatFieldCases"].as_array().unwrap() {
            let value: HeartbeatData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.java_to_string(), case["toString"]);
            assert_eq!(value.client_id.is_none(), case["clientNull"]);
            assert_eq!(value.producer_data_set.is_none(), case["producersNull"]);
            assert_eq!(value.consumer_data_set.is_none(), case["consumersNull"]);
            assert_eq!(
                value
                    .producer_data_set
                    .as_ref()
                    .map_or(-1, |set| set.with(HashSet::len) as i64),
                case["producerCount"]
            );
            assert_eq!(
                encoded["producerDataSet"]
                    .as_array()
                    .map_or(-1, |set| set.len() as i64),
                case["producerCount"]
            );
            assert_eq!(
                value
                    .consumer_data_set
                    .as_ref()
                    .map_or(-1, |set| set.with(HashSet::len) as i64),
                case["consumerCount"]
            );
            assert_eq!(value.heartbeat_fingerprint, case["fingerprint"]);
            assert_eq!(
                i64::from(value.compute_heartbeat_fingerprint().unwrap()),
                case["computedFingerprint"]
            );
            assert_eq!(value.is_without_sub, case["withoutSub"]);
            assert_eq!(encoded.get("clientID").is_some(), case["encodedHasClient"]);
            assert_eq!(
                encoded.get("producerDataSet").is_some(),
                case["encodedHasProducers"]
            );
            assert_eq!(
                encoded.get("consumerDataSet").is_some(),
                case["encodedHasConsumers"]
            );
        }
    }

    #[test]
    fn heartbeat_null_set_members_match_invoked_java() {
        fn sort_arrays(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        sort_arrays(value);
                    }
                    values.sort_unstable_by_key(serde_json::Value::to_string);
                }
                serde_json::Value::Object(values) => {
                    for value in values.values_mut() {
                        sort_arrays(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["heartbeatNullElementCases"].as_array().unwrap() {
            let data: HeartbeatData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            assert_eq!(
                data.producer_data_set
                    .as_ref()
                    .map(|set| set.with(HashSet::len)),
                case["producerCount"].as_u64().map(|count| count as usize),
                "{} producer count",
                case["case"]
            );
            assert_eq!(
                data.producer_data_set.as_ref().is_some_and(|values| {
                    values.with(|values| values.iter().any(Option::is_none))
                }),
                case["producerHasNull"],
                "{} producer null",
                case["case"]
            );
            assert_eq!(
                data.consumer_data_set
                    .as_ref()
                    .map(|set| set.with(HashSet::len)),
                case["consumerCount"].as_u64().map(|count| count as usize),
                "{} consumer count",
                case["case"]
            );
            assert_eq!(
                data.consumer_data_set.as_ref().is_some_and(|values| {
                    values.with(|values| values.iter().any(Option::is_none))
                }),
                case["consumerHasNull"],
                "{} consumer null",
                case["case"]
            );

            if let Some(consumers) = data.consumer_data_set.as_ref() {
                consumers.with(|consumers| {
                    if let Some(consumer) = consumers.iter().flatten().next() {
                        if case.get("subscriptionCount").is_some() {
                            consumer.with(|consumer| {
                                let subscriptions =
                                    consumer.subscription_data_set.as_ref().unwrap();
                                subscriptions.with(|subscriptions| {
                                    assert_eq!(
                                        subscriptions.len(),
                                        case["subscriptionCount"].as_u64().unwrap() as usize
                                    );
                                    assert_eq!(
                                        subscriptions.contains(&None),
                                        case["subscriptionHasNull"]
                                    );
                                });
                            });
                        }
                    }
                });
            }

            let encoded = serde_json::to_value(&data).unwrap();
            for field in ["producerDataSet", "consumerDataSet"] {
                let mut actual = encoded[field].clone();
                let fixture_field = match field {
                    "producerDataSet" => "encodedProducerDataSet",
                    "consumerDataSet" => "encodedConsumerDataSet",
                    _ => unreachable!(),
                };
                let mut expected = case[fixture_field].clone();
                sort_arrays(&mut actual);
                sort_arrays(&mut expected);
                assert_eq!(actual, expected, "{} {field} JSON", case["case"]);
            }
            assert_eq!(
                data.java_to_string(),
                case["toString"],
                "{} text",
                case["case"]
            );

            let fingerprint = data.compute_heartbeat_fingerprint();
            match case["fingerprintError"].as_str() {
                None => assert_eq!(
                    fingerprint.unwrap(),
                    case["computedFingerprint"].as_i64().unwrap() as i32,
                    "{} fingerprint",
                    case["case"]
                ),
                Some("NullPointerException") if case["case"] == "consumer-null" => {
                    assert!(matches!(
                        fingerprint,
                        Err(HeartbeatFingerprintError::NullConsumerData)
                    ));
                }
                Some("NullPointerException") => {
                    assert!(matches!(
                        fingerprint,
                        Err(HeartbeatFingerprintError::NullSubscriptionData)
                    ));
                }
                Some(error) => panic!("unexpected Java fingerprint error: {error}"),
            }
        }
    }

    #[test]
    fn heartbeat_alias_inputs_match_invoked_java_property_names() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["heartbeatAliasCases"].as_array().unwrap() {
            let value: HeartbeatData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            assert_eq!(
                value.client_id,
                case["clientID"].as_str().map(str::to_owned),
                "{} clientID",
                case["rawInput"]
            );
            assert_eq!(
                value.is_without_sub, case["withoutSub"],
                "{} withoutSub",
                case["rawInput"]
            );
            let encoded = serde_json::to_value(value).unwrap();
            for (field, key) in [
                ("clientID", "encodedClientID"),
                ("withoutSub", "encodedWithoutSub"),
            ] {
                assert_eq!(
                    encoded
                        .get(field)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    case[key],
                    "{} encode {field}",
                    case["rawInput"]
                );
            }
        }
    }

    #[test]
    fn primitive_nulls_match_invoked_java_decoder() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["primitiveNullCases"].as_array().unwrap() {
            let raw = case["rawInput"].as_str().unwrap();
            let actual = match case["case"].as_str().unwrap() {
                "subscription-version" => serde_json::from_str::<SubscriptionData>(raw)
                    .map(|value| serde_json::json!(value.sub_version)),
                "subscription-class-filter" => serde_json::from_str::<SubscriptionData>(raw)
                    .map(|value| serde_json::json!(value.class_filter_mode)),
                "heartbeat-fingerprint" => serde_json::from_str::<HeartbeatData>(raw)
                    .map(|value| serde_json::json!(value.heartbeat_fingerprint)),
                "heartbeat-without-sub" => serde_json::from_str::<HeartbeatData>(raw)
                    .map(|value| serde_json::json!(value.is_without_sub)),
                "consumer-unit-mode" => serde_json::from_str::<ConsumerData>(raw)
                    .map(|value| serde_json::json!(value.unit_mode)),
                name => panic!("unknown primitive case: {name}"),
            };
            assert_eq!(actual.unwrap(), case["value"], "{}", case["case"]);
        }
    }

    #[test]
    fn public_heartbeat_fingerprint_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let data: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
            assert_eq!(
                data.compute_heartbeat_fingerprint().unwrap(),
                case["fingerprint"].as_i64().unwrap() as i32,
                "{}",
                case["case"]
            );
        }
    }

    fn current_millis() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[test]
    fn consumer_metadata_multiplicity_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["multiplicityCases"].as_array().unwrap() {
            let expected = case["body"]["consumerDataSet"].as_array().unwrap();
            let data: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
            assert_eq!(
                data.consumer_data_set.as_ref().unwrap().with(HashSet::len),
                expected.len(),
                "{}",
                case["case"]
            );
            let encoded = serde_json::to_value(data).unwrap();
            assert_eq!(
                encoded["consumerDataSet"].as_array().unwrap().len(),
                expected.len()
            );
        }
    }

    #[test]
    fn heartbeat_json_uses_java_field_names_and_preserves_subscription_data() {
        let mut heartbeat = HeartbeatData {
            client_id: Some("client".into()),
            ..HeartbeatData::default()
        };
        let subscriptions =
            SharedValue::from(HashSet::from([Some(SharedValue::from(SubscriptionData {
                topic: Some("topic".into()),
                sub_string: Some("*".into()),
                sub_version: i64::MAX,
                ..SubscriptionData::default()
            }))]));
        let consumer = ConsumerData {
            group_name: Some("group".into()),
            consume_from_where: Some(ConsumeFromWhere::ConsumeFromLastOffset),
            subscription_data_set: Some(subscriptions),
            ..ConsumerData::default()
        };
        heartbeat.consumer_data_set = Some(SharedValue::from(HashSet::from([Some(
            SharedValue::from(consumer),
        )])));
        let value = serde_json::to_value(&heartbeat).unwrap();
        assert_eq!(value["clientID"], "client");
        assert_eq!(value["withoutSub"], false);
        assert!(value.get("isWithoutSub").is_none());
        assert!(
            serde_json::from_value::<HeartbeatData>(serde_json::json!({
                "withoutSub": true
            }))
            .unwrap()
            .is_without_sub
        );
        assert_eq!(
            value["consumerDataSet"][0]["subscriptionDataSet"][0]["subVersion"],
            i64::MAX
        );
        assert!(
            value["consumerDataSet"][0]["subscriptionDataSet"][0]
                .get("filterClassSource")
                .is_none()
        );
    }
}
