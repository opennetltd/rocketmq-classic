//! RocketMQ subscription group configuration DTOs.
//!
//! These types mirror the Java 5.5.1 `remoting.protocol.subscription` API and
//! its Fastjson body representation. Shared handles preserve the mutable
//! references returned and retained by Java getters and setters.

use crate::{SharedValue, serde_fastjson};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
};

pub type SubscriptionGroupAttributeMap = HashMap<String, Option<String>>;

/// Java `remoting.protocol.subscription.GroupForbidden` response DTO.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupForbidden {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    readable: Option<bool>,
}

impl GroupForbidden {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, value: Option<String>) {
        self.group = value;
    }

    pub fn get_readable(&self) -> Option<bool> {
        self.readable
    }

    pub fn set_readable(&mut self, value: Option<bool>) {
        self.readable = value;
    }

    /// Returns Java `GroupForbidden.hashCode()` using Java string and Boolean
    /// hash values and the field order in the pinned implementation.
    pub fn java_hash_code(&self) -> i32 {
        [
            java_optional_string_hash(self.group.as_deref()),
            self.readable.map_or(0, java_boolean_hash),
            java_optional_string_hash(self.topic.as_deref()),
        ]
        .into_iter()
        .fold(1_i32, |hash, value| {
            hash.wrapping_mul(31).wrapping_add(value)
        })
    }

    /// Returns Java `GroupForbidden.toString()` output.
    pub fn java_to_string(&self) -> String {
        format!(
            "GroupForbidden [topic={}, group={}, readable={}]",
            self.topic.as_deref().unwrap_or("null"),
            self.group.as_deref().unwrap_or("null"),
            self.readable
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_owned()),
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

    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice_nullable(data)
    }
}

/// An insertion-ordered Rust representation of Java's `Set<SimpleSubscriptionData>`.
/// Insertion and equality use Java element equality, which ignores `version`.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionDataSet {
    entries: Vec<Option<SharedValue<SimpleSubscriptionData>>>,
}

impl SubscriptionDataSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, value: Option<SharedValue<SimpleSubscriptionData>>) -> bool {
        if self
            .entries
            .iter()
            .any(|existing| subscription_value_eq(existing, &value))
        {
            return false;
        }
        self.entries.push(value);
        true
    }

    pub fn contains(&self, value: &Option<SharedValue<SimpleSubscriptionData>>) -> bool {
        self.entries
            .iter()
            .any(|existing| subscription_value_eq(existing, value))
    }

    pub fn remove(&mut self, value: &Option<SharedValue<SimpleSubscriptionData>>) -> bool {
        let Some(index) = self
            .entries
            .iter()
            .position(|existing| subscription_value_eq(existing, value))
        else {
            return false;
        };
        self.entries.remove(index);
        true
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Option<SharedValue<SimpleSubscriptionData>>> {
        self.entries.iter()
    }

    pub fn as_slice(&self) -> &[Option<SharedValue<SimpleSubscriptionData>>] {
        &self.entries
    }
}

impl FromIterator<Option<SharedValue<SimpleSubscriptionData>>> for SubscriptionDataSet {
    fn from_iter<I: IntoIterator<Item = Option<SharedValue<SimpleSubscriptionData>>>>(
        iter: I,
    ) -> Self {
        let mut set = Self::new();
        set.extend(iter);
        set
    }
}

impl Extend<Option<SharedValue<SimpleSubscriptionData>>> for SubscriptionDataSet {
    fn extend<I: IntoIterator<Item = Option<SharedValue<SimpleSubscriptionData>>>>(
        &mut self,
        iter: I,
    ) {
        for value in iter {
            self.insert(value);
        }
    }
}

impl Serialize for SubscriptionDataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.entries.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SubscriptionDataSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<Option<SimpleSubscriptionData>>::deserialize(deserializer)?;
        Ok(values
            .into_iter()
            .map(|value| value.map(SharedValue::new))
            .collect())
    }
}

impl PartialEq for SubscriptionDataSet {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.entries.iter().all(|value| other.contains(value))
            && other.entries.iter().all(|value| self.contains(value))
    }
}

impl Eq for SubscriptionDataSet {}

fn subscription_value_eq(
    left: &Option<SharedValue<SimpleSubscriptionData>>,
    right: &Option<SharedValue<SimpleSubscriptionData>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.with(|left| right.with(|right| left == right)),
        _ => false,
    }
}

const DEFAULT_CUSTOMIZED_RETRY_DELAYS: [i64; 18] = [
    1_000, 5_000, 10_000, 30_000, 60_000, 120_000, 180_000, 240_000, 300_000, 360_000, 420_000,
    480_000, 540_000, 600_000, 1_200_000, 1_800_000, 3_600_000, 7_200_000,
];

fn default_group_retry_policy() -> Option<SharedValue<GroupRetryPolicy>> {
    Some(SharedValue::new(GroupRetryPolicy::default()))
}

fn default_attributes() -> Option<SharedValue<SubscriptionGroupAttributeMap>> {
    Some(SharedValue::new(SubscriptionGroupAttributeMap::new()))
}

fn default_group_retry_type() -> Option<GroupRetryPolicyType> {
    Some(GroupRetryPolicyType::Customized)
}

/// Java `SimpleSubscriptionData`. Equality and the Java hash code intentionally
/// ignore `version`, as the pinned class does.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SimpleSubscriptionData {
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression: Option<String>,
    #[serde(default)]
    version: i64,
}

impl SimpleSubscriptionData {
    pub fn new(
        topic: Option<String>,
        expression_type: Option<String>,
        expression: Option<String>,
        version: i64,
    ) -> Self {
        Self {
            topic,
            expression_type,
            expression,
            version,
        }
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_expression_type(&self) -> Option<&str> {
        self.expression_type.as_deref()
    }

    pub fn set_expression_type(&mut self, value: Option<String>) {
        self.expression_type = value;
    }

    pub fn get_expression(&self) -> Option<&str> {
        self.expression.as_deref()
    }

    pub fn set_expression(&mut self, value: Option<String>) {
        self.expression = value;
    }

    pub fn get_version(&self) -> i64 {
        self.version
    }

    pub fn set_version(&mut self, value: i64) {
        self.version = value;
    }

    pub fn java_hash_code(&self) -> i32 {
        let mut result = 1_i32;
        for value in [&self.topic, &self.expression_type, &self.expression] {
            result = result
                .wrapping_mul(31)
                .wrapping_add(java_optional_string_hash(value.as_deref()));
        }
        result
    }
}

impl PartialEq for SimpleSubscriptionData {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.expression_type == other.expression_type
            && self.expression == other.expression
    }
}

impl Eq for SimpleSubscriptionData {}

impl Hash for SimpleSubscriptionData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.expression_type.hash(state);
        self.expression.hash(state);
    }
}

impl fmt::Display for SimpleSubscriptionData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SimpleSubscriptionData{{topic={}, expressionType={}, expression={}, version={}}}",
            display_nullable(&self.topic),
            display_nullable(&self.expression_type),
            display_nullable(&self.expression),
            self.version
        )
    }
}

/// Java `GroupRetryPolicyType`, in declaration/ordinal order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GroupRetryPolicyType {
    Exponential,
    Customized,
}

impl GroupRetryPolicyType {
    pub const fn values() -> [Self; 2] {
        [Self::Exponential, Self::Customized]
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Exponential => "EXPONENTIAL",
            Self::Customized => "CUSTOMIZED",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Exponential => 0,
            Self::Customized => 1,
        }
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, crate::codes::JavaEnumLookupError> {
        match name {
            None => Err(crate::codes::JavaEnumLookupError::NullName),
            Some("EXPONENTIAL") => Ok(Self::Exponential),
            Some("CUSTOMIZED") => Ok(Self::Customized),
            Some(other) => Err(crate::codes::JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType",
                name: other.to_owned(),
            }),
        }
    }
}

impl fmt::Display for GroupRetryPolicyType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// The policy operation shared by Java's exponential and customized policies.
pub trait RetryPolicy {
    fn next_delay_duration(&self, reconsume_times: i32) -> i64;
}

/// Java `ExponentialRetryPolicy`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExponentialRetryPolicy {
    initial: i64,
    max: i64,
    multiplier: i64,
}

impl Default for ExponentialRetryPolicy {
    fn default() -> Self {
        Self {
            initial: 5_000,
            max: 7_200_000,
            multiplier: 2,
        }
    }
}

impl ExponentialRetryPolicy {
    pub fn new(initial: i64, max: i64, multiplier: i64) -> Self {
        Self {
            initial,
            max,
            multiplier,
        }
    }

    pub fn get_initial(&self) -> i64 {
        self.initial
    }

    pub fn set_initial(&mut self, value: i64) {
        self.initial = value;
    }

    pub fn get_max(&self) -> i64 {
        self.max
    }

    pub fn set_max(&mut self, value: i64) {
        self.max = value;
    }

    pub fn get_multiplier(&self) -> i64 {
        self.multiplier
    }

    pub fn set_multiplier(&mut self, value: i64) {
        self.multiplier = value;
    }
}

impl RetryPolicy for ExponentialRetryPolicy {
    fn next_delay_duration(&self, reconsume_times: i32) -> i64 {
        let times = reconsume_times.clamp(0, 32);
        let factor = (self.multiplier as f64).powf(times as f64) as i64;
        self.initial.wrapping_mul(factor).min(self.max)
    }
}

impl fmt::Display for ExponentialRetryPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ExponentialRetryPolicy{{initial={}, max={}, multiplier={}}}",
            self.initial, self.max, self.multiplier
        )
    }
}

/// Java `CustomizedRetryPolicy`, including the mutable `long[]` reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CustomizedRetryPolicy {
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<SharedValue<Vec<i64>>>,
}

impl Default for CustomizedRetryPolicy {
    fn default() -> Self {
        Self {
            next: Some(SharedValue::new(DEFAULT_CUSTOMIZED_RETRY_DELAYS.to_vec())),
        }
    }
}

impl CustomizedRetryPolicy {
    pub fn new(next: Option<SharedValue<Vec<i64>>>) -> Self {
        Self { next }
    }

    pub fn get_next(&self) -> Option<SharedValue<Vec<i64>>> {
        self.next.clone()
    }

    pub fn set_next(&mut self, value: Option<SharedValue<Vec<i64>>>) {
        self.next = value;
    }
}

impl RetryPolicy for CustomizedRetryPolicy {
    fn next_delay_duration(&self, reconsume_times: i32) -> i64 {
        let values = self
            .next
            .as_ref()
            .expect("CustomizedRetryPolicy.next is null")
            .snapshot();
        let mut index = reconsume_times.max(0).wrapping_add(2);
        if index >= values.len() as i32 {
            index = values.len() as i32 - 1;
        }
        values[index as usize]
    }
}

impl fmt::Display for CustomizedRetryPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values = self.next.as_ref().map(SharedValue::snapshot);
        write!(formatter, "CustomizedRetryPolicy{{next={values:?}}}")
    }
}

/// The effective retry policy returned by `GroupRetryPolicy::getRetryPolicy`.
#[derive(Debug, Clone)]
pub enum ResolvedRetryPolicy {
    Exponential(SharedValue<ExponentialRetryPolicy>),
    Customized(SharedValue<CustomizedRetryPolicy>),
    DefaultCustomized(CustomizedRetryPolicy),
}

impl RetryPolicy for ResolvedRetryPolicy {
    fn next_delay_duration(&self, reconsume_times: i32) -> i64 {
        match self {
            Self::Exponential(policy) => {
                policy.with(|policy| policy.next_delay_duration(reconsume_times))
            }
            Self::Customized(policy) => {
                policy.with(|policy| policy.next_delay_duration(reconsume_times))
            }
            Self::DefaultCustomized(policy) => policy.next_delay_duration(reconsume_times),
        }
    }
}

/// Java `GroupRetryPolicy`; the nested policy references remain nullable and
/// retain identity when assigned.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GroupRetryPolicy {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    policy_type: Option<GroupRetryPolicyType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exponential_retry_policy: Option<SharedValue<ExponentialRetryPolicy>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    customized_retry_policy: Option<SharedValue<CustomizedRetryPolicy>>,
}

impl Default for GroupRetryPolicy {
    fn default() -> Self {
        Self {
            policy_type: default_group_retry_type(),
            exponential_retry_policy: None,
            customized_retry_policy: None,
        }
    }
}

impl GroupRetryPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_type(&self) -> Option<GroupRetryPolicyType> {
        self.policy_type
    }

    pub fn set_type(&mut self, value: Option<GroupRetryPolicyType>) {
        self.policy_type = value;
    }

    pub fn get_exponential_retry_policy(&self) -> Option<SharedValue<ExponentialRetryPolicy>> {
        self.exponential_retry_policy.clone()
    }

    pub fn set_exponential_retry_policy(
        &mut self,
        value: Option<SharedValue<ExponentialRetryPolicy>>,
    ) {
        self.exponential_retry_policy = value;
    }

    pub fn get_customized_retry_policy(&self) -> Option<SharedValue<CustomizedRetryPolicy>> {
        self.customized_retry_policy.clone()
    }

    pub fn set_customized_retry_policy(
        &mut self,
        value: Option<SharedValue<CustomizedRetryPolicy>>,
    ) {
        self.customized_retry_policy = value;
    }

    pub fn get_retry_policy(&self) -> ResolvedRetryPolicy {
        match self.policy_type {
            Some(GroupRetryPolicyType::Exponential) => self
                .exponential_retry_policy
                .clone()
                .map(ResolvedRetryPolicy::Exponential)
                .unwrap_or_else(default_resolved_retry_policy),
            Some(GroupRetryPolicyType::Customized) => self
                .customized_retry_policy
                .clone()
                .map(ResolvedRetryPolicy::Customized)
                .unwrap_or_else(default_resolved_retry_policy),
            None => default_resolved_retry_policy(),
        }
    }
}

fn default_resolved_retry_policy() -> ResolvedRetryPolicy {
    ResolvedRetryPolicy::DefaultCustomized(CustomizedRetryPolicy::default())
}

impl fmt::Display for GroupRetryPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exponential = self
            .exponential_retry_policy
            .as_ref()
            .map(|policy| policy.with(ToString::to_string));
        let customized = self
            .customized_retry_policy
            .as_ref()
            .map(|policy| policy.with(ToString::to_string));
        write!(
            formatter,
            "GroupRetryPolicy{{type={}, exponentialRetryPolicy={}, customizedRetryPolicy={}}}",
            self.policy_type.map_or("null", GroupRetryPolicyType::name),
            exponential.as_deref().unwrap_or("null"),
            customized.as_deref().unwrap_or("null")
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SubscriptionGroupAttributeError {
    #[error("SubscriptionGroupConfig.attributes is null")]
    NullAttributes,
    #[error("invalid long value for subscription group attribute {attribute}: {value}")]
    InvalidLong {
        attribute: &'static str,
        value: String,
    },
    #[error("value for subscription group attribute {attribute} does not fit in i32: {value}")]
    IntOutOfRange { attribute: &'static str, value: i64 },
}

/// Java `SubscriptionGroupConfig` with Java defaults, nullable fields,
/// equality/hash behavior, computed Lite attributes and mutable aliases.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionGroupConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    group_name: Option<String>,
    consume_enable: bool,
    consume_from_min_enable: bool,
    consume_broadcast_enable: bool,
    consume_message_orderly: bool,
    retry_queue_nums: i32,
    retry_max_times: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_retry_policy: Option<SharedValue<GroupRetryPolicy>>,
    broker_id: i64,
    which_broker_when_consume_slowly: i64,
    notify_consumer_ids_changed_enable: bool,
    group_sys_flag: i32,
    consume_timeout_minute: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_data_set: Option<SharedValue<SubscriptionDataSet>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attributes: Option<SharedValue<SubscriptionGroupAttributeMap>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct SubscriptionGroupConfigFields {
    #[serde(deserialize_with = "serde_fastjson::deserialize_nullable_string")]
    group_name: Option<String>,
    consume_enable: bool,
    consume_from_min_enable: bool,
    consume_broadcast_enable: bool,
    consume_message_orderly: bool,
    retry_queue_nums: i32,
    retry_max_times: i32,
    group_retry_policy: Option<SharedValue<GroupRetryPolicy>>,
    broker_id: i64,
    which_broker_when_consume_slowly: i64,
    notify_consumer_ids_changed_enable: bool,
    group_sys_flag: i32,
    consume_timeout_minute: i32,
    subscription_data_set: Option<SharedValue<SubscriptionDataSet>>,
    attributes: Option<SharedValue<SubscriptionGroupAttributeMap>>,
}

impl Default for SubscriptionGroupConfigFields {
    fn default() -> Self {
        let value = SubscriptionGroupConfig::default();
        Self {
            group_name: value.group_name,
            consume_enable: value.consume_enable,
            consume_from_min_enable: value.consume_from_min_enable,
            consume_broadcast_enable: value.consume_broadcast_enable,
            consume_message_orderly: value.consume_message_orderly,
            retry_queue_nums: value.retry_queue_nums,
            retry_max_times: value.retry_max_times,
            group_retry_policy: value.group_retry_policy,
            broker_id: value.broker_id,
            which_broker_when_consume_slowly: value.which_broker_when_consume_slowly,
            notify_consumer_ids_changed_enable: value.notify_consumer_ids_changed_enable,
            group_sys_flag: value.group_sys_flag,
            consume_timeout_minute: value.consume_timeout_minute,
            subscription_data_set: value.subscription_data_set,
            attributes: value.attributes,
        }
    }
}

impl<'de> Deserialize<'de> for SubscriptionGroupConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: SubscriptionGroupConfigFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            group_name: fields.group_name,
            consume_enable: fields.consume_enable,
            consume_from_min_enable: fields.consume_from_min_enable,
            consume_broadcast_enable: fields.consume_broadcast_enable,
            consume_message_orderly: fields.consume_message_orderly,
            retry_queue_nums: fields.retry_queue_nums,
            retry_max_times: fields.retry_max_times,
            group_retry_policy: fields.group_retry_policy,
            broker_id: fields.broker_id,
            which_broker_when_consume_slowly: fields.which_broker_when_consume_slowly,
            notify_consumer_ids_changed_enable: fields.notify_consumer_ids_changed_enable,
            group_sys_flag: fields.group_sys_flag,
            consume_timeout_minute: fields.consume_timeout_minute,
            subscription_data_set: fields.subscription_data_set,
            attributes: fields.attributes,
        })
    }
}

impl Default for SubscriptionGroupConfig {
    fn default() -> Self {
        Self {
            group_name: None,
            consume_enable: true,
            consume_from_min_enable: true,
            consume_broadcast_enable: true,
            consume_message_orderly: false,
            retry_queue_nums: 1,
            retry_max_times: 16,
            group_retry_policy: default_group_retry_policy(),
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
            consume_timeout_minute: 15,
            subscription_data_set: None,
            attributes: default_attributes(),
        }
    }
}

impl SubscriptionGroupConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_group_name(&self) -> Option<&str> {
        self.group_name.as_deref()
    }

    pub fn set_group_name(&mut self, value: Option<String>) {
        self.group_name = value;
    }

    pub fn is_consume_enable(&self) -> bool {
        self.consume_enable
    }

    pub fn set_consume_enable(&mut self, value: bool) {
        self.consume_enable = value;
    }

    pub fn is_consume_from_min_enable(&self) -> bool {
        self.consume_from_min_enable
    }

    pub fn set_consume_from_min_enable(&mut self, value: bool) {
        self.consume_from_min_enable = value;
    }

    pub fn is_consume_broadcast_enable(&self) -> bool {
        self.consume_broadcast_enable
    }

    pub fn set_consume_broadcast_enable(&mut self, value: bool) {
        self.consume_broadcast_enable = value;
    }

    pub fn is_consume_message_orderly(&self) -> bool {
        self.consume_message_orderly
    }

    pub fn set_consume_message_orderly(&mut self, value: bool) {
        self.consume_message_orderly = value;
    }

    pub fn get_retry_queue_nums(&self) -> i32 {
        self.retry_queue_nums
    }

    pub fn set_retry_queue_nums(&mut self, value: i32) {
        self.retry_queue_nums = value;
    }

    pub fn get_retry_max_times(&self) -> i32 {
        self.retry_max_times
    }

    pub fn set_retry_max_times(&mut self, value: i32) {
        self.retry_max_times = value;
    }

    pub fn get_group_retry_policy(&self) -> Option<SharedValue<GroupRetryPolicy>> {
        self.group_retry_policy.clone()
    }

    pub fn set_group_retry_policy(&mut self, value: Option<SharedValue<GroupRetryPolicy>>) {
        self.group_retry_policy = value;
    }

    pub fn get_broker_id(&self) -> i64 {
        self.broker_id
    }

    pub fn set_broker_id(&mut self, value: i64) {
        self.broker_id = value;
    }

    pub fn get_which_broker_when_consume_slowly(&self) -> i64 {
        self.which_broker_when_consume_slowly
    }

    pub fn set_which_broker_when_consume_slowly(&mut self, value: i64) {
        self.which_broker_when_consume_slowly = value;
    }

    pub fn is_notify_consumer_ids_changed_enable(&self) -> bool {
        self.notify_consumer_ids_changed_enable
    }

    pub fn set_notify_consumer_ids_changed_enable(&mut self, value: bool) {
        self.notify_consumer_ids_changed_enable = value;
    }

    pub fn get_group_sys_flag(&self) -> i32 {
        self.group_sys_flag
    }

    pub fn set_group_sys_flag(&mut self, value: i32) {
        self.group_sys_flag = value;
    }

    pub fn get_consume_timeout_minute(&self) -> i32 {
        self.consume_timeout_minute
    }

    pub fn set_consume_timeout_minute(&mut self, value: i32) {
        self.consume_timeout_minute = value;
    }

    pub fn get_subscription_data_set(&self) -> Option<SharedValue<SubscriptionDataSet>> {
        self.subscription_data_set.clone()
    }

    pub fn set_subscription_data_set(&mut self, value: Option<SharedValue<SubscriptionDataSet>>) {
        self.subscription_data_set = value;
    }

    pub fn get_attributes(&self) -> Option<SharedValue<SubscriptionGroupAttributeMap>> {
        self.attributes.clone()
    }

    pub fn set_attributes(&mut self, value: Option<SharedValue<SubscriptionGroupAttributeMap>>) {
        self.attributes = value;
    }

    /// Returns `priority.factor`, falling back to Java's default value 100
    /// when the entry is absent or cannot be parsed as a signed long.
    pub fn get_priority_factor(&self) -> Result<i64, SubscriptionGroupAttributeError> {
        let value = self.attribute_value("priority.factor")?;
        Ok(value
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(100))
    }

    pub fn set_lite_bind_topic(
        &mut self,
        value: Option<&str>,
    ) -> Result<(), SubscriptionGroupAttributeError> {
        if let Some(value) = value {
            self.attributes
                .as_ref()
                .ok_or(SubscriptionGroupAttributeError::NullAttributes)?
                .with_mut(|attributes| {
                    attributes.insert("lite.bind.topic".to_owned(), Some(value.to_owned()));
                });
        }
        Ok(())
    }

    pub fn get_lite_bind_topic(&self) -> Result<Option<String>, SubscriptionGroupAttributeError> {
        self.attribute_value("lite.bind.topic")
    }

    pub fn get_lite_sub_client_quota(&self) -> Result<i32, SubscriptionGroupAttributeError> {
        let Some(value) = self.attribute_value("lite.sub.client.quota")? else {
            return Ok(2_000);
        };
        let parsed =
            value
                .parse::<i64>()
                .map_err(|_| SubscriptionGroupAttributeError::InvalidLong {
                    attribute: "lite.sub.client.quota",
                    value,
                })?;
        i32::try_from(parsed).map_err(|_| SubscriptionGroupAttributeError::IntOutOfRange {
            attribute: "lite.sub.client.quota",
            value: parsed,
        })
    }

    pub fn set_lite_sub_exclusive(
        &mut self,
        value: bool,
    ) -> Result<(), SubscriptionGroupAttributeError> {
        if value {
            self.attributes
                .as_ref()
                .ok_or(SubscriptionGroupAttributeError::NullAttributes)?
                .with_mut(|attributes| {
                    attributes.insert("lite.sub.model".to_owned(), Some("Exclusive".to_owned()));
                });
        }
        Ok(())
    }

    pub fn is_lite_sub_exclusive(&self) -> Result<bool, SubscriptionGroupAttributeError> {
        Ok(self
            .attribute_value("lite.sub.model")?
            .is_some_and(|value| value == "Exclusive"))
    }

    pub fn is_reset_offset_in_exclusive_mode(
        &self,
    ) -> Result<bool, SubscriptionGroupAttributeError> {
        Ok(self
            .attribute_value("lite.sub.reset.offset.exclusive")?
            .is_some_and(|value| value.eq_ignore_ascii_case("true")))
    }

    pub fn is_reset_offset_on_unsubscribe(&self) -> Result<bool, SubscriptionGroupAttributeError> {
        Ok(self
            .attribute_value("lite.sub.reset.offset.unsubscribe")?
            .is_some_and(|value| value.eq_ignore_ascii_case("true")))
    }

    pub fn get_max_client_event_count(&self) -> Result<i32, SubscriptionGroupAttributeError> {
        let value = self.attribute_value("lite.sub.client.max.event.cnt")?;
        Ok(value
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(-1))
    }

    pub fn set_wildcard_lite_group(
        &mut self,
        value: bool,
    ) -> Result<(), SubscriptionGroupAttributeError> {
        if value {
            self.attributes
                .as_ref()
                .ok_or(SubscriptionGroupAttributeError::NullAttributes)?
                .with_mut(|attributes| {
                    attributes.insert("lite.sub.wildcard".to_owned(), Some("true".to_owned()));
                });
        }
        Ok(())
    }

    pub fn is_wildcard_lite_group(&self) -> Result<bool, SubscriptionGroupAttributeError> {
        let attributes = self
            .attributes
            .as_ref()
            .ok_or(SubscriptionGroupAttributeError::NullAttributes)?;
        Ok(attributes.with(|attributes| attributes.contains_key("lite.sub.wildcard")))
    }

    fn attribute_value(
        &self,
        name: &'static str,
    ) -> Result<Option<String>, SubscriptionGroupAttributeError> {
        let attributes = self
            .attributes
            .as_ref()
            .ok_or(SubscriptionGroupAttributeError::NullAttributes)?;
        Ok(attributes.with(|attributes| attributes.get(name).cloned().flatten()))
    }

    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }

    /// Returns Java's `SubscriptionGroupConfig.hashCode()` value. A null
    /// attributes map follows Java's direct dereference and is reported as an
    /// error rather than panicking.
    pub fn java_hash_code(&self) -> Result<i32, SubscriptionGroupAttributeError> {
        let mut result = 1_i32;
        let mut add = |value: i32| {
            result = result.wrapping_mul(31).wrapping_add(value);
        };
        add(java_long_hash(self.broker_id));
        add(java_boolean_hash(self.consume_broadcast_enable));
        add(java_boolean_hash(self.consume_enable));
        add(java_boolean_hash(self.consume_from_min_enable));
        add(java_boolean_hash(self.notify_consumer_ids_changed_enable));
        add(java_boolean_hash(self.consume_message_orderly));
        add(java_optional_string_hash(self.group_name.as_deref()));
        add(self.retry_max_times);
        add(self.retry_queue_nums);
        add(java_long_hash(self.which_broker_when_consume_slowly));
        add(self.group_sys_flag);
        add(self.consume_timeout_minute);
        add(self.subscription_set_hash_code());
        let attributes = self
            .attributes
            .as_ref()
            .ok_or(SubscriptionGroupAttributeError::NullAttributes)?;
        add(attributes.with(java_attribute_map_hash));
        Ok(result)
    }

    fn subscription_set_hash_code(&self) -> i32 {
        self.subscription_data_set.as_ref().map_or(0, |values| {
            values.with(|values| {
                values.iter().fold(0_i32, |hash, value| {
                    hash.wrapping_add(value.as_ref().map_or(0, |value| {
                        value.with(SimpleSubscriptionData::java_hash_code)
                    }))
                })
            })
        })
    }

    /// Encodes this DTO using the same sorted Fastjson-compatible writer as
    /// the other protocol bodies.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        crate::route::java_fastjson_json(self, pretty_format)
    }

    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }
}

impl PartialEq for SubscriptionGroupConfig {
    fn eq(&self, other: &Self) -> bool {
        self.group_name == other.group_name
            && self.consume_enable == other.consume_enable
            && self.consume_from_min_enable == other.consume_from_min_enable
            && self.consume_broadcast_enable == other.consume_broadcast_enable
            && self.consume_message_orderly == other.consume_message_orderly
            && self.retry_queue_nums == other.retry_queue_nums
            && self.retry_max_times == other.retry_max_times
            && self.which_broker_when_consume_slowly == other.which_broker_when_consume_slowly
            && self.notify_consumer_ids_changed_enable == other.notify_consumer_ids_changed_enable
            && self.group_sys_flag == other.group_sys_flag
            && self.consume_timeout_minute == other.consume_timeout_minute
            && subscription_sets_equal(
                self.subscription_data_set.as_ref(),
                other.subscription_data_set.as_ref(),
            )
            && option_shared_map_equal(self.attributes.as_ref(), other.attributes.as_ref())
    }
}

impl Eq for SubscriptionGroupConfig {}

impl fmt::Display for SubscriptionGroupConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let retry_policy = self
            .group_retry_policy
            .as_ref()
            .map(|policy| policy.with(ToString::to_string));
        let subscriptions = self.subscription_data_set.as_ref().map(|set| {
            set.with(|set| {
                let values = set
                    .iter()
                    .map(|subscription| {
                        subscription.as_ref().map_or_else(
                            || "null".to_owned(),
                            |subscription| subscription.with(ToString::to_string),
                        )
                    })
                    .collect::<Vec<_>>();
                format!("[{}]", values.join(", "))
            })
        });
        let attributes = self.attributes.as_ref().map(|attributes| {
            attributes.with(|attributes| {
                let mut entries = attributes.iter().collect::<Vec<_>>();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                let values = entries
                    .into_iter()
                    .map(|(key, value)| format!("{key}={}", value.as_deref().unwrap_or("null")))
                    .collect::<Vec<_>>();
                format!("{{{}}}", values.join(", "))
            })
        });
        write!(
            formatter,
            "SubscriptionGroupConfig{{groupName={}, consumeEnable={}, consumeFromMinEnable={}, consumeBroadcastEnable={}, consumeMessageOrderly={}, retryQueueNums={}, retryMaxTimes={}, groupRetryPolicy={}, brokerId={}, whichBrokerWhenConsumeSlowly={}, notifyConsumerIdsChangedEnable={}, groupSysFlag={}, consumeTimeoutMinute={}, subscriptionDataSet={}, attributes={}}}",
            display_nullable(&self.group_name),
            self.consume_enable,
            self.consume_from_min_enable,
            self.consume_broadcast_enable,
            self.consume_message_orderly,
            self.retry_queue_nums,
            self.retry_max_times,
            retry_policy.as_deref().unwrap_or("null"),
            self.broker_id,
            self.which_broker_when_consume_slowly,
            self.notify_consumer_ids_changed_enable,
            self.group_sys_flag,
            self.consume_timeout_minute,
            subscriptions.as_deref().unwrap_or("null"),
            attributes.as_deref().unwrap_or("null")
        )
    }
}

fn subscription_sets_equal(
    left: Option<&SharedValue<SubscriptionDataSet>>,
    right: Option<&SharedValue<SubscriptionDataSet>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.snapshot() == right.snapshot(),
        _ => false,
    }
}

fn option_shared_map_equal(
    left: Option<&SharedValue<SubscriptionGroupAttributeMap>>,
    right: Option<&SharedValue<SubscriptionGroupAttributeMap>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.with(|left| right.with(|right| left == right)),
        _ => false,
    }
}

fn java_attribute_map_hash(attributes: &SubscriptionGroupAttributeMap) -> i32 {
    attributes.iter().fold(0_i32, |hash, (key, value)| {
        let value_hash = java_optional_string_hash(value.as_deref());
        hash.wrapping_add(java_string_hash(key) ^ value_hash)
    })
}

fn java_long_hash(value: i64) -> i32 {
    let value = value as u64;
    (value ^ (value >> 32)) as i32
}

fn java_boolean_hash(value: bool) -> i32 {
    if value { 1231 } else { 1237 }
}

fn java_optional_string_hash(value: Option<&str>) -> i32 {
    value.map_or(0, java_string_hash)
}

fn java_string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn display_nullable(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("null")
}

#[cfg(test)]
mod tests {
    use super::{
        CustomizedRetryPolicy, ExponentialRetryPolicy, GroupForbidden, GroupRetryPolicy,
        GroupRetryPolicyType, RetryPolicy, SimpleSubscriptionData, SubscriptionDataSet,
        SubscriptionGroupConfig,
    };
    use crate::SharedValue;
    use serde_json::{Value, json};
    use std::collections::HashMap;

    fn admin_fixture() -> Value {
        serde_json::from_str(include_str!(
            "../../../fixtures/admin/query-consume-time-span.json"
        ))
        .unwrap()
    }

    #[test]
    fn group_forbidden_java_object_methods_match_pinned_fixture() {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["groupForbiddenDtoOperations"];

        let mut populated = GroupForbidden::default();
        populated.set_topic(Some("orders".to_owned()));
        populated.set_group(Some("workers".to_owned()));
        populated.set_readable(Some(false));
        assert_eq!(populated.java_hash_code(), expected["populatedHashCode"]);
        assert_eq!(populated.java_to_string(), expected["populatedToString"]);
        assert_eq!(
            populated.java_to_json(false).unwrap(),
            expected["populatedJson"]
        );
        assert_eq!(
            populated.java_encode().unwrap(),
            expected["populatedJson"].as_str().unwrap().as_bytes()
        );
        assert_eq!(populated, populated.clone());
        assert_eq!(
            populated == GroupForbidden::default(),
            expected["populatedEqualsDefault"]
        );

        let default = GroupForbidden::default();
        assert_eq!(default.java_hash_code(), expected["defaultHashCode"]);
        assert_eq!(default.java_to_string(), expected["defaultToString"]);
        assert_eq!(
            default.java_to_json(false).unwrap(),
            expected["defaultJson"]
        );
    }

    fn populated_config() -> SubscriptionGroupConfig {
        let mut config = SubscriptionGroupConfig::default();
        config.set_group_name(Some("workers".into()));
        config.set_consume_enable(false);
        config.set_consume_from_min_enable(false);
        config.set_consume_broadcast_enable(false);
        config.set_consume_message_orderly(true);
        config.set_retry_queue_nums(4);
        config.set_retry_max_times(8);
        let mut retry_policy = GroupRetryPolicy::default();
        retry_policy.set_type(Some(GroupRetryPolicyType::Exponential));
        retry_policy.set_exponential_retry_policy(Some(SharedValue::new(
            ExponentialRetryPolicy::new(1_000, 60_000, 3),
        )));
        config.set_group_retry_policy(Some(SharedValue::new(retry_policy)));
        config.set_broker_id(7);
        config.set_which_broker_when_consume_slowly(9);
        config.set_notify_consumer_ids_changed_enable(false);
        config.set_group_sys_flag(3);
        config.set_consume_timeout_minute(12);
        let subscriptions: SubscriptionDataSet = [
            Some(SharedValue::new(SimpleSubscriptionData::new(
                Some("orders".into()),
                Some("TAG".into()),
                Some("tag-a || tag-b".into()),
                10,
            ))),
            Some(SharedValue::new(SimpleSubscriptionData::new(
                Some("payments".into()),
                Some("SQL92".into()),
                Some("amount > 0".into()),
                20,
            ))),
        ]
        .into_iter()
        .collect();
        config.set_subscription_data_set(Some(SharedValue::new(subscriptions)));
        config.set_attributes(Some(SharedValue::new(HashMap::from([
            ("tenant".into(), Some("north".into())),
            ("stage".into(), Some("blue".into())),
        ]))));
        config
    }

    #[test]
    fn defaults_and_populated_request_body_match_invoked_java_fixture() {
        let fixture = admin_fixture()["updateOrCreateSubscriptionGroup"].clone();
        let default = SubscriptionGroupConfig::default();
        assert_eq!(
            default.java_to_json(false).unwrap(),
            fixture["dtoSemantics"]["defaultJson"].as_str().unwrap()
        );
        let populated = populated_config();
        assert_eq!(
            populated.java_encode().unwrap(),
            fixture["success"]["requestBody"]
                .as_str()
                .unwrap()
                .as_bytes()
        );
        assert_eq!(
            populated.to_string(),
            fixture["success"]["configToString"].as_str().unwrap()
        );
        assert_eq!(
            default.java_hash_code().unwrap(),
            fixture["dtoSemantics"]["defaultConfigHashCode"]
                .as_i64()
                .unwrap() as i32
        );
        assert_eq!(
            default
                .get_group_retry_policy()
                .unwrap()
                .with(|policy| policy.get_retry_policy().next_delay_duration(0)),
            fixture["dtoSemantics"]["defaultRetryDelayAtZero"]
                .as_i64()
                .unwrap()
        );
    }

    #[test]
    fn subscription_group_public_accessor_surface_matches_java_inventory() {
        let operation = admin_fixture()["updateOrCreateSubscriptionGroup"].clone();
        assert_eq!(
            operation["configPublicMethods"].as_array().unwrap().len(),
            44
        );
        assert_eq!(
            operation["groupRetryPolicyPublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
        assert_eq!(
            operation["simpleSubscriptionDataPublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            11
        );
        assert_eq!(
            operation["exponentialRetryPolicyPublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            8
        );
        assert_eq!(
            operation["customizedRetryPolicyPublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
        assert_eq!(
            operation["retryPolicyPublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let mut simple = SimpleSubscriptionData::new(
            Some("orders".into()),
            Some("TAG".into()),
            Some("a".into()),
            1,
        );
        assert_eq!(simple.get_topic(), Some("orders"));
        assert_eq!(simple.get_expression_type(), Some("TAG"));
        assert_eq!(simple.get_expression(), Some("a"));
        assert_eq!(simple.get_version(), 1);
        simple.set_topic(Some("payments".into()));
        simple.set_expression_type(Some("SQL92".into()));
        simple.set_expression(Some("amount > 0".into()));
        simple.set_version(2);
        assert_eq!(simple.get_topic(), Some("payments"));
        assert_eq!(simple.get_expression_type(), Some("SQL92"));
        assert_eq!(simple.get_expression(), Some("amount > 0"));
        assert_eq!(simple.get_version(), 2);

        let mut exponential = ExponentialRetryPolicy::default();
        assert_eq!(exponential.get_initial(), 5_000);
        assert_eq!(exponential.get_max(), 7_200_000);
        assert_eq!(exponential.get_multiplier(), 2);
        exponential.set_initial(1_000);
        exponential.set_max(60_000);
        exponential.set_multiplier(3);
        assert_eq!(exponential.get_initial(), 1_000);
        assert_eq!(exponential.get_max(), 60_000);
        assert_eq!(exponential.get_multiplier(), 3);

        let mut group_policy = GroupRetryPolicy::new();
        assert_eq!(
            group_policy.get_type(),
            Some(GroupRetryPolicyType::Customized)
        );
        assert!(group_policy.get_exponential_retry_policy().is_none());
        assert!(group_policy.get_customized_retry_policy().is_none());
        let custom = SharedValue::new(CustomizedRetryPolicy::default());
        group_policy.set_type(Some(GroupRetryPolicyType::Customized));
        group_policy.set_customized_retry_policy(Some(custom.clone()));
        assert!(
            group_policy
                .get_customized_retry_policy()
                .unwrap()
                .ptr_eq(&custom)
        );
        assert_eq!(
            group_policy.get_retry_policy().next_delay_duration(0),
            10_000
        );
        assert_eq!(
            GroupRetryPolicyType::values(),
            [
                GroupRetryPolicyType::Exponential,
                GroupRetryPolicyType::Customized
            ]
        );
        assert_eq!(GroupRetryPolicyType::Exponential.name(), "EXPONENTIAL");
        assert_eq!(GroupRetryPolicyType::Customized.ordinal(), 1);
        assert_eq!(
            GroupRetryPolicyType::value_of_name(Some("CUSTOMIZED")).unwrap(),
            GroupRetryPolicyType::Customized
        );

        let config = populated_config();
        assert_eq!(config.get_group_name(), Some("workers"));
        assert!(!config.is_consume_enable());
        assert!(!config.is_consume_from_min_enable());
        assert!(!config.is_consume_broadcast_enable());
        assert!(config.is_consume_message_orderly());
        assert_eq!(config.get_retry_queue_nums(), 4);
        assert_eq!(config.get_retry_max_times(), 8);
        assert_eq!(config.get_broker_id(), 7);
        assert_eq!(config.get_which_broker_when_consume_slowly(), 9);
        assert!(!config.is_notify_consumer_ids_changed_enable());
        assert_eq!(config.get_group_sys_flag(), 3);
        assert_eq!(config.get_consume_timeout_minute(), 12);
        let retry = config.get_group_retry_policy().unwrap();
        retry.with(|retry| {
            assert_eq!(retry.get_type(), Some(GroupRetryPolicyType::Exponential));
            let exponential = retry.get_exponential_retry_policy().unwrap();
            assert_eq!(exponential.with(|value| value.get_initial()), 1_000);
            assert_eq!(exponential.with(|value| value.get_max()), 60_000);
            assert_eq!(exponential.with(|value| value.get_multiplier()), 3);
            assert_eq!(retry.get_retry_policy().next_delay_duration(1), 3_000);
        });
        let subscriptions = config.get_subscription_data_set().unwrap().snapshot();
        assert_eq!(subscriptions.len(), 2);
        assert_eq!(
            subscriptions.as_slice()[0]
                .as_ref()
                .unwrap()
                .with(|subscription| subscription.get_topic().map(str::to_owned)),
            Some("orders".into())
        );
        let attributes = config.get_attributes().unwrap();
        assert_eq!(
            attributes.with(|attributes| attributes.get("stage").cloned().flatten()),
            Some("blue".into())
        );
    }

    #[test]
    fn group_config_reference_identity_and_java_equality_are_preserved() {
        let fixture = admin_fixture()["updateOrCreateSubscriptionGroup"]["dtoSemantics"].clone();
        let simple = SimpleSubscriptionData::new(
            Some("orders".into()),
            Some("TAG".into()),
            Some("a".into()),
            1,
        );
        assert_eq!(
            simple.java_hash_code(),
            fixture["simpleSubscriptionJavaHashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(
            simple.to_string(),
            fixture["simpleSubscriptionToString"].as_str().unwrap()
        );
        let mut config = SubscriptionGroupConfig::default();
        let attributes = SharedValue::new(HashMap::from([(
            "stage".to_owned(),
            Some("blue".to_owned()),
        )]));
        let subscriptions = SharedValue::new(
            [Some(SharedValue::new(SimpleSubscriptionData::new(
                Some("orders".into()),
                Some("TAG".into()),
                Some("a".into()),
                1,
            )))]
            .into_iter()
            .collect::<SubscriptionDataSet>(),
        );
        let policy = SharedValue::new(GroupRetryPolicy::default());
        config.set_attributes(Some(attributes.clone()));
        config.set_subscription_data_set(Some(subscriptions.clone()));
        config.set_group_retry_policy(Some(policy.clone()));
        assert!(config.get_attributes().unwrap().ptr_eq(&attributes));
        assert!(
            config
                .get_subscription_data_set()
                .unwrap()
                .ptr_eq(&subscriptions)
        );
        assert!(config.get_group_retry_policy().unwrap().ptr_eq(&policy));

        let mut same_except_version = SubscriptionGroupConfig::default();
        same_except_version.set_subscription_data_set(Some(SharedValue::new(
            [Some(SharedValue::new(SimpleSubscriptionData::new(
                Some("orders".into()),
                Some("TAG".into()),
                Some("a".into()),
                2,
            )))]
            .into_iter()
            .collect::<SubscriptionDataSet>(),
        )));
        same_except_version.set_attributes(Some(SharedValue::new(HashMap::from([(
            "stage".to_owned(),
            Some("blue".to_owned()),
        )]))));
        assert_eq!(config, same_except_version);
        assert!(fixture["configEqualityIgnoresBrokerId"].as_bool().unwrap());

        let mut different_broker = same_except_version.clone();
        different_broker.set_broker_id(99);
        assert_eq!(same_except_version, different_broker);
        assert!(
            fixture["configHashCodesDifferByBrokerId"]
                .as_bool()
                .unwrap()
        );
        assert_ne!(
            same_except_version.java_hash_code().unwrap(),
            different_broker.java_hash_code().unwrap()
        );
    }

    #[test]
    fn subscription_data_set_matches_java_duplicate_and_null_membership() {
        let fixture = admin_fixture()["updateOrCreateSubscriptionGroup"]["dtoSemantics"].clone();
        let mut subscriptions = SubscriptionDataSet::new();
        let first = Some(SharedValue::new(SimpleSubscriptionData::new(
            Some("orders".into()),
            Some("TAG".into()),
            Some("a".into()),
            1,
        )));
        let same_ignoring_version = Some(SharedValue::new(SimpleSubscriptionData::new(
            Some("orders".into()),
            Some("TAG".into()),
            Some("a".into()),
            2,
        )));
        assert!(subscriptions.insert(first.clone()));
        assert!(!subscriptions.insert(same_ignoring_version.clone()));
        assert_eq!(subscriptions.len(), 1);
        assert!(subscriptions.contains(&same_ignoring_version));
        assert!(
            fixture["setDeduplicatesVersionOnlyDifference"]
                .as_bool()
                .unwrap()
        );

        assert!(subscriptions.insert(None));
        assert!(!subscriptions.insert(None));
        assert_eq!(subscriptions.len(), 2);
        assert!(subscriptions.contains(&None));
        assert!(fixture["setPreservesOneNullElement"].as_bool().unwrap());

        let encoded = serde_json::to_value(&subscriptions).unwrap();
        let mut decoded: SubscriptionDataSet = serde_json::from_value(encoded).unwrap();
        assert_eq!(subscriptions, decoded);
        assert_eq!(decoded.len(), 2);
        assert!(decoded.remove(&same_ignoring_version));
        assert!(decoded.remove(&None));
        assert!(decoded.is_empty());

        let duplicate_body = json!([
            {"topic": "orders", "expressionType": "TAG", "expression": "a", "version": 1},
            {"topic": "orders", "expressionType": "TAG", "expression": "a", "version": 2},
            null,
            null
        ]);
        let decoded_duplicates: SubscriptionDataSet =
            serde_json::from_value(duplicate_body).unwrap();
        assert_eq!(decoded_duplicates.len(), 2);
        assert!(decoded_duplicates.contains(&same_ignoring_version));
        assert!(decoded_duplicates.contains(&None));
        let reversed = [None, first].into_iter().collect::<SubscriptionDataSet>();
        assert_eq!(decoded_duplicates, reversed);
    }

    #[test]
    fn retry_policy_defaults_clamping_and_custom_alias_match_java() {
        let fixture = admin_fixture()["updateOrCreateSubscriptionGroup"]["dtoSemantics"].clone();
        let exponential = ExponentialRetryPolicy::new(1_000, 60_000, 3);
        let actual = [-1, 0, 1, 5, 33].map(|times| exponential.next_delay_duration(times));
        assert_eq!(
            actual,
            fixture["exponentialRetryDelays"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_i64().unwrap())
                .collect::<Vec<_>>()[..]
        );

        let next = SharedValue::new(vec![11, 22, 33]);
        let mut customized = CustomizedRetryPolicy::new(Some(next.clone()));
        assert!(customized.get_next().unwrap().ptr_eq(&next));
        assert_eq!(customized.next_delay_duration(0), 33);
        next.with_mut(|values| values.push(44));
        assert_eq!(customized.next_delay_duration(1), 44);
        customized.set_next(None);
        assert!(customized.get_next().is_none());
    }

    #[test]
    fn lite_subscription_attribute_helpers_use_java_names_and_defaults() {
        let fixture = admin_fixture()["updateOrCreateSubscriptionGroup"]["dtoSemantics"].clone();
        let mut config = SubscriptionGroupConfig::default();
        assert_eq!(config.get_priority_factor().unwrap(), 100);
        assert_eq!(config.get_lite_bind_topic().unwrap(), None);
        assert_eq!(config.get_lite_sub_client_quota().unwrap(), 2_000);
        assert_eq!(config.get_max_client_event_count().unwrap(), -1);
        assert_eq!(
            fixture["defaultPriorityFactor"].as_i64().unwrap(),
            config.get_priority_factor().unwrap()
        );
        assert_eq!(
            fixture["defaultLiteSubClientQuota"].as_i64().unwrap(),
            i64::from(config.get_lite_sub_client_quota().unwrap())
        );
        assert_eq!(
            fixture["defaultMaxClientEventCount"].as_i64().unwrap(),
            i64::from(config.get_max_client_event_count().unwrap())
        );
        config.set_lite_bind_topic(Some("lite-orders")).unwrap();
        config.set_lite_sub_exclusive(true).unwrap();
        config.set_wildcard_lite_group(true).unwrap();
        config.get_attributes().unwrap().with_mut(|attributes| {
            attributes.insert("priority.factor".into(), Some("17".into()));
            attributes.insert("lite.sub.client.quota".into(), Some("123".into()));
            attributes.insert("lite.sub.client.max.event.cnt".into(), Some("42".into()));
            attributes.insert(
                "lite.sub.reset.offset.exclusive".into(),
                Some("TRUE".into()),
            );
            attributes.insert(
                "lite.sub.reset.offset.unsubscribe".into(),
                Some("false".into()),
            );
        });
        assert_eq!(config.get_priority_factor().unwrap(), 17);
        assert_eq!(
            config.get_lite_bind_topic().unwrap().as_deref(),
            Some("lite-orders")
        );
        assert_eq!(config.get_lite_sub_client_quota().unwrap(), 123);
        assert_eq!(config.get_max_client_event_count().unwrap(), 42);
        assert!(config.is_lite_sub_exclusive().unwrap());
        assert!(config.is_wildcard_lite_group().unwrap());
        assert!(config.is_reset_offset_in_exclusive_mode().unwrap());
        assert!(!config.is_reset_offset_on_unsubscribe().unwrap());
        assert_eq!(fixture["liteBindTopic"], "lite-orders");
        assert_eq!(fixture["liteSubExclusive"], true);
        assert_eq!(fixture["wildcardLiteGroup"], true);
        assert_eq!(fixture["priorityFactor"], 17);
        assert_eq!(fixture["liteSubClientQuota"], 123);
        assert_eq!(fixture["maxClientEventCount"], 42);
        assert_eq!(fixture["resetOffsetInExclusiveMode"], true);
        assert_eq!(fixture["resetOffsetOnUnsubscribe"], false);
        config.set_lite_bind_topic(None).unwrap();
        config.set_lite_sub_exclusive(false).unwrap();
        config.set_wildcard_lite_group(false).unwrap();
        assert!(
            fixture["falseLiteSettersPreserveAttributes"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            config.get_lite_bind_topic().unwrap().as_deref(),
            Some("lite-orders")
        );
        assert!(config.is_lite_sub_exclusive().unwrap());
        assert!(config.is_wildcard_lite_group().unwrap());
        config.get_attributes().unwrap().with_mut(|attributes| {
            attributes.insert("priority.factor".into(), Some("bad".into()));
            attributes.insert("lite.sub.client.quota".into(), Some("bad".into()));
        });
        assert_eq!(config.get_priority_factor().unwrap(), 100);
        assert!(config.get_lite_sub_client_quota().is_err());
        assert!(
            fixture["invalidPriorityFactorUsesDefault"]
                .as_bool()
                .unwrap()
        );
        assert!(
            fixture["invalidMaxClientEventCountUsesDefault"]
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn nullable_and_duplicate_fastjson_fields_follow_java_defaults() {
        let decoded = SubscriptionGroupConfig::java_decode(
            br#"{"groupName":"first","groupName":"last","attributes":null,"subscriptionDataSet":null,"groupRetryPolicy":null}"#,
        )
        .unwrap();
        assert_eq!(decoded.get_group_name(), Some("last"));
        assert!(decoded.get_attributes().is_none());
        assert!(decoded.get_subscription_data_set().is_none());
        assert!(decoded.get_group_retry_policy().is_none());
        assert!(decoded.get_priority_factor().is_err());
        assert_eq!(
            json!(GroupRetryPolicyType::Exponential),
            json!("EXPONENTIAL")
        );
    }
}
