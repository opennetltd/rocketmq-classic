//! Remoting RPC latency metric hooks.
//!
//! The default recorder is absent, matching Java's no-op histogram until
//! `RemotingMetricsManager.initMetrics` installs a recorder.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use crate::command::{ProcessTimer, RemotingCommand};

pub const HISTOGRAM_RPC_LATENCY: &str = "rocketmq_rpc_latency";
pub const LABEL_PROTOCOL_TYPE: &str = "protocol_type";
pub const LABEL_REQUEST_CODE: &str = "request_code";
pub const LABEL_RESPONSE_CODE: &str = "response_code";
pub const LABEL_IS_LONG_POLLING: &str = "is_long_polling";
pub const LABEL_RESULT: &str = "result";

#[derive(Debug, PartialEq, Eq)]
pub struct AttributeKey<T> {
    name: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T: AttributeTypeOf> AttributeKey<T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the OpenTelemetry type accepted by this key.
    pub const fn attribute_type(&self) -> AttributeType {
        T::ATTRIBUTE_TYPE
    }
}

impl<T> Copy for AttributeKey<T> {}

impl<T> Clone for AttributeKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

pub const LABEL_PROTOCOL_TYPE_KEY: AttributeKey<String> = AttributeKey::new(LABEL_PROTOCOL_TYPE);
pub const LABEL_REQUEST_CODE_KEY: AttributeKey<String> = AttributeKey::new(LABEL_REQUEST_CODE);
pub const LABEL_RESPONSE_CODE_KEY: AttributeKey<String> = AttributeKey::new(LABEL_RESPONSE_CODE);
pub const LABEL_IS_LONG_POLLING_KEY: AttributeKey<bool> = AttributeKey::new(LABEL_IS_LONG_POLLING);
pub const LABEL_RESULT_KEY: AttributeKey<String> = AttributeKey::new(LABEL_RESULT);

pub const PROTOCOL_TYPE_REMOTING: &str = "remoting";
pub const RESULT_ONEWAY: &str = "oneway";
pub const RESULT_SUCCESS: &str = "success";
pub const RESULT_CANCELED: &str = "cancelled";
pub const RESULT_PROCESS_REQUEST_FAILED: &str = "process_request_failed";
pub const RESULT_WRITE_CHANNEL_FAILED: &str = "write_channel_failed";

const DESCRIPTION_RPC_LATENCY: &str = "Rpc latency";
const UNIT_MILLISECONDS: &str = "milliseconds";
const RPC_LATENCY_BUCKETS_MILLISECONDS: [u64; 9] = [1, 3, 5, 7, 10, 100, 1_000, 2_000, 3_000];

/// OpenTelemetry attribute value kinds supported by the pinned Java API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttributeType {
    Boolean,
    Integer,
    Double,
    String,
    BooleanArray,
    IntegerArray,
    DoubleArray,
    StringArray,
}

/// Associates a Rust value type with its OpenTelemetry attribute kind.
pub trait AttributeTypeOf {
    const ATTRIBUTE_TYPE: AttributeType;
}

impl AttributeTypeOf for bool {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::Boolean;
}

impl AttributeTypeOf for i64 {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::Integer;
}

impl AttributeTypeOf for f64 {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::Double;
}

impl AttributeTypeOf for String {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::String;
}

impl AttributeTypeOf for &str {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::String;
}

impl AttributeTypeOf for Vec<bool> {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::BooleanArray;
}

impl AttributeTypeOf for Vec<i64> {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::IntegerArray;
}

impl AttributeTypeOf for Vec<f64> {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::DoubleArray;
}

impl AttributeTypeOf for Vec<String> {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::StringArray;
}

impl AttributeTypeOf for Vec<&str> {
    const ATTRIBUTE_TYPE: AttributeType = AttributeType::StringArray;
}

/// Values accepted by OpenTelemetry attributes used for remoting metrics.
#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    Double(f64),
    StringArray(Vec<String>),
    BooleanArray(Vec<bool>),
    IntegerArray(Vec<i64>),
    DoubleArray(Vec<f64>),
}

impl AttributeValue {
    pub const fn attribute_type(&self) -> AttributeType {
        match self {
            Self::String(_) => AttributeType::String,
            Self::Boolean(_) => AttributeType::Boolean,
            Self::Integer(_) => AttributeType::Integer,
            Self::Double(_) => AttributeType::Double,
            Self::StringArray(_) => AttributeType::StringArray,
            Self::BooleanArray(_) => AttributeType::BooleanArray,
            Self::IntegerArray(_) => AttributeType::IntegerArray,
            Self::DoubleArray(_) => AttributeType::DoubleArray,
        }
    }
}

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<bool> for AttributeValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i32> for AttributeValue {
    fn from(value: i32) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl From<i64> for AttributeValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for AttributeValue {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<Vec<String>> for AttributeValue {
    fn from(value: Vec<String>) -> Self {
        Self::StringArray(value)
    }
}

impl<'a> From<Vec<&'a str>> for AttributeValue {
    fn from(value: Vec<&'a str>) -> Self {
        Self::StringArray(value.into_iter().map(str::to_owned).collect())
    }
}

impl From<Vec<bool>> for AttributeValue {
    fn from(value: Vec<bool>) -> Self {
        Self::BooleanArray(value)
    }
}

impl From<Vec<i64>> for AttributeValue {
    fn from(value: Vec<i64>) -> Self {
        Self::IntegerArray(value)
    }
}

impl From<Vec<f64>> for AttributeValue {
    fn from(value: Vec<f64>) -> Self {
        Self::DoubleArray(value)
    }
}

/// Immutable set of metric attributes, matching OpenTelemetry `Attributes`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Attributes(BTreeMap<String, AttributeValue>);

impl Attributes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get<T: AttributeTypeOf>(&self, key: AttributeKey<T>) -> Option<&AttributeValue> {
        self.0
            .get(key.name())
            .filter(|value| value.attribute_type() == key.attribute_type())
    }

    pub fn as_map(&self) -> &BTreeMap<String, AttributeValue> {
        &self.0
    }

    pub fn to_builder(&self) -> AttributesBuilder {
        AttributesBuilder {
            attributes: self.0.clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &AttributeValue)> {
        self.0.iter()
    }

    pub fn for_each(&self, mut callback: impl FnMut(&str, &AttributeValue)) {
        self.0.iter().for_each(|(key, value)| callback(key, value));
    }
}

impl std::ops::Index<&str> for Attributes {
    type Output = AttributeValue;

    fn index(&self, key: &str) -> &Self::Output {
        &self.0[key]
    }
}

/// Rust-side adapter for Java's `io.opentelemetry.api.metrics.LongHistogram`.
pub trait RpcLatencyHistogram: Send + Sync + 'static {
    fn record(&self, elapsed_milliseconds: u64, attributes: &Attributes);
}

/// Supplies the base attributes used by `new_attributes_builder`.
pub type AttributesBuilderSupplier = Arc<dyn Fn() -> AttributesBuilder + Send + Sync + 'static>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AttributesBuilder {
    attributes: BTreeMap<String, AttributeValue>,
}

impl AttributesBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<String>, value: impl Into<AttributeValue>) -> &mut Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    pub fn put_key<T: Into<AttributeValue> + AttributeTypeOf>(
        &mut self,
        key: AttributeKey<T>,
        value: T,
    ) -> &mut Self {
        self.attributes.insert(key.name().to_owned(), value.into());
        self
    }

    pub fn build(self) -> Attributes {
        Attributes(self.attributes)
    }

    /// Copies all values from another attribute set into this builder.
    pub fn put_all(&mut self, attributes: &Attributes) -> &mut Self {
        self.attributes.extend(
            attributes
                .as_map()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        self
    }

    /// Removes the attribute matching this key name and value type.
    pub fn remove<T: AttributeTypeOf>(&mut self, key: AttributeKey<T>) -> &mut Self {
        if self
            .attributes
            .get(key.name())
            .is_some_and(|value| value.attribute_type() == key.attribute_type())
        {
            self.attributes.remove(key.name());
        }
        self
    }

    /// Removes attributes matching a key name and OpenTelemetry value type.
    pub fn remove_if(
        &mut self,
        mut predicate: impl FnMut(&str, AttributeType) -> bool,
    ) -> &mut Self {
        self.attributes
            .retain(|key, value| !predicate(key, value.attribute_type()));
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricView {
    pub instrument_type: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub unit: &'static str,
    pub boundaries_milliseconds: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteAndFlushStatus {
    Success,
    Cancelled,
    Failed,
}

#[derive(Clone)]
struct MetricsConfig {
    histogram: Arc<dyn RpcLatencyHistogram>,
    attributes_builder_supplier: Option<AttributesBuilderSupplier>,
}

struct NoopRpcLatencyHistogram;

impl RpcLatencyHistogram for NoopRpcLatencyHistogram {
    fn record(&self, _elapsed_milliseconds: u64, _attributes: &Attributes) {}
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            histogram: Arc::new(NoopRpcLatencyHistogram),
            attributes_builder_supplier: None,
        }
    }
}

/// Per-remoting-client metrics manager corresponding to Java's
/// `RemotingMetricsManager`.
pub struct RemotingMetricsManager {
    config: RwLock<MetricsConfig>,
}

impl Default for RemotingMetricsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RemotingMetricsManager {
    pub fn new() -> Self {
        Self {
            config: RwLock::new(MetricsConfig::default()),
        }
    }

    /// Installs a histogram adapter and optional base-attribute supplier.
    pub fn init_metrics(
        &self,
        histogram: Arc<dyn RpcLatencyHistogram>,
        attributes_builder_supplier: Option<AttributesBuilderSupplier>,
    ) {
        *self.config.write().unwrap() = MetricsConfig {
            histogram,
            attributes_builder_supplier,
        };
    }

    /// Returns the configured histogram adapter. The default adapter is a no-op.
    pub fn get_rpc_latency(&self) -> Arc<dyn RpcLatencyHistogram> {
        self.config.read().unwrap().histogram.clone()
    }

    /// Returns the current base-attribute supplier, if one is installed.
    pub fn get_attributes_builder_supplier(&self) -> Option<AttributesBuilderSupplier> {
        self.config
            .read()
            .unwrap()
            .attributes_builder_supplier
            .clone()
    }

    /// Replaces the supplier used to create base metric attributes.
    pub fn set_attributes_builder_supplier(&self, supplier: Option<AttributesBuilderSupplier>) {
        self.config.write().unwrap().attributes_builder_supplier = supplier;
    }

    /// Creates attributes for a remoting observation.
    ///
    /// With no supplier the result is empty. When a supplier is installed, its
    /// attributes are retained and `protocol_type=remoting` is added, matching
    /// the Java manager's conditional label behavior.
    pub fn new_attributes_builder(&self) -> AttributesBuilder {
        match self.get_attributes_builder_supplier() {
            Some(supplier) => {
                let mut builder = supplier();
                builder.put_key(LABEL_PROTOCOL_TYPE_KEY, PROTOCOL_TYPE_REMOTING.to_owned());
                builder
            }
            None => AttributesBuilder::new(),
        }
    }

    /// Records a latency value through the configured histogram.
    pub fn record_rpc_latency(&self, elapsed_milliseconds: u64, attributes: &Attributes) {
        let histogram = self.get_rpc_latency();
        histogram.record(elapsed_milliseconds, attributes);
    }

    /// Returns the Java remoting RPC-latency histogram view definition.
    pub fn get_metrics_view(&self) -> Vec<MetricView> {
        vec![MetricView {
            instrument_type: "HISTOGRAM",
            name: HISTOGRAM_RPC_LATENCY,
            description: DESCRIPTION_RPC_LATENCY,
            unit: UNIT_MILLISECONDS,
            boundaries_milliseconds: RPC_LATENCY_BUCKETS_MILLISECONDS.to_vec(),
        }]
    }

    /// Maps a completed transport write to Java's result label.
    pub const fn get_write_and_flush_result(&self, status: WriteAndFlushStatus) -> &'static str {
        match status {
            WriteAndFlushStatus::Success => RESULT_SUCCESS,
            WriteAndFlushStatus::Cancelled => RESULT_CANCELED,
            WriteAndFlushStatus::Failed => RESULT_WRITE_CHANNEL_FAILED,
        }
    }
}

pub(crate) fn response_attributes_builder(
    manager: &RemotingMetricsManager,
    request: &RemotingCommand,
    response_code: i32,
) -> AttributesBuilder {
    let mut attributes = manager.new_attributes_builder();
    attributes
        .put_key(LABEL_IS_LONG_POLLING_KEY, request.is_suspended())
        .put_key(
            LABEL_REQUEST_CODE_KEY,
            crate::remoting_helper::get_request_code_desc(request.code),
        )
        .put_key(
            LABEL_RESPONSE_CODE_KEY,
            crate::remoting_helper::get_response_code_desc(response_code),
        );
    attributes
}

pub(crate) fn record_process_request_failed(
    manager: &RemotingMetricsManager,
    request: &RemotingCommand,
) {
    let Some(timer) = request.get_process_timer() else {
        return;
    };
    let mut attributes = manager.new_attributes_builder();
    attributes
        .put_key(
            LABEL_REQUEST_CODE_KEY,
            crate::remoting_helper::get_request_code_desc(request.code),
        )
        .put_key(LABEL_RESULT_KEY, RESULT_PROCESS_REQUEST_FAILED.to_owned());
    let elapsed = timer.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    manager.record_rpc_latency(elapsed, &attributes.build());
}

pub(crate) fn record_request_latency(
    manager: &RemotingMetricsManager,
    timer: Option<&ProcessTimer>,
    attributes: AttributesBuilder,
    result: &'static str,
) {
    let Some(timer) = timer else {
        return;
    };
    let mut attributes = attributes;
    attributes.put_key(LABEL_RESULT_KEY, result.to_owned());
    let elapsed = timer.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    manager.record_rpc_latency(elapsed, &attributes.build());
}

pub(crate) struct PendingWriteMetric {
    manager: Arc<RemotingMetricsManager>,
    timer: Option<ProcessTimer>,
    attributes: Option<AttributesBuilder>,
}

impl PendingWriteMetric {
    pub(crate) fn new(
        manager: Arc<RemotingMetricsManager>,
        timer: Option<ProcessTimer>,
        attributes: AttributesBuilder,
    ) -> Self {
        Self {
            manager,
            timer,
            attributes: Some(attributes),
        }
    }

    pub(crate) fn finish(mut self, status: WriteAndFlushStatus) {
        let result = self.manager.get_write_and_flush_result(status);
        self.record(result);
    }

    fn record(&mut self, result: &'static str) {
        if let Some(attributes) = self.attributes.take() {
            record_request_latency(&self.manager, self.timer.as_ref(), attributes, result);
        }
    }
}

impl Drop for PendingWriteMetric {
    fn drop(&mut self) {
        self.record(RESULT_CANCELED);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use serde_json::Value;

    use super::*;

    #[derive(Default)]
    struct RecordingHistogram(Mutex<Vec<(u64, Attributes)>>);

    impl RpcLatencyHistogram for RecordingHistogram {
        fn record(&self, elapsed_milliseconds: u64, attributes: &Attributes) {
            self.0
                .lock()
                .unwrap()
                .push((elapsed_milliseconds, attributes.clone()));
        }
    }

    fn java_fixture() -> Value {
        serde_json::from_str(include_str!(
            "../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap()
    }

    fn attributes_as_java_json(attributes: &Attributes) -> Value {
        let values = attributes
            .iter()
            .map(|(key, value)| {
                let (type_name, value) = match value {
                    AttributeValue::String(value) => ("STRING", serde_json::json!(value)),
                    AttributeValue::Boolean(value) => ("BOOLEAN", serde_json::json!(value)),
                    AttributeValue::Integer(value) => ("LONG", serde_json::json!(value)),
                    AttributeValue::Double(value) => ("DOUBLE", serde_json::json!(value)),
                    AttributeValue::StringArray(value) => {
                        ("STRING_ARRAY", serde_json::json!(value))
                    }
                    AttributeValue::BooleanArray(value) => {
                        ("BOOLEAN_ARRAY", serde_json::json!(value))
                    }
                    AttributeValue::IntegerArray(value) => ("LONG_ARRAY", serde_json::json!(value)),
                    AttributeValue::DoubleArray(value) => {
                        ("DOUBLE_ARRAY", serde_json::json!(value))
                    }
                };
                (
                    key.clone(),
                    serde_json::json!({"type": type_name, "value": value}),
                )
            })
            .collect::<BTreeMap<_, _>>();
        serde_json::to_value(values).unwrap()
    }

    #[test]
    fn metrics_api_labels_and_results_match_the_pinned_java_fixture() {
        let fixture = java_fixture();
        let java_metrics = &fixture["remotingMetricsBehavior"];
        let manager = Arc::new(RemotingMetricsManager::new());
        assert_eq!(
            fixture["remotingMetricsConstantFields"],
            serde_json::json!([
                "io.opentelemetry.api.common.AttributeKey LABEL_IS_LONG_POLLING_KEY",
                "io.opentelemetry.api.common.AttributeKey LABEL_PROTOCOL_TYPE_KEY",
                "io.opentelemetry.api.common.AttributeKey LABEL_REQUEST_CODE_KEY",
                "io.opentelemetry.api.common.AttributeKey LABEL_RESPONSE_CODE_KEY",
                "io.opentelemetry.api.common.AttributeKey LABEL_RESULT_KEY",
                "java.lang.String HISTOGRAM_RPC_LATENCY",
                "java.lang.String LABEL_IS_LONG_POLLING",
                "java.lang.String LABEL_PROTOCOL_TYPE",
                "java.lang.String LABEL_REQUEST_CODE",
                "java.lang.String LABEL_RESPONSE_CODE",
                "java.lang.String LABEL_RESULT",
                "java.lang.String PROTOCOL_TYPE_REMOTING",
                "java.lang.String RESULT_CANCELED",
                "java.lang.String RESULT_ONEWAY",
                "java.lang.String RESULT_PROCESS_REQUEST_FAILED",
                "java.lang.String RESULT_SUCCESS",
                "java.lang.String RESULT_WRITE_CHANNEL_FAILED"
            ])
        );
        assert_eq!(
            fixture["attributeKeyPublicMethods"],
            serde_json::json!([
                "io.opentelemetry.api.common.AttributeKey booleanArrayKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeKey booleanKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeKey doubleArrayKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeKey doubleKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeKey longArrayKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeKey longKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeKey stringArrayKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeKey stringKey(java.lang.String)",
                "io.opentelemetry.api.common.AttributeType getType()",
                "java.lang.String getKey()"
            ])
        );
        assert_eq!(
            fixture["attributesPublicMethods"],
            serde_json::json!([
                "boolean isEmpty()",
                "int size()",
                "io.opentelemetry.api.common.Attributes empty()",
                "io.opentelemetry.api.common.Attributes of(io.opentelemetry.api.common.AttributeKey,java.lang.Object)",
                "io.opentelemetry.api.common.Attributes of(io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object)",
                "io.opentelemetry.api.common.Attributes of(io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object)",
                "io.opentelemetry.api.common.Attributes of(io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object)",
                "io.opentelemetry.api.common.Attributes of(io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object)",
                "io.opentelemetry.api.common.Attributes of(io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object,io.opentelemetry.api.common.AttributeKey,java.lang.Object)",
                "io.opentelemetry.api.common.AttributesBuilder builder()",
                "io.opentelemetry.api.common.AttributesBuilder toBuilder()",
                "java.lang.Object get(io.opentelemetry.api.common.AttributeKey)",
                "java.util.Map asMap()",
                "void forEach(java.util.function.BiConsumer)"
            ])
        );
        assert_eq!(
            fixture["attributesBuilderPublicMethods"],
            serde_json::json!([
                "io.opentelemetry.api.common.Attributes build()",
                "io.opentelemetry.api.common.AttributesBuilder put(io.opentelemetry.api.common.AttributeKey,[Ljava.lang.Object;)",
                "io.opentelemetry.api.common.AttributesBuilder put(io.opentelemetry.api.common.AttributeKey,int)",
                "io.opentelemetry.api.common.AttributesBuilder put(io.opentelemetry.api.common.AttributeKey,java.lang.Object)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,[D)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,[J)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,[Ljava.lang.String;)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,[Z)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,boolean)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,double)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,java.lang.String)",
                "io.opentelemetry.api.common.AttributesBuilder put(java.lang.String,long)",
                "io.opentelemetry.api.common.AttributesBuilder putAll(io.opentelemetry.api.common.Attributes)",
                "io.opentelemetry.api.common.AttributesBuilder remove(io.opentelemetry.api.common.AttributeKey)",
                "io.opentelemetry.api.common.AttributesBuilder removeIf(java.util.function.Predicate)"
            ])
        );
        assert_eq!(
            fixture["remotingMetricsManagerPublicConstructors"],
            serde_json::json!(["org.apache.rocketmq.remoting.metrics.RemotingMetricsManager()"])
        );
        assert_eq!(
            fixture["remotingMetricsManagerPublicFields"],
            serde_json::json!([])
        );
        assert_eq!(
            fixture["remotingMetricsManagerPublicMethods"],
            serde_json::json!([
                "io.opentelemetry.api.common.AttributesBuilder newAttributesBuilder()",
                "io.opentelemetry.api.metrics.LongHistogram getRpcLatency()",
                "java.lang.String getWriteAndFlushResult(io.netty.util.concurrent.Future)",
                "java.util.List getMetricsView()",
                "java.util.function.Supplier getAttributesBuilderSupplier()",
                "void initMetrics(io.opentelemetry.api.metrics.Meter,java.util.function.Supplier)",
                "void setAttributesBuilderSupplier(java.util.function.Supplier)"
            ])
        );
        let java_attribute_builder = &fixture["attributesBuilderBehavior"];
        let mut values = AttributesBuilder::new();
        values
            .put("string", "value")
            .put("boolean", true)
            .put("long", 42_i64)
            .put("double", 1.25_f64)
            .put("string_array", vec!["first", "second"])
            .put("boolean_array", vec![true, false])
            .put("long_array", vec![3_i64, 4_i64])
            .put("double_array", vec![2.5_f64, 3.5_f64]);
        values.put_key(AttributeKey::<i64>::new("long_from_int"), i64::from(7_i32));
        let values = values.build();
        assert_eq!(
            attributes_as_java_json(&values),
            java_attribute_builder["values"]
        );
        let mut copied_values = values.to_builder();
        copied_values.put("from_to_builder", "copied");
        assert_eq!(
            attributes_as_java_json(&copied_values.build()),
            java_attribute_builder["afterToBuilder"]
        );
        assert_eq!(
            values.len(),
            java_attribute_builder["values"].as_object().unwrap().len()
        );
        assert!(!values.is_empty());
        assert_eq!(values.as_map().len(), values.len());
        assert_eq!(
            AttributeKey::<Vec<f64>>::new("double_array").attribute_type(),
            AttributeType::DoubleArray
        );
        let mut visited_keys = Vec::new();
        values.for_each(|key, _| visited_keys.push(key.to_owned()));
        assert_eq!(
            visited_keys,
            values
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            values.get(AttributeKey::<f64>::new("double")),
            Some(&AttributeValue::Double(1.25))
        );
        assert_eq!(values.get(AttributeKey::<String>::new("double")), None);

        let mut operations = AttributesBuilder::new();
        operations
            .put("removed", "remove")
            .put("filtered", 9_i64)
            .put("type_mismatch", "keep");
        let mut copied_builder = AttributesBuilder::new();
        copied_builder.put("copied", "put_all");
        let copied = copied_builder.build();
        operations.put_all(&copied);
        operations.remove(AttributeKey::<String>::new("removed"));
        operations.remove(AttributeKey::<i64>::new("type_mismatch"));
        operations
            .remove_if(|key, value_type| key == "filtered" && value_type == AttributeType::Integer);
        assert_eq!(
            attributes_as_java_json(&operations.build()),
            java_attribute_builder["afterBuilderOperations"]
        );
        assert_eq!(
            fixture["remotingMetricsHostMethods"],
            serde_json::json!([
                "org.apache.rocketmq.remoting.metrics.RemotingMetricsManager getRemotingMetricsManager()",
                "void setRemotingMetricsManager(org.apache.rocketmq.remoting.metrics.RemotingMetricsManager)"
            ])
        );
        assert!(manager.get_attributes_builder_supplier().is_none());
        assert!(manager.new_attributes_builder().build().is_empty());
        manager.record_rpc_latency(0, &Attributes::new());

        assert_eq!(
            manager.get_metrics_view(),
            vec![MetricView {
                instrument_type: "HISTOGRAM",
                name: HISTOGRAM_RPC_LATENCY,
                description: "Rpc latency",
                unit: "milliseconds",
                boundaries_milliseconds: java_metrics["metricsViewBoundariesMilliseconds"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_u64().unwrap())
                    .collect(),
            }]
        );
        assert_eq!(java_metrics["defaultAttributesEmpty"], true);
        assert!(
            java_metrics["defaultHistogramClass"]
                .as_str()
                .unwrap()
                .ends_with("NopLongHistogram")
        );
        assert_eq!(java_metrics["metricsViewCount"], 1);
        assert_eq!(java_metrics["supplierGetterPresent"], true);
        assert_eq!(java_metrics["suppliedBaseAttribute"], "value");
        assert_eq!(java_metrics["suppliedProtocolType"], PROTOCOL_TYPE_REMOTING);
        assert_eq!(
            java_metrics["writeResults"],
            serde_json::json!([RESULT_SUCCESS, RESULT_CANCELED, RESULT_WRITE_CHANNEL_FAILED])
        );
        assert_eq!(
            manager.get_write_and_flush_result(WriteAndFlushStatus::Success),
            RESULT_SUCCESS
        );
        assert_eq!(
            manager.get_write_and_flush_result(WriteAndFlushStatus::Cancelled),
            RESULT_CANCELED
        );
        assert_eq!(
            manager.get_write_and_flush_result(WriteAndFlushStatus::Failed),
            RESULT_WRITE_CHANNEL_FAILED
        );

        let recorder = Arc::new(RecordingHistogram::default());
        let supplier: AttributesBuilderSupplier = Arc::new(|| {
            let mut builder = AttributesBuilder::new();
            builder.put("base", "value");
            builder
        });
        manager.init_metrics(recorder.clone(), Some(supplier));
        assert!(manager.get_attributes_builder_supplier().is_some());

        let request = RemotingCommand {
            code: rocketmq_classic_protocol::codes::request::SEND_MESSAGE,
            suspended: true,
            process_timer: Some(ProcessTimer::new()),
            ..RemotingCommand::default()
        };
        let response_attributes = response_attributes_builder(
            &manager,
            &request,
            rocketmq_classic_protocol::codes::response::SUCCESS,
        )
        .build();
        assert_eq!(
            response_attributes["base"],
            AttributeValue::String("value".into())
        );
        assert_eq!(
            response_attributes[LABEL_PROTOCOL_TYPE],
            AttributeValue::String(PROTOCOL_TYPE_REMOTING.into())
        );
        assert_eq!(
            response_attributes[LABEL_IS_LONG_POLLING],
            AttributeValue::Boolean(true)
        );
        assert_eq!(
            response_attributes[LABEL_REQUEST_CODE],
            AttributeValue::String("send_message".into())
        );
        assert_eq!(
            response_attributes[LABEL_RESPONSE_CODE],
            AttributeValue::String("success".into())
        );

        record_request_latency(
            &manager,
            request.get_process_timer(),
            response_attributes_builder(
                &manager,
                &request,
                rocketmq_classic_protocol::codes::response::SUCCESS,
            ),
            RESULT_ONEWAY,
        );
        PendingWriteMetric::new(
            manager.clone(),
            request.get_process_timer().cloned(),
            response_attributes_builder(
                &manager,
                &request,
                rocketmq_classic_protocol::codes::response::SUCCESS,
            ),
        )
        .finish(WriteAndFlushStatus::Success);
        PendingWriteMetric::new(
            manager.clone(),
            request.get_process_timer().cloned(),
            response_attributes_builder(
                &manager,
                &request,
                rocketmq_classic_protocol::codes::response::SUCCESS,
            ),
        )
        .finish(WriteAndFlushStatus::Failed);
        record_process_request_failed(&manager, &request);
        drop(PendingWriteMetric::new(
            manager.clone(),
            request.get_process_timer().cloned(),
            response_attributes_builder(
                &manager,
                &request,
                rocketmq_classic_protocol::codes::response::SUCCESS,
            ),
        ));

        let observations = recorder.0.lock().unwrap();
        assert_eq!(observations.len(), 5);
        assert_eq!(
            observations[0].1[LABEL_RESULT],
            AttributeValue::String(RESULT_ONEWAY.into())
        );
        assert_eq!(
            observations[1].1[LABEL_RESULT],
            AttributeValue::String(RESULT_SUCCESS.into())
        );
        assert_eq!(
            observations[2].1[LABEL_RESULT],
            AttributeValue::String(RESULT_WRITE_CHANNEL_FAILED.into())
        );
        assert_eq!(
            observations[3].1[LABEL_RESULT],
            AttributeValue::String(RESULT_PROCESS_REQUEST_FAILED.into())
        );
        assert_eq!(observations[3].1.as_map().get(LABEL_RESPONSE_CODE), None);
        assert_eq!(observations[3].1.as_map().get(LABEL_IS_LONG_POLLING), None);
        assert_eq!(
            observations[4].1[LABEL_RESULT],
            AttributeValue::String(RESULT_CANCELED.into())
        );
    }
}
