use std::collections::{BTreeMap, HashMap};

use crate::serde_fastjson;
use crate::{JavaJsonWriterFeature, SharedValue};
use rocketmq_classic_common::message::MessageQueue;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};

pub type SharedBrokerAddressMap = SharedValue<serde_fastjson::NullableLongStringMap>;
pub type SharedTopicQueueMappingIds = SharedValue<BTreeMap<i32, i32>>;
pub type SharedQueueDataHandle = SharedValue<QueueData>;
pub type SharedBrokerDataHandle = SharedValue<BrokerData>;
pub type SharedTopicQueueMappingInfoHandle = SharedValue<TopicQueueMappingInfo>;
pub type SharedRouteQueueDataList = SharedValue<Vec<Option<SharedQueueDataHandle>>>;
pub type SharedRouteBrokerDataList = SharedValue<Vec<Option<SharedBrokerDataHandle>>>;
pub type SharedFilterServerList = SharedValue<Vec<Option<String>>>;
pub type SharedFilterServerTable = SharedValue<BTreeMap<String, Option<SharedFilterServerList>>>;
pub type SharedTopicQueueMappingTable =
    SharedValue<BTreeMap<String, Option<SharedTopicQueueMappingInfoHandle>>>;

const JAVA_REFERENCE_OUTPUT_MARKER_KEY: &str = "\0rocketmq_classic_json_reference";

impl From<Vec<Option<QueueData>>> for SharedRouteQueueDataList {
    fn from(queues: Vec<Option<QueueData>>) -> Self {
        SharedValue::new(
            queues
                .into_iter()
                .map(|queue| queue.map(SharedValue::new))
                .collect(),
        )
    }
}

impl From<Vec<Option<BrokerData>>> for SharedRouteBrokerDataList {
    fn from(brokers: Vec<Option<BrokerData>>) -> Self {
        SharedValue::new(
            brokers
                .into_iter()
                .map(|broker| broker.map(SharedValue::new))
                .collect(),
        )
    }
}

impl From<BTreeMap<String, Option<Vec<Option<String>>>>> for SharedFilterServerTable {
    fn from(filters: BTreeMap<String, Option<Vec<Option<String>>>>) -> Self {
        SharedValue::new(
            filters
                .into_iter()
                .map(|(broker, servers)| (broker, servers.map(SharedValue::new)))
                .collect(),
        )
    }
}

impl From<BTreeMap<String, Option<TopicQueueMappingInfo>>> for SharedTopicQueueMappingTable {
    fn from(mappings: BTreeMap<String, Option<TopicQueueMappingInfo>>) -> Self {
        SharedValue::new(
            mappings
                .into_iter()
                .map(|(broker, mapping)| (broker, mapping.map(SharedValue::new)))
                .collect(),
        )
    }
}

fn serialize_shared_nullable_long_string_map<S>(
    value: &Option<SharedBrokerAddressMap>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(addresses) = value else {
        return serializer.serialize_none();
    };
    if crate::shared::shared_value_reference_markers_enabled() {
        let mapped = addresses.with(|entries| {
            entries
                .iter()
                .map(|(key, value)| {
                    let key = key.map_or_else(|| "null".to_owned(), |key| key.to_string());
                    let value = value.as_ref().map_or(serde_json::Value::Null, |value| {
                        serde_json::Value::String(value.clone())
                    });
                    (key, value)
                })
                .collect()
        });
        return crate::shared::serialize_shared_value_identity(
            addresses.reference_identity(),
            &serde_json::Value::Object(mapped),
            serializer,
        );
    }
    let snapshot = Some(addresses.snapshot());
    serde_fastjson::serialize_nullable_long_string_map(&snapshot, serializer)
}

pub(crate) fn java_fastjson_json<T: Serialize>(
    value: &T,
    pretty_format: bool,
) -> Result<String, serde_json::Error> {
    java_fastjson_json_with_features(value, pretty_format, &[])
}

pub(crate) fn java_serialize_value_with_features<T: Serialize>(
    value: &T,
    features: &[JavaJsonWriterFeature],
) -> Result<serde_json::Value, serde_json::Error> {
    if !features.contains(&JavaJsonWriterFeature::ReferenceDetection) {
        return serde_json::to_value(value);
    }
    let mut value =
        crate::shared::with_shared_value_reference_markers(|| serde_json::to_value(value))?;
    apply_java_reference_detection(&mut value);
    Ok(value)
}

fn apply_java_reference_detection(value: &mut serde_json::Value) {
    fn resolve(value: &mut serde_json::Value, path: &str, seen: &mut HashMap<usize, String>) {
        let marker = value.as_object().and_then(|fields| {
            if fields.len() != 1 {
                return None;
            }
            let payload = fields
                .get(crate::shared::SHARED_VALUE_REFERENCE_MARKER_KEY)?
                .as_array()?;
            let identity = usize::try_from(payload.first()?.as_u64()?).ok()?;
            Some((identity, payload.get(1)?.clone()))
        });
        if let Some((identity, inner)) = marker {
            if let Some(first_path) = seen.get(&identity) {
                let mut reference = serde_json::Map::new();
                reference.insert(
                    JAVA_REFERENCE_OUTPUT_MARKER_KEY.to_owned(),
                    serde_json::Value::String(first_path.clone()),
                );
                *value = serde_json::Value::Object(reference);
                return;
            }
            seen.insert(identity, path.to_owned());
            *value = inner;
            resolve(value, path, seen);
            return;
        }

        if let Some(fields) = value.as_object_mut() {
            for (name, field) in fields {
                let child_path = if is_java_reference_path_identifier(name) {
                    format!("{path}.{name}")
                } else {
                    format!(
                        "{path}[{}]",
                        serde_json::to_string(name).unwrap_or_default()
                    )
                };
                resolve(field, &child_path, seen);
            }
        } else if let Some(items) = value.as_array_mut() {
            for (index, item) in items.iter_mut().enumerate() {
                resolve(item, &format!("{path}[{index}]"), seen);
            }
        }
    }

    fn is_java_reference_path_identifier(name: &str) -> bool {
        let mut chars = name.chars();
        chars
            .next()
            .is_some_and(|first| first == '_' || first == '$' || first.is_ascii_alphabetic())
            && chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_alphanumeric())
    }

    resolve(value, "$", &mut HashMap::new());
}

fn is_java_reference_value(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|fields| {
        fields.len() == 1 && fields.contains_key(JAVA_REFERENCE_OUTPUT_MARKER_KEY)
    })
}

pub(crate) fn apply_java_reference_output_markers(value: &mut serde_json::Value) {
    if is_java_reference_value(value) {
        if let Some(path) = value
            .as_object()
            .and_then(|fields| fields.get(JAVA_REFERENCE_OUTPUT_MARKER_KEY))
            .cloned()
        {
            *value = serde_json::json!({"$ref": path});
        }
        return;
    }
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                apply_java_reference_output_markers(item);
            }
        }
        serde_json::Value::Object(fields) => {
            for field in fields.values_mut() {
                apply_java_reference_output_markers(field);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

pub(crate) fn java_fastjson_json_with_features<T: Serialize>(
    value: &T,
    pretty_format: bool,
    features: &[JavaJsonWriterFeature],
) -> Result<String, serde_json::Error> {
    validate_java_writer_features(features)?;
    if let Some(output) = java_non_serializable_writer_output(features)? {
        return Ok(output);
    }
    let indent = java_json_indent(pretty_format, features);
    let mut value = java_serialize_value_with_features(value, features)?;
    apply_java_json_writer_value_features(
        &mut value,
        features.contains(&JavaJsonWriterFeature::WriteNonStringValueAsString),
        features.contains(&JavaJsonWriterFeature::WriteBooleanAsNumber),
        features.contains(&JavaJsonWriterFeature::WriteEnumUsingOrdinal),
        features.contains(&JavaJsonWriterFeature::BrowserCompatible),
    );
    if features.contains(&JavaJsonWriterFeature::NotWriteDefaultValue) {
        omit_default_writer_values(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::NotWriteEmptyArray) {
        omit_empty_writer_values(&mut value, false);
    }
    if features.contains(&JavaJsonWriterFeature::IgnoreEmpty) {
        omit_empty_writer_values(&mut value, true);
    }
    if features.contains(&JavaJsonWriterFeature::WriteLongAsString) {
        apply_java_json_long_as_string(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::IgnoreNonFieldGetter) {
        omit_java_non_field_getter_properties(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::WriteNulls)
        || features.contains(&JavaJsonWriterFeature::WriteMapNullValue)
    {
        apply_java_write_nulls::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::WriteNullListAsEmpty) {
        apply_java_null_lists::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::WriteNullStringAsEmpty) {
        apply_java_null_strings::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::NullAsDefaultValue) {
        apply_java_null_defaults::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::WriteClassName) {
        apply_java_class_names::<T>(
            &mut value,
            features.contains(&JavaJsonWriterFeature::NotWriteRootClassName),
            features.contains(&JavaJsonWriterFeature::NotWriteHashMapArrayListClassName),
        )?;
    }
    let mut bean_to_array_maps = HashMap::new();
    if features.contains(&JavaJsonWriterFeature::BeanToArray) {
        apply_java_bean_to_array::<T>(
            &mut value,
            &mut bean_to_array_maps,
            features.contains(&JavaJsonWriterFeature::WriteEnumsUsingName)
                || features.contains(&JavaJsonWriterFeature::WriteEnumUsingToString),
        )?;
    }
    apply_java_reference_output_markers(&mut value);
    let mut output = String::new();
    let writer_options = JavaRouteWriterOptions {
        unquote_numeric_map_keys: !features
            .contains(&JavaJsonWriterFeature::WriteNonStringValueAsString)
            && !features.contains(&JavaJsonWriterFeature::WriteNonStringKeyAsString)
            && !features.contains(&JavaJsonWriterFeature::BrowserCompatible)
            && !features.contains(&JavaJsonWriterFeature::IgnoreNonFieldGetter),
        stringify_long_map_keys: features.contains(&JavaJsonWriterFeature::WriteLongAsString),
        use_single_quotes: features.contains(&JavaJsonWriterFeature::UseSingleQuotes),
        unquote_field_names: features.contains(&JavaJsonWriterFeature::UnquoteFieldName),
        browser_secure: features.contains(&JavaJsonWriterFeature::BrowserSecure),
        write_class_name: features.contains(&JavaJsonWriterFeature::WriteClassName),
        not_write_set_class_name: features.contains(&JavaJsonWriterFeature::NotWriteSetClassName),
    };
    let mut writer = JavaRouteWriter {
        indent,
        writer_options,
        map_fields: &bean_to_array_maps,
        path: Vec::new(),
    };
    write_java_route_json(&value, None, 0, &mut writer, &mut output)?;
    if features.contains(&JavaJsonWriterFeature::EscapeNoneAscii) {
        Ok(escape_none_ascii_json(&output))
    } else {
        Ok(output)
    }
}

pub(crate) fn omit_java_non_field_getter_properties(value: &mut serde_json::Value) {
    let Some(spans) = value
        .get_mut("consumeTimeSpanSet")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };

    for span in spans {
        let Some(fields) = span.as_object_mut() else {
            continue;
        };
        fields.remove("consumeTimeStampStr");
        fields.remove("maxTimeStampStr");
        fields.remove("minTimeStampStr");
    }
}

pub(crate) fn omit_default_writer_values(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields.iter_mut() {
                if is_java_map_property(name) {
                    // Map entries are values, not bean fields; preserve scalar
                    // defaults such as zero offsets and queue IDs.
                    omit_default_writer_map_values(value);
                } else {
                    omit_default_writer_values(value);
                }
            }
            fields.retain(|name, value| {
                if name == "autoCommit" {
                    // The pinned Java writer keeps this property for both
                    // boolean values on ConsumeMessageDirectlyResult.
                    return true;
                }
                match value {
                    serde_json::Value::Null => false,
                    serde_json::Value::Bool(value) => *value,
                    serde_json::Value::Number(value) => {
                        value.as_i64() != Some(0)
                            && value.as_u64() != Some(0)
                            && value.as_f64() != Some(0.0)
                    }
                    serde_json::Value::String(value) => !value.is_empty(),
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => true,
                }
            });
        }
        serde_json::Value::Array(values) => {
            for value in values {
                omit_default_writer_values(value);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

pub(crate) fn is_java_map_property(name: &str) -> bool {
    matches!(
        name,
        "attachments"
            | "brokerAddrs"
            | "consumerTable"
            | "currIdMap"
            | "filterServerTable"
            | "hostedQueues"
            | "messageQueueTable"
            | "messageRequestModeMap"
            | "offsetTable"
            | "properties"
            | "statusTable"
            | "topicQueueMappingByBroker"
            | "userConsumerInfo"
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JavaBeanKind {
    BatchAck,
    BatchAckMessageRequestBody,
    BrokerData,
    CheckClientRequestBody,
    ConsumeMessageDirectlyResult,
    ConsumerData,
    GetConsumerListByGroupResponseBody,
    GetConsumerStatusBody,
    HeartbeatData,
    LiteSubscriptionCtlRequestBody,
    LiteSubscriptionDto,
    LockBatchRequestBody,
    LockBatchResponseBody,
    MessageQueue,
    MessageQueueAssignment,
    MessageRequestModeSerializeWrapper,
    QueryAssignmentRequestBody,
    QueryAssignmentResponseBody,
    QueryConsumeTimeSpanBody,
    QueueData,
    QueueTimeSpan,
    ResetOffsetBody,
    SetMessageRequestModeRequestBody,
    SubscriptionData,
    LogicQueueMappingItem,
    TopicQueueMappingDetail,
    TopicQueueMappingInfo,
    TopicRouteData,
    UnlockBatchRequestBody,
    ProducerData,
}

impl JavaBeanKind {
    fn field_names(self) -> &'static [&'static str] {
        match self {
            Self::BatchAck => &["b", "c", "it", "pt", "q", "r", "rq", "so", "t"],
            Self::BatchAckMessageRequestBody => &["acks", "brokerName"],
            Self::BrokerData => &[
                "brokerAddrs",
                "brokerName",
                "cluster",
                "enableActingMaster",
                "zoneName",
            ],
            Self::CheckClientRequestBody => &["clientId", "group", "namespace", "subscriptionData"],
            Self::ConsumeMessageDirectlyResult => &[
                "autoCommit",
                "consumeResult",
                "order",
                "remark",
                "spentTimeMills",
            ],
            Self::ConsumerData => &[
                "consumeFromWhere",
                "consumeType",
                "groupName",
                "messageModel",
                "subscriptionDataSet",
                "unitMode",
            ],
            Self::GetConsumerListByGroupResponseBody => &["consumerIdList"],
            Self::GetConsumerStatusBody => &["consumerTable", "messageQueueTable"],
            Self::HeartbeatData => &[
                "clientID",
                "consumerDataSet",
                "heartbeatFingerprint",
                "producerDataSet",
                "withoutSub",
            ],
            Self::LiteSubscriptionCtlRequestBody => &["subscriptionSet"],
            Self::LiteSubscriptionDto => &[
                "action",
                "clientId",
                "group",
                "liteTopicSet",
                "offsetOption",
                "topic",
                "version",
            ],
            Self::LockBatchRequestBody | Self::UnlockBatchRequestBody => {
                &["clientId", "consumerGroup", "mqSet", "onlyThisBroker"]
            }
            Self::LockBatchResponseBody => &["lockOKMQSet"],
            Self::LogicQueueMappingItem => &[
                "bname",
                "endOffset",
                "gen",
                "logicOffset",
                "queueId",
                "startOffset",
                "timeOfEnd",
                "timeOfStart",
            ],
            Self::MessageQueue => &["brokerName", "queueId", "topic"],
            Self::MessageQueueAssignment => &["attachments", "messageQueue", "mode"],
            Self::MessageRequestModeSerializeWrapper => &["messageRequestModeMap"],
            Self::QueryAssignmentRequestBody => &[
                "clientId",
                "consumerGroup",
                "messageModel",
                "strategyName",
                "topic",
            ],
            Self::QueryAssignmentResponseBody => &["messageQueueAssignments"],
            Self::QueryConsumeTimeSpanBody => &["consumeTimeSpanSet"],
            Self::QueueData => &[
                "brokerName",
                "perm",
                "readQueueNums",
                "topicSysFlag",
                "writeQueueNums",
            ],
            Self::QueueTimeSpan => &[
                "consumeTimeStamp",
                "consumeTimeStampStr",
                "delayTime",
                "maxTimeStamp",
                "maxTimeStampStr",
                "messageQueue",
                "minTimeStamp",
                "minTimeStampStr",
            ],
            Self::ResetOffsetBody => &["offsetTable"],
            Self::SetMessageRequestModeRequestBody => {
                &["consumerGroup", "mode", "popShareQueueNum", "topic"]
            }
            Self::SubscriptionData => &[
                "classFilterMode",
                "codeSet",
                "expressionType",
                "subString",
                "subVersion",
                "tagsSet",
                "topic",
            ],
            Self::TopicQueueMappingInfo => &[
                "bname",
                "currIdMap",
                "dirty",
                "epoch",
                "scope",
                "topic",
                "totalQueues",
            ],
            Self::TopicQueueMappingDetail => &[
                "bname",
                "currIdMap",
                "dirty",
                "epoch",
                "hostedQueues",
                "scope",
                "topic",
                "totalQueues",
            ],
            Self::TopicRouteData => &[
                "brokerDatas",
                "filterServerTable",
                "orderTopicConf",
                "queueDatas",
                "topicQueueMappingByBroker",
            ],
            Self::ProducerData => &["groupName"],
        }
    }

    fn child_kind(self, field: &str) -> Option<Self> {
        match (self, field) {
            (Self::BatchAckMessageRequestBody, "acks") => Some(Self::BatchAck),
            (Self::CheckClientRequestBody, "subscriptionData") => Some(Self::SubscriptionData),
            (Self::ConsumerData, "subscriptionDataSet") => Some(Self::SubscriptionData),
            (Self::HeartbeatData, "consumerDataSet") => Some(Self::ConsumerData),
            (Self::HeartbeatData, "producerDataSet") => Some(Self::ProducerData),
            (Self::LiteSubscriptionCtlRequestBody, "subscriptionSet") => {
                Some(Self::LiteSubscriptionDto)
            }
            (Self::LockBatchRequestBody | Self::UnlockBatchRequestBody, "mqSet") => {
                Some(Self::MessageQueue)
            }
            (Self::LockBatchResponseBody, "lockOKMQSet") => Some(Self::MessageQueue),
            (Self::MessageQueueAssignment, "messageQueue") => Some(Self::MessageQueue),
            (Self::QueueTimeSpan, "messageQueue") => Some(Self::MessageQueue),
            (Self::QueryAssignmentResponseBody, "messageQueueAssignments") => {
                Some(Self::MessageQueueAssignment)
            }
            (Self::QueryConsumeTimeSpanBody, "consumeTimeSpanSet") => Some(Self::QueueTimeSpan),
            (Self::TopicRouteData, "brokerDatas") => Some(Self::BrokerData),
            (Self::TopicRouteData, "queueDatas") => Some(Self::QueueData),
            _ => None,
        }
    }

    fn root_for_type_name(type_name: &str) -> Option<Self> {
        let name = type_name.rsplit("::").next()?;
        Some(match name {
            "BatchAckMessageRequestBody" => Self::BatchAckMessageRequestBody,
            "CheckClientRequestBody" => Self::CheckClientRequestBody,
            "ConsumeMessageDirectlyResult" => Self::ConsumeMessageDirectlyResult,
            "ConsumerListByGroupResponseBody" => Self::GetConsumerListByGroupResponseBody,
            "GetConsumerStatusBody" => Self::GetConsumerStatusBody,
            "HeartbeatData" => Self::HeartbeatData,
            "LogicQueueMappingItem" => Self::LogicQueueMappingItem,
            "LiteSubscriptionCtlRequestBody" => Self::LiteSubscriptionCtlRequestBody,
            "LockBatchRequestBody" => Self::LockBatchRequestBody,
            "LockBatchResponseBody" => Self::LockBatchResponseBody,
            "MessageRequestModeSerializeWrapper" => Self::MessageRequestModeSerializeWrapper,
            "QueryAssignmentRequestBody" => Self::QueryAssignmentRequestBody,
            "QueryAssignmentResponseBody" => Self::QueryAssignmentResponseBody,
            "QueryConsumeTimeSpanBody" => Self::QueryConsumeTimeSpanBody,
            "ResetOffsetBody" => Self::ResetOffsetBody,
            "SetMessageRequestModeRequestBody" => Self::SetMessageRequestModeRequestBody,
            "TopicQueueMappingDetail" => Self::TopicQueueMappingDetail,
            "TopicQueueMappingInfo" => Self::TopicQueueMappingInfo,
            "TopicRouteData" => Self::TopicRouteData,
            "UnlockBatchRequestBody" => Self::UnlockBatchRequestBody,
            _ => return None,
        })
    }

    fn java_class_name(self) -> &'static str {
        match self {
            Self::BatchAck => "org.apache.rocketmq.remoting.protocol.body.BatchAck",
            Self::BatchAckMessageRequestBody => {
                "org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody"
            }
            Self::BrokerData => "org.apache.rocketmq.remoting.protocol.route.BrokerData",
            Self::CheckClientRequestBody => {
                "org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody"
            }
            Self::ConsumeMessageDirectlyResult => {
                "org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult"
            }
            Self::ConsumerData => "org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData",
            Self::GetConsumerListByGroupResponseBody => {
                "org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody"
            }
            Self::GetConsumerStatusBody => {
                "org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody"
            }
            Self::HeartbeatData => "org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData",
            Self::LiteSubscriptionCtlRequestBody => {
                "org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody"
            }
            Self::LiteSubscriptionDto => "org.apache.rocketmq.common.lite.LiteSubscriptionDTO",
            Self::LogicQueueMappingItem => {
                "org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem"
            }
            Self::LockBatchRequestBody => {
                "org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody"
            }
            Self::LockBatchResponseBody => {
                "org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody"
            }
            Self::MessageQueue => "org.apache.rocketmq.common.message.MessageQueue",
            Self::MessageQueueAssignment => {
                "org.apache.rocketmq.common.message.MessageQueueAssignment"
            }
            Self::MessageRequestModeSerializeWrapper => {
                "org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper"
            }
            Self::QueryAssignmentRequestBody => {
                "org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody"
            }
            Self::QueryAssignmentResponseBody => {
                "org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody"
            }
            Self::QueryConsumeTimeSpanBody => {
                "org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody"
            }
            Self::QueueData => "org.apache.rocketmq.remoting.protocol.route.QueueData",
            Self::QueueTimeSpan => "org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan",
            Self::ResetOffsetBody => "org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody",
            Self::SetMessageRequestModeRequestBody => {
                "org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody"
            }
            Self::SubscriptionData => {
                "org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData"
            }
            Self::TopicQueueMappingInfo => {
                "org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo"
            }
            Self::TopicQueueMappingDetail => {
                "org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail"
            }
            Self::TopicRouteData => "org.apache.rocketmq.remoting.protocol.route.TopicRouteData",
            Self::UnlockBatchRequestBody => {
                "org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody"
            }
            Self::ProducerData => "org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData",
        }
    }

    fn is_set_field(self, field: &str) -> bool {
        matches!(
            (self, field),
            (Self::ConsumerData, "subscriptionDataSet")
                | (Self::HeartbeatData, "consumerDataSet" | "producerDataSet")
                | (Self::LiteSubscriptionCtlRequestBody, "subscriptionSet")
                | (Self::LiteSubscriptionDto, "liteTopicSet")
                | (
                    Self::LockBatchRequestBody | Self::UnlockBatchRequestBody,
                    "mqSet"
                )
                | (Self::LockBatchResponseBody, "lockOKMQSet")
                | (Self::QueryAssignmentResponseBody, "messageQueueAssignments")
                | (Self::SubscriptionData, "codeSet" | "tagsSet")
        )
    }
}

fn java_root_kind<T>() -> Result<JavaBeanKind, serde_json::Error> {
    JavaBeanKind::root_for_type_name(std::any::type_name::<T>()).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom(format!(
            "Fastjson WriteClassName has no DTO field schema for {}",
            std::any::type_name::<T>()
        ))
    })
}

pub(crate) fn apply_java_message_queue_class_name(value: &mut serde_json::Value) {
    insert_java_class_name(value, JavaBeanKind::MessageQueue);
}

fn insert_java_class_name(value: &mut serde_json::Value, kind: JavaBeanKind) {
    if is_java_reference_value(value) {
        return;
    }
    if let Some(fields) = value.as_object_mut() {
        fields.insert(
            "@type".to_owned(),
            serde_json::Value::String(kind.java_class_name().to_owned()),
        );
    }
}

pub(crate) fn is_java_long_field(field: Option<&str>) -> bool {
    matches!(
        field,
        Some(
            "consumeTimeStamp"
                | "delayTime"
                | "endOffset"
                | "epoch"
                | "invisibleTime"
                | "logicOffset"
                | "maxTimeStamp"
                | "minTimeStamp"
                | "popTime"
                | "it"
                | "pt"
                | "so"
                | "spentTimeMills"
                | "startOffset"
                | "subVersion"
                | "timeOfEnd"
                | "timeOfStart"
                | "version"
        )
    )
}

pub(crate) fn is_small_java_long(value: &serde_json::Number) -> bool {
    value
        .as_i64()
        .is_some_and(|value| (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&value))
}

fn java_hash_map_class_name(value: &mut serde_json::Value, not_write_hash_map_array_list: bool) {
    if !not_write_hash_map_array_list {
        if let Some(fields) = value.as_object_mut() {
            fields.insert(
                "@type".to_owned(),
                serde_json::Value::String("java.util.HashMap".to_owned()),
            );
        }
    }
}

fn java_concurrent_hash_map_class_name(value: &mut serde_json::Value) {
    // Fastjson's NotWriteHashMapArrayListClassName only suppresses HashMap and
    // ArrayList, not ConcurrentHashMap.
    if let Some(fields) = value.as_object_mut() {
        fields.insert(
            "@type".to_owned(),
            serde_json::Value::String("java.util.concurrent.ConcurrentHashMap".to_owned()),
        );
    }
}

fn for_each_topic_queue_mapping_detail_logic_item_mut(
    value: &mut serde_json::Value,
    mut apply: impl FnMut(&mut serde_json::Value),
) {
    let Some(entries) = value.as_object_mut() else {
        return;
    };
    for items in entries
        .values_mut()
        .filter_map(serde_json::Value::as_array_mut)
    {
        for item in items {
            apply(item);
        }
    }
}

fn apply_java_class_names_to_bean(
    value: &mut serde_json::Value,
    kind: JavaBeanKind,
    include_type: bool,
    not_write_hash_map_array_list: bool,
) {
    if is_java_reference_value(value) {
        return;
    }
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    if include_type {
        fields.insert(
            "@type".to_owned(),
            serde_json::Value::String(kind.java_class_name().to_owned()),
        );
    }

    for &name in kind.field_names() {
        let Some(field) = fields.get_mut(name) else {
            continue;
        };

        if let Some(child_kind) = kind.child_kind(name) {
            if kind.is_set_field(name)
                || (kind == JavaBeanKind::BatchAckMessageRequestBody && name == "acks")
            {
                if let Some(items) = field.as_array_mut() {
                    for item in items {
                        apply_java_class_names_to_bean(
                            item,
                            child_kind,
                            true,
                            not_write_hash_map_array_list,
                        );
                    }
                }
            } else {
                match field {
                    serde_json::Value::Array(items) => {
                        for item in items {
                            apply_java_class_names_to_bean(
                                item,
                                child_kind,
                                false,
                                not_write_hash_map_array_list,
                            );
                        }
                    }
                    serde_json::Value::Object(_) => apply_java_class_names_to_bean(
                        field,
                        child_kind,
                        false,
                        not_write_hash_map_array_list,
                    ),
                    serde_json::Value::Null
                    | serde_json::Value::Bool(_)
                    | serde_json::Value::Number(_)
                    | serde_json::Value::String(_) => {}
                }
            }
            continue;
        }

        match (kind, name) {
            (JavaBeanKind::GetConsumerStatusBody, "messageQueueTable")
            | (JavaBeanKind::GetConsumerStatusBody, "consumerTable")
            | (JavaBeanKind::ResetOffsetBody, "offsetTable")
            | (JavaBeanKind::TopicRouteData, "topicQueueMappingByBroker")
            | (JavaBeanKind::MessageQueueAssignment, "attachments") => {
                java_hash_map_class_name(field, not_write_hash_map_array_list);
                if kind == JavaBeanKind::TopicRouteData && name == "topicQueueMappingByBroker" {
                    if let Some(entries) = field.as_object_mut() {
                        for value in entries.values_mut() {
                            apply_java_class_names_to_bean(
                                value,
                                JavaBeanKind::TopicQueueMappingInfo,
                                false,
                                not_write_hash_map_array_list,
                            );
                        }
                    }
                } else if kind == JavaBeanKind::GetConsumerStatusBody && name == "consumerTable" {
                    if let Some(entries) = field.as_object_mut() {
                        for value in entries.values_mut() {
                            java_hash_map_class_name(value, not_write_hash_map_array_list);
                        }
                    }
                }
            }
            (
                JavaBeanKind::TopicQueueMappingInfo | JavaBeanKind::TopicQueueMappingDetail,
                "currIdMap",
            ) => {
                java_concurrent_hash_map_class_name(field);
            }
            (JavaBeanKind::TopicQueueMappingDetail, "hostedQueues") => {
                java_concurrent_hash_map_class_name(field);
                for_each_topic_queue_mapping_detail_logic_item_mut(field, |item| {
                    apply_java_class_names_to_bean(
                        item,
                        JavaBeanKind::LogicQueueMappingItem,
                        true,
                        not_write_hash_map_array_list,
                    );
                });
            }
            (JavaBeanKind::MessageRequestModeSerializeWrapper, "messageRequestModeMap") => {
                if let Some(topics) = field.as_object_mut() {
                    for groups in topics.values_mut() {
                        if let Some(groups) = groups.as_object_mut() {
                            for mode in groups.values_mut() {
                                apply_java_class_names_to_bean(
                                    mode,
                                    JavaBeanKind::SetMessageRequestModeRequestBody,
                                    true,
                                    not_write_hash_map_array_list,
                                );
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

pub(crate) fn apply_java_class_names<T>(
    value: &mut serde_json::Value,
    not_write_root_class_name: bool,
    not_write_hash_map_array_list: bool,
) -> Result<(), serde_json::Error> {
    let kind = java_root_kind::<T>()?;
    apply_java_class_names_to_bean(
        value,
        kind,
        !not_write_root_class_name,
        not_write_hash_map_array_list,
    );
    Ok(())
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) enum JavaJsonPathSegment {
    ArrayIndex(usize),
    ObjectKey(String),
}

pub(crate) type BeanToArrayMapFields = HashMap<Vec<JavaJsonPathSegment>, &'static str>;

pub(crate) fn apply_java_bean_to_array<T>(
    value: &mut serde_json::Value,
    map_fields: &mut BeanToArrayMapFields,
    use_enum_names: bool,
) -> Result<(), serde_json::Error> {
    let kind = JavaBeanKind::root_for_type_name(std::any::type_name::<T>()).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom(format!(
            "Fastjson BeanToArray has no DTO field schema for {}",
            std::any::type_name::<T>()
        ))
    })?;
    serialize_java_bean_as_array(value, kind, &mut Vec::new(), map_fields, use_enum_names);
    Ok(())
}

pub(crate) fn apply_java_write_nulls<T>(
    value: &mut serde_json::Value,
) -> Result<(), serde_json::Error> {
    let kind = JavaBeanKind::root_for_type_name(std::any::type_name::<T>()).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom(format!(
            "Fastjson WriteNulls has no DTO field schema for {}",
            std::any::type_name::<T>()
        ))
    })?;
    include_java_nullable_fields(value, kind);
    Ok(())
}

pub(crate) fn apply_java_null_lists<T>(
    value: &mut serde_json::Value,
) -> Result<(), serde_json::Error> {
    let kind = JavaBeanKind::root_for_type_name(std::any::type_name::<T>()).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom(format!(
            "Fastjson WriteNullListAsEmpty has no DTO field schema for {}",
            std::any::type_name::<T>()
        ))
    })?;
    write_java_null_lists(value, kind);
    Ok(())
}

pub(crate) fn apply_java_null_strings<T>(
    value: &mut serde_json::Value,
) -> Result<(), serde_json::Error> {
    let kind = JavaBeanKind::root_for_type_name(std::any::type_name::<T>()).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom(format!(
            "Fastjson WriteNullStringAsEmpty has no DTO field schema for {}",
            std::any::type_name::<T>()
        ))
    })?;
    write_java_null_strings(value, kind);
    Ok(())
}

pub(crate) fn apply_java_null_defaults<T>(
    value: &mut serde_json::Value,
) -> Result<(), serde_json::Error> {
    let kind = JavaBeanKind::root_for_type_name(std::any::type_name::<T>()).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom(format!(
            "Fastjson NullAsDefaultValue has no DTO field schema for {}",
            std::any::type_name::<T>()
        ))
    })?;
    include_java_nullable_fields(value, kind);
    write_java_null_lists(value, kind);
    write_java_null_strings(value, kind);
    write_java_null_objects(value, kind);
    Ok(())
}

fn write_java_null_objects(value: &mut serde_json::Value, kind: JavaBeanKind) {
    if is_java_reference_value(value) {
        return;
    }
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    for &name in kind.field_names() {
        let child_kind = kind.child_kind(name);
        let is_map = java_is_nullable_map_field(kind, name);
        let is_object = java_is_nullable_object_field(kind, name);
        if !is_map && child_kind.is_none() && !is_object {
            continue;
        }
        let field = fields
            .entry(name.to_owned())
            .or_insert(serde_json::Value::Null);
        if field.is_null() {
            *field = serde_json::Value::Object(serde_json::Map::new());
            continue;
        }
        if let Some(child_kind) = child_kind {
            match field {
                serde_json::Value::Array(items) => {
                    for item in items {
                        write_java_null_objects(item, child_kind);
                    }
                }
                serde_json::Value::Object(_) => write_java_null_objects(field, child_kind),
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => {}
            }
        } else if kind == JavaBeanKind::TopicRouteData && name == "topicQueueMappingByBroker" {
            if let Some(entries) = field.as_object_mut() {
                for entry in entries.values_mut() {
                    write_java_null_objects(entry, JavaBeanKind::TopicQueueMappingInfo);
                }
            }
        } else if kind == JavaBeanKind::TopicQueueMappingDetail && name == "hostedQueues" {
            for_each_topic_queue_mapping_detail_logic_item_mut(field, |item| {
                write_java_null_objects(item, JavaBeanKind::LogicQueueMappingItem);
            });
        } else if kind == JavaBeanKind::MessageRequestModeSerializeWrapper
            && name == "messageRequestModeMap"
        {
            if let Some(topics) = field.as_object_mut() {
                for groups in topics.values_mut() {
                    if let Some(groups) = groups.as_object_mut() {
                        for mode in groups.values_mut() {
                            write_java_null_objects(
                                mode,
                                JavaBeanKind::SetMessageRequestModeRequestBody,
                            );
                        }
                    }
                }
            }
        }
    }
}

fn java_is_nullable_map_field(kind: JavaBeanKind, name: &str) -> bool {
    matches!(
        (kind, name),
        (JavaBeanKind::BrokerData, "brokerAddrs")
            | (
                JavaBeanKind::GetConsumerStatusBody,
                "consumerTable" | "messageQueueTable"
            )
            | (JavaBeanKind::MessageQueueAssignment, "attachments")
            | (
                JavaBeanKind::MessageRequestModeSerializeWrapper,
                "messageRequestModeMap"
            )
            | (JavaBeanKind::ResetOffsetBody, "offsetTable")
            | (
                JavaBeanKind::TopicQueueMappingInfo | JavaBeanKind::TopicQueueMappingDetail,
                "currIdMap"
            )
            | (JavaBeanKind::TopicQueueMappingDetail, "hostedQueues")
            | (
                JavaBeanKind::TopicRouteData,
                "filterServerTable" | "topicQueueMappingByBroker"
            )
    )
}

fn java_is_nullable_object_field(kind: JavaBeanKind, name: &str) -> bool {
    matches!(
        (kind, name),
        (JavaBeanKind::LiteSubscriptionDto, "offsetOption")
    )
}

fn write_java_null_strings(value: &mut serde_json::Value, kind: JavaBeanKind) {
    if is_java_reference_value(value) {
        return;
    }
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    for &name in kind.field_names() {
        if java_is_nullable_string_field(kind, name) {
            let field = fields
                .entry(name.to_owned())
                .or_insert(serde_json::Value::Null);
            if field.is_null() {
                *field = serde_json::Value::String(String::new());
            }
            continue;
        }
        let Some(field) = fields.get_mut(name) else {
            continue;
        };
        if java_is_nullable_string_collection(kind, name) {
            if let Some(items) = field.as_array_mut() {
                for item in items {
                    if item.is_null() {
                        *item = serde_json::Value::String(String::new());
                    }
                }
            }
            continue;
        }
        if let Some(child_kind) = kind.child_kind(name) {
            match field {
                serde_json::Value::Array(items) => {
                    for item in items {
                        write_java_null_strings(item, child_kind);
                    }
                }
                serde_json::Value::Object(_) => write_java_null_strings(field, child_kind),
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => {}
            }
        } else if kind == JavaBeanKind::TopicRouteData && name == "topicQueueMappingByBroker" {
            if let Some(entries) = field.as_object_mut() {
                for entry in entries.values_mut() {
                    write_java_null_strings(entry, JavaBeanKind::TopicQueueMappingInfo);
                }
            }
        } else if kind == JavaBeanKind::TopicQueueMappingDetail && name == "hostedQueues" {
            for_each_topic_queue_mapping_detail_logic_item_mut(field, |item| {
                write_java_null_strings(item, JavaBeanKind::LogicQueueMappingItem);
            });
        } else if kind == JavaBeanKind::MessageRequestModeSerializeWrapper
            && name == "messageRequestModeMap"
        {
            if let Some(topics) = field.as_object_mut() {
                for groups in topics.values_mut() {
                    if let Some(groups) = groups.as_object_mut() {
                        for mode in groups.values_mut() {
                            write_java_null_strings(
                                mode,
                                JavaBeanKind::SetMessageRequestModeRequestBody,
                            );
                        }
                    }
                }
            }
        }
    }
}

fn java_is_nullable_string_field(kind: JavaBeanKind, name: &str) -> bool {
    matches!(
        (kind, name),
        (JavaBeanKind::BatchAck, "c" | "r" | "t")
            | (JavaBeanKind::BatchAckMessageRequestBody, "brokerName")
            | (
                JavaBeanKind::BrokerData,
                "brokerName" | "cluster" | "zoneName"
            )
            | (
                JavaBeanKind::CheckClientRequestBody,
                "clientId" | "group" | "namespace"
            )
            | (JavaBeanKind::ConsumeMessageDirectlyResult, "remark")
            | (
                JavaBeanKind::ConsumerData | JavaBeanKind::ProducerData,
                "groupName"
            )
            | (JavaBeanKind::HeartbeatData, "clientID")
            | (
                JavaBeanKind::LiteSubscriptionDto,
                "clientId" | "group" | "topic"
            )
            | (
                JavaBeanKind::LockBatchRequestBody | JavaBeanKind::UnlockBatchRequestBody,
                "clientId" | "consumerGroup"
            )
            | (JavaBeanKind::MessageQueue, "brokerName" | "topic")
            | (
                JavaBeanKind::QueryAssignmentRequestBody,
                "clientId" | "consumerGroup" | "strategyName" | "topic"
            )
            | (JavaBeanKind::QueueData, "brokerName")
            | (
                JavaBeanKind::SetMessageRequestModeRequestBody,
                "consumerGroup" | "topic"
            )
            | (
                JavaBeanKind::SubscriptionData,
                "expressionType" | "subString" | "topic"
            )
            | (
                JavaBeanKind::TopicQueueMappingInfo,
                "bname" | "scope" | "topic"
            )
            | (
                JavaBeanKind::TopicQueueMappingDetail,
                "bname" | "scope" | "topic"
            )
            | (JavaBeanKind::LogicQueueMappingItem, "bname")
            | (JavaBeanKind::TopicRouteData, "orderTopicConf")
    )
}

fn java_is_nullable_string_collection(kind: JavaBeanKind, name: &str) -> bool {
    matches!(
        (kind, name),
        (
            JavaBeanKind::GetConsumerListByGroupResponseBody,
            "consumerIdList"
        ) | (JavaBeanKind::LiteSubscriptionDto, "liteTopicSet")
            | (JavaBeanKind::SubscriptionData, "tagsSet")
    )
}

fn write_java_null_lists(value: &mut serde_json::Value, kind: JavaBeanKind) {
    if is_java_reference_value(value) {
        return;
    }
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    for &name in kind.field_names() {
        if java_is_nullable_list_field(kind, name) {
            let field = fields
                .entry(name.to_owned())
                .or_insert(serde_json::Value::Null);
            if field.is_null() {
                *field = serde_json::Value::Array(Vec::new());
            }
            continue;
        }
        let Some(field) = fields.get_mut(name) else {
            continue;
        };
        if let Some(child_kind) = kind.child_kind(name) {
            match field {
                serde_json::Value::Array(items) => {
                    for item in items {
                        write_java_null_lists(item, child_kind);
                    }
                }
                serde_json::Value::Object(_) => write_java_null_lists(field, child_kind),
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => {}
            }
        } else if kind == JavaBeanKind::TopicRouteData && name == "topicQueueMappingByBroker" {
            if let Some(entries) = field.as_object_mut() {
                for entry in entries.values_mut() {
                    write_java_null_lists(entry, JavaBeanKind::TopicQueueMappingInfo);
                }
            }
        } else if kind == JavaBeanKind::TopicQueueMappingDetail && name == "hostedQueues" {
            for_each_topic_queue_mapping_detail_logic_item_mut(field, |item| {
                write_java_null_lists(item, JavaBeanKind::LogicQueueMappingItem);
            });
        } else if kind == JavaBeanKind::MessageRequestModeSerializeWrapper
            && name == "messageRequestModeMap"
        {
            if let Some(topics) = field.as_object_mut() {
                for groups in topics.values_mut() {
                    if let Some(groups) = groups.as_object_mut() {
                        for mode in groups.values_mut() {
                            write_java_null_lists(
                                mode,
                                JavaBeanKind::SetMessageRequestModeRequestBody,
                            );
                        }
                    }
                }
            }
        }
    }
}

fn java_is_nullable_list_field(kind: JavaBeanKind, name: &str) -> bool {
    matches!(
        (kind, name),
        (JavaBeanKind::BatchAckMessageRequestBody, "acks")
            | (JavaBeanKind::ConsumerData, "subscriptionDataSet")
            | (
                JavaBeanKind::GetConsumerListByGroupResponseBody,
                "consumerIdList"
            )
            | (
                JavaBeanKind::HeartbeatData,
                "consumerDataSet" | "producerDataSet"
            )
            | (
                JavaBeanKind::LiteSubscriptionCtlRequestBody,
                "subscriptionSet"
            )
            | (JavaBeanKind::LiteSubscriptionDto, "liteTopicSet")
            | (
                JavaBeanKind::LockBatchRequestBody | JavaBeanKind::UnlockBatchRequestBody,
                "mqSet"
            )
            | (JavaBeanKind::LockBatchResponseBody, "lockOKMQSet")
            | (
                JavaBeanKind::QueryAssignmentResponseBody,
                "messageQueueAssignments"
            )
            | (JavaBeanKind::QueryConsumeTimeSpanBody, "consumeTimeSpanSet")
            | (JavaBeanKind::SubscriptionData, "codeSet" | "tagsSet")
            | (JavaBeanKind::TopicRouteData, "brokerDatas" | "queueDatas")
    )
}

fn include_java_nullable_fields(value: &mut serde_json::Value, kind: JavaBeanKind) {
    if is_java_reference_value(value) {
        return;
    }
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    for &name in kind.field_names() {
        let field = fields
            .entry(name.to_owned())
            .or_insert(serde_json::Value::Null);
        if let Some(child_kind) = kind.child_kind(name) {
            match field {
                serde_json::Value::Array(items) => {
                    for item in items {
                        include_java_nullable_fields(item, child_kind);
                    }
                }
                serde_json::Value::Object(_) => include_java_nullable_fields(field, child_kind),
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => {}
            }
        } else if kind == JavaBeanKind::TopicRouteData && name == "topicQueueMappingByBroker" {
            if let Some(entries) = field.as_object_mut() {
                for entry in entries.values_mut() {
                    include_java_nullable_fields(entry, JavaBeanKind::TopicQueueMappingInfo);
                }
            }
        } else if kind == JavaBeanKind::TopicQueueMappingDetail && name == "hostedQueues" {
            for_each_topic_queue_mapping_detail_logic_item_mut(field, |item| {
                include_java_nullable_fields(item, JavaBeanKind::LogicQueueMappingItem);
            });
        } else if kind == JavaBeanKind::MessageRequestModeSerializeWrapper
            && name == "messageRequestModeMap"
        {
            if let Some(topics) = field.as_object_mut() {
                for groups in topics.values_mut() {
                    if let Some(groups) = groups.as_object_mut() {
                        for mode in groups.values_mut() {
                            include_java_nullable_fields(
                                mode,
                                JavaBeanKind::SetMessageRequestModeRequestBody,
                            );
                        }
                    }
                }
            }
        }
    }
}

fn serialize_java_bean_as_array(
    value: &mut serde_json::Value,
    kind: JavaBeanKind,
    path: &mut Vec<JavaJsonPathSegment>,
    map_fields: &mut BeanToArrayMapFields,
    use_enum_names: bool,
) {
    if is_java_reference_value(value) {
        return;
    }
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    let mut fields = std::mem::take(fields);
    let mut values = Vec::with_capacity(kind.field_names().len());
    for (index, &name) in kind.field_names().iter().enumerate() {
        let mut value = fields.remove(name).unwrap_or(serde_json::Value::Null);
        path.push(JavaJsonPathSegment::ArrayIndex(index));
        if is_java_map_property(name) {
            map_fields.insert(path.clone(), name);
        }
        if let Some(child_kind) = kind.child_kind(name) {
            match &mut value {
                serde_json::Value::Array(items) => {
                    for (item_index, item) in items.iter_mut().enumerate() {
                        path.push(JavaJsonPathSegment::ArrayIndex(item_index));
                        serialize_java_bean_as_array(
                            item,
                            child_kind,
                            path,
                            map_fields,
                            use_enum_names,
                        );
                        path.pop();
                    }
                }
                _ => serialize_java_bean_as_array(
                    &mut value,
                    child_kind,
                    path,
                    map_fields,
                    use_enum_names,
                ),
            }
        } else if kind == JavaBeanKind::TopicRouteData && name == "topicQueueMappingByBroker" {
            apply_java_bean_map_values(&mut value, path, map_fields, use_enum_names);
        } else if kind == JavaBeanKind::TopicQueueMappingDetail && name == "hostedQueues" {
            apply_java_bean_map_list_values(&mut value, path, map_fields, use_enum_names);
        } else if kind == JavaBeanKind::GetConsumerStatusBody && name == "consumerTable" {
            mark_nested_java_bean_map_values(&value, path, map_fields, "consumerTable");
        } else if kind == JavaBeanKind::MessageRequestModeSerializeWrapper
            && name == "messageRequestModeMap"
        {
            if let Some(topics) = value.as_object_mut() {
                for (topic, groups) in topics {
                    path.push(JavaJsonPathSegment::ObjectKey(topic.clone()));
                    if let Some(groups) = groups.as_object_mut() {
                        map_fields.insert(path.clone(), "messageRequestModeMap");
                        for (group, mode) in groups {
                            path.push(JavaJsonPathSegment::ObjectKey(group.clone()));
                            serialize_java_bean_as_array(
                                mode,
                                JavaBeanKind::SetMessageRequestModeRequestBody,
                                path,
                                map_fields,
                                use_enum_names,
                            );
                            path.pop();
                        }
                    }
                    path.pop();
                }
            }
        }
        if !use_enum_names {
            if let Some(ordinal) = java_enum_ordinal(name, &value) {
                value = serde_json::Value::Number(ordinal.into());
            }
        }
        values.push(value);
        path.pop();
    }
    *value = serde_json::Value::Array(values);
}

pub(crate) fn apply_java_message_queue_bean_to_array(
    value: &mut serde_json::Value,
    use_enum_names: bool,
) {
    serialize_java_bean_as_array(
        value,
        JavaBeanKind::MessageQueue,
        &mut Vec::new(),
        &mut HashMap::new(),
        use_enum_names,
    );
}

fn mark_nested_java_bean_map_values(
    value: &serde_json::Value,
    path: &mut Vec<JavaJsonPathSegment>,
    map_fields: &mut BeanToArrayMapFields,
    field_name: &'static str,
) {
    if let Some(entries) = value.as_object() {
        for (key, value) in entries {
            if value.is_object() {
                path.push(JavaJsonPathSegment::ObjectKey(key.clone()));
                map_fields.insert(path.clone(), field_name);
                path.pop();
            }
        }
    }
}

fn apply_java_bean_map_values(
    value: &mut serde_json::Value,
    path: &mut Vec<JavaJsonPathSegment>,
    map_fields: &mut BeanToArrayMapFields,
    use_enum_names: bool,
) {
    if let Some(entries) = value.as_object_mut() {
        for (key, value) in entries {
            path.push(JavaJsonPathSegment::ObjectKey(key.clone()));
            serialize_java_bean_as_array(
                value,
                JavaBeanKind::TopicQueueMappingInfo,
                path,
                map_fields,
                use_enum_names,
            );
            path.pop();
        }
    }
}

fn apply_java_bean_map_list_values(
    value: &mut serde_json::Value,
    path: &mut Vec<JavaJsonPathSegment>,
    map_fields: &mut BeanToArrayMapFields,
    use_enum_names: bool,
) {
    if let Some(entries) = value.as_object_mut() {
        for (key, items) in entries {
            path.push(JavaJsonPathSegment::ObjectKey(key.clone()));
            if let Some(items) = items.as_array_mut() {
                for (index, item) in items.iter_mut().enumerate() {
                    path.push(JavaJsonPathSegment::ArrayIndex(index));
                    serialize_java_bean_as_array(
                        item,
                        JavaBeanKind::LogicQueueMappingItem,
                        path,
                        map_fields,
                        use_enum_names,
                    );
                    path.pop();
                }
            }
            path.pop();
        }
    }
}

fn omit_default_writer_map_values(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                match value {
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                        omit_default_writer_values(value);
                    }
                    serde_json::Value::Null
                    | serde_json::Value::Bool(_)
                    | serde_json::Value::Number(_)
                    | serde_json::Value::String(_) => {}
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                omit_default_writer_values(value);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

pub(crate) fn omit_empty_writer_values(value: &mut serde_json::Value, omit_empty_strings: bool) {
    match value {
        serde_json::Value::Object(fields) => {
            for value in fields.values_mut() {
                omit_empty_writer_values(value, omit_empty_strings);
            }
            fields.retain(|_, value| match value {
                serde_json::Value::Array(values) => !values.is_empty(),
                serde_json::Value::Object(fields) => !fields.is_empty(),
                serde_json::Value::String(string) => !omit_empty_strings || !string.is_empty(),
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_) => true,
            });
        }
        serde_json::Value::Array(values) => {
            for value in values {
                omit_empty_writer_values(value, omit_empty_strings);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

pub(crate) fn java_json_indent(
    pretty_format: bool,
    features: &[JavaJsonWriterFeature],
) -> Option<&'static str> {
    if features.contains(&JavaJsonWriterFeature::PrettyFormatWith4Space) {
        Some("    ")
    } else if features.contains(&JavaJsonWriterFeature::PrettyFormatWith2Space) {
        Some("  ")
    } else if pretty_format || features.contains(&JavaJsonWriterFeature::PrettyFormat) {
        Some("\t")
    } else {
        None
    }
}

pub(crate) fn validate_java_writer_features(
    features: &[JavaJsonWriterFeature],
) -> Result<(), serde_json::Error> {
    if let Some(feature) = features.iter().find(|feature| !feature.is_implemented()) {
        return Err(<serde_json::Error as serde::ser::Error>::custom(format!(
            "Fastjson2 writer feature {} has not been mapped yet",
            feature.name()
        )));
    }
    Ok(())
}

pub(crate) fn java_non_serializable_writer_output(
    features: &[JavaJsonWriterFeature],
) -> Result<Option<String>, serde_json::Error> {
    // Fastjson2's IgnoreNoneSerializable result takes precedence when both
    // non-serializable policies are enabled.
    if features.contains(&JavaJsonWriterFeature::IgnoreNoneSerializable) {
        return Ok(Some("null".to_owned()));
    }
    if features.contains(&JavaJsonWriterFeature::ErrorOnNoneSerializable) {
        return Err(<serde_json::Error as serde::ser::Error>::custom(
            "JSONException",
        ));
    }
    Ok(None)
}

pub(crate) fn apply_java_json_writer_value_features(
    value: &mut serde_json::Value,
    write_non_string_values_as_strings: bool,
    write_boolean_as_number: bool,
    write_enum_using_ordinal: bool,
    browser_compatible: bool,
) {
    apply_java_json_writer_value_features_inner(
        value,
        write_non_string_values_as_strings,
        write_boolean_as_number,
        write_enum_using_ordinal,
        browser_compatible,
        false,
        false,
    );
}

fn apply_java_json_writer_value_features_inner(
    value: &mut serde_json::Value,
    write_non_string_values_as_strings: bool,
    write_boolean_as_number: bool,
    write_enum_using_ordinal: bool,
    browser_compatible: bool,
    object_is_map: bool,
    map_values_are_maps: bool,
) {
    if is_java_reference_value(value) {
        return;
    }
    match value {
        serde_json::Value::Bool(boolean) => {
            if write_non_string_values_as_strings {
                *value = serde_json::Value::String(boolean.to_string());
            } else if write_boolean_as_number {
                *value = serde_json::Value::Number(serde_json::Number::from(if *boolean {
                    1
                } else {
                    0
                }));
            }
        }
        serde_json::Value::Number(number) => {
            if write_non_string_values_as_strings
                || (browser_compatible && !is_browser_safe_integer(number))
            {
                *value = serde_json::Value::String(number.to_string());
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                apply_java_json_writer_value_features_inner(
                    value,
                    write_non_string_values_as_strings,
                    write_boolean_as_number,
                    write_enum_using_ordinal,
                    browser_compatible,
                    false,
                    false,
                );
            }
        }
        serde_json::Value::Object(fields) => {
            for (name, value) in fields.iter_mut() {
                if write_enum_using_ordinal && !object_is_map {
                    if let Some(ordinal) = java_enum_ordinal(name, value) {
                        *value = serde_json::Value::Number(serde_json::Number::from(ordinal));
                    }
                }
                let child_is_map = if object_is_map {
                    map_values_are_maps
                } else {
                    is_java_map_property(name)
                };
                let child_map_values_are_maps = !object_is_map && name == "messageRequestModeMap";
                apply_java_json_writer_value_features_inner(
                    value,
                    write_non_string_values_as_strings,
                    write_boolean_as_number,
                    write_enum_using_ordinal,
                    browser_compatible,
                    child_is_map,
                    child_map_values_are_maps,
                );
            }
        }
        serde_json::Value::Null | serde_json::Value::String(_) => {}
    }
}

fn is_browser_safe_integer(number: &serde_json::Number) -> bool {
    const JAVASCRIPT_INTEGER_MIN: i64 = -9_007_199_254_740_991;
    const JAVASCRIPT_INTEGER_MAX: u64 = 9_007_199_254_740_991;

    if let Some(value) = number.as_i64() {
        (JAVASCRIPT_INTEGER_MIN..=JAVASCRIPT_INTEGER_MAX as i64).contains(&value)
    } else if let Some(value) = number.as_u64() {
        value <= JAVASCRIPT_INTEGER_MAX
    } else {
        // BrowserCompatible stringifies unsafe integer fields, while floating
        // values retain their numeric representation.
        true
    }
}

fn java_enum_ordinal(field_name: &str, value: &serde_json::Value) -> Option<i64> {
    let value = value.as_str()?;
    let ordinal = match field_name {
        "action" => match value {
            "PARTIAL_ADD" => 0,
            "PARTIAL_REMOVE" => 1,
            "COMPLETE_ADD" => 2,
            "COMPLETE_REMOVE" => 3,
            _ => return None,
        },
        "consumeFromWhere" => match value {
            "CONSUME_FROM_LAST_OFFSET" => 0,
            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" => 1,
            "CONSUME_FROM_MIN_OFFSET" => 2,
            "CONSUME_FROM_MAX_OFFSET" => 3,
            "CONSUME_FROM_FIRST_OFFSET" => 4,
            "CONSUME_FROM_TIMESTAMP" => 5,
            _ => return None,
        },
        "consumeResult" => match value {
            "CR_SUCCESS" => 0,
            "CR_LATER" => 1,
            "CR_ROLLBACK" => 2,
            "CR_COMMIT" => 3,
            "CR_THROW_EXCEPTION" => 4,
            "CR_RETURN_NULL" => 5,
            _ => return None,
        },
        "consumeType" => match value {
            "CONSUME_ACTIVELY" => 0,
            "CONSUME_PASSIVELY" => 1,
            "CONSUME_POP" => 2,
            _ => return None,
        },
        "messageModel" => match value {
            "BROADCASTING" => 0,
            "CLUSTERING" => 1,
            "LITE_SELECTIVE" => 2,
            _ => return None,
        },
        "mode" => match value {
            "PULL" => 0,
            "POP" => 1,
            _ => return None,
        },
        "optionType" => match value {
            "POLICY" => 0,
            "OFFSET" => 1,
            "TAIL_N" => 2,
            "TIMESTAMP" => 3,
            _ => return None,
        },
        _ => return None,
    };
    Some(ordinal)
}

pub(crate) fn apply_java_json_long_as_string(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(
                    name.as_str(),
                    "consumerTable" | "messageQueueTable" | "offsetTable"
                ) {
                    apply_java_json_long_map_values(value);
                } else if is_java_long_property(name) {
                    stringify_json_number(value);
                } else {
                    apply_java_json_long_as_string(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                apply_java_json_long_as_string(value);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

fn is_java_long_property(name: &str) -> bool {
    matches!(
        name,
        "consumeTimeStamp"
            | "delayTime"
            | "epoch"
            | "endOffset"
            | "invisibleTime"
            | "logicOffset"
            | "maxTimeStamp"
            | "minTimeStamp"
            | "popTime"
            | "spentTimeMills"
            | "startOffset"
            | "subVersion"
            | "timeOfEnd"
            | "timeOfStart"
            | "updateTime"
            | "value"
            | "version"
            | "so"
            | "pt"
            | "it"
    )
}

fn apply_java_json_long_map_values(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                match value {
                    serde_json::Value::Number(_) => stringify_json_number(value),
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                        apply_java_json_long_map_values(value);
                    }
                    serde_json::Value::Null
                    | serde_json::Value::Bool(_)
                    | serde_json::Value::String(_) => {}
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                apply_java_json_long_map_values(value);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

fn stringify_json_number(value: &mut serde_json::Value) {
    if let serde_json::Value::Number(number) = value {
        *value = serde_json::Value::String(number.to_string());
    }
}

pub(crate) fn escape_none_ascii_json(json: &str) -> String {
    let mut output = String::with_capacity(json.len());
    let mut quote = None;
    let mut escaped = false;
    for character in json.chars() {
        let Some(delimiter) = quote else {
            output.push(character);
            if matches!(character, '"' | '\'') {
                quote = Some(character);
            }
            continue;
        };

        if escaped {
            output.push(character);
            escaped = false;
            continue;
        }

        match character {
            character if character == delimiter => {
                output.push(character);
                quote = None;
            }
            '\\' => {
                output.push(character);
                escaped = true;
            }
            character if character.is_ascii() => output.push(character),
            character => {
                let value = character as u32;
                if value < 0x800 {
                    // Fastjson2's pinned UTF-16 writer leaves two-byte code units unescaped.
                    output.push(character);
                } else if value <= 0xFFFF {
                    output.push_str(&format!("\\u{value:04X}"));
                } else {
                    let supplementary = value - 0x1_0000;
                    let high_surrogate = 0xD800 + (supplementary >> 10);
                    let low_surrogate = 0xDC00 + (supplementary & 0x3FF);
                    output.push_str(&format!("\\u{high_surrogate:04X}\\u{low_surrogate:04X}"));
                }
            }
        }
    }
    output
}

fn is_java_unquoted_field_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first == '$' || first.is_ascii_alphabetic())
        && chars.all(|character| {
            character == '_' || character == '$' || character.is_ascii_alphanumeric()
        })
}

pub(crate) fn is_java_enum_field(name: &str) -> bool {
    matches!(
        name,
        "action"
            | "consumeFromWhere"
            | "consumeResult"
            | "consumeType"
            | "messageModel"
            | "mode"
            | "optionType"
    )
}

pub(crate) fn write_java_json_string_with_browser_secure(
    value: &str,
    use_single_quotes: bool,
    browser_secure: bool,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    if !use_single_quotes {
        let encoded = serde_json::to_string(value)?;
        if browser_secure {
            for character in encoded.chars() {
                if matches!(character, '<' | '>' | '(' | ')') {
                    output.push_str(&format!("\\u{:04X}", character as u32));
                } else {
                    output.push(character);
                }
            }
        } else {
            output.push_str(&encoded);
        }
        return Ok(());
    }

    output.push('\'');
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\'' => output.push_str("\\'"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000C}' => output.push_str("\\f"),
            character if browser_secure && matches!(character, '<' | '>' | '(' | ')') => {
                output.push_str(&format!("\\u{:04X}", character as u32));
            }
            character if character <= '\u{001F}' => {
                output.push_str(&format!("\\u{:04X}", character as u32));
            }
            character => output.push(character),
        }
    }
    output.push('\'');
    Ok(())
}

pub(crate) fn write_java_json_field_name(
    key: &str,
    value: &serde_json::Value,
    is_map_entry: bool,
    use_single_quotes: bool,
    unquote_field_names: bool,
    browser_secure: bool,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    // Fastjson's specialized boolean field writer keeps its normal quoted
    // property name even when either field-name feature is enabled.
    let eligible_for_unquote = is_map_entry || !value.is_boolean();
    let eligible_for_single_quotes =
        is_map_entry || (!value.is_boolean() && !is_java_enum_field(key));
    if eligible_for_unquote
        && unquote_field_names
        && (is_map_entry || is_java_unquoted_field_name(key))
    {
        output.push_str(key);
        return Ok(());
    }
    write_java_json_string_with_browser_secure(
        key,
        eligible_for_single_quotes && use_single_quotes,
        browser_secure,
        output,
    )
}

#[derive(Clone, Copy, Default)]
struct JavaRouteWriterOptions {
    unquote_numeric_map_keys: bool,
    stringify_long_map_keys: bool,
    use_single_quotes: bool,
    unquote_field_names: bool,
    browser_secure: bool,
    write_class_name: bool,
    not_write_set_class_name: bool,
}

struct JavaRouteWriter<'a> {
    indent: Option<&'a str>,
    writer_options: JavaRouteWriterOptions,
    map_fields: &'a BeanToArrayMapFields,
    path: Vec<JavaJsonPathSegment>,
}

fn write_java_route_json(
    value: &serde_json::Value,
    field_name: Option<&str>,
    depth: usize,
    writer: &mut JavaRouteWriter<'_>,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    let indent = writer.indent;
    let writer_options = writer.writer_options;
    let field_name = writer.map_fields.get(&writer.path).copied().or(field_name);
    match value {
        serde_json::Value::Object(fields)
            if fields.is_empty() && (indent.is_none() || field_name.is_some()) =>
        {
            output.push_str("{}")
        }
        serde_json::Value::Object(fields) if fields.is_empty() => {
            output.push_str("{\n");
            write_java_indent(output, depth + 1, indent);
            output.push('\n');
            write_java_indent(output, depth, indent);
            output.push('}');
        }
        serde_json::Value::Object(fields) => {
            let numeric_keys = writer_options.unquote_numeric_map_keys
                && matches!(
                    field_name,
                    Some("brokerAddrs" | "currIdMap" | "hostedQueues")
                );
            let long_keys_are_strings =
                writer_options.stringify_long_map_keys && field_name == Some("brokerAddrs");
            let mut entries: Vec<_> = fields.iter().collect();
            if let Some(type_index) = entries.iter().position(|(key, _)| key.as_str() == "@type") {
                entries.rotate_left(type_index);
            }
            output.push('{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if indent.is_some() {
                    output.push('\n');
                    write_java_indent(output, depth + 1, indent);
                }
                if numeric_keys && !long_keys_are_strings && key.parse::<i64>().is_ok() {
                    output.push_str(key);
                    if writer_options.write_class_name
                        && field_name == Some("brokerAddrs")
                        && key.parse::<i32>().is_ok()
                    {
                        output.push('L');
                    }
                } else {
                    write_java_json_field_name(
                        key,
                        value,
                        field_name.is_some_and(is_java_map_property),
                        writer_options.use_single_quotes,
                        writer_options.unquote_field_names,
                        writer_options.browser_secure,
                        output,
                    )?;
                }
                output.push(':');
                writer
                    .path
                    .push(JavaJsonPathSegment::ObjectKey(key.clone()));
                let result = write_java_route_json(value, Some(key), depth + 1, writer, output);
                writer.path.pop();
                result?;
                if index + 1 != fields.len() {
                    output.push(',');
                }
            }
            if indent.is_some() {
                output.push('\n');
                write_java_indent(output, depth, indent);
            }
            output.push('}');
        }
        serde_json::Value::Array(values) if values.is_empty() && indent.is_none() => {
            if writer_options.write_class_name
                && !writer_options.not_write_set_class_name
                && is_java_set_property(field_name)
            {
                output.push_str("Set");
            }
            output.push_str("[]")
        }
        serde_json::Value::Array(values) if values.is_empty() => {
            if writer_options.write_class_name
                && !writer_options.not_write_set_class_name
                && is_java_set_property(field_name)
            {
                output.push_str("Set");
            }
            output.push_str("[\n");
            write_java_indent(output, depth + 1, indent);
            output.push('\n');
            write_java_indent(output, depth, indent);
            output.push(']');
        }
        serde_json::Value::Array(values) => {
            if writer_options.write_class_name
                && !writer_options.not_write_set_class_name
                && is_java_set_property(field_name)
            {
                output.push_str("Set");
            }
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if indent.is_some() {
                    output.push('\n');
                    write_java_indent(output, depth + 1, indent);
                }
                writer.path.push(JavaJsonPathSegment::ArrayIndex(index));
                let result = write_java_route_json(value, None, depth + 1, writer, output);
                writer.path.pop();
                result?;
                if index + 1 != values.len() {
                    output.push(',');
                }
            }
            if indent.is_some() {
                output.push('\n');
                write_java_indent(output, depth, indent);
            }
            output.push(']');
        }
        serde_json::Value::String(value) => {
            let use_single_quotes_for_value = writer_options.use_single_quotes
                && field_name.is_none_or(|name| !is_java_enum_field(name));
            write_java_json_string_with_browser_secure(
                value,
                use_single_quotes_for_value,
                writer_options.browser_secure,
                output,
            )?;
        }
        serde_json::Value::Number(number) => {
            output.push_str(&serde_json::to_string(value)?);
            if writer_options.write_class_name
                && is_java_long_field(field_name)
                && is_small_java_long(number)
            {
                output.push('L');
            }
        }
        _ => output.push_str(&serde_json::to_string(value)?),
    }
    Ok(())
}

pub(crate) fn is_java_set_property(field: Option<&str>) -> bool {
    matches!(
        field,
        Some(
            "codeSet"
                | "consumerDataSet"
                | "liteTopicSet"
                | "lockOKMQSet"
                | "messageQueueAssignments"
                | "mqSet"
                | "producerDataSet"
                | "subscriptionDataSet"
                | "subscriptionSet"
                | "tagsSet"
        )
    )
}

fn write_java_indent(output: &mut String, depth: usize, indent: Option<&str>) {
    let Some(indent) = indent else {
        return;
    };
    for _ in 0..depth {
        output.push_str(indent);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RouteObjectKeyKind {
    Bean,
    IntegerMap,
    StringMap,
    StringDtoMap,
    GenericMap,
}

enum RouteJsonContainer {
    Object {
        key_kind: RouteObjectKeyKind,
        expecting_key: bool,
        field: Option<String>,
    },
    Array {
        object_values: RouteObjectKeyKind,
    },
}

fn route_object_key_kind_for_value(stack: &mut [RouteJsonContainer]) -> RouteObjectKeyKind {
    let Some(parent) = stack.last_mut() else {
        return RouteObjectKeyKind::Bean;
    };
    match parent {
        RouteJsonContainer::Object {
            key_kind: RouteObjectKeyKind::Bean,
            field,
            ..
        } => match field.take().as_deref() {
            Some("brokerAddrs" | "currIdMap") => RouteObjectKeyKind::IntegerMap,
            Some("filterServerTable") => RouteObjectKeyKind::StringMap,
            Some("topicQueueMappingByBroker") => RouteObjectKeyKind::StringDtoMap,
            _ => RouteObjectKeyKind::GenericMap,
        },
        RouteJsonContainer::Object {
            key_kind: RouteObjectKeyKind::StringDtoMap,
            field,
            ..
        } => {
            field.take();
            RouteObjectKeyKind::Bean
        }
        RouteJsonContainer::Object { field, .. } => {
            field.take();
            RouteObjectKeyKind::GenericMap
        }
        RouteJsonContainer::Array { object_values } => *object_values,
    }
}

fn route_array_object_value_kind(stack: &mut [RouteJsonContainer]) -> RouteObjectKeyKind {
    let Some(parent) = stack.last_mut() else {
        return RouteObjectKeyKind::Bean;
    };
    match parent {
        RouteJsonContainer::Object {
            key_kind: RouteObjectKeyKind::Bean,
            field,
            ..
        } => match field.take().as_deref() {
            Some("queueDatas" | "brokerDatas") => RouteObjectKeyKind::Bean,
            _ => RouteObjectKeyKind::GenericMap,
        },
        RouteJsonContainer::Object { field, .. } => {
            field.take();
            RouteObjectKeyKind::GenericMap
        }
        RouteJsonContainer::Array { .. } => RouteObjectKeyKind::GenericMap,
    }
}

fn unquoted_numeric_object_key_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    if bytes
        .get(index)
        .is_some_and(|byte| matches!(*byte, b'+' | b'-'))
    {
        index += 1;
    }
    let integer_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    if index == integer_start {
        return None;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == fraction_start {
            return None;
        }
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_start {
            return None;
        }
    }
    Some(index)
}

fn java_numeric_object_key(raw: &str) -> String {
    let normalized = serde_fastjson::normalize_json_input(raw);
    if normalized.contains(['e', 'E']) {
        if let Ok(number) = normalized.parse::<f64>() {
            if let Some(number) = serde_json::Number::from_f64(number) {
                return number.to_string();
            }
        }
    }
    normalized
}

fn quote_unquoted_numeric_object_keys(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    let mut stack = Vec::<RouteJsonContainer>::new();

    while index < input.len() {
        let Some(character) = input[index..].chars().next() else {
            break;
        };
        if character == '"' {
            let start = index;
            index += 1;
            while index < input.len() {
                match bytes[index] {
                    b'\\' => index = (index + 2).min(input.len()),
                    b'"' => {
                        index += 1;
                        break;
                    }
                    _ => index += 1,
                }
            }
            let key = serde_json::from_str::<String>(&input[start..index]).ok();
            if let Some(RouteJsonContainer::Object {
                expecting_key,
                field,
                ..
            }) = stack.last_mut()
            {
                if *expecting_key {
                    *expecting_key = false;
                    *field = key;
                } else {
                    field.take();
                }
            }
            output.push_str(&input[start..index]);
            continue;
        }
        if character == '{' {
            let key_kind = route_object_key_kind_for_value(&mut stack);
            stack.push(RouteJsonContainer::Object {
                key_kind,
                expecting_key: true,
                field: None,
            });
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        if character == '[' {
            let object_values = route_array_object_value_kind(&mut stack);
            stack.push(RouteJsonContainer::Array { object_values });
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        if character == '}' || character == ']' {
            stack.pop();
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        if character == ',' {
            if let Some(RouteJsonContainer::Object {
                expecting_key,
                field,
                ..
            }) = stack.last_mut()
            {
                *expecting_key = true;
                field.take();
            }
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        if character == ':' {
            if let Some(RouteJsonContainer::Object { expecting_key, .. }) = stack.last_mut() {
                *expecting_key = false;
            }
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        let key_kind = match stack.last() {
            Some(RouteJsonContainer::Object {
                key_kind,
                expecting_key: true,
                ..
            }) if matches!(
                key_kind,
                RouteObjectKeyKind::IntegerMap
                    | RouteObjectKeyKind::StringMap
                    | RouteObjectKeyKind::StringDtoMap
                    | RouteObjectKeyKind::GenericMap
            ) =>
            {
                Some(*key_kind)
            }
            _ => None,
        };
        if key_kind == Some(RouteObjectKeyKind::IntegerMap) && input[index..].starts_with("null") {
            let key_end = index + "null".len();
            let mut colon = key_end;
            while bytes
                .get(colon)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                colon += 1;
            }
            if bytes.get(colon) == Some(&b':') {
                output.push_str("\"null\"");
                index = key_end;
                if let Some(RouteJsonContainer::Object {
                    expecting_key,
                    field,
                    ..
                }) = stack.last_mut()
                {
                    *expecting_key = false;
                    field.take();
                }
                continue;
            }
        }
        if key_kind.is_some()
            && character.is_ascii()
            && matches!(bytes[index], b'+' | b'-' | b'0'..=b'9')
        {
            let key_start = index;
            if let Some(key_end) = unquoted_numeric_object_key_end(bytes, key_start) {
                let mut colon = key_end;
                while bytes
                    .get(colon)
                    .is_some_and(|byte| byte.is_ascii_whitespace())
                {
                    colon += 1;
                }
                if bytes.get(colon) == Some(&b':') {
                    let key_kind = match stack.last() {
                        Some(RouteJsonContainer::Object { key_kind, .. }) => *key_kind,
                        _ => unreachable!(),
                    };
                    let key = &input[key_start..key_end];
                    output.push('"');
                    if key_kind == RouteObjectKeyKind::IntegerMap {
                        output.push_str(key);
                    } else {
                        output.push_str(&java_numeric_object_key(key));
                    }
                    output.push('"');
                    index = key_end;
                    if let Some(RouteJsonContainer::Object {
                        expecting_key,
                        field,
                        ..
                    }) = stack.last_mut()
                    {
                        *expecting_key = false;
                        field.take();
                    }
                    continue;
                }
            }
        }
        if character.is_ascii_whitespace() {
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        if let Some(RouteJsonContainer::Object {
            expecting_key: false,
            field,
            ..
        }) = stack.last_mut()
        {
            field.take();
        }
        output.push(character);
        index += character.len_utf8();
    }
    output
}

pub(crate) fn parse_java_route_json<T: DeserializeOwned>(
    input: &str,
) -> Result<T, serde_json::Error> {
    let normalized = serde_fastjson::normalize_json_input(input);
    let quoted_keys = quote_unquoted_numeric_object_keys(&normalized);
    serde_json::from_str(&quoted_keys)
}

fn parse_java_route_json_nullable<T: DeserializeOwned>(
    input: Option<&str>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let normalized = serde_fastjson::normalize_json_input(input);
    let quoted_keys = quote_unquoted_numeric_object_keys(&normalized);
    serde_json::from_str(&quoted_keys)
}

fn decode_java_route_json_nullable<T: DeserializeOwned>(
    data: Option<&[u8]>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(data) = data else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(data);
    parse_java_route_json_nullable(Some(&input))
}

fn decode_java_route_json_list_nullable<T: DeserializeOwned>(
    data: Option<&[u8]>,
) -> Result<Option<Vec<Option<T>>>, serde_json::Error> {
    let Some(data) = data else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(data);
    let normalized = serde_fastjson::normalize_json_input(&input);
    let quoted_keys = quote_unquoted_numeric_object_keys(&normalized);
    serde_json::from_str(&quoted_keys)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueueData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub broker_name: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub read_queue_nums: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub write_queue_nums: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub perm: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub topic_sys_flag: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueDataFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    broker_name: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    read_queue_nums: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    write_queue_nums: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    perm: i32,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    topic_sys_flag: i32,
}

impl<'de> Deserialize<'de> for QueueData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueueDataFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            broker_name: fields.broker_name,
            read_queue_nums: fields.read_queue_nums,
            write_queue_nums: fields.write_queue_nums,
            perm: fields.perm,
            topic_sys_flag: fields.topic_sys_flag,
        })
    }
}

/// Failure cases from the pinned Java `QueueData.compareTo` method.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum QueueDataCompareError {
    #[error("Java NullPointerException: null QueueData compareTo argument")]
    NullOther,
    #[error("QueueData broker name is null")]
    NullBrokerName,
}

impl QueueData {
    /// Returns the pinned Java 5.5.1 `QueueData.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = 1i32;
        result = result.wrapping_mul(31).wrapping_add(
            self.broker_name
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        result = result.wrapping_mul(31).wrapping_add(self.perm);
        result = result.wrapping_mul(31).wrapping_add(self.read_queue_nums);
        result = result.wrapping_mul(31).wrapping_add(self.write_queue_nums);
        result.wrapping_mul(31).wrapping_add(self.topic_sys_flag)
    }

    /// Returns the pinned Java 5.5.1 `QueueData.compareTo` result.
    ///
    /// Java compares broker names as UTF-16 strings and throws
    /// `NullPointerException` when either name is null.
    pub fn java_compare_to(&self, other: &Self) -> Result<i32, QueueDataCompareError> {
        let name = self
            .broker_name
            .as_deref()
            .ok_or(QueueDataCompareError::NullBrokerName)?;
        let other_name = other
            .broker_name
            .as_deref()
            .ok_or(QueueDataCompareError::NullBrokerName)?;
        Ok(java_string_compare_to(name, other_name))
    }

    /// Compares with an optional value and maps Java's null-argument failure.
    pub fn java_compare_to_nullable(
        &self,
        other: Option<&Self>,
    ) -> Result<i32, QueueDataCompareError> {
        other
            .map(|other| self.java_compare_to(other))
            .unwrap_or(Err(QueueDataCompareError::NullOther))
    }

    /// Returns the pinned Java 5.5.1 `QueueData.toString` text.
    pub fn java_to_string(&self) -> String {
        format!(
            "QueueData [brokerName={}, readQueueNums={}, writeQueueNums={}, perm={}, topicSysFlag={}]",
            self.broker_name.as_deref().unwrap_or("null"),
            self.read_queue_nums,
            self.write_queue_nums,
            self.perm,
            self.topic_sys_flag
        )
    }
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub cluster: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub broker_name: Option<String>,
    #[serde(
        serialize_with = "serialize_shared_nullable_long_string_map",
        skip_serializing_if = "Option::is_none"
    )]
    pub broker_addrs: Option<SharedBrokerAddressMap>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub zone_name: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub enable_acting_master: bool,
}

impl Clone for BrokerData {
    fn clone(&self) -> Self {
        Self {
            cluster: self.cluster.clone(),
            broker_name: self.broker_name.clone(),
            broker_addrs: self
                .broker_addrs
                .as_ref()
                .map(|addresses| SharedValue::new(addresses.snapshot())),
            zone_name: self.zone_name.clone(),
            enable_acting_master: self.enable_acting_master,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrokerDataFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    cluster: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    broker_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_optional_long_string_map"
    )]
    broker_addrs: Option<serde_fastjson::NullableLongStringMap>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    zone_name: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    enable_acting_master: bool,
}

impl<'de> Deserialize<'de> for BrokerData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: BrokerDataFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            cluster: fields.cluster,
            broker_name: fields.broker_name,
            broker_addrs: fields.broker_addrs.map(SharedValue::new),
            zone_name: fields.zone_name,
            enable_acting_master: fields.enable_acting_master,
        })
    }
}

impl PartialEq for BrokerData {
    fn eq(&self, other: &Self) -> bool {
        self.broker_name == other.broker_name
            && self.broker_addrs.as_ref().map(SharedValue::snapshot)
                == other.broker_addrs.as_ref().map(SharedValue::snapshot)
    }
}

impl Eq for BrokerData {}

/// Failure cases from the pinned Java `BrokerData.compareTo` method.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BrokerDataCompareError {
    #[error("Java NullPointerException: null BrokerData compareTo argument")]
    NullOther,
    #[error("BrokerData broker name is null")]
    NullBrokerName,
}

/// Java exception branches from `BrokerData.selectBrokerAddr`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BrokerDataSelectAddrError {
    #[error("BrokerData broker address map is null (Java NullPointerException)")]
    NullAddressMap,
    #[error("BrokerData has no address candidates (Java IllegalArgumentException)")]
    NoAddressCandidates,
}

impl BrokerData {
    pub fn get_broker_addrs(&self) -> Option<SharedBrokerAddressMap> {
        self.broker_addrs.clone()
    }

    pub fn set_broker_addrs(&mut self, value: Option<SharedBrokerAddressMap>) {
        self.broker_addrs = value;
    }

    pub fn set_broker_addrs_owned(&mut self, value: Option<serde_fastjson::NullableLongStringMap>) {
        self.broker_addrs = value.map(SharedValue::new);
    }

    /// Selects the master address when present; otherwise randomly selects one
    /// value, matching pinned Java 5.5.1 `BrokerData.selectBrokerAddr`.
    ///
    /// A present null master falls through to random selection. Null selected
    /// values are represented as `Ok(None)`; a null map and an empty map retain
    /// Java's `NullPointerException` and `IllegalArgumentException` outcomes.
    pub fn java_select_broker_addr(&self) -> Result<Option<String>, BrokerDataSelectAddrError> {
        self.java_select_broker_addr_with(|length| rand::random_range(0..length))
    }

    fn java_select_broker_addr_with(
        &self,
        mut select: impl FnMut(usize) -> usize,
    ) -> Result<Option<String>, BrokerDataSelectAddrError> {
        let addresses = self
            .broker_addrs
            .as_ref()
            .ok_or(BrokerDataSelectAddrError::NullAddressMap)?;
        addresses.with(|addresses| {
            if let Some(Some(master)) = addresses.get(&Some(0)) {
                return Ok(Some(master.clone()));
            }
            if addresses.is_empty() {
                return Err(BrokerDataSelectAddrError::NoAddressCandidates);
            }
            let index = select(addresses.len());
            Ok(addresses.values().nth(index).cloned().flatten())
        })
    }

    /// Returns the pinned Java 5.5.1 `BrokerData.hashCode` result.
    ///
    /// Java hashes only the broker address map and broker name, using the
    /// Java `Map`, `Long`, and UTF-16 `String` hash rules.
    pub fn java_hash_code(&self) -> i32 {
        let addresses_hash = self
            .broker_addrs
            .as_ref()
            .map(|addresses| {
                addresses.with(|addresses| {
                    addresses.iter().fold(0i32, |hash, (broker_id, address)| {
                        let address_hash =
                            address.as_deref().map(java_string_hash_code).unwrap_or(0);
                        hash.wrapping_add(
                            java_long_hash_code(broker_id.unwrap_or_default()) ^ address_hash,
                        )
                    })
                })
            })
            .unwrap_or(0);
        let broker_name_hash = self
            .broker_name
            .as_deref()
            .map(java_string_hash_code)
            .unwrap_or(0);
        31i32
            .wrapping_mul(31i32.wrapping_add(addresses_hash))
            .wrapping_add(broker_name_hash)
    }

    /// Returns the pinned Java 5.5.1 `BrokerData.compareTo` result.
    ///
    /// Java compares broker names as UTF-16 strings and returns the raw code
    /// unit difference. Its implementation throws `NullPointerException` if
    /// either broker name is null; this method represents that as an error.
    pub fn java_compare_to(&self, other: &Self) -> Result<i32, BrokerDataCompareError> {
        let name = self
            .broker_name
            .as_deref()
            .ok_or(BrokerDataCompareError::NullBrokerName)?;
        let other_name = other
            .broker_name
            .as_deref()
            .ok_or(BrokerDataCompareError::NullBrokerName)?;
        Ok(java_string_compare_to(name, other_name))
    }

    /// Compares with an optional value and maps Java's null-argument failure.
    pub fn java_compare_to_nullable(
        &self,
        other: Option<&Self>,
    ) -> Result<i32, BrokerDataCompareError> {
        other
            .map(|other| self.java_compare_to(other))
            .unwrap_or(Err(BrokerDataCompareError::NullOther))
    }

    /// Returns the pinned Java 5.5.1 `BrokerData.toString` text.
    ///
    /// Address entries use deterministic Rust key order. Java's `HashMap`
    /// iteration order is unspecified.
    pub fn java_to_string(&self) -> String {
        let broker_addrs = self.broker_addrs.as_ref().map_or_else(
            || "null".to_owned(),
            |addresses| {
                addresses.with(|addresses| {
                    let entries = addresses
                        .iter()
                        .map(|(broker_id, address)| {
                            format!(
                                "{}={}",
                                broker_id.map_or_else(|| "null".to_owned(), |id| id.to_string()),
                                address.as_deref().unwrap_or("null")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{{{entries}}}")
                })
            },
        );
        format!(
            "BrokerData [brokerName={}, brokerAddrs={}, enableActingMaster={}]",
            self.broker_name.as_deref().unwrap_or("null"),
            broker_addrs,
            self.enable_acting_master
        )
    }
}

fn java_long_hash_code(value: i64) -> i32 {
    let bits = value as u64;
    (bits ^ (bits >> 32)) as u32 as i32
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn java_list_hash_code<T>(values: &[T], mut value_hash: impl FnMut(&T) -> i32) -> i32 {
    values.iter().fold(1i32, |hash, value| {
        hash.wrapping_mul(31).wrapping_add(value_hash(value))
    })
}

fn java_string_compare_to(left: &str, right: &str) -> i32 {
    let left_units: Vec<u16> = left.encode_utf16().collect();
    let right_units: Vec<u16> = right.encode_utf16().collect();
    for (&left, &right) in left_units.iter().zip(&right_units) {
        if left != right {
            return i32::from(left) - i32::from(right);
        }
    }
    left_units.len() as i32 - right_units.len() as i32
}

#[cfg(test)]
mod queue_data_method_tests {
    use super::{QueueData, QueueDataCompareError};

    #[test]
    fn queue_data_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["queueDataMethods"].as_array().unwrap() {
            let left: QueueData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: QueueData = serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"]);
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(left.java_to_string(), case["leftToString"]);
            assert_eq!(right.java_to_string(), case["rightToString"]);
            let result = left.java_compare_to(&right);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["result"].as_i64().unwrap() as i32);
            } else {
                assert_eq!(result.unwrap_err(), QueueDataCompareError::NullBrokerName);
            }
        }
        assert_eq!(
            fixture["compareToNullOther"]["queueData"],
            "NullPointerException"
        );
        assert_eq!(
            QueueData::default().java_compare_to_nullable(None),
            Err(QueueDataCompareError::NullOther)
        );
    }
}

#[cfg(test)]
mod broker_data_equality_tests {
    use super::BrokerData;

    #[test]
    fn broker_data_equality_matches_invoked_java_equals() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataEquality"].as_array().unwrap() {
            let left: BrokerData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: BrokerData = serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"], "{}", case["id"]);
        }
    }
}

#[cfg(test)]
mod broker_data_hash_code_tests {
    use super::BrokerData;

    #[test]
    fn broker_data_hash_code_matches_invoked_java_hash_code() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataHashCode"].as_array().unwrap() {
            let broker: BrokerData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(
                broker.java_hash_code(),
                case["hashCode"].as_i64().unwrap() as i32,
                "{}",
                case["id"]
            );
        }
    }
}

#[cfg(test)]
mod broker_data_to_string_tests {
    use super::BrokerData;

    #[test]
    fn broker_data_to_string_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataToString"].as_array().unwrap() {
            let broker: BrokerData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(broker.java_to_string(), case["toString"]);
        }
    }
}

#[cfg(test)]
mod broker_data_select_addr_tests {
    use super::{BrokerData, BrokerDataSelectAddrError};
    use std::collections::BTreeMap;

    #[test]
    fn broker_data_select_addr_matches_invoked_java_outcomes() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["brokerDataSelectAddr"].as_array().unwrap();
        assert_eq!(cases.len(), 10);
        for case in cases.iter().skip(7) {
            assert!(case["input"]["brokerAddrs"]["null"].is_string());
        }
        for case in cases {
            let broker: BrokerData = serde_json::from_value(case["input"].clone()).unwrap();
            if let Some(exception) = case["exception"].as_str() {
                let error = broker.java_select_broker_addr().unwrap_err();
                let expected = match exception {
                    "NullPointerException" => BrokerDataSelectAddrError::NullAddressMap,
                    "IllegalArgumentException" => BrokerDataSelectAddrError::NoAddressCandidates,
                    other => panic!("unexpected Java exception {other}"),
                };
                assert_eq!(error, expected, "{}", case["id"]);
                continue;
            }
            match broker.java_select_broker_addr().unwrap() {
                Some(address) => assert!(
                    case["addresses"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|allowed| allowed.as_str() == Some(address.as_str())),
                    "unexpected random address for {}: {address}",
                    case["id"]
                ),
                None => assert_eq!(case["nullAddress"], true, "{}", case["id"]),
            }
            for index in 0..broker.broker_addrs.as_ref().unwrap().with(BTreeMap::len) {
                match broker.java_select_broker_addr_with(|_| index).unwrap() {
                    Some(address) => assert!(
                        case["addresses"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|allowed| allowed.as_str() == Some(address.as_str())),
                        "unexpected address for {}: {address}",
                        case["id"]
                    ),
                    None => assert_eq!(case["nullAddress"], true, "{}", case["id"]),
                }
            }
        }
    }
}

#[cfg(test)]
mod broker_data_compare_to_tests {
    use super::{BrokerData, BrokerDataCompareError};

    #[test]
    fn broker_data_compare_to_matches_invoked_java_comparable() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["brokerDataCompareTo"].as_array().unwrap() {
            let left: BrokerData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: BrokerData = serde_json::from_value(case["right"].clone()).unwrap();
            let result = left.java_compare_to(&right);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["result"].as_i64().unwrap() as i32);
            } else {
                assert_eq!(result.unwrap_err(), BrokerDataCompareError::NullBrokerName);
            }
        }
        assert_eq!(
            fixture["compareToNullOther"]["brokerData"],
            "NullPointerException"
        );
        assert_eq!(
            BrokerData::default().java_compare_to_nullable(None),
            Err(BrokerDataCompareError::NullOther)
        );
    }
}

#[cfg(test)]
mod nullable_broker_address_tests {
    use super::BrokerData;

    #[test]
    fn null_map_value_is_distinct_from_missing_key_and_null_map() {
        let value = serde_json::json!({"cluster":"c","brokerName":"b","brokerAddrs":{"0":null,"1":"slave:10911"}});
        let broker: BrokerData = serde_json::from_value(value.clone()).unwrap();
        let addresses = broker.broker_addrs.as_ref().unwrap();
        addresses.with(|addresses| {
            assert_eq!(addresses.get(&Some(0)), Some(&None));
            assert!(!addresses.contains_key(&Some(2)));
            assert_eq!(
                addresses.get(&Some(1)).and_then(Option::as_deref),
                Some("slave:10911")
            );
        });
        assert_eq!(
            serde_json::to_value(&broker).unwrap()["brokerAddrs"],
            value["brokerAddrs"]
        );
        let null_map: BrokerData = serde_json::from_value(
            serde_json::json!({"cluster":"c","brokerName":"b","brokerAddrs":null}),
        )
        .unwrap();
        assert!(null_map.broker_addrs.is_none());
    }
}

#[cfg(test)]
mod nullable_nested_route_tests {
    use super::TopicRouteData;

    #[test]
    fn nested_broker_and_queue_names_follow_invoked_java_defaults_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableNestedRouteFields"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("pinned Java accepts this nested route input");
            let queue = route.queue_datas_snapshot().unwrap()[0]
                .as_ref()
                .unwrap()
                .clone();
            let encoded = serde_json::to_value(&route).unwrap();
            assert_eq!(
                encoded["queueDatas"][0].get("brokerName").is_none(),
                case["queueNameNull"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("brokerName").is_none(),
                case["brokerNameNull"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("cluster").is_none(),
                case["clusterNull"]
            );
            assert_eq!(
                encoded["queueDatas"][0].get("brokerName").is_some(),
                case["encodedHasQueueName"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("brokerName").is_some(),
                case["encodedHasBrokerName"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("cluster").is_some(),
                case["encodedHasCluster"]
            );
            assert_eq!(queue.read_queue_nums, case["queueRead"]);
            assert_eq!(queue.write_queue_nums, case["queueWrite"]);
            assert_eq!(queue.perm, case["queuePerm"]);
            assert_eq!(queue.topic_sys_flag, case["queueFlag"]);
            let subscriptions = route.subscribe_queues("topic");
            assert_eq!(
                subscriptions.len(),
                case["subscribeQueueCount"].as_u64().unwrap() as usize
            );
            assert_eq!(
                subscriptions
                    .iter()
                    .any(|queue| queue.broker_name().is_none()),
                case["subscribeQueueBrokerNull"]
            );
            assert_eq!(
                subscriptions.iter().any(|queue| serde_json::to_value(queue)
                    .unwrap()
                    .get("brokerName")
                    .is_none()),
                case["subscribeJsonBrokerOmitted"]
            );
        }
    }
}

#[cfg(test)]
mod nullable_message_queue_tests {
    use rocketmq_classic_common::message::MessageQueue;

    #[test]
    fn nullable_message_queue_fields_match_invoked_java_defaults_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableMessageQueueFields"].as_array().unwrap() {
            let queue: MessageQueue = serde_json::from_value(case["input"].clone())
                .expect("pinned Java decodes this MessageQueue");
            let encoded = serde_json::to_value(&queue).unwrap();
            assert_eq!(encoded.get("topic").is_none(), case["topicNull"]);
            assert_eq!(encoded.get("brokerName").is_none(), case["brokerNull"]);
            assert_eq!(encoded.get("topic").is_some(), case["encodedHasTopic"]);
            assert_eq!(
                encoded.get("brokerName").is_some(),
                case["encodedHasBroker"]
            );
            assert_eq!(queue.queue_id(), case["queueId"].as_i64().unwrap() as i32);
        }
    }

    #[test]
    fn message_queue_compare_to_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["messageQueueCompareTo"].as_array().unwrap() {
            let left: MessageQueue = serde_json::from_value(case["left"].clone()).unwrap();
            let right: MessageQueue = serde_json::from_value(case["right"].clone()).unwrap();
            let result = left.java_compare_to(&right);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["result"].as_i64().unwrap() as i32);
            } else {
                assert!(result.is_err());
            }
        }
        assert_eq!(
            fixture["compareToNullOther"]["messageQueue"],
            "NullPointerException"
        );
        assert_eq!(
            rocketmq_classic_common::message::MessageQueue::new("topic", "broker", 0)
                .java_compare_to_nullable(None),
            Err(rocketmq_classic_common::message::MessageQueueCompareError::NullOther)
        );
    }

    #[test]
    fn message_queue_hash_equals_and_text_match_invoked_java() {
        use rocketmq_classic_common::message::MessageQueue;

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["messageQueueMethods"].as_array().unwrap() {
            let queue: MessageQueue = serde_json::from_value(case["queue"].clone()).unwrap();
            let copy = queue.clone();
            let constructed = MessageQueue::new_nullable(
                queue.topic().map(str::to_owned),
                queue.broker_name().map(str::to_owned),
                queue.queue_id(),
            );
            assert_eq!(constructed, queue, "{} constructor fields", case["case"]);
            if let (Some(topic), Some(broker_name)) = (queue.topic(), queue.broker_name()) {
                assert_eq!(
                    MessageQueue::new(topic, broker_name, queue.queue_id()),
                    queue,
                    "{} non-null constructor fields",
                    case["case"]
                );
            }
            assert_eq!(
                queue.java_hash_code(),
                case["hashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(queue.to_string(), case["toString"].as_str().unwrap());
            assert_eq!(queue == queue, case["equalsSelf"].as_bool().unwrap());
            assert_eq!(queue == copy, case["equalsCopy"].as_bool().unwrap());
            assert!(!case["equalsNull"].as_bool().unwrap());
            assert!(!case["equalsWrongType"].as_bool().unwrap());
            if case["case"] == "defaults" {
                assert_eq!(MessageQueue::default(), queue);
            }

            let mut topic_changed = queue.clone();
            topic_changed.set_topic(Some(match queue.topic() {
                Some(topic) => format!("{topic}x"),
                None => "topic".to_owned(),
            }));
            assert_eq!(
                queue == topic_changed,
                case["equalsTopicChanged"].as_bool().unwrap()
            );

            let mut broker_changed = queue.clone();
            broker_changed.set_broker_name(Some(match queue.broker_name() {
                Some(broker) => format!("{broker}x"),
                None => "broker".to_owned(),
            }));
            assert_eq!(
                queue == broker_changed,
                case["equalsBrokerChanged"].as_bool().unwrap()
            );

            let mut queue_id_changed = queue.clone();
            queue_id_changed.set_queue_id(queue.queue_id().wrapping_add(1));
            assert_eq!(
                queue == queue_id_changed,
                case["equalsQueueIdChanged"].as_bool().unwrap()
            );
        }
    }
}

#[cfg(test)]
mod nullable_route_collection_tests {
    use super::TopicRouteData;

    #[test]
    fn source_explicit_null_route_collections_decode_without_becoming_empty() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableRouteCollections"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("Java decodes explicit null route collections");
            let encoded = serde_json::to_value(route).unwrap();
            for (field, decoded_key, encoded_key) in [
                ("queueDatas", "decodedQueueNull", "encodedHasQueue"),
                ("brokerDatas", "decodedBrokerNull", "encodedHasBroker"),
                ("filterServerTable", "decodedFilterNull", "encodedHasFilter"),
            ] {
                assert_eq!(encoded.get(field).is_none(), case[decoded_key]);
                assert_eq!(encoded.get(field).is_some(), case[encoded_key]);
            }
            assert_eq!(
                encoded.get("topicQueueMappingByBroker").is_some(),
                case["encodedHasMapping"]
            );
        }
    }

    #[test]
    fn java_copy_constructor_normalizes_nullable_route_collections() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableRouteCollections"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let copy = route.java_copy_constructed();
            assert_eq!(
                copy.queue_datas_snapshot().unwrap().is_empty(),
                case["copyQueueEmpty"]
            );
            assert_eq!(
                copy.broker_datas_snapshot().unwrap().is_empty(),
                case["copyBrokerEmpty"]
            );
            assert_eq!(
                copy.filter_server_table_snapshot().unwrap().is_empty(),
                case["copyFilterEmpty"]
            );
            assert_eq!(
                copy.topic_queue_mapping_by_broker.is_none(),
                case["input"]["topicQueueMappingByBroker"].is_null()
            );
        }
    }
}

#[cfg(test)]
mod nullable_static_mapping_tests {
    use super::TopicQueueMappingInfo;
    use std::collections::BTreeMap;

    #[test]
    fn static_mapping_optional_fields_match_invoked_java_decode_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableStaticMappingFields"].as_array().unwrap() {
            let mapping: TopicQueueMappingInfo = serde_json::from_value(case["input"].clone())
                .expect("pinned Java accepts this mapping input");
            let encoded = serde_json::to_value(&mapping).unwrap();
            for (field, null_key, encoded_key) in [
                ("topic", "topicNull", "encodedHasTopic"),
                ("bname", "bnameNull", "encodedHasBname"),
                ("scope", "scopeNull", "encodedHasScope"),
                ("currIdMap", "currIdMapNull", "encodedHasCurrIdMap"),
            ] {
                assert_eq!(encoded.get(field).is_none(), case[null_key]);
                assert_eq!(encoded.get(field).is_some(), case[encoded_key]);
            }
            assert_eq!(
                mapping
                    .curr_id_map
                    .as_ref()
                    .is_some_and(|map| map.with(BTreeMap::is_empty)),
                case["currIdMapEmpty"]
            );
        }
    }

    #[test]
    fn static_mapping_map_entry_inputs_match_invoked_java_acceptance() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["staticMappingMapEntryCases"].as_array().unwrap() {
            let mapping: Result<TopicQueueMappingInfo, _> =
                serde_json::from_str(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "accepted";
            assert_eq!(
                mapping.is_ok(),
                java_accepts,
                "{} ({})",
                case["case"],
                case["error"]
            );
            if let Ok(mapping) = mapping {
                assert_eq!(
                    mapping
                        .curr_id_map
                        .as_ref()
                        .map(|values| values.with(BTreeMap::len)),
                    case["mapSize"].as_u64().map(|size| size as usize)
                );
                assert_eq!(
                    mapping
                        .curr_id_map
                        .as_ref()
                        .is_some_and(|values| values.with(|values| values.contains_key(&0))),
                    case["hasKeyZero"]
                );
                assert_eq!(
                    mapping
                        .curr_id_map
                        .as_ref()
                        .and_then(|values| { values.with(|values| values.get(&0).copied()) }),
                    case["keyZeroValue"].as_i64().map(|value| value as i32)
                );
            }
        }
    }
}

#[cfg(test)]
mod overlapping_static_mapping_tests {
    use super::TopicRouteData;

    #[test]
    fn overlapping_static_owners_follow_invoked_java_epochs_and_fallback() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["overlappingStaticMappingOwners"]
            .as_array()
            .unwrap()
        {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let endpoints = route.static_queue_endpoints("topic").unwrap();
            let rows: Vec<_> = endpoints
                .iter()
                .map(|(queue, owner)| {
                    serde_json::json!({
                        "queueId": queue.queue_id(),
                        "brokerName": queue.broker_name(),
                        "owner": owner,
                    })
                })
                .collect();
            assert_eq!(serde_json::json!(rows), case["endpoints"]);
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueMappingInfo {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub topic: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub total_queues: i32,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub bname: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    pub epoch: i64,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub dirty: bool,
    #[serde(
        default = "default_mapping_scope",
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub curr_id_map: Option<SharedTopicQueueMappingIds>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TopicQueueMappingInfoFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
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
        default = "default_mapping_scope",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    scope: Option<String>,
    #[serde(
        default = "default_mapping_ids",
        deserialize_with = "serde_fastjson::deserialize_optional_int_map"
    )]
    curr_id_map: Option<BTreeMap<i32, i32>>,
}

impl<'de> Deserialize<'de> for TopicQueueMappingInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = serde_fastjson::deserialize_fastjson_last_wins_object::<
            _,
            TopicQueueMappingInfoFields,
        >(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            total_queues: fields.total_queues,
            bname: fields.bname,
            epoch: fields.epoch,
            dirty: fields.dirty,
            scope: fields.scope,
            curr_id_map: fields.curr_id_map.map(SharedValue::new),
        })
    }
}

impl Clone for TopicQueueMappingInfo {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            total_queues: self.total_queues,
            bname: self.bname.clone(),
            epoch: self.epoch,
            dirty: self.dirty,
            scope: self.scope.clone(),
            curr_id_map: self
                .curr_id_map
                .as_ref()
                .map(|ids| SharedValue::new(ids.snapshot())),
        }
    }
}

impl PartialEq for TopicQueueMappingInfo {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.total_queues == other.total_queues
            && self.bname == other.bname
            && self.epoch == other.epoch
            && self.dirty == other.dirty
            && self.scope == other.scope
            && self.curr_id_map.as_ref().map(SharedValue::snapshot)
                == other.curr_id_map.as_ref().map(SharedValue::snapshot)
    }
}

impl Eq for TopicQueueMappingInfo {}

fn default_mapping_ids() -> Option<BTreeMap<i32, i32>> {
    Some(BTreeMap::new())
}

impl Default for TopicQueueMappingInfo {
    fn default() -> Self {
        Self {
            topic: None,
            total_queues: 0,
            bname: None,
            epoch: 0,
            dirty: false,
            scope: default_mapping_scope(),
            curr_id_map: Some(SharedValue::new(BTreeMap::new())),
        }
    }
}

impl TopicQueueMappingInfo {
    /// Pinned Java `TopicQueueMappingInfo.LEVEL_0` static-topic generation.
    pub const LEVEL_0: i32 = 0;

    /// Constructs a static-topic mapping using Java's constructor defaults.
    pub fn new(
        topic: Option<String>,
        total_queues: i32,
        bname: Option<String>,
        epoch: i64,
    ) -> Self {
        Self {
            topic,
            total_queues,
            bname,
            epoch,
            dirty: false,
            scope: default_mapping_scope(),
            curr_id_map: default_mapping_ids().map(SharedValue::new),
        }
    }

    pub fn get_curr_id_map(&self) -> Option<SharedTopicQueueMappingIds> {
        self.curr_id_map.clone()
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_total_queues(&self) -> i32 {
        self.total_queues
    }

    pub fn set_total_queues(&mut self, value: i32) {
        self.total_queues = value;
    }

    pub fn get_bname(&self) -> Option<&str> {
        self.bname.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.bname = value;
    }

    pub fn get_epoch(&self) -> i64 {
        self.epoch
    }

    pub fn set_epoch(&mut self, value: i64) {
        self.epoch = value;
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn set_dirty(&mut self, value: bool) {
        self.dirty = value;
    }

    pub fn get_scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    pub fn set_scope(&mut self, value: Option<String>) {
        self.scope = value;
    }

    pub fn set_curr_id_map(&mut self, value: Option<SharedTopicQueueMappingIds>) {
        self.curr_id_map = value;
    }

    pub fn set_curr_id_map_owned(&mut self, value: Option<BTreeMap<i32, i32>>) {
        self.curr_id_map = value.map(SharedValue::new);
    }

    /// Encodes the inherited Java `RemotingSerializable.toJson(boolean)` form.
    /// Bean fields are key-sorted and integer map keys remain unquoted as in Fastjson2.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        java_fastjson_json(self, pretty_format)
    }

    /// Encodes the inherited Java `RemotingSerializable.encode()` UTF-8 bytes.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Encodes with the supported inherited Fastjson2 writer features.
    pub fn java_encode_with_features(
        &self,
        features: &[JavaJsonWriterFeature],
    ) -> Result<Vec<u8>, serde_json::Error> {
        Ok(java_fastjson_json_with_features(self, false, features)?.into_bytes())
    }

    /// Decodes the inherited Java `RemotingSerializable.decode` form, including
    /// unquoted integer keys emitted for `currIdMap` by Fastjson2.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java utility form, including null bytes
    /// and JSON `null` as a null result.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        decode_java_route_json_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.decodeList` form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java list utility, preserving null list
    /// results and null members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        decode_java_route_json_list_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.fromJson` form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        parse_java_route_json(json)
    }

    /// Decodes the inherited nullable Java `fromJson` utility form.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        parse_java_route_json_nullable(json)
    }

    /// Returns the pinned Java 5.5.1 `TopicQueueMappingInfo.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = self
            .topic
            .as_deref()
            .map(java_string_hash_code)
            .unwrap_or(0);
        result = result.wrapping_mul(31).wrapping_add(
            self.scope
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        result = result.wrapping_mul(31).wrapping_add(self.total_queues);
        result = result.wrapping_mul(31).wrapping_add(
            self.bname
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        result = result
            .wrapping_mul(31)
            .wrapping_add(java_long_hash_code(self.epoch));
        result = result.wrapping_mul(31).wrapping_add(i32::from(self.dirty));
        result.wrapping_mul(31).wrapping_add(
            self.curr_id_map
                .as_ref()
                .map(|ids| {
                    ids.with(|ids| {
                        ids.iter().fold(0i32, |hash, (logical, physical)| {
                            hash.wrapping_add(*logical ^ *physical)
                        })
                    })
                })
                .unwrap_or(0),
        )
    }

    /// Returns the pinned Java 5.5.1 `TopicQueueMappingInfo.toString` text.
    ///
    /// The ID map is emitted in its deterministic Rust key order. Java stores
    /// this map in a `ConcurrentHashMap`, whose iteration order is unspecified.
    pub fn java_to_string(&self) -> String {
        let curr_id_map = self.curr_id_map.as_ref().map_or_else(
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
            "TopicQueueMappingInfo{{topic='{}', scope='{}', totalQueues={}, bname='{}', epoch={}, dirty={}, currIdMap={}}}",
            self.topic.as_deref().unwrap_or("null"),
            self.scope.as_deref().unwrap_or("null"),
            self.total_queues,
            self.bname.as_deref().unwrap_or("null"),
            self.epoch,
            self.dirty,
            curr_id_map
        )
    }
}

#[cfg(test)]
mod topic_queue_mapping_method_tests {
    use super::TopicQueueMappingInfo;

    #[test]
    fn topic_queue_mapping_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let constants = &fixture["topicQueueMappingConstants"];
        assert_eq!(TopicQueueMappingInfo::LEVEL_0, constants["infoLevel0"]);
        assert_eq!(
            crate::body::TopicQueueMappingDetail::LEVEL_0,
            constants["detailLevel0"]
        );
        for case in fixture["topicQueueMappingMethods"].as_array().unwrap() {
            let left: TopicQueueMappingInfo = serde_json::from_value(case["left"].clone()).unwrap();
            let right: TopicQueueMappingInfo =
                serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"]);
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(left.java_to_string(), case["leftToString"]);
            assert_eq!(right.java_to_string(), case["rightToString"]);
        }
    }
}

#[cfg(test)]
mod topic_route_data_method_tests {
    use super::TopicRouteData;

    #[test]
    fn topic_route_data_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataMethods"].as_array().unwrap() {
            let left: TopicRouteData = serde_json::from_value(case["left"].clone()).unwrap();
            let right: TopicRouteData = serde_json::from_value(case["right"].clone()).unwrap();
            assert_eq!(left == right, case["equals"]);
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32
            );
        }
    }

    #[test]
    fn topic_route_data_nullable_nested_values_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let method_case = &fixture["topicRouteDataMethods"][4];
        let left: TopicRouteData = serde_json::from_value(method_case["left"].clone())
            .expect("pinned Java accepts null nested route values");
        let right: TopicRouteData = serde_json::from_value(method_case["right"].clone())
            .expect("pinned Java accepts null nested route values");
        assert_eq!(left == right, method_case["equals"]);
        assert_eq!(
            left.java_hash_code(),
            method_case["leftHashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(
            right.java_hash_code(),
            method_case["rightHashCode"].as_i64().unwrap() as i32
        );
        let string_case = fixture["topicRouteDataToString"]
            .as_array()
            .unwrap()
            .last()
            .unwrap();
        let route: TopicRouteData = serde_json::from_value(string_case["input"].clone())
            .expect("pinned Java accepts null nested route values");
        assert_eq!(route.java_to_string(), string_case["toString"]);
    }
}

#[cfg(test)]
mod topic_route_data_to_string_tests {
    use super::TopicRouteData;

    #[test]
    fn topic_route_data_to_string_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataToString"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(route.java_to_string(), case["toString"]);
        }
    }
}

#[cfg(test)]
mod topic_route_data_changed_method_tests {
    use super::TopicRouteData;

    #[test]
    fn topic_route_data_changed_matches_invoked_java_comparison_and_sort_errors() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataChanged"].as_array().unwrap() {
            let current: TopicRouteData = serde_json::from_value(case["current"].clone()).unwrap();
            let old: Option<TopicRouteData> = if case["old"].is_null() {
                None
            } else {
                Some(serde_json::from_value(case["old"].clone()).unwrap())
            };
            let result = current.java_topic_route_data_changed(old.as_ref());
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["changed"]);
            } else {
                assert!(result.is_err());
            }
        }
    }
}

#[cfg(test)]
mod topic_route_data_clone_method_tests {
    use super::{SharedFilterServerList, SharedTopicQueueMappingInfoHandle, TopicRouteData};
    use serde_json::{Value, json};

    fn first_queue_alias(source: &TopicRouteData, copy: &TopicRouteData) -> bool {
        let (Some(source), Some(copy)) = (&source.queue_datas, &copy.queue_datas) else {
            return false;
        };
        source.with(|source| {
            copy.with(|copy| match (source.first(), copy.first()) {
                (Some(None), Some(None)) => true,
                (Some(Some(source)), Some(Some(copy))) => source.ptr_eq(copy),
                _ => false,
            })
        })
    }

    fn first_broker_alias(source: &TopicRouteData, copy: &TopicRouteData) -> bool {
        let (Some(source), Some(copy)) = (&source.broker_datas, &copy.broker_datas) else {
            return false;
        };
        source.with(|source| {
            copy.with(|copy| match (source.first(), copy.first()) {
                (Some(None), Some(None)) => true,
                (Some(Some(source)), Some(Some(copy))) => source.ptr_eq(copy),
                _ => false,
            })
        })
    }

    fn broker_filter_list(route: &TopicRouteData) -> Option<SharedFilterServerList> {
        route
            .filter_server_table
            .as_ref()?
            .with(|filters| filters.get("broker").and_then(Clone::clone))
    }

    fn broker_mapping(route: &TopicRouteData) -> Option<SharedTopicQueueMappingInfoHandle> {
        route
            .topic_queue_mapping_by_broker
            .as_ref()?
            .with(|mappings| mappings.get("broker").and_then(Clone::clone))
    }

    fn collection_aliases(source: &TopicRouteData, copy: Option<&TopicRouteData>) -> Value {
        let Some(copy) = copy else {
            return json!({
                "queueList": false,
                "queueElement": false,
                "brokerList": false,
                "brokerElement": false,
                "filterTable": false,
                "filterList": false,
                "mappingTable": false,
                "mappingValue": false,
            });
        };
        json!({
            "queueList": source.queue_datas.as_ref().zip(copy.queue_datas.as_ref())
                .is_some_and(|(left, right)| left.ptr_eq(right)),
            "queueElement": first_queue_alias(source, copy),
            "brokerList": source.broker_datas.as_ref().zip(copy.broker_datas.as_ref())
                .is_some_and(|(left, right)| left.ptr_eq(right)),
            "brokerElement": first_broker_alias(source, copy),
            "filterTable": source.filter_server_table.as_ref().zip(copy.filter_server_table.as_ref())
                .is_some_and(|(left, right)| left.ptr_eq(right)),
            "filterList": broker_filter_list(source).zip(broker_filter_list(copy))
                .is_some_and(|(left, right)| left.ptr_eq(&right)),
            "mappingTable": source.topic_queue_mapping_by_broker.as_ref()
                .zip(copy.topic_queue_mapping_by_broker.as_ref())
                .is_some_and(|(left, right)| left.ptr_eq(right)),
            "mappingValue": broker_mapping(source).zip(broker_mapping(copy))
                .is_some_and(|(left, right)| left.ptr_eq(&right)),
        })
    }

    #[test]
    fn topic_route_data_copy_and_clone_methods_match_invoked_java_values_and_failures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataClone"].as_array().unwrap() {
            let source: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let expected_copy: TopicRouteData =
                serde_json::from_value(case["copyConstructor"].clone()).unwrap();
            assert_eq!(
                source.java_copy_constructed(),
                expected_copy,
                "{}",
                case["id"]
            );
            let copy = source.java_copy_constructed();
            assert_eq!(
                collection_aliases(&source, Some(&copy)),
                case["copyCollectionAliases"],
                "{} copy aliases",
                case["id"]
            );

            let clone = source.clone_topic_route_data();
            assert_eq!(
                clone.is_ok(),
                case["cloneOutcome"] == "OK",
                "{}",
                case["id"]
            );
            if let Ok(clone) = clone {
                let expected: TopicRouteData =
                    serde_json::from_value(case["clone"].clone()).unwrap();
                assert_eq!(clone, expected, "{}", case["id"]);
                assert_eq!(
                    collection_aliases(&source, Some(&clone)),
                    case["cloneCollectionAliases"],
                    "{} clone aliases",
                    case["id"]
                );
            } else {
                assert_eq!(
                    collection_aliases(&source, None),
                    case["cloneCollectionAliases"],
                    "{} clone aliases",
                    case["id"]
                );
            }

            let deep_clone = source.deep_clone_topic_route_data();
            assert_eq!(
                deep_clone.is_ok(),
                case["deepCloneOutcome"] == "OK",
                "{}",
                case["id"]
            );
            if let Ok(deep_clone) = deep_clone {
                let expected: TopicRouteData =
                    serde_json::from_value(case["deepClone"].clone()).unwrap();
                assert_eq!(deep_clone, expected, "{}", case["id"]);
                assert_eq!(
                    collection_aliases(&source, Some(&deep_clone)),
                    case["deepCloneCollectionAliases"],
                    "{} deep clone aliases",
                    case["id"]
                );
            } else {
                assert_eq!(
                    collection_aliases(&source, None),
                    case["deepCloneCollectionAliases"],
                    "{} deep clone aliases",
                    case["id"]
                );
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub order_topic_conf: Option<String>,
    /// Preserves both a null Java list and null QueueData entries.
    #[serde(
        default = "default_route_vec",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single",
        skip_serializing_if = "Option::is_none"
    )]
    pub queue_datas: Option<SharedRouteQueueDataList>,
    /// Preserves both a null Java list and null BrokerData entries.
    #[serde(
        default = "default_route_vec",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single",
        skip_serializing_if = "Option::is_none"
    )]
    pub broker_datas: Option<SharedRouteBrokerDataList>,
    /// Preserves null filter lists and null String entries inside those lists.
    #[serde(
        default = "default_filter_table",
        deserialize_with = "serde_fastjson::deserialize_optional_string_list_map",
        skip_serializing_if = "Option::is_none"
    )]
    pub filter_server_table: Option<SharedFilterServerTable>,
    /// Preserves null mapping values in the Java route map.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_queue_mapping_by_broker: Option<SharedTopicQueueMappingTable>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TopicRouteDataFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    order_topic_conf: Option<String>,
    #[serde(
        default = "default_route_vec",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single"
    )]
    queue_datas: Option<Vec<Option<QueueData>>>,
    #[serde(
        default = "default_route_vec",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single"
    )]
    broker_datas: Option<Vec<Option<BrokerData>>>,
    #[serde(
        default = "default_filter_table",
        deserialize_with = "serde_fastjson::deserialize_optional_string_list_map"
    )]
    filter_server_table: Option<BTreeMap<String, Option<Vec<Option<String>>>>>,
    topic_queue_mapping_by_broker: Option<BTreeMap<String, Option<TopicQueueMappingInfo>>>,
}

impl<'de> Deserialize<'de> for TopicRouteData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = serde_fastjson::deserialize_fastjson_last_wins_object::<
            _,
            TopicRouteDataFields,
        >(deserializer)?;
        Ok(Self {
            order_topic_conf: fields.order_topic_conf,
            queue_datas: fields.queue_datas.map(|queues| {
                SharedValue::new(
                    queues
                        .into_iter()
                        .map(|queue| queue.map(SharedValue::new))
                        .collect(),
                )
            }),
            broker_datas: fields.broker_datas.map(|brokers| {
                SharedValue::new(
                    brokers
                        .into_iter()
                        .map(|broker| broker.map(SharedValue::new))
                        .collect(),
                )
            }),
            filter_server_table: fields.filter_server_table.map(|filters| {
                SharedValue::new(
                    filters
                        .into_iter()
                        .map(|(broker, servers)| (broker, servers.map(SharedValue::new)))
                        .collect(),
                )
            }),
            topic_queue_mapping_by_broker: fields.topic_queue_mapping_by_broker.map(|mappings| {
                SharedValue::new(
                    mappings
                        .into_iter()
                        .map(|(broker, mapping)| (broker, mapping.map(SharedValue::new)))
                        .collect(),
                )
            }),
        })
    }
}

#[cfg(test)]
mod route_dto_input_cases_tests {
    use super::{
        BrokerData, QueueData, TopicQueueMappingInfo, TopicRouteData, parse_java_route_json,
    };
    use rocketmq_classic_common::message::MessageQueue;

    fn null_omission_equivalent(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
        match (actual, expected) {
            (serde_json::Value::Object(actual), serde_json::Value::Object(expected)) => {
                actual.iter().all(|(key, value)| match expected.get(key) {
                    Some(expected) => null_omission_equivalent(value, expected),
                    None => value.is_null(),
                }) && expected.iter().all(|(key, value)| match actual.get(key) {
                    Some(actual) => null_omission_equivalent(actual, value),
                    None => value.is_null(),
                })
            }
            (serde_json::Value::Array(actual), serde_json::Value::Array(expected)) => {
                actual.len() == expected.len()
                    && actual
                        .iter()
                        .zip(expected)
                        .all(|(actual, expected)| null_omission_equivalent(actual, expected))
            }
            _ => actual == expected,
        }
    }

    #[test]
    fn route_dto_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["routeDtoInputCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            527,
            "Java oracle includes root shapes, exact coercions, duplicate fields, collection shapes, per-field token kinds, and raw C0 string probes"
        );
        for dto in [
            "queueData",
            "messageQueue",
            "brokerData",
            "mapping",
            "topicRouteData",
        ] {
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
            "queue-duplicate-broker-name-last-wins",
            "queue-duplicate-read-queue-nums-last-wins",
            "queue-duplicate-write-queue-nums-last-wins",
            "queue-duplicate-perm-last-wins",
            "queue-duplicate-topic-sys-flag-last-wins",
            "broker-duplicate-cluster-last-wins",
            "broker-duplicate-name-last-wins",
            "broker-duplicate-addresses-last-wins",
            "broker-duplicate-zone-last-wins",
            "broker-duplicate-acting-master-last-wins",
            "mapping-duplicate-topic-last-wins",
            "mapping-duplicate-total-queues-last-wins",
            "mapping-duplicate-bname-last-wins",
            "mapping-duplicate-epoch-last-wins",
            "mapping-duplicate-dirty-last-wins",
            "mapping-duplicate-scope-last-wins",
            "mapping-duplicate-curr-id-map-last-wins",
            "topic-route-duplicate-order-topic-last-wins",
            "topic-route-duplicate-queue-datas-last-wins",
            "topic-route-duplicate-broker-datas-last-wins",
            "topic-route-duplicate-filter-server-table-last-wins",
            "topic-route-duplicate-mapping-table-last-wins",
            "route-queue-datas-single-object",
            "route-broker-datas-single-object",
            "route-queue-datas-null-element",
            "route-broker-datas-null-element",
            "queue-uppercase-property",
            "queue-titlecase-property",
            "queue-snakecase-property",
            "queue-case-collision",
            "broker-uppercase-property",
            "mapping-uppercase-property",
            "topic-route-uppercase-property",
            "broker-boolean-high-precision-decimal-one",
            "broker-address-high-precision-number-strings",
            "broker-address-unquoted-integer-keys",
            "broker-address-unquoted-leading-zero-key",
            "broker-address-unquoted-plus-key",
            "broker-address-unquoted-fraction-key",
            "broker-address-unquoted-negative-fraction-key",
            "broker-address-unquoted-exponent-key",
            "filter-server-high-precision-number-strings",
            "queue-unquoted-fraction-property-key",
            "queue-unquoted-integer-property-key",
            "queue-unquoted-plus-property-key",
            "queue-unknown-nested-unquoted-numeric-key",
            "broker-address-map-value-null",
            "broker-address-map-value-string",
            "broker-address-map-value-integer",
            "broker-address-map-value-fraction",
            "broker-address-map-value-boolean",
            "broker-address-map-value-array",
            "broker-address-map-value-object",
            "broker-address-object-value-unquoted-numeric-key",
            "filter-server-list-member-null",
            "filter-server-list-member-string",
            "filter-server-list-member-integer",
            "filter-server-list-member-fraction",
            "filter-server-list-member-boolean",
            "filter-server-list-member-array",
            "filter-server-list-member-object",
            "mapping-curr-id-map-value-null",
            "mapping-curr-id-map-value-string",
            "mapping-curr-id-map-value-integer",
            "mapping-curr-id-map-value-fraction",
            "mapping-curr-id-map-value-boolean",
            "mapping-curr-id-map-value-array",
            "mapping-curr-id-map-value-object",
            "route-queue-data-element-null",
            "route-queue-data-element-string",
            "route-queue-data-element-integer",
            "route-queue-data-element-fraction",
            "route-queue-data-element-boolean",
            "route-queue-data-element-array",
            "route-queue-data-element-object",
            "route-broker-data-element-null",
            "route-broker-data-element-string",
            "route-broker-data-element-integer",
            "route-broker-data-element-fraction",
            "route-broker-data-element-boolean",
            "route-broker-data-element-array",
            "route-broker-data-element-object",
            "route-filter-server-map-value-null",
            "route-filter-server-map-value-string",
            "route-filter-server-map-value-integer",
            "route-filter-server-map-value-fraction",
            "route-filter-server-map-value-boolean",
            "route-filter-server-map-value-array",
            "route-filter-server-map-value-object",
            "route-filter-server-map-value-empty-string",
            "route-filter-server-unquoted-fraction-key",
            "route-filter-server-unquoted-plus-key",
            "route-filter-server-unquoted-exponent-key",
            "route-mapping-table-unquoted-leading-zero-key",
            "route-mapping-table-unquoted-plus-key",
            "route-mapping-table-unquoted-exponent-key",
            "route-mapping-table-map-value-null",
            "route-mapping-table-map-value-string",
            "route-mapping-table-map-value-integer",
            "route-mapping-table-map-value-fraction",
            "route-mapping-table-map-value-boolean",
            "route-mapping-table-map-value-array",
            "route-mapping-table-map-value-object",
            "queue-unicode-decimal-string",
            "mapping-unicode-decimal-strings",
            "message-queue-unicode-decimal-id",
            "message-queue-id-plus-string",
            "message-queue-id-leading-zero-string",
            "message-queue-id-fraction-string",
            "message-queue-id-exponent-string",
            "message-queue-id-whitespace-string",
            "message-queue-id-string-overflow",
            "message-queue-id-string-underflow",
            "broker-address-key-plus-leading-zero",
            "broker-address-key-fraction-string",
            "broker-address-key-exponent-string",
            "broker-address-key-whitespace-string",
            "broker-address-key-long-maximum",
            "broker-address-key-long-overflow",
            "broker-address-key-normalization-collision-forward",
            "broker-address-key-normalization-collision-reverse",
            "broker-address-key-normalization-collision-repeat-first-key-last-wins",
            "broker-address-key-normalization-collision-repeat-alias-last-wins",
            "broker-address-unquoted-null-key",
            "broker-address-quoted-null-key",
            "mapping-id-key-plus-leading-zero",
            "mapping-id-key-fraction-string",
            "mapping-id-key-exponent-string",
            "mapping-id-key-whitespace-string",
            "mapping-id-key-int-maximum",
            "mapping-id-key-int-overflow",
            "mapping-id-key-normalization-collision-forward",
            "mapping-id-key-normalization-collision-reverse",
            "mapping-id-key-normalization-collision-repeat-first-key-last-wins",
            "mapping-id-key-normalization-collision-repeat-alias-last-wins",
            "mapping-id-unquoted-null-key",
            "mapping-id-quoted-null-key",
            "mapping-long-plus-maximum-string",
            "mapping-long-leading-zero-string",
            "mapping-long-fraction-string",
            "mapping-long-maximum-decimal-string",
            "mapping-long-minimum-decimal-string",
            "mapping-long-overflow-decimal-string",
            "mapping-long-underflow-decimal-string",
            "mapping-long-maximum-decimal-number",
            "mapping-long-overflow-decimal-number",
            "mapping-long-underflow-decimal-number",
            "mapping-curr-id-map-high-precision-decimals",
            "mapping-curr-id-map-unquoted-integer-keys",
            "mapping-curr-id-map-unquoted-leading-zero-key",
            "mapping-curr-id-map-unquoted-plus-key",
            "mapping-curr-id-map-unquoted-fraction-key",
            "mapping-curr-id-map-unquoted-negative-fraction-key",
            "mapping-curr-id-map-unquoted-exponent-key",
            "broker-unicode-decimal-address-key",
            "mapping-unicode-decimal-id-key",
            "queue-fastjson-leading-plus-number",
            "queue-fastjson-leading-zero-number",
            "queue-fastjson-leading-plus-fraction",
            "queue-fastjson-leading-zero-fraction",
            "mapping-fastjson-leading-plus-exponent",
            "mapping-fastjson-leading-zero-exponent",
            "queue-fastjson-vertical-tab-prefix",
            "queue-fastjson-form-feed-prefix",
            "queue-fastjson-form-feed-before-number",
            "queue-fastjson-form-feed-around-punctuation",
            "queue-fastjson-vertical-tab-before-number",
            "queue-fastjson-nbsp-prefix",
            "queue-fastjson-bom-prefix",
            "queue-fastjson-bom-before-number",
            "shape-mapping-currIdMap-array",
            "shape-topicRouteData-queueDatas-object",
            "shape-brokerData-enableActingMaster-fraction",
            "route-raw-c0-0000-order-topic-value",
            "route-raw-c0-000c-field-name",
            "route-raw-c0-001f-filter-key",
            "route-raw-c0-001f-filter-value",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        for code_unit in 0..=0x1f {
            let hex = format!("{code_unit:04x}");
            for position in [
                "order-topic-value",
                "field-name",
                "filter-key",
                "filter-value",
            ] {
                let id = format!("route-raw-c0-{hex}-{position}");
                assert!(
                    cases.iter().any(|case| case["id"] == id),
                    "missing raw-control probe {id}"
                );
            }
        }
        for id in [
            "broker-address-unquoted-null-key",
            "broker-address-quoted-null-key",
        ] {
            let case = cases.iter().find(|case| case["id"] == id).unwrap();
            assert_eq!(case["brokerAddrsNullKeyPresent"], true, "{id}");
            assert_eq!(case["brokerAddrsStringNullKeyPresent"], false, "{id}");
            let broker: BrokerData = parse_java_route_json(case["rawInput"].as_str().unwrap())
                .expect("pinned Java accepts nullable Long map keys");
            let addresses = broker.broker_addrs.unwrap();
            addresses.with(|addresses| {
                assert!(addresses.contains_key(&None), "{id}");
                assert_eq!(addresses.len(), 1, "{id}");
            });
        }
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded =
                match case["type"].as_str().unwrap() {
                    "queueData" => {
                        parse_java_route_json::<QueueData>(raw_input).and_then(serde_json::to_value)
                    }
                    "messageQueue" => parse_java_route_json::<MessageQueue>(raw_input)
                        .and_then(serde_json::to_value),
                    "brokerData" => parse_java_route_json::<BrokerData>(raw_input)
                        .and_then(serde_json::to_value),
                    "mapping" => parse_java_route_json::<TopicQueueMappingInfo>(raw_input)
                        .and_then(serde_json::to_value),
                    "topicRouteData" => parse_java_route_json::<TopicRouteData>(raw_input)
                        .and_then(serde_json::to_value),
                    other => panic!("unknown DTO case type {other}"),
                };
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(decoded.is_ok(), java_accepts, "{}: {decoded:?}", case["id"]);
            if let Ok(decoded) = decoded {
                for (field, expected) in case["fields"].as_object().unwrap() {
                    let actual = decoded.get(field);
                    if expected.is_null() {
                        assert!(
                            actual.is_none_or(serde_json::Value::is_null),
                            "{}.{field}: expected null/omitted, got {actual:?}",
                            case["id"]
                        );
                    } else {
                        assert!(
                            actual.is_some_and(|actual| null_omission_equivalent(actual, expected)),
                            "{}.{field}: actual {actual:?}, expected {expected:?}",
                            case["id"]
                        );
                    }
                }
            }
        }
    }
}

fn default_route_vec<T>() -> Option<Vec<T>> {
    Some(Vec::new())
}

fn default_filter_table() -> Option<BTreeMap<String, Option<Vec<Option<String>>>>> {
    Some(BTreeMap::new())
}

impl Default for TopicRouteData {
    fn default() -> Self {
        Self {
            order_topic_conf: None,
            queue_datas: Some(SharedValue::new(Vec::new())),
            broker_datas: Some(SharedValue::new(Vec::new())),
            filter_server_table: Some(SharedValue::new(BTreeMap::new())),
            topic_queue_mapping_by_broker: None,
        }
    }
}

impl Clone for TopicRouteData {
    fn clone(&self) -> Self {
        Self {
            order_topic_conf: self.order_topic_conf.clone(),
            queue_datas: self.queue_datas_snapshot().map(|queues| {
                SharedValue::new(
                    queues
                        .into_iter()
                        .map(|queue| queue.map(SharedValue::new))
                        .collect(),
                )
            }),
            broker_datas: self.broker_datas_snapshot().map(|brokers| {
                SharedValue::new(
                    brokers
                        .into_iter()
                        .map(|broker| broker.map(SharedValue::new))
                        .collect(),
                )
            }),
            filter_server_table: self.filter_server_table_snapshot().map(|filters| {
                SharedValue::new(
                    filters
                        .into_iter()
                        .map(|(broker, servers)| (broker, servers.map(SharedValue::new)))
                        .collect(),
                )
            }),
            topic_queue_mapping_by_broker: self.topic_queue_mapping_by_broker_snapshot().map(
                |mappings| {
                    SharedValue::new(
                        mappings
                            .into_iter()
                            .map(|(broker, mapping)| (broker, mapping.map(SharedValue::new)))
                            .collect(),
                    )
                },
            ),
        }
    }
}

impl PartialEq for TopicRouteData {
    fn eq(&self, other: &Self) -> bool {
        self.order_topic_conf == other.order_topic_conf
            && self.queue_datas_snapshot() == other.queue_datas_snapshot()
            && self.broker_datas_snapshot() == other.broker_datas_snapshot()
            && self.filter_server_table_snapshot() == other.filter_server_table_snapshot()
            && self.topic_queue_mapping_by_broker_snapshot()
                == other.topic_queue_mapping_by_broker_snapshot()
    }
}

impl Eq for TopicRouteData {}

/// Null-value failures produced when Java `TopicRouteData.topicRouteDataChanged`
/// sorts its nested DTO lists.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TopicRouteDataChangedError {
    #[error("null QueueData entry during sort")]
    NullQueueData,
    #[error("null QueueData broker name during sort")]
    NullQueueBrokerName,
    #[error("null BrokerData entry during sort")]
    NullBrokerData,
    #[error("null BrokerData broker name during sort")]
    NullBrokerName,
}

/// Null-field failures from the pinned Java route copy/clone helpers.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TopicRouteDataCloneError {
    #[error("null queue data list (Java NullPointerException)")]
    NullQueueDataList,
    #[error("null broker data list (Java NullPointerException)")]
    NullBrokerDataList,
    #[error("null filter server table (Java NullPointerException)")]
    NullFilterServerTable,
    #[error("null QueueData entry during deep clone (Java NullPointerException)")]
    NullQueueData,
    #[error("null BrokerData entry during deep clone (Java NullPointerException)")]
    NullBrokerData,
    #[error("null filter server list during deep clone (Java NullPointerException)")]
    NullFilterServerList,
    #[error("null TopicQueueMappingInfo during deep clone (Java NullPointerException)")]
    NullTopicQueueMappingInfo,
    #[error("null mapping ID map during deep clone (Java NullPointerException)")]
    NullMappingIdMap,
}

fn default_mapping_scope() -> Option<String> {
    Some("__global__".to_owned())
}

impl TopicRouteData {
    pub fn get_queue_datas(&self) -> Option<SharedRouteQueueDataList> {
        self.queue_datas.clone()
    }

    pub fn set_queue_datas(&mut self, value: Option<SharedRouteQueueDataList>) {
        self.queue_datas = value;
    }

    pub fn set_queue_datas_owned(&mut self, value: Option<Vec<Option<QueueData>>>) {
        self.queue_datas = value.map(|queues| {
            SharedValue::new(
                queues
                    .into_iter()
                    .map(|queue| queue.map(SharedValue::new))
                    .collect(),
            )
        });
    }

    pub fn get_broker_datas(&self) -> Option<SharedRouteBrokerDataList> {
        self.broker_datas.clone()
    }

    pub fn set_broker_datas(&mut self, value: Option<SharedRouteBrokerDataList>) {
        self.broker_datas = value;
    }

    pub fn set_broker_datas_owned(&mut self, value: Option<Vec<Option<BrokerData>>>) {
        self.broker_datas = value.map(|brokers| {
            SharedValue::new(
                brokers
                    .into_iter()
                    .map(|broker| broker.map(SharedValue::new))
                    .collect(),
            )
        });
    }

    pub fn get_filter_server_table(&self) -> Option<SharedFilterServerTable> {
        self.filter_server_table.clone()
    }

    pub fn set_filter_server_table(&mut self, value: Option<SharedFilterServerTable>) {
        self.filter_server_table = value;
    }

    pub fn set_filter_server_table_owned(
        &mut self,
        value: Option<BTreeMap<String, Option<Vec<Option<String>>>>>,
    ) {
        self.filter_server_table = value.map(|filters| {
            SharedValue::new(
                filters
                    .into_iter()
                    .map(|(broker, servers)| (broker, servers.map(SharedValue::new)))
                    .collect(),
            )
        });
    }

    pub fn get_topic_queue_mapping_by_broker(&self) -> Option<SharedTopicQueueMappingTable> {
        self.topic_queue_mapping_by_broker.clone()
    }

    pub fn set_topic_queue_mapping_by_broker(
        &mut self,
        value: Option<SharedTopicQueueMappingTable>,
    ) {
        self.topic_queue_mapping_by_broker = value;
    }

    pub fn set_topic_queue_mapping_by_broker_owned(
        &mut self,
        value: Option<BTreeMap<String, Option<TopicQueueMappingInfo>>>,
    ) {
        self.topic_queue_mapping_by_broker = value.map(|mappings| {
            SharedValue::new(
                mappings
                    .into_iter()
                    .map(|(broker, mapping)| (broker, mapping.map(SharedValue::new)))
                    .collect(),
            )
        });
    }

    pub fn queue_datas_snapshot(&self) -> Option<Vec<Option<QueueData>>> {
        self.queue_datas.as_ref().map(|queues| {
            queues.with(|queues| {
                queues
                    .iter()
                    .map(|queue| queue.as_ref().map(|queue| queue.with(Clone::clone)))
                    .collect()
            })
        })
    }

    pub fn broker_datas_snapshot(&self) -> Option<Vec<Option<BrokerData>>> {
        self.broker_datas.as_ref().map(|brokers| {
            brokers.with(|brokers| {
                brokers
                    .iter()
                    .map(|broker| broker.as_ref().map(|broker| broker.with(Clone::clone)))
                    .collect()
            })
        })
    }

    pub fn filter_server_table_snapshot(
        &self,
    ) -> Option<BTreeMap<String, Option<Vec<Option<String>>>>> {
        self.filter_server_table.as_ref().map(|filters| {
            filters.with(|filters| {
                filters
                    .iter()
                    .map(|(broker, servers)| {
                        (
                            broker.clone(),
                            servers.as_ref().map(|servers| servers.snapshot()),
                        )
                    })
                    .collect()
            })
        })
    }

    pub fn topic_queue_mapping_by_broker_snapshot(
        &self,
    ) -> Option<BTreeMap<String, Option<TopicQueueMappingInfo>>> {
        self.topic_queue_mapping_by_broker.as_ref().map(|mappings| {
            mappings.with(|mappings| {
                mappings
                    .iter()
                    .map(|(broker, mapping)| {
                        (
                            broker.clone(),
                            mapping.as_ref().map(|mapping| mapping.with(Clone::clone)),
                        )
                    })
                    .collect()
            })
        })
    }

    /// Encodes the inherited Java `RemotingSerializable.toJson(boolean)` form.
    /// Bean fields are key-sorted and integer map keys remain unquoted as in Fastjson2.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        java_fastjson_json(self, pretty_format)
    }

    /// Encodes the inherited Java `RemotingSerializable.encode()` UTF-8 bytes.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Encodes with the supported inherited Fastjson2 writer features.
    pub fn java_encode_with_features(
        &self,
        features: &[JavaJsonWriterFeature],
    ) -> Result<Vec<u8>, serde_json::Error> {
        Ok(java_fastjson_json_with_features(self, false, features)?.into_bytes())
    }

    /// Decodes the inherited Java `RemotingSerializable.decode` form, including
    /// unquoted integer keys emitted for nested broker and static-topic maps.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java utility form, including null bytes
    /// and JSON `null` as a null result.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        decode_java_route_json_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.decodeList` form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        parse_java_route_json(&String::from_utf8_lossy(data))
    }

    /// Decodes the inherited nullable Java list utility, preserving null list
    /// results and null members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        decode_java_route_json_list_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.fromJson` form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        parse_java_route_json(json)
    }

    /// Decodes the inherited nullable Java `fromJson` utility form.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        parse_java_route_json_nullable(json)
    }

    /// Copies the DTO using the pinned Java `TopicRouteData` copy-constructor
    /// collection defaults. Explicit null queue, broker and filter collections
    /// become empty containers; the mapping table remains nullable.
    ///
    /// Like Java, the new outer collections are independent while nested DTO
    /// elements and filter lists retain their shared object identities.
    pub fn java_copy_constructed(&self) -> Self {
        Self {
            order_topic_conf: self.order_topic_conf.clone(),
            queue_datas: Some(SharedValue::new(
                self.queue_datas
                    .as_ref()
                    .map(|queues| queues.with(Clone::clone))
                    .unwrap_or_default(),
            )),
            broker_datas: Some(SharedValue::new(
                self.broker_datas
                    .as_ref()
                    .map(|brokers| brokers.with(Clone::clone))
                    .unwrap_or_default(),
            )),
            filter_server_table: Some(SharedValue::new(
                self.filter_server_table
                    .as_ref()
                    .map(|filters| filters.with(Clone::clone))
                    .unwrap_or_default(),
            )),
            topic_queue_mapping_by_broker: self
                .topic_queue_mapping_by_broker
                .as_ref()
                .map(|mappings| SharedValue::new(mappings.with(Clone::clone))),
        }
    }

    /// Copies the route using Java's `cloneTopicRouteData` container checks.
    ///
    /// Java shallow-clones the outer containers and shares mutable nested DTOs.
    /// Rust creates new outer shared containers that retain the same nested
    /// element handles. The runtime's raw table separately preserves aliases
    /// with `TopicRouteDataHandle`.
    pub fn clone_topic_route_data(&self) -> Result<Self, TopicRouteDataCloneError> {
        let queue_datas = self
            .queue_datas
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullQueueDataList)?;
        let broker_datas = self
            .broker_datas
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullBrokerDataList)?;
        let filter_server_table = self
            .filter_server_table
            .as_ref()
            .ok_or(TopicRouteDataCloneError::NullFilterServerTable)?;
        Ok(Self {
            order_topic_conf: self.order_topic_conf.clone(),
            queue_datas: Some(SharedValue::new(queue_datas.with(Clone::clone))),
            broker_datas: Some(SharedValue::new(broker_datas.with(Clone::clone))),
            filter_server_table: Some(SharedValue::new(filter_server_table.with(Clone::clone))),
            topic_queue_mapping_by_broker: self
                .topic_queue_mapping_by_broker
                .as_ref()
                .map(|mappings| SharedValue::new(mappings.with(Clone::clone))),
        })
    }

    /// Performs Java `deepCloneTopicRouteData` validation and returns an
    /// owned structural copy of every route DTO and nested collection.
    pub fn deep_clone_topic_route_data(&self) -> Result<Self, TopicRouteDataCloneError> {
        let queue_datas = self
            .queue_datas_snapshot()
            .ok_or(TopicRouteDataCloneError::NullQueueDataList)?
            .iter()
            .map(|queue| {
                queue
                    .as_ref()
                    .cloned()
                    .ok_or(TopicRouteDataCloneError::NullQueueData)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let broker_datas = self
            .broker_datas_snapshot()
            .ok_or(TopicRouteDataCloneError::NullBrokerDataList)?
            .iter()
            .map(|broker| {
                broker
                    .as_ref()
                    .cloned()
                    .ok_or(TopicRouteDataCloneError::NullBrokerData)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filter_server_table = self
            .filter_server_table_snapshot()
            .ok_or(TopicRouteDataCloneError::NullFilterServerTable)?
            .iter()
            .map(|(broker, servers)| {
                Ok((
                    broker.clone(),
                    Some(
                        servers
                            .as_ref()
                            .ok_or(TopicRouteDataCloneError::NullFilterServerList)?
                            .clone(),
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, TopicRouteDataCloneError>>()?;
        let topic_queue_mapping_by_broker = self
            .topic_queue_mapping_by_broker_snapshot()
            .as_ref()
            .map(|mappings| {
                mappings
                    .iter()
                    .map(|(broker, mapping)| {
                        let mapping = mapping
                            .as_ref()
                            .ok_or(TopicRouteDataCloneError::NullTopicQueueMappingInfo)?;
                        mapping
                            .curr_id_map
                            .as_ref()
                            .ok_or(TopicRouteDataCloneError::NullMappingIdMap)?;
                        let copy = mapping.clone();
                        Ok((broker.clone(), Some(copy)))
                    })
                    .collect::<Result<BTreeMap<_, _>, TopicRouteDataCloneError>>()
            })
            .transpose()?;
        Ok(Self {
            order_topic_conf: self.order_topic_conf.clone(),
            queue_datas: Some(SharedValue::new(
                queue_datas
                    .into_iter()
                    .map(|queue| Some(SharedValue::new(queue)))
                    .collect(),
            )),
            broker_datas: Some(SharedValue::new(
                broker_datas
                    .into_iter()
                    .map(|broker| Some(SharedValue::new(broker)))
                    .collect(),
            )),
            filter_server_table: Some(SharedValue::new(
                filter_server_table
                    .into_iter()
                    .map(|(broker, servers)| (broker, servers.map(SharedValue::new)))
                    .collect(),
            )),
            topic_queue_mapping_by_broker: topic_queue_mapping_by_broker.map(|mappings| {
                SharedValue::new(
                    mappings
                        .into_iter()
                        .map(|(broker, mapping)| (broker, mapping.map(SharedValue::new)))
                        .collect(),
                )
            }),
        })
    }

    /// Returns the pinned Java 5.5.1 `TopicRouteData.hashCode` result.
    pub fn java_hash_code(&self) -> i32 {
        let broker_datas = self.broker_datas_snapshot();
        let queue_datas = self.queue_datas_snapshot();
        let filter_server_table = self.filter_server_table_snapshot();
        let mapping_table = self.topic_queue_mapping_by_broker_snapshot();
        let broker_hash = broker_datas
            .as_deref()
            .map(|brokers| {
                java_list_hash_code(brokers, |broker| {
                    broker.as_ref().map(BrokerData::java_hash_code).unwrap_or(0)
                })
            })
            .unwrap_or(0);
        let order_hash = self
            .order_topic_conf
            .as_deref()
            .map(java_string_hash_code)
            .unwrap_or(0);
        let queue_hash = queue_datas
            .as_deref()
            .map(|queues| {
                java_list_hash_code(queues, |queue| {
                    queue.as_ref().map(QueueData::java_hash_code).unwrap_or(0)
                })
            })
            .unwrap_or(0);
        let filter_hash = filter_server_table
            .as_ref()
            .map(|filters| {
                filters.iter().fold(0i32, |hash, (broker, servers)| {
                    let servers_hash = servers
                        .as_ref()
                        .map(|servers| {
                            java_list_hash_code(servers, |server| {
                                server.as_deref().map(java_string_hash_code).unwrap_or(0)
                            })
                        })
                        .unwrap_or(0);
                    hash.wrapping_add(java_string_hash_code(broker) ^ servers_hash)
                })
            })
            .unwrap_or(0);
        let mapping_hash = mapping_table
            .as_ref()
            .map(|mappings| {
                mappings.iter().fold(0i32, |hash, (broker, mapping)| {
                    let mapping_hash = mapping
                        .as_ref()
                        .map(TopicQueueMappingInfo::java_hash_code)
                        .unwrap_or(0);
                    hash.wrapping_add(java_string_hash_code(broker) ^ mapping_hash)
                })
            })
            .unwrap_or(0);
        [
            broker_hash,
            order_hash,
            queue_hash,
            filter_hash,
            mapping_hash,
        ]
        .into_iter()
        .fold(1i32, |hash, value| {
            hash.wrapping_mul(31).wrapping_add(value)
        })
    }

    /// Returns the pinned Java 5.5.1 `TopicRouteData.toString` text.
    ///
    /// Map entries use deterministic Rust key order. Java's `HashMap`
    /// iteration order is unspecified.
    pub fn java_to_string(&self) -> String {
        let queue_data_snapshot = self.queue_datas_snapshot();
        let broker_data_snapshot = self.broker_datas_snapshot();
        let filter_server_snapshot = self.filter_server_table_snapshot();
        let mapping_snapshot = self.topic_queue_mapping_by_broker_snapshot();
        let queue_datas = queue_data_snapshot.as_deref().map_or_else(
            || "null".to_owned(),
            |queues| {
                let values = queues
                    .iter()
                    .map(|queue| {
                        queue
                            .as_ref()
                            .map(QueueData::java_to_string)
                            .unwrap_or_else(|| "null".to_owned())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{values}]")
            },
        );
        let broker_datas = broker_data_snapshot.as_deref().map_or_else(
            || "null".to_owned(),
            |brokers| {
                let values = brokers
                    .iter()
                    .map(|broker| {
                        broker
                            .as_ref()
                            .map(BrokerData::java_to_string)
                            .unwrap_or_else(|| "null".to_owned())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{values}]")
            },
        );
        let filter_server_table = filter_server_snapshot.as_ref().map_or_else(
            || "null".to_owned(),
            |filters| {
                let entries = filters
                    .iter()
                    .map(|(broker, servers)| {
                        let servers = servers.as_ref().map_or_else(
                            || "null".to_owned(),
                            |servers| {
                                let values = servers
                                    .iter()
                                    .map(|server| server.as_deref().unwrap_or("null"))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("[{values}]")
                            },
                        );
                        format!("{broker}={servers}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        let mapping_table = mapping_snapshot.as_ref().map_or_else(
            || "null".to_owned(),
            |mappings| {
                let entries = mappings
                    .iter()
                    .map(|(broker, mapping)| {
                        format!(
                            "{broker}={}",
                            mapping
                                .as_ref()
                                .map(TopicQueueMappingInfo::java_to_string)
                                .unwrap_or_else(|| "null".to_owned())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        format!(
            "TopicRouteData [orderTopicConf={}, queueDatas={}, brokerDatas={}, filterServerTable={}, topicQueueMappingInfoTable={}]",
            self.order_topic_conf.as_deref().unwrap_or("null"),
            queue_datas,
            broker_datas,
            filter_server_table,
            mapping_table
        )
    }

    /// Matches pinned Java 5.5.1 `TopicRouteData.topicRouteDataChanged`.
    ///
    /// Java first copy-constructs both routes, normalizing null queue/broker/
    /// filter collections to empty, then sorts QueueData and BrokerData using
    /// their `compareTo` methods. A null list element or broker name fails
    /// only when sorting compares more than one element.
    pub fn java_topic_route_data_changed(
        &self,
        old: Option<&Self>,
    ) -> Result<bool, TopicRouteDataChangedError> {
        let Some(old) = old else {
            return Ok(true);
        };
        let (mut current, mut old) = (self.java_copy_constructed(), old.java_copy_constructed());
        for route in [&mut current, &mut old] {
            let mut queues = route.queue_datas_snapshot().unwrap_or_default();
            if queues.len() > 1 {
                if queues.iter().any(Option::is_none) {
                    return Err(TopicRouteDataChangedError::NullQueueData);
                }
                if queues
                    .iter()
                    .flatten()
                    .any(|queue| queue.broker_name.is_none())
                {
                    return Err(TopicRouteDataChangedError::NullQueueBrokerName);
                }
            }
            queues.sort_by(|left, right| {
                left.as_ref()
                    .expect("null QueueData entries were rejected before sorting")
                    .java_compare_to(
                        right
                            .as_ref()
                            .expect("null QueueData entries were rejected before sorting"),
                    )
                    .expect("null QueueData names were rejected before sorting")
                    .cmp(&0)
            });
            route.set_queue_datas_owned(Some(queues));

            let mut brokers = route.broker_datas_snapshot().unwrap_or_default();
            if brokers.len() > 1 {
                if brokers.iter().any(Option::is_none) {
                    return Err(TopicRouteDataChangedError::NullBrokerData);
                }
                if brokers
                    .iter()
                    .flatten()
                    .any(|broker| broker.broker_name.is_none())
                {
                    return Err(TopicRouteDataChangedError::NullBrokerName);
                }
            }
            brokers.sort_by(|left, right| {
                left.as_ref()
                    .expect("null BrokerData entries were rejected before sorting")
                    .java_compare_to(
                        right
                            .as_ref()
                            .expect("null BrokerData entries were rejected before sorting"),
                    )
                    .expect("null BrokerData names were rejected before sorting")
                    .cmp(&0)
            });
            route.set_broker_datas_owned(Some(brokers));
            if route.filter_server_table.is_none() {
                route.set_filter_server_table_owned(Some(BTreeMap::new()));
            }
        }
        Ok(current != old)
    }

    pub fn publish_queues(&self, topic: &str) -> Vec<MessageQueue> {
        if let Some(order_conf) = self
            .order_topic_conf
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            return order_conf
                .split(';')
                .filter_map(|entry| {
                    let (broker, count) = entry.split_once(':')?;
                    let count = count.parse::<i32>().ok()?;
                    Some((0..count).map(move |queue_id| MessageQueue::new(topic, broker, queue_id)))
                })
                .flatten()
                .collect();
        }
        if self.order_topic_conf.is_none()
            && self
                .topic_queue_mapping_by_broker_snapshot()
                .is_some_and(|mappings| !mappings.is_empty())
        {
            let mut queues = self
                .static_queue_endpoints(topic)
                .unwrap_or_default()
                .into_keys()
                .collect::<Vec<_>>();
            queues.sort_by_key(MessageQueue::queue_id);
            return queues;
        }

        let broker_datas = self.broker_datas_snapshot().unwrap_or_default();
        let mut masters = BTreeMap::new();
        for broker in broker_datas.iter().flatten() {
            masters
                .entry(broker.broker_name.as_deref())
                .or_insert_with(|| {
                    broker.broker_addrs.as_ref().is_some_and(|addresses| {
                        addresses.with(|addresses| addresses.contains_key(&Some(0)))
                    })
                });
        }
        let queue_data_snapshot = self.queue_datas_snapshot().unwrap_or_default();
        let mut queue_data = queue_data_snapshot.iter().flatten().collect::<Vec<_>>();
        queue_data.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        queue_data
            .into_iter()
            .filter(|data| {
                data.write_queue_nums > 0
                    && data.perm & 0x2 != 0
                    && masters
                        .get(&data.broker_name.as_deref())
                        .copied()
                        .unwrap_or(false)
            })
            .flat_map(|data| {
                (0..data.write_queue_nums).map(|queue_id| {
                    MessageQueue::new_nullable(
                        Some(topic.to_owned()),
                        data.broker_name.clone(),
                        queue_id,
                    )
                })
            })
            .collect()
    }

    pub fn subscribe_queues(&self, topic: &str) -> Vec<MessageQueue> {
        if self
            .topic_queue_mapping_by_broker_snapshot()
            .is_some_and(|mappings| !mappings.is_empty())
        {
            return self
                .static_queue_endpoints(topic)
                .unwrap_or_default()
                .into_keys()
                .collect();
        }
        let queue_data_snapshot = self.queue_datas_snapshot().unwrap_or_default();
        let mut counts = BTreeMap::<Option<&str>, i32>::new();
        for data in queue_data_snapshot
            .iter()
            .flatten()
            .filter(|data| data.read_queue_nums > 0 && data.perm & 0x4 != 0)
        {
            let count = counts.entry(data.broker_name.as_deref()).or_default();
            *count = (*count).max(data.read_queue_nums);
        }
        counts
            .into_iter()
            .flat_map(|(broker, count)| {
                (0..count).map(move |queue_id| {
                    MessageQueue::new_nullable(
                        Some(topic.to_owned()),
                        broker.map(str::to_owned),
                        queue_id,
                    )
                })
            })
            .collect()
    }

    pub fn static_queue_endpoints(&self, topic: &str) -> Option<BTreeMap<MessageQueue, String>> {
        let mappings = self.topic_queue_mapping_by_broker_snapshot()?;
        let mut owners = BTreeMap::<(String, i32), &TopicQueueMappingInfo>::new();
        for mapping in mappings.values().flatten() {
            if let (Some(scope), Some(ids)) = (&mapping.scope, &mapping.curr_id_map) {
                ids.with(|ids| {
                    for id in ids.keys() {
                        let key = (logical_broker(scope), *id);
                        if owners
                            .get(&key)
                            .is_none_or(|old| old.epoch <= mapping.epoch)
                        {
                            owners.insert(key, mapping);
                        }
                    }
                });
            }
        }
        let mut endpoints = BTreeMap::new();
        for queue in self.static_topic_queues(topic)? {
            let broker = owners
                .get(&(
                    queue.broker_name().unwrap_or_default().to_owned(),
                    queue.queue_id(),
                ))
                .and_then(|mapping| mapping.bname.clone())
                .unwrap_or_else(|| "__syslo__none__".to_owned());
            endpoints.insert(queue, broker);
        }
        Some(endpoints)
    }

    fn static_topic_queues(&self, topic: &str) -> Option<Vec<MessageQueue>> {
        let mappings = self.topic_queue_mapping_by_broker_snapshot()?;
        let mut totals = BTreeMap::<String, i32>::new();
        for mapping in mappings.values().flatten() {
            if let Some(scope) = mapping.scope.as_deref().filter(|_| mapping.epoch >= -1) {
                // Java never updates its -1 threshold. Its endpoint map also
                // deduplicates logical names that different scope strings alias.
                let total = totals.entry(logical_broker(scope)).or_default();
                *total = (*total).max(mapping.total_queues);
            }
        }
        Some(
            totals
                .into_iter()
                .flat_map(|(broker, count)| {
                    (0..count).map(move |queue_id| MessageQueue::new(topic, &broker, queue_id))
                })
                .collect(),
        )
    }
}

fn logical_broker(scope: &str) -> String {
    // TopicQueueMappingUtils only strips the prefix for the exact global scope.
    format!(
        "__syslo__{}",
        if scope == "__global__" {
            &scope[2..]
        } else {
            scope
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_detection_emits_java_paths_for_shared_route_dtos() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let mut route = TopicRouteData::default();
        let shared_broker = SharedValue::new(BrokerData {
            broker_name: Some("shared-broker".to_owned()),
            ..BrokerData::default()
        });
        route.set_broker_datas(Some(SharedValue::new(vec![
            Some(shared_broker.clone()),
            Some(shared_broker),
        ])));

        assert_eq!(
            route
                .java_encode_with_features(&[JavaJsonWriterFeature::ReferenceDetection])
                .unwrap(),
            fixture["referenceDetectionSharedRouteCase"]["referenceDetection"]
                .as_str()
                .unwrap()
                .as_bytes(),
        );
    }

    #[test]
    fn static_conversion_matches_pinned_java_epoch_scope_and_endpoint_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let mut checked = 0;
        for case in fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|case| case["input"]["topicQueueMappingByBroker"].is_object())
        {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(
                serde_json::to_value(route.publish_queues("topic")).unwrap(),
                case["publish"]
            );
            assert_eq!(
                serde_json::to_value(route.subscribe_queues("topic")).unwrap(),
                case["subscribe"]
            );
            let endpoints = route.static_queue_endpoints("topic").unwrap();
            assert_eq!(
                serde_json::to_value(
                    endpoints
                        .iter()
                        .map(|(queue, broker)| serde_json::json!({"queue":queue,"broker":broker}))
                        .collect::<Vec<_>>()
                )
                .unwrap(),
                case["endpoints"]
            );
            checked += 1;
        }
        assert_eq!(checked, 6);
    }

    #[test]
    fn ordinary_publish_uses_first_matching_broker_and_static_publish_orders_ids_across_scopes() {
        let mut route = route();
        let mut duplicate = route.broker_datas_snapshot().unwrap()[0]
            .as_ref()
            .unwrap()
            .clone();
        duplicate.broker_addrs =
            Some(BTreeMap::from([(Some(0), Some("other-master".into()))]).into());
        route.broker_datas.as_ref().unwrap().with_mut(|brokers| {
            brokers[0].as_ref().unwrap().with_mut(|broker| {
                broker.broker_addrs =
                    Some(BTreeMap::from([(Some(1), Some("slave".into()))]).into());
            });
            brokers.push(Some(SharedValue::new(duplicate)));
        });
        assert!(route.publish_queues("topic").is_empty());
        route.topic_queue_mapping_by_broker = Some(
            BTreeMap::from([
                (
                    "a".into(),
                    Some(TopicQueueMappingInfo {
                        total_queues: 2,
                        epoch: 0,
                        scope: Some("a".into()),
                        curr_id_map: Some(BTreeMap::new().into()),
                        ..Default::default()
                    }),
                ),
                (
                    "b".into(),
                    Some(TopicQueueMappingInfo {
                        total_queues: 2,
                        epoch: 0,
                        scope: Some("b".into()),
                        curr_id_map: Some(BTreeMap::new().into()),
                        ..Default::default()
                    }),
                ),
            ])
            .into(),
        );
        assert_eq!(
            route
                .publish_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 0, 1, 1]
        );
    }

    #[test]
    fn duplicate_queue_data_retains_java_stable_publish_order_and_set_subscription() {
        let route = route();
        route.broker_datas.as_ref().unwrap().with_mut(|brokers| {
            brokers[0].as_ref().unwrap().with_mut(|broker| {
                broker.broker_addrs =
                    Some(BTreeMap::from([(Some(0), Some("master".into()))]).into());
            });
        });
        route.queue_datas.as_ref().unwrap().with_mut(|queues| {
            queues.retain(|queue| {
                queue.as_ref().is_some_and(|queue| {
                    queue.with(|queue| queue.broker_name.as_deref() == Some("b1"))
                })
            });
        });
        let mut second = route.queue_datas_snapshot().unwrap()[0]
            .as_ref()
            .unwrap()
            .clone();
        second.read_queue_nums = 1;
        second.write_queue_nums = 1;
        route.queue_datas.as_ref().unwrap().with_mut(|queues| {
            queues.push(Some(SharedValue::new(second)));
        });
        assert_eq!(
            route
                .publish_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 1, 2, 0]
        );
        assert_eq!(
            route
                .subscribe_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    fn route() -> TopicRouteData {
        TopicRouteData {
            queue_datas: Some(
                vec![
                    Some(QueueData {
                        broker_name: Some("b1".into()),
                        read_queue_nums: 2,
                        write_queue_nums: 3,
                        perm: 6,
                        topic_sys_flag: 0,
                    }),
                    Some(QueueData {
                        broker_name: Some("b2".into()),
                        read_queue_nums: 4,
                        write_queue_nums: 4,
                        perm: 4,
                        topic_sys_flag: 0,
                    }),
                    Some(QueueData {
                        broker_name: Some("missing".into()),
                        read_queue_nums: 9,
                        write_queue_nums: 9,
                        perm: 6,
                        topic_sys_flag: 0,
                    }),
                ]
                .into(),
            ),
            broker_datas: Some(
                vec![
                    Some(BrokerData {
                        broker_name: Some("b1".into()),
                        ..BrokerData::default()
                    }),
                    Some(BrokerData {
                        broker_name: Some("b2".into()),
                        ..BrokerData::default()
                    }),
                ]
                .into(),
            ),
            ..TopicRouteData::default()
        }
    }

    #[test]
    fn converts_writable_and_readable_queues_without_absent_masters() {
        let route = route();
        route.broker_datas.as_ref().unwrap().with_mut(|brokers| {
            brokers[0].as_ref().unwrap().with_mut(|broker| {
                broker.broker_addrs =
                    Some(BTreeMap::from([(Some(0), Some("b1-master".into()))]).into());
            });
        });
        assert_eq!(route.publish_queues("t").len(), 3);
        assert_eq!(route.subscribe_queues("t").len(), 15);
    }

    #[test]
    fn order_topic_configuration_takes_precedence_over_route_data() {
        let mut route = route();
        route.order_topic_conf = Some("ordered:2;other:1".into());
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 3);
        assert_eq!(queues[0].broker_name(), Some("ordered"));
        assert_eq!(queues[2].queue_id(), 0);
    }

    #[test]
    fn static_routes_allocate_logical_queue_space_per_scope() {
        let mut route = route();
        route.topic_queue_mapping_by_broker = Some(
            BTreeMap::from([(
                "b1".into(),
                Some(TopicQueueMappingInfo {
                    topic: Some("t".into()),
                    total_queues: 8,
                    bname: Some("b1".into()),
                    epoch: 0,
                    dirty: false,
                    scope: Some("__global__".into()),
                    curr_id_map: Some(BTreeMap::from([(0, 0)]).into()),
                }),
            )])
            .into(),
        );
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 8);
        assert_eq!(queues[0].broker_name(), Some("__syslo__global__"));
        assert_eq!(queues[1].broker_name(), Some("__syslo__global__"));
        let endpoints = route.static_queue_endpoints("t").unwrap();
        assert_eq!(endpoints.get(&queues[0]).unwrap(), "b1");
        assert_eq!(endpoints.get(&queues[1]).unwrap(), "__syslo__none__");
    }

    #[test]
    fn preserves_static_topic_mapping_and_json_field_names() {
        let mut route = route();
        route.topic_queue_mapping_by_broker = Some(
            BTreeMap::from([(
                "b1".into(),
                Some(TopicQueueMappingInfo {
                    topic: Some("t".into()),
                    total_queues: 8,
                    bname: Some("b1".into()),
                    epoch: i64::MAX,
                    dirty: true,
                    scope: Some("global".into()),
                    curr_id_map: Some(BTreeMap::from([(0, 2)]).into()),
                }),
            )])
            .into(),
        );
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["topicQueueMappingByBroker"]["b1"]["totalQueues"], 8);
        assert_eq!(json["topicQueueMappingByBroker"]["b1"]["epoch"], i64::MAX);
    }

    #[test]
    fn inherited_remoting_serialization_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();

        macro_rules! check_case {
            ($type:ty, $case:expr) => {{
                let case = $case;
                let raw_input = case["rawInput"].as_str().unwrap();
                let value: $type = serde_json::from_str(raw_input).unwrap();
                let native_json = value.java_to_json(false).unwrap();
                assert_eq!(
                    value.java_encode_with_features(&[]).unwrap(),
                    native_json.as_bytes(),
                    "{} empty writer-feature array",
                    case["id"]
                );
                let browser_compatible = value
                    .java_encode_with_features(&[JavaJsonWriterFeature::BrowserCompatible])
                    .unwrap();
                let expected_browser_compatible = case["writerFeatureOverrides"]
                    ["BrowserCompatible"]
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
                assert_eq!(native_json, case["instanceJson"], "{} toJson", case["id"]);
                assert_eq!(
                    native_json, case["staticJson"],
                    "{} static toJson",
                    case["id"]
                );
                assert_eq!(
                    native_json.as_bytes(),
                    case["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                    "{} encode",
                    case["id"]
                );
                assert_eq!(
                    native_json.as_bytes(),
                    case["staticEncodedJson"].as_str().unwrap().as_bytes(),
                    "{} static encode",
                    case["id"]
                );
                assert_eq!(
                    native_json.as_bytes(),
                    case["featureEncodedJson"].as_str().unwrap().as_bytes(),
                    "{} feature encode",
                    case["id"]
                );
                let featured = value
                    .java_encode_with_features(&[
                        JavaJsonWriterFeature::WriteNonStringValueAsString,
                    ])
                    .unwrap();
                let expected_featured =
                    case["writerFeatureOverrides"]["WriteNonStringValueAsString"]
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
                let expected_boolean_featured =
                    case["writerFeatureOverrides"]["WriteBooleanAsNumber"]
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
                let expected_combined_featured = case["writerFeatureOverrides"][
                                "WriteBooleanAsNumber+WriteNonStringValueAsString"
                            ]
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
                for (feature, feature_name) in [
                    (JavaJsonWriterFeature::NotWriteEmptyArray, "NotWriteEmptyArray"),
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
                    (JavaJsonWriterFeature::WriteNameAsSymbol, "WriteNameAsSymbol"),
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
                    (JavaJsonWriterFeature::UnquoteFieldName, "UnquoteFieldName"),
                    (
                        JavaJsonWriterFeature::IgnoreNoneSerializable,
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
                assert_eq!(
                    case["writerFeatureOverrides"]["ErrorOnNoneSerializable"]["error"],
                    "JSONException",
                    "{} Java error feature result",
                    case["id"]
                );
                assert!(
                    value
                        .java_encode_with_features(&[
                            JavaJsonWriterFeature::ErrorOnNoneSerializable,
                        ])
                        .is_err(),
                    "{} ErrorOnNoneSerializable",
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
                    "{} IgnoreNoneSerializable takes precedence",
                    case["id"]
                );
                assert_eq!(
                    value
                        .java_encode_with_features(&[
                            JavaJsonWriterFeature::PrettyFormatWith2Space,
                            JavaJsonWriterFeature::PrettyFormatWith4Space,
                        ])
                        .unwrap(),
                    case["writerFeatureOverrides"]["PrettyFormatWith4Space"]
                        .as_str()
                        .unwrap()
                        .as_bytes(),
                    "{} Fastjson2 4-space feature takes precedence",
                    case["id"]
                );
                assert_eq!(
                    value.java_to_json(true).unwrap(),
                    case["instancePrettyJson"],
                    "{} pretty toJson",
                    case["id"]
                );
                assert_eq!(
                    value.java_to_json(true).unwrap(),
                    case["staticPrettyJson"],
                    "{} static pretty toJson",
                    case["id"]
                );

                let decoded_expected: $type =
                    <$type>::java_from_json(case["decodedJson"].as_str().unwrap()).unwrap();
                let expected = serde_json::to_value(decoded_expected).unwrap();
                let decoded = <$type>::java_decode(native_json.as_bytes()).unwrap();
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    expected,
                    "{} decode",
                    case["id"]
                );
                let from_json = <$type>::java_from_json(&native_json).unwrap();
                assert_eq!(
                    serde_json::to_value(from_json).unwrap(),
                    expected,
                    "{} fromJson",
                    case["id"]
                );
                let list_json = format!("[{native_json}]");
                let decoded_list = <$type>::java_decode_list(list_json.as_bytes()).unwrap();
                assert_eq!(decoded_list.len(), case["decodedListCount"]);
                assert_eq!(
                    serde_json::to_value(&decoded_list[0]).unwrap(),
                    expected,
                    "{} decodeList",
                    case["id"]
                );
            }};
        }

        for case in fixture["inheritedSerializationCases"].as_array().unwrap() {
            let id = case["id"].as_str().unwrap();
            if id.starts_with("route-") {
                check_case!(TopicRouteData, case);
            } else if id.starts_with("detail-") {
                check_case!(crate::body::TopicQueueMappingDetail, case);
            } else if id.starts_with("logic-item-") {
                check_case!(crate::body::LogicQueueMappingItem, case);
            } else {
                check_case!(TopicQueueMappingInfo, case);
            }
        }

        let case = &fixture["writerFeatureEscapingCase"];
        let value: TopicRouteData =
            serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
        assert_eq!(
            value.java_encode().unwrap(),
            case["instanceEncodedJson"].as_str().unwrap().as_bytes()
        );
        assert_eq!(
            value
                .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                .unwrap(),
            case["writerFeatureOverrides"]["EscapeNoneAscii"]
                .as_str()
                .unwrap()
                .as_bytes()
        );
    }

    #[test]
    fn inherited_remoting_serialization_null_utilities_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let java = &fixture["inheritedSerializationNullUtilities"];
        assert_eq!(java["decodeNullBytesReturnsNull"], true);
        assert_eq!(java["decodeJsonNullReturnsNull"], true);
        assert_eq!(java["fromJsonNullInputReturnsNull"], true);
        assert_eq!(java["fromJsonJsonNullReturnsNull"], true);
        assert_eq!(java["decodeListNullBytesReturnsNull"], true);
        assert_eq!(java["decodeListJsonNullReturnsNull"], true);
        assert_eq!(java["decodeListNullMemberCount"], 1);
        assert_eq!(java["decodeListHasNullMember"], true);
        assert_eq!(java["mappingDecodeNullBytesReturnsNull"], true);
        assert_eq!(java["mappingDecodeJsonNullReturnsNull"], true);
        assert_eq!(java["mappingFromJsonNullInputReturnsNull"], true);
        assert_eq!(java["mappingFromJsonJsonNullReturnsNull"], true);
        assert_eq!(java["mappingDecodeListNullBytesReturnsNull"], true);
        assert_eq!(java["mappingDecodeListJsonNullReturnsNull"], true);
        assert_eq!(java["mappingDecodeListNullMemberCount"], 1);
        assert_eq!(java["mappingDecodeListHasNullMember"], true);

        assert!(
            TopicRouteData::java_decode_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicRouteData::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        let decoded = TopicRouteData::java_decode_list_nullable(Some(b"[null]"))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].is_none());

        assert!(
            TopicQueueMappingInfo::java_decode_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            TopicQueueMappingInfo::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        let decoded = TopicQueueMappingInfo::java_decode_list_nullable(Some(b"[null]"))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].is_none());
    }
}
