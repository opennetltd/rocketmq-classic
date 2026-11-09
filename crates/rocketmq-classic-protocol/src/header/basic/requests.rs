use super::*;

impl_rpc_request_header_semantics!(
    QueryConsumerOffsetRequestHeader,
    UpdateConsumerOffsetRequestHeader,
    QueryConsumeTimeSpanRequestHeader,
    SearchOffsetRequestHeader,
    GetMaxOffsetRequestHeader,
    GetMinOffsetRequestHeader,
    GetEarliestMsgStoretimeRequestHeader,
    PullMessageRequestHeader,
    UnregisterClientRequestHeader,
    GetRouteInfoRequestHeader,
    CreateTopicRequestHeader,
    DeleteTopicRequestHeader,
    DeleteTopicFromNamesrvRequestHeader,
    DeleteSubscriptionGroupRequestHeader,
    QueryMessageRequestHeader,
    QueryTopicsByConsumerRequestHeader,
    QuerySubscriptionByConsumerRequestHeader,
    QueryTopicConsumeByWhoRequestHeader,
    QueryConsumeQueueRequestHeader,
    QueryCorrectionOffsetRequestHeader,
    GetTopicStatsInfoRequestHeader,
    GetTopicConfigRequestHeader,
    GetSubscriptionGroupConfigRequestHeader,
    GetConsumerListByGroupRequestHeader,
    LockBatchMqRequestHeader,
    UnlockBatchMqRequestHeader,
    ConsumerSendMsgBackRequestHeader,
    RecallMessageRequestHeader,
    EndTransactionRequestHeader,
    CheckTransactionStateRequestHeader,
    ReplyMessageRequestHeader,
    NotificationRequestHeader,
    ResetOffsetRequestHeader,
    HeartbeatRequestHeader,
    GetConsumerStatusRequestHeader,
    GetConsumerConnectionListRequestHeader,
    GetConsumeStatsRequestHeader,
    GetConsumerRunningInfoRequestHeader,
    CloneGroupOffsetRequestHeader,
    UpdateGroupForbiddenRequestHeader,
    PopMessageRequestHeader,
    PopLiteMessageRequestHeader,
    AckMessageRequestHeader,
    ChangeInvisibleTimeRequestHeader,
    PollingInfoRequestHeader,
    SendMessageRequestHeaderV1,
    SendMessageRequestHeaderV2,
    LiteSubscriptionCtlRequestHeader,
    NotifyUnsubscribeLiteRequestHeader,
);

impl PartialEq for ConsumeMessageDirectlyResultRequestHeader {
    fn eq(&self, other: &Self) -> bool {
        self.namespace == other.namespace
            && self.namespaced == other.namespaced
            && self.rpc_broker_name == other.rpc_broker_name
            && self.oneway == other.oneway
    }
}

impl Eq for ConsumeMessageDirectlyResultRequestHeader {}

impl Hash for ConsumeMessageDirectlyResultRequestHeader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.namespaced.hash(state);
        self.rpc_broker_name.hash(state);
        self.oneway.hash(state);
    }
}

#[derive(Debug, Clone, Default)]
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryConsumerOffsetRequestHeader;
/// let _ = QueryConsumerOffsetRequestHeader::default().consumer_group;
/// ```
pub struct QueryConsumerOffsetRequestHeader {
    pub(crate) consumer_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) set_zero_if_not_found: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) logical: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl QueryConsumerOffsetRequestHeader {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, value: Option<String>) {
        self.consumer_group = value;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: Option<i32>) {
        self.queue_id = value;
    }

    pub fn get_set_zero_if_not_found(&self) -> Option<bool> {
        self.set_zero_if_not_found
    }

    pub fn set_set_zero_if_not_found(&mut self, value: Option<bool>) {
        self.set_zero_if_not_found = value;
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

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }
}

impl std::fmt::Display for QueryConsumerOffsetRequestHeader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "QueryConsumerOffsetRequestHeader{{consumerGroup={}, topic={}, queueId={}, setZeroIfNotFound={}}}",
            self.consumer_group.as_deref().unwrap_or("null"),
            self.topic.as_deref().unwrap_or("null"),
            self.queue_id
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
            self.set_zero_if_not_found
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
        )
    }
}

impl CommandHeader for QueryConsumerOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "setZeroIfNotFound", self.set_zero_if_not_found);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: Some(required_string(fields, "consumerGroup")?.to_owned()),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            queue_id: Some(required_i32(fields, "queueId")?),
            set_zero_if_not_found: optional_java_boolean(fields, "setZeroIfNotFound"),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            set_zero_if_not_found: optional_java_boolean(fields, "setZeroIfNotFound"),
            broker_name: optional_string(fields, "bname"),
        })
    }
}

#[derive(Debug, Clone, Default)]
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::UpdateConsumerOffsetRequestHeader;
/// let _ = UpdateConsumerOffsetRequestHeader::default().consumer_group;
/// ```
pub struct UpdateConsumerOffsetRequestHeader {
    pub(crate) consumer_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) commit_offset: Option<i64>,
    pub(crate) broker_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) logical: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl UpdateConsumerOffsetRequestHeader {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, value: Option<String>) {
        self.consumer_group = value;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: Option<i32>) {
        self.queue_id = value;
    }

    pub fn get_commit_offset(&self) -> Option<i64> {
        self.commit_offset
    }

    pub fn set_commit_offset(&mut self, value: Option<i64>) {
        self.commit_offset = value;
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

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }
}

impl std::fmt::Display for UpdateConsumerOffsetRequestHeader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "UpdateConsumerOffsetRequestHeader{{consumerGroup={}, topic={}, queueId={}, commitOffset={}}}",
            self.consumer_group.as_deref().unwrap_or("null"),
            self.topic.as_deref().unwrap_or("null"),
            self.queue_id
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
            self.commit_offset
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
        )
    }
}

impl CommandHeader for UpdateConsumerOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "commitOffset", self.commit_offset);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: Some(required_string(fields, "consumerGroup")?.to_owned()),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            queue_id: Some(required_i32(fields, "queueId")?),
            commit_offset: Some(required_i64(fields, "commitOffset")?),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            commit_offset: optional_i64_with_mode(fields, "commitOffset", false)?,
            broker_name: optional_string(fields, "bname"),
        })
    }
}

/// Pinned Java `QueryConsumeTimeSpanRequestHeader`, including its inherited
/// nullable `TopicRequestHeader.lo` property.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryConsumeTimeSpanRequestHeader;
/// let _ = QueryConsumeTimeSpanRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryConsumeTimeSpanRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) group: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl QueryConsumeTimeSpanRequestHeader {
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
}

impl_topic_request_header_accessors!(QueryConsumeTimeSpanRequestHeader);

impl CommandHeader for QueryConsumeTimeSpanRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "group", self.group.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
            topic: optional_string(fields, "topic"),
            group: optional_string(fields, "group"),
        })
    }
}

/// Java `GetTopicStatsInfoRequestHeader`; its annotated topic is nullable at
/// decode time because `checkFields()` is a no-op.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetTopicStatsInfoRequestHeader;
/// let _ = GetTopicStatsInfoRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetTopicStatsInfoRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl QueryConsumeQueueRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: i32) {
        self.queue_id = value;
    }

    pub fn get_index(&self) -> i64 {
        self.index
    }

    pub fn set_index(&mut self, value: i64) {
        self.index = value;
    }

    pub fn get_count(&self) -> i32 {
        self.count
    }

    pub fn set_count(&mut self, value: i32) {
        self.count = value;
    }

    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, value: Option<String>) {
        self.consumer_group = value;
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

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }
}

/// Java `GetAllSubscriptionGroupRequestHeader`. Its annotated sequence remains
/// nullable because `checkFields()` is a no-op.
#[derive(Debug, Clone, Default)]
pub struct GetAllSubscriptionGroupRequestHeader {
    group_seq: Option<i32>,
    data_version: Option<String>,
    max_group_num: Option<i32>,
}

impl GetAllSubscriptionGroupRequestHeader {
    pub fn get_group_seq(&self) -> Option<i32> {
        self.group_seq
    }

    pub fn set_group_seq(&mut self, value: Option<i32>) {
        self.group_seq = value;
    }

    pub fn get_data_version(&self) -> Option<&str> {
        self.data_version.as_deref()
    }

    pub fn set_data_version(&mut self, value: Option<String>) {
        self.data_version = value;
    }

    pub fn get_max_group_num(&self) -> Option<i32> {
        self.max_group_num
    }

    pub fn set_max_group_num(&mut self, value: Option<i32>) {
        self.max_group_num = value;
    }
}

/// Java `GetAllTopicConfigRequestHeader`. Its annotated sequence is nullable
/// because `checkFields()` is a no-op.
#[derive(Debug, Clone, Default)]
pub struct GetAllTopicConfigRequestHeader {
    topic_seq: Option<i32>,
    data_version: Option<String>,
    max_topic_num: Option<i32>,
}

impl GetAllTopicConfigRequestHeader {
    pub fn get_topic_seq(&self) -> Option<i32> {
        self.topic_seq
    }

    pub fn set_topic_seq(&mut self, value: Option<i32>) {
        self.topic_seq = value;
    }

    pub fn get_data_version(&self) -> Option<&str> {
        self.data_version.as_deref()
    }

    pub fn set_data_version(&mut self, value: Option<String>) {
        self.data_version = value;
    }

    pub fn get_max_topic_num(&self) -> Option<i32> {
        self.max_topic_num
    }

    pub fn set_max_topic_num(&mut self, value: Option<i32>) {
        self.max_topic_num = value;
    }
}

/// Java `GetTopicsByClusterRequestHeader` carries the target cluster name.
#[derive(Debug, Clone, Default)]
pub struct GetTopicsByClusterRequestHeader {
    pub(crate) cluster: Option<String>,
}

impl GetTopicsByClusterRequestHeader {
    pub fn get_cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn set_cluster(&mut self, value: Option<String>) {
        self.cluster = value;
    }
}

impl CommandHeader for GetTopicsByClusterRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "cluster", self.cluster.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            cluster: optional_string(fields, "cluster"),
        })
    }
}

/// Java `GetSubscriptionGroupConfigRequestHeader` carries a nullable group
/// and inherits the namespace, broker-name, and oneway RPC fields.
#[derive(Debug, Clone, Default)]
pub struct GetSubscriptionGroupConfigRequestHeader {
    pub(crate) group: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl GetSubscriptionGroupConfigRequestHeader {
    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, value: Option<String>) {
        self.group = value;
    }
}

impl_rpc_request_header_accessors!(GetSubscriptionGroupConfigRequestHeader);

impl CommandHeader for GetSubscriptionGroupConfigRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "group", self.group.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: optional_string(fields, "group"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

impl UpdateGroupForbiddenRequestHeader {
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
}

impl CommandHeader for UpdateGroupForbiddenRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "group", self.group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "readable", self.readable);
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: optional_string(fields, "topic"),
            logical: optional_java_boolean(fields, "lo"),
            group: optional_string(fields, "group"),
            readable: optional_java_boolean(fields, "readable"),
        })
    }
}

impl GetTopicStatsInfoRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }
}

impl GetTopicConfigRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }
}

impl_topic_request_header_accessors!(GetTopicStatsInfoRequestHeader);

impl_topic_request_header_accessors!(GetTopicConfigRequestHeader);

impl CommandHeader for GetTopicStatsInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

impl CommandHeader for GetTopicConfigRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

impl CommandHeader for GetAllTopicConfigRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topicSeq", self.topic_seq);
        put_optional(fields, "dataVersion", self.data_version.as_deref());
        put_optional(fields, "maxTopicNum", self.max_topic_num);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic_seq: optional_i32(fields, "topicSeq")?,
            data_version: optional_string(fields, "dataVersion"),
            max_topic_num: optional_i32(fields, "maxTopicNum")?,
        })
    }
}

impl CommandHeader for QueryConsumeQueueRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("index".into(), self.index.to_string());
        fields.insert("count".into(), self.count.to_string());
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, false)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        // Java's reflective header decoder retains primitive zero defaults when
        // an incoming numeric field is absent or malformed in either mode.
        Ok(Self {
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32(fields, "queueId")
                .unwrap_or_default()
                .unwrap_or_default(),
            index: optional_i64(fields, "index")
                .unwrap_or_default()
                .unwrap_or_default(),
            count: optional_i32(fields, "count")
                .unwrap_or_default()
                .unwrap_or_default(),
            consumer_group: optional_string(fields, "consumerGroup"),
            broker_name: optional_string(fields, "bname"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

impl CommandHeader for GetAllSubscriptionGroupRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "groupSeq", self.group_seq);
        put_optional(fields, "dataVersion", self.data_version.as_deref());
        put_optional(fields, "maxGroupNum", self.max_group_num);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            group_seq: optional_i32(fields, "groupSeq").unwrap_or(None),
            data_version: optional_string(fields, "dataVersion"),
            max_group_num: optional_i32(fields, "maxGroupNum").unwrap_or(None),
        })
    }
}

/// Pinned Java `DeleteTopicRequestHeader`; its annotated topic remains
/// nullable because Java's `checkFields()` is a no-op.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::DeleteTopicRequestHeader;
/// let _ = DeleteTopicRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct DeleteTopicRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl DeleteTopicRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }
}

impl_topic_request_header_accessors!(DeleteTopicRequestHeader);

impl CommandHeader for DeleteTopicRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

/// Pinned Java Namesrv `DeleteTopicFromNamesrvRequestHeader`. Its annotated
/// topic is nullable because Java's `checkFields()` is a no-op.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::DeleteTopicFromNamesrvRequestHeader;
/// let _ = DeleteTopicFromNamesrvRequestHeader::default().cluster_name;
/// ```
#[derive(Debug, Clone, Default)]
pub struct DeleteTopicFromNamesrvRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) cluster_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl DeleteTopicFromNamesrvRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_cluster_name(&self) -> Option<&str> {
        self.cluster_name.as_deref()
    }

    pub fn set_cluster_name(&mut self, value: Option<String>) {
        self.cluster_name = value;
    }
}

impl_topic_request_header_accessors!(DeleteTopicFromNamesrvRequestHeader);

impl CommandHeader for DeleteTopicFromNamesrvRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "clusterName", self.cluster_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            cluster_name: optional_string(fields, "clusterName"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

/// Pinned Java Namesrv `DeleteKVConfigRequestHeader`. Its annotated namespace
/// and key remain nullable because Java's `checkFields()` is a no-op.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::DeleteKvConfigRequestHeader;
/// let _ = DeleteKvConfigRequestHeader::default().key;
/// ```
#[derive(Debug, Clone, Default)]
pub struct DeleteKvConfigRequestHeader {
    pub(crate) namespace: Option<String>,
    pub(crate) key: Option<String>,
}

impl DeleteKvConfigRequestHeader {
    pub fn get_namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn set_namespace(&mut self, value: Option<String>) {
        self.namespace = value;
    }

    pub fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn set_key(&mut self, value: Option<String>) {
        self.key = value;
    }
}

impl CommandHeader for DeleteKvConfigRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "namespace", self.namespace.as_deref());
        put_optional(fields, "key", self.key.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "namespace"),
            key: optional_string(fields, "key"),
        })
    }
}

/// Pinned Java NameServer `GetKVConfigRequestHeader`. The annotations do not
/// enforce non-null values because Java's `checkFields()` is a no-op.
#[derive(Debug, Clone, Default)]
pub struct GetKvConfigRequestHeader {
    pub(crate) namespace: Option<String>,
    pub(crate) key: Option<String>,
}

impl GetKvConfigRequestHeader {
    pub fn get_namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn set_namespace(&mut self, value: Option<String>) {
        self.namespace = value;
    }

    pub fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn set_key(&mut self, value: Option<String>) {
        self.key = value;
    }
}

impl CommandHeader for GetKvConfigRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "namespace", self.namespace.as_deref());
        put_optional(fields, "key", self.key.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "namespace"),
            key: optional_string(fields, "key"),
        })
    }
}

/// Pinned Java NameServer `PutKVConfigRequestHeader`. Java accepts absent
/// values because `checkFields()` is a no-op.
#[derive(Debug, Clone, Default)]
pub struct PutKvConfigRequestHeader {
    pub(crate) namespace: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) value: Option<String>,
}

impl PutKvConfigRequestHeader {
    pub fn get_namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn set_namespace(&mut self, value: Option<String>) {
        self.namespace = value;
    }

    pub fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn set_key(&mut self, value: Option<String>) {
        self.key = value;
    }

    pub fn get_value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    pub fn set_value(&mut self, value: Option<String>) {
        self.value = value;
    }
}

impl CommandHeader for PutKvConfigRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "namespace", self.namespace.as_deref());
        put_optional(fields, "key", self.key.as_deref());
        put_optional(fields, "value", self.value.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "namespace"),
            key: optional_string(fields, "key"),
            value: optional_string(fields, "value"),
        })
    }
}

/// Pinned Java NameServer `GetKVListByNamespaceRequestHeader`. The nullable
/// namespace matches Java's no-op `checkFields()`.
#[derive(Debug, Clone, Default)]
pub struct GetKvListByNamespaceRequestHeader {
    pub(crate) namespace: Option<String>,
}

impl GetKvListByNamespaceRequestHeader {
    pub fn get_namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn set_namespace(&mut self, value: Option<String>) {
        self.namespace = value;
    }
}

impl CommandHeader for GetKvListByNamespaceRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "namespace", self.namespace.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "namespace"),
        })
    }
}

/// Pinned Java NameServer `WipeWritePermOfBrokerRequestHeader`. Its annotated
/// broker name stays nullable because Java's `checkFields()` is a no-op.
#[derive(Debug, Clone, Default)]
pub struct WipeWritePermOfBrokerRequestHeader {
    pub(crate) broker_name: Option<String>,
}

impl WipeWritePermOfBrokerRequestHeader {
    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }
}

impl CommandHeader for WipeWritePermOfBrokerRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "brokerName", self.broker_name.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            broker_name: optional_string(fields, "brokerName"),
        })
    }
}

/// Pinned Java NameServer `AddWritePermOfBrokerRequestHeader`. Its annotated
/// broker name stays nullable because Java's `checkFields()` is a no-op.
#[derive(Debug, Clone, Default)]
pub struct AddWritePermOfBrokerRequestHeader {
    pub(crate) broker_name: Option<String>,
}

impl AddWritePermOfBrokerRequestHeader {
    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }
}

impl CommandHeader for AddWritePermOfBrokerRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "brokerName", self.broker_name.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            broker_name: optional_string(fields, "brokerName"),
        })
    }
}

/// Java's `AddBrokerRequestHeader` carries a nullable broker configuration
/// path. Its pinned `checkFields()` implementation does not validate it.
#[derive(Debug, Clone, Default)]
pub struct AddBrokerRequestHeader {
    pub(crate) config_path: Option<String>,
}

impl AddBrokerRequestHeader {
    pub fn get_config_path(&self) -> Option<&str> {
        self.config_path.as_deref()
    }

    pub fn set_config_path(&mut self, value: Option<String>) {
        self.config_path = value;
    }
}

impl CommandHeader for AddBrokerRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "configPath", self.config_path.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            config_path: optional_string(fields, "configPath"),
        })
    }
}

/// Java's `RemoveBrokerRequestHeader` keeps all fields nullable, including its
/// boxed broker ID. The Broker implementation performs validation later.
#[derive(Debug, Clone, Default)]
pub struct RemoveBrokerRequestHeader {
    pub(crate) broker_cluster_name: Option<String>,
    pub(crate) broker_name: Option<String>,
    pub(crate) broker_id: Option<i64>,
}

impl RemoveBrokerRequestHeader {
    pub fn get_broker_cluster_name(&self) -> Option<&str> {
        self.broker_cluster_name.as_deref()
    }

    pub fn set_broker_cluster_name(&mut self, value: Option<String>) {
        self.broker_cluster_name = value;
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

    pub fn get_broker_id(&self) -> Option<i64> {
        self.broker_id
    }

    pub fn set_broker_id(&mut self, value: Option<i64>) {
        self.broker_id = value;
    }
}

impl CommandHeader for RemoveBrokerRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(
            fields,
            "brokerClusterName",
            self.broker_cluster_name.as_deref(),
        );
        put_optional(fields, "brokerName", self.broker_name.as_deref());
        put_optional(fields, "brokerId", self.broker_id);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            broker_cluster_name: optional_string(fields, "brokerClusterName"),
            broker_name: optional_string(fields, "brokerName"),
            broker_id: optional_i64(fields, "brokerId")?,
        })
    }
}

/// Pinned Java `CloneGroupOffsetRequestHeader`. Its `checkFields()` is a
/// no-op, so nullable source/destination groups and topic remain accepted at
/// the client request boundary. The inherited `RpcRequestHeader` equality and
/// hash contract considers only its four inherited fields.
#[derive(Debug, Clone, Default)]
pub struct CloneGroupOffsetRequestHeader {
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) src_group: Option<String>,
    pub(crate) dest_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) offline: bool,
}

impl CloneGroupOffsetRequestHeader {
    pub fn get_src_group(&self) -> Option<&str> {
        self.src_group.as_deref()
    }

    pub fn set_src_group(&mut self, value: Option<String>) {
        self.src_group = value;
    }

    pub fn get_dest_group(&self) -> Option<&str> {
        self.dest_group.as_deref()
    }

    pub fn set_dest_group(&mut self, value: Option<String>) {
        self.dest_group = value;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn is_offline(&self) -> bool {
        self.offline
    }

    pub fn set_offline(&mut self, value: bool) {
        self.offline = value;
    }
}

impl CommandHeader for CloneGroupOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "srcGroup", self.src_group.as_deref());
        put_optional(fields, "destGroup", self.dest_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert("offline".to_owned(), self.offline.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            src_group: optional_string(fields, "srcGroup"),
            dest_group: optional_string(fields, "destGroup"),
            topic: optional_string(fields, "topic"),
            offline: optional_java_boolean(fields, "offline").unwrap_or_default(),
        })
    }
}

/// Pinned Java `DeleteSubscriptionGroupRequestHeader`. Its annotated group
/// name remains nullable because Java's `checkFields()` is a no-op; the
/// primitive `cleanOffset` field defaults to and encodes `false`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::DeleteSubscriptionGroupRequestHeader;
/// let _ = DeleteSubscriptionGroupRequestHeader::default().group_name;
/// ```
#[derive(Debug, Clone, Default)]
pub struct DeleteSubscriptionGroupRequestHeader {
    pub(crate) group_name: Option<String>,
    pub(crate) clean_offset: bool,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl DeleteSubscriptionGroupRequestHeader {
    pub fn get_group_name(&self) -> Option<&str> {
        self.group_name.as_deref()
    }

    pub fn set_group_name(&mut self, value: Option<String>) {
        self.group_name = value;
    }

    pub fn is_clean_offset(&self) -> bool {
        self.clean_offset
    }

    pub fn set_clean_offset(&mut self, value: bool) {
        self.clean_offset = value;
    }
}

impl_rpc_request_header_accessors!(DeleteSubscriptionGroupRequestHeader);

impl CommandHeader for DeleteSubscriptionGroupRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "groupName", self.group_name.as_deref());
        fields.insert("cleanOffset".into(), self.clean_offset.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            group_name: optional_string(fields, "groupName"),
            clean_offset: java_boolean(fields, "cleanOffset"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Pinned Java `QueryTopicsByConsumerRequestHeader` used by
/// `QUERY_TOPICS_BY_CONSUMER`. Java's `checkFields()` is a no-op even though
/// `group` carries a non-null annotation, so the request remains nullable here.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryTopicsByConsumerRequestHeader;
/// let _ = QueryTopicsByConsumerRequestHeader::default().group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryTopicsByConsumerRequestHeader {
    pub(crate) group: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl QueryTopicsByConsumerRequestHeader {
    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, value: Option<String>) {
        self.group = value;
    }

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

impl CommandHeader for QueryTopicsByConsumerRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "group", self.group.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: optional_string(fields, "group"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Pinned Java `QuerySubscriptionByConsumerRequestHeader` used by
/// `QUERY_SUBSCRIPTION_BY_CONSUMER`. Java's `checkFields()` is a no-op, so the
/// annotated group and the TopicRequestHeader fields remain nullable.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QuerySubscriptionByConsumerRequestHeader;
/// let _ = QuerySubscriptionByConsumerRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct QuerySubscriptionByConsumerRequestHeader {
    pub(crate) group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl QuerySubscriptionByConsumerRequestHeader {
    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, value: Option<String>) {
        self.group = value;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

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

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }
}

impl CommandHeader for QuerySubscriptionByConsumerRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "group", self.group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: optional_string(fields, "group"),
            topic: optional_string(fields, "topic"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

/// Pinned Java `QueryTopicConsumeByWhoRequestHeader` used by
/// `QUERY_TOPIC_CONSUME_BY_WHO`. Java's `checkFields()` is a no-op, so its topic
/// and inherited `RpcRequestHeader`/`TopicRequestHeader` fields remain nullable.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryTopicConsumeByWhoRequestHeader;
/// let _ = QueryTopicConsumeByWhoRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryTopicConsumeByWhoRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl QueryTopicConsumeByWhoRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

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

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }
}

impl CommandHeader for QueryTopicConsumeByWhoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

impl QueryCorrectionOffsetRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_filter_groups(&self) -> Option<&str> {
        self.filter_groups.as_deref()
    }

    pub fn set_filter_groups(&mut self, value: Option<String>) {
        self.filter_groups = value;
    }

    pub fn get_compare_group(&self) -> Option<&str> {
        self.compare_group.as_deref()
    }

    pub fn set_compare_group(&mut self, value: Option<String>) {
        self.compare_group = value;
    }
}

impl CommandHeader for QueryCorrectionOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "filterGroups", self.filter_groups.as_deref());
        put_optional(fields, "compareGroup", self.compare_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: optional_string(fields, "topic"),
            logical: optional_java_boolean(fields, "lo"),
            filter_groups: optional_string(fields, "filterGroups"),
            compare_group: optional_string(fields, "compareGroup"),
        })
    }
}

/// Pinned Java `SearchOffsetRequestHeader` keeps its request fields non-public.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SearchOffsetRequestHeader;
/// let _ = SearchOffsetRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct SearchOffsetRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) lite_topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) timestamp: Option<i64>,
    pub(crate) boundary_type: Option<BoundaryType>,
    pub(crate) logical: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl SearchOffsetRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_lite_topic(&self) -> Option<&str> {
        self.lite_topic.as_deref()
    }

    pub fn set_lite_topic(&mut self, value: Option<String>) {
        self.lite_topic = value;
    }

    pub fn get_queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: Option<i32>) {
        self.queue_id = value;
    }

    pub fn get_timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    pub fn set_timestamp(&mut self, value: Option<i64>) {
        self.timestamp = value;
    }

    pub fn set_boundary_type(&mut self, value: Option<BoundaryType>) {
        self.boundary_type = value;
    }

    /// Returns Java `SearchOffsetRequestHeader.getBoundaryType()` semantics.
    pub fn get_boundary_type(&self) -> BoundaryType {
        self.boundary_type.unwrap_or_default()
    }

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }

    pub fn get_bname(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

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

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }
}

impl CommandHeader for SearchOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "liteTopic", self.lite_topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "timestamp", self.timestamp);
        put_optional(
            fields,
            "boundaryType",
            self.boundary_type.map(BoundaryType::name),
        );
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        let boundary_type = fields
            .get("boundaryType")
            .map(|value| {
                BoundaryType::value_of_name(Some(value)).map_err(|error| {
                    ProtocolError::InvalidField {
                        field: "boundaryType",
                        reason: error.to_string(),
                    }
                })
            })
            .transpose()?;
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            lite_topic: optional_string(fields, "liteTopic"),
            queue_id: Some(required_i32(fields, "queueId")?),
            timestamp: Some(required_i64(fields, "timestamp")?),
            boundary_type,
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: optional_string(fields, "topic"),
            lite_topic: optional_string(fields, "liteTopic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            timestamp: optional_i64_with_mode(fields, "timestamp", false)?,
            boundary_type: fields
                .get("boundaryType")
                .map(|value| BoundaryType::get_type(Some(value))),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }
}

/// Pinned Java `GetMaxOffsetRequestHeader` keeps its request fields non-public.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMaxOffsetRequestHeader;
/// let _ = GetMaxOffsetRequestHeader::default().committed;
/// ```
#[derive(Debug, Clone)]
pub struct GetMaxOffsetRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) committed: bool,
    pub(crate) logical: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl GetMaxOffsetRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: Option<i32>) {
        self.queue_id = value;
    }

    pub fn is_committed(&self) -> bool {
        self.committed
    }

    pub fn set_committed(&mut self, value: bool) {
        self.committed = value;
    }

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }

    pub fn get_bname(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

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

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }
}

impl Default for GetMaxOffsetRequestHeader {
    fn default() -> Self {
        Self {
            topic: None,
            queue_id: None,
            committed: true,
            logical: None,
            broker_name: None,
            namespace: None,
            namespaced: None,
            oneway: None,
        }
    }
}

impl CommandHeader for GetMaxOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        fields.insert("committed".into(), self.committed.to_string());
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            queue_id: Some(required_i32(fields, "queueId")?),
            committed: optional_java_boolean(fields, "committed").unwrap_or(true),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            committed: optional_java_boolean(fields, "committed").unwrap_or(true),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }
}

/// Pinned Java `GetMinOffsetRequestHeader` keeps its request fields non-public.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMinOffsetRequestHeader;
/// let _ = GetMinOffsetRequestHeader::default().queue_id;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetMinOffsetRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) logical: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl GetMinOffsetRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: Option<i32>) {
        self.queue_id = value;
    }

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }

    pub fn get_bname(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

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

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }
}

impl CommandHeader for GetMinOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            queue_id: Some(required_i32(fields, "queueId")?),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }
}

/// Pinned Java `GetEarliestMsgStoretimeRequestHeader` keeps its request fields non-public.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetEarliestMsgStoretimeRequestHeader;
/// let _ = GetEarliestMsgStoretimeRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetEarliestMsgStoretimeRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) logical: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl GetEarliestMsgStoretimeRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: Option<i32>) {
        self.queue_id = value;
    }

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }

    pub fn get_bname(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

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

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }
}

impl CommandHeader for GetEarliestMsgStoretimeRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            queue_id: Some(required_i32(fields, "queueId")?),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PullMessageRequestHeader;
/// let _ = PullMessageRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct PullMessageRequestHeader {
    pub(crate) consumer_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) lite_topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) queue_offset: Option<i64>,
    pub(crate) max_msg_nums: Option<i32>,
    pub(crate) sys_flag: Option<i32>,
    pub(crate) commit_offset: Option<i64>,
    pub(crate) suspend_timeout_millis: Option<i64>,
    pub(crate) subscription: Option<String>,
    pub(crate) sub_version: Option<i64>,
    pub(crate) expression_type: Option<String>,
    pub(crate) max_msg_bytes: Option<i32>,
    pub(crate) request_source: Option<i32>,
    pub(crate) proxy_forward_client_id: Option<String>,
    pub(crate) broker_name: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) logical: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for PullMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "liteTopic", self.lite_topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "queueOffset", self.queue_offset);
        put_optional(fields, "maxMsgNums", self.max_msg_nums);
        put_optional(fields, "sysFlag", self.sys_flag);
        put_optional(fields, "commitOffset", self.commit_offset);
        put_optional(fields, "suspendTimeoutMillis", self.suspend_timeout_millis);
        put_optional(fields, "subscription", self.subscription.as_deref());
        put_optional(fields, "subVersion", self.sub_version);
        put_optional(fields, "expressionType", self.expression_type.as_deref());
        put_optional(fields, "maxMsgBytes", self.max_msg_bytes);
        put_optional(fields, "requestSource", self.request_source);
        put_optional(
            fields,
            "proxyFrowardClientId",
            self.proxy_forward_client_id.as_deref(),
        );
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        let header = Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            lite_topic: optional_string(fields, "liteTopic"),
            queue_id: optional_i32_with_mode(fields, "queueId", use_fast_encode)?,
            queue_offset: optional_i64_with_mode(fields, "queueOffset", use_fast_encode)?,
            max_msg_nums: optional_i32_with_mode(fields, "maxMsgNums", use_fast_encode)?,
            sys_flag: optional_i32_with_mode(fields, "sysFlag", use_fast_encode)?,
            commit_offset: optional_i64_with_mode(fields, "commitOffset", use_fast_encode)?,
            suspend_timeout_millis: optional_i64_with_mode(
                fields,
                "suspendTimeoutMillis",
                use_fast_encode,
            )?,
            subscription: optional_string(fields, "subscription"),
            sub_version: optional_i64_with_mode(fields, "subVersion", use_fast_encode)?,
            expression_type: optional_string(fields, "expressionType"),
            max_msg_bytes: optional_i32_with_mode(fields, "maxMsgBytes", use_fast_encode)?,
            request_source: optional_i32_with_mode(fields, "requestSource", use_fast_encode)?,
            proxy_forward_client_id: optional_string(fields, "proxyFrowardClientId"),
            broker_name: optional_string(fields, "bname"),
        };
        header.check_fields()?;
        Ok(header)
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::UnregisterClientRequestHeader;
/// let _ = UnregisterClientRequestHeader::default().client_id;
/// ```
#[derive(Debug, Clone, Default)]
pub struct UnregisterClientRequestHeader {
    pub(crate) client_id: String,
    pub(crate) producer_group: Option<String>,
    pub(crate) consumer_group: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for UnregisterClientRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("clientID".into(), self.client_id.clone());
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            client_id: required_string(fields, "clientID")?.to_owned(),
            producer_group: optional_string(fields, "producerGroup"),
            consumer_group: optional_string(fields, "consumerGroup"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetRouteInfoRequestHeader;
/// let _ = GetRouteInfoRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetRouteInfoRequestHeader {
    pub(crate) topic: String,
    pub(crate) accept_standard_json_only: Option<bool>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for GetRouteInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        put_optional(
            fields,
            "acceptStandardJsonOnly",
            self.accept_standard_json_only,
        );
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            accept_standard_json_only: optional_java_boolean(fields, "acceptStandardJsonOnly"),
        })
    }
}

/// Pinned Java auth `CreateUserRequestHeader`. Java's `checkFields()` is a no-op,
/// so `username` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct CreateUserRequestHeader {
    username: Option<String>,
}

impl CommandHeader for CreateUserRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "username", self.username.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            username: optional_string(fields, "username"),
        })
    }
}

impl_header_field_accessors!(
    CreateUserRequestHeader,
    optional_string,
    username,
    [get_username],
    [set_username]
);

/// Pinned Java auth `UpdateUserRequestHeader`. Java's `checkFields()` is a no-op,
/// so `username` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct UpdateUserRequestHeader {
    username: Option<String>,
}

impl CommandHeader for UpdateUserRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "username", self.username.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            username: optional_string(fields, "username"),
        })
    }
}

impl_header_field_accessors!(
    UpdateUserRequestHeader,
    optional_string,
    username,
    [get_username],
    [set_username]
);

/// Pinned Java auth `DeleteUserRequestHeader`. Java's `checkFields()` is a no-op,
/// so `username` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct DeleteUserRequestHeader {
    username: Option<String>,
}

impl CommandHeader for DeleteUserRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "username", self.username.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            username: optional_string(fields, "username"),
        })
    }
}

impl_header_field_accessors!(
    DeleteUserRequestHeader,
    optional_string,
    username,
    [get_username],
    [set_username]
);

/// Pinned Java auth `GetUserRequestHeader`. Java's `checkFields()` is a no-op,
/// so `username` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct GetUserRequestHeader {
    username: Option<String>,
}

impl CommandHeader for GetUserRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "username", self.username.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            username: optional_string(fields, "username"),
        })
    }
}

impl_header_field_accessors!(
    GetUserRequestHeader,
    optional_string,
    username,
    [get_username],
    [set_username]
);

/// Pinned Java auth `GetAclRequestHeader`. Java's `checkFields()` is a no-op,
/// so `subject` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct GetAclRequestHeader {
    subject: Option<String>,
}

impl CommandHeader for GetAclRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "subject", self.subject.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            subject: optional_string(fields, "subject"),
        })
    }
}

impl_header_field_accessors!(
    GetAclRequestHeader,
    optional_string,
    subject,
    [get_subject],
    [set_subject]
);

/// Pinned Java auth `CreateAclRequestHeader`. Java's `checkFields()` is a no-op,
/// so `subject` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct CreateAclRequestHeader {
    subject: Option<String>,
}

impl CommandHeader for CreateAclRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "subject", self.subject.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            subject: optional_string(fields, "subject"),
        })
    }
}

impl_header_field_accessors!(
    CreateAclRequestHeader,
    optional_string,
    subject,
    [get_subject],
    [set_subject]
);

/// Pinned Java auth `DeleteAclRequestHeader`. Java's `checkFields()` is a no-op,
/// so all three fields remain nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct DeleteAclRequestHeader {
    subject: Option<String>,
    policy_type: Option<String>,
    resource: Option<String>,
}

impl CommandHeader for DeleteAclRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "subject", self.subject.as_deref());
        put_optional(fields, "policyType", self.policy_type.as_deref());
        put_optional(fields, "resource", self.resource.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            subject: optional_string(fields, "subject"),
            policy_type: optional_string(fields, "policyType"),
            resource: optional_string(fields, "resource"),
        })
    }
}

impl_header_field_accessors!(
    DeleteAclRequestHeader,
    optional_string,
    subject,
    [get_subject],
    [set_subject]
);

impl_header_field_accessors!(
    DeleteAclRequestHeader,
    optional_string,
    policy_type,
    [get_policy_type],
    [set_policy_type]
);

impl_header_field_accessors!(
    DeleteAclRequestHeader,
    optional_string,
    resource,
    [get_resource],
    [set_resource]
);

/// Pinned Java auth `UpdateAclRequestHeader`. Java's `checkFields()` is a no-op,
/// so `subject` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct UpdateAclRequestHeader {
    subject: Option<String>,
}

impl CommandHeader for UpdateAclRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "subject", self.subject.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            subject: optional_string(fields, "subject"),
        })
    }
}

impl_header_field_accessors!(
    UpdateAclRequestHeader,
    optional_string,
    subject,
    [get_subject],
    [set_subject]
);

/// Pinned Java auth `ListAclsRequestHeader`. Java's `checkFields()` is a no-op,
/// so both filters remain nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct ListAclsRequestHeader {
    subject_filter: Option<String>,
    resource_filter: Option<String>,
}

impl CommandHeader for ListAclsRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "subjectFilter", self.subject_filter.as_deref());
        put_optional(fields, "resourceFilter", self.resource_filter.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            subject_filter: optional_string(fields, "subjectFilter"),
            resource_filter: optional_string(fields, "resourceFilter"),
        })
    }
}

impl_header_field_accessors!(
    ListAclsRequestHeader,
    optional_string,
    subject_filter,
    [get_subject_filter],
    [set_subject_filter]
);

impl_header_field_accessors!(
    ListAclsRequestHeader,
    optional_string,
    resource_filter,
    [get_resource_filter],
    [set_resource_filter]
);

/// Pinned Java auth `ListUsersRequestHeader`. Java's `checkFields()` is a no-op,
/// so `filter` remains nullable at the command-header boundary.
#[derive(Debug, Clone, Default)]
pub struct ListUsersRequestHeader {
    filter: Option<String>,
}

impl CommandHeader for ListUsersRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "filter", self.filter.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            filter: optional_string(fields, "filter"),
        })
    }
}

impl_header_field_accessors!(
    ListUsersRequestHeader,
    optional_string,
    filter,
    [get_filter],
    [set_filter]
);

/// Pinned Java `CreateTopicRequestHeader` used by `UPDATE_AND_CREATE_TOPIC`.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::CreateTopicRequestHeader;
/// let _ = CreateTopicRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone)]
pub struct CreateTopicRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) default_topic: Option<String>,
    pub(crate) read_queue_nums: Option<i32>,
    pub(crate) write_queue_nums: Option<i32>,
    pub(crate) perm: Option<i32>,
    pub(crate) topic_filter_type: Option<String>,
    pub(crate) topic_sys_flag: Option<i32>,
    pub(crate) order: Option<bool>,
    pub(crate) attributes: Option<String>,
    pub(crate) force: Option<bool>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl Default for CreateTopicRequestHeader {
    fn default() -> Self {
        Self {
            topic: None,
            default_topic: None,
            read_queue_nums: None,
            write_queue_nums: None,
            perm: None,
            topic_filter_type: None,
            topic_sys_flag: None,
            order: Some(false),
            attributes: None,
            force: Some(false),
            namespace: None,
            namespaced: None,
            broker_name: None,
            oneway: None,
            logical: None,
        }
    }
}

impl CreateTopicRequestHeader {
    /// Resolves Java's `getTopicFilterTypeEnum()` and its `valueOf` failure.
    pub fn get_topic_filter_type_enum(&self) -> Result<TopicFilterType, ProtocolError> {
        match self.topic_filter_type.as_deref() {
            Some("SINGLE_TAG") => Ok(TopicFilterType::SingleTag),
            Some("MULTI_TAG") => Ok(TopicFilterType::MultiTag),
            value => Err(ProtocolError::InvalidField {
                field: "topicFilterType",
                reason: format!("unknown Java TopicFilterType {}", value.unwrap_or("null")),
            }),
        }
    }
}

impl CommandHeader for CreateTopicRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "defaultTopic", self.default_topic.as_deref());
        put_optional(fields, "readQueueNums", self.read_queue_nums);
        put_optional(fields, "writeQueueNums", self.write_queue_nums);
        put_optional(fields, "perm", self.perm);
        put_optional(fields, "topicFilterType", self.topic_filter_type.as_deref());
        put_optional(fields, "topicSysFlag", self.topic_sys_flag);
        put_optional(fields, "order", self.order);
        put_optional(fields, "attributes", self.attributes.as_deref());
        put_optional(fields, "force", self.force);
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        let header = Self {
            topic: optional_string(fields, "topic"),
            default_topic: optional_string(fields, "defaultTopic"),
            read_queue_nums: fields
                .get("readQueueNums")
                .and_then(|value| value.parse::<i32>().ok()),
            write_queue_nums: fields
                .get("writeQueueNums")
                .and_then(|value| value.parse::<i32>().ok()),
            perm: fields
                .get("perm")
                .and_then(|value| value.parse::<i32>().ok()),
            topic_filter_type: optional_string(fields, "topicFilterType"),
            topic_sys_flag: fields
                .get("topicSysFlag")
                .and_then(|value| value.parse::<i32>().ok()),
            order: Some(
                fields
                    .get("order")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            ),
            attributes: optional_string(fields, "attributes"),
            force: Some(
                fields
                    .get("force")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            ),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        };
        header.check_fields()?;
        Ok(header)
    }

    fn check_fields(&self) -> Result<(), ProtocolError> {
        self.get_topic_filter_type_enum().map(|_| ())
    }
}

/// Pinned Java `QueryMessageRequestHeader` used by `QUERY_MESSAGE`.
/// Pinned Java `QueryMessageRequestHeader` keeps its String, Integer, and Long
/// values nullable; its `checkFields()` method does not require them.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryMessageRequestHeader;
/// let _ = QueryMessageRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryMessageRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) max_num: Option<i32>,
    pub(crate) begin_timestamp: Option<i64>,
    pub(crate) end_timestamp: Option<i64>,
    pub(crate) index_type: Option<String>,
    pub(crate) last_key: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl CommandHeader for QueryMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "key", self.key.as_deref());
        put_optional(fields, "maxNum", self.max_num);
        put_optional(fields, "beginTimestamp", self.begin_timestamp);
        put_optional(fields, "endTimestamp", self.end_timestamp);
        put_optional(fields, "indexType", self.index_type.as_deref());
        put_optional(fields, "lastKey", self.last_key.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            key: optional_string(fields, "key"),
            max_num: optional_i32_with_mode(fields, "maxNum", false)?,
            begin_timestamp: optional_i64_with_mode(fields, "beginTimestamp", false)?,
            end_timestamp: optional_i64_with_mode(fields, "endTimestamp", false)?,
            index_type: optional_string(fields, "indexType"),
            last_key: optional_string(fields, "lastKey"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Self::decode_fields(fields)
    }
}

/// Pinned Java `ViewMessageRequestHeader` keeps its String and Long values
/// nullable and inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ViewMessageRequestHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<ViewMessageRequestHeader>(); }
/// ```
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ViewMessageRequestHeader;
/// let _ = ViewMessageRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ViewMessageRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) offset: Option<i64>,
}

impl CommandHeader for ViewMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Self::decode_fields(fields)
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetConsumerListByGroupRequestHeader;
/// let _ = GetConsumerListByGroupRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetConsumerListByGroupRequestHeader {
    pub(crate) consumer_group: String,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::LockBatchMqRequestHeader;
/// let _ = LockBatchMqRequestHeader::default().namespace;
/// ```
#[derive(Debug, Clone, Default)]
pub struct LockBatchMqRequestHeader {
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for LockBatchMqRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::UnlockBatchMqRequestHeader;
/// let _ = UnlockBatchMqRequestHeader::default().namespace;
/// ```
#[derive(Debug, Clone, Default)]
pub struct UnlockBatchMqRequestHeader {
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for UnlockBatchMqRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

impl CommandHeader for GetConsumerListByGroupRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ConsumerSendMsgBackRequestHeader;
/// let _ = ConsumerSendMsgBackRequestHeader::default().group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ConsumerSendMsgBackRequestHeader {
    pub(crate) group: String,
    pub(crate) broker_name: Option<String>,
    pub(crate) origin_topic: String,
    pub(crate) offset: i64,
    pub(crate) delay_level: i32,
    pub(crate) origin_msg_id: Option<String>,
    pub(crate) max_reconsume_times: Option<i32>,
    pub(crate) unit_mode: bool,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for ConsumerSendMsgBackRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("group".into(), self.group.clone());
        put_optional(fields, "bname", self.broker_name.as_deref());
        fields.insert("originTopic".into(), self.origin_topic.clone());
        fields.insert("offset".into(), self.offset.to_string());
        fields.insert("delayLevel".into(), self.delay_level.to_string());
        put_optional(fields, "originMsgId", self.origin_msg_id.as_deref());
        put_optional(fields, "maxReconsumeTimes", self.max_reconsume_times);
        fields.insert("unitMode".into(), self.unit_mode.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            group: required_string(fields, "group")?.to_owned(),
            broker_name: optional_string(fields, "bname"),
            origin_topic: required_string(fields, "originTopic")?.to_owned(),
            offset: required_i64(fields, "offset")?,
            delay_level: required_i32(fields, "delayLevel")?,
            origin_msg_id: optional_string(fields, "originMsgId"),
            max_reconsume_times: optional_i32(fields, "maxReconsumeTimes")?,
            unit_mode: fields
                .get("unitMode")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            group: required_string(fields, "group")?.to_owned(),
            broker_name: optional_string(fields, "bname"),
            origin_topic: required_string(fields, "originTopic")?.to_owned(),
            offset: required_i64(fields, "offset")?,
            delay_level: required_i32(fields, "delayLevel")?,
            origin_msg_id: optional_string(fields, "originMsgId"),
            max_reconsume_times: optional_i32_with_mode(fields, "maxReconsumeTimes", false)?,
            unit_mode: fields
                .get("unitMode")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        })
    }
}

/// Java 5.5.1 `RecallMessageRequestHeader`, including nullable fields inherited
/// from `RpcRequestHeader` and `TopicRequestHeader`.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::RecallMessageRequestHeader;
/// let _ = RecallMessageRequestHeader::default().producer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct RecallMessageRequestHeader {
    pub(crate) producer_group: Option<String>,
    pub(crate) topic: String,
    pub(crate) recall_handle: String,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

impl CommandHeader for RecallMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("recallHandle".into(), self.recall_handle.clone());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            producer_group: optional_string(fields, "producerGroup"),
            topic: required_string(fields, "topic")?.to_owned(),
            recall_handle: required_string(fields, "recallHandle")?.to_owned(),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::EndTransactionRequestHeader;
/// let _ = EndTransactionRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone)]
pub struct EndTransactionRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) producer_group: Option<String>,
    pub(crate) transaction_state_table_offset: Option<i64>,
    pub(crate) commit_log_offset: Option<i64>,
    pub(crate) commit_or_rollback: Option<i32>,
    pub(crate) from_transaction_check: Option<bool>,
    pub(crate) msg_id: Option<String>,
    pub(crate) transaction_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl Default for EndTransactionRequestHeader {
    fn default() -> Self {
        Self {
            topic: None,
            producer_group: None,
            transaction_state_table_offset: None,
            commit_log_offset: None,
            commit_or_rollback: None,
            from_transaction_check: Some(false),
            msg_id: None,
            transaction_id: None,
            namespace: None,
            namespaced: None,
            broker_name: None,
            oneway: None,
        }
    }
}

impl CommandHeader for EndTransactionRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        put_optional(
            fields,
            "tranStateTableOffset",
            self.transaction_state_table_offset,
        );
        put_optional(fields, "commitLogOffset", self.commit_log_offset);
        put_optional(fields, "commitOrRollback", self.commit_or_rollback);
        put_optional(fields, "fromTransactionCheck", self.from_transaction_check);
        put_optional(fields, "msgId", self.msg_id.as_deref());
        put_optional(fields, "transactionId", self.transaction_id.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        let header = Self {
            topic: optional_string(fields, "topic"),
            producer_group: optional_string(fields, "producerGroup"),
            transaction_state_table_offset: fields
                .get("tranStateTableOffset")
                .and_then(|value| value.parse::<i64>().ok()),
            commit_log_offset: fields
                .get("commitLogOffset")
                .and_then(|value| value.parse::<i64>().ok()),
            commit_or_rollback: fields
                .get("commitOrRollback")
                .and_then(|value| value.parse::<i32>().ok()),
            from_transaction_check: Some(
                fields
                    .get("fromTransactionCheck")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            ),
            msg_id: optional_string(fields, "msgId"),
            transaction_id: optional_string(fields, "transactionId"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        };
        header.check_fields()?;
        Ok(header)
    }

    fn check_fields(&self) -> Result<(), ProtocolError> {
        if matches!(self.commit_or_rollback, Some(0 | 8 | 12)) {
            Ok(())
        } else {
            Err(ProtocolError::InvalidField {
                field: "commitOrRollback",
                reason: "value invalid".into(),
            })
        }
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::CheckTransactionStateRequestHeader;
/// let _ = CheckTransactionStateRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct CheckTransactionStateRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) transaction_state_table_offset: i64,
    pub(crate) commit_log_offset: i64,
    pub(crate) msg_id: Option<String>,
    pub(crate) transaction_id: Option<String>,
    pub(crate) offset_msg_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for CheckTransactionStateRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert(
            "tranStateTableOffset".into(),
            self.transaction_state_table_offset.to_string(),
        );
        fields.insert("commitLogOffset".into(), self.commit_log_offset.to_string());
        put_optional(fields, "msgId", self.msg_id.as_deref());
        put_optional(fields, "transactionId", self.transaction_id.as_deref());
        put_optional(fields, "offsetMsgId", self.offset_msg_id.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            transaction_state_table_offset: required_i64(fields, "tranStateTableOffset")?,
            commit_log_offset: required_i64(fields, "commitLogOffset")?,
            msg_id: optional_string(fields, "msgId"),
            transaction_id: optional_string(fields, "transactionId"),
            offset_msg_id: optional_string(fields, "offsetMsgId"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ReplyMessageRequestHeader;
/// let _ = ReplyMessageRequestHeader::default().producer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReplyMessageRequestHeader {
    pub(crate) producer_group: String,
    pub(crate) topic: String,
    pub(crate) default_topic: String,
    pub(crate) default_topic_queue_nums: i32,
    pub(crate) queue_id: i32,
    pub(crate) sys_flag: i32,
    pub(crate) born_timestamp: i64,
    pub(crate) flag: i32,
    pub(crate) properties: Option<String>,
    pub(crate) reconsume_times: Option<i32>,
    pub(crate) unit_mode: bool,
    pub(crate) born_host: String,
    pub(crate) store_host: String,
    pub(crate) store_timestamp: i64,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::NotificationRequestHeader;
/// let _ = NotificationRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct NotificationRequestHeader {
    pub(crate) consumer_group: String,
    pub(crate) topic: String,
    pub(crate) queue_id: i32,
    pub(crate) poll_time: i64,
    pub(crate) born_time: i64,
    pub(crate) order: Option<bool>,
    pub(crate) attempt_id: Option<String>,
    pub(crate) lite_consumer: bool,
    pub(crate) client_id: Option<String>,
    pub(crate) expression_type: Option<String>,
    pub(crate) expression: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for NotificationRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("pollTime".into(), self.poll_time.to_string());
        fields.insert("bornTime".into(), self.born_time.to_string());
        put_optional(fields, "order", self.order);
        put_optional(fields, "attemptId", self.attempt_id.as_deref());
        fields.insert("isLiteConsumer".into(), self.lite_consumer.to_string());
        put_optional(fields, "clientId", self.client_id.as_deref());
        put_optional(fields, "expType", self.expression_type.as_deref());
        put_optional(fields, "exp", self.expression.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            poll_time: required_i64(fields, "pollTime")?,
            born_time: required_i64(fields, "bornTime")?,
            order: optional_java_boolean(fields, "order"),
            attempt_id: optional_string(fields, "attemptId"),
            lite_consumer: java_boolean(fields, "isLiteConsumer"),
            client_id: optional_string(fields, "clientId"),
            expression_type: optional_string(fields, "expType"),
            expression: optional_string(fields, "exp"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ResetOffsetRequestHeader;
/// let _ = ResetOffsetRequestHeader::default().group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ResetOffsetRequestHeader {
    pub(crate) group: String,
    pub(crate) topic: String,
    pub(crate) queue_id: i32,
    pub(crate) offset: Option<i64>,
    pub(crate) timestamp: i64,
    pub(crate) force: bool,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for ResetOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("group".into(), self.group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        put_optional(fields, "offset", self.offset);
        fields.insert("timestamp".into(), self.timestamp.to_string());
        fields.insert("isForce".into(), self.force.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            group: required_string(fields, "group")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: optional_i32(fields, "queueId")?.unwrap_or(-1),
            offset: optional_i64(fields, "offset")?,
            timestamp: required_i64(fields, "timestamp")?,
            force: java_boolean(fields, "isForce"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            group: required_string(fields, "group")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?.unwrap_or(-1),
            offset: optional_i64_with_mode(fields, "offset", false)?,
            timestamp: required_i64(fields, "timestamp")?,
            force: java_boolean(fields, "isForce"),
        })
    }
}

/// Java 5.5.1's `ResetMasterFlushOffsetHeader` carries a nullable boxed `Long`.
/// Reflection decoding leaves a missing or malformed value unset, and the
/// pinned `checkFields()` method performs no validation.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ResetMasterFlushOffsetHeader;
/// let _ = ResetMasterFlushOffsetHeader::default().master_flush_offset;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ResetMasterFlushOffsetHeader {
    pub(crate) master_flush_offset: Option<i64>,
}

impl ResetMasterFlushOffsetHeader {
    pub fn get_master_flush_offset(&self) -> Option<i64> {
        self.master_flush_offset
    }

    pub fn set_master_flush_offset(&mut self, value: Option<i64>) {
        self.master_flush_offset = value;
    }
}

impl CommandHeader for ResetMasterFlushOffsetHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "masterFlushOffset", self.master_flush_offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, false)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            master_flush_offset: optional_i64_with_mode(fields, "masterFlushOffset", false)?,
        })
    }
}

/// Java `ExportRocksDBConfigToJsonRequestHeader`, whose nullable config field
/// is serialized without validation by `checkFields()`.
#[derive(Debug, Clone, Default)]
pub struct ExportRocksDBConfigToJsonRequestHeader {
    pub(crate) config_type: Option<String>,
}

impl ExportRocksDBConfigToJsonRequestHeader {
    pub fn fetch_config_type(&self) -> Result<Vec<ExportRocksDBConfigType>, ProtocolError> {
        let value = self
            .config_type
            .as_deref()
            .ok_or(ProtocolError::MissingField("configType"))?;
        ExportRocksDBConfigType::from_string(value)
    }

    pub fn update_config_type(&mut self, config_types: &[ExportRocksDBConfigType]) {
        self.config_type = Some(ExportRocksDBConfigType::to_string(config_types));
    }

    pub fn get_config_type(&self) -> Option<&str> {
        self.config_type.as_deref()
    }

    pub fn set_config_type(&mut self, value: Option<String>) {
        self.config_type = value;
    }
}

/// Pinned Java `CheckRocksdbCqWriteProgressRequestHeader`. The annotated
/// topic remains nullable because Java's `checkFields()` is a no-op.
#[derive(Debug, Clone, Default)]
pub struct CheckRocksdbCqWriteProgressRequestHeader {
    pub(crate) topic: Option<String>,
    pub(crate) check_store_time: i64,
}

impl CheckRocksdbCqWriteProgressRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }

    pub fn get_check_store_time(&self) -> i64 {
        self.check_store_time
    }

    pub fn set_check_store_time(&mut self, value: i64) {
        self.check_store_time = value;
    }
}

impl CommandHeader for CheckRocksdbCqWriteProgressRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert(
            "checkStoreTime".to_owned(),
            self.check_store_time.to_string(),
        );
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            check_store_time: optional_i64_with_mode(fields, "checkStoreTime", false)?
                .unwrap_or_default(),
        })
    }
}

impl CommandHeader for ExportRocksDBConfigToJsonRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "configType", self.config_type.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            config_type: optional_string(fields, "configType"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetConsumerStatusRequestHeader;
/// let _ = GetConsumerStatusRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetConsumerStatusRequestHeader {
    pub(crate) topic: String,
    pub(crate) group: String,
    pub(crate) client_addr: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for GetConsumerStatusRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("group".into(), self.group.clone());
        put_optional(fields, "clientAddr", self.client_addr.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            group: required_string(fields, "group")?.to_owned(),
            client_addr: optional_string(fields, "clientAddr"),
        })
    }
}

/// Pinned Java `GetConsumerConnectionListRequestHeader`. Java's
/// `checkFields()` is a no-op, so the annotated group remains nullable.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetConsumerConnectionListRequestHeader;
/// let _ = GetConsumerConnectionListRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetConsumerConnectionListRequestHeader {
    pub(crate) consumer_group: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for GetConsumerConnectionListRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_group: optional_string(fields, "consumerGroup"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Pinned Java `GetConsumeStatsRequestHeader`, including its no-op validation
/// and the inherited nullable `TopicRequestHeader`/`RpcRequestHeader` fields.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetConsumeStatsRequestHeader;
/// let _ = GetConsumeStatsRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetConsumeStatsRequestHeader {
    pub(crate) consumer_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) topic_list: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl GetConsumeStatsRequestHeader {
    pub fn fetch_topic_list(&self) -> Vec<String> {
        if self
            .topic_list
            .as_deref()
            .is_none_or(|topic_list| topic_list.trim().is_empty())
        {
            return Vec::new();
        }
        self.topic_list
            .as_deref()
            .unwrap_or_default()
            .split(';')
            .filter(|topic| !topic.is_empty())
            .map(str::to_owned)
            .collect()
    }

    pub fn update_topic_list(&mut self, topics: Option<&[Option<String>]>) {
        let Some(topics) = topics.filter(|topics| !topics.is_empty()) else {
            return;
        };
        let mut value = String::new();
        for topic in topics {
            value.push_str(topic.as_deref().unwrap_or("null"));
            value.push(';');
        }
        self.topic_list = Some(value);
    }
}

impl CommandHeader for GetConsumeStatsRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "topicList", self.topic_list.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            topic_list: optional_string(fields, "topicList"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Pinned Java `GetConsumeStatsInBrokerHeader`. Its `checkFields()` is a
/// no-op, and Java emits the primitive `isOrder` field even when false.
#[derive(Debug, Clone, Default)]
pub struct GetConsumeStatsInBrokerRequestHeader {
    is_order: bool,
}

impl GetConsumeStatsInBrokerRequestHeader {
    pub fn is_order(&self) -> bool {
        self.is_order
    }

    pub fn set_is_order(&mut self, value: bool) {
        self.is_order = value;
    }
}

impl CommandHeader for GetConsumeStatsInBrokerRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("isOrder".to_owned(), self.is_order.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            is_order: optional_java_boolean(fields, "isOrder").unwrap_or(false),
        })
    }
}

/// Pinned Java `ViewBrokerStatsDataRequestHeader`. Java's `checkFields()` is
/// a no-op, so both annotated statistics identifiers remain nullable.
#[derive(Debug, Clone, Default)]
pub struct ViewBrokerStatsDataRequestHeader {
    stats_name: Option<String>,
    stats_key: Option<String>,
}

impl ViewBrokerStatsDataRequestHeader {
    pub fn get_stats_name(&self) -> Option<&str> {
        self.stats_name.as_deref()
    }

    pub fn set_stats_name(&mut self, value: Option<String>) {
        self.stats_name = value;
    }

    pub fn get_stats_key(&self) -> Option<&str> {
        self.stats_key.as_deref()
    }

    pub fn set_stats_key(&mut self, value: Option<String>) {
        self.stats_key = value;
    }
}

impl CommandHeader for ViewBrokerStatsDataRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "statsName", self.stats_name.as_deref());
        put_optional(fields, "statsKey", self.stats_key.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            stats_name: optional_string(fields, "statsName"),
            stats_key: optional_string(fields, "statsKey"),
        })
    }
}

/// Pinned Java `GetConsumerRunningInfoRequestHeader`, including the inherited
/// `RpcRequestHeader` namespace and broker-routing fields.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetConsumerRunningInfoRequestHeader;
/// let _ = GetConsumerRunningInfoRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetConsumerRunningInfoRequestHeader {
    pub(crate) consumer_group: String,
    pub(crate) client_id: String,
    pub(crate) jstack_enable: bool,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for GetConsumerRunningInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("clientId".into(), self.client_id.clone());
        fields.insert("jstackEnable".into(), self.jstack_enable.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            client_id: required_string(fields, "clientId")?.to_owned(),
            jstack_enable: java_boolean(fields, "jstackEnable"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ConsumeMessageDirectlyResultRequestHeader;
/// let _ = ConsumeMessageDirectlyResultRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ConsumeMessageDirectlyResultRequestHeader {
    pub(crate) consumer_group: String,
    pub(crate) client_id: Option<String>,
    pub(crate) msg_id: Option<String>,
    pub(crate) broker_name: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) topic_sys_flag: Option<i32>,
    pub(crate) group_sys_flag: Option<i32>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    /// The inherited `RpcRequestHeader.bname`; distinct from this class's
    /// `brokerName` payload field.
    pub(crate) rpc_broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
    pub(crate) logical: Option<bool>,
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopMessageRequestHeader;
/// let _ = PopMessageRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone)]
pub struct PopMessageRequestHeader {
    pub(crate) consumer_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) queue_id: i32,
    pub(crate) max_msg_nums: i32,
    pub(crate) invisible_time: i64,
    pub(crate) poll_time: i64,
    pub(crate) born_time: i64,
    pub(crate) init_mode: i32,
    pub(crate) expression_type: Option<String>,
    pub(crate) expression: Option<String>,
    pub(crate) order: Option<bool>,
    pub(crate) attempt_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl Default for PopMessageRequestHeader {
    fn default() -> Self {
        Self {
            consumer_group: None,
            topic: None,
            queue_id: 0,
            max_msg_nums: 0,
            invisible_time: 0,
            poll_time: 0,
            born_time: 0,
            init_mode: 0,
            expression_type: None,
            expression: None,
            order: Some(false),
            attempt_id: None,
            namespace: None,
            namespaced: None,
            broker_name: None,
            oneway: None,
        }
    }
}

impl CommandHeader for PopMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("maxMsgNums".into(), self.max_msg_nums.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("pollTime".into(), self.poll_time.to_string());
        fields.insert("bornTime".into(), self.born_time.to_string());
        fields.insert("initMode".into(), self.init_mode.to_string());
        put_optional(fields, "expType", self.expression_type.as_deref());
        put_optional(fields, "exp", self.expression.as_deref());
        put_optional(fields, "order", self.order);
        put_optional(fields, "attemptId", self.attempt_id.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, false)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?.unwrap_or_default(),
            max_msg_nums: optional_i32_with_mode(fields, "maxMsgNums", false)?.unwrap_or_default(),
            invisible_time: optional_i64_with_mode(fields, "invisibleTime", false)?
                .unwrap_or_default(),
            poll_time: optional_i64_with_mode(fields, "pollTime", false)?.unwrap_or_default(),
            born_time: optional_i64_with_mode(fields, "bornTime", false)?.unwrap_or_default(),
            init_mode: optional_i32_with_mode(fields, "initMode", false)?.unwrap_or_default(),
            expression_type: optional_string(fields, "expType"),
            expression: optional_string(fields, "exp"),
            order: Some(optional_java_boolean(fields, "order").unwrap_or(false)),
            attempt_id: optional_string(fields, "attemptId"),
        })
    }
}

impl PopMessageRequestHeader {
    /// Sets the backing queue ID exposed through Java's public setter.
    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    /// Returns Java's `getQueueId()` value, which normalizes every negative
    /// backing field to `-1`.
    pub fn get_queue_id(&self) -> i32 {
        if self.queue_id < 0 { -1 } else { self.queue_id }
    }

    /// Mirrors Java's nullable `getOrder()` result.
    pub fn get_order(&self) -> Option<bool> {
        self.order
    }

    /// Mirrors Java's `isOrder()` null-to-false conversion.
    pub fn is_order(&self) -> bool {
        self.order.unwrap_or(false)
    }

    /// Sets the nullable Java `order` value.
    pub fn set_order(&mut self, order: Option<bool>) {
        self.order = order;
    }

    /// Mirrors Java's `isTimeoutTooMuch()` helper using the system wall clock.
    pub fn is_timeout_too_much(&self) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.is_timeout_too_much_at(now_millis)
    }

    /// Returns the pinned Java header's `toString()` representation.
    pub fn java_to_string(&self) -> String {
        format!(
            "PopMessageRequestHeader{{consumerGroup={}, topic={}, queueId={}, maxMsgNums={}, invisibleTime={}, pollTime={}, bornTime={}, initMode={}, expType={}, exp={}, order={}, attemptId={}}}",
            java_nullable_string(self.consumer_group.as_deref()),
            java_nullable_string(self.topic.as_deref()),
            self.queue_id,
            self.max_msg_nums,
            self.invisible_time,
            self.poll_time,
            self.born_time,
            self.init_mode,
            java_nullable_string(self.expression_type.as_deref()),
            java_nullable_string(self.expression.as_deref()),
            java_nullable_bool(self.order),
            java_nullable_string(self.attempt_id.as_deref()),
        )
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::AckMessageRequestHeader;
/// let _ = AckMessageRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct AckMessageRequestHeader {
    pub(crate) consumer_group: String,
    pub(crate) topic: String,
    pub(crate) queue_id: i32,
    pub(crate) extra_info: String,
    pub(crate) offset: i64,
    pub(crate) lite_topic: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for AckMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("extraInfo".into(), self.extra_info.clone());
        fields.insert("offset".into(), self.offset.to_string());
        put_optional(fields, "liteTopic", self.lite_topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            extra_info: required_string(fields, "extraInfo")?.to_owned(),
            offset: required_i64(fields, "offset")?,
            lite_topic: optional_string(fields, "liteTopic"),
        })
    }
}

/// Java's `ChangeInvisibleTimeRequestHeader` uses nullable strings and boxed
/// numeric fields; invalid reflection conversions leave those fields unset.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ChangeInvisibleTimeRequestHeader;
/// let _ = ChangeInvisibleTimeRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ChangeInvisibleTimeRequestHeader {
    pub(crate) consumer_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) extra_info: Option<String>,
    pub(crate) offset: Option<i64>,
    pub(crate) invisible_time: Option<i64>,
    pub(crate) lite_topic: Option<String>,
    pub(crate) suspend: bool,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PollingInfoRequestHeader;
/// let _ = PollingInfoRequestHeader::default().consumer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct PollingInfoRequestHeader {
    pub(crate) consumer_group: String,
    pub(crate) topic: String,
    pub(crate) queue_id: i32,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for PollingInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
        })
    }
}

/// Java 5.5.1's `PopLiteMessageRequestHeader` uses reflection decoding and
/// inherits equality/hash behavior from `RpcRequestHeader`.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopLiteMessageRequestHeader;
/// let _ = PopLiteMessageRequestHeader::default().client_id;
/// ```
#[derive(Debug, Clone, Default)]
pub struct PopLiteMessageRequestHeader {
    pub(crate) client_id: Option<String>,
    pub(crate) consumer_group: Option<String>,
    pub(crate) topic: Option<String>,
    pub(crate) max_msg_num: i32,
    pub(crate) invisible_time: i64,
    pub(crate) poll_time: i64,
    pub(crate) born_time: i64,
    pub(crate) attempt_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for PopLiteMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "clientId", self.client_id.as_deref());
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert("maxMsgNum".into(), self.max_msg_num.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("pollTime".into(), self.poll_time.to_string());
        fields.insert("bornTime".into(), self.born_time.to_string());
        put_optional(fields, "attemptId", self.attempt_id.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, false)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            client_id: optional_string(fields, "clientId"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            max_msg_num: optional_i32_with_mode(fields, "maxMsgNum", false)?.unwrap_or_default(),
            invisible_time: optional_i64_with_mode(fields, "invisibleTime", false)?
                .unwrap_or_default(),
            poll_time: optional_i64_with_mode(fields, "pollTime", false)?.unwrap_or_default(),
            born_time: optional_i64_with_mode(fields, "bornTime", false)?.unwrap_or_default(),
            attempt_id: optional_string(fields, "attemptId"),
        })
    }
}

impl PopLiteMessageRequestHeader {
    /// Mirrors Java's `isTimeoutTooMuch()` helper using the system wall clock.
    pub fn is_timeout_too_much(&self) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.is_timeout_too_much_at(now_millis)
    }

    /// Returns the pinned Java header's `toString()` representation.
    pub fn java_to_string(&self) -> String {
        format!(
            "PopLiteMessageRequestHeader{{consumerGroup={}, topic={}, maxMsgNum={}, invisibleTime={}, pollTime={}, bornTime={}, attemptId={}, clientId={}}}",
            java_nullable_string(self.consumer_group.as_deref()),
            java_nullable_string(self.topic.as_deref()),
            self.max_msg_num,
            self.invisible_time,
            self.poll_time,
            self.born_time,
            java_nullable_string(self.attempt_id.as_deref()),
            java_nullable_string(self.client_id.as_deref()),
        )
    }
}

impl LiteSubscriptionCtlRequestHeader {
    /// Returns the integer produced by Java `Objects.hash(ns, nsd, bname,
    /// oway)` in `RpcRequestHeader.hashCode()`.
    pub fn java_hash_code(&self) -> i32 {
        let mut hash = 1_i32;
        for value_hash in [
            self.namespace
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
            self.namespaced.map(java_boolean_hash_code).unwrap_or(0),
            self.broker_name
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
            self.oneway.map(java_boolean_hash_code).unwrap_or(0),
        ] {
            hash = hash.wrapping_mul(31).wrapping_add(value_hash);
        }
        hash
    }

    /// Returns Java `RpcRequestHeader.toString()` output.
    pub fn java_to_string(&self) -> String {
        format!(
            "LiteSubscriptionCtlRequestHeader{{namespace={}, namespaced={}, brokerName={}, oneway={}}}",
            java_nullable_string(self.namespace.as_deref()),
            java_nullable_bool(self.namespaced),
            java_nullable_string(self.broker_name.as_deref()),
            java_nullable_bool(self.oneway),
        )
    }
}

impl CommandHeader for LiteSubscriptionCtlRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Java `GetLiteTopicInfoRequestHeader` for the asynchronous Lite topic-info RPC.
/// Its `checkFields()` is a no-op even though `parentTopic` carries a
/// `CFNotNull` annotation, so both fields remain nullable at the wire boundary.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetLiteTopicInfoRequestHeader;
/// let _ = GetLiteTopicInfoRequestHeader::default().parent_topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetLiteTopicInfoRequestHeader {
    parent_topic: Option<String>,
    lite_topic: Option<String>,
}

impl CommandHeader for GetLiteTopicInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "parentTopic", self.parent_topic.clone());
        put_optional(fields, "liteTopic", self.lite_topic.clone());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            parent_topic: fields.get("parentTopic").cloned(),
            lite_topic: fields.get("liteTopic").cloned(),
        })
    }
}

impl GetLiteTopicInfoRequestHeader {
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
}

/// Java `GetParentTopicInfoRequestHeader` for the synchronous Lite parent
/// topic lookup. The topic is nullable because Java's `checkFields()` is a
/// no-op despite the `CFNotNull` annotation.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetParentTopicInfoRequestHeader;
/// let _ = GetParentTopicInfoRequestHeader::default().topic;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetParentTopicInfoRequestHeader {
    topic: Option<String>,
}

impl CommandHeader for GetParentTopicInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.clone());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: fields.get("topic").cloned(),
        })
    }
}

impl GetParentTopicInfoRequestHeader {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, value: Option<String>) {
        self.topic = value;
    }
}

/// Java `GetLiteClientInfoRequestHeader`. Java validates only `maxCount`,
/// which defaults to 1000; annotated `parentTopic` and `group` fields remain
/// nullable at this header boundary.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetLiteClientInfoRequestHeader;
/// let _ = GetLiteClientInfoRequestHeader::default().max_count;
/// ```
#[derive(Debug, Clone)]
pub struct GetLiteClientInfoRequestHeader {
    parent_topic: Option<String>,
    group: Option<String>,
    client_id: Option<String>,
    max_count: i32,
}

impl Default for GetLiteClientInfoRequestHeader {
    fn default() -> Self {
        Self {
            parent_topic: None,
            group: None,
            client_id: None,
            max_count: 1000,
        }
    }
}

impl CommandHeader for GetLiteClientInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "parentTopic", self.parent_topic.clone());
        put_optional(fields, "group", self.group.clone());
        put_optional(fields, "clientId", self.client_id.clone());
        fields.insert("maxCount".into(), self.max_count.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        let max_count = fields
            .get("maxCount")
            .map(|value| {
                value
                    .parse::<i32>()
                    .map_err(|_| ProtocolError::InvalidField {
                        field: "maxCount",
                        reason: format!("not a Java int: {value}"),
                    })
            })
            .transpose()?
            .unwrap_or(1000);
        Ok(Self {
            parent_topic: fields.get("parentTopic").cloned(),
            group: fields.get("group").cloned(),
            client_id: fields.get("clientId").cloned(),
            max_count,
        })
    }

    fn check_fields(&self) -> Result<(), ProtocolError> {
        if self.max_count <= 0 {
            return Err(ProtocolError::InvalidField {
                field: "maxCount",
                reason: "[maxCount] field invalid".into(),
            });
        }
        Ok(())
    }
}

impl GetLiteClientInfoRequestHeader {
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

    pub fn get_max_count(&self) -> i32 {
        self.max_count
    }

    pub fn set_max_count(&mut self, value: i32) {
        self.max_count = value;
    }
}

/// Java `GetLiteGroupInfoRequestHeader`. The annotated `group` value remains
/// nullable because Java's `checkFields()` is a no-op; primitive `topK`
/// defaults to zero and is always encoded.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetLiteGroupInfoRequestHeader;
/// let _ = GetLiteGroupInfoRequestHeader::default().top_k;
/// ```
#[derive(Debug, Clone, Default)]
pub struct GetLiteGroupInfoRequestHeader {
    group: Option<String>,
    lite_topic: Option<String>,
    top_k: i32,
}

impl CommandHeader for GetLiteGroupInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "group", self.group.clone());
        put_optional(fields, "liteTopic", self.lite_topic.clone());
        fields.insert("topK".into(), self.top_k.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        let top_k = fields
            .get("topK")
            .map(|value| {
                value
                    .parse::<i32>()
                    .map_err(|_| ProtocolError::InvalidField {
                        field: "topK",
                        reason: format!("not a Java int: {value}"),
                    })
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            group: fields.get("group").cloned(),
            lite_topic: fields.get("liteTopic").cloned(),
            top_k,
        })
    }
}

impl GetLiteGroupInfoRequestHeader {
    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, value: Option<String>) {
        self.group = value;
    }

    pub fn get_lite_topic(&self) -> Option<&str> {
        self.lite_topic.as_deref()
    }

    pub fn set_lite_topic(&mut self, value: Option<String>) {
        self.lite_topic = value;
    }

    pub fn get_top_k(&self) -> i32 {
        self.top_k
    }

    pub fn set_top_k(&mut self, value: i32) {
        self.top_k = value;
    }
}

/// Java 5.5.1's client heartbeat RPC header. The Java class declares no
/// payload fields and inherits its namespace and routing fields from
/// `RpcRequestHeader`.
/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::HeartbeatRequestHeader;
/// let _ = HeartbeatRequestHeader::default().namespace;
/// ```
#[derive(Debug, Clone, Default)]
pub struct HeartbeatRequestHeader {
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl HeartbeatRequestHeader {
    #[deprecated(note = "use broker_name")]
    pub fn get_bname(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    #[deprecated(note = "use set_broker_name")]
    pub fn set_bname(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }

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

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }

    /// Returns the integer produced by Java `Objects.hash(ns, nsd, bname,
    /// oway)` in `RpcRequestHeader.hashCode()`.
    pub fn java_hash_code(&self) -> i32 {
        let mut hash = 1_i32;
        for value_hash in [
            self.namespace
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
            self.namespaced.map(java_boolean_hash_code).unwrap_or(0),
            self.broker_name
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
            self.oneway.map(java_boolean_hash_code).unwrap_or(0),
        ] {
            hash = hash.wrapping_mul(31).wrapping_add(value_hash);
        }
        hash
    }

    /// Returns Java `RpcRequestHeader.toString()` output.
    pub fn java_to_string(&self) -> String {
        format!(
            "HeartbeatRequestHeader{{namespace={}, namespaced={}, brokerName={}, oneway={}}}",
            java_nullable_string(self.namespace.as_deref()),
            java_nullable_bool(self.namespaced),
            java_nullable_string(self.broker_name.as_deref()),
            java_nullable_bool(self.oneway),
        )
    }
}

impl CommandHeader for HeartbeatRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

impl CommandHeader for NotifyUnsubscribeLiteRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("liteTopic".into(), self.lite_topic.clone());
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("clientId".into(), self.client_id.clone());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            lite_topic: required_string(fields, "liteTopic")?.to_owned(),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            client_id: required_string(fields, "clientId")?.to_owned(),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

impl CommandHeader for ChangeInvisibleTimeRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "extraInfo", self.extra_info.as_deref());
        put_optional(fields, "offset", self.offset);
        put_optional(fields, "invisibleTime", self.invisible_time);
        put_optional(fields, "liteTopic", self.lite_topic.as_deref());
        fields.insert("suspend".into(), self.suspend.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, false)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            extra_info: optional_string(fields, "extraInfo"),
            offset: optional_i64_with_mode(fields, "offset", false)?,
            invisible_time: optional_i64_with_mode(fields, "invisibleTime", false)?,
            lite_topic: optional_string(fields, "liteTopic"),
            suspend: java_boolean(fields, "suspend"),
        })
    }
}

impl CommandHeader for ConsumeMessageDirectlyResultRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        put_optional(fields, "clientId", self.client_id.as_deref());
        put_optional(fields, "msgId", self.msg_id.as_deref());
        put_optional(fields, "brokerName", self.broker_name.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "topicSysFlag", self.topic_sys_flag);
        put_optional(fields, "groupSysFlag", self.group_sys_flag);
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.rpc_broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            rpc_broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            client_id: optional_string(fields, "clientId"),
            msg_id: optional_string(fields, "msgId"),
            broker_name: optional_string(fields, "brokerName"),
            topic: optional_string(fields, "topic"),
            topic_sys_flag: optional_i32(fields, "topicSysFlag")?,
            group_sys_flag: optional_i32(fields, "groupSysFlag")?,
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            rpc_broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            client_id: optional_string(fields, "clientId"),
            msg_id: optional_string(fields, "msgId"),
            broker_name: optional_string(fields, "brokerName"),
            topic: optional_string(fields, "topic"),
            topic_sys_flag: optional_i32_with_mode(fields, "topicSysFlag", false)?,
            group_sys_flag: optional_i32_with_mode(fields, "groupSysFlag", false)?,
        })
    }
}

impl CommandHeader for ReplyMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("producerGroup".into(), self.producer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("defaultTopic".into(), self.default_topic.clone());
        fields.insert(
            "defaultTopicQueueNums".into(),
            self.default_topic_queue_nums.to_string(),
        );
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("sysFlag".into(), self.sys_flag.to_string());
        fields.insert("bornTimestamp".into(), self.born_timestamp.to_string());
        fields.insert("flag".into(), self.flag.to_string());
        put_optional(fields, "properties", self.properties.as_deref());
        put_optional(fields, "reconsumeTimes", self.reconsume_times);
        fields.insert("unitMode".into(), self.unit_mode.to_string());
        fields.insert("bornHost".into(), self.born_host.clone());
        fields.insert("storeHost".into(), self.store_host.clone());
        fields.insert("storeTimestamp".into(), self.store_timestamp.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            producer_group: required_string(fields, "producerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            default_topic: required_string(fields, "defaultTopic")?.to_owned(),
            default_topic_queue_nums: required_i32(fields, "defaultTopicQueueNums")?,
            queue_id: required_i32(fields, "queueId")?,
            sys_flag: required_i32(fields, "sysFlag")?,
            born_timestamp: required_i64(fields, "bornTimestamp")?,
            flag: required_i32(fields, "flag")?,
            properties: optional_string(fields, "properties"),
            reconsume_times: optional_i32(fields, "reconsumeTimes")?,
            unit_mode: java_boolean(fields, "unitMode"),
            born_host: required_string(fields, "bornHost")?.to_owned(),
            store_host: required_string(fields, "storeHost")?.to_owned(),
            store_timestamp: required_i64(fields, "storeTimestamp")?,
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            producer_group: required_string(fields, "producerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            default_topic: required_string(fields, "defaultTopic")?.to_owned(),
            default_topic_queue_nums: required_i32(fields, "defaultTopicQueueNums")?,
            queue_id: required_i32(fields, "queueId")?,
            sys_flag: required_i32(fields, "sysFlag")?,
            born_timestamp: required_i64(fields, "bornTimestamp")?,
            flag: required_i32(fields, "flag")?,
            properties: optional_string(fields, "properties"),
            reconsume_times: optional_i32_with_mode(fields, "reconsumeTimes", false)?,
            unit_mode: java_boolean(fields, "unitMode"),
            born_host: required_string(fields, "bornHost")?.to_owned(),
            store_host: required_string(fields, "storeHost")?.to_owned(),
            store_timestamp: required_i64(fields, "storeTimestamp")?,
        })
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SendMessageRequestHeaderV2;
/// let _ = SendMessageRequestHeaderV2::default().a;
/// ```
#[derive(Debug, Clone, Default)]
pub struct SendMessageRequestHeaderV2 {
    pub(crate) a: Option<String>,
    pub(crate) b: Option<String>,
    pub(crate) c: Option<String>,
    pub(crate) d: Option<i32>,
    pub(crate) e: Option<i32>,
    pub(crate) f: Option<i32>,
    pub(crate) g: Option<i64>,
    pub(crate) h: Option<i32>,
    pub(crate) i: Option<String>,
    pub(crate) j: Option<i32>,
    pub(crate) k: Option<bool>,
    pub(crate) l: Option<i32>,
    pub(crate) m: Option<bool>,
    pub(crate) n: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) logical: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for SendMessageRequestHeaderV2 {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "a", self.a.as_deref());
        put_optional(fields, "b", self.b.as_deref());
        put_optional(fields, "c", self.c.as_deref());
        put_optional(fields, "d", self.d);
        put_optional(fields, "e", self.e);
        put_optional(fields, "f", self.f);
        put_optional(fields, "g", self.g);
        put_optional(fields, "h", self.h);
        put_optional(fields, "i", self.i.as_deref());
        put_optional(fields, "j", self.j);
        put_optional(fields, "k", self.k);
        put_optional(fields, "l", self.l);
        put_optional(fields, "m", self.m);
        put_optional(fields, "n", self.n.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: (!use_fast_encode)
                .then(|| optional_string(fields, "ns"))
                .flatten(),
            namespaced: (!use_fast_encode)
                .then(|| optional_java_boolean(fields, "nsd"))
                .flatten(),
            logical: (!use_fast_encode)
                .then(|| optional_java_boolean(fields, "lo"))
                .flatten(),
            broker_name: (!use_fast_encode)
                .then(|| optional_string(fields, "bname"))
                .flatten(),
            oneway: (!use_fast_encode)
                .then(|| optional_java_boolean(fields, "oway"))
                .flatten(),
            a: optional_string(fields, "a"),
            b: optional_string(fields, "b"),
            c: optional_string(fields, "c"),
            d: optional_i32_with_mode(fields, "d", use_fast_encode)?,
            e: optional_i32_with_mode(fields, "e", use_fast_encode)?,
            f: optional_i32_with_mode(fields, "f", use_fast_encode)?,
            g: optional_i64_with_mode(fields, "g", use_fast_encode)?,
            h: optional_i32_with_mode(fields, "h", use_fast_encode)?,
            i: optional_string(fields, "i"),
            j: optional_i32_with_mode(fields, "j", use_fast_encode)?,
            k: optional_java_boolean(fields, "k"),
            l: optional_i32_with_mode(fields, "l", use_fast_encode)?,
            m: optional_java_boolean(fields, "m"),
            n: optional_string(fields, "n"),
        })
    }
}

impl SendMessageRequestHeaderV2 {
    pub fn from_v1(header: &SendMessageRequestHeaderV1) -> Self {
        Self {
            a: header.producer_group.clone(),
            b: Some(header.topic.clone()),
            c: Some(header.default_topic.clone()),
            d: Some(header.default_topic_queue_nums),
            e: Some(header.queue_id),
            f: Some(header.sys_flag),
            g: Some(header.born_timestamp),
            h: Some(header.flag),
            i: header.properties.clone(),
            j: header.reconsume_times,
            k: header.unit_mode,
            l: header.max_reconsume_times,
            m: header.batch,
            n: header.broker_name.clone(),
            ..Self::default()
        }
    }
}

/// Java 5.5.1 exposes this value through its accessor API, not as a public field.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SendMessageRequestHeaderV1;
/// let _ = SendMessageRequestHeaderV1::default().producer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct SendMessageRequestHeaderV1 {
    pub(crate) producer_group: Option<String>,
    pub(crate) topic: String,
    pub(crate) default_topic: String,
    pub(crate) default_topic_queue_nums: i32,
    pub(crate) queue_id: i32,
    pub(crate) sys_flag: i32,
    pub(crate) born_timestamp: i64,
    pub(crate) flag: i32,
    pub(crate) properties: Option<String>,
    pub(crate) reconsume_times: Option<i32>,
    pub(crate) unit_mode: Option<bool>,
    pub(crate) max_reconsume_times: Option<i32>,
    pub(crate) batch: Option<bool>,
    pub(crate) namespace: Option<String>,
    pub(crate) namespaced: Option<bool>,
    pub(crate) broker_name: Option<String>,
    pub(crate) logical: Option<bool>,
    pub(crate) oneway: Option<bool>,
}

impl CommandHeader for SendMessageRequestHeaderV1 {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("defaultTopic".into(), self.default_topic.clone());
        fields.insert(
            "defaultTopicQueueNums".into(),
            self.default_topic_queue_nums.to_string(),
        );
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("sysFlag".into(), self.sys_flag.to_string());
        fields.insert("bornTimestamp".into(), self.born_timestamp.to_string());
        fields.insert("flag".into(), self.flag.to_string());
        put_optional(fields, "properties", self.properties.as_deref());
        put_optional(fields, "reconsumeTimes", self.reconsume_times);
        put_optional(fields, "unitMode", self.unit_mode);
        put_optional(fields, "maxReconsumeTimes", self.max_reconsume_times);
        put_optional(fields, "batch", self.batch);
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            producer_group: optional_string(fields, "producerGroup"),
            topic: required_string(fields, "topic")?.to_owned(),
            default_topic: required_string(fields, "defaultTopic")?.to_owned(),
            default_topic_queue_nums: required_i32(fields, "defaultTopicQueueNums")?,
            queue_id: required_i32(fields, "queueId")?,
            sys_flag: required_i32(fields, "sysFlag")?,
            born_timestamp: required_i64(fields, "bornTimestamp")?,
            flag: required_i32(fields, "flag")?,
            properties: optional_string(fields, "properties"),
            reconsume_times: optional_i32(fields, "reconsumeTimes")?,
            unit_mode: optional_java_boolean(fields, "unitMode"),
            max_reconsume_times: optional_i32(fields, "maxReconsumeTimes")?,
            batch: optional_java_boolean(fields, "batch"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            producer_group: optional_string(fields, "producerGroup"),
            topic: required_string(fields, "topic")?.to_owned(),
            default_topic: required_string(fields, "defaultTopic")?.to_owned(),
            default_topic_queue_nums: required_i32(fields, "defaultTopicQueueNums")?,
            queue_id: required_i32(fields, "queueId")?,
            sys_flag: required_i32(fields, "sysFlag")?,
            born_timestamp: required_i64(fields, "bornTimestamp")?,
            flag: required_i32(fields, "flag")?,
            properties: optional_string(fields, "properties"),
            reconsume_times: optional_i32_with_mode(fields, "reconsumeTimes", false)?,
            unit_mode: optional_java_boolean(fields, "unitMode"),
            max_reconsume_times: optional_i32_with_mode(fields, "maxReconsumeTimes", false)?,
            batch: optional_java_boolean(fields, "batch"),
        })
    }
}

impl_topic_request_header_accessors!(
    PullMessageRequestHeader,
    CreateTopicRequestHeader,
    QueryMessageRequestHeader,
    QueryCorrectionOffsetRequestHeader,
    UpdateGroupForbiddenRequestHeader,
    RecallMessageRequestHeader,
    SendMessageRequestHeaderV2,
    SendMessageRequestHeaderV1
);

impl_rpc_request_header_accessors!(
    LiteSubscriptionCtlRequestHeader,
    UnregisterClientRequestHeader,
    GetRouteInfoRequestHeader,
    GetConsumerListByGroupRequestHeader,
    LockBatchMqRequestHeader,
    UnlockBatchMqRequestHeader,
    ConsumerSendMsgBackRequestHeader,
    EndTransactionRequestHeader,
    CheckTransactionStateRequestHeader,
    ReplyMessageRequestHeader,
    NotificationRequestHeader,
    ResetOffsetRequestHeader,
    GetConsumerStatusRequestHeader,
    GetConsumerConnectionListRequestHeader,
    GetConsumeStatsRequestHeader,
    GetConsumerRunningInfoRequestHeader,
    CloneGroupOffsetRequestHeader,
    PopMessageRequestHeader,
    AckMessageRequestHeader,
    ChangeInvisibleTimeRequestHeader,
    PollingInfoRequestHeader,
    PopLiteMessageRequestHeader,
    NotifyUnsubscribeLiteRequestHeader
);

impl ConsumeMessageDirectlyResultRequestHeader {
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

    pub fn get_bname(&self) -> Option<&str> {
        self.rpc_broker_name.as_deref()
    }

    pub fn set_bname(&mut self, value: Option<String>) {
        self.rpc_broker_name = value;
    }

    pub fn get_oneway(&self) -> Option<bool> {
        self.oneway
    }

    pub fn set_oneway(&mut self, value: Option<bool>) {
        self.oneway = value;
    }

    pub fn get_lo(&self) -> Option<bool> {
        self.logical
    }

    pub fn set_lo(&mut self, value: Option<bool>) {
        self.logical = value;
    }
}

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_string,
    lite_topic,
    [get_lite_topic],
    [set_lite_topic]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i64),
    queue_offset,
    [get_queue_offset],
    [set_queue_offset]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i32),
    max_msg_nums,
    [get_max_msg_nums],
    [set_max_msg_nums]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i32),
    sys_flag,
    [get_sys_flag],
    [set_sys_flag]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i64),
    commit_offset,
    [get_commit_offset],
    [set_commit_offset]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i64),
    suspend_timeout_millis,
    [get_suspend_timeout_millis],
    [set_suspend_timeout_millis]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_string,
    subscription,
    [get_subscription],
    [set_subscription]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i64),
    sub_version,
    [get_sub_version],
    [set_sub_version]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_string,
    expression_type,
    [get_expression_type],
    [set_expression_type]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i32),
    max_msg_bytes,
    [get_max_msg_bytes],
    [set_max_msg_bytes]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_copy(i32),
    request_source,
    [get_request_source],
    [set_request_source]
);

impl_header_field_accessors!(
    PullMessageRequestHeader,
    optional_string,
    proxy_forward_client_id,
    [get_proxy_froward_client_id],
    [set_proxy_froward_client_id]
);

impl_header_field_accessors!(
    UnregisterClientRequestHeader,
    string,
    client_id,
    [get_client_id],
    [set_client_id]
);

impl_header_field_accessors!(
    UnregisterClientRequestHeader,
    optional_string,
    producer_group,
    [get_producer_group],
    [set_producer_group]
);

impl_header_field_accessors!(
    UnregisterClientRequestHeader,
    optional_string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    GetRouteInfoRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    GetRouteInfoRequestHeader,
    optional_copy(bool),
    accept_standard_json_only,
    [get_accept_standard_json_only],
    [set_accept_standard_json_only]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_string,
    default_topic,
    [get_default_topic],
    [set_default_topic]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_copy(i32),
    read_queue_nums,
    [get_read_queue_nums],
    [set_read_queue_nums]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_copy(i32),
    write_queue_nums,
    [get_write_queue_nums],
    [set_write_queue_nums]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_copy(i32),
    perm,
    [get_perm],
    [set_perm]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_string,
    topic_filter_type,
    [get_topic_filter_type],
    [set_topic_filter_type]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_copy(i32),
    topic_sys_flag,
    [get_topic_sys_flag],
    [set_topic_sys_flag]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_copy(bool),
    order,
    [get_order],
    [set_order]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_string,
    attributes,
    [get_attributes],
    [set_attributes]
);

impl_header_field_accessors!(
    CreateTopicRequestHeader,
    optional_copy(bool),
    force,
    [get_force],
    [set_force]
);

impl_header_field_accessors!(
    QueryMessageRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    QueryMessageRequestHeader,
    optional_string,
    key,
    [get_key],
    [set_key]
);

impl_header_field_accessors!(
    QueryMessageRequestHeader,
    optional_copy(i32),
    max_num,
    [get_max_num],
    [set_max_num]
);

impl_header_field_accessors!(
    QueryMessageRequestHeader,
    optional_copy(i64),
    begin_timestamp,
    [get_begin_timestamp],
    [set_begin_timestamp]
);

impl_header_field_accessors!(
    QueryMessageRequestHeader,
    optional_copy(i64),
    end_timestamp,
    [get_end_timestamp],
    [set_end_timestamp]
);

impl_header_field_accessors!(
    QueryMessageRequestHeader,
    optional_string,
    index_type,
    [get_index_type],
    [set_index_type]
);

impl_header_field_accessors!(
    QueryMessageRequestHeader,
    optional_string,
    last_key,
    [get_last_key],
    [set_last_key]
);

impl_header_field_accessors!(
    ViewMessageRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    ViewMessageRequestHeader,
    optional_copy(i64),
    offset,
    [get_offset],
    [set_offset]
);

impl_header_field_accessors!(
    GetConsumerListByGroupRequestHeader,
    string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    ConsumerSendMsgBackRequestHeader,
    string,
    group,
    [get_group],
    [set_group]
);

impl_header_field_accessors!(
    ConsumerSendMsgBackRequestHeader,
    string,
    origin_topic,
    [get_origin_topic],
    [set_origin_topic]
);

impl_header_field_accessors!(
    ConsumerSendMsgBackRequestHeader,
    copy(i64),
    offset,
    [get_offset],
    [set_offset]
);

impl_header_field_accessors!(
    ConsumerSendMsgBackRequestHeader,
    copy(i32),
    delay_level,
    [get_delay_level],
    [set_delay_level]
);

impl_header_field_accessors!(
    ConsumerSendMsgBackRequestHeader,
    optional_string,
    origin_msg_id,
    [get_origin_msg_id],
    [set_origin_msg_id]
);

impl_header_field_accessors!(
    ConsumerSendMsgBackRequestHeader,
    optional_copy(i32),
    max_reconsume_times,
    [get_max_reconsume_times],
    [set_max_reconsume_times]
);

impl_header_field_accessors!(
    ConsumerSendMsgBackRequestHeader,
    copy(bool),
    unit_mode,
    [is_unit_mode],
    [set_unit_mode]
);

impl_header_field_accessors!(
    RecallMessageRequestHeader,
    optional_string,
    producer_group,
    [get_producer_group],
    [set_producer_group]
);

impl_header_field_accessors!(
    RecallMessageRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    RecallMessageRequestHeader,
    string,
    recall_handle,
    [get_recall_handle],
    [set_recall_handle]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_string,
    producer_group,
    [get_producer_group],
    [set_producer_group]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_copy(i64),
    transaction_state_table_offset,
    [get_tran_state_table_offset],
    [set_tran_state_table_offset]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_copy(i64),
    commit_log_offset,
    [get_commit_log_offset],
    [set_commit_log_offset]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_copy(i32),
    commit_or_rollback,
    [get_commit_or_rollback],
    [set_commit_or_rollback]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_copy(bool),
    from_transaction_check,
    [get_from_transaction_check],
    [set_from_transaction_check]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_string,
    msg_id,
    [get_msg_id],
    [set_msg_id]
);

impl_header_field_accessors!(
    EndTransactionRequestHeader,
    optional_string,
    transaction_id,
    [get_transaction_id],
    [set_transaction_id]
);

impl_header_field_accessors!(
    CheckTransactionStateRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    CheckTransactionStateRequestHeader,
    copy(i64),
    transaction_state_table_offset,
    [get_tran_state_table_offset],
    [set_tran_state_table_offset]
);

impl_header_field_accessors!(
    CheckTransactionStateRequestHeader,
    copy(i64),
    commit_log_offset,
    [get_commit_log_offset],
    [set_commit_log_offset]
);

impl_header_field_accessors!(
    CheckTransactionStateRequestHeader,
    optional_string,
    msg_id,
    [get_msg_id],
    [set_msg_id]
);

impl_header_field_accessors!(
    CheckTransactionStateRequestHeader,
    optional_string,
    transaction_id,
    [get_transaction_id],
    [set_transaction_id]
);

impl_header_field_accessors!(
    CheckTransactionStateRequestHeader,
    optional_string,
    offset_msg_id,
    [get_offset_msg_id],
    [set_offset_msg_id]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    string,
    producer_group,
    [get_producer_group],
    [set_producer_group]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    string,
    default_topic,
    [get_default_topic],
    [set_default_topic]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    copy(i32),
    default_topic_queue_nums,
    [get_default_topic_queue_nums],
    [set_default_topic_queue_nums]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    copy(i32),
    sys_flag,
    [get_sys_flag],
    [set_sys_flag]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    copy(i64),
    born_timestamp,
    [get_born_timestamp],
    [set_born_timestamp]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    copy(i32),
    flag,
    [get_flag],
    [set_flag]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    optional_string,
    properties,
    [get_properties],
    [set_properties]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    optional_copy(i32),
    reconsume_times,
    [get_reconsume_times],
    [set_reconsume_times]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    copy(bool),
    unit_mode,
    [is_unit_mode],
    [set_unit_mode]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    string,
    born_host,
    [get_born_host],
    [set_born_host]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    string,
    store_host,
    [get_store_host],
    [set_store_host]
);

impl_header_field_accessors!(
    ReplyMessageRequestHeader,
    copy(i64),
    store_timestamp,
    [get_store_timestamp],
    [set_store_timestamp]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    copy(i64),
    poll_time,
    [get_poll_time],
    [set_poll_time]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    copy(i64),
    born_time,
    [get_born_time],
    [set_born_time]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    optional_copy(bool),
    order,
    [get_order],
    [set_order]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    optional_string,
    attempt_id,
    [get_attempt_id],
    [set_attempt_id]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    copy(bool),
    lite_consumer,
    [is_lite_consumer],
    [set_lite_consumer]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    optional_string,
    client_id,
    [get_client_id],
    [set_client_id]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    optional_string,
    expression_type,
    [get_exp_type],
    [set_exp_type]
);

impl_header_field_accessors!(
    NotificationRequestHeader,
    optional_string,
    expression,
    [get_exp],
    [set_exp]
);

impl_header_field_accessors!(
    ResetOffsetRequestHeader,
    string,
    group,
    [get_group],
    [set_group]
);

impl_header_field_accessors!(
    ResetOffsetRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    ResetOffsetRequestHeader,
    copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    ResetOffsetRequestHeader,
    optional_copy(i64),
    offset,
    [get_offset],
    [set_offset]
);

impl_header_field_accessors!(
    ResetOffsetRequestHeader,
    copy(i64),
    timestamp,
    [get_timestamp],
    [set_timestamp]
);

impl_header_field_accessors!(
    ResetOffsetRequestHeader,
    copy(bool),
    force,
    [is_force],
    [set_force]
);

impl_header_field_accessors!(
    GetConsumerStatusRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    GetConsumerStatusRequestHeader,
    string,
    group,
    [get_group],
    [set_group]
);

impl_header_field_accessors!(
    GetConsumerStatusRequestHeader,
    optional_string,
    client_addr,
    [get_client_addr],
    [set_client_addr]
);

impl_header_field_accessors!(
    GetConsumerConnectionListRequestHeader,
    optional_string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    GetConsumeStatsRequestHeader,
    optional_string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    GetConsumeStatsRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    GetConsumeStatsRequestHeader,
    optional_string,
    topic_list,
    [get_topic_list],
    [set_topic_list]
);

impl_header_field_accessors!(
    GetConsumerRunningInfoRequestHeader,
    string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    GetConsumerRunningInfoRequestHeader,
    string,
    client_id,
    [get_client_id],
    [set_client_id]
);

impl_header_field_accessors!(
    GetConsumerRunningInfoRequestHeader,
    copy(bool),
    jstack_enable,
    [is_jstack_enable],
    [set_jstack_enable]
);

impl_header_field_accessors!(
    ConsumeMessageDirectlyResultRequestHeader,
    string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    ConsumeMessageDirectlyResultRequestHeader,
    optional_string,
    client_id,
    [get_client_id],
    [set_client_id]
);

impl_header_field_accessors!(
    ConsumeMessageDirectlyResultRequestHeader,
    optional_string,
    msg_id,
    [get_msg_id],
    [set_msg_id]
);

impl_header_field_accessors!(
    ConsumeMessageDirectlyResultRequestHeader,
    optional_string,
    broker_name,
    [get_broker_name],
    [set_broker_name]
);

impl_header_field_accessors!(
    ConsumeMessageDirectlyResultRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    ConsumeMessageDirectlyResultRequestHeader,
    optional_copy(i32),
    topic_sys_flag,
    [get_topic_sys_flag],
    [set_topic_sys_flag]
);

impl_header_field_accessors!(
    ConsumeMessageDirectlyResultRequestHeader,
    optional_copy(i32),
    group_sys_flag,
    [get_group_sys_flag],
    [set_group_sys_flag]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    optional_string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    copy(i32),
    max_msg_nums,
    [get_max_msg_nums],
    [set_max_msg_nums]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    copy(i64),
    invisible_time,
    [get_invisible_time],
    [set_invisible_time]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    copy(i64),
    poll_time,
    [get_poll_time],
    [set_poll_time]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    copy(i64),
    born_time,
    [get_born_time],
    [set_born_time]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    copy(i32),
    init_mode,
    [get_init_mode],
    [set_init_mode]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    optional_string,
    expression_type,
    [get_exp_type],
    [set_exp_type]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    optional_string,
    expression,
    [get_exp],
    [set_exp]
);

impl_header_field_accessors!(
    PopMessageRequestHeader,
    optional_string,
    attempt_id,
    [get_attempt_id],
    [set_attempt_id]
);

impl_header_field_accessors!(
    AckMessageRequestHeader,
    string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    AckMessageRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    AckMessageRequestHeader,
    copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    AckMessageRequestHeader,
    string,
    extra_info,
    [get_extra_info],
    [set_extra_info]
);

impl_header_field_accessors!(
    AckMessageRequestHeader,
    copy(i64),
    offset,
    [get_offset],
    [set_offset]
);

impl_header_field_accessors!(
    AckMessageRequestHeader,
    optional_string,
    lite_topic,
    [get_lite_topic],
    [set_lite_topic]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    optional_string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    optional_copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    optional_string,
    extra_info,
    [get_extra_info],
    [set_extra_info]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    optional_copy(i64),
    offset,
    [get_offset],
    [set_offset]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    optional_copy(i64),
    invisible_time,
    [get_invisible_time],
    [set_invisible_time]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    optional_string,
    lite_topic,
    [get_lite_topic],
    [set_lite_topic]
);

impl_header_field_accessors!(
    ChangeInvisibleTimeRequestHeader,
    copy(bool),
    suspend,
    [is_suspend],
    [set_suspend]
);

impl_header_field_accessors!(
    PollingInfoRequestHeader,
    string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    PollingInfoRequestHeader,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    PollingInfoRequestHeader,
    copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    optional_string,
    client_id,
    [get_client_id],
    [set_client_id]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    optional_string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    optional_string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    copy(i32),
    max_msg_num,
    [get_max_msg_num],
    [set_max_msg_num]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    copy(i64),
    invisible_time,
    [get_invisible_time],
    [set_invisible_time]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    copy(i64),
    poll_time,
    [get_poll_time],
    [set_poll_time]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    copy(i64),
    born_time,
    [get_born_time],
    [set_born_time]
);

impl_header_field_accessors!(
    PopLiteMessageRequestHeader,
    optional_string,
    attempt_id,
    [get_attempt_id],
    [set_attempt_id]
);

impl_header_field_accessors!(
    NotifyUnsubscribeLiteRequestHeader,
    string,
    lite_topic,
    [get_lite_topic],
    [set_lite_topic]
);

impl_header_field_accessors!(
    NotifyUnsubscribeLiteRequestHeader,
    string,
    consumer_group,
    [get_consumer_group],
    [set_consumer_group]
);

impl_header_field_accessors!(
    NotifyUnsubscribeLiteRequestHeader,
    string,
    client_id,
    [get_client_id],
    [set_client_id]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_string,
    a,
    [get_a],
    [set_a]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_string,
    b,
    [get_b],
    [set_b]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_string,
    c,
    [get_c],
    [set_c]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(i32),
    d,
    [get_d],
    [set_d]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(i32),
    e,
    [get_e],
    [set_e]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(i32),
    f,
    [get_f],
    [set_f]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(i64),
    g,
    [get_g],
    [set_g]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(i32),
    h,
    [get_h],
    [set_h]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_string,
    i,
    [get_i],
    [set_i]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(i32),
    j,
    [get_j],
    [set_j]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(bool),
    k,
    [is_k],
    [set_k]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(i32),
    l,
    [get_l],
    [set_l]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV2,
    optional_copy(bool),
    m,
    [is_m],
    [set_m]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    optional_string,
    producer_group,
    [get_producer_group],
    [set_producer_group]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    string,
    topic,
    [get_topic],
    [set_topic]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    string,
    default_topic,
    [get_default_topic],
    [set_default_topic]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    copy(i32),
    default_topic_queue_nums,
    [get_default_topic_queue_nums],
    [set_default_topic_queue_nums]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    copy(i32),
    queue_id,
    [get_queue_id],
    [set_queue_id]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    copy(i32),
    sys_flag,
    [get_sys_flag],
    [set_sys_flag]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    copy(i64),
    born_timestamp,
    [get_born_timestamp],
    [set_born_timestamp]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    copy(i32),
    flag,
    [get_flag],
    [set_flag]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    optional_string,
    properties,
    [get_properties],
    [set_properties]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    optional_copy(i32),
    reconsume_times,
    [get_reconsume_times],
    [set_reconsume_times]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    optional_copy(bool),
    unit_mode,
    [is_unit_mode],
    [set_unit_mode]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    optional_copy(i32),
    max_reconsume_times,
    [get_max_reconsume_times],
    [set_max_reconsume_times]
);

impl_header_field_accessors!(
    SendMessageRequestHeaderV1,
    optional_copy(bool),
    batch,
    [is_batch],
    [set_batch]
);

impl crate::header::FastCodesHeader for PullMessageRequestHeader {}

impl crate::header::FastCodesHeader for SendMessageRequestHeaderV2 {}
