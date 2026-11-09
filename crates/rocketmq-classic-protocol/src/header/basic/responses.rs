use super::*;

/// Pinned Java `QueryConsumerOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryConsumerOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<QueryConsumerOffsetResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryConsumerOffsetResponseHeader;
/// let _ = QueryConsumerOffsetResponseHeader::default().offset;
/// ```
pub struct QueryConsumerOffsetResponseHeader {
    pub(crate) offset: Option<i64>,
}

impl QueryConsumerOffsetResponseHeader {
    pub fn get_offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn set_offset(&mut self, value: Option<i64>) {
        self.offset = value;
    }
}

impl CommandHeader for QueryConsumerOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

/// Java `GetAllSubscriptionGroupResponseHeader`; this field is optional when
/// talking to pre-pagination Brokers.
#[derive(Debug, Clone, Default)]
pub struct GetAllSubscriptionGroupResponseHeader {
    total_group_num: Option<i32>,
}

impl GetAllSubscriptionGroupResponseHeader {
    pub fn get_total_group_num(&self) -> Option<i32> {
        self.total_group_num
    }

    pub fn set_total_group_num(&mut self, value: Option<i32>) {
        self.total_group_num = value;
    }
}

/// Java `GetAllTopicConfigResponseHeader`; old Brokers omit this optional
/// total so the client can treat the response body as the complete snapshot.
#[derive(Debug, Clone, Default)]
pub struct GetAllTopicConfigResponseHeader {
    total_topic_num: Option<i32>,
}

impl GetAllTopicConfigResponseHeader {
    pub fn get_total_topic_num(&self) -> Option<i32> {
        self.total_topic_num
    }

    pub fn set_total_topic_num(&mut self, value: Option<i32>) {
        self.total_topic_num = value;
    }
}

impl CommandHeader for GetAllTopicConfigResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "totalTopicNum", self.total_topic_num);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            total_topic_num: optional_i32(fields, "totalTopicNum")?,
        })
    }
}

impl CommandHeader for GetAllSubscriptionGroupResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "totalGroupNum", self.total_group_num);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            total_group_num: optional_i32(fields, "totalGroupNum").unwrap_or(None),
        })
    }
}

/// Pinned Java NameServer `GetKVConfigResponseHeader`; the value is nullable.
#[derive(Debug, Clone, Default)]
pub struct GetKvConfigResponseHeader {
    pub(crate) value: Option<String>,
}

impl GetKvConfigResponseHeader {
    pub fn get_value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    pub fn set_value(&mut self, value: Option<String>) {
        self.value = value;
    }
}

impl CommandHeader for GetKvConfigResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "value", self.value.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            value: optional_string(fields, "value"),
        })
    }
}

/// Pinned Java NameServer `WipeWritePermOfBrokerResponseHeader`. The boxed
/// topic count stays nullable until the owning RPC performs Java's unboxing.
#[derive(Debug, Clone, Copy, Default)]
pub struct WipeWritePermOfBrokerResponseHeader {
    pub(crate) wipe_topic_count: Option<i32>,
}

impl WipeWritePermOfBrokerResponseHeader {
    pub fn get_wipe_topic_count(&self) -> Option<i32> {
        self.wipe_topic_count
    }

    pub fn set_wipe_topic_count(&mut self, value: Option<i32>) {
        self.wipe_topic_count = value;
    }
}

impl CommandHeader for WipeWritePermOfBrokerResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "wipeTopicCount", self.wipe_topic_count);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            wipe_topic_count: optional_i32_with_mode(fields, "wipeTopicCount", false)?,
        })
    }
}

/// Pinned Java NameServer `AddWritePermOfBrokerResponseHeader`. The boxed
/// topic count stays nullable until the owning RPC performs Java's unboxing.
#[derive(Debug, Clone, Copy, Default)]
pub struct AddWritePermOfBrokerResponseHeader {
    pub(crate) add_topic_count: Option<i32>,
}

impl AddWritePermOfBrokerResponseHeader {
    pub fn get_add_topic_count(&self) -> Option<i32> {
        self.add_topic_count
    }

    pub fn set_add_topic_count(&mut self, value: Option<i32>) {
        self.add_topic_count = value;
    }
}

impl CommandHeader for AddWritePermOfBrokerResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "addTopicCount", self.add_topic_count);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            add_topic_count: optional_i32_with_mode(fields, "addTopicCount", false)?,
        })
    }
}

/// Pinned Java `SearchOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SearchOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<SearchOffsetResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SearchOffsetResponseHeader;
/// let _ = SearchOffsetResponseHeader::default().offset;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchOffsetResponseHeader {
    pub(crate) offset: Option<i64>,
}

impl SearchOffsetResponseHeader {
    pub fn get_offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn set_offset(&mut self, value: Option<i64>) {
        self.offset = value;
    }
}

impl CommandHeader for SearchOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

/// Pinned Java `GetMaxOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMaxOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<GetMaxOffsetResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMaxOffsetResponseHeader;
/// let _ = GetMaxOffsetResponseHeader::default().offset;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct GetMaxOffsetResponseHeader {
    pub(crate) offset: Option<i64>,
}

impl GetMaxOffsetResponseHeader {
    pub fn get_offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn set_offset(&mut self, value: Option<i64>) {
        self.offset = value;
    }
}

impl CommandHeader for GetMaxOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

/// Pinned Java `GetMinOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMinOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<GetMinOffsetResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMinOffsetResponseHeader;
/// let _ = GetMinOffsetResponseHeader::default().offset;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct GetMinOffsetResponseHeader {
    pub(crate) offset: Option<i64>,
}

impl GetMinOffsetResponseHeader {
    pub fn get_offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn set_offset(&mut self, value: Option<i64>) {
        self.offset = value;
    }
}

impl CommandHeader for GetMinOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

/// Pinned Java `GetEarliestMsgStoretimeResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetEarliestMsgStoretimeResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<GetEarliestMsgStoretimeResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetEarliestMsgStoretimeResponseHeader;
/// let _ = GetEarliestMsgStoretimeResponseHeader::default().timestamp;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct GetEarliestMsgStoretimeResponseHeader {
    pub(crate) timestamp: Option<i64>,
}

impl GetEarliestMsgStoretimeResponseHeader {
    pub fn get_timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    pub fn set_timestamp(&mut self, value: Option<i64>) {
        self.timestamp = value;
    }
}

impl CommandHeader for GetEarliestMsgStoretimeResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "timestamp", self.timestamp);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            timestamp: Some(required_i64(fields, "timestamp")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            timestamp: optional_i64_with_mode(fields, "timestamp", false)?,
        })
    }
}

/// Pinned Java `PullMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PullMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PullMessageResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PullMessageResponseHeader;
/// let _ = PullMessageResponseHeader::default().next_begin_offset;
/// ```
#[derive(Debug, Clone, Default)]
pub struct PullMessageResponseHeader {
    pub(crate) suggest_which_broker_id: Option<i64>,
    pub(crate) next_begin_offset: Option<i64>,
    pub(crate) min_offset: Option<i64>,
    pub(crate) max_offset: Option<i64>,
    pub(crate) offset_delta: Option<i64>,
    pub(crate) topic_sys_flag: Option<i32>,
    pub(crate) group_sys_flag: Option<i32>,
    pub(crate) forbidden_type: Option<i32>,
}

impl PullMessageResponseHeader {
    pub fn get_suggest_which_broker_id(&self) -> Option<i64> {
        self.suggest_which_broker_id
    }

    pub fn set_suggest_which_broker_id(&mut self, value: Option<i64>) {
        self.suggest_which_broker_id = value;
    }

    pub fn get_next_begin_offset(&self) -> Option<i64> {
        self.next_begin_offset
    }

    pub fn set_next_begin_offset(&mut self, value: Option<i64>) {
        self.next_begin_offset = value;
    }

    pub fn get_min_offset(&self) -> Option<i64> {
        self.min_offset
    }

    pub fn set_min_offset(&mut self, value: Option<i64>) {
        self.min_offset = value;
    }

    pub fn get_max_offset(&self) -> Option<i64> {
        self.max_offset
    }

    pub fn set_max_offset(&mut self, value: Option<i64>) {
        self.max_offset = value;
    }

    pub fn get_offset_delta(&self) -> Option<i64> {
        self.offset_delta
    }

    pub fn set_offset_delta(&mut self, value: Option<i64>) {
        self.offset_delta = value;
    }

    pub fn get_topic_sys_flag(&self) -> Option<i32> {
        self.topic_sys_flag
    }

    pub fn set_topic_sys_flag(&mut self, value: Option<i32>) {
        self.topic_sys_flag = value;
    }

    pub fn get_group_sys_flag(&self) -> Option<i32> {
        self.group_sys_flag
    }

    pub fn set_group_sys_flag(&mut self, value: Option<i32>) {
        self.group_sys_flag = value;
    }

    pub fn get_forbidden_type(&self) -> Option<i32> {
        self.forbidden_type
    }

    pub fn set_forbidden_type(&mut self, value: Option<i32>) {
        self.forbidden_type = value;
    }
}

impl CommandHeader for PullMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "suggestWhichBrokerId", self.suggest_which_broker_id);
        put_optional(fields, "nextBeginOffset", self.next_begin_offset);
        put_optional(fields, "minOffset", self.min_offset);
        put_optional(fields, "maxOffset", self.max_offset);
        put_optional(fields, "offsetDelta", self.offset_delta);
        put_optional(fields, "topicSysFlag", self.topic_sys_flag);
        put_optional(fields, "groupSysFlag", self.group_sys_flag);
        put_optional(fields, "forbiddenType", self.forbidden_type);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            suggest_which_broker_id: optional_i64_with_mode(
                fields,
                "suggestWhichBrokerId",
                use_fast_encode,
            )?,
            next_begin_offset: optional_i64_with_mode(fields, "nextBeginOffset", use_fast_encode)?,
            min_offset: optional_i64_with_mode(fields, "minOffset", use_fast_encode)?,
            max_offset: optional_i64_with_mode(fields, "maxOffset", use_fast_encode)?,
            offset_delta: optional_i64_with_mode(fields, "offsetDelta", use_fast_encode)?,
            topic_sys_flag: optional_i32_with_mode(fields, "topicSysFlag", use_fast_encode)?,
            group_sys_flag: optional_i32_with_mode(fields, "groupSysFlag", use_fast_encode)?,
            forbidden_type: optional_i32_with_mode(fields, "forbiddenType", use_fast_encode)?,
        })
    }
}

/// Pinned Java `QueryMessageResponseHeader` keeps both Long values nullable and
/// inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<QueryMessageResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryMessageResponseHeader;
/// let _ = QueryMessageResponseHeader::default().index_last_update_timestamp;
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryMessageResponseHeader {
    pub(crate) index_last_update_timestamp: Option<i64>,
    pub(crate) index_last_update_phyoffset: Option<i64>,
}

impl QueryMessageResponseHeader {
    pub fn get_index_last_update_timestamp(&self) -> Option<i64> {
        self.index_last_update_timestamp
    }

    pub fn set_index_last_update_timestamp(&mut self, value: Option<i64>) {
        self.index_last_update_timestamp = value;
    }

    pub fn get_index_last_update_phyoffset(&self) -> Option<i64> {
        self.index_last_update_phyoffset
    }

    pub fn set_index_last_update_phyoffset(&mut self, value: Option<i64>) {
        self.index_last_update_phyoffset = value;
    }
}

impl CommandHeader for QueryMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(
            fields,
            "indexLastUpdateTimestamp",
            self.index_last_update_timestamp,
        );
        put_optional(
            fields,
            "indexLastUpdatePhyoffset",
            self.index_last_update_phyoffset,
        );
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            index_last_update_timestamp: optional_i64_with_mode(
                fields,
                "indexLastUpdateTimestamp",
                false,
            )?,
            index_last_update_phyoffset: optional_i64_with_mode(
                fields,
                "indexLastUpdatePhyoffset",
                false,
            )?,
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Self::decode_fields(fields)
    }
}

/// Pinned Java `RecallMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::RecallMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<RecallMessageResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::RecallMessageResponseHeader;
/// let _ = RecallMessageResponseHeader::default().msg_id;
/// ```
#[derive(Debug, Clone, Default)]
pub struct RecallMessageResponseHeader {
    pub(crate) msg_id: Option<String>,
}

impl RecallMessageResponseHeader {
    pub fn get_msg_id(&self) -> Option<&str> {
        self.msg_id.as_deref()
    }

    pub fn set_msg_id(&mut self, value: Option<String>) {
        self.msg_id = value;
    }
}

impl CommandHeader for RecallMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "msgId", self.msg_id.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            msg_id: optional_string(fields, "msgId"),
        })
    }
}

/// Pinned Java `SendMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SendMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<SendMessageResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SendMessageResponseHeader;
/// let _ = SendMessageResponseHeader::default().msg_id;
/// ```
#[derive(Debug, Clone, Default)]
pub struct SendMessageResponseHeader {
    pub(crate) msg_id: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) queue_offset: Option<i64>,
    pub(crate) transaction_id: Option<String>,
    pub(crate) batch_uniq_id: Option<String>,
    pub(crate) recall_handle: Option<String>,
}

impl SendMessageResponseHeader {
    pub fn get_msg_id(&self) -> Option<&str> {
        self.msg_id.as_deref()
    }

    pub fn set_msg_id(&mut self, value: Option<String>) {
        self.msg_id = value;
    }

    pub fn get_queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, value: Option<i32>) {
        self.queue_id = value;
    }

    pub fn get_queue_offset(&self) -> Option<i64> {
        self.queue_offset
    }

    pub fn set_queue_offset(&mut self, value: Option<i64>) {
        self.queue_offset = value;
    }

    pub fn get_transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub fn set_transaction_id(&mut self, value: Option<String>) {
        self.transaction_id = value;
    }

    pub fn get_batch_uniq_id(&self) -> Option<&str> {
        self.batch_uniq_id.as_deref()
    }

    pub fn set_batch_uniq_id(&mut self, value: Option<String>) {
        self.batch_uniq_id = value;
    }

    pub fn get_recall_handle(&self) -> Option<&str> {
        self.recall_handle.as_deref()
    }

    pub fn set_recall_handle(&mut self, value: Option<String>) {
        self.recall_handle = value;
    }
}

/// Pinned Java `UnregisterClientResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::UnregisterClientResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<UnregisterClientResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct UnregisterClientResponseHeader;

impl CommandHeader for UnregisterClientResponseHeader {
    fn encode_fields(&self, _fields: &mut HeaderMap) {}

    fn decode_fields(_fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

/// Pinned Java `EndTransactionResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::EndTransactionResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<EndTransactionResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct EndTransactionResponseHeader;

impl CommandHeader for EndTransactionResponseHeader {
    fn encode_fields(&self, _fields: &mut HeaderMap) {}
    fn decode_fields(_fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

/// Pinned Java `CheckTransactionStateResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::CheckTransactionStateResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<CheckTransactionStateResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::CheckTransactionStateResponseHeader;
/// let _ = CheckTransactionStateResponseHeader::default().producer_group;
/// ```
#[derive(Debug, Clone, Default)]
pub struct CheckTransactionStateResponseHeader {
    pub(crate) producer_group: Option<String>,
    pub(crate) transaction_state_table_offset: Option<i64>,
    pub(crate) commit_log_offset: Option<i64>,
    pub(crate) commit_or_rollback: Option<i32>,
}

impl CheckTransactionStateResponseHeader {
    pub fn get_producer_group(&self) -> Option<&str> {
        self.producer_group.as_deref()
    }

    pub fn set_producer_group(&mut self, value: Option<String>) {
        self.producer_group = value;
    }

    pub fn get_tran_state_table_offset(&self) -> Option<i64> {
        self.transaction_state_table_offset
    }

    pub fn set_tran_state_table_offset(&mut self, value: Option<i64>) {
        self.transaction_state_table_offset = value;
    }

    pub fn get_commit_log_offset(&self) -> Option<i64> {
        self.commit_log_offset
    }

    pub fn set_commit_log_offset(&mut self, value: Option<i64>) {
        self.commit_log_offset = value;
    }

    pub fn get_commit_or_rollback(&self) -> Option<i32> {
        self.commit_or_rollback
    }

    pub fn set_commit_or_rollback(&mut self, value: Option<i32>) {
        self.commit_or_rollback = value;
    }
}

impl CommandHeader for CheckTransactionStateResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        put_optional(
            fields,
            "tranStateTableOffset",
            self.transaction_state_table_offset,
        );
        put_optional(fields, "commitLogOffset", self.commit_log_offset);
        put_optional(fields, "commitOrRollback", self.commit_or_rollback);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            producer_group: Some(required_string(fields, "producerGroup")?.to_owned()),
            transaction_state_table_offset: Some(required_i64(fields, "tranStateTableOffset")?),
            commit_log_offset: Some(required_i64(fields, "commitLogOffset")?),
            commit_or_rollback: Some(required_i32(fields, "commitOrRollback")?),
        })
    }

    fn check_fields(&self) -> Result<(), ProtocolError> {
        if matches!(self.commit_or_rollback, Some(8 | 12)) {
            Ok(())
        } else {
            Err(ProtocolError::InvalidField {
                field: "commitOrRollback",
                reason: "value invalid".into(),
            })
        }
    }
}

/// Pinned Java `NotificationResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::NotificationResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<NotificationResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::NotificationResponseHeader;
/// let _ = NotificationResponseHeader::default().has_msg;
/// ```
#[derive(Debug, Clone, Default)]
pub struct NotificationResponseHeader {
    pub(crate) has_msg: bool,
    pub(crate) polling_full: bool,
}

impl NotificationResponseHeader {
    pub fn is_has_msg(&self) -> bool {
        self.has_msg
    }

    pub fn set_has_msg(&mut self, value: bool) {
        self.has_msg = value;
    }

    pub fn is_polling_full(&self) -> bool {
        self.polling_full
    }

    pub fn set_polling_full(&mut self, value: bool) {
        self.polling_full = value;
    }
}

impl CommandHeader for NotificationResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("hasMsg".into(), self.has_msg.to_string());
        fields.insert("pollingFull".into(), self.polling_full.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            has_msg: java_boolean(fields, "hasMsg"),
            polling_full: java_boolean(fields, "pollingFull"),
        })
    }
}

/// Pinned Java `PopMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PopMessageResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopMessageResponseHeader;
/// let _ = PopMessageResponseHeader::default().pop_time;
/// ```
#[derive(Debug, Clone, Default)]
pub struct PopMessageResponseHeader {
    pub(crate) pop_time: i64,
    pub(crate) invisible_time: i64,
    pub(crate) revive_queue_id: i32,
    pub(crate) rest_num: i64,
    pub(crate) start_offset_info: Option<String>,
    pub(crate) msg_offset_info: Option<String>,
    pub(crate) order_count_info: Option<String>,
}

impl PopMessageResponseHeader {
    pub fn get_pop_time(&self) -> i64 {
        self.pop_time
    }

    pub fn set_pop_time(&mut self, value: i64) {
        self.pop_time = value;
    }

    pub fn get_invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub fn set_invisible_time(&mut self, value: i64) {
        self.invisible_time = value;
    }

    pub fn get_revive_qid(&self) -> i32 {
        self.revive_queue_id
    }

    pub fn set_revive_qid(&mut self, value: i32) {
        self.revive_queue_id = value;
    }

    pub fn get_rest_num(&self) -> i64 {
        self.rest_num
    }

    pub fn set_rest_num(&mut self, value: i64) {
        self.rest_num = value;
    }

    pub fn get_start_offset_info(&self) -> Option<&str> {
        self.start_offset_info.as_deref()
    }

    pub fn set_start_offset_info(&mut self, value: Option<String>) {
        self.start_offset_info = value;
    }

    pub fn get_msg_offset_info(&self) -> Option<&str> {
        self.msg_offset_info.as_deref()
    }

    pub fn set_msg_offset_info(&mut self, value: Option<String>) {
        self.msg_offset_info = value;
    }

    pub fn get_order_count_info(&self) -> Option<&str> {
        self.order_count_info.as_deref()
    }

    pub fn set_order_count_info(&mut self, value: Option<String>) {
        self.order_count_info = value;
    }
}

impl CommandHeader for PopMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("popTime".into(), self.pop_time.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("reviveQid".into(), self.revive_queue_id.to_string());
        fields.insert("restNum".into(), self.rest_num.to_string());
        put_optional(fields, "startOffsetInfo", self.start_offset_info.as_deref());
        put_optional(fields, "msgOffsetInfo", self.msg_offset_info.as_deref());
        put_optional(fields, "orderCountInfo", self.order_count_info.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            pop_time: optional_i64_with_mode(fields, "popTime", false)?.unwrap_or_default(),
            invisible_time: optional_i64_with_mode(fields, "invisibleTime", false)?
                .unwrap_or_default(),
            revive_queue_id: optional_i32_with_mode(fields, "reviveQid", false)?
                .unwrap_or_default(),
            rest_num: optional_i64_with_mode(fields, "restNum", false)?.unwrap_or_default(),
            start_offset_info: optional_string(fields, "startOffsetInfo"),
            msg_offset_info: optional_string(fields, "msgOffsetInfo"),
            order_count_info: optional_string(fields, "orderCountInfo"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Self::decode_fields(fields)
    }
}

/// Pinned Java `PopLiteMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopLiteMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PopLiteMessageResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopLiteMessageResponseHeader;
/// let _ = PopLiteMessageResponseHeader::default().pop_time;
/// ```
#[derive(Debug, Clone, Default)]
pub struct PopLiteMessageResponseHeader {
    pub(crate) pop_time: i64,
    pub(crate) invisible_time: i64,
    pub(crate) revive_queue_id: i32,
    pub(crate) start_offset_info: Option<String>,
    pub(crate) msg_offset_info: Option<String>,
    pub(crate) order_count_info: Option<String>,
}

impl PopLiteMessageResponseHeader {
    pub fn get_pop_time(&self) -> i64 {
        self.pop_time
    }

    pub fn set_pop_time(&mut self, value: i64) {
        self.pop_time = value;
    }

    pub fn get_invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub fn set_invisible_time(&mut self, value: i64) {
        self.invisible_time = value;
    }

    pub fn get_revive_qid(&self) -> i32 {
        self.revive_queue_id
    }

    pub fn set_revive_qid(&mut self, value: i32) {
        self.revive_queue_id = value;
    }

    pub fn get_start_offset_info(&self) -> Option<&str> {
        self.start_offset_info.as_deref()
    }

    pub fn set_start_offset_info(&mut self, value: Option<String>) {
        self.start_offset_info = value;
    }

    pub fn get_msg_offset_info(&self) -> Option<&str> {
        self.msg_offset_info.as_deref()
    }

    pub fn set_msg_offset_info(&mut self, value: Option<String>) {
        self.msg_offset_info = value;
    }

    pub fn get_order_count_info(&self) -> Option<&str> {
        self.order_count_info.as_deref()
    }

    pub fn set_order_count_info(&mut self, value: Option<String>) {
        self.order_count_info = value;
    }
}

impl CommandHeader for PopLiteMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("popTime".into(), self.pop_time.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("reviveQid".into(), self.revive_queue_id.to_string());
        put_optional(fields, "startOffsetInfo", self.start_offset_info.as_deref());
        put_optional(fields, "msgOffsetInfo", self.msg_offset_info.as_deref());
        put_optional(fields, "orderCountInfo", self.order_count_info.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            pop_time: optional_i64_with_mode(fields, "popTime", false)?.unwrap_or_default(),
            invisible_time: optional_i64_with_mode(fields, "invisibleTime", false)?
                .unwrap_or_default(),
            revive_queue_id: optional_i32_with_mode(fields, "reviveQid", false)?
                .unwrap_or_default(),
            start_offset_info: optional_string(fields, "startOffsetInfo"),
            msg_offset_info: optional_string(fields, "msgOffsetInfo"),
            order_count_info: optional_string(fields, "orderCountInfo"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Self::decode_fields(fields)
    }
}

/// Pinned Java `PollingInfoResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PollingInfoResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PollingInfoResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PollingInfoResponseHeader;
/// let _ = PollingInfoResponseHeader::default().polling_num;
/// ```
#[derive(Debug, Clone, Default)]
pub struct PollingInfoResponseHeader {
    pub(crate) polling_num: i32,
}

impl PollingInfoResponseHeader {
    pub fn get_polling_num(&self) -> i32 {
        self.polling_num
    }

    pub fn set_polling_num(&mut self, value: i32) {
        self.polling_num = value;
    }
}

impl CommandHeader for PollingInfoResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("pollingNum".into(), self.polling_num.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            polling_num: required_i32(fields, "pollingNum")?,
        })
    }
}

/// Java's response header for the POP change-invisibility RPC.
///
/// Missing and malformed primitive fields retain Java's zero defaults during
/// reflective custom-header decoding.
///
/// Java inherits identity-based `Object.equals` and `Object.hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ChangeInvisibleTimeResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<ChangeInvisibleTimeResponseHeader>(); }
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::ChangeInvisibleTimeResponseHeader;
/// let _ = ChangeInvisibleTimeResponseHeader::default().revive_qid;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct ChangeInvisibleTimeResponseHeader {
    pub(crate) pop_time: i64,
    pub(crate) invisible_time: i64,
    pub(crate) revive_qid: i32,
}

impl ChangeInvisibleTimeResponseHeader {
    pub fn get_pop_time(&self) -> i64 {
        self.pop_time
    }

    pub fn set_pop_time(&mut self, value: i64) {
        self.pop_time = value;
    }

    pub fn get_invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub fn set_invisible_time(&mut self, value: i64) {
        self.invisible_time = value;
    }

    pub fn get_revive_qid(&self) -> i32 {
        self.revive_qid
    }

    pub fn set_revive_qid(&mut self, value: i32) {
        self.revive_qid = value;
    }
}

impl CommandHeader for ChangeInvisibleTimeResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("popTime".into(), self.pop_time.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("reviveQid".into(), self.revive_qid.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            pop_time: fields
                .get("popTime")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default(),
            invisible_time: fields
                .get("invisibleTime")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default(),
            revive_qid: fields
                .get("reviveQid")
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or_default(),
        })
    }
}

impl CommandHeader for SendMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "msgId", self.msg_id.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "queueOffset", self.queue_offset);
        put_optional(fields, "transactionId", self.transaction_id.as_deref());
        put_optional(fields, "batchUniqId", self.batch_uniq_id.as_deref());
        put_optional(fields, "recallHandle", self.recall_handle.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        let header = Self {
            msg_id: optional_string(fields, "msgId"),
            queue_id: optional_i32_with_mode(fields, "queueId", use_fast_encode)?,
            queue_offset: optional_i64_with_mode(fields, "queueOffset", use_fast_encode)?,
            transaction_id: optional_string(fields, "transactionId"),
            batch_uniq_id: optional_string(fields, "batchUniqId"),
            recall_handle: optional_string(fields, "recallHandle"),
        };
        header.check_fields()?;
        Ok(header)
    }
}

impl crate::header::FastCodesHeader for PullMessageResponseHeader {}

impl crate::header::FastCodesHeader for SendMessageResponseHeader {}
