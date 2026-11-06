use base64::Engine;
use rocketmq_classic_common::message::{
    MessageClientExt, MessageExt, MessageExtSubtype, MessageHostAddress,
    MessageHostAddressBytesError, MessageQueue, NullableMessageProperties, SharedMessageBody,
    SharedMessageProperties,
};
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, ProcessQueue, PullConsumerConfig, PushConsumer, PushConsumerConfig,
    PushMessageListener,
    offset::{OffsetStore, RemoteBrokerOffsetStore},
    pull::PullConsumer,
};
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

struct OtherConsumer;
impl ConsumerRuntime for OtherConsumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
}

struct PriorityPropertyOnlySubtype;

impl MessageExtSubtype for PriorityPropertyOnlySubtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn property(&self, key: Option<&str>) -> Option<Option<String>> {
        (key == Some(rocketmq_classic_common::message::constants::PROPERTY_PRIORITY))
            .then(|| Some("23".to_owned()))
    }
}

#[derive(Default)]
struct DerivedGetterSubtype {
    getter_calls: std::sync::atomic::AtomicUsize,
}

impl MessageExtSubtype for DerivedGetterSubtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn tags(&self) -> Option<Option<String>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(Some("override-tags".to_owned()))
    }

    fn keys(&self) -> Option<Option<String>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(Some("override-keys".to_owned()))
    }

    fn wait_store_msg_ok(&self) -> Option<bool> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(true)
    }

    fn buyer_id(&self) -> Option<Option<String>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(Some("override-buyer".to_owned()))
    }

    fn delay_time_level(&self) -> Option<i32> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(31)
    }

    fn delay_time_sec(&self) -> Option<i64> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(32)
    }

    fn delay_time_ms(&self) -> Option<i64> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(33)
    }

    fn deliver_time_ms(&self) -> Option<i64> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(34)
    }

    fn topic_sys_flag(&self) -> Option<Option<i32>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(Some(35))
    }

    fn group_sys_flag(&self) -> Option<Option<i32>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(Some(36))
    }

    fn property(&self, key: Option<&str>) -> Option<Option<String>> {
        use rocketmq_classic_common::message::constants as property;
        let value = match key {
            Some(property::PROPERTY_TAGS) => "property-tags",
            Some(property::PROPERTY_KEYS) => "property-keys",
            Some(property::PROPERTY_WAIT_STORE_MSG_OK) => "false",
            Some(property::PROPERTY_BUYER_ID) => "property-buyer",
            Some(property::PROPERTY_DELAY_TIME_LEVEL) => "9",
            Some(property::PROPERTY_TIMER_DELAY_SEC) => "123",
            Some(property::PROPERTY_TIMER_DELAY_MS) => "456",
            Some(property::PROPERTY_TIMER_DELIVER_MS) => "789",
            Some(property::PROPERTY_TRANSIENT_TOPIC_CONFIG) => "73",
            Some(property::PROPERTY_TRANSIENT_GROUP_CONFIG) => "-19",
            _ => return None,
        };
        Some(Some(value.to_owned()))
    }
}

#[derive(Default)]
struct ExplicitNullGetterSubtype {
    getter_calls: std::sync::atomic::AtomicUsize,
}

impl MessageExtSubtype for ExplicitNullGetterSubtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn tags(&self) -> Option<Option<String>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(None)
    }

    fn keys(&self) -> Option<Option<String>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(None)
    }

    fn buyer_id(&self) -> Option<Option<String>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(None)
    }

    fn topic_sys_flag(&self) -> Option<Option<i32>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(None)
    }

    fn group_sys_flag(&self) -> Option<Option<i32>> {
        self.getter_calls.fetch_add(1, Ordering::SeqCst);
        Some(None)
    }

    fn property(&self, key: Option<&str>) -> Option<Option<String>> {
        use rocketmq_classic_common::message::constants as property;
        let value = match key {
            Some(property::PROPERTY_TAGS) => "property-tags",
            Some(property::PROPERTY_KEYS) => "property-keys",
            Some(property::PROPERTY_BUYER_ID) => "property-buyer",
            Some(property::PROPERTY_TRANSIENT_TOPIC_CONFIG) => "malformed-topic",
            Some(property::PROPERTY_TRANSIENT_GROUP_CONFIG) => "malformed-group",
            _ => return None,
        };
        Some(Some(value.to_owned()))
    }
}

#[derive(Default)]
struct CustomDirectMessageClientExt {
    offset_message_id: Mutex<Option<String>>,
}

impl MessageExtSubtype for CustomDirectMessageClientExt {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn offset_message_id(&self) -> Option<Option<String>> {
        Some(self.offset_message_id.lock().unwrap().clone())
    }

    fn set_offset_message_id(&self, offset_message_id: Option<String>) -> bool {
        *self.offset_message_id.lock().unwrap() =
            offset_message_id.map(|value| format!("subtype-{value}"));
        true
    }
}

fn callback_record(message: &MessageExt, queue: &MessageQueue) -> Value {
    let message_class = message
        .runtime_subtype_name()
        .map(|name| name.rsplit("::").next().unwrap_or(name))
        .unwrap_or(if message.is_message_client_ext() {
            "MessageClientExt"
        } else {
            "MessageExt"
        });
    let mut record = json!({"messageClass": message_class,
        "topic": message.topic_nullable(), "broker": message.broker_name(),
        "queueTopic": queue.topic(), "queueBroker": queue.broker_name(), "queueId": queue.queue_id(),
        "messageId": message.message_id_for_runtime_type(), "offsetMessageId": message.offset_message_id_for_runtime_type(),
        "reconsumeTimes": message.reconsume_times(), "queueOffset": message.queue_offset(),
        "flag": message.flag(),
        "storeHostPresent": message.store_host().is_some(),
        "bodyLength": message.body_nullable().map(|body| body.len()),
        "propertiesPresent": message.properties_nullable().is_some()});
    if let Some(subtype) = message.runtime_subtype::<CustomDirectMessageExt>() {
        record["messageQueueId"] = json!(message.queue_id());
        record["tags"] = json!(message.tags());
        record["keys"] = json!(message.keys());
        let properties = message.shared_properties();
        if let Some(properties) = properties {
            record["subtypeProperty"] = json!(properties.get(Some("subtype-only")).flatten());
            properties.insert(Some("through-view".into()), Some("visible".into()));
            let map_mutation_visible_to_subtype =
                subtype.custom_properties.get(Some("through-view")) == Some(Some("visible".into()));
            record["mapMutationVisibleToSubtype"] = json!(map_mutation_visible_to_subtype);
            subtype
                .custom_properties
                .insert(Some("through-subtype".into()), Some("visible".into()));
            let subtype_mutation_visible_to_map =
                properties.get(Some("through-subtype")) == Some(Some("visible".into()));
            record["subtypeMutationVisibleToMap"] = json!(subtype_mutation_visible_to_map);
            record["propertiesSharesSubtypeMap"] =
                json!(map_mutation_visible_to_subtype && subtype_mutation_visible_to_map);
            record["mutationVisibleToLaterGetter"] = json!(
                message
                    .get_properties()
                    .and_then(|properties| properties.get("through-view").cloned())
                    == Some("visible".into())
            );
        }
        let subtype_body = subtype.custom_body.lock().unwrap().clone();
        if let (Some(body), Some(subtype_body)) = (message.shared_body(), subtype_body) {
            let caller_body = subtype.caller_body.lock().unwrap().clone();
            record["bodySetterRetainsCallerArray"] =
                json!(caller_body.is_some_and(|caller| body.ptr_eq(&caller)));
            record["bodySetterMutationVisibleToGetter"] = json!(message.body().first() == Some(&2));
            record["bodySharesSubtypeArray"] = json!(body.ptr_eq(&subtype_body));
            body.with_bytes_mut(|bytes| {
                if let Some(first) = bytes.first_mut() {
                    *first = 9;
                }
            });
            record["bodyMutationVisibleToSubtype"] =
                json!(subtype_body.snapshot().first() == Some(&9));
            subtype_body.with_bytes_mut(|bytes| {
                if let Some(first) = bytes.first_mut() {
                    *first = 10;
                }
            });
            record["subtypeMutationVisibleToGetter"] = json!(message.body().first() == Some(&10));
        }
        record["bornTimestamp"] = json!(message.born_timestamp());
        record["storeTimestamp"] = json!(message.store_timestamp());
        record["sysFlag"] = json!(message.sys_flag());
        record["bodyCRC"] = json!(message.body_crc());
        record["commitLogOffset"] = json!(message.commit_log_offset());
        record["storeSize"] = json!(message.store_size());
        record["preparedTransactionOffset"] = json!(message.prepared_transaction_offset());
        record["bornHostString"] = json!(message.born_host_string());
        record["bornHostNameString"] = json!(message.born_host_name_string());
        record["bornHostBytes"] = json!(message.born_host_bytes().unwrap());
        record["storeHostBytes"] = json!(message.store_host_bytes().unwrap());
        let mut born_host_buffer = [0_u8; 8];
        let born_host_buffer_len = message
            .write_born_host_bytes(&mut born_host_buffer)
            .unwrap();
        record["bornHostBytesBuffer"] = json!(&born_host_buffer[..born_host_buffer_len]);
        let mut store_host_buffer = [0_u8; 8];
        let store_host_buffer_len = message
            .write_store_host_bytes(&mut store_host_buffer)
            .unwrap();
        record["storeHostBytesBuffer"] = json!(&store_host_buffer[..store_host_buffer_len]);
        record["bornHost"] = json!(message.born_host().map(ToString::to_string));
        record["storeHost"] = json!(message.store_host().map(ToString::to_string));
        record["topicSysFlag"] = json!(message.topic_sys_flag().unwrap());
        record["groupSysFlag"] = json!(message.group_sys_flag().unwrap());
        record["priority"] = json!(message.priority());
        record["priorityProperty"] = json!(
            message.get_property(rocketmq_classic_common::message::constants::PROPERTY_PRIORITY)
        );
        record["waitStore"] = json!(message.is_wait_store_msg_ok());
        record["buyerId"] = json!(message.buyer_id());
        record["delayTimeLevel"] = json!(message.delay_time_level().unwrap());
        record["delayTimeSec"] = json!(message.delay_time_sec().unwrap());
        record["delayTimeMs"] = json!(message.delay_time_ms().unwrap());
        record["deliverTimeMs"] = json!(message.deliver_time_ms().unwrap());
        record["userProperty"] = json!(message.get_user_property("user-probe"));
        record["hasProperty"] = json!(message.has_property("subtype-only"));
        record["transactionId"] = json!(message.transaction_id());
        record["customField"] = json!(subtype.custom_field());
        record["messageDisplay"] = json!(message.to_string());
    }
    record
}

struct CustomDirectMessageExt {
    broker_was_set: AtomicBool,
    topic_writes: std::sync::atomic::AtomicUsize,
    reconsume_writes: std::sync::atomic::AtomicUsize,
    queue_offset_writes: std::sync::atomic::AtomicUsize,
    body_writes: std::sync::atomic::AtomicUsize,
    flag_writes: std::sync::atomic::AtomicUsize,
    max_offset_reads: std::sync::atomic::AtomicUsize,
    tags_reads: std::sync::atomic::AtomicUsize,
    keys_reads: std::sync::atomic::AtomicUsize,
    properties_reads: std::sync::atomic::AtomicUsize,
    store_host_writes: std::sync::atomic::AtomicUsize,
    metadata_writes: std::sync::atomic::AtomicUsize,
    host_flag_writes: std::sync::atomic::AtomicUsize,
    topic_sys_flag_writes: std::sync::atomic::AtomicUsize,
    group_sys_flag_writes: std::sync::atomic::AtomicUsize,
    property_setter_writes: std::sync::atomic::AtomicUsize,
    user_property_reads: std::sync::atomic::AtomicUsize,
    has_property_reads: std::sync::atomic::AtomicUsize,
    transaction_id_reads: std::sync::atomic::AtomicUsize,
    user_api_writes: std::sync::atomic::AtomicUsize,
    priority_reads: std::sync::atomic::AtomicUsize,
    priority_writes: std::sync::atomic::AtomicUsize,
    custom_priority: std::sync::atomic::AtomicI32,
    custom_born_host: MessageHostAddress,
    custom_store_host: MessageHostAddress,
    custom_body: Mutex<Option<SharedMessageBody>>,
    caller_body: Mutex<Option<SharedMessageBody>>,
    custom_properties: SharedMessageProperties,
}

impl CustomDirectMessageExt {
    fn new() -> Self {
        Self {
            broker_was_set: AtomicBool::new(false),
            topic_writes: std::sync::atomic::AtomicUsize::new(0),
            reconsume_writes: std::sync::atomic::AtomicUsize::new(0),
            queue_offset_writes: std::sync::atomic::AtomicUsize::new(0),
            body_writes: std::sync::atomic::AtomicUsize::new(0),
            flag_writes: std::sync::atomic::AtomicUsize::new(0),
            max_offset_reads: std::sync::atomic::AtomicUsize::new(0),
            tags_reads: std::sync::atomic::AtomicUsize::new(0),
            keys_reads: std::sync::atomic::AtomicUsize::new(0),
            properties_reads: std::sync::atomic::AtomicUsize::new(0),
            store_host_writes: std::sync::atomic::AtomicUsize::new(0),
            metadata_writes: std::sync::atomic::AtomicUsize::new(0),
            host_flag_writes: std::sync::atomic::AtomicUsize::new(0),
            topic_sys_flag_writes: std::sync::atomic::AtomicUsize::new(0),
            group_sys_flag_writes: std::sync::atomic::AtomicUsize::new(0),
            property_setter_writes: std::sync::atomic::AtomicUsize::new(0),
            user_property_reads: std::sync::atomic::AtomicUsize::new(0),
            has_property_reads: std::sync::atomic::AtomicUsize::new(0),
            transaction_id_reads: std::sync::atomic::AtomicUsize::new(0),
            user_api_writes: std::sync::atomic::AtomicUsize::new(0),
            priority_reads: std::sync::atomic::AtomicUsize::new(0),
            priority_writes: std::sync::atomic::AtomicUsize::new(0),
            custom_priority: std::sync::atomic::AtomicI32::new(37),
            custom_born_host: MessageHostAddress::new("127.0.0.1", 10913),
            custom_store_host: MessageHostAddress::new("127.0.0.1", 10913),
            custom_body: Mutex::new(None),
            caller_body: Mutex::new(None),
            custom_properties: SharedMessageProperties::from_nullable_properties(
                NullableMessageProperties::from([(
                    Some("subtype-only".to_owned()),
                    Some("visible".to_owned()),
                )]),
            ),
        }
    }

    fn custom_field(&self) -> String {
        format!(
            "topicWrites={};reconsumeWrites={};queueOffsetWrites={};bodyWrites={};flagWrites={};maxOffsetReads={};tagsReads={};keysReads={};propertiesReads={};storeHostWrites={};metadataWrites={};hostFlagWrites={};topicSysFlagWrites={};groupSysFlagWrites={};propertySetterWrites={};userPropertyReads={};hasPropertyReads={};transactionIdReads={};userApiWrites={};priorityWrites={};priorityReads={}",
            self.topic_writes.load(Ordering::SeqCst),
            self.reconsume_writes.load(Ordering::SeqCst),
            self.queue_offset_writes.load(Ordering::SeqCst),
            self.body_writes.load(Ordering::SeqCst),
            self.flag_writes.load(Ordering::SeqCst),
            self.max_offset_reads.load(Ordering::SeqCst),
            self.tags_reads.load(Ordering::SeqCst),
            self.keys_reads.load(Ordering::SeqCst),
            self.properties_reads.load(Ordering::SeqCst),
            self.store_host_writes.load(Ordering::SeqCst),
            self.metadata_writes.load(Ordering::SeqCst),
            self.host_flag_writes.load(Ordering::SeqCst),
            self.topic_sys_flag_writes.load(Ordering::SeqCst),
            self.group_sys_flag_writes.load(Ordering::SeqCst),
            self.property_setter_writes.load(Ordering::SeqCst),
            self.user_property_reads.load(Ordering::SeqCst),
            self.has_property_reads.load(Ordering::SeqCst),
            self.transaction_id_reads.load(Ordering::SeqCst),
            self.user_api_writes.load(Ordering::SeqCst),
            self.priority_writes.load(Ordering::SeqCst),
            self.priority_reads.load(Ordering::SeqCst)
        )
    }
}

impl MessageExtSubtype for CustomDirectMessageExt {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn topic(&self) -> Option<Option<&str>> {
        Some(Some("%RETRY%tenant%direct"))
    }

    fn set_topic(&self, _topic: Option<String>) -> bool {
        self.topic_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn property(&self, key: Option<&str>) -> Option<Option<String>> {
        match key {
            Some(rocketmq_classic_common::message::constants::PROPERTY_MAX_OFFSET) => {
                self.max_offset_reads.fetch_add(1, Ordering::SeqCst);
                Some(Some("987654333".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_TAGS) => {
                self.tags_reads.fetch_add(1, Ordering::SeqCst);
                Some(Some("subtype-tag".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_KEYS) => {
                self.keys_reads.fetch_add(1, Ordering::SeqCst);
                Some(Some("subtype-key".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG) => {
                Some(Some("73".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG) => {
                Some(Some("-19".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_WAIT_STORE_MSG_OK) => {
                Some(Some("false".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_BUYER_ID) => {
                Some(Some("subtype-buyer".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_DELAY_TIME_LEVEL) => {
                Some(Some("9".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_TIMER_DELAY_SEC) => {
                Some(Some("123".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_TIMER_DELAY_MS) => {
                Some(Some("456".to_owned()))
            }
            Some(rocketmq_classic_common::message::constants::PROPERTY_TIMER_DELIVER_MS) => {
                Some(Some("789".to_owned()))
            }
            _ => None,
        }
    }

    fn priority(&self) -> Option<i32> {
        self.priority_reads.fetch_add(1, Ordering::SeqCst);
        Some(self.custom_priority.load(Ordering::SeqCst))
    }

    fn set_priority(&self, priority: i32) -> bool {
        self.priority_writes.fetch_add(1, Ordering::SeqCst);
        self.custom_priority.store(priority, Ordering::SeqCst);
        true
    }

    fn user_property(&self, key: Option<&str>) -> Option<Option<String>> {
        if key == Some("user-probe") {
            self.user_property_reads.fetch_add(1, Ordering::SeqCst);
            Some(Some("subtype-user-value".to_owned()))
        } else {
            None
        }
    }

    fn has_property(&self, key: Option<&str>) -> Option<bool> {
        if key == Some("subtype-only") {
            self.has_property_reads.fetch_add(1, Ordering::SeqCst);
            Some(false)
        } else {
            None
        }
    }

    fn transaction_id(&self) -> Option<Option<&str>> {
        self.transaction_id_reads.fetch_add(1, Ordering::SeqCst);
        Some(Some("subtype-transaction"))
    }

    fn live_properties(&self) -> Option<Option<&SharedMessageProperties>> {
        self.properties_reads.fetch_add(1, Ordering::SeqCst);
        Some(Some(&self.custom_properties))
    }

    fn set_topic_sys_flag(&self, _topic_sys_flag: Option<i32>) -> bool {
        self.topic_sys_flag_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_group_sys_flag(&self, _group_sys_flag: Option<i32>) -> bool {
        self.group_sys_flag_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_wait_store_msg_ok(&self, _wait: bool) -> bool {
        self.property_setter_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_buyer_id(&self, _buyer_id: Option<String>) -> bool {
        self.property_setter_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_instance_id(&self, _instance_id: Option<String>) -> bool {
        self.property_setter_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_delay_time_level(&self, _level: i32) -> bool {
        self.property_setter_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_delay_time_sec(&self, _seconds: i64) -> bool {
        self.property_setter_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_delay_time_ms(&self, _milliseconds: i64) -> bool {
        self.property_setter_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_deliver_time_ms(&self, _milliseconds: i64) -> bool {
        self.property_setter_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn put_user_property(&self, _key: Option<String>, _value: Option<String>) -> bool {
        self.user_api_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_tags(&self, _tags: Option<String>) -> bool {
        self.user_api_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_keys(&self, _keys: Option<String>) -> bool {
        self.user_api_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_transaction_id(&self, _transaction_id: Option<String>) -> bool {
        self.user_api_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn store_host(&self) -> Option<Option<&MessageHostAddress>> {
        Some(Some(&self.custom_store_host))
    }

    fn set_store_host(&self, _store_host: Option<MessageHostAddress>) -> bool {
        self.store_host_writes.fetch_add(1, Ordering::SeqCst);
        false
    }

    fn born_timestamp(&self) -> Option<i64> {
        Some(1_700_000_000_123)
    }

    fn set_born_timestamp(&self, _born_timestamp: i64) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn born_host(&self) -> Option<Option<&MessageHostAddress>> {
        Some(Some(&self.custom_born_host))
    }

    fn born_host_string(&self) -> Option<Option<String>> {
        Some(Some("custom-born-host-address".to_owned()))
    }

    fn born_host_name_string(&self) -> Option<Option<&str>> {
        Some(Some("custom-born-host-name"))
    }

    fn born_host_bytes(&self) -> Option<Vec<u8>> {
        Some(vec![10, 11, 12])
    }

    fn store_host_bytes(&self) -> Option<Vec<u8>> {
        Some(vec![13, 14, 15])
    }

    fn write_born_host_bytes(
        &self,
        output: &mut [u8],
    ) -> Option<Result<usize, MessageHostAddressBytesError>> {
        Some(write_custom_host_bytes(&[1, 2, 3, 4], output))
    }

    fn write_store_host_bytes(
        &self,
        output: &mut [u8],
    ) -> Option<Result<usize, MessageHostAddressBytesError>> {
        Some(write_custom_host_bytes(&[5, 6, 7, 8], output))
    }

    fn set_born_host(&self, _born_host: Option<MessageHostAddress>) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        false
    }

    fn store_timestamp(&self) -> Option<i64> {
        Some(1_700_000_000_456)
    }

    fn set_store_timestamp(&self, _store_timestamp: i64) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn sys_flag(&self) -> Option<i32> {
        Some(0x1234)
    }

    fn set_sys_flag(&self, _sys_flag: i32) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_born_host_v6_flag(&self) -> bool {
        self.host_flag_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn set_store_host_address_v6_flag(&self) -> bool {
        self.host_flag_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn body_crc(&self) -> Option<i32> {
        Some(0x4567)
    }

    fn set_body_crc(&self, _body_crc: i32) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn commit_log_offset(&self) -> Option<i64> {
        Some(9_876_543)
    }

    fn set_commit_log_offset(&self, _commit_log_offset: i64) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn store_size(&self) -> Option<i32> {
        Some(765)
    }

    fn set_store_size(&self, _store_size: i32) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn prepared_transaction_offset(&self) -> Option<i64> {
        Some(1_234_567)
    }

    fn set_prepared_transaction_offset(&self, _prepared_transaction_offset: i64) -> bool {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn body(&self) -> Option<Option<Vec<u8>>> {
        Some(
            self.custom_body
                .lock()
                .unwrap()
                .as_ref()
                .map(SharedMessageBody::snapshot),
        )
    }

    fn shared_body(&self) -> Option<Option<SharedMessageBody>> {
        Some(self.custom_body.lock().unwrap().clone())
    }

    fn set_body(&self, body: Option<Vec<u8>>) -> bool {
        self.body_writes.fetch_add(1, Ordering::SeqCst);
        *self.custom_body.lock().unwrap() = body.map(SharedMessageBody::new);
        true
    }

    fn set_shared_body(&self, body: Option<SharedMessageBody>) -> bool {
        self.body_writes.fetch_add(1, Ordering::SeqCst);
        *self.custom_body.lock().unwrap() = body.clone();
        *self.caller_body.lock().unwrap() = body;
        true
    }

    fn flag(&self) -> Option<i32> {
        Some(71)
    }

    fn set_flag(&self, _flag: i32) -> bool {
        self.flag_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn broker_name(&self) -> Option<Option<&str>> {
        Some(
            self.broker_was_set
                .load(Ordering::SeqCst)
                .then_some("broker-a"),
        )
    }

    fn set_broker_name(&self, broker_name: Option<String>) -> bool {
        self.broker_was_set
            .store(broker_name.is_some(), Ordering::SeqCst);
        true
    }

    fn queue_id(&self) -> Option<i32> {
        Some(37)
    }

    fn set_queue_id(&self, _queue_id: i32) -> bool {
        true
    }

    fn queue_offset(&self) -> Option<i64> {
        Some(987_654_321)
    }

    fn set_queue_offset(&self, _queue_offset: i64) -> bool {
        self.queue_offset_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn message_id(&self) -> Option<Option<String>> {
        Some(Some("subtype-message-id".to_owned()))
    }

    fn set_message_id(&self, _message_id: Option<String>) -> bool {
        true
    }

    fn reconsume_times(&self) -> Option<i32> {
        Some(13)
    }

    fn set_reconsume_times(&self, _reconsume_times: i32) -> bool {
        self.reconsume_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn display_value(&self) -> Option<String> {
        Some(format!(
            "CustomDirectMessageExt[topicWrites={}]",
            self.topic_writes.load(Ordering::SeqCst)
        ))
    }
}

fn write_custom_host_bytes(
    bytes: &[u8],
    output: &mut [u8],
) -> Result<usize, MessageHostAddressBytesError> {
    if output.len() < bytes.len() {
        return Err(MessageHostAddressBytesError::BufferTooSmall {
            needed: bytes.len(),
            actual: output.len(),
        });
    }
    output[..bytes.len()].copy_from_slice(bytes);
    Ok(bytes.len())
}

#[tokio::test]
async fn direct_runtime_consumption_matches_java_status_callbacks_and_offset_isolation() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let cases = fixture["directConsume"].as_array().unwrap();
    assert_eq!(cases.len(), 29);
    for case in cases {
        let name = case["name"].as_str().unwrap().to_owned();
        let manager = ClientRuntimeManager::shared_default();
        let mut client_config = ClientConfig::new(format!("runtime-direct-{name}"));
        client_config.set_namesrv_addr(Some(String::new()));
        client_config.set_instance_name(Some(name.clone()));
        client_config.set_namespace(Some("tenant".into()));
        let runtime = manager.get_or_create(&client_config).await.unwrap();
        let store = Arc::new(RemoteBrokerOffsetStore::new(
            runtime.clone(),
            "tenant%direct",
        ));
        let queue = MessageQueue::new("tenant%orders", "broker-a", 3);
        store.update_offset(&queue, 123, false);
        let mut config = PushConsumerConfig::new("direct").unwrap();
        *config.client_config_mut() = client_config.clone();
        let push = Arc::new(PushConsumer::new(config));
        push.set_offset_store(Some(store.clone())).await.unwrap();
        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let records = callbacks.clone();
        let listener_name = name.clone();
        if name.starts_with("orderly") {
            push.register_message_listener(PushMessageListener::orderly(
                move |messages: Vec<MessageExt>, mut context: ConsumeOrderlyContext| {
                    let records = records.clone();
                    let name = listener_name.clone();
                    async move {
                        records
                            .lock()
                            .unwrap()
                            .push(callback_record(&messages[0], context.message_queue()));
                        context.set_auto_commit(false);
                        let status = if name.ends_with("null-status") {
                            None
                        } else if name.ends_with("commit") {
                            Some(ConsumeOrderlyStatus::Commit)
                        } else if name.ends_with("rollback") {
                            Some(ConsumeOrderlyStatus::Rollback)
                        } else if name.ends_with("later") {
                            Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment)
                        } else {
                            Some(ConsumeOrderlyStatus::Success)
                        };
                        (status, context)
                    }
                },
            ))
            .await
            .unwrap();
        } else {
            push.register_message_listener(PushMessageListener::concurrently(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let records = records.clone();
                    let name = listener_name.clone();
                    async move {
                        records
                            .lock()
                            .unwrap()
                            .push(callback_record(&messages[0], context.message_queue()));
                        assert!(!name.ends_with("throws"), "listener failed");
                        let status = if name.ends_with("null-status") {
                            None
                        } else if name.ends_with("later") {
                            Some(ConsumeConcurrentlyStatus::ReconsumeLater)
                        } else {
                            Some(ConsumeConcurrentlyStatus::ConsumeSuccess)
                        };
                        (status, context)
                    }
                },
            ))
            .await
            .unwrap();
        }
        push.start().await.unwrap();
        let push_owner = runtime.select_consumer("tenant%direct").unwrap().unwrap();
        let mut pull_config = PullConsumerConfig::new("pull").unwrap();
        *pull_config.client_config_mut() = client_config;
        let pull = Arc::new(PullConsumer::new(pull_config));
        if name == "pull-group" {
            pull.start().await.unwrap();
        }
        let selected: Arc<dyn ConsumerRuntime> = if name == "other-group" {
            Arc::new(OtherConsumer)
        } else if name == "pull-group" {
            runtime.select_consumer("tenant%pull").unwrap().unwrap()
        } else {
            push_owner
        };
        if name == "push-null-service" {
            push.shutdown().await.unwrap();
        }
        runtime
            .consumer_table()
            .put("selected", selected)
            .await
            .unwrap();
        let client_message = case["inputClient"].as_bool().unwrap();
        let physical = case["inputMessageId"].as_str().map(str::to_owned);
        let unique = case["inputUniq"].as_str().map(str::to_owned);
        // Java accepts a MessageClientExt through its MessageExt parameter and
        // preserves the runtime subclass for the listener callback.
        let message: MessageExt = if name == "concurrent-custom-subtype" {
            let mut message = MessageExt::default();
            message.set_runtime_subtype(Arc::new(CustomDirectMessageExt::new()));
            message.set_topic_nullable(case["inputTopic"].as_str().map(str::to_owned));
            message.set_queue_id(3);
            let caller_body = SharedMessageBody::new([1]);
            message.set_body_shared(Some(caller_body.clone()));
            caller_body.with_bytes_mut(|bytes| bytes[0] = 2);
            message.set_message_id(physical);
            message
                .set_system_property("RETRY_TOPIC", "subtype-retry-target")
                .unwrap();
            message.set_reconsume_times(2);
            message.set_queue_offset(17);
            message.set_flag(5);
            message.set_store_host(Some(MessageHostAddress::new("127.0.0.1", 10911)));
            message.set_born_timestamp(1);
            message.set_store_timestamp(2);
            message.set_sys_flag(3);
            message.set_born_host_v6_flag();
            message.set_store_host_address_v6_flag();
            message.set_body_crc(4);
            message.set_commit_log_offset(5);
            message.set_store_size(6);
            message.set_prepared_transaction_offset(7);
            message.set_born_host(Some(MessageHostAddress::new("127.0.0.1", 10912)));
            message.set_topic_sys_flag(Some(5));
            message.set_group_sys_flag(Some(6));
            message.set_priority(-7).unwrap();
            message.set_wait_store_msg_ok(true).unwrap();
            message.set_buyer_id("base-buyer");
            message.set_instance_id("base-instance");
            message.set_delay_time_level(4);
            message.set_delay_time_sec(5);
            message.set_delay_time_ms(6);
            message.set_deliver_time_ms(7);
            message
                .put_user_property("base-user", "base-value")
                .unwrap();
            message.put_user_property("", "").unwrap();
            message.put_user_property_nullable(None, None).unwrap();
            message.set_tags("base-tags").unwrap();
            message.set_keys("base-key").unwrap();
            message.set_keys_from(["first", "second"]);
            message.set_transaction_id(Some("base-transaction".to_owned()));
            message
        } else if name.starts_with("concurrent-client-custom-subtype") {
            let mut message = MessageClientExt::new();
            message.set_runtime_subtype(Arc::new(CustomDirectMessageClientExt::default()));
            message.set_topic_nullable(case["inputTopic"].as_str().map(str::to_owned));
            message.set_queue_id(3);
            message.set_body(vec![1]);
            if let Some(unique) = unique.as_deref() {
                message.set_system_property("UNIQ_KEY", unique).unwrap();
            }
            message.set_offset_message_id(physical);
            message.into_message_ext()
        } else if client_message {
            let mut message = MessageClientExt::new();
            message.set_topic_nullable(case["inputTopic"].as_str().map(str::to_owned));
            message.set_queue_id(3);
            message.set_body(vec![1]);
            if let Some(unique) = unique.as_deref() {
                message.set_system_property("UNIQ_KEY", unique).unwrap();
            }
            message.set_offset_message_id(physical);
            message.into_message_ext()
        } else {
            let mut message = MessageExt::default();
            message.set_topic_nullable(case["inputTopic"].as_str().map(str::to_owned));
            message.set_queue_id(3);
            message.set_body(vec![1]);
            if let Some(unique) = unique {
                message.set_system_property("UNIQ_KEY", unique).unwrap();
            }
            message.set_message_id(physical);
            message
        };
        let message = (!case["nullMessage"].as_bool().unwrap()).then_some(message);
        let result = runtime
            .consume_message_directly(message, case["group"].as_str(), case["broker"].as_str())
            .await;
        if case["error"].is_null() {
            let result = result.unwrap();
            assert_eq!(
                result.is_some(),
                case["resultPresent"].as_bool().unwrap(),
                "{name}"
            );
            if let Some(result) = result {
                assert_eq!(
                    serde_json::to_value(result.get_consume_result()).unwrap(),
                    case["result"],
                    "{name}"
                );
                assert_eq!(
                    result.is_order(),
                    case["order"].as_bool().unwrap(),
                    "{name}"
                );
                assert_eq!(
                    result.is_auto_commit(),
                    case["autoCommit"].as_bool().unwrap(),
                    "{name}"
                );
                assert_eq!(
                    result.get_remark().is_some(),
                    case["remarkPresent"].as_bool().unwrap(),
                    "{name}"
                );
                assert!(result.get_spent_time_mills() >= 0);
            }
        } else {
            assert!(
                matches!(
                    result,
                    Err(RuntimeError::ConsumerOperation(_) | RuntimeError::InvalidRoute(_))
                ),
                "{name}: {result:?}"
            );
        }
        assert_eq!(
            json!(*callbacks.lock().unwrap()),
            case["callbacks"],
            "{name}"
        );
        assert_eq!(
            store.clone_offset_table(None),
            HashMap::from([(queue, 123)]),
            "{name}"
        );
        runtime.consumer_table().remove("selected").await.unwrap();
        pull.shutdown().await.unwrap();
        push.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}

#[test]
fn inherited_priority_methods_use_subtype_property_and_base_setter_semantics() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["priorityPropertySubtype"];
    let mut message = MessageExt::default();
    message.set_runtime_subtype(Arc::new(PriorityPropertyOnlySubtype));

    assert_eq!(message.set_priority(17), Ok(()));
    assert_eq!(
        message.priority(),
        expected["priority"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.get_property(rocketmq_classic_common::message::constants::PROPERTY_PRIORITY),
        expected["priorityProperty"].as_str().map(str::to_owned)
    );
    assert_eq!(
        message.get_properties().and_then(|properties| properties
            .get(rocketmq_classic_common::message::constants::PROPERTY_PRIORITY)
            .cloned()),
        expected["storedPriorityProperty"]
            .as_str()
            .map(str::to_owned)
    );
    assert_eq!(
        message.set_priority(-1),
        Err(rocketmq_classic_common::message::MessagePropertyError::NegativePriority)
    );
    assert_eq!(expected["negativePriorityRejected"], true);
}

#[test]
fn direct_subtype_overrides_win_over_property_derived_message_getters() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["derivedGetterSubtype"];
    let mut message = MessageExt::default();
    message.set_runtime_subtype(Arc::new(DerivedGetterSubtype::default()));

    assert_eq!(message.tags(), expected["tags"].as_str().map(str::to_owned));
    assert_eq!(message.keys(), expected["keys"].as_str().map(str::to_owned));
    assert_eq!(
        message.is_wait_store_msg_ok(),
        expected["waitStore"].as_bool().unwrap()
    );
    assert_eq!(
        message.buyer_id(),
        expected["buyerId"].as_str().map(str::to_owned)
    );
    assert_eq!(
        message.delay_time_level().unwrap(),
        expected["delayTimeLevel"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.delay_time_sec().unwrap(),
        expected["delayTimeSec"].as_i64().unwrap()
    );
    assert_eq!(
        message.delay_time_ms().unwrap(),
        expected["delayTimeMs"].as_i64().unwrap()
    );
    assert_eq!(
        message.deliver_time_ms().unwrap(),
        expected["deliverTimeMs"].as_i64().unwrap()
    );
    assert_eq!(
        message.topic_sys_flag().unwrap(),
        Some(expected["topicSysFlag"].as_i64().unwrap() as i32)
    );
    assert_eq!(
        message.group_sys_flag().unwrap(),
        Some(expected["groupSysFlag"].as_i64().unwrap() as i32)
    );
    assert_eq!(
        message.get_property(rocketmq_classic_common::message::constants::PROPERTY_TAGS),
        expected["propertyTags"].as_str().map(str::to_owned)
    );
    assert_eq!(
        message.get_property(rocketmq_classic_common::message::constants::PROPERTY_KEYS),
        expected["propertyKeys"].as_str().map(str::to_owned)
    );
    assert_eq!(
        message
            .runtime_subtype::<DerivedGetterSubtype>()
            .unwrap()
            .getter_calls
            .load(Ordering::SeqCst),
        expected["getterCalls"].as_u64().unwrap() as usize
    );
}

#[test]
fn direct_subtype_nullable_getter_overrides_preserve_java_null() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["derivedGetterNullSubtype"];
    let mut message = MessageExt::default();
    message.set_runtime_subtype(Arc::new(ExplicitNullGetterSubtype::default()));

    assert_eq!(message.tags(), expected["tags"].as_str().map(str::to_owned));
    assert_eq!(message.keys(), expected["keys"].as_str().map(str::to_owned));
    assert_eq!(
        message.buyer_id(),
        expected["buyerId"].as_str().map(str::to_owned)
    );
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);
    assert_eq!(
        message.get_property(rocketmq_classic_common::message::constants::PROPERTY_TAGS),
        expected["propertyTags"].as_str().map(str::to_owned)
    );
    assert_eq!(
        message.get_property(rocketmq_classic_common::message::constants::PROPERTY_KEYS),
        expected["propertyKeys"].as_str().map(str::to_owned)
    );
    assert_eq!(
        message.get_property(rocketmq_classic_common::message::constants::PROPERTY_BUYER_ID),
        expected["propertyBuyerId"].as_str().map(str::to_owned)
    );
    assert_eq!(
        message
            .get_property(
                rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG
            )
            .as_deref(),
        expected["propertyTopicSysFlag"].as_str()
    );
    assert_eq!(
        message
            .get_property(
                rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG
            )
            .as_deref(),
        expected["propertyGroupSysFlag"].as_str()
    );
    assert_eq!(
        message
            .runtime_subtype::<ExplicitNullGetterSubtype>()
            .unwrap()
            .getter_calls
            .load(Ordering::SeqCst),
        expected["getterCalls"].as_u64().unwrap() as usize
    );
}

#[test]
fn custom_subtype_queue_offset_matches_java_process_queue() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["customSubtypeProcessQueue"];

    let mut message = MessageExt::default();
    message.set_runtime_subtype(Arc::new(CustomDirectMessageExt::new()));
    message.set_queue_offset(17);
    message.set_body(vec![1, 2, 3]);
    let process_queue = ProcessQueue::new();
    let dispatch = process_queue
        .put_message(&[message.clone()])
        .expect("valid subtype message");
    let offsets = process_queue
        .get_msg_tree_map()
        .read()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let subtype = message.runtime_subtype::<CustomDirectMessageExt>().unwrap();

    assert_eq!(
        json!({
            "dispatch": dispatch,
            "count": process_queue.get_msg_count().get(),
            "size": process_queue.get_msg_size().get(),
            "msgAccCnt": process_queue.get_msg_acc_cnt(),
            "offsets": offsets,
            "customField": subtype.custom_field(),
        }),
        *expected
    );
}

#[tokio::test]
async fn inbound_direct_consume_decodes_client_ids_before_invoking_the_owner() {
    use rocketmq_classic_common::message::{DecodeOptions, decode_one, encode_message_ext};
    use rocketmq_classic_core::runtime::ConsumerOperation;
    use rocketmq_classic_protocol::codes::{request, response};
    use rocketmq_classic_remoting::RemotingCommand;
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "ipv4-v1")
        .unwrap();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap();
    let (mut message, _) = decode_one(&bytes, DecodeOptions::default()).unwrap();
    let physical_id = message.message_id().unwrap().to_owned();
    message.set_topic("tenant%orders");
    message.set_store_size(0);
    message
        .set_system_property("UNIQ_KEY", "wire-client-id")
        .unwrap();
    let body = encode_message_ext(&message, false).unwrap();
    let mut config = PushConsumerConfig::new("wire-client-ids").unwrap();
    config
        .client_config_mut()
        .set_client_ip(Some("wire-client-ids".into()));
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::shared_default()
        .get_or_create(config.client_config())
        .await
        .unwrap();
    let consumer = Arc::new(PushConsumer::new(config));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let callbacks = observed.clone();
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let callbacks = callbacks.clone();
                async move {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(callback_record(&messages[0], context.message_queue()));
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        ))
        .await
        .unwrap();
    consumer.start().await.unwrap();
    let processor = runtime
        .select_consumer("tenant%wire-client-ids")
        .unwrap()
        .unwrap()
        .inbound_processor(ConsumerOperation::ConsumeDirectly)
        .unwrap();
    let result = tokio::task::spawn_blocking(move || {
        processor.process(RemotingCommand {
            code: request::CONSUME_MESSAGE_DIRECTLY,
            ext_fields: Some(
                std::collections::BTreeMap::from([
                    ("consumerGroup".into(), "tenant%wire-client-ids".into()),
                    ("brokerName".into(), "wire-broker".into()),
                ])
                .into(),
            ),
            body: Some(body),
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(result.code, response::SUCCESS);
    let callbacks = observed.lock().unwrap().clone();
    assert_eq!(callbacks.len(), 1);
    assert_eq!(callbacks[0]["messageId"], "wire-client-id");
    assert_eq!(callbacks[0]["offsetMessageId"], physical_id);
    assert_eq!(callbacks[0]["topic"], "orders");
    assert_eq!(callbacks[0]["queueTopic"], "tenant%orders");
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}
