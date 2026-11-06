use base64::Engine;
use rocketmq_classic_common::{
    message::{
        BLANK_MAGIC_CODE, CHARSET_UTF8, CompactMessageError, DecodeError, DecodeOptions,
        MESSAGE_FLAG_POSITION, MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_POSITION,
        MESSAGE_MAGIC_CODE_V2, MESSAGE_PHYSIC_OFFSET_POSITION, MESSAGE_STORE_TIMESTAMP_POSITION,
        Message, MessageClientExt, MessageExt, MessageExtSubtype, MessageHostAddress,
        MessageHostAddressBytesError, MessageId, MessageIdError, MessageProperties, MessageVersion,
        MessageVersionError, PHY_POS_POSITION, PROPERTY_SEPARATOR, QUEUE_OFFSET_POSITION,
        SYSFLAG_POSITION, TopicFilterType, append_crc32_property, client_decode, client_decodes,
        client_decodes_with_body, count_inner_messages, create_message_id,
        create_message_id_from_buffers, decode_compact_message, decode_compact_messages,
        decode_inner_batch, decode_many_as_client_compatible, decode_many_compatible,
        decode_message_id, decode_one, decode_one_as_client, decode_properties_nullable,
        decode_record_properties, encode_compact_message, encode_compact_messages,
        encode_message_ext, encode_message_ext_uniquely, encode_nullable_property_entries,
        encode_properties_nullable,
    },
    sysflag::{message, pull},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// These fixtures both inspect or mutate the process-wide Java-compatible
// reserved-property set, so keep them isolated from one another.
static MESSAGE_CONSTANTS_MUTATION_LOCK: Mutex<()> = Mutex::new(());

#[derive(Default)]
struct OffsetOnlyClientExtSubtype {
    offset_message_id: Mutex<Option<String>>,
    offset_writes: Mutex<Vec<Option<String>>>,
}

impl MessageExtSubtype for OffsetOnlyClientExtSubtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn offset_message_id(&self) -> Option<Option<String>> {
        Some(self.offset_message_id.lock().unwrap().clone())
    }

    fn set_offset_message_id(&self, offset_message_id: Option<String>) -> bool {
        self.offset_writes
            .lock()
            .unwrap()
            .push(offset_message_id.clone());
        *self.offset_message_id.lock().unwrap() =
            offset_message_id.map(|value| format!("subtype-{value}"));
        true
    }
}

#[derive(Default)]
struct ClientMessageIdOverrideSubtype {
    message_id: Mutex<Option<String>>,
    message_id_writes: Mutex<Vec<Option<String>>>,
}

impl MessageExtSubtype for ClientMessageIdOverrideSubtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn message_id(&self) -> Option<Option<String>> {
        Some(self.message_id.lock().unwrap().clone())
    }

    fn set_message_id(&self, message_id: Option<String>) -> bool {
        self.message_id_writes
            .lock()
            .unwrap()
            .push(message_id.clone());
        *self.message_id.lock().unwrap() = message_id.map(|value| format!("subtype-{value}"));
        true
    }
}

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap()
}

fn object_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn record(case: &Value) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap()
}

fn message_ext_from(message: Message) -> MessageExt {
    let mut extended = MessageExt::new();
    extended.set_topic_nullable(message.topic_nullable().map(str::to_owned));
    extended.set_body_nullable(message.body_nullable().map(|body| body.to_vec()));
    extended.set_flag(message.flag());
    let properties = message
        .properties_nullable()
        .is_some()
        .then(|| message.nullable_property_entries().collect());
    extended.set_nullable_properties(properties);
    extended.set_transaction_id(message.transaction_id().map(str::to_owned));
    extended
}

fn decoded_snapshot(message: &MessageExt, consumed: usize) -> Value {
    let mut snapshot = json!({
        "messageClass": if message.is_message_client_ext() {
            "MessageClientExt"
        } else {
            "MessageExt"
        },
        "topic": message.topic_nullable(),
        "sysFlag": message.sys_flag(),
        "messageId": message.message_id(),
        "consumed": consumed,
    });
    if let Some(body_base64) = message
        .body_nullable()
        .map(|body| base64::engine::general_purpose::STANDARD.encode(&*body))
    {
        snapshot["bodyBase64"] = json!(body_base64);
    }
    if let Some(properties) = message.properties_nullable() {
        snapshot["properties"] = serde_json::to_value(&*properties).unwrap();
    }
    if let Some(offset_message_id) = message.offset_message_id() {
        snapshot["offsetMsgId"] = json!(offset_message_id);
    }
    snapshot
}

fn encoded_body(record: &[u8]) -> &[u8] {
    let sys_flag = i32::from_be_bytes(record[36..40].try_into().unwrap());
    let born_host_length = if sys_flag & message::BORNHOST_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let store_host_length = if sys_flag & message::STOREHOSTADDRESS_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let body_length_offset =
        4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + born_host_length + 8 + store_host_length + 4 + 8;
    let body_length = i32::from_be_bytes(
        record[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    &record[body_length_offset + 4..body_length_offset + 4 + body_length]
}

#[test]
fn java_551_message_records_decode_to_the_pinned_results() {
    let fixture = fixture();
    assert_eq!(
        fixture["provenance"]["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    for case in fixture["cases"].as_array().unwrap() {
        let bytes = record(case);
        let (decoded, consumed) = decode_one(
            &bytes,
            DecodeOptions {
                verify_crc: true,
                ..DecodeOptions::default()
            },
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
        let expected = &case["decoded"];
        assert_eq!(consumed, bytes.len(), "{}", case["name"]);
        let (record_properties, properties_position) = decode_record_properties(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(record_properties).unwrap(),
            expected["recordProperties"],
            "{}",
            case["name"]
        );
        assert_eq!(
            properties_position,
            expected["recordPropertiesPosition"].as_u64().unwrap() as usize,
            "{}",
            case["name"]
        );
        assert_eq!(decoded.topic(), expected["topic"]);
        if let Some(expected_body) = expected["bodyBase64"].as_str() {
            assert_eq!(
                decoded.body_nullable().map(|body| body.to_vec()),
                Some(
                    base64::engine::general_purpose::STANDARD
                        .decode(expected_body)
                        .unwrap()
                )
            );
        } else {
            assert!(decoded.body_nullable().is_none());
        }
        assert_eq!(decoded.flag(), expected["flag"]);
        assert_eq!(decoded.body_crc(), expected["bodyCRC"]);
        assert_eq!(decoded.queue_id(), expected["queueId"]);
        assert_eq!(decoded.queue_offset(), expected["queueOffset"]);
        assert_eq!(decoded.commit_log_offset(), expected["commitLogOffset"]);
        assert_eq!(decoded.sys_flag(), expected["sysFlag"]);
        assert_eq!(decoded.born_timestamp(), expected["bornTimestamp"]);
        assert_eq!(decoded.store_timestamp(), expected["storeTimestamp"]);
        assert_eq!(
            decoded.born_host().unwrap().to_string(),
            expected["bornHost"]
        );
        assert_eq!(
            decoded.store_host().unwrap().to_string(),
            expected["storeHost"]
        );
        assert_eq!(decoded.reconsume_times(), expected["reconsumeTimes"]);
        assert_eq!(
            decoded.prepared_transaction_offset(),
            expected["preparedTransactionOffset"]
        );
        assert_eq!(decoded.message_id().unwrap(), expected["msgId"]);
        assert_eq!(
            serde_json::to_value(decoded.properties()).unwrap(),
            expected["properties"]
        );
        assert_eq!(decoded.store_size() as usize, bytes.len());
    }
}

#[test]
fn message_ext_scalar_public_api_matches_java_551_fixture() {
    let expected = &fixture()["messageExtPublicApi"];
    assert_eq!(expected["publicConstructorCount"], 2);
    assert_eq!(
        expected["publicConstructors"],
        json!([
            "()",
            "(int, long, java.net.SocketAddress, long, java.net.SocketAddress, java.lang.String)"
        ])
    );
    assert_eq!(
        expected["declaredPublicMethods"],
        json!([
            "int getBodyCRC()",
            "int getQueueId()",
            "int getReconsumeTimes()",
            "int getStoreSize()",
            "int getSysFlag()",
            "java.lang.Integer getGroupSysFlag()",
            "java.lang.Integer getTopicSysFlag()",
            "java.lang.String getBornHostNameString()",
            "java.lang.String getBornHostString()",
            "java.lang.String getBrokerName()",
            "java.lang.String getMsgId()",
            "java.lang.String toString()",
            "java.net.SocketAddress getBornHost()",
            "java.net.SocketAddress getStoreHost()",
            "java.nio.ByteBuffer getBornHostBytes()",
            "java.nio.ByteBuffer getBornHostBytes(java.nio.ByteBuffer)",
            "java.nio.ByteBuffer getStoreHostBytes()",
            "java.nio.ByteBuffer getStoreHostBytes(java.nio.ByteBuffer)",
            "java.nio.ByteBuffer socketAddress2ByteBuffer(java.net.SocketAddress)",
            "java.nio.ByteBuffer socketAddress2ByteBuffer(java.net.SocketAddress, java.nio.ByteBuffer)",
            "long getBornTimestamp()",
            "long getCommitLogOffset()",
            "long getPreparedTransactionOffset()",
            "long getQueueOffset()",
            "long getStoreTimestamp()",
            "org.apache.rocketmq.common.TopicFilterType parseTopicFilterType(int)",
            "void setBodyCRC(int)",
            "void setBornHost(java.net.SocketAddress)",
            "void setBornHostV6Flag()",
            "void setBornTimestamp(long)",
            "void setBrokerName(java.lang.String)",
            "void setCommitLogOffset(long)",
            "void setGroupSysFlag(java.lang.Integer)",
            "void setMsgId(java.lang.String)",
            "void setPreparedTransactionOffset(long)",
            "void setQueueId(int)",
            "void setQueueOffset(long)",
            "void setReconsumeTimes(int)",
            "void setStoreHost(java.net.SocketAddress)",
            "void setStoreHostAddressV6Flag()",
            "void setStoreSize(int)",
            "void setStoreTimestamp(long)",
            "void setSysFlag(int)",
            "void setTopicSysFlag(java.lang.Integer)"
        ])
    );
    let defaults = MessageExt::new();
    assert_eq!(
        defaults.queue_id(),
        expected["defaultQueueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.born_timestamp(),
        expected["defaultBornTimestamp"].as_i64().unwrap()
    );
    assert_eq!(
        defaults.store_timestamp(),
        expected["defaultStoreTimestamp"].as_i64().unwrap()
    );
    assert_eq!(defaults.message_id(), expected["defaultMessageId"].as_str());
    assert_eq!(
        defaults.sys_flag(),
        expected["defaultSysFlag"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.body_crc(),
        expected["defaultBodyCrc"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.queue_offset(),
        expected["defaultQueueOffset"].as_i64().unwrap()
    );
    assert_eq!(
        defaults.commit_log_offset(),
        expected["defaultCommitLogOffset"].as_i64().unwrap()
    );
    assert_eq!(
        defaults.store_size(),
        expected["defaultStoreSize"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.reconsume_times(),
        expected["defaultReconsumeTimes"].as_i64().unwrap() as i32
    );
    assert_eq!(
        defaults.prepared_transaction_offset(),
        expected["defaultPreparedTransactionOffset"]
            .as_i64()
            .unwrap()
    );

    let mut message = MessageExt::default();
    message.set_queue_id(-17);
    message.set_born_timestamp(i64::MIN + 17);
    message.set_store_timestamp(i64::MAX - 17);
    message.set_message_id(Some("message-ext-id".to_owned()));
    message.set_sys_flag(-33);
    message.set_body_crc(i32::MIN);
    message.set_queue_offset(i64::MIN);
    message.set_commit_log_offset(i64::MAX);
    message.set_store_size(-21);
    message.set_reconsume_times(-4);
    message.set_prepared_transaction_offset(i64::MAX - 1);
    assert_eq!(
        message.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.born_timestamp(),
        expected["bornTimestamp"].as_i64().unwrap()
    );
    assert_eq!(
        message.store_timestamp(),
        expected["storeTimestamp"].as_i64().unwrap()
    );
    assert_eq!(message.message_id(), expected["messageId"].as_str());
    assert_eq!(
        message.sys_flag(),
        expected["sysFlag"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.body_crc(),
        expected["bodyCrc"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.queue_offset(),
        expected["queueOffset"].as_i64().unwrap()
    );
    assert_eq!(
        message.commit_log_offset(),
        expected["commitLogOffset"].as_i64().unwrap()
    );
    assert_eq!(
        message.store_size(),
        expected["storeSize"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.reconsume_times(),
        expected["reconsumeTimes"].as_i64().unwrap() as i32
    );
    assert_eq!(
        message.prepared_transaction_offset(),
        expected["preparedTransactionOffset"].as_i64().unwrap()
    );
}

#[test]
fn message_client_ext_api_matches_java_551_fixture() {
    let expected = &fixture()["messageClientExtApi"];
    assert_eq!(expected["publicConstructorCount"].as_u64(), Some(1));
    assert_eq!(expected["publicNoArgConstructor"].as_bool(), Some(true));
    assert_eq!(
        expected["declaredPublicMethods"],
        json!([
            "java.lang.String getMsgId()",
            "java.lang.String getOffsetMsgId()",
            "void setMsgId(java.lang.String)",
            "void setOffsetMsgId(java.lang.String)"
        ])
    );
    let mut message = MessageClientExt::new();
    assert!(message.is_message_client_ext());
    assert_eq!(
        message.message_id().as_deref(),
        expected["defaultMessageId"].as_str()
    );
    assert_eq!(
        message.offset_message_id().as_deref(),
        expected["defaultOffsetMessageId"].as_str()
    );
    message.set_message_id(Some("ignored-initial".to_owned()));
    assert_eq!(
        message.message_id().as_deref(),
        expected["setMessageIdOnDefault"].as_str()
    );
    assert_eq!(
        message.offset_message_id().as_deref(),
        expected["setMessageIdDoesNotSetOffset"].as_str()
    );
    message.set_offset_message_id(Some("offset-1".to_owned()));
    assert_eq!(
        message.offset_message_id().as_deref(),
        expected["offsetMessageId"].as_str()
    );
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdFallsBackToOffset"].as_str()
    );
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-1",
        )
        .unwrap();
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdPrefersUnique"].as_str()
    );
    assert_eq!(
        message.offset_message_id().as_deref(),
        expected["offsetRemainsWhenUniqueExists"].as_str()
    );
    message.set_message_id(Some("ignored-after-unique".to_owned()));
    assert_eq!(
        message.message_id().as_deref(),
        expected["setMessageIdDoesNotReplaceUnique"].as_str()
    );
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-2",
        )
        .unwrap();
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdReadsLiveUniqueProperty"].as_str()
    );
    message.set_offset_message_id(Some("offset-2".to_owned()));
    assert_eq!(
        message.offset_message_id().as_deref(),
        expected["offsetMessageIdReplaced"].as_str()
    );
    assert_eq!(
        message.message_id().as_deref(),
        expected["uniqueSurvivesOffsetReplacement"].as_str()
    );
    message.clear_system_property(
        rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    );
    assert_eq!(
        message.message_id().as_deref(),
        expected["messageIdFallsBackAfterUniqueClear"].as_str()
    );
    message.set_message_id(Some("ignored-after-clear".to_owned()));
    assert_eq!(
        message.message_id().as_deref(),
        expected["setMessageIdDoesNotReplaceOffsetFallback"].as_str()
    );
    message.set_offset_message_id(None);
    assert_eq!(message.offset_message_id(), None);
    assert_eq!(message.message_id(), None);
    assert_eq!(expected["offsetMessageIdCleared"].as_bool(), Some(true));
    assert_eq!(
        expected["messageIdAfterOffsetCleared"].as_bool(),
        Some(true)
    );

    // MessageClientExt inherits these setters and getters from MessageExt
    // and Message in Java. Exercise method resolution through the Rust wrapper.
    message.set_topic("inherited-topic");
    assert_eq!(
        message.topic(),
        expected["inheritedTopic"].as_str().unwrap()
    );
    message.set_queue_id(-17);
    assert_eq!(
        message.queue_id(),
        expected["inheritedQueueId"].as_i64().unwrap() as i32
    );
    message.set_store_timestamp(1_700_000_000_123);
    assert_eq!(
        message.store_timestamp(),
        expected["inheritedStoreTimestamp"].as_i64().unwrap()
    );
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-before-conversion",
        )
        .unwrap();
    let mut base_view = message.into_message_ext();
    assert!(base_view.is_message_client_ext());
    assert_eq!(
        base_view.message_id_for_runtime_type().as_deref(),
        Some("unique-before-conversion")
    );
    base_view.set_message_id(Some("ignored-after-conversion".into()));
    base_view
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-after-conversion",
        )
        .unwrap();
    assert_eq!(
        base_view.message_id_for_runtime_type().as_deref(),
        Some("unique-after-conversion")
    );
    base_view.clear_system_property(
        rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    );
    assert_eq!(base_view.message_id_for_runtime_type().as_deref(), None);
    base_view.set_message_id(Some("ignored-without-unique".into()));
    assert_eq!(base_view.message_id_for_runtime_type().as_deref(), None);
}

#[test]
fn message_client_ext_offset_methods_dispatch_to_runtime_subtype() {
    let subtype = Arc::new(OffsetOnlyClientExtSubtype::default());
    let mut message = MessageClientExt::new();
    message.set_runtime_subtype(subtype.clone());
    message.set_offset_message_id(Some("physical-id".to_owned()));

    assert_eq!(
        message.offset_message_id().as_deref(),
        Some("subtype-physical-id")
    );
    assert_eq!(message.message_id().as_deref(), Some("subtype-physical-id"));

    message.set_message_id(Some("ignored-id".to_owned()));
    assert_eq!(message.message_id().as_deref(), Some("subtype-physical-id"));
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-id",
        )
        .unwrap();
    assert_eq!(message.message_id().as_deref(), Some("unique-id"));
    assert_eq!(
        message.offset_message_id().as_deref(),
        Some("subtype-physical-id")
    );
    message.clear_system_property(
        rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
    );

    let base_view = message.into_message_ext();
    assert!(base_view.is_message_client_ext());
    assert_eq!(
        base_view.message_id_for_runtime_type().as_deref(),
        Some("subtype-physical-id")
    );
    assert_eq!(
        base_view.offset_message_id_for_runtime_type().as_deref(),
        Some("subtype-physical-id")
    );
    assert_eq!(
        subtype.offset_writes.lock().unwrap().as_slice(),
        &[Some("physical-id".to_owned())]
    );
}

#[test]
fn message_client_ext_message_id_overrides_dispatch_to_runtime_subtype() {
    let expected = &fixture()["messageClientExtApi"];
    let subtype = Arc::new(ClientMessageIdOverrideSubtype {
        message_id: Mutex::new(
            expected["customSubtypeMessageId"]
                .as_str()
                .map(str::to_owned),
        ),
        ..ClientMessageIdOverrideSubtype::default()
    });
    let mut message = MessageClientExt::new();
    message.set_runtime_subtype(subtype.clone());
    assert_eq!(
        message.message_id().as_deref(),
        expected["customSubtypeMessageId"].as_str()
    );
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-shadow",
        )
        .unwrap();
    assert_eq!(
        message.message_id().as_deref(),
        expected["customSubtypeMessageIdIgnoresUnique"].as_str()
    );
    message.set_message_id(Some("replacement".into()));
    assert_eq!(
        message.message_id().as_deref(),
        expected["customSubtypeMessageIdAfterSet"].as_str()
    );
    assert_eq!(
        message.offset_message_id(),
        None,
        "the custom message-ID setter must not mutate the base offset ID"
    );
    assert_eq!(
        expected["customSubtypeOffsetUnaffected"].as_bool(),
        Some(true)
    );
    message.set_message_id(None);
    assert_eq!(message.message_id(), None);
    assert_eq!(
        expected["customSubtypeMessageIdCleared"].as_bool(),
        Some(true)
    );
    assert_eq!(
        subtype.message_id_writes.lock().unwrap().as_slice(),
        &[Some("replacement".to_owned()), None]
    );
}

#[test]
fn client_message_decoder_apis_match_java_551() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let mut joined = Vec::new();

    for (index, case) in cases.iter().enumerate() {
        let bytes = record(case);
        let (decoded, consumed) =
            client_decode(&bytes, true).unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
        assert_eq!(consumed, bytes.len(), "{}", case["name"]);
        assert_eq!(
            decoded.message_id(),
            case["clientDecoded"]["msgId"].as_str()
        );
        assert_eq!(
            decoded.offset_message_id(),
            case["clientDecoded"]["offsetMsgId"].as_str()
        );
        if index < 2 {
            joined.extend_from_slice(&bytes);
        }
    }

    let batch_options = DecodeOptions {
        read_body: false,
        decompress_body: false,
        set_properties_string: true,
        verify_crc: true,
    };
    let batch = decode_many_as_client_compatible(&joined, batch_options);
    let expected = &cases[..2];
    assert_eq!(batch.len(), expected.len());
    for (message, case) in batch.iter().zip(expected) {
        assert!(message.body().is_empty());
        assert_eq!(
            message.message_id(),
            case["clientDecoded"]["msgId"].as_str()
        );
        assert_eq!(
            message.offset_message_id(),
            case["clientDecoded"]["offsetMsgId"].as_str()
        );
        assert_eq!(
            message.properties().contains_key("propertiesString"),
            !case["decoded"]["properties"]
                .as_object()
                .unwrap()
                .is_empty()
        );
    }

    let default_batch = client_decodes(&joined).unwrap();
    assert_eq!(default_batch.len(), expected.len());
    let body_skipped_batch = client_decodes_with_body(&joined, false).unwrap();
    assert_eq!(body_skipped_batch.len(), expected.len());
    assert!(
        body_skipped_batch
            .iter()
            .all(|message| message.body().is_empty())
    );
}

#[test]
fn client_batch_decode_keeps_prefix_and_suppresses_malformed_tail_like_java_551() {
    let fixture = fixture();
    let expected = &fixture["messageBatchDecodeApi"];
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(expected["malformedBatchBase64"].as_str().unwrap())
        .unwrap();

    let decoded = decode_many_as_client_compatible(&bytes, DecodeOptions::default());
    assert_eq!(
        decoded.len(),
        expected["decodedCount"].as_u64().unwrap() as usize
    );
    assert_eq!(decoded.len(), 1);
    assert_eq!(expected["firstMessageClass"], "MessageClientExt");
    assert!(decoded[0].is_message_client_ext());
    assert_eq!(decoded[0].topic(), expected["firstTopic"]);
    assert_eq!(decoded[0].message_id(), expected["firstMessageId"].as_str());
    assert_eq!(
        decoded[0].offset_message_id(),
        expected["firstOffsetMessageId"].as_str()
    );
    assert_eq!(decoded[0].queue_offset(), expected["firstQueueOffset"]);
    assert_eq!(
        expected["inputPosition"], expected["inputLimit"],
        "pinned Java moves its ByteBuffer to the limit after the malformed tail"
    );
    assert_eq!(expected["decodesDefaultCount"], expected["decodedCount"]);
    assert_eq!(
        expected["decodesDefaultInputPosition"],
        expected["decodesDefaultInputLimit"]
    );
    assert_eq!(
        expected["decodesDefaultFirstMessageClass"],
        "MessageClientExt"
    );
    assert_eq!(
        expected["decodesCount"], expected["decodedCount"],
        "Java decodes(ByteBuffer, readBody) also keeps the valid prefix"
    );
    assert_eq!(
        expected["decodesInputPosition"],
        expected["decodesInputLimit"]
    );
    assert_eq!(expected["decodesNoBodyCount"], expected["decodesCount"]);
    assert_eq!(
        expected["decodesNoBodyInputPosition"],
        expected["decodesNoBodyInputLimit"]
    );
    let client_decoded = client_decodes(&bytes).unwrap();
    assert_eq!(
        client_decoded.len(),
        expected["decodesDefaultCount"].as_u64().unwrap() as usize
    );
    assert_eq!(client_decoded.len(), 1);
    assert!(client_decoded[0].is_message_client_ext());
    let client_without_body = client_decodes_with_body(&bytes, false).unwrap();
    assert_eq!(client_without_body.len(), client_decoded.len());
    assert!(client_without_body[0].is_message_client_ext());
    assert!(client_without_body[0].body().is_empty());

    let generic_batch = decode_many_compatible(&bytes, DecodeOptions::default());
    assert_eq!(
        generic_batch.len(),
        expected["genericBatchCount"].as_u64().unwrap() as usize
    );
    assert_eq!(expected["genericBatchFirstMessageClass"], "MessageExt");
    assert!(!generic_batch[0].is_message_client_ext());
    assert_eq!(
        expected["genericBatchInputPosition"],
        expected["genericBatchInputLimit"]
    );
    assert_eq!(
        expected["genericBatchNoBodyCount"],
        expected["genericBatchCount"]
    );
    assert_eq!(
        expected["genericBatchNoBodyInputPosition"],
        expected["genericBatchNoBodyInputLimit"]
    );
    let generic_batch_without_body = decode_many_compatible(
        &bytes,
        DecodeOptions {
            read_body: false,
            ..DecodeOptions::default()
        },
    );
    assert_eq!(generic_batch_without_body.len(), generic_batch.len());
    assert!(!generic_batch_without_body[0].is_message_client_ext());
    assert!(generic_batch_without_body[0].body().is_empty());
}

#[test]
fn message_decoder_overloads_match_invoked_java_551() {
    let fixture = fixture();
    let decode_cases = [
        ("decodeDefault", DecodeOptions::default(), false),
        (
            "decodeReadBodyFalse",
            DecodeOptions {
                read_body: false,
                ..DecodeOptions::default()
            },
            false,
        ),
        ("decodeReadBodyTrue", DecodeOptions::default(), false),
        (
            "decodeNoBodyNoDecompress",
            DecodeOptions {
                read_body: false,
                decompress_body: false,
                ..DecodeOptions::default()
            },
            false,
        ),
        (
            "decodeNoBodyDecompress",
            DecodeOptions {
                read_body: false,
                ..DecodeOptions::default()
            },
            false,
        ),
        (
            "decodeBodyNoDecompress",
            DecodeOptions {
                decompress_body: false,
                ..DecodeOptions::default()
            },
            false,
        ),
        ("decodeBodyDecompress", DecodeOptions::default(), false),
        (
            "decodeGenericFourArguments",
            DecodeOptions::default(),
            false,
        ),
        ("decodeClientFourArguments", DecodeOptions::default(), true),
        (
            "decodeGenericFiveArguments",
            DecodeOptions::default(),
            false,
        ),
        (
            "decodePropertiesStringFiveArguments",
            DecodeOptions {
                set_properties_string: true,
                ..DecodeOptions::default()
            },
            false,
        ),
        ("decodeWithoutCrcCheck", DecodeOptions::default(), false),
        (
            "decodeWithCrcCheck",
            DecodeOptions {
                verify_crc: true,
                ..DecodeOptions::default()
            },
            false,
        ),
    ];

    for case in fixture["cases"].as_array().unwrap() {
        let bytes = record(case);
        for (name, options, client) in decode_cases {
            let decoded = if client {
                decode_one_as_client(&bytes, options)
            } else {
                decode_one(&bytes, options)
            }
            .unwrap_or_else(|error| panic!("{} {name}: {error}", case["name"]));
            assert_eq!(
                decoded_snapshot(&decoded.0, decoded.1),
                case["decodeOverloads"][name],
                "{} {name}",
                case["name"]
            );
        }

        for read_body in [false, true] {
            let (decoded, consumed) = client_decode(&bytes, read_body)
                .unwrap_or_else(|error| panic!("{} clientDecode: {error}", case["name"]));
            let name = if read_body {
                "clientDecodeReadBodyTrue"
            } else {
                "clientDecodeReadBodyFalse"
            };
            assert_eq!(
                decoded_snapshot(&decoded, consumed),
                case["decodeOverloads"][name],
                "{} {name}",
                case["name"]
            );
        }
    }
}

#[test]
fn message_ext_socket_address_api_matches_java_551() {
    let fixture = fixture();
    let expected = &fixture["messageExtHostApi"];
    let born = MessageHostAddress::from_ip_address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 10911);
    let born_socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 10911);
    let from_socket_addr = MessageHostAddress::from_socket_addr(born_socket_addr);
    let store = MessageHostAddress::unresolved("broker.invalid", 10912);
    let mut message = MessageExt::default();
    assert_eq!(expected["defaultBornHostBytesThrowsNullPointer"], true);
    assert_eq!(
        expected["defaultBornHostBufferBytesThrowsNullPointer"],
        true
    );
    assert_eq!(expected["defaultStoreHostBytesThrowsNullPointer"], true);
    assert_eq!(
        expected["defaultStoreHostBufferBytesThrowsNullPointer"],
        true
    );
    assert_eq!(
        message.born_host_bytes(),
        Err(MessageHostAddressBytesError::MissingAddress)
    );
    assert_eq!(
        message.store_host_bytes(),
        Err(MessageHostAddressBytesError::MissingAddress)
    );
    assert_eq!(
        message.write_born_host_bytes(&mut [0_u8; 8]),
        Err(MessageHostAddressBytesError::MissingAddress)
    );
    assert_eq!(
        message.write_store_host_bytes(&mut [0_u8; 20]),
        Err(MessageHostAddressBytesError::MissingAddress)
    );
    assert_eq!(
        message.broker_name(),
        expected["brokerNameDefault"].as_str()
    );
    message.set_broker_name(Some("broker-a".to_owned()));
    assert_eq!(message.broker_name(), expected["brokerNameSet"].as_str());
    message.set_broker_name(None);
    assert_eq!(
        message.broker_name(),
        expected["brokerNameCleared"].as_str()
    );
    message.set_born_host(Some(born.clone()));
    message.set_store_host(Some(store.clone()));

    assert_eq!(expected["bornHostClass"], "java.net.InetSocketAddress");
    assert_eq!(born.to_string(), expected["bornHostToString"]);
    assert_eq!(
        born.ip_address(),
        Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)))
    );
    assert!(born.is_resolved());
    assert_eq!(born.socket_addr(), Some(born_socket_addr));
    assert_eq!(from_socket_addr.to_string(), expected["bornHostToString"]);
    assert_eq!(
        from_socket_addr.host_name(),
        expected["bornHostNameString"].as_str().unwrap()
    );
    assert_eq!(from_socket_addr.ip_address(), Some(born_socket_addr.ip()));
    assert!(from_socket_addr.is_resolved());
    assert_eq!(from_socket_addr.socket_addr(), Some(born_socket_addr));
    assert_eq!(
        message.born_host_string().as_deref(),
        expected["bornHostString"].as_str()
    );
    assert_eq!(
        message.born_host_name_string(),
        expected["bornHostNameString"].as_str()
    );
    assert_eq!(
        born.socket_address_string(),
        expected["ipv4SocketAddressString"]
            .as_str()
            .map(str::to_owned)
    );
    let born_bytes = born.network_bytes().unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&born_bytes),
        expected["bornHostBytes"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&born_bytes),
        expected["bornHostBufferBytes"]
    );
    assert_eq!(message.born_host_bytes().unwrap(), born_bytes);
    let mut born_output = [0_u8; 8];
    assert_eq!(message.write_born_host_bytes(&mut born_output), Ok(8));
    assert_eq!(born_output.as_slice(), born_bytes);
    let mut short_output = [0_u8; 7];
    assert_eq!(
        message.write_born_host_bytes(&mut short_output),
        Err(MessageHostAddressBytesError::BufferTooSmall {
            needed: 8,
            actual: 7
        })
    );
    assert_eq!(expected["ipv4ShortBufferOverflows"], true);
    assert_eq!(expected["ipv4ShortBufferPosition"], 4);
    assert_eq!(&short_output[..4], &born_bytes[..4]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(short_output),
        expected["ipv4ShortBufferBytes"]
    );
    assert_eq!(expected["bornHostGetterRetainsObject"], true);
    assert_eq!(expected["storeHostClass"], "java.net.InetSocketAddress");
    assert_eq!(store.to_string(), expected["storeHostToString"]);
    assert!(!store.is_resolved());
    assert_eq!(store.ip_address(), None);
    assert_eq!(store.socket_addr(), None);
    assert_eq!(store.socket_address_string(), None);
    assert_eq!(expected["storeHostUnresolved"], true);
    assert_eq!(expected["storeHostGetterRetainsObject"], true);
    assert_eq!(
        message.store_host_bytes(),
        Err(MessageHostAddressBytesError::UnresolvedAddress)
    );
    assert_eq!(expected["unresolvedHostBytesThrow"], true);
    assert_eq!(expected["unresolvedHostBufferBytesThrow"], true);
    assert_eq!(
        message.write_store_host_bytes(&mut [0_u8; 20]),
        Err(MessageHostAddressBytesError::UnresolvedAddress)
    );
    let constructed = MessageExt::with_socket_addresses(
        7,
        13,
        Some(born.clone()),
        17,
        Some(store.clone()),
        Some("message-id".to_owned()),
    );
    assert_eq!(constructed.queue_id(), expected["constructorQueueId"]);
    assert_eq!(
        constructed.born_timestamp(),
        expected["constructorBornTimestamp"]
    );
    assert_eq!(
        constructed.born_host().unwrap().to_string(),
        expected["constructorBornHost"]
    );
    assert_eq!(
        constructed.store_timestamp(),
        expected["constructorStoreTimestamp"]
    );
    assert_eq!(
        constructed.store_host().unwrap().to_string(),
        expected["constructorStoreHost"]
    );
    assert_eq!(
        constructed.message_id(),
        expected["constructorMessageId"].as_str()
    );
    let null_host_constructed = MessageExt::with_socket_addresses(
        expected["nullHostConstructorQueueId"].as_i64().unwrap() as i32,
        expected["nullHostConstructorBornTimestamp"]
            .as_i64()
            .unwrap(),
        None,
        expected["nullHostConstructorStoreTimestamp"]
            .as_i64()
            .unwrap(),
        None,
        None,
    );
    assert_eq!(
        null_host_constructed.queue_id(),
        expected["nullHostConstructorQueueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        null_host_constructed.born_timestamp(),
        expected["nullHostConstructorBornTimestamp"]
            .as_i64()
            .unwrap()
    );
    assert_eq!(null_host_constructed.born_host(), None);
    assert_eq!(
        null_host_constructed.store_timestamp(),
        expected["nullHostConstructorStoreTimestamp"]
            .as_i64()
            .unwrap()
    );
    assert_eq!(null_host_constructed.store_host(), None);
    assert_eq!(null_host_constructed.message_id(), None);
    assert_eq!(expected["nullHostConstructorBornHostIsNull"], true);
    assert_eq!(expected["nullHostConstructorStoreHostIsNull"], true);
    assert_eq!(expected["nullHostConstructorMessageIdIsNull"], true);

    let ipv6 = MessageHostAddress::from_ip_address(
        IpAddr::V6("2001:db8::20".parse::<Ipv6Addr>().unwrap()),
        10915,
    );
    assert_eq!(ipv6.to_string(), expected["ipv6HostToString"]);
    assert_eq!(
        ipv6.host_address_string().as_deref(),
        expected["ipv6HostString"].as_str()
    );
    assert_eq!(
        ipv6.host_name(),
        expected["ipv6HostNameString"].as_str().unwrap()
    );
    assert_eq!(
        ipv6.socket_address_string(),
        expected["ipv6SocketAddressString"]
            .as_str()
            .map(str::to_owned)
    );
    let ipv6_bytes = ipv6.network_bytes().unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&ipv6_bytes),
        expected["ipv6HostBytes"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&ipv6_bytes),
        expected["ipv6HostBufferBytes"]
    );
    let mut ipv6_message = MessageExt::default();
    ipv6_message.set_born_host(Some(ipv6.clone()));
    ipv6_message.set_store_host(Some(ipv6.clone()));
    assert_eq!(ipv6_message.store_host_bytes().unwrap(), ipv6_bytes);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(ipv6_message.store_host_bytes().unwrap()),
        expected["ipv6StoreHostBytes"]
    );
    let mut short_ipv6_output = [0_u8; 19];
    assert_eq!(
        ipv6_message.write_born_host_bytes(&mut short_ipv6_output),
        Err(MessageHostAddressBytesError::BufferTooSmall {
            needed: 20,
            actual: 19
        })
    );
    assert_eq!(expected["ipv6ShortBufferOverflows"], true);
    assert_eq!(expected["ipv6ShortBufferPosition"], 16);
    assert_eq!(&short_ipv6_output[..16], &ipv6_bytes[..16]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(short_ipv6_output),
        expected["ipv6ShortBufferBytes"]
    );

    let from_host = MessageHostAddress::new("127.0.0.1", 10913);
    assert_eq!(from_host.to_string(), expected["stringHostToString"]);
    assert_eq!(
        from_host.host_address_string().as_deref(),
        expected["stringHostAddress"].as_str()
    );
    assert_eq!(
        from_host.host_name(),
        expected["stringHostName"].as_str().unwrap()
    );

    let encoded = serde_json::to_value(&message).unwrap();
    assert_eq!(encoded["bornHost"], expected["bornHostToString"]);
    assert_eq!(encoded["storeHost"], expected["storeHostToString"]);
    let decoded: MessageExt = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded.born_host(), Some(&born));
    assert_eq!(decoded.store_host(), Some(&store));
}

#[test]
fn message_id_api_matches_java_551() {
    let expected = &fixture()["messageExtHostApi"];
    assert_eq!(
        expected["messageIdPublicConstructors"],
        json!(["MessageId(java.net.SocketAddress, long)"])
    );
    assert_eq!(
        expected["messageIdDeclaredPublicMethods"],
        json!([
            "java.net.SocketAddress getAddress()",
            "long getOffset()",
            "void setAddress(java.net.SocketAddress)",
            "void setOffset(long)"
        ])
    );
    let ipv4 = MessageHostAddress::from_ip_address(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 10911);
    let ipv6 = MessageHostAddress::from_ip_address(
        IpAddr::V6("2001:db8::20".parse::<Ipv6Addr>().unwrap()),
        10915,
    );
    let ipv4_offset = -123_456_789_012_345_i64;
    let ipv6_offset = i64::MIN + 17;

    let ipv4_id = create_message_id(&ipv4, ipv4_offset).unwrap();
    let ipv6_id = create_message_id(&ipv6, ipv6_offset).unwrap();
    assert_eq!(ipv4_id, expected["messageIdIpv4Created"]);
    assert_eq!(ipv6_id, expected["messageIdIpv6Created"]);
    let ipv4_address_bytes = ipv4.network_bytes().unwrap();
    let mut ipv4_address = Cursor::new(ipv4_address_bytes.as_slice());
    let mut ipv4_output_bytes = [0; 16];
    let mut ipv4_output = Cursor::new(&mut ipv4_output_bytes[..]);
    assert_eq!(
        create_message_id_from_buffers(&mut ipv4_output, &mut ipv4_address, ipv4_offset).unwrap(),
        expected["messageIdIpv4CreatedFromBytes"]
    );
    assert_eq!(
        ipv4_address.position(),
        expected["messageIdIpv4AddressPositionAfter"]
    );
    assert_eq!(
        ipv4_output.position(),
        expected["messageIdIpv4OutputPositionAfter"]
    );

    let ipv6_address_bytes = ipv6.network_bytes().unwrap();
    let mut ipv6_address = Cursor::new(ipv6_address_bytes.as_slice());
    let mut ipv6_output_bytes = [0; 28];
    let mut ipv6_output = Cursor::new(&mut ipv6_output_bytes[..]);
    assert_eq!(
        create_message_id_from_buffers(&mut ipv6_output, &mut ipv6_address, ipv6_offset).unwrap(),
        expected["messageIdIpv6CreatedFromBytes"]
    );
    assert_eq!(
        ipv6_address.position(),
        expected["messageIdIpv6AddressPositionAfter"]
    );
    assert_eq!(
        ipv6_output.position(),
        expected["messageIdIpv6OutputPositionAfter"]
    );

    let mut positioned_address = Cursor::new(ipv4_address_bytes.as_slice());
    positioned_address.set_position(2);
    let mut positioned_output_bytes = [0; 16];
    positioned_output_bytes[..5].copy_from_slice(&[1, 2, 3, 4, 5]);
    let mut positioned_output = Cursor::new(&mut positioned_output_bytes[..]);
    positioned_output.set_position(5);
    assert_eq!(
        create_message_id_from_buffers(
            &mut positioned_output,
            &mut positioned_address,
            ipv4_offset
        )
        .unwrap(),
        expected["messageIdIpv4CreatedFromPositionedBuffers"]
    );
    assert_eq!(
        positioned_address.position(),
        expected["messageIdPositionedAddressPositionAfter"]
    );
    assert_eq!(
        positioned_output.position(),
        expected["messageIdPositionedOutputPositionAfter"]
    );

    let short_address_bytes = [1, 2, 3, 4];
    let mut short_address = Cursor::new(short_address_bytes.as_slice());
    let mut short_output_bytes = [0; 28];
    let mut short_output = Cursor::new(&mut short_output_bytes[..]);
    assert_eq!(
        create_message_id_from_buffers(&mut short_output, &mut short_address, 1234).unwrap(),
        expected["messageIdShortAddressOutput"]
    );
    assert_eq!(
        short_address.position(),
        expected["messageIdShortAddressPositionAfter"]
    );
    assert_eq!(
        short_output.position(),
        expected["messageIdShortOutputPositionAfter"]
    );

    let small_output_address_bytes = [0; 8];
    let mut small_output_address = Cursor::new(small_output_address_bytes.as_slice());
    let mut small_output_bytes = [0; 15];
    let mut small_output = Cursor::new(&mut small_output_bytes[..]);
    assert_eq!(
        create_message_id_from_buffers(&mut small_output, &mut small_output_address, 0),
        Err(MessageIdError::OutputBufferTooSmall {
            required: 16,
            capacity: 15
        })
    );
    assert_eq!(expected["messageIdOutputLimitThrowsIllegalArgument"], true);
    assert_eq!(
        small_output_address.position(),
        expected["messageIdOutputLimitAddressPosition"]
    );
    assert_eq!(
        small_output.position(),
        expected["messageIdOutputLimitOutputPosition"]
    );

    let too_long_address_bytes = [0; 29];
    let mut too_long_address = Cursor::new(too_long_address_bytes.as_slice());
    let mut too_long_output_bytes = [0; 28];
    let mut too_long_output = Cursor::new(&mut too_long_output_bytes[..]);
    assert_eq!(
        create_message_id_from_buffers(&mut too_long_output, &mut too_long_address, 0),
        Err(MessageIdError::BufferOverflow {
            needed: 29,
            remaining: 28
        })
    );
    assert_eq!(
        too_long_address.position(),
        expected["messageIdAddressPutOverflowAddressPosition"]
    );
    assert_eq!(
        too_long_output.position(),
        expected["messageIdAddressPutOverflowOutputPosition"]
    );

    let offset_overflow_address_bytes = [0; 25];
    let mut offset_overflow_address = Cursor::new(offset_overflow_address_bytes.as_slice());
    let mut offset_overflow_output_bytes = [0; 28];
    let mut offset_overflow_output = Cursor::new(&mut offset_overflow_output_bytes[..]);
    assert_eq!(
        create_message_id_from_buffers(
            &mut offset_overflow_output,
            &mut offset_overflow_address,
            0
        ),
        Err(MessageIdError::BufferOverflow {
            needed: 8,
            remaining: 3
        })
    );
    assert_eq!(
        offset_overflow_address.position(),
        expected["messageIdOffsetPutOverflowAddressPosition"]
    );
    assert_eq!(
        offset_overflow_output.position(),
        expected["messageIdOffsetPutOverflowOutputPosition"]
    );

    let decoded_ipv4 = decode_message_id(&ipv4_id).unwrap();
    assert_eq!(
        decoded_ipv4
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdIpv4DecodedHost"].as_str()
    );
    assert_eq!(
        decoded_ipv4.address().unwrap().port(),
        expected["messageIdIpv4DecodedPort"]
    );
    assert_eq!(decoded_ipv4.offset(), ipv4_offset);
    assert_eq!(
        decoded_ipv4.offset(),
        expected["messageIdIpv4DecodedOffset"]
    );
    let decoded_ipv6 = decode_message_id(&ipv6_id).unwrap();
    assert_eq!(
        decoded_ipv6
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdIpv6DecodedHost"].as_str()
    );
    assert_eq!(
        decoded_ipv6.address().unwrap().port(),
        expected["messageIdIpv6DecodedPort"]
    );
    assert_eq!(decoded_ipv6.offset(), ipv6_offset);
    assert_eq!(
        decoded_ipv6.offset(),
        expected["messageIdIpv6DecodedOffset"]
    );
    assert_eq!(
        decode_message_id(&format!("{ipv6_id}FFFF"))
            .unwrap()
            .offset(),
        expected["messageIdIpv6ExtraBytesIgnored"]
    );

    let mut mutable_id = MessageId::new(Some(ipv4), 42);
    mutable_id.set_address(Some(ipv6.clone()));
    mutable_id.set_offset(-17);
    assert_eq!(
        mutable_id
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdSetterHost"].as_str()
    );
    assert_eq!(
        mutable_id.address().unwrap().port(),
        expected["messageIdSetterPort"]
    );
    assert_eq!(mutable_id.offset(), expected["messageIdSetterOffset"]);
    mutable_id.set_address(None);
    assert_eq!(mutable_id.address(), None);
    assert_eq!(expected["messageIdNullableAddress"], true);
    assert_eq!(expected["messageIdNullableSetter"], true);

    assert_eq!(
        create_message_id(&MessageHostAddress::unresolved("broker.invalid", 10911), 0),
        Err(MessageIdError::UnresolvedAddress)
    );
    assert_eq!(
        decode_message_id("").unwrap_err(),
        MessageIdError::EmptyMessageId
    );
    assert_eq!(expected["messageIdEmptyThrowsNullPointer"], true);
    assert_eq!(
        decode_message_id("0").unwrap_err(),
        MessageIdError::InsufficientBytes {
            required: 28,
            available: 0
        }
    );
    assert_eq!(expected["messageIdShortThrowsBufferUnderflow"], true);
    assert_eq!(
        decode_message_id("7F000001FFFFFFFF0000000000003039").unwrap_err(),
        MessageIdError::InvalidPort { port: -1 }
    );
    assert_eq!(expected["messageIdBadPortThrowsIllegalArgument"], true);
    let invalid_hex = decode_message_id("GG00000100002A9F0000000000003039").unwrap();
    assert_eq!(
        invalid_hex
            .address()
            .unwrap()
            .host_address_string()
            .as_deref(),
        expected["messageIdInvalidHexHost"].as_str()
    );
}

#[test]
fn crc32_property_helpers_match_both_java_buffers() {
    let expected = &fixture()["messageDecoderCrcApi"]["cases"];
    for case in expected.as_array().unwrap() {
        let mut bytes = Vec::new();
        append_crc32_property(&mut bytes, case["crc32"].as_i64().unwrap() as i32);
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        assert_eq!(encoded, case["byteBufferBase64"]);
        assert_eq!(case["byteBufferBase64"], case["byteBufBase64"]);
    }
}

#[test]
fn message_decoder_public_constants_match_java_551() {
    let expected = &fixture()["messageDecoderPublicApi"];
    assert_eq!(
        expected["declaredPublicConstructors"],
        json!(["MessageDecoder()"])
    );
    assert_eq!(
        expected["declaredPublicMethods"],
        json!([
            "byte[] encode(org.apache.rocketmq.common.message.MessageExt, boolean)",
            "byte[] encodeMessage(org.apache.rocketmq.common.message.Message)",
            "byte[] encodeMessages(java.util.List)",
            "byte[] encodeUniquely(org.apache.rocketmq.common.message.MessageExt, boolean)",
            "int countInnerMsgNum(java.nio.ByteBuffer)",
            "java.lang.String createMessageId(java.net.SocketAddress, long)",
            "java.lang.String createMessageId(java.nio.ByteBuffer, java.nio.ByteBuffer, long)",
            "java.lang.String messageProperties2String(java.util.Map)",
            "java.util.List decodeMessages(java.nio.ByteBuffer)",
            "java.util.List decodes(java.nio.ByteBuffer)",
            "java.util.List decodes(java.nio.ByteBuffer, boolean)",
            "java.util.List decodesBatch(java.nio.ByteBuffer, boolean, boolean, boolean)",
            "java.util.Map decodeProperties(java.nio.ByteBuffer)",
            "java.util.Map string2messageProperties(java.lang.String)",
            "org.apache.rocketmq.common.message.Message decodeMessage(java.nio.ByteBuffer)",
            "org.apache.rocketmq.common.message.MessageExt clientDecode(java.nio.ByteBuffer, boolean)",
            "org.apache.rocketmq.common.message.MessageExt decode(java.nio.ByteBuffer)",
            "org.apache.rocketmq.common.message.MessageExt decode(java.nio.ByteBuffer, boolean)",
            "org.apache.rocketmq.common.message.MessageExt decode(java.nio.ByteBuffer, boolean, boolean)",
            "org.apache.rocketmq.common.message.MessageExt decode(java.nio.ByteBuffer, boolean, boolean, boolean)",
            "org.apache.rocketmq.common.message.MessageExt decode(java.nio.ByteBuffer, boolean, boolean, boolean, boolean)",
            "org.apache.rocketmq.common.message.MessageExt decode(java.nio.ByteBuffer, boolean, boolean, boolean, boolean, boolean)",
            "org.apache.rocketmq.common.message.MessageId decodeMessageId(java.lang.String)",
            "void createCrc32(io.netty.buffer.ByteBuf, int)",
            "void createCrc32(java.nio.ByteBuffer, int)",
            "void decodeMessage(org.apache.rocketmq.common.message.MessageExt, java.util.List)"
        ])
    );
    assert_eq!(CHARSET_UTF8, expected["charsetUtf8"]);
    assert_eq!(MESSAGE_MAGIC_CODE, expected["messageMagicCode"]);
    assert_eq!(MESSAGE_MAGIC_CODE_V2, expected["messageMagicCodeV2"]);
    assert_eq!(BLANK_MAGIC_CODE, expected["blankMagicCode"]);
    assert_eq!(
        MESSAGE_MAGIC_CODE_POSITION,
        expected["messageMagicCodePosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        MESSAGE_FLAG_POSITION,
        expected["messageFlagPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        MESSAGE_PHYSIC_OFFSET_POSITION,
        expected["messagePhysicOffsetPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        MESSAGE_STORE_TIMESTAMP_POSITION,
        expected["messageStoreTimestampPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        PHY_POS_POSITION,
        expected["phyPosPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        QUEUE_OFFSET_POSITION,
        expected["queueOffsetPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        SYSFLAG_POSITION,
        expected["sysflagPosition"].as_u64().unwrap() as usize
    );
    assert_eq!(
        rocketmq_classic_common::message::NAME_VALUE_SEPARATOR as i32,
        expected["nameValueSeparator"]
    );
    assert_eq!(PROPERTY_SEPARATOR as i32, expected["propertySeparator"]);
}

#[test]
fn message_sys_flag_public_api_matches_java_551() {
    let expected = &fixture()["messageSysFlagPublicApi"];
    assert_eq!(expected["publicConstructors"], json!(["MessageSysFlag()"]));
    assert_eq!(
        expected["declaredPublicMethods"],
        json!([
            "boolean check(int, int)",
            "int clearCompressedFlag(int)",
            "int getTransactionValue(int)",
            "int resetTransactionValue(int, int)",
            "org.apache.rocketmq.common.compression.CompressionType getCompressionType(int)"
        ])
    );
    assert_eq!(
        expected["publicConstants"],
        json!({
            "BORNHOST_V6_FLAG": message::BORNHOST_V6_FLAG,
            "COMPRESSED_FLAG": message::COMPRESSED_FLAG,
            "COMPRESSION_LZ4_TYPE": message::COMPRESSION_LZ4_TYPE,
            "COMPRESSION_TYPE_COMPARATOR": message::COMPRESSION_TYPE_COMPARATOR,
            "COMPRESSION_ZLIB_TYPE": message::COMPRESSION_ZLIB_TYPE,
            "COMPRESSION_ZSTD_TYPE": message::COMPRESSION_ZSTD_TYPE,
            "INNER_BATCH_FLAG": message::INNER_BATCH_FLAG,
            "MULTI_TAGS_FLAG": message::MULTI_TAGS_FLAG,
            "NEED_UNWRAP_FLAG": message::NEED_UNWRAP_FLAG,
            "STOREHOSTADDRESS_V6_FLAG": message::STOREHOSTADDRESS_V6_FLAG,
            "TRANSACTION_COMMIT_TYPE": message::TRANSACTION_COMMIT_TYPE,
            "TRANSACTION_NOT_TYPE": message::TRANSACTION_NOT_TYPE,
            "TRANSACTION_PREPARED_TYPE": message::TRANSACTION_PREPARED_TYPE,
            "TRANSACTION_ROLLBACK_TYPE": message::TRANSACTION_ROLLBACK_TYPE
        })
    );
}

#[test]
fn pull_sys_flag_public_api_matches_java_551() {
    let expected = &fixture()["pullSysFlagPublicApi"];
    assert_eq!(expected["publicConstants"], json!({}));
    assert_eq!(expected["publicConstructors"], json!(["PullSysFlag()"]));
    assert_eq!(
        expected["declaredPublicMethods"],
        json!([
            "boolean hasClassFilterFlag(int)",
            "boolean hasCommitOffsetFlag(int)",
            "boolean hasLitePullFlag(int)",
            "boolean hasSubscriptionFlag(int)",
            "boolean hasSuspendFlag(int)",
            "int buildSysFlag(boolean, boolean, boolean, boolean)",
            "int buildSysFlag(boolean, boolean, boolean, boolean, boolean)",
            "int buildSysFlagWithSubscription(int)",
            "int clearCommitOffsetFlag(int)",
            "int clearSuspendFlag(int)"
        ])
    );

    assert_eq!(pull::build_pull_sys_flag(false, true, false, true), 0x0a);
    assert_eq!(
        pull::build_pull_sys_flag_with_lite(false, true, false, true, true),
        0x1a
    );
    let every_flag = pull::build_pull_sys_flag_with_lite(true, true, true, true, true);
    assert_eq!(every_flag, 0x1f);
    assert!(pull::has_commit_offset_flag(every_flag));
    assert!(pull::has_suspend_flag(every_flag));
    assert!(pull::has_subscription_flag(every_flag));
    assert!(pull::has_class_filter_flag(every_flag));
    assert!(pull::has_lite_pull_flag(every_flag));
    assert_eq!(pull::clear_commit_offset_flag(every_flag), 0x1e);
    assert_eq!(pull::clear_suspend_flag(every_flag), 0x1d);
    assert_eq!(pull::build_sys_flag_with_subscription(0x1b), 0x1f);
}

#[test]
fn message_constants_match_java_551() {
    let _guard = MESSAGE_CONSTANTS_MUTATION_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    use rocketmq_classic_common::message::constants;

    let expected = &fixture()["messageConstantsApi"];
    let message_const = [
        ("PROPERTY_KEYS", constants::PROPERTY_KEYS),
        ("PROPERTY_TAGS", constants::PROPERTY_TAGS),
        (
            "PROPERTY_WAIT_STORE_MSG_OK",
            constants::PROPERTY_WAIT_STORE_MSG_OK,
        ),
        (
            "PROPERTY_DELAY_TIME_LEVEL",
            constants::PROPERTY_DELAY_TIME_LEVEL,
        ),
        ("PROPERTY_RETRY_TOPIC", constants::PROPERTY_RETRY_TOPIC),
        ("PROPERTY_ORIGIN_GROUP", constants::PROPERTY_ORIGIN_GROUP),
        ("PROPERTY_REAL_TOPIC", constants::PROPERTY_REAL_TOPIC),
        ("PROPERTY_REAL_QUEUE_ID", constants::PROPERTY_REAL_QUEUE_ID),
        (
            "PROPERTY_TRANSACTION_PREPARED",
            constants::PROPERTY_TRANSACTION_PREPARED,
        ),
        (
            "PROPERTY_PRODUCER_GROUP",
            constants::PROPERTY_PRODUCER_GROUP,
        ),
        ("PROPERTY_MIN_OFFSET", constants::PROPERTY_MIN_OFFSET),
        ("PROPERTY_MAX_OFFSET", constants::PROPERTY_MAX_OFFSET),
        ("PROPERTY_BUYER_ID", constants::PROPERTY_BUYER_ID),
        (
            "PROPERTY_ORIGIN_MESSAGE_ID",
            constants::PROPERTY_ORIGIN_MESSAGE_ID,
        ),
        ("PROPERTY_TRANSFER_FLAG", constants::PROPERTY_TRANSFER_FLAG),
        (
            "PROPERTY_CORRECTION_FLAG",
            constants::PROPERTY_CORRECTION_FLAG,
        ),
        ("PROPERTY_MQ2_FLAG", constants::PROPERTY_MQ2_FLAG),
        (
            "PROPERTY_RECONSUME_TIME",
            constants::PROPERTY_RECONSUME_TIME,
        ),
        ("PROPERTY_MSG_REGION", constants::PROPERTY_MSG_REGION),
        ("PROPERTY_TRACE_SWITCH", constants::PROPERTY_TRACE_SWITCH),
        (
            "PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX",
            constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ),
        ("PROPERTY_TRANS_OFFSET", constants::PROPERTY_TRANS_OFFSET),
        (
            "PROPERTY_EXTEND_UNIQ_INFO",
            constants::PROPERTY_EXTEND_UNIQ_INFO,
        ),
        (
            "PROPERTY_MAX_RECONSUME_TIMES",
            constants::PROPERTY_MAX_RECONSUME_TIMES,
        ),
        (
            "PROPERTY_CONSUME_START_TIMESTAMP",
            constants::PROPERTY_CONSUME_START_TIMESTAMP,
        ),
        ("PROPERTY_PRIORITY", constants::PROPERTY_PRIORITY),
        ("PROPERTY_INNER_NUM", constants::PROPERTY_INNER_NUM),
        ("PROPERTY_INNER_BASE", constants::PROPERTY_INNER_BASE),
        ("DUP_INFO", constants::DUP_INFO),
        (
            "PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS",
            constants::PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
        ),
        (
            "PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET",
            constants::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
        ),
        (
            "PROPERTY_TRANSACTION_ID",
            constants::PROPERTY_TRANSACTION_ID,
        ),
        (
            "PROPERTY_TRANSACTION_CHECK_TIMES",
            constants::PROPERTY_TRANSACTION_CHECK_TIMES,
        ),
        ("PROPERTY_INSTANCE_ID", constants::PROPERTY_INSTANCE_ID),
        (
            "PROPERTY_CORRELATION_ID",
            constants::PROPERTY_CORRELATION_ID,
        ),
        (
            "PROPERTY_MESSAGE_REPLY_TO_CLIENT",
            constants::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
        ),
        ("PROPERTY_MESSAGE_TTL", constants::PROPERTY_MESSAGE_TTL),
        (
            "PROPERTY_REPLY_MESSAGE_ARRIVE_TIME",
            constants::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
        ),
        (
            "PROPERTY_PUSH_REPLY_TIME",
            constants::PROPERTY_PUSH_REPLY_TIME,
        ),
        ("PROPERTY_CLUSTER", constants::PROPERTY_CLUSTER),
        ("PROPERTY_MESSAGE_TYPE", constants::PROPERTY_MESSAGE_TYPE),
        ("PROPERTY_POP_CK", constants::PROPERTY_POP_CK),
        ("PROPERTY_POP_CK_OFFSET", constants::PROPERTY_POP_CK_OFFSET),
        (
            "PROPERTY_FIRST_POP_TIME",
            constants::PROPERTY_FIRST_POP_TIME,
        ),
        ("PROPERTY_SHARDING_KEY", constants::PROPERTY_SHARDING_KEY),
        ("PROPERTY_LITE_TOPIC", constants::PROPERTY_LITE_TOPIC),
        (
            "PROPERTY_FORWARD_QUEUE_ID",
            constants::PROPERTY_FORWARD_QUEUE_ID,
        ),
        ("PROPERTY_REDIRECT", constants::PROPERTY_REDIRECT),
        (
            "PROPERTY_INNER_MULTI_DISPATCH",
            constants::PROPERTY_INNER_MULTI_DISPATCH,
        ),
        (
            "PROPERTY_INNER_MULTI_QUEUE_OFFSET",
            constants::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
        ),
        ("PROPERTY_TRACE_CONTEXT", constants::PROPERTY_TRACE_CONTEXT),
        (
            "PROPERTY_TIMER_DELAY_SEC",
            constants::PROPERTY_TIMER_DELAY_SEC,
        ),
        (
            "PROPERTY_TIMER_DELIVER_MS",
            constants::PROPERTY_TIMER_DELIVER_MS,
        ),
        ("PROPERTY_BORN_HOST", constants::PROPERTY_BORN_HOST),
        (
            "PROPERTY_BORN_TIMESTAMP",
            constants::PROPERTY_BORN_TIMESTAMP,
        ),
        (
            "PROPERTY_TRANSIENT_PREFIX",
            constants::PROPERTY_TRANSIENT_PREFIX,
        ),
        (
            "PROPERTY_TRANSIENT_TOPIC_CONFIG",
            constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
        ),
        (
            "PROPERTY_TRANSIENT_GROUP_CONFIG",
            constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
        ),
        ("KEY_SEPARATOR", constants::KEY_SEPARATOR),
        ("INDEX_KEY_TYPE", constants::INDEX_KEY_TYPE),
        ("INDEX_UNIQUE_TYPE", constants::INDEX_UNIQUE_TYPE),
        ("INDEX_TAG_TYPE", constants::INDEX_TAG_TYPE),
        ("TIMER_ENGINE_TYPE", constants::TIMER_ENGINE_TYPE),
        (
            "TIMER_ENGINE_ROCKSDB_TIMELINE",
            constants::TIMER_ENGINE_ROCKSDB_TIMELINE,
        ),
        (
            "TIMER_ENGINE_FILE_TIME_WHEEL",
            constants::TIMER_ENGINE_FILE_TIME_WHEEL,
        ),
        (
            "PROPERTY_TIMER_ENQUEUE_MS",
            constants::PROPERTY_TIMER_ENQUEUE_MS,
        ),
        (
            "PROPERTY_TIMER_DEQUEUE_MS",
            constants::PROPERTY_TIMER_DEQUEUE_MS,
        ),
        (
            "PROPERTY_TIMER_ROLL_TIMES",
            constants::PROPERTY_TIMER_ROLL_TIMES,
        ),
        ("PROPERTY_TIMER_OUT_MS", constants::PROPERTY_TIMER_OUT_MS),
        (
            "PROPERTY_TIMER_DEL_UNIQKEY",
            constants::PROPERTY_TIMER_DEL_UNIQKEY,
        ),
        (
            "PROPERTY_TIMER_ROLL_LABEL",
            constants::PROPERTY_TIMER_ROLL_LABEL,
        ),
        (
            "PROPERTY_TIMER_DELAY_LEVEL",
            constants::PROPERTY_TIMER_DELAY_LEVEL,
        ),
        (
            "PROPERTY_TIMER_DELAY_MS",
            constants::PROPERTY_TIMER_DELAY_MS,
        ),
        ("PROPERTY_CRC32", constants::PROPERTY_CRC32),
        (
            "PROPERTY_DLQ_ORIGIN_TOPIC",
            constants::PROPERTY_DLQ_ORIGIN_TOPIC,
        ),
        (
            "PROPERTY_DLQ_ORIGIN_MESSAGE_ID",
            constants::PROPERTY_DLQ_ORIGIN_MESSAGE_ID,
        ),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    assert_eq!(message_const.len(), 76);
    let mut message_const_public_fields = message_const
        .keys()
        .map(|name| format!("public static final java.lang.String {name}"))
        .collect::<Vec<_>>();
    message_const_public_fields
        .push("public static final java.util.HashSet<java.lang.String> STRING_HASH_SET".to_owned());
    message_const_public_fields.sort();
    assert_eq!(
        expected["messageConstPublicFields"],
        json!(message_const_public_fields)
    );
    assert_eq!(
        expected["messageConstPublicConstructors"],
        json!(["MessageConst()"])
    );
    assert_eq!(expected["messageConstDeclaredPublicMethods"], json!([]));
    assert_eq!(
        serde_json::to_value(&message_const).unwrap(),
        expected["messageConst"]
    );

    let mix_all = BTreeMap::from([
        ("REPLY_TOPIC_POSTFIX", constants::REPLY_TOPIC_POSTFIX),
        ("REPLY_MESSAGE_FLAG", constants::REPLY_MESSAGE_FLAG),
    ]);
    assert_eq!(serde_json::to_value(mix_all).unwrap(), expected["mixAll"]);

    let mut reserved_names = {
        constants::STRING_HASH_SET
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|property| property.clone())
            .collect::<Vec<_>>()
    };
    reserved_names.sort();
    assert_eq!(
        serde_json::to_value(reserved_names).unwrap(),
        expected["reservedNames"]
    );
}

#[test]
fn message_queue_public_api_matches_java_551() {
    let expected = &fixture()["messageQueuePublicApi"];
    assert_eq!(expected["declaredPublicFields"], json!([]));
    assert_eq!(
        expected["publicConstructors"],
        json!([
            "MessageQueue()",
            "MessageQueue(java.lang.String, java.lang.String, int)",
            "MessageQueue(org.apache.rocketmq.common.message.MessageQueue)"
        ])
    );
    assert_eq!(
        expected["declaredPublicMethods"],
        json!([
            "boolean equals(java.lang.Object)",
            "int compareTo(java.lang.Object)",
            "int compareTo(org.apache.rocketmq.common.message.MessageQueue)",
            "int getQueueId()",
            "int hashCode()",
            "java.lang.String getBrokerName()",
            "java.lang.String getTopic()",
            "java.lang.String toString()",
            "void setBrokerName(java.lang.String)",
            "void setQueueId(int)",
            "void setTopic(java.lang.String)"
        ])
    );
}

#[test]
fn message_version_api_matches_java_551_fixture() {
    let expected = &fixture()["messageVersionApi"];
    assert_eq!(expected["publicConstructors"], json!([]));
    assert_eq!(
        expected["declaredPublicMethods"],
        json!([
            "int getMagicCode()",
            "int getTopicLength(java.nio.ByteBuffer)",
            "int getTopicLength(java.nio.ByteBuffer, int)",
            "int getTopicLengthSize()",
            "org.apache.rocketmq.common.message.MessageVersion valueOf(java.lang.String)",
            "org.apache.rocketmq.common.message.MessageVersion valueOfMagicCode(int)",
            "org.apache.rocketmq.common.message.MessageVersion[] values()",
            "void putTopicLength(java.nio.ByteBuffer, int)"
        ])
    );
    let versions = [MessageVersion::V1, MessageVersion::V2];
    assert_eq!(expected["values"].as_array().unwrap().len(), versions.len());

    for (index, version) in versions.into_iter().enumerate() {
        let item = &expected["values"][index];
        assert_eq!(version.name(), item["name"]);
        assert_eq!(version.to_string(), item["toString"]);
        assert_eq!(version.ordinal(), item["ordinal"].as_i64().unwrap() as i32);
        assert_eq!(version.magic_code(), item["magicCode"]);
        assert_eq!(version.topic_length_size(), item["topicLengthSize"]);
        assert_eq!(
            MessageVersion::value_of_name(Some(item["valueOfName"].as_str().unwrap())).unwrap(),
            version
        );
        assert_eq!(
            MessageVersion::value_of_magic_code(version.magic_code()).unwrap(),
            version
        );
        assert_eq!(item["valueOfMagicCode"], version.name());
        let other = if index == 0 {
            MessageVersion::V2
        } else {
            MessageVersion::V1
        };
        assert_eq!(
            version.java_compare_to(other),
            version.ordinal() - other.ordinal()
        );
        if index == 0 {
            assert!(item["compareToPrevious"].is_null());
        } else {
            assert_eq!(
                version.java_compare_to(versions[index - 1]),
                item["compareToPrevious"]
            );
        }

        let read_bytes: &[u8] = match version {
            MessageVersion::V1 => &[0x55, 0xff, 0x7f],
            MessageVersion::V2 => &[0x55, 0xff, 0xfe, 0x7f, 0xff],
        };
        let mut relative = &read_bytes[1..];
        assert_eq!(
            version.get_topic_length(&mut relative).unwrap(),
            item["relativeTopicLength"]
        );
        assert_eq!(
            relative.len(),
            read_bytes.len() - 1 - version.topic_length_size()
        );
        assert_eq!(
            item["relativePositionAfterRead"].as_u64().unwrap() as usize,
            1 + version.topic_length_size()
        );
        let indexed = version
            .get_topic_length_at(
                read_bytes,
                if version == MessageVersion::V1 { 2 } else { 3 },
            )
            .unwrap();
        assert_eq!(indexed, item["indexedTopicLength"]);

        let mut appended = vec![0];
        version.put_topic_length(&mut appended, 0x123456);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(&appended),
            item["putBytesBase64"]
        );
        assert_eq!(
            appended.len(),
            item["positionAfterWrite"].as_u64().unwrap() as usize
        );
    }

    for case in expected["narrowingCases"].as_array().unwrap() {
        let version =
            MessageVersion::value_of_name(Some(case["version"].as_str().unwrap())).unwrap();
        let topic_length = case["topicLength"].as_i64().unwrap() as i32;
        let mut bytes = Vec::new();
        version.put_topic_length(&mut bytes, topic_length);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(&bytes),
            case["bytesBase64"]
        );
        let mut input = bytes.as_slice();
        assert_eq!(
            version.get_topic_length(&mut input).unwrap(),
            case["readBack"]
        );
        assert!(input.is_empty());
    }

    assert_eq!(
        MessageVersion::value_of_name(None),
        Err(MessageVersionError::NullEnumName)
    );
    assert_eq!(
        MessageVersion::value_of_name(Some("MESSAGE_VERSION_V3")),
        Err(MessageVersionError::UnknownEnumName(
            "MESSAGE_VERSION_V3".to_owned()
        ))
    );
    assert_eq!(
        expected["invalidMagicCodeException"],
        "IllegalArgumentException"
    );
    assert!(matches!(
        MessageVersion::value_of_magic_code(0),
        Err(MessageVersionError::InvalidMagicCode(0))
    ));
    assert_eq!(
        expected["unknownEnumNameException"],
        "IllegalArgumentException"
    );
    assert_eq!(expected["nullEnumNameException"], "NullPointerException");

    let mut truncated = &[0xff][..];
    assert!(matches!(
        MessageVersion::V2.get_topic_length(&mut truncated),
        Err(MessageVersionError::Truncated {
            offset: 0,
            needed: 2,
            available: 1
        })
    ));
    assert_eq!(truncated, &[0xff]);
    assert_eq!(
        expected["relativeUnderflowException"],
        "BufferUnderflowException"
    );
    assert_eq!(
        expected["indexedOutOfBoundsException"],
        "IndexOutOfBoundsException"
    );
    assert!(matches!(
        MessageVersion::V2.get_topic_length_at(&[0], 0),
        Err(MessageVersionError::Truncated {
            offset: 0,
            needed: 2,
            available: 1
        })
    ));
}

#[test]
fn message_decoder_property_map_methods_match_java_551() {
    let expected = &fixture()["messageDecoderPropertiesApi"];
    assert_eq!(encode_properties_nullable(None), expected["nullMapEncoded"]);
    assert_eq!(
        encode_properties_nullable(Some(&MessageProperties::new())),
        expected["emptyMapEncoded"]
    );
    let null_value: [(Option<&str>, Option<&str>); 1] = [(Some("ignored"), None)];
    assert_eq!(
        encode_nullable_property_entries(Some(&null_value)),
        expected["nullValueEncoded"]
    );
    let null_key: [(Option<&str>, Option<&str>); 1] = [(None, Some("value"))];
    assert_eq!(
        encode_nullable_property_entries(Some(&null_key)),
        expected["nullKeyEncoded"]
    );
    assert_eq!(
        serde_json::to_value(decode_properties_nullable(None)).unwrap(),
        expected["nullInputDecoded"]
    );
    assert_eq!(
        serde_json::to_value(decode_properties_nullable(Some(""))).unwrap(),
        expected["emptyInputDecoded"]
    );
    let encoded = expected["encodedInput"].as_str().unwrap();
    assert_eq!(
        serde_json::to_value(decode_properties_nullable(Some(encoded))).unwrap(),
        expected["encodedInputDecoded"]
    );

    let zero_store_size_record = base64::engine::general_purpose::STANDARD
        .decode(expected["zeroStoreSizeRecordBase64"].as_str().unwrap())
        .unwrap();
    let (zero_store_properties, zero_store_position) =
        decode_record_properties(&zero_store_size_record).unwrap();
    assert_eq!(
        serde_json::to_value(zero_store_properties).unwrap(),
        expected["zeroStoreSizeRecordProperties"]
    );
    assert_eq!(
        zero_store_position,
        expected["zeroStoreSizeRecordPosition"].as_u64().unwrap() as usize
    );
    for decoded_case in expected["malformedStoreSizeFullDecodes"]
        .as_array()
        .unwrap()
    {
        let record = base64::engine::general_purpose::STANDARD
            .decode(decoded_case["recordBase64"].as_str().unwrap())
            .unwrap();
        let (decoded, consumed) = decode_one(&record, DecodeOptions::default()).unwrap();
        let snapshot = decoded_snapshot(&decoded, consumed);
        assert_eq!(snapshot, decoded_case["decoded"]);
        assert_eq!(
            decoded.store_size(),
            decoded_case["decodedStoreSize"].as_i64().unwrap() as i32
        );
    }
    let malformed_store_size_cases = expected["malformedStoreSizeFullDecodes"]
        .as_array()
        .unwrap();
    let mut malformed_store_size_batch = Vec::new();
    for decoded_case in &malformed_store_size_cases[..2] {
        malformed_store_size_batch.extend(
            base64::engine::general_purpose::STANDARD
                .decode(decoded_case["recordBase64"].as_str().unwrap())
                .unwrap(),
        );
    }
    let decoded_batch =
        decode_many_compatible(&malformed_store_size_batch, DecodeOptions::default());
    assert_eq!(
        decoded_batch
            .iter()
            .map(MessageExt::store_size)
            .collect::<Vec<_>>(),
        [0, -1]
    );
    for key in [
        "negativeBodyLengthFullDecode",
        "negativePropertiesLengthFullDecode",
    ] {
        let decoded_case = &expected[key];
        let record = base64::engine::general_purpose::STANDARD
            .decode(decoded_case["recordBase64"].as_str().unwrap())
            .unwrap();
        let (decoded, consumed) = decode_one(&record, DecodeOptions::default()).unwrap();
        assert_eq!(
            decoded_snapshot(&decoded, consumed),
            decoded_case["decoded"]
        );
        assert_eq!(
            decoded.store_size(),
            decoded_case["decodedStoreSize"].as_i64().unwrap() as i32
        );
    }

    let malformed_properties_record = base64::engine::general_purpose::STANDARD
        .decode(
            expected["malformedPropertiesRecordBase64"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    let (malformed_properties, malformed_properties_position) =
        decode_record_properties(&malformed_properties_record).unwrap();
    assert_eq!(
        serde_json::to_value(malformed_properties).unwrap(),
        expected["malformedPropertiesRecordProperties"]
    );
    assert_eq!(
        malformed_properties_position,
        expected["malformedPropertiesRecordPosition"]
            .as_u64()
            .unwrap() as usize
    );

    for (base64_key, properties_key, position_key) in [
        (
            "zeroPropertiesLengthRecordBase64",
            "zeroPropertiesLengthRecordProperties",
            "zeroPropertiesLengthRecordPosition",
        ),
        (
            "negativePropertiesLengthRecordBase64",
            "negativePropertiesLengthRecordProperties",
            "negativePropertiesLengthRecordPosition",
        ),
    ] {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(expected[base64_key].as_str().unwrap())
            .unwrap();
        let (properties, position) = decode_record_properties(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(properties).unwrap(),
            expected[properties_key]
        );
        assert_eq!(position, expected[position_key].as_u64().unwrap() as usize);
    }

    let truncated_properties_record = base64::engine::general_purpose::STANDARD
        .decode(
            expected["truncatedPropertiesRecordBase64"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        expected["truncatedPropertiesRecordError"],
        "BufferUnderflowException"
    );
    let error = decode_record_properties(&truncated_properties_record).unwrap_err();
    assert!(matches!(
        error,
        DecodeError::Truncated {
            offset,
            field: "properties"
        } if offset == expected["truncatedPropertiesRecordPosition"].as_u64().unwrap() as usize
    ));
}

#[test]
fn compact_message_codec_and_inner_batch_match_java_551() {
    let expected = &fixture()["messageCompactApi"];
    assert_eq!(expected["zeroCountRemainsRunning"], true);
    assert_eq!(expected["negativeCycleCountRemainsRunning"], true);
    let mut first = Message::default();
    first.set_body([0, 255, 42]);
    first.set_flag(-17);
    first.set_system_property("k", "v").unwrap();
    let mut second = Message::default();
    second.set_body(Vec::new());

    let first_bytes = encode_compact_message(&first).unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&first_bytes),
        expected["singleEncodedBase64"]
    );
    let batch_bytes = encode_compact_messages(&[first, second]).unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&batch_bytes),
        expected["batchEncodedBase64"]
    );
    let mut batch_cursor = Cursor::new(batch_bytes.as_slice());
    assert_eq!(
        count_inner_messages(&mut batch_cursor).unwrap(),
        expected["innerMessageCount"].as_u64().unwrap() as usize
    );
    assert_eq!(
        batch_cursor.position(),
        expected["innerMessageCountPositionAfter"]
    );
    let mut empty_cursor = Cursor::new(&[][..]);
    assert_eq!(
        count_inner_messages(&mut empty_cursor).unwrap(),
        expected["emptyInnerMessageCount"].as_u64().unwrap() as usize
    );
    assert_eq!(
        empty_cursor.position(),
        expected["emptyInnerMessageCountPositionAfter"]
    );
    let mut positioned_batch_bytes = vec![91, 92, 93];
    positioned_batch_bytes.extend_from_slice(&batch_bytes);
    let mut positioned_batch_cursor = Cursor::new(positioned_batch_bytes.as_slice());
    positioned_batch_cursor.set_position(3);
    assert_eq!(
        count_inner_messages(&mut positioned_batch_cursor).unwrap(),
        expected["positionedInnerMessageCount"].as_u64().unwrap() as usize
    );
    assert_eq!(
        positioned_batch_cursor.position(),
        expected["positionedInnerMessageCountPositionAfter"]
    );

    let (decoded_first, consumed) = decode_compact_message(&first_bytes).unwrap();
    assert_eq!(consumed, first_bytes.len());
    assert_eq!(decoded_first.topic_nullable(), None);
    assert_eq!(decoded_first.flag(), expected["singleDecoded"]["flag"]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(decoded_first.body()),
        expected["singleDecoded"]["bodyBase64"]
    );
    assert_eq!(
        serde_json::to_value(decoded_first.properties_nullable().unwrap()).unwrap(),
        expected["singleDecoded"]["properties"]
    );

    let decoded_batch = decode_compact_messages(&batch_bytes).unwrap();
    assert_eq!(decoded_batch.len(), 2);
    for (message, case) in decoded_batch
        .iter()
        .zip(expected["batchDecoded"].as_array().unwrap())
    {
        assert_eq!(message.topic_nullable(), None);
        assert_eq!(message.flag(), case["flag"]);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(message.body()),
            case["bodyBase64"]
        );
        assert_eq!(
            serde_json::to_value(message.properties_nullable().unwrap()).unwrap(),
            case["properties"]
        );
    }

    let mut outer = MessageExt::default();
    outer.set_topic("outer-topic");
    outer.set_body(batch_bytes.clone());
    outer.set_queue_id(7);
    outer.set_queue_offset(100);
    outer.set_sys_flag(0x40);
    outer.set_born_timestamp(11);
    outer.set_born_host(Some(MessageHostAddress::new("192.0.2.10", 10911)));
    outer.set_store_timestamp(12);
    outer.set_store_host(Some(MessageHostAddress::new("198.51.100.20", 10912)));
    outer.set_commit_log_offset(13);
    outer.set_wait_store_msg_ok(false).unwrap();
    let mut expanded = decode_inner_batch(&outer).unwrap();
    assert_eq!(expanded.len(), 2);
    for (message, case) in expanded
        .iter()
        .zip(expected["expanded"].as_array().unwrap())
    {
        assert_eq!(message.topic(), case["topic"]);
        assert_eq!(message.queue_id(), case["queueId"]);
        assert_eq!(message.queue_offset(), case["queueOffset"]);
        assert_eq!(message.flag(), case["flag"]);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(message.body()),
            case["bodyBase64"]
        );
        assert_eq!(
            serde_json::to_value(message.properties()).unwrap(),
            case["properties"]
        );
        assert_eq!(message.is_wait_store_msg_ok(), case["waitStore"]);
        assert_eq!(message.sys_flag(), case["sysFlag"]);
        assert_eq!(message.born_timestamp(), case["bornTimestamp"]);
        assert_eq!(message.born_host().unwrap().to_string(), case["bornHost"]);
        assert_eq!(message.store_timestamp(), case["storeTimestamp"]);
        assert_eq!(message.store_host().unwrap().to_string(), case["storeHost"]);
        assert_eq!(message.commit_log_offset(), case["commitLogOffset"]);
        assert!(message.is_message_client_ext());
    }

    let runtime_api = &expected["expandedClientExtApi"];
    let first_expanded = &mut expanded[0];
    assert_eq!(
        first_expanded.is_message_client_ext(),
        runtime_api["runtimeClass"] == "MessageClientExt"
    );
    assert_eq!(
        first_expanded.message_id_for_runtime_type().as_deref(),
        runtime_api["initialMessageId"].as_str()
    );
    assert_eq!(
        first_expanded
            .offset_message_id_for_runtime_type()
            .as_deref(),
        runtime_api["initialOffsetMessageId"].as_str()
    );
    first_expanded.set_message_id(Some("ignored-message-id".to_owned()));
    assert_eq!(
        first_expanded.message_id_for_runtime_type().as_deref(),
        runtime_api["messageIdAfterSetMessageId"].as_str()
    );
    assert_eq!(
        first_expanded
            .offset_message_id_for_runtime_type()
            .as_deref(),
        runtime_api["offsetMessageIdAfterSetMessageId"].as_str()
    );
    first_expanded.set_offset_message_id(Some("physical-offset-id".to_owned()));
    assert_eq!(
        first_expanded.message_id_for_runtime_type().as_deref(),
        runtime_api["messageIdAfterSetOffsetMessageId"].as_str()
    );
    assert_eq!(
        first_expanded
            .offset_message_id_for_runtime_type()
            .as_deref(),
        runtime_api["offsetMessageIdAfterSetOffsetMessageId"].as_str()
    );
    first_expanded
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            "unique-id",
        )
        .unwrap();
    assert_eq!(
        first_expanded.message_id_for_runtime_type().as_deref(),
        runtime_api["messageIdWithUniqueKey"].as_str()
    );
    assert_eq!(
        first_expanded
            .offset_message_id_for_runtime_type()
            .as_deref(),
        runtime_api["offsetMessageIdWithUniqueKey"].as_str()
    );

    assert_eq!(
        encode_compact_message(&Message::default()),
        Err(CompactMessageError::MissingBody)
    );
    assert!(expected["nullBodyEncodeThrows"].as_bool().unwrap());
    assert!(
        expected["shortDecodeThrowsBufferUnderflow"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["negativeBodyDecodeThrows"].as_bool().unwrap());
    assert!(
        expected["incompleteCountThrowsIllegalArgument"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        decode_compact_message(&[0]).unwrap_err(),
        CompactMessageError::IncompleteField {
            field: "total size",
            length: 4
        }
    );
    let mut negative_body = vec![0; 16];
    negative_body.extend_from_slice(&(-1_i32).to_be_bytes());
    assert_eq!(
        decode_compact_message(&negative_body).unwrap_err(),
        CompactMessageError::NegativeLength {
            field: "body",
            length: -1
        }
    );
    let incomplete_count_bytes = [0, 0, 0, 5];
    let mut incomplete_count_cursor = Cursor::new(incomplete_count_bytes.as_slice());
    assert_eq!(
        count_inner_messages(&mut incomplete_count_cursor).unwrap_err(),
        CompactMessageError::IncompleteInnerMessage {
            offset: 0,
            size: 5,
            available: 4
        }
    );
    assert_eq!(
        incomplete_count_cursor.position(),
        expected["incompleteCountPositionAfter"]
    );
    let mut short_count_cursor = Cursor::new(&[0, 0, 0][..]);
    assert_eq!(
        count_inner_messages(&mut short_count_cursor),
        Err(CompactMessageError::Truncated {
            field: "inner message size"
        })
    );
    assert_eq!(expected["shortCountThrowsBufferUnderflow"], true);
    assert_eq!(
        short_count_cursor.position(),
        expected["shortCountPositionAfter"]
    );
    let mut sub_four_count_cursor = Cursor::new(&[0, 0, 0, 3, 0, 0, 0, 4][..]);
    assert_eq!(
        count_inner_messages(&mut sub_four_count_cursor),
        Err(CompactMessageError::IncompleteInnerMessage {
            offset: 3,
            size: 50_331_648,
            available: 5
        })
    );
    assert_eq!(expected["subFourCountThrowsIllegalArgument"], true);
    assert_eq!(
        sub_four_count_cursor.position(),
        expected["subFourCountPositionAfter"]
    );
    let mut negative_count_cursor = Cursor::new(&[u8::MAX; 4][..]);
    assert_eq!(
        count_inner_messages(&mut negative_count_cursor),
        Err(CompactMessageError::InvalidInnerSize { size: -1 })
    );
    assert_eq!(expected["negativeCountThrowsIllegalArgument"], true);
    assert_eq!(
        negative_count_cursor.position(),
        expected["negativeCountPositionAfter"]
    );
    let mut rewinding_count_cursor = Cursor::new(&[0, 0, 0, 8, 0, 0, 0, 0, 255, 255, 255, 253][..]);
    assert_eq!(
        count_inner_messages(&mut rewinding_count_cursor),
        Err(CompactMessageError::IncompleteInnerMessage {
            offset: 5,
            size: 255,
            available: 7
        })
    );
    assert_eq!(expected["rewindingCountThrowsIllegalArgument"], true);
    assert_eq!(
        rewinding_count_cursor.position(),
        expected["rewindingCountPositionAfter"]
    );
}

#[test]
fn count_inner_zero_and_negative_cycle_match_java_nontermination() {
    const PROBE_MODE: &str = "ROCKETMQ_COUNT_INNER_PROBE_MODE";
    const PROBE_MARKER: &str = "ROCKETMQ_COUNT_INNER_PROBE_MARKER";

    if let Ok(mode) = std::env::var(PROBE_MODE) {
        let input: &[u8] = match mode.as_str() {
            "zero" => &[0, 0, 0, 0],
            "negative-cycle" => {
                let mut input = Box::new([0_u8; 16]);
                input[..4].copy_from_slice(&4_i32.to_be_bytes());
                input[4..8].copy_from_slice(&8_i32.to_be_bytes());
                input[12..16].copy_from_slice(&(-8_i32).to_be_bytes());
                let marker = std::env::var_os(PROBE_MARKER).unwrap();
                std::fs::write(marker, b"entered").unwrap();
                let mut cursor = Cursor::new(input.as_slice());
                let result = count_inner_messages(&mut cursor);
                panic!("negative cycle returned unexpectedly: {result:?}");
            }
            other => panic!("unknown count probe mode: {other}"),
        };
        let marker = std::env::var_os(PROBE_MARKER).unwrap();
        std::fs::write(marker, b"entered").unwrap();
        let mut cursor = Cursor::new(input);
        let result = count_inner_messages(&mut cursor);
        panic!("zero-size loop returned unexpectedly: {result:?}");
    }

    let executable = std::env::current_exe().unwrap();
    for mode in ["zero", "negative-cycle"] {
        let marker = std::env::temp_dir().join(format!(
            "rocketmq-count-inner-probe-{}-{mode}",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&marker);
        let mut child = Command::new(&executable)
            .args([
                "--exact",
                "count_inner_zero_and_negative_cycle_match_java_nontermination",
                "--nocapture",
            ])
            .env(PROBE_MODE, mode)
            .env(PROBE_MARKER, &marker)
            .spawn()
            .unwrap();
        let startup_deadline = Instant::now() + Duration::from_secs(5);
        while !marker.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("{mode} probe exited before entering count loop: {status}");
            }
            assert!(
                Instant::now() < startup_deadline,
                "{mode} probe did not start"
            );
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(50));
        assert!(
            child.try_wait().unwrap().is_none(),
            "{mode} count loop returned instead of matching Java's nontermination"
        );
        child.kill().unwrap();
        let _ = child.wait();
        std::fs::remove_file(marker).unwrap();
    }
}

#[test]
fn message_ext_encoder_matches_java_551() {
    fn message_ext_for_encoding(sys_flag: i32, born_ip: &str, store_ip: &str) -> MessageExt {
        let mut message_ext = MessageExt::default();
        message_ext.set_topic("orders");
        message_ext.set_body([0, 255, 42, 55]);
        message_ext.set_flag(-23);
        message_ext.set_system_property("k", "v").unwrap();
        message_ext.set_body_crc(0x1234_5678);
        message_ext.set_queue_id(7);
        message_ext.set_queue_offset(23);
        message_ext.set_commit_log_offset(4567);
        message_ext.set_sys_flag(sys_flag);
        message_ext.set_born_timestamp(123);
        message_ext.set_born_host(Some(MessageHostAddress::from_ip_address(
            born_ip.parse().unwrap(),
            10_911,
        )));
        message_ext.set_store_timestamp(234);
        message_ext.set_store_host(Some(MessageHostAddress::from_ip_address(
            store_ip.parse().unwrap(),
            10_912,
        )));
        message_ext.set_reconsume_times(2);
        message_ext.set_prepared_transaction_offset(345);
        message_ext
    }

    let expected = &fixture()["messageExtEncodeApi"];
    let ipv4 = message_ext_for_encoding(0, "192.0.2.10", "198.51.100.20");
    let ipv4_encoded = encode_message_ext(&ipv4, false).unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(&ipv4_encoded),
        expected["ipv4StandardBase64"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .encode(encode_message_ext_uniquely(&ipv4, false).unwrap()),
        expected["ipv4UniqueBase64"]
    );

    let ipv6_flag = message::BORNHOST_V6_FLAG | message::STOREHOSTADDRESS_V6_FLAG;
    let ipv6 = message_ext_for_encoding(ipv6_flag, "2001:db8::10", "2001:db8::20");
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(encode_message_ext(&ipv6, false).unwrap()),
        expected["ipv6StandardBase64"]
    );
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .encode(encode_message_ext_uniquely(&ipv6, false).unwrap()),
        expected["ipv6UniqueBase64"]
    );

    let mut padded = ipv4.clone();
    padded.set_store_size(ipv4_encoded.len() as i32 + 3);
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .encode(encode_message_ext(&padded, false).unwrap()),
        expected["paddedStandardBase64"]
    );

    let mut too_small = ipv4.clone();
    too_small.set_store_size(1);
    assert!(encode_message_ext(&too_small, false).is_err());
    assert_eq!(expected["tooSmallStoreSizeThrows"], true);

    for case in expected["compressed"].as_array().unwrap() {
        let sys_flag = message::COMPRESSED_FLAG | case["compressionType"].as_i64().unwrap() as i32;
        let compressed = message_ext_for_encoding(sys_flag, "192.0.2.10", "198.51.100.20");
        let encoded = encode_message_ext(&compressed, true).unwrap();
        let (decoded, consumed) = decode_one(&encoded, DecodeOptions::default()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(decoded.body()),
            case["decodedBodyBase64"]
        );
        assert_eq!(decoded.sys_flag(), case["decodedSysFlag"]);
    }
}

#[test]
fn nullable_message_property_keys_match_java_551() {
    let expected = &fixture()["messagePublicApi"];
    let mut message = Message::default();
    assert_eq!(
        message.set_property_nullable(None, Some("value".to_owned())),
        None
    );
    assert_eq!(
        message.get_nullable_property(None).as_deref(),
        Some("value")
    );
    assert!(message.has_nullable_property(None));
    assert_eq!(expected["nullKeyPropertyPresent"], true);
    assert_eq!(expected["nullKeyPropertyValue"], "value");
    assert_eq!(message.to_string(), expected["nullKeyPropertyString"]);
    let entries = message.nullable_property_entries().collect::<Vec<_>>();
    assert_eq!(entries, vec![(None, Some("value".to_owned()))]);
    assert_eq!(
        encode_nullable_property_entries(Some(&entries)),
        expected["nullKeyPropertyEncoded"]
    );

    let serialized = serde_json::to_value(&message).unwrap();
    assert_eq!(serialized["nullKeyProperty"]["value"], "value");
    let round_trip: Message = serde_json::from_value(serialized).unwrap();
    assert_eq!(
        round_trip.get_nullable_property(None).as_deref(),
        Some("value")
    );
    assert!(round_trip.has_nullable_property(None));

    let mut compact_message = Message::new("orders", [1, 2, 3]);
    compact_message.set_property_nullable(None, Some("value".to_owned()));
    let compact = encode_compact_message(&compact_message).unwrap();
    let (decoded_compact, consumed) = decode_compact_message(&compact).unwrap();
    assert_eq!(consumed, compact.len());
    assert_eq!(
        decoded_compact.get_property("null").as_deref(),
        Some("value")
    );

    let mut message_ext = MessageExt::default();
    message_ext.set_topic("orders");
    message_ext.set_body([1, 2, 3]);
    message_ext.set_property_nullable(None, Some("value".to_owned()));
    message_ext.set_born_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_911,
    )));
    message_ext.set_store_host(Some(MessageHostAddress::from_ip_address(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        10_912,
    )));
    let record = encode_message_ext(&message_ext, false).unwrap();
    let (record_properties, _) = decode_record_properties(&record).unwrap();
    let record_properties = record_properties.unwrap();
    assert_eq!(
        record_properties.get("null").map(String::as_str),
        Some("value")
    );

    let previous = message.set_property_nullable(None, None);
    assert_eq!(previous, Some(Some("value".to_owned())));
    assert!(message.has_nullable_property(None));
    assert_eq!(message.get_nullable_property(None), None);
    assert_eq!(expected["nullKeyNullValuePresent"], true);
    assert_eq!(expected["nullKeyNullValueEncoded"], "");
    assert_eq!(
        encode_nullable_property_entries(Some(
            &message.nullable_property_entries().collect::<Vec<_>>()
        )),
        expected["nullKeyNullValueEncoded"]
    );
    assert_eq!(message.clear_property_nullable(None), Some(None));
    assert!(!message.has_nullable_property(None));
    assert!(message.get_properties().is_some());
    assert_eq!(expected["nullKeyPropertyCleared"], true);
    assert_eq!(expected["nullKeyPropertyMapRemainsPresent"], true);

    let mut nullable_properties =
        rocketmq_classic_common::message::NullableMessageProperties::new();
    nullable_properties.insert(None, Some("value".to_owned()));
    nullable_properties.insert(Some("nullable".to_owned()), None);
    message.set_nullable_properties(Some(nullable_properties));
    assert_eq!(
        message.get_nullable_property(None).as_deref(),
        Some("value")
    );
    assert_eq!(message.get_property("nullable").as_deref(), None);
    assert!(message.has_property("nullable"));
    message.set_properties(None);
    assert!(!message.has_nullable_property(None));

    let mut null_key_replacement =
        rocketmq_classic_common::message::NullableMessageProperties::new();
    null_key_replacement.insert(None, Some("replacement".to_owned()));
    message.set_nullable_properties(Some(null_key_replacement));
    assert_eq!(
        message.get_nullable_property(None).as_deref(),
        Some("replacement")
    );
    assert_eq!(expected["setPropertiesNullKeyPresent"], true);
    assert_eq!(expected["setPropertiesNullKeyValue"], "replacement");
    assert_eq!(
        encode_nullable_property_entries(Some(
            &message.nullable_property_entries().collect::<Vec<_>>()
        )),
        expected["setPropertiesNullKeyEncoded"]
    );
}

#[test]
fn message_public_api_matches_java_551_fixture() {
    let _guard = MESSAGE_CONSTANTS_MUTATION_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let expected = &fixture()["messagePublicApi"];
    assert_eq!(
        expected["messagePublicConstructors"],
        json!([
            "Message()",
            "Message(java.lang.String, byte[])",
            "Message(java.lang.String, java.lang.String, byte[])",
            "Message(java.lang.String, java.lang.String, java.lang.String, byte[])",
            "Message(java.lang.String, java.lang.String, java.lang.String, int, byte[], boolean)"
        ])
    );
    assert_eq!(
        expected["messagePublicMethods"],
        json!([
            "boolean hasProperty(java.lang.String)",
            "boolean isWaitStoreMsgOK()",
            "byte[] getBody()",
            "int getDelayTimeLevel()",
            "int getFlag()",
            "int getPriority()",
            "java.lang.String getBuyerId()",
            "java.lang.String getKeys()",
            "java.lang.String getProperty(java.lang.String)",
            "java.lang.String getTags()",
            "java.lang.String getTopic()",
            "java.lang.String getTransactionId()",
            "java.lang.String getUserProperty(java.lang.String)",
            "java.lang.String toString()",
            "java.util.Map getProperties()",
            "long getDelayTimeMs()",
            "long getDelayTimeSec()",
            "long getDeliverTimeMs()",
            "void putUserProperty(java.lang.String, java.lang.String)",
            "void setBody(byte[])",
            "void setBuyerId(java.lang.String)",
            "void setDelayTimeLevel(int)",
            "void setDelayTimeMs(long)",
            "void setDelayTimeSec(long)",
            "void setDeliverTimeMs(long)",
            "void setFlag(int)",
            "void setInstanceId(java.lang.String)",
            "void setKeys(java.lang.String)",
            "void setKeys(java.util.Collection)",
            "void setPriority(int)",
            "void setTags(java.lang.String)",
            "void setTopic(java.lang.String)",
            "void setTransactionId(java.lang.String)",
            "void setWaitStoreMsgOK(boolean)"
        ])
    );
    assert_eq!(
        expected["messageAccessorPublicMethods"],
        json!([
            "java.lang.String getConsumeStartTimeStamp(org.apache.rocketmq.common.message.Message)",
            "java.lang.String getCorrectionFlag(org.apache.rocketmq.common.message.Message)",
            "java.lang.String getMQ2Flag(org.apache.rocketmq.common.message.Message)",
            "java.lang.String getMaxReconsumeTimes(org.apache.rocketmq.common.message.Message)",
            "java.lang.String getOriginMessageId(org.apache.rocketmq.common.message.Message)",
            "java.lang.String getReconsumeTime(org.apache.rocketmq.common.message.Message)",
            "java.lang.String getTransferFlag(org.apache.rocketmq.common.message.Message)",
            "java.util.Map deepCopyProperties(java.util.Map)",
            "org.apache.rocketmq.common.message.Message cloneMessage(org.apache.rocketmq.common.message.Message)",
            "void clearProperty(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void putProperty(org.apache.rocketmq.common.message.Message, java.lang.String, java.lang.String)",
            "void setConsumeStartTimeStamp(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void setCorrectionFlag(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void setLiteTopic(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void setMQ2Flag(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void setMaxReconsumeTimes(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void setOriginMessageId(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void setProperties(org.apache.rocketmq.common.message.Message, java.util.Map)",
            "void setReconsumeTime(org.apache.rocketmq.common.message.Message, java.lang.String)",
            "void setTransferFlag(org.apache.rocketmq.common.message.Message, java.lang.String)"
        ])
    );
    let default_message = Message::default();
    assert_eq!(default_message.topic_nullable(), None);
    assert_eq!(default_message.body_nullable(), None);
    assert_eq!(default_message.get_properties(), None);
    assert_eq!(default_message.to_string(), expected["defaultToString"]);
    assert_eq!(expected["messageEqualsSelf"], true);
    assert_eq!(expected["messageEqualsDistinctSameState"], false);
    assert_eq!(expected["messageExtEqualsSelf"], true);
    assert_eq!(expected["messageExtEqualsDistinctSameState"], false);
    assert_eq!(expected["messageEqualsDeclaringClass"], "java.lang.Object");
    assert_eq!(
        expected["messageHashCodeDeclaringClass"],
        "java.lang.Object"
    );
    assert_eq!(expected["messageHashCodeStableAfterMutation"], true);
    assert_eq!(
        expected["messageExtEqualsDeclaringClass"],
        "java.lang.Object"
    );
    assert_eq!(
        expected["messageExtHashCodeDeclaringClass"],
        "java.lang.Object"
    );
    assert_eq!(expected["messageExtHashCodeStableAfterMutation"], true);
    assert_eq!(expected["messageExtEqualsBaseView"], true);
    assert_eq!(expected["baseViewEqualsMessageExt"], true);
    assert_eq!(expected["messageClientExtEqualsBaseView"], true);
    assert_eq!(expected["baseViewEqualsMessageClientExt"], true);
    assert_eq!(expected["messageClientExtEqualsExtView"], true);
    let default_message_ext = MessageExt::default();
    assert!(PartialEq::eq(&default_message_ext, &default_message_ext));
    assert_ne!(default_message_ext, MessageExt::default());
    let default_after_lookup = default_message.clone();
    assert_eq!(default_after_lookup.get_property("missing"), None);
    assert_eq!(
        default_after_lookup
            .get_properties()
            .map(|properties| properties.is_empty()),
        Some(true)
    );
    assert_eq!(expected["defaultTopicIsNull"], true);
    assert_eq!(expected["defaultBodyIsNull"], true);
    assert_eq!(expected["defaultPropertiesIsNull"], true);
    assert_eq!(expected["propertyLookupInitializesProperties"], true);

    let nullable_constructor = Message::new_nullable(None, None);
    assert_eq!(nullable_constructor.topic_nullable(), None);
    assert_eq!(nullable_constructor.body_nullable(), None);
    assert!(nullable_constructor.is_wait_store_msg_ok());
    assert_eq!(expected["nullableConstructorTopicIsNull"], true);
    assert_eq!(expected["nullableConstructorBodyIsNull"], true);
    assert_eq!(expected["nullableConstructorWaitStore"], true);

    let nullable_tags_constructor = Message::with_tags(None, None, None);
    assert_eq!(nullable_tags_constructor.topic_nullable(), None);
    assert_eq!(nullable_tags_constructor.body_nullable(), None);
    assert!(!nullable_tags_constructor.has_property("TAGS"));
    assert_eq!(expected["nullableTagsConstructorTopicIsNull"], true);
    assert_eq!(expected["nullableTagsConstructorBodyIsNull"], true);
    assert_eq!(expected["nullableTagsConstructorHasTags"], false);

    let nullable_keys_constructor = Message::with_tags_and_keys(None, None, None, None);
    assert_eq!(nullable_keys_constructor.topic_nullable(), None);
    assert_eq!(nullable_keys_constructor.body_nullable(), None);
    assert!(!nullable_keys_constructor.has_property("TAGS"));
    assert!(!nullable_keys_constructor.has_property("KEYS"));
    assert_eq!(expected["nullableKeysConstructorTopicIsNull"], true);
    assert_eq!(expected["nullableKeysConstructorBodyIsNull"], true);
    assert_eq!(expected["nullableKeysConstructorHasTags"], false);
    assert_eq!(expected["nullableKeysConstructorHasKeys"], false);

    let nullable_fields = Message::with_nullable_fields(None, None, None, -3, None, false);
    assert_eq!(nullable_fields.topic_nullable(), None);
    assert_eq!(nullable_fields.body_nullable(), None);
    assert!(!nullable_fields.is_wait_store_msg_ok());
    assert_eq!(expected["nullableFieldsTopicIsNull"], true);
    assert_eq!(expected["nullableFieldsBodyIsNull"], true);
    assert_eq!(expected["nullableFieldsWaitStore"], false);

    let mut nullable_setters = Message::new("before", [1]);
    nullable_setters.set_topic_nullable(None);
    nullable_setters.set_body_nullable(None);
    assert_eq!(nullable_setters.topic_nullable(), None);
    assert_eq!(nullable_setters.body_nullable(), None);
    assert_eq!(expected["nullableSetterTopicIsNull"], true);
    assert_eq!(expected["nullableSetterBodyIsNull"], true);

    let mut direct_setters = Message::default();
    assert_eq!(
        direct_setters.transaction_id(),
        expected["defaultTransactionId"].as_str()
    );
    assert_eq!(direct_setters.set_tags("setter-tag"), Ok(None));
    assert_eq!(direct_setters.set_keys("setter-key"), Ok(None));
    assert_eq!(direct_setters.set_wait_store_msg_ok(false), Ok(None));
    direct_setters.set_transaction_id(Some("transaction-setter".to_owned()));
    assert_eq!(
        direct_setters.tags().as_deref(),
        expected["setTagsGetter"].as_str()
    );
    assert_eq!(
        direct_setters.keys().as_deref(),
        expected["setKeysStringGetter"].as_str()
    );
    assert_eq!(
        direct_setters.is_wait_store_msg_ok(),
        expected["setWaitStoreMsgOKFalse"]
    );
    assert_eq!(
        direct_setters
            .get_property(rocketmq_classic_common::message::constants::PROPERTY_WAIT_STORE_MSG_OK)
            .as_deref(),
        expected["setWaitStoreMsgOKProperty"].as_str()
    );
    assert_eq!(
        direct_setters.transaction_id(),
        expected["setTransactionIdGetter"].as_str()
    );
    assert_eq!(
        direct_setters.set_wait_store_msg_ok(true),
        Ok(Some("false".to_owned()))
    );
    assert_eq!(
        direct_setters.is_wait_store_msg_ok(),
        expected["setWaitStoreMsgOKTrue"]
    );
    direct_setters.set_tags_nullable(None);
    direct_setters.set_keys_nullable(None);
    direct_setters.set_transaction_id(None);
    assert!(direct_setters.has_property("TAGS"));
    assert!(direct_setters.has_property("KEYS"));
    assert_eq!(direct_setters.tags(), None);
    assert_eq!(direct_setters.keys(), None);
    assert_eq!(direct_setters.transaction_id(), None);
    assert_eq!(expected["nullSetterTagsRemainsPresent"], true);
    assert_eq!(expected["nullSetterTagsGetter"], serde_json::Value::Null);
    assert_eq!(expected["nullSetterKeysRemainsPresent"], true);
    assert_eq!(expected["nullSetterKeysGetter"], serde_json::Value::Null);
    assert_eq!(expected["nullSetterTransactionId"], serde_json::Value::Null);

    let mut nullable_properties = Message::default();
    nullable_properties.set_tags_nullable(None);
    nullable_properties.set_buyer_id_nullable(None);
    nullable_properties.set_instance_id_nullable(None);
    for (property, getter_is_null, remains_present) in [
        ("TAGS", "nullTagsGetterIsNull", "nullTagsRemainsPresent"),
        (
            "BUYER_ID",
            "nullBuyerGetterIsNull",
            "nullBuyerRemainsPresent",
        ),
        (
            "INSTANCE_ID",
            "nullInstanceGetterIsNull",
            "nullInstanceRemainsPresent",
        ),
    ] {
        assert_eq!(nullable_properties.get_property(property), None);
        assert_eq!(
            nullable_properties.has_property(property),
            expected[remains_present]
        );
        assert_eq!(expected[getter_is_null], true);
    }
    assert_eq!(
        serde_json::to_value(nullable_properties.property_entries().collect::<Vec<_>>()).unwrap(),
        expected["nullablePropertyEntries"]
    );
    let nullable_map = serde_json::to_value(&nullable_properties).unwrap();
    assert!(nullable_map["topic"].is_null());
    assert!(nullable_map["body"].is_null());
    assert!(nullable_map["properties"]["TAGS"].is_null());
    let nullable_round_trip: Message = serde_json::from_value(nullable_map).unwrap();
    assert!(nullable_round_trip.topic_nullable().is_none());
    assert!(nullable_round_trip.body_nullable().is_none());
    assert!(nullable_round_trip.has_property("TAGS"));
    assert_eq!(nullable_round_trip.get_property("TAGS"), None);

    let mut nullable_tag_string = Message::default();
    nullable_tag_string.set_tags_nullable(None);
    assert_eq!(
        nullable_tag_string.to_string(),
        expected["nullablePropertyString"]
    );
    assert_eq!(
        nullable_properties.put_user_property_nullable(None, Some("value".to_owned())),
        Err(rocketmq_classic_common::message::MessagePropertyError::EmptyKey)
    );
    assert_eq!(
        nullable_properties.put_user_property_nullable(Some("key".to_owned()), None),
        Err(rocketmq_classic_common::message::MessagePropertyError::EmptyValue)
    );
    assert_eq!(expected["nullPropertyNameRejected"], true);
    assert_eq!(expected["nullPropertyValueRejected"], true);
    let mut whitespace_property = Message::default();
    assert_eq!(
        whitespace_property.put_user_property("\u{00a0}", "value"),
        Ok(None)
    );
    assert_eq!(
        whitespace_property.put_user_property("key", "\u{2003}"),
        Ok(None)
    );
    assert_eq!(
        whitespace_property
            .put_user_property_nullable(Some("custom".to_owned()), Some("\u{00a0}".to_owned())),
        Ok(None)
    );
    assert_eq!(
        whitespace_property.put_user_property("\u{001f} ", "value"),
        Err(rocketmq_classic_common::message::MessagePropertyError::EmptyKey)
    );
    assert_eq!(expected["nonBreakingSpaceUserPropertyNameAccepted"], true);
    assert_eq!(expected["emSpaceUserPropertyValueAccepted"], true);
    assert_eq!(expected["asciiControlOnlyUserPropertyRejected"], true);
    let mut priority = Message::default();
    assert_eq!(
        priority.priority(),
        expected["absentPriority"].as_i64().unwrap() as i32
    );
    assert_eq!(expected["absentPriorityInitializesProperties"], true);
    assert_eq!(
        priority
            .get_properties_mut()
            .map(|properties| properties.is_empty()),
        Some(true)
    );
    assert_eq!(priority.set_priority(17), Ok(()));
    assert_eq!(
        priority.priority(),
        expected["setPriority"].as_i64().unwrap() as i32
    );
    assert_eq!(
        priority
            .get_property(rocketmq_classic_common::message::constants::PROPERTY_PRIORITY)
            .as_deref(),
        Some(expected["setPriorityProperty"].as_str().unwrap())
    );
    assert_eq!(
        priority.set_priority(-1),
        Err(rocketmq_classic_common::message::MessagePropertyError::NegativePriority)
    );
    assert_eq!(expected["negativePriorityRejected"], true);
    assert_eq!(
        rocketmq_classic_common::message::MessagePropertyError::NegativePriority.to_string(),
        expected["negativePriorityErrorMessage"].as_str().unwrap()
    );
    assert_eq!(
        priority.priority(),
        expected["priorityAfterNegative"].as_i64().unwrap() as i32
    );
    assert_eq!(priority.set_priority(0), Ok(()));
    assert_eq!(
        priority.priority(),
        expected["zeroPriority"].as_i64().unwrap() as i32
    );
    for input in expected["priorityInputs"].as_array().unwrap() {
        let mut candidate = Message::default();
        candidate
            .set_system_property(
                rocketmq_classic_common::message::constants::PROPERTY_PRIORITY,
                input["input"].as_str().unwrap(),
            )
            .unwrap();
        assert_eq!(
            candidate.priority(),
            input["priority"].as_i64().unwrap() as i32
        );
    }
    let dynamic_reserved_property = "__RMQ_DYNAMIC_RESERVED_FIXTURE__";
    let dynamic_reserved_key = Some(dynamic_reserved_property.to_owned());
    let dynamic_was_reserved = {
        let mut properties = rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let existed = properties.contains(&dynamic_reserved_key);
        properties.insert(dynamic_reserved_key.clone());
        existed
    };
    let dynamic_reserved_result =
        Message::default().put_user_property(dynamic_reserved_property, "value");
    if !dynamic_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&dynamic_reserved_key);
    }
    assert_eq!(
        dynamic_reserved_result,
        Err(rocketmq_classic_common::message::MessagePropertyError::ReservedProperty)
    );
    let null_reserved_was_reserved = {
        let mut properties = rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let existed = properties.contains(&None);
        properties.insert(None);
        existed
    };
    let null_reserved_result =
        Message::default().put_user_property_nullable(None, Some("value".to_owned()));
    if !null_reserved_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&None);
    }
    assert_eq!(
        null_reserved_result,
        Err(rocketmq_classic_common::message::MessagePropertyError::ReservedProperty)
    );
    assert_eq!(expected["mutatedNullReservedPropertyPresent"], true);
    assert_eq!(
        expected["nullReservedPropertyExceptionClass"],
        "java.lang.RuntimeException"
    );
    let empty_reserved_key = Some(String::new());
    let empty_reserved_was_reserved = {
        let mut properties = rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let existed = properties.contains(&empty_reserved_key);
        properties.insert(empty_reserved_key.clone());
        existed
    };
    let empty_reserved_result = Message::default().put_user_property("", "value");
    if !empty_reserved_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&empty_reserved_key);
    }
    assert_eq!(
        empty_reserved_result,
        Err(rocketmq_classic_common::message::MessagePropertyError::ReservedProperty)
    );
    assert_eq!(expected["mutatedEmptyReservedPropertyPresent"], true);
    assert_eq!(
        expected["emptyReservedPropertyExceptionClass"],
        "java.lang.RuntimeException"
    );
    let keys_was_reserved = rocketmq_classic_common::message::constants::STRING_HASH_SET
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&Some(
            rocketmq_classic_common::message::constants::PROPERTY_KEYS.to_owned(),
        ));
    let removed_reserved_result = Message::default().put_user_property(
        rocketmq_classic_common::message::constants::PROPERTY_KEYS,
        "user-value",
    );
    if keys_was_reserved {
        rocketmq_classic_common::message::constants::STRING_HASH_SET
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(Some(
                rocketmq_classic_common::message::constants::PROPERTY_KEYS.to_owned(),
            ));
    }
    assert!(keys_was_reserved);
    assert_eq!(removed_reserved_result, Ok(None));
    assert_eq!(expected["mutatedReservedPropertyRejected"], true);
    assert_eq!(expected["removedReservedPropertyAccepted"], true);
    let null_name_lookup = Message::default();
    assert_eq!(null_name_lookup.get_nullable_property(None), None);
    assert_eq!(
        null_name_lookup
            .get_properties()
            .map(|properties| properties.is_empty()),
        Some(true)
    );
    assert_eq!(expected["nullNameLookupIsNull"], true);
    assert_eq!(expected["nullNameLookupInitializesProperties"], true);
    let null_name_contains = Message::default();
    assert!(!null_name_contains.has_nullable_property(None));
    assert_eq!(null_name_contains.get_properties(), None);
    assert_eq!(expected["nullNameContainsIsFalse"], true);
    assert_eq!(expected["nullNameContainsKeepsPropertiesNull"], true);
    let null_user_name = Message::default();
    assert_eq!(null_user_name.get_nullable_user_property(None), None);
    assert_eq!(expected["nullUserNameLookupIsNull"], true);

    let mut replaced_properties = Message::default();
    replaced_properties.set_properties(Some(std::collections::BTreeMap::from([(
        "old".to_owned(),
        "value".to_owned(),
    )])));
    assert_eq!(
        replaced_properties.get_property("old").as_deref(),
        Some("value")
    );
    assert_eq!(expected["setPropertiesInitialValue"], "value");
    replaced_properties.set_properties(Some(std::collections::BTreeMap::from([(
        "new".to_owned(),
        "replacement".to_owned(),
    )])));
    assert!(!replaced_properties.has_property("old"));
    assert_eq!(
        replaced_properties.get_property("new").as_deref(),
        Some("replacement")
    );
    assert_eq!(expected["setPropertiesReplacesOldKey"], true);
    assert_eq!(expected["setPropertiesReplacementValue"], "replacement");
    replaced_properties.set_properties(Some(std::collections::BTreeMap::new()));
    assert_eq!(
        replaced_properties
            .get_properties()
            .map(|map| map.is_empty()),
        Some(true)
    );
    assert_eq!(expected["setPropertiesEmptyMapIsPresent"], true);
    replaced_properties.set_properties(None);
    assert_eq!(replaced_properties.get_properties(), None);
    assert_eq!(expected["setPropertiesNullMapIsNull"], true);

    let caller_properties = rocketmq_classic_common::message::SharedMessageProperties::new();
    caller_properties.insert(Some("initial".to_owned()), Some("caller".to_owned()));
    let mut shared_properties_message = Message::new("shared-properties-topic", [1]);
    shared_properties_message.set_properties_shared(Some(caller_properties.clone()));
    assert_eq!(
        shared_properties_message.get_property("initial").as_deref(),
        Some("caller")
    );
    assert_eq!(expected["setPropertiesRetainsCallerMap"], true);
    caller_properties.insert(
        Some("caller-mutation".to_owned()),
        Some("visible-to-message".to_owned()),
    );
    assert_eq!(
        shared_properties_message
            .get_property("caller-mutation")
            .as_deref(),
        expected["setPropertiesCallerMutationVisible"].as_str()
    );
    shared_properties_message
        .shared_properties()
        .unwrap()
        .insert(
            Some("message-mutation".to_owned()),
            Some("visible-to-caller".to_owned()),
        );
    assert_eq!(
        caller_properties.get(Some("message-mutation")),
        Some(Some(
            expected["setPropertiesMessageMutationVisible"]
                .as_str()
                .unwrap()
                .to_owned()
        ))
    );
    let nullable_shared_properties =
        rocketmq_classic_common::message::SharedMessageProperties::from_nullable_properties(
            std::collections::BTreeMap::from([(None, Some("replacement".to_owned()))]),
        );
    let mut nullable_shared_message = Message::default();
    nullable_shared_message.set_properties_shared(Some(nullable_shared_properties.clone()));
    assert_eq!(
        nullable_shared_message
            .get_nullable_property(None)
            .as_deref(),
        expected["setPropertiesNullKeyValue"].as_str()
    );
    assert_eq!(expected["setPropertiesNullKeyPresent"], true);
    assert_eq!(
        nullable_shared_properties.insert(None, None),
        Some(Some("replacement".to_owned()))
    );
    assert!(nullable_shared_message.has_nullable_property(None));
    assert_eq!(nullable_shared_message.get_nullable_property(None), None);
    assert_eq!(nullable_shared_properties.remove(None), Some(None));
    assert!(!nullable_shared_message.has_nullable_property(None));

    let caller_body = rocketmq_classic_common::message::SharedMessageBody::new([1, 2, 3]);
    let mut shared_body_message = Message::new("shared-body-topic", [0]);
    shared_body_message.set_body_shared(Some(caller_body.clone()));
    let attached_body = shared_body_message.shared_body().unwrap();
    assert!(caller_body.ptr_eq(&attached_body));
    assert_eq!(expected["setBodyRetainsCallerArray"], true);
    caller_body.with_bytes_mut(|body| {
        body[0] = expected["setBodyCallerMutationValue"].as_u64().unwrap() as u8;
    });
    assert_eq!(
        shared_body_message.body()[0],
        expected["setBodyCallerMutationValue"].as_u64().unwrap() as u8
    );
    assert_eq!(expected["setBodyCallerMutationVisibleToMessage"], true);
    shared_body_message.body_mut().unwrap()[1] =
        expected["setBodyMessageMutationValue"].as_u64().unwrap() as u8;
    assert_eq!(
        caller_body.with_bytes(|body| body[1]),
        expected["setBodyMessageMutationValue"].as_u64().unwrap() as u8
    );
    assert_eq!(expected["setBodyMessageMutationVisibleToCaller"], true);

    let replacement_body = rocketmq_classic_common::message::SharedMessageBody::new([4, 5, 6]);
    shared_body_message.set_body_shared(Some(replacement_body.clone()));
    assert!(replacement_body.ptr_eq(&shared_body_message.shared_body().unwrap()));
    assert_eq!(expected["setBodyReplacementRetainsNewArray"], true);
    caller_body.with_bytes_mut(|body| body[2] = 10);
    assert_eq!(shared_body_message.body()[2], 6);
    assert_eq!(expected["setBodyReplacementDetachesOldArray"], true);

    let two_argument_body = rocketmq_classic_common::message::SharedMessageBody::new([1, 2]);
    let two_argument_message = Message::new_shared("two-argument", two_argument_body.clone());
    assert!(two_argument_body.ptr_eq(&two_argument_message.shared_body().unwrap()));
    assert_eq!(expected["constructorBodyAliasTwoArguments"], true);
    two_argument_body.with_bytes_mut(|body| body[0] = 9);
    assert_eq!(two_argument_message.body()[0], 9);
    assert_eq!(
        expected["constructorBodyMutationVisibleForTwoArguments"],
        true
    );

    let tags_body = rocketmq_classic_common::message::SharedMessageBody::new([3, 4]);
    let tags_message = Message::with_tags_shared(
        Some("with-tags".to_owned()),
        Some("green".to_owned()),
        Some(tags_body.clone()),
    );
    assert!(tags_body.ptr_eq(&tags_message.shared_body().unwrap()));
    assert_eq!(expected["constructorBodyAliasWithTags"], true);

    let tags_and_keys_body = rocketmq_classic_common::message::SharedMessageBody::new([5, 6]);
    let tags_and_keys_message = Message::with_tags_and_keys_shared(
        Some("with-tags-and-keys".to_owned()),
        Some("blue".to_owned()),
        Some("key".to_owned()),
        Some(tags_and_keys_body.clone()),
    );
    assert!(tags_and_keys_body.ptr_eq(&tags_and_keys_message.shared_body().unwrap()));
    assert_eq!(expected["constructorBodyAliasWithTagsAndKeys"], true);

    let full_fields_body = rocketmq_classic_common::message::SharedMessageBody::new([7, 8]);
    let full_fields_message = Message::with_fields_shared(
        "with-fields",
        Some("yellow"),
        Some("key"),
        -11,
        full_fields_body.clone(),
        false,
    );
    assert!(full_fields_body.ptr_eq(&full_fields_message.shared_body().unwrap()));
    assert_eq!(expected["constructorBodyAliasWithAllFields"], true);
    let nullable_shared_constructor = Message::new_nullable_shared(None, None);
    assert!(nullable_shared_constructor.topic_nullable().is_none());
    assert!(nullable_shared_constructor.body_nullable().is_none());
    assert_eq!(expected["nullableConstructorTopicIsNull"], true);
    assert_eq!(expected["nullableConstructorBodyIsNull"], true);

    assert_eq!(expected["deepCopyPropertiesNullMapIsNull"], true);
    assert_eq!(expected["deepCopyPropertiesReturnsDistinctMap"], true);
    assert_eq!(expected["deepCopyPropertiesPreservesNullKeyAndValue"], true);
    assert_eq!(expected["deepCopyPropertiesInitialValue"], "original");
    assert_eq!(expected["deepCopyPropertiesIgnoresSourceMutation"], true);
    assert_eq!(
        expected["deepCopyPropertiesSourceIgnoresCopyMutation"],
        true
    );
    let mut accessor_clone_source = Message::new("clone-topic", [0, 255, 42]);
    accessor_clone_source.set_flag(29);
    accessor_clone_source.set_transaction_id(Some("source-transaction".to_owned()));
    accessor_clone_source
        .put_user_property("clone-key", "clone-value")
        .unwrap();
    let accessor_clone = accessor_clone_source.clone_message();
    assert!(!std::ptr::eq(&accessor_clone, &accessor_clone_source));
    assert_eq!(expected["accessorCloneIsDistinct"], true);
    assert_eq!(accessor_clone.topic(), expected["accessorCloneTopic"]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(accessor_clone.body()),
        expected["accessorCloneBodyBase64"]
    );
    assert_eq!(accessor_clone.flag(), expected["accessorCloneFlag"]);
    assert_eq!(
        accessor_clone.get_property("clone-key").as_deref(),
        expected["accessorCloneProperty"].as_str()
    );
    assert_eq!(accessor_clone.transaction_id(), None);
    assert_eq!(expected["accessorCloneTransactionIdIsNull"], true);
    assert_eq!(
        accessor_clone_source.transaction_id(),
        expected["accessorCloneSourceTransactionId"].as_str()
    );
    let mut alias_source = Message::new("clone-alias-topic", [1, 2, 3]);
    alias_source.set_properties(Some(std::collections::BTreeMap::from([(
        "shared".to_owned(),
        "initial".to_owned(),
    )])));
    let mut alias_clone = alias_source.clone_message();
    assert_eq!(expected["accessorCloneSharesBodyArray"], true);
    assert_eq!(expected["accessorCloneSharesPropertiesMap"], true);
    alias_clone.body_mut().unwrap()[0] = expected["accessorCloneBodyMutationVisibleInSource"]
        .as_u64()
        .unwrap() as u8;
    assert_eq!(
        alias_source.body()[0],
        expected["accessorCloneBodyMutationVisibleInSource"]
            .as_u64()
            .unwrap() as u8
    );
    alias_source
        .set_system_property("source-only", "source")
        .unwrap();
    assert_eq!(
        alias_clone.get_property("source-only").as_deref(),
        expected["accessorClonePropertyMutationVisibleInClone"].as_str()
    );
    alias_clone
        .set_system_property("clone-only", "clone")
        .unwrap();
    assert_eq!(
        alias_source.get_property("clone-only").as_deref(),
        expected["accessorClonePropertyMutationVisibleInSource"].as_str()
    );
    alias_clone.set_body([7, 8]);
    assert_eq!(expected["accessorCloneSetBodyDetachesArray"], true);
    assert_eq!(
        alias_source.body()[0],
        expected["accessorCloneSetBodyLeavesSourceArray"]
            .as_u64()
            .unwrap() as u8
    );
    alias_clone.set_properties(Some(std::collections::BTreeMap::from([(
        "detached".to_owned(),
        "value".to_owned(),
    )])));
    assert_eq!(expected["accessorCloneSetPropertiesDetachesMap"], true);
    alias_clone
        .set_system_property("clone-after-detach", "clone")
        .unwrap();
    assert_eq!(
        expected["accessorCloneDetachedPropertyMutationLeavesSource"],
        !alias_source.has_property("clone-after-detach")
    );
    let mut independent_clone = accessor_clone.clone();
    independent_clone.set_body([9, 8, 7]);
    independent_clone
        .put_user_property("copy-only", "copy")
        .unwrap();
    assert_eq!(accessor_clone_source.body().as_ref(), &[0, 255, 42]);
    assert_eq!(accessor_clone_source.get_property("copy-only"), None);
    let mut copy_source = rocketmq_classic_common::message::NullableMessageProperties::new();
    copy_source.insert(None, None);
    copy_source.insert(Some("present".to_owned()), Some("original".to_owned()));
    let mut copied_properties = copy_source.clone();
    copy_source.insert(Some("source-only".to_owned()), Some("source".to_owned()));
    assert!(!copied_properties.contains_key(&Some("source-only".to_owned())));
    copied_properties.insert(Some("copy-only".to_owned()), Some("copy".to_owned()));
    assert!(!copy_source.contains_key(&Some("copy-only".to_owned())));
    assert_eq!(copied_properties.get(&None), Some(&None));
    assert_eq!(
        copied_properties.get(&Some("present".to_owned())),
        Some(&Some("original".to_owned()))
    );
    let absent_properties: Option<rocketmq_classic_common::message::NullableMessageProperties> =
        None;
    assert_eq!(absent_properties.clone(), None);

    let helper_values = &expected["messageAccessorHelperValues"];
    let mut accessor_helpers = Message::default();
    for (property, value, key) in [
        (
            rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG,
            "transfer",
            "transferFlag",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_CORRECTION_FLAG,
            "correction",
            "correctionFlag",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_ORIGIN_MESSAGE_ID,
            "origin-id",
            "originMessageId",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_MQ2_FLAG,
            "mq2",
            "mq2Flag",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_RECONSUME_TIME,
            "4",
            "reconsumeTime",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_MAX_RECONSUME_TIMES,
            "8",
            "maxReconsumeTimes",
        ),
        (
            rocketmq_classic_common::message::constants::PROPERTY_CONSUME_START_TIMESTAMP,
            "1700000000123",
            "consumeStartTimeStamp",
        ),
    ] {
        accessor_helpers
            .set_system_property(property, value)
            .unwrap();
        assert_eq!(
            accessor_helpers.get_property(property).as_deref(),
            helper_values[key].as_str()
        );
    }
    accessor_helpers
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_LITE_TOPIC,
            "orders-lite",
        )
        .unwrap();
    assert_eq!(
        helper_values["liteTopic"],
        accessor_helpers
            .get_property(rocketmq_classic_common::message::constants::PROPERTY_LITE_TOPIC)
            .unwrap()
    );
    accessor_helpers.set_system_property_nullable(
        rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG,
        None,
    );
    assert!(
        accessor_helpers
            .has_property(rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG)
    );
    assert_eq!(
        accessor_helpers
            .get_property(rocketmq_classic_common::message::constants::PROPERTY_TRANSFER_FLAG),
        None
    );
    assert_eq!(helper_values["transferFlagNullIsPresent"], true);
    assert_eq!(
        helper_values["transferFlagNullValue"],
        serde_json::Value::Null
    );

    let mut collection_with_null = Message::default();
    collection_with_null
        .set_keys_from_nullable(Some(vec![Some("left"), None, Some("right")]))
        .unwrap();
    assert_eq!(
        collection_with_null.keys().as_deref(),
        expected["collectionKeysWithNullMember"].as_str()
    );
    assert_eq!(expected["collectionKeysWithNullMember"], "left null right");
    let mut null_collection = Message::default();
    assert_eq!(
        null_collection.set_keys_from_nullable(None::<Vec<Option<&str>>>),
        Err(rocketmq_classic_common::message::MessageKeyCollectionError::NullCollection)
    );
    assert_eq!(expected["nullCollectionKeysThrows"], true);

    let mut message = Message::with_fields(
        "orders",
        Some("green"),
        Some("first second"),
        17,
        [0, 255, 42],
        false,
    );
    assert_eq!(message.topic(), expected["topic"]);
    assert_eq!(message.tags().as_deref(), expected["tags"].as_str());
    assert_eq!(message.keys().as_deref(), expected["keys"].as_str());
    assert_eq!(message.flag(), expected["flag"]);
    assert_eq!(
        base64::engine::general_purpose::STANDARD.encode(message.body()),
        expected["body"]
    );
    assert_eq!(message.is_wait_store_msg_ok(), expected["waitStore"]);
    assert_eq!(
        message.get_property("TAGS").as_deref(),
        expected["propertyTags"].as_str()
    );
    assert_eq!(
        message.get_user_property("TAGS").as_deref(),
        expected["userPropertyTags"].as_str()
    );
    assert_eq!(message.get_property("missing"), None);
    assert_eq!(message.has_property("TAGS"), expected["hasTags"]);
    assert_eq!(message.has_property("missing"), expected["hasMissing"]);

    message.set_keys_from(["third", "fourth"]);
    assert_eq!(
        message.keys().as_deref(),
        expected["collectionKeys"].as_str()
    );
    message.set_buyer_id("buyer-1");
    message.set_instance_id("instance-1");
    assert_eq!(message.buyer_id().as_deref(), expected["buyerId"].as_str());
    assert_eq!(
        message.get_property("INSTANCE_ID").as_deref(),
        expected["instanceIdProperty"].as_str()
    );

    message.set_delay_time_level(-7);
    message.set_delay_time_sec(i64::MIN);
    message.set_delay_time_ms(9_007_199_254_740_993);
    message.set_deliver_time_ms(i64::MAX);
    assert_eq!(
        message.delay_time_level().unwrap(),
        expected["delayTimeLevel"]
    );
    assert_eq!(message.delay_time_sec().unwrap(), expected["delayTimeSec"]);
    assert_eq!(message.delay_time_ms().unwrap(), expected["delayTimeMs"]);
    assert_eq!(
        message.deliver_time_ms().unwrap(),
        expected["deliverTimeMs"]
    );

    let absent = Message::default();
    assert_eq!(
        absent.delay_time_level().unwrap(),
        expected["absentDelayTimeLevel"]
    );
    assert_eq!(
        absent.delay_time_sec().unwrap(),
        expected["absentDelayTimeSec"]
    );
    assert_eq!(
        absent.delay_time_ms().unwrap(),
        expected["absentDelayTimeMs"]
    );
    assert_eq!(
        absent.deliver_time_ms().unwrap(),
        expected["absentDeliverTimeMs"]
    );

    let mut unicode = Message::default();
    unicode.set_system_property("DELAY", "١٢").unwrap();
    unicode
        .set_system_property("TIMER_DELAY_SEC", "+٣٤")
        .unwrap();
    assert_eq!(
        unicode.delay_time_level().unwrap(),
        expected["unicodeDelayTimeLevel"]
    );
    assert_eq!(
        unicode.delay_time_sec().unwrap(),
        expected["unicodeDelayTimeSec"]
    );

    unicode.set_system_property("DELAY", "2147483648").unwrap();
    assert_eq!(
        unicode.delay_time_level().is_err(),
        expected["overflowDelayTimeLevelThrows"]
    );
    unicode
        .set_system_property("TIMER_DELAY_MS", "9223372036854775808")
        .unwrap();
    assert_eq!(
        unicode.delay_time_ms().is_err(),
        expected["overflowDelayTimeMsThrows"]
    );

    for case in expected["numericPropertyInputs"].as_array().unwrap() {
        let property = case["property"].as_str().unwrap();
        let input = case["input"].as_str().unwrap();
        let mut candidate = Message::default();
        candidate.set_system_property(property, input).unwrap();
        let parsed = match property {
            "DELAY" => candidate.delay_time_level().map(i64::from),
            "TIMER_DELAY_SEC" => candidate.delay_time_sec(),
            "TIMER_DELAY_MS" => candidate.delay_time_ms(),
            "TIMER_DELIVER_MS" => candidate.deliver_time_ms(),
            _ => panic!("unexpected Java property {property}"),
        };
        assert_eq!(parsed.is_ok(), case["accepted"], "{property}={input:?}");
        if let Some(value) = case["value"].as_i64() {
            assert_eq!(parsed.unwrap(), value, "{property}={input:?}");
        }
    }

    let mut diagnostic = Message::new("invoice", [0, 255, 42]);
    assert_eq!(diagnostic.to_string(), expected["messageToString"]);
    diagnostic
        .get_properties_mut()
        .unwrap()
        .insert("DIRECT".to_owned(), "through-map".to_owned());
    assert_eq!(
        diagnostic.get_property("DIRECT").as_deref(),
        expected["directMapMutation"].as_str()
    );

    let mut extended = message_ext_from(message);
    extended.set_topic("changed-through-inherited-api");
    assert_eq!(extended.topic(), "changed-through-inherited-api");
}

#[test]
fn message_object_identity_traits_match_java_551_fixture() {
    let expected = &fixture()["messagePublicApi"];
    let mut message = Message::default();
    let message_hash = object_hash(&message);
    assert_eq!(
        PartialEq::eq(&message, &message),
        expected["messageEqualsSelf"]
    );
    assert_eq!(
        message == Message::default(),
        expected["messageEqualsDistinctSameState"]
    );
    message.set_flag(9);
    assert_eq!(
        object_hash(&message) == message_hash,
        expected["messageHashCodeStableAfterMutation"]
    );

    let message_clone = message.clone();
    assert_eq!(message, message_clone);
    assert_eq!(object_hash(&message), object_hash(&message_clone));
    let accessor_clone = message.clone_message();
    assert_ne!(message, accessor_clone);
    let decoded = decode_one(&record(&fixture()["cases"][0]), DecodeOptions::default())
        .unwrap()
        .0;
    let decoded_again = decode_one(&record(&fixture()["cases"][0]), DecodeOptions::default())
        .unwrap()
        .0;
    assert_ne!(decoded, decoded_again);
    let serialized = serde_json::to_string(&message).unwrap();
    let deserialized: Message = serde_json::from_str(&serialized).unwrap();
    assert_ne!(message, deserialized);

    let mut extended = MessageExt::default();
    let extended_hash = object_hash(&extended);
    extended.set_queue_id(9);
    let base_from_extended: &Message = &extended;
    assert_eq!(
        PartialEq::eq(base_from_extended, &extended),
        expected["messageExtEqualsBaseView"]
    );
    assert_eq!(
        PartialEq::eq(&extended, base_from_extended),
        expected["baseViewEqualsMessageExt"]
    );
    assert_eq!(object_hash(base_from_extended), object_hash(&extended));
    assert_eq!(
        object_hash(&extended) == extended_hash,
        expected["messageExtHashCodeStableAfterMutation"]
    );
    let serialized_extended = serde_json::to_string(&extended).unwrap();
    let deserialized_extended: MessageExt = serde_json::from_str(&serialized_extended).unwrap();
    assert_ne!(extended, deserialized_extended);
    assert_eq!(extended, extended.clone());
    assert_eq!(extended_hash, object_hash(&extended));

    assert_eq!(
        expected["messageClientExtHashCodeDeclaringClass"],
        "java.lang.Object"
    );
    assert_eq!(
        expected["messageClientExtHashCodeStableAfterMutation"],
        true
    );
    let mut client_extended = MessageClientExt::default();
    let client_hash = object_hash(&client_extended);
    client_extended.set_topic_nullable(Some("hash-mutated-topic".to_owned()));
    assert_eq!(
        object_hash(&client_extended) == client_hash,
        expected["messageClientExtHashCodeStableAfterMutation"]
    );
    let base_from_client: &Message = &client_extended;
    let extended_from_client: &MessageExt = &client_extended;
    assert_eq!(
        PartialEq::eq(base_from_client, &client_extended),
        expected["messageClientExtEqualsBaseView"]
    );
    assert_eq!(
        PartialEq::eq(&client_extended, base_from_client),
        expected["baseViewEqualsMessageClientExt"]
    );
    assert_eq!(
        PartialEq::eq(&client_extended, extended_from_client),
        expected["messageClientExtEqualsExtView"]
    );
    assert_eq!(object_hash(base_from_client), object_hash(&client_extended));
    assert_eq!(
        object_hash(extended_from_client),
        object_hash(&client_extended)
    );
    assert_eq!(
        PartialEq::eq(&client_extended, &client_extended),
        expected["messageClientExtEqualsSelf"]
    );
    assert_eq!(
        PartialEq::eq(&client_extended, &MessageClientExt::default()),
        expected["messageClientExtEqualsDistinctSameState"]
    );
    let serialized_client_extended = serde_json::to_string(&client_extended).unwrap();
    let deserialized_client_extended: MessageClientExt =
        serde_json::from_str(&serialized_client_extended).unwrap();
    assert_ne!(client_extended, deserialized_client_extended);
    assert!(deserialized_client_extended.is_message_client_ext());
    let client_clone = client_extended.clone();
    assert_eq!(client_extended, client_clone);
}

#[test]
fn message_ext_system_flag_helpers_match_java_551() {
    let expected = &fixture()["messageExtSystemFlagsApi"];
    let mut message = MessageExt::default();
    assert_eq!(expected["topicSysFlagAbsentIsNull"], true);
    assert_eq!(expected["groupSysFlagAbsentIsNull"], true);
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);

    message.set_topic_sys_flag(Some(73));
    message.set_group_sys_flag(Some(-19));
    assert_eq!(message.topic_sys_flag().unwrap(), Some(73));
    assert_eq!(message.group_sys_flag().unwrap(), Some(-19));
    assert_eq!(expected["topicSysFlagSet"], 73);
    assert_eq!(expected["groupSysFlagSet"], -19);
    message.set_topic_sys_flag(None);
    message.set_group_sys_flag(None);
    assert_eq!(expected["topicSysFlagCleared"], true);
    assert_eq!(expected["groupSysFlagCleared"], true);
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);

    for key in [
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
    ] {
        message.set_system_property(key, "").unwrap();
    }
    assert_eq!(expected["topicSysFlagEmptyIsNull"], true);
    assert_eq!(expected["groupSysFlagEmptyIsNull"], true);
    assert_eq!(message.topic_sys_flag().unwrap(), None);
    assert_eq!(message.group_sys_flag().unwrap(), None);

    for key in [
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
        rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
    ] {
        message.set_system_property(key, "bad").unwrap();
    }
    assert_eq!(expected["topicSysFlagMalformedThrows"], true);
    assert_eq!(expected["groupSysFlagMalformedThrows"], true);
    assert!(message.topic_sys_flag().is_err());
    assert!(message.group_sys_flag().is_err());
    message.set_topic_sys_flag(Some(1));
    message
        .set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
            "١",
        )
        .unwrap();
    assert_eq!(
        message.topic_sys_flag().unwrap(),
        Some(expected["topicSysFlagUnicodeDigit"].as_i64().unwrap() as i32)
    );

    message.set_sys_flag(0x40);
    message.set_born_host_v6_flag();
    assert_eq!(message.sys_flag(), expected["bornV6FlagAfterSetter"]);
    message.set_store_host_address_v6_flag();
    assert_eq!(message.sys_flag(), expected["storeV6FlagAfterSetter"]);
    assert_eq!(expected["singleTagFilterType"], "SINGLE_TAG");
    assert_eq!(
        MessageExt::parse_topic_filter_type(0x42),
        TopicFilterType::MultiTag
    );
    assert_eq!(expected["multiTagFilterType"], "MULTI_TAG");
    assert_eq!(
        MessageExt::parse_topic_filter_type(0),
        TopicFilterType::SingleTag
    );

    let mut diagnostic = Message::default();
    diagnostic.set_topic("invoice");
    diagnostic.set_body(vec![0, 255, 42]);
    diagnostic.set_flag(5);
    diagnostic.set_system_property("KEY", "value").unwrap();
    let diagnostic = message_ext_from(diagnostic);
    assert_eq!(
        diagnostic.to_string(),
        expected["messageExtToString"].as_str().unwrap()
    );
}

#[test]
fn decode_many_handles_concatenated_records_and_requires_complete_progress() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let joined: Vec<u8> = cases.iter().flat_map(record).collect();
    let decoded = decode_many_compatible(&joined, DecodeOptions::default());
    assert_eq!(decoded.len(), cases.len());
    let mut offset = 0;
    for case in cases {
        let bytes = record(case);
        for end in 0..bytes.len() {
            assert!(
                decode_one(&bytes[..end], DecodeOptions::default()).is_err(),
                "{} prefix length {end}",
                case["name"]
            );
        }
        offset += bytes.len();
    }
    assert_eq!(offset, joined.len());
}

#[test]
fn body_decode_options_match_java_and_reject_truncated_topics() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    for case in cases {
        let bytes = record(case);
        for option in case["decodeOptions"].as_array().unwrap() {
            let decoded = decode_one(
                &bytes,
                DecodeOptions {
                    read_body: option["readBody"].as_bool().unwrap(),
                    decompress_body: option["decompressBody"].as_bool().unwrap(),
                    set_properties_string: false,
                    verify_crc: true,
                },
            )
            .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
            .0;
            if let Some(expected_body) = option["bodyBase64"].as_str() {
                assert_eq!(
                    decoded.body_nullable().map(|body| body.to_vec()),
                    Some(
                        base64::engine::general_purpose::STANDARD
                            .decode(expected_body)
                            .unwrap()
                    ),
                    "{} readBody={} decompressBody={}",
                    case["name"],
                    option["readBody"],
                    option["decompressBody"]
                );
            } else {
                assert!(
                    decoded.body_nullable().is_none(),
                    "{} readBody={} decompressBody={}",
                    case["name"],
                    option["readBody"],
                    option["decompressBody"]
                );
            }
            assert_eq!(
                decoded.sys_flag(),
                option["sysFlag"].as_i64().unwrap() as i32
            );
        }
        let with_properties_string = decode_one(
            &bytes,
            DecodeOptions {
                set_properties_string: true,
                verify_crc: true,
                ..DecodeOptions::default()
            },
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
        .0;
        assert_eq!(
            serde_json::to_value(with_properties_string.properties()).unwrap(),
            case["decoded"]["propertiesWithString"],
            "{} propertiesString",
            case["name"]
        );
    }

    for case in cases.iter().filter(|case| {
        case["name"]
            .as_str()
            .is_some_and(|name| name.starts_with("compressed-"))
    }) {
        let compressed = record(case);
        let preserved = decode_one(
            &compressed,
            DecodeOptions {
                read_body: true,
                decompress_body: false,
                set_properties_string: false,
                verify_crc: true,
            },
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
        .0;
        assert_ne!(preserved.sys_flag() & message::COMPRESSED_FLAG, 0);
        assert_eq!(preserved.body().as_ref(), encoded_body(&compressed));
        for decompress_body in [false, true] {
            let skipped = decode_one(
                &compressed,
                DecodeOptions {
                    read_body: false,
                    decompress_body,
                    set_properties_string: false,
                    verify_crc: true,
                },
            )
            .unwrap_or_else(|error| panic!("{}: {error}", case["name"]))
            .0;
            assert!(skipped.body().is_empty());
            assert_ne!(skipped.sys_flag() & message::COMPRESSED_FLAG, 0);
        }
    }

    let topic_record = record(&cases[0]);
    let sys_flag = i32::from_be_bytes(topic_record[36..40].try_into().unwrap());
    let born_host_length = if sys_flag & message::BORNHOST_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let store_host_length = if sys_flag & message::STOREHOSTADDRESS_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let body_length_offset =
        4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + born_host_length + 8 + store_host_length + 4 + 8;
    let body_length = i32::from_be_bytes(
        topic_record[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let topic_length_offset = body_length_offset + 4 + body_length;
    let mut invalid_topic_size = topic_record;
    invalid_topic_size[topic_length_offset] = i8::MAX as u8;
    assert!(matches!(
        decode_one(&invalid_topic_size, DecodeOptions::default()),
        Err(DecodeError::Truncated { field: "topic", .. })
    ));
    let mut empty_compressed = record(
        cases
            .iter()
            .find(|case| case["name"] == "compressed-zlib-v1")
            .unwrap(),
    );
    let body_length_offset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8;
    let body_length = i32::from_be_bytes(
        empty_compressed[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    empty_compressed.drain(body_length_offset + 4..body_length_offset + 4 + body_length);
    let empty_record_size = empty_compressed.len() as i32;
    empty_compressed[0..4].copy_from_slice(&empty_record_size.to_be_bytes());
    empty_compressed[body_length_offset..body_length_offset + 4]
        .copy_from_slice(&0_i32.to_be_bytes());
    empty_compressed[8..12].copy_from_slice(&123_i32.to_be_bytes());
    let decoded_empty = decode_one(
        &empty_compressed,
        DecodeOptions {
            verify_crc: true,
            ..DecodeOptions::default()
        },
    )
    .unwrap()
    .0;
    assert!(decoded_empty.body().is_empty());
    assert_ne!(decoded_empty.sys_flag() & message::COMPRESSED_FLAG, 0);
}

#[test]
fn malformed_magic_lengths_and_crc_fail_as_structured_errors() {
    let case = &fixture()["cases"][0];
    let valid = record(case);
    for end in 0..valid.len() {
        assert!(decode_one(&valid[..end], DecodeOptions::default()).is_err());
    }
    let mut bad_magic = valid.clone();
    bad_magic[4..8].copy_from_slice(&0_i32.to_be_bytes());
    assert!(matches!(
        decode_one(&bad_magic, DecodeOptions::default()),
        Err(DecodeError::UnknownMagic { .. })
    ));
    for declared_store_size in [0, -1, i32::MAX] {
        let mut malformed_store_size = valid.clone();
        malformed_store_size[..4].copy_from_slice(&declared_store_size.to_be_bytes());
        let (decoded, consumed) =
            decode_one(&malformed_store_size, DecodeOptions::default()).unwrap();
        assert_eq!(decoded.store_size(), declared_store_size);
        assert_eq!(consumed, valid.len());
    }
    let mut bad_body_length = valid.clone();
    let body_length_offset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8;
    bad_body_length[body_length_offset..body_length_offset + 4]
        .copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(decode_one(&bad_body_length, DecodeOptions::default()).is_err());
    let mut bad_properties_length = valid.clone();
    let body_length_offset = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8;
    let body_length = i32::from_be_bytes(
        valid[body_length_offset..body_length_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let topic_length_offset = body_length_offset + 4 + body_length;
    let topic_length = valid[topic_length_offset] as usize;
    let properties_length_offset = topic_length_offset + 1 + topic_length;
    bad_properties_length[properties_length_offset..properties_length_offset + 2]
        .copy_from_slice(&(-1_i16).to_be_bytes());
    let (decoded_without_properties, consumed) =
        decode_one(&bad_properties_length, DecodeOptions::default()).unwrap();
    assert!(decoded_without_properties.properties_nullable().is_none());
    assert_eq!(consumed, properties_length_offset + 2);
    let compressed = record(
        fixture()["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "compressed-zstd-v1")
            .unwrap(),
    );
    let mut unknown_compression = compressed;
    let sys_flag = i32::from_be_bytes(unknown_compression[36..40].try_into().unwrap());
    unknown_compression[36..40].copy_from_slice(
        &((sys_flag & !message::COMPRESSION_TYPE_COMPARATOR) | (4 << 8)).to_be_bytes(),
    );
    assert!(matches!(
        decode_one(&unknown_compression, DecodeOptions::default()),
        Err(DecodeError::CompressionType(_))
    ));
    let mut bad_crc = valid.clone();
    bad_crc[8..12].copy_from_slice(&0_i32.to_be_bytes());
    assert!(matches!(
        decode_one(
            &bad_crc,
            DecodeOptions {
                read_body: true,
                decompress_body: true,
                set_properties_string: false,
                verify_crc: true
            }
        ),
        Err(DecodeError::CrcMismatch)
    ));
}
