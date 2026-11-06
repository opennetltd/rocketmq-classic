use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};

use rocketmq_classic_common::message::{
    Message, constants::*, encode_compact_messages, encode_nullable_property_entries,
};
use rocketmq_classic_core::unique_id::UniqueIdGenerator;

use crate::{ProducerConfig, ProducerError};

/// A Java-compatible message batch whose metadata uses the same accessors as
/// `Message`. Iteration preserves nullable Java elements as `Option<Message>`.
pub struct MessageBatch {
    messages: Option<Vec<Option<Message>>>,
    message: Message,
}

impl MessageBatch {
    /// Constructs a batch from an owned message vector without validation,
    /// matching Java's public `MessageBatch(List<Message>)` constructor.
    pub fn new(messages: Vec<Message>) -> Self {
        Self::new_nullable(Some(messages.into_iter().map(Some).collect()))
    }

    /// Constructs a batch from a nullable Java-style list, including nullable
    /// list elements. This preserves the constructor's deferred null failures.
    pub fn new_nullable(messages: Option<Vec<Option<Message>>>) -> Self {
        Self {
            messages,
            message: Message::default(),
        }
    }

    /// Validates the same-topic, wait-store, retry-topic, and delay rules used
    /// by Java's `MessageBatch.generateFromList`.
    pub fn generate_from_list(messages: Vec<Message>) -> Result<Self, ProducerError> {
        Self::generate_from_list_nullable(Some(messages.into_iter().map(Some).collect()))
    }

    /// Nullable adapter for Java's `generateFromList(Collection)` boundary.
    pub fn generate_from_list_nullable(
        messages: Option<Vec<Option<Message>>>,
    ) -> Result<Self, ProducerError> {
        let messages = messages.ok_or(ProducerError::BatchListNull)?;
        if messages.is_empty() {
            return Err(ProducerError::EmptyBatch);
        }

        let mut first_topic = None;
        let mut first_wait_store = None;
        for maybe_message in &messages {
            let message = maybe_message.as_ref().ok_or(ProducerError::NullMessage)?;
            validate_batch_delay(message)?;
            let topic = message
                .topic_nullable()
                .ok_or(ProducerError::BatchTopicNull)?;
            if topic.starts_with("%RETRY%") {
                return Err(ProducerError::RetryBatchMessage);
            }
            if let Some(first_topic) = first_topic.as_deref() {
                if first_topic != topic {
                    return Err(ProducerError::BatchTopicMismatch);
                }
                if first_wait_store != Some(message.is_wait_store_msg_ok()) {
                    return Err(ProducerError::BatchWaitStoreMismatch);
                }
            } else {
                first_topic = Some(topic.to_owned());
                first_wait_store = Some(message.is_wait_store_msg_ok());
            }
        }

        let mut batch = Self::new_nullable(Some(messages));
        batch.message.set_topic_nullable(first_topic);
        let _ = batch
            .message
            .set_wait_store_msg_ok(first_wait_store.expect("a validated batch is non-empty"));
        Ok(batch)
    }

    /// Encodes the compact records produced by Java's `MessageBatch.encode`.
    pub fn encode(&self) -> Result<Vec<u8>, ProducerError> {
        let messages = self
            .messages
            .as_ref()
            .ok_or(ProducerError::BatchListNull)?
            .iter()
            .map(|message| message.clone().ok_or(ProducerError::NullMessage))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(encode_compact_messages(&messages)?)
    }
}

impl IntoIterator for MessageBatch {
    type Item = Option<Message>;
    type IntoIter = std::vec::IntoIter<Option<Message>>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages
            .expect("Java MessageBatch.iterator() fails when its list is null")
            .into_iter()
    }
}

impl<'a> IntoIterator for &'a MessageBatch {
    type Item = &'a Option<Message>;
    type IntoIter = std::slice::Iter<'a, Option<Message>>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages
            .as_ref()
            .expect("Java MessageBatch.iterator() fails when its list is null")
            .iter()
    }
}

impl<'a> IntoIterator for &'a mut MessageBatch {
    type Item = &'a mut Option<Message>;
    type IntoIter = std::slice::IterMut<'a, Option<Message>>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages
            .as_mut()
            .expect("Java MessageBatch.iterator() fails when its list is null")
            .iter_mut()
    }
}

impl Deref for MessageBatch {
    type Target = Message;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl DerefMut for MessageBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
    }
}

impl PartialEq for MessageBatch {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl Eq for MessageBatch {}

impl PartialEq<Message> for MessageBatch {
    fn eq(&self, other: &Message) -> bool {
        self.message == *other
    }
}

impl PartialEq<MessageBatch> for Message {
    fn eq(&self, other: &MessageBatch) -> bool {
        *self == other.message
    }
}

impl Hash for MessageBatch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.message.hash(state);
    }
}

impl std::fmt::Display for MessageBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.message, formatter)
    }
}

pub(crate) fn encode_prepared_batch(
    messages: &[Message],
    config: &ProducerConfig,
    ids: &UniqueIdGenerator,
) -> Result<Message, ProducerError> {
    let Some(first) = messages.first() else {
        return Err(ProducerError::EmptyBatch);
    };
    let topic = first.topic();
    let wait_store_msg_ok = first.is_wait_store_msg_ok();
    let mut encoded_messages = Vec::with_capacity(messages.len());

    for message in messages {
        let mut copy = message.clone();
        ids.set_if_missing(&mut copy);
        encoded_messages.push(encode_message(&copy)?);
    }

    finish_batch(topic, wait_store_msg_ok, encoded_messages, config, ids)
}

fn finish_batch(
    topic: &str,
    wait_store_msg_ok: bool,
    encoded_messages: Vec<Vec<u8>>,
    config: &ProducerConfig,
    ids: &UniqueIdGenerator,
) -> Result<Message, ProducerError> {
    let total_size = encoded_messages.iter().try_fold(0usize, |total, encoded| {
        total
            .checked_add(encoded.len())
            .ok_or(ProducerError::BatchTooLarge {
                actual: usize::MAX,
                maximum: config.max_message_size(),
            })
    })?;

    let mut body = Vec::with_capacity(total_size);
    for encoded in encoded_messages {
        body.extend_from_slice(&encoded);
    }
    let mut batch = Message::new(topic, body);
    let _ = batch.set_wait_store_msg_ok(wait_store_msg_ok);
    ids.set_if_missing(&mut batch);
    Ok(batch)
}

fn validate_batch_delay(message: &Message) -> Result<(), ProducerError> {
    let property_positive = |property: &str, long: bool| -> Result<bool, ProducerError> {
        let Some(value) = message.properties().get(property).cloned() else {
            return Ok(false);
        };
        if long {
            value
                .parse::<i64>()
                .map(|value| value > 0)
                .map_err(|_| ProducerError::InvalidBatchDelay)
        } else {
            value
                .parse::<i32>()
                .map(|value| value > 0)
                .map_err(|_| ProducerError::InvalidBatchDelay)
        }
    };
    if property_positive(PROPERTY_DELAY_TIME_LEVEL, false)?
        || property_positive(PROPERTY_TIMER_DELAY_MS, true)?
        || property_positive(PROPERTY_TIMER_DELAY_SEC, true)?
        || property_positive(PROPERTY_TIMER_DELIVER_MS, true)?
    {
        Err(ProducerError::DelayedBatchMessage)
    } else {
        Ok(())
    }
}

fn encode_message(message: &Message) -> Result<Vec<u8>, ProducerError> {
    let body = message.body();
    let property_entries = message.nullable_property_entries().collect::<Vec<_>>();
    let properties = encode_nullable_property_entries(Some(&property_entries));
    let properties = properties.as_bytes();
    if properties.len() > i16::MAX as usize {
        return Err(ProducerError::BatchPropertiesTooLarge);
    }
    let body_len = u32::try_from(body.len()).map_err(|_| ProducerError::BatchTooLarge {
        actual: body.len(),
        maximum: i32::MAX,
    })?;
    let property_len =
        u16::try_from(properties.len()).map_err(|_| ProducerError::BatchPropertiesTooLarge)?;
    let size = 4usize
        .checked_add(4 + 4 + 4 + 4)
        .and_then(|size| size.checked_add(body.len()))
        .and_then(|size| size.checked_add(2 + properties.len()))
        .ok_or(ProducerError::BatchTooLarge {
            actual: usize::MAX,
            maximum: i32::MAX,
        })?;
    let size = u32::try_from(size).map_err(|_| ProducerError::BatchTooLarge {
        actual: size,
        maximum: i32::MAX,
    })?;

    let mut encoded = Vec::with_capacity(size as usize);
    encoded.extend_from_slice(&size.to_be_bytes());
    encoded.extend_from_slice(&0_i32.to_be_bytes()); // Java's batch MAGICCODE placeholder.
    encoded.extend_from_slice(&0_i32.to_be_bytes()); // Java's batch BODYCRC placeholder.
    encoded.extend_from_slice(&message.flag().to_be_bytes());
    encoded.extend_from_slice(&body_len.to_be_bytes());
    encoded.extend_from_slice(&body);
    encoded.extend_from_slice(&property_len.to_be_bytes());
    encoded.extend_from_slice(properties);
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use rocketmq_classic_common::message::constants::PROPERTY_DELAY_TIME_LEVEL;

    use crate::validation;

    use super::*;

    fn config() -> ProducerConfig {
        ProducerConfig::new("batch-producer").unwrap()
    }

    #[test]
    fn batch_body_matches_message_decoder_record_layout_and_does_not_mutate_inputs() {
        let config = config();
        let ids = UniqueIdGenerator::new(Some("10.10.10.111"));
        let first = Message::new("orders", b"first".to_vec());
        let second = Message::new("orders", b"second".to_vec());
        let batch = encode_prepared_batch(&[first.clone(), second.clone()], &config, &ids).unwrap();
        let batch_body = batch.body();
        assert_eq!(batch.topic(), "orders");
        assert_eq!(first.body().as_ref(), b"first");
        assert_eq!(second.body().as_ref(), b"second");
        assert!(
            !first
                .properties()
                .contains_key(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        );
        let first_record_size = u32::from_be_bytes(batch_body[0..4].try_into().unwrap()) as usize;
        assert_eq!(&batch_body[4..12], &[0; 8]);
        let first_body_size = u32::from_be_bytes(batch_body[16..20].try_into().unwrap()) as usize;
        let first_properties_size = u16::from_be_bytes(
            batch_body[20 + first_body_size..22 + first_body_size]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(
            first_record_size,
            22 + first_body_size + first_properties_size
        );
        assert_eq!(first_body_size, 5);
        let first_properties =
            std::str::from_utf8(&batch_body[22 + first_body_size..first_record_size]).unwrap();
        assert!(first_properties.contains("UNIQ_KEY"));
        assert!(first_properties.contains("WAIT"));
        let second_record_size = u32::from_be_bytes(
            batch_body[first_record_size..first_record_size + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(second_record_size, batch_body.len() - first_record_size);
    }

    #[test]
    fn encoded_batch_size_is_checked_by_send_validation() {
        let mut small = config();
        small.set_max_message_size(64);
        let ids = UniqueIdGenerator::new(Some("10.10.10.111"));
        let messages = vec![Message::new("orders", b"x".to_vec()); 2];
        let batch = encode_prepared_batch(&messages, &small, &ids).unwrap();
        assert!(batch.body().len() > small.max_message_size() as usize);
        assert!(matches!(
            validation::validate_message(Some(&batch), &small),
            Err(ProducerError::BodyTooLarge { maximum: 64, .. })
        ));
    }

    #[test]
    fn encoded_batch_size_limit_includes_inserted_unique_ids() {
        let messages = [
            Message::new("orders", b"first".to_vec()),
            Message::new("orders", b"second".to_vec()),
        ];
        let ids = UniqueIdGenerator::new(Some("10.10.10.111"));
        let baseline = encode_prepared_batch(&messages, &config(), &ids).unwrap();
        let encoded_size = baseline.body().len();

        let mut exact_limit = config();
        exact_limit.set_max_message_size(encoded_size as i32);
        let exact = encode_prepared_batch(&messages, &exact_limit, &ids).unwrap();
        assert!(validation::validate_message(Some(&exact), &exact_limit).is_ok());

        let mut one_byte_too_small = config();
        one_byte_too_small.set_max_message_size(encoded_size as i32 - 1);
        let batch = encode_prepared_batch(&messages, &one_byte_too_small, &ids).unwrap();
        assert!(matches!(
            validation::validate_message(Some(&batch), &one_byte_too_small),
            Err(ProducerError::BodyTooLarge { actual, .. }) if actual == encoded_size
        ));
    }

    #[test]
    fn batch_record_bytes_match_pinned_java_message_decoder_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let messages = [
            Message::new("orders", b"first".to_vec()),
            Message::new("orders", b"second".to_vec()),
        ];
        let mut actual = Vec::new();
        for message in &messages {
            actual.extend_from_slice(&encode_message(message).unwrap());
        }
        let mut actual_hex = String::with_capacity(actual.len() * 2);
        for byte in &actual {
            use std::fmt::Write;
            write!(&mut actual_hex, "{byte:02X}").unwrap();
        }
        assert_eq!(
            actual_hex,
            fixture["batchEncoding"]["bodyHex"].as_str().unwrap()
        );
        assert_eq!(
            actual.len(),
            fixture["batchEncoding"]["length"].as_u64().unwrap() as usize
        );

        let batch = MessageBatch::generate_from_list(messages.to_vec()).unwrap();
        assert_eq!(batch.topic(), "orders");
        assert_eq!(batch.encode().unwrap(), actual);
        assert_eq!(batch.into_iter().count(), messages.len());
    }

    #[test]
    fn message_batch_generate_from_list_matches_java_validation_order() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        assert!(matches!(
            MessageBatch::generate_from_list(Vec::new()),
            Err(ProducerError::EmptyBatch)
        ));
        assert_eq!(
            ProducerError::EmptyBatch.to_string(),
            fixture["messageBatchValidationFailures"]["empty"]["message"]
        );

        let mut delayed = Message::new("orders", b"delayed".to_vec());
        delayed.set_delay_time_level(1);
        assert!(matches!(
            MessageBatch::generate_from_list(vec![delayed]),
            Err(ProducerError::DelayedBatchMessage)
        ));
        assert_eq!(
            fixture["messageBatchValidationFailures"]["delayed"]["class"],
            "java.lang.UnsupportedOperationException"
        );
        assert_eq!(
            ProducerError::DelayedBatchMessage.to_string(),
            fixture["messageBatchValidationFailures"]["delayed"]["message"]
        );

        type DelaySetter = fn(&mut Message, i64);
        let timer_cases: [(&str, DelaySetter); 3] = [
            ("timerDelayMs", Message::set_delay_time_ms),
            ("timerDelaySec", Message::set_delay_time_sec),
            ("timerDeliverMs", Message::set_deliver_time_ms),
        ];
        for (case, set_delay) in timer_cases {
            let mut timer_delayed = Message::new("orders", b"timer-delayed".to_vec());
            set_delay(&mut timer_delayed, 1);
            let error = match MessageBatch::generate_from_list(vec![timer_delayed]) {
                Err(error) => error,
                Ok(_) => panic!("{case} must be rejected for batching"),
            };
            assert!(matches!(error, ProducerError::DelayedBatchMessage));
            let java_error = &fixture["messageBatchValidationFailures"][case];
            assert_eq!(
                java_error["class"],
                "java.lang.UnsupportedOperationException"
            );
            assert_eq!(java_error["message"], error.to_string());
        }

        assert!(matches!(
            MessageBatch::generate_from_list(vec![
                Message::new("%RETRY%workers", b"retry".to_vec()),
                Message::new("orders", b"other".to_vec()),
            ]),
            Err(ProducerError::RetryBatchMessage)
        ));
        assert_eq!(
            fixture["messageBatchValidationFailures"]["retryTopic"]["class"],
            "java.lang.UnsupportedOperationException"
        );
        assert_eq!(
            ProducerError::RetryBatchMessage.to_string(),
            fixture["messageBatchValidationFailures"]["retryTopic"]["message"]
        );

        assert!(matches!(
            MessageBatch::generate_from_list(vec![
                Message::new("orders", b"first".to_vec()),
                Message::new("payments", b"second".to_vec()),
            ]),
            Err(ProducerError::BatchTopicMismatch)
        ));
        assert_eq!(
            fixture["messageBatchValidationFailures"]["mixedTopics"]["class"],
            "java.lang.UnsupportedOperationException"
        );
        assert_eq!(
            ProducerError::BatchTopicMismatch.to_string(),
            fixture["messageBatchValidationFailures"]["mixedTopics"]["message"]
        );

        let mut wait_store_mismatch = Message::new("orders", b"second".to_vec());
        wait_store_mismatch.set_wait_store_msg_ok(false).unwrap();
        assert!(matches!(
            MessageBatch::generate_from_list(vec![
                Message::new("orders", b"first".to_vec()),
                wait_store_mismatch,
            ]),
            Err(ProducerError::BatchWaitStoreMismatch)
        ));
        assert_eq!(
            fixture["messageBatchValidationFailures"]["waitStoreMismatch"]["class"],
            "java.lang.UnsupportedOperationException"
        );
        assert_eq!(
            ProducerError::BatchWaitStoreMismatch.to_string(),
            fixture["messageBatchValidationFailures"]["waitStoreMismatch"]["message"]
        );

        let batch = MessageBatch::generate_from_list(vec![
            Message::new("orders", b"first".to_vec()),
            Message::new("orders", b"second".to_vec()),
        ])
        .unwrap();
        assert_eq!(batch.topic(), "orders");
        assert!(batch.is_wait_store_msg_ok());
    }

    #[test]
    fn message_batch_nullable_list_and_elements_match_java_failure_boundaries() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let java = &fixture["messageBatchInputSemantics"];
        assert_eq!(java["nullConstructorAccepted"], true);
        assert_eq!(
            java["nullConstructorIteratorFailure"],
            "java.lang.NullPointerException"
        );
        assert_eq!(
            java["nullConstructorEncodeFailure"],
            "java.lang.NullPointerException"
        );
        assert_eq!(java["nullElementIteratorReturnsNull"], true);
        assert_eq!(
            java["nullElementEncodeFailure"],
            "java.lang.NullPointerException"
        );
        assert_eq!(java["emptyConstructorEncodedLength"], 0);
        assert_eq!(
            java["nullGenerateFromListFailure"],
            "java.lang.IllegalArgumentException"
        );
        assert_eq!(
            java["emptyGenerateFromListFailure"],
            "java.lang.IllegalArgumentException"
        );
        assert_eq!(
            java["nullElementGenerateFromListFailure"],
            "java.lang.NullPointerException"
        );

        let null_list_for_iter = MessageBatch::new_nullable(None);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                null_list_for_iter.into_iter()
            }))
            .is_err()
        );
        let null_list = MessageBatch::new_nullable(None);
        assert!(matches!(
            null_list.encode(),
            Err(ProducerError::BatchListNull)
        ));
        assert!(matches!(
            MessageBatch::new_nullable(Some(Vec::new())).encode(),
            Ok(bytes) if bytes.is_empty()
        ));

        let null_element_for_iter = MessageBatch::new_nullable(Some(vec![None]));
        assert_eq!(null_element_for_iter.into_iter().next(), Some(None));
        let null_element = MessageBatch::new_nullable(Some(vec![None]));
        assert!(matches!(
            null_element.encode(),
            Err(ProducerError::NullMessage)
        ));
        assert!(matches!(
            MessageBatch::generate_from_list_nullable(None),
            Err(ProducerError::BatchListNull)
        ));
        assert_eq!(
            ProducerError::BatchListNull.to_string(),
            fixture["messageBatchValidationFailures"]["empty"]["message"]
        );
        assert!(matches!(
            MessageBatch::generate_from_list_nullable(Some(Vec::new())),
            Err(ProducerError::EmptyBatch)
        ));
        assert!(matches!(
            MessageBatch::generate_from_list_nullable(Some(vec![None])),
            Err(ProducerError::NullMessage)
        ));
    }

    #[test]
    fn message_batch_public_api_matches_java_551() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let api = &fixture["messageBatchPublicApi"];
        assert_eq!(
            api["className"],
            "org.apache.rocketmq.common.message.MessageBatch"
        );
        assert_eq!(api["declaredPublicFields"], serde_json::json!([]));
        assert_eq!(
            api["publicConstructors"],
            serde_json::json!(["org.apache.rocketmq.common.message.MessageBatch(java.util.List)"])
        );
        assert_eq!(
            api["declaredPublicMethods"],
            serde_json::json!([
                "[B org.apache.rocketmq.common.message.MessageBatch.encode()",
                "java.util.Iterator org.apache.rocketmq.common.message.MessageBatch.iterator()",
                "org.apache.rocketmq.common.message.MessageBatch org.apache.rocketmq.common.message.MessageBatch.generateFromList(java.util.Collection)"
            ])
        );
        assert_eq!(api["declaredPublicMethodCount"], 3);
    }

    #[test]
    fn batch_message_with_null_property_key_uses_java_wire_key() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
        let expected = fixture["messagePublicApi"]["nullKeyPropertyEncoded"]
            .as_str()
            .unwrap();
        let mut message = Message::new("orders", b"body".to_vec());
        message.set_property_nullable(None, Some("value".to_owned()));

        let encoded = encode_message(&message).unwrap();
        let size = u32::from_be_bytes(encoded[..4].try_into().unwrap()) as usize;
        let body_length = u32::from_be_bytes(encoded[16..20].try_into().unwrap()) as usize;
        let properties_length = u16::from_be_bytes(
            encoded[20 + body_length..22 + body_length]
                .try_into()
                .unwrap(),
        ) as usize;
        let properties = std::str::from_utf8(&encoded[22 + body_length..size]).unwrap();
        assert_eq!(properties.len(), properties_length);
        assert!(properties.contains(expected));

        let batch =
            encode_prepared_batch(&[message], &config(), &UniqueIdGenerator::new(None)).unwrap();
        let batch_body = batch.body();
        let first_size = u32::from_be_bytes(batch_body[..4].try_into().unwrap()) as usize;
        let first_body_length = u32::from_be_bytes(batch_body[16..20].try_into().unwrap()) as usize;
        let first_properties_length = u16::from_be_bytes(
            batch_body[20 + first_body_length..22 + first_body_length]
                .try_into()
                .unwrap(),
        ) as usize;
        let first_properties =
            std::str::from_utf8(&batch_body[22 + first_body_length..first_size]).unwrap();
        assert_eq!(first_properties.len(), first_properties_length);
        assert!(first_properties.contains(expected));
    }

    #[test]
    fn batch_validation_decisions_match_pinned_java_fixtures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        for row in fixture["batchValidation"].as_array().unwrap() {
            let valid = match row["input"].as_str().unwrap() {
                "empty" => MessageBatch::generate_from_list(Vec::new()).is_ok(),
                "mixed-topics" => MessageBatch::generate_from_list(
                    [
                        Message::new("orders", b"one".to_vec()),
                        Message::new("payments", b"two".to_vec()),
                    ]
                    .to_vec(),
                )
                .is_ok(),
                "wait-store-mismatch" => {
                    let mut second = Message::new("orders", b"two".to_vec());
                    second.set_wait_store_msg_ok(false).unwrap();
                    MessageBatch::generate_from_list(vec![
                        Message::new("orders", b"one".to_vec()),
                        second,
                    ])
                    .is_ok()
                }
                "delayed" => {
                    let mut message = Message::new("orders", b"one".to_vec());
                    message
                        .set_system_property(PROPERTY_DELAY_TIME_LEVEL, "1")
                        .unwrap();
                    MessageBatch::generate_from_list(vec![message]).is_ok()
                }
                "retry-topic" => MessageBatch::generate_from_list(vec![Message::new(
                    "%RETRY%workers",
                    b"one".to_vec(),
                )])
                .is_ok(),
                "different-tags-allowed" => {
                    let mut first = Message::new("orders", b"one".to_vec());
                    let mut second = Message::new("orders", b"two".to_vec());
                    first.set_tags("one").unwrap();
                    second.set_tags("two").unwrap();
                    MessageBatch::generate_from_list(vec![first, second]).is_ok()
                }
                other => panic!("unknown Java batch fixture case: {other}"),
            };
            assert_eq!(valid, row["valid"].as_bool().unwrap(), "{row}");
        }
    }
}
