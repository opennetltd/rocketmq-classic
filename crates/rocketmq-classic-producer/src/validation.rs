use std::collections::HashMap;

use rocketmq_classic_common::{
    message::{Message, constants::PROPERTY_INNER_MULTI_DISPATCH},
    permission::PermName,
    topic::TopicValidator,
};
use rocketmq_classic_protocol::codes::response;

use crate::{ProducerConfig, ProducerError};

const GROUP_MAX_LENGTH: usize = 120;
const TOPIC_MAX_LENGTH: usize = 127;

fn java_blank(value: &str) -> bool {
    value.is_empty()
        || value.chars().all(|ch| {
            (ch.is_whitespace() || matches!(ch, '\u{001c}'..='\u{001f}'))
                && !matches!(ch, '\u{00a0}' | '\u{2007}' | '\u{202f}')
        })
}

fn java_length(value: &str) -> usize {
    value.encode_utf16().count()
}

fn illegal_topic_or_group(value: &str) -> bool {
    value
        .chars()
        .any(|ch| !matches!(ch, '%' | '|' | '-' | '_') && !ch.is_ascii_alphanumeric())
}

pub fn validate_group(group: &str) -> Result<(), ProducerError> {
    if java_blank(group) {
        return Err(ProducerError::BlankGroup);
    }
    if java_length(group) > GROUP_MAX_LENGTH {
        return Err(ProducerError::GroupTooLong);
    }
    if illegal_topic_or_group(group) {
        return Err(ProducerError::IllegalGroup);
    }
    Ok(())
}

pub fn validate_topic(topic: &str) -> Result<(), ProducerError> {
    if java_blank(topic) {
        return Err(ProducerError::BlankTopic);
    }
    if java_length(topic) > TOPIC_MAX_LENGTH {
        return Err(ProducerError::TopicTooLong);
    }
    if illegal_topic_or_group(topic) {
        return Err(ProducerError::IllegalTopic);
    }
    Ok(())
}

pub fn is_system_topic(topic: &str) -> bool {
    TopicValidator::is_system_topic(topic)
}

pub fn validate_system_topic(topic: &str) -> Result<(), ProducerError> {
    if is_system_topic(topic) {
        Err(ProducerError::SystemTopic)
    } else {
        Ok(())
    }
}

pub fn validate_send_topic(topic: &str) -> Result<(), ProducerError> {
    if TopicValidator::is_not_allowed_send_topic(topic) {
        Err(ProducerError::ForbiddenTopic)
    } else {
        Ok(())
    }
}

pub fn validate_message(
    message: Option<&Message>,
    config: &ProducerConfig,
) -> Result<(), ProducerError> {
    let message = message.ok_or(ProducerError::NullMessage)?;
    validate_topic(message.topic())?;
    // Java checkMessage calls isNotAllowedSendTopic, but does not reject every
    // topic returned by isSystemTopic.
    validate_send_topic(message.topic())?;
    if message.body().is_empty() {
        return Err(ProducerError::EmptyBody);
    }
    if message.body().len() as i64 > config.max_message_size() as i64 {
        return Err(ProducerError::BodyTooLarge {
            actual: message.body().len(),
            maximum: config.max_message_size(),
        });
    }
    if let Some(path) = message.properties().get(PROPERTY_INNER_MULTI_DISPATCH) {
        #[cfg(windows)]
        let separator = '\\';
        #[cfg(not(windows))]
        let separator = '/';
        if path.contains(separator) {
            return Err(ProducerError::InvalidMultiDispatch);
        }
    }
    Ok(())
}

/// Matches Java `Validators.checkTopicConfig` using the permission read from
/// `TopicConfig.getPerm()`.
pub fn check_topic_config(permission: i32) -> Result<(), ProducerError> {
    if PermName::is_valid(permission) {
        Ok(())
    } else {
        Err(ProducerError::ClientValidation {
            response_code: response::NO_PERMISSION,
            message: format!("topicPermission value: {permission} is invalid."),
        })
    }
}

/// Matches Java `Validators.checkBrokerConfig` for the string properties
/// exposed by Rust's map-shaped configuration input.
pub fn check_broker_config(properties: &HashMap<String, String>) -> Result<(), ProducerError> {
    let Some(value) = properties.get("brokerPermission") else {
        return Ok(());
    };
    if PermName::is_valid_string(value)? {
        Ok(())
    } else {
        Err(ProducerError::ClientValidation {
            response_code: response::NO_PERMISSION,
            message: format!("brokerPermission value: {value} is invalid."),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ProducerConfig {
        ProducerConfig::new("publishers").unwrap()
    }

    #[test]
    fn topic_and_group_validation_matches_java_lengths_and_characters() {
        assert!(matches!(
            validate_group("  "),
            Err(ProducerError::BlankGroup)
        ));
        assert!(validate_group(&"g".repeat(120)).is_ok());
        assert!(matches!(
            validate_group(&"g".repeat(121)),
            Err(ProducerError::GroupTooLong)
        ));
        assert!(validate_topic(&"t".repeat(127)).is_ok());
        assert!(matches!(
            validate_topic(&"t".repeat(128)),
            Err(ProducerError::TopicTooLong)
        ));
        assert!(matches!(
            validate_topic(&format!("bad.{}", "t".repeat(124))),
            Err(ProducerError::TopicTooLong)
        ));
        assert!(matches!(
            validate_topic(&format!("%RETRY%{}", "t".repeat(121))),
            Err(ProducerError::TopicTooLong)
        ));
        assert!(matches!(
            validate_topic("bad.topic"),
            Err(ProducerError::IllegalTopic)
        ));
        assert!(validate_system_topic("rmq_sys_custom").is_err());
        assert!(validate_send_topic("RMQ_SYS_TRACE_TOPIC").is_ok());
        assert!(validate_send_topic("SCHEDULE_TOPIC_XXXX").is_err());

        let custom = "phase16_dynamic_system_topic";
        TopicValidator::add_system_topic(custom);
        assert!(is_system_topic(custom));
        assert!(validate_system_topic(custom).is_err());
    }

    #[test]
    fn message_validation_distinguishes_null_and_empty_body_and_enforces_size() {
        let config = config();
        assert!(matches!(
            validate_message(None, &config),
            Err(ProducerError::NullMessage)
        ));
        let empty = Message::new("orders", Vec::new());
        assert!(matches!(
            validate_message(Some(&empty), &config),
            Err(ProducerError::EmptyBody)
        ));
        let max = Message::new("orders", vec![0; 4 * 1024 * 1024]);
        assert!(validate_message(Some(&max), &config).is_ok());
        let too_big = Message::new("orders", vec![0; 4 * 1024 * 1024 + 1]);
        assert!(matches!(
            validate_message(Some(&too_big), &config),
            Err(ProducerError::BodyTooLarge { .. })
        ));
    }

    #[test]
    fn runtime_message_size_limit_applies_to_later_validation() {
        let producer = crate::Producer::new(config());
        producer.set_max_message_size(4);
        let five_bytes = Message::new("orders", vec![0; 5]);
        assert!(matches!(
            validate_message(Some(&five_bytes), producer.config()),
            Err(ProducerError::BodyTooLarge {
                maximum: 4,
                actual: 5
            })
        ));

        producer.set_max_message_size(5);
        assert!(validate_message(Some(&five_bytes), producer.config()).is_ok());
    }
}
