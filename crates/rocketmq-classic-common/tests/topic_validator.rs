use std::collections::BTreeSet;

use rocketmq_classic_common::topic::{TopicValidator, TopicValidatorError};
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../fixtures/namespace/topic-validator.json"
    ))
    .unwrap()
}

fn optional_string(value: &Value) -> Option<&str> {
    value.as_str()
}

#[test]
fn public_topic_validator_surface_matches_pinned_java_fixture() {
    let fixture = fixture();
    let constants = &fixture["constants"];
    assert_eq!(
        constants["AUTO_CREATE_TOPIC_KEY_TOPIC"],
        TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_SCHEDULE_TOPIC"],
        TopicValidator::RMQ_SYS_SCHEDULE_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_BENCHMARK_TOPIC"],
        TopicValidator::RMQ_SYS_BENCHMARK_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRANS_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRACE_TOPIC"],
        TopicValidator::RMQ_SYS_TRACE_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRANS_OP_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC"],
        TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_SELF_TEST_TOPIC"],
        TopicValidator::RMQ_SYS_SELF_TEST_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_OFFSET_MOVED_EVENT"],
        TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT
    );
    assert_eq!(
        constants["RMQ_SYS_ROCKSDB_OFFSET_TOPIC"],
        TopicValidator::RMQ_SYS_ROCKSDB_OFFSET_TOPIC
    );
    assert_eq!(
        constants["SYSTEM_TOPIC_PREFIX"],
        TopicValidator::SYSTEM_TOPIC_PREFIX
    );
    assert_eq!(
        constants["SYNC_BROKER_MEMBER_GROUP_PREFIX"],
        TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX
    );

    let valid_char_map = TopicValidator::valid_char_bit_map();
    let initial_valid_characters = valid_char_map
        .read()
        .unwrap()
        .iter()
        .enumerate()
        .filter_map(|(code, valid)| valid.then_some(char::from_u32(code as u32).unwrap()))
        .collect::<String>();
    assert_eq!(fixture["validCharacters"], initial_valid_characters);

    for case in fixture["topicCases"].as_array().unwrap() {
        let value = optional_string(&case["value"]);
        let result = TopicValidator::validate_topic(value);
        assert_eq!(
            case["valid"],
            result.is_valid(),
            "topic case {}",
            case["name"]
        );
        assert_eq!(
            optional_string(&case["remark"]),
            result.remark(),
            "topic case {}",
            case["name"]
        );
        if let Some(value) = value {
            assert_eq!(
                case["illegal"],
                TopicValidator::is_topic_or_group_illegal(value),
                "topic character case {}",
                case["name"]
            );
        }
    }

    for case in fixture["groupCases"].as_array().unwrap() {
        let value = optional_string(&case["value"]);
        let result = TopicValidator::validate_group(value);
        assert_eq!(
            case["valid"],
            result.is_valid(),
            "group case {}",
            case["name"]
        );
        assert_eq!(
            optional_string(&case["remark"]),
            result.remark(),
            "group case {}",
            case["name"]
        );
        if let Some(value) = value {
            assert_eq!(
                case["illegal"],
                TopicValidator::is_topic_or_group_illegal(value),
                "group character case {}",
                case["name"]
            );
        }
    }

    for case in fixture["validateResultConstructorCases"]
        .as_array()
        .unwrap()
    {
        let remark_is_null = case["remarkIsNull"].as_bool().unwrap();
        let remark = if remark_is_null {
            None
        } else {
            Some(case["remark"].as_str().unwrap())
        };
        let result = rocketmq_classic_common::topic::ValidateResult::new(
            case["valid"].as_bool().unwrap(),
            remark.map(str::to_owned),
        );
        assert_eq!(
            case["valid"],
            result.is_valid(),
            "ValidateResult constructor case {}",
            case["name"]
        );
        assert_eq!(
            remark,
            result.remark(),
            "ValidateResult constructor case {}",
            case["name"]
        );
    }

    let identity = &fixture["validateResultIdentity"];
    assert_eq!(identity["equalsDeclaredBy"], "java.lang.Object");
    assert_eq!(identity["hashCodeDeclaredBy"], "java.lang.Object");
    assert_eq!(identity["sameObjectEqualsItself"], true);
    assert_eq!(identity["distinctEqualFieldsEquals"], false);

    let classification = &fixture["classification"];
    let custom_system_topic = classification["customSystemTopic"].as_str().unwrap();
    let custom_blocked_topic = classification["customBlockedTopic"].as_str().unwrap();
    TopicValidator::add_system_topic(custom_system_topic);
    TopicValidator::not_allowed_send_topic_set()
        .write()
        .unwrap()
        .insert(Some(custom_blocked_topic.to_owned()));
    assert_eq!(
        classification["systemTopics"],
        serde_json::to_value(
            TopicValidator::system_topic_set()
                .read()
                .unwrap()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        )
        .unwrap()
    );
    assert_eq!(
        classification["blockedTopics"],
        serde_json::to_value(
            TopicValidator::not_allowed_send_topic_set()
                .read()
                .unwrap()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        )
        .unwrap()
    );
    assert_eq!(
        classification["customSystemTopicIsSystem"],
        TopicValidator::is_system_topic(custom_system_topic)
    );
    assert_eq!(
        classification["customSystemTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(custom_system_topic)
    );
    assert_eq!(
        classification["customBlockedTopicIsSystem"],
        TopicValidator::is_system_topic(custom_blocked_topic)
    );
    assert_eq!(
        classification["customBlockedTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(custom_blocked_topic)
    );
    assert_eq!(
        classification["traceTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(TopicValidator::RMQ_SYS_TRACE_TOPIC)
    );
    assert_eq!(
        classification["scheduleTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC)
    );

    let nullable = &fixture["nullableBehavior"];
    {
        let mut system_topics = TopicValidator::system_topic_set().write().unwrap();
        system_topics.remove(&None);
    }
    {
        let mut blocked_topics = TopicValidator::not_allowed_send_topic_set()
            .write()
            .unwrap();
        blocked_topics.remove(&None);
    }
    assert_eq!(nullable["illegalNullException"], "NullPointerException");
    assert_eq!(
        TopicValidator::is_topic_or_group_illegal_nullable(None),
        Err(TopicValidatorError::NullString)
    );
    assert_eq!(
        nullable["illegalSomeValid"],
        TopicValidator::is_topic_or_group_illegal_nullable(Some("valid-topic")).unwrap()
    );
    assert_eq!(
        nullable["illegalSomeInvalid"],
        TopicValidator::is_topic_or_group_illegal_nullable(Some("invalid.topic")).unwrap()
    );
    assert_eq!(
        nullable["systemNullBeforeAddException"],
        "NullPointerException"
    );
    assert_eq!(
        TopicValidator::is_system_topic_nullable(None),
        Err(TopicValidatorError::NullString)
    );
    assert_eq!(
        nullable["systemSomeKnownTopic"],
        TopicValidator::is_system_topic_nullable(Some("TBW102")).unwrap()
    );
    assert_eq!(
        nullable["systemSomePrefixTopic"],
        TopicValidator::is_system_topic_nullable(Some("rmq_sys_custom")).unwrap()
    );
    assert_eq!(
        nullable["systemSomeUnknownTopic"],
        TopicValidator::is_system_topic_nullable(Some("custom-topic")).unwrap()
    );
    assert_eq!(nullable["blockedNullBeforeAdd"], false);
    assert!(!TopicValidator::is_not_allowed_send_topic_nullable(None));
    assert_eq!(
        nullable["blockedSomeKnownTopic"],
        TopicValidator::is_not_allowed_send_topic_nullable(Some("SCHEDULE_TOPIC_XXXX"))
    );
    assert_eq!(
        nullable["blockedSomeUnknownTopic"],
        TopicValidator::is_not_allowed_send_topic_nullable(Some("custom-topic"))
    );

    TopicValidator::add_system_topic_nullable(None);
    assert_eq!(nullable["systemNullEntryPresentAfterAdd"], true);
    assert!(
        TopicValidator::system_topic_set()
            .read()
            .unwrap()
            .contains(&None)
    );
    assert_eq!(nullable["systemNullAfterAddIsSystem"], true);
    assert_eq!(TopicValidator::is_system_topic_nullable(None), Ok(true));

    TopicValidator::not_allowed_send_topic_set()
        .write()
        .unwrap()
        .insert(None);
    assert_eq!(nullable["blockedNullEntryPresentAfterAdd"], true);
    assert_eq!(nullable["blockedNullAfterAdd"], true);
    assert!(TopicValidator::is_not_allowed_send_topic_nullable(None));
    {
        let mut system_topics = TopicValidator::system_topic_set().write().unwrap();
        system_topics.remove(&None);
    }
    {
        let mut blocked_topics = TopicValidator::not_allowed_send_topic_set()
            .write()
            .unwrap();
        blocked_topics.remove(&None);
    }

    let mutation = &fixture["validCharMutation"];
    let tilde_was_valid = mutation["wasInitiallyValid"].as_bool().unwrap();
    {
        let mut map = valid_char_map.write().unwrap();
        map[b'~' as usize] = true;
    }
    assert_eq!(
        mutation["illegalAfterAllowing"],
        TopicValidator::is_topic_or_group_illegal("custom~")
    );
    assert_eq!(
        mutation["topicValidationAfterAllowing"],
        TopicValidator::validate_topic(Some("custom~")).is_valid()
    );
    {
        let mut map = valid_char_map.write().unwrap();
        map[b'~' as usize] = tilde_was_valid;
    }
    assert_eq!(mutation["restored"], true);
}
