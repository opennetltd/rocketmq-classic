use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    process::Command,
    sync::Mutex,
};

use rocketmq_classic_common::{
    message::constants,
    topic::{
        LITE_EXPIRATION_ATTRIBUTE, TOPIC_MESSAGE_TYPE_ATTRIBUTE, TopicMessageType,
        TopicMessageTypeError,
    },
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        TopicConfig, TopicConfigAndQueueMapping, TopicConfigAttributeMap, TopicConfigError,
        TopicConfigFilterType, TopicQueueMappingDetail,
    },
};
use serde_json::{Value, json};

static TOPIC_CONFIG_DEFAULTS_TEST_LOCK: Mutex<()> = Mutex::new(());

fn fixture() -> Value {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    fixture["topicConfigModelOperations"].clone()
}

fn assert_config(config: &TopicConfig, expected: &Value) {
    let mut actual = serde_json::to_value(config).unwrap();
    actual["topicMessageType"] = json!(config.topic_message_type().unwrap().get_value());
    actual["liteTopicExpiration"] = json!(config.lite_topic_expiration().unwrap());
    actual["toString"] = json!(config.java_to_string());
    assert_eq!(&actual, expected);
}

fn assert_config_fields(config: &TopicConfig, expected: &Value) {
    let mut actual = serde_json::to_value(config).unwrap();
    actual["topicMessageType"] = json!(config.topic_message_type().unwrap().get_value());
    actual["liteTopicExpiration"] = json!(config.lite_topic_expiration().unwrap());
    actual.as_object_mut().unwrap().remove("toString");
    let mut expected = expected.clone();
    expected.as_object_mut().unwrap().remove("toString");
    assert_eq!(actual, expected);
}

fn attributes(entries: &[(&str, Option<&str>)]) -> TopicConfigAttributeMap {
    entries
        .iter()
        .map(|(key, value)| (Some((*key).to_owned()), value.map(str::to_owned)))
        .collect()
}

fn nullable_attributes(entries: &[(Option<&str>, Option<&str>)]) -> TopicConfigAttributeMap {
    entries
        .iter()
        .map(|(key, value)| (key.map(str::to_owned), value.map(str::to_owned)))
        .collect()
}

#[test]
fn topic_config_methods_and_type_helpers_match_invoked_java_551() {
    let _defaults_lock = TOPIC_CONFIG_DEFAULTS_TEST_LOCK.lock().unwrap();
    let fixture = fixture();
    assert_config(&TopicConfig::default(), &fixture["default"]);
    assert_eq!(
        TopicConfig::default().encode().unwrap(),
        fixture["defaultEncoded"].as_str().unwrap()
    );

    let constructors = [
        TopicConfig::new("orders"),
        TopicConfig::with_queue_counts("orders", 2, 3),
        TopicConfig::with_perm("orders", 2, 3, 7),
        TopicConfig::with_topic_sys_flag("orders", 2, 3, 7, 11),
    ];
    for (config, expected) in constructors
        .iter()
        .zip(fixture["constructors"].as_array().unwrap())
    {
        assert_config(config, expected);
    }

    let mut setter_state = TopicConfig::default();
    setter_state.set_topic_name(Some("setter-topic".to_owned()));
    setter_state.set_read_queue_nums(31);
    setter_state.set_write_queue_nums(32);
    setter_state.set_perm(7);
    setter_state.set_topic_filter_type(Some(TopicConfigFilterType::MultiTag));
    setter_state.set_topic_sys_flag(33);
    setter_state.set_order(true);
    setter_state.set_attributes(Some(attributes(&[(
        TOPIC_MESSAGE_TYPE_ATTRIBUTE,
        Some("FIFO"),
    )])));
    assert_config(&setter_state, &fixture["setterState"]);

    let previous_read = TopicConfig::default_read_queue_nums();
    let previous_write = TopicConfig::default_write_queue_nums();
    TopicConfig::set_default_read_queue_nums(27);
    TopicConfig::set_default_write_queue_nums(29);
    let mutable_defaults = TopicConfig::new("dynamic");
    assert_config(&mutable_defaults, &fixture["mutableStaticDefaults"]);
    TopicConfig::set_default_read_queue_nums(previous_read);
    TopicConfig::set_default_write_queue_nums(previous_write);

    let shared_attributes = SharedValue::new(attributes(&[
        (LITE_EXPIRATION_ATTRIBUTE, Some("17")),
        (TOPIC_MESSAGE_TYPE_ATTRIBUTE, Some("NORMAL")),
    ]));
    let mut lite = TopicConfig::with_topic_sys_flag("lite-orders", 4, 5, 6, 8);
    lite.set_attributes_shared(Some(shared_attributes.clone()));
    lite.set_topic_message_type(Some(TopicMessageType::Lite))
        .unwrap();
    lite.set_lite_topic_expiration(23).unwrap();
    assert_config(&lite, &fixture["lite"]);
    assert_eq!(
        lite.encode().unwrap(),
        fixture["liteEncoded"].as_str().unwrap()
    );

    let mut collision_attributes = TopicConfigAttributeMap::new();
    collision_attributes.insert(Some("BB".into()), Some("second".into()));
    collision_attributes.insert(Some("Aa".into()), Some("first".into()));
    let mut collision = TopicConfig::new("collision");
    collision.set_attributes(Some(collision_attributes));
    assert_eq!(
        collision.encode().unwrap(),
        fixture["hashCollisionEncoded"].as_str().unwrap()
    );
    assert_eq!(
        collision.java_to_string(),
        fixture["hashCollisionToString"].as_str().unwrap()
    );

    let mut reverse_collision_attributes = TopicConfigAttributeMap::new();
    reverse_collision_attributes.insert(Some("Aa".into()), Some("first".into()));
    reverse_collision_attributes.insert(Some("BB".into()), Some("second".into()));
    let mut reverse_collision = TopicConfig::new("collision");
    reverse_collision.set_attributes(Some(reverse_collision_attributes));
    assert_eq!(
        reverse_collision.encode().unwrap(),
        fixture["hashCollisionReverseEncoded"].as_str().unwrap()
    );
    assert_eq!(collision, reverse_collision);
    let mut collision_hash = DefaultHasher::new();
    collision.hash(&mut collision_hash);
    let mut reverse_collision_hash = DefaultHasher::new();
    reverse_collision.hash(&mut reverse_collision_hash);
    assert_eq!(collision_hash.finish(), reverse_collision_hash.finish());

    let mut collision_decoded = TopicConfig::new("before");
    assert!(
        collision_decoded
            .decode(fixture["hashCollisionDecodeInput"].as_str().unwrap())
            .unwrap()
    );
    assert_eq!(
        collision_decoded.encode().unwrap(),
        fixture["hashCollisionEncoded"].as_str().unwrap()
    );

    let copied = TopicConfig::copy_from(&lite);
    assert_eq!(lite, copied);
    assert_eq!(fixture["copyEquals"], true);
    assert_eq!(fixture["copyHashEqual"], true);
    let mut lite_hash = DefaultHasher::new();
    lite.hash(&mut lite_hash);
    let mut copied_hash = DefaultHasher::new();
    copied.hash(&mut copied_hash);
    assert_eq!(lite_hash.finish(), copied_hash.finish());
    copied.attributes().unwrap().with_mut(|values| {
        values.insert(
            Some(LITE_EXPIRATION_ATTRIBUTE.to_owned()),
            Some("29".to_owned()),
        );
    });
    assert_eq!(lite.lite_topic_expiration().unwrap(), 29);
    assert_eq!(fixture["copyRetainsAttributeAlias"], true);

    let mut decoded = TopicConfig::new("before");
    decoded.set_read_queue_nums(40);
    assert_eq!(
        decoded
            .decode(fixture["decodeInput"].as_str().unwrap())
            .unwrap(),
        fixture["decodeResult"]
    );
    assert_config(&decoded, &fixture["decoded"]);
    assert_eq!(
        decoded.encode().unwrap(),
        fixture["decodedEncoded"].as_str().unwrap()
    );

    let mut trailing = TopicConfig::new("retained");
    assert_eq!(
        trailing
            .decode(fixture["trailingInput"].as_str().unwrap())
            .unwrap(),
        fixture["trailingResult"]
    );
    assert_config(&trailing, &fixture["trailingState"]);

    let mut unicode_numbers = TopicConfig::new("retained");
    assert_eq!(
        unicode_numbers
            .decode(fixture["unicodeNumberInput"].as_str().unwrap())
            .unwrap(),
        fixture["unicodeNumberResult"]
    );
    assert_config(&unicode_numbers, &fixture["unicodeNumberState"]);

    let mut invalid_filter = TopicConfig::new("retained");
    assert!(matches!(
        invalid_filter.decode(fixture["invalidFilterInput"].as_str().unwrap()),
        Err(TopicConfigError::InvalidFilterType(value)) if value == "bad-filter"
    ));
    assert_eq!(fixture["invalidFilterError"], "IllegalArgumentException");
    assert_config(&invalid_filter, &fixture["invalidFilterState"]);

    let mut too_short = TopicConfig::new("retained");
    assert_eq!(
        too_short
            .decode(fixture["tooShortInput"].as_str().unwrap())
            .unwrap(),
        fixture["tooShortResult"]
    );
    assert_config(&too_short, &fixture["tooShortState"]);

    let mut malformed_attributes = TopicConfig::new("retained");
    malformed_attributes.set_attributes(Some(attributes(&[(
        TOPIC_MESSAGE_TYPE_ATTRIBUTE,
        Some("LITE"),
    )])));
    assert_eq!(
        malformed_attributes
            .decode(fixture["malformedAttributesInput"].as_str().unwrap())
            .unwrap(),
        fixture["malformedAttributesResult"]
    );
    assert_config(&malformed_attributes, &fixture["malformedAttributesState"]);

    let mut partial = TopicConfig::new("retained");
    partial.set_read_queue_nums(90);
    assert!(matches!(
        partial.decode(fixture["partialDecodeInput"].as_str().unwrap()),
        Err(TopicConfigError::InvalidInteger { .. })
    ));
    assert_eq!(fixture["partialDecodeError"], "NumberFormatException");
    assert_config(&partial, &fixture["partialDecodeState"]);

    let mut unknown_type = TopicConfig::new("orders");
    unknown_type.set_attributes(Some(attributes(&[(
        TOPIC_MESSAGE_TYPE_ATTRIBUTE,
        Some("unknown"),
    )])));
    assert!(matches!(
        unknown_type.topic_message_type(),
        Err(TopicConfigError::InvalidTopicMessageType(
            TopicMessageTypeError::UnknownName(value)
        )) if value == "unknown"
    ));
    assert_eq!(
        fixture["unknownMessageTypeError"],
        "IllegalArgumentException"
    );

    let mut null_attributes = TopicConfig::new("orders");
    null_attributes.set_attributes(None);
    assert_eq!(
        null_attributes.topic_message_type().unwrap(),
        TopicMessageType::Normal
    );
    assert_eq!(null_attributes.lite_topic_expiration().unwrap(), -1);
    assert_eq!(fixture["nullAttributesMessageType"], "NORMAL");
    assert_eq!(fixture["nullAttributesLiteExpiration"], -1);
    assert!(matches!(
        null_attributes.set_topic_message_type(None),
        Err(TopicConfigError::NullTopicMessageType)
    ));
    assert!(matches!(
        TopicConfig::decode_nullable(None),
        Err(TopicConfigError::NullInput)
    ));
    assert_eq!(fixture["nullDecodeError"], "NullPointerException");

    let mut invalid_expiration = TopicConfig::new("orders");
    invalid_expiration.set_attributes(Some(attributes(&[
        (TOPIC_MESSAGE_TYPE_ATTRIBUTE, Some("LITE")),
        (LITE_EXPIRATION_ATTRIBUTE, Some("not-an-int")),
    ])));
    assert_config(&invalid_expiration, &fixture["invalidExpiration"]);
}

#[test]
fn topic_config_nullable_attribute_keys_match_invoked_java_551() {
    let _defaults_lock = TOPIC_CONFIG_DEFAULTS_TEST_LOCK.lock().unwrap();
    let expected = fixture()["nullableAttributeApi"].clone();
    let shared_attributes = SharedValue::new(nullable_attributes(&[
        (None, Some("null-key")),
        (Some("null"), Some("string-null-key")),
        (Some("nullable-value"), None),
    ]));
    let mut config = TopicConfig::new("nullable-attributes");
    config.set_attributes_shared(Some(shared_attributes.clone()));

    let getter_attributes = config.attributes().unwrap();
    assert_eq!(
        shared_attributes.ptr_eq(&getter_attributes),
        expected["setterKeepsMapIdentity"]
    );
    assert_eq!(
        shared_attributes.ptr_eq(&getter_attributes),
        expected["getterKeepsMapIdentity"]
    );
    getter_attributes.with_mut(|values| {
        values.insert(
            Some("from-getter".to_owned()),
            Some("getter-mutation".to_owned()),
        );
    });
    shared_attributes.with_mut(|values| {
        values.insert(
            Some("from-caller".to_owned()),
            Some("caller-mutation".to_owned()),
        );
    });
    assert_eq!(
        config
            .attributes()
            .unwrap()
            .with(|values| values.contains_key(&Some("from-caller".to_owned()))),
        expected["mutationsVisibleThroughGetter"]
    );
    assert_eq!(
        shared_attributes.with(|values| values.contains_key(&Some("from-getter".to_owned()))),
        expected["mutationsVisibleThroughCaller"]
    );

    let mut entries = shared_attributes.with(|values| {
        values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    });
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    let actual_entries = entries
        .into_iter()
        .map(|(key, value)| {
            json!({
                "keyIsNull": key.is_none(),
                "key": key.unwrap_or_default(),
                "valueIsNull": value.is_none(),
                "value": value.unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(json!(actual_entries), expected["entries"]);

    let encoded = config.encode().unwrap();
    assert_eq!(encoded, expected["encoded"]);
    let attributes_json = expected["attributesJson"].as_str().unwrap();
    assert!(encoded.ends_with(attributes_json));
    let dto_json = serde_json::to_string(&config).unwrap();
    assert!(dto_json.contains(&format!("\"attributes\":{attributes_json},")));
    assert_eq!(config.java_to_string(), expected["toString"]);

    let mut decoded = TopicConfig::new("before");
    assert_eq!(
        decoded
            .decode(expected["decodeInput"].as_str().unwrap())
            .unwrap(),
        expected["decodeResult"]
    );
    let decoded_attributes = decoded.attributes().unwrap();
    assert_eq!(
        decoded_attributes.with(|values| values.contains_key(&Some("null".to_owned()))),
        expected["decodedStringNullKeyPresent"]
    );
    assert_eq!(
        decoded_attributes.with(|values| values.contains_key(&None)),
        expected["decodedJavaNullKeyPresent"]
    );
    assert_eq!(decoded.encode().unwrap(), expected["decodedEncoded"]);
}

#[test]
fn topic_config_and_queue_mapping_constructor_and_methods_match_java_551() {
    let _defaults_lock = TOPIC_CONFIG_DEFAULTS_TEST_LOCK.lock().unwrap();
    let fixture = fixture()["topicConfigAndQueueMappingApi"].clone();
    let api: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let api = &api["topicConfigAndQueueMappingApi"];
    assert_eq!(api["declaredPublicMethodCount"], 5);
    assert_eq!(api["publicMethodCount"], 27);
    assert_eq!(
        api["publicConstructors"],
        json!([
            "org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping()",
            "org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping(org.apache.rocketmq.common.TopicConfig,org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail)"
        ])
    );
    for method in [
        "equals",
        "hashCode",
        "toString",
        "getMappingDetail",
        "setMappingDetail",
    ] {
        assert!(
            api["declaredPublicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|signature| signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))),
            "pinned TopicConfigAndQueueMapping lacks {method}"
        );
    }

    let defaults = TopicConfigAndQueueMapping::default();
    assert_config_fields(defaults.topic_config(), &fixture["default"]["topicConfig"]);
    assert!(defaults.get_mapping_detail().is_none());
    assert_eq!(
        defaults.java_to_string(),
        fixture["default"]["toString"].as_str().unwrap()
    );

    let mut null_fields_config = TopicConfig::default();
    null_fields_config.set_topic_name(None);
    null_fields_config.set_topic_filter_type(None);
    null_fields_config.set_attributes(None);
    let null_fields_wrapper = TopicConfigAndQueueMapping::new(&null_fields_config, None);
    assert_config_fields(
        null_fields_wrapper.topic_config(),
        &fixture["nullFields"]["topicConfig"],
    );
    assert_eq!(
        null_fields_wrapper.java_to_string(),
        fixture["nullFields"]["toString"].as_str().unwrap()
    );

    let inherited_config = TopicConfig::with_topic_sys_flag("inherited-before", 1, 2, 3, 4);
    let mut inherited_mapping_detail: TopicQueueMappingDetail =
        serde_json::from_value(fixture["detailBefore"].clone()).unwrap();
    inherited_mapping_detail.set_topic(Some("inherited-mapping".to_owned()));
    let mut inherited_wrapper = TopicConfigAndQueueMapping::new(
        &inherited_config,
        Some(SharedValue::new(inherited_mapping_detail)),
    );
    inherited_wrapper.set_topic_name(Some("inherited-after".to_owned()));
    inherited_wrapper.set_read_queue_nums(41);
    inherited_wrapper.set_write_queue_nums(42);
    inherited_wrapper.set_perm(5);
    inherited_wrapper.set_topic_filter_type(Some(TopicConfigFilterType::MultiTag));
    inherited_wrapper.set_topic_sys_flag(43);
    inherited_wrapper.set_order(true);
    assert_config_fields(
        inherited_wrapper.topic_config(),
        &fixture["inheritedSetters"]["topicConfig"],
    );
    assert_eq!(
        inherited_wrapper.java_to_string(),
        fixture["inheritedSetters"]["toString"].as_str().unwrap()
    );

    let mut source = TopicConfig::with_topic_sys_flag("source-topic", 3, 4, 6, 7);
    let shared_attributes = SharedValue::new(attributes(&[("owner", Some("initial"))]));
    source.set_attributes_shared(Some(shared_attributes.clone()));
    let mapping_detail: TopicQueueMappingDetail =
        serde_json::from_value(fixture["detailBefore"].clone()).unwrap();
    let mapping_detail = SharedValue::new(mapping_detail);
    let mut wrapper = TopicConfigAndQueueMapping::new(&source, Some(mapping_detail.clone()));

    assert_config_fields(
        wrapper.topic_config(),
        &fixture["constructed"]["topicConfig"],
    );
    assert_eq!(
        serde_json::to_value(mapping_detail.snapshot()).unwrap(),
        fixture["constructed"]["mappingDetail"]
    );
    assert_eq!(
        wrapper.java_to_string(),
        fixture["constructed"]["toString"].as_str().unwrap()
    );
    assert_eq!(
        wrapper
            .get_mapping_detail()
            .unwrap()
            .ptr_eq(&mapping_detail),
        fixture["getterPreservesMappingIdentity"]
    );

    source.set_topic_name(Some("source-mutated".to_owned()));
    source.set_read_queue_nums(99);
    shared_attributes.with_mut(|values| {
        values.insert(Some("owner".to_owned()), Some("updated".to_owned()));
    });
    assert_config_fields(
        wrapper.topic_config(),
        &fixture["afterSourceMutation"]["topicConfig"],
    );
    assert_eq!(
        wrapper.java_to_string(),
        fixture["afterSourceMutation"]["toString"].as_str().unwrap()
    );

    mapping_detail.with_mut(|detail| detail.set_topic(Some("mapping-mutated".to_owned())));
    assert_eq!(
        wrapper.java_to_string(),
        fixture["afterMappingMutation"]["toString"]
            .as_str()
            .unwrap()
    );

    let replacement = SharedValue::new(TopicQueueMappingDetail::new(
        Some("replacement".to_owned()),
        1,
        Some("broker-2".to_owned()),
        1,
    ));
    wrapper.set_mapping_detail(Some(replacement.clone()));
    assert_eq!(
        wrapper.get_mapping_detail().unwrap().ptr_eq(&replacement),
        fixture["setterPreservesMappingIdentity"]
    );
    wrapper.set_mapping_detail(None);
    assert_eq!(
        wrapper.get_mapping_detail().is_none(),
        fixture["setterAcceptsNull"]
    );

    assert!(matches!(
        TopicConfigAndQueueMapping::new_nullable(None, None),
        Err(TopicConfigError::NullInput)
    ));
    assert_eq!(
        fixture["nullConfigConstructorError"],
        "NullPointerException"
    );

    for case in fixture["equality"].as_array().unwrap() {
        let left: TopicConfigAndQueueMapping =
            serde_json::from_value(case["left"].clone()).unwrap();
        let right: TopicConfigAndQueueMapping =
            serde_json::from_value(case["right"].clone()).unwrap();
        let expected_equals = case["equals"].as_bool().unwrap();
        assert_eq!(left == right, expected_equals, "{}", case["case"]);
        assert_eq!(
            left.java_to_string(),
            case["leftToString"].as_str().unwrap(),
            "{} left text",
            case["case"]
        );
        assert_eq!(
            right.java_to_string(),
            case["rightToString"].as_str().unwrap(),
            "{} right text",
            case["case"]
        );
        if expected_equals {
            assert_eq!(case["hashCodeEqual"], true, "{}", case["case"]);
            let mut left_hash = DefaultHasher::new();
            left.hash(&mut left_hash);
            let mut right_hash = DefaultHasher::new();
            right.hash(&mut right_hash);
            assert_eq!(left_hash.finish(), right_hash.finish(), "{}", case["case"]);
        }
    }
}

#[test]
fn topic_message_type_precedence_and_enum_surface_match_java() {
    let fixture = fixture();
    assert_eq!(
        TopicMessageType::values()
            .iter()
            .map(|value| value.get_value())
            .collect::<Vec<_>>(),
        fixture["messageTypeValues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        TopicMessageType::Lite.metrics_value(),
        fixture["messageTypeMetrics"].as_str().unwrap()
    );

    let mut transaction = HashMap::from([
        (
            constants::PROPERTY_TRANSACTION_PREPARED.to_owned(),
            Some("TrUe".to_owned()),
        ),
        (
            constants::PROPERTY_DELAY_TIME_LEVEL.to_owned(),
            Some("1".to_owned()),
        ),
    ]);
    let precedence = &fixture["messageTypePrecedence"];
    assert_eq!(
        TopicMessageType::parse_from_message_property(Some(&transaction))
            .unwrap()
            .get_value(),
        precedence["transaction"]
    );
    transaction.remove(constants::PROPERTY_TRANSACTION_PREPARED);
    assert_eq!(
        TopicMessageType::parse_from_message_property(Some(&transaction))
            .unwrap()
            .get_value(),
        "DELAY"
    );
    let fifo_and_priority = HashMap::from([
        (
            constants::PROPERTY_SHARDING_KEY.to_owned(),
            Some("key".to_owned()),
        ),
        (
            constants::PROPERTY_PRIORITY.to_owned(),
            Some("1".to_owned()),
        ),
    ]);
    assert_eq!(
        TopicMessageType::parse_from_message_property(Some(&fifo_and_priority))
            .unwrap()
            .get_value(),
        precedence["fifo"]
    );
    assert_eq!(
        TopicMessageType::parse_from_message_property(Some(&HashMap::from([(
            constants::PROPERTY_LITE_TOPIC.to_owned(),
            Some("orders".to_owned()),
        )])))
        .unwrap()
        .get_value(),
        precedence["lite"]
    );
    assert_eq!(
        TopicMessageType::parse_from_message_property(Some(&HashMap::new()))
            .unwrap()
            .get_value(),
        precedence["normal"]
    );
    let null_property = HashMap::from([
        (constants::PROPERTY_TRANSACTION_PREPARED.to_owned(), None),
        (constants::PROPERTY_DELAY_TIME_LEVEL.to_owned(), None),
    ]);
    assert_eq!(
        TopicMessageType::parse_from_message_property(Some(&null_property))
            .unwrap()
            .get_value(),
        precedence["nullDelay"]
    );
    assert!(matches!(
        TopicMessageType::parse_from_message_property(None),
        Err(TopicMessageTypeError::NullMessageProperties)
    ));

    let topic_config_api =
        &serde_json::from_str::<Value>(include_str!("../../../fixtures/producer/core.json"))
            .unwrap()["staticTopicConfigApi"];
    let methods = topic_config_api["declaredPublicMethods"]
        .as_array()
        .unwrap();
    for method in [
        "getTopicName",
        "setTopicName",
        "getReadQueueNums",
        "setReadQueueNums",
        "getWriteQueueNums",
        "setWriteQueueNums",
        "getPerm",
        "setPerm",
        "getTopicFilterType",
        "setTopicFilterType",
        "getTopicSysFlag",
        "setTopicSysFlag",
        "isOrder",
        "setOrder",
        "getAttributes",
        "setAttributes",
        "getTopicMessageType",
        "setTopicMessageType",
        "setLiteTopicExpiration",
        "getLiteTopicExpiration",
        "encode",
        "decode",
        "equals",
        "hashCode",
        "toString",
    ] {
        assert!(
            methods.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }),
            "pinned TopicConfig lacks {method}"
        );
    }
    let topic_message_type_api =
        &serde_json::from_str::<Value>(include_str!("../../../fixtures/producer/core.json"))
            .unwrap()["topicMessageTypeApi"];
    for method in [
        "getValue",
        "getMetricsValue",
        "topicMessageTypeSet",
        "parseFromMessageProperty",
        "values",
        "valueOf",
        "name",
        "ordinal",
    ] {
        assert!(
            topic_message_type_api["publicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{method}(")))
                }),
            "pinned TopicMessageType lacks {method}"
        );
    }
    assert_eq!(
        topic_config_api["publicConstructors"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    assert_eq!(
        serde_json::from_str::<Value>(include_str!("../../../fixtures/producer/core.json"))
            .unwrap()["staticTopicConfigPublicFields"],
        json!([
            "int org.apache.rocketmq.common.TopicConfig.defaultReadQueueNums",
            "int org.apache.rocketmq.common.TopicConfig.defaultWriteQueueNums"
        ])
    );

    let filter_values = TopicConfigFilterType::values();
    assert_eq!(
        filter_values.map(|value| value.name()),
        ["SINGLE_TAG", "MULTI_TAG"]
    );
}

#[test]
fn topic_message_type_metrics_match_java_turkish_default_locale() {
    const CHILD_MARKER: &str = "ROCKETMQ_TOPIC_MESSAGE_TYPE_TURKISH_CHILD";
    if std::env::var_os(CHILD_MARKER).is_some() {
        let fixture = fixture();
        let expected = fixture["messageTypeMetricsTurkish"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();
        let actual = TopicMessageType::values()
            .iter()
            .map(|value| value.metrics_value())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "topic_message_type_metrics_match_java_turkish_default_locale",
        ])
        .env(CHILD_MARKER, "1")
        .env("LC_ALL", "tr_TR.UTF-8")
        .env_remove("LC_CTYPE")
        .env("LANG", "C")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Turkish locale child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
