use rocketmq_classic_common::message::{
    MessageExt, MessageUtilsError, constants::PROPERTY_SHARDING_KEY, delete_property,
    get_sharding_key_index, get_sharding_key_index_by_msg, get_sharding_key_indexes,
};
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../fixtures/messages/message-utils.json"
    ))
    .unwrap()
}

#[test]
fn message_utils_match_pinned_java_551_fixture() {
    let fixture = fixture();
    assert_eq!(
        fixture["artifact"].as_str(),
        Some("org.apache.rocketmq:rocketmq-client:5.5.1")
    );
    assert_eq!(
        fixture["sourceCommit"].as_str(),
        Some("efa1be59d6fc747ad220f036c86030f05f0419f9")
    );

    let public_methods = fixture["publicMethods"].as_array().unwrap();
    assert_eq!(public_methods.len(), 4);
    for method in [
        "getShardingKeyIndex",
        "getShardingKeyIndexByMsg",
        "getShardingKeyIndexes",
        "deleteProperty",
    ] {
        assert!(
            public_methods
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(method)),
            "missing Java public method {method}"
        );
    }

    for case in fixture["shardingIndices"].as_array().unwrap() {
        let key = case["key"].as_str().unwrap();
        let index_size = case["indexSize"].as_i64().unwrap() as i32;
        if let Some(error) = case["error"].as_str() {
            assert_eq!(error, "ArithmeticException");
            assert_eq!(
                get_sharding_key_index(key, index_size),
                Err(MessageUtilsError::ZeroIndexSize)
            );
        } else {
            assert_eq!(
                get_sharding_key_index(key, index_size).unwrap(),
                case["index"].as_i64().unwrap() as i32,
                "key {key:?}, index size {index_size}"
            );
        }
    }

    for case in fixture["messageShardingIndices"].as_array().unwrap() {
        let mut message = MessageExt::new();
        if case["name"] != "missing" {
            message.set_property_nullable(
                Some(PROPERTY_SHARDING_KEY.to_owned()),
                case["key"].as_str().map(str::to_owned),
            );
        }
        assert_eq!(
            get_sharding_key_index_by_msg(&message, case["indexSize"].as_i64().unwrap() as i32)
                .unwrap(),
            case["index"].as_i64().unwrap() as i32,
            "message case {}",
            case["name"]
        );
    }

    for case in fixture["distinctMessageIndexes"].as_array().unwrap() {
        let messages = case["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|key| {
                let mut message = MessageExt::new();
                message.set_property_nullable(
                    Some(PROPERTY_SHARDING_KEY.to_owned()),
                    key.as_str().map(str::to_owned),
                );
                message
            })
            .collect::<Vec<_>>();
        let index_size = case["indexSize"].as_i64().unwrap() as i32;
        let mut indexes = get_sharding_key_indexes(messages.iter(), index_size)
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        indexes.sort_unstable();
        let expected = case["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|index| index.as_i64().unwrap() as i32)
            .collect::<Vec<_>>();
        assert_eq!(indexes, expected, "message set case {}", case["name"]);
    }

    for case in fixture["deletePropertyCases"].as_array().unwrap() {
        let properties = case["properties"].as_str();
        let property_name = case["propertyName"].as_str().unwrap();
        assert_eq!(
            delete_property(properties, property_name),
            case["result"].as_str().map(str::to_owned),
            "delete-property case {}",
            case["name"]
        );
    }
}

#[test]
fn delete_property_handles_empty_name_without_unbounded_loop() {
    assert_eq!(
        delete_property(Some("a\u{1}1\u{2}"), ""),
        Some("a\u{1}1\u{2}".into())
    );
}
