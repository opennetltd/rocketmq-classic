use rocketmq_classic_protocol::{
    body::{ConsumeMessageResult, LiteSubscriptionAction, MessageRequestMode, OffsetOptionType},
    heartbeat::{ConsumeFromWhere, ConsumeType, MessageModel},
};

#[test]
fn protocol_enum_methods_match_invoked_java_values() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let groups = &fixture["protocolEnumMethodCases"];

    macro_rules! assert_enum_cases {
        ($enum_type:ty, $cases:expr) => {{
            let cases = $cases.as_array().unwrap();
            let values = <$enum_type>::values();
            let expected_names = cases
                .iter()
                .find(|case| case["case"] == "constant")
                .unwrap()["values"]
                .as_array()
                .unwrap()
                .iter()
                .map(|name| name.as_str().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                values.iter().map(|value| value.name()).collect::<Vec<_>>(),
                expected_names
            );
            let mut mutated_values = <$enum_type>::values();
            mutated_values[0] = values[values.len() - 1];
            assert_eq!(<$enum_type>::values()[0], values[0]);

            for case in cases {
                match case["case"].as_str().unwrap() {
                    "constant" => {
                        let name = case["name"].as_str().unwrap();
                        let value = <$enum_type>::value_of_name(Some(name)).unwrap();
                        let ordinal = case["ordinal"].as_i64().unwrap() as i32;
                        assert_eq!(value.name(), name, "{case}");
                        assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                        assert_eq!(value.ordinal(), ordinal, "{case}");
                        assert_eq!(value, values[ordinal as usize], "{case}");
                        assert_eq!(case["valueOfName"], name, "{case}");
                        assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                        let next = values[((ordinal as usize) + 1) % values.len()];
                        assert_eq!(
                            value.java_compare_to(&next),
                            case["compareNext"].as_i64().unwrap() as i32,
                            "{case}"
                        );
                    }
                    "lookup" => {
                        let result = <$enum_type>::value_of_name(case["input"].as_str());
                        match result {
                            Ok(value) => {
                                assert_eq!(case["outcome"], "OK", "{case}");
                                assert_eq!(case["decoded"], value.name(), "{case}");
                            }
                            Err(error) => {
                                let expected = if case["input"].is_null() {
                                    "NullPointerException"
                                } else {
                                    "IllegalArgumentException"
                                };
                                assert_eq!(case["outcome"], expected, "{case}");
                                assert_eq!(case["message"], error.to_string(), "{case}");
                            }
                        }
                    }
                    other => panic!("unknown Java protocol enum case: {other}"),
                }
            }
        }};
    }

    assert_enum_cases!(MessageRequestMode, &groups["MessageRequestMode"]);
    assert_enum_cases!(LiteSubscriptionAction, &groups["LiteSubscriptionAction"]);
    assert_enum_cases!(OffsetOptionType, &groups["OffsetOptionType"]);
    assert_enum_cases!(ConsumeMessageResult, &groups["ConsumeMessageResult"]);
    assert_enum_cases!(ConsumeType, &groups["ConsumeType"]);
    assert_enum_cases!(MessageModel, &groups["MessageModel"]);
    assert_enum_cases!(ConsumeFromWhere, &groups["ConsumeFromWhere"]);

    assert_eq!(
        MessageModel::Broadcasting.cmp(&MessageModel::Clustering),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        MessageModel::Clustering.cmp(&MessageModel::LiteSelective),
        std::cmp::Ordering::Less
    );
}
