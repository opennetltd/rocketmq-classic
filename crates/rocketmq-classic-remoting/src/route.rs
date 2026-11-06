use rocketmq_classic_protocol::codes::JavaEnumLookupError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessageQueueRouteState {
    Expired,
    ReadOnly,
    Normal,
    WriteOnly,
}

impl MessageQueueRouteState {
    pub const fn values() -> [Self; 4] {
        [Self::Expired, Self::ReadOnly, Self::Normal, Self::WriteOnly]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("Expired") => Ok(Self::Expired),
            Some("ReadOnly") => Ok(Self::ReadOnly),
            Some("Normal") => Ok(Self::Normal),
            Some("WriteOnly") => Ok(Self::WriteOnly),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.route.MessageQueueRouteState",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Expired => "Expired",
            Self::ReadOnly => "ReadOnly",
            Self::Normal => "Normal",
            Self::WriteOnly => "WriteOnly",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Expired => 0,
            Self::ReadOnly => 1,
            Self::Normal => 2,
            Self::WriteOnly => 3,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for MessageQueueRouteState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_queue_route_state_matches_invoked_java_enum() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let cases = fixture["remotingEnumMethodCases"]["MessageQueueRouteState"]
            .as_array()
            .unwrap();
        let values = MessageQueueRouteState::values();
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
        let mut mutated_values = MessageQueueRouteState::values();
        mutated_values[0] = values[values.len() - 1];
        assert_eq!(MessageQueueRouteState::values()[0], values[0]);

        for case in cases {
            match case["case"].as_str().unwrap() {
                "constant" => {
                    let name = case["name"].as_str().unwrap();
                    let value = MessageQueueRouteState::value_of_name(Some(name)).unwrap();
                    let ordinal = case["ordinal"].as_i64().unwrap() as i32;
                    assert_eq!(value.name(), name, "{case}");
                    assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                    assert_eq!(value.ordinal(), ordinal, "{case}");
                    assert_eq!(value, values[ordinal as usize], "{case}");
                    assert_eq!(
                        value.java_compare_to(values[(ordinal as usize + 1) % values.len()]),
                        case["compareNext"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                    assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                }
                "lookup" => {
                    let input = case["input"].as_str();
                    let result = MessageQueueRouteState::value_of_name(input);
                    match result {
                        Ok(value) => {
                            assert_eq!(case["outcome"], "OK", "{case}");
                            assert_eq!(case["decoded"], value.name(), "{case}");
                        }
                        Err(error) => {
                            let expected = if input.is_none() {
                                "NullPointerException"
                            } else {
                                "IllegalArgumentException"
                            };
                            assert_eq!(case["outcome"], expected, "{case}");
                            assert_eq!(case["message"], error.to_string(), "{case}");
                        }
                    }
                }
                other => panic!("unknown Java route state enum case: {other}"),
            }
        }
    }
}
