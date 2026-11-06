use rocketmq_classic_common::permission::PermName;
use serde_json::Value;

fn java_fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap()
}

#[test]
fn perm_name_numeric_methods_match_the_pinned_java_fixture() {
    let fixture = java_fixture();
    for row in fixture["permNameCases"].as_array().unwrap() {
        let permission = row["input"].as_i64().unwrap() as i32;
        assert_eq!(PermName::perm_to_string(permission), row["text"]);
        assert_eq!(PermName::is_readable(permission), row["readable"]);
        assert_eq!(PermName::is_writeable(permission), row["writeable"]);
        assert_eq!(PermName::is_inherited(permission), row["inherited"]);
        assert_eq!(PermName::is_priority(permission), row["priority"]);
        assert_eq!(PermName::is_accessible(permission), row["accessible"]);
        assert_eq!(PermName::is_valid(permission), row["valid"]);
    }
}

#[test]
fn perm_name_string_overload_matches_java_parse_and_validation() {
    let fixture = java_fixture();
    for row in fixture["permNameStringCases"].as_array().unwrap() {
        let input = row["input"].as_str().unwrap();
        if let Some(valid) = row["valid"].as_bool() {
            assert_eq!(
                PermName::is_valid_string(input).unwrap(),
                valid,
                "{input:?}"
            );
        } else {
            let error = PermName::is_valid_string(input).unwrap_err();
            assert_eq!(row["errorClass"], "NumberFormatException");
            assert_eq!(error.to_string(), row["message"].as_str().unwrap());
        }
    }
}

#[test]
fn perm_name_constants_match_java_public_fields() {
    let fixture = java_fixture();
    let expected = [
        ("INDEX_PERM_INHERIT", PermName::INDEX_PERM_INHERIT),
        ("INDEX_PERM_PRIORITY", PermName::INDEX_PERM_PRIORITY),
        ("INDEX_PERM_READ", PermName::INDEX_PERM_READ),
        ("INDEX_PERM_WRITE", PermName::INDEX_PERM_WRITE),
        ("PERM_INHERIT", PermName::PERM_INHERIT),
        ("PERM_PRIORITY", PermName::PERM_PRIORITY),
        ("PERM_READ", PermName::PERM_READ),
        ("PERM_WRITE", PermName::PERM_WRITE),
    ];
    let fields = fixture["permNamePublicFields"].as_array().unwrap();
    assert_eq!(fields.len(), expected.len());
    for (field, (name, value)) in fields.iter().zip(expected) {
        assert_eq!(
            field.as_str().unwrap(),
            format!("int org.apache.rocketmq.common.constant.PermName.{name}")
        );
        assert_eq!(
            value,
            match name {
                "INDEX_PERM_INHERIT" => 0,
                "INDEX_PERM_PRIORITY" => 3,
                "INDEX_PERM_READ" => 2,
                "INDEX_PERM_WRITE" => 1,
                "PERM_INHERIT" => 1,
                "PERM_PRIORITY" => 8,
                "PERM_READ" => 4,
                "PERM_WRITE" => 2,
                _ => unreachable!(),
            }
        );
    }
}
