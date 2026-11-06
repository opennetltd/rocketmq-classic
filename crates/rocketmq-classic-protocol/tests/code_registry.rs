use std::collections::BTreeMap;

use serde_json::Value;

const JAVA_FIXTURE: &str = include_str!("../../../fixtures/protocol/request-response-codes.json");
const REQUEST_SOURCE: &str = include_str!("../src/codes/request.rs");
const RESPONSE_SOURCE: &str = include_str!("../src/codes/response.rs");

#[test]
fn public_code_registries_match_all_pinned_java_constants() {
    let fixture: Value = serde_json::from_str(JAVA_FIXTURE).expect("valid Java code fixture");
    assert_eq!(
        fixture["artifact"],
        "org.apache.rocketmq:rocketmq-remoting:5.5.1"
    );
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    assert_eq!(fixture["serverBaselineContacted"], false);

    let request = java_code_map(&fixture, "requestCode");
    assert_eq!(
        request.len(),
        166,
        "pinned Java RequestCode declaration count"
    );
    assert_eq!(parse_rust_constants(REQUEST_SOURCE), request);

    let response = java_code_map(&fixture, "responseCode");
    let remoting_system = java_code_map(&fixture, "remotingSysResponseCode");
    assert_eq!(
        response.len(),
        68,
        "pinned Java ResponseCode public field count, including inherited fields"
    );
    assert_eq!(
        remoting_system.len(),
        5,
        "pinned Java RemotingSysResponseCode declaration count"
    );

    for (java_name, value) in &remoting_system {
        assert_eq!(
            response.get(java_name),
            Some(value),
            "ResponseCode inherits RemotingSysResponseCode.{java_name}"
        );
    }
    assert_eq!(
        parse_rust_constants(RESPONSE_SOURCE),
        response,
        "Rust response fields match ResponseCode's public fields, including inherited fields"
    );
}

fn java_code_map(fixture: &Value, key: &str) -> BTreeMap<String, i64> {
    fixture[key]
        .as_object()
        .unwrap_or_else(|| panic!("fixture {key} must be an object"))
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                value
                    .as_i64()
                    .unwrap_or_else(|| panic!("fixture {key}.{name} must be an integer")),
            )
        })
        .collect()
}

fn parse_rust_constants(source: &str) -> BTreeMap<String, i64> {
    let mut expressions = BTreeMap::<String, String>::new();
    for line in source.lines() {
        let Some(declaration) = line.trim().strip_prefix("pub const ") else {
            continue;
        };
        let Some((name, value)) = declaration.split_once(": i32 = ") else {
            continue;
        };
        let value = value
            .strip_suffix(';')
            .unwrap_or_else(|| panic!("Rust constant {name} is missing a semicolon"))
            .trim();
        assert!(
            expressions
                .insert(name.to_owned(), value.to_owned())
                .is_none(),
            "duplicate Rust constant {name}"
        );
    }

    let mut resolved = BTreeMap::<String, i64>::new();
    loop {
        let previous_len = resolved.len();
        for (name, expression) in &expressions {
            if resolved.contains_key(name) {
                continue;
            }
            let value = expression
                .parse::<i64>()
                .ok()
                .or_else(|| resolved.get(expression).copied());
            if let Some(value) = value {
                resolved.insert(name.clone(), value);
            }
        }
        if resolved.len() == previous_len {
            break;
        }
    }

    assert_eq!(
        resolved.len(),
        expressions.len(),
        "every code constant must be a numeric literal or resolve to another constant"
    );
    resolved
}
