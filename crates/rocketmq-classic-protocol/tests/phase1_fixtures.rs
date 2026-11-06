use rocketmq_classic_protocol::codes::{LanguageCode, SerializeType};

#[test]
fn phase_zero_command_fixture_metadata_round_trips_codes_and_wire_tags() {
    let fixtures = [
        ("route-001-get-existing-topic", 105, SerializeType::Json),
        ("heartbeat-001", 34, SerializeType::RocketMq),
        ("prod-001-sync-send", 10, SerializeType::Json),
        ("cons-002-pull-found", 11, SerializeType::RocketMq),
        ("cons-003-pull-no-new-msg", 11, SerializeType::Json),
        ("cons-001-query-offset", 14, SerializeType::RocketMq),
        ("cons-004-update-offset", 15, SerializeType::Json),
        ("order-001-lock-unlock", 41, SerializeType::RocketMq),
    ];
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

    for (stem, code, serialization) in fixtures {
        let metadata = std::fs::read_to_string(format!("{root}/fixtures/remoting/{stem}.json"))
            .expect("Phase 0 Java fixture metadata exists");
        let metadata: serde_json::Value = serde_json::from_str(&metadata).unwrap();
        assert_eq!(metadata["java_client_version"], "5.5.1", "{stem}");
        assert_eq!(metadata["request_code"], code, "{stem}");
        assert_eq!(
            metadata["serialization"],
            serialization_name(serialization),
            "{stem}"
        );
        assert_eq!(metadata["normalized_fields"]["code"], code, "{stem}");
        assert_eq!(metadata["normalized_fields"]["language"], "RUST", "{stem}");
        assert_eq!(
            metadata["normalized_fields"]["serialization"],
            serialization_name(serialization),
            "{stem}"
        );
        assert_eq!(metadata["normalized_fields"]["opaque"], 7, "{stem}");
        assert_eq!(LanguageCode::Rust.code(), 12);
    }
}

fn serialization_name(value: SerializeType) -> &'static str {
    match value {
        SerializeType::Json => "JSON",
        SerializeType::RocketMq => "ROCKETMQ",
    }
}
