use rocketmq_classic_common::utils::create_correlation_id;

#[test]
fn correlation_id_matches_the_invoked_java_api_and_uuid_v4_format() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/producer/request-reply.json"
    ))
    .unwrap();

    assert_eq!(
        fixture["publicCorrelationIdUtilMethods"],
        serde_json::json!(["java.lang.String createCorrelationId()"])
    );

    let id = create_correlation_id();
    assert_eq!(id.len(), fixture["correlationId"]["length"]);
    assert_eq!(id.as_bytes()[14], b'4');
    assert!(matches!(id.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
    assert!(id.bytes().enumerate().all(|(index, byte)| {
        matches!(index, 8 | 13 | 18 | 23)
            .then_some(byte == b'-')
            .unwrap_or_else(|| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }));
}
