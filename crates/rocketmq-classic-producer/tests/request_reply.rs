use rocketmq_classic_common::message::{Message, constants::*, decode_properties};
use rocketmq_classic_producer::{ReplyMessageError, create_reply_message};
use rocketmq_classic_protocol::header::{CommandHeader, HeaderMap, ReplyMessageRequestHeader};

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../fixtures/producer/request-reply.json"
    ))
    .unwrap()
}

#[test]
fn reply_message_matches_the_invoked_java_utility_fixture() {
    let fixture = fixture();
    let mut request = Message::new("orders", b"request-body".to_vec());
    request
        .set_system_property(PROPERTY_CLUSTER, "cluster-east")
        .unwrap();
    request
        .set_system_property(PROPERTY_CORRELATION_ID, "correlation-fixture-01")
        .unwrap();
    request
        .set_system_property(PROPERTY_MESSAGE_REPLY_TO_CLIENT, "client-fixture-02")
        .unwrap();
    request
        .set_system_property(PROPERTY_MESSAGE_TTL, "3200")
        .unwrap();

    let body = "reply-內容".as_bytes();
    let reply = create_reply_message(Some(&request), body).unwrap();
    let expected = &fixture["replyMessage"];
    assert_eq!(reply.topic(), expected["topic"]);
    assert_eq!(reply.body().to_vec(), body);
    assert_eq!(
        &*reply.properties(),
        &serde_json::from_value(expected["properties"].clone()).unwrap()
    );
    assert_eq!(reply.properties()[PROPERTY_MESSAGE_TYPE], "reply");
    assert_eq!(
        PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
        fixture["replyConstants"]["arrivalTimeProperty"]
    );
}

#[test]
fn reply_helper_rejects_java_invalid_request_inputs() {
    assert_eq!(
        create_reply_message(None, b"reply").unwrap_err(),
        ReplyMessageError::NullRequest
    );
    let request = Message::new("orders", b"request".to_vec());
    assert_eq!(
        create_reply_message(Some(&request), b"reply").unwrap_err(),
        ReplyMessageError::MissingCluster
    );
    assert_eq!(
        ReplyMessageError::NullRequest.java_error_code(),
        fixture()["createReplyErrorCode"]
    );
}

#[test]
fn reply_header_matches_java_serialized_ext_fields() {
    let fixture = fixture();
    let expected: HeaderMap = serde_json::from_value(fixture["replyHeader"].clone()).unwrap();
    let header = ReplyMessageRequestHeader::decode_fields(&expected).unwrap();
    let mut actual = HeaderMap::new();
    header.encode_fields(&mut actual);
    for (key, value) in expected {
        assert_eq!(actual.get(&key), Some(&value), "header field {key}");
    }
    let properties = decode_properties(header.get_properties().unwrap_or_default());
    assert_eq!(
        properties[PROPERTY_CORRELATION_ID],
        "correlation-fixture-01"
    );
    assert_eq!(fixture["replyRequestCode"], 326);
}
