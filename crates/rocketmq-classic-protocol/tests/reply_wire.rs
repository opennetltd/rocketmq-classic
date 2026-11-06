use rocketmq_classic_protocol::header::{
    CommandHeader, HeaderMap, PopMessageResponseHeader, ReplyMessageRequestHeader,
    SendMessageRequestHeaderV2,
};

fn java_frame(stem: &str, code: i32) -> (HeaderMap, Vec<u8>) {
    let path = format!(
        "{}/../../fixtures/remoting/header-{stem}.bin",
        env!("CARGO_MANIFEST_DIR")
    );
    let frame = std::fs::read(path).unwrap();
    let total = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
    assert_eq!(total, frame.len() - 4);
    let header_size = u32::from_be_bytes(frame[4..8].try_into().unwrap()) as usize & 0xffffff;
    let header: serde_json::Value = serde_json::from_slice(&frame[8..8 + header_size]).unwrap();
    assert_eq!(header["code"], code);
    let fields = serde_json::from_value(header["extFields"].clone()).unwrap();
    (fields, frame[8 + header_size..].to_vec())
}

fn assert_full_fields<T: CommandHeader + std::fmt::Debug>(fields: &HeaderMap) {
    let decoded = T::decode_fields(fields).unwrap();
    let mut encoded = HeaderMap::new();
    decoded.encode_fields(&mut encoded);
    assert_eq!(
        &encoded, fields,
        "all Java fields survive Rust decode/encode"
    );
}

#[test]
fn reply_v2_and_push_use_distinct_source_backed_headers_and_binary_body() {
    let expected = vec![0, 255, 128, 42, 10];
    let (v2, body) = java_frame("reply-v2-request", 325);
    assert_full_fields::<SendMessageRequestHeaderV2>(&v2);
    assert_eq!(body, expected);
    assert_eq!(v2.get("a").map(String::as_str), Some("producers"));

    let (push, body) = java_frame("reply-push-request", 326);
    assert_full_fields::<ReplyMessageRequestHeader>(&push);
    assert_eq!(body, expected);
    assert_eq!(
        push.get("bornHost").map(String::as_str),
        Some("127.0.0.1:1001")
    );
}

#[test]
fn pop_response_java_header_preserves_every_field() {
    let (fields, body) = java_frame("pop-response", 0);
    assert_full_fields::<PopMessageResponseHeader>(&fields);
    assert!(body.is_empty());
}
