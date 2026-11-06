use std::{fs, path::PathBuf};

use rocketmq_classic_protocol::{
    codes::JavaEnumLookupError,
    header::{
        BoundaryType, CommandHeader, GetEarliestMsgStoretimeRequestHeader,
        GetEarliestMsgStoretimeResponseHeader, GetMaxOffsetRequestHeader,
        GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader, GetMinOffsetResponseHeader,
        HeaderMap, SearchOffsetRequestHeader, SearchOffsetResponseHeader,
    },
};
use serde_json::Value;

const CASES: [&str; 6] = [
    "search-offset-lower-empty",
    "search-offset-upper",
    "max-offset-committed-empty",
    "min-offset",
    "earliest-storetime-signed",
    "search-offset-error",
];

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/rpc-invoked-offset-boundaries")
        .join(format!("{name}.json"));
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn expected_fields(fixture: &Value, direction: &str) -> HeaderMap {
    serde_json::from_value(fixture[direction]["ext"].clone()).unwrap()
}

#[test]
fn boundary_type_matches_java_constants_names_and_wire_values() {
    assert_eq!(
        BoundaryType::values(),
        [BoundaryType::Lower, BoundaryType::Upper]
    );
    assert_eq!(BoundaryType::Lower.name(), "LOWER");
    assert_eq!(BoundaryType::Upper.name(), "UPPER");
    assert_eq!(BoundaryType::Lower.get_name(), "lower");
    assert_eq!(BoundaryType::Upper.get_name(), "upper");
    assert_eq!(BoundaryType::get_type(Some("upper")), BoundaryType::Upper);
    assert_eq!(BoundaryType::get_type(Some("UPPER")), BoundaryType::Upper);
    assert_eq!(BoundaryType::get_type(Some("unknown")), BoundaryType::Lower);
    assert_eq!(BoundaryType::get_type(None), BoundaryType::Lower);
    assert_eq!(BoundaryType::Lower.ordinal(), 0);
    assert_eq!(BoundaryType::Upper.ordinal(), 1);
    assert_eq!(BoundaryType::Lower.java_compare_to(BoundaryType::Upper), -1);
    assert_eq!(
        BoundaryType::value_of_name(Some("UPPER")),
        Ok(BoundaryType::Upper)
    );
    assert_eq!(
        BoundaryType::value_of_name(None),
        Err(JavaEnumLookupError::NullName)
    );
    assert!(matches!(
        BoundaryType::value_of_name(Some("lower")),
        Err(JavaEnumLookupError::UnknownName { .. })
    ));
}

#[test]
fn offset_boundary_request_and_response_headers_match_java_551_fixtures() {
    for name in CASES {
        let fixture = fixture(name);
        assert_eq!(
            fixture["provenance"]["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(fixture["provenance"]["serverBaselineContacted"], false);
        let request = &fixture["request"];
        let fields = match request["code"].as_i64().unwrap() as i32 {
            29 => {
                let boundary_type =
                    BoundaryType::value_of_name(request["ext"]["boundaryType"].as_str()).unwrap();
                let mut header = SearchOffsetRequestHeader::default();
                header.set_topic(Some(request["ext"]["topic"].as_str().unwrap().to_owned()));
                header.set_queue_id(Some(
                    request["ext"]["queueId"].as_str().unwrap().parse().unwrap(),
                ));
                header.set_timestamp(Some(
                    request["ext"]["timestamp"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap(),
                ));
                header.set_boundary_type(Some(boundary_type));
                header.set_broker_name(request["ext"]["bname"].as_str().map(str::to_owned));
                let mut fields = HeaderMap::new();
                header.encode_fields(&mut fields);
                fields
            }
            30 => {
                let mut header = GetMaxOffsetRequestHeader::default();
                header.set_topic(Some(request["ext"]["topic"].as_str().unwrap().to_owned()));
                header.set_queue_id(Some(
                    request["ext"]["queueId"].as_str().unwrap().parse().unwrap(),
                ));
                header.set_committed(request["ext"]["committed"] == "true");
                header.set_broker_name(request["ext"]["bname"].as_str().map(str::to_owned));
                let mut fields = HeaderMap::new();
                header.encode_fields(&mut fields);
                fields
            }
            31 => {
                let mut header = GetMinOffsetRequestHeader::default();
                header.set_topic(Some(request["ext"]["topic"].as_str().unwrap().to_owned()));
                header.set_queue_id(Some(
                    request["ext"]["queueId"].as_str().unwrap().parse().unwrap(),
                ));
                header.set_broker_name(request["ext"]["bname"].as_str().map(str::to_owned));
                let mut fields = HeaderMap::new();
                header.encode_fields(&mut fields);
                fields
            }
            32 => {
                let mut header = GetEarliestMsgStoretimeRequestHeader::default();
                header.set_topic(Some(request["ext"]["topic"].as_str().unwrap().to_owned()));
                header.set_queue_id(Some(
                    request["ext"]["queueId"].as_str().unwrap().parse().unwrap(),
                ));
                header.set_broker_name(request["ext"]["bname"].as_str().map(str::to_owned));
                let mut fields = HeaderMap::new();
                header.encode_fields(&mut fields);
                fields
            }
            code => panic!("unexpected offset boundary request code {code}"),
        };
        assert_eq!(fields, expected_fields(&fixture, "request"), "{name}");

        if fixture["response"]["code"] == 0 {
            let response = &fixture["response"];
            let fields = expected_fields(&fixture, "response");
            match request["code"].as_i64().unwrap() as i32 {
                29 => assert_eq!(
                    SearchOffsetResponseHeader::decode_fields(&fields)
                        .unwrap()
                        .get_offset()
                        .unwrap()
                        .to_string(),
                    response["ext"]["offset"].as_str().unwrap(),
                    "{name}"
                ),
                30 => assert_eq!(
                    GetMaxOffsetResponseHeader::decode_fields(&fields)
                        .unwrap()
                        .get_offset()
                        .unwrap()
                        .to_string(),
                    response["ext"]["offset"].as_str().unwrap(),
                    "{name}"
                ),
                31 => assert_eq!(
                    GetMinOffsetResponseHeader::decode_fields(&fields)
                        .unwrap()
                        .get_offset()
                        .unwrap()
                        .to_string(),
                    response["ext"]["offset"].as_str().unwrap(),
                    "{name}"
                ),
                32 => assert_eq!(
                    GetEarliestMsgStoretimeResponseHeader::decode_fields(&fields)
                        .unwrap()
                        .get_timestamp()
                        .unwrap()
                        .to_string(),
                    response["ext"]["timestamp"].as_str().unwrap(),
                    "{name}"
                ),
                code => panic!("unexpected offset boundary response code {code}"),
            }
        }
    }
}
