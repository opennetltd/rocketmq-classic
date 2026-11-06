use std::{fs, path::PathBuf};

use bytes::BytesMut;
use rocketmq_classic_protocol::{
    codes::{LanguageCode, SerializeType},
    header::{CommandHeader, GetRouteInfoRequestHeader},
};
use rocketmq_classic_remoting::{
    CodecError,
    codec::{frame, json_header, rocketmq_header},
    command::RemotingCommand,
};

fn assert_same_wire_fields(actual: &RemotingCommand, expected: &RemotingCommand) {
    assert_eq!(actual.code, expected.code);
    assert_eq!(actual.language, expected.language);
    assert_eq!(actual.version, expected.version);
    assert_eq!(actual.opaque, expected.opaque);
    assert_eq!(actual.flag, expected.flag);
    assert_eq!(actual.remark, expected.remark);
    assert_eq!(actual.ext_fields, expected.ext_fields);
    assert_eq!(actual.body, expected.body);
    assert_eq!(actual.serialization, expected.serialization);
}

#[test]
fn nullable_language_matches_invoked_java_json_and_binary_boundaries() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["nullableSettings"];
    let command = RemotingCommand {
        code: 34,
        language: None,
        ..Default::default()
    };
    let json = json_header::encode(&command).unwrap();
    let fields: serde_json::Value = serde_json::from_slice(&json).unwrap();
    assert_eq!(
        fields.get("language").is_some(),
        java["JSON"]["languageKeyPresent"]
    );
    assert_eq!(
        json_header::decode(&json).unwrap().language,
        Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
    );
    assert_eq!(java["JSON"]["decodedLanguage"], "JAVA");
    let unknown_language = json_header::decode(br#"{"code":34,"language":"not-a-language"}"#)
        .unwrap()
        .language;
    assert_eq!(
        unknown_language.is_none(),
        java["JSON"]["unknownLanguageDecodedNull"]
    );
    assert_eq!(
        unknown_language, None,
        "Java LanguageCode.getCode(String) returns null for an unknown name"
    );
    let explicit_null = br#"{"code":34,"language":null}"#;
    assert_eq!(
        json_header::decode(explicit_null)
            .unwrap()
            .language
            .is_none(),
        java["JSON"]["explicitNullDecodedLanguageNull"]
    );
    assert!(rocketmq_header::encode(&command).is_err());
    assert_eq!(java["ROCKETMQ"]["outcome"], "NullPointerException");
}

#[test]
fn json_header_inputs_match_invoked_java_fastjson_decoder() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/response-future-api.json"
    ))
    .unwrap();
    let cases = fixture["remotingCommandJsonHeaderInputs"]
        .as_array()
        .unwrap();
    assert_eq!(cases.len(), 3);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let input = case["input"].as_str().unwrap();
        assert_eq!(case["outcome"], "returned", "{name}");
        let command =
            json_header::decode(input.as_bytes()).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            command.code,
            case["code"].as_i64().unwrap() as i32,
            "{name}"
        );
        assert_eq!(
            command.opaque,
            case["opaque"].as_i64().unwrap() as i32,
            "{name}"
        );
        assert_eq!(command.remark.as_deref(), case["remark"].as_str(), "{name}");
    }
}

#[test]
fn unknown_binary_language_code_matches_java_value_of_byte_null() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let unknown = &fixture["languageCodeMethodCases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["case"] == "byte" && case["input"] == -1)
        .unwrap()["decoded"];
    assert!(unknown.is_null());

    let command = RemotingCommand {
        language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
        ..Default::default()
    };
    let mut bytes = rocketmq_header::encode(&command).unwrap();
    bytes[2] = u8::MAX;
    assert_eq!(rocketmq_header::decode(&bytes).unwrap().language, None);
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/remoting")
}

#[test]
fn p2_v01_java_fixtures_cross_decode_for_both_wire_serializations() {
    let mut seen_json = false;
    let mut seen_rocketmq = false;
    for entry in fs::read_dir(fixture_dir()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|extension| extension != "bin") {
            continue;
        }
        let frame_bytes = fs::read(&path).unwrap();
        let mut input = BytesMut::from(frame_bytes.as_slice());
        let command = frame::decode_next(&mut input).unwrap().unwrap();
        assert!(input.is_empty());
        if path
            .file_name()
            .is_some_and(|name| name == "route-001-get-existing-topic.bin")
        {
            assert_eq!(
                command
                    .ext_fields
                    .as_ref()
                    .unwrap()
                    .get("oracle")
                    .map(String::as_str),
                Some("5.5.1")
            );
        }
        match command.serialization {
            SerializeType::Json => seen_json = true,
            SerializeType::RocketMq => seen_rocketmq = true,
        }
    }
    assert!(seen_json && seen_rocketmq);
    let private_large_code = fs::read(fixture_dir().join("private-large-code.bin")).unwrap();
    let mut input = BytesMut::from(private_large_code.as_slice());
    assert_eq!(frame::decode_next(&mut input).unwrap().unwrap().code, 4_614);
    let unknown_code = RemotingCommand {
        code: 987_654,
        ..RemotingCommand::default()
    };
    let unknown_frame = frame::encode(&unknown_code).unwrap();
    let mut input = BytesMut::from(unknown_frame.as_slice());
    assert_eq!(
        frame::decode_next(&mut input).unwrap().unwrap().code,
        987_654
    );
}

#[test]
fn p2_v01_private_codec_matches_java_fast_and_ordinary_header_layouts() {
    let proof_bytes = fs::read(fixture_dir().join("header-proof-001.bin")).unwrap();
    let mut proof_input = BytesMut::from(proof_bytes.as_slice());
    let proof = frame::decode_next(&mut proof_input).unwrap().unwrap();
    let raw_fields = proof.ext_fields.as_ref().unwrap();
    let typed = GetRouteInfoRequestHeader::decode_fields(raw_fields).unwrap();
    assert_eq!(typed.get_topic(), "fixture-topic");
    assert_eq!(raw_fields.get("oracle").map(String::as_str), Some("5.5.1"));
    for name in ["header-fast-001.bin", "header-ordinary-001.bin"] {
        let bytes = fs::read(fixture_dir().join(name)).unwrap();
        let mut input = BytesMut::from(bytes.as_slice());
        let command = frame::decode_next(&mut input).unwrap().unwrap();
        assert_eq!(command.remark.as_deref(), Some("header-equivalent"));
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("topic").unwrap(),
            "fixture-topic"
        );
    }
    let fast = fs::read(fixture_dir().join("header-fast-001.bin")).unwrap();
    let ordinary = fs::read(fixture_dir().join("header-ordinary-001.bin")).unwrap();
    let mut fast = BytesMut::from(fast.as_slice());
    let mut ordinary = BytesMut::from(ordinary.as_slice());
    let fast_command = frame::decode_next(&mut fast).unwrap().unwrap();
    let ordinary_command = frame::decode_next(&mut ordinary).unwrap().unwrap();
    assert_same_wire_fields(&fast_command, &ordinary_command);
}

#[test]
fn private_rocketmq_header_narrows_code_and_version_like_java() {
    let java_fixture = fs::read(fixture_dir().join("private-large-code-version.bin")).unwrap();
    let command = RemotingCommand {
        code: 987_654,
        language: Some(LanguageCode::Rust),
        version: 987_654,
        opaque: 7,
        remark: Some("PRIVATE-LARGE-CODE-VERSION".into()),
        ext_fields: Some(
            std::collections::BTreeMap::from([("oracle".into(), "5.5.1".into())]).into(),
        ),
        serialization: SerializeType::RocketMq,
        ..RemotingCommand::default()
    };

    assert_eq!(frame::encode(&command).unwrap(), java_fixture);
    let mut input = BytesMut::from(java_fixture.as_slice());
    let decoded = frame::decode_next(&mut input).unwrap().unwrap();
    assert_eq!(decoded.code, 4_614);
    assert_eq!(decoded.version, 4_614);
}

#[test]
fn private_rocketmq_header_narrows_ext_field_key_length_like_java() {
    let java_fixture = fs::read(fixture_dir().join("edge-cases/private-large-key.bin")).unwrap();
    let key = "k".repeat(32_768);
    let remark = "PRIVATE-LARGE-KEY";
    let command = RemotingCommand {
        code: 34,
        language: Some(LanguageCode::Rust),
        opaque: 7,
        remark: Some(remark.into()),
        ext_fields: Some(std::collections::BTreeMap::from([(key, "v".into())]).into()),
        serialization: SerializeType::RocketMq,
        ..RemotingCommand::default()
    };

    assert_eq!(frame::encode(&command).unwrap(), java_fixture);
    let key_length_offset = 8 + 21 + remark.len();
    assert_eq!(
        &java_fixture[key_length_offset..key_length_offset + 2],
        &(32_768u16).to_be_bytes()
    );
}

#[test]
fn p2_v03_every_split_and_concatenated_frames_preserve_incomplete_bytes() {
    let command = RemotingCommand {
        code: 34,
        remark: Some("split-test".into()),
        body: Some(vec![1, 2, 3]),
        ..RemotingCommand::default()
    };
    let bytes = frame::encode(&command).unwrap();
    for split in 0..bytes.len() {
        let mut input = BytesMut::from(&bytes[..split]);
        let before = input.clone();
        assert!(frame::decode_next(&mut input).unwrap().is_none());
        assert_eq!(input, before, "input changed at split {split}");
        input.extend_from_slice(&bytes[split..]);
        let decoded = frame::decode_next(&mut input).unwrap().unwrap();
        assert_same_wire_fields(&decoded, &command);
        assert!(input.is_empty());
    }
    let mut joined = BytesMut::from(bytes.as_slice());
    joined.extend_from_slice(&bytes);
    let first = frame::decode_next(&mut joined).unwrap().unwrap();
    assert_same_wire_fields(&first, &command);
    let second = frame::decode_next(&mut joined).unwrap().unwrap();
    assert_same_wire_fields(&second, &command);
    assert!(joined.is_empty());
}

#[test]
fn p2_v02_rejects_invalid_lengths_limits_and_unknown_serialization() {
    for declared in [0u32, 1, 3, 0x8000_0000, u32::MAX] {
        let mut input = BytesMut::from(declared.to_be_bytes().as_slice());
        assert!(frame::decode_next(&mut input).is_err());
    }
    let max_wire_length = 0x00ff_ffffu32;
    let mut max_frame_prefix = BytesMut::from(max_wire_length.to_be_bytes().as_slice());
    assert!(matches!(
        frame::decode_next(&mut max_frame_prefix),
        Ok(None)
    ));
    let mut invalid_serialization = BytesMut::from(&[0, 0, 0, 4, 2, 0, 0, 0][..]);
    assert!(matches!(
        frame::decode_next(&mut invalid_serialization),
        Err(CodecError::UnsupportedSerialization(2))
    ));
    let mut header_larger_than_payload = BytesMut::from(&[0, 0, 0, 4, 0, 0xff, 0xff, 0xff][..]);
    assert!(matches!(
        frame::decode_next(&mut header_larger_than_payload),
        Err(CodecError::HeaderTooLarge { .. })
    ));
    let mut truncated_nested_header = BytesMut::from(&[0, 0, 0, 28, 1, 0, 0, 24][..]);
    truncated_nested_header.extend_from_slice(&[0, 10, 12, 0, 0]);
    truncated_nested_header.extend_from_slice(&0i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&0i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&0i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&5i32.to_be_bytes());
    truncated_nested_header.extend_from_slice(&[1, 2, 3]);
    assert!(matches!(
        frame::decode_next(&mut truncated_nested_header),
        Err(CodecError::MalformedHeader("RocketMQ", _))
    ));
    let mut command_frame = BytesMut::from(&[0, 0, 0, 32, 1, 0, 0, 28][..]);
    command_frame.extend_from_slice(&[0, 10, 12, 0, 0]);
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    command_frame.extend_from_slice(&7i32.to_be_bytes());
    command_frame.extend_from_slice(&[0, 1, 0xff]);
    command_frame.extend_from_slice(&0i32.to_be_bytes());
    assert!(matches!(
        frame::decode_next(&mut command_frame),
        Err(CodecError::InvalidUtf8)
    ));
    let command = RemotingCommand::default();
    let encoded = frame::encode(&command).unwrap();
    assert_eq!(frame::encode(&command).unwrap(), encoded);
    let too_wide = RemotingCommand {
        code: 32_768,
        serialization: SerializeType::RocketMq,
        ..RemotingCommand::default()
    };
    let wrapped = frame::encode(&too_wide).unwrap();
    let mut input = BytesMut::from(wrapped.as_slice());
    assert_eq!(
        frame::decode_next(&mut input).unwrap().unwrap().code,
        i16::MIN as i32
    );
}
