use std::{collections::BTreeMap, fs, path::PathBuf};

use bytes::BytesMut;
use rocketmq_classic_protocol::header::{
    CommandHeader, PullMessageResponseHeader, QueryConsumerOffsetResponseHeader,
    SendMessageResponseHeader,
};
use rocketmq_classic_remoting::codec::frame;

#[test]
fn pinned_java_rpc_replies_decode_in_rust() {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc");
    let entries: Vec<_> = fs::read_dir(&directory).unwrap().collect();
    assert_eq!(entries.len(), 30, "all 15 Java RPC evidence pairs required");
    for item in entries {
        let path = item.unwrap().path();
        if path.extension().and_then(|x| x.to_str()) != Some("json") {
            continue;
        }
        let expected: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(expected["java_client_version"], "5.5.1");
        let frame_bytes = fs::read(path.with_extension("bin")).unwrap();
        let mut input = BytesMut::from(frame_bytes.as_slice());
        let command = frame::decode_next(&mut input)
            .unwrap()
            .expect("full Java frame");
        assert!(input.is_empty());
        assert_eq!(
            command.code,
            expected["response_code"].as_i64().unwrap() as i32
        );
        assert_eq!(
            command.is_response(),
            expected["response"].as_bool().unwrap()
        );
        let actual = command.ext_fields.unwrap_or_default();
        let canonical: BTreeMap<String, String> =
            serde_json::from_value(expected["ext_fields"].clone()).unwrap();
        assert_eq!(actual, canonical, "{}", path.display());
        let body = command.body.unwrap_or_default();
        assert_eq!(hex(&body), expected["body_hex"].as_str().unwrap());
        if let Ok(dir) = std::env::var("P5_RUST_FRAMES") {
            fs::create_dir_all(&dir).unwrap();
            let frame = frame::encode(&rocketmq_classic_remoting::RemotingCommand {
                code: command.code,
                flag: command.flag,
                ext_fields: Some(actual.clone()),
                body: Some(body.clone()),
                ..rocketmq_classic_remoting::RemotingCommand::default()
            })
            .unwrap();
            fs::write(
                PathBuf::from(dir).join(path.with_extension("bin").file_name().unwrap()),
                frame,
            )
            .unwrap();
        }
        let outcome = expected["java_outcome"].as_str().unwrap();
        let code = expected["request_code"].as_u64().unwrap();
        if outcome == "MALFORMED_SUCCESS" {
            assert!(match code {
                10 => match SendMessageResponseHeader::decode_fields(&actual) {
                    Ok(header) => {
                        header.get_queue_id().is_none() || header.get_queue_offset().is_none()
                    }
                    Err(_) => true,
                },
                11 => match PullMessageResponseHeader::decode_fields(&actual) {
                    Ok(header) => {
                        header.get_next_begin_offset().is_none()
                            && header.get_min_offset().is_none()
                            && header.get_max_offset().is_none()
                            && header.get_suggest_which_broker_id().is_none()
                    }
                    Err(_) => false,
                },
                14 => QueryConsumerOffsetResponseHeader::decode_fields(&actual).is_err(),
                _ => false,
            });
            continue;
        }
        if outcome == "BROKER_ERROR" || outcome == "OFFSET_NOT_FOUND" {
            assert_ne!(command.code, 0);
            continue;
        }
        match code {
            10 => {
                let header = SendMessageResponseHeader::decode_fields(&actual).unwrap();
                assert_eq!(header.get_msg_id(), Some("offset-msg-1"));
                assert_eq!(header.get_queue_offset(), Some(99));
                assert!(
                    [
                        "SEND_OK",
                        "FLUSH_DISK_TIMEOUT",
                        "FLUSH_SLAVE_TIMEOUT",
                        "SLAVE_NOT_AVAILABLE"
                    ]
                    .contains(&outcome)
                );
            }
            11 => {
                let header = PullMessageResponseHeader::decode_fields(&actual).unwrap();
                assert_eq!(header.get_next_begin_offset(), Some(10));
                assert!(
                    ["FOUND", "NO_NEW_MSG", "NO_MATCHED_MSG", "OFFSET_ILLEGAL"].contains(&outcome)
                );
            }
            14 => {
                let header = QueryConsumerOffsetResponseHeader::decode_fields(&actual).unwrap();
                assert_eq!(header.get_offset(), Some(9));
                assert_eq!(outcome, "OFFSET");
            }
            code => panic!("unexpected successful request code {code}"),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").unwrap();
    }
    output
}
