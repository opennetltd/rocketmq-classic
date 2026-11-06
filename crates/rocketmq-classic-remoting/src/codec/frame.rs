use bytes::{Buf, BytesMut};
use rocketmq_classic_protocol::codes::SerializeType;

use crate::{
    codec::{json_header, rocketmq_header},
    command::RemotingCommand,
    error::CodecError,
};

/// Java Netty's default inbound frame limit; it includes the 4-byte length prefix.
pub(crate) const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// The outbound Java encoder has no `frameMaxLength` guard; its length prefix
/// can represent at most a signed Java `int` excluding the 4-byte prefix.
pub(crate) const JAVA_ENCODER_MAX_FRAME_SIZE: usize = i32::MAX as usize + 4;

// ponytail: Java's FastCodesHeader optimization is covered by equivalent golden frames;
// keep the common encoder exact until Phase 18 measurements justify specialization.
/// Encode a complete command frame, matching `RemotingCommand.encode()`.
pub fn encode(command: &RemotingCommand) -> Result<Vec<u8>, CodecError> {
    let mut frame = Vec::new();
    encode_into(command, &mut frame)?;
    Ok(frame)
}

/// Encode the Java `RemotingCommand.encodeHeader(int)` prefix for a body length.
///
/// The returned bytes contain the four-byte declared frame length, the marked
/// header length, and the serialized command header. They do not contain the
/// body. Java accepts signed body lengths and uses wrapping `int` arithmetic;
/// this method preserves that observable prefix behavior.
pub fn encode_header(command: &RemotingCommand, body_length: i32) -> Result<Vec<u8>, CodecError> {
    let mut output = Vec::new();
    encode_header_into(command, body_length, &mut output)?;
    Ok(output)
}

/// Encode the Java no-argument `encodeHeader()` form using the command body size.
pub fn encode_header_for_body(command: &RemotingCommand) -> Result<Vec<u8>, CodecError> {
    let body_length = command.body.as_ref().map_or(0, |body| body.len());
    let body_length =
        i32::try_from(body_length).map_err(|_| CodecError::FieldTooWide("body length"))?;
    encode_header(command, body_length)
}

/// Append a Java-compatible `encodeHeader(int)` prefix to an existing buffer.
///
/// This maps the output-buffer capability of Java's `fastEncodeHeader(ByteBuf)`.
pub fn encode_header_into(
    command: &RemotingCommand,
    body_length: i32,
    output: &mut Vec<u8>,
) -> Result<(), CodecError> {
    let header = match command.serialization {
        SerializeType::Json => json_header::encode(command)?,
        SerializeType::RocketMq => rocketmq_header::encode(command)?,
    };
    if header.len() > 0x00ff_ffff {
        return Err(CodecError::FieldTooWide("header length"));
    }

    let declared_length = 4i32
        .wrapping_add(header.len() as i32)
        .wrapping_add(body_length);
    let marked_header = (u32::from(command.serialization.code()) << 24) | header.len() as u32;
    output.reserve(8 + header.len());
    output.extend_from_slice(&declared_length.to_be_bytes());
    output.extend_from_slice(&marked_header.to_be_bytes());
    output.extend_from_slice(&header);
    Ok(())
}

/// Encode a complete command frame into a caller-owned output buffer.
pub fn encode_into(command: &RemotingCommand, frame: &mut Vec<u8>) -> Result<(), CodecError> {
    encode_into_with_limit(command, JAVA_ENCODER_MAX_FRAME_SIZE, frame)
}

pub(crate) fn encode_into_with_limit(
    command: &RemotingCommand,
    max_frame_size: usize,
    frame: &mut Vec<u8>,
) -> Result<(), CodecError> {
    let max_header_length = max_frame_size.saturating_sub(8).min(0x00ff_ffff);
    let header = match command.serialization {
        SerializeType::Json => json_header::encode_with_limit(command, max_header_length)?,
        SerializeType::RocketMq => rocketmq_header::encode_with_limit(command, max_header_length)?,
    };
    if header.len() > max_header_length || header.len() > 0x00ff_ffff {
        return Err(CodecError::FieldTooWide("header length"));
    }
    let body = command.body.as_deref().unwrap_or_default();
    let declared_length = 4usize
        .checked_add(header.len())
        .and_then(|length| length.checked_add(body.len()))
        .ok_or(CodecError::FieldTooWide("frame length"))?;
    let total_length = declared_length
        .checked_add(4)
        .ok_or(CodecError::FieldTooWide("frame length"))?;
    if total_length > max_frame_size {
        return Err(CodecError::FrameTooLarge {
            length: total_length,
            limit: max_frame_size,
        });
    }
    let declared =
        u32::try_from(declared_length).map_err(|_| CodecError::FieldTooWide("frame length"))?;
    let marked_header = (u32::from(command.serialization.code()) << 24) | header.len() as u32;
    frame.clear();
    frame.reserve(total_length);
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(&marked_header.to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    Ok(())
}

/// Decode the next complete frame, leaving incomplete input buffered.
///
/// The transport's configured inbound frame bound is applied by its decoder.
/// Direct command decoding follows the signed Java length-prefix limit.
pub fn decode_next(input: &mut BytesMut) -> Result<Option<RemotingCommand>, CodecError> {
    decode_next_with_limit(input, JAVA_ENCODER_MAX_FRAME_SIZE)
}

pub(crate) fn decode_next_with_limit(
    input: &mut BytesMut,
    max_frame_size: usize,
) -> Result<Option<RemotingCommand>, CodecError> {
    if input.len() < 4 {
        return Ok(None);
    }
    let declared = u32::from_be_bytes(input[..4].try_into().unwrap());
    if declared > i32::MAX as u32 {
        return Err(CodecError::InvalidFrameLength(declared as i32));
    }
    let declared_length = declared as usize;
    if declared_length < 4 {
        return Err(CodecError::InvalidFrameLength(declared as i32));
    }
    let total_length = declared_length
        .checked_add(4)
        .ok_or(CodecError::InvalidFrameLength(declared as i32))?;
    if total_length > max_frame_size {
        return Err(CodecError::FrameTooLarge {
            length: total_length,
            limit: max_frame_size,
        });
    }
    if input.len() < 8 {
        return Ok(None);
    }
    let marked_header = u32::from_be_bytes(input[4..8].try_into().unwrap());
    let protocol_code = (marked_header >> 24) as u8;
    let serialization = SerializeType::from_java_byte(protocol_code as i8)
        .ok_or(CodecError::UnsupportedSerialization(protocol_code))?;
    let header_length = (marked_header & 0x00ff_ffff) as usize;
    let payload_length = declared_length - 4;
    if header_length > payload_length {
        return Err(CodecError::HeaderTooLarge {
            header: header_length,
            payload: payload_length,
        });
    }
    if input.len() < total_length {
        return Ok(None);
    }
    let header_end = 8 + header_length;
    let decoded = match serialization {
        SerializeType::Json => json_header::decode(&input[8..header_end])?,
        SerializeType::RocketMq => rocketmq_header::decode(&input[8..header_end])?,
    };
    let mut command = decoded;
    command.serialization = serialization;
    let body = &input[header_end..total_length];
    command.body = (!body.is_empty()).then(|| body.to_vec());
    input.advance(total_length);
    Ok(Some(command))
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use bytes::BytesMut;

    use super::{
        decode_next_with_limit, encode, encode_header, encode_header_for_body, encode_header_into,
        encode_into_with_limit,
    };
    use crate::{command::RemotingCommand, error::CodecError};
    use rocketmq_classic_protocol::codes::SerializeType;

    fn hex(bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").unwrap();
        }
        output
    }

    fn header_command(serialization: SerializeType) -> RemotingCommand {
        RemotingCommand {
            code: 17,
            version: 1,
            opaque: 42,
            flag: 2,
            remark: Some("header".into()),
            serialization,
            body: Some(vec![1, 2, 3]),
            ..RemotingCommand::default()
        }
    }

    #[test]
    fn transport_decoder_and_encoder_apply_the_internal_frame_bound() {
        let command = RemotingCommand::default();
        let encoded = encode(&command).unwrap();

        let mut output = Vec::new();
        assert!(encode_into_with_limit(&command, encoded.len() - 1, &mut output).is_err());

        let mut input = BytesMut::from(encoded.as_slice());
        assert!(matches!(
            decode_next_with_limit(&mut input, encoded.len() - 1),
            Err(CodecError::FrameTooLarge { .. })
        ));

        let mut input = BytesMut::from(encoded.as_slice());
        let decoded = decode_next_with_limit(&mut input, encoded.len())
            .unwrap()
            .expect("complete frame must decode to a command");
        assert_eq!(decoded.code, command.code);
        assert_eq!(decoded.language, command.language);
        assert_eq!(decoded.version, command.version);
        assert_eq!(decoded.opaque, command.opaque);
        assert_eq!(decoded.flag, command.flag);
        assert_eq!(decoded.remark, command.remark);
        assert_eq!(decoded.ext_fields, command.ext_fields);
        assert_eq!(decoded.body, command.body);
        assert_eq!(decoded.serialization, command.serialization);
        assert!(input.is_empty());
    }

    #[test]
    fn header_prefix_apis_match_invoked_java_551() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        let cases = &fixture["remotingHeaderEncodingBehavior"];

        for (serialization, name) in [
            (SerializeType::Json, "JSON"),
            (SerializeType::RocketMq, "ROCKETMQ"),
        ] {
            let command = header_command(serialization);
            let codec = &cases[name];
            for case in codec["bodyLengths"].as_array().unwrap() {
                let body_length = case["bodyLength"].as_i64().unwrap() as i32;
                assert_eq!(
                    hex(&encode_header(&command, body_length).unwrap()),
                    case["hex"].as_str().unwrap(),
                    "{name} body length {body_length}"
                );
            }

            let no_argument = encode_header_for_body(&command).unwrap();
            assert_eq!(hex(&no_argument), codec["noArgumentHex"].as_str().unwrap());

            let append = &codec["fastAppend"];
            let mut output = vec![0xa1, 0xb2];
            encode_header_into(&command, 3, &mut output).unwrap();
            assert_eq!(hex(&output[..2]), append["initialHex"].as_str().unwrap());
            assert_eq!(append["prefixPreserved"], true);
            assert_eq!(
                hex(&output[2..]),
                append["suffixHex"].as_str().unwrap(),
                "{name} append-buffer encoding"
            );
            assert_eq!(
                output.len(),
                append["writerIndex"].as_u64().unwrap() as usize
            );
        }
    }
}
