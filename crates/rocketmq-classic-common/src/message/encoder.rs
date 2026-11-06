use thiserror::Error;

use crate::{compression::compress, sysflag::message};

use super::{MessageExt, MessageHostAddress, encode_nullable_property_entries};

const MESSAGE_MAGIC_CODE: i32 = -626_843_481;

/// Errors while encoding a Java `MessageExt` broker record.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessageExtEncodeError {
    #[error("message topic is absent")]
    MissingTopic,
    #[error("message body is absent")]
    MissingBody,
    #[error("message {field} host is absent")]
    MissingHost { field: &'static str },
    #[error("message {field} host is unresolved")]
    UnresolvedHost { field: &'static str },
    #[error("message {field} host has {actual} bytes but its system flag requires {expected}")]
    HostLengthMismatch {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("message record length calculation overflowed")]
    SizeOverflow,
    #[error("message record computed an invalid size {size}")]
    InvalidSize { size: i64 },
    #[error("message record needs {needed} bytes but its configured store size is {actual}")]
    BufferTooSmall { needed: usize, actual: usize },
    #[error("message body compression failed: {0}")]
    Compression(String),
    #[error("message compression flag is invalid: {0}")]
    CompressionType(String),
}

/// Encode a full Java `MessageDecoder.encode(MessageExt, needCompress)` record.
pub fn encode_message_ext(
    message_ext: &MessageExt,
    need_compress: bool,
) -> Result<Vec<u8>, MessageExtEncodeError> {
    encode_message_ext_inner(message_ext, need_compress, false)
}

/// Encode Java's unique-store form, which omits store timestamp and store host.
pub fn encode_message_ext_uniquely(
    message_ext: &MessageExt,
    need_compress: bool,
) -> Result<Vec<u8>, MessageExtEncodeError> {
    encode_message_ext_inner(message_ext, need_compress, true)
}

fn encode_message_ext_inner(
    message_ext: &MessageExt,
    need_compress: bool,
    uniquely: bool,
) -> Result<Vec<u8>, MessageExtEncodeError> {
    let topic = message_ext
        .topic_nullable()
        .ok_or(MessageExtEncodeError::MissingTopic)?
        .as_bytes();
    let body = message_ext
        .body_nullable()
        .ok_or(MessageExtEncodeError::MissingBody)?;
    let property_entries = message_ext.nullable_property_entries().collect::<Vec<_>>();
    let properties = encode_nullable_property_entries(Some(&property_entries)).into_bytes();
    let sys_flag = message_ext.sys_flag();
    let born_host_length = if sys_flag & message::BORNHOST_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let store_host_length = if sys_flag & message::STOREHOSTADDRESS_V6_FLAG == 0 {
        8
    } else {
        20
    };
    let born_host = encode_host(message_ext.born_host(), "born", born_host_length)?;
    let store_host = if uniquely {
        Vec::new()
    } else {
        encode_host(message_ext.store_host(), "store", store_host_length)?
    };
    let encoded_body = encode_body(&body, sys_flag, need_compress, uniquely)?;

    let topic_length = (topic.len() as u8) as i8;
    let properties_length = properties.len() as i16;
    let body_length =
        i32::try_from(encoded_body.len()).map_err(|_| MessageExtEncodeError::SizeOverflow)?;
    let calculated_size = if uniquely {
        4_i64
            + 4
            + 4
            + 4
            + 4
            + 8
            + 8
            + 4
            + 8
            + born_host_length as i64
            + 4
            + 8
            + 4
            + i64::from(body_length)
            + 1
            + i64::from(topic_length)
            + 2
            + i64::from(properties_length)
    } else {
        4_i64
            + 4
            + 4
            + 4
            + 4
            + 8
            + 8
            + 4
            + 8
            + born_host_length as i64
            + 8
            + store_host_length as i64
            + 4
            + 8
            + 4
            + i64::from(body_length)
            + 1
            + i64::from(topic_length)
            + 2
            + i64::from(properties_length)
    };
    let configured_store_size = message_ext.store_size();
    let declared_size = if configured_store_size > 0 {
        configured_store_size
    } else {
        i32::try_from(calculated_size).map_err(|_| MessageExtEncodeError::InvalidSize {
            size: calculated_size,
        })?
    };
    if declared_size <= 0 {
        return Err(MessageExtEncodeError::InvalidSize {
            size: i64::from(declared_size),
        });
    }
    let output_length = if uniquely && configured_store_size > 0 {
        configured_store_size
            .checked_sub(8)
            .ok_or(MessageExtEncodeError::InvalidSize {
                size: i64::from(configured_store_size) - 8,
            })?
    } else {
        declared_size
    };
    let output_length =
        usize::try_from(output_length).map_err(|_| MessageExtEncodeError::InvalidSize {
            size: i64::from(output_length),
        })?;
    let mut output = vec![0; output_length];
    let mut cursor = 0;
    put_i32(&mut output, &mut cursor, declared_size)?;
    put_i32(&mut output, &mut cursor, MESSAGE_MAGIC_CODE)?;
    put_i32(&mut output, &mut cursor, message_ext.body_crc())?;
    put_i32(&mut output, &mut cursor, message_ext.queue_id())?;
    put_i32(&mut output, &mut cursor, message_ext.flag())?;
    put_i64(&mut output, &mut cursor, message_ext.queue_offset())?;
    put_i64(&mut output, &mut cursor, message_ext.commit_log_offset())?;
    put_i32(&mut output, &mut cursor, sys_flag)?;
    put_i64(&mut output, &mut cursor, message_ext.born_timestamp())?;
    put_bytes(&mut output, &mut cursor, &born_host)?;
    if !uniquely {
        put_i64(&mut output, &mut cursor, message_ext.store_timestamp())?;
        put_bytes(&mut output, &mut cursor, &store_host)?;
    }
    put_i32(&mut output, &mut cursor, message_ext.reconsume_times())?;
    put_i64(
        &mut output,
        &mut cursor,
        message_ext.prepared_transaction_offset(),
    )?;
    put_i32(&mut output, &mut cursor, body_length)?;
    put_bytes(&mut output, &mut cursor, &encoded_body)?;
    put_bytes(&mut output, &mut cursor, &[topic_length as u8])?;
    put_bytes(&mut output, &mut cursor, topic)?;
    put_bytes(&mut output, &mut cursor, &properties_length.to_be_bytes())?;
    put_bytes(&mut output, &mut cursor, &properties)?;
    Ok(output)
}

fn encode_host(
    host: Option<&MessageHostAddress>,
    field: &'static str,
    expected_length: usize,
) -> Result<Vec<u8>, MessageExtEncodeError> {
    let host = host.ok_or(MessageExtEncodeError::MissingHost { field })?;
    let bytes = host
        .network_bytes()
        .ok_or(MessageExtEncodeError::UnresolvedHost { field })?;
    if bytes.len() != expected_length {
        return Err(MessageExtEncodeError::HostLengthMismatch {
            field,
            expected: expected_length,
            actual: bytes.len(),
        });
    }
    Ok(bytes)
}

fn encode_body(
    body: &[u8],
    sys_flag: i32,
    need_compress: bool,
    uniquely: bool,
) -> Result<Vec<u8>, MessageExtEncodeError> {
    if !need_compress || sys_flag & message::COMPRESSED_FLAG == 0 {
        return Ok(body.to_vec());
    }
    let compression_type = if uniquely {
        crate::sysflag::message::CompressionType::Zlib
    } else {
        message::compression_type(sys_flag)
            .map_err(|error| MessageExtEncodeError::CompressionType(error.to_string()))?
    };
    compress(compression_type, body, 5)
        .map_err(|error| MessageExtEncodeError::Compression(error.to_string()))
}

fn put_i32(output: &mut [u8], cursor: &mut usize, value: i32) -> Result<(), MessageExtEncodeError> {
    put_bytes(output, cursor, &value.to_be_bytes())
}

fn put_i64(output: &mut [u8], cursor: &mut usize, value: i64) -> Result<(), MessageExtEncodeError> {
    put_bytes(output, cursor, &value.to_be_bytes())
}

fn put_bytes(
    output: &mut [u8],
    cursor: &mut usize,
    bytes: &[u8],
) -> Result<(), MessageExtEncodeError> {
    let end = cursor
        .checked_add(bytes.len())
        .ok_or(MessageExtEncodeError::SizeOverflow)?;
    let actual = output.len();
    let destination =
        output
            .get_mut(*cursor..end)
            .ok_or(MessageExtEncodeError::BufferTooSmall {
                needed: end,
                actual,
            })?;
    destination.copy_from_slice(bytes);
    *cursor = end;
    Ok(())
}
