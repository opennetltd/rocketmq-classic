use std::io;

use thiserror::Error;

use super::message_id::encode_message_id;
use super::{MessageExt, MessageHostAddress, MessageProperties, decode_properties};
use crate::{compression::decompress_limited, sysflag::message};

const MESSAGE_MAGIC_CODE_V1: i32 = -626_843_481;
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626_843_477;
/// Java `MessageDecoder.CHARSET_UTF8`, represented by its encoding name.
pub const CHARSET_UTF8: &str = "UTF-8";
pub const MESSAGE_MAGIC_CODE: i32 = MESSAGE_MAGIC_CODE_V1;
pub const MESSAGE_MAGIC_CODE_POSITION: usize = 4;
pub const MESSAGE_FLAG_POSITION: usize = 16;
pub const MESSAGE_PHYSIC_OFFSET_POSITION: usize = 28;
pub const MESSAGE_STORE_TIMESTAMP_POSITION: usize = 56;
pub const PHY_POS_POSITION: usize = 28;
pub const QUEUE_OFFSET_POSITION: usize = 20;
pub const SYSFLAG_POSITION: usize = 36;
pub const BLANK_MAGIC_CODE: i32 = -875_286_124;

/// Java `MessageVersion`, including the signed V1/V2 topic-length widths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessageVersionError {
    #[error("name is null")]
    NullEnumName,
    #[error("No enum constant org.apache.rocketmq.common.message.MessageVersion.{0}")]
    UnknownEnumName(String),
    #[error("Invalid magicCode {0}")]
    InvalidMagicCode(i32),
    #[error(
        "message version topic length at byte {offset} needs {needed} bytes but only {available} are available"
    )]
    Truncated {
        offset: usize,
        needed: usize,
        available: usize,
    },
}

impl MessageVersion {
    pub const fn values() -> [Self; 2] {
        [Self::V1, Self::V2]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, MessageVersionError> {
        match name {
            None => Err(MessageVersionError::NullEnumName),
            Some("MESSAGE_VERSION_V1") => Ok(Self::V1),
            Some("MESSAGE_VERSION_V2") => Ok(Self::V2),
            Some(other) => Err(MessageVersionError::UnknownEnumName(other.to_owned())),
        }
    }

    pub fn value_of_magic_code(magic_code: i32) -> Result<Self, MessageVersionError> {
        match magic_code {
            MESSAGE_MAGIC_CODE_V1 => Ok(Self::V1),
            MESSAGE_MAGIC_CODE_V2 => Ok(Self::V2),
            other => Err(MessageVersionError::InvalidMagicCode(other)),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::V1 => "MESSAGE_VERSION_V1",
            Self::V2 => "MESSAGE_VERSION_V2",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::V1 => 0,
            Self::V2 => 1,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }

    pub const fn magic_code(self) -> i32 {
        match self {
            Self::V1 => MESSAGE_MAGIC_CODE_V1,
            Self::V2 => MESSAGE_MAGIC_CODE_V2,
        }
    }

    pub const fn topic_length_size(self) -> usize {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Read the signed topic length and advance the remaining-byte view.
    pub fn get_topic_length(self, buffer: &mut &[u8]) -> Result<i32, MessageVersionError> {
        let width = self.topic_length_size();
        if buffer.len() < width {
            return Err(MessageVersionError::Truncated {
                offset: 0,
                needed: width,
                available: buffer.len(),
            });
        }
        let (length_bytes, remaining) = buffer.split_at(width);
        *buffer = remaining;
        Ok(match self {
            Self::V1 => i8::from_be_bytes([length_bytes[0]]) as i32,
            Self::V2 => i16::from_be_bytes([length_bytes[0], length_bytes[1]]) as i32,
        })
    }

    /// Read the signed topic length at `index` without changing the input.
    pub fn get_topic_length_at(
        self,
        buffer: &[u8],
        index: usize,
    ) -> Result<i32, MessageVersionError> {
        let width = self.topic_length_size();
        let end = index
            .checked_add(width)
            .ok_or(MessageVersionError::Truncated {
                offset: index,
                needed: width,
                available: buffer.len().saturating_sub(index),
            })?;
        let Some(length_bytes) = buffer.get(index..end) else {
            return Err(MessageVersionError::Truncated {
                offset: index,
                needed: width,
                available: buffer.len().saturating_sub(index),
            });
        };
        Ok(match self {
            Self::V1 => i8::from_be_bytes([length_bytes[0]]) as i32,
            Self::V2 => i16::from_be_bytes([length_bytes[0], length_bytes[1]]) as i32,
        })
    }

    /// Append the Java-narrowed V1/V2 topic length to the output buffer.
    pub fn put_topic_length(self, buffer: &mut Vec<u8>, topic_length: i32) {
        match self {
            Self::V1 => buffer.push(topic_length as i8 as u8),
            Self::V2 => buffer.extend_from_slice(&(topic_length as i16).to_be_bytes()),
        }
    }
}

impl std::fmt::Display for MessageVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Append the Java `MessageDecoder.createCrc32` property record to a buffer.
/// The ten decimal digits are emitted least-significant first, matching Java.
pub fn append_crc32_property(output: &mut Vec<u8>, mut crc32: i32) {
    output.extend_from_slice(super::constants::PROPERTY_CRC32.as_bytes());
    output.push(super::NAME_VALUE_SEPARATOR as u8);
    for _ in 0..10 {
        let digit = if crc32 > 0 {
            let digit = (crc32 % 10) as u8;
            crc32 /= 10;
            digit
        } else {
            0
        };
        output.push(b'0' + digit);
    }
    output.push(super::PROPERTY_SEPARATOR as u8);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeOptions {
    pub read_body: bool,
    pub decompress_body: bool,
    pub set_properties_string: bool,
    pub verify_crc: bool,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            read_body: true,
            decompress_body: true,
            set_properties_string: false,
            verify_crc: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecodeLimits {
    max_record_bytes: usize,
    max_decompressed_body_bytes: usize,
    max_total_decoded_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            // Java's MessageDecoder has no fixed record, body, or batch cap.
            // Keep the explicit limits for internal callers that need a
            // bounded parser, while the compatibility entrypoints accept any
            // valid Java record and batch.
            max_record_bytes: usize::MAX,
            max_decompressed_body_bytes: usize::MAX,
            max_total_decoded_bytes: usize::MAX,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    #[error("message record at byte {offset} is truncated while reading {field}")]
    Truncated { offset: usize, field: &'static str },
    #[error("message record at byte {offset} has invalid store size {size}")]
    InvalidStoreSize { offset: usize, size: i32 },
    #[error("message record at byte {offset} has unknown magic code {magic_code}")]
    UnknownMagic { offset: usize, magic_code: i32 },
    #[error("message record at byte {offset} has negative {field} length {length}")]
    NegativeLength {
        offset: usize,
        field: &'static str,
        length: i32,
    },
    #[error("message record {field} length {length} exceeds limit {limit}")]
    LimitExceeded {
        field: &'static str,
        length: usize,
        limit: usize,
    },
    #[error("message record declares {record_size} bytes but only {available} bytes are available")]
    IncompleteRecord {
        record_size: usize,
        available: usize,
    },
    #[error("message record has {remaining} trailing bytes after its properties")]
    TrailingBytes { remaining: usize },
    #[error("message record {field} port {port} is outside the socket range")]
    InvalidSocketPort { field: &'static str, port: i32 },
    #[error("body CRC does not match the record CRC field")]
    CrcMismatch,
    #[error("message record uses an unsupported compression type: {0}")]
    CompressionType(String),
    #[error("message decompression failed: {0}")]
    Decompression(String),
    #[error("aggregate decoded data exceeds limit {limit} bytes")]
    AggregateLimitExceeded { limit: usize },
}

/// Decode one Broker message record from the start of `record`.
///
/// Returns the decoded message and the number of bytes consumed from its
/// fields. The declared store size is retained on `MessageExt` but does not
/// bound parsing, matching Java `MessageDecoder.decode`.
pub fn decode_one(
    record: &[u8],
    options: DecodeOptions,
) -> Result<(MessageExt, usize), DecodeError> {
    decode_one_with_limits(record, options, DecodeLimits::default())
}

/// Decode a record with Java `MessageDecoder` client-message-ID semantics.
/// The physical ID is retained as `offset_message_id`; `message_id` uses the
/// `UNIQ_KEY` property when present and otherwise falls back to the physical ID.
pub fn decode_one_as_client(
    record: &[u8],
    options: DecodeOptions,
) -> Result<(MessageExt, usize), DecodeError> {
    let (mut message, consumed) = decode_one(record, options)?;
    assign_client_message_ids(&mut message);
    Ok((message, consumed))
}

/// Java `MessageDecoder.clientDecode(ByteBuffer, readBody)` with its default
/// decompression, property-string, and CRC-check options.
pub fn client_decode(record: &[u8], read_body: bool) -> Result<(MessageExt, usize), DecodeError> {
    decode_one_as_client(
        record,
        DecodeOptions {
            read_body,
            ..DecodeOptions::default()
        },
    )
}

/// Decode the property map from a Broker message record and return Java's
/// final `ByteBuffer` position as bytes consumed from the record start.
///
/// Java's `MessageDecoder.decodeProperties(ByteBuffer)` returns null for
/// nonpositive property lengths and advances just past the length field. A
/// positive length that parses no entries returns an empty map and consumes
/// the property bytes.
pub fn decode_record_properties(
    record: &[u8],
) -> Result<(Option<MessageProperties>, usize), DecodeError> {
    let sys_flag = read_i32_at(record, SYSFLAG_POSITION, "system flag")?;
    let magic_code = read_i32_at(record, MESSAGE_MAGIC_CODE_POSITION, "magic code")?;
    let version =
        MessageVersion::value_of_magic_code(magic_code).map_err(|_| DecodeError::UnknownMagic {
            offset: MESSAGE_MAGIC_CODE_POSITION,
            magic_code,
        })?;

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
    let body_size_position = 68_usize
        .checked_add(born_host_length)
        .and_then(|offset| offset.checked_add(store_host_length))
        .ok_or(DecodeError::Truncated {
            offset: record.len(),
            field: "body size",
        })?;
    let body_size = read_i32_at(record, body_size_position, "body size")?;
    let topic_length_base = body_size_position
        .checked_add(4)
        .ok_or(DecodeError::Truncated {
            offset: body_size_position,
            field: "topic length",
        })?;
    let topic_length_position =
        offset_by_signed_length(topic_length_base, body_size, "topic length")?;
    let topic_length = version
        .get_topic_length_at(record, topic_length_position)
        .map_err(|_| DecodeError::Truncated {
            offset: topic_length_position,
            field: "topic length",
        })?;
    let properties_length_base = topic_length_position
        .checked_add(version.topic_length_size())
        .ok_or(DecodeError::Truncated {
            offset: topic_length_position,
            field: "properties length",
        })?;
    let properties_length_position =
        offset_by_signed_length(properties_length_base, topic_length, "properties length")?;
    let properties_length = read_i16_at(record, properties_length_position, "properties length")?;
    let properties_start =
        properties_length_position
            .checked_add(2)
            .ok_or(DecodeError::Truncated {
                offset: properties_length_position,
                field: "properties",
            })?;
    if properties_length <= 0 {
        return Ok((None, properties_start));
    }

    let properties_end = properties_start
        .checked_add(properties_length as usize)
        .ok_or(DecodeError::Truncated {
            offset: properties_start,
            field: "properties",
        })?;
    let properties_bytes =
        record
            .get(properties_start..properties_end)
            .ok_or(DecodeError::Truncated {
                offset: properties_start,
                field: "properties",
            })?;
    let properties_string = String::from_utf8_lossy(properties_bytes);
    Ok((Some(decode_properties(&properties_string)), properties_end))
}

fn read_i16_at(record: &[u8], offset: usize, field: &'static str) -> Result<i16, DecodeError> {
    let end = offset
        .checked_add(2)
        .ok_or(DecodeError::Truncated { offset, field })?;
    let bytes = record
        .get(offset..end)
        .ok_or(DecodeError::Truncated { offset, field })?;
    Ok(i16::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| DecodeError::Truncated { offset, field })?,
    ))
}

fn read_i32_at(record: &[u8], offset: usize, field: &'static str) -> Result<i32, DecodeError> {
    let end = offset
        .checked_add(4)
        .ok_or(DecodeError::Truncated { offset, field })?;
    let bytes = record
        .get(offset..end)
        .ok_or(DecodeError::Truncated { offset, field })?;
    Ok(i32::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| DecodeError::Truncated { offset, field })?,
    ))
}

fn offset_by_signed_length(
    offset: usize,
    length: i32,
    field: &'static str,
) -> Result<usize, DecodeError> {
    if length >= 0 {
        offset
            .checked_add(length as usize)
            .ok_or(DecodeError::Truncated { offset, field })
    } else {
        offset
            .checked_sub(length.unsigned_abs() as usize)
            .ok_or(DecodeError::Truncated { offset, field })
    }
}

fn decode_one_with_limits(
    record: &[u8],
    options: DecodeOptions,
    limits: DecodeLimits,
) -> Result<(MessageExt, usize), DecodeError> {
    let mut input = Cursor::new(record);
    let store_size = input.i32("store size")?;
    let magic_offset = input.position();
    let magic_code = input.i32("magic code")?;
    let topic_width = match magic_code {
        MESSAGE_MAGIC_CODE_V1 => 1,
        MESSAGE_MAGIC_CODE_V2 => 2,
        other => {
            return Err(DecodeError::UnknownMagic {
                offset: magic_offset,
                magic_code: other,
            });
        }
    };

    let body_crc = input.i32("body CRC")?;
    let queue_id = input.i32("queue ID")?;
    let flag = input.i32("flag")?;
    let queue_offset = input.i64("queue offset")?;
    let commit_log_offset = input.i64("physical offset")?;
    let mut sys_flag = input.i32("system flag")?;
    let born_timestamp = input.i64("born timestamp")?;
    let born_host = input.host("born host", sys_flag & message::BORNHOST_V6_FLAG != 0)?;
    let store_timestamp = input.i64("store timestamp")?;
    let store_host = input.host(
        "store host",
        sys_flag & message::STOREHOSTADDRESS_V6_FLAG != 0,
    )?;
    let reconsume_times = input.i32("reconsume count")?;
    let prepared_transaction_offset = input.i64("prepared transaction offset")?;

    let declared_body_length = input.i32("body length")?;
    let body_length = if declared_body_length > 0 {
        declared_body_length as usize
    } else {
        0
    };
    ensure_record_limit(input.position(), body_length, limits.max_record_bytes)?;
    let encoded_body = input.bytes(body_length, "body")?;
    let mut body = (options.read_body && body_length > 0).then(|| encoded_body.to_vec());
    if options.verify_crc
        && options.read_body
        && body_length > 0
        && body
            .as_deref()
            .is_some_and(|body| crc32(body) & 0x7fff_ffff != body_crc as u32)
    {
        return Err(DecodeError::CrcMismatch);
    }
    if options.read_body
        && options.decompress_body
        && body_length > 0
        && sys_flag & message::COMPRESSED_FLAG != 0
    {
        let compression_type = message::compression_type(sys_flag)
            .map_err(|error| DecodeError::CompressionType(error.to_string()))?;
        let encoded_body = body.as_deref().expect("read body when decompressing");
        body = Some(
            decompress_limited(
                compression_type,
                encoded_body,
                limits.max_decompressed_body_bytes,
            )
            .map_err(|error| {
                if error.kind() == io::ErrorKind::InvalidData
                    && error.to_string().contains("exceeds configured limit")
                {
                    DecodeError::LimitExceeded {
                        field: "decompressed body",
                        length: limits.max_decompressed_body_bytes.saturating_add(1),
                        limit: limits.max_decompressed_body_bytes,
                    }
                } else {
                    DecodeError::Decompression(error.to_string())
                }
            })?,
        );
        sys_flag &= !message::COMPRESSED_FLAG;
    }

    let topic_length_offset = input.position();
    let topic_length = if topic_width == 1 {
        input.i8("topic length")? as i32
    } else {
        input.i16("topic length")? as i32
    };
    let topic_length = nonnegative_length(topic_length_offset, "topic", topic_length)?;
    ensure_record_limit(input.position(), topic_length, limits.max_record_bytes)?;
    let topic = String::from_utf8_lossy(input.bytes(topic_length, "topic")?).into_owned();

    let declared_properties_length = input.i16("properties length")? as i32;
    let properties_length = if declared_properties_length > 0 {
        declared_properties_length as usize
    } else {
        0
    };
    ensure_record_limit(input.position(), properties_length, limits.max_record_bytes)?;
    let properties = String::from_utf8_lossy(input.bytes(properties_length, "properties")?);
    ensure_record_limit(input.position(), 0, limits.max_record_bytes)?;

    let decoded_properties = (properties_length > 0).then(|| {
        let mut decoded_properties = decode_properties(&properties);
        if options.set_properties_string {
            decoded_properties.insert("propertiesString".to_owned(), properties.into_owned());
        }
        decoded_properties
    });
    let mut message = MessageExt::from_message(super::Message::from_decoded(
        topic,
        body,
        decoded_properties,
    ));
    message.set_flag(flag);
    message.set_queue_id(queue_id);
    message.set_queue_offset(queue_offset);
    message.set_commit_log_offset(commit_log_offset);
    message.set_sys_flag(sys_flag);
    message.set_born_timestamp(born_timestamp);
    message.set_store_timestamp(store_timestamp);
    message.set_reconsume_times(reconsume_times);
    message.set_prepared_transaction_offset(prepared_transaction_offset);
    message.set_store_size(store_size);
    message.set_body_crc(body_crc);
    message.set_born_host(Some(born_host));
    message.set_store_host(Some(store_host.clone()));
    message.set_message_id(Some(message_id_from_record(&input, commit_log_offset)));
    Ok((message, input.position()))
}

fn ensure_record_limit(
    position: usize,
    additional: usize,
    limit: usize,
) -> Result<(), DecodeError> {
    let length = position
        .checked_add(additional)
        .ok_or(DecodeError::LimitExceeded {
            field: "record",
            length: usize::MAX,
            limit,
        })?;
    if length > limit {
        return Err(DecodeError::LimitExceeded {
            field: "record",
            length,
            limit,
        });
    }
    Ok(())
}

/// Decode a batch with Java `decodesBatch(..., isClient=false)` failure
/// handling. Stops at the first invalid record and returns the successfully
/// decoded prefix, matching Java's `null`-then-break behavior.
pub fn decode_many_compatible(records: &[u8], options: DecodeOptions) -> Vec<MessageExt> {
    decode_many_prefix_with_limits(records, options, DecodeLimits::default()).0
}

/// Decode a batch using Java `MessageDecoder.decodesBatch(..., isClient=true)`
/// failure handling. Decoding stops at the first invalid record and returns
/// the successfully decoded prefix, matching Java's `null`-then-break
/// behavior.
pub fn decode_many_as_client_compatible(records: &[u8], options: DecodeOptions) -> Vec<MessageExt> {
    let mut messages = decode_many_compatible(records, options);
    for message in &mut messages {
        assign_client_message_ids(message);
    }
    messages
}

/// Java `MessageDecoder.decodes(ByteBuffer)` with the default body read enabled.
pub fn client_decodes(records: &[u8]) -> Result<Vec<MessageExt>, DecodeError> {
    client_decodes_with_body(records, true)
}

/// Java `MessageDecoder.decodes(ByteBuffer, readBody)`.
pub fn client_decodes_with_body(
    records: &[u8],
    read_body: bool,
) -> Result<Vec<MessageExt>, DecodeError> {
    Ok(decode_many_as_client_compatible(
        records,
        DecodeOptions {
            read_body,
            ..DecodeOptions::default()
        },
    ))
}

fn assign_client_message_ids(message: &mut MessageExt) {
    let offset_message_id = message.message_id().map(str::to_owned);
    let client_message_id = message
        .properties()
        .get(super::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .cloned()
        .or_else(|| offset_message_id.clone());
    message.apply_client_decode_ids(offset_message_id, client_message_id);
}

fn decode_many_prefix_with_limits(
    records: &[u8],
    options: DecodeOptions,
    limits: DecodeLimits,
) -> (Vec<MessageExt>, Option<DecodeError>) {
    let mut decoded = Vec::new();
    let mut offset = 0usize;
    let mut decoded_bytes = 0usize;
    while offset < records.len() {
        let (message, consumed) = match decode_one_with_limits(&records[offset..], options, limits)
        {
            Ok(decoded) => decoded,
            Err(error) => return (decoded, Some(error)),
        };
        if consumed == 0 {
            return (
                decoded,
                Some(DecodeError::InvalidStoreSize { offset, size: 0 }),
            );
        }
        let property_bytes = message
            .properties()
            .iter()
            .try_fold(0_usize, |size, (key, value)| {
                size.checked_add(key.len())
                    .and_then(|size| size.checked_add(value.len()))
                    .and_then(|size| size.checked_add(2 * std::mem::size_of::<String>()))
                    .and_then(|size| size.checked_add(3 * std::mem::size_of::<usize>()))
            });
        let Some(property_bytes) = property_bytes else {
            return (
                decoded,
                Some(DecodeError::AggregateLimitExceeded {
                    limit: limits.max_total_decoded_bytes,
                }),
            );
        };
        let Some(next_decoded_bytes) = decoded_bytes
            .checked_add(std::mem::size_of::<MessageExt>())
            .and_then(|size| size.checked_add(message.body().len()))
            .and_then(|size| size.checked_add(message.topic().len()))
            .and_then(|size| size.checked_add(property_bytes))
        else {
            return (
                decoded,
                Some(DecodeError::AggregateLimitExceeded {
                    limit: limits.max_total_decoded_bytes,
                }),
            );
        };
        if next_decoded_bytes > limits.max_total_decoded_bytes {
            return (
                decoded,
                Some(DecodeError::AggregateLimitExceeded {
                    limit: limits.max_total_decoded_bytes,
                }),
            );
        }
        let Some(next_offset) = offset.checked_add(consumed) else {
            return (
                decoded,
                Some(DecodeError::AggregateLimitExceeded {
                    limit: limits.max_total_decoded_bytes,
                }),
            );
        };
        decoded_bytes = next_decoded_bytes;
        offset = next_offset;
        decoded.push(message);
    }
    (decoded, None)
}

fn nonnegative_length(
    offset: usize,
    field: &'static str,
    length: i32,
) -> Result<usize, DecodeError> {
    usize::try_from(length).map_err(|_| DecodeError::NegativeLength {
        offset,
        field,
        length,
    })
}

fn message_id_from_record(input: &Cursor<'_>, offset: i64) -> String {
    let host_start = input.store_host_start;
    let host_length = input.store_host_length;
    encode_message_id(&input.record[host_start..host_start + host_length], offset)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

struct Cursor<'a> {
    record: &'a [u8],
    position: usize,
    store_host_start: usize,
    store_host_length: usize,
}

impl<'a> Cursor<'a> {
    fn new(record: &'a [u8]) -> Self {
        Self {
            record,
            position: 0,
            store_host_start: 0,
            store_host_length: 0,
        }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn bytes(&mut self, count: usize, field: &'static str) -> Result<&'a [u8], DecodeError> {
        let offset = self.position;
        let end = self
            .position
            .checked_add(count)
            .ok_or(DecodeError::Truncated { offset, field })?;
        let bytes = self
            .record
            .get(self.position..end)
            .ok_or(DecodeError::Truncated { offset, field })?;
        self.position = end;
        Ok(bytes)
    }

    fn i8(&mut self, field: &'static str) -> Result<i8, DecodeError> {
        Ok(self.bytes(1, field)?[0] as i8)
    }

    fn i16(&mut self, field: &'static str) -> Result<i16, DecodeError> {
        Ok(i16::from_be_bytes(
            self.bytes(2, field)?
                .try_into()
                .map_err(|_| DecodeError::Truncated {
                    offset: self.position,
                    field,
                })?,
        ))
    }

    fn i32(&mut self, field: &'static str) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(
            self.bytes(4, field)?
                .try_into()
                .map_err(|_| DecodeError::Truncated {
                    offset: self.position,
                    field,
                })?,
        ))
    }

    fn i64(&mut self, field: &'static str) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(
            self.bytes(8, field)?
                .try_into()
                .map_err(|_| DecodeError::Truncated {
                    offset: self.position,
                    field,
                })?,
        ))
    }

    fn host(&mut self, field: &'static str, ipv6: bool) -> Result<MessageHostAddress, DecodeError> {
        let ip_length = if ipv6 { 16 } else { 4 };
        let start = self.position;
        let ip_bytes = self.bytes(ip_length, field)?;
        let address = if ipv6 {
            let address =
                std::net::Ipv6Addr::from(<[u8; 16]>::try_from(ip_bytes).map_err(|_| {
                    DecodeError::Truncated {
                        offset: start,
                        field,
                    }
                })?);
            std::net::IpAddr::V6(address)
        } else {
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                ip_bytes[0],
                ip_bytes[1],
                ip_bytes[2],
                ip_bytes[3],
            ))
        };
        let raw_port = self.i32(field)?;
        let port = u16::try_from(raw_port).map_err(|_| DecodeError::InvalidSocketPort {
            field,
            port: raw_port,
        })?;
        self.store_host_start = start;
        self.store_host_length = ip_length + 4;
        Ok(MessageHostAddress::from_ip_address(address, port))
    }
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::Value;
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    fn fixture_cases() -> Vec<(String, Vec<u8>)> {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../../fixtures/messages/records.json"))
                .expect("message fixture is valid JSON");
        fixture["cases"]
            .as_array()
            .expect("message fixture has cases")
            .iter()
            .map(|case| {
                (
                    case["name"]
                        .as_str()
                        .expect("case name is a string")
                        .to_owned(),
                    STANDARD
                        .decode(
                            case["recordBase64"]
                                .as_str()
                                .expect("recordBase64 is a string"),
                        )
                        .expect("fixture record is valid Base64"),
                )
            })
            .collect()
    }

    fn compressed_record(body: Vec<u8>) -> Vec<u8> {
        let mut message = MessageExt::new();
        message.set_topic("orders");
        message.set_body(body);
        message.set_sys_flag(message::COMPRESSED_FLAG);
        message.set_born_host(Some(MessageHostAddress::from_ip_address(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            10_911,
        )));
        message.set_store_host(Some(MessageHostAddress::from_ip_address(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            10_912,
        )));
        super::super::encode_message_ext(&message, true).expect("compressed message encodes")
    }

    #[test]
    fn compatible_batch_decode_keeps_valid_records_past_rust_only_limits() {
        const MIB: usize = 1024 * 1024;

        let record = compressed_record(vec![0x5a; 3 * MIB]);
        let mut batch = Vec::new();
        for _ in 0..24 {
            batch.extend_from_slice(&record);
        }

        let decoded = decode_many_compatible(&batch, DecodeOptions::default());
        assert_eq!(decoded.len(), 24);
        assert!(
            decoded
                .iter()
                .all(|message| message.body().len() == 3 * MIB)
        );

        let large_record = compressed_record(vec![0xa5; 65 * MIB]);
        let decoded = decode_many_compatible(&large_record, DecodeOptions::default());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].body().len(), 65 * MIB);
    }

    #[test]
    fn internal_decode_bounds_reject_oversized_records_and_decompressed_bodies() {
        let cases = fixture_cases();
        let plain = &cases[0].1;
        let limits = DecodeLimits {
            max_record_bytes: 8,
            ..DecodeLimits::default()
        };
        assert!(matches!(
            decode_one_with_limits(plain, DecodeOptions::default(), limits),
            Err(DecodeError::LimitExceeded {
                field: "record",
                ..
            })
        ));

        let limits = DecodeLimits {
            max_decompressed_body_bytes: 12,
            ..DecodeLimits::default()
        };
        for (name, record) in cases
            .iter()
            .filter(|(name, _)| name.starts_with("compressed-"))
        {
            assert!(
                matches!(
                    decode_one_with_limits(record, DecodeOptions::default(), limits),
                    Err(DecodeError::LimitExceeded {
                        field: "decompressed body",
                        ..
                    })
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn internal_decode_bounds_reject_oversized_aggregate_results() {
        let record = &fixture_cases()[0].1;
        let limits = DecodeLimits {
            max_total_decoded_bytes: 16,
            ..DecodeLimits::default()
        };
        let (decoded, failure) =
            decode_many_prefix_with_limits(record, DecodeOptions::default(), limits);
        assert!(decoded.is_empty());
        assert!(matches!(
            failure,
            Some(DecodeError::AggregateLimitExceeded { .. })
        ));
    }

    #[test]
    fn malformed_batch_tail_is_an_internal_decode_error_after_valid_prefix() {
        let mut records = fixture_cases()[0].1.clone();
        records.push(1);

        let (decoded, failure) = decode_many_prefix_with_limits(
            &records,
            DecodeOptions::default(),
            DecodeLimits::default(),
        );

        assert_eq!(decoded.len(), 1);
        assert!(failure.is_some());
    }
}
