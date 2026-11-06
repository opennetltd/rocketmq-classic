use std::collections::BTreeMap;

use rocketmq_classic_protocol::codes::LanguageCode;

use crate::{command::RemotingCommand, error::CodecError, ext_fields::ExtFields};

const FIXED_HEADER_LEN: usize = 21;

/// Java 5.5.1 private-header order: code i16, language u8, version i16,
/// opaque i32, flag i32, remark length i32 + UTF-8, then map length i32 and
/// entries of key length i16 + UTF-8 + value length i32 + UTF-8.
/// Encode a RocketMQ binary remoting header.
pub fn encode(command: &RemotingCommand) -> Result<Vec<u8>, CodecError> {
    encode_with_limit(command, 0x00ff_ffff)
}

pub(crate) fn encode_with_limit(
    command: &RemotingCommand,
    max_header_length: usize,
) -> Result<Vec<u8>, CodecError> {
    // Java's `ByteBuf.writeShort` and `ByteBuffer.putShort` retain the low
    // sixteen bits for both values, including inputs outside the signed range.
    let code = command.code as i16;
    let version = command.version as i16;
    let remark = command
        .remark
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("")
        .as_bytes();
    if remark.len() > i32::MAX as usize {
        return Err(CodecError::FieldTooWide("remark"));
    }
    let mut entries = Vec::new();
    if FIXED_HEADER_LEN + remark.len() > max_header_length {
        return Err(CodecError::FieldTooWide("header length"));
    }
    if let Some(fields) = command.ext_fields_for_encoding() {
        let fields = fields.as_ref();
        for (key, value) in fields.non_null_fields() {
            let key = key.as_bytes();
            let value = value.as_bytes();
            // Java's RocketMQ `writeStr` stores key byte lengths with setShort.
            let key_len = key.len() as i16;
            let value_len = i32::try_from(value.len())
                .map_err(|_| CodecError::FieldTooWide("ext field value"))?;
            entries.extend_from_slice(&key_len.to_be_bytes());
            entries.extend_from_slice(key);
            entries.extend_from_slice(&value_len.to_be_bytes());
            entries.extend_from_slice(value);
            let size = FIXED_HEADER_LEN
                .checked_add(remark.len())
                .and_then(|size| size.checked_add(entries.len()))
                .ok_or(CodecError::FieldTooWide("header"))?;
            if size > max_header_length {
                return Err(CodecError::FieldTooWide("header length"));
            }
        }
    }
    let entries_len =
        i32::try_from(entries.len()).map_err(|_| CodecError::FieldTooWide("ext fields"))?;
    let capacity = FIXED_HEADER_LEN
        .checked_add(remark.len())
        .and_then(|size| size.checked_add(entries.len()))
        .ok_or(CodecError::FieldTooWide("header"))?;
    if capacity > max_header_length {
        return Err(CodecError::FieldTooWide("header length"));
    }
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(&code.to_be_bytes());
    output.push(command.language.ok_or(CodecError::MissingLanguage)?.code());
    output.extend_from_slice(&version.to_be_bytes());
    output.extend_from_slice(&command.opaque.to_be_bytes());
    output.extend_from_slice(&command.flag.to_be_bytes());
    output.extend_from_slice(&(remark.len() as i32).to_be_bytes());
    output.extend_from_slice(remark);
    output.extend_from_slice(&entries_len.to_be_bytes());
    output.extend_from_slice(&entries);
    Ok(output)
}

pub fn decode(bytes: &[u8]) -> Result<RemotingCommand, CodecError> {
    if bytes.len() < FIXED_HEADER_LEN {
        return Err(malformed("header is shorter than its fixed fields"));
    }
    let mut cursor = Cursor { bytes, offset: 0 };
    let code = cursor.i16()? as i32;
    let language = LanguageCode::from_java_byte(cursor.u8()? as i8);
    let version = cursor.i16()? as i32;
    let opaque = cursor.i32()?;
    let flag = cursor.i32()?;
    let remark = cursor.string_i32()?.filter(|value| !value.is_empty());
    let entries_length = cursor.nonnegative_i32("ext-fields length")?;
    let entries = cursor.take(entries_length)?;
    if cursor.offset != bytes.len() {
        return Err(malformed("trailing bytes after ext fields"));
    }
    let ext_fields = decode_entries(entries)?;
    Ok(RemotingCommand {
        code,
        language,
        version,
        opaque,
        flag,
        remark,
        ext_fields,
        ..RemotingCommand::default()
    })
}

fn decode_entries(bytes: &[u8]) -> Result<Option<ExtFields>, CodecError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut cursor = Cursor { bytes, offset: 0 };
    let mut fields = BTreeMap::new();
    while cursor.offset < bytes.len() {
        let key_len = cursor.i16()?;
        if key_len <= 0 {
            return Err(malformed("ext-field key length must be positive"));
        }
        let key = cursor.utf8(key_len as usize)?;
        let value_len = cursor.nonnegative_i32("ext-field value length")?;
        let value = cursor.utf8(value_len)?;
        fields.insert(key, value);
    }
    Ok(Some(fields.into()))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| malformed("length overflow"))?;
        if end > self.bytes.len() {
            return Err(malformed("truncated field"));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16, CodecError> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, CodecError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn nonnegative_i32(&mut self, field: &'static str) -> Result<usize, CodecError> {
        usize::try_from(self.i32()?).map_err(|_| malformed(&format!("negative {field}")))
    }

    fn utf8(&mut self, length: usize) -> Result<String, CodecError> {
        std::str::from_utf8(self.take(length)?)
            .map(str::to_owned)
            .map_err(|_| CodecError::InvalidUtf8)
    }

    fn string_i32(&mut self) -> Result<Option<String>, CodecError> {
        let length = self.nonnegative_i32("string length")?;
        if length == 0 {
            Ok(None)
        } else {
            self.utf8(length).map(Some)
        }
    }
}

fn malformed(reason: &str) -> CodecError {
    CodecError::MalformedHeader("RocketMQ", reason.into())
}
