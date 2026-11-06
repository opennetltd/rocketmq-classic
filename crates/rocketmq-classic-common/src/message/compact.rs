use std::io::Cursor;
use thiserror::Error;

use super::{Message, MessageExt, decode_properties, encode_nullable_property_entries};

/// Failures while encoding or decoding Java's compact inner-message records.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompactMessageError {
    #[error("compact message is missing its {field}")]
    Truncated { field: &'static str },
    #[error("compact message has negative {field} length {length}")]
    NegativeLength { field: &'static str, length: i32 },
    #[error("compact message {field} length {length} exceeds the available bytes")]
    IncompleteField { field: &'static str, length: usize },
    #[error("compact message body is absent")]
    MissingBody,
    #[error("compact message properties length {length} exceeds Java's signed-short range")]
    PropertiesTooLong { length: usize },
    #[error("compact message size exceeds Java's signed-int range")]
    SizeOverflow,
    #[error("inner-message size {size} is smaller than its four-byte size field")]
    InvalidInnerSize { size: i32 },
    #[error(
        "inner-message at byte {offset} declares {size} bytes but only {available} are available"
    )]
    IncompleteInnerMessage {
        offset: usize,
        size: usize,
        available: usize,
    },
}

/// Encode a Java `MessageDecoder.encodeMessage` compact record.
pub fn encode_compact_message(message: &Message) -> Result<Vec<u8>, CompactMessageError> {
    let body = message
        .body_nullable()
        .ok_or(CompactMessageError::MissingBody)?;
    let property_entries = message.nullable_property_entries().collect::<Vec<_>>();
    let property_bytes = encode_nullable_property_entries(Some(&property_entries)).into_bytes();
    let properties_length = i16::try_from(property_bytes.len()).map_err(|_| {
        CompactMessageError::PropertiesTooLong {
            length: property_bytes.len(),
        }
    })?;
    let body_length = i32::try_from(body.len()).map_err(|_| CompactMessageError::SizeOverflow)?;
    let total_size = 22_usize
        .checked_add(body.len())
        .and_then(|size| size.checked_add(property_bytes.len()))
        .filter(|size| i32::try_from(*size).is_ok())
        .ok_or(CompactMessageError::SizeOverflow)?;

    let mut record = Vec::with_capacity(total_size);
    record.extend_from_slice(&(total_size as i32).to_be_bytes());
    record.extend_from_slice(&0_i32.to_be_bytes()); // compact magic code
    record.extend_from_slice(&0_i32.to_be_bytes()); // body CRC
    record.extend_from_slice(&message.flag().to_be_bytes());
    record.extend_from_slice(&body_length.to_be_bytes());
    record.extend_from_slice(&body);
    record.extend_from_slice(&properties_length.to_be_bytes());
    record.extend_from_slice(&property_bytes);
    Ok(record)
}

/// Decode one Java `MessageDecoder.decodeMessage` compact record.
/// Returns the message and the number of bytes consumed.
pub fn decode_compact_message(record: &[u8]) -> Result<(Message, usize), CompactMessageError> {
    let mut input = CompactCursor::new(record);
    input.i32("total size")?;
    input.i32("magic code")?;
    input.i32("body CRC")?;
    let flag = input.i32("flag")?;
    let body_length = input.i32("body length")?;
    let body_length =
        usize::try_from(body_length).map_err(|_| CompactMessageError::NegativeLength {
            field: "body",
            length: body_length,
        })?;
    let body = input.bytes(body_length, "body")?.to_vec();
    let properties_length = input.i16("properties length")?;
    let properties_length =
        usize::try_from(properties_length).map_err(|_| CompactMessageError::NegativeLength {
            field: "properties",
            length: i32::from(properties_length),
        })?;
    let properties = input.bytes(properties_length, "properties")?;
    let properties = decode_properties(&String::from_utf8_lossy(properties));
    Ok((
        Message::from_compact(body, flag, properties),
        input.position(),
    ))
}

/// Encode multiple compact messages into one concatenated buffer.
pub fn encode_compact_messages(messages: &[Message]) -> Result<Vec<u8>, CompactMessageError> {
    let mut records = Vec::new();
    for message in messages {
        let record = encode_compact_message(message)?;
        records
            .len()
            .checked_add(record.len())
            .ok_or(CompactMessageError::SizeOverflow)?;
        records.extend_from_slice(&record);
    }
    Ok(records)
}

/// Decode concatenated compact message records.
pub fn decode_compact_messages(records: &[u8]) -> Result<Vec<Message>, CompactMessageError> {
    let mut messages = Vec::new();
    let mut offset = 0usize;
    while offset < records.len() {
        let (message, consumed) = decode_compact_message(&records[offset..])?;
        if consumed == 0 {
            return Err(CompactMessageError::InvalidInnerSize { size: 0 });
        }
        offset = offset
            .checked_add(consumed)
            .ok_or(CompactMessageError::SizeOverflow)?;
        messages.push(message);
    }
    Ok(messages)
}

/// Count concatenated inner records using their declared total-size fields.
/// The cursor advances to its limit on success; after a failed declared-size
/// position update it remains just past the size prefix, matching Java's
/// `ByteBuffer.getInt` followed by `position(currPos + size)`.
pub fn count_inner_messages(records: &mut Cursor<&[u8]>) -> Result<usize, CompactMessageError> {
    let mut count = 0usize;
    let starting_position = usize::try_from(records.position()).unwrap_or(usize::MAX);
    let limit = records.get_ref().len();
    if starting_position > limit {
        return Err(CompactMessageError::Truncated {
            field: "inner message size",
        });
    }
    while usize::try_from(records.position()).unwrap_or(usize::MAX) < limit {
        let offset = usize::try_from(records.position()).unwrap_or(usize::MAX);
        let available = limit - offset;
        let size_bytes = records
            .get_ref()
            .get(offset..offset.saturating_add(4))
            .ok_or(CompactMessageError::Truncated {
                field: "inner message size",
            })?;
        let size = i32::from_be_bytes(size_bytes.try_into().expect("four bytes were selected"));
        // Java ByteBuffer.getInt advances the position before validating the
        // declared end position below.
        records.set_position((offset + 4) as u64);
        if size < 0 {
            let current = i32::try_from(offset).map_err(|_| CompactMessageError::SizeOverflow)?;
            let target = current.wrapping_add(size);
            if target < 0 || usize::try_from(target).unwrap_or(usize::MAX) > limit {
                return Err(CompactMessageError::InvalidInnerSize { size });
            }
            records.set_position(target as u64);
            count = count
                .checked_add(1)
                .ok_or(CompactMessageError::SizeOverflow)?;
            continue;
        }
        let size = usize::try_from(size).map_err(|_| CompactMessageError::SizeOverflow)?;
        if size > available {
            return Err(CompactMessageError::IncompleteInnerMessage {
                offset: offset - starting_position,
                size,
                available,
            });
        }
        let end = offset
            .checked_add(size)
            .ok_or(CompactMessageError::SizeOverflow)?;
        records.set_position(end as u64);
        count = count
            .checked_add(1)
            .ok_or(CompactMessageError::SizeOverflow)?;
    }
    Ok(count)
}

/// Expand a compact body as Java `MessageDecoder.decodeMessage(MessageExt, list)` does.
pub fn decode_inner_batch(outer: &MessageExt) -> Result<Vec<MessageExt>, CompactMessageError> {
    let body = outer
        .body_nullable()
        .ok_or(CompactMessageError::MissingBody)?;
    decode_compact_messages(&body).map(|messages| {
        messages
            .into_iter()
            .enumerate()
            .map(|(index, message)| {
                let mut message = message;
                message.set_topic_nullable(outer.topic_nullable().map(str::to_owned));
                let _ = message.set_wait_store_msg_ok(outer.is_wait_store_msg_ok());
                let mut inner = MessageExt::from_message(message);
                inner.mark_client_ext();
                inner.set_queue_id(outer.queue_id());
                inner.set_queue_offset(outer.queue_offset().wrapping_add(index as i64));
                inner.set_sys_flag(outer.sys_flag());
                inner.set_born_timestamp(outer.born_timestamp());
                inner.set_born_host(outer.born_host().cloned());
                inner.set_store_timestamp(outer.store_timestamp());
                inner.set_store_host(outer.store_host().cloned());
                inner.set_commit_log_offset(outer.commit_log_offset());
                inner
            })
            .collect()
    })
}

struct CompactCursor<'a> {
    record: &'a [u8],
    position: usize,
}

impl<'a> CompactCursor<'a> {
    fn new(record: &'a [u8]) -> Self {
        Self {
            record,
            position: 0,
        }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn bytes(
        &mut self,
        length: usize,
        field: &'static str,
    ) -> Result<&'a [u8], CompactMessageError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CompactMessageError::SizeOverflow)?;
        let bytes = self
            .record
            .get(self.position..end)
            .ok_or(CompactMessageError::IncompleteField { field, length })?;
        self.position = end;
        Ok(bytes)
    }

    fn i16(&mut self, field: &'static str) -> Result<i16, CompactMessageError> {
        let bytes = self.bytes(2, field)?;
        Ok(i16::from_be_bytes(
            bytes.try_into().expect("two bytes were selected"),
        ))
    }

    fn i32(&mut self, field: &'static str) -> Result<i32, CompactMessageError> {
        let bytes = self.bytes(4, field)?;
        Ok(i32::from_be_bytes(
            bytes.try_into().expect("four bytes were selected"),
        ))
    }
}
