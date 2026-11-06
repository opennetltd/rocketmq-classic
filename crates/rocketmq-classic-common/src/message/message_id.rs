use std::io::Cursor;
use std::net::IpAddr;

use thiserror::Error;

use super::MessageHostAddress;

/// Java `MessageId`, with nullable address and mutable offset fields.
#[derive(Debug)]
pub struct MessageId {
    address: Option<MessageHostAddress>,
    offset: i64,
}

impl MessageId {
    pub fn new(address: Option<MessageHostAddress>, offset: i64) -> Self {
        Self { address, offset }
    }

    pub fn address(&self) -> Option<&MessageHostAddress> {
        self.address.as_ref()
    }

    pub fn set_address(&mut self, address: Option<MessageHostAddress>) {
        self.address = address;
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn set_offset(&mut self, offset: i64) {
        self.offset = offset;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessageIdError {
    #[error("cannot create a message ID from an unresolved address")]
    UnresolvedAddress,
    #[error("message ID cannot be empty")]
    EmptyMessageId,
    #[error("message ID has {available} decoded bytes; {required} are required")]
    InsufficientBytes { required: usize, available: usize },
    #[error("message ID port {port} is outside the socket range")]
    InvalidPort { port: i32 },
    #[error("message ID output buffer requires {required} bytes but has capacity {capacity}")]
    OutputBufferTooSmall { required: usize, capacity: usize },
    #[error("message ID address cursor position {position} exceeds its limit {limit}")]
    AddressCursorOutOfRange { position: u64, limit: usize },
    #[error("message ID buffer write needs {needed} bytes but only {remaining} remain")]
    BufferOverflow { needed: usize, remaining: usize },
}

/// Create a Java-compatible uppercase message ID from a resolved socket address
/// and its offset (or transaction ID hash code).
pub fn create_message_id(
    address: &MessageHostAddress,
    offset: i64,
) -> Result<String, MessageIdError> {
    let host_bytes = address
        .network_bytes()
        .ok_or(MessageIdError::UnresolvedAddress)?;
    Ok(encode_message_id(&host_bytes, offset))
}

/// Create a Java-compatible message ID from Java-style address and output
/// buffers. On success, the address cursor advances to its limit and the
/// output cursor advances past the copied address and eight-byte offset.
pub fn create_message_id_from_buffers(
    input: &mut Cursor<&mut [u8]>,
    address: &mut Cursor<&[u8]>,
    offset: i64,
) -> Result<String, MessageIdError> {
    let address_limit = address.get_ref().len();
    let input_capacity = input.get_ref().len();
    let message_id_length = if address_limit == 8 { 16 } else { 28 };
    // MessageDecoder.createMessageId flips the output buffer before setting
    // its limit, so the output position is zero even when the caller supplied
    // a previously positioned buffer.
    input.set_position(0);
    if input_capacity < message_id_length {
        return Err(MessageIdError::OutputBufferTooSmall {
            required: message_id_length,
            capacity: input_capacity,
        });
    }

    let address_position = address.position();
    if address_position > address_limit as u64 {
        return Err(MessageIdError::AddressCursorOutOfRange {
            position: address_position,
            limit: address_limit,
        });
    }
    let address_bytes = &address.get_ref()[address_position as usize..address_limit];
    let copied = address_bytes.len();
    if copied > message_id_length {
        return Err(MessageIdError::BufferOverflow {
            needed: copied,
            remaining: message_id_length,
        });
    }
    input.get_mut()[..copied].copy_from_slice(address_bytes);
    input.set_position(copied as u64);
    address.set_position(address_limit as u64);

    let remaining = message_id_length - copied;
    if remaining < 8 {
        return Err(MessageIdError::BufferOverflow {
            needed: 8,
            remaining,
        });
    }
    input.get_mut()[copied..copied + 8].copy_from_slice(&offset.to_be_bytes());
    input.set_position((copied + 8) as u64);

    let output = input.get_ref().to_vec();
    Ok(encode_bytes_as_hex(&output))
}

/// Decode Java `MessageDecoder.decodeMessageId` output.
///
/// Java selects IPv4 only when the input has exactly 32 UTF-16 code units;
/// every other length selects IPv6. Invalid hexadecimal characters map to the
/// same all-one nibble used by RocketMQ's `UtilAll.string2bytes`.
pub fn decode_message_id(value: &str) -> Result<MessageId, MessageIdError> {
    if value.is_empty() {
        return Err(MessageIdError::EmptyMessageId);
    }

    let utf16_length = value.encode_utf16().count();
    let decoded: Vec<u8> = value
        .to_uppercase()
        .encode_utf16()
        .collect::<Vec<_>>()
        .chunks_exact(2)
        .map(|pair| ((hex_nibble(pair[0]) << 4) | hex_nibble(pair[1])) as u8)
        .collect();
    let ip_length = if utf16_length == 32 { 4 } else { 16 };
    let required = ip_length + 4 + 8;
    if decoded.len() < required {
        return Err(MessageIdError::InsufficientBytes {
            required,
            available: decoded.len(),
        });
    }

    let address_bytes = &decoded[..ip_length];
    let port_offset = ip_length;
    let port = i32::from_be_bytes(
        decoded[port_offset..port_offset + 4]
            .try_into()
            .expect("the message ID length was checked"),
    );
    let port = u16::try_from(port).map_err(|_| MessageIdError::InvalidPort { port })?;
    let ip = if ip_length == 4 {
        IpAddr::from(<[u8; 4]>::try_from(address_bytes).expect("IPv4 length was selected"))
    } else {
        IpAddr::from(<[u8; 16]>::try_from(address_bytes).expect("IPv6 length was selected"))
    };
    let offset_start = port_offset + 4;
    let offset = i64::from_be_bytes(
        decoded[offset_start..offset_start + 8]
            .try_into()
            .expect("the message ID length was checked"),
    );

    Ok(MessageId::new(
        Some(MessageHostAddress::from_ip_address(ip, port)),
        offset,
    ))
}

pub(crate) fn encode_message_id(host_bytes: &[u8], offset: i64) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity((host_bytes.len() + 8) * 2);
    for byte in host_bytes.iter().copied().chain(offset.to_be_bytes()) {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn encode_bytes_as_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn hex_nibble(value: u16) -> i32 {
    match value {
        0x30..=0x39 => i32::from(value - u16::from(b'0')),
        0x41..=0x46 => i32::from(value - u16::from(b'A') + 10),
        _ => -1,
    }
}
