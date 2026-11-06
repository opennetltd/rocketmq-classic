//! Public utilities shared by RocketMQ Classic client components.

pub mod name_server_address;
#[doc(hidden)]
pub mod system_properties;

pub use name_server_address::{
    ENDPOINT_PREFIX, INST_ENDPOINT_PATTERN, INSTANCE_PREFIX, INSTANCE_REGEX,
    NAMESRV_ENDPOINT_PATTERN, NameServerAddressError, get_name_server_addresses,
    get_name_srv_addr_from_namesrv_endpoint, matches_namesrv_endpoint,
    parse_instance_id_from_endpoint, validate_instance_endpoint,
};

/// Creates a lowercase UUID v4 correlation ID for request/reply messages.
///
/// This matches Java's `CorrelationIdUtil.createCorrelationId()` text format.
pub fn create_correlation_id() -> String {
    let mut bytes = rand::random::<[u8; 16]>();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(36);
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            output.push('-');
        }
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::create_correlation_id;

    #[test]
    fn correlation_id_is_a_lowercase_uuid_v4() {
        let id = create_correlation_id();
        assert_eq!(id.len(), 36);
        assert_eq!(id.as_bytes()[14], b'4');
        assert!(matches!(id.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
        assert!(id.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23)
                .then_some(byte == b'-')
                .unwrap_or_else(|| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }));
    }
}
