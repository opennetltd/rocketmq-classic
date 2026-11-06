use std::collections::HashSet;

use thiserror::Error;

use super::{
    MessageExt, NAME_VALUE_SEPARATOR, PROPERTY_SEPARATOR, constants::PROPERTY_SHARDING_KEY,
};

/// Errors returned by Java `MessageUtils` operations.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum MessageUtilsError {
    /// Java's integer remainder throws `ArithmeticException` for a zero size.
    #[error("sharding index size must not be zero")]
    ZeroIndexSize,
}

/// Returns the Java 5.5.1 `MessageUtils.getShardingKeyIndex` result.
///
/// The hash is Guava's seed-zero Murmur3-32 over UTF-8. A zero index size is
/// reported as an error, corresponding to Java's `ArithmeticException`.
pub fn get_sharding_key_index(
    sharding_key: &str,
    index_size: i32,
) -> Result<i32, MessageUtilsError> {
    if index_size == 0 {
        return Err(MessageUtilsError::ZeroIndexSize);
    }

    let hash = murmur3_32(sharding_key.as_bytes());
    // Java integer remainder defines MIN_VALUE % -1 as zero, while Rust's
    // checked remainder reports overflow for that one pair.
    let remainder = if hash == i32::MIN && index_size == -1 {
        0
    } else {
        hash % index_size
    };
    Ok(remainder.wrapping_abs())
}

/// Returns the shard index for a message's `__SHARDINGKEY` property.
/// Missing or null properties use Java's empty-string fallback.
pub fn get_sharding_key_index_by_msg(
    message: &MessageExt,
    index_size: i32,
) -> Result<i32, MessageUtilsError> {
    let sharding_key = message
        .get_property(PROPERTY_SHARDING_KEY)
        .unwrap_or_default();
    get_sharding_key_index(&sharding_key, index_size)
}

/// Returns the distinct shard indexes used by the supplied messages.
pub fn get_sharding_key_indexes<'a>(
    messages: impl IntoIterator<Item = &'a MessageExt>,
    index_size: i32,
) -> Result<HashSet<i32>, MessageUtilsError> {
    messages
        .into_iter()
        .map(|message| get_sharding_key_index_by_msg(message, index_size))
        .collect()
}

/// Removes every encoded property with the given exact key from a Java-style
/// property string. A missing input remains `None`.
pub fn delete_property(properties_string: Option<&str>, name: &str) -> Option<String> {
    let properties_string = properties_string?;
    // Java's implementation does not make progress for an empty name. Keep
    // this malformed input bounded and leave the input unchanged.
    if name.is_empty() {
        return Some(properties_string.to_owned());
    }

    let mut output = String::with_capacity(properties_string.len());
    let mut retained_start = 0;
    let mut search_start = 0;

    while search_start <= properties_string.len() {
        let Some(relative_match) = properties_string[search_start..].find(name) else {
            break;
        };
        let match_start = search_start + relative_match;
        let match_end = match_start + name.len();
        search_start = match_end;

        let starts_property = match_start == 0
            || properties_string.as_bytes().get(match_start - 1)
                == Some(&(PROPERTY_SEPARATOR as u8));
        let ends_key =
            properties_string.as_bytes().get(match_end) == Some(&(NAME_VALUE_SEPARATOR as u8));
        if !starts_property || !ends_key {
            continue;
        }

        output.push_str(&properties_string[retained_start..match_start]);
        let value_start = match_end + NAME_VALUE_SEPARATOR.len_utf8();
        let Some(separator_offset) = properties_string[value_start..].find(PROPERTY_SEPARATOR)
        else {
            // The final property has no trailing separator, so Java drops it
            // and returns the retained prefix.
            return Some(output);
        };
        retained_start = value_start + separator_offset + PROPERTY_SEPARATOR.len_utf8();
        search_start = retained_start;
    }

    output.push_str(&properties_string[retained_start..]);
    Some(output)
}

fn murmur3_32(bytes: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;

    let mut hash = 0_u32;
    let mut chunks = bytes.chunks_exact(4);
    for chunk in &mut chunks {
        let mut key = u32::from_le_bytes(chunk.try_into().expect("four-byte chunk"));
        key = key.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        hash ^= key;
        hash = hash
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }

    let tail = chunks.remainder();
    let mut key = 0_u32;
    for (shift, byte) in tail.iter().enumerate() {
        key |= u32::from(*byte) << (shift * 8);
    }
    if !tail.is_empty() {
        key = key.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        hash ^= key;
    }

    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2_ae35);
    hash ^= hash >> 16;
    hash as i32
}
