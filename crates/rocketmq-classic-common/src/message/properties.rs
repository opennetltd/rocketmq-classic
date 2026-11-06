use std::collections::BTreeMap;

use super::{MessageProperties, NAME_VALUE_SEPARATOR, PROPERTY_SEPARATOR};

/// Serialize message properties with RocketMQ's U+0001/U+0002 separators.
/// Null Java values are omitted; Rust's owned map cannot contain null values.
pub fn encode_properties(properties: &MessageProperties) -> String {
    let mut encoded = String::new();
    for (key, value) in properties {
        encoded.push_str(key);
        encoded.push(NAME_VALUE_SEPARATOR);
        encoded.push_str(value);
        encoded.push(PROPERTY_SEPARATOR);
    }
    encoded
}

/// Java `MessageDecoder.messageProperties2String` for a nullable property map.
pub fn encode_properties_nullable(properties: Option<&MessageProperties>) -> String {
    properties.map(encode_properties).unwrap_or_default()
}

/// Serialize Java-style nullable property entries, including a null key or
/// null value. Java omits null values and appends the literal `"null"` for a
/// null key. The slice order is retained; Java Map iteration order is unspecified.
pub fn encode_nullable_property_entries<K, V>(entries: Option<&[(Option<K>, Option<V>)]>) -> String
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let Some(entries) = entries else {
        return String::new();
    };
    let mut encoded = String::new();
    for (key, value) in entries {
        let Some(value) = value.as_ref() else {
            continue;
        };
        encoded.push_str(key.as_ref().map(AsRef::as_ref).unwrap_or("null"));
        encoded.push(NAME_VALUE_SEPARATOR);
        encoded.push_str(value.as_ref());
        encoded.push(PROPERTY_SEPARATOR);
    }
    encoded
}

/// Parse properties using Java MessageDecoder's permissive record rules.
/// Duplicate keys use last nonempty-value wins; malformed, too-short,
/// empty-key, and empty-value entries are ignored without user-property validation.
pub fn decode_properties(encoded: &str) -> MessageProperties {
    let mut properties = BTreeMap::new();
    for record in encoded.split(PROPERTY_SEPARATOR) {
        if record.encode_utf16().count() < 3 {
            continue;
        }
        let Some((key, value)) = record.split_once(NAME_VALUE_SEPARATOR) else {
            continue;
        };
        if !key.is_empty() && !value.is_empty() {
            properties.insert(key.to_owned(), value.to_owned());
        }
    }
    properties
}

/// Java `MessageDecoder.string2messageProperties` returns an empty map for a
/// null input; this adapter preserves that behavior with an `Option<&str>`.
pub fn decode_properties_nullable(encoded: Option<&str>) -> MessageProperties {
    encoded.map(decode_properties).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_property_map_encodes_to_empty_string() {
        assert_eq!(encode_properties(&MessageProperties::new()), "");
        assert!(decode_properties("").is_empty());
    }

    #[test]
    fn encode_and_decode_unicode_properties() {
        let mut properties = MessageProperties::new();
        properties.insert("鍵".into(), "值🙂".into());
        assert_eq!(
            decode_properties(&encode_properties(&properties)),
            properties
        );
    }

    #[test]
    fn java_malformed_duplicate_and_unknown_entries() {
        let decoded = decode_properties(
            "x\u{1}1\u{2}broken\u{2}a\u{1}2\u{2}a\u{1}3\u{2}EMPTY\u{1}\u{2}UNKNOWN\u{1}kept\u{2}",
        );
        assert_eq!(decoded.get("a").map(String::as_str), Some("3"));
        assert_eq!(decoded.get("UNKNOWN").map(String::as_str), Some("kept"));
        assert_eq!(decoded.get("x").map(String::as_str), Some("1"));
        assert!(!decoded.contains_key("EMPTY"));
        assert!(!decoded.contains_key("broken"));
    }

    #[test]
    fn property_records_match_pinned_java_message_decoder_oracle() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/inbound-routing.json"
        ))
        .unwrap();
        for case in fixture["propertyCases"].as_array().unwrap() {
            let input = case["input"].as_str().unwrap();
            assert_eq!(
                serde_json::to_value(decode_properties(input)).unwrap(),
                case["properties"],
                "input {input:?}"
            );
        }
    }
}
