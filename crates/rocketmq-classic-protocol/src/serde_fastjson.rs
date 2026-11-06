//! Narrow JSON coercions observed while decoding RocketMQ DTOs with Java
//! Fastjson 2.0.63. These helpers preserve the Rust DTO types while matching
//! the pinned Java input conversions for scalar JSON values.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt;

use indexmap::IndexMap;
use serde::de::{MapAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};
use serde_json::Value;

const FASTJSON_NUMBER_TOKEN_MARKER_PREFIX: &str = "\0rocketmq-fastjson-number-token:";

pub type NullableStringListTable = BTreeMap<String, Option<Vec<Option<String>>>>;
pub type NullableKeyStringMap = BTreeMap<Option<String>, Option<String>>;
pub type NullableLongStringMap = BTreeMap<Option<i64>, Option<String>>;

enum FastjsonStringValue {
    Null,
    Bool(bool),
    Number(String),
    NumberToken(String),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl FastjsonStringValue {
    fn java_string(self) -> Result<String, serde_json::Error> {
        match self {
            Self::Null => Ok("null".to_owned()),
            Self::Bool(value) => Ok(value.to_string()),
            Self::Number(value) => Ok(value),
            Self::NumberToken(_) => Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Fastjson cannot convert a leading-dot number to a Java string",
            ))),
            Self::String(value) => Ok(value),
            Self::Array(_) | Self::Object(_) => {
                let mut output = String::new();
                write_fastjson_string_value(&self, &mut output)?;
                Ok(output)
            }
        }
    }
}

fn write_fastjson_string_value(
    value: &FastjsonStringValue,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    match value {
        FastjsonStringValue::Null => output.push_str("null"),
        FastjsonStringValue::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        FastjsonStringValue::Number(value) => output.push_str(value),
        FastjsonStringValue::NumberToken(value) => output.push_str(
            &normalize_fastjson_number_token(value)
                .expect("Fastjson number marker stores a valid numeric token"),
        ),
        FastjsonStringValue::String(value) => output.push_str(&serde_json::to_string(value)?),
        FastjsonStringValue::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_fastjson_string_value(value, output)?;
            }
            output.push(']');
        }
        FastjsonStringValue::Object(values) => {
            output.push('{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                if key.parse::<i64>().is_ok() {
                    output.push_str(key);
                } else {
                    output.push_str(&serde_json::to_string(key)?);
                }
                output.push(':');
                write_fastjson_string_value(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

struct FastjsonRawObject(Vec<(String, Box<serde_json::value::RawValue>)>);

struct FastjsonRawEntries(Vec<(String, Box<serde_json::value::RawValue>)>);

impl<'de> Deserialize<'de> for FastjsonRawObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FastjsonRawObjectVisitor;

        impl<'de> Visitor<'de> for FastjsonRawObjectVisitor {
            type Value = FastjsonRawObject;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = Vec::<(String, Box<serde_json::value::RawValue>)>::new();
                while let Some((key, value)) = map.next_entry()? {
                    if let Some((_, current)) = values.iter_mut().find(|(name, _)| *name == key) {
                        *current = value;
                    } else {
                        values.push((key, value));
                    }
                }
                Ok(FastjsonRawObject(values))
            }
        }

        deserializer.deserialize_map(FastjsonRawObjectVisitor)
    }
}

impl<'de> Deserialize<'de> for FastjsonRawEntries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FastjsonRawEntriesVisitor;

        impl<'de> Visitor<'de> for FastjsonRawEntriesVisitor {
            type Value = FastjsonRawEntries;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut entries = Vec::<(String, Box<serde_json::value::RawValue>)>::new();
                while let Some((key, value)) = map.next_entry()? {
                    entries.push((key, value));
                }
                Ok(FastjsonRawEntries(entries))
            }
        }

        deserializer.deserialize_map(FastjsonRawEntriesVisitor)
    }
}

/// Deserialize an object after applying Fastjson's last-property-wins rule.
/// Raw value tokens preserve the numeric spellings needed by field coercions.
pub fn deserialize_fastjson_last_wins_object<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let FastjsonRawObject(values) = FastjsonRawObject::deserialize(deserializer)?;
    let mut json = String::from("{");
    for (index, (key, value)) in values.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&serde_json::to_string(key).map_err(de::Error::custom)?);
        json.push(':');
        json.push_str(value.get());
    }
    json.push('}');
    serde_json::from_str(&json).map_err(de::Error::custom)
}

impl<'de> Deserialize<'de> for FastjsonStringValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        fastjson_string_value_from_raw::<D::Error>(raw.get())
    }
}

fn fastjson_string_value_from_raw<E>(raw: &str) -> Result<FastjsonStringValue, E>
where
    E: de::Error,
{
    let raw = raw.trim();
    match raw {
        "null" => return Ok(FastjsonStringValue::Null),
        "true" => return Ok(FastjsonStringValue::Bool(true)),
        "false" => return Ok(FastjsonStringValue::Bool(false)),
        _ => {}
    }
    if raw.starts_with('"') {
        if let Some(token) = fastjson_number_token_from_raw(raw) {
            return Ok(FastjsonStringValue::NumberToken(token));
        }
        return serde_json::from_str::<String>(raw)
            .map(FastjsonStringValue::String)
            .map_err(de::Error::custom);
    }
    if raw.starts_with('[') {
        let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?;
        return values
            .iter()
            .map(|value| fastjson_string_value_from_raw::<E>(value.get()))
            .collect::<Result<Vec<_>, _>>()
            .map(FastjsonStringValue::Array);
    }
    if raw.starts_with('{') {
        let FastjsonRawObject(values) =
            serde_json::from_str::<FastjsonRawObject>(raw).map_err(de::Error::custom)?;
        return values
            .into_iter()
            .map(|(key, value)| {
                fastjson_string_value_from_raw::<E>(value.get()).map(|value| (key, value))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(FastjsonStringValue::Object);
    }
    if raw.starts_with('-') || raw.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        let number = if raw.contains(['e', 'E']) {
            let value = raw
                .parse::<f64>()
                .map_err(|_| E::custom("Fastjson numeric value is not representable"))?;
            if !value.is_finite() {
                return Err(E::custom("Fastjson numeric value is not finite"));
            }
            java_double_to_string(value)
        } else {
            raw.to_owned()
        };
        return Ok(FastjsonStringValue::Number(number));
    }
    Err(E::custom("expected a JSON value"))
}

impl Serialize for FastjsonStringValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Number(value) => serde_json::value::RawValue::from_string(value.clone())
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer),
            Self::NumberToken(value) => {
                let normalized = normalize_fastjson_number_token(value)
                    .expect("Fastjson number marker stores a valid numeric token");
                serde_json::value::RawValue::from_string(normalized)
                    .map_err(serde::ser::Error::custom)?
                    .serialize(serializer)
            }
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(value)?;
                }
                sequence.end()
            }
            Self::Object(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

/// Normalizes Fastjson's invoked JSON syntax, whitespace, and numeric tokens
/// before Serde parsing.
pub fn normalize_json_input(input: &str) -> String {
    normalize_json_input_with_marker(input, true)
}

fn normalize_json_input_for<T>(input: &str) -> String {
    let preserve_leading_dot_tokens = std::any::type_name::<T>() != std::any::type_name::<Value>();
    normalize_json_input_with_marker(input, preserve_leading_dot_tokens)
}

fn normalize_json_input_with_marker(input: &str, preserve_leading_dot_tokens: bool) -> String {
    let input = input.strip_prefix('\u{feff}').unwrap_or(input);
    let syntax_normalized = normalize_fastjson_syntax(input);
    let whitespace_normalized = normalize_fastjson_whitespace(&syntax_normalized);
    let eof_normalized = truncate_at_fastjson_eof_marker(&whitespace_normalized);
    let comma_normalized = normalize_fastjson_trailing_commas(&eof_normalized);
    normalize_fastjson_number_tokens(&comma_normalized, false, preserve_leading_dot_tokens)
}

fn truncate_at_fastjson_eof_marker(input: &str) -> String {
    // Fastjson treats U+001A as the end of the input once it has read a
    // complete value. It ignores all following bytes, not only more EOF
    // markers and whitespace. Raw controls inside strings have already been
    // escaped by `normalize_fastjson_whitespace`.
    input
        .as_bytes()
        .iter()
        .position(|byte| *byte == 0x1a)
        .map_or_else(|| input.to_owned(), |end| input[..end].to_owned())
}

pub(crate) fn normalize_fastjson_integer_map_keys(input: &str) -> String {
    normalize_fastjson_number_tokens(input, true, false)
}

fn normalize_fastjson_syntax(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut previous_significant = None;
    let mut containers = Vec::new();

    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                normalize_fastjson_string(bytes, &mut index, quote, &mut output);
                previous_significant = Some(b'"');
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') || bytes.get(index + 1) == Some(&b'*') => {
                let comment_start = index;
                let line_comment = bytes[index + 1] == b'/';
                let comment_end = fastjson_comment_end(bytes, comment_start, line_comment);
                let next = next_fastjson_significant(bytes, comment_end);
                let object_comma = previous_significant == Some(b',')
                    && containers.last() == Some(&b'{')
                    && next != Some(b'}');
                let allowed = match previous_significant {
                    None | Some(b'{' | b'[' | b'}' | b']') => true,
                    Some(b',') => object_comma,
                    _ => false,
                };
                if allowed {
                    output.push(b' ');
                    index = comment_end;
                } else {
                    output.extend_from_slice(&bytes[index..comment_end]);
                    index = comment_end;
                }
            }
            character => {
                output.push(character);
                index += 1;
                if character == b'{' || character == b'[' {
                    containers.push(character);
                } else if (character == b'}' || character == b']')
                    && matches!(
                        (containers.last().copied(), character),
                        (Some(b'{'), b'}') | (Some(b'['), b']')
                    )
                {
                    containers.pop();
                }
                if !is_fastjson_json_space(character) {
                    previous_significant = Some(character);
                }
            }
        }
    }

    String::from_utf8(output).expect("normalizing valid JSON text preserves UTF-8")
}

fn normalize_fastjson_string(bytes: &[u8], index: &mut usize, quote: u8, output: &mut Vec<u8>) {
    output.push(b'"');
    *index += 1;
    while *index < bytes.len() {
        if bytes[*index] == b'\\' {
            if let Some((code_unit, end, trailing_unit)) =
                fastjson_escape_code_unit(bytes, *index, quote, b'u')
            {
                if (0xd800..=0xdbff).contains(&code_unit) {
                    if let Some(low) = trailing_unit.filter(|low| (0xdc00..=0xdfff).contains(low)) {
                        push_json_unicode_escape(output, code_unit);
                        push_json_unicode_escape(output, low);
                        *index = end;
                    } else {
                        match fastjson_escape_code_unit(bytes, end, quote, b'u') {
                            Some((low, paired_end, trailing_low))
                                if (0xdc00..=0xdfff).contains(&low) =>
                            {
                                push_json_unicode_escape(output, code_unit);
                                push_json_unicode_escape(output, low);
                                if trailing_low.is_some() {
                                    output.extend_from_slice(b"\\uFFFD");
                                }
                                *index = paired_end;
                            }
                            _ => {
                                output.extend_from_slice(b"\\uFFFD");
                                *index = end;
                            }
                        }
                    }
                } else if (0xdc00..=0xdfff).contains(&code_unit) {
                    output.extend_from_slice(b"\\uFFFD");
                    if trailing_unit.is_some() {
                        output.extend_from_slice(b"\\uFFFD");
                    }
                    *index = end;
                } else {
                    push_json_unicode_escape(output, code_unit);
                    if trailing_unit.is_some() {
                        output.extend_from_slice(b"\\uFFFD");
                    }
                    *index = end;
                }
            } else if let Some((code_unit, end, _)) =
                fastjson_escape_code_unit(bytes, *index, quote, b'x')
            {
                push_json_unicode_escape(output, code_unit);
                *index = end;
            } else if let Some((code_unit, end, trailing_surrogate)) =
                fastjson_non_ascii_escape_code_unit(bytes, *index)
            {
                push_json_unicode_escape(output, code_unit);
                if trailing_surrogate {
                    output.extend_from_slice(b"\\uFFFD");
                }
                *index = end;
            } else if let Some(escaped) = bytes.get(*index + 1).copied() {
                if let Some(code_unit) = fastjson_char1_escape_code_unit(u16::from(escaped)) {
                    push_json_unicode_escape(output, code_unit);
                    *index += 2;
                } else if escaped.is_ascii() {
                    output.extend_from_slice(&bytes[*index..=*index + 1]);
                    *index += 2;
                } else {
                    let follower = std::str::from_utf8(&bytes[*index + 1..])
                        .expect("input is valid UTF-8")
                        .chars()
                        .next()
                        .expect("a non-ASCII escape follower is present");
                    let end = *index + 1 + follower.len_utf8();
                    output.extend_from_slice(&bytes[*index..end]);
                    *index = end;
                }
            } else {
                output.push(b'\\');
                *index += 1;
            }
        } else if bytes[*index] == quote {
            output.push(b'"');
            *index += 1;
            return;
        } else if quote == b'\'' && bytes[*index] == b'"' {
            output.extend_from_slice(b"\\u0022");
            *index += 1;
        } else {
            output.push(bytes[*index]);
            *index += 1;
        }
    }
}

fn fastjson_escape_digits(
    bytes: &[u8],
    start: usize,
    quote: u8,
    count: usize,
) -> Option<(usize, [u16; 4], Option<u16>)> {
    let marker = *bytes.get(start.checked_add(1)?)?;
    let mut index = start.checked_add(2)?;
    if count > 4 {
        return None;
    }
    let mut digits = [0; 4];
    let mut digit_count = 0;
    let mut trailing_unit = None;
    while digit_count < count {
        let tail = std::str::from_utf8(bytes.get(index..)?).ok()?;
        let escaped = tail.chars().next()?;
        if escaped == char::from(quote) || (marker == b'x' && u32::from(escaped) > 0x66) {
            return None;
        }
        let mut encoded = [0; 2];
        for unit in escaped.encode_utf16(&mut encoded).iter().copied() {
            if digit_count == count {
                trailing_unit = Some(unit);
                break;
            }
            digits[digit_count] = unit;
            digit_count += 1;
        }
        index += escaped.len_utf8();
    }
    Some((index, digits, trailing_unit))
}

fn fastjson_escape_code_unit(
    bytes: &[u8],
    start: usize,
    quote: u8,
    marker: u8,
) -> Option<(u16, usize, Option<u16>)> {
    if bytes.get(start) != Some(&b'\\') || bytes.get(start + 1) != Some(&marker) {
        return None;
    }
    let count = if marker == b'u' { 4 } else { 2 };
    let (end, digits, trailing_unit) = fastjson_escape_digits(bytes, start, quote, count)?;
    let mut value = 0u16;
    for digit in digits.iter().take(count).copied() {
        let nibble = if marker == b'x' {
            fastjson_x_escape_digit(digit)?
        } else {
            fastjson_hex_digit_or_zero(digit)
        };
        value = value.wrapping_shl(4).wrapping_add(u16::from(nibble));
    }
    Some((value, end, trailing_unit))
}

fn fastjson_non_ascii_escape_code_unit(bytes: &[u8], start: usize) -> Option<(u16, usize, bool)> {
    if bytes.get(start) != Some(&b'\\') {
        return None;
    }
    let follower_start = start.checked_add(1)?;
    let tail = std::str::from_utf8(bytes.get(follower_start..)?).ok()?;
    let follower = tail.chars().next()?;
    if follower.is_ascii() {
        return None;
    }
    let mut encoded = [0; 2];
    let units = follower.encode_utf16(&mut encoded);
    let first = *units.first()?;
    let trailing_surrogate = units.len() == 2;
    let code_unit = fastjson_char1_escape_code_unit(first)?;
    Some((
        code_unit,
        follower_start + follower.len_utf8(),
        trailing_surrogate,
    ))
}

fn fastjson_char1_escape_code_unit(escaped: u16) -> Option<u16> {
    let escaped = (escaped & 0x7f) as u8;
    match escaped {
        b'0'..=b'7' => Some(u16::from(escaped - b'0')),
        b'b' => Some(0x0008),
        b't' => Some(0x0009),
        b'n' => Some(0x000a),
        b'v' => Some(0x000b),
        b'f' | b'F' => Some(0x000c),
        b'r' => Some(0x000d),
        b'"' | b'\'' | b'/' | b'\\' => Some(u16::from(escaped)),
        b' ' | b'#' | b'&' | b'(' | b')' | b',' | b'.' | b'@' | b'[' | b']' | b'_' | b'~' => {
            Some(u16::from(escaped))
        }
        _ => None,
    }
}

fn fastjson_hex_digit_or_zero(code_unit: u16) -> u8 {
    ((code_unit & 0x0f) + if code_unit & 0x40 != 0 { 9 } else { 0 }) as u8 & 0x0f
}

fn fastjson_x_escape_digit(code_unit: u16) -> Option<u8> {
    if code_unit > u16::from(b'f') {
        return None;
    }
    Some(match code_unit as u8 {
        b'0'..=b'9' => code_unit as u8 - b'0',
        b'a'..=b'f' => code_unit as u8 - b'a' + 10,
        b'A'..=b'F' => code_unit as u8 - b'A' + 10,
        _ => 0,
    })
}

fn push_json_unicode_escape(output: &mut Vec<u8>, value: u16) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    output.extend_from_slice(b"\\u");
    output.push(HEX[((value >> 12) & 0x0f) as usize]);
    output.push(HEX[((value >> 8) & 0x0f) as usize]);
    output.push(HEX[((value >> 4) & 0x0f) as usize]);
    output.push(HEX[(value & 0x0f) as usize]);
}

fn fastjson_comment_end(bytes: &[u8], start: usize, line_comment: bool) -> usize {
    if line_comment {
        let mut end = start + 2;
        while end < bytes.len() && bytes[end] != b'\n' {
            end += 1;
        }
        end
    } else {
        let mut end = start + 2;
        while end + 1 < bytes.len() && !(bytes[end] == b'*' && bytes[end + 1] == b'/') {
            end += 1;
        }
        if end + 1 < bytes.len() {
            end + 2
        } else {
            bytes.len()
        }
    }
}

fn next_fastjson_significant(bytes: &[u8], mut index: usize) -> Option<u8> {
    loop {
        while bytes
            .get(index)
            .is_some_and(|byte| is_fastjson_json_space(*byte))
        {
            index += 1;
        }
        match bytes.get(index..) {
            Some(rest) if rest.starts_with(b"//") => {
                index = fastjson_comment_end(bytes, index, true);
                if bytes.get(index) == Some(&b'\n') {
                    index += 1;
                }
            }
            Some(rest) if rest.starts_with(b"/*") => {
                index = fastjson_comment_end(bytes, index, false);
            }
            Some(rest) => return rest.first().copied(),
            None => return None,
        }
    }
}

fn is_fastjson_json_space(byte: u8) -> bool {
    matches!(byte, 0 | b' ' | b'\n' | b'\r' | b'\x0c' | b'\t' | b'\x08')
}

fn normalize_fastjson_trailing_commas(input: &str) -> String {
    let characters: Vec<char> = input.chars().collect();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    let mut previous_significant = None;

    while index < characters.len() {
        let character = characters[index];
        if in_string {
            output.push(character);
            index += 1;
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
                previous_significant = Some('"');
            }
            continue;
        }

        if character == '"' {
            in_string = true;
            output.push(character);
            index += 1;
            continue;
        }

        if character == ',' {
            let mut next = index + 1;
            while characters
                .get(next)
                .is_some_and(|next| next.is_ascii_whitespace())
            {
                next += 1;
            }
            let follows_container_value = matches!(previous_significant, Some('}' | ']' | '"'))
                || previous_significant.is_some_and(|previous| {
                    previous.is_ascii_alphanumeric() || matches!(previous, '+' | '-')
                });
            if follows_container_value && matches!(characters.get(next), Some('}' | ']')) {
                index += 1;
                continue;
            }
        }

        output.push(character);
        index += 1;
        if !character.is_ascii_whitespace() {
            previous_significant = Some(character);
        }
    }

    output
}

/// Parses a JSON document with the Fastjson syntax cases recorded by the Java
/// compatibility oracle, then applies this module's field coercions.
pub fn from_fastjson_str<T: DeserializeOwned>(input: &str) -> Result<T, serde_json::Error> {
    let normalized = normalize_json_input_for::<T>(input);
    serde_json::from_str(&normalized)
}

pub fn from_fastjson_slice<T: DeserializeOwned>(input: &[u8]) -> Result<T, serde_json::Error> {
    let input = String::from_utf8_lossy(input);
    from_fastjson_str(&input)
}

pub fn from_fastjson_str_nullable<T: DeserializeOwned>(
    input: Option<&str>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let normalized = normalize_json_input_for::<T>(input);
    serde_json::from_str(&normalized)
}

pub fn from_fastjson_slice_nullable<T: DeserializeOwned>(
    input: Option<&[u8]>,
) -> Result<Option<T>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(input);
    from_fastjson_str_nullable(Some(&input))
}

pub fn from_fastjson_list_nullable<T: DeserializeOwned>(
    input: Option<&[u8]>,
) -> Result<Option<Vec<Option<T>>>, serde_json::Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let input = String::from_utf8_lossy(input);
    let normalized = normalize_json_input_for::<T>(&input);
    serde_json::from_str(&normalized)
}

fn normalize_fastjson_whitespace(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut previous_significant = None;
    let bytes = input.as_bytes();

    for (index, character) in input.char_indices() {
        if in_string {
            if character <= '\u{001f}' && !escaped {
                let code_unit = character as u32;
                output.push_str("\\u00");
                output.push(char::from(
                    b"0123456789abcdef"[((code_unit >> 4) & 0x0f) as usize],
                ));
                output.push(char::from(b"0123456789abcdef"[(code_unit & 0x0f) as usize]));
            } else {
                output.push(character);
            }
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
                previous_significant = Some('"');
            }
        } else if character == '"' {
            in_string = true;
            output.push(character);
        } else if character.is_ascii() && is_fastjson_json_space(character as u8) {
            output.push(' ');
        } else if character == '\u{00a0}' {
            let next =
                next_fastjson_significant(bytes, index + character.len_utf8()).map(char::from);
            if fastjson_nonbreaking_space_allowed(previous_significant, next) {
                output.push(' ');
            } else {
                output.push(character);
                previous_significant = Some(character);
            }
        } else {
            output.push(character);
            previous_significant = Some(character);
        }
    }
    output
}

fn fastjson_nonbreaking_space_allowed(previous: Option<char>, next: Option<char>) -> bool {
    match previous {
        Some('{') => matches!(next, Some('"' | '}')),
        Some(',') => true,
        Some('"') => matches!(next, Some(':' | ',' | '}' | ']')),
        Some('}') | Some(']') => matches!(next, Some(',' | '}' | ']') | None),
        Some(character) if character.is_ascii_alphanumeric() || matches!(character, '+' | '-') => {
            matches!(next, Some(',' | '}' | ']'))
        }
        _ => false,
    }
}

fn normalize_fastjson_number_tokens(
    input: &str,
    quote_integer_keys: bool,
    preserve_leading_dot_tokens: bool,
) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;

    while index < input.len() {
        let Some(character) = input[index..].chars().next() else {
            break;
        };
        if in_string {
            output.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += character.len_utf8();
            continue;
        }
        if character == '"' {
            if let Some(end) = json_string_end(input, index) {
                let raw = &input[index..end];
                if fastjson_number_token_from_raw(raw).is_some() {
                    output.push_str(&escape_reserved_fastjson_marker(raw));
                } else {
                    output.push_str(raw);
                }
                index = end;
            } else {
                in_string = true;
                output.push(character);
                index += character.len_utf8();
            }
            continue;
        }

        if matches!(bytes[index], b'+' | b'-' | b'.' | b'0'..=b'9') {
            let token_start = index;
            while bytes.get(index).is_some_and(|byte| {
                !byte.is_ascii_whitespace()
                    && !matches!(*byte, b',' | b']' | b'}' | b'{' | b'[' | b':')
            }) {
                index += 1;
            }
            let token = &input[token_start..index];
            let mut next = index;
            while bytes.get(next).is_some_and(u8::is_ascii_whitespace) {
                next += 1;
            }
            if quote_integer_keys
                && bytes.get(next) == Some(&b':')
                && is_fastjson_integer_key(token)
            {
                output.push('"');
                output.push_str(token);
                output.push('"');
                continue;
            }
            if bytes.get(next) != Some(&b':') {
                if let Some(normalized) = normalize_fastjson_number_token(token) {
                    if preserve_leading_dot_tokens && token.starts_with('.') {
                        output.push_str(&fastjson_number_token_marker(token));
                    } else {
                        output.push_str(&normalized);
                    }
                    continue;
                }
            }
            output.push_str(token);
            continue;
        }

        output.push(character);
        index += character.len_utf8();
    }
    output
}

fn json_string_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut escaped = false;
    for (offset, byte) in bytes.get(start + 1..)?.iter().copied().enumerate() {
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Some(start + 2 + offset);
        }
    }
    None
}

fn fastjson_number_token_marker(token: &str) -> String {
    serde_json::to_string(&format!("{FASTJSON_NUMBER_TOKEN_MARKER_PREFIX}{token}"))
        .expect("Fastjson marker is a valid string")
}

fn fastjson_number_token_from_raw(raw: &str) -> Option<String> {
    let value = serde_json::from_str::<String>(raw).ok()?;
    let token = value.strip_prefix(FASTJSON_NUMBER_TOKEN_MARKER_PREFIX)?;
    normalize_fastjson_number_token(token)?;
    (serde_json::to_string(&value).ok()?.as_str() == raw).then(|| token.to_owned())
}

fn escape_reserved_fastjson_marker(raw: &str) -> String {
    let value = serde_json::from_str::<String>(raw).expect("recognized marker is a valid string");
    let mut escaped = serde_json::to_string(&value).expect("marker string can be encoded");
    let bytes = escaped.as_bytes();
    let mut index = 1;
    while index < bytes.len() - 1 {
        if bytes[index] == b'\\' {
            index += if bytes.get(index + 1) == Some(&b'u') {
                6
            } else {
                2
            };
        } else if bytes[index].is_ascii_alphabetic() {
            break;
        } else {
            index += 1;
        }
    }
    assert!(
        index < bytes.len() - 1,
        "Fastjson marker contains an ASCII letter"
    );
    let replacement = format!("\\u{:04x}", bytes[index]);
    escaped.replace_range(index..index + 1, &replacement);
    escaped
}

fn is_fastjson_integer_key(token: &str) -> bool {
    let digits = token.strip_prefix(['+', '-']).unwrap_or(token);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn normalize_fastjson_number_token(token: &str) -> Option<String> {
    let suffix = token
        .as_bytes()
        .last()
        .copied()
        .filter(|suffix| matches!(suffix, b'L' | b'F' | b'D' | b'B' | b'S'));
    let token = suffix.map_or(token, |_| &token[..token.len() - 1]);
    let bytes = token.as_bytes();
    let mut index = 0;
    let negative = bytes.first() == Some(&b'-');
    let positive = bytes.first() == Some(&b'+');
    if negative || positive {
        index += 1;
    }
    let integer_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    let has_integer = index != integer_start;
    let integer_end = index;

    let mut decimal_point = None;
    let mut fraction_start = None;
    let mut fraction_end = None;
    if bytes.get(index) == Some(&b'.') {
        decimal_point = Some(index);
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        fraction_start = Some(start);
        fraction_end = Some(index);
    }
    if !has_integer && (decimal_point.is_none() || fraction_start == fraction_end) {
        return None;
    }
    if fraction_start == fraction_end && !matches!(bytes.get(index), None | Some(b'e' | b'E')) {
        return None;
    }
    let mut exponent_start = None;
    let mut incomplete_exponent = false;
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        exponent_start = Some(index);
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_digits_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_digits_start {
            if index == bytes.len() {
                incomplete_exponent = true;
                exponent_start = None;
            } else {
                return None;
            }
        }
    }
    if index != bytes.len() {
        return None;
    }

    let leading_zeroes = integer_end - integer_start > 1 && bytes[integer_start] == b'0';
    let floating_suffix = matches!(suffix, Some(b'F' | b'D'));
    let trailing_decimal_point = decimal_point.is_some() && fraction_start == fraction_end;
    if !positive
        && !leading_zeroes
        && has_integer
        && !trailing_decimal_point
        && !incomplete_exponent
        && suffix.is_none()
    {
        return None;
    }

    let mut significant_start = integer_start;
    while has_integer && significant_start + 1 < integer_end && bytes[significant_start] == b'0' {
        significant_start += 1;
    }
    let mut normalized = String::with_capacity(token.len());
    if negative {
        normalized.push('-');
    }
    if has_integer {
        normalized.push_str(&token[significant_start..integer_end]);
    } else {
        normalized.push('0');
    }
    let omit_incomplete_trailing_decimal =
        incomplete_exponent && trailing_decimal_point && !floating_suffix;
    if let (Some(_), Some(start), Some(end)) = (decimal_point, fraction_start, fraction_end) {
        if !omit_incomplete_trailing_decimal {
            normalized.push('.');
            if start == end {
                normalized.push('0');
            } else {
                normalized.push_str(&token[start..end]);
            }
        }
    } else if floating_suffix && exponent_start.is_none() {
        normalized.push_str(".0");
    }
    if let Some(exponent_start) = exponent_start {
        normalized.push_str(&token[exponent_start..]);
    }
    match suffix {
        Some(b'F') => normalized
            .parse::<f32>()
            .ok()
            .filter(|value| value.is_finite())
            .map(java_float_to_string),
        Some(b'D') => normalized
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(java_double_to_string),
        _ => Some(normalized),
    }
}

fn java_float_to_string(value: f32) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0".to_owned()
        } else {
            "0.0".to_owned()
        };
    }
    java_floating_to_string(value.to_string(), f64::from(value).abs())
}

fn java_double_to_string(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0".to_owned()
        } else {
            "0.0".to_owned()
        };
    }
    java_floating_to_string(value.to_string(), value.abs())
}

fn java_floating_to_string(shortest: String, magnitude: f64) -> String {
    let scientific = !(1.0e-3..1.0e7).contains(&magnitude);
    if !scientific {
        return if shortest.contains('.') {
            shortest
        } else {
            format!("{shortest}.0")
        };
    }

    let (sign, unsigned) = shortest
        .strip_prefix('-')
        .map_or(("", shortest.as_str()), |value| ("-", value));
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{integer}{fraction}");
    let first_nonzero = digits
        .bytes()
        .position(|digit| digit != b'0')
        .expect("nonzero finite floating value has a significant digit");
    let exponent = integer.len() as i32 - first_nonzero as i32 - 1;
    let significant = digits[first_nonzero..].trim_end_matches('0');
    let mut mantissa = String::with_capacity(significant.len() + 2);
    mantissa.push(char::from(significant.as_bytes()[0]));
    mantissa.push('.');
    if significant.len() == 1 {
        mantissa.push('0');
    } else {
        mantissa.push_str(&significant[1..]);
    }
    format!("{sign}{mantissa}E{exponent}")
}

pub fn deserialize_nullable_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = FastjsonStringValue::deserialize(deserializer)?;
    match value {
        FastjsonStringValue::Null => Ok(None),
        FastjsonStringValue::String(value) => Ok(Some(value)),
        value => value.java_string().map(Some).map_err(de::Error::custom),
    }
}

pub fn deserialize_i32<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    // Retain the numeric lexeme so Java BigDecimal-to-int conversion doesn't
    // lose low bits through an intermediate f64.
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    fastjson_integer_raw::<D::Error>(raw.get(), i32::MIN as i128, i32::MAX as i128, false)
        .map(|value| value as i32)
}

pub fn deserialize_i64_reject_object<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    fastjson_integer_raw::<D::Error>(raw.get(), i64::MIN as i128, i64::MAX as i128, true)
        .map(|value| value as i64)
}

pub fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let normalized = fastjson_number_token_from_raw(raw.get())
        .and_then(|token| normalize_fastjson_number_token(&token));
    let value = if let Some(normalized) = normalized {
        serde_json::from_str::<Value>(&normalized).map_err(de::Error::custom)?
    } else {
        serde_json::from_str::<Value>(raw.get()).map_err(de::Error::custom)?
    };
    match value {
        Value::Null => Ok(false),
        Value::Bool(value) => Ok(value),
        Value::Number(value) => {
            let value = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()));
            Ok(matches!(value, Some(-1 | 1)))
        }
        Value::String(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Value::String(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        value => Err(de::Error::custom(format!(
            "Fastjson cannot coerce {value} to a Java boolean"
        ))),
    }
}

pub fn serialize_nullable_long_string_map<S>(
    value: &Option<NullableLongStringMap>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(entries) = value else {
        return serializer.serialize_none();
    };
    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        let key = key.map_or_else(|| "null".to_owned(), |key| key.to_string());
        map.serialize_entry(&key, value)?;
    }
    map.end()
}

pub fn deserialize_optional_long_string_map<'de, D>(
    deserializer: D,
) -> Result<Option<NullableLongStringMap>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let normalized = normalize_fastjson_integer_map_keys(raw.get());
    let FastjsonRawEntries(entries) =
        serde_json::from_str::<FastjsonRawEntries>(&normalized).map_err(de::Error::custom)?;
    let mut result = BTreeMap::new();
    for (key, value) in entries {
        let key = if key == "null" {
            None
        } else {
            Some(fastjson_integer::<D::Error>(
                &Value::String(key),
                i64::MIN as i128,
                i64::MAX as i128,
            )? as i64)
        };
        let value = if value.get().trim() == "null" {
            None
        } else {
            Some(
                fastjson_string_value_from_raw::<D::Error>(value.get())?
                    .java_string()
                    .map_err(de::Error::custom)?,
            )
        };
        result.insert(key, value);
    }
    Ok(Some(result))
}

pub fn deserialize_optional_int_map<'de, D>(
    deserializer: D,
) -> Result<Option<BTreeMap<i32, i32>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let normalized = normalize_fastjson_integer_map_keys(raw.get());
    let FastjsonRawEntries(entries) =
        serde_json::from_str::<FastjsonRawEntries>(&normalized).map_err(de::Error::custom)?;
    let mut result = BTreeMap::new();
    for (key, value) in entries {
        let key =
            fastjson_integer::<D::Error>(&Value::String(key), i32::MIN as i128, i32::MAX as i128)?
                as i32;
        if value.get().trim() == "null" {
            return Err(de::Error::custom(
                "Fastjson mapping ID value causes Java NullPointerException",
            ));
        }
        let value = fastjson_integer_raw::<D::Error>(
            value.get(),
            i32::MIN as i128,
            i32::MAX as i128,
            false,
        )?;
        result.insert(key, value as i32);
    }
    Ok(Some(result))
}

pub fn deserialize_optional_string_list_map<'de, D>(
    deserializer: D,
) -> Result<Option<NullableStringListTable>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let entries =
        serde_json::from_str::<BTreeMap<String, Box<serde_json::value::RawValue>>>(raw.get())
            .map_err(de::Error::custom)?;
    let mut result = BTreeMap::new();
    for (key, value) in entries {
        let raw_value = value.get().trim();
        let value = if raw_value == "null" {
            None
        } else if raw_value.starts_with('"') {
            let value = serde_json::from_str::<String>(raw_value).map_err(de::Error::custom)?;
            Some(if value.is_empty() {
                Vec::new()
            } else {
                vec![Some(value)]
            })
        } else if raw_value.starts_with('{') {
            Some(vec![fastjson_nullable_string_from_raw::<D::Error>(
                raw_value,
            )?])
        } else {
            let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw_value)
                .map_err(|_| {
                de::Error::custom("expected a Fastjson string, object, array, or null")
            })?;
            Some(
                values
                    .iter()
                    .map(|value| fastjson_nullable_string_from_raw::<D::Error>(value.get()))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };
        result.insert(key, value);
    }
    Ok(Some(result))
}

pub fn deserialize_optional_nullable_string_list<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }
    if raw.starts_with('"') {
        let value = serde_json::from_str::<String>(raw).map_err(de::Error::custom)?;
        return Ok(Some(if value.is_empty() {
            Vec::new()
        } else {
            vec![Some(value)]
        }));
    }
    if raw.starts_with('[') {
        let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?;
        return values
            .iter()
            .map(|value| fastjson_nullable_string_from_raw::<D::Error>(value.get()))
            .collect::<Result<Vec<_>, _>>()
            .map(Some);
    }
    Ok(None)
}

/// Decode a Java list field that Fastjson accepts as an array, a singleton
/// object, or null. Preserve null rows and parse each DTO from its raw token so
/// its own Fastjson-compatible duplicate-property and scalar rules still run.
pub fn deserialize_optional_nullable_vec_or_single<'de, D, T>(
    deserializer: D,
) -> Result<Option<Vec<Option<T>>>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }
    if raw.starts_with('[') {
        let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?;
        return values
            .iter()
            .map(|value| {
                if value.get().trim() == "null" {
                    Ok(None)
                } else {
                    let normalized = normalize_json_input(value.get());
                    serde_json::from_str::<T>(&normalized)
                        .map(Some)
                        .map_err(de::Error::custom)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some);
    }
    if raw.starts_with('{') {
        let normalized = normalize_json_input(raw);
        return serde_json::from_str::<T>(&normalized)
            .map(|value| Some(vec![Some(value)]))
            .map_err(de::Error::custom);
    }
    Err(de::Error::custom(
        "expected a JSON array, object, or null for a Java List field",
    ))
}

pub fn deserialize_optional_nullable_key_string_map<'de, D>(
    deserializer: D,
) -> Result<Option<NullableKeyStringMap>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let entries =
        serde_json::from_str::<BTreeMap<String, Box<serde_json::value::RawValue>>>(raw.get())
            .map_err(de::Error::custom)?;
    entries
        .into_iter()
        .map(|(key, value)| {
            fastjson_nullable_string_from_raw::<D::Error>(value.get())
                .map(|value| (Some(key), value))
        })
        .collect::<Result<NullableKeyStringMap, _>>()
        .map(Some)
}

pub fn deserialize_optional_nullable_string_index_map<'de, D>(
    deserializer: D,
) -> Result<Option<IndexMap<String, Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let entries =
        serde_json::from_str::<IndexMap<String, Box<serde_json::value::RawValue>>>(raw.get())
            .map_err(de::Error::custom)?;
    entries
        .into_iter()
        .map(|(key, value)| {
            fastjson_nullable_string_from_raw::<D::Error>(value.get()).map(|value| (key, value))
        })
        .collect::<Result<IndexMap<_, _>, _>>()
        .map(Some)
}

fn fastjson_nullable_string_from_raw<E>(raw: &str) -> Result<Option<String>, E>
where
    E: de::Error,
{
    if raw.trim() == "null" {
        return Ok(None);
    }
    fastjson_string_value_from_raw::<E>(raw)?
        .java_string()
        .map(Some)
        .map_err(de::Error::custom)
}

pub fn deserialize_optional_nullable_string_set<'de, D>(
    deserializer: D,
) -> Result<Option<HashSet<Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = FastjsonStringValue::deserialize(deserializer)?;
    match value {
        FastjsonStringValue::Null => Ok(None),
        FastjsonStringValue::String(value) => Ok(Some(HashSet::from([Some(value)]))),
        value @ FastjsonStringValue::Object(_) => value
            .java_string()
            .map(|value| Some(HashSet::from([Some(value)])))
            .map_err(de::Error::custom),
        FastjsonStringValue::Array(values) => values
            .into_iter()
            .map(|value| match value {
                FastjsonStringValue::Null => Ok(None),
                value => value.java_string().map(Some).map_err(de::Error::custom),
            })
            .collect::<Result<HashSet<_>, _>>()
            .map(Some),
        _ => Err(de::Error::custom(
            "expected a JSON array, string, object, or null",
        )),
    }
}

pub fn deserialize_optional_nullable_int_set<'de, D>(
    deserializer: D,
) -> Result<Option<HashSet<Option<i32>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }
    let values = if raw.starts_with('[') {
        serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?
    } else {
        vec![
            serde_json::from_str::<Box<serde_json::value::RawValue>>(raw)
                .map_err(de::Error::custom)?,
        ]
    };
    values
        .iter()
        .map(|value| fastjson_nullable_int_set_value::<D::Error>(value.get()))
        .collect::<Result<HashSet<_>, _>>()
        .map(Some)
}

fn fastjson_nullable_int_set_value<E>(raw: &str) -> Result<Option<i32>, E>
where
    E: de::Error,
{
    let raw = raw.trim();
    if raw == "null" {
        return Ok(None);
    }
    if raw.starts_with('{') {
        return Ok(Some(0));
    }
    if raw == "true" {
        return Ok(Some(1));
    }
    if raw == "false" {
        return Ok(Some(0));
    }
    fastjson_integer_raw::<E>(raw, i32::MIN as i128, i32::MAX as i128, false)
        .map(|value| Some(value as i32))
}

fn fastjson_integer<E>(value: &Value, minimum: i128, maximum: i128) -> Result<i128, E>
where
    E: de::Error,
{
    match value {
        Value::Null => Ok(0),
        Value::Bool(value) => Ok(i128::from(*value)),
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                return checked_integer::<E>(integer as i128, minimum, maximum);
            }
            if let Some(integer) = value.as_u64() {
                return checked_integer::<E>(integer as i128, minimum, maximum);
            }
            let number = value
                .as_f64()
                .ok_or_else(|| E::custom("Fastjson numeric value is not representable"))?;
            if !number.is_finite() {
                return Err(E::custom("Fastjson numeric value is not finite"));
            }
            // Fastjson's primitive numeric conversion truncates decimal
            // values; narrowing follows Java Number.intValue/longValue.
            let truncated = number.trunc() as i128;
            if minimum == i32::MIN as i128 && maximum == i32::MAX as i128 {
                Ok((truncated as i32) as i128)
            } else {
                Ok((truncated as i64) as i128)
            }
        }
        Value::String(value) => {
            if let Ok(integer) = value.parse::<i128>() {
                return checked_integer::<E>(integer, minimum, maximum);
            }
            if !value.contains(['.', 'e', 'E']) {
                return Err(E::custom("Fastjson cannot coerce string to integer"));
            }
            let bits = if minimum == i32::MIN as i128 && maximum == i32::MAX as i128 {
                32
            } else {
                64
            };
            decimal_integer::<E>(value, bits)
        }
        Value::Object(entries) if entries.is_empty() => Ok(0),
        _ => Err(E::custom("Fastjson cannot coerce JSON value to integer")),
    }
}

fn fastjson_integer_raw<E>(
    raw: &str,
    minimum: i128,
    maximum: i128,
    reject_object: bool,
) -> Result<i128, E>
where
    E: de::Error,
{
    let raw = raw.trim();
    if let Some(token) = fastjson_number_token_from_raw(raw) {
        let normalized = normalize_fastjson_number_token(&token)
            .ok_or_else(|| E::custom("Fastjson cannot coerce numeric token to integer"))?;
        let bits = if minimum == i32::MIN as i128 && maximum == i32::MAX as i128 {
            32
        } else {
            64
        };
        return decimal_integer::<E>(&normalized, bits);
    }
    if raw.starts_with('{') {
        if reject_object {
            return Err(E::custom(
                "Fastjson cannot coerce JSON object to a Java long",
            ));
        }
        let value = serde_json::from_str::<Value>(raw).map_err(E::custom)?;
        return fastjson_integer::<E>(&value, minimum, maximum);
    }
    if raw.starts_with('[') {
        return Err(E::custom("Fastjson cannot coerce JSON value to integer"));
    }
    if raw.starts_with('"') {
        let value = serde_json::from_str::<String>(raw).map_err(E::custom)?;
        return fastjson_integer::<E>(&Value::String(value), minimum, maximum);
    }
    if raw == "null" {
        return Ok(0);
    }
    if raw == "true" {
        return Ok(1);
    }
    if raw == "false" {
        return Ok(0);
    }
    if raw.contains(['.', 'e', 'E']) {
        let bits = if minimum == i32::MIN as i128 && maximum == i32::MAX as i128 {
            32
        } else {
            64
        };
        return decimal_integer::<E>(raw, bits);
    }
    let integer = raw
        .parse::<i128>()
        .map_err(|_| E::custom("Fastjson integer value is out of range"))?;
    checked_integer::<E>(integer, minimum, maximum)
}

/// Converts a Java BigDecimal-compatible token to the low 32 or 64 bits of
/// its truncated integer value, matching `Number.intValue/longValue`.
fn decimal_integer<E>(value: &str, bits: u32) -> Result<i128, E>
where
    E: de::Error,
{
    let bytes = value.as_bytes();
    let (negative, start) = match bytes.first() {
        Some(b'-') => (true, 1),
        Some(b'+') => (false, 1),
        _ => (false, 0),
    };
    let exponent_index = bytes[start..]
        .iter()
        .position(|byte| matches!(byte, b'e' | b'E'))
        .map(|index| index + start);
    let mantissa_end = exponent_index.unwrap_or(bytes.len());
    let exponent = if let Some(index) = exponent_index {
        parse_decimal_exponent::<E>(&bytes[index + 1..])?
    } else {
        0
    };
    let mantissa = &bytes[start..mantissa_end];
    let decimal_index = mantissa.iter().position(|byte| *byte == b'.');
    let (integer_digits, fractional_digits) = if let Some(index) = decimal_index {
        if mantissa[index + 1..].contains(&b'.') {
            return Err(E::custom("Fastjson cannot coerce string to integer"));
        }
        (&mantissa[..index], &mantissa[index + 1..])
    } else {
        (mantissa, &[][..])
    };
    if integer_digits
        .iter()
        .chain(fractional_digits)
        .any(|byte| !byte.is_ascii_digit())
        || (integer_digits.is_empty() && fractional_digits.is_empty())
    {
        return Err(E::custom("Fastjson cannot coerce string to integer"));
    }

    let total_digits = integer_digits.len() + fractional_digits.len();
    let total_digits_i64 = i64::try_from(total_digits)
        .map_err(|_| E::custom("Fastjson numeric string is too long"))?;
    let fractional_digits_i64 = i64::try_from(fractional_digits.len())
        .map_err(|_| E::custom("Fastjson numeric string is too long"))?;
    let scale = fractional_digits_i64.saturating_sub(exponent);
    let (digits_to_take, appended_zeroes) = if scale > 0 {
        if scale >= total_digits_i64 {
            return Ok(0);
        }
        ((total_digits_i64 - scale) as usize, 0)
    } else {
        (total_digits, scale.saturating_neg() as u64)
    };

    let mut low_bits = 0_u64;
    for byte in integer_digits
        .iter()
        .chain(fractional_digits)
        .take(digits_to_take)
    {
        low_bits = low_bits
            .wrapping_mul(10)
            .wrapping_add(u64::from(byte - b'0'));
    }
    low_bits = low_bits.wrapping_mul(pow10_wrapping(appended_zeroes));
    if negative {
        low_bits = low_bits.wrapping_neg();
    }

    Ok(if bits == 32 {
        (low_bits as u32 as i32) as i128
    } else {
        (low_bits as i64) as i128
    })
}

fn parse_decimal_exponent<E>(bytes: &[u8]) -> Result<i64, E>
where
    E: de::Error,
{
    let (negative, digits) = match bytes.first() {
        Some(b'-') => (true, &bytes[1..]),
        Some(b'+') => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    if digits.is_empty() || digits.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err(E::custom("Fastjson cannot coerce string to integer"));
    }
    let magnitude = digits.iter().fold(0_u64, |value, byte| {
        value
            .saturating_mul(10)
            .saturating_add(u64::from(byte - b'0'))
    });
    Ok(if negative {
        if magnitude > i64::MAX as u64 {
            i64::MIN
        } else {
            -(magnitude as i64)
        }
    } else {
        magnitude.min(i64::MAX as u64) as i64
    })
}

fn pow10_wrapping(mut exponent: u64) -> u64 {
    let mut result = 1_u64;
    let mut power = 10_u64;
    while exponent != 0 {
        if exponent & 1 != 0 {
            result = result.wrapping_mul(power);
        }
        power = power.wrapping_mul(power);
        exponent >>= 1;
    }
    result
}

fn checked_integer<E>(value: i128, minimum: i128, maximum: i128) -> Result<i128, E>
where
    E: de::Error,
{
    if (minimum..=maximum).contains(&value) {
        Ok(value)
    } else {
        Err(E::custom("Fastjson integer is out of range"))
    }
}

#[cfg(test)]
mod syntax_compat_tests {
    use super::{from_fastjson_str, normalize_json_input};
    use serde::Deserialize;
    use serde_json::{Value, json};

    #[derive(Debug, Deserialize)]
    struct NumericAndStringFields {
        #[serde(default, deserialize_with = "super::deserialize_nullable_string")]
        topic: Option<String>,
        #[serde(default, deserialize_with = "super::deserialize_i32")]
        sub_version: i32,
    }

    #[test]
    fn parses_single_quotes_comments_and_trailing_commas() {
        let input = r#"/* prefix */ {'topic':'it\'s // literal /* text */', 'tagsSet':['tag',], 'subVersion':+02,} // suffix"#;
        let decoded = from_fastjson_str::<Value>(input).unwrap();
        assert_eq!(
            decoded,
            json!({
                "topic": "it's // literal /* text */",
                "tagsSet": ["tag"],
                "subVersion": 2
            })
        );
    }

    #[test]
    fn does_not_treat_comment_markers_inside_double_quoted_strings_as_comments() {
        let decoded = from_fastjson_str::<Value>(r#"{"topic":"/* text */ // text"}"#).unwrap();
        assert_eq!(decoded["topic"], "/* text */ // text");
    }

    #[test]
    fn quotes_fastjson_integer_object_keys_for_serde() {
        let normalized = super::normalize_fastjson_integer_map_keys("{2:1,-1:4}");
        let decoded =
            serde_json::from_str::<std::collections::BTreeMap<i32, i32>>(&normalized).unwrap();
        assert_eq!(decoded, std::collections::BTreeMap::from([(2, 1), (-1, 4)]));
    }

    #[test]
    fn preserves_raw_control_characters_inside_fastjson_strings() {
        let input = "{\"a\u{0000}b\":\"x\u{0001}\ny\u{001f}\"}";
        let decoded = from_fastjson_str::<Value>(input).unwrap();
        assert_eq!(decoded["a\u{0000}b"], "x\u{0001}\ny\u{001f}");
    }

    #[test]
    fn fastjson_eof_marker_ends_the_document_and_ignores_the_suffix() {
        let decoded = from_fastjson_str::<Value>("{}\u{001a} \u{001a}").unwrap();
        assert_eq!(decoded, Value::Object(Default::default()));
        let decoded = from_fastjson_str::<Value>("{}\u{001a} ignored {\"trailing\":true}").unwrap();
        assert_eq!(decoded, Value::Object(Default::default()));
        let input = "{\"topic\":\"inside\u{001a}string\"}\u{001a}ignored".to_owned();
        let decoded = from_fastjson_str::<Value>(&input).unwrap();
        assert_eq!(decoded["topic"], "inside\u{001a}string");
        assert!(from_fastjson_str::<Value>("\u{001a}{}").is_err());
        assert!(from_fastjson_str::<Value>("{\u{001a}}").is_err());
    }

    #[test]
    fn parses_fastjson_numeric_suffix_and_decimal_point_forms() {
        let decoded =
            from_fastjson_str::<Value>("{\"values\":[1L,1D,1F,1B,1S,-.5,1.,1.e2D,1e2F]}").unwrap();
        assert_eq!(
            decoded,
            serde_json::json!({"values": [1, 1.0, 1.0, 1, 1, -0.5, 1.0, 100.0, 100.0]})
        );
        assert_eq!(
            from_fastjson_str::<Value>(r#"{"value":.5}"#).unwrap(),
            serde_json::json!({"value": 0.5})
        );
    }

    #[test]
    fn preserves_leading_dot_number_field_coercions() {
        let numeric: NumericAndStringFields = from_fastjson_str(r#"{"sub_version":.5}"#).unwrap();
        assert_eq!(numeric.sub_version, 0);
        assert!(from_fastjson_str::<NumericAndStringFields>(r#"{"topic":.5}"#).is_err());

        let explicit_positive: NumericAndStringFields =
            from_fastjson_str(r#"{"topic":+.5}"#).unwrap();
        assert_eq!(explicit_positive.topic.as_deref(), Some("0.5"));

        let suffixed_numeric: NumericAndStringFields =
            from_fastjson_str(r#"{"sub_version":.5F}"#).unwrap();
        assert_eq!(suffixed_numeric.sub_version, 0);
        assert!(from_fastjson_str::<NumericAndStringFields>(r#"{"topic":.5F}"#).is_err());
    }

    #[test]
    fn accepts_fastjson_incomplete_exponents_as_their_numeric_prefix() {
        assert_eq!(
            normalize_json_input(r#"{"sub_version":1e+}"#),
            r#"{"sub_version":1}"#
        );
        let integer: NumericAndStringFields = from_fastjson_str(r#"{"sub_version":1e+}"#).unwrap();
        assert_eq!(integer.sub_version, 1);
        let plain: NumericAndStringFields = from_fastjson_str(r#"{"topic":1e+}"#).unwrap();
        assert_eq!(plain.topic.as_deref(), Some("1"));
        let float_suffix: NumericAndStringFields = from_fastjson_str(r#"{"topic":1e+F}"#).unwrap();
        assert_eq!(float_suffix.topic.as_deref(), Some("1.0"));
        let trailing_dot: NumericAndStringFields = from_fastjson_str(r#"{"topic":1.E}"#).unwrap();
        assert_eq!(trailing_dot.topic.as_deref(), Some("1"));
    }

    #[test]
    fn preserves_user_strings_matching_the_internal_number_marker() {
        let value = format!("{} .5", super::FASTJSON_NUMBER_TOKEN_MARKER_PREFIX);
        let raw = serde_json::to_string(&json!({"topic": value})).unwrap();
        let decoded: Value = from_fastjson_str(&raw).unwrap();
        assert_eq!(decoded["topic"], value);

        let marker = super::fastjson_number_token_marker(".5");
        let normalized = normalize_json_input(&marker);
        assert_ne!(normalized, marker);
        assert_eq!(
            serde_json::from_str::<String>(&normalized).unwrap(),
            format!("{}.5", super::FASTJSON_NUMBER_TOKEN_MARKER_PREFIX)
        );
    }

    #[test]
    fn formats_fastjson_float_and_double_tokens_like_java() {
        assert_eq!(
            super::java_float_to_string("1.23456789".parse().unwrap()),
            "1.2345679"
        );
        assert_eq!(super::java_float_to_string(10_000_000.0), "1.0E7");
        assert_eq!(super::java_double_to_string(100.0), "100.0");
        assert_eq!(super::java_double_to_string(10_000_000.0), "1.0E7");
        assert_eq!(super::java_double_to_string(0.001), "0.001");
        assert_eq!(super::java_double_to_string(0.0001), "1.0E-4");
        assert_eq!(super::java_double_to_string(-0.0), "-0.0");
    }
}
