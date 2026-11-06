//! Private Fastjson coercions needed by the common `MessageQueue` DTO.

use std::fmt;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, de, de::DeserializeOwned};
use serde_json::Value;

enum FastjsonStringValue {
    Null,
    Bool(bool),
    Number(String),
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
            serde_json::Number::from_f64(value)
                .ok_or_else(|| E::custom("Fastjson numeric value is not finite"))?
                .to_string()
        } else {
            raw.to_owned()
        };
        return Ok(FastjsonStringValue::Number(number));
    }
    Err(E::custom("expected a JSON value"))
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
