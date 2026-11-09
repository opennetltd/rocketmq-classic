//! Private Fastjson coercions needed by the common `MessageQueue` DTO.

use std::fmt;

use crate::utils::fastjson_number::{checked_integer, decimal_integer, fastjson_integer};
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
