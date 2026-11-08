//! Shared Fastjson integer conversion; raw-token policy remains with each DTO parser.

use serde::de;
use serde_json::Value;

pub fn fastjson_integer<E>(value: &Value, minimum: i128, maximum: i128) -> Result<i128, E>
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

/// Converts a Java BigDecimal-compatible token to the low 32 or 64 bits of
/// its truncated integer value, matching `Number.intValue/longValue`.
pub fn decimal_integer<E>(value: &str, bits: u32) -> Result<i128, E>
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

pub fn checked_integer<E>(value: i128, minimum: i128, maximum: i128) -> Result<i128, E>
where
    E: de::Error,
{
    if (minimum..=maximum).contains(&value) {
        Ok(value)
    } else {
        Err(E::custom("Fastjson integer is out of range"))
    }
}
