/// Parses the decimal integer subset accepted by Java's `Integer.parseInt`.
///
/// Java applies `Character.digit` to UTF-16 code units, so BMP decimal digits
/// are accepted while supplementary mathematical digits are not.
pub(super) fn parse_i32(value: &str) -> Option<i32> {
    let mut units = value.encode_utf16().peekable();
    let negative = match units.peek().copied()? {
        0x2d => {
            units.next();
            true
        }
        0x2b => {
            units.next();
            false
        }
        _ => false,
    };
    units.peek()?;
    let limit = if negative { i32::MIN } else { -i32::MAX };
    let multiply_limit = limit / 10;
    let mut result = 0_i32;
    for unit in units {
        let digit = i32::from(java_decimal_digit(unit)?);
        if result < multiply_limit {
            return None;
        }
        result *= 10;
        if result < limit + digit {
            return None;
        }
        result -= digit;
    }
    Some(if negative { result } else { -result })
}

fn java_decimal_digit(unit: u16) -> Option<u16> {
    const ZEROS: &[u16] = &[
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
        0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
        0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];
    ZEROS
        .iter()
        .find_map(|zero| unit.checked_sub(*zero).filter(|digit| *digit < 10))
}

#[cfg(test)]
mod tests {
    use super::parse_i32;

    #[test]
    fn parses_java_decimal_inputs_and_boundaries() {
        assert_eq!(parse_i32("+123"), Some(123));
        assert_eq!(parse_i32("-2147483648"), Some(i32::MIN));
        assert_eq!(parse_i32("2147483647"), Some(i32::MAX));
        assert_eq!(parse_i32("١٢٣"), Some(123));
        assert_eq!(parse_i32(""), None);
        assert_eq!(parse_i32(" 123"), None);
        assert_eq!(parse_i32("2147483648"), None);
        assert_eq!(parse_i32("𝟙"), None);
    }
}
