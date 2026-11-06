/// Java signed decimal parsing uses Character.digit on UTF-16 units, not only
/// ASCII digits. Negative accumulation also represents the signed minimum.
pub(crate) fn parse_long(value: &str) -> Option<i64> {
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
    let mut result = 0i64;
    for unit in units {
        result = result
            .checked_mul(10)?
            .checked_sub(i64::from(decimal_digit(unit)?))?;
    }
    if negative {
        Some(result)
    } else {
        result.checked_neg()
    }
}

pub(crate) fn decimal_digit(unit: u16) -> Option<u16> {
    // Exhaustively verified against Character.digit(char, 10) in the pinned
    // Java route oracle. Shared by Integer.parseInt and Long.parseLong paths.
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
