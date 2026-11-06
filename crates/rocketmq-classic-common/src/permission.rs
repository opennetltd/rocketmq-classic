use thiserror::Error;

/// Java `org.apache.rocketmq.common.constant.PermName` permission masks.
pub struct PermName;

/// Failure from Java-compatible `Integer.parseInt` used by permission strings.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("For input string: \"{0}\"")]
pub struct PermissionParseError(String);

impl PermName {
    pub const INDEX_PERM_PRIORITY: i32 = 3;
    pub const INDEX_PERM_READ: i32 = 2;
    pub const INDEX_PERM_WRITE: i32 = 1;
    pub const INDEX_PERM_INHERIT: i32 = 0;

    pub const PERM_PRIORITY: i32 = 1 << Self::INDEX_PERM_PRIORITY;
    pub const PERM_READ: i32 = 1 << Self::INDEX_PERM_READ;
    pub const PERM_WRITE: i32 = 1 << Self::INDEX_PERM_WRITE;
    pub const PERM_INHERIT: i32 = 1 << Self::INDEX_PERM_INHERIT;

    pub fn perm_to_string(permission: i32) -> String {
        let mut result = ['-', '-', '-'];
        if Self::is_readable(permission) {
            result[0] = 'R';
        }
        if Self::is_writeable(permission) {
            result[1] = 'W';
        }
        if Self::is_inherited(permission) {
            result[2] = 'X';
        }
        result.iter().collect()
    }

    pub const fn is_readable(permission: i32) -> bool {
        permission & Self::PERM_READ == Self::PERM_READ
    }

    pub const fn is_writeable(permission: i32) -> bool {
        permission & Self::PERM_WRITE == Self::PERM_WRITE
    }

    pub const fn is_inherited(permission: i32) -> bool {
        permission & Self::PERM_INHERIT == Self::PERM_INHERIT
    }

    pub const fn is_valid(permission: i32) -> bool {
        permission >= 0 && permission < Self::PERM_PRIORITY
    }

    /// Parses and validates the Java `isValid(String)` overload.
    pub fn is_valid_string(permission: &str) -> Result<bool, PermissionParseError> {
        parse_java_i32(permission)
            .map(Self::is_valid)
            .ok_or_else(|| PermissionParseError(permission.to_owned()))
    }

    pub const fn is_priority(permission: i32) -> bool {
        permission & Self::PERM_PRIORITY == Self::PERM_PRIORITY
    }

    pub const fn is_accessible(permission: i32) -> bool {
        Self::is_readable(permission) || Self::is_writeable(permission)
    }
}

fn parse_java_i32(value: &str) -> Option<i32> {
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
    use super::{PermName, PermissionParseError};

    #[test]
    fn permission_masks_and_format_match_java_values() {
        assert_eq!(PermName::perm_to_string(0), "---");
        assert_eq!(PermName::perm_to_string(1), "--X");
        assert_eq!(PermName::perm_to_string(4), "R--");
        assert_eq!(PermName::perm_to_string(6), "RW-");
        assert_eq!(PermName::perm_to_string(7), "RWX");
        assert_eq!(PermName::perm_to_string(15), "RWX");
        assert!(!PermName::is_valid(-1));
        assert!(PermName::is_valid(7));
        assert!(!PermName::is_valid(8));
        assert!(PermName::is_priority(8));
        assert!(PermName::is_accessible(2));
        assert!(!PermName::is_accessible(1));
    }

    #[test]
    fn permission_string_parser_matches_java_integer_inputs() {
        for (input, expected) in [("0", true), ("+7", true), ("٧", true), ("8", false)] {
            assert_eq!(PermName::is_valid_string(input), Ok(expected), "{input:?}");
        }
        for input in ["", " 7", "2147483648", "--1"] {
            assert_eq!(
                PermName::is_valid_string(input),
                Err(PermissionParseError(input.to_owned())),
                "{input:?}"
            );
        }
    }
}
