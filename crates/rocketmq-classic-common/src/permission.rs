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
    crate::utils::java_number::parse_long(value)?
        .try_into()
        .ok()
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
