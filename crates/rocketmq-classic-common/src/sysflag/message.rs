pub const COMPRESSED_FLAG: i32 = 0x1;
pub const MULTI_TAGS_FLAG: i32 = 1 << 1;
pub const TRANSACTION_NOT_TYPE: i32 = 0;
pub const TRANSACTION_PREPARED_TYPE: i32 = 1 << 2;
pub const TRANSACTION_COMMIT_TYPE: i32 = 2 << 2;
pub const TRANSACTION_ROLLBACK_TYPE: i32 = 3 << 2;
pub const BORNHOST_V6_FLAG: i32 = 1 << 4;
pub const STOREHOSTADDRESS_V6_FLAG: i32 = 1 << 5;
pub const NEED_UNWRAP_FLAG: i32 = 1 << 6;
pub const INNER_BATCH_FLAG: i32 = 1 << 7;
pub const COMPRESSION_LZ4_TYPE: i32 = 1 << 8;
pub const COMPRESSION_ZSTD_TYPE: i32 = 2 << 8;
pub const COMPRESSION_ZLIB_TYPE: i32 = 3 << 8;
pub const COMPRESSION_TYPE_COMPARATOR: i32 = 7 << 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Lz4,
    Zstd,
    Zlib,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompressionTypeError {
    #[error("Name is null")]
    NullEnumName,
    #[error("No enum constant org.apache.rocketmq.common.compression.CompressionType.{0}")]
    UnknownEnumName(String),
    #[error("compression type name is null")]
    NullName,
    #[error("Unsupported compress type name: {0}")]
    UnsupportedName(String),
    #[error("Unknown compress type value: {0}")]
    UnknownValue(i32),
}

impl CompressionType {
    pub const fn values() -> [Self; 3] {
        [Self::Lz4, Self::Zstd, Self::Zlib]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, CompressionTypeError> {
        match name {
            None => Err(CompressionTypeError::NullEnumName),
            Some("LZ4") => Ok(Self::Lz4),
            Some("ZSTD") => Ok(Self::Zstd),
            Some("ZLIB") => Ok(Self::Zlib),
            Some(other) => Err(CompressionTypeError::UnknownEnumName(other.to_owned())),
        }
    }

    pub fn of(name: Option<&str>) -> Result<Self, CompressionTypeError> {
        let name = name.ok_or(CompressionTypeError::NullName)?;
        // java.lang.String.trim() removes only UTF-16 code units <= U+0020.
        let uppercase_name = crate::topic::java_default_locale_uppercase(
            name.trim_matches(|character: char| character <= '\u{0020}'),
        );
        match uppercase_name.as_str() {
            "LZ4" => Ok(Self::Lz4),
            "ZSTD" => Ok(Self::Zstd),
            "ZLIB" => Ok(Self::Zlib),
            _ => Err(CompressionTypeError::UnsupportedName(name.to_owned())),
        }
    }

    pub fn find_by_value(value: i32) -> Result<Self, CompressionTypeError> {
        match value {
            0 | 3 => Ok(Self::Zlib),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            other => Err(CompressionTypeError::UnknownValue(other)),
        }
    }

    pub const fn get_value(self) -> i32 {
        match self {
            Self::Lz4 => 1,
            Self::Zstd => 2,
            Self::Zlib => 3,
        }
    }

    pub const fn get_compression_flag(self) -> i32 {
        match self {
            Self::Lz4 => COMPRESSION_LZ4_TYPE,
            Self::Zstd => COMPRESSION_ZSTD_TYPE,
            Self::Zlib => COMPRESSION_ZLIB_TYPE,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Lz4 => "LZ4",
            Self::Zstd => "ZSTD",
            Self::Zlib => "ZLIB",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Lz4 => 0,
            Self::Zstd => 1,
            Self::Zlib => 2,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

pub fn clear_compressed_flag(flag: i32) -> i32 {
    flag & !COMPRESSED_FLAG
}
pub fn check(flag: i32, expected_flag: i32) -> bool {
    flag & expected_flag != 0
}

pub fn transaction_value(flag: i32) -> i32 {
    flag & TRANSACTION_ROLLBACK_TYPE
}
pub fn reset_transaction_value(flag: i32, value: i32) -> i32 {
    (flag & !TRANSACTION_ROLLBACK_TYPE) | value
}
pub fn compression_type(flag: i32) -> Result<CompressionType, CompressionTypeError> {
    CompressionType::find_by_value((flag & COMPRESSION_TYPE_COMPARATOR) >> 8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_type_and_flag_helpers_preserve_other_bits() {
        assert!(check(COMPRESSED_FLAG | 0x4000, COMPRESSED_FLAG));
        assert_eq!(clear_compressed_flag(COMPRESSED_FLAG | 0x4000), 0x4000);
        assert_eq!(compression_type(0), Ok(CompressionType::Zlib));
        assert_eq!(
            compression_type(COMPRESSION_ZSTD_TYPE),
            Ok(CompressionType::Zstd)
        );
        assert_eq!(
            compression_type(4 << 8),
            Err(CompressionTypeError::UnknownValue(4))
        );
    }

    #[test]
    fn compression_type_enum_and_flag_lookups_match_invoked_java_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let mut mutated_values = CompressionType::values();
        mutated_values[0] = CompressionType::Zlib;
        assert_eq!(CompressionType::values()[0], CompressionType::Lz4);

        for case in fixture["compressionTypeMethodCases"].as_array().unwrap() {
            let result = match case["case"].as_str().unwrap() {
                "constant" => {
                    let name = case["name"].as_str().unwrap();
                    let value = CompressionType::value_of_name(Some(name)).unwrap();
                    let values = CompressionType::values();
                    let expected_names = case["values"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|name| name.as_str().unwrap())
                        .collect::<Vec<_>>();
                    assert_eq!(value.name(), name, "{case}");
                    assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                    assert_eq!(
                        value.get_value(),
                        case["value"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                    assert_eq!(
                        value.get_compression_flag(),
                        case["compressionFlag"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                    assert_eq!(
                        value.ordinal(),
                        case["ordinal"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                    assert_eq!(
                        values.iter().map(|value| value.name()).collect::<Vec<_>>(),
                        expected_names,
                        "{case}"
                    );
                    assert_eq!(case["valueOfName"], name, "{case}");
                    assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                    let next = values[(value.ordinal() as usize + 1) % values.len()];
                    assert_eq!(
                        value.java_compare_to(next),
                        case["compareNext"].as_i64().unwrap() as i32,
                        "{case}"
                    );
                    assert_eq!(
                        CompressionType::find_by_value(value.get_value()),
                        Ok(value),
                        "{case}"
                    );
                    continue;
                }
                "enumNameLookup" => CompressionType::value_of_name(case["input"].as_str()),
                "of" => CompressionType::of(case["input"].as_str()),
                "findByValue" => {
                    CompressionType::find_by_value(case["input"].as_i64().unwrap() as i32)
                }
                "messageSysFlag" => compression_type(case["flag"].as_i64().unwrap() as i32),
                other => panic!("unknown Java CompressionType case: {other}"),
            };
            match result {
                Ok(value) => {
                    assert_eq!(case["outcome"], "OK", "{case}");
                    assert_eq!(case["decoded"], value.name(), "{case}");
                }
                Err(error) => {
                    let outcome = match &error {
                        CompressionTypeError::NullEnumName | CompressionTypeError::NullName => {
                            "NullPointerException"
                        }
                        CompressionTypeError::UnknownEnumName(_) => "IllegalArgumentException",
                        CompressionTypeError::UnsupportedName(_)
                        | CompressionTypeError::UnknownValue(_) => "RuntimeException",
                    };
                    assert_eq!(case["outcome"], outcome, "{case}");
                    if !matches!(error, CompressionTypeError::NullName) {
                        assert_eq!(case["message"], error.to_string(), "{case}");
                    }
                }
            }
        }
    }

    #[test]
    fn compression_type_of_matches_turkish_default_locale() {
        const CHILD_MARKER: &str = "ROCKETMQ_CLASSIC_TURKISH_COMPRESSION_TEST";

        if std::env::var_os(CHILD_MARKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "sysflag::message::tests::compression_type_of_matches_turkish_default_locale",
                ])
                .env("LC_ALL", "tr_TR.UTF-8")
                .env(CHILD_MARKER, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "Turkish-locale child test failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["compressionTypeTurkishLocaleCases"]
            .as_array()
            .expect("Java Turkish compression cases")
        {
            let input = case["input"].as_str().unwrap();
            match CompressionType::of(Some(input)) {
                Ok(value) => {
                    assert_eq!(case["outcome"], "OK", "{case}");
                    assert_eq!(case["decoded"], value.name(), "{case}");
                }
                Err(error) => {
                    assert_eq!(case["outcome"], "RuntimeException", "{case}");
                    assert_eq!(case["message"], error.to_string(), "{case}");
                }
            }
        }
    }

    #[test]
    fn check_tests_any_expected_mask_bit() {
        assert!(check(0b1010, 0b0010));
        assert!(!check(0b1000, 0b0010));
    }

    #[test]
    fn transaction_mask_is_replaced_without_changing_reserved_bits() {
        let flag = TRANSACTION_PREPARED_TYPE | 0x4000;
        assert_eq!(transaction_value(flag), TRANSACTION_PREPARED_TYPE);
        assert_eq!(
            reset_transaction_value(flag, TRANSACTION_COMMIT_TYPE),
            TRANSACTION_COMMIT_TYPE | 0x4000
        );
    }
}
