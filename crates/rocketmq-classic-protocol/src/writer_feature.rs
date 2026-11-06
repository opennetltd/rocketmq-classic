/// A Fastjson2 writer option accepted by inherited RocketMQ
/// `RemotingSerializable.encode(JSONWriter.Feature...)` adapters.
///
/// The variants and their order match the pinned Fastjson2 2.0.63 enum. The
/// adapters currently implement `FieldBased`, `WriteNullBooleanAsFalse`,
/// `WriteNulls`, `WriteMapNullValue`, `WriteNullListAsEmpty`,
/// `WriteNullStringAsEmpty`, `WriteNullNumberAsZero`, `NullAsDefaultValue`,
/// `WriteBooleanAsNumber`,
/// `WriteNonStringValueAsString`, `PrettyFormat`, `EscapeNoneAscii`,
/// `PrettyFormatWith2Space`, `PrettyFormatWith4Space`, `NotWriteEmptyArray`,
/// `IgnoreEmpty`, `NotWriteDefaultValue`, `WriteNonStringKeyAsString`,
/// `MapSortField`, `SortMapEntriesByKeys`, `LargeObject`, `WriteLongAsString`,
/// `WriteEnumUsingOrdinal`, `WriteEnumsUsingName`,
/// `WriteEnumUsingToString`, `IgnoreErrorGetter`, `UseSingleQuotes`,
/// `UnquoteFieldName`, `IgnoreNoneSerializable`, `ErrorOnNoneSerializable`,
/// `BrowserCompatible`,
/// `BrowserSecure`, `OptimizedForAscii`, `WriteNameAsSymbol`, `NotWriteRootClassName`,
/// `NotWriteHashMapArrayListClassName`, `NotWriteNumberClassName`, and
/// `NotWriteSetClassName`, `WriteByteArrayAsBase64`, `WritePairAsJavaBean`,
/// `WriteThrowableClassName`, `WriterUtilDateAsMillis`,
/// `WriteBigDecimalAsPlain`, and `WriteFloatSpecialAsString`.
/// `OptimizedForAscii` is output-preserving because the Rust writer already
/// emits UTF-8 JSON.
/// `WriteEnumsUsingName` and `WriteEnumUsingToString` preserve output for the
/// mapped DTO graph: the invoked enum values already serialize as their Java
/// names, and their `toString()` values match those names.
/// `IgnoreErrorGetter` preserves output because these Rust DTO adapters read
/// stored values directly and invoke no fallible Java-style getters.
/// `IgnoreNonFieldGetter` omits the getter-only QueueTimeSpan timestamp strings
/// and uses Fastjson's filtered map-key encoding for the mapped DTO graph.
/// `MapSortField` and `SortMapEntriesByKeys` preserve output because these
/// adapters serialize map-backed JSON objects in key order already; the Java
/// fixtures include populated maps and record no overrides for either flag.
/// `LargeObject` raises Fastjson2's writer array-capacity ceiling from 64 MiB
/// to 1 GiB. Rust's growable JSON output has no corresponding fixed ceiling, so
/// the flag needs no output or capacity change in these adapters.
/// `WriteNameAsSymbol` is output-preserving for RocketMQ's UTF-8 text JSON
/// encoder; Fastjson2 uses symbols in its JSONB type-name path. The four
/// `NotWrite*ClassName` features preserve output when supplied alone: the
/// mapped API does not enable `WriteClassName`, and pinned Fastjson only uses
/// three of these options while deciding whether to write type metadata; it
/// has no writer callsite for `NotWriteSetClassName`. `BeanToArray` serializes
/// mapped DTO beans in Java bean-property order, including typed bean values
/// nested in collections and the route/request-mode maps, while preserving
/// user JSON maps as maps. `WriteNulls` and `WriteMapNullValue` restore omitted
/// nullable fields from the mapped Java DTO schemas as explicit JSON nulls,
/// including nested typed DTOs. `WriteNullListAsEmpty` writes schema-identified
/// nullable collection properties as empty arrays, including nested DTOs. The
/// `WriteNullStringAsEmpty` option replaces null schema-identified String fields
/// and nullable String collection members with empty strings. The
/// `NullAsDefaultValue` option applies schema-identified String, collection,
/// map, and nested DTO defaults (`""`, `[]`, and `{}`); nullable enum fields
/// remain null as in Fastjson2. The
/// `ReferenceDetection` option emits Fastjson `$ref` objects for repeated
/// shared DTO handles.
/// `WriteClassName` writes RocketMQ's type metadata on root DTOs, polymorphic
/// set/list members, interface-typed maps, and MessageQueue map keys. It also
/// writes Fastjson's `Set` collection marker and the `L` suffix for small
/// values held in Java `long` fields. `NotWriteRootClassName`,
/// `NotWriteHashMapArrayListClassName`, and `NotWriteSetClassName` suppress
/// their corresponding type markers when combined with this feature.
/// `WriteNullNumberAsZero` option preserves output for the mapped DTO graph:
/// numeric bean fields are primitive, while Fastjson's map and collection
/// writers emit bare nulls for null entries independent of this flag.
/// The pinned 48-value enum is fully mapped for the supported RocketMQ DTO
/// adapters. Byte-array, Pair, Throwable, and `java.util.Date` options
/// preserve output because those values are absent from the mapped DTO graph;
/// BatchAck's BitSet uses RocketMQ's custom serializer. The DTO graph also has
/// no `BigDecimal`, `Float`, or `Double` fields, so their number-format options
/// preserve output.
/// `FieldBased` also preserves output for the mapped RocketMQ DTO graph: the
/// invoked Java fixtures produce the default bytes for every body, route, and
/// heartbeat case, including getter-derived QueueTimeSpan fields.
/// `WriteNullBooleanAsFalse` preserves output for the current body, route, and
/// heartbeat adapters: the 41 body, 8 route, and 3 heartbeat fixture encodings
/// have no feature-specific overrides. Other Java `RemotingSerializable`
/// types with boxed Boolean fields, including `BrokerReplicasInfo`, are outside
/// these client DTO adapters and are not covered by this mapping.
#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JavaJsonWriterFeature {
    FieldBased,
    IgnoreNoneSerializable,
    ErrorOnNoneSerializable,
    BeanToArray,
    WriteNulls,
    WriteMapNullValue,
    BrowserCompatible,
    NullAsDefaultValue,
    WriteBooleanAsNumber,
    /// Serialize booleans and numbers as JSON strings.
    WriteNonStringValueAsString,
    WriteClassName,
    NotWriteRootClassName,
    NotWriteHashMapArrayListClassName,
    NotWriteDefaultValue,
    WriteEnumsUsingName,
    WriteEnumUsingToString,
    IgnoreErrorGetter,
    PrettyFormat,
    ReferenceDetection,
    WriteNameAsSymbol,
    WriteBigDecimalAsPlain,
    UseSingleQuotes,
    MapSortField,
    WriteNullListAsEmpty,
    WriteNullStringAsEmpty,
    WriteNullNumberAsZero,
    WriteNullBooleanAsFalse,
    NotWriteEmptyArray,
    IgnoreEmpty,
    WriteNonStringKeyAsString,
    WritePairAsJavaBean,
    OptimizedForAscii,
    EscapeNoneAscii,
    WriteByteArrayAsBase64,
    IgnoreNonFieldGetter,
    LargeObject,
    WriteLongAsString,
    BrowserSecure,
    WriteEnumUsingOrdinal,
    WriteThrowableClassName,
    UnquoteFieldName,
    NotWriteSetClassName,
    NotWriteNumberClassName,
    SortMapEntriesByKeys,
    PrettyFormatWith2Space,
    PrettyFormatWith4Space,
    WriterUtilDateAsMillis,
    WriteFloatSpecialAsString,
}

impl JavaJsonWriterFeature {
    /// Returns all 48 features in the pinned Java enum order.
    pub const fn values() -> [Self; 48] {
        [
            Self::FieldBased,
            Self::IgnoreNoneSerializable,
            Self::ErrorOnNoneSerializable,
            Self::BeanToArray,
            Self::WriteNulls,
            Self::WriteMapNullValue,
            Self::BrowserCompatible,
            Self::NullAsDefaultValue,
            Self::WriteBooleanAsNumber,
            Self::WriteNonStringValueAsString,
            Self::WriteClassName,
            Self::NotWriteRootClassName,
            Self::NotWriteHashMapArrayListClassName,
            Self::NotWriteDefaultValue,
            Self::WriteEnumsUsingName,
            Self::WriteEnumUsingToString,
            Self::IgnoreErrorGetter,
            Self::PrettyFormat,
            Self::ReferenceDetection,
            Self::WriteNameAsSymbol,
            Self::WriteBigDecimalAsPlain,
            Self::UseSingleQuotes,
            Self::MapSortField,
            Self::WriteNullListAsEmpty,
            Self::WriteNullStringAsEmpty,
            Self::WriteNullNumberAsZero,
            Self::WriteNullBooleanAsFalse,
            Self::NotWriteEmptyArray,
            Self::IgnoreEmpty,
            Self::WriteNonStringKeyAsString,
            Self::WritePairAsJavaBean,
            Self::OptimizedForAscii,
            Self::EscapeNoneAscii,
            Self::WriteByteArrayAsBase64,
            Self::IgnoreNonFieldGetter,
            Self::LargeObject,
            Self::WriteLongAsString,
            Self::BrowserSecure,
            Self::WriteEnumUsingOrdinal,
            Self::WriteThrowableClassName,
            Self::UnquoteFieldName,
            Self::NotWriteSetClassName,
            Self::NotWriteNumberClassName,
            Self::SortMapEntriesByKeys,
            Self::PrettyFormatWith2Space,
            Self::PrettyFormatWith4Space,
            Self::WriterUtilDateAsMillis,
            Self::WriteFloatSpecialAsString,
        ]
    }

    /// Returns the matching Fastjson2 enum constant name.
    pub const fn name(self) -> &'static str {
        const NAMES: [&str; 48] = [
            "FieldBased",
            "IgnoreNoneSerializable",
            "ErrorOnNoneSerializable",
            "BeanToArray",
            "WriteNulls",
            "WriteMapNullValue",
            "BrowserCompatible",
            "NullAsDefaultValue",
            "WriteBooleanAsNumber",
            "WriteNonStringValueAsString",
            "WriteClassName",
            "NotWriteRootClassName",
            "NotWriteHashMapArrayListClassName",
            "NotWriteDefaultValue",
            "WriteEnumsUsingName",
            "WriteEnumUsingToString",
            "IgnoreErrorGetter",
            "PrettyFormat",
            "ReferenceDetection",
            "WriteNameAsSymbol",
            "WriteBigDecimalAsPlain",
            "UseSingleQuotes",
            "MapSortField",
            "WriteNullListAsEmpty",
            "WriteNullStringAsEmpty",
            "WriteNullNumberAsZero",
            "WriteNullBooleanAsFalse",
            "NotWriteEmptyArray",
            "IgnoreEmpty",
            "WriteNonStringKeyAsString",
            "WritePairAsJavaBean",
            "OptimizedForAscii",
            "EscapeNoneAscii",
            "WriteByteArrayAsBase64",
            "IgnoreNonFieldGetter",
            "LargeObject",
            "WriteLongAsString",
            "BrowserSecure",
            "WriteEnumUsingOrdinal",
            "WriteThrowableClassName",
            "UnquoteFieldName",
            "NotWriteSetClassName",
            "NotWriteNumberClassName",
            "SortMapEntriesByKeys",
            "PrettyFormatWith2Space",
            "PrettyFormatWith4Space",
            "WriterUtilDateAsMillis",
            "WriteFloatSpecialAsString",
        ];
        NAMES[self as usize]
    }

    /// Returns the Java ordinal.
    pub const fn ordinal(self) -> i32 {
        self as i32
    }

    pub(crate) const fn is_implemented(self) -> bool {
        matches!(
            self,
            Self::BeanToArray
                | Self::WriteNulls
                | Self::WriteMapNullValue
                | Self::WriteNullListAsEmpty
                | Self::WriteNullStringAsEmpty
                | Self::WriteNullNumberAsZero
                | Self::NullAsDefaultValue
                | Self::WriteClassName
                | Self::ReferenceDetection
                | Self::FieldBased
                | Self::WriteNullBooleanAsFalse
                | Self::PrettyFormat
                | Self::PrettyFormatWith2Space
                | Self::PrettyFormatWith4Space
                | Self::EscapeNoneAscii
                | Self::WriteBooleanAsNumber
                | Self::WriteNonStringValueAsString
                | Self::MapSortField
                | Self::SortMapEntriesByKeys
                | Self::LargeObject
                | Self::NotWriteEmptyArray
                | Self::IgnoreEmpty
                | Self::NotWriteDefaultValue
                | Self::WriteNonStringKeyAsString
                | Self::WriteLongAsString
                | Self::WriteEnumUsingOrdinal
                | Self::WriteEnumsUsingName
                | Self::WriteEnumUsingToString
                | Self::IgnoreErrorGetter
                | Self::IgnoreNonFieldGetter
                | Self::BrowserCompatible
                | Self::OptimizedForAscii
                | Self::WriteNameAsSymbol
                | Self::NotWriteRootClassName
                | Self::NotWriteHashMapArrayListClassName
                | Self::NotWriteNumberClassName
                | Self::NotWriteSetClassName
                | Self::WriteByteArrayAsBase64
                | Self::WritePairAsJavaBean
                | Self::WriteThrowableClassName
                | Self::WriterUtilDateAsMillis
                | Self::WriteBigDecimalAsPlain
                | Self::WriteFloatSpecialAsString
                | Self::BrowserSecure
                | Self::UseSingleQuotes
                | Self::UnquoteFieldName
                | Self::IgnoreNoneSerializable
                | Self::ErrorOnNoneSerializable
        )
    }
}

#[cfg(test)]
mod tests {
    use super::JavaJsonWriterFeature;

    #[test]
    fn feature_names_and_ordinals_match_pinned_java_enum_inventory() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let java_names = fixture["remotingSerializationWriterFeatures"]
            .as_array()
            .unwrap();
        let rust_features = JavaJsonWriterFeature::values();
        assert_eq!(java_names.len(), rust_features.len());
        for (ordinal, (java_name, feature)) in java_names.iter().zip(rust_features).enumerate() {
            assert_eq!(feature.name(), java_name.as_str().unwrap());
            assert_eq!(feature.ordinal(), ordinal as i32);
            assert!(
                feature.is_implemented(),
                "pinned Java writer feature {} must have a mapped Rust implementation",
                feature.name()
            );
        }
    }

    #[test]
    fn single_quoted_strings_match_fastjson_escaping() {
        let mut output = String::new();
        crate::route::write_java_json_string_with_browser_secure(
            "a'b\"c\\d\n漢<>()",
            true,
            false,
            &mut output,
        )
        .unwrap();
        assert_eq!(output, r#"'a\'b"c\\d\n漢<>()'"#);
        assert_eq!(
            crate::route::escape_none_ascii_json(&output),
            r#"'a\'b"c\\d\n\u6F22<>()'"#
        );
    }

    #[test]
    fn browser_secure_strings_escape_html_sensitive_characters() {
        let mut output = String::new();
        crate::route::write_java_json_string_with_browser_secure(
            "a'b\"c\\d\n漢<>()",
            true,
            true,
            &mut output,
        )
        .unwrap();
        assert_eq!(output, r#"'a\'b"c\\d\n漢\u003C\u003E\u0028\u0029'"#);
    }

    #[test]
    fn enum_ordinal_writer_covers_protocol_enum_fields() {
        let mut value = serde_json::json!([
            {"properties": {"mode": "POP", "action": "PARTIAL_ADD"}},
            {"messageRequestModeMap": {"topic": {"group": {"mode": "POP"}}}},
            {"action": "PARTIAL_ADD"},
            {"action": "PARTIAL_REMOVE"},
            {"action": "COMPLETE_ADD"},
            {"action": "COMPLETE_REMOVE"},
            {"consumeFromWhere": "CONSUME_FROM_LAST_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"},
            {"consumeFromWhere": "CONSUME_FROM_MIN_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_MAX_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_FIRST_OFFSET"},
            {"consumeFromWhere": "CONSUME_FROM_TIMESTAMP"},
            {"consumeResult": "CR_SUCCESS"},
            {"consumeResult": "CR_LATER"},
            {"consumeResult": "CR_ROLLBACK"},
            {"consumeResult": "CR_COMMIT"},
            {"consumeResult": "CR_THROW_EXCEPTION"},
            {"consumeResult": "CR_RETURN_NULL"},
            {"consumeType": "CONSUME_ACTIVELY"},
            {"consumeType": "CONSUME_PASSIVELY"},
            {"consumeType": "CONSUME_POP"},
            {"messageModel": "BROADCASTING"},
            {"messageModel": "CLUSTERING"},
            {"messageModel": "LITE_SELECTIVE"},
            {"mode": "PULL"},
            {"mode": "POP"},
            {"optionType": "POLICY"},
            {"optionType": "OFFSET"},
            {"optionType": "TAIL_N"},
            {"optionType": "TIMESTAMP"},
        ]);
        crate::route::apply_java_json_writer_value_features(&mut value, false, false, true, false);
        let expected = serde_json::json!([
            {"properties": {"mode": "POP", "action": "PARTIAL_ADD"}},
            {"messageRequestModeMap": {"topic": {"group": {"mode": 1}}}},
            {"action": 0}, {"action": 1}, {"action": 2}, {"action": 3},
            {"consumeFromWhere": 0}, {"consumeFromWhere": 1},
            {"consumeFromWhere": 2}, {"consumeFromWhere": 3},
            {"consumeFromWhere": 4}, {"consumeFromWhere": 5},
            {"consumeResult": 0}, {"consumeResult": 1}, {"consumeResult": 2},
            {"consumeResult": 3}, {"consumeResult": 4}, {"consumeResult": 5},
            {"consumeType": 0}, {"consumeType": 1}, {"consumeType": 2},
            {"messageModel": 0}, {"messageModel": 1}, {"messageModel": 2},
            {"mode": 0}, {"mode": 1},
            {"optionType": 0}, {"optionType": 1}, {"optionType": 2}, {"optionType": 3},
        ]);
        assert_eq!(value, expected);
    }
}
