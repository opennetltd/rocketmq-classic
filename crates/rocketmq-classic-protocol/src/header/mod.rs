use std::collections::BTreeMap;

use thiserror::Error;

pub type HeaderMap = BTreeMap<String, String>;

pub use crate::body::ConsumerListByGroupResponseBody as GetConsumerListByGroupResponseBody;

mod basic;
pub use basic::{
    AckMessageRequestHeader, AddBrokerRequestHeader, AddWritePermOfBrokerRequestHeader,
    AddWritePermOfBrokerResponseHeader, BoundaryType, ChangeInvisibleTimeRequestHeader,
    ChangeInvisibleTimeResponseHeader, CheckRocksdbCqWriteProgressRequestHeader,
    CheckTransactionStateRequestHeader, CheckTransactionStateResponseHeader,
    CloneGroupOffsetRequestHeader, ConsumeMessageDirectlyResultRequestHeader,
    ConsumerSendMsgBackRequestHeader, CreateAclRequestHeader, CreateTopicRequestHeader,
    CreateUserRequestHeader, DeleteAclRequestHeader, DeleteKvConfigRequestHeader,
    DeleteSubscriptionGroupRequestHeader, DeleteTopicFromNamesrvRequestHeader,
    DeleteTopicRequestHeader, DeleteUserRequestHeader, EndTransactionRequestHeader,
    EndTransactionResponseHeader, ExportRocksDBConfigToJsonRequestHeader, ExportRocksDBConfigType,
    GetAclRequestHeader, GetAllSubscriptionGroupRequestHeader,
    GetAllSubscriptionGroupResponseHeader, GetAllTopicConfigRequestHeader,
    GetAllTopicConfigResponseHeader, GetConsumeStatsInBrokerRequestHeader,
    GetConsumeStatsRequestHeader, GetConsumerConnectionListRequestHeader,
    GetConsumerListByGroupRequestHeader, GetConsumerRunningInfoRequestHeader,
    GetConsumerStatusRequestHeader, GetEarliestMsgStoretimeRequestHeader,
    GetEarliestMsgStoretimeResponseHeader, GetKvConfigRequestHeader, GetKvConfigResponseHeader,
    GetKvListByNamespaceRequestHeader, GetLiteClientInfoRequestHeader,
    GetLiteGroupInfoRequestHeader, GetLiteTopicInfoRequestHeader, GetMaxOffsetRequestHeader,
    GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader, GetMinOffsetResponseHeader,
    GetParentTopicInfoRequestHeader, GetRouteInfoRequestHeader,
    GetSubscriptionGroupConfigRequestHeader, GetTopicConfigRequestHeader,
    GetTopicStatsInfoRequestHeader, GetTopicsByClusterRequestHeader, GetUserRequestHeader,
    HeartbeatRequestHeader, ListAclsRequestHeader, ListUsersRequestHeader,
    LiteSubscriptionCtlRequestHeader, LockBatchMqRequestHeader, NotificationRequestHeader,
    NotificationResponseHeader, NotifyUnsubscribeLiteRequestHeader, PollingInfoRequestHeader,
    PollingInfoResponseHeader, PopLiteMessageRequestHeader, PopLiteMessageResponseHeader,
    PopMessageRequestHeader, PopMessageResponseHeader, PullMessageRequestHeader,
    PullMessageResponseHeader, PutKvConfigRequestHeader, QueryConsumeQueueRequestHeader,
    QueryConsumeTimeSpanRequestHeader, QueryConsumerOffsetRequestHeader,
    QueryConsumerOffsetResponseHeader, QueryCorrectionOffsetRequestHeader,
    QueryMessageRequestHeader, QueryMessageResponseHeader,
    QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
    QueryTopicsByConsumerRequestHeader, RecallMessageRequestHeader, RecallMessageResponseHeader,
    RemoveBrokerRequestHeader, ReplyMessageRequestHeader, ResetMasterFlushOffsetHeader,
    ResetOffsetRequestHeader, SearchOffsetRequestHeader, SearchOffsetResponseHeader,
    SendMessageRequestHeaderV1, SendMessageRequestHeaderV2, SendMessageResponseHeader,
    UnlockBatchMqRequestHeader, UnregisterClientRequestHeader, UnregisterClientResponseHeader,
    UpdateAclRequestHeader, UpdateConsumerOffsetRequestHeader, UpdateGroupForbiddenRequestHeader,
    UpdateUserRequestHeader, ViewBrokerStatsDataRequestHeader, ViewMessageRequestHeader,
    WipeWritePermOfBrokerRequestHeader, WipeWritePermOfBrokerResponseHeader,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    #[error("missing required header field `{0}`")]
    MissingField(&'static str),
    #[error("invalid header field `{field}`: {reason}")]
    InvalidField { field: &'static str, reason: String },
}

pub trait CommandHeader: Sized {
    fn encode_fields(&self, fields: &mut HeaderMap);
    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError>;

    /// Decodes a header through the selected Java custom-header path.
    #[doc(hidden)]
    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Self::decode_fields_checked(fields)
    }

    /// Validate a decoded header, matching Java `CommandCustomHeader.checkFields()`.
    fn check_fields(&self) -> Result<(), ProtocolError> {
        Ok(())
    }

    /// Decode fields and run the header's source-compatible validation hook.
    fn decode_fields_checked(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        let header = Self::decode_fields(fields)?;
        header.check_fields()?;
        Ok(header)
    }
}

/// Java `FastCodesHeader` behavior for headers that support the fast field
/// codec. Rust exposes the helpers over its typed header map instead of a
/// Netty `ByteBuf`.
pub trait FastCodesHeader: CommandHeader {
    /// Returns a present field and preserves Java's nullable missing-field
    /// result. Java logs the missing value; Rust leaves logging to the caller.
    fn get_and_check_not_null<'a>(&self, fields: &'a HeaderMap, field: &str) -> Option<&'a str> {
        fields.get(field).map(String::as_str)
    }

    /// Writes the field when the Java `Object` value is non-null.
    fn write_if_not_null<T: ToString>(&self, fields: &mut HeaderMap, key: &str, value: Option<T>) {
        if let Some(value) = value {
            fields.insert(key.to_owned(), value.to_string());
        }
    }

    /// Writes a Java `long` field using its signed decimal representation.
    fn write_long(&self, fields: &mut HeaderMap, key: &str, value: i64) {
        fields.insert(key.to_owned(), value.to_string());
    }

    /// Writes a Java `int` field using its signed decimal representation.
    fn write_int(&self, fields: &mut HeaderMap, key: &str, value: i32) {
        fields.insert(key.to_owned(), value.to_string());
    }
}

/// Parses the signed decimal form accepted by Java's `Long.parseLong`,
/// including decimal digits from BMP Unicode scripts.
pub(crate) fn parse_java_i64(value: &str) -> Option<i64> {
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
    let limit = if negative { i64::MIN } else { -i64::MAX };
    let multiply_limit = limit / 10;
    let mut result = 0_i64;
    for unit in units {
        let digit = i64::from(java_decimal_digit(unit)?);
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

/// Parses the signed decimal form accepted by Java's `Integer.parseInt`.
pub(crate) fn parse_java_i32(value: &str) -> Option<i32> {
    parse_java_i64(value)?.try_into().ok()
}

fn java_decimal_digit(unit: u16) -> Option<u16> {
    const ZEROES: &[u16] = &[
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
        0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
        0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];
    ZEROES
        .iter()
        .find_map(|zero| unit.checked_sub(*zero).filter(|digit| *digit < 10))
}

fn required_string<'a>(
    fields: &'a HeaderMap,
    field: &'static str,
) -> Result<&'a str, ProtocolError> {
    fields
        .get(field)
        .map(String::as_str)
        .ok_or(ProtocolError::MissingField(field))
}

fn optional_string(fields: &HeaderMap, field: &str) -> Option<String> {
    fields.get(field).cloned()
}

fn required_i32(fields: &HeaderMap, field: &'static str) -> Result<i32, ProtocolError> {
    parse_java_i32(required_string(fields, field)?).ok_or_else(|| invalid_number(field))
}

fn optional_i32(fields: &HeaderMap, field: &'static str) -> Result<Option<i32>, ProtocolError> {
    fields
        .get(field)
        .map(|value| parse_java_i32(value).ok_or_else(|| invalid_number(field)))
        .transpose()
}

fn required_i64(fields: &HeaderMap, field: &'static str) -> Result<i64, ProtocolError> {
    parse_java_i64(required_string(fields, field)?).ok_or_else(|| invalid_number(field))
}

fn optional_i64(fields: &HeaderMap, field: &'static str) -> Result<Option<i64>, ProtocolError> {
    fields
        .get(field)
        .map(|value| parse_java_i64(value).ok_or_else(|| invalid_number(field)))
        .transpose()
}

fn invalid_number(field: &'static str) -> ProtocolError {
    ProtocolError::InvalidField {
        field,
        reason: "expected a valid number".into(),
    }
}

fn optional_i32_with_mode(
    fields: &HeaderMap,
    field: &'static str,
    use_fast_encode: bool,
) -> Result<Option<i32>, ProtocolError> {
    let decoded = optional_i32(fields, field);
    if use_fast_encode {
        decoded
    } else {
        Ok(decoded.unwrap_or(None))
    }
}

fn optional_i64_with_mode(
    fields: &HeaderMap,
    field: &'static str,
    use_fast_encode: bool,
) -> Result<Option<i64>, ProtocolError> {
    let decoded = optional_i64(fields, field);
    if use_fast_encode {
        decoded
    } else {
        Ok(decoded.unwrap_or(None))
    }
}

fn java_boolean(fields: &HeaderMap, field: &str) -> bool {
    fields
        .get(field)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn optional_java_boolean(fields: &HeaderMap, field: &str) -> Option<bool> {
    fields
        .get(field)
        .map(|value| value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_mapped_header_round_trips_without_consuming_raw_fields() {
        let header = super::basic::GetRouteInfoRequestHeader {
            topic: "orders".into(),
            ..Default::default()
        };
        let mut raw = HeaderMap::from([("opaque".into(), "preserved".into())]);
        header.encode_fields(&mut raw);
        let decoded = super::basic::GetRouteInfoRequestHeader::decode_fields(&raw).unwrap();
        assert_eq!(decoded.topic, header.topic);
        assert_eq!(raw.get("opaque").map(String::as_str), Some("preserved"));
    }

    #[test]
    fn fast_codes_helpers_match_java_nullable_field_writes() {
        let header = super::basic::PullMessageRequestHeader::default();
        let mut fields = HeaderMap::new();
        header.write_if_not_null(&mut fields, "queueId", Some(7));
        header.write_if_not_null(&mut fields, "missing", None::<&str>);

        assert_eq!(fields.get("queueId").map(String::as_str), Some("7"));
        assert!(!fields.contains_key("missing"));
        assert_eq!(header.get_and_check_not_null(&fields, "queueId"), Some("7"));
        assert_eq!(header.get_and_check_not_null(&fields, "missing"), None);
    }

    #[test]
    fn field_parsers_match_java_boolean_and_signed_number_behavior() {
        assert_eq!(
            required_i32(
                &HeaderMap::from([("value".into(), i32::MIN.to_string())]),
                "value"
            ),
            Ok(i32::MIN)
        );
        assert_eq!(
            optional_i64(
                &HeaderMap::from([("value".into(), i64::MAX.to_string())]),
                "value"
            ),
            Ok(Some(i64::MAX))
        );
        assert!(matches!(
            required_i32(
                &HeaderMap::from([("value".into(), "2147483648".into())]),
                "value"
            ),
            Err(ProtocolError::InvalidField { field: "value", .. })
        ));
        assert!(matches!(
            required_string(&HeaderMap::new(), "value"),
            Err(ProtocolError::MissingField("value"))
        ));
        assert!(java_boolean(
            &HeaderMap::from([("flag".into(), "TrUe".into())]),
            "flag"
        ));
        for value in ["false", "1", " true ", "yes"] {
            assert!(!java_boolean(
                &HeaderMap::from([("flag".into(), value.into())]),
                "flag"
            ));
        }
        assert_eq!(optional_java_boolean(&HeaderMap::new(), "flag"), None);
    }
}
