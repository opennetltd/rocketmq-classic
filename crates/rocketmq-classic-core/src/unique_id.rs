use std::{
    hash::{BuildHasher, Hash, Hasher},
    net::IpAddr,
    sync::{
        OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use rocketmq_classic_common::message::{
    Message, constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};

static COUNTER: AtomicU32 = AtomicU32::new(0);
static PROCESS_HASH: OnceLock<u32> = OnceLock::new();

#[derive(Debug, Error, PartialEq, Eq)]
pub enum UniqueIdError {
    #[error("unique ID must not be empty")]
    EmptyId,
    #[error("{method} needs at least {required} decoded bytes, found {actual}")]
    InsufficientBytes {
        method: &'static str,
        required: usize,
        actual: usize,
    },
    #[error("local timezone offset is unavailable")]
    LocalTimezoneUnavailable,
    #[error("unique ID timestamp is outside the supported range")]
    TimestampOutOfRange,
}

/// Decode the IP bytes at the start of a Java unique message ID.
pub fn get_ip_from_id(message_id: &str) -> Result<Vec<u8>, UniqueIdError> {
    let bytes = decode_java_hex(message_id)?;
    let ip_length = if bytes.len() == 28 { 16 } else { 4 };
    if bytes.len() < ip_length {
        return Err(UniqueIdError::InsufficientBytes {
            method: "getIPFromID",
            required: ip_length,
            actual: bytes.len(),
        });
    }
    Ok(bytes[..ip_length].to_vec())
}

/// Decode the IP address text used by Java's `MessageClientIDSetter`.
pub fn get_ip_str_from_id(message_id: &str) -> Result<String, UniqueIdError> {
    let bytes = get_ip_from_id(message_id)?;
    if bytes.len() == 4 {
        return Ok(format!(
            "{}.{}.{}.{}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        ));
    }
    Ok(bytes
        .chunks_exact(2)
        .map(|group| format!("{:02x}{:02x}", group[0], group[1]))
        .collect::<Vec<_>>()
        .join(":"))
}

/// Read Java's unsigned 16-bit process ID field from a unique message ID.
pub fn get_pid_from_id(message_id: &str) -> Result<u16, UniqueIdError> {
    let bytes = decode_java_hex(message_id)?;
    let Some(offset) = bytes.len().checked_sub(12) else {
        return Err(UniqueIdError::InsufficientBytes {
            method: "getPidFromID",
            required: 12,
            actual: bytes.len(),
        });
    };
    Ok(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]))
}

/// Return the near creation time in Unix milliseconds, matching Java's
/// month-relative timestamp inference and the process-local calendar zone.
pub fn get_nearly_time_from_id(message_id: &str) -> Result<i64, UniqueIdError> {
    let bytes = decode_java_hex(message_id)?;
    let ip_length = if bytes.len() == 28 { 16 } else { 4 };
    let timestamp_offset = ip_length + 6;
    if bytes.len() < timestamp_offset + 4 {
        return Err(UniqueIdError::InsufficientBytes {
            method: "getNearlyTimeFromID",
            required: timestamp_offset + 4,
            actual: bytes.len(),
        });
    }
    let span_millis = u32::from_be_bytes(
        bytes[timestamp_offset..timestamp_offset + 4]
            .try_into()
            .expect("four-byte unique ID timestamp"),
    ) as i64;
    let now = now_millis();
    let now_utc = OffsetDateTime::from_unix_timestamp_nanos(i128::from(now) * 1_000_000)
        .map_err(|_| UniqueIdError::TimestampOutOfRange)?;
    let local_offset =
        UtcOffset::local_offset_at(now_utc).map_err(|_| UniqueIdError::LocalTimezoneUnavailable)?;
    let local_now = now_utc.to_offset(local_offset);
    let month_start = local_month_start_millis(local_now.year(), local_now.month(), local_offset)?;
    let selected_month_start = if month_start + span_millis >= now {
        let (year, month) = previous_month(local_now.year(), local_now.month());
        local_month_start_millis(year, month, local_offset)?
    } else {
        month_start
    };
    selected_month_start
        .checked_add(span_millis)
        .ok_or(UniqueIdError::TimestampOutOfRange)
}

/// Generate the four-byte fallback address used by Java's ID generator.
pub fn create_fake_ip() -> [u8; 4] {
    let millis = now_millis() as u64;
    millis.to_be_bytes()[4..]
        .try_into()
        .expect("four bytes from the low half of a timestamp")
}

fn decode_java_hex(input: &str) -> Result<Vec<u8>, UniqueIdError> {
    if input.is_empty() {
        return Err(UniqueIdError::EmptyId);
    }
    let units: Vec<u16> = input.encode_utf16().collect();
    Ok(units
        .chunks_exact(2)
        .map(|pair| {
            let high = java_hex_nibble(pair[0]);
            let low = java_hex_nibble(pair[1]);
            ((high << 4) | low) as u8
        })
        .collect())
}

fn java_hex_nibble(unit: u16) -> i32 {
    match unit {
        0x0030..=0x0039 => i32::from(unit - 0x0030),
        0x0041..=0x0046 => i32::from(unit - 0x0041 + 10),
        0x0061..=0x0066 => i32::from(unit - 0x0061 + 10),
        _ => -1,
    }
}

fn previous_month(year: i32, month: Month) -> (i32, Month) {
    match month {
        Month::January => (year - 1, Month::December),
        month => (
            year,
            Month::try_from(u8::from(month) - 1).expect("previous month is valid"),
        ),
    }
}

fn local_month_start_millis(
    year: i32,
    month: Month,
    offset_hint: UtcOffset,
) -> Result<i64, UniqueIdError> {
    let date =
        Date::from_calendar_date(year, month, 1).map_err(|_| UniqueIdError::TimestampOutOfRange)?;
    let local_midnight = PrimitiveDateTime::new(date, Time::MIDNIGHT);
    let mut offset = offset_hint;
    for _ in 0..3 {
        let candidate = local_midnight.assume_offset(offset);
        let actual = UtcOffset::local_offset_at(candidate)
            .map_err(|_| UniqueIdError::LocalTimezoneUnavailable)?;
        if actual == offset {
            break;
        }
        offset = actual;
    }
    i64::try_from(
        local_midnight
            .assume_offset(offset)
            .unix_timestamp_nanos()
            .div_euclid(1_000_000),
    )
    .map_err(|_| UniqueIdError::TimestampOutOfRange)
}

/// Emits the same 16/28-byte hexadecimal layout as Java's
/// `MessageClientIDSetter`: IP, process ID, process identity, month-relative
/// milliseconds, and a wrapping 16-bit counter.
#[doc(hidden)]
pub struct UniqueIdGenerator {
    ip: Vec<u8>,
}

impl UniqueIdGenerator {
    pub fn new(client_ip: Option<&str>) -> Self {
        let ip = client_ip
            .and_then(|value| value.parse::<IpAddr>().ok())
            .map(|address| match address {
                IpAddr::V4(address) => address.octets().to_vec(),
                IpAddr::V6(address) => address.octets().to_vec(),
            })
            .unwrap_or_else(fake_ip);
        Self { ip }
    }

    pub fn set_if_missing(&self, message: &mut Message) -> String {
        if let Some(id) = message
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .cloned()
        {
            return id;
        }
        let id = self.create();
        let _ = message.set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, id.clone());
        id
    }

    pub fn create(&self) -> String {
        let millis = now_millis();
        let month_start =
            local_month_start_millis_at(millis).unwrap_or_else(|_| month_start_millis(millis));
        let mut bytes = Vec::with_capacity(self.ip.len() + 12);
        bytes.extend_from_slice(&self.ip);
        bytes.extend_from_slice(&(std::process::id() as u16).to_be_bytes());
        bytes.extend_from_slice(&process_hash().to_be_bytes());
        let mut diff = millis.wrapping_sub(month_start) as i32;
        if diff < 0 && diff > -1_000_000 {
            diff = 0;
        }
        bytes.extend_from_slice(&diff.to_be_bytes());
        bytes.extend_from_slice(&(COUNTER.fetch_add(1, Ordering::Relaxed) as u16).to_be_bytes());
        encode_hex(&bytes)
    }
}

fn process_hash() -> u32 {
    *PROCESS_HASH.get_or_init(|| {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        std::process::id().hash(&mut hasher);
        SystemTime::now().hash(&mut hasher);
        std::thread::current().id().hash(&mut hasher);
        hasher.finish() as u32
    })
}

fn fake_ip() -> Vec<u8> {
    create_fake_ip().to_vec()
}

fn local_month_start_millis_at(millis: i64) -> Result<i64, UniqueIdError> {
    let timestamp = OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        .map_err(|_| UniqueIdError::TimestampOutOfRange)?;
    let offset = UtcOffset::local_offset_at(timestamp)
        .map_err(|_| UniqueIdError::LocalTimezoneUnavailable)?;
    let local = timestamp.to_offset(offset);
    local_month_start_millis(local.year(), local.month(), offset)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn month_start_millis(millis: i64) -> i64 {
    const DAY_MILLIS: i64 = 86_400_000;
    let day = millis.div_euclid(DAY_MILLIS);
    let (year, month, _) = civil_from_days(day);
    days_from_civil(year, month, 1) * DAY_MILLIS
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex(input: &str) -> Vec<u8> {
        input
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |value: u8| match value {
                    b'0'..=b'9' => value - b'0',
                    b'A'..=b'F' => value - b'A' + 10,
                    _ => panic!("invalid hex digit"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }

    #[test]
    fn generated_id_uses_the_java_client_id_layout_and_advancing_counter() {
        let generator = UniqueIdGenerator::new(Some("10.10.10.111"));
        let first = generator.create();
        let second = generator.create();
        let first_bytes = decode_hex(&first);
        let second_bytes = decode_hex(&second);
        assert_eq!(first.len(), 32);
        assert_eq!(&first_bytes[..4], &[10, 10, 10, 111]);
        assert_eq!(first_bytes.len(), 16);
        let first_counter = u16::from_be_bytes([first_bytes[14], first_bytes[15]]);
        let second_counter = u16::from_be_bytes([second_bytes[14], second_bytes[15]]);
        assert_ne!(first_counter, second_counter);
        assert_ne!(second_counter.wrapping_sub(first_counter), 0);
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_lowercase())
        );
    }

    #[test]
    fn existing_unique_id_is_preserved_when_preparing_a_message() {
        let generator = UniqueIdGenerator::new(Some("10.10.10.111"));
        let mut message = Message::new("orders", b"body".to_vec());
        message
            .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "existing")
            .unwrap();
        assert_eq!(generator.set_if_missing(&mut message), "existing");
        assert_eq!(
            message.properties()[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX],
            "existing"
        );
    }

    #[test]
    fn generated_id_layout_matches_the_pinned_java_unique_id_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let shape = &fixture["uniqueId"];
        assert_eq!(shape["processIdBytes"], 2);
        assert_eq!(shape["processIdentityBytes"], 4);
        assert_eq!(shape["monthTimestampBytes"], 4);
        assert_eq!(shape["counterBytes"], 2);
        assert!(shape["hexUppercase"].as_bool().unwrap());

        for (address, ip_length) in [("10.10.10.111", 4usize), ("2001:db8::1", 16)] {
            let generator = UniqueIdGenerator::new(Some(address));
            let id = generator.create();
            let bytes = decode_hex(&id);
            assert_eq!(bytes.len(), ip_length + 12);
            assert_eq!(
                id.len(),
                (ip_length
                    + shape["processIdBytes"].as_u64().unwrap() as usize
                    + shape["processIdentityBytes"].as_u64().unwrap() as usize
                    + shape["monthTimestampBytes"].as_u64().unwrap() as usize
                    + shape["counterBytes"].as_u64().unwrap() as usize)
                    * 2
            );
            assert!(
                id.bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_lowercase())
            );
        }
    }

    #[test]
    fn public_unique_id_helpers_match_the_pinned_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["uniqueIdHelpers"];
        let public_api = &expected["publicApi"];
        assert_eq!(
            public_api["className"],
            "org.apache.rocketmq.common.message.MessageClientIDSetter"
        );
        assert_eq!(public_api["declaredPublicFields"], serde_json::json!([]));
        assert_eq!(
            public_api["publicConstructors"],
            serde_json::json!(["org.apache.rocketmq.common.message.MessageClientIDSetter()"])
        );
        let declared_methods = serde_json::json!([
            "[B org.apache.rocketmq.common.message.MessageClientIDSetter.createFakeIP()",
            "[B org.apache.rocketmq.common.message.MessageClientIDSetter.getIPFromID(java.lang.String)",
            "int org.apache.rocketmq.common.message.MessageClientIDSetter.getPidFromID(java.lang.String)",
            "java.lang.String org.apache.rocketmq.common.message.MessageClientIDSetter.createUniqID()",
            "java.lang.String org.apache.rocketmq.common.message.MessageClientIDSetter.getIPStrFromID(java.lang.String)",
            "java.lang.String org.apache.rocketmq.common.message.MessageClientIDSetter.getUniqID(org.apache.rocketmq.common.message.Message)",
            "java.util.Date org.apache.rocketmq.common.message.MessageClientIDSetter.getNearlyTimeFromID(java.lang.String)",
            "void org.apache.rocketmq.common.message.MessageClientIDSetter.setUniqID(org.apache.rocketmq.common.message.Message)"
        ]);
        assert_eq!(public_api["declaredPublicMethods"], declared_methods);
        assert_eq!(public_api["declaredPublicMethodCount"], 8);
        assert_eq!(public_api["publicMethods"], declared_methods);
        assert_eq!(public_api["publicMethodCount"], 8);

        for prefix in ["ipv4", "ipv6"] {
            let id = expected[format!("{prefix}Id")].as_str().unwrap();
            let ip_bytes_key = format!("{prefix}AddressBytes");
            let address_key = format!("{prefix}Address");
            let pid_key = format!("{prefix}Pid");
            let nearly_time_key = format!("{prefix}NearlyTimeMonthOffsetMillis");
            assert_eq!(
                encode_hex(&get_ip_from_id(id).unwrap()),
                expected[ip_bytes_key].as_str().unwrap()
            );
            assert_eq!(
                get_ip_str_from_id(id).unwrap(),
                expected[address_key].as_str().unwrap()
            );
            assert_eq!(
                get_pid_from_id(id).unwrap(),
                expected[pid_key].as_u64().unwrap() as u16
            );
            let nearly_time = get_nearly_time_from_id(id).unwrap();
            let local_month_start = local_month_start_millis_at(nearly_time).unwrap();
            assert_eq!(
                nearly_time - local_month_start,
                expected[nearly_time_key].as_i64().unwrap()
            );
        }

        assert_eq!(
            get_ip_from_id(expected["ipv4Id"].as_str().unwrap()).unwrap(),
            get_ip_from_id("C000020A1").unwrap()
        );
        assert_eq!(
            encode_hex(&get_ip_from_id("G0H0I0J0").unwrap()),
            expected["invalidHexNibbles"]
        );
        assert_eq!(create_fake_ip().len(), expected["fakeIpLength"]);
        assert!(expected["shortIpThrows"].as_bool().unwrap());
        assert!(expected["shortPidThrows"].as_bool().unwrap());
        assert!(expected["shortTimestampThrows"].as_bool().unwrap());
        assert!(get_ip_from_id("AA").is_err());
        assert!(get_pid_from_id("AA").is_err());
        assert!(get_nearly_time_from_id("AA").is_err());
    }
}
