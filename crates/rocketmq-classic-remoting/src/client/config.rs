use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::OnceLock;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::TransportMode;
use crate::error::TransportError;

pub(super) const JAVA_FRAME_MAX_LENGTH_PROPERTY: &str = "com.rocketmq.remoting.frameMaxLength";
pub(super) const JAVA_CLIENT_ASYNC_SEMAPHORE_VALUE_PROPERTY: &str =
    "com.rocketmq.remoting.clientAsyncSemaphoreValue";
pub(super) const JAVA_CLIENT_ONEWAY_SEMAPHORE_VALUE_PROPERTY: &str =
    "com.rocketmq.remoting.clientOnewaySemaphoreValue";
pub(super) const JAVA_CLIENT_WORKER_SIZE_PROPERTY: &str =
    "com.rocketmq.remoting.client.worker.size";
pub(super) const JAVA_CLIENT_CONNECT_TIMEOUT_PROPERTY: &str =
    "com.rocketmq.remoting.client.connect.timeout";
pub(super) const JAVA_CLIENT_CHANNEL_MAX_IDLE_TIME_PROPERTY: &str =
    "com.rocketmq.remoting.client.channel.maxIdleTimeSeconds";
pub(super) const JAVA_CLIENT_CLOSE_SOCKET_IF_TIMEOUT_PROPERTY: &str =
    "com.rocketmq.remoting.client.closeSocketIfTimeout";
pub(super) const JAVA_CLIENT_SOCKET_SNDBUF_SIZE_PROPERTY: &str =
    "com.rocketmq.remoting.socket.sndbuf.size";
pub(super) const JAVA_CLIENT_SOCKET_RCVBUF_SIZE_PROPERTY: &str =
    "com.rocketmq.remoting.socket.rcvbuf.size";
pub(super) const JAVA_CLIENT_WRITE_BUFFER_HIGH_WATER_MARK_PROPERTY: &str =
    "com.rocketmq.remoting.write.buffer.high.water.mark";
pub(super) const JAVA_CLIENT_WRITE_BUFFER_LOW_WATER_MARK_PROPERTY: &str =
    "com.rocketmq.remoting.write.buffer.low.water.mark";
static JAVA_FRAME_MAX_LENGTH: OnceLock<Result<usize, String>> = OnceLock::new();
static JAVA_CLIENT_DEFAULTS: OnceLock<Result<JavaClientDefaults, String>> = OnceLock::new();

#[derive(Clone, Copy)]
struct JavaClientDefaults {
    async_semaphore_value: i32,
    oneway_semaphore_value: i32,
    worker_threads: i32,
    connect_timeout: i32,
    max_idle_time: i32,
    close_socket_if_timeout: bool,
    client_socket_send_buffer_size: i32,
    client_socket_receive_buffer_size: i32,
    write_buffer_high_water_mark: i32,
    write_buffer_low_water_mark: i32,
}

fn read_java_int_property(property: &str, default: i32) -> Result<i32, String> {
    match std::env::var(property) {
        Ok(value) => super::java_number::parse_i32(&value).ok_or_else(|| {
            format!("Java system property {property} is not a signed decimal integer")
        }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "Java system property {property} is not valid Unicode"
        )),
    }
}

fn read_java_bool_property(property: &str, default: bool) -> bool {
    match std::env::var(property) {
        Ok(value) => value.eq_ignore_ascii_case("true"),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => false,
    }
}

fn java_client_defaults() -> JavaClientDefaults {
    JAVA_CLIENT_DEFAULTS
        .get_or_init(|| {
            Ok(JavaClientDefaults {
                async_semaphore_value: read_java_int_property(
                    JAVA_CLIENT_ASYNC_SEMAPHORE_VALUE_PROPERTY,
                    65535,
                )?,
                oneway_semaphore_value: read_java_int_property(
                    JAVA_CLIENT_ONEWAY_SEMAPHORE_VALUE_PROPERTY,
                    65535,
                )?,
                worker_threads: read_java_int_property(JAVA_CLIENT_WORKER_SIZE_PROPERTY, 4)?,
                connect_timeout: read_java_int_property(
                    JAVA_CLIENT_CONNECT_TIMEOUT_PROPERTY,
                    3000,
                )?,
                max_idle_time: read_java_int_property(
                    JAVA_CLIENT_CHANNEL_MAX_IDLE_TIME_PROPERTY,
                    120,
                )?,
                close_socket_if_timeout: read_java_bool_property(
                    JAVA_CLIENT_CLOSE_SOCKET_IF_TIMEOUT_PROPERTY,
                    true,
                ),
                client_socket_send_buffer_size: read_java_int_property(
                    JAVA_CLIENT_SOCKET_SNDBUF_SIZE_PROPERTY,
                    0,
                )?,
                client_socket_receive_buffer_size: read_java_int_property(
                    JAVA_CLIENT_SOCKET_RCVBUF_SIZE_PROPERTY,
                    0,
                )?,
                write_buffer_high_water_mark: read_java_int_property(
                    JAVA_CLIENT_WRITE_BUFFER_HIGH_WATER_MARK_PROPERTY,
                    0,
                )?,
                write_buffer_low_water_mark: read_java_int_property(
                    JAVA_CLIENT_WRITE_BUFFER_LOW_WATER_MARK_PROPERTY,
                    0,
                )?,
            })
        })
        .clone()
        .unwrap_or_else(|error| panic!("{error}"))
}

pub(super) fn java_frame_max_length() -> Result<usize, TransportError> {
    JAVA_FRAME_MAX_LENGTH
        .get_or_init(read_java_frame_max_length)
        .clone()
        .map_err(TransportError::InvalidConfiguration)
}

fn read_java_frame_max_length() -> Result<usize, String> {
    match std::env::var(JAVA_FRAME_MAX_LENGTH_PROPERTY) {
        Ok(value) => parse_java_frame_max_length(&value),
        Err(std::env::VarError::NotPresent) => Ok(crate::codec::frame::DEFAULT_MAX_FRAME_SIZE),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!(
            "Java property {JAVA_FRAME_MAX_LENGTH_PROPERTY} is not valid Unicode"
        )),
    }
}

fn parse_java_frame_max_length(value: &str) -> Result<usize, String> {
    let length = super::java_number::parse_i32(value).ok_or_else(|| {
        format!("Java property {JAVA_FRAME_MAX_LENGTH_PROPERTY} is not a signed decimal integer")
    })?;
    usize::try_from(length)
        .ok()
        .filter(|length| *length > 0)
        .ok_or_else(|| format!("Java property {JAVA_FRAME_MAX_LENGTH_PROPERTY} must be positive"))
}

/// SOCKS5 proxy settings corresponding to Java's `SocksProxyConfig`.
#[derive(Clone, Debug, Default, Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct SocksProxyConfig {
    #[serde(rename = "addr")]
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

// Java keeps malformed proxy entries until a matching route is connected.
// Keep those cases private so the public, address-bearing config stays honest.
#[derive(Clone, Debug)]
pub(super) enum ParsedProxyRoute {
    Direct,
    Proxy(SocksProxyConfig),
    // Java retains credentials even when this route can never reach a proxy.
    #[allow(dead_code)]
    MissingAddress {
        username: Option<String>,
        password: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub(super) struct ProxySelection {
    pub(super) cidr: String,
    pub(super) route: ParsedProxyRoute,
}

impl ProxySelection {
    pub(super) fn connect_proxy(&self) -> Result<Option<&SocksProxyConfig>, TransportError> {
        match &self.route {
            ParsedProxyRoute::Direct => Ok(None),
            ParsedProxyRoute::Proxy(proxy) => Ok(Some(proxy)),
            ParsedProxyRoute::MissingAddress { .. } => Err(TransportError::Connect(format!(
                "matched SOCKS proxy route {} has no address",
                self.cidr
            ))),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ProxyRoutes {
    routes: HashMap<Option<String>, ParsedProxyRoute>,
    java_iteration_order: Vec<Option<String>>,
}

impl ProxyRoutes {
    fn from_entries(entries: Vec<(Option<String>, ParsedProxyRoute)>) -> Self {
        let mut routes = HashMap::with_capacity(entries.len());
        let mut insertion_order = Vec::with_capacity(entries.len());
        for (key, route) in entries {
            if let Some(existing) = routes.get_mut(&key) {
                *existing = route;
            } else {
                insertion_order.push(key.clone());
                routes.insert(key, route);
            }
        }
        let java_iteration_order = java_hash_map_iteration_order(&insertion_order);
        Self {
            routes,
            java_iteration_order,
        }
    }
}

impl Deref for ProxyRoutes {
    type Target = HashMap<Option<String>, ParsedProxyRoute>;

    fn deref(&self) -> &Self::Target {
        &self.routes
    }
}

impl IntoIterator for ProxyRoutes {
    type Item = (Option<String>, ParsedProxyRoute);
    type IntoIter = std::collections::hash_map::IntoIter<Option<String>, ParsedProxyRoute>;

    fn into_iter(self) -> Self::IntoIter {
        self.routes.into_iter()
    }
}

pub(super) trait ProxyRouteMap<T> {
    fn contains_null_key(&self) -> bool;
    fn entries_in_iteration_order(&self) -> Vec<(&Option<String>, &T)>;
}

impl<T> ProxyRouteMap<T> for HashMap<Option<String>, T> {
    fn contains_null_key(&self) -> bool {
        self.contains_key(&None)
    }

    fn entries_in_iteration_order(&self) -> Vec<(&Option<String>, &T)> {
        self.iter().collect()
    }
}

impl ProxyRouteMap<ParsedProxyRoute> for ProxyRoutes {
    fn contains_null_key(&self) -> bool {
        self.routes.contains_key(&None)
    }

    fn entries_in_iteration_order(&self) -> Vec<(&Option<String>, &ParsedProxyRoute)> {
        self.java_iteration_order
            .iter()
            .filter_map(|key| self.routes.get_key_value(key))
            .collect()
    }
}

fn java_hash_map_iteration_order(insertion_order: &[Option<String>]) -> Vec<Option<String>> {
    if insertion_order.is_empty() {
        return Vec::new();
    }

    let source_capacity = java_default_hash_map_capacity(insertion_order.len());
    let destination_capacity = java_put_all_hash_map_capacity(insertion_order.len());
    let source_order = bucket_order(insertion_order, source_capacity);
    let mut destination_order = source_order;
    destination_order
        .sort_by_key(|index| java_hash_map_bucket(&insertion_order[*index], destination_capacity));
    destination_order
        .into_iter()
        .map(|index| insertion_order[index].clone())
        .collect()
}

fn bucket_order(keys: &[Option<String>], capacity: usize) -> Vec<usize> {
    let mut indexes = (0..keys.len()).collect::<Vec<_>>();
    indexes.sort_by_key(|index| java_hash_map_bucket(&keys[*index], capacity));
    indexes
}

fn java_default_hash_map_capacity(size: usize) -> usize {
    let mut capacity = 16;
    while size > capacity * 3 / 4 && capacity < (1 << 30) {
        capacity *= 2;
    }
    capacity
}

fn java_put_all_hash_map_capacity(size: usize) -> usize {
    let requested = ((size as f32 / 0.75_f32) + 1.0_f32) as usize;
    requested.max(1).next_power_of_two().min(1 << 30)
}

fn java_hash_map_bucket(key: &Option<String>, capacity: usize) -> usize {
    let hash = key.as_deref().map_or(0, java_string_hash_code);
    let spread = hash ^ (hash >> 16);
    (spread as usize) & (capacity - 1)
}

fn java_string_hash_code(value: &str) -> u32 {
    value.encode_utf16().fold(0u32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(u32::from(unit))
    })
}

impl From<SocksProxyConfig> for ParsedProxyRoute {
    fn from(value: SocksProxyConfig) -> Self {
        Self::Proxy(value)
    }
}

#[derive(Clone, Debug)]
enum FastjsonValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl FastjsonValue {
    fn java_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Null => "null".into(),
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => java_number_string(value),
            Self::Array(_) | Self::Object(_) => {
                let mut output = String::new();
                self.write_compact_json(&mut output);
                output
            }
        }
    }

    fn write_compact_json(&self, output: &mut String) {
        match self {
            Self::Null => output.push_str("null"),
            Self::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Self::Number(value) => output.push_str(&java_number_string(value)),
            Self::String(value) => write_json_string(value, output),
            Self::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    value.write_compact_json(output);
                }
                output.push(']');
            }
            Self::Object(fields) => {
                output.push('{');
                for (index, (key, value)) in fields.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_json_string(key, output);
                    output.push(':');
                    value.write_compact_json(output);
                }
                output.push('}');
            }
        }
    }
}

thread_local! {
    static FASTJSON_NUMBER_TOKENS: RefCell<VecDeque<String>> = const { RefCell::new(VecDeque::new()) };
}

struct FastjsonNumberTokenScope(VecDeque<String>);

impl FastjsonNumberTokenScope {
    fn install(tokens: VecDeque<String>) -> Self {
        let previous = FASTJSON_NUMBER_TOKENS
            .with(|current| std::mem::replace(&mut *current.borrow_mut(), tokens));
        Self(previous)
    }
}

impl Drop for FastjsonNumberTokenScope {
    fn drop(&mut self) {
        FASTJSON_NUMBER_TOKENS.with(|current| {
            *current.borrow_mut() = std::mem::take(&mut self.0);
        });
    }
}

fn take_fastjson_number_token<E: serde::de::Error>() -> Result<String, E> {
    FASTJSON_NUMBER_TOKENS.with(|tokens| {
        tokens
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| E::custom("missing Fastjson numeric token"))
    })
}

fn java_number_string(token: &str) -> String {
    if let Some((number, suffix)) = token.split_at_checked(token.len().saturating_sub(1)) {
        match suffix.as_bytes().first().copied() {
            Some(b'd' | b'D') => {
                if let Some(value) = parse_fastjson_float(number) {
                    return if value.is_nan() {
                        "NaN".into()
                    } else if value.is_infinite() {
                        if value.is_sign_negative() {
                            "-Infinity".into()
                        } else {
                            "Infinity".into()
                        }
                    } else if value == 0.0 {
                        "0.0".into()
                    } else {
                        java_double_string(value)
                    };
                }
            }
            Some(b'f' | b'F') => {
                if let Some(value) = parse_fastjson_float(number).map(|value| value as f32) {
                    return java_float_string(value);
                }
            }
            Some(b'l' | b'L') => {
                return canonical_integer_string(number);
            }
            _ => {}
        }
    }

    if token.contains(['e', 'E']) {
        let value = parse_fastjson_float(token).unwrap_or(f64::NAN);
        if value.is_nan() {
            return token.to_owned();
        }
        if value.is_infinite() {
            return if value.is_sign_negative() {
                "-Infinity".into()
            } else {
                "Infinity".into()
            };
        }
        if value == 0.0 {
            return "0.0".into();
        }
        return java_double_string(value);
    }

    let (negative, unsigned) = match token.as_bytes().first() {
        Some(b'-') => (true, &token[1..]),
        Some(b'+') => (false, &token[1..]),
        _ => (false, token),
    };
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let integer = integer.trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let is_zero = integer == "0" && fraction.bytes().all(|digit| digit == b'0');
    let mut output = String::new();
    if negative && !is_zero {
        output.push('-');
    }
    output.push_str(integer);
    if token.contains('.') {
        output.push('.');
        if fraction.is_empty() {
            output.push('0');
        } else {
            output.push_str(fraction);
        }
    }
    output
}

fn java_route_key_number_string(token: &str) -> String {
    let canonical = java_number_string(token);
    if canonical != "0.0" {
        return canonical;
    }

    let floating_token = token
        .strip_suffix('d')
        .or_else(|| token.strip_suffix('D'))
        .or_else(|| token.strip_suffix('f'))
        .or_else(|| token.strip_suffix('F'))
        .unwrap_or(token);
    let Some((mantissa, _)) = floating_token.split_once(['e', 'E']) else {
        return canonical;
    };
    if mantissa.starts_with('-')
        && mantissa
            .bytes()
            .any(|digit| digit.is_ascii_digit() && digit != b'0')
    {
        "-0.0".into()
    } else {
        canonical
    }
}

fn canonical_integer_string(token: &str) -> String {
    let (negative, digits) = match token.as_bytes().first() {
        Some(b'-') => (true, &token[1..]),
        Some(b'+') => (false, &token[1..]),
        _ => (false, token),
    };
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        "0".into()
    } else if negative {
        format!("-{digits}")
    } else {
        digits.to_owned()
    }
}

fn java_float_string(value: f32) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".into()
        } else {
            "Infinity".into()
        };
    }
    if value == 0.0 {
        return "0.0".into();
    }

    let rendered = value.abs().to_string();
    let (mantissa, exponent) = rendered
        .split_once('e')
        .or_else(|| rendered.split_once('E'))
        .map_or((rendered.as_str(), 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(0))
        });
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let first_significant = digits
        .bytes()
        .position(|digit| digit != b'0')
        .expect("nonzero float has a significant digit");
    let decimal_exponent = decimal_position + exponent - first_significant as i32 - 1;
    digits.drain(..first_significant);
    while digits.ends_with('0') {
        digits.pop();
    }

    let mut output = String::new();
    if value.is_sign_negative() {
        output.push('-');
    }
    if !(-3..7).contains(&decimal_exponent) {
        output.push(digits.as_bytes()[0] as char);
        output.push('.');
        if digits.len() == 1 {
            output.push('0');
        } else {
            output.push_str(&digits[1..]);
        }
        output.push('E');
        output.push_str(&decimal_exponent.to_string());
        return output;
    }

    let decimal_position = decimal_exponent + 1;
    if decimal_position <= 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-decimal_position) as usize));
        output.push_str(&digits);
    } else if decimal_position as usize >= digits.len() {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n(
            '0',
            decimal_position as usize - digits.len(),
        ));
        output.push_str(".0");
    } else {
        let decimal_position = decimal_position as usize;
        output.push_str(&digits[..decimal_position]);
        output.push('.');
        output.push_str(&digits[decimal_position..]);
    }
    output
}

fn parse_fastjson_float(token: &str) -> Option<f64> {
    let token = token.strip_prefix('+').unwrap_or(token);
    let exponent_position = token.find(['e', 'E']).unwrap_or(token.len());
    let (mantissa, exponent) = token.split_at(exponent_position);
    let (sign, unsigned) = match mantissa.as_bytes().first() {
        Some(b'-') => ("-", &mantissa[1..]),
        _ => ("", mantissa),
    };
    let normalized_mantissa = if let Some(fraction) = unsigned.strip_prefix('.') {
        format!("{sign}0.{fraction}")
    } else if unsigned.ends_with('.') {
        format!("{sign}{unsigned}0")
    } else {
        mantissa.to_owned()
    };
    format!("{normalized_mantissa}{exponent}").parse().ok()
}

fn java_double_string(value: f64) -> String {
    let rendered = serde_json::Number::from_f64(value.abs())
        .expect("finite float is a JSON number")
        .to_string();
    let (mantissa, exponent) = rendered
        .split_once('e')
        .or_else(|| rendered.split_once('E'))
        .map_or((rendered.as_str(), 0), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(0))
        });
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let first_significant = digits
        .bytes()
        .position(|digit| digit != b'0')
        .expect("nonzero float has a significant digit");
    let decimal_exponent = decimal_position + exponent - first_significant as i32 - 1;
    digits.drain(..first_significant);
    while digits.ends_with('0') {
        digits.pop();
    }

    let mut output = String::new();
    if value.is_sign_negative() {
        output.push('-');
    }
    if !(-3..7).contains(&decimal_exponent) {
        output.push(digits.as_bytes()[0] as char);
        output.push('.');
        if digits.len() == 1 {
            output.push('0');
        } else {
            output.push_str(&digits[1..]);
        }
        output.push('E');
        output.push_str(&decimal_exponent.to_string());
        return output;
    }

    let decimal_position = decimal_exponent + 1;
    if decimal_position <= 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-decimal_position) as usize));
        output.push_str(&digits);
    } else if decimal_position as usize >= digits.len() {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n(
            '0',
            decimal_position as usize - digits.len(),
        ));
        output.push_str(".0");
    } else {
        let decimal_position = decimal_position as usize;
        output.push_str(&digits[..decimal_position]);
        output.push('.');
        output.push_str(&digits[decimal_position..]);
    }
    output
}

fn write_json_string(value: &str, output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0000}'..='\u{001f}' => {
                let byte = character as u8;
                output.push_str("\\u00");
                output.push(HEX[(byte >> 4) as usize] as char);
                output.push(HEX[(byte & 0x0f) as usize] as char);
            }
            other => output.push(other),
        }
    }
    output.push('"');
}

impl<'de> Deserialize<'de> for FastjsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FastjsonValueVisitor;

        impl<'de> Visitor<'de> for FastjsonValueVisitor {
            type Value = FastjsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Null)
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Bool(value))
            }

            fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(FastjsonValue::Number(take_fastjson_number_token()?))
            }

            fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(FastjsonValue::Number(take_fastjson_number_token()?))
            }

            fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(FastjsonValue::Number(take_fastjson_number_token()?))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(FastjsonValue::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(FastjsonValue::String(value))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(FastjsonValue::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields: Vec<(String, FastjsonValue)> = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, FastjsonValue>()? {
                    if let Some((_, existing)) = fields.iter_mut().find(|(name, _)| *name == key) {
                        *existing = value;
                    } else {
                        fields.push((key, value));
                    }
                }
                Ok(FastjsonValue::Object(fields))
            }
        }

        deserializer.deserialize_any(FastjsonValueVisitor)
    }
}

#[derive(Clone, Deserialize)]
#[serde(transparent)]
struct RawProxyRoute(HashMap<String, Option<FastjsonValue>>);

#[derive(Default)]
struct OrderedRawProxyRoutes(Vec<(String, Option<RawProxyRoute>)>);

impl<'de> Deserialize<'de> for OrderedRawProxyRoutes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OrderedRoutesVisitor;

        impl<'de> Visitor<'de> for OrderedRoutesVisitor {
            type Value = OrderedRawProxyRoutes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a map of Fastjson SOCKS proxy routes")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut routes: Vec<(String, Option<RawProxyRoute>)> = Vec::new();
                while let Some((key, route)) = map.next_entry()? {
                    if let Some((_, existing)) = routes
                        .iter_mut()
                        .find(|(existing_key, _)| existing_key == &key)
                    {
                        *existing = route;
                    } else {
                        routes.push((key, route));
                    }
                }
                Ok(OrderedRawProxyRoutes(routes))
            }
        }

        deserializer.deserialize_map(OrderedRoutesVisitor)
    }
}

impl RawProxyRoute {
    fn java_string_field(&self, name: &str) -> Option<String> {
        self.0
            .get(name)
            .and_then(Option::as_ref)
            .map(FastjsonValue::java_string)
    }

    fn reference(&self) -> Option<&str> {
        match self.0.get("$ref").and_then(Option::as_ref) {
            Some(FastjsonValue::String(value)) => Some(value),
            _ => None,
        }
    }
}

fn fastjson_root_reference_key(path: &str) -> Option<String> {
    if let Some(key) = path.strip_prefix("$.") {
        return (!key.is_empty() && !key.chars().any(|value| matches!(value, '.' | '[' | ']')))
            .then(|| key.to_owned());
    }

    let bytes = path.as_bytes();
    if !bytes.starts_with(b"$[") || bytes.last() != Some(&b']') {
        return None;
    }
    let mut index = 2;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        index += 1;
    }
    let quote = match bytes.get(index) {
        Some(b'\'') => b'\'',
        Some(b'\"') => b'\"',
        _ => return None,
    };
    index += 1;
    let mut key = String::new();
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                index += 1;
                let escaped = *bytes.get(index)?;
                key.push(char::from(escaped));
                index += 1;
            }
            current if current == quote => {
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|byte| byte.is_ascii_whitespace())
                {
                    index += 1;
                }
                return (bytes.get(index..) == Some(b"]") && index + 1 == bytes.len())
                    .then_some(key);
            }
            current if current.is_ascii() => {
                key.push(char::from(current));
                index += 1;
            }
            _ => {
                let rest = std::str::from_utf8(bytes.get(index..)?).ok()?;
                let character = rest.chars().next()?;
                key.push(character);
                index += character.len_utf8();
            }
        }
    }
    None
}

fn resolve_proxy_route_reference(
    key: &str,
    routes: &HashMap<String, Option<RawProxyRoute>>,
    resolving: &mut HashSet<String>,
) -> Result<ParsedProxyRoute, TransportError> {
    let Some(entry) = routes.get(key) else {
        return Ok(ParsedProxyRoute::Direct);
    };
    let Some(raw) = entry else {
        return Ok(ParsedProxyRoute::Direct);
    };
    if let Some(reference) = raw.reference() {
        if reference == "#-1" {
            return Ok(ParsedProxyRoute::Direct);
        }
        if reference == "#-2" {
            return Ok(ParsedProxyRoute::MissingAddress {
                username: None,
                password: None,
            });
        }
        let Some(target) = fastjson_root_reference_key(reference) else {
            if !matches!(reference.chars().next(), Some('$' | '#' | '@' | '.')) {
                return Ok(ParsedProxyRoute::MissingAddress {
                    username: None,
                    password: None,
                });
            }
            return Err(TransportError::InvalidConfiguration(
                "Fastjson SOCKS route reference path is unsupported".into(),
            ));
        };
        let target = format!("\0S{target}");
        if !resolving.insert(key.to_owned()) {
            return Ok(ParsedProxyRoute::Direct);
        }
        let resolved = resolve_proxy_route_reference(&target, routes, resolving);
        resolving.remove(key);
        return resolved;
    }
    Ok(match raw.java_string_field("addr") {
        Some(address) => ParsedProxyRoute::Proxy(SocksProxyConfig {
            address,
            username: raw.java_string_field("username"),
            password: raw.java_string_field("password"),
        }),
        None => ParsedProxyRoute::MissingAddress {
            username: raw.java_string_field("username"),
            password: raw.java_string_field("password"),
        },
    })
}

fn contains_fastjson_rejected_literal(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut index = 0;
    let mut root_opened = false;
    while index < bytes.len() {
        // Fastjson accepts a BOM only at the start of the document. JSON5 also
        // treats most Unicode whitespace as trivia, which Fastjson rejects
        // outside strings and comments; a small source-probed set is accepted
        // after the root object opens.
        if bytes[index..].starts_with(&[0xef, 0xbb, 0xbf]) {
            if index != 0 {
                return true;
            }
            index += 3;
            continue;
        }
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        let Some(&escaped) = bytes.get(index + 1) else {
                            return true;
                        };
                        match escaped {
                            b'"' | b'\'' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
                            | b'v' | b'0' => index += 2,
                            b'x' => match fastjson_escape_digits(bytes, index, quote, 2) {
                                Some((end, _, _)) => index = end,
                                None => return true,
                            },
                            b'u' => match fastjson_escape_digits(bytes, index, quote, 4) {
                                Some((end, _, _)) => index = end,
                                None => return true,
                            },
                            escaped if fastjson_additional_escape_code_unit(escaped).is_some() => {
                                index += 2;
                            }
                            _ => match fastjson_non_ascii_escape_code_unit(bytes, index) {
                                Some((_, end, _)) => index = end,
                                None => return true,
                            },
                        }
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                let mut previous = index;
                while previous > 0 && bytes[previous - 1].is_ascii_whitespace() {
                    previous -= 1;
                }
                if previous > 0 && bytes[previous - 1] == b':' {
                    return true;
                }
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let mut previous = index;
                while previous > 0 && bytes[previous - 1].is_ascii_whitespace() {
                    previous -= 1;
                }
                if previous > 0 && bytes[previous - 1] == b':' {
                    return true;
                }
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'\x0b' => return true,
            b'.' if bytes.get(index + 1).is_some_and(u8::is_ascii_digit) => {
                let signed = index > 0
                    && matches!(bytes[index - 1], b'+' | b'-')
                    && (index < 2 || !is_number_token_byte(bytes[index - 2]));
                if !signed && (index == 0 || !is_number_token_byte(bytes[index - 1])) {
                    return true;
                }
                index += 1;
            }
            b'0' if (index == 0 || !is_number_token_byte(bytes[index - 1]))
                && matches!(bytes.get(index + 1), Some(b'x' | b'X'))
                && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit) =>
            {
                let mut end = index + 2;
                while bytes.get(end).is_some_and(u8::is_ascii_hexdigit) {
                    end += 1;
                }
                if bytes
                    .get(end)
                    .is_none_or(|byte| !is_number_token_byte(*byte))
                {
                    let next = skip_json5_trivia(bytes, end);
                    if bytes.get(next) != Some(&b':') {
                        return true;
                    }
                }
                index = end;
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'$'
                if index == 0 || !is_identifier_token_byte(bytes[index - 1]) =>
            {
                let start = index;
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|byte| is_identifier_token_byte(*byte))
                {
                    index += 1;
                }
                let token = &bytes[start..index];
                let next = skip_json5_trivia(bytes, index);
                if bytes.get(next) == Some(&b':')
                    || matches!(token, b"NaN" | b"Infinity" | b"undefined")
                {
                    return true;
                }
            }
            b'{' => {
                root_opened = true;
                index += 1;
            }
            _ if !bytes[index].is_ascii() => {
                let character = input[index..]
                    .chars()
                    .next()
                    .expect("a non-ASCII byte starts a UTF-8 code point");
                if character.is_whitespace()
                    && (!root_opened || !fastjson_internal_trivia(character))
                {
                    return true;
                }
                index += character.len_utf8();
            }
            _ => index += 1,
        }
    }
    false
}

fn fastjson_internal_trivia(character: char) -> bool {
    matches!(
        character,
        '\u{0080}'
            | '\u{0088}'
            | '\u{0089}'
            | '\u{008a}'
            | '\u{008c}'
            | '\u{008d}'
            | '\u{00a0}'
            | '\u{00c0}'
            | '\u{00c8}'
            | '\u{00c9}'
            | '\u{00ca}'
            | '\u{00cc}'
            | '\u{00cd}'
            | '\u{00e0}'
    )
}

#[derive(Clone, Copy, Debug)]
enum FastjsonRouteKeyError {
    UnsignedLeadingDotNumber,
}

fn normalize_fastjson_root_route_keys(input: &str) -> Result<String, FastjsonRouteKeyError> {
    let bytes = input.as_bytes();
    let mut start = 0;
    if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        start = 3;
    }
    start = skip_fastjson_trivia(bytes, start);
    if bytes.get(start) != Some(&b'{') {
        return Ok(input.to_owned());
    }

    let mut replacements = Vec::<(usize, usize, Vec<u8>)>::new();
    let mut index = start + 1;
    loop {
        index = skip_fastjson_trivia(bytes, index);
        if matches!(bytes.get(index), Some(b'}' | b',')) || index >= bytes.len() {
            if bytes.get(index) == Some(&b',') {
                index += 1;
                continue;
            }
            break;
        }

        let key_start = index;
        let (key_end, encoded_key) = match bytes.get(index) {
            Some(b'"' | b'\'') => {
                let quote = bytes[index];
                let mut end = index + 1;
                while end < bytes.len() {
                    if bytes[end] == b'\\' {
                        end = (end + 2).min(bytes.len());
                    } else if bytes[end] == quote {
                        end += 1;
                        break;
                    } else {
                        end += 1;
                    }
                }
                if end > bytes.len() || bytes.get(end - 1) != Some(&quote) {
                    break;
                }
                let mut encoded = Vec::with_capacity(end - index + 8);
                encoded.push(quote);
                encoded.extend_from_slice(b"\\u0000S");
                encoded.extend_from_slice(&bytes[index + 1..end - 1]);
                encoded.push(quote);
                (end, Some(encoded))
            }
            _ => {
                let decimal_end = decimal_number_end(bytes, index)
                    .filter(|end| bytes.get(skip_fastjson_trivia(bytes, *end)) == Some(&b':'));
                if bytes.get(index) == Some(&b'.') && decimal_end.is_some() {
                    return Err(FastjsonRouteKeyError::UnsignedLeadingDotNumber);
                }
                let numeric_end = decimal_end;
                let token_end = numeric_end.unwrap_or_else(|| {
                    let mut end = index;
                    while bytes
                        .get(end)
                        .is_some_and(|byte| is_identifier_token_byte(*byte))
                    {
                        end += 1;
                    }
                    end
                });
                let token = &bytes[index..token_end];
                let encoded = if numeric_end.is_some() {
                    let token = std::str::from_utf8(token).unwrap_or_default();
                    let mut text = String::new();
                    write_json_string(
                        &format!("\0S{}", java_route_key_number_string(token)),
                        &mut text,
                    );
                    Some(text.into_bytes())
                } else if matches!(token, b"true" | b"false") {
                    let mut encoded = Vec::new();
                    let mut text = String::new();
                    write_json_string(
                        &format!("\0S{}", std::str::from_utf8(token).unwrap()),
                        &mut text,
                    );
                    encoded.extend_from_slice(text.as_bytes());
                    Some(encoded)
                } else if token == b"null" {
                    let mut encoded = Vec::new();
                    let mut text = String::new();
                    write_json_string("\0N", &mut text);
                    encoded.extend_from_slice(text.as_bytes());
                    Some(encoded)
                } else {
                    None
                };
                (token_end, encoded)
            }
        };
        if key_end == key_start || bytes.get(skip_fastjson_trivia(bytes, key_end)) != Some(&b':') {
            break;
        }
        if let Some(encoded_key) = encoded_key {
            replacements.push((key_start, key_end, encoded_key));
        }
        let colon = skip_fastjson_trivia(bytes, key_end);
        index = skip_fastjson_root_value(bytes, colon + 1);
        if bytes.get(index) == Some(&b',') {
            index += 1;
        } else {
            break;
        }
    }

    if replacements.is_empty() {
        return Ok(input.to_owned());
    }
    let mut output = Vec::with_capacity(bytes.len() + replacements.len() * 8);
    let mut copied_through = 0;
    for (key_start, key_end, encoded_key) in replacements {
        output.extend_from_slice(&bytes[copied_through..key_start]);
        output.extend_from_slice(&encoded_key);
        copied_through = key_end;
    }
    output.extend_from_slice(&bytes[copied_through..]);
    Ok(String::from_utf8(output).expect("root-key normalization preserves UTF-8"))
}

fn skip_fastjson_trivia(bytes: &[u8], mut index: usize) -> usize {
    loop {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"//") {
            index += 2;
            while index < bytes.len()
                && bytes[index] != b'\n'
                && bytes[index] != b'\r'
                && !bytes[index..].starts_with(&[0xe2, 0x80, 0xa8])
                && !bytes[index..].starts_with(&[0xe2, 0x80, 0xa9])
            {
                index += 1;
            }
        } else if bytes.get(index..index + 2) == Some(b"/*") {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
        } else {
            return index;
        }
    }
}

fn skip_fastjson_root_value(bytes: &[u8], mut index: usize) -> usize {
    let mut closers = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index = skip_fastjson_trivia(bytes, index);
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_fastjson_trivia(bytes, index);
            }
            b'{' => {
                closers.push(b'}');
                index += 1;
            }
            b'[' => {
                closers.push(b']');
                index += 1;
            }
            b'}' | b']' if closers.is_empty() => return index,
            b'}' | b']' => {
                closers.pop();
                index += 1;
            }
            b',' if closers.is_empty() => return index,
            _ => index += 1,
        }
    }
    index
}

fn decode_fastjson_route_key(key: String) -> Option<String> {
    if let Some(value) = key.strip_prefix("\0S") {
        Some(value.to_owned())
    } else if key == "\0N" {
        None
    } else {
        Some(key)
    }
}

fn is_number_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$' | b'.')
}

fn is_identifier_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

fn skip_json5_trivia(bytes: &[u8], mut index: usize) -> usize {
    loop {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"//") {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
        } else if bytes.get(index..index + 2) == Some(b"/*") {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
        } else {
            return index;
        }
    }
}

fn normalize_fastjson_source(input: &str) -> String {
    // Fastjson drops line separators from line comments and permits them in strings,
    // while Rust Strings cannot hold isolated UTF-16 surrogate code units.
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut root_opened = false;
    while index < bytes.len() {
        if bytes.get(index..index + 2) == Some(b"//") {
            output.extend_from_slice(b"//");
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                if bytes[index] == b'\r'
                    || bytes[index..].starts_with(&[0xe2, 0x80, 0xa8])
                    || bytes[index..].starts_with(&[0xe2, 0x80, 0xa9])
                {
                    index += if bytes[index] == b'\r' { 1 } else { 3 };
                } else {
                    output.push(bytes[index]);
                    index += 1;
                }
            }
            if bytes.get(index) == Some(&b'\n') {
                // The LF terminates Fastjson's line comment and must survive
                // the outside-string control-character normalization below.
                output.push(b'\n');
                index += 1;
            }
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"/*") {
            let start = index;
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            if index + 1 < bytes.len() {
                index += 2;
            } else {
                index = bytes.len();
            }
            output.extend_from_slice(&bytes[start..index]);
            continue;
        }
        if !matches!(bytes[index], b'"' | b'\'') {
            if matches!(bytes[index], 0 | 8 | 9 | 10 | 12 | 13) {
                // Fastjson 2.0.63's JSONReaderUTF16.next skips this exact
                // control-character bitset in addition to ordinary spaces.
                output.push(b' ');
                index += 1;
                continue;
            }
            if !bytes[index].is_ascii() {
                let character = input[index..]
                    .chars()
                    .next()
                    .expect("a non-ASCII byte starts a UTF-8 code point");
                if root_opened && fastjson_internal_trivia(character) {
                    output.push(b' ');
                    index += character.len_utf8();
                    continue;
                }
                let end = index + character.len_utf8();
                output.extend_from_slice(&bytes[index..end]);
                index = end;
                continue;
            }
            if bytes[index] == b'{' {
                root_opened = true;
            }
            output.push(bytes[index]);
            index += 1;
            continue;
        }
        let quote = bytes[index];
        output.push(quote);
        index += 1;
        while index < bytes.len() {
            if bytes[index] == b'\\' {
                if let Some((code_unit, end, trailing_unit)) =
                    fastjson_escape_code_unit(bytes, index, quote, b'u')
                {
                    if (0xd800..=0xdbff).contains(&code_unit) {
                        if let Some(low) =
                            trailing_unit.filter(|low| (0xdc00..=0xdfff).contains(low))
                        {
                            // Fastjson counts UTF-16 units, so a supplementary scalar can
                            // straddle the fourth digit. Its high unit completes this escape
                            // and its low unit pairs with the decoded high surrogate.
                            push_json_unicode_escape(&mut output, code_unit);
                            push_json_unicode_escape(&mut output, low);
                            index = end;
                        } else {
                            match fastjson_escape_code_unit(bytes, end, quote, b'u') {
                                Some((low, paired_end, trailing_low))
                                    if (0xdc00..=0xdfff).contains(&low) =>
                                {
                                    push_json_unicode_escape(&mut output, code_unit);
                                    push_json_unicode_escape(&mut output, low);
                                    if trailing_low.is_some() {
                                        output.extend_from_slice(b"\\uFFFD");
                                    }
                                    index = paired_end;
                                }
                                _ => {
                                    output.extend_from_slice(b"\\uFFFD");
                                    index = end;
                                }
                            }
                        }
                    } else if (0xdc00..=0xdfff).contains(&code_unit) {
                        output.extend_from_slice(b"\\uFFFD");
                        if trailing_unit.is_some() {
                            output.extend_from_slice(b"\\uFFFD");
                        }
                        index = end;
                    } else {
                        push_json_unicode_escape(&mut output, code_unit);
                        if trailing_unit.is_some() {
                            // The low unit of a supplementary scalar fell outside this
                            // fixed-width escape and remains raw, unpaired string content.
                            output.extend_from_slice(b"\\uFFFD");
                        }
                        index = end;
                    }
                } else if let Some((code_unit, end, _trailing_unit)) =
                    fastjson_escape_code_unit(bytes, index, quote, b'x')
                {
                    push_json_unicode_escape(&mut output, code_unit);
                    index = end;
                } else if let Some((escaped, code_unit)) =
                    bytes.get(index + 1).copied().and_then(|escaped| {
                        fastjson_additional_escape_code_unit(escaped)
                            .map(|code_unit| (escaped, code_unit))
                    })
                {
                    if fastjson_identity_escape(escaped) {
                        output.push(escaped);
                    } else {
                        push_json_unicode_escape(&mut output, code_unit);
                    }
                    index += 2;
                } else if let Some((code_unit, end, trailing_surrogate)) =
                    fastjson_non_ascii_escape_code_unit(bytes, index)
                {
                    push_json_unicode_escape(&mut output, code_unit);
                    if trailing_surrogate {
                        // Fastjson consumes one UTF-16 unit after the slash;
                        // the unpaired low surrogate then remains as raw text.
                        output.extend_from_slice(b"\\uFFFD");
                    }
                    index = end;
                } else {
                    let end = (index + 2).min(bytes.len());
                    output.extend_from_slice(&bytes[index..end]);
                    index = end;
                }
            } else if bytes[index..].starts_with(&[0xe2, 0x80, 0xa8]) {
                output.extend_from_slice(b"\\u2028");
                index += 3;
            } else {
                let byte = bytes[index];
                output.push(byte);
                index += 1;
                if byte == quote {
                    break;
                }
            }
        }
    }
    String::from_utf8(output).expect("source normalization preserves valid UTF-8")
}

fn fastjson_escape_digits(
    bytes: &[u8],
    start: usize,
    quote: u8,
    count: usize,
) -> Option<(usize, [u16; 4], Option<u16>)> {
    let marker = *bytes.get(start.checked_add(1)?)?;
    let mut index = start.checked_add(2)?;
    if count > 4 {
        return None;
    }
    let mut digits = [0; 4];
    let mut digit_count = 0;
    let mut trailing_unit = None;
    while digit_count < count {
        let tail = std::str::from_utf8(bytes.get(index..)?).ok()?;
        let escaped = tail.chars().next()?;
        if escaped == char::from(quote) {
            return None;
        }
        if marker == b'x' && u32::from(escaped) > 0x66 {
            // Fastjson 2.0.63 indexes its x-escape lookup table directly with
            // each UTF-16 code unit; values above ASCII 'f' throw.
            return None;
        }
        let mut encoded = [0; 2];
        for unit in escaped.encode_utf16(&mut encoded).iter().copied() {
            if digit_count == count {
                trailing_unit = Some(unit);
                break;
            }
            digits[digit_count] = unit;
            digit_count += 1;
        }
        index += escaped.len_utf8();
    }
    Some((index, digits, trailing_unit))
}

fn fastjson_escape_code_unit(
    bytes: &[u8],
    start: usize,
    quote: u8,
    marker: u8,
) -> Option<(u16, usize, Option<u16>)> {
    if bytes.get(start) != Some(&b'\\') || bytes.get(start + 1) != Some(&marker) {
        return None;
    }
    let count = if marker == b'u' { 4 } else { 2 };
    let (end, digits, trailing_unit) = fastjson_escape_digits(bytes, start, quote, count)?;
    let mut value = 0u16;
    let marker = bytes[start + 1];
    for digit in digits.iter().take(count).copied() {
        let nibble = if marker == b'x' {
            fastjson_x_escape_digit(digit)?
        } else {
            fastjson_hex_digit_or_zero(digit)
        };
        value = value.wrapping_shl(4).wrapping_add(u16::from(nibble));
    }
    Some((value, end, trailing_unit))
}

fn fastjson_non_ascii_escape_code_unit(bytes: &[u8], start: usize) -> Option<(u16, usize, bool)> {
    if bytes.get(start) != Some(&b'\\') {
        return None;
    }
    let follower_start = start.checked_add(1)?;
    let tail = std::str::from_utf8(bytes.get(follower_start..)?).ok()?;
    let follower = tail.chars().next()?;
    if follower.is_ascii() {
        return None;
    }
    let mut encoded = [0; 2];
    let units = follower.encode_utf16(&mut encoded);
    let first = *units.first()?;
    let trailing_surrogate = units.len() == 2;
    let code_unit = fastjson_char1_escape_code_unit(first)?;
    Some((
        code_unit,
        follower_start + follower.len_utf8(),
        trailing_surrogate,
    ))
}

fn fastjson_char1_escape_code_unit(escaped: u16) -> Option<u16> {
    let escaped = (escaped & 0x7f) as u8;
    match escaped {
        b'0'..=b'7' => Some(u16::from(escaped - b'0')),
        b'"' | b'\'' | b'/' | b'\\' => Some(u16::from(escaped)),
        b'b' => Some(0x0008),
        b'f' | b'F' => Some(0x000c),
        b'n' => Some(0x000a),
        b'r' => Some(0x000d),
        b't' => Some(0x0009),
        b'v' => Some(0x000b),
        byte if fastjson_identity_escape(byte) => Some(u16::from(byte)),
        _ => None,
    }
}

fn fastjson_hex_digit_or_zero(code_unit: u16) -> u8 {
    // Fastjson 2.0.63's packed UTF-16 hex decoder folds each char's low byte
    // into a nibble. This reproduces its results for non-ASCII code units too.
    ((code_unit & 0x0f) + if code_unit & 0x40 != 0 { 9 } else { 0 }) as u8 & 0x0f
}

fn fastjson_x_escape_digit(code_unit: u16) -> Option<u8> {
    if code_unit > u16::from(b'f') {
        return None;
    }
    Some(match code_unit as u8 {
        b'0'..=b'9' => code_unit as u8 - b'0',
        b'a'..=b'f' => code_unit as u8 - b'a' + 10,
        b'A'..=b'F' => code_unit as u8 - b'A' + 10,
        _ => 0,
    })
}

fn fastjson_additional_escape_code_unit(escaped: u8) -> Option<u16> {
    match escaped {
        b'1'..=b'7' => Some(u16::from(escaped - b'0')),
        b'F' => Some(0x000c),
        byte if fastjson_identity_escape(byte) => Some(u16::from(byte)),
        _ => None,
    }
}

fn fastjson_identity_escape(escaped: u8) -> bool {
    matches!(
        escaped,
        b' ' | b'#' | b'&' | b'(' | b')' | b',' | b'.' | b'@' | b'[' | b']' | b'_' | b'~'
    )
}

fn push_json_unicode_escape(output: &mut Vec<u8>, value: u16) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    output.extend_from_slice(b"\\u");
    output.push(HEX[((value >> 12) & 0x0f) as usize]);
    output.push(HEX[((value >> 8) & 0x0f) as usize]);
    output.push(HEX[((value >> 4) & 0x0f) as usize]);
    output.push(HEX[(value & 0x0f) as usize]);
}

fn recover_fastjson_eof_line_comment(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut index = 0;
    let mut closing = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                let comment_start = index;
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                if index == bytes.len() {
                    if closing.len() > 1 {
                        // Fastjson tolerates an unfinished root object here, but not a nested proxy object.
                        return input.to_owned();
                    }
                    let mut output =
                        String::from_utf8(bytes[..comment_start].to_vec()).expect("valid UTF-8");
                    for delimiter in closing.iter().rev() {
                        output.push(*delimiter as char);
                    }
                    return output;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'{' => {
                closing.push(b'}');
                index += 1;
            }
            b'[' => {
                closing.push(b']');
                index += 1;
            }
            0x1a => {
                // Fastjson's UTF-16 reader treats SUB as its end-of-input sentinel.
                // It can finish an open root object, but not a nested value.
                if closing.len() > 1 {
                    return String::from_utf8(bytes[..index].to_vec()).expect("valid UTF-8");
                }
                let mut output = String::from_utf8(bytes[..index].to_vec()).expect("valid UTF-8");
                for delimiter in closing.iter().rev() {
                    output.push(*delimiter as char);
                }
                return output;
            }
            b'}' | b']' => {
                if closing.pop() != Some(bytes[index]) {
                    return input.to_owned();
                }
                index += 1;
            }
            _ => index += 1,
        }
    }
    input.to_owned()
}

fn normalize_fastjson_numbers(input: &str) -> (String, VecDeque<String>) {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut tokens = VecDeque::new();
    let mut copied_through = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            _ => {
                let Some(end) = decimal_number_end(bytes, index) else {
                    index += 1;
                    continue;
                };
                if index > 0 && is_number_token_byte(bytes[index - 1]) {
                    index = end;
                    continue;
                }
                let next = skip_json5_trivia(bytes, end);
                if bytes.get(next) == Some(&b':') {
                    index = end;
                    continue;
                }
                tokens.push_back(String::from_utf8_lossy(&bytes[index..end]).into_owned());
                output.extend_from_slice(&bytes[copied_through..index]);
                output.push(b'0');
                copied_through = end;
                index = end;
            }
        }
    }
    output.extend_from_slice(&bytes[copied_through..]);
    (
        String::from_utf8(output).expect("normalization preserves valid UTF-8"),
        tokens,
    )
}

fn decimal_number_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start;
    if matches!(bytes.get(index), Some(b'+' | b'-')) {
        if index > 0 && is_number_token_byte(bytes[index - 1]) {
            return None;
        }
        index += 1;
    } else if index > 0 && is_number_token_byte(bytes[index - 1]) {
        return None;
    }

    if !matches!(bytes.get(index), Some(b'0'..=b'9' | b'.')) {
        return None;
    }
    if bytes.get(index) == Some(&b'0')
        && matches!(
            bytes.get(index + 1),
            Some(b'x' | b'X' | b'b' | b'B' | b'o' | b'O')
        )
    {
        return None;
    }

    let mut digits = 0;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        digits += 1;
        index += 1;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            digits += 1;
            index += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        let exponent = index;
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if exponent_start == index {
            return Some(exponent);
        }
    }
    if matches!(
        bytes.get(index),
        Some(b'd' | b'D' | b'f' | b'F' | b'l' | b'L')
    ) && !bytes
        .get(index + 1)
        .is_some_and(|byte| is_number_token_byte(*byte))
    {
        if matches!(bytes[index], b'l' | b'L')
            && bytes[start..index]
                .iter()
                .any(|byte| matches!(byte, b'.' | b'e' | b'E'))
        {
            return None;
        }
        return Some(index + 1);
    }
    if bytes
        .get(index)
        .is_some_and(|byte| is_number_token_byte(*byte))
    {
        return None;
    }
    Some(index)
}

impl SocksProxyConfig {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            username: None,
            password: None,
        }
    }

    pub fn with_credentials(
        address: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            address: address.into(),
            username: Some(username.into()),
            password: Some(password.into()),
        }
    }
}

/// Client controls mapped from Java `NettyClientConfig` settings used by this transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ConnectionSettings {
    pub(super) send_buffer_size: i32,
    pub(super) receive_buffer_size: i32,
    pub(super) write_buffer_watermarks: (usize, usize),
    pub(super) pooled_byte_buffer_allocator_enabled: bool,
}

impl ConnectionSettings {
    pub(super) fn direct(
        send_buffer_size: i32,
        receive_buffer_size: i32,
        write_buffer_low_water_mark: i32,
        write_buffer_high_water_mark: i32,
        pooled_byte_buffer_allocator_enabled: bool,
    ) -> Result<Self, TransportError> {
        if write_buffer_low_water_mark > 0
            && write_buffer_high_water_mark > 0
            && write_buffer_low_water_mark > write_buffer_high_water_mark
        {
            return Err(TransportError::InvalidConfiguration(
                "write buffer low water mark must not exceed high water mark when both are enabled"
                    .into(),
            ));
        }

        Ok(Self {
            send_buffer_size,
            receive_buffer_size,
            write_buffer_watermarks: watermarks(
                write_buffer_low_water_mark,
                write_buffer_high_water_mark,
            ),
            pooled_byte_buffer_allocator_enabled,
        })
    }

    pub(super) fn proxy(
        send_buffer_size: i32,
        receive_buffer_size: i32,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            send_buffer_size,
            receive_buffer_size,
            write_buffer_watermarks: (64 * 1024, 32 * 1024),
            pooled_byte_buffer_allocator_enabled: false,
        })
    }
}

fn watermarks(low: i32, high: i32) -> (usize, usize) {
    if low > 0 && high > 0 {
        (high as usize, low as usize)
    } else {
        (64 * 1024, 32 * 1024)
    }
}

#[derive(Clone, Debug)]
/// Configuration values for a [`RemotingClient`](super::RemotingClient).
///
/// Configuration validation occurs when constructing a client, while the
/// signed worker count is checked by `start()` like Java's worker-group setup.
/// Java has no standalone `NettyClientConfig` validator.
///
/// ```compile_fail
/// use rocketmq_classic_remoting::client::ClientConfig;
/// let _ = ClientConfig::default().validate();
/// ```
pub struct ClientConfig {
    pub transport: TransportMode,
    /// Java `connectTimeoutMillis`; negative values leave Netty's 30-second default in effect.
    pub connect_timeout: i32,
    /// Java `channelNotActiveInterval` in milliseconds; inert in the pinned client's normal flow.
    pub channel_not_active_interval: i64,
    pub async_semaphore_value: i32,
    pub oneway_semaphore_value: i32,
    /// Java `clientWorkerThreads`; accepts the signed `int` domain and requires a positive value at `start()`.
    pub worker_threads: i32,
    /// Java `clientCallbackExecutorThreads`; non-positive values use four callback workers at start.
    pub callback_executor_threads: i32,
    pub disable_callback_executor: bool,
    /// Java `clientSocketSndBufSize`; retains the signed `int` value.
    pub client_socket_send_buffer_size: i32,
    /// Java `clientSocketRcvBufSize`; retains the signed `int` value.
    pub client_socket_receive_buffer_size: i32,
    /// Java `writeBufferHighWaterMark`; retains the signed `int` value.
    pub write_buffer_high_water_mark: i32,
    /// Java `writeBufferLowWaterMark`; retains the signed `int` value.
    pub write_buffer_low_water_mark: i32,
    pub pooled_byte_buffer_allocator_enabled: bool,
    pub disable_netty_worker_group: bool,
    /// Java `clientChannelMaxIdleTimeSeconds`; non-positive values disable idle detection.
    pub max_idle_time: i32,
    pub close_socket_if_timeout: bool,
    pub scan_available_nameserver: bool,
    pub enable_reconnect_for_go_away: bool,
    /// Java `long maxReconnectIntervalTimeSeconds`; retained but unused by the pinned client.
    pub max_reconnect_interval_time_seconds: i64,
    /// Java `NettyClientConfig.socksProxyConfig` JSON text, keyed by IPv4 CIDR.
    pub socks_proxy_config: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        let defaults = java_client_defaults();
        Self {
            transport: TransportMode::Plain,
            connect_timeout: defaults.connect_timeout,
            channel_not_active_interval: 60_000,
            async_semaphore_value: defaults.async_semaphore_value,
            oneway_semaphore_value: defaults.oneway_semaphore_value,
            worker_threads: defaults.worker_threads,
            callback_executor_threads: std::thread::available_parallelism()
                .map(|parallelism| parallelism.get().min(i32::MAX as usize) as i32)
                .unwrap_or(1),
            disable_callback_executor: false,
            client_socket_send_buffer_size: defaults.client_socket_send_buffer_size,
            client_socket_receive_buffer_size: defaults.client_socket_receive_buffer_size,
            write_buffer_high_water_mark: defaults.write_buffer_high_water_mark,
            write_buffer_low_water_mark: defaults.write_buffer_low_water_mark,
            pooled_byte_buffer_allocator_enabled: false,
            disable_netty_worker_group: false,
            max_idle_time: defaults.max_idle_time,
            close_socket_if_timeout: defaults.close_socket_if_timeout,
            scan_available_nameserver: true,
            enable_reconnect_for_go_away: true,
            max_reconnect_interval_time_seconds: 60,
            socks_proxy_config: "{}".into(),
        }
    }
}

impl ClientConfig {
    #[cfg(test)]
    pub(super) fn max_idle_time_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.max_idle_time.max(0) as u64)
    }

    pub(super) fn validate(&self) -> Result<(), TransportError> {
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn write_buffer_watermarks(&self) -> (usize, usize) {
        watermarks(
            self.write_buffer_low_water_mark,
            self.write_buffer_high_water_mark,
        )
    }

    /// Java-compatible getter/setter behavior: retain JSON text until client construction.
    pub fn get_socks_proxy_config(&self) -> &str {
        &self.socks_proxy_config
    }

    pub fn set_socks_proxy_config(&mut self, json: impl Into<String>) {
        self.socks_proxy_config = json.into();
    }

    pub(super) fn parse_socks_proxy_config(&self) -> Result<ProxyRoutes, TransportError> {
        if self.socks_proxy_config.is_empty() {
            return Ok(ProxyRoutes::default());
        }
        let source = strip_fastjson_document_prefix(&self.socks_proxy_config);
        let keyed = normalize_fastjson_root_route_keys(source).map_err(|error| {
            let reason = match error {
                FastjsonRouteKeyError::UnsignedLeadingDotNumber => {
                    "Fastjson SOCKS parser rejects an unsigned leading-dot route key"
                }
            };
            TransportError::InvalidConfiguration(reason.into())
        })?;
        if contains_fastjson_rejected_literal(&keyed) {
            return Err(TransportError::InvalidConfiguration(
                "Fastjson SOCKS parser rejects this JSON5 literal".into(),
            ));
        }
        let string_normalized = normalize_fastjson_source(&keyed);
        let recovered = recover_fastjson_eof_line_comment(&string_normalized);
        let (normalized, number_tokens) = normalize_fastjson_numbers(&recovered);
        let _number_scope = FastjsonNumberTokenScope::install(number_tokens);
        let parsed: Option<OrderedRawProxyRoutes> = json5::from_str(&normalized)
            .map_err(|error| TransportError::InvalidConfiguration(error.to_string()))?;
        let ordered_raw_routes = parsed.unwrap_or_default().0;
        let raw_routes = ordered_raw_routes
            .iter()
            .cloned()
            .collect::<HashMap<_, _>>();
        let routes = ordered_raw_routes
            .iter()
            .map(|(encoded_cidr, _)| {
                let route =
                    resolve_proxy_route_reference(encoded_cidr, &raw_routes, &mut HashSet::new())?;
                Ok((decode_fastjson_route_key(encoded_cidr.clone()), route))
            })
            .collect::<Result<Vec<_>, TransportError>>()?;
        Ok(ProxyRoutes::from_entries(routes))
    }

    pub(super) fn proxy_for<T, R>(
        routes: &R,
        host: &str,
    ) -> Result<Option<ProxySelection>, TransportError>
    where
        T: Clone + Into<ParsedProxyRoute>,
        R: ProxyRouteMap<T>,
    {
        if routes.contains_null_key() {
            return Err(TransportError::InvalidConfiguration(
                "cannot select a SOCKS route with a null CIDR key".into(),
            ));
        }
        for (cidr, route) in routes.entries_in_iteration_order() {
            let cidr = cidr.as_ref().expect("null route keys rejected above");
            if cidr == "0.0.0.0/0" || ipv4_in_cidr(host, cidr)? {
                return Ok(Some(ProxySelection {
                    cidr: cidr.to_owned(),
                    route: route.clone().into(),
                }));
            }
        }
        Ok(None)
    }
}

pub(super) fn connect_timeout_duration(timeout_millis: i32) -> std::time::Duration {
    if timeout_millis < 0 {
        std::time::Duration::from_secs(30)
    } else {
        std::time::Duration::from_millis(timeout_millis as u64)
    }
}

pub(super) fn scan_interval(timeout_millis: i32) -> std::time::Duration {
    std::time::Duration::from_millis(timeout_millis.max(100) as u64)
}

/// Fastjson 2.0.63 skips these UTF-16 units before its root value. The
/// compatibility fixture probes every BMP code unit at the document boundary.
fn strip_fastjson_document_prefix(input: &str) -> &str {
    input.trim_start_matches(|character| {
        matches!(
            character,
            '\u{0000}'
                | '\u{0008}'
                | '\t'
                | '\n'
                | '\u{000c}'
                | '\r'
                | ' '
                | '\u{feff}'
                | '\u{fffe}'
        )
    })
}

fn ipv4_in_cidr(address: &str, cidr: &str) -> Result<bool, TransportError> {
    let address = java_ip_to_int(address)?;
    let mut parts = cidr.split('/');
    let network = parts.next().unwrap_or_default();
    let prefix = parts.next().ok_or_else(|| {
        TransportError::InvalidConfiguration(format!("invalid SOCKS proxy CIDR: {cidr}"))
    })?;
    let network = java_ip_to_int(network)?;
    let prefix = prefix.parse::<i32>().map_err(|error| {
        TransportError::InvalidConfiguration(format!("invalid SOCKS proxy CIDR {cidr}: {error}"))
    })?;
    // Java int shifts mask the shift distance to five bits, including /0 and out-of-range prefixes.
    let mask = (-1_i32).wrapping_shl(32_i32.wrapping_sub(prefix) as u32);
    Ok((address & mask) == (network & mask))
}

fn java_ip_to_int(ip: &str) -> Result<i32, TransportError> {
    // Java's ipToInt reads the first four split fields and ignores any extras.
    let mut address = 0_i32;
    let mut octets = ip.split('.');
    for index in 0..4 {
        let octet = octets.next().ok_or_else(|| {
            TransportError::InvalidConfiguration(format!(
                "invalid IPv4 address in SOCKS proxy CIDR: {ip}"
            ))
        })?;
        let value = octet.parse::<i32>().map_err(|error| {
            TransportError::InvalidConfiguration(format!(
                "invalid IPv4 address in SOCKS proxy CIDR {ip}: {error}"
            ))
        })?;
        address |= value.wrapping_shl((24 - index * 8) as u32);
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::{
        ClientConfig, ConnectionSettings, ParsedProxyRoute, SocksProxyConfig,
        connect_timeout_duration, scan_interval,
    };

    #[test]
    fn frame_max_length_property_uses_java_integer_and_positive_limit_rules() {
        assert_eq!(super::parse_java_frame_max_length("4096").unwrap(), 4096);
        assert_eq!(super::parse_java_frame_max_length("١٢٣").unwrap(), 123);
        assert!(super::parse_java_frame_max_length("0").is_err());
        assert!(super::parse_java_frame_max_length("-1").is_err());
        assert!(super::parse_java_frame_max_length(" 4096").is_err());
        assert!(super::parse_java_frame_max_length("2147483648").is_err());
    }

    #[test]
    fn signed_time_settings_match_java_normalization() {
        let defaults = ClientConfig::default();
        assert_eq!(
            connect_timeout_duration(defaults.connect_timeout),
            std::time::Duration::from_secs(3)
        );
        assert_eq!(
            scan_interval(defaults.connect_timeout),
            std::time::Duration::from_secs(3)
        );

        let negative = ClientConfig {
            connect_timeout: -1,
            max_idle_time: -1,
            channel_not_active_interval: -1,
            ..defaults
        };
        assert!(negative.validate().is_ok());
        assert_eq!(
            connect_timeout_duration(negative.connect_timeout),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            scan_interval(negative.connect_timeout),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(negative.max_idle_time_duration(), std::time::Duration::ZERO);
    }

    #[test]
    fn write_buffer_watermarks_match_java_enablement() {
        let defaults = ClientConfig::default();
        assert_eq!(defaults.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let low_only = ClientConfig {
            write_buffer_low_water_mark: 10,
            ..defaults.clone()
        };
        assert!(low_only.validate().is_ok());
        assert_eq!(low_only.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let high_only = ClientConfig {
            write_buffer_high_water_mark: 128,
            ..defaults.clone()
        };
        assert!(high_only.validate().is_ok());
        assert_eq!(high_only.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let enabled = ClientConfig {
            write_buffer_high_water_mark: 128,
            write_buffer_low_water_mark: 32,
            ..defaults.clone()
        };
        assert_eq!(enabled.write_buffer_watermarks(), (128, 32));

        let disabled = ClientConfig {
            write_buffer_high_water_mark: 128,
            write_buffer_low_water_mark: 0,
            ..defaults.clone()
        };
        assert_eq!(disabled.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let invalid = ClientConfig {
            write_buffer_high_water_mark: 32,
            write_buffer_low_water_mark: 128,
            ..defaults
        };
        assert!(invalid.validate().is_ok());
        assert!(
            ConnectionSettings::direct(
                invalid.client_socket_send_buffer_size,
                invalid.client_socket_receive_buffer_size,
                invalid.write_buffer_low_water_mark,
                invalid.write_buffer_high_water_mark,
                invalid.pooled_byte_buffer_allocator_enabled,
            )
            .is_err()
        );
    }

    #[test]
    fn java_socks_proxy_json_round_trips_java_field_names() {
        let mut config = ClientConfig::default();
        let json = r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"user","password":"secret"}}"#;
        config.set_socks_proxy_config(json);
        assert_eq!(config.get_socks_proxy_config(), json);
        assert!(matches!(
            &config.parse_socks_proxy_config().unwrap()[&Some("10.1.0.0/16".into())],
            super::ParsedProxyRoute::Proxy(proxy)
                if proxy == &SocksProxyConfig::with_credentials("proxy.internal:1080", "user", "secret")
        ));
        let malformed = ClientConfig {
            socks_proxy_config: r#"{"broken":{]"#.into(),
            ..ClientConfig::default()
        };
        assert!(malformed.validate().is_ok());
        assert!(malformed.parse_socks_proxy_config().is_err());
        let null_map = ClientConfig {
            socks_proxy_config: "null".into(),
            ..ClientConfig::default()
        };
        assert!(null_map.parse_socks_proxy_config().unwrap().is_empty());
    }

    #[test]
    fn socks_route_keys_preserve_null_separately_from_string_null() {
        let mut null_config = ClientConfig::default();
        null_config.set_socks_proxy_config("{null:null}");
        let null_route = null_config.parse_socks_proxy_config().unwrap();
        assert!(null_route.contains_key(&None));
        assert!(ClientConfig::proxy_for(&null_route, "127.0.0.1").is_err());

        let mut config = ClientConfig::default();
        config.set_socks_proxy_config("{null:null, 'null':null}");
        let routes = config.parse_socks_proxy_config().unwrap();
        assert_eq!(routes.len(), 2);
        assert!(routes.contains_key(&None));
        assert!(routes.contains_key(&Some("null".into())));
    }

    #[test]
    fn fastjson_literal_guard_ignores_quoted_and_comment_text() {
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":0x2a}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":NaN}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":Infinity}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{addr: "proxy:1080"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":"0x2a"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{'addr':"proxy:1080"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{/* NaN and 0x2a are comment text */"10.0.0.0/8":{"addr":"proxy"}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            "{\u{2028}\"10.0.0.0/8\":{\"addr\":\"proxy\"}}"
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            "{\"10.0.0.0/8\":{\"addr\":\"line\u{2028}separator\"}}"
        ));
        assert!(super::contains_fastjson_rejected_literal(
            "{\u{000b}\"10.0.0.0/8\":{\"addr\":\"proxy\"}}"
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            "{\"10.0.0.0/8\":{\"addr\":\"line\u{000b}separator\"}}"
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{0x2a:{"addr":"proxy"}}"#
        ));
    }

    #[test]
    fn every_bmp_leading_code_unit_matches_invoked_java_fastjson() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let cases = &fixture["socksLeadingCodeUnits"];
        let route_accepted = cases["route"]
            .as_array()
            .unwrap()
            .iter()
            .map(|code_unit| code_unit.as_u64().unwrap() as u16)
            .collect::<std::collections::BTreeSet<_>>();
        let null_accepted = cases["null"]
            .as_array()
            .unwrap()
            .iter()
            .map(|code_unit| code_unit.as_u64().unwrap() as u16)
            .collect::<std::collections::BTreeSet<_>>();
        let route = r#"{"10.0.0.0/8":{"addr":"proxy:1080"}}"#;
        let mut config = ClientConfig::default();
        for code_unit in 0..=u16::MAX {
            assert!(
                !(0xd800..=0xdfff).contains(&code_unit)
                    || (!route_accepted.contains(&code_unit)
                        && !null_accepted.contains(&code_unit)),
                "Java cannot express a surrogate code unit as accepted leading trivia: U+{code_unit:04X}"
            );
            let prefix = String::from_utf16_lossy(&[code_unit]);
            config.set_socks_proxy_config(format!("{prefix}{route}"));
            assert_eq!(
                config.parse_socks_proxy_config().is_ok(),
                route_accepted.contains(&code_unit),
                "route prefix U+{code_unit:04X} ({prefix:?})"
            );
            config.set_socks_proxy_config(format!("{prefix}null"));
            let parsed_null = config.parse_socks_proxy_config();
            assert_eq!(
                parsed_null.is_ok(),
                null_accepted.contains(&code_unit),
                "null prefix U+{code_unit:04X} ({prefix:?})"
            );
            if null_accepted.contains(&code_unit) {
                assert!(parsed_null.unwrap().is_empty(), "U+{code_unit:04X}");
            }
        }

        let control_cases = fixture["socksControlBoundaryCases"].as_object().unwrap();
        let boundaries = [
            (
                "root_after_open",
                "{",
                r#""10.0.0.0/8":{"addr":"proxy:1080"}}"#,
            ),
            (
                "root_before_colon",
                r#"{"10.0.0.0/8""#,
                r#":{"addr":"proxy:1080"}}"#,
            ),
            (
                "root_after_colon",
                r#"{"10.0.0.0/8":"#,
                r#"{"addr":"proxy:1080"}}"#,
            ),
            (
                "proxy_after_open",
                r#"{"10.0.0.0/8":{"#,
                r#""addr":"proxy:1080"}}"#,
            ),
            (
                "proxy_before_close",
                r#"{"10.0.0.0/8":{"addr":"proxy:1080""#,
                "}}",
            ),
            (
                "root_before_close",
                r#"{"10.0.0.0/8":{"addr":"proxy:1080"}"#,
                "}",
            ),
        ];
        for (name, prefix, suffix) in boundaries {
            let accepted = control_cases[name]
                .as_array()
                .unwrap()
                .iter()
                .map(|code_unit| code_unit.as_u64().unwrap() as u16)
                .collect::<std::collections::BTreeSet<_>>();
            for code_point in 0..=32 {
                let code_unit = code_point as u16;
                let inserted = char::from_u32(code_point).unwrap().to_string();
                config.set_socks_proxy_config(format!("{prefix}{inserted}{suffix}"));
                assert_eq!(
                    config.parse_socks_proxy_config().is_ok(),
                    accepted.contains(&code_unit),
                    "{name}, U+{code_unit:04X} ({inserted:?})"
                );
            }
        }

        let unicode_cases = fixture["socksUnicodeTriviaCases"].as_object().unwrap();
        for (name, prefix, suffix) in boundaries {
            for case in unicode_cases[name].as_array().unwrap() {
                let code_unit = case["codeUnit"].as_u64().unwrap() as u32;
                let accepted = case["accepted"].as_bool().unwrap();
                let inserted = char::from_u32(code_unit).unwrap().to_string();
                config.set_socks_proxy_config(format!("{prefix}{inserted}{suffix}"));
                assert_eq!(
                    config.parse_socks_proxy_config().is_ok(),
                    accepted,
                    "{name}, U+{code_unit:04X} ({inserted:?})"
                );
            }
        }

        let bmp_cases = fixture["socksBmpBoundaryObservations"].as_object().unwrap();
        for (name, prefix, suffix) in boundaries {
            let mut expected = vec![None; u16::MAX as usize + 1];
            for group in bmp_cases[name].as_array().unwrap() {
                let observation = group["observation"].clone();
                for code_unit in group["codeUnits"].as_array().unwrap() {
                    let code_unit = code_unit.as_u64().unwrap() as usize;
                    expected[code_unit] = Some(observation.clone());
                }
            }
            for code_unit in 0..=u16::MAX {
                let inserted = char::from_u32(u32::from(code_unit))
                    .map(|character| character.to_string())
                    .unwrap_or_else(|| String::from_utf16_lossy(&[code_unit]));
                config.set_socks_proxy_config(format!("{prefix}{inserted}{suffix}"));
                let actual = config.parse_socks_proxy_config().ok().map(|routes| {
                    let mut entries = routes.into_iter().collect::<Vec<_>>();
                    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                    let entries = entries
                        .into_iter()
                        .map(|(key, route)| {
                            let route = match route {
                                ParsedProxyRoute::Direct => serde_json::Value::Null,
                                ParsedProxyRoute::MissingAddress { username, password } => {
                                    serde_json::json!({
                                        "addr": null,
                                        "username": username,
                                        "password": password
                                    })
                                }
                                ParsedProxyRoute::Proxy(proxy) => serde_json::json!({
                                    "addr": proxy.address,
                                    "username": proxy.username,
                                    "password": proxy.password
                                }),
                            };
                            serde_json::json!({"key": key, "route": route})
                        })
                        .collect::<Vec<_>>();
                    serde_json::json!({"isNull": false, "entries": entries})
                });
                assert_eq!(
                    actual,
                    expected[usize::from(code_unit)],
                    "{name}, U+{code_unit:04X} ({inserted:?})"
                );
            }
        }

        let eof_observations = fixture["socksSubstituteEofObservations"]
            .as_object()
            .unwrap();
        let eof_boundaries = [
            (
                "root_after_open",
                "{",
                r#""10.0.0.0/8":{"addr":"proxy:1080"}}"#,
            ),
            (
                "root_before_close",
                r#"{"10.0.0.0/8":{"addr":"proxy:1080"}"#,
                "}",
            ),
        ];
        for (name, prefix, suffix) in eof_boundaries {
            let observation = &eof_observations[name];
            assert_eq!(observation["outcome"], "OK");
            config.set_socks_proxy_config(format!("{prefix}\u{001a}{suffix}"));
            let routes = config.parse_socks_proxy_config().unwrap();
            let mut keys = routes
                .keys()
                .map(|key| key.clone().unwrap())
                .collect::<Vec<_>>();
            keys.sort();
            assert_eq!(
                keys,
                observation["keys"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|key| key.as_str().unwrap().to_owned())
                    .collect::<Vec<_>>(),
                "{name}"
            );
            let selected = ClientConfig::proxy_for(&routes, "10.0.0.1").unwrap();
            assert_eq!(
                selected.and_then(|selection| match selection.route {
                    super::ParsedProxyRoute::Proxy(proxy) => Some(proxy.address),
                    super::ParsedProxyRoute::Direct
                    | super::ParsedProxyRoute::MissingAddress { .. } => None,
                }),
                observation["addr"].as_str().map(str::to_owned),
                "{name}"
            );
        }
    }

    #[test]
    fn fastjson_constructor_inputs_preserve_matched_route_behavior() {
        let mut config = ClientConfig::default();
        config.set_socks_proxy_config(
            "{'10.1.0.0/16':{'addr':'proxy:1080'},'10.2.0.0/16':null,'10.3.0.0/16':{}}",
        );
        let routes = config.parse_socks_proxy_config().unwrap();
        let selected = ClientConfig::proxy_for(&routes, "10.1.2.3")
            .unwrap()
            .unwrap();
        assert_eq!(selected.cidr, "10.1.0.0/16");
        assert!(matches!(
            selected.route,
            super::ParsedProxyRoute::Proxy(SocksProxyConfig { address, .. })
                if address == "proxy:1080"
        ));
        let selected = ClientConfig::proxy_for(&routes, "10.2.2.3")
            .unwrap()
            .unwrap();
        assert_eq!(selected.cidr, "10.2.0.0/16");
        assert!(matches!(selected.route, super::ParsedProxyRoute::Direct));
        let selected = ClientConfig::proxy_for(&routes, "10.3.2.3")
            .unwrap()
            .unwrap();
        assert_eq!(selected.cidr, "10.3.0.0/16");
        assert!(selected.connect_proxy().is_err());
        assert!(matches!(
            selected.route,
            super::ParsedProxyRoute::MissingAddress { .. }
        ));
    }

    #[test]
    fn scalar_socks_fields_match_invoked_java_parsed_proxy_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let cases = fixture["socksConstruction"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            499,
            "Java oracle includes 126 prior cases, 256 byte-range followers, 15 Unicode escape cases, five Unicode-whitespace cases, ten root-shape cases, ten grammar-edge cases, four nested bean-property cases, three EOF-comment boundaries, three rejected base-prefixed route keys, eight unquoted route-key forms, five numeric padding/suffix forms, four canonical numeric-key collision cases, five cross-type key collision cases, three rejected signed base-prefixed route keys, two rejected unsigned leading-dot route keys, thirteen accepted signed/fraction/exponent/suffix route-key forms, nine rejected special/identifier route-key forms, seven exponent overflow/underflow route-key forms, four signed-zero edge/collision forms, and seven resolved, unresolved, cyclic, relative, or root Fastjson object-reference cases"
        );
        for escaped in 0..=0x7f {
            let mut expected = String::from("{\"10.0.0.0/8\":{\"addr\":\"A\\");
            expected.push(char::from_u32(escaped).unwrap());
            expected.push_str("Z\"}}");
            assert_eq!(
                cases[126 + escaped as usize]["input"],
                expected,
                "ASCII escape follower U+{escaped:04X}"
            );
        }
        for escaped in 0x80..=0xff {
            let mut expected = String::from("{\"10.0.0.0/8\":{\"addr\":\"A\\");
            expected.push(char::from_u32(escaped).unwrap());
            expected.push_str("Z\"}}");
            assert_eq!(
                cases[254 + (escaped as usize - 0x80)]["input"],
                expected,
                "non-ASCII escape follower U+{escaped:04X}"
            );
        }
        let unicode_escapes = [
            "u1λ34", "u12λ4", "u12١4", "u12１4", "u1𝟚34", "xλ0", "x0١", "u12\t4", "u12\n4", "𝟚",
            "uλ234", "u123λ", "u12𝟚4", "u123𝟚", "uD83𝟚",
        ];
        for (offset, escaped) in unicode_escapes.iter().enumerate() {
            let expected = format!("{{\"10.0.0.0/8\":{{\"addr\":\"A\\{escaped}Z\"}}}}");
            assert_eq!(
                cases[382 + offset]["input"],
                expected,
                "Unicode escape probe {offset}"
            );
        }
        for (case, expected) in cases.iter().skip(397).zip([
            "\u{00a0}{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\u{2003}\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":\u{3000}{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}\u{2029}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}\u{feff}",
        ]) {
            assert_eq!(case["input"], expected);
        }
        for (case, expected) in cases.iter().skip(402).zip([
            "[]",
            "\"text\"",
            "true",
            "42",
            "[{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}]",
            "{\"10.0.0.0/8\":[]}",
            "{\"10.0.0.0/8\":\"proxy:1080\"}",
            "{\"10.0.0.0/8\":7}",
            "{\"10.0.0.0/8\":true}",
            "{\"10.0.0.0/8\":{\"addr\":\"first\"},\"10.0.0.0/8\":{\"addr\":\"last\"}}",
        ]) {
            assert_eq!(case["input"], expected);
        }
        for (case, expected) in cases.iter().skip(412).zip([
            "{\"10.0.0.0/8\":{\"addr\":proxy}}",
            "{\"10.0.0.0/8\":{\"addr\":proxy:1080}}",
            "{\"10.0.0.0/8\":{addr:\"proxy:1080\",},}",
            "{'10.0.0.0/8':{'addr':'proxy:1080',},}",
            "/* lead */{\"10.0.0.0/8\":/* route */{\"addr\":\"proxy:1080\"}/* inner */}/* tail */",
            "// lead\r{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"username\":\"u\"// field\r,\"addr\":\"proxy:1080\"}}",
            "{\"10.0.0.0/8\":{\"addr\":[\"first\",\"last\",]}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",,}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"}} // eof comment",
        ]) {
            assert_eq!(case["input"], expected);
        }
        for (case, expected) in cases.iter().skip(422).zip([
            "{\"10.0.0.0/8\":{\"addr\":\"first\",\"addr\":\"last\",\"username\":\"u1\",\"username\":\"u2\",\"password\":\"p1\",\"password\":\"p2\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"username\":\"first\",\"username\":null,\"password\":false}}",
            "{\"10.0.0.0/8\":{\"ADDR\":\"upper-proxy:1080\",\"USERNAME\":\"upper-user\",\"PASSWORD\":\"upper-password\"}}",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\",\"userName\":\"camel-user\",\"password\":\"pw\"}}",
        ]) {
            assert_eq!(case["input"], expected);
        }
        for (case, expected) in cases.iter().skip(426).zip([
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\"} // eof with root open",
            "{\"10.0.0.0/8\":{\"addr\":\"proxy:1080\" // eof with route open",
            "{\"10.0.0.0/8\":{\"addr\":[\"first\",\"last\" // eof with array open",
        ]) {
            assert_eq!(case["input"], expected);
        }
        for (case, expected) in cases.iter().skip(cases.len() - 67).take(60).zip([
            "{default:{\"addr\":\"proxy:1080\"}}",
            "{10:{\"addr\":\"proxy:1080\"}}",
            "{true:{\"addr\":\"proxy:1080\"}}",
            "{false:{\"addr\":\"proxy:1080\"}}",
            "{null:{\"addr\":\"proxy:1080\"}}",
            "{1.5:{\"addr\":\"proxy:1080\"}}",
            "{1e2:{\"addr\":\"proxy:1080\"}}",
            "{+10:{\"addr\":\"proxy:1080\"}}",
            "{0123:{\"addr\":\"proxy:1080\"}}",
            "{123L:{\"addr\":\"proxy:1080\"}}",
            "{1.25F:{\"addr\":\"proxy:1080\"}}",
            "{1D:{\"addr\":\"proxy:1080\"}}",
            "{1e7F:{\"addr\":\"proxy:1080\"}}",
            "{1:{\"addr\":\"first\"},01:{\"addr\":\"last\"}}",
            "{01:{\"addr\":\"first\"},1:{\"addr\":\"last\"}}",
            "{1:{\"addr\":\"first\"},01:{\"addr\":\"middle\"},1:{\"addr\":\"last\"}}",
            "{1:{\"addr\":\"first\"},01:{\"addr\":\"middle\"},01:{\"addr\":\"last\"}}",
            "{1:{\"addr\":\"first\"},\"1\":{\"addr\":\"last\"}}",
            "{\"1\":{\"addr\":\"first\"},1:{\"addr\":\"last\"}}",
            "{true:{\"addr\":\"first\"},\"true\":{\"addr\":\"last\"}}",
            "{\"true\":{\"addr\":\"first\"},true:{\"addr\":\"last\"}}",
            "{null:{\"addr\":\"null-key\"},'null':{\"addr\":\"string-key\"}}",
            "{-0x2a:{\"addr\":\"proxy:1080\"}}",
            "{+0b10:{\"addr\":\"proxy:1080\"}}",
            "{-0o10:{\"addr\":\"proxy:1080\"}}",
            "{.5F:{\"addr\":\"proxy:1080\"}}",
            "{-10:{\"addr\":\"proxy:1080\"}}",
            "{-0:{\"addr\":\"proxy:1080\"}}",
            "{-0.0:{\"addr\":\"proxy:1080\"}}",
            "{-.5:{\"addr\":\"proxy:1080\"}}",
            "{+.5:{\"addr\":\"proxy:1080\"}}",
            "{.5:{\"addr\":\"proxy:1080\"}}",
            "{1e+2:{\"addr\":\"proxy:1080\"}}",
            "{1e-2:{\"addr\":\"proxy:1080\"}}",
            "{1E7:{\"addr\":\"proxy:1080\"}}",
            "{1e-4F:{\"addr\":\"proxy:1080\"}}",
            "{-10L:{\"addr\":\"proxy:1080\"}}",
            "{+10L:{\"addr\":\"proxy:1080\"}}",
            "{1.0D:{\"addr\":\"proxy:1080\"}}",
            "{1e+7D:{\"addr\":\"proxy:1080\"}}",
            "{NaN:{\"addr\":\"proxy:1080\"}}",
            "{Infinity:{\"addr\":\"proxy:1080\"}}",
            "{-Infinity:{\"addr\":\"proxy:1080\"}}",
            "{undefined:{\"addr\":\"proxy:1080\"}}",
            "{TRUE:{\"addr\":\"proxy:1080\"}}",
            "{False:{\"addr\":\"proxy:1080\"}}",
            "{NULL:{\"addr\":\"proxy:1080\"}}",
            "{Null:{\"addr\":\"proxy:1080\"}}",
            "{falsehood:{\"addr\":\"proxy:1080\"}}",
            "{1e309:{\"addr\":\"proxy:1080\"}}",
            "{-1e309:{\"addr\":\"proxy:1080\"}}",
            "{1e-1000:{\"addr\":\"proxy:1080\"}}",
            "{-1e-1000:{\"addr\":\"proxy:1080\"}}",
            "{1e-1000:{\"addr\":\"first\"},0.0:{\"addr\":\"last\"}}",
            "{0.0:{\"addr\":\"first\"},1e-1000:{\"addr\":\"last\"}}",
            "{1e309:{\"addr\":\"first\"},'Infinity':{\"addr\":\"last\"}}",
            "{-0e-1000:{\"addr\":\"proxy:1080\"}}",
            "{-1e-1000D:{\"addr\":\"proxy:1080\"}}",
            "{-1e-1000F:{\"addr\":\"proxy:1080\"}}",
            "{-1e-1000D:{\"addr\":\"first\"},0.0:{\"addr\":\"last\"}}",
        ]) {
            assert_eq!(case["input"], expected);
        }

        let mut outcome_mismatches = Vec::new();
        for case in cases.iter().skip(19) {
            let mut config = ClientConfig::default();
            config.set_socks_proxy_config(case["input"].as_str().unwrap());
            let parsed = config.parse_socks_proxy_config();
            let java_accepted = case["outcome"] == "OK";
            if parsed.is_ok() != java_accepted {
                outcome_mismatches.push(format!(
                    "input={:?}, Java={}, Rust={}",
                    case["input"],
                    case["outcome"],
                    if parsed.is_ok() { "OK" } else { "error" }
                ));
                continue;
            }
            if !java_accepted {
                continue;
            }
            let routes = parsed.unwrap();
            if let Some(expected_keys) = case["routeKeys"].as_array() {
                let mut actual_keys = routes.keys().map(Option::as_deref).collect::<Vec<_>>();
                actual_keys.sort();
                let mut expected_keys = expected_keys
                    .iter()
                    .map(serde_json::Value::as_str)
                    .collect::<Vec<_>>();
                expected_keys.sort();
                assert_eq!(
                    actual_keys, expected_keys,
                    "route keys for {}",
                    case["input"]
                );
            }
            if let Some(expected_address) = case["routeKeyOneAddr"].as_str() {
                let Some(super::ParsedProxyRoute::Proxy(proxy)) = routes.get(&Some("1".into()))
                else {
                    panic!("Java route key 1 is absent for {}", case["input"]);
                };
                assert_eq!(proxy.address, expected_address, "{}", case["input"]);
            }
            if let Some(expected_address) = case["routeKeyTrueAddr"].as_str() {
                let Some(super::ParsedProxyRoute::Proxy(proxy)) = routes.get(&Some("true".into()))
                else {
                    panic!("Java route key true is absent for {}", case["input"]);
                };
                assert_eq!(proxy.address, expected_address, "{}", case["input"]);
            }
            if let Some(expected_address) = case["routeKeyNullAddr"].as_str() {
                let Some(super::ParsedProxyRoute::Proxy(proxy)) = routes.get(&None) else {
                    panic!("Java null route key is absent for {}", case["input"]);
                };
                assert_eq!(proxy.address, expected_address, "{}", case["input"]);
            }
            if let Some(expected_address) = case["routeKeyStringNullAddr"].as_str() {
                let Some(super::ParsedProxyRoute::Proxy(proxy)) = routes.get(&Some("null".into()))
                else {
                    panic!("Java string route key null is absent for {}", case["input"]);
                };
                assert_eq!(proxy.address, expected_address, "{}", case["input"]);
            }
            if let Some(expected_address) = case["referencedRouteAddr"].as_str() {
                let Some(super::ParsedProxyRoute::Proxy(proxy)) =
                    routes.get(&Some("192.168.0.0/16".into()))
                else {
                    panic!("Java referenced route is absent for {}", case["input"]);
                };
                assert_eq!(proxy.address, expected_address, "{}", case["input"]);
                assert_eq!(
                    proxy.username.as_deref(),
                    case["referencedRouteUsername"].as_str(),
                    "{}",
                    case["input"]
                );
                assert_eq!(
                    proxy.password.as_deref(),
                    case["referencedRoutePassword"].as_str(),
                    "{}",
                    case["input"]
                );
                assert_eq!(
                    super::ClientConfig::proxy_for(&routes, "192.168.1.9")
                        .unwrap()
                        .as_ref()
                        .and_then(|selection| match &selection.route {
                            super::ParsedProxyRoute::Proxy(proxy) => Some(proxy.address.as_str()),
                            super::ParsedProxyRoute::Direct
                            | super::ParsedProxyRoute::MissingAddress { .. } => None,
                        }),
                    Some(expected_address),
                    "{}",
                    case["input"]
                );
            }
            if let Some(expected_address) = case["chainedReferenceAddr"].as_str() {
                let Some(super::ParsedProxyRoute::Proxy(proxy)) =
                    routes.get(&Some("172.16.0.0/12".into()))
                else {
                    panic!("Java chained reference is absent for {}", case["input"]);
                };
                assert_eq!(proxy.address, expected_address, "{}", case["input"]);
                assert_eq!(
                    proxy.username.as_deref(),
                    case["chainedReferenceUsername"].as_str(),
                    "{}",
                    case["input"]
                );
                assert_eq!(
                    proxy.password.as_deref(),
                    case["chainedReferencePassword"].as_str(),
                    "{}",
                    case["input"]
                );
                assert_eq!(
                    super::ClientConfig::proxy_for(&routes, "172.16.1.9")
                        .unwrap()
                        .as_ref()
                        .and_then(|selection| match &selection.route {
                            super::ParsedProxyRoute::Proxy(proxy) => Some(proxy.address.as_str()),
                            super::ParsedProxyRoute::Direct
                            | super::ParsedProxyRoute::MissingAddress { .. } => None,
                        }),
                    Some(expected_address),
                    "{}",
                    case["input"]
                );
            }
            let Some(route) = routes.get(&Some("10.0.0.0/8".into())) else {
                assert_eq!(case["routePresent"], false, "{}", case["input"]);
                continue;
            };
            assert_eq!(case["routePresent"], true, "{}", case["input"]);
            match route {
                super::ParsedProxyRoute::Proxy(proxy) => {
                    if let Some(code_units) = case["parsedAddrUtf16"].as_array() {
                        let code_units = code_units
                            .iter()
                            .map(|unit| unit.as_u64().unwrap() as u16)
                            .collect::<Vec<_>>();
                        // Java String can retain lone surrogates; native String
                        // replaces each unpaired code unit with U+FFFD.
                        let native_expected = code_units
                            .iter()
                            .map(|unit| {
                                if (0xd800..=0xdfff).contains(unit) {
                                    '\u{fffd}'
                                } else {
                                    char::from_u32(u32::from(*unit)).unwrap()
                                }
                            })
                            .collect::<String>();
                        assert_eq!(proxy.address, native_expected);
                    } else {
                        assert_eq!(Some(proxy.address.as_str()), case["parsedAddr"].as_str());
                    }
                    assert_eq!(proxy.username.as_deref(), case["parsedUsername"].as_str());
                    assert_eq!(proxy.password.as_deref(), case["parsedPassword"].as_str());
                }
                super::ParsedProxyRoute::MissingAddress { username, password } => {
                    assert!(case["parsedAddr"].is_null());
                    assert_eq!(username.as_deref(), case["parsedUsername"].as_str());
                    assert_eq!(password.as_deref(), case["parsedPassword"].as_str());
                }
                super::ParsedProxyRoute::Direct => panic!("Java decoded a nonnull route object"),
            }
        }
        assert!(
            outcome_mismatches.is_empty(),
            "SOCKS parser outcomes differ:\n{}",
            outcome_mismatches.join("\n")
        );
    }

    #[test]
    fn socks_proxy_field_token_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let cases = fixture["socksFieldShapeCases"].as_array().unwrap();
        assert_eq!(cases.len(), 21);
        for case in cases {
            let mut config = ClientConfig::default();
            config.set_socks_proxy_config(case["input"].as_str().unwrap());
            let parsed = config.parse_socks_proxy_config();
            let java_accepted = case["outcome"] == "OK";
            assert_eq!(parsed.is_ok(), java_accepted, "{}", case["id"]);
            if !java_accepted {
                continue;
            }
            let routes = parsed.unwrap();
            let route = routes
                .get(&Some("10.0.0.0/8".into()))
                .unwrap_or_else(|| panic!("missing route for {}", case["id"]));
            assert_eq!(case["routePresent"], true, "{}", case["id"]);
            match route {
                super::ParsedProxyRoute::Proxy(proxy) => {
                    assert_eq!(
                        Some(proxy.address.as_str()),
                        case["parsedAddr"].as_str(),
                        "{}",
                        case["id"]
                    );
                    assert_eq!(
                        proxy.username.as_deref(),
                        case["parsedUsername"].as_str(),
                        "{}",
                        case["id"]
                    );
                    assert_eq!(
                        proxy.password.as_deref(),
                        case["parsedPassword"].as_str(),
                        "{}",
                        case["id"]
                    );
                }
                super::ParsedProxyRoute::MissingAddress { username, password } => {
                    assert!(case["parsedAddr"].is_null(), "{}", case["id"]);
                    assert_eq!(
                        username.as_deref(),
                        case["parsedUsername"].as_str(),
                        "{}",
                        case["id"]
                    );
                    assert_eq!(
                        password.as_deref(),
                        case["parsedPassword"].as_str(),
                        "{}",
                        case["id"]
                    );
                }
                super::ParsedProxyRoute::Direct => {
                    panic!("Java decoded a null route for {}", case["id"])
                }
            }
        }
    }

    #[test]
    fn socks_escape_follower_matches_invoked_java_fastjson_for_bmp_scalars() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        assert_eq!(fixture["socksEscapeFollowerProbedCodeUnitCount"], 65_536);
        let mut expected = vec![None; u16::MAX as usize + 1];
        for group in fixture["socksEscapeFollowerObservations"]
            .as_array()
            .unwrap()
        {
            let observation = group["observation"].clone();
            for code_unit in group["codeUnits"].as_array().unwrap() {
                let code_unit = code_unit.as_u64().unwrap() as usize;
                expected[code_unit] = Some(observation.clone());
            }
        }
        assert_eq!(
            expected.iter().filter(|entry| entry.is_some()).count(),
            15_872
        );

        let mut config = ClientConfig::default();
        let prefix = "{\"10.0.0.0/8\":{\"addr\":\"A\\";
        let suffix = "Z\"}}";
        let mut compared = 0;
        for code_unit in 0..=u16::MAX {
            if (0xd800..=0xdfff).contains(&code_unit) {
                // A Rust String cannot carry the isolated Java UTF-16 unit.
                continue;
            }
            let follower = char::from_u32(u32::from(code_unit)).unwrap();
            config.set_socks_proxy_config(format!("{prefix}{follower}{suffix}"));
            let actual = config.parse_socks_proxy_config().ok().and_then(|routes| {
                routes
                    .get(&Some("10.0.0.0/8".into()))
                    .map(|route| match route {
                        super::ParsedProxyRoute::Proxy(proxy) => serde_json::json!({
                            "addr": proxy.address,
                            "username": proxy.username,
                            "password": proxy.password
                        }),
                        super::ParsedProxyRoute::MissingAddress { username, password } => {
                            serde_json::json!({"addr": null, "username": username, "password": password})
                        }
                        super::ParsedProxyRoute::Direct => serde_json::json!({
                            "addr": null,
                            "username": null,
                            "password": null
                        }),
                    })
            });
            assert_eq!(
                actual,
                expected[usize::from(code_unit)],
                "U+{code_unit:04X} ({follower:?})"
            );
            compared += 1;
        }
        assert_eq!(compared, 63_488);
    }

    #[test]
    fn socks_supplementary_escape_follower_matches_each_java_high_surrogate() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let cases = &fixture["socksSupplementaryEscapeFollowerCases"];
        assert_eq!(cases["probedHighSurrogateCount"], 1024);
        let accepted = cases["accepted"]
            .as_array()
            .unwrap()
            .iter()
            .map(|case| {
                (
                    case["codePoint"].as_u64().unwrap() as u32,
                    case["addr"].as_str().unwrap().to_owned(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(accepted.len(), 248);

        let mut config = ClientConfig::default();
        let prefix = "{\"10.0.0.0/8\":{\"addr\":\"A\\";
        let suffix = "Z\"}}";
        for high in 0xd800u32..=0xdbff {
            let code_point = 0x10000 + ((high - 0xd800) << 10);
            let follower = char::from_u32(code_point).unwrap();
            config.set_socks_proxy_config(format!("{prefix}{follower}{suffix}"));
            match accepted.get(&code_point) {
                Some(expected_addr) => {
                    let routes = config.parse_socks_proxy_config().unwrap_or_else(|error| {
                        panic!("U+{code_point:06X} should parse as Java: {error}")
                    });
                    let route = routes.get(&Some("10.0.0.0/8".into())).unwrap();
                    match route {
                        super::ParsedProxyRoute::Proxy(proxy) => {
                            assert_eq!(proxy.address, *expected_addr, "U+{code_point:06X}");
                        }
                        other => panic!("U+{code_point:06X} produced {other:?}"),
                    }
                }
                None => assert!(
                    config.parse_socks_proxy_config().is_err(),
                    "U+{code_point:06X} should be rejected like Java"
                ),
            }
        }
    }

    #[test]
    fn every_supplementary_escape_follower_matches_invoked_java_fastjson() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let cases = &fixture["socksSupplementaryEscapeFollowerCases"];
        assert_eq!(cases["probedCodePointCount"], 1_048_576);
        let ranges = cases["acceptedRanges"].as_array().unwrap();
        let accepted_count: u64 = ranges
            .iter()
            .map(|range| range["end"].as_u64().unwrap() - range["start"].as_u64().unwrap() + 1)
            .sum();
        assert_eq!(accepted_count, cases["acceptedCodePointCount"]);

        let mut range_index = 0;
        let mut config = ClientConfig::default();
        let prefix = "{\"10.0.0.0/8\":{\"addr\":\"A\\";
        let suffix = "Z\"}}";
        let mut compared = 0;
        for code_point in 0x10000u32..=0x10ffff {
            let code_point_u64 = u64::from(code_point);
            while range_index < ranges.len()
                && ranges[range_index]["end"].as_u64().unwrap() < code_point_u64
            {
                range_index += 1;
            }
            let expected = ranges.get(range_index).and_then(|range| {
                (range["start"].as_u64().unwrap() <= code_point_u64)
                    .then(|| range["addr"].as_str().unwrap().to_owned())
            });
            let follower = char::from_u32(code_point).unwrap();
            config.set_socks_proxy_config(format!("{prefix}{follower}{suffix}"));
            let actual = config.parse_socks_proxy_config().ok().and_then(|routes| {
                routes
                    .get(&Some("10.0.0.0/8".into()))
                    .and_then(|route| match route {
                        super::ParsedProxyRoute::Proxy(proxy) => Some(proxy.address.clone()),
                        super::ParsedProxyRoute::MissingAddress { .. }
                        | super::ParsedProxyRoute::Direct => None,
                    })
            });
            assert_eq!(actual, expected, "U+{code_point:06X} ({follower:?})");
            compared += 1;
        }
        assert_eq!(compared, 1_048_576);
    }

    #[test]
    fn socks_cidr_matching_matches_invoked_java_helper() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["socksCidr"].as_array().unwrap() {
            let result = super::ipv4_in_cidr(
                case["address"].as_str().unwrap(),
                case["cidr"].as_str().unwrap(),
            );
            assert_eq!(result.is_ok(), case["outcome"] == "OK", "{case}");
            if let Ok(matches) = result {
                assert_eq!(matches, case["match"]);
            }
        }
    }

    #[test]
    fn socks_proxy_selection_matches_invoked_java_remoting_client() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["socksProxySelection"].as_array().unwrap() {
            let mut config = ClientConfig::default();
            config.set_socks_proxy_config(case["config"].as_str().unwrap());
            let routes = match config.parse_socks_proxy_config() {
                Ok(routes) => routes,
                Err(error) => {
                    assert_ne!(case["outcome"], "OK", "{case}: {error}");
                    continue;
                }
            };
            let selected = ClientConfig::proxy_for(&routes, case["host"].as_str().unwrap());
            assert_eq!(selected.is_ok(), case["outcome"] == "OK", "{case}");
            if case["outcome"] != "OK" {
                continue;
            }
            let selected = selected.unwrap();
            assert_eq!(
                selected.as_ref().map(|selection| selection.cidr.as_str()),
                case["proxyCidr"].as_str(),
                "{case}"
            );
            assert_eq!(
                selected
                    .as_ref()
                    .and_then(|selection| match &selection.route {
                        super::ParsedProxyRoute::Proxy(proxy) => Some(proxy.address.as_str()),
                        super::ParsedProxyRoute::Direct
                        | super::ParsedProxyRoute::MissingAddress { .. } => None,
                    }),
                case["proxyAddress"].as_str(),
                "{case}"
            );
        }
    }

    #[test]
    fn socks_proxy_routes_match_java_ipv4_cidr_and_error_behavior() {
        let private_route = std::collections::HashMap::from([(
            Some("10.20.0.0/16".into()),
            SocksProxyConfig::new("private-proxy:1080"),
        )]);
        let selected = ClientConfig::proxy_for(&private_route, "10.20.4.5")
            .unwrap()
            .unwrap();
        assert_eq!(selected.cidr, "10.20.0.0/16");
        assert!(matches!(
            selected.route,
            ParsedProxyRoute::Proxy(SocksProxyConfig { address, .. })
                if address == "private-proxy:1080"
        ));
        let default_route = std::collections::HashMap::from([(
            Some("0.0.0.0/0".into()),
            SocksProxyConfig::new("default-proxy:1080"),
        )]);
        let selected = ClientConfig::proxy_for(&default_route, "192.0.2.5")
            .unwrap()
            .unwrap();
        assert!(matches!(
            selected.route,
            ParsedProxyRoute::Proxy(SocksProxyConfig { address, .. })
                if address == "default-proxy:1080"
        ));
        assert!(
            ClientConfig::proxy_for(&default_route, "broker.example")
                .unwrap()
                .is_some()
        );
        assert!(
            ClientConfig::proxy_for(&default_route, "2001:db8::1")
                .unwrap()
                .is_some()
        );

        let zero_prefix = std::collections::HashMap::from([(
            Some("10.20.0.0/0".into()),
            SocksProxyConfig::new("zero-prefix-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&zero_prefix, "10.20.4.5")
                .unwrap()
                .is_none()
        );
        assert!(ClientConfig::proxy_for(&private_route, "broker.example").is_err());
        let invalid_cidr = std::collections::HashMap::from([(
            Some("10.20.0.0/not-a-prefix".into()),
            SocksProxyConfig::new("proxy:1080"),
        )]);
        assert!(ClientConfig::proxy_for(&invalid_cidr, "10.20.4.5").is_err());

        // Java ipToInt parses signed integers and Java int shifts mask shift counts to 0..31.
        let java_shifted_prefix = std::collections::HashMap::from([(
            Some("10.20.0.0/33".into()),
            SocksProxyConfig::new("shifted-prefix-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&java_shifted_prefix, "10.20.4.5")
                .unwrap()
                .is_some()
        );
        let java_integer_octets = std::collections::HashMap::from([(
            Some("266.20.0.0/16".into()),
            SocksProxyConfig::new("integer-octet-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&java_integer_octets, "10.20.4.5")
                .unwrap()
                .is_some()
        );
    }
}
