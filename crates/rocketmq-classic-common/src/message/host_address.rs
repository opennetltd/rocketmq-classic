use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// The resolved or unresolved `InetSocketAddress` values stored by Java `MessageExt`.
///
/// The host name is retained separately from the resolved IP address, matching
/// Java's `getHostString()` behavior without requiring reverse DNS lookups.
///
/// Java socket-address text is available through Rust's `Display`/`ToString`;
/// the exact-format helper remains internal:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageHostAddress;
/// let _ = MessageHostAddress::new("127.0.0.1", 10911).java_to_string();
/// ```
#[derive(Debug, Clone)]
pub struct MessageHostAddress {
    host_name: String,
    address: Option<SocketAddr>,
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessageHostAddressError {
    #[error("socket address has no host/port separator")]
    MissingPortSeparator,
    #[error("socket address has an invalid Java integer port")]
    InvalidPort,
    #[error("socket address port is outside the valid range")]
    PortOutOfRange,
    #[error("socket address text is malformed")]
    MalformedAddress,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessageHostAddressBytesError {
    #[error("message host address is absent")]
    MissingAddress,
    #[error("unresolved socket address has no network bytes")]
    UnresolvedAddress,
    #[error("output buffer has {actual} bytes but {needed} are required")]
    BufferTooSmall { needed: usize, actual: usize },
}

impl MessageHostAddress {
    /// Resolve a host using the same `InetSocketAddress(String, int)` model as Java.
    /// If resolution fails, retain the host as an unresolved address.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host_name = host.into();
        let address = host_name
            .parse::<IpAddr>()
            .ok()
            .map(|address| SocketAddr::new(address, port))
            .or_else(|| (host_name.as_str(), port).to_socket_addrs().ok()?.next());
        Self {
            host_name,
            address,
            port,
        }
    }

    /// Construct the unresolved form returned by Java's
    /// `InetSocketAddress.createUnresolved`.
    pub fn unresolved(host: impl Into<String>, port: u16) -> Self {
        Self {
            host_name: host.into(),
            address: None,
            port,
        }
    }

    /// Construct a resolved address from the IP value stored in a Broker record.
    pub fn from_ip_address(address: IpAddr, port: u16) -> Self {
        Self {
            host_name: java_ip_string(address),
            address: Some(SocketAddr::new(address, port)),
            port,
        }
    }

    /// Construct a resolved address from Rust's standard socket-address type.
    pub fn from_socket_addr(address: SocketAddr) -> Self {
        Self {
            host_name: java_socket_ip_string(address),
            address: Some(address),
            port: address.port(),
        }
    }

    /// Parse Java `NetworkUtil.string2SocketAddress` input, including Java's
    /// signed decimal port syntax and socket port bounds.
    pub fn parse_java_socket_address(value: &str) -> Result<Self, MessageHostAddressError> {
        let (host, raw_port) = value
            .rsplit_once(':')
            .ok_or(MessageHostAddressError::MissingPortSeparator)?;
        let port = parse_java_i32(raw_port).ok_or(MessageHostAddressError::InvalidPort)?;
        let port = u16::try_from(port).map_err(|_| MessageHostAddressError::PortOutOfRange)?;
        Ok(Self::new(host, port))
    }

    /// Java `InetSocketAddress.getHostString()` without reverse DNS lookup.
    pub fn host_name(&self) -> &str {
        &self.host_name
    }

    /// Java `InetSocketAddress.getAddress()`, or `None` for unresolved hosts.
    pub fn ip_address(&self) -> Option<IpAddr> {
        self.address.map(|address| address.ip())
    }

    /// Java `InetSocketAddress.getPort()`.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Whether the address has an IP value, as in `InetSocketAddress.isUnresolved()`.
    pub fn is_resolved(&self) -> bool {
        self.address.is_some()
    }

    /// Return the Rust standard socket address when the host is resolved.
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        self.address
    }

    /// Java `InetAddress.getHostAddress()` for resolved addresses.
    pub fn host_address_string(&self) -> Option<String> {
        self.address.map(java_socket_ip_string)
    }

    /// Java `NetworkUtil.socketAddress2String`: resolved numeric host plus port.
    /// Unresolved addresses have no equivalent because Java dereferences `getAddress()`.
    pub fn socket_address_string(&self) -> Option<String> {
        Some(format!("{}:{}", self.host_address_string()?, self.port))
    }

    /// Java `MessageExt.getBornHostBytes()` / `getStoreHostBytes()` bytes:
    /// the address octets followed by the big-endian 32-bit port.
    pub fn network_bytes(&self) -> Option<Vec<u8>> {
        let address = self.address?;
        let mut bytes = Vec::with_capacity(if address.is_ipv4() { 8 } else { 20 });
        match address.ip() {
            IpAddr::V4(address) => bytes.extend_from_slice(&address.octets()),
            IpAddr::V6(address) => bytes.extend_from_slice(&address.octets()),
        }
        bytes.extend_from_slice(&i32::from(self.port).to_be_bytes());
        Some(bytes)
    }

    /// Write Java's host-byte layout into a caller-provided Rust byte slice.
    /// Java writes the address before the port, so a short slice may contain
    /// the address prefix when the typed insufficient-buffer error is returned.
    pub fn write_network_bytes(
        &self,
        output: &mut [u8],
    ) -> Result<usize, MessageHostAddressBytesError> {
        let address = self
            .address
            .ok_or(MessageHostAddressBytesError::UnresolvedAddress)?;
        let mut address_bytes = [0_u8; 16];
        let address_len = match address.ip() {
            IpAddr::V4(address) => {
                address_bytes[..4].copy_from_slice(&address.octets());
                4
            }
            IpAddr::V6(address) => {
                address_bytes.copy_from_slice(&address.octets());
                16
            }
        };
        let needed = address_len + 4;
        if output.len() < address_len {
            return Err(MessageHostAddressBytesError::BufferTooSmall {
                needed,
                actual: output.len(),
            });
        }
        output[..address_len].copy_from_slice(&address_bytes[..address_len]);
        if output.len() < needed {
            return Err(MessageHostAddressBytesError::BufferTooSmall {
                needed,
                actual: output.len(),
            });
        }
        output[address_len..needed].copy_from_slice(&i32::from(self.port).to_be_bytes());
        Ok(needed)
    }

    /// Java `InetSocketAddress.toString()` form used by the message fixture model.
    fn java_to_string(&self) -> String {
        match self.address {
            Some(socket_address) => {
                let address = java_socket_host_string(socket_address);
                if self.host_name == java_socket_ip_string(socket_address) {
                    format!("/{address}:{}", self.port)
                } else {
                    format!("{}/{address}:{}", self.host_name, self.port)
                }
            }
            None => format!("{}/<unresolved>:{}", self.host_name, self.port),
        }
    }

    fn from_java_to_string(value: &str) -> Result<Self, MessageHostAddressError> {
        let (host_part, raw_port) = value
            .rsplit_once(':')
            .ok_or(MessageHostAddressError::MissingPortSeparator)?;
        let port = raw_port
            .parse::<u16>()
            .map_err(|_| MessageHostAddressError::MalformedAddress)?;

        if let Some((host_name, address_part)) = host_part.rsplit_once('/') {
            if address_part == "<unresolved>" {
                return Ok(Self::unresolved(host_name, port));
            }
            let address = parse_socket_address_literal(address_part, port)
                .ok_or(MessageHostAddressError::MalformedAddress)?;
            let host_name = if host_name.is_empty() {
                java_socket_ip_string(address)
            } else {
                host_name.to_owned()
            };
            return Ok(Self {
                host_name,
                address: Some(address),
                port,
            });
        }

        Ok(Self::new(host_part, port))
    }
}

impl PartialEq for MessageHostAddress {
    fn eq(&self, other: &Self) -> bool {
        if self.port != other.port {
            return false;
        }
        match (self.address, other.address) {
            (Some(left), Some(right)) => socket_address_eq(left, right),
            (None, None) => self.host_name.eq_ignore_ascii_case(&other.host_name),
            _ => false,
        }
    }
}

impl Eq for MessageHostAddress {}

impl Hash for MessageHostAddress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.address {
            Some(address) => {
                1_u8.hash(state);
                hash_socket_address(address, state);
            }
            None => {
                0_u8.hash(state);
                self.host_name.to_ascii_lowercase().hash(state);
            }
        }
        self.port.hash(state);
    }
}

fn socket_address_eq(left: SocketAddr, right: SocketAddr) -> bool {
    match (left, right) {
        (SocketAddr::V4(left), SocketAddr::V4(right)) => {
            left.ip() == right.ip() && left.port() == right.port()
        }
        (SocketAddr::V6(left), SocketAddr::V6(right)) => {
            left.ip() == right.ip()
                && left.port() == right.port()
                && left.scope_id() == right.scope_id()
        }
        _ => false,
    }
}

fn hash_socket_address<H: Hasher>(address: SocketAddr, state: &mut H) {
    match address {
        SocketAddr::V4(address) => {
            4_u8.hash(state);
            address.ip().hash(state);
            address.port().hash(state);
        }
        SocketAddr::V6(address) => {
            6_u8.hash(state);
            address.ip().hash(state);
            address.port().hash(state);
            address.scope_id().hash(state);
        }
    }
}

impl fmt::Display for MessageHostAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.java_to_string())
    }
}

impl Serialize for MessageHostAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.java_to_string())
    }
}

impl<'de> Deserialize<'de> for MessageHostAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_java_to_string(&value).map_err(serde::de::Error::custom)
    }
}

pub(super) fn parse_java_i32(value: &str) -> Option<i32> {
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

pub(super) fn parse_java_i64(value: &str) -> Option<i64> {
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

fn java_ip_string(address: IpAddr) -> String {
    match address {
        IpAddr::V4(address) => address.to_string(),
        IpAddr::V6(address) => address
            .segments()
            .iter()
            .map(|segment| format!("{segment:x}"))
            .collect::<Vec<_>>()
            .join(":"),
    }
}

fn java_socket_ip_string(address: SocketAddr) -> String {
    match address {
        SocketAddr::V4(address) => address.ip().to_string(),
        SocketAddr::V6(address) => {
            let scope = if address.scope_id() == 0 {
                String::new()
            } else {
                format!("%{}", address.scope_id())
            };
            format!("{}{}", java_ip_string(IpAddr::V6(*address.ip())), scope)
        }
    }
}

fn java_socket_host_string(address: SocketAddr) -> String {
    match address {
        SocketAddr::V4(address) => address.ip().to_string(),
        SocketAddr::V6(address) => format!("[{}]", java_socket_ip_string(SocketAddr::V6(address))),
    }
}

fn parse_socket_address_literal(value: &str, port: u16) -> Option<SocketAddr> {
    let value = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(value);
    if let Ok(address) = value.parse::<IpAddr>() {
        return Some(SocketAddr::new(address, port));
    }
    format!("[{value}]:{port}").parse().ok()
}
