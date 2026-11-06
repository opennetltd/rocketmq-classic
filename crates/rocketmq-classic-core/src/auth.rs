//! Client-side ACL request signing compatible with Java 5.5.1.
//!
//! Java's public helpers are exposed through [`AclUtils`] and [`AclSigner`].
//! Implementation-only helpers and a non-Java one-argument adapter are not public:
//!
//! ```compile_fail
//! use rocketmq_classic_core::auth::cal_signature;
//! ```
//!
//! The Java `AclUtils.combineRequestContent` overload requires an explicit
//! field map:
//!
//! ```compile_fail
//! use rocketmq_classic_core::auth::AclUtils;
//! use rocketmq_classic_remoting::RemotingCommand;
//! AclUtils::combine_request_content(&RemotingCommand::default());
//! ```
//!
//! Java's `AclClientRPCHook` keeps its credentials private, and `AclException`
//! exposes causes through the standard error chain rather than string-only
//! accessors or constructors:
//!
//! ```compile_fail
//! use rocketmq_classic_core::auth::{AclClientRpcHook, SessionCredentials};
//! let hook = AclClientRpcHook::new(SessionCredentials::new("access", "secret"));
//! let _credentials = hook.session_credentials();
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_core::auth::AclException;
//! let _ = AclException::with_cause_message("request failed", "cause");
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_core::auth::AclException;
//! let _ = AclException::with_status_code_and_cause("denied", 1, "request failed", "cause");
//! ```
//!
//! Java credentials expose these values on `SessionCredentials`, not as
//! module-level constants:
//!
//! ```compile_fail
//! use rocketmq_classic_core::auth::{
//!     ACCESS_KEY, CHARSET, KEY_FILE_PROPERTY, SECRET_KEY, SECURITY_TOKEN, SIGNATURE,
//! };
//! # fn main() {}
//! ```

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use icu_properties::{
    CodePointMapData, CodePointSetData,
    props::{CanonicalCombiningClass, SoftDotted},
};
use md5::Md5;
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RpcHook, RpcHookError},
};
use sha1::Sha1;
use sha2::Sha256;

const ACCESS_KEY: &str = "AccessKey";
const SECRET_KEY: &str = "SecretKey";
const CHARSET: &str = "UTF-8";
const KEY_FILE_PROPERTY: &str = "rocketmq.client.keyFile";
const SECURITY_TOKEN: &str = "SecurityToken";
const SIGNATURE: &str = "Signature";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AclUtilsError {
    #[error("NetAddress examine scope Exception netAddress is {0}")]
    InvalidAddressScope(String),
    #[error("invalid numeric ACL scope: {0}")]
    InvalidNumericScope(String),
    #[error("ACL scope index {index} exceeds {available} address parts")]
    ScopeIndexOutOfBounds { index: usize, available: usize },
    #[error("ACL utility I/O failed: {0}")]
    Io(String),
    #[error("ACL YAML parse failed: {0}")]
    Yaml(String),
}

/// Structured counterpart to Java's public `AclException` runtime exception.
#[derive(Clone, Default)]
pub struct AclException {
    status: Option<String>,
    code: i32,
    message: Option<String>,
    cause_message: Option<String>,
    cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

impl AclException {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
            ..Self::default()
        }
    }

    pub fn with_status_code(status: impl Into<String>, code: i32) -> Self {
        Self {
            status: Some(status.into()),
            code,
            ..Self::default()
        }
    }

    pub fn with_status_code_and_message(
        status: impl Into<String>,
        code: i32,
        message: impl Into<String>,
    ) -> Self {
        Self {
            message: Some(message.into()),
            ..Self::with_status_code(status, code)
        }
    }

    /// Builds the Rust equivalent of Java's `(message, Throwable)` constructor.
    pub fn with_cause(
        message: impl Into<String>,
        cause: Arc<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        let cause_message = cause.to_string();
        Self {
            message: Some(message.into()),
            cause_message: Some(cause_message),
            cause: Some(cause),
            ..Self::default()
        }
    }

    /// Builds the Rust equivalent of Java's `(status, code, message, Throwable)` constructor.
    pub fn with_status_code_and_source(
        status: impl Into<String>,
        code: i32,
        message: impl Into<String>,
        cause: Arc<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        let cause_message = cause.to_string();
        Self {
            message: Some(message.into()),
            cause_message: Some(cause_message),
            cause: Some(cause),
            ..Self::with_status_code(status, code)
        }
    }

    pub fn status(&self) -> Option<&str> {
        self.status.as_deref()
    }

    pub fn set_status(&mut self, status: Option<String>) {
        self.status = status;
    }

    pub const fn code(&self) -> i32 {
        self.code
    }

    pub fn set_code(&mut self, code: i32) {
        self.code = code;
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn cause_message(&self) -> Option<&str> {
        self.cause_message.as_deref()
    }

    pub fn cause(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.cause.as_deref()
    }
}

impl fmt::Debug for AclException {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AclException")
            .field("status", &self.status)
            .field("code", &self.code)
            .field("message", &self.message)
            .field("cause_message", &self.cause_message)
            .finish()
    }
}

impl PartialEq for AclException {
    fn eq(&self, other: &Self) -> bool {
        self.status == other.status
            && self.code == other.code
            && self.message == other.message
            && self.cause_message == other.cause_message
    }
}

impl Eq for AclException {}

impl fmt::Display for AclException {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(message) = &self.message {
            formatter.write_str(message)
        } else {
            Ok(())
        }
    }
}

impl std::error::Error for AclException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_deref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

/// Java `AclSigner.SigningAlgorithm` values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SigningAlgorithm {
    #[default]
    HmacSha1,
    HmacSha256,
    HmacMd5,
}

impl SigningAlgorithm {
    pub const DEFAULT_ALGORITHM: Self = Self::HmacSha1;

    pub const fn java_name(self) -> &'static str {
        match self {
            Self::HmacSha1 => "HmacSHA1",
            Self::HmacSha256 => "HmacSHA256",
            Self::HmacMd5 => "HmacMD5",
        }
    }

    pub const fn values() -> [Self; 3] {
        [Self::HmacSha1, Self::HmacSha256, Self::HmacMd5]
    }

    pub fn from_java_name(name: Option<&str>) -> Result<Self, &'static str> {
        match name {
            None => Err("SigningAlgorithm name is null"),
            Some("HmacSHA1") => Ok(Self::HmacSha1),
            Some("HmacSHA256") => Ok(Self::HmacSha256),
            Some("HmacMD5") => Ok(Self::HmacMd5),
            Some(_) => Err("unknown SigningAlgorithm name"),
        }
    }
}

impl fmt::Display for SigningAlgorithm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.java_name())
    }
}

/// Credentials used by Java's `SessionCredentials` and `AclClientRPCHook`.
///
/// Debug output redacts every credential value.
/// The Rust-only token builder stays internal:
///
/// ```compile_fail
/// use rocketmq_classic_core::auth::SessionCredentials;
/// let credentials = SessionCredentials::new("access", "secret").with_security_token("token");
/// ```
///
/// The Java YAML factories are on `AclUtils`, not this type:
///
/// ```compile_fail
/// use rocketmq_classic_core::auth::SessionCredentials;
/// let _ = SessionCredentials::acl_rpc_hook_from_yaml("AccessKey: access");
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_core::auth::SessionCredentials;
/// let _ = SessionCredentials::acl_rpc_hook_from_yaml_file("acl.yml");
/// ```
#[derive(Clone)]
pub struct SessionCredentials {
    access_key: Option<String>,
    secret_key: Option<String>,
    signature: Option<String>,
    security_token: Option<String>,
}

impl Default for SessionCredentials {
    fn default() -> Self {
        Self::from_key_file(default_key_file_path())
    }
}

impl SessionCredentials {
    pub const CHARSET: &'static str = CHARSET;
    pub const ACCESS_KEY: &'static str = "AccessKey";
    pub const SECRET_KEY: &'static str = "SecretKey";
    pub const SIGNATURE: &'static str = "Signature";
    pub const SECURITY_TOKEN: &'static str = "SecurityToken";

    fn from_key_file(path: impl AsRef<Path>) -> Self {
        let mut credentials = Self {
            access_key: None,
            secret_key: None,
            signature: None,
            security_token: None,
        };
        if let Ok(content) = std::fs::read(path) {
            if let Some(properties) = parse_java_properties(&content) {
                credentials.update_content(&properties);
            }
        }
        credentials
    }
}

impl PartialEq for SessionCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.access_key == other.access_key
            && self.secret_key == other.secret_key
            && self.signature == other.signature
    }
}

impl Eq for SessionCredentials {}

impl Hash for SessionCredentials {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.access_key.hash(state);
        self.secret_key.hash(state);
        self.signature.hash(state);
    }
}

impl SessionCredentials {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: Some(access_key.into()),
            secret_key: Some(secret_key.into()),
            signature: None,
            security_token: None,
        }
    }

    pub fn new_with_security_token(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        security_token: Option<String>,
    ) -> Self {
        Self {
            access_key: Some(access_key.into()),
            secret_key: Some(secret_key.into()),
            signature: None,
            security_token,
        }
    }

    pub fn access_key(&self) -> Option<&str> {
        self.access_key.as_deref()
    }

    pub fn set_access_key(&mut self, value: Option<String>) {
        self.access_key = value;
    }

    pub fn secret_key(&self) -> Option<&str> {
        self.secret_key.as_deref()
    }

    pub fn set_secret_key(&mut self, value: Option<String>) {
        self.secret_key = value;
    }

    pub fn signature(&self) -> Option<&str> {
        self.signature.as_deref()
    }

    pub fn set_signature(&mut self, value: Option<String>) {
        self.signature = value;
    }

    pub fn security_token(&self) -> Option<&str> {
        self.security_token.as_deref()
    }

    pub fn set_security_token(&mut self, value: Option<String>) {
        self.security_token = value;
    }

    /// Resolves Java's `KEY_FILE` property using this process's configured home directory.
    pub fn key_file_path() -> PathBuf {
        default_key_file_path()
    }

    /// Applies the same trimmed property updates as Java `updateContent`.
    pub fn update_content(&mut self, properties: &HashMap<String, String>) {
        if let Some(value) = properties.get(ACCESS_KEY) {
            self.access_key = Some(java_trim(value).to_owned());
        }
        if let Some(value) = properties.get(SECRET_KEY) {
            self.secret_key = Some(java_trim(value).to_owned());
        }
        if let Some(value) = properties.get(SECURITY_TOKEN) {
            self.security_token = Some(java_trim(value).to_owned());
        }
    }

    /// Java's `AclUtils.getAclRPCHook(InputStream)` path for YAML credentials.
    ///
    /// Invalid YAML, absent credentials, and blank credentials map to Java's
    /// `null` result. Only accessKey and secretKey are consumed by the Java
    /// factory; an optional SecurityToken remains available through the direct
    /// `SessionCredentials` constructors.
    fn acl_rpc_hook_from_yaml(yaml: &str) -> Option<AclClientRpcHook> {
        let document: yaml_serde::Value = yaml_serde::from_str(yaml).ok()?;
        let fields = document.as_mapping()?;
        let access_key =
            yaml_scalar_string(fields.get(yaml_serde::Value::String("accessKey".to_owned()))?)?;
        let secret_key =
            yaml_scalar_string(fields.get(yaml_serde::Value::String("secretKey".to_owned()))?)?;
        if access_key.chars().all(char::is_whitespace)
            || secret_key.chars().all(char::is_whitespace)
        {
            return None;
        }
        Some(AclClientRpcHook::new(Self::new(access_key, secret_key)))
    }
}

impl fmt::Debug for SessionCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionCredentials")
            .field(
                "access_key",
                &self.access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("signature", &self.signature.as_ref().map(|_| "[REDACTED]"))
            .field(
                "security_token",
                &self.security_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Signs a byte string using Java ACL's HMAC algorithms and padded Base64.
fn cal_signature(data: &[u8], secret_key: &str, algorithm: SigningAlgorithm) -> String {
    cal_signature_with_key_bytes(data, secret_key.as_bytes(), algorithm)
}

/// Signs the exact bytes supplied by Java's Charset overloads.
fn cal_signature_with_key_bytes(
    data: &[u8],
    secret_key: &[u8],
    algorithm: SigningAlgorithm,
) -> String {
    let bytes = match algorithm {
        SigningAlgorithm::HmacSha1 => {
            let mut mac = Hmac::<Sha1>::new_from_slice(secret_key)
                .expect("HMAC accepts keys of every length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        SigningAlgorithm::HmacSha256 => {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret_key)
                .expect("HMAC accepts keys of every length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        SigningAlgorithm::HmacMd5 => {
            let mut mac =
                Hmac::<Md5>::new_from_slice(secret_key).expect("HMAC accepts keys of every length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
    };
    STANDARD.encode(bytes)
}

/// Java `AclSigner.calSignature(byte[], String)` with its default HmacSHA1 algorithm.
fn cal_signature_default(data: &[u8], secret_key: &str) -> String {
    cal_signature(data, secret_key, SigningAlgorithm::DEFAULT_ALGORITHM)
}

/// Java `AclSigner.calSignature(String, String)` using UTF-8 for both values.
fn cal_signature_text_default(data: &str, secret_key: &str) -> String {
    cal_signature_default(data.as_bytes(), secret_key)
}

/// Java's overload family grouped under one Rust namespace.
pub struct AclSigner;

impl AclSigner {
    pub const DEFAULT_CHARSET: &'static str = CHARSET;
    pub const DEFAULT_ALGORITHM: SigningAlgorithm = SigningAlgorithm::DEFAULT_ALGORITHM;

    pub fn cal_signature(data: &[u8], secret_key: &str) -> String {
        cal_signature_default(data, secret_key)
    }

    pub fn cal_signature_text(data: &str, secret_key: &str) -> String {
        cal_signature_text_default(data, secret_key)
    }

    pub fn cal_signature_with_algorithm(
        data: &[u8],
        secret_key: &str,
        algorithm: SigningAlgorithm,
    ) -> String {
        cal_signature(data, secret_key, algorithm)
    }

    /// Signs pre-encoded input and key bytes, matching Java's explicit-Charset
    /// overloads without choosing a Rust encoding library for callers.
    pub fn cal_signature_with_material(
        data: &[u8],
        secret_key: &[u8],
        algorithm: SigningAlgorithm,
    ) -> String {
        cal_signature_with_key_bytes(data, secret_key, algorithm)
    }
}

/// Java `AclUtils.combineRequestContent` canonicalizes ext-field values and
/// appends the request body. Keys use Java's UTF-16 `String` ordering.
fn combine_request_content(request: &RemotingCommand) -> Vec<u8> {
    let mut fields = request
        .ext_fields
        .as_ref()
        .map(|fields| {
            fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    fields.sort_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));

    combine_request_content_with_fields(request, fields)
}

/// Java `AclUtils.combineRequestContent(RemotingCommand, SortedMap)` overload.
/// The caller supplies fields in the desired iteration order; `Signature` is
/// excluded and the request body is appended after all remaining values.
fn combine_request_content_with_fields<'a, I>(request: &RemotingCommand, fields: I) -> Vec<u8>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut output = Vec::new();
    for (key, value) in fields {
        if key != SIGNATURE {
            output.extend_from_slice(value.as_bytes());
        }
    }
    if let Some(body) = request.body.as_deref() {
        output.extend_from_slice(body);
    }
    output
}

/// Java `AclUtils.combineBytes`, preserving the input buffer when only one side
/// contains bytes and allocating only when both sides are non-empty.
fn combine_bytes<'a>(first: Option<&'a [u8]>, second: Option<&'a [u8]>) -> Option<Cow<'a, [u8]>> {
    match (first, second) {
        (None, second) => second.map(Cow::Borrowed),
        (Some([]), second) => second.map(Cow::Borrowed),
        (first, None) => first.map(Cow::Borrowed),
        (first, Some([])) => first.map(Cow::Borrowed),
        (Some(first), Some(second)) => {
            let mut joined = Vec::with_capacity(first.len() + second.len());
            joined.extend_from_slice(first);
            joined.extend_from_slice(second);
            Some(Cow::Owned(joined))
        }
    }
}

fn is_colon(value: &str) -> bool {
    value.contains(':')
}

fn is_asterisk(value: &str) -> bool {
    value.contains('*')
}

fn is_comma(value: &str) -> bool {
    value.contains(',')
}

fn is_minus(value: &str) -> bool {
    value.contains('-')
}

fn is_scope_value(value: i32) -> bool {
    (0..=255).contains(&value)
}

fn is_scope_token(value: &str) -> Result<bool, AclUtilsError> {
    let trimmed = value.trim_matches(|character: char| character <= '\u{20}');
    let parsed = trimmed
        .parse::<i32>()
        .map_err(|_| AclUtilsError::InvalidNumericScope(value.to_owned()))?;
    Ok(is_scope_value(parsed))
}

fn is_scope_segments(values: &[&str], index: usize) -> Result<bool, AclUtilsError> {
    if index > values.len() {
        return Err(AclUtilsError::ScopeIndexOutOfBounds {
            index,
            available: values.len(),
        });
    }
    for value in values.iter().take(index) {
        if !is_scope_token(value)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn is_ipv6_scope_value(value: i32) -> bool {
    (0..=0xffff).contains(&value)
}

fn is_ipv6_scope_segment(value: &str) -> bool {
    i32::from_str_radix(value, 16)
        .map(is_ipv6_scope_value)
        .unwrap_or(false)
}

fn is_ipv6_scope_segments(values: &[&str], index: usize) -> Result<bool, AclUtilsError> {
    if index > values.len() {
        return Err(AclUtilsError::ScopeIndexOutOfBounds {
            index,
            available: values.len(),
        });
    }
    Ok(values
        .iter()
        .take(index)
        .all(|value| is_ipv6_scope_segment(value)))
}

fn is_scope_address(address: &str, index: usize) -> Result<bool, AclUtilsError> {
    if is_colon(address) {
        let expanded = expand_ip(address, 8);
        let parts = expanded
            .split(':')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        return is_ipv6_scope_segments(&parts, index);
    }
    let parts = address
        .split('.')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 4 {
        return Ok(false);
    }
    is_scope_segments(&parts, index)
}

fn verify_scope(address: &str, index: usize) -> Result<(), AclUtilsError> {
    if is_scope_address(address, index)? {
        Ok(())
    } else {
        Err(AclUtilsError::InvalidAddressScope(address.to_owned()))
    }
}

fn ipv6_address_check(address: &str) -> Result<(), AclUtilsError> {
    if !is_asterisk(address) && !is_minus(address) {
        return Ok(());
    }
    let asterisk = address.find('*');
    let minus = address.find('-');
    if asterisk.is_some_and(|index| index + 1 != address.len()) {
        return Err(AclUtilsError::InvalidAddressScope(address.to_owned()));
    }
    if let Some(minus) = minus {
        let colon_bound = match asterisk {
            None => address.rfind(':').map_or(-1, |index| index as isize),
            Some(last) => address[..last]
                .rfind(':')
                .and_then(|index| address[..index].rfind(':'))
                .map_or(-1, |index| index as isize),
        };
        if minus as isize <= colon_bound {
            return Err(AclUtilsError::InvalidAddressScope(address.to_owned()));
        }
    }
    Ok(())
}

fn java_default_locale_language() -> Option<String> {
    ["LC_ALL", "LC_CTYPE", "LANG"]
        .into_iter()
        .filter_map(std::env::var_os)
        .map(|locale| locale.to_string_lossy().into_owned())
        .find(|locale| !locale.is_empty())
        .map(|locale| {
            locale
                .split(['_', '-', '.', '@'])
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
        })
}

fn java_default_locale_uppercase(value: &str) -> String {
    match java_default_locale_language().as_deref() {
        Some("tr" | "az") => value
            .chars()
            .flat_map(|character| match character {
                'i' => 'İ'.to_uppercase(),
                'ı' => 'I'.to_uppercase(),
                _ => character.to_uppercase(),
            })
            .collect(),
        Some("lt") => {
            let mut result = String::new();
            let soft_dotted = CodePointSetData::new::<SoftDotted>();
            let combining_class = CodePointMapData::<CanonicalCombiningClass>::new();
            let mut after_soft_dotted = false;
            for character in value.chars() {
                let class = combining_class.get(character);
                // Java's Lithuanian `After_Soft_Dotted` rule removes the
                // combining dot, but that dot still blocks a following
                // dot because its combining class is 230.
                if character != '\u{0307}' || !after_soft_dotted {
                    result.extend(character.to_uppercase());
                }

                if class == CanonicalCombiningClass::NotReordered {
                    after_soft_dotted = soft_dotted.contains(character);
                } else if class == CanonicalCombiningClass::Above {
                    after_soft_dotted = false;
                }
            }
            result
        }
        _ => value.to_uppercase(),
    }
}

fn expand_ip(address: &str, part: i32) -> String {
    let mut expanded = java_default_locale_uppercase(address);
    let separator_count = expanded.matches(':').count() as i32;
    let pad_count = part - separator_count;
    if pad_count > 0 {
        expanded = expanded.replace("::", &":".repeat((pad_count + 1) as usize));
    }
    expanded
        .split(':')
        .map(|segment| {
            let java_length = segment.encode_utf16().count();
            if java_length < 4 {
                format!("{}{segment}", "0".repeat(4 - java_length))
            } else {
                segment.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(":")
}

fn v6_ip_process(address: &str) -> Result<String, AclUtilsError> {
    let has_asterisk = is_asterisk(address);
    let has_minus = is_minus(address);
    let (part, sub_address) = if has_asterisk && has_minus {
        let last_colon = address
            .rfind(':')
            .ok_or_else(|| AclUtilsError::InvalidAddressScope(address.to_owned()))?;
        let second_last_colon = address[..last_colon]
            .rfind(':')
            .ok_or_else(|| AclUtilsError::InvalidAddressScope(address.to_owned()))?;
        (6, &address[..second_last_colon])
    } else if !has_asterisk && !has_minus {
        (8, address)
    } else {
        let last_colon = address
            .rfind(':')
            .ok_or_else(|| AclUtilsError::InvalidAddressScope(address.to_owned()))?;
        (7, &address[..last_colon])
    };
    Ok(expand_ip(sub_address, part))
}

fn get_addresses(address: &str, partial_address: &str) -> Result<Vec<String>, AclUtilsError> {
    if partial_address.len() < 2 {
        return Err(AclUtilsError::InvalidAddressScope(address.to_owned()));
    }
    let prefix_end = address
        .find('{')
        .ok_or_else(|| AclUtilsError::InvalidAddressScope(address.to_owned()))?;
    let prefix = &address[..prefix_end];
    Ok(partial_address[1..partial_address.len() - 1]
        .split(',')
        .filter(|part| !part.is_empty())
        .map(|part| format!("{prefix}{part}"))
        .collect())
}

/// Rust facade for Java's static `AclUtils` helper family.
pub struct AclUtils;

impl AclUtils {
    pub fn combine_request_content_with_fields<'a, I>(
        request: &RemotingCommand,
        fields: I,
    ) -> Vec<u8>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        combine_request_content_with_fields(request, fields)
    }

    pub fn combine_bytes<'a>(
        first: Option<&'a [u8]>,
        second: Option<&'a [u8]>,
    ) -> Option<Cow<'a, [u8]>> {
        combine_bytes(first, second)
    }

    pub fn cal_signature(data: &[u8], secret_key: &str) -> String {
        cal_signature_default(data, secret_key)
    }

    pub fn ipv6_address_check(address: &str) -> Result<(), AclUtilsError> {
        ipv6_address_check(address)
    }

    pub fn v6_ip_process(address: &str) -> Result<String, AclUtilsError> {
        v6_ip_process(address)
    }

    pub fn verify(address: &str, index: usize) -> Result<(), AclUtilsError> {
        verify_scope(address, index)
    }

    pub fn is_scope_address(address: &str, index: usize) -> Result<bool, AclUtilsError> {
        is_scope_address(address, index)
    }

    pub fn is_scope_segments(values: &[&str], index: usize) -> Result<bool, AclUtilsError> {
        is_scope_segments(values, index)
    }

    pub fn is_scope_token(value: &str) -> Result<bool, AclUtilsError> {
        is_scope_token(value)
    }

    pub fn is_scope_value(value: i32) -> bool {
        is_scope_value(value)
    }

    pub fn is_colon(value: &str) -> bool {
        is_colon(value)
    }

    pub fn is_asterisk(value: &str) -> bool {
        is_asterisk(value)
    }

    pub fn is_comma(value: &str) -> bool {
        is_comma(value)
    }

    pub fn is_minus(value: &str) -> bool {
        is_minus(value)
    }

    pub fn is_ipv6_scope_segments(values: &[&str], index: usize) -> Result<bool, AclUtilsError> {
        is_ipv6_scope_segments(values, index)
    }

    pub fn is_ipv6_scope_value(value: i32) -> bool {
        is_ipv6_scope_value(value)
    }

    pub fn expand_ip(address: &str, part: i32) -> String {
        expand_ip(address, part)
    }

    pub fn get_addresses(
        address: &str,
        partial_address: &str,
    ) -> Result<Vec<String>, AclUtilsError> {
        get_addresses(address, partial_address)
    }

    pub fn get_yaml_data_object<R, T>(reader: R) -> Result<Option<T>, AclUtilsError>
    where
        R: Read,
        T: serde::de::DeserializeOwned,
    {
        get_yaml_data_object(reader)
    }

    pub fn get_yaml_data_object_from_path<T>(
        path: impl AsRef<Path>,
    ) -> Result<Option<T>, AclUtilsError>
    where
        T: serde::de::DeserializeOwned,
    {
        get_yaml_data_object_from_path(path)
    }

    pub fn get_acl_rpc_hook<R: Read>(reader: R) -> Option<AclClientRpcHook> {
        get_acl_rpc_hook_from_reader(reader)
    }

    pub fn get_acl_rpc_hook_from_file(path: impl AsRef<Path>) -> Option<AclClientRpcHook> {
        get_acl_rpc_hook_from_file(path)
    }
}

fn get_yaml_data_object<R, T>(mut reader: R) -> Result<Option<T>, AclUtilsError>
where
    R: Read,
    T: serde::de::DeserializeOwned,
{
    let mut content = String::new();
    reader
        .read_to_string(&mut content)
        .map_err(|error| AclUtilsError::Io(error.to_string()))?;
    yaml_serde::from_str::<Option<T>>(&content)
        .map_err(|error| AclUtilsError::Yaml(error.to_string()))
}

fn get_yaml_data_object_from_path<T>(path: impl AsRef<Path>) -> Result<Option<T>, AclUtilsError>
where
    T: serde::de::DeserializeOwned,
{
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(AclUtilsError::Io(error.to_string())),
    };
    get_yaml_data_object(file)
}

fn get_acl_rpc_hook_from_reader<R: Read>(reader: R) -> Option<AclClientRpcHook> {
    let mut content = String::new();
    let mut reader = reader;
    reader.read_to_string(&mut content).ok()?;
    SessionCredentials::acl_rpc_hook_from_yaml(&content)
}

fn get_acl_rpc_hook_from_file(path: impl AsRef<Path>) -> Option<AclClientRpcHook> {
    let file = std::fs::File::open(path).ok()?;
    get_acl_rpc_hook_from_reader(file)
}

/// Java `AclClientRPCHook` adapted to the mutable Rust RPC hook boundary.
#[derive(Clone, Debug)]
pub struct AclClientRpcHook {
    credentials: SessionCredentials,
}

impl AclClientRpcHook {
    pub fn new(credentials: SessionCredentials) -> Self {
        Self { credentials }
    }

    #[cfg(test)]
    pub(crate) fn session_credentials(&self) -> &SessionCredentials {
        &self.credentials
    }
}

impl RpcHook for AclClientRpcHook {
    fn before_request_mut_result(
        &self,
        _remote_addr: &str,
        request: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        let Some(access_key) = self.credentials.access_key.as_deref() else {
            return Err(RpcHookError::Failed("missing ACL AccessKey".into()));
        };
        let Some(secret_key) = self.credentials.secret_key.as_deref() else {
            return Err(RpcHookError::Failed("missing ACL SecretKey".into()));
        };
        let fields = request.ext_fields.get_or_insert_with(Default::default);
        fields.insert(ACCESS_KEY.to_owned(), access_key.to_owned());
        if let Some(token) = &self.credentials.security_token {
            fields.insert(SECURITY_TOKEN.to_owned(), token.clone());
        }
        request.make_custom_header_to_net();
        let canonical = combine_request_content(request);
        let signature = cal_signature(&canonical, secret_key, SigningAlgorithm::HmacSha1);
        request
            .ext_fields
            .get_or_insert_with(Default::default)
            .insert(SIGNATURE.to_owned(), signature);
        Ok(())
    }
}

fn yaml_scalar_string(value: &yaml_serde::Value) -> Option<String> {
    match value {
        yaml_serde::Value::String(value) => Some(value.clone()),
        yaml_serde::Value::Number(value) => Some(value.to_string()),
        yaml_serde::Value::Bool(value) => Some(value.to_string()),
        yaml_serde::Value::Null
        | yaml_serde::Value::Sequence(_)
        | yaml_serde::Value::Mapping(_) => None,
        _ => None,
    }
}

fn java_trim(value: &str) -> &str {
    value.trim_matches(|character: char| character <= '\u{20}')
}

fn default_key_file_path() -> PathBuf {
    std::env::var_os(KEY_FILE_PROPERTY)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."))
                .join("key")
        })
}

/// Reads the ISO-8859-1 property stream semantics used by Java
/// `Properties.load(InputStream)` after RocketMQ has decoded and re-encoded the
/// key file as UTF-8.
pub(crate) fn parse_java_properties(bytes: &[u8]) -> Option<HashMap<String, String>> {
    let mut values = HashMap::new();
    let mut logical_line = Vec::new();
    let mut continuing = false;
    for physical_line in bytes.split(|byte| *byte == b'\n') {
        let physical_line = physical_line.strip_suffix(b"\r").unwrap_or(physical_line);
        let line = if continuing {
            let first = physical_line
                .iter()
                .position(|byte| !matches!(*byte, b' ' | b'\t' | 0x0c))
                .unwrap_or(physical_line.len());
            &physical_line[first..]
        } else {
            let first = physical_line
                .iter()
                .position(|byte| !matches!(*byte, b' ' | b'\t' | 0x0c))
                .unwrap_or(physical_line.len());
            let content = &physical_line[first..];
            if content.is_empty() || matches!(content[0], b'#' | b'!') {
                continue;
            }
            content
        };
        logical_line.extend_from_slice(line);
        let slashes = logical_line
            .iter()
            .rev()
            .take_while(|byte| **byte == b'\\')
            .count();
        continuing = slashes % 2 == 1;
        if continuing {
            logical_line.pop();
            continue;
        }
        if !logical_line.is_empty() {
            if let Some((key, value)) = split_property(&logical_line) {
                values.insert(unescape_property(key)?, unescape_property(value)?);
            }
        }
        logical_line.clear();
    }
    // Java Properties.load treats a final odd backslash as an escaped line
    // terminator even when there is no following physical line. The slash is
    // discarded and the accumulated property is still loaded.
    if continuing && !logical_line.is_empty() {
        if let Some((key, value)) = split_property(&logical_line) {
            values.insert(unescape_property(key)?, unescape_property(value)?);
        }
    }
    Some(values)
}

fn split_property(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let mut escaped = false;
    let mut key_end = line.len();
    for (index, byte) in line.iter().copied().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match byte {
            b'\\' => escaped = true,
            b'=' | b':' | b' ' | b'\t' | 0x0c => {
                key_end = index;
                break;
            }
            _ => {}
        }
    }
    let key = &line[..key_end];
    let mut value_start = key_end;
    while value_start < line.len() && matches!(line[value_start], b' ' | b'\t' | 0x0c) {
        value_start += 1;
    }
    if value_start < line.len() && matches!(line[value_start], b'=' | b':') {
        value_start += 1;
    }
    while value_start < line.len() && matches!(line[value_start], b' ' | b'\t' | 0x0c) {
        value_start += 1;
    }
    Some((key, &line[value_start..]))
}

fn unescape_property(bytes: &[u8]) -> Option<String> {
    let mut units = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte != b'\\' {
            units.push(byte as u16);
            index += 1;
            continue;
        }
        index += 1;
        let escaped = *bytes.get(index)?;
        index += 1;
        match escaped {
            b't' => units.push(b'\t' as u16),
            b'n' => units.push(b'\n' as u16),
            b'r' => units.push(b'\r' as u16),
            b'f' => units.push(0x000c),
            b'u' => {
                let end = index.checked_add(4)?;
                let digits = bytes.get(index..end)?;
                let hex = digits.iter().try_fold(0_u16, |value, digit| {
                    let digit = match digit {
                        b'0'..=b'9' => digit - b'0',
                        b'a'..=b'f' => digit - b'a' + 10,
                        b'A'..=b'F' => digit - b'A' + 10,
                        _ => return None,
                    };
                    Some((value << 4) | u16::from(digit))
                })?;
                units.push(hex);
                index = end;
            }
            other => units.push(other as u16),
        }
    }
    Some(String::from_utf16_lossy(&units))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_protocol::header::CreateTopicRequestHeader;
    use rocketmq_classic_remoting::client::RpcHook;
    use serde_json::Value;

    fn fixture() -> Value {
        serde_json::from_str(include_str!(
            "../../../fixtures/security/acl-signatures.json"
        ))
        .unwrap()
    }

    #[test]
    fn java_acl_hook_signatures_and_canonical_bytes_match_fixtures() {
        let fixture = fixture();
        for case in fixture["cases"].as_array().unwrap() {
            let credentials = &case["credentials"];
            let mut credentials_value = SessionCredentials::new(
                credentials["accessKey"].as_str().unwrap(),
                credentials["secretKey"].as_str().unwrap(),
            );
            if let Some(token) = credentials["securityToken"].as_str() {
                credentials_value.set_security_token(Some(token.to_owned()));
            }
            let hook = AclClientRpcHook::new(credentials_value);
            let input_fields = case["inputFields"].as_object().unwrap();
            let mut request = RemotingCommand {
                ext_fields: Some(
                    input_fields
                        .iter()
                        .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                        .collect(),
                ),
                body: Some(
                    STANDARD
                        .decode(case["bodyBase64"].as_str().unwrap())
                        .unwrap(),
                ),
                ..Default::default()
            };
            hook.before_request_mut_result("broker:10911", &mut request)
                .unwrap();
            assert_eq!(
                serde_json::to_value(&request.ext_fields).unwrap(),
                case["fields"],
                "{}",
                case["name"]
            );
            assert_eq!(
                STANDARD.encode(combine_request_content(&request)),
                case["canonicalBase64"],
                "{}",
                case["name"]
            );
            assert_eq!(
                request.ext_fields.as_ref().unwrap()[SIGNATURE],
                case["signature"]
            );
        }
    }

    #[test]
    fn acl_hook_materializes_typed_headers_before_signing_like_java() {
        let case = &fixture()["customHeaderCase"];
        let mut header = CreateTopicRequestHeader::default();
        header.set_topic(Some("header-topic".to_owned()));
        header.set_default_topic(Some("header-default".to_owned()));
        header.set_read_queue_nums(Some(3));
        let mut request =
            RemotingCommand::create_request_command_with_header(17, Some(header)).unwrap();
        request.body = Some(b"header-body".to_vec());

        AclClientRpcHook::new(SessionCredentials::new("header-access", "header-secret"))
            .before_request_mut_result("broker:10911", &mut request)
            .unwrap();

        assert_eq!(
            serde_json::to_value(&request.ext_fields).unwrap(),
            case["fields"]
        );
        assert_eq!(
            STANDARD.encode(combine_request_content(&request)),
            case["canonicalBase64"]
        );
        assert_eq!(
            request.ext_fields.as_ref().unwrap()[SIGNATURE],
            case["signature"]
        );
    }

    #[test]
    fn every_java_acl_signing_algorithm_matches_the_oracle() {
        let fixture = fixture();
        for algorithm in SigningAlgorithm::values() {
            assert_eq!(
                AclSigner::cal_signature_with_algorithm(b"RocketMQ", "12345678", algorithm),
                fixture["algorithmSignatures"][algorithm.java_name()]
            );
        }
    }

    #[test]
    fn credential_debug_output_redacts_secret_material() {
        let credentials = SessionCredentials::new_with_security_token(
            "visible",
            "secret-value",
            Some("token-value".to_owned()),
        );
        let debug = format!("{credentials:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("visible"));
        assert!(!debug.contains("secret-value"));
        assert!(!debug.contains("token-value"));
    }

    #[test]
    fn credential_constructors_properties_and_equality_match_java_fixture() {
        let fixture = fixture();
        let states = &fixture["credentialStates"];

        let two_arguments = SessionCredentials::new("ak-two", "sk-two");
        assert_eq!(credential_fields(&two_arguments), states["twoArguments"]);

        let three_arguments = SessionCredentials::new_with_security_token(
            "ak-three",
            "sk-three",
            Some("token-three".to_owned()),
        );
        assert_eq!(
            credential_fields(&three_arguments),
            states["threeArguments"]
        );

        let mut updated = SessionCredentials::new("", "");
        updated.set_signature(Some("signature-property".to_owned()));
        updated.update_content(&HashMap::from([
            (ACCESS_KEY.to_owned(), "  ak-property\t".to_owned()),
            (SECRET_KEY.to_owned(), " sk-property ".to_owned()),
            (SECURITY_TOKEN.to_owned(), " token-property ".to_owned()),
        ]));
        assert_eq!(credential_fields(&updated), states["updatedProperties"]);

        let key_file = b"# RocketMQ key file\nAccessKey=file-ak\nSecretKey=se\\u0063ret\nSecurityToken=token-file\n";
        let properties = parse_java_properties(key_file).unwrap();
        let mut from_key_file = SessionCredentials::new("", "");
        from_key_file.update_content(&properties);
        assert_eq!(
            credential_fields(&from_key_file),
            states["keyFileProperties"]
        );

        let token_a =
            SessionCredentials::new_with_security_token("ak", "sk", Some("token-a".to_owned()));
        let token_b =
            SessionCredentials::new_with_security_token("ak", "sk", Some("token-b".to_owned()));
        assert_eq!(
            token_a == token_b,
            states["tokenIgnoredByEquality"].as_bool().unwrap()
        );
        assert_eq!(
            hash_credentials(&token_a) == hash_credentials(&token_b),
            states["tokenIgnoredByHash"].as_bool().unwrap()
        );
        let mut signature_changed = token_b;
        signature_changed.set_signature(Some("signature".to_owned()));
        assert_eq!(
            token_a != signature_changed,
            states["signatureIncludedByEquality"].as_bool().unwrap()
        );
    }

    #[test]
    fn default_credentials_load_java_key_file_properties() {
        let unique = format!(
            "rocketmq-acl-key-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::write(
            &path,
            b"AccessKey=file-ak\nSecretKey=se\\u0063ret\nSecurityToken=token-file\n",
        )
        .unwrap();
        let credentials = SessionCredentials::from_key_file(&path);
        std::fs::remove_file(path).unwrap();
        assert_eq!(credentials.access_key(), Some("file-ak"));
        assert_eq!(credentials.secret_key(), Some("secret"));
        assert_eq!(credentials.security_token(), Some("token-file"));
    }

    #[test]
    fn yaml_acl_hook_factory_matches_java_factory_cases() {
        let cases = [
            ("valid", "accessKey: yaml-ak\nsecretKey: yaml-sk\n"),
            ("missingSecret", "accessKey: yaml-ak\n"),
            ("blankAccess", "accessKey: '  '\nsecretKey: yaml-sk\n"),
            ("malformed", "accessKey: [broken\nsecretKey: yaml-sk\n"),
        ];
        let fixture = fixture();
        for (name, yaml) in cases {
            let expected = &fixture["yamlHookCases"][name];
            let hook = SessionCredentials::acl_rpc_hook_from_yaml(yaml);
            assert_eq!(hook.is_some(), expected["hookPresent"], "{name}");
            if let Some(hook) = hook {
                assert_eq!(
                    credential_fields(hook.session_credentials()),
                    expected["credentials"],
                    "{name}"
                );
            }
        }

        let unique = format!(
            "rocketmq-acl-yaml-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::write(&path, "accessKey: yaml-file-ak\nsecretKey: yaml-file-sk\n").unwrap();
        let hook = AclUtils::get_acl_rpc_hook_from_file(&path).unwrap();
        assert_eq!(
            hook.session_credentials().access_key(),
            Some("yaml-file-ak")
        );
        assert_eq!(
            hook.session_credentials().secret_key(),
            Some("yaml-file-sk")
        );
        std::fs::remove_file(&path).unwrap();
        assert!(AclUtils::get_acl_rpc_hook_from_file(&path).is_none());
    }

    #[test]
    fn public_acl_utility_surface_matches_pinned_java_fixture() {
        use std::borrow::Cow;

        let fixture = fixture();
        let _acl_utils = AclUtils;
        assert_eq!(
            fixture["aclUtilsPublicConstructors"],
            serde_json::json!(["public org.apache.rocketmq.acl.common.AclUtils()"])
        );
        assert_eq!(
            fixture["aclUtilsPublicMethods"],
            serde_json::json!([
                "public static [B combineBytes([B,[B)",
                "public static [B combineRequestContent(org.apache.rocketmq.remoting.protocol.RemotingCommand,java.util.SortedMap)",
                "public static [Ljava.lang.String; getAddresses(java.lang.String,java.lang.String)",
                "public static boolean isAsterisk(java.lang.String)",
                "public static boolean isColon(java.lang.String)",
                "public static boolean isComma(java.lang.String)",
                "public static boolean isIPv6Scope([Ljava.lang.String;,int)",
                "public static boolean isIPv6Scope(int)",
                "public static boolean isMinus(java.lang.String)",
                "public static boolean isScope([Ljava.lang.String;,int)",
                "public static boolean isScope(int)",
                "public static boolean isScope(java.lang.String)",
                "public static boolean isScope(java.lang.String,int)",
                "public static java.lang.Object getYamlDataObject(java.io.InputStream,java.lang.Class)",
                "public static java.lang.Object getYamlDataObject(java.lang.String,java.lang.Class)",
                "public static java.lang.String calSignature([B,java.lang.String)",
                "public static java.lang.String expandIP(java.lang.String,int)",
                "public static java.lang.String v6ipProcess(java.lang.String)",
                "public static org.apache.rocketmq.remoting.RPCHook getAclRPCHook(java.io.InputStream)",
                "public static org.apache.rocketmq.remoting.RPCHook getAclRPCHook(java.lang.String)",
                "public static void IPv6AddressCheck(java.lang.String)",
                "public static void verify(java.lang.String,int)"
            ])
        );
        let utilities = &fixture["utilityCases"];
        assert_eq!(
            AclUtils::cal_signature(b"RocketMQ", "12345678"),
            utilities["defaultSignature"]
        );
        assert_eq!(
            AclSigner::cal_signature_with_material(&[0xe9], &[0xe7], SigningAlgorithm::HmacSha256),
            utilities["customCharsetSignature"]
        );
        let custom_content = &utilities["customRequestContent"];
        let ordered_fields = custom_content["fieldsInOrder"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| {
                (
                    field[0].as_str().unwrap().to_owned(),
                    field[1].as_str().unwrap().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        let request = RemotingCommand {
            ext_fields: Some(
                std::collections::BTreeMap::from([(
                    "unused".to_owned(),
                    "request-field-not-in-map".to_owned(),
                )])
                .into(),
            ),
            body: Some(
                STANDARD
                    .decode(custom_content["bodyBase64"].as_str().unwrap())
                    .unwrap(),
            ),
            ..RemotingCommand::default()
        };
        let combined = AclUtils::combine_request_content_with_fields(
            &request,
            ordered_fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        assert_eq!(STANDARD.encode(combined), custom_content["combinedBase64"]);
        assert_eq!(AclSigner::DEFAULT_CHARSET, CHARSET);
        assert_eq!(AclSigner::DEFAULT_ALGORITHM, SigningAlgorithm::HmacSha1);
        assert_eq!(SessionCredentials::ACCESS_KEY, ACCESS_KEY);
        assert_eq!(SessionCredentials::SECRET_KEY, SECRET_KEY);
        assert_eq!(SessionCredentials::SIGNATURE, SIGNATURE);
        assert_eq!(SessionCredentials::SECURITY_TOKEN, SECURITY_TOKEN);

        let body = b"body";
        let prefix = b"prefix";
        let cases = utilities["combineBytes"].as_array().unwrap();
        let combined = [
            AclUtils::combine_bytes(None, None),
            AclUtils::combine_bytes(Some(&[]), Some(body)),
            AclUtils::combine_bytes(Some(prefix), Some(&[])),
            AclUtils::combine_bytes(Some(&[]), Some(&[])),
            AclUtils::combine_bytes(Some(prefix), Some(body)),
        ];
        for (case, output) in cases.iter().zip(combined.iter()) {
            let base64 = output.as_ref().map(|bytes| STANDARD.encode(bytes.as_ref()));
            assert_eq!(base64, case["combinedBase64"].as_str().map(str::to_owned));
        }
        assert!(matches!(combined[1], Some(Cow::Borrowed(value)) if std::ptr::eq(value, body)));
        assert!(matches!(combined[2], Some(Cow::Borrowed(value)) if std::ptr::eq(value, prefix)));
        assert!(matches!(combined[3], Some(Cow::Borrowed(value)) if value.is_empty()));
        assert!(matches!(combined[4], Some(Cow::Owned(_))));

        let address = &utilities["addressPredicates"];
        for (name, value) in [
            (
                "ipv4Valid",
                AclUtils::is_scope_address("192.168.0.255", 4).unwrap(),
            ),
            (
                "ipv4OutOfRange",
                AclUtils::is_scope_address("192.168.256.1", 4).unwrap(),
            ),
            (
                "ipv4InvalidShape",
                AclUtils::is_scope_address("192.168.1", 4).unwrap(),
            ),
            (
                "ipv6ValidPrefix",
                AclUtils::is_scope_address("2001:db8::1", 2).unwrap(),
            ),
            (
                "ipv6OutOfRange",
                AclUtils::is_scope_address("2001:10000::1", 2).unwrap(),
            ),
            ("scopeValueLow", AclUtils::is_scope_value(-1)),
            ("scopeValueMax", AclUtils::is_scope_value(255)),
            ("ipv6ScopeMax", AclUtils::is_ipv6_scope_value(65535)),
            ("ipv6ScopeTooLarge", AclUtils::is_ipv6_scope_value(65536)),
            ("colon", AclUtils::is_colon("2001:db8::1")),
            ("asterisk", AclUtils::is_asterisk("192.168.*")),
            ("comma", AclUtils::is_comma("a,b")),
            ("minus", AclUtils::is_minus("1-2")),
        ] {
            assert_eq!(address[name], value, "{name}");
        }
        assert_eq!(
            address["scopeTokenWhitespace"],
            AclUtils::is_scope_token(" 42 ").unwrap()
        );
        assert_eq!(
            address["scopeTokenOutOfRange"],
            AclUtils::is_scope_token("256").unwrap()
        );

        let segments = &utilities["segmentPredicates"];
        for (name, value) in [
            (
                "ipv4",
                AclUtils::is_scope_segments(&["192", "168", "1", "2"], 3).unwrap(),
            ),
            (
                "ipv4Bad",
                AclUtils::is_scope_segments(&["192", "999", "1", "2"], 3).unwrap(),
            ),
            (
                "ipv6",
                AclUtils::is_ipv6_scope_segments(&["2001", "DB8", "FFFF"], 3).unwrap(),
            ),
            (
                "ipv6Bad",
                AclUtils::is_ipv6_scope_segments(&["2001", "10000"], 2).unwrap(),
            ),
        ] {
            assert_eq!(segments[name], value, "{name}");
        }

        assert_eq!(
            AclUtils::expand_ip("2001:db8::1", 8),
            utilities["expandedIp"].as_str().unwrap()
        );
        assert_eq!(
            AclUtils::v6_ip_process("2::ac5:78:1-200:*"),
            Ok(utilities["processedIp"].as_str().unwrap().to_owned())
        );
        assert_eq!(
            serde_json::to_value(AclUtils::get_addresses("192.168.{1,2}", "{1,2}").unwrap())
                .unwrap(),
            utilities["braceAddresses"]
        );
        assert_eq!(
            AclUtils::ipv6_address_check("2::ac5:78:1-200:*").map(|()| "ok"),
            Ok("ok")
        );
        assert_eq!(
            AclUtils::ipv6_address_check("2::ac5-78:1")
                .unwrap_err()
                .to_string(),
            utilities["invalidIpv6Check"]
        );
        assert_eq!(
            AclUtils::verify("192.168.0.1", 4).map(|()| "ok"),
            Ok(utilities["validVerify"].as_str().unwrap())
        );
        assert_eq!(
            AclUtils::verify("192.168.256.1", 4)
                .unwrap_err()
                .to_string(),
            utilities["invalidVerify"]
        );

        let yaml_reader = AclUtils::get_yaml_data_object::<_, serde_json::Value>(
            std::io::Cursor::new(b"accessKey: yaml-ak\nsecretKey: yaml-sk\n"),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(yaml_reader).unwrap(),
            utilities["yamlReader"]
        );
        assert_eq!(
            AclUtils::get_yaml_data_object::<_, serde_json::Value>(std::io::Cursor::new(b"")),
            Ok(None)
        );
        assert_eq!(utilities["yamlNull"], serde_json::Value::Null);

        let unique = format!(
            "rocketmq-acl-yaml-path-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        std::fs::write(&path, "accessKey: yaml-path-ak\nsecretKey: yaml-path-sk\n").unwrap();
        let yaml_path =
            AclUtils::get_yaml_data_object_from_path::<serde_json::Value>(&path).unwrap();
        assert_eq!(
            serde_json::to_value(yaml_path).unwrap(),
            utilities["yamlPath"]
        );
        let yaml_hook = AclUtils::get_acl_rpc_hook_from_file(&path).unwrap();
        assert_eq!(
            yaml_hook.session_credentials().access_key(),
            utilities["yamlPathHook"]["credentials"]["accessKey"].as_str()
        );
        assert_eq!(
            AclUtils::get_yaml_data_object_from_path::<serde_json::Value>(
                path.with_extension("missing"),
            )
            .unwrap(),
            None
        );
        std::fs::remove_file(path).unwrap();

        let yaml_hook = AclUtils::get_acl_rpc_hook(std::io::Cursor::new(
            b"accessKey: yaml-ak\nsecretKey: yaml-sk\n",
        ))
        .unwrap();
        assert_eq!(
            yaml_hook.session_credentials().access_key(),
            Some("yaml-ak")
        );
    }

    #[test]
    fn acl_exception_constructors_and_mutators_match_java_fixture() {
        let cases = &fixture()["exceptionCases"];
        let status_code = AclException::with_status_code("AUTH_FAILED", 17);
        assert_eq!(status_code.status(), cases["statusCode"]["status"].as_str());
        assert_eq!(
            status_code.code(),
            cases["statusCode"]["code"].as_i64().unwrap() as i32
        );
        assert_eq!(
            status_code.message(),
            cases["statusCode"]["message"].as_str()
        );

        let status_message =
            AclException::with_status_code_and_message("AUTH_FAILED", 17, "denied");
        assert_eq!(
            status_message.message(),
            cases["statusCodeMessage"]["message"].as_str()
        );

        let message = AclException::new("invalid ACL");
        assert_eq!(message.status(), cases["message"]["status"].as_str());
        assert_eq!(
            message.code(),
            cases["message"]["code"].as_i64().unwrap() as i32
        );

        let message_cause =
            AclException::with_cause("invalid ACL", Arc::new(std::io::Error::other("cause")));
        assert_eq!(
            message_cause.cause_message(),
            cases["messageCause"]["causeMessage"].as_str()
        );
        assert!(
            message_cause
                .cause()
                .and_then(|cause| cause.downcast_ref::<std::io::Error>())
                .is_some()
        );
        assert!(
            std::error::Error::source(&message_cause)
                .and_then(|cause| cause.downcast_ref::<std::io::Error>())
                .is_some()
        );

        let status_cause = AclException::with_status_code_and_source(
            "AUTH_FAILED",
            17,
            "denied",
            Arc::new(std::io::Error::other("cause")),
        );
        assert_eq!(
            status_cause.cause_message(),
            cases["statusCodeMessageCause"]["causeMessage"].as_str()
        );
        assert!(
            status_cause
                .cause()
                .and_then(|cause| cause.downcast_ref::<std::io::Error>())
                .is_some()
        );

        let mut mutable = AclException::with_status_code("initial", 1);
        mutable.set_status(Some("UPDATED".into()));
        mutable.set_code(23);
        assert_eq!(mutable.status(), cases["mutable"]["status"].as_str());
        assert_eq!(
            mutable.code(),
            cases["mutable"]["code"].as_i64().unwrap() as i32
        );
    }

    #[test]
    fn acl_expand_ip_matches_turkish_java_default_locale() {
        const CHILD_MARKER: &str = "ROCKETMQ_CLASSIC_TURKISH_ACL_EXPAND_IP_TEST";

        if std::env::var_os(CHILD_MARKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auth::tests::acl_expand_ip_matches_turkish_java_default_locale",
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

        let cases = &fixture()["utilityCases"]["expandedIpTurkishLocaleCases"];
        for case in cases.as_array().expect("Java Turkish expandIP cases") {
            let input = case["input"].as_str().unwrap();
            let part = case["part"].as_i64().unwrap() as i32;
            let expected = case["output"].as_str().unwrap();
            assert_eq!(AclUtils::expand_ip(input, part), expected, "{case}");
        }
    }

    #[test]
    fn acl_expand_ip_matches_lithuanian_java_default_locale() {
        const CHILD_MARKER: &str = "ROCKETMQ_CLASSIC_LITHUANIAN_ACL_EXPAND_IP_TEST";

        if std::env::var_os(CHILD_MARKER).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auth::tests::acl_expand_ip_matches_lithuanian_java_default_locale",
                ])
                .env("LC_ALL", "lt_LT.UTF-8")
                .env(CHILD_MARKER, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "Lithuanian-locale child test failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let cases = &fixture()["utilityCases"]["expandedIpLithuanianLocaleCases"];
        for case in cases.as_array().expect("Java Lithuanian expandIP cases") {
            let input = case["input"].as_str().unwrap();
            let part = case["part"].as_i64().unwrap() as i32;
            let expected = case["output"].as_str().unwrap();
            assert_eq!(AclUtils::expand_ip(input, part), expected, "{case}");
        }
    }

    fn credential_fields(credentials: &SessionCredentials) -> Value {
        let mut fields = serde_json::Map::new();
        for (name, value) in [
            ("accessKey", credentials.access_key()),
            ("secretKey", credentials.secret_key()),
            ("signature", credentials.signature()),
            ("securityToken", credentials.security_token()),
        ] {
            if let Some(value) = value {
                fields.insert(name.to_owned(), Value::String(value.to_owned()));
            }
        }
        Value::Object(fields)
    }

    fn hash_credentials(credentials: &SessionCredentials) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        credentials.hash(&mut hasher);
        hasher.finish()
    }
}
