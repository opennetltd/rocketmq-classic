//! Public helpers matching Java's NameServerAddressUtils.

use thiserror::Error;

pub const INSTANCE_PREFIX: &str = "MQ_INST_";
pub const INSTANCE_REGEX: &str = r"MQ_INST_\w+_\w+";
pub const ENDPOINT_PREFIX: &str = r"(\w+://|)";
pub const NAMESRV_ENDPOINT_PATTERN: &str = r"^http://.*";
pub const INST_ENDPOINT_PATTERN: &str = r"^(\w+://|)MQ_INST_\w+_\w+\..*";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NameServerAddressError {
    #[error("instance endpoint must not be null")]
    NullEndpoint,
    #[error("instance ID substring bounds are invalid")]
    InvalidInstanceIdBounds,
}

/// Reads the Java system-property key and then the NAMESRV_ADDR environment fallback.
///
/// Factory-written Java properties use the shared hidden overlay; otherwise
/// Rust reads the exact property key from the process environment.
pub fn get_name_server_addresses() -> Option<String> {
    super::system_properties::get_property("rocketmq.namesrv.addr")
        .or_else(|| std::env::var("NAMESRV_ADDR").ok())
}

/// Tests Java's NAMESRV_ENDPOINT_PATTERN.matcher(value).matches() behavior.
///
/// Passing None maps Java's NullPointerException to NameServerAddressError::NullEndpoint.
pub fn matches_namesrv_endpoint(endpoint: Option<&str>) -> Result<bool, NameServerAddressError> {
    let endpoint = endpoint.ok_or(NameServerAddressError::NullEndpoint)?;
    Ok(endpoint.starts_with("http://") && !has_java_line_terminator(endpoint))
}

/// Tests a non-null endpoint using Java's instance-endpoint pattern.
///
/// Passing None maps Java's NullPointerException to NameServerAddressError::NullEndpoint.
pub fn validate_instance_endpoint(endpoint: Option<&str>) -> Result<bool, NameServerAddressError> {
    let endpoint = endpoint.ok_or(NameServerAddressError::NullEndpoint)?;
    Ok(validate_instance_endpoint_non_null(endpoint))
}

fn validate_instance_endpoint_non_null(endpoint: &str) -> bool {
    if has_java_line_terminator(endpoint) {
        return false;
    }

    if let Some((scheme, suffix)) = endpoint.split_once("://") {
        if is_java_word(scheme) && instance_pattern_matches(suffix) {
            return true;
        }
    }
    instance_pattern_matches(endpoint)
}

fn instance_pattern_matches(endpoint: &str) -> bool {
    let Some((instance, _)) = endpoint.split_once('.') else {
        return false;
    };
    let Some(suffix) = instance.strip_prefix(INSTANCE_PREFIX) else {
        return false;
    };
    suffix
        .bytes()
        .enumerate()
        .any(|(index, byte)| byte == b'_' && index > 0 && index + 1 < suffix.len())
        && suffix.bytes().all(is_java_word_byte)
}

/// Extracts the instance identifier using Java's first-dot and last-slash bounds.
///
/// Java returns null for a null or empty input. Invalid substring bounds map to
/// NameServerAddressError::InvalidInstanceIdBounds.
pub fn parse_instance_id_from_endpoint(
    endpoint: Option<&str>,
) -> Result<Option<String>, NameServerAddressError> {
    let Some(endpoint) = endpoint.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let start = endpoint.rfind('/').map_or(0, |index| index + 1);
    let end = endpoint
        .find('.')
        .ok_or(NameServerAddressError::InvalidInstanceIdBounds)?;
    let instance = endpoint
        .get(start..end)
        .ok_or(NameServerAddressError::InvalidInstanceIdBounds)?;
    Ok(Some(instance.to_owned()))
}

/// Returns the part after the final slash, or None for Java null/empty input.
pub fn get_name_srv_addr_from_namesrv_endpoint(endpoint: Option<&str>) -> Option<String> {
    let endpoint = endpoint.filter(|value| !value.is_empty())?;
    Some(endpoint.rsplit('/').next().unwrap_or_default().to_owned())
}

fn is_java_word(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(is_java_word_byte)
}

fn is_java_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn has_java_line_terminator(value: &str) -> bool {
    value.chars().any(|character| {
        matches!(
            character,
            '\n' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}'
        )
    })
}
