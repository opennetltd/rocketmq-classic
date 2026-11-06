//! Private ClientConfig adapters for the public pinned NamespaceUtil helpers.

use rocketmq_classic_common::utils::name_server_address::{
    NameServerAddressError, get_name_srv_addr_from_namesrv_endpoint, matches_namesrv_endpoint,
    parse_instance_id_from_endpoint, validate_instance_endpoint,
};
use rocketmq_classic_protocol::NamespaceUtil;

pub(super) fn wrap(namespace: Option<&str>, resource: Option<&str>) -> Option<String> {
    NamespaceUtil::wrap_namespace(namespace, resource)
}

pub(crate) fn unwrap(namespace: Option<&str>, resource: Option<&str>) -> Option<String> {
    NamespaceUtil::without_namespace_for(resource, namespace)
}

pub(super) fn address(raw: &str) -> String {
    let trimmed = raw.trim_matches(|value: char| value <= '\u{0020}');
    if matches_namesrv_endpoint(Some(trimmed)).unwrap_or(false) {
        // Validation trims, but Java substring extraction uses the raw string.
        get_name_srv_addr_from_namesrv_endpoint(Some(raw)).unwrap_or_default()
    } else {
        raw.into()
    }
}

pub(super) fn instance_endpoint(raw: &str) -> bool {
    validate_instance_endpoint(Some(raw)).unwrap_or(false)
}

pub(super) fn instance(raw: &str) -> Result<String, super::ConfigError> {
    parse_instance_id_from_endpoint(Some(raw))
        .map_err(|error| match error {
            NameServerAddressError::NullEndpoint
            | NameServerAddressError::InvalidInstanceIdBounds => {
                super::ConfigError::InvalidInstanceEndpoint
            }
        })?
        .ok_or(super::ConfigError::InvalidInstanceEndpoint)
}
