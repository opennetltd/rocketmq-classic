//! Namespace resource transformations matching RocketMQ Java 5.5.1.
//!
//! The Java constants are associated with `NamespaceUtil`, not exported as
//! module-level constants.
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::namespace::{
//!     DLQ_PREFIX_LENGTH, NAMESPACE_SEPARATOR, RETRY_PREFIX_LENGTH, STRING_BLANK,
//! };
//! # fn main() {}
//! ```

use rocketmq_classic_common::topic::TopicValidator;

const NAMESPACE_SEPARATOR: char = '%';
const STRING_BLANK: &str = "";
const RETRY_PREFIX_LENGTH: usize = 7;
const DLQ_PREFIX_LENGTH: usize = 5;

const RETRY_PREFIX: &str = "%RETRY%";
const DLQ_PREFIX: &str = "%DLQ%";
const SYSTEM_CONSUMER_GROUP_PREFIX: &str = "CID_RMQ_SYS_";

/// Public namespace operations corresponding to Java's `NamespaceUtil`.
pub struct NamespaceUtil;

impl NamespaceUtil {
    pub const NAMESPACE_SEPARATOR: char = crate::namespace::NAMESPACE_SEPARATOR;
    pub const STRING_BLANK: &'static str = crate::namespace::STRING_BLANK;
    pub const RETRY_PREFIX_LENGTH: usize = crate::namespace::RETRY_PREFIX_LENGTH;
    pub const DLQ_PREFIX_LENGTH: usize = crate::namespace::DLQ_PREFIX_LENGTH;

    pub fn without_namespace(resource: Option<&str>) -> Option<String> {
        let resource = resource?;
        if resource.is_empty() || is_system_resource(resource) {
            return Some(resource.to_owned());
        }

        let (prefix, plain) = without_retry_and_dlq_parts(resource);
        match plain.split_once(NAMESPACE_SEPARATOR) {
            Some((namespace, rest)) if !namespace.is_empty() => Some(format!("{prefix}{rest}")),
            _ => Some(resource.to_owned()),
        }
    }

    pub fn without_namespace_for(
        resource: Option<&str>,
        namespace: Option<&str>,
    ) -> Option<String> {
        let resource = resource?;
        let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
            return Some(resource.to_owned());
        };
        let (_, plain) = without_retry_and_dlq_parts(resource);
        if plain.starts_with(&format!("{namespace}{NAMESPACE_SEPARATOR}")) {
            Self::without_namespace(Some(resource))
        } else {
            Some(resource.to_owned())
        }
    }

    pub fn wrap_namespace(namespace: Option<&str>, resource: Option<&str>) -> Option<String> {
        let resource = resource?;
        let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
            return Some(resource.to_owned());
        };
        if resource.is_empty()
            || is_system_resource(resource)
            || Self::is_already_with_namespace(Some(resource), Some(namespace))
        {
            return Some(resource.to_owned());
        }
        let (prefix, plain) = without_retry_and_dlq_parts(resource);
        Some(format!("{prefix}{namespace}{NAMESPACE_SEPARATOR}{plain}"))
    }

    pub fn is_already_with_namespace(resource: Option<&str>, namespace: Option<&str>) -> bool {
        let (Some(resource), Some(namespace)) = (
            resource.filter(|value| !value.is_empty()),
            namespace.filter(|value| !value.is_empty()),
        ) else {
            return false;
        };
        if is_system_resource(resource) {
            return false;
        }
        let (_, plain) = without_retry_and_dlq_parts(resource);
        plain.starts_with(&format!("{namespace}{NAMESPACE_SEPARATOR}"))
    }

    pub fn wrap_namespace_and_retry(
        namespace: Option<&str>,
        consumer_group: Option<&str>,
    ) -> Option<String> {
        let consumer_group = consumer_group.filter(|value| !value.is_empty())?;
        Some(format!(
            "{RETRY_PREFIX}{}",
            Self::wrap_namespace(namespace, Some(consumer_group))
                .expect("non-empty consumer group")
        ))
    }

    pub fn get_namespace_from_resource(resource: Option<&str>) -> String {
        let Some(resource) = resource.filter(|value| !value.is_empty()) else {
            return STRING_BLANK.to_owned();
        };
        if is_system_resource(resource) {
            return STRING_BLANK.to_owned();
        }
        let (_, plain) = without_retry_and_dlq_parts(resource);
        plain
            .split_once(NAMESPACE_SEPARATOR)
            .filter(|(namespace, _)| !namespace.is_empty())
            .map_or_else(
                || STRING_BLANK.to_owned(),
                |(namespace, _)| namespace.to_owned(),
            )
    }

    pub fn without_retry_and_dlq(resource: Option<&str>) -> String {
        resource.map_or_else(String::new, |resource| {
            without_retry_and_dlq_parts(resource).1.to_owned()
        })
    }

    pub fn is_retry_topic(resource: Option<&str>) -> bool {
        resource.is_some_and(|resource| {
            !resource.trim().is_empty() && resource.starts_with(RETRY_PREFIX)
        })
    }

    pub fn is_dlq_topic(resource: Option<&str>) -> bool {
        resource
            .is_some_and(|resource| !resource.trim().is_empty() && resource.starts_with(DLQ_PREFIX))
    }
}

fn without_retry_and_dlq_parts(resource: &str) -> (&'static str, &str) {
    if let Some(plain) = resource.strip_prefix(RETRY_PREFIX) {
        (RETRY_PREFIX, plain)
    } else if let Some(plain) = resource.strip_prefix(DLQ_PREFIX) {
        (DLQ_PREFIX, plain)
    } else {
        ("", resource)
    }
}

fn is_system_resource(resource: &str) -> bool {
    TopicValidator::is_system_topic(resource) || resource.starts_with(SYSTEM_CONSUMER_GROUP_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::NamespaceUtil;

    #[test]
    fn wraps_retry_and_dlq_resources_once() {
        assert_eq!(
            NamespaceUtil::wrap_namespace(Some("tenant"), Some("%RETRY%workers")).as_deref(),
            Some("%RETRY%tenant%workers")
        );
        assert_eq!(
            NamespaceUtil::wrap_namespace(Some("tenant"), Some("%DLQ%workers")).as_deref(),
            Some("%DLQ%tenant%workers")
        );
        assert_eq!(
            NamespaceUtil::without_namespace(Some("%RETRY%tenant%workers")).as_deref(),
            Some("%RETRY%workers")
        );
    }
}
