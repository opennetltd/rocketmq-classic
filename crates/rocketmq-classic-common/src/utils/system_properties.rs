//! Hidden process-wide Java-property bridge used across client crates.
//!
//! This module is public only so sibling crates can share the in-process
//! overlay. It is hidden from generated documentation and is not re-exported
//! from the `rocketmq-classic` facade.

use std::{
    collections::HashMap,
    env,
    sync::{OnceLock, RwLock},
};

const VIP_CHANNEL_PROPERTY: &str = "com.rocketmq.sendMessageWithVIPChannel";
const NAMESRV_ADDR_PROPERTY: &str = "rocketmq.namesrv.addr";
const NAMESRV_DOMAIN_PROPERTY: &str = "rocketmq.namesrv.domain";
const NAMESRV_DOMAIN_SUBGROUP_PROPERTY: &str = "rocketmq.namesrv.domain.subgroup";

static OVERRIDES: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

fn overrides() -> &'static RwLock<HashMap<String, String>> {
    OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
}

fn set(key: &str, value: &str) {
    overrides()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(key.to_owned(), value.to_owned());
}

/// Reads the Java-style property overlay before the process environment.
///
/// This is a hidden cross-crate bridge; the public facade does not re-export
/// it as a user-configurable property store.
#[doc(hidden)]
pub fn get_property(key: &str) -> Option<String> {
    overrides()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(key)
        .cloned()
        .or_else(|| env::var(key).ok())
}

/// Applies `MQClientAPIFactory.init` property writes in Java source order.
///
/// The return value is false for the two Java constructor failures: a missing
/// address in static mode, or a null domain subgroup after the domain has
/// already been written. `None` maps to Java null; an empty string remains a
/// valid non-null property value where Java accepts it.
#[doc(hidden)]
pub fn apply_mq_client_api_factory_properties(
    namesrv_addr: Option<&str>,
    namesrv_domain: Option<&str>,
    namesrv_domain_subgroup: Option<&str>,
) -> bool {
    set(VIP_CHANNEL_PROPERTY, "false");

    if namesrv_domain.is_none_or(str::is_empty) {
        let Some(namesrv_addr) = namesrv_addr.filter(|value| !value.is_empty()) else {
            return false;
        };
        set(NAMESRV_ADDR_PROPERTY, namesrv_addr);
        return true;
    }

    set(NAMESRV_DOMAIN_PROPERTY, namesrv_domain.unwrap());
    let Some(namesrv_domain_subgroup) = namesrv_domain_subgroup else {
        return false;
    };
    set(NAMESRV_DOMAIN_SUBGROUP_PROPERTY, namesrv_domain_subgroup);
    true
}
