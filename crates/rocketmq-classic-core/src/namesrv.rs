use rocketmq_classic_remoting::client::RemotingClient;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Rust-native counterpart of Java's TopAddressing SPI. Implementations may
/// supply addresses on demand and notify the registered callback later.
pub trait TopAddressing: Send + Sync + 'static {
    fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>>;
    fn register_change_callback(&self, _callback: Arc<dyn NameServerUpdateCallback>) {}
}

pub trait NameServerUpdateCallback: Send + Sync + 'static {
    fn on_name_server_address_change(&self, addresses: Option<&str>) -> Option<String>;
}

pub struct DefaultTopAddressing {
    ws_addr: String,
    unit_name: Option<String>,
    parameters: BTreeMap<String, String>,
    providers: Vec<Arc<dyn TopAddressing>>,
    ns_addr: Mutex<Option<String>>,
}

impl DefaultTopAddressing {
    pub fn new(ws_addr: impl Into<String>, unit_name: Option<String>) -> Self {
        Self {
            ws_addr: ws_addr.into(),
            unit_name,
            parameters: BTreeMap::new(),
            providers: Vec::new(),
            ns_addr: Mutex::new(None),
        }
    }

    pub fn with_parameters(
        ws_addr: impl Into<String>,
        unit_name: Option<String>,
        parameters: BTreeMap<String, String>,
    ) -> Self {
        let mut result = Self::new(ws_addr, unit_name);
        result.parameters = parameters;
        result
    }

    /// Add a provider in Java ServiceLoader order without using a process
    /// singleton. Chained calls retain each provider in insertion order.
    pub fn with_provider(mut self, provider: Arc<dyn TopAddressing>) -> Self {
        self.providers.push(provider);
        self
    }

    pub fn ns_addr(&self) -> Option<String> {
        self.ns_addr.lock().ok().and_then(|value| value.clone())
    }
    pub fn set_ns_addr(&self, value: Option<String>) {
        if let Ok(mut current) = self.ns_addr.lock() {
            *current = value;
        }
    }

    fn request_url(&self) -> String {
        let mut url = self.ws_addr.clone();
        if let Some(unit) = self
            .unit_name
            .as_deref()
            .filter(|unit| !java_util_blank(unit))
        {
            url.push('-');
            url.push_str(unit);
            url.push_str("?nofix=1");
            if !self.parameters.is_empty() {
                url.push('&');
            }
        } else if !self.parameters.is_empty() {
            url.push('?');
        }
        for (index, (key, value)) in self.parameters.iter().enumerate() {
            if index > 0 {
                url.push('&');
            }
            url.push_str(key);
            url.push('=');
            url.push_str(value);
        }
        url
    }

    pub async fn fetch_ns_addr_with_timeout(
        &self,
        _verbose: bool,
        timeout_millis: i64,
    ) -> Option<String> {
        // HttpURLConnection receives a Java long-to-int cast; zero disables
        // connect/read timeouts. Invalid negative values produce no address.
        let timeout_millis = timeout_millis as i32;
        if timeout_millis < 0 {
            return None;
        }
        // Use the same default backend as remoting; do not activate a second
        // rustls provider feature and make existing TLS builders ambiguous.
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .pool_max_idle_per_host(0)
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 20 {
                    attempt.error("too many redirects")
                } else if attempt
                    .previous()
                    .last()
                    .is_some_and(|previous| previous.scheme() != attempt.url().scheme())
                {
                    attempt.stop()
                } else {
                    attempt.follow()
                }
            }));
        if timeout_millis != 0 {
            builder = builder
                .connect_timeout(Duration::from_millis(timeout_millis as u64))
                .read_timeout(Duration::from_millis(timeout_millis as u64));
        }
        let client = builder.build().ok()?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let response = client
            .get(self.request_url())
            .header("Client-Version", "V5_5_1")
            .header(
                "Content-Type",
                "application/x-www-form-urlencoded;charset=UTF-8",
            )
            .header("Metaq-Client-RequestTS", timestamp.to_string())
            .send()
            .await
            .ok()?;
        if response.status() != reqwest::StatusCode::OK {
            return None;
        }
        let body = response.bytes().await.ok()?;
        Some(clear_newline(&String::from_utf8_lossy(&body)))
    }
}

impl TopAddressing for DefaultTopAddressing {
    fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async move {
            for provider in &self.providers {
                if let Some(address) = provider
                    .fetch_ns_addr()
                    .await
                    .filter(|value| !value.is_empty())
                {
                    return Some(address);
                }
            }
            self.fetch_ns_addr_with_timeout(true, 3000).await
        })
    }
    fn register_change_callback(&self, callback: Arc<dyn NameServerUpdateCallback>) {
        for provider in &self.providers {
            provider.register_change_callback(callback.clone());
        }
    }
}

// UtilAll delegates to StringUtils/Character.isWhitespace, not Unicode White_Space.
pub(crate) fn java_util_blank(value: &str) -> bool {
    value.chars().all(crate::config::java_whitespace)
}

fn clear_newline(value: &str) -> String {
    let value = value.trim_matches(|ch: char| ch <= '\u{20}');
    value
        .split_once('\r')
        .or_else(|| value.split_once('\n'))
        .map_or(value, |(first, _)| first)
        .to_owned()
}

pub(crate) fn ws_addr(properties: &std::collections::HashMap<String, String>) -> String {
    let domain = properties
        .get("rocketmq.namesrv.domain")
        .map(String::as_str)
        .unwrap_or("jmenv.tbsite.net");
    let subgroup = properties
        .get("rocketmq.namesrv.domain.subgroup")
        .map(String::as_str)
        .unwrap_or("nsaddr");
    let port = if domain.find(':').is_some_and(|index| index > 0) {
        ""
    } else {
        ":8080"
    };
    format!("http://{domain}{port}/rocketmq/{subgroup}")
}

pub(crate) struct DiscoveryState {
    current: Mutex<Option<String>>,
    remoting: RemotingClient,
}

impl DiscoveryState {
    pub fn new(remoting: RemotingClient) -> Arc<Self> {
        Arc::new(Self {
            current: Mutex::new(None),
            remoting,
        })
    }
    pub fn callback(self: &Arc<Self>) -> Arc<dyn NameServerUpdateCallback> {
        Arc::new(Callback(Arc::downgrade(self)))
    }
    pub fn current(&self) -> Option<String> {
        self.current.lock().ok().and_then(|value| value.clone())
    }
    pub fn fetched(&self, addresses: Option<String>) -> Option<String> {
        self.change(addresses.as_deref().filter(|value| !java_util_blank(value)))
    }
    fn change(&self, addresses: Option<&str>) -> Option<String> {
        let mut current = self.current.lock().ok()?;
        if let Some(addresses) = addresses {
            if current.as_deref() != Some(addresses) {
                self.remoting
                    .update_name_server_address_list(split_addresses(addresses));
                *current = Some(addresses.to_owned());
            }
        }
        current.clone()
    }
}

pub(crate) fn split_addresses(value: &str) -> Vec<String> {
    if value.is_empty() {
        return vec![String::new()];
    }
    let value = value.trim_end_matches(';');
    if value.is_empty() {
        return Vec::new();
    }
    value.split(';').map(str::to_owned).collect()
}

struct Callback(Weak<DiscoveryState>);
impl NameServerUpdateCallback for Callback {
    fn on_name_server_address_change(&self, addresses: Option<&str>) -> Option<String> {
        self.0.upgrade()?.change(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoked_java_discovery_url_normalization_and_callbacks_match() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/discovery.json")).unwrap();
        for case in fixture["normalization"].as_array().unwrap() {
            assert_eq!(
                clear_newline(case["input"].as_str().unwrap()),
                case["result"].as_str().unwrap()
            );
        }
        for case in fixture["urls"].as_array().unwrap() {
            let properties = std::collections::HashMap::from([
                (
                    "rocketmq.namesrv.domain".into(),
                    case["domain"].as_str().unwrap().into(),
                ),
                (
                    "rocketmq.namesrv.domain.subgroup".into(),
                    case["subgroup"].as_str().unwrap().into(),
                ),
            ]);
            assert_eq!(ws_addr(&properties), case["result"].as_str().unwrap());
        }
        let remoting = RemotingClient::new(rocketmq_classic_remoting::client::TransportMode::Plain);
        remoting.update_name_server_address_list(vec!["explicit:9876".into()]);
        let state = DiscoveryState::new(remoting.clone());
        let callback = state.callback();
        for case in fixture["callbacks"].as_array().unwrap() {
            assert_eq!(
                callback
                    .on_name_server_address_change(case["input"].as_str())
                    .as_deref(),
                case["result"].as_str()
            );
            let mut addresses = remoting.get_name_server_address_list().unwrap();
            addresses.sort();
            assert_eq!(serde_json::to_value(addresses).unwrap(), case["addresses"]);
        }
    }
    #[test]
    fn java_domain_url_parameters_and_blank_rules() {
        assert_eq!(
            ws_addr(&Default::default()),
            "http://jmenv.tbsite.net:8080/rocketmq/nsaddr"
        );
        assert_eq!(
            ws_addr(&std::collections::HashMap::from([(
                "rocketmq.namesrv.domain".into(),
                "host:81".into()
            )])),
            "http://host:81/rocketmq/nsaddr"
        );
        assert_eq!(
            DefaultTopAddressing::with_parameters(
                "http://host/path",
                Some("unit".into()),
                BTreeMap::from([("a".into(), "x y".into())])
            )
            .request_url(),
            "http://host/path-unit?nofix=1&a=x y"
        );
        assert!(!java_util_blank("\u{a0}"));
        assert_eq!(split_addresses("a;;b;;"), ["a", "", "b"]);
        assert!(split_addresses(";;").is_empty());
        assert_eq!(split_addresses(""), [""]);
    }

    #[test]
    fn nameserver_address_property_matches_invoked_java_accessors() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/discovery.json")).unwrap();
        let java = &fixture["nsAddrProperty"];
        let addressing = DefaultTopAddressing::new("http://unused.invalid", None);
        assert_eq!(addressing.ns_addr().as_deref(), java["initial"].as_str());
        addressing.set_ns_addr(Some("discovered:9876".into()));
        assert_eq!(addressing.ns_addr().as_deref(), java["afterSet"].as_str());
        addressing.set_ns_addr(None);
        assert_eq!(addressing.ns_addr().as_deref(), java["afterClear"].as_str());
    }
}
