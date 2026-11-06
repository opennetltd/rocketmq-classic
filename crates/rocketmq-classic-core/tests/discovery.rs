use rocketmq_classic_core::namesrv::{DefaultTopAddressing, TopAddressing};
use rocketmq_classic_core::{
    config::ClientConfig, namesrv::NameServerUpdateCallback, runtime::ClientRuntimeManager,
};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Provider {
    calls: AtomicUsize,
    results: Mutex<VecDeque<Option<String>>>,
    callback: Mutex<Option<Arc<dyn NameServerUpdateCallback>>>,
}
impl TopAddressing for Provider {
    fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Some("\u{3000}".into()))
        })
    }
    fn register_change_callback(&self, callback: Arc<dyn NameServerUpdateCallback>) {
        *self.callback.lock().unwrap() = Some(callback);
    }
}

async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

struct PanickingProvider;
impl TopAddressing for PanickingProvider {
    fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async {
            panic!("scripted addressing failure");
        })
    }
}

struct PanickingRegistration;
impl TopAddressing for PanickingRegistration {
    fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async { None })
    }
    fn register_change_callback(&self, _: Arc<dyn NameServerUpdateCallback>) {
        panic!("scripted registration failure");
    }
}

struct OrderedProvider {
    name: &'static str,
    address: Option<String>,
    fetch_calls: Arc<Mutex<Vec<String>>>,
    callback_calls: Arc<Mutex<Vec<String>>>,
    callback: Mutex<Option<Arc<dyn NameServerUpdateCallback>>>,
}

impl TopAddressing for OrderedProvider {
    fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
        Box::pin(async move {
            self.fetch_calls.lock().unwrap().push(self.name.to_owned());
            self.address.clone()
        })
    }

    fn register_change_callback(&self, callback: Arc<dyn NameServerUpdateCallback>) {
        self.callback_calls
            .lock()
            .unwrap()
            .push(self.name.to_owned());
        *self.callback.lock().unwrap() = Some(callback);
    }
}

struct EchoCallback;

impl NameServerUpdateCallback for EchoCallback {
    fn on_name_server_address_change(&self, addresses: Option<&str>) -> Option<String> {
        addresses.map(str::to_owned)
    }
}

#[tokio::test]
async fn failed_provider_registration_is_not_published_and_can_retry() {
    let manager = ClientRuntimeManager::with_top_addressing(Arc::new(PanickingRegistration));
    let config = ClientConfig::new("registration-failure");
    for _ in 0..2 {
        assert!(matches!(
            manager.get_or_create(&config).await,
            Err(rocketmq_classic_core::runtime::RuntimeError::AddressingCallbackFailed)
        ));
    }
}

#[tokio::test]
async fn provider_failure_is_best_effort_and_does_not_break_startup() {
    let manager = ClientRuntimeManager::with_top_addressing(Arc::new(PanickingProvider));
    let runtime = manager
        .get_or_create(&ClientConfig::new("provider-failure"))
        .await
        .unwrap();
    runtime.start().await.unwrap();
    assert!(runtime.name_server_addr().is_none());
    assert!(runtime.shutdown().await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn startup_discovery_fixed_rate_retains_blank_and_stops_with_runtime() {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        results: Mutex::new(VecDeque::from([
            Some("first:9876;other:9876;".into()),
            Some("\u{3000}".into()),
            Some("new:9876".into()),
        ])),
        callback: Mutex::new(None),
    });
    let manager = ClientRuntimeManager::with_top_addressing(provider.clone());
    let runtime = manager
        .get_or_create(&ClientConfig::new("discovery"))
        .await
        .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    runtime.start().await.unwrap();
    settle().await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime.name_server_addr().as_deref(),
        Some("first:9876;other:9876;")
    );
    let mut addresses = runtime.remoting().get_name_server_address_list().unwrap();
    addresses.sort();
    assert_eq!(addresses, ["first:9876", "other:9876"]);
    tokio::time::advance(std::time::Duration::from_millis(9999)).await;
    settle().await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        runtime.name_server_addr().as_deref(),
        Some("first:9876;other:9876;")
    );
    tokio::time::advance(std::time::Duration::from_secs(120)).await;
    settle().await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    assert_eq!(runtime.name_server_addr().as_deref(), Some("new:9876"));
    assert!(runtime.shutdown().await.unwrap());
    tokio::time::advance(std::time::Duration::from_secs(360)).await;
    settle().await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    let callback = provider.callback.lock().unwrap().clone().unwrap();
    drop(runtime);
    assert!(
        callback
            .on_name_server_address_change(Some("after-drop:9876"))
            .is_none()
    );
}

#[tokio::test]
async fn explicit_nameserver_skips_fetch_but_provider_callbacks_update_like_java() {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        results: Mutex::default(),
        callback: Mutex::default(),
    });
    let manager = ClientRuntimeManager::with_top_addressing(provider.clone());
    let mut config = ClientConfig::new("explicit");
    config.set_namesrv_addr(Some("explicit:9876".into()));
    let runtime = manager.get_or_create(&config).await.unwrap();
    runtime.start().await.unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let callback = provider.callback.lock().unwrap().clone().unwrap();
    assert!(callback.on_name_server_address_change(None).is_none());
    assert_eq!(
        callback
            .on_name_server_address_change(Some("updated:9876"))
            .as_deref(),
        Some("updated:9876")
    );
    assert_eq!(
        runtime.remoting().get_name_server_address_list().unwrap(),
        ["updated:9876"]
    );
    // Unlike fetch, the Java callback only rejects null, not blank strings.
    assert_eq!(
        callback.on_name_server_address_change(Some("")).as_deref(),
        Some("")
    );
    assert_eq!(
        runtime.remoting().get_name_server_address_list().unwrap(),
        [""]
    );
    assert!(runtime.shutdown().await.unwrap());
}

#[tokio::test]
async fn chained_addressing_providers_match_java_service_loader_order() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/discovery.json")).unwrap();
    let java = &fixture["providerSequence"];
    let names = |key: &str| {
        java[key]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    let fetch_calls = Arc::new(Mutex::new(Vec::new()));
    let callback_calls = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::new(OrderedProvider {
        name: "first",
        address: None,
        fetch_calls: fetch_calls.clone(),
        callback_calls: callback_calls.clone(),
        callback: Mutex::new(None),
    });
    let second = Arc::new(OrderedProvider {
        name: "second",
        address: Some("second:9876".into()),
        fetch_calls: fetch_calls.clone(),
        callback_calls: callback_calls.clone(),
        callback: Mutex::new(None),
    });
    let third = Arc::new(OrderedProvider {
        name: "third",
        address: Some("third:9876".into()),
        fetch_calls: fetch_calls.clone(),
        callback_calls: callback_calls.clone(),
        callback: Mutex::new(None),
    });
    let addressing = DefaultTopAddressing::new("http://127.0.0.1:1/ns", None)
        .with_provider(first.clone())
        .with_provider(second.clone())
        .with_provider(third.clone());

    assert_eq!(
        addressing.fetch_ns_addr().await.as_deref(),
        java["fetchResult"].as_str()
    );
    assert_eq!(*fetch_calls.lock().unwrap(), names("fetchCalls"));

    let callback: Arc<dyn NameServerUpdateCallback> = Arc::new(EchoCallback);
    addressing.register_change_callback(callback.clone());
    assert_eq!(*callback_calls.lock().unwrap(), names("callbackCalls"));
    for provider in [first, second, third] {
        let registered = provider.callback.lock().unwrap().clone().unwrap();
        assert!(Arc::ptr_eq(&callback, &registered));
    }
}
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn http_discovery_uses_java_url_headers_and_newline_normalization() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/rocketmq/nsaddr", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get /rocketmq/nsaddr-unit?nofix=1 http/1.1\r\n"));
        assert!(request.contains("client-version: v5_5_1\r\n"));
        assert!(
            request.contains("content-type: application/x-www-form-urlencoded;charset=utf-8\r\n")
        );
        assert!(request.contains("metaq-client-requestts: "));
        let body = " \t127.0.0.1:9876;127.0.0.2:9876\nignored\rmore \n";
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    let addressing = DefaultTopAddressing::new(url, Some("unit".into()));
    // Java clearNewLine checks CR before LF, not the earliest newline.
    assert_eq!(
        addressing.fetch_ns_addr().await.as_deref(),
        Some("127.0.0.1:9876;127.0.0.2:9876\nignored")
    );
    peer.await.unwrap();
}

async fn read_http_headers(stream: &mut tokio::net::TcpStream) -> String {
    let mut request = Vec::new();
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).await.unwrap();
        request.push(byte[0]);
        if request.ends_with(b"\r\n\r\n") {
            return String::from_utf8(request).unwrap();
        }
    }
}

#[tokio::test]
async fn http_non_success_blank_chunked_and_read_timeout_are_handled() {
    for (response, expected) in [
        (
            "HTTP/1.1 503 Unavailable\r\nContent-Length: 11\r\nConnection: close\r\n\r\nnot-address",
            None,
        ),
        (
            "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\n \t\n",
            Some(""),
        ),
        (
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n9\r\nhost:9876\r\n0\r\n\r\n",
            Some("host:9876"),
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addressing = DefaultTopAddressing::new(
            format!("http://{}/ns", listener.local_addr().unwrap()),
            None,
        );
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_http_headers(&mut stream).await;
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        assert_eq!(addressing.fetch_ns_addr().await.as_deref(), expected);
        peer.await.unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addressing = DefaultTopAddressing::new(
        format!("http://{}/ns", listener.local_addr().unwrap()),
        None,
    );
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_http_headers(&mut stream).await;
        std::future::pending::<()>().await;
    });
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            addressing.fetch_ns_addr_with_timeout(false, 20)
        )
        .await
        .unwrap()
        .is_none()
    );
    peer.abort();
    let _ = peer.await;
}

#[tokio::test]
async fn empty_custom_provider_falls_back_to_http_and_follows_same_scheme_redirect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        results: Mutex::new(VecDeque::from([Some(String::new())])),
        callback: Mutex::default(),
    });
    let addressing = DefaultTopAddressing::new(
        format!("http://{}/ns", listener.local_addr().unwrap()),
        None,
    )
    .with_provider(provider.clone());
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(
            read_http_headers(&mut stream)
                .await
                .starts_with("GET /ns HTTP/1.1")
        );
        stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: /new\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(
            read_http_headers(&mut stream)
                .await
                .starts_with("GET /new HTTP/1.1")
        );
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\nhost:9876",
            )
            .await
            .unwrap();
    });
    assert_eq!(
        addressing.fetch_ns_addr().await.as_deref(),
        Some("host:9876")
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    peer.await.unwrap();
}
