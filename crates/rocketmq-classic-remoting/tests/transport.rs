use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use rocketmq_classic_protocol::codes::{SerializeType, request, response};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{
        CallbackExecutor, ChannelEvent, ChannelEventListener, ChannelHandle, ClientConfig,
        InvokeCallback, NettyEventType, RemotingClient, RequestContext, RequestExecutor,
        RequestPipeline, RequestPipelineError, RequestProcessor, ResponseWaitError,
        ResponseWaitInterrupt, RpcHook, RpcHookError, TransportMode, tls::TlsConfig,
    },
    codec::frame,
    error::TransportError,
    metrics::{
        AttributeValue, Attributes, HISTOGRAM_RPC_LATENCY, LABEL_IS_LONG_POLLING,
        LABEL_REQUEST_CODE, LABEL_RESPONSE_CODE, LABEL_RESULT, RESULT_ONEWAY, RESULT_SUCCESS,
        RemotingMetricsManager, RpcLatencyHistogram,
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

async fn read_command<S: AsyncRead + Unpin>(stream: &mut S) -> RemotingCommand {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let frame_length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = BytesMut::with_capacity(frame_length + 4);
    bytes.extend_from_slice(&prefix);
    bytes.resize(frame_length + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn try_read_command<S: AsyncRead + Unpin>(stream: &mut S) -> Option<RemotingCommand> {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).await.ok()?;
    let frame_length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = BytesMut::with_capacity(frame_length + 4);
    bytes.extend_from_slice(&prefix);
    bytes.resize(frame_length + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes).ok().flatten()
}

fn config_is_accepted(config: ClientConfig) -> Result<(), TransportError> {
    RemotingClient::with_config(config).map(drop)
}

#[test]
fn response_future_wait_interrupt_types_are_public() {
    let interrupt = ResponseWaitInterrupt::new();
    interrupt.interrupt();
    assert_eq!(
        ResponseWaitError::Interrupted.java_exception_name(),
        "java.lang.InterruptedException"
    );
}

#[tokio::test]
async fn mutable_rpc_hook_fields_reach_sync_async_oneway_and_callback_snapshots() {
    struct Mutator;
    impl RpcHook for Mutator {
        fn before_request_mut_result(
            &self,
            _: &str,
            request: &mut RemotingCommand,
        ) -> Result<(), RpcHookError> {
            request
                .ext_fields
                .get_or_insert_with(Default::default)
                .insert("signed".into(), "yes".into());
            Ok(())
        }
        fn after_response(&self, _: &str, request: &RemotingCommand, _: &RemotingCommand) {
            assert_eq!(request.ext_fields.as_ref().unwrap()["signed"], "yes");
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for code in [1, 2, 3] {
            let command = read_command(&mut stream).await;
            assert_eq!(command.code, code);
            assert_eq!(command.ext_fields.as_ref().unwrap()["signed"], "yes");
            if code != 3 {
                let mut reply = RemotingCommand {
                    opaque: command.opaque,
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
            } else {
                assert!(command.is_oneway());
            }
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.add_rpc_hook(Arc::new(Mutator));
    client.start().unwrap();
    client
        .invoke_sync_with_deadline(
            &address,
            RemotingCommand {
                code: 1,
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    let (sender, mut receive) = tokio::sync::mpsc::unbounded_channel();
    client
        .invoke_async(
            &address,
            RemotingCommand {
                code: 2,
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(3),
            Arc::new(ContextCallback {
                sender,
                events: Arc::default(),
            }),
        )
        .unwrap();
    let context = tokio::time::timeout(Duration::from_secs(3), receive.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(context.error.is_none());
    assert_eq!(
        context.request.ext_fields.as_ref().unwrap()["signed"],
        "yes"
    );
    client
        .invoke_oneway(
            &address,
            RemotingCommand {
                code: 3,
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap();
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn mutable_inbound_hooks_reach_pipeline_processor_after_hook_and_response_identity() {
    struct Mutator;
    impl RpcHook for Mutator {
        fn before_request_mut_result(
            &self,
            _: &str,
            request: &mut RemotingCommand,
        ) -> Result<(), RpcHookError> {
            if request.code == 601 {
                request
                    .ext_fields
                    .get_or_insert_with(Default::default)
                    .insert("inbound".into(), "yes".into());
                request.opaque = 999;
            }
            Ok(())
        }
        fn after_response(&self, _: &str, request: &RemotingCommand, _: &RemotingCommand) {
            if request.code == 601 {
                assert_eq!(request.opaque, 999);
                assert_eq!(request.ext_fields.as_ref().unwrap()["inbound"], "yes");
            }
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let initial = read_command(&mut stream).await;
        let mut reply = RemotingCommand {
            opaque: initial.opaque,
            ..Default::default()
        };
        reply.mark_response();
        write_command(&mut stream, &reply).await;
        write_command(
            &mut stream,
            &RemotingCommand {
                code: 601,
                opaque: 401,
                ..Default::default()
            },
        )
        .await;
        let response = read_command(&mut stream).await;
        assert_eq!(response.opaque, 999);
        assert!(response.is_response());
        assert_eq!(response.code, 0);
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.add_rpc_hook(Arc::new(Mutator));
    client.set_request_pipeline(Some(Arc::new(|request: &RemotingCommand| {
        assert_eq!(request.ext_fields.as_ref().unwrap()["inbound"], "yes");
        assert_eq!(request.opaque, 999);
        Ok(())
    })));
    client.register_processor(
        601,
        Arc::new(|request: RemotingCommand| {
            assert_eq!(request.ext_fields.as_ref().unwrap()["inbound"], "yes");
            assert_eq!(request.opaque, 999);
            Ok(Some(RemotingCommand::default()))
        }),
    );
    client.start().unwrap();
    client
        .invoke_sync_with_deadline(
            &address,
            RemotingCommand {
                code: 600,
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
    client.shutdown().await;
}

async fn write_command<S: AsyncWriteExt + Unpin>(stream: &mut S, command: &RemotingCommand) {
    stream
        .write_all(&frame::encode(command).unwrap())
        .await
        .unwrap();
}

fn command(code: i32) -> RemotingCommand {
    RemotingCommand {
        code,
        serialization: SerializeType::Json,
        ..RemotingCommand::default()
    }
}

#[tokio::test]
async fn empty_host_endpoint_connects_to_java_ipv4_loopback_target() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        assert_eq!(request.code, 77);
        let mut response = RemotingCommand {
            opaque: request.opaque,
            ..Default::default()
        };
        response.mark_response();
        write_command(&mut stream, &response).await;
    });

    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let response = client
        .invoke_sync(&format!(":{port}"), command(77), Duration::from_secs(2))
        .await
        .unwrap();
    assert!(response.is_response());
    client.shutdown().await;
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn raw_address_spellings_keep_distinct_java_channel_keys() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut streams = Vec::new();
        for _ in 0..2 {
            let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
            streams.push(stream);
        }
        streams
    });

    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let address = address.to_string();
    let (host, port) = address.rsplit_once(':').unwrap();
    let alternate = format!("{host}:0{port}");
    assert!(
        client
            .is_address_reachable(&address, Instant::now() + Duration::from_secs(2))
            .await
    );
    assert!(
        client
            .is_address_reachable(&alternate, Instant::now() + Duration::from_secs(2))
            .await
    );
    assert_eq!(peer.await.unwrap().len(), 2);
    client.shutdown().await;
}

#[test]
fn p3_client_can_be_constructed_without_runtime_and_start_requires_one() {
    let client = RemotingClient::new(TransportMode::Plain);
    assert!(matches!(
        client.start(),
        Err(TransportError::InvalidConfiguration(_))
    ));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        client.start().unwrap();
        client.shutdown().await;
    });
}

#[tokio::test]
async fn p3_client_start_is_explicit_and_idempotent() {
    let client = RemotingClient::new(TransportMode::Plain);
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(matches!(
        client.invoke("127.0.0.1:1", command(1), deadline).await,
        Err(TransportError::NotStarted)
    ));
    assert!(matches!(
        client
            .invoke_sync("127.0.0.1:1", command(1), Duration::from_secs(1))
            .await,
        Err(TransportError::NotStarted)
    ));
    assert!(matches!(
        client.invoke_name_server(command(1), deadline).await,
        Err(TransportError::NotStarted)
    ));
    assert!(!client.is_address_reachable("127.0.0.1:1", deadline).await);
    assert!(matches!(
        client.invoke_oneway("127.0.0.1:1", command(1), deadline),
        Err(TransportError::NotStarted)
    ));
    assert!(matches!(
        client.invoke_async(
            "127.0.0.1:1",
            command(1),
            deadline,
            Arc::new(|_: Result<RemotingCommand, TransportError>| {}),
        ),
        Err(TransportError::NotStarted)
    ));

    client.start().unwrap();
    client.start().unwrap();
    assert!(matches!(
        client.invoke("127.0.0.1:1", command(1), deadline).await,
        Err(TransportError::ConnectRequest { .. })
    ));
    client.shutdown().await;
    assert_eq!(client.start(), Err(TransportError::Shutdown));
}

#[tokio::test]
async fn zero_timeout_connects_before_expiring_without_sending_a_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();

    assert!(matches!(
        client
            .invoke_sync(&address, command(77), Duration::ZERO)
            .await,
        Err(TransportError::TimeoutWithContext {
            phase: rocketmq_classic_remoting::error::TimeoutPhase::Operation,
            ..
        })
    ));
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .expect("synchronous Java invocation obtains the channel first")
        .unwrap();
    let mut byte = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), socket.read(&mut byte))
            .await
            .is_err()
    );

    client.shutdown().await;
    assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
}

struct ContextCallback {
    sender: tokio::sync::mpsc::UnboundedSender<CallbackSnapshot>,
    events: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

struct CallbackSnapshot {
    request: RemotingCommand,
    response: Option<RemotingCommand>,
    error: Option<TransportError>,
    timeout_millis: i64,
    send_request_ok: bool,
    timed_out: bool,
    channel_address: Option<String>,
}

struct EventCallback(Arc<std::sync::Mutex<Vec<&'static str>>>);

impl InvokeCallback for EventCallback {
    fn operation_complete(&self, _: &rocketmq_classic_remoting::client::ResponseFuture) {
        self.0.lock().unwrap().push("complete");
    }

    fn operation_fail(&self, _: &TransportError) {
        self.0.lock().unwrap().push("fail");
    }
}

impl InvokeCallback for ContextCallback {
    fn operation_complete(&self, future: &rocketmq_classic_remoting::client::ResponseFuture) {
        self.events.lock().unwrap().push("complete");
        let _ = self.sender.send(CallbackSnapshot {
            request: future.request_command().unwrap(),
            response: future.response_command(),
            error: future.cause(),
            timeout_millis: future.timeout_millis(),
            send_request_ok: future.is_send_request_ok(),
            timed_out: future.is_timeout(),
            channel_address: future
                .channel()
                .map(|channel| channel.remote_addr().to_owned()),
        });
    }

    fn operation_succeed(&self, _: &RemotingCommand) {
        self.events.lock().unwrap().push("succeed");
    }

    fn operation_fail(&self, _: &TransportError) {
        self.events.lock().unwrap().push("fail");
    }
}

async fn wait_for_callback_events(
    events: &Arc<std::sync::Mutex<Vec<&'static str>>>,
    expected: &[&'static str],
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if events.lock().unwrap().as_slice() == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("callback event sequence was incomplete");
}

#[tokio::test]
async fn p3_async_connect_failure_calls_fail_without_complete() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .invoke_async(
            address,
            command(1),
            Instant::now() + Duration::from_secs(1),
            Arc::new(EventCallback(events.clone())),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while events.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["fail"]);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_shutdown_drains_callback_from_pending_invoke() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        let _ = request_seen_tx.send(());
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .invoke_async(
            address.to_string(),
            command(91),
            Instant::now() + Duration::from_secs(5),
            Arc::new(ContextCallback {
                sender: callback_tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), request_seen_rx)
        .await
        .expect("peer did not receive invoke before shutdown")
        .unwrap();

    client.shutdown().await;

    let context = tokio::time::timeout(Duration::from_secs(1), callback_rx.recv())
        .await
        .expect("shutdown lost the pending invoke callback")
        .unwrap();
    assert_eq!(context.error, Some(TransportError::Shutdown));
    wait_for_callback_events(&events, &["complete", "fail"]).await;
    peer.abort();
}

#[tokio::test]
async fn p3_future_connect_failure_includes_java_remote_address_and_null_cause() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    for result in [
        client
            .invoke(
                &address,
                command(1),
                Instant::now() + Duration::from_secs(1),
            )
            .await,
        client
            .invoke_sync(&address, command(1), Duration::from_secs(1))
            .await,
    ] {
        assert!(matches!(
            result,
            Err(TransportError::ConnectRequest {
                remote_addr,
                cause: Some(_),
            }) if remote_addr == address
        ));
    }
    client.shutdown().await;
}

#[tokio::test]
async fn p3_result_closure_receives_connect_failure_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);

    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let sender = Arc::new(std::sync::Mutex::new(Some(sender)));
    client
        .invoke_async(
            address.clone(),
            command(71),
            Instant::now() + Duration::from_secs(1),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = sender.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        Err(TransportError::ConnectRequest { remote_addr, .. }) if remote_addr == address
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn p3_async_callback_contains_response_future_metadata_and_timeout_grace() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut response = command(19);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut request = command(18);
    request.opaque = 73;
    client
        .invoke_async(
            address.to_string(),
            request,
            Instant::now() + Duration::from_millis(50),
            Arc::new(ContextCallback {
                sender: callback_tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    let context = tokio::time::timeout(Duration::from_secs(2), callback_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.response.as_ref().unwrap().code, 19);
    assert_eq!(
        context.request.opaque,
        context.response.as_ref().unwrap().opaque
    );
    assert!((0..=50).contains(&context.timeout_millis));
    assert!(context.send_request_ok);
    assert!(context.timed_out);
    assert!(context.error.is_none());
    assert_eq!(
        context.channel_address.as_deref(),
        Some(address.to_string().as_str())
    );
    assert!(context.channel_address.is_some());
    tokio::time::timeout(Duration::from_secs(2), async {
        while events.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("success callback should follow the completion callback");
    assert_eq!(*events.lock().unwrap(), vec!["complete", "succeed"]);
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_expired_async_callback_retains_request_and_timeout_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut byte).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(1)
            )
            .await
    );
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut request = command(20);
    request.opaque = 74;
    client
        .invoke_async(
            address.to_string(),
            request,
            Instant::now() - Duration::from_millis(100),
            Arc::new(ContextCallback {
                sender: callback_tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    let context = callback_rx.recv().await.unwrap();
    assert_eq!(context.request.code, 20);
    assert_eq!(context.request.opaque, 74);
    assert!(context.response.is_none());
    assert!(matches!(
        context.error,
        Some(TransportError::TimeoutWithContext {
            phase: rocketmq_classic_remoting::error::TimeoutPhase::Operation,
            remote_addr: Some(ref remote_addr),
            ..
        }) if remote_addr == &address.to_string()
    ));
    assert!(context.timeout_millis < 0);
    assert!(context.timed_out);
    assert!(context.send_request_ok);
    assert_eq!(
        context.channel_address.as_deref(),
        Some(address.to_string().as_str())
    );
    assert!(context.channel_address.is_some());
    wait_for_callback_events(&events, &["complete", "fail"]).await;
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_nameserver_update_shuffles_changed_lists_and_preserves_duplicates() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    assert_eq!(client.get_name_server_address_list(), None);
    client.update_name_server_address_list(Vec::new());
    assert_eq!(client.get_name_server_address_list(), None);
    let addresses = vec![
        "127.0.0.1:1001".into(),
        "127.0.0.1:1001".into(),
        "127.0.0.1:1002".into(),
    ];
    client.update_name_server_address_list(addresses);
    let reordered_same_list = vec![
        "127.0.0.1:1002".into(),
        "127.0.0.1:1001".into(),
        "127.0.0.1:1001".into(),
    ];
    client.update_name_server_address_list(reordered_same_list);
    let initial = client.get_name_server_address_list().unwrap();
    assert_eq!(initial.len(), 3);
    assert_eq!(
        initial
            .iter()
            .filter(|addr| *addr == "127.0.0.1:1001")
            .count(),
        2
    );
    assert_eq!(
        initial
            .iter()
            .filter(|addr| *addr == "127.0.0.1:1002")
            .count(),
        1
    );
    client.update_name_server_address_list(vec![
        "127.0.0.1:1002".into(),
        "127.0.0.1:1001".into(),
        "127.0.0.1:1001".into(),
    ]);
    assert_eq!(client.get_name_server_address_list(), Some(initial));

    client.update_name_server_address_list(vec![
        "127.0.0.1:1002".into(),
        "127.0.0.1:1002".into(),
        "127.0.0.1:1003".into(),
    ]);
    let mut updated = client.get_name_server_address_list().unwrap();
    updated.sort();
    assert_eq!(
        updated,
        vec![
            "127.0.0.1:1002".to_owned(),
            "127.0.0.1:1002".to_owned(),
            "127.0.0.1:1003".to_owned(),
        ]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn p18_nameserver_getter_exposes_live_nullable_list_with_lock_scope() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    assert!(client.with_name_server_address_list(|addresses| addresses.is_none()));

    client.update_name_server_address_list(vec!["127.0.0.1:1001".into(), "127.0.0.1:1002".into()]);
    client.with_name_server_address_list(|mut addresses| {
        addresses.as_mut().unwrap().clear();
    });
    assert_eq!(client.get_name_server_address_list(), Some(Vec::new()));

    // An empty update is ignored, while a caller mutation of the live Java-style list persists.
    client.update_name_server_address_list(Vec::new());
    assert_eq!(client.get_name_server_address_list(), Some(Vec::new()));
    assert!(matches!(
        client
            .invoke_name_server(command(1), Instant::now() + Duration::from_secs(1))
            .await,
        Err(TransportError::InvalidConfiguration(_))
    ));

    client.update_name_server_address_list(vec!["127.0.0.1:1003".into()]);
    assert_eq!(
        client.get_name_server_address_list(),
        Some(vec!["127.0.0.1:1003".into()])
    );
    client.shutdown().await;
}

#[tokio::test]
async fn p3_nameserver_scan_probes_addresses_concurrently_and_tracks_results() {
    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_address = first_listener.local_addr().unwrap();
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_address = second_listener.local_addr().unwrap();
    let accept_first = tokio::spawn(async move { first_listener.accept().await.unwrap() });
    let accept_second = tokio::spawn(async move { second_listener.accept().await.unwrap() });
    let client = RemotingClient::with_config(ClientConfig {
        connect_timeout: 40,
        scan_available_nameserver: true,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.update_name_server_address_list(vec![
        first_address.to_string(),
        second_address.to_string(),
    ]);
    let accepted = tokio::time::timeout(Duration::from_millis(250), async {
        let (first, second) = tokio::join!(accept_first, accept_second);
        (first.unwrap(), second.unwrap())
    })
    .await
    .expect("scan must attempt each NameServer without serial connect-timeout delay");
    tokio::time::timeout(Duration::from_secs(1), async {
        while client.get_available_name_srv_list().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(accepted);
    client.shutdown().await;
}

#[tokio::test]
async fn p18_nameserver_scan_allows_zero_connect_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = RemotingClient::with_config(ClientConfig {
        connect_timeout: 0,
        scan_available_nameserver: true,
        ..ClientConfig::default()
    })
    .unwrap();
    client.update_name_server_address_list(vec![address.to_string()]);
    client.start().unwrap();

    let (stream, _) = tokio::time::timeout(Duration::from_millis(500), listener.accept())
        .await
        .expect("zero connect timeout should allow a NameServer scan to connect")
        .unwrap();
    client.shutdown().await;
    drop(stream);
}

#[tokio::test]
async fn p3_nameserver_scan_setting_is_read_at_start_time() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = RemotingClient::with_config(ClientConfig {
        connect_timeout: 40,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    assert!(!client.get_scan_available_nameserver());
    client.update_name_server_address_list(vec![address.to_string()]);
    client.set_scan_available_nameserver(true);
    assert!(client.get_scan_available_nameserver());

    client.start().unwrap();
    let (stream, _) = tokio::time::timeout(Duration::from_millis(250), listener.accept())
        .await
        .expect("start-time config update should schedule an immediate scan")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client
            .get_available_name_srv_list()
            .contains(&address.to_string())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    client.shutdown().await;
    drop(stream);
}

#[tokio::test]
async fn p18_nameserver_proxy_scan_uses_timeout_cached_at_first_route_use() {
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let proxy_peer = tokio::spawn(async move {
        let (mut client, _) = tokio::time::timeout(Duration::from_secs(2), proxy.accept())
            .await
            .expect("NameServer scan did not connect through the proxy")
            .unwrap();
        let mut greeting = [0; 2];
        client.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting[0], 5);
        let mut methods = vec![0; usize::from(greeting[1])];
        client.read_exact(&mut methods).await.unwrap();
        assert_eq!(methods, [0]);
        let started = Instant::now();
        let mut byte = [0; 1];
        let closed = tokio::time::timeout(Duration::from_secs(2), client.read(&mut byte)).await;
        assert!(
            closed.is_ok(),
            "proxy scan did not apply its connect timeout"
        );
        started.elapsed()
    });
    let client = RemotingClient::with_config(ClientConfig {
        connect_timeout: 40,
        scan_available_nameserver: true,
        client_socket_send_buffer_size: 64 * 1024,
        client_socket_receive_buffer_size: 64 * 1024,
        socks_proxy_config: format!(r#"{{"127.0.0.0/8":{{"addr":"{}"}}}}"#, proxy_address),
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.set_connect_timeout_millis(300);
    client.update_name_server_address_list(vec!["127.0.0.1:19003".into()]);

    let elapsed_result = tokio::time::timeout(Duration::from_secs(3), proxy_peer).await;
    client.shutdown().await;
    let elapsed = elapsed_result.expect("proxy scan did not finish").unwrap();
    assert!(
        elapsed >= Duration::from_millis(200),
        "proxy scan used the start-time timeout instead of the first-use value: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(2));
}

#[tokio::test]
async fn p3_nameserver_update_prunes_availability_on_next_scan() {
    async fn peer() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut byte = [0];
            let _ = stream.read_exact(&mut byte).await;
        });
        (address, task)
    }

    let (first_address, first_peer) = peer().await;
    let (second_address, second_peer) = peer().await;
    let client = RemotingClient::with_config(ClientConfig {
        connect_timeout: 300,
        scan_available_nameserver: true,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.update_name_server_address_list(vec![first_address.clone(), second_address.clone()]);
    tokio::time::timeout(Duration::from_secs(1), async {
        while client.get_available_name_srv_list().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    client.update_name_server_address_list(vec![first_address.clone()]);
    let mut available = client.get_available_name_srv_list();
    available.sort();
    let mut expected_available = vec![first_address, second_address.clone()];
    expected_available.sort();
    assert_eq!(available, expected_available);
    tokio::time::timeout(Duration::from_secs(1), async {
        while client
            .get_available_name_srv_list()
            .contains(&second_address)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await;
    first_peer.await.unwrap();
    second_peer.await.unwrap();
}

#[tokio::test]
async fn p3_nameserver_list_selection_and_availability_tracking() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (close_peer_tx, close_peer_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(17);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = close_peer_rx.await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        connect_timeout: 3_600_000,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.update_name_server_address_list(vec![address.to_string(), "127.0.0.1:1".into()]);
    assert_eq!(client.get_name_server_address_list().unwrap().len(), 2);
    client.update_name_server_address_list(Vec::new());
    assert_eq!(client.get_name_server_address_list().unwrap().len(), 2);
    assert!(client.get_available_name_srv_list().is_empty());
    let response = client
        .invoke_name_server(command(1), Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(response.code, 17);
    assert_eq!(
        client.get_available_name_srv_list(),
        vec![address.to_string()]
    );
    let _ = close_peer_tx.send(());
    peer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.get_available_name_srv_list().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.update_name_server_address_list(vec!["127.0.0.1:1".into()]);
    assert!(client.get_available_name_srv_list().is_empty());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_nameserver_update_keeps_non_chosen_removed_connection_open() {
    async fn peer() -> (
        String,
        tokio::sync::oneshot::Receiver<()>,
        tokio::task::JoinHandle<std::io::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = accepted.send(());
            let request = read_command(&mut stream).await;
            let mut response = command(2);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
            let mut byte = [0];
            stream.read_exact(&mut byte).await.map(|_| ())
        });
        (address, accepted_rx, task)
    }

    let (first_address, first_accepted, mut first_peer) = peer().await;
    let (second_address, second_accepted, mut second_peer) = peer().await;
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.update_name_server_address_list(vec![first_address.clone(), second_address.clone()]);
    let selected_response = client
        .invoke_name_server(command(1), Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(selected_response.is_response());
    let selected = client
        .get_available_name_srv_list()
        .into_iter()
        .next()
        .unwrap();
    let other_address = if selected == first_address {
        &second_address
    } else {
        &first_address
    };
    let other_response = client
        .invoke(
            other_address,
            command(1),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(other_response.is_response());
    first_accepted.await.unwrap();
    second_accepted.await.unwrap();

    client.update_name_server_address_list(vec![selected.clone()]);
    tokio::task::yield_now().await;
    if other_address == &first_address {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut first_peer)
                .await
                .is_err()
        );
    } else {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second_peer)
                .await
                .is_err()
        );
    }

    client.shutdown().await;
    assert!(first_peer.await.unwrap().is_err());
    assert!(second_peer.await.unwrap().is_err());
}

#[tokio::test]
async fn p3_nameserver_selection_fails_over_after_connect_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good = listener.local_addr().unwrap();
    let bad_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad = bad_listener.local_addr().unwrap();
    drop(bad_listener);
    let (keep_peer_open_tx, keep_peer_open_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(18);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = keep_peer_open_rx.await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.update_name_server_address_list(vec![bad.to_string(), good.to_string()]);
    let response = client
        .invoke_name_server(command(2), Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(response.code, 18);
    assert_eq!(client.get_available_name_srv_list(), vec![good.to_string()]);
    let _ = keep_peer_open_tx.send(());
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_nameserver_recovers_selection_after_chosen_channel_closes() {
    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_address = first_listener.local_addr().unwrap();
    let (close_first_tx, close_first_rx) = tokio::sync::oneshot::channel();
    let first_peer = tokio::spawn(async move {
        let (mut stream, _) = first_listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(21);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = close_first_rx.await;
    });
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_address = second_listener.local_addr().unwrap();
    let (close_second_tx, close_second_rx) = tokio::sync::oneshot::channel();
    let second_peer = tokio::spawn(async move {
        let (mut stream, _) = second_listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(22);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = close_second_rx.await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.update_name_server_address_list(vec![
        first_address.to_string(),
        second_address.to_string(),
    ]);
    let first_response = client
        .invoke_name_server(command(1), Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let (closed_address, expected_recovery, close_selected, close_backup) =
        match first_response.code {
            21 => (
                first_address.to_string(),
                22,
                Box::new(move || {
                    let _ = close_first_tx.send(());
                }) as Box<dyn FnOnce()>,
                Box::new(move || {
                    let _ = close_second_tx.send(());
                }) as Box<dyn FnOnce()>,
            ),
            22 => (
                second_address.to_string(),
                21,
                Box::new(move || {
                    let _ = close_second_tx.send(());
                }) as Box<dyn FnOnce()>,
                Box::new(move || {
                    let _ = close_first_tx.send(());
                }) as Box<dyn FnOnce()>,
            ),
            code => panic!("unexpected NameServer response {code}"),
        };
    assert_eq!(client.get_available_name_srv_list(), vec![closed_address]);
    close_selected();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.get_available_name_srv_list().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .invoke_name_server(command(2), Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .code,
        expected_recovery
    );
    close_backup();
    first_peer.await.unwrap();
    second_peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_channel_management_matches_java_unknown_writable_and_explicit_close_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0; 1];
        stream.read(&mut byte).await.unwrap()
    });
    let client = RemotingClient::with_config(ClientConfig {
        client_socket_send_buffer_size: 32 * 1024,
        client_socket_receive_buffer_size: 32 * 1024,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let address_string = address.to_string();
    assert!(client.is_channel_writable(&address_string));
    assert!(
        client
            .is_address_reachable(&address_string, Instant::now() + Duration::from_secs(2),)
            .await
    );
    assert!(client.is_channel_writable(&address_string));
    client.close_channels(std::slice::from_ref(&address_string));
    assert_eq!(peer.await.unwrap(), 0);
    assert!(client.is_channel_writable(&address_string));
    assert!(
        !client
            .is_address_reachable(&address_string, Instant::now() + Duration::from_millis(20),)
            .await
    );
    client.shutdown().await;
}

#[tokio::test]
async fn p3_close_channels_interrupts_pending_pull_requests_only() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let pull = read_command(&mut stream).await;
        received_tx.send(pull.code).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let pull_client = client.clone();
    let pull_address = address.clone();
    let pull = tokio::spawn(async move {
        pull_client
            .invoke(
                &pull_address,
                command(request::PULL_MESSAGE),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), received_rx)
            .await
            .unwrap()
            .unwrap(),
        request::PULL_MESSAGE
    );
    client.close_channels(std::slice::from_ref(&address));
    assert!(matches!(
        pull.await.unwrap(),
        Err(TransportError::Interrupted)
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_v01_parallel_responses_and_opaque_lifecycle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        for _ in 0..2 {
            received.push(read_command(&mut stream).await);
        }
        for request in received.into_iter().rev() {
            let mut response = command(request::GET_ROUTEINFO_BY_TOPIC);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let address = addr.to_string();
    let (first, second) = tokio::join!(
        client.invoke(&address, command(request::GET_ROUTEINFO_BY_TOPIC), deadline),
        client.invoke(&address, command(request::GET_ROUTEINFO_BY_TOPIC), deadline),
    );
    assert!(first.unwrap().is_response());
    assert!(second.unwrap().is_response());
    peer.await.unwrap();
    client.shutdown().await;
    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p3_v02_single_flight_shares_connection_and_stale_close_keeps_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_peer = accepted.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        accepted_peer.fetch_add(1, Ordering::SeqCst);
        for _ in 0..3 {
            let request = read_command(&mut stream).await;
            assert!(request.is_oneway());
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let address = addr.to_string();
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let mut calls = Vec::new();
    for code in 1..=3 {
        let client = client.clone();
        let address = address.clone();
        let barrier = barrier.clone();
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .invoke_oneway(&address, command(code), deadline)
                .unwrap();
        }));
    }
    barrier.wait().await;
    for call in calls {
        call.await.unwrap();
    }
    peer.await.unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_v03_inbound_dispatch_and_reordered_parallel_responses() {
    #[derive(Default)]
    struct RecordingHistogram(Mutex<Vec<(u64, Attributes)>>);
    impl RpcLatencyHistogram for RecordingHistogram {
        fn record(&self, elapsed_milliseconds: u64, attributes: &Attributes) {
            self.0
                .lock()
                .unwrap()
                .push((elapsed_milliseconds, attributes.clone()));
        }
    }

    struct MarkLongPollingRequest;
    impl RpcHook for MarkLongPollingRequest {
        fn before_request_mut_result(
            &self,
            _: &str,
            request: &mut RemotingCommand,
        ) -> Result<(), RpcHookError> {
            if request.code == 777_777 {
                request.set_suspended(true);
            }
            Ok(())
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outbound = read_command(&mut stream).await;
        let mut inbound = command(777_777);
        inbound.opaque = 41;
        write_command(&mut stream, &inbound).await;
        let processor_response = read_command(&mut stream).await;
        let mut response = command(55);
        response.opaque = outbound.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let mut unsupported = command(888_888);
        unsupported.opaque = 42;
        write_command(&mut stream, &unsupported).await;
        let unsupported_response = read_command(&mut stream).await;
        let mut oneway = command(999_999);
        oneway.opaque = 43;
        oneway.mark_oneway();
        write_command(&mut stream, &oneway).await;
        let mut unexpected = [0; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stream.read(&mut unexpected))
                .await
                .is_err()
        );
        (processor_response, unsupported_response)
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let histogram = Arc::new(RecordingHistogram::default());
    let manager = Arc::new(RemotingMetricsManager::new());
    manager.init_metrics(histogram.clone(), None);
    client.set_remoting_metrics_manager(Some(manager.clone()));
    client.add_rpc_hook(Arc::new(MarkLongPollingRequest));
    assert!(Arc::ptr_eq(
        &client.get_remoting_metrics_manager().unwrap(),
        &manager
    ));
    client.start().unwrap();
    client.register_processor(
        777_777,
        Arc::new(|_request: RemotingCommand| Ok(Some(command(99)))),
    );
    let response = client
        .invoke(
            &addr.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(response.is_response());
    let (processor_response, unsupported_response) = peer.await.unwrap();
    assert_eq!(processor_response.code, 99);
    assert_eq!(processor_response.opaque, 41);
    assert!(processor_response.is_response());
    assert_eq!(
        unsupported_response.code,
        response::REQUEST_CODE_NOT_SUPPORTED
    );
    assert_eq!(
        unsupported_response.remark.as_deref(),
        Some(" request type 888888 not supported")
    );
    assert_eq!(unsupported_response.opaque, 42);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if histogram.0.lock().unwrap().len() == 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all inbound response metrics should be recorded");
    {
        let observations = histogram.0.lock().unwrap();
        assert_eq!(observations.len(), 3);
        let success = observations
            .iter()
            .find(|(_, labels)| {
                labels[LABEL_REQUEST_CODE] == AttributeValue::String("777777".into())
            })
            .unwrap();
        assert_eq!(
            success.1[LABEL_RESPONSE_CODE],
            AttributeValue::String("99".into())
        );
        assert_eq!(
            success.1[LABEL_IS_LONG_POLLING],
            AttributeValue::Boolean(true)
        );
        assert_eq!(
            success.1[LABEL_RESULT],
            AttributeValue::String(RESULT_SUCCESS.into())
        );
        let oneway = observations
            .iter()
            .find(|(_, labels)| {
                labels[LABEL_REQUEST_CODE] == AttributeValue::String("999999".into())
            })
            .unwrap();
        assert_eq!(
            oneway.1[LABEL_RESULT],
            AttributeValue::String(RESULT_ONEWAY.into())
        );
        assert!(!oneway.1.as_map().contains_key("protocol_type"));
        assert_eq!(manager.get_metrics_view()[0].name, HISTOGRAM_RPC_LATENCY);
    }
    client.shutdown().await;
}

#[tokio::test]
async fn p3_inbound_responses_survive_unbounded_outbound_backlog() {
    const INBOUND_REQUESTS: usize = 140;
    const RESPONSE_BODY_SIZE: usize = 64 * 1024;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (saturated_tx, saturated_rx) = tokio::sync::oneshot::channel();
    let (drain_tx, drain_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outbound = read_command(&mut stream).await;
        for opaque in 0..INBOUND_REQUESTS {
            let mut request = command(900_010);
            request.opaque = opaque as i32;
            write_command(&mut stream, &request).await;
        }
        saturated_tx.send(()).unwrap();
        let _ = drain_rx.await;
        let mut response = command(outbound.code);
        response.opaque = outbound.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let mut responses = Vec::with_capacity(INBOUND_REQUESTS);
        for _ in 0..INBOUND_REQUESTS {
            responses.push(read_command(&mut stream).await);
        }
        responses
    });
    let client = RemotingClient::with_config(ClientConfig {
        client_socket_send_buffer_size: 1024,
        write_buffer_high_water_mark: 64 * 1024,
        write_buffer_low_water_mark: 32 * 1024,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.register_processor(
        900_010,
        Arc::new(|_: RemotingCommand| {
            Ok(Some(RemotingCommand {
                body: Some(vec![0x5a; RESPONSE_BODY_SIZE]),
                ..command(910)
            }))
        }),
    );
    let invoke = tokio::spawn({
        let client = client.clone();
        let address = address.to_string();
        async move {
            client
                .invoke(
                    &address,
                    command(911),
                    Instant::now() + Duration::from_secs(10),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), saturated_rx)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.is_channel_writable(&address.to_string()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued outbound response bytes should exceed the configured watermark");

    let _ = drain_tx.send(());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), invoke)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .code,
        911
    );
    let responses = tokio::time::timeout(Duration::from_secs(10), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(responses.len(), INBOUND_REQUESTS);
    assert!(responses.iter().all(|response| {
        response.code == 910
            && response.is_response()
            && response.body.as_deref() == Some(&vec![0x5a; RESPONSE_BODY_SIZE])
    }));
    assert!(client.is_channel_writable(&address.to_string()));
    client.shutdown().await;
}

#[tokio::test]
async fn p3_disabled_worker_group_dispatches_inbound_request_on_connection_path() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outbound = read_command(&mut stream).await;
        let mut response = command(outbound.code);
        response.opaque = outbound.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let mut request = command(900_005);
        request.opaque = 45;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::with_config(ClientConfig {
        disable_netty_worker_group: true,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.register_processor(
        900_005,
        Arc::new(|_: RemotingCommand| Ok(Some(command(905)))),
    );
    client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    let response = peer.await.unwrap();
    assert_eq!(response.code, 905);
    assert_eq!(response.opaque, 45);
    client.shutdown().await;
}

#[tokio::test]
async fn p18_worker_group_setting_can_change_for_channels_after_start() {
    async fn peer(listener: TcpListener) -> tokio::task::JoinHandle<RemotingCommand> {
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let outbound = read_command(&mut stream).await;
            let mut response = command(outbound.code);
            response.opaque = outbound.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
            let mut request = command(900_005);
            request.opaque = 45;
            write_command(&mut stream, &request).await;
            read_command(&mut stream).await
        })
    }

    let worker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_address = worker_listener.local_addr().unwrap();
    let worker_peer = peer(worker_listener).await;
    let direct_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let direct_address = direct_listener.local_addr().unwrap();
    let direct_peer = peer(direct_listener).await;
    let client = RemotingClient::with_config(ClientConfig {
        disable_netty_worker_group: true,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.register_processor(
        900_005,
        Arc::new(|_: RemotingCommand| Ok(Some(command(905)))),
    );

    client.set_disable_netty_worker_group(false);
    assert!(!client.get_disable_netty_worker_group());
    client
        .invoke(
            &worker_address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(worker_peer.await.unwrap().code, 905);

    client.set_disable_netty_worker_group(true);
    assert!(client.get_disable_netty_worker_group());
    client
        .invoke(
            &direct_address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(direct_peer.await.unwrap().code, 905);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_inbound_oneway_runs_processor_but_never_writes_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(900_003);
        request.mark_oneway();
        write_command(&mut stream, &request).await;
        let mut unsupported_oneway = command(900_004);
        unsupported_oneway.mark_oneway();
        write_command(&mut stream, &unsupported_oneway).await;
        let mut unexpected = [0; 1];
        let result =
            tokio::time::timeout(Duration::from_millis(100), stream.read(&mut unexpected)).await;
        result.is_err()
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.register_processor(
        900_003,
        Arc::new(move |request: RemotingCommand| {
            let _ = processed_tx.send(request.is_oneway());
            Ok(Some(command(903)))
        }),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    assert!(processed_rx.recv().await.unwrap());
    assert!(peer.await.unwrap());
    client.shutdown().await;
}

#[tokio::test]
async fn p18_inbound_oneway_processor_failure_never_writes_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(900_006);
        request.mark_oneway();
        write_command(&mut stream, &request).await;

        let mut unexpected = [0; 1];
        match tokio::time::timeout(Duration::from_millis(250), stream.read(&mut unexpected)).await {
            Err(_) | Ok(Ok(0)) => true,
            Ok(Ok(_)) | Ok(Err(_)) => false,
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    client.register_processor(
        900_006,
        Arc::new(move |_: RemotingCommand| {
            let _ = processed_tx.send(());
            Err("processor failed".into())
        }),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    processed_rx.recv().await.unwrap();
    assert!(peer.await.unwrap());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_default_processor_handles_unknown_codes_but_exact_registration_wins() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut unknown = command(900_001);
        unknown.opaque = 41;
        write_command(&mut stream, &unknown).await;
        let mut exact = command(900_002);
        exact.opaque = 42;
        write_command(&mut stream, &exact).await;
        let first = read_command(&mut stream).await;
        let second = read_command(&mut stream).await;
        (first, second)
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.with_processor_table(|processors| {
        processors.insert(
            900_002,
            rocketmq_classic_remoting::client::ProcessorRegistration::new(
                Arc::new(|request: RemotingCommand| {
                    assert!(
                        request
                            .get_process_timer()
                            .is_some_and(|timer| timer.is_running())
                    );
                    Ok(Some(command(902)))
                }),
                None,
            ),
        );
    });
    client.register_default_processor(Some(Arc::new(|request: RemotingCommand| {
        assert!(
            request
                .get_process_timer()
                .is_some_and(|timer| timer.is_running())
        );
        Ok(Some(command(901)))
    })));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let (first, second) = peer.await.unwrap();
    let responses = [first, second]
        .into_iter()
        .map(|response| (response.opaque, response.code))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(responses.get(&41), Some(&901));
    assert_eq!(responses.get(&42), Some(&902));
    client.register_default_processor(None);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_u01_raw_route_deadline_and_shutdown() {
    let unstarted = RemotingClient::new(TransportMode::Plain);
    unstarted.shutdown().await;
    assert_eq!(unstarted.start(), Err(TransportError::Shutdown));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut reply = command(request::GET_ROUTEINFO_BY_TOPIC);
        reply.opaque = request.opaque;
        reply.mark_response();
        write_command(&mut stream, &reply).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let response = client
        .invoke(
            &addr.to_string(),
            command(request::GET_ROUTEINFO_BY_TOPIC),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(response.is_response());
    peer.await.unwrap();
    let expired = client
        .invoke(&addr.to_string(), command(1), Instant::now())
        .await;
    assert!(matches!(expired, Err(error) if error.is_timeout()));
    client.shutdown().await;
    assert!(matches!(
        client
            .invoke(
                &addr.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(1)
            )
            .await,
        Err(TransportError::Shutdown)
    ));
}

#[tokio::test]
async fn p3_u01_inflight_invoke_accepts_late_response_before_scanner_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let timed_out = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let mut response = command(91);
        response.opaque = timed_out.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let next = read_command(&mut stream).await;
        let mut response = command(92);
        response.opaque = next.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let result = client
        .invoke(
            &address.to_string(),
            command(request::GET_ROUTEINFO_BY_TOPIC),
            Instant::now() + Duration::from_millis(20),
        )
        .await;
    assert_eq!(result.unwrap().code, 91);
    assert_eq!(
        client
            .invoke(
                &address.to_string(),
                command(request::GET_ROUTEINFO_BY_TOPIC),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap()
            .code,
        92
    );
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_sync_timeout_closes_socket_when_java_heuristic_requires_it() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        let mut byte = [0; 1];
        tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    assert!(matches!(
        client
            .invoke_sync(
                &address.to_string(),
                command(request::GET_ROUTEINFO_BY_TOPIC),
                Duration::from_millis(200),
            )
            .await,
        Err(error) if error.is_timeout()
    ));
    assert_eq!(peer.await.unwrap(), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_sync_timeout_reads_close_socket_setting_after_client_start() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        let mut byte = [0; 1];
        tokio::time::timeout(Duration::from_millis(500), stream.read(&mut byte))
            .await
            .is_err()
    });
    let client = RemotingClient::with_config(ClientConfig {
        close_socket_if_timeout: true,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.set_client_close_socket_if_timeout(false);
    assert!(!client.get_client_close_socket_if_timeout());
    assert!(matches!(
        client
            .invoke_sync(
                &address.to_string(),
                command(request::GET_ROUTEINFO_BY_TOPIC),
                Duration::from_millis(200),
            )
            .await,
        Err(error) if error.is_timeout()
    ));
    assert!(peer.await.unwrap());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_sync_admission_failure_maps_to_send_request_and_closes_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        let _ = received_tx.send(());
        let mut byte = [0; 1];
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        close_socket_if_timeout: true,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let first_client = client.clone();
    let first_address = address.to_string();
    let first = tokio::spawn(async move {
        first_client
            .invoke_sync(&first_address, command(1), Duration::from_secs(2))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), received_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        client
            .invoke_sync(&address.to_string(), command(2), Duration::from_millis(300))
            .await,
        Err(TransportError::SendRequest { .. })
    ));
    assert_eq!(peer.await.unwrap(), 0);
    assert!(matches!(
        first.await.unwrap(),
        Err(TransportError::SendRequest { .. })
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn p3_sync_admission_shutdown_keeps_shutdown_category() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        let _ = received_tx.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let first_client = client.clone();
    let first_address = address.to_string();
    let first = tokio::spawn(async move {
        first_client
            .invoke_sync(&first_address, command(1), Duration::from_secs(2))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), received_rx)
        .await
        .unwrap()
        .unwrap();
    let second_client = client.clone();
    let second_address = address.to_string();
    let second = tokio::spawn(async move {
        second_client
            .invoke_sync(&second_address, command(2), Duration::from_secs(2))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    client.shutdown().await;
    assert!(matches!(
        first.await.unwrap(),
        Err(TransportError::Shutdown)
    ));
    assert!(matches!(
        second.await.unwrap(),
        Err(TransportError::Shutdown)
    ));
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_oneway_returns_after_scheduling_before_peer_receives_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client
        .invoke_oneway(
            &address.to_string(),
            command(13),
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), accepted_rx)
        .await
        .unwrap()
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap();
    assert!(received.is_oneway());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_oneway_returns_after_scheduling_connection_acquisition() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client
        .invoke_oneway(
            &address.to_string(),
            command(14),
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_oneway_admission_timeout_is_not_returned_after_call_boundary() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        tokio::time::timeout(Duration::from_millis(200), read_command(&mut stream)).await
    });
    let client = RemotingClient::with_config(ClientConfig {
        oneway_semaphore_value: 0,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client
        .invoke_oneway(
            &address.to_string(),
            command(14),
            Instant::now() + Duration::from_millis(50),
        )
        .unwrap();
    assert!(
        peer.await.unwrap().is_err(),
        "an expired admission must not write a frame"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn p3_oneway_write_completion_is_not_returned_after_call_boundary() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        client_socket_send_buffer_size: 1024,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client
        .invoke_oneway(
            &address.to_string(),
            RemotingCommand {
                body: Some(vec![7; 8 * 1024 * 1024]),
                ..command(15)
            },
            Instant::now() + Duration::from_millis(250),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), accepted_rx)
        .await
        .unwrap()
        .unwrap();
    peer.abort();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_oneway_reports_invalid_address_synchronously() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let result = client.invoke_oneway(
        "invalid-address",
        command(14),
        Instant::now() + Duration::from_secs(1),
    );
    assert!(matches!(result, Err(TransportError::Connect(_))));
}

#[tokio::test]
async fn p3_oneway_starts_connection_acquisition_at_call_time() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client
        .invoke_oneway(
            &address.to_string(),
            command(14),
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), accepted_rx)
        .await
        .expect("connection acquisition should be scheduled before method return")
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap();
    assert!(received.is_oneway());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_outbound_rpc_hook_runs_before_oneway_admission_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (drain_tx, drain_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = drain_rx.await;
        let first = read_command(&mut stream).await;
        let second = read_command(&mut stream).await;
        (first.code, second.code)
    });
    let client = RemotingClient::with_config(ClientConfig {
        oneway_semaphore_value: 1,
        client_socket_send_buffer_size: 1024,
        max_idle_time: 0,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    client.add_rpc_hook(Arc::new(BeforeCounter(hook_calls.clone())));
    let hook_flags = Arc::new(std::sync::Mutex::new(Vec::new()));
    client.add_rpc_hook(Arc::new(OnewayHookFlagRecorder(hook_flags.clone())));
    let first = RemotingCommand {
        body: Some(vec![7; 8 * 1024 * 1024]),
        ..command(101)
    };
    client
        .invoke_oneway(
            &address.to_string(),
            first,
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while hook_calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    client
        .invoke_oneway(
            &address.to_string(),
            command(102),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while hook_calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("oneway hook must run before waiting for semaphore admission");
    let _ = drain_tx.send(());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(8), peer)
            .await
            .unwrap()
            .unwrap(),
        (101, 102)
    );
    assert_eq!(*hook_flags.lock().unwrap(), vec![false, false]);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_u01_tls_verifies_ca_and_matches_java_hostname_behavior() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = certificate.cert.pem();
    let mut ca = TlsConfig::default();
    ca.ca_pem = Some(cert_pem.as_bytes().to_vec());
    ca.test_mode = false;
    ca.auth_server = true;
    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.key_pair.serialize_der(),
                ),
            ),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let peer = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(mut stream) = acceptor.accept(socket).await else {
                continue;
            };
            if let Ok(request) =
                tokio::time::timeout(Duration::from_secs(1), read_command(&mut stream)).await
            {
                let mut reply = command(request::GET_ROUTEINFO_BY_TOPIC);
                reply.opaque = request.opaque;
                reply.mark_response();
                let _ = write_command(&mut stream, &reply).await;
            }
        }
    });
    let client = RemotingClient::new(TransportMode::Tls(ca.clone()));
    client.start().unwrap();
    let result = client
        .invoke(
            &format!("localhost:{port}"),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    assert!(result.unwrap().is_response());
    client.shutdown().await;

    let bad_name = RemotingClient::new(TransportMode::Tls(ca));

    bad_name.start().unwrap();
    let result = bad_name
        .invoke(
            &format!("127.0.0.1:{port}"),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    assert!(result.unwrap().is_response());
    bad_name.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_tls_matches_java_trust_all_modes() {
    let certificate =
        rcgen::generate_simple_self_signed(vec!["certificate-name-does-not-match".into()]).unwrap();
    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.key_pair.serialize_der(),
                ),
            ),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let peer = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(mut stream) = acceptor.accept(socket).await else {
                continue;
            };
            if let Ok(request) =
                tokio::time::timeout(Duration::from_secs(1), read_command(&mut stream)).await
            {
                let mut reply = command(request::GET_ROUTEINFO_BY_TOPIC);
                reply.opaque = request.opaque;
                reply.mark_response();
                let _ = write_command(&mut stream, &reply).await;
            }
        }
    });
    let mut test_mode_config = TlsConfig::default();
    test_mode_config.test_mode = true;
    test_mode_config.auth_server = true;
    let mut disabled_auth_config = TlsConfig::default();
    disabled_auth_config.test_mode = false;
    disabled_auth_config.auth_server = false;
    for config in [test_mode_config, disabled_auth_config] {
        let client = RemotingClient::new(TransportMode::Tls(config));
        client.start().unwrap();
        let response = client
            .invoke(
                &format!("localhost:{port}"),
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert!(response.is_response());
        client.shutdown().await;
    }
    peer.await.unwrap();
}

#[tokio::test]
async fn p18_use_tls_setting_is_live_for_new_channels_with_a_captured_context() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.key_pair.serialize_der(),
                ),
            ),
        )
        .unwrap();
    let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls_port = tls_listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let tls_peer = tokio::spawn(async move {
        let (socket, _) = tls_listener.accept().await.unwrap();
        let mut stream = acceptor.accept(socket).await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(request.code);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });

    let plain_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plain_address = plain_listener.local_addr().unwrap();
    let plain_peer = tokio::spawn(async move {
        let (mut stream, _) = plain_listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(request.code);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });

    let client = RemotingClient::with_config(ClientConfig {
        transport: TransportMode::Tls(TlsConfig::default()),
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    assert!(client.get_use_tls());
    client.start().unwrap();
    client.set_use_tls(false);
    assert!(!client.get_use_tls());
    let plain_response = client
        .invoke(
            &plain_address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(plain_response.is_response());

    client.set_use_tls(true);
    assert!(client.get_use_tls());
    let tls_response = client
        .invoke(
            &format!("localhost:{tls_port}"),
            command(2),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(tls_response.is_response());
    plain_peer.await.unwrap();
    tls_peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p18_enabling_use_tls_without_a_captured_context_stays_plain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(request.code);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });

    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    assert!(!client.get_use_tls());
    client.start().unwrap();
    client.set_use_tls(true);
    assert!(client.get_use_tls());
    let response = client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(response.is_response());
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_nameserver_channel_interval_has_no_implicit_scan_behavior() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        channel_not_active_interval: 20,
        max_idle_time: 0,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.update_name_server_address_list(vec![address.to_string()]);
    assert_eq!(
        client
            .invoke(
                &address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap()
            .code,
        42
    );
    client.shutdown().await;
    peer.await.unwrap();
}

struct IdleListener(tokio::sync::mpsc::UnboundedSender<String>);

impl rocketmq_classic_remoting::client::ChannelEventListener for IdleListener {
    fn on_idle(&self, remote_addr: &str, _: &ChannelHandle) {
        let _ = self.0.send(remote_addr.to_owned());
    }
}

#[tokio::test]
async fn p3_max_idle_time_observes_partial_read_activity_like_all_idle_timer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = stream.write_all(&[0, 0]).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig {
            max_idle_time: 1,
            scan_available_nameserver: false,
            ..ClientConfig::default()
        },
        Some(Arc::new(IdleListener(event_tx))),
    )
    .unwrap();
    client.start().unwrap();
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(1)
            )
            .await
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(750), event_rx.recv())
            .await
            .is_err()
    );
    let event_address = tokio::time::timeout(Duration::from_millis(800), event_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event_address, address.to_string());
    client.shutdown().await;
    peer.abort();
}

#[tokio::test]
async fn p18_idle_timeout_setting_applies_to_new_channels_only() {
    let idle_disabled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let idle_disabled_address = idle_disabled_listener.local_addr().unwrap();
    let idle_enabled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let idle_enabled_address = idle_enabled_listener.local_addr().unwrap();
    let client = RemotingClient::with_config(ClientConfig {
        max_idle_time: 0,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    assert_eq!(client.get_client_channel_max_idle_time_seconds(), 0);
    assert!(
        client
            .is_address_reachable(
                &idle_disabled_address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let (mut idle_disabled, _) =
        tokio::time::timeout(Duration::from_secs(1), idle_disabled_listener.accept())
            .await
            .unwrap()
            .unwrap();

    client.set_client_channel_max_idle_time_seconds(1);
    assert_eq!(client.get_client_channel_max_idle_time_seconds(), 1);
    assert!(
        client
            .is_address_reachable(
                &idle_enabled_address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let (mut idle_enabled, _) =
        tokio::time::timeout(Duration::from_secs(1), idle_enabled_listener.accept())
            .await
            .unwrap()
            .unwrap();

    let mut byte = [0; 1];
    let new_channel_closed =
        tokio::time::timeout(Duration::from_secs(2), idle_enabled.read(&mut byte)).await;
    assert!(
        new_channel_closed.is_ok(),
        "new channel did not use the live idle timeout"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), idle_disabled.read(&mut byte))
            .await
            .is_err(),
        "changing the setting altered an existing channel's idle handler"
    );
    client.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p16_slow_channel_observer_does_not_block_transport_and_drains_before_shutdown_returns() {
    struct Listener {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        live: Arc<AtomicUsize>,
    }
    struct Live(Arc<AtomicUsize>);
    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl ChannelEventListener for Listener {
        fn on_active_async<'a>(
            &'a self,
            _: &'a str,
            _: &'a ChannelHandle,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.live.fetch_add(1, Ordering::SeqCst);
                let _live = Live(self.live.clone());
                self.entered.notify_one();
                self.release.notified().await;
            })
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let observer = Arc::new(Listener {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        live: Arc::new(AtomicUsize::new(0)),
    });
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig {
            scan_available_nameserver: false,
            ..ClientConfig::default()
        },
        Some(observer.clone()),
    )
    .unwrap();
    client.start().unwrap();

    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(2),
            client.invoke(
                &address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(2),
            ),
        )
        .await
        .expect("transport request must not wait for the observer")
        .unwrap()
        .code,
        42
    );
    tokio::time::timeout(Duration::from_secs(1), observer.entered.notified())
        .await
        .expect("the observer should receive the active event");
    assert_eq!(observer.live.load(Ordering::SeqCst), 1);
    observer.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), client.shutdown())
        .await
        .expect("shutdown should wait for the active observer");
    tokio::time::timeout(Duration::from_secs(1), async {
        while observer.live.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the active observer should finish before shutdown returns");
    assert_eq!(observer.live.load(Ordering::SeqCst), 0);
    peer.await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn p16_slow_sync_channel_observer_does_not_block_transport_runtime() {
    struct Listener {
        entered: Arc<std::sync::atomic::AtomicBool>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl ChannelEventListener for Listener {
        fn on_connect(&self, _: &str, _: &ChannelHandle) {
            self.entered.store(true, Ordering::Release);
            let _ = self
                .release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_millis(1500));
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let peer_entered = entered.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let Some(request) = try_read_command(&mut stream).await else {
            return;
        };
        while !peer_entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });

    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let observer = Arc::new(Listener {
        entered,
        release: std::sync::Mutex::new(release_rx),
    });
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig {
            scan_available_nameserver: false,
            ..ClientConfig::default()
        },
        Some(observer),
    )
    .unwrap();
    client.start().unwrap();

    let result = tokio::time::timeout(
        Duration::from_millis(750),
        client.invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        ),
    )
    .await;
    let _ = release_tx.send(());
    client.shutdown().await;
    let _ = peer.await;

    assert_eq!(
        result
            .expect("a slow synchronous observer must not stall the Tokio runtime")
            .unwrap()
            .code,
        42
    );
}

#[tokio::test]
async fn p18_channel_events_preserve_channel_identity_and_redact_peer() {
    struct Listener(Arc<std::sync::Mutex<Vec<ChannelEvent>>>);
    impl ChannelEventListener for Listener {
        fn on_channel_event(&self, event: &ChannelEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig::default(),
        Some(Arc::new(Listener(events.clone()))),
    )
    .unwrap();
    client.start().unwrap();
    assert_eq!(
        client
            .invoke(
                &address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .code,
        42
    );
    peer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.event_type == NettyEventType::Active)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active event was not delivered");

    {
        let events = events.lock().unwrap();
        let connect = events
            .iter()
            .find(|event| event.event_type == NettyEventType::Connect)
            .unwrap();
        let active = events
            .iter()
            .find(|event| event.event_type == NettyEventType::Active)
            .unwrap();
        assert_eq!(connect.remote_addr, address.to_string());
        assert_eq!(active.remote_addr, address.to_string());
        assert_eq!(connect.channel, active.channel);
        assert_eq!(active.channel.remote_addr(), address.to_string());
    }
    client.shutdown().await;
}

#[tokio::test]
async fn p18_channel_event_callbacks_retain_channel_identity() {
    struct Listener(Arc<std::sync::Mutex<Vec<(NettyEventType, ChannelHandle)>>>);
    impl ChannelEventListener for Listener {
        fn on_connect(&self, _: &str, channel: &ChannelHandle) {
            self.0
                .lock()
                .unwrap()
                .push((NettyEventType::Connect, channel.clone()));
        }

        fn on_active(&self, _: &str, channel: &ChannelHandle) {
            self.0
                .lock()
                .unwrap()
                .push((NettyEventType::Active, channel.clone()));
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let callbacks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig::default(),
        Some(Arc::new(Listener(callbacks.clone()))),
    )
    .unwrap();
    client.start().unwrap();
    assert_eq!(
        client
            .invoke(
                &address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .code,
        42
    );
    peer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if callbacks
                .lock()
                .unwrap()
                .iter()
                .any(|(event_type, _)| *event_type == NettyEventType::Active)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active callback was not delivered");

    {
        let callbacks = callbacks.lock().unwrap();
        let connect = &callbacks
            .iter()
            .find(|(event_type, _)| *event_type == NettyEventType::Connect)
            .expect("connect callback was not delivered")
            .1;
        let active = &callbacks
            .iter()
            .find(|(event_type, _)| *event_type == NettyEventType::Active)
            .expect("active callback was not delivered")
            .1;
        assert_eq!(connect, active);
        assert_eq!(connect.remote_addr(), address.to_string());
    }
    client.shutdown().await;
}

#[tokio::test]
async fn p18_channel_handle_writes_commands_and_closes_its_connection() {
    struct ActiveChannel(tokio::sync::mpsc::UnboundedSender<ChannelHandle>);
    impl ChannelEventListener for ActiveChannel {
        fn on_active(&self, _: &str, channel: &ChannelHandle) {
            let _ = self.0.send(channel.clone());
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (written_tx, written_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let deadline_write = read_command(&mut stream).await;
        let no_deadline_write = read_command(&mut stream).await;
        let _ = written_tx.send((deadline_write.code, no_deadline_write.code));
        let mut byte = [0; 1];
        let _ = stream.read(&mut byte).await;
    });
    let (channel_tx, mut channel_rx) = tokio::sync::mpsc::unbounded_channel();
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig::default(),
        Some(Arc::new(ActiveChannel(channel_tx))),
    )
    .unwrap();
    client.start().unwrap();
    assert_eq!(
        client
            .invoke(
                &address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .code,
        42
    );
    let channel = tokio::time::timeout(Duration::from_secs(1), channel_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(channel.remote_addr(), address.to_string());
    assert!(channel.is_open());
    assert!(channel.is_active());
    assert!(channel.is_writable());
    channel
        .write_and_flush(&command(77), Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    channel
        .write_and_flush_without_deadline(&command(78))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(written_rx.await.unwrap(), (77, 78));
    assert!(channel.close());
    assert!(!channel.is_open());
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_controls_validate_defaults_hooks_callback_and_channel_events() {
    let defaults = ClientConfig::default();
    assert!(matches!(defaults.transport, TransportMode::Plain));
    assert_eq!(defaults.connect_timeout, 3000);
    assert!(
        config_is_accepted(ClientConfig {
            connect_timeout: -1,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert_eq!(defaults.async_semaphore_value, 65535);
    assert_eq!(defaults.oneway_semaphore_value, 65535);
    assert_eq!(defaults.worker_threads, 4);
    for worker_threads in [i32::MIN, -1, 0, 1, i32::MAX] {
        assert!(
            config_is_accepted(ClientConfig {
                worker_threads,
                ..defaults.clone()
            })
            .is_ok()
        );
    }
    assert!(!defaults.disable_callback_executor);
    assert_eq!(
        defaults.callback_executor_threads,
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get().min(i32::MAX as usize) as i32)
            .unwrap_or(1)
    );
    assert_eq!(defaults.channel_not_active_interval, 60_000);
    assert!(
        config_is_accepted(ClientConfig {
            channel_not_active_interval: -1,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(
        config_is_accepted(ClientConfig {
            max_idle_time: 0,
            ..defaults.clone()
        })
        .is_ok()
    );
    for zero_capacity in [
        ClientConfig {
            connect_timeout: 0,
            ..defaults.clone()
        },
        ClientConfig {
            async_semaphore_value: 0,
            ..defaults.clone()
        },
        ClientConfig {
            oneway_semaphore_value: 0,
            ..defaults.clone()
        },
    ] {
        assert!(config_is_accepted(zero_capacity).is_ok());
    }
    for semaphore_value in [i32::MIN, -1, 0, i32::MAX] {
        assert!(
            config_is_accepted(ClientConfig {
                async_semaphore_value: semaphore_value,
                ..defaults.clone()
            })
            .is_ok()
        );
        assert!(
            config_is_accepted(ClientConfig {
                oneway_semaphore_value: semaphore_value,
                ..defaults.clone()
            })
            .is_ok()
        );
    }
    assert_eq!(defaults.client_socket_send_buffer_size, 0);
    assert_eq!(defaults.client_socket_receive_buffer_size, 0);
    assert_eq!(defaults.write_buffer_high_water_mark, 0);
    assert_eq!(defaults.write_buffer_low_water_mark, 0);
    assert!(!defaults.pooled_byte_buffer_allocator_enabled);
    assert_eq!(defaults.max_idle_time, 120);
    assert!(
        config_is_accepted(ClientConfig {
            max_idle_time: -1,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(defaults.close_socket_if_timeout);
    assert!(defaults.scan_available_nameserver);
    assert!(defaults.enable_reconnect_for_go_away);
    assert!(!defaults.disable_netty_worker_group);
    assert_eq!(defaults.max_reconnect_interval_time_seconds, 60);
    for value in [i64::MIN, -1, 0, i64::MAX] {
        assert!(
            config_is_accepted(ClientConfig {
                max_reconnect_interval_time_seconds: value,
                ..defaults.clone()
            })
            .is_ok()
        );
    }
    assert_eq!(defaults.socks_proxy_config, "{}");
    let callback_executor_disabled = RemotingClient::with_config(ClientConfig {
        disable_callback_executor: true,
        ..defaults.clone()
    })
    .unwrap();
    callback_executor_disabled.start().unwrap();
    assert!(callback_executor_disabled.get_disable_callback_executor());
    let retained_executor: Arc<dyn CallbackExecutor> = Arc::new(ThreadCallbackExecutor);
    callback_executor_disabled.set_callback_executor(Some(retained_executor.clone()));
    assert!(callback_executor_disabled.get_callback_executor().is_none());
    callback_executor_disabled.set_disable_callback_executor(false);
    assert!(!callback_executor_disabled.get_disable_callback_executor());
    assert!(Arc::ptr_eq(
        &callback_executor_disabled.get_callback_executor().unwrap(),
        &retained_executor
    ));
    callback_executor_disabled.set_disable_callback_executor(true);
    assert!(callback_executor_disabled.get_callback_executor().is_none());
    callback_executor_disabled.shutdown().await;
    assert!(
        config_is_accepted(ClientConfig {
            socks_proxy_config: r#"{"0.0.0.0/0":{"addr":"localhost:1080"}}"#.into(),
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(
        config_is_accepted(ClientConfig {
            worker_threads: 0,
            ..defaults.clone()
        })
        .is_ok()
    );
    for callback_executor_threads in [i32::MIN, -1, 0, 1, 4, i32::MAX] {
        assert!(
            config_is_accepted(ClientConfig {
                callback_executor_threads,
                ..defaults.clone()
            })
            .is_ok()
        );
    }
    assert!(
        config_is_accepted(ClientConfig {
            client_socket_send_buffer_size: i32::MIN,
            client_socket_receive_buffer_size: i32::MAX,
            write_buffer_low_water_mark: i32::MIN,
            write_buffer_high_water_mark: i32::MAX,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(
        config_is_accepted(ClientConfig {
            write_buffer_low_water_mark: 10,
            write_buffer_high_water_mark: 5,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(
        config_is_accepted(ClientConfig {
            pooled_byte_buffer_allocator_enabled: true,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(
        config_is_accepted(ClientConfig {
            write_buffer_low_water_mark: 0,
            write_buffer_high_water_mark: 128,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(
        config_is_accepted(ClientConfig {
            write_buffer_high_water_mark: 128,
            ..defaults.clone()
        })
        .is_ok()
    );
    assert!(
        config_is_accepted(ClientConfig {
            socks_proxy_config: r#"{"not-a-cidr":{"addr":"invalid","username":"user"}}"#.into(),
            ..defaults.clone()
        })
        .is_ok()
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(12);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let observed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let hook_observed = observed.clone();
    client.add_rpc_hook(Arc::new(move |_: &str, _: &RemotingCommand| {
        hook_observed.lock().unwrap().push("before".into());
    }));
    let callback_observed = observed.clone();
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    let callback = Arc::new(move |result: Result<RemotingCommand, TransportError>| {
        callback_observed.lock().unwrap().push("callback".into());
        if let Some(sender) = callback_tx.lock().unwrap().take() {
            let _ = sender.send(result);
        }
    });
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            callback,
        )
        .unwrap();
    assert!(callback_rx.await.unwrap().unwrap().is_response());
    peer.await.unwrap();
    client.shutdown().await;
    assert!(
        observed
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "before")
    );
    assert!(
        observed
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "callback")
    );
}

struct PanickingCallbackExecutor;

impl CallbackExecutor for PanickingCallbackExecutor {
    fn execute(
        &self,
        _task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
        panic!("executor panic");
    }
}

struct InlineCallbackExecutor;

impl CallbackExecutor for InlineCallbackExecutor {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
        task();
        Ok(())
    }
}

struct RejectingCallbackExecutor;

impl CallbackExecutor for RejectingCallbackExecutor {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
        Err(task)
    }
}

struct ThreadCallbackExecutor;

impl CallbackExecutor for ThreadCallbackExecutor {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
        std::thread::spawn(task);
        Ok(())
    }
}

struct CountingCallbackExecutor(Arc<AtomicUsize>);

impl CallbackExecutor for CountingCallbackExecutor {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        task();
        Ok(())
    }
}

struct TestHook(Arc<std::sync::Mutex<Vec<&'static str>>>);
impl RpcHook for TestHook {
    fn equality_key(&self) -> Option<String> {
        Some("test-hook".into())
    }

    fn before_request(&self, _: &str, _: &RemotingCommand) {
        self.0.lock().unwrap().push("before");
    }
    fn after_response(&self, _: &str, _: &RemotingCommand, _: &RemotingCommand) {
        self.0.lock().unwrap().push("after");
    }
}

#[tokio::test]
async fn p3_rpc_hook_live_list_can_be_mutated_and_is_shared() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook: Arc<dyn RpcHook> = Arc::new(TestHook(events));
    client.with_rpc_hooks(|hooks| hooks.push(hook.clone()));
    assert!(Arc::ptr_eq(&client.get_rpc_hooks()[0], &hook));
    client.with_rpc_hooks(|hooks| hooks.clear());
    assert!(client.get_rpc_hooks().is_empty());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_rpc_hooks_deduplicate_and_clear() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let request = read_command(&mut stream).await;
            let mut response = command(12);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook: Arc<dyn RpcHook> = Arc::new(TestHook(events.clone()));
    client.add_rpc_hook(hook.clone());
    let equivalent_hook: Arc<dyn RpcHook> = Arc::new(TestHook(events.clone()));
    client.add_rpc_hook(equivalent_hook.clone());
    assert_eq!(client.get_rpc_hooks().len(), 1);
    assert!(client.remove_rpc_hook(&equivalent_hook));
    assert!(client.get_rpc_hooks().is_empty());
    client.add_rpc_hook(hook.clone());
    client.add_rpc_hook(Arc::new(TestHook(events.clone())));
    client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["before", "after"]);
    client.clear_rpc_hooks();
    client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["before", "after"]);
    peer.await.unwrap();
    client.shutdown().await;
}

struct BeforeCounter(Arc<AtomicUsize>);
impl RpcHook for BeforeCounter {
    fn before_request(&self, _: &str, _: &RemotingCommand) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct OnewayHookFlagRecorder(Arc<std::sync::Mutex<Vec<bool>>>);
impl RpcHook for OnewayHookFlagRecorder {
    fn before_request(&self, _: &str, request: &RemotingCommand) {
        self.0.lock().unwrap().push(request.is_oneway());
    }
}

struct AfterRequestRecorder(Arc<std::sync::Mutex<Vec<(RemotingCommand, RemotingCommand)>>>);
impl RpcHook for AfterRequestRecorder {
    fn after_response(&self, _: &str, request: &RemotingCommand, response: &RemotingCommand) {
        self.0
            .lock()
            .unwrap()
            .push((request.clone(), response.clone()));
    }
}

struct TestEvents(Arc<std::sync::Mutex<Vec<&'static str>>>);
impl ChannelEventListener for TestEvents {
    fn on_connect(&self, _: &str, _: &ChannelHandle) {
        self.0.lock().unwrap().push("connect");
    }

    fn on_exception(&self, _: &str, _: &ChannelHandle) {
        self.0.lock().unwrap().push("exception");
    }

    fn on_close(&self, _: &str, _: &ChannelHandle) {
        self.0.lock().unwrap().push("close");
    }

    fn on_active(&self, _: &str, _: &ChannelHandle) {
        self.0.lock().unwrap().push("active");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p3_callback_executor_runs_callbacks_on_bounded_workers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let request = read_command(&mut stream).await;
            let mut response = command(12);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
        }
    });
    let client = RemotingClient::with_config(ClientConfig {
        callback_executor_threads: 2,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let active = active.clone();
        let max_active = max_active.clone();
        let completed = completed.clone();
        client
            .invoke_async(
                address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(2),
                Arc::new(move |_: Result<RemotingCommand, TransportError>| {
                    let running = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(running, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(30));
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while completed.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p3_java_max_callback_capacity_starts_and_grows_on_demand() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = fixture["callbackThreadsConstruction"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["input"] == i32::MAX)
        .unwrap();
    assert_eq!(java["effective"], i32::MAX);
    assert_eq!(java["activeAtConstruction"], 0);

    let client = RemotingClient::with_config(ClientConfig {
        callback_executor_threads: i32::MAX,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    for code in [1, 2] {
        let active = active.clone();
        let max_active = max_active.clone();
        let completed = completed.clone();
        client
            .invoke_async(
                "127.0.0.1:1",
                command(code),
                Instant::now() - Duration::from_millis(1),
                Arc::new(move |_: Result<RemotingCommand, TransportError>| {
                    let running = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(running, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(30));
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while completed.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_caller_callback_executor_routes_callbacks_to_owned_executor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(12);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    assert!(client.get_callback_executor().is_some());
    let callback_executor: Arc<dyn CallbackExecutor> = Arc::new(ThreadCallbackExecutor);
    client.set_callback_executor(Some(callback_executor.clone()));
    assert!(Arc::ptr_eq(
        &client.get_callback_executor().unwrap(),
        &callback_executor
    ));
    client.set_callback_executor(None);
    let default_executor = client.get_callback_executor().unwrap();
    assert!(!Arc::ptr_eq(&default_executor, &callback_executor));
    client.set_callback_executor(Some(callback_executor));
    let caller_thread = std::thread::current().id();
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                let sender = callback_tx.lock().unwrap().take().unwrap();
                let _ = sender.send((result, std::thread::current().id()));
            }),
        )
        .unwrap();
    let (result, callback_thread) = callback_rx.await.unwrap();
    assert!(result.unwrap().is_response());
    assert_ne!(callback_thread, caller_thread);
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_callback_executor_disable_setting_is_read_at_completion_time() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for code in [12, 13] {
            let request = read_command(&mut stream).await;
            let mut response = command(code);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
        }
    });
    let client = RemotingClient::with_config(ClientConfig {
        disable_callback_executor: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    assert!(!client.get_disable_callback_executor());
    let executions = Arc::new(AtomicUsize::new(0));
    let executor: Arc<dyn CallbackExecutor> =
        Arc::new(CountingCallbackExecutor(executions.clone()));
    client.set_callback_executor(Some(executor.clone()));

    client.set_disable_callback_executor(true);
    assert!(client.get_disable_callback_executor());
    assert!(client.get_callback_executor().is_none());
    let (first_tx, mut first_rx) = tokio::sync::mpsc::unbounded_channel();
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                let _ = first_tx.send(result.map(|response| response.code));
            }),
        )
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), first_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        12
    );
    assert_eq!(executions.load(Ordering::SeqCst), 0);

    client.set_disable_callback_executor(false);
    assert!(!client.get_disable_callback_executor());
    assert!(Arc::ptr_eq(
        &client.get_callback_executor().unwrap(),
        &executor
    ));
    let (second_tx, mut second_rx) = tokio::sync::mpsc::unbounded_channel();
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                let _ = second_tx.send(result.map(|response| response.code));
            }),
        )
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), second_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        13
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_callback_executor_panic_falls_back_and_callback_panic_is_contained() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut byte).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(1)
            )
            .await
    );
    client.set_callback_executor(Some(Arc::new(PanickingCallbackExecutor)));
    let executor_fallback = Arc::new(AtomicUsize::new(0));
    let seen = executor_fallback.clone();
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() - Duration::from_millis(1),
            Arc::new(move |_: Result<RemotingCommand, TransportError>| {
                seen.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while executor_fallback.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("panicking executor should fall back to inline callback delivery");
    assert_eq!(executor_fallback.load(Ordering::SeqCst), 1);

    client.set_callback_executor(Some(Arc::new(InlineCallbackExecutor)));
    client
        .invoke_async(
            address.to_string(),
            command(2),
            Instant::now() - Duration::from_millis(1),
            Arc::new(|_: Result<RemotingCommand, TransportError>| panic!("callback panic")),
        )
        .unwrap();
    let after_callback_panic = Arc::new(AtomicUsize::new(0));
    let seen = after_callback_panic.clone();
    client
        .invoke_async(
            address.to_string(),
            command(3),
            Instant::now() - Duration::from_millis(1),
            Arc::new(move |_: Result<RemotingCommand, TransportError>| {
                seen.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while after_callback_panic.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("callback panic should not prevent a later callback");
    assert_eq!(after_callback_panic.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_callback_executor_rejection_runs_callback_inline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut byte).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(1)
            )
            .await
    );
    client.set_callback_executor(Some(Arc::new(RejectingCallbackExecutor)));
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() - Duration::from_millis(1),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                let _ = callback_tx.lock().unwrap().take().unwrap().send(result);
            }),
        )
        .unwrap();
    assert!(matches!(
        callback_rx.await.unwrap(),
        Err(TransportError::TimeoutWithContext {
            phase: rocketmq_classic_remoting::error::TimeoutPhase::Operation,
            ..
        })
    ));
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_async_connection_is_acquired_before_admission_wait() {
    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_address = first_listener.local_addr().unwrap();
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_address = second_listener.local_addr().unwrap();
    let (second_connection_tx, second_connection_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first_stream, _) = first_listener.accept().await.unwrap();
        let first_request = read_command(&mut first_stream).await;
        let (mut second_stream, _) = second_listener.accept().await.unwrap();
        let _ = second_connection_tx.send(());
        let mut first_response = command(31);
        first_response.opaque = first_request.opaque;
        first_response.mark_response();
        write_command(&mut first_stream, &first_response).await;
        let second_request = read_command(&mut second_stream).await;
        let mut second_response = command(32);
        second_response.opaque = second_request.opaque;
        second_response.mark_response();
        write_command(&mut second_stream, &second_response).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let (first_tx, first_rx) = tokio::sync::oneshot::channel();
    let first_tx = Arc::new(std::sync::Mutex::new(Some(first_tx)));
    client
        .invoke_async(
            first_address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = first_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    let second_tx = Arc::new(std::sync::Mutex::new(Some(second_tx)));
    client
        .invoke_async(
            second_address.to_string(),
            command(2),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = second_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), second_connection_rx)
        .await
        .expect("second connection was not acquired while waiting for admission")
        .unwrap();
    assert_eq!(first_rx.await.unwrap().unwrap().code, 31);
    assert_eq!(second_rx.await.unwrap().unwrap().code, 32);
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_outbound_rpc_hook_runs_before_async_admission_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let (hook_tx, hook_rx) = tokio::sync::oneshot::channel();
    let hook_tx = Arc::new(std::sync::Mutex::new(Some(hook_tx)));
    let hook_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_for_hook = hook_count.clone();
    client.add_rpc_hook(Arc::new(move |_: &str, _: &RemotingCommand| {
        if count_for_hook.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            if let Some(sender) = hook_tx.lock().unwrap().take() {
                let _ = sender.send(());
            }
        }
    }));
    let (first_tx, _first_rx) = tokio::sync::oneshot::channel();
    let first_tx = Arc::new(std::sync::Mutex::new(Some(first_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = first_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    let second_tx = Arc::new(std::sync::Mutex::new(Some(second_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(2),
            Instant::now() + Duration::from_millis(100),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = second_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), hook_rx)
        .await
        .expect("the second hook must run before the saturated semaphore wait")
        .unwrap();
    assert_eq!(hook_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(second_rx.await.unwrap().unwrap_err().is_timeout());
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_sync_invoke_obeys_async_admission_and_runs_hooks_first() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _first = read_command(&mut stream).await;
        let _ = received_tx.send(());
        let mut byte = [0; 1];
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    client.add_rpc_hook(Arc::new(BeforeCounter(hook_calls.clone())));
    let first_client = client.clone();
    let first_address = address.to_string();
    let first = tokio::spawn(async move {
        first_client
            .invoke_sync(&first_address, command(1), Duration::from_secs(2))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), received_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        client
            .invoke_sync(&address.to_string(), command(2), Duration::from_millis(100))
            .await,
        Err(TransportError::SendRequest { .. })
    ));
    assert_eq!(hook_calls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        first.await.unwrap(),
        Err(TransportError::SendRequest { .. })
    ));
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_async_admission_timeout_completes_through_callback() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        let _request = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = callback_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    accepted_rx.await.unwrap();
    let (second_context_tx, mut second_context_rx) = tokio::sync::mpsc::unbounded_channel();
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_millis(20),
            Arc::new(ContextCallback {
                sender: second_context_tx,
                events: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
        )
        .unwrap();
    let second_context = second_context_rx.recv().await.unwrap();
    assert!(
        second_context
            .error
            .as_ref()
            .is_some_and(TransportError::is_timeout)
    );
    assert!(second_context.send_request_ok);
    assert!(
        !second_context.timed_out,
        "Java's exceptional callback wrapper starts a new ResponseFuture at completion"
    );
    assert!(callback_rx.await.unwrap().is_err());
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_async_peer_close_preserves_public_wrapper_default_send_status() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        drop(stream);
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(ContextCallback {
                sender: tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    let context = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        context.error,
        Some(TransportError::SendRequest {
            remote_addr: address.to_string(),
            cause: None,
        })
    );
    assert!(context.send_request_ok);
    wait_for_callback_events(&events, &["complete", "fail"]).await;
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_async_callback_completes_on_deadline_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1200)).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_millis(20),
            Arc::new(ContextCallback {
                sender: tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .error
            .unwrap()
            .is_timeout()
    );
    wait_for_callback_events(&events, &["complete", "fail"]).await;
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_async_go_away_retry_connect_failure_completes_before_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        drop(listener);
        let request = read_command(&mut first).await;
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = request.opaque;
        go_away.mark_response();
        write_command(&mut first, &go_away).await;
        drop(first);
        request.opaque
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .invoke_async(
            address.to_string(),
            RemotingCommand {
                flag: 1 << 8,
                remark: Some("original request metadata".into()),
                ext_fields: Some(
                    std::collections::BTreeMap::from([("trace".into(), "original".into())]).into(),
                ),
                body: Some(vec![7, 8, 9]),
                ..command(1)
            },
            Instant::now() + Duration::from_secs(2),
            Arc::new(ContextCallback {
                sender: tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    let context = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        context.error,
        Some(TransportError::ConnectRequest {
            remote_addr,
            cause: Some(_),
        }) if remote_addr == address.to_string()
    ));
    assert!(context.response.is_none());
    assert_eq!(context.request.flag, 1 << 8);
    assert_eq!(
        context.request.remark.as_deref(),
        Some("original request metadata")
    );
    assert_eq!(
        context.request.ext_fields.unwrap().get("trace").unwrap(),
        "original"
    );
    assert_eq!(context.request.body, Some(vec![7, 8, 9]));
    assert_eq!(context.request.opaque, peer.await.unwrap());
    wait_for_callback_events(&events, &["complete", "fail"]).await;
    client.shutdown().await;
}

#[tokio::test]
async fn p3_async_second_go_away_callback_retains_original_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut opaques = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_command(&mut stream).await;
            opaques.push(request.opaque);
            let mut go_away = command(response::GO_AWAY);
            go_away.opaque = request.opaque;
            go_away.mark_response();
            write_command(&mut stream, &go_away).await;
        }
        opaques
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let request = RemotingCommand {
        flag: 1 << 8,
        remark: Some("original request metadata".into()),
        ext_fields: Some(
            std::collections::BTreeMap::from([("trace".into(), "original".into())]).into(),
        ),
        body: Some(vec![7, 8, 9]),
        ..command(1)
    };
    client
        .invoke_async(
            address.to_string(),
            request,
            Instant::now() + Duration::from_secs(2),
            Arc::new(ContextCallback {
                sender: tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    let context = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let Some(TransportError::SendRequest { remote_addr, cause }) = context.error else {
        panic!("expected Java-compatible send-request error");
    };
    assert_eq!(remote_addr, address.to_string());
    assert!(
        cause
            .unwrap()
            .to_string()
            .starts_with("Receive GO_AWAY twice in request from channelId=")
    );
    assert!(context.response.is_none());
    assert_eq!(context.request.flag, 1 << 8);
    assert_eq!(
        context.request.remark.as_deref(),
        Some("original request metadata")
    );
    assert_eq!(
        context.request.ext_fields.unwrap().get("trace").unwrap(),
        "original"
    );
    assert_eq!(context.request.body, Some(vec![7, 8, 9]));
    assert_eq!(context.request.opaque, peer.await.unwrap()[0]);
    wait_for_callback_events(&events, &["complete", "fail"]).await;
    client.shutdown().await;
}

#[tokio::test]
async fn p3_async_go_away_retry_callback_reports_reconstructed_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let request = read_command(&mut first).await;
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = request.opaque;
        go_away.mark_response();
        write_command(&mut first, &go_away).await;
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        let retry = read_command(&mut second).await;
        let mut response = command(42);
        response.opaque = retry.opaque;
        response.mark_response();
        write_command(&mut second, &response).await;
        retry
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let request = RemotingCommand {
        flag: 1 << 8,
        remark: Some("original remark".into()),
        ext_fields: Some(
            std::collections::BTreeMap::from([("trace".into(), "preserved".into())]).into(),
        ),
        body: Some(vec![4, 5, 6]),
        ..command(1)
    };
    client
        .invoke_async(
            address.to_string(),
            request,
            Instant::now() + Duration::from_secs(2),
            Arc::new(ContextCallback {
                sender: tx,
                events: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
        )
        .unwrap();
    let context = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.response.unwrap().code, 42);
    assert_eq!(
        context.request.language,
        Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
    );
    assert_eq!(context.request.flag, 0);
    assert_eq!(context.request.remark, None);
    assert_eq!(
        context.request.ext_fields.unwrap().get("trace").unwrap(),
        "preserved"
    );
    assert_eq!(context.request.body, Some(vec![4, 5, 6]));
    assert_ne!(context.request.opaque, 0);
    client.shutdown().await;
    let retry = peer.await.unwrap();
    assert_eq!(context.request.opaque, retry.opaque);
}

#[tokio::test]
async fn p3_go_away_retries_once_on_a_new_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let request = read_command(&mut first).await;
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = request.opaque;
        go_away.mark_response();
        write_command(&mut first, &go_away).await;
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        let retry = read_command(&mut second).await;
        let mut response = command(42);
        response.opaque = retry.opaque;
        response.mark_response();
        write_command(&mut second, &response).await;
        retry
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    client.add_rpc_hook(Arc::new(BeforeCounter(hook_calls.clone())));
    let after_hook = Arc::new(std::sync::Mutex::new(Vec::new()));
    client.add_rpc_hook(Arc::new(AfterRequestRecorder(after_hook.clone())));
    let original = RemotingCommand {
        flag: 1 << 8,
        remark: Some("must not survive Java-style request reconstruction".into()),
        ext_fields: Some(
            std::collections::BTreeMap::from([("trace".into(), "preserved".into())]).into(),
        ),
        body: Some(vec![1, 2, 3]),
        ..command(1)
    };
    let result = client
        .invoke(
            &address.to_string(),
            original,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(result.code, 42);
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    let (after_request, after_response) = after_hook.lock().unwrap().pop().unwrap();
    assert_eq!(
        after_request.remark.as_deref(),
        Some("must not survive Java-style request reconstruction")
    );
    assert_eq!(after_request.flag, 1 << 8);
    assert_eq!(after_response.code, 42);
    client.shutdown().await;
    let retry = peer.await.unwrap();
    assert_eq!(retry.code, 1);
    assert_eq!(
        retry.language,
        Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
    );
    assert_eq!(retry.flag, 0);
    assert_eq!(retry.remark, None);
    assert_eq!(retry.ext_fields.unwrap().get("trace").unwrap(), "preserved");
    assert_eq!(retry.body, Some(vec![1, 2, 3]));
}

#[tokio::test]
async fn p3_go_away_reads_live_setting_after_client_start() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let request = read_command(&mut first).await;
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = request.opaque;
        go_away.mark_response();
        write_command(&mut first, &go_away).await;
        drop(first);

        let (mut second, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("live reconnect setting did not create a replacement connection")
            .unwrap();
        let retry = read_command(&mut second).await;
        let mut response = command(42);
        response.opaque = retry.opaque;
        response.mark_response();
        write_command(&mut second, &response).await;
        retry
    });
    let client = RemotingClient::with_config(ClientConfig {
        enable_reconnect_for_go_away: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    client.set_enable_reconnect_for_go_away(true);
    assert!(client.get_enable_reconnect_for_go_away());

    let result = client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(result.code, 42);
    client.shutdown().await;
    let retry = peer.await.unwrap();
    assert_eq!(retry.code, 1);
}

#[tokio::test]
async fn p3_go_away_replacement_keeps_other_old_connection_requests_alive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut old_connection, _) = listener.accept().await.unwrap();
        let first = read_command(&mut old_connection).await;
        let second = read_command(&mut old_connection).await;
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = first.opaque;
        go_away.mark_response();
        write_command(&mut old_connection, &go_away).await;

        let (mut replacement, _) = listener.accept().await.unwrap();
        let retry = read_command(&mut replacement).await;
        let mut existing_response = command(62);
        existing_response.opaque = second.opaque;
        existing_response.mark_response();
        write_command(&mut old_connection, &existing_response).await;

        let mut retry_response = command(61);
        retry_response.opaque = retry.opaque;
        retry_response.mark_response();
        write_command(&mut replacement, &retry_response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let first_client = client.clone();
    let first_address = address.to_string();
    let first = tokio::spawn(async move {
        first_client
            .invoke(
                &first_address,
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });
    let second_client = client.clone();
    let second_address = address.to_string();
    let second = tokio::spawn(async move {
        second_client
            .invoke(
                &second_address,
                command(2),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });

    let results = tokio::time::timeout(Duration::from_secs(2), async {
        (first.await.unwrap(), second.await.unwrap())
    })
    .await
    .expect("replacement must leave other requests on the old connection alive");
    assert_eq!(results.0.unwrap().code, 61);
    assert_eq!(results.1.unwrap().code, 62);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_close_channels_closes_go_away_current_but_keeps_retired_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (retry_seen_tx, retry_seen_rx) = tokio::sync::oneshot::channel();
    let (old_closed_tx, old_closed_rx) = tokio::sync::oneshot::channel();
    let (replacement_closed_tx, replacement_closed_rx) = tokio::sync::oneshot::channel();
    let (continue_old_tx, continue_old_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut old_connection, _) = listener.accept().await.unwrap();
        let first_request = read_command(&mut old_connection).await;
        let second_request = read_command(&mut old_connection).await;
        let (retry_request, other_pending) = if first_request.code == 1 {
            (first_request, second_request)
        } else {
            (second_request, first_request)
        };
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = retry_request.opaque;
        go_away.mark_response();
        write_command(&mut old_connection, &go_away).await;

        let (mut replacement, _) = listener.accept().await.unwrap();
        let _retry = read_command(&mut replacement).await;
        let _ = retry_seen_tx.send(());
        let mut byte = [0];
        replacement.read_exact(&mut byte).await.unwrap_err();
        let _ = replacement_closed_tx.send(());
        let _ = continue_old_rx.await;
        let mut old_response = command(62);
        old_response.opaque = other_pending.opaque;
        old_response.mark_response();
        write_command(&mut old_connection, &old_response).await;
        old_connection.read_exact(&mut byte).await.unwrap_err();
        let _ = old_closed_tx.send(());
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let first_client = client.clone();
    let first_address = address.to_string();
    let first = tokio::spawn(async move {
        first_client
            .invoke(
                &first_address,
                command(1),
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });
    let second_client = client.clone();
    let second_address = address.to_string();
    let second = tokio::spawn(async move {
        second_client
            .invoke(
                &second_address,
                command(2),
                Instant::now() + Duration::from_secs(3),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), retry_seen_rx)
        .await
        .expect("replacement retry was not written")
        .unwrap();
    client.close_channels(&[address.to_string()]);

    tokio::time::timeout(Duration::from_secs(2), replacement_closed_rx)
        .await
        .expect("close_channels must close the wrapper's current channel")
        .unwrap();
    let first_result = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("closing the current generation must terminate its pending retry")
        .unwrap();
    assert!(first_result.is_err());
    assert!(
        !second.is_finished(),
        "the retired channel remains open and can complete its pending call"
    );
    let _ = continue_old_tx.send(());
    let second_result = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("the retired generation's pending call must complete")
        .unwrap();
    assert_eq!(second_result.unwrap().code, 62);
    client.shutdown().await;
    tokio::time::timeout(Duration::from_secs(2), old_closed_rx)
        .await
        .expect("shutdown must close the retained old generation")
        .unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_all_requests_on_retired_connection_retry_once_on_shared_replacement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut old_connection, _) = listener.accept().await.unwrap();
        let first = read_command(&mut old_connection).await;
        let second = read_command(&mut old_connection).await;

        let mut first_go_away = command(response::GO_AWAY);
        first_go_away.opaque = first.opaque;
        first_go_away.mark_response();
        write_command(&mut old_connection, &first_go_away).await;
        let (mut replacement, _) = listener.accept().await.unwrap();
        let first_retry = read_command(&mut replacement).await;
        assert_eq!(first_retry.code, first.code);

        // Java's ChannelWrapper only retries the second old-channel response
        // after the first reconnect has swapped the wrapper's current channel.
        // That keeps this assertion independent of the tryLock race while
        // checking that both requests reuse the same replacement generation.
        let mut second_go_away = command(response::GO_AWAY);
        second_go_away.opaque = second.opaque;
        second_go_away.mark_response();
        write_command(&mut old_connection, &second_go_away).await;
        let second_retry =
            tokio::time::timeout(Duration::from_secs(1), read_command(&mut replacement))
                .await
                .expect("second GO_AWAY request must retry on the replacement");
        assert_eq!(second_retry.code, second.code);

        for retry in [first_retry, second_retry] {
            let mut response = command(60 + retry.code);
            response.opaque = retry.opaque;
            response.mark_response();
            write_command(&mut replacement, &response).await;
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let first_client = client.clone();
    let first_address = address.to_string();
    let first = tokio::spawn(async move {
        first_client
            .invoke(
                &first_address,
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });
    let second_client = client.clone();
    let second_address = address.to_string();
    let second = tokio::spawn(async move {
        second_client
            .invoke(
                &second_address,
                command(2),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });

    let (first, second) = tokio::time::timeout(Duration::from_secs(3), async {
        (first.await.unwrap(), second.await.unwrap())
    })
    .await
    .expect("GO_AWAY requests did not finish");
    assert_eq!(first.unwrap().code, 61);
    assert_eq!(second.unwrap().code, 62);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_go_away_retry_reacquires_async_admission() {
    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = first_listener.local_addr().unwrap();
    let competitor_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let competitor_address = competitor_listener.local_addr().unwrap();
    let (go_away_tx, go_away_rx) = tokio::sync::oneshot::channel();
    let (competitor_seen_tx, competitor_seen_rx) = tokio::sync::oneshot::channel();
    let (release_competitor_tx, release_competitor_rx) = tokio::sync::oneshot::channel();
    let (retry_seen_tx, retry_seen_rx) = tokio::sync::oneshot::channel();
    let peers = tokio::spawn(async move {
        let (mut first_stream, _) = first_listener.accept().await.unwrap();
        let first_request = read_command(&mut first_stream).await;
        let _ = go_away_rx.await;
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = first_request.opaque;
        go_away.mark_response();
        write_command(&mut first_stream, &go_away).await;
        drop(first_stream);

        let (mut competitor_stream, _) = competitor_listener.accept().await.unwrap();
        let competitor_request = read_command(&mut competitor_stream).await;
        let _ = competitor_seen_tx.send(());
        let _ = release_competitor_rx.await;
        let mut competitor_response = command(52);
        competitor_response.opaque = competitor_request.opaque;
        competitor_response.mark_response();
        write_command(&mut competitor_stream, &competitor_response).await;

        let (mut retry_stream, _) = first_listener.accept().await.unwrap();
        let retry_request = read_command(&mut retry_stream).await;
        let mut retry_response = command(53);
        retry_response.opaque = retry_request.opaque;
        retry_response.mark_response();
        write_command(&mut retry_stream, &retry_response).await;
        let _ = retry_seen_tx.send(retry_request.code);
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let (competitor_hook_tx, competitor_hook_rx) = tokio::sync::oneshot::channel();
    let competitor_hook_tx = Arc::new(std::sync::Mutex::new(Some(competitor_hook_tx)));
    let hook_sender = competitor_hook_tx.clone();
    client.add_rpc_hook(Arc::new(move |_: &str, request: &RemotingCommand| {
        if request.code == 52 {
            if let Some(sender) = hook_sender.lock().unwrap().take() {
                let _ = sender.send(());
            }
        }
    }));

    let (first_done_tx, first_done_rx) = tokio::sync::oneshot::channel();
    let first_done_tx = Arc::new(std::sync::Mutex::new(Some(first_done_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(51),
            Instant::now() + Duration::from_secs(3),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = first_done_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    let (competitor_done_tx, competitor_done_rx) = tokio::sync::oneshot::channel();
    let competitor_done_tx = Arc::new(std::sync::Mutex::new(Some(competitor_done_tx)));
    client
        .invoke_async(
            competitor_address.to_string(),
            command(52),
            Instant::now() + Duration::from_secs(3),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = competitor_done_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), competitor_hook_rx)
        .await
        .expect("competitor must reach admission wait before GO_AWAY")
        .unwrap();
    let _ = go_away_tx.send(());
    tokio::time::timeout(Duration::from_secs(1), competitor_seen_rx)
        .await
        .expect("queued competitor should receive the released admission permit")
        .unwrap();
    let mut retry_seen_rx = retry_seen_rx;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut retry_seen_rx)
            .await
            .is_err()
    );
    let _ = release_competitor_tx.send(());
    assert_eq!(competitor_done_rx.await.unwrap().unwrap().code, 52);
    assert_eq!(first_done_rx.await.unwrap().unwrap().code, 53);
    assert_eq!(retry_seen_rx.await.unwrap(), 51);
    client.shutdown().await;
    peers.await.unwrap();
}

#[tokio::test]
async fn p3_connect_failure_does_not_run_before_rpc_hooks() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    client.add_rpc_hook(Arc::new(BeforeCounter(hook_calls.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let result = client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(hook_calls.load(Ordering::SeqCst), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_go_away_response_maps_to_java_send_exception() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(response::GO_AWAY);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        enable_reconnect_for_go_away: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let error = client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    let TransportError::SendRequest { remote_addr, cause } = error else {
        panic!("expected Java-compatible send-request error");
    };
    assert_eq!(remote_addr, address.to_string());
    assert!(
        cause
            .unwrap()
            .to_string()
            .starts_with("Receive GO_AWAY from channelId=")
    );
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_malformed_frame_notifies_exception_then_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_seen_tx, request_seen_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        let _ = request_seen_tx.send(());
        stream.write_all(&[0, 0, 0, 0]).await.unwrap();
    });
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig::default(),
        Some(Arc::new(TestEvents(events.clone()))),
    )
    .unwrap();
    client.start().unwrap();
    let invoke_client = client.clone();
    let invoke_address = address.to_string();
    let invoke = tokio::spawn(async move {
        invoke_client
            .invoke(
                &invoke_address,
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), request_seen_rx)
        .await
        .expect("peer did not observe invoke")
        .unwrap();
    let result = invoke.await.unwrap();
    assert!(matches!(
        result,
        Err(TransportError::SendRequest {
            remote_addr,
            cause: None,
        }) if remote_addr == address.to_string()
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let complete = {
                let events = events.lock().unwrap();
                events.contains(&"active") && events.contains(&"close")
            };
            if complete {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec!["connect", "active", "exception", "close"]
    );
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_channel_connect_event_fires_when_dial_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig::default(),
        Some(Arc::new(TestEvents(events.clone()))),
    )
    .unwrap();
    client.start().unwrap();
    assert!(
        client
            .invoke(
                &address,
                command(1),
                Instant::now() + Duration::from_secs(1)
            )
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while events.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["connect"]);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_hooks_and_channel_connect_event_follow_successful_exchange() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(12);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let channel_listener: Arc<dyn ChannelEventListener> = Arc::new(TestEvents(events.clone()));
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig::default(),
        Some(channel_listener.clone()),
    )
    .unwrap();
    client.start().unwrap();
    client.add_rpc_hook(Arc::new(TestHook(events.clone())));
    assert!(Arc::ptr_eq(
        &client.get_channel_event_listener().unwrap(),
        &channel_listener
    ));
    assert!(client.get_channel_event_listener().is_some());
    client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if events.lock().unwrap().contains(&"connect") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await;
    let events = events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|event| **event == "connect").count(),
        1
    );
    assert!(
        events.iter().position(|event| *event == "before")
            < events.iter().position(|event| *event == "after")
    );
}

struct AbortPipeline;

impl RequestPipeline for AbortPipeline {
    fn execute(&self, _: &RemotingCommand) -> Result<(), i32> {
        Ok(())
    }

    fn execute_detailed(&self, _: &RemotingCommand) -> Result<(), RequestPipelineError> {
        Err(RequestPipelineError::Abort {
            response_code: 812,
            remark: Some("pipeline stopped request".into()),
        })
    }
}

struct FailedPipeline;

impl RequestPipeline for FailedPipeline {
    fn execute(&self, _: &RemotingCommand) -> Result<(), i32> {
        Ok(())
    }

    fn execute_detailed(&self, _: &RemotingCommand) -> Result<(), RequestPipelineError> {
        Err(RequestPipelineError::Failed("pipeline failed".into()))
    }
}

struct MutatingPipelineStage {
    code: i32,
    remark: String,
    opaque: i32,
    ext_field_key: &'static str,
    ext_field_value: &'static str,
}

impl RequestPipeline for MutatingPipelineStage {
    fn execute(&self, _: &RemotingCommand) -> Result<(), i32> {
        Ok(())
    }

    fn execute_detailed_with_context_mut(
        &self,
        _: &RequestContext,
        request: &mut RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        request.code = self.code;
        request.remark = Some(self.remark.clone());
        request.opaque = self.opaque;
        request
            .ext_fields
            .get_or_insert_with(Default::default)
            .insert(self.ext_field_key.into(), self.ext_field_value.into());
        Ok(())
    }
}

struct PipelineRequestObserver(Arc<std::sync::Mutex<Option<RemotingCommand>>>);

impl RequestPipeline for PipelineRequestObserver {
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32> {
        *self.0.lock().unwrap() = Some(request.clone());
        Ok(())
    }
}

struct AfterResponseCounter(Arc<AtomicUsize>);

impl RpcHook for AfterResponseCounter {
    fn after_response_optional_result(
        &self,
        _: &str,
        _: &RemotingCommand,
        _: Option<&RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct MutatingAfterResponse {
    request_code: i32,
    request_remark: String,
    code: i32,
    remark: String,
    ext_field_key: &'static str,
    ext_field_value: &'static str,
}

impl RpcHook for MutatingAfterResponse {
    fn after_response_mut_result(
        &self,
        _: &str,
        request: &mut RemotingCommand,
        response: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        request.code = self.request_code;
        request.remark = Some(self.request_remark.clone());
        request
            .ext_fields
            .get_or_insert_with(Default::default)
            .insert("request-hook".into(), "changed".into());
        response.code = self.code;
        response.remark = Some(self.remark.clone());
        response
            .ext_fields
            .get_or_insert_with(Default::default)
            .insert(self.ext_field_key.into(), self.ext_field_value.into());
        Ok(())
    }
}

struct AfterResponseRequestObserver(Arc<std::sync::Mutex<Option<RemotingCommand>>>);

impl RpcHook for AfterResponseRequestObserver {
    fn after_response_mut_result(
        &self,
        _: &str,
        request: &mut RemotingCommand,
        _: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        *self.0.lock().unwrap() = Some(request.clone());
        Ok(())
    }
}

#[tokio::test]
async fn p18_outbound_after_hook_can_mutate_response_returned_to_caller() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(900_009);
        response.opaque = request.opaque;
        response.remark = Some("wire response".into());
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/hooks.json")).unwrap();
    let java_mutated = &fixture["afterResponseMutation"];
    let expected_code = java_mutated["code"].as_i64().unwrap() as i32;
    let expected_remark = java_mutated["remark"].as_str().unwrap().to_owned();
    let expected_ext_field = java_mutated["extFields"]["hook"]
        .as_str()
        .unwrap()
        .to_owned();
    let expected_request_code = java_mutated["requestCode"].as_i64().unwrap() as i32;
    let expected_request_remark = java_mutated["requestRemark"].as_str().unwrap().to_owned();
    let expected_request_ext_field = java_mutated["requestExtFields"]["request-hook"]
        .as_str()
        .unwrap()
        .to_owned();
    let observed_request = Arc::new(std::sync::Mutex::new(None));
    client.add_rpc_hook(Arc::new(MutatingAfterResponse {
        request_code: expected_request_code,
        request_remark: expected_request_remark.clone(),
        code: expected_code,
        remark: expected_remark.clone(),
        ext_field_key: "hook",
        ext_field_value: "changed",
    }));
    client.add_rpc_hook(Arc::new(AfterResponseRequestObserver(
        observed_request.clone(),
    )));
    let response = client
        .invoke(
            &address.to_string(),
            command(900_008),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(response.code, expected_code);
    assert_eq!(response.remark.as_deref(), Some(expected_remark.as_str()));
    assert_eq!(
        response.ext_fields.as_ref().unwrap()["hook"],
        expected_ext_field
    );
    {
        let observed_request = observed_request.lock().unwrap();
        let observed_request = observed_request.as_ref().unwrap();
        assert_eq!(observed_request.code, expected_request_code);
        assert_eq!(
            observed_request.remark.as_deref(),
            Some(expected_request_remark.as_str())
        );
        assert_eq!(
            observed_request.ext_fields.as_ref().unwrap()["request-hook"],
            expected_request_ext_field
        );
    }
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p18_inbound_after_hook_can_mutate_response_written_to_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        write_command(&mut stream, &command(900_011)).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/hooks.json")).unwrap();
    let java_mutated = &fixture["afterResponseMutation"];
    let expected_code = java_mutated["code"].as_i64().unwrap() as i32;
    let expected_remark = java_mutated["remark"].as_str().unwrap().to_owned();
    let expected_ext_field = java_mutated["extFields"]["hook"]
        .as_str()
        .unwrap()
        .to_owned();
    let expected_request_code = java_mutated["requestCode"].as_i64().unwrap() as i32;
    let expected_request_remark = java_mutated["requestRemark"].as_str().unwrap().to_owned();
    let expected_request_ext_field = java_mutated["requestExtFields"]["request-hook"]
        .as_str()
        .unwrap()
        .to_owned();
    client.register_processor(
        900_011,
        Arc::new(|_: RemotingCommand| Ok(Some(command(900_012)))),
    );
    let observed_request = Arc::new(std::sync::Mutex::new(None));
    client.add_rpc_hook(Arc::new(MutatingAfterResponse {
        request_code: expected_request_code,
        request_remark: expected_request_remark.clone(),
        code: expected_code,
        remark: expected_remark.clone(),
        ext_field_key: "hook",
        ext_field_value: "changed",
    }));
    client.add_rpc_hook(Arc::new(AfterResponseRequestObserver(
        observed_request.clone(),
    )));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, expected_code);
    assert_eq!(response.remark.as_deref(), Some(expected_remark.as_str()));
    assert_eq!(
        response.ext_fields.as_ref().unwrap()["hook"],
        expected_ext_field
    );
    {
        let observed_request = observed_request.lock().unwrap();
        let observed_request = observed_request.as_ref().unwrap();
        assert_eq!(observed_request.code, expected_request_code);
        assert_eq!(
            observed_request.remark.as_deref(),
            Some(expected_request_remark.as_str())
        );
        assert_eq!(
            observed_request.ext_fields.as_ref().unwrap()["request-hook"],
            expected_request_ext_field
        );
    }
    client.shutdown().await;
}

struct FallibleHook;

impl RpcHook for FallibleHook {
    fn before_request_result(
        &self,
        _: &str,
        request: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        if request.code == 830 {
            Err(RpcHookError::Abort {
                response_code: 831,
                remark: Some("hook stopped request".into()),
            })
        } else {
            Ok(())
        }
    }

    fn after_response_result(
        &self,
        _: &str,
        request: &RemotingCommand,
        _: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        if request.code == 832 {
            Err(RpcHookError::Failed("after hook failed".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn p3_rpc_hook_abort_and_failures_map_to_java_response_paths() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for (code, opaque) in [(830, 71), (832, 72)] {
            let mut request = command(code);
            request.opaque = opaque;
            write_command(&mut stream, &request).await;
        }
        let first = read_command(&mut stream).await;
        let second = read_command(&mut stream).await;
        [first, second]
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let processor_calls = Arc::new(AtomicUsize::new(0));
    for code in [830, 832] {
        let processor_calls = processor_calls.clone();
        client.register_processor(
            code,
            Arc::new(move |_: RemotingCommand| {
                processor_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(command(833)))
            }),
        );
    }
    client.add_rpc_hook(Arc::new(FallibleHook));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let responses = peer.await.unwrap();
    let responses = responses
        .into_iter()
        .map(|response| (response.opaque, response))
        .collect::<std::collections::HashMap<_, _>>();
    let aborted = responses.get(&71).unwrap();
    assert_eq!(aborted.code, 831);
    assert_eq!(aborted.remark.as_deref(), Some("hook stopped request"));
    assert_eq!(responses.get(&72).unwrap().code, response::SYSTEM_ERROR);
    assert_eq!(
        responses.get(&72).unwrap().remark.as_deref(),
        Some("after hook failed")
    );
    assert_eq!(processor_calls.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

struct EquivalentTestHook {
    key: &'static str,
    counter: Arc<AtomicUsize>,
}

impl RpcHook for EquivalentTestHook {
    fn equality_key(&self) -> Option<String> {
        Some(self.key.into())
    }

    fn before_request(&self, _: &str, _: &RemotingCommand) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn p3_processor_failure_is_exposed_in_system_error_remark() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(845);
        request.opaque = 74;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.register_processor(
        845,
        Arc::new(|_: RemotingCommand| Err("processor failed".into())),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, response::SYSTEM_ERROR);
    assert_eq!(response.remark.as_deref(), Some("processor failed"));
    assert_eq!(response.opaque, 74);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_rpc_hook_equality_keys_match_java_contains_semantics() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(843);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    client.add_rpc_hook(Arc::new(EquivalentTestHook {
        key: "same",
        counter: calls.clone(),
    }));
    client.add_rpc_hook(Arc::new(EquivalentTestHook {
        key: "same",
        counter: calls.clone(),
    }));
    client
        .invoke(
            &address.to_string(),
            command(844),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

struct OutboundFailingHook;

impl RpcHook for OutboundFailingHook {
    fn before_request_result(
        &self,
        _: &str,
        request: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        if request.code == 840 {
            Err(RpcHookError::Failed("before rejected request".into()))
        } else {
            Ok(())
        }
    }

    fn after_response_result(
        &self,
        _: &str,
        request: &RemotingCommand,
        _: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        if request.code == 841 {
            Err(RpcHookError::Failed("after rejected response".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn p3_outbound_hook_failures_are_not_ignored() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        assert_eq!(request.code, 841);
        let mut response = command(842);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.add_rpc_hook(Arc::new(OutboundFailingHook));

    assert!(matches!(
        client
            .invoke(
                &address.to_string(),
                command(840),
                Instant::now() + Duration::from_secs(2),
            )
            .await,
        Err(TransportError::RpcHook(message)) if message == "before rejected request"
    ));
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(840),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result| {
                if let Some(callback_tx) = callback_tx.lock().unwrap().take() {
                    let _ = callback_tx.send(result);
                }
            }),
        )
        .unwrap();
    assert!(matches!(
        callback_rx.await.unwrap(),
        Err(TransportError::RpcHook(message)) if message == "before rejected request"
    ));
    assert!(matches!(
        client
            .invoke(
                &address.to_string(),
                command(841),
                Instant::now() + Duration::from_secs(2),
            )
            .await,
        Err(TransportError::RpcHook(message)) if message == "after rejected response"
    ));
    peer.await.unwrap();
    client.shutdown().await;
}

struct BeforeHookFailure {
    after_hook_code: Arc<AtomicUsize>,
    after_hook_remark: Arc<std::sync::Mutex<Option<Option<String>>>>,
}

impl RpcHook for BeforeHookFailure {
    fn before_request_result(&self, _: &str, _: &RemotingCommand) -> Result<(), RpcHookError> {
        Err(RpcHookError::Failed("before hook failed".into()))
    }

    fn after_response_optional_result(
        &self,
        _: &str,
        _: &RemotingCommand,
        response: Option<&RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        if let Some(response) = response {
            self.after_hook_code
                .store(response.code as usize, Ordering::SeqCst);
        }
        *self.after_hook_remark.lock().unwrap() = Some(response.and_then(|r| r.remark.clone()));
        Ok(())
    }
}

#[tokio::test]
async fn p3_before_hook_failure_runs_pipeline_and_after_hook_but_skips_processor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(836);
        request.opaque = 73;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let pipeline_calls = Arc::new(AtomicUsize::new(0));
    let after_hook_code = Arc::new(AtomicUsize::new(0));
    let after_hook_remark = Arc::new(std::sync::Mutex::new(None));
    let processor_calls = Arc::new(AtomicUsize::new(0));
    let pipeline_counter = pipeline_calls.clone();
    client.set_request_pipeline(Some(Arc::new(move |_: &RemotingCommand| {
        pipeline_counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })));
    client.register_processor(836, {
        let processor_calls = processor_calls.clone();
        Arc::new(move |_: RemotingCommand| {
            processor_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(command(837)))
        })
    });
    client.add_rpc_hook(Arc::new(BeforeHookFailure {
        after_hook_code: after_hook_code.clone(),
        after_hook_remark: after_hook_remark.clone(),
    }));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, response::SYSTEM_ERROR);
    assert_eq!(response.remark.as_deref(), Some("before hook failed"));
    assert_eq!(pipeline_calls.load(Ordering::SeqCst), 1);
    assert_eq!(processor_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        after_hook_code.load(Ordering::SeqCst),
        response::SYSTEM_ERROR as usize
    );
    assert_eq!(*after_hook_remark.lock().unwrap(), Some(None));
    client.shutdown().await;
}

struct OptionalResponseHook(Arc<AtomicUsize>);

impl RpcHook for OptionalResponseHook {
    fn after_response_optional_result(
        &self,
        _: &str,
        _: &RemotingCommand,
        response: Option<&RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        if response.is_none() {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::test]
async fn p3_after_hook_runs_when_processor_returns_no_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outgoing = read_command(&mut stream).await;
        let mut response = command(835);
        response.opaque = outgoing.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let mut request = command(834);
        request.opaque = 73;
        write_command(&mut stream, &request).await;
        let mut byte = [0; 1];
        tokio::time::timeout(Duration::from_millis(100), stream.read(&mut byte))
            .await
            .is_err()
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let after_hook_calls = Arc::new(AtomicUsize::new(0));
    client.register_processor(834, Arc::new(|_: RemotingCommand| Ok(None)));
    client.add_rpc_hook(Arc::new(OptionalResponseHook(after_hook_calls.clone())));
    client
        .invoke(
            &address.to_string(),
            command(836),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(peer.await.unwrap());
    assert_eq!(after_hook_calls.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_request_pipeline_pipe_runs_new_stage_before_existing_stage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(810);
        request.opaque = 52;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let stages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let old_stage = stages.clone();
    client.set_request_pipeline(Some(Arc::new(move |_: &RemotingCommand| {
        old_stage.lock().unwrap().push("existing");
        Ok(())
    })));
    let new_stage = stages.clone();
    client.pipe_request_pipeline(Arc::new(move |_: &RemotingCommand| {
        new_stage.lock().unwrap().push("new");
        Ok(())
    }));
    client.register_processor(810, Arc::new(|_: RemotingCommand| Ok(Some(command(812)))));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, 812);
    assert_eq!(*stages.lock().unwrap(), vec!["new", "existing"]);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_pipeline_abort_preserves_java_style_code_and_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(811);
        request.opaque = 53;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.register_processor(811, Arc::new(|_: RemotingCommand| Ok(Some(command(813)))));
    client.set_request_pipeline(Some(Arc::new(AbortPipeline)));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, 812);
    assert_eq!(response.remark.as_deref(), Some("pipeline stopped request"));
    assert_eq!(response.opaque, 53);
    client.shutdown().await;
}

#[tokio::test]
async fn p18_pipeline_failure_maps_to_system_error_without_running_later_stages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(900_007);
        request.opaque = 75;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let processor_calls = Arc::new(AtomicUsize::new(0));
    let after_hook_calls = Arc::new(AtomicUsize::new(0));
    let processor_calls_for_handler = processor_calls.clone();
    client.register_processor(
        900_007,
        Arc::new(move |_: RemotingCommand| {
            processor_calls_for_handler.fetch_add(1, Ordering::SeqCst);
            Ok(Some(command(900_008)))
        }),
    );
    client.set_request_pipeline(Some(Arc::new(FailedPipeline)));
    client.add_rpc_hook(Arc::new(AfterResponseCounter(after_hook_calls.clone())));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, response::SYSTEM_ERROR);
    assert_eq!(response.remark.as_deref(), Some("pipeline failed"));
    assert_eq!(response.opaque, 75);
    assert_eq!(processor_calls.load(Ordering::SeqCst), 0);
    assert_eq!(after_hook_calls.load(Ordering::SeqCst), 0);
    client.shutdown().await;
}

struct InvokedFailureFlowHook {
    case: String,
    calls: Arc<Mutex<Vec<String>>>,
}

impl RpcHook for InvokedFailureFlowHook {
    fn before_request_result(&self, _: &str, _: &RemotingCommand) -> Result<(), RpcHookError> {
        self.calls.lock().unwrap().push("before".into());
        match self.case.as_str() {
            "before" => Err(RpcHookError::Failed("before failed".into())),
            "abort_before" => Err(RpcHookError::Abort {
                response_code: 831,
                remark: Some("before aborted".into()),
            }),
            _ => Ok(()),
        }
    }

    fn after_response_optional_result(
        &self,
        _: &str,
        _: &RemotingCommand,
        response: Option<&RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        let code = response.map_or_else(|| "null".into(), |response| response.code.to_string());
        self.calls.lock().unwrap().push(format!("after:{code}"));
        match self.case.as_str() {
            "after" => Err(RpcHookError::Failed("after failed".into())),
            "abort_after" => Err(RpcHookError::Abort {
                response_code: 832,
                remark: Some("after aborted".into()),
            }),
            _ => Ok(()),
        }
    }
}

struct InvokedFailureFlowPipeline {
    case: String,
    calls: Arc<Mutex<Vec<String>>>,
}

impl RequestPipeline for InvokedFailureFlowPipeline {
    fn execute(&self, _: &RemotingCommand) -> Result<(), i32> {
        Ok(())
    }

    fn execute_detailed(&self, _: &RemotingCommand) -> Result<(), RequestPipelineError> {
        self.calls.lock().unwrap().push("pipeline".into());
        match self.case.as_str() {
            "pipeline" => Err(RequestPipelineError::Failed("pipeline failed".into())),
            "abort_pipeline" => Err(RequestPipelineError::Abort {
                response_code: 833,
                remark: Some("pipeline aborted".into()),
            }),
            _ => Ok(()),
        }
    }
}

#[tokio::test]
async fn p18_inbound_hook_pipeline_and_processor_failures_match_java_flow_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/hooks.json")).unwrap();
    for case in fixture["inboundFailureFlows"]["cases"].as_array().unwrap() {
        let name = case["case"].as_str().unwrap().to_owned();
        let expected_calls = case["calls"].as_array().unwrap();
        let expected_calls = expected_calls
            .iter()
            .map(|call| call.as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let expected_present = case["responsePresent"].as_bool().unwrap();
        let expected_code = case["responseCode"].as_i64().map(|code| code as i32);
        let expected_opaque = case["responseOpaque"].as_i64().map(|opaque| opaque as i32);
        let expected_remark = case["responseRemark"].as_str().map(str::to_owned);
        let oneway = name.starts_with("oneway_");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = command(900_030);
            request.opaque = 71;
            if oneway {
                request.mark_oneway();
            }
            write_command(&mut stream, &request).await;
            if oneway {
                tokio::time::timeout(Duration::from_millis(100), try_read_command(&mut stream))
                    .await
                    .ok()
                    .flatten()
            } else {
                Some(read_command(&mut stream).await)
            }
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        client.add_rpc_hook(Arc::new(InvokedFailureFlowHook {
            case: name.clone(),
            calls: calls.clone(),
        }));
        client.set_request_pipeline(Some(Arc::new(InvokedFailureFlowPipeline {
            case: name.clone(),
            calls: calls.clone(),
        })));
        let processor_calls = calls.clone();
        let processor_case = name.clone();
        client.register_processor(
            900_030,
            Arc::new(move |_: RemotingCommand| {
                processor_calls.lock().unwrap().push("processor".into());
                if processor_case == "processor" || processor_case == "oneway_processor" {
                    Err("processor failed".into())
                } else {
                    Ok(Some(command(0)))
                }
            }),
        );
        assert!(
            client
                .is_address_reachable(
                    &address.to_string(),
                    Instant::now() + Duration::from_secs(2)
                )
                .await,
            "{name}: connection should be accepted"
        );

        let response = peer.await.unwrap();
        assert_eq!(
            response.is_some(),
            expected_present,
            "{name}: response presence"
        );
        if let Some(response) = response {
            assert_eq!(Some(response.code), expected_code, "{name}: response code");
            assert_eq!(Some(response.opaque), expected_opaque, "{name}: opaque");
            if expected_remark
                .as_deref()
                .is_some_and(|remark| remark != "java.lang.IllegalStateException")
            {
                assert_eq!(
                    response.remark.as_deref(),
                    expected_remark.as_deref(),
                    "{name}: remark"
                );
            } else if expected_remark.is_some() {
                assert!(
                    response.remark.is_some(),
                    "{name}: Java reports exception diagnostic"
                );
            }
        }
        assert_eq!(
            *calls.lock().unwrap(),
            expected_calls,
            "{name}: hook/processor order"
        );
        client.shutdown().await;
    }
}

#[tokio::test]
async fn p18_request_pipeline_mutates_request_before_later_stage_and_processor() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/hooks.json")).unwrap();
    let java_mutated = &fixture["requestPipelineMutation"];
    let expected_input_opaque = java_mutated["inputOpaque"].as_i64().unwrap() as i32;
    let expected_opaque = java_mutated["opaque"].as_i64().unwrap() as i32;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(900_014);
        request.opaque = expected_input_opaque;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let expected_code = java_mutated["code"].as_i64().unwrap() as i32;
    let expected_remark = java_mutated["remark"].as_str().unwrap().to_owned();
    let expected_ext_field = java_mutated["extFields"]["pipeline"]
        .as_str()
        .unwrap()
        .to_owned();
    let observed_request = Arc::new(std::sync::Mutex::new(None));
    client.set_request_pipeline(Some(Arc::new(PipelineRequestObserver(
        observed_request.clone(),
    ))));
    client.pipe_request_pipeline(Arc::new(MutatingPipelineStage {
        code: expected_code,
        remark: expected_remark.clone(),
        opaque: expected_opaque,
        ext_field_key: "pipeline",
        ext_field_value: "changed",
    }));
    client.register_processor(
        900_014,
        Arc::new(|request: RemotingCommand| {
            Ok(Some(RemotingCommand {
                code: request.code,
                remark: request.remark,
                ext_fields: request.ext_fields,
                ..RemotingCommand::default()
            }))
        }),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, expected_code);
    assert_eq!(response.opaque, expected_opaque);
    assert_eq!(response.remark.as_deref(), Some(expected_remark.as_str()));
    assert_eq!(
        response.ext_fields.as_ref().unwrap()["pipeline"],
        expected_ext_field
    );
    {
        let observed_request = observed_request.lock().unwrap();
        let observed_request = observed_request.as_ref().unwrap();
        assert_eq!(observed_request.code, expected_code);
        assert_eq!(observed_request.opaque, expected_opaque);
        assert_eq!(
            observed_request.remark.as_deref(),
            Some(expected_remark.as_str())
        );
        assert_eq!(
            observed_request.ext_fields.as_ref().unwrap()["pipeline"],
            expected_ext_field
        );
    }
    client.shutdown().await;
}

struct ThreadRequestExecutor {
    executions: Arc<AtomicUsize>,
}

impl RequestExecutor for ThreadRequestExecutor {
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(task);
        Ok(())
    }
}

struct RejectRequestExecutor;

impl RequestExecutor for RejectRequestExecutor {
    fn execute(&self, _task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
        Err("executor saturated".into())
    }
}

#[tokio::test]
async fn p3_processor_specific_and_default_executors_route_or_reject_tasks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut first = command(820);
        first.opaque = 61;
        write_command(&mut stream, &first).await;
        let mut second = command(821);
        second.opaque = 62;
        write_command(&mut stream, &second).await;
        let one = read_command(&mut stream).await;
        let two = read_command(&mut stream).await;
        [one, two]
    });
    let client = RemotingClient::with_config(ClientConfig {
        callback_executor_threads: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    let (processor_tx, processor_rx) = tokio::sync::oneshot::channel();
    let processor_tx = Arc::new(std::sync::Mutex::new(Some(processor_tx)));
    let executions = Arc::new(AtomicUsize::new(0));
    client.register_processor_with_executor(
        820,
        Arc::new(move |_: RemotingCommand| {
            if let Some(sender) = processor_tx.lock().unwrap().take() {
                let _ = sender.send(std::thread::current().id());
            }
            Ok(Some(command(822)))
        }),
        Some(Arc::new(ThreadRequestExecutor {
            executions: executions.clone(),
        })),
    );
    client.register_default_processor_with_executor(
        Some(Arc::new(|_: RemotingCommand| Ok(Some(command(823))))),
        Some(Arc::new(RejectRequestExecutor)),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let processor_thread = tokio::time::timeout(Duration::from_secs(2), processor_rx)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(processor_thread, std::thread::current().id());
    let responses = peer.await.unwrap();
    let responses = responses
        .into_iter()
        .map(|response| (response.opaque, response.code))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(responses.get(&61), Some(&822));
    assert_eq!(responses.get(&62), Some(&response::SYSTEM_BUSY));
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_request_pipeline_and_processor_rejection_return_configured_codes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outbound = read_command(&mut stream).await;
        let mut pipeline_request = command(78);
        pipeline_request.opaque = 41;
        write_command(&mut stream, &pipeline_request).await;
        let pipeline_reply = read_command(&mut stream).await;
        let mut rejected_request = command(77);
        rejected_request.opaque = 42;
        write_command(&mut stream, &rejected_request).await;
        let rejected_reply = read_command(&mut stream).await;
        let mut response = command(9);
        response.opaque = outbound.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        (pipeline_reply, rejected_reply)
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.register_processor(77, Arc::new(RejectingProcessor));
    client.register_processor(78, Arc::new(|_: RemotingCommand| Ok(None)));
    client.set_request_pipeline(Some(Arc::new(|_: &RemotingCommand| Err(733))));
    let deadline = Instant::now() + Duration::from_secs(2);
    assert_eq!(
        client
            .invoke(&address.to_string(), command(1), deadline)
            .await
            .unwrap()
            .code,
        9
    );
    client.set_request_pipeline(None);
    client.shutdown().await;
    let (pipeline_reply, rejected_reply) = peer.await.unwrap();
    assert_eq!(pipeline_reply.code, 733);
    assert!(pipeline_reply.is_response());
    assert_eq!(rejected_reply.code, 2);
    assert!(rejected_reply.is_response());
}

#[tokio::test]
async fn p18_request_context_preserves_channel_identity_through_pipeline_and_processor() {
    type WriteCompletion = tokio::task::JoinHandle<Result<(), TransportError>>;

    struct ContextPipeline {
        observed: Arc<std::sync::Mutex<Vec<ChannelHandle>>>,
        write_completions: Arc<std::sync::Mutex<Vec<WriteCompletion>>>,
        write_command: bool,
    }

    impl RequestPipeline for ContextPipeline {
        fn execute(&self, _: &RemotingCommand) -> Result<(), i32> {
            Ok(())
        }

        fn execute_detailed_with_context(
            &self,
            context: &RequestContext,
            _: &RemotingCommand,
        ) -> Result<(), RequestPipelineError> {
            self.observed
                .lock()
                .unwrap()
                .push(context.channel().clone());
            assert_eq!(context.remote_addr(), context.channel().remote_addr());
            assert!(context.channel().is_open());
            assert!(context.channel().is_active());
            assert!(context.channel().is_writable());
            if self.write_command {
                let mut command = command(80);
                command.opaque = 90;
                let completion = context
                    .write_and_flush(&command)
                    .map_err(|error| RequestPipelineError::Failed(error.to_string()))?;
                self.write_completions
                    .lock()
                    .unwrap()
                    .push(tokio::spawn(completion));
            }
            Ok(())
        }
    }

    struct ContextProcessor(Arc<std::sync::Mutex<Vec<ChannelHandle>>>);

    impl RequestProcessor for ContextProcessor {
        fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
            Ok(None)
        }

        fn process_with_request_context(
            &self,
            context: &RequestContext,
            _: RemotingCommand,
        ) -> Result<Option<RemotingCommand>, String> {
            self.0.lock().unwrap().push(context.channel().clone());
            Ok(Some(command(81)))
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outbound = read_command(&mut stream).await;
        let mut request = command(78);
        request.opaque = 41;
        write_command(&mut stream, &request).await;
        let extra = read_command(&mut stream).await;
        let response = read_command(&mut stream).await;
        let mut invoke_reply = command(9);
        invoke_reply.opaque = outbound.opaque;
        invoke_reply.mark_response();
        write_command(&mut stream, &invoke_reply).await;
        (extra, response)
    });

    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let write_completions = Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.register_processor(78, Arc::new(ContextProcessor(observed.clone())));
    client.set_request_pipeline(Some(Arc::new(ContextPipeline {
        observed: observed.clone(),
        write_completions: write_completions.clone(),
        write_command: false,
    })));
    client.pipe_request_pipeline(Arc::new(ContextPipeline {
        observed: observed.clone(),
        write_completions: write_completions.clone(),
        write_command: true,
    }));

    assert_eq!(
        client
            .invoke(
                &address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .code,
        9
    );
    let (extra, response) = peer.await.unwrap();
    assert_eq!((extra.code, extra.opaque), (80, 90));
    assert!(!extra.is_response());
    assert_eq!((response.code, response.opaque), (81, 41));
    assert!(response.is_response());
    let write_completions = std::mem::take(&mut *write_completions.lock().unwrap());
    assert_eq!(write_completions.len(), 1);
    assert!(
        write_completions
            .into_iter()
            .next()
            .unwrap()
            .await
            .unwrap()
            .is_ok()
    );

    {
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 3);
        assert!(observed.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(observed[0].remote_addr(), address.to_string());
    }
    client.shutdown().await;
}

struct RejectingProcessor;
impl RequestProcessor for RejectingProcessor {
    fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        Ok(None)
    }

    fn reject_request(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn p3_u01_socks5_proxy_tunnels_a_remoting_request() {
    socks5_proxy_tunnels_a_remoting_request(true).await;
}

#[tokio::test]
async fn p3_u01_socks5_no_auth_proxy_tunnels_a_remoting_request() {
    socks5_proxy_tunnels_a_remoting_request(false).await;
}

#[tokio::test]
async fn p18_proxy_connect_timeout_uses_live_first_use_value_and_caches_it() {
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let proxy_peer = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut client, _) = proxy.accept().await.unwrap();
            let mut greeting = [0; 2];
            client.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting[0], 5);
            let mut methods = vec![0; usize::from(greeting[1])];
            client.read_exact(&mut methods).await.unwrap();
            assert_eq!(methods, [0]);
            let mut byte = [0; 1];
            let _ = client.read(&mut byte).await;
        }
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        client_socket_send_buffer_size: 64 * 1024,
        client_socket_receive_buffer_size: 64 * 1024,
        socks_proxy_config: format!(r#"{{"127.0.0.0/8":{{"addr":"{}"}}}}"#, proxy_address),
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();

    client.set_connect_timeout_millis(125);
    let first = tokio::time::timeout(
        Duration::from_secs(1),
        client.is_address_reachable("127.0.0.1:19001", Instant::now() + Duration::from_secs(5)),
    )
    .await;

    client.set_connect_timeout_millis(2000);
    let second = tokio::time::timeout(
        Duration::from_secs(1),
        client.is_address_reachable("127.0.0.1:19002", Instant::now() + Duration::from_secs(5)),
    )
    .await;
    client.shutdown().await;
    tokio::time::timeout(Duration::from_secs(2), proxy_peer)
        .await
        .expect("scripted proxy did not observe both timed out connections")
        .unwrap();

    assert_eq!(
        first,
        Ok(false),
        "first proxy bootstrap missed the live value"
    );
    assert_eq!(
        second,
        Ok(false),
        "cached proxy bootstrap changed its timeout"
    );
}

async fn socks5_proxy_tunnels_a_remoting_request(username_password: bool) {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap();
    let broker_peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let proxy_peer = tokio::spawn(async move {
        let (mut client, _) = proxy.accept().await.unwrap();
        let mut greeting_header = [0; 2];
        client.read_exact(&mut greeting_header).await.unwrap();
        assert_eq!(greeting_header[0], 5);
        let mut methods = vec![0; usize::from(greeting_header[1])];
        client.read_exact(&mut methods).await.unwrap();
        if username_password {
            assert_eq!(methods, [0, 2]);
            client.write_all(&[5, 2]).await.unwrap();
            let mut auth_header = [0; 2];
            client.read_exact(&mut auth_header).await.unwrap();
            assert_eq!(&auth_header, &[1, 6]);
            let mut username = [0; 6];
            client.read_exact(&mut username).await.unwrap();
            assert_eq!(&username, b"rocket");
            let mut password_length = [0; 1];
            client.read_exact(&mut password_length).await.unwrap();
            assert_eq!(password_length[0], 6);
            let mut password = [0; 6];
            client.read_exact(&mut password).await.unwrap();
            assert_eq!(&password, b"secret");
            client.write_all(&[1, 0]).await.unwrap();
        } else {
            assert_eq!(methods, [0]);
            client.write_all(&[5, 0]).await.unwrap();
        }
        let mut request = [0; 10];
        client.read_exact(&mut request[..4]).await.unwrap();
        assert_eq!(&request[..4], &[5, 1, 0, 1]);
        client.read_exact(&mut request[4..]).await.unwrap();
        let target = std::net::SocketAddr::new(
            std::net::Ipv4Addr::new(request[4], request[5], request[6], request[7]).into(),
            u16::from_be_bytes([request[8], request[9]]),
        );
        assert_eq!(target, broker_address);
        let upstream = tokio::net::TcpStream::connect(target).await.unwrap();
        client
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 1])
            .await
            .unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut upstream_read, mut upstream_write) = upstream.into_split();
        let a = tokio::io::copy(&mut client_read, &mut upstream_write);
        let b = tokio::io::copy(&mut upstream_read, &mut client_write);
        let _ = tokio::join!(a, b);
    });
    let proxy_config = if username_password {
        format!(
            r#"{{"127.0.0.0/8":{{"addr":"{}","username":"rocket","password":"secret"}}}}"#,
            proxy_address
        )
    } else {
        format!(r#"{{"127.0.0.0/8":{{"addr":"{}"}}}}"#, proxy_address)
    };
    let config = ClientConfig {
        socks_proxy_config: proxy_config,
        client_socket_send_buffer_size: 64 * 1024,
        client_socket_receive_buffer_size: 64 * 1024,
        ..ClientConfig::default()
    };
    let client = RemotingClient::with_config(config).unwrap();
    client.start().unwrap();
    let response = client
        .invoke(
            &broker_address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(response.code, 42);
    client.shutdown().await;
    broker_peer.await.unwrap();
    proxy_peer.await.unwrap();
}

#[tokio::test]
async fn p3_u01_tls_rejects_untrusted_ca() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.key_pair.serialize_der(),
                ),
            ),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), acceptor.accept(socket)).await;
    });
    let mut tls = TlsConfig::default();
    tls.ca_pem = Some(Vec::new());
    tls.test_mode = false;
    tls.auth_server = true;
    let client = RemotingClient::new(TransportMode::Tls(tls));
    client.start().unwrap();
    let result = client
        .invoke(
            &format!("localhost:{port}"),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    assert!(matches!(
        result,
        Err(TransportError::ConnectRequest {
            remote_addr,
            cause: Some(_),
        }) if remote_addr == format!("localhost:{port}")
    ));
    client.shutdown().await;
    peer.await.unwrap();
}
