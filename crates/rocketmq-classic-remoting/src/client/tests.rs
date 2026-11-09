use super::*;
use crate::codec::frame::DEFAULT_MAX_FRAME_SIZE;

#[test]
fn callback_executor_thread_settings_match_java_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/runtime/client-config.json"
    ))
    .unwrap();

    for case in fixture["callbackThreadsConstruction"].as_array().unwrap() {
        let configured = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
        let expected = usize::try_from(case["effective"].as_i64().unwrap()).unwrap();
        assert_eq!(case["outcome"], "OK");
        assert_eq!(callback_worker_limit(configured), expected);

        let client = RemotingClient::with_config(ClientConfig {
            callback_executor_threads: configured,
            ..ClientConfig::default()
        })
        .unwrap();
        assert_eq!(client.inner.config.callback_executor_threads, configured);
    }
}

#[test]
fn java_tls_property_file_is_reloaded_for_each_client_context() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/security/tls-settings-defaults.json"
    ))
    .unwrap();
    let expected_modes = fixture["propertyFileReload"]["observedTestModes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_bool().unwrap())
        .collect::<Vec<_>>();
    let expected_auth_server = fixture["propertyFileReload"]["observedAuthServer"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_bool().unwrap())
        .collect::<Vec<_>>();

    let directory = std::env::temp_dir().join(format!(
        "rocketmq-tls-context-reload-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let properties_file = directory.join("tls.properties");
    std::fs::write(
        &properties_file,
        "tls.test.mode.enable=true\ntls.client.authServer=false\n",
    )
    .unwrap();
    let properties = HashMap::new();
    let mut tls = TlsConfig::from_java_properties(&properties, Some(&properties_file)).unwrap();
    // This test exercises property reload, not the machine's native CA
    // store. Supply a pinned local trust anchor so it is reproducible on
    // hosts that do not expose system trust roots.
    tls.ca_pem = Some(include_bytes!("../../tests/fixtures/tls/java-server.pem").to_vec());

    let mut observed_modes = Vec::new();
    let mut observed_auth_server = Vec::new();
    for (test_mode, auth_server) in [(true, Some(false)), (false, Some(true)), (true, None)] {
        let mut snapshot = format!("tls.test.mode.enable={test_mode}\n");
        if let Some(auth_server) = auth_server {
            snapshot.push_str(&format!("tls.client.authServer={auth_server}\n"));
        }
        std::fs::write(&properties_file, snapshot).unwrap();
        let client = RemotingClient::with_config(ClientConfig {
            transport: TransportMode::Tls(tls.clone()),
            ..ClientConfig::default()
        })
        .unwrap();
        let TransportMode::Tls(context_config) = &client.inner.config.transport else {
            unreachable!("TLS configuration should be retained by the remoting client");
        };
        observed_modes.push(context_config.test_mode);
        observed_auth_server.push(context_config.auth_server);
    }
    std::fs::remove_dir_all(directory).unwrap();
    assert_eq!(observed_modes, expected_modes);
    assert_eq!(observed_auth_server, expected_auth_server);
}

#[tokio::test]
async fn java_frame_max_length_property_is_applied_to_new_connections() {
    const CHILD_MARKER: &str = "ROCKETMQ_FRAME_MAX_LENGTH_CHILD";
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/runtime/netty-frame-limit.json"
    ))
    .unwrap();
    let expected = fixture["configured"].as_u64().unwrap() as usize;

    if std::env::var_os(CHILD_MARKER).is_some() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream
                .write_all(&(expected as u32).to_be_bytes())
                .await
                .unwrap();
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        });
        let client = RemotingClient::with_config(ClientConfig::default()).unwrap();
        client.start().unwrap();
        let connection = client
            .inner
            .connection(&address, Instant::now() + std::time::Duration::from_secs(3))
            .await
            .unwrap();
        let oversized = RemotingCommand {
            body: Some(vec![0; expected]),
            ..RemotingCommand::default()
        };
        let outbound = connection
            .encode_command(&oversized, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE)
            .unwrap();
        assert!(outbound.len() > expected);
        let inbound_rejected = tokio::time::timeout(std::time::Duration::from_secs(3), peer).await;
        assert!(
            inbound_rejected.is_ok(),
            "connection did not close after a frame exceeded the configured inbound limit"
        );
        inbound_rejected.unwrap().unwrap();
        assert!(connection.is_closed());
        connection.join().await;
        client.shutdown().await;
        return;
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("client::tests::java_frame_max_length_property_is_applied_to_new_connections")
        .arg("--nocapture")
        .env(config::JAVA_FRAME_MAX_LENGTH_PROPERTY, expected.to_string())
        .env(CHILD_MARKER, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child test failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn channel_event_peer_address_redacts_uri_userinfo() {
    assert_eq!(
        redact_peer_address("tcp://user:secret@broker.example:10911/path"),
        "tcp://[REDACTED]@broker.example:10911/path"
    );
    assert_eq!(
        redact_peer_address("[2001:db8::1]:10911"),
        "[2001:db8::1]:10911"
    );
}

#[test]
fn netty_event_type_methods_match_invoked_java_enum() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/runtime/client-config.json"
    ))
    .unwrap();
    let cases = fixture["nettyEventTypeMethodCases"].as_array().unwrap();
    let values = NettyEventType::values();
    let expected_names = cases
        .iter()
        .find(|case| case["case"] == "constant")
        .unwrap()["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values.iter().map(|value| value.name()).collect::<Vec<_>>(),
        expected_names
    );
    let mut mutated_values = NettyEventType::values();
    mutated_values[0] = values[values.len() - 1];
    assert_eq!(NettyEventType::values()[0], values[0]);

    for case in cases {
        match case["case"].as_str().unwrap() {
            "constant" => {
                let name = case["name"].as_str().unwrap();
                let value = NettyEventType::value_of_name(Some(name)).unwrap();
                let ordinal = case["ordinal"].as_i64().unwrap() as i32;
                assert_eq!(value.name(), name, "{case}");
                assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                assert_eq!(value.ordinal(), ordinal, "{case}");
                assert_eq!(value, values[ordinal as usize], "{case}");
                assert_eq!(case["valueOfName"], name, "{case}");
                assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                let next = values[(ordinal as usize + 1) % values.len()];
                assert_eq!(
                    value.java_compare_to(next),
                    case["compareNext"].as_i64().unwrap() as i32,
                    "{case}"
                );
            }
            "lookup" => match NettyEventType::value_of_name(case["input"].as_str()) {
                Ok(value) => {
                    assert_eq!(case["outcome"], "OK", "{case}");
                    assert_eq!(case["decoded"], value.name(), "{case}");
                }
                Err(error) => {
                    let expected = if case["input"].is_null() {
                        "NullPointerException"
                    } else {
                        "IllegalArgumentException"
                    };
                    assert_eq!(case["outcome"], expected, "{case}");
                    assert_eq!(case["message"], error.to_string(), "{case}");
                }
            },
            other => panic!("unknown Java NettyEventType case: {other}"),
        }
    }
}

#[test]
fn mutable_hooks_are_ordered_and_legacy_hooks_observe_prior_mutation() {
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
                .insert("mutated".into(), "yes".into());
            Ok(())
        }
    }
    let observed = Arc::new(AtomicBool::new(false));
    let flag = observed.clone();
    let hooks: Vec<Arc<dyn RpcHook>> = vec![
        Arc::new(Mutator),
        Arc::new(move |_: &str, command: &RemotingCommand| {
            assert_eq!(command.ext_fields.as_ref().unwrap()["mutated"], "yes");
            flag.store(true, Ordering::Release);
        }),
    ];
    let mut command = RemotingCommand::default();
    run_before_rpc_hooks(&hooks, "peer", &mut command).unwrap();
    assert!(observed.load(Ordering::Acquire));
}
use tokio::sync::Notify;

#[tokio::test]
async fn async_active_listener_is_owned_panic_isolated_and_drained_after_shutdown() {
    struct Listener {
        calls: std::sync::atomic::AtomicUsize,
        live: Arc<std::sync::atomic::AtomicUsize>,
        entered: Notify,
        exited: Arc<Notify>,
        release: Arc<Notify>,
    }
    struct Live {
        live: Arc<std::sync::atomic::AtomicUsize>,
        exited: Arc<Notify>,
    }
    impl Drop for Live {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
            self.exited.notify_one();
        }
    }
    impl ChannelEventListener for Listener {
        fn on_active_async<'a>(
            &'a self,
            _: &'a str,
            _: &'a ChannelHandle,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                assert_ne!(call, 0, "scripted active listener panic");
                self.live.fetch_add(1, Ordering::SeqCst);
                let _live = Live {
                    live: self.live.clone(),
                    exited: self.exited.clone(),
                };
                self.entered.notify_one();
                self.release.notified().await;
            })
        }
    }
    let release = Arc::new(Notify::new());
    let listener = Arc::new(Listener {
        calls: std::sync::atomic::AtomicUsize::new(0),
        live: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        entered: Notify::new(),
        exited: Arc::new(Notify::new()),
        release: release.clone(),
    });
    let (sender, receiver) = mpsc::channel(2);
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let (worker_stopped, _) = tokio::sync::watch::channel(false);
    let worker =
        start_channel_event_worker(Some(listener.clone()), receiver, shutdown, worker_stopped);
    sender
        .send(ChannelEvent {
            remote_addr: "broker".into(),
            event_type: NettyEventType::Active,
            channel: ChannelHandle::pending("broker"),
        })
        .await
        .unwrap();
    sender
        .send(ChannelEvent {
            remote_addr: "broker".into(),
            event_type: NettyEventType::Active,
            channel: ChannelHandle::pending("broker"),
        })
        .await
        .unwrap();
    listener.entered.notified().await;
    assert_eq!(listener.calls.load(Ordering::SeqCst), 2);
    assert_eq!(listener.live.load(Ordering::SeqCst), 1);
    stop.send_replace(true);
    let mut worker = worker;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut worker)
            .await
            .is_err()
    );
    release.notify_one();
    worker.await.unwrap();
    if listener.live.load(Ordering::SeqCst) != 0 {
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            listener.exited.notified(),
        )
        .await
        .expect("active listener future did not finish after release");
    }
    assert_eq!(listener.live.load(Ordering::SeqCst), 0);
}

#[test]
fn owned_processor_cleanup_preserves_replacement_even_for_same_handle() {
    let client = RemotingClient::new(TransportMode::Plain);
    let processor: Arc<dyn RequestProcessor> = Arc::new(|_: RemotingCommand| Ok(None));
    let old = client.register_owned_processor(39, processor.clone());
    let replacement = client.register_owned_processor(39, processor.clone());
    drop(old);
    assert!(client.with_processor_table(|table| table.contains_key(&39)));
    drop(replacement);
    assert!(!client.with_processor_table(|table| table.contains_key(&39)));
    let lease = client.register_owned_processor(39, processor.clone());
    client.register_processor(39, processor);
    drop(lease);
    assert!(client.with_processor_table(|table| table.contains_key(&39)));
}

#[test]
fn name_server_scan_batch_matches_java_abort_policy_submission_order() {
    let addresses: Vec<_> = (0..50).map(|index| format!("address-{index}")).collect();
    let first = name_server_scan_batch(addresses.clone(), 42);
    assert_eq!(first.len(), 42);
    assert_eq!(first.first().unwrap(), "address-0");
    assert_eq!(first.last().unwrap(), "address-41");

    // The rejected submission aborts that Java scan loop. A later tick
    // begins at the first configured address instead of rotating skipped
    // entries ahead of previously submitted ones.
    let second = name_server_scan_batch(addresses.clone(), 5);
    assert_eq!(second, addresses[..5]);
    assert!(!second.contains(&"address-42".to_owned()));
    assert!(name_server_scan_batch(addresses.clone(), 0).is_empty());
    assert_eq!(name_server_scan_batch(addresses, 42)[0], "address-0");
}

use std::sync::atomic::AtomicUsize;

fn test_command() -> RemotingCommand {
    RemotingCommand::default()
}

fn install_test_connection(
    client: &RemotingClient,
    address: &str,
    stream: connection::BoxedStream,
    generation: u64,
) -> Arc<connection::Connection> {
    let connection = connection::Connection::spawn(
        stream,
        generation,
        Arc::downgrade(&client.inner),
        client.inner.frame_max_length().unwrap(),
        (64 * 1024, 32 * 1024),
        address.to_owned(),
        false,
    );
    client
        .inner
        .connections
        .lock()
        .unwrap()
        .insert(address.to_owned(), connection.clone());
    connection
}

#[tokio::test]
async fn sync_response_is_read_while_request_write_is_pending() {
    use tokio::io::{AsyncWriteExt, duplex};

    let java_fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../../fixtures/runtime/hooks.json")).unwrap();
    assert_eq!(
        java_fixture["outboundWriteFlows"]["syncReturnedBeforeWriteCompletion"],
        true
    );
    let address = "127.0.0.1:10911";
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let (client_stream, mut peer_stream) = duplex(1);
    let connection = install_test_connection(&client, address, Box::new(client_stream), 9901);
    let request = RemotingCommand {
        code: 1,
        body: Some(vec![0; 1024 * 1024]),
        ..RemotingCommand::default()
    };
    let invoke_client = client.clone();
    let mut invoke = tokio::spawn(async move {
        invoke_client
            .invoke_sync_with_deadline(
                address,
                request,
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while client.is_channel_writable(address) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("large request did not stall the tiny socket writer");
    assert!(connection.queued_write_bytes() > 0);

    let mut response = RemotingCommand {
        opaque: 0,
        ..RemotingCommand::default()
    };
    response.mark_response();
    peer_stream
        .write_all(&crate::codec::frame::encode(&response).unwrap())
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_millis(250), &mut invoke)
        .await
        .expect("response handling waited for the blocked outbound write")
        .unwrap()
        .unwrap();
    assert!(result.is_response());
    assert!(
        connection.queued_write_bytes() > 0,
        "the response must complete before the queued request finishes writing"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn oneway_write_remains_queued_after_the_call_deadline() {
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, duplex};

    let java_fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../../fixtures/runtime/hooks.json")).unwrap();
    let java_write = &java_fixture["outboundWriteFlows"];
    assert_eq!(java_write["onewayReturnedBeforeWriteCompletion"], true);
    assert_eq!(java_write["onewayPermitsWhileWritePending"], 0);
    assert_eq!(java_write["onewayPermitsAfterWriteCompletion"], 1);
    let address = "127.0.0.1:10912";
    let client = RemotingClient::with_config(ClientConfig {
        oneway_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    const BODY_LEN: usize = 256 * 1024;
    let (client_stream, mut peer_stream) = duplex(4096);
    let connection = install_test_connection(&client, address, Box::new(client_stream), 9902);
    let deadline = Instant::now() + std::time::Duration::from_millis(200);
    client
        .invoke_oneway(
            address,
            RemotingCommand {
                code: 2,
                body: Some(vec![0; BODY_LEN]),
                ..RemotingCommand::default()
            },
            deadline,
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while connection.queued_write_bytes() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one-way request was not enqueued");
    assert_eq!(client.inner.oneway_admission.available_permits(), 0);
    tokio::time::sleep_until(deadline + std::time::Duration::from_millis(50)).await;

    let sent = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut prefix = [0; 4];
        peer_stream.read_exact(&mut prefix).await.unwrap();
        let frame_length = u32::from_be_bytes(prefix) as usize;
        let mut framed = BytesMut::with_capacity(frame_length + 4);
        framed.extend_from_slice(&prefix);
        framed.resize(frame_length + 4, 0);
        peer_stream.read_exact(&mut framed[4..]).await.unwrap();
        crate::codec::frame::decode_next(&mut framed)
            .unwrap()
            .unwrap()
    })
    .await
    .expect("queued one-way frame did not finish after the peer drained it");
    assert!(sent.is_oneway());
    assert_eq!(sent.body.as_ref().unwrap().len(), BODY_LEN);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while client.inner.oneway_admission.available_permits() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one-way permit was not released when the queued write completed");
    client.shutdown().await;
}

#[tokio::test]
async fn sync_wait_timeout_retains_pending_admission_until_terminal_cleanup() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for late_reply in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (seen_tx, seen_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut len = [0; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut buffer = bytes::BytesMut::from(&len[..]);
            buffer.resize(u32::from_be_bytes(len) as usize + 4, 0);
            stream.read_exact(&mut buffer[4..]).await.unwrap();
            let request = crate::codec::frame::decode_next(&mut buffer)
                .unwrap()
                .unwrap();
            seen_tx.send(request.opaque).unwrap();
            release_rx.await.unwrap();
            if late_reply {
                let mut reply = test_command();
                reply.opaque = request.opaque;
                reply.mark_response();
                stream
                    .write_all(&crate::codec::frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
            }
        });
        let client = RemotingClient::with_config(ClientConfig {
            close_socket_if_timeout: false,
            async_semaphore_value: 1,
            ..Default::default()
        })
        .unwrap();
        client.start().unwrap();
        let result = client
            .invoke_sync(&addr, test_command(), std::time::Duration::from_millis(100))
            .await;
        assert!(result.unwrap_err().is_timeout());
        let opaque = seen_rx.await.unwrap();
        let generation = {
            let pending = client.inner.pending.lock().unwrap();
            let entry = pending
                .get(&opaque)
                .expect("sync timeout must retain pending entry");
            assert!(entry.deadline > Instant::now());
            entry.generation
        };
        assert_eq!(client.inner.async_admission.available_permits(), 0);
        if !late_reply {
            client.inner.expire_pending(opaque, generation);
        }
        release_tx.send(()).unwrap();
        peer.await.unwrap();
        time::timeout(std::time::Duration::from_secs(1), async {
            while !client.inner.pending.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(client.inner.async_admission.available_permits(), 1);
        client.shutdown().await;
        assert!(client.inner.pending.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn oneway_never_registers_pending_even_when_peer_replies() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut frame = bytes::BytesMut::from(&length[..]);
        frame.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut frame[4..]).await.unwrap();
        let request = crate::codec::frame::decode_next(&mut frame)
            .unwrap()
            .unwrap();
        assert!(request.is_oneway());
        seen_tx.send(request.opaque).unwrap();
        reply_rx.await.unwrap();
        let mut response = RemotingCommand {
            opaque: request.opaque,
            ..RemotingCommand::default()
        };
        response.mark_response();
        stream
            .write_all(&crate::codec::frame::encode(&response).unwrap())
            .await
            .unwrap();
    });

    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client
        .invoke_oneway(
            &address,
            test_command(),
            Instant::now() + std::time::Duration::from_secs(2),
        )
        .unwrap();
    let opaque = seen_rx.await.unwrap();
    assert!(!client.inner.pending.lock().unwrap().contains_key(&opaque));
    assert!(client.inner.pending.lock().unwrap().is_empty());
    reply_tx.send(()).unwrap();
    peer.await.unwrap();
    assert!(client.inner.pending.lock().unwrap().is_empty());
    client.shutdown().await;
}

#[tokio::test]
async fn late_oneway_response_does_not_complete_concurrent_pending_request() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn read_request(stream: &mut tokio::net::TcpStream) -> RemotingCommand {
        let mut length = [0u8; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut frame = bytes::BytesMut::from(&length[..]);
        frame.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut frame[4..]).await.unwrap();
        crate::codec::frame::decode_next(&mut frame)
            .unwrap()
            .unwrap()
    }

    async fn reply(stream: &mut tokio::net::TcpStream, opaque: i32) {
        let mut response = RemotingCommand {
            opaque,
            ..RemotingCommand::default()
        };
        response.mark_response();
        stream
            .write_all(&crate::codec::frame::encode(&response).unwrap())
            .await
            .unwrap();
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (pending_tx, pending_rx) = oneshot::channel();
    let (oneway_tx, oneway_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let oneway = read_request(&mut stream).await;
        assert!(oneway.is_oneway());
        oneway_tx.send(()).unwrap();
        let pending = read_request(&mut stream).await;
        assert!(!pending.is_oneway());
        assert_ne!(oneway.opaque, pending.opaque);
        pending_tx.send(pending.opaque).unwrap();
        release_rx.await.unwrap();
        reply(&mut stream, oneway.opaque).await;
        let probe = read_request(&mut stream).await;
        reply(&mut stream, probe.opaque).await;
        // The probe reply acknowledges that the reader passed the late oneway reply.
        (pending.opaque, stream)
    });

    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let mut oneway = test_command();
    oneway.opaque = 777;
    client
        .invoke_oneway(
            &address,
            oneway,
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    oneway_rx.await.unwrap();
    let invoke_client = client.clone();
    let invoke_address = address.clone();
    let pending = tokio::spawn(async move {
        invoke_client
            .invoke(
                &invoke_address,
                test_command(),
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .await
    });
    let opaque = pending_rx.await.unwrap();
    assert!(client.inner.pending.lock().unwrap().contains_key(&opaque));
    release_tx.send(()).unwrap();
    let probe = client
        .invoke(
            &address,
            test_command(),
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_ne!(probe.opaque, opaque);
    assert!(
        !pending.is_finished(),
        "late oneway response completed pending invoke"
    );
    assert!(client.inner.pending.lock().unwrap().contains_key(&opaque));
    let (target, mut stream) = peer.await.unwrap();
    assert_eq!(target, opaque);
    reply(&mut stream, opaque).await;
    assert_eq!(pending.await.unwrap().unwrap().opaque, opaque);
    assert!(client.inner.pending.lock().unwrap().is_empty());
    client.shutdown().await;
}

#[tokio::test]
async fn oneway_connection_acquisition_enters_connect_before_return() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client
        .invoke_oneway(
            &address.to_string(),
            test_command(),
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
    {
        let connecting = client.inner.connecting.lock().unwrap();
        let gate = connecting
            .get(&address.to_string())
            .and_then(Weak::upgrade)
            .expect("acquisition should register its single-flight gate before return");
        assert!(gate.try_lock().is_err(), "dial future should hold the gate");
    }
    client.shutdown().await;
}

#[tokio::test]
async fn spawned_connection_future_is_polled_before_return() {
    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let polls_for_future = polls.clone();
    let task = spawn_after_initial_poll(async move {
        polls_for_future.fetch_add(1, Ordering::SeqCst);
        std::future::pending::<()>().await;
    })
    .expect("pending future should be spawned");
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    task.abort();
    let _ = task.await;
}

#[test]
fn stale_name_server_probe_does_not_override_current_generation_availability() {
    assert_eq!(
        name_server_probe_availability(Ok(Some(7)), Some(7)),
        Some(true)
    );
    assert_eq!(name_server_probe_availability(Ok(Some(7)), Some(8)), None);
    assert_eq!(name_server_probe_availability(Ok(None), Some(8)), None);
    assert_eq!(
        name_server_probe_availability(Ok(Some(7)), None),
        Some(false)
    );
    assert_eq!(name_server_probe_availability(Ok(None), None), Some(false));
    assert_eq!(
        name_server_probe_availability(
            Err(TransportError::Connect("probe failed".into())),
            Some(8)
        ),
        None
    );
    assert_eq!(
        name_server_probe_availability(Err(TransportError::Connect("probe failed".into())), None),
        None
    );
}

#[tokio::test]
async fn admission_matches_java_zero_timeout_and_wait_timeout_categories() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
    assert_eq!(
        client
            .inner
            .acquire_admission(semaphore.clone(), Instant::now())
            .await
            .unwrap_err(),
        TransportError::TooManyRequests
    );
    assert!(
        client
            .inner
            .acquire_admission(
                semaphore,
                Instant::now() + std::time::Duration::from_millis(10),
            )
            .await
            .unwrap_err()
            .is_timeout()
    );
    let available = Arc::new(tokio::sync::Semaphore::new(1));
    let _permit = client
        .inner
        .acquire_admission(available.clone(), Instant::now())
        .await
        .unwrap();
    assert_eq!(
        client
            .inner
            .acquire_admission(available, Instant::now())
            .await
            .unwrap_err(),
        TransportError::TooManyRequests
    );
    client.shutdown().await;
}

#[tokio::test]
async fn signed_semaphore_settings_match_java_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/runtime/client-config.json"
    ))
    .unwrap();
    let defaults = ClientConfig::default();
    assert_eq!(
        fixture["remotingSemaphoreConfig"]["defaults"]["async"].as_i64(),
        Some(i64::from(defaults.async_semaphore_value))
    );
    assert_eq!(
        fixture["remotingSemaphoreConfig"]["defaults"]["oneway"].as_i64(),
        Some(i64::from(defaults.oneway_semaphore_value))
    );

    for case in fixture["remotingSemaphoreConfig"]["cases"]
        .as_array()
        .unwrap()
    {
        let value = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
        assert_eq!(case["async"].as_i64(), Some(i64::from(value)));
        assert_eq!(case["oneway"].as_i64(), Some(i64::from(value)));
        let client = RemotingClient::with_config(ClientConfig {
            async_semaphore_value: value,
            oneway_semaphore_value: value,
            ..ClientConfig::default()
        })
        .unwrap();
        let expected_capacity = if value == i32::MIN {
            i32::MAX as usize + 1
        } else {
            value.max(0) as usize
        }
        .min(tokio::sync::Semaphore::MAX_PERMITS);

        for (semaphore, acquired_key, after_key) in [
            (
                &client.inner.async_admission,
                "asyncAcquireImmediate",
                "asyncAvailableAfterAcquire",
            ),
            (
                &client.inner.oneway_admission,
                "onewayAcquireImmediate",
                "onewayAvailableAfterAcquire",
            ),
        ] {
            assert_eq!(semaphore.available_permits(), expected_capacity);
            let java_acquired = case[acquired_key].as_bool().unwrap();
            if java_acquired {
                let permit = client
                    .inner
                    .acquire_admission(semaphore.clone(), Instant::now())
                    .await
                    .unwrap();
                assert_eq!(
                    semaphore.available_permits() as i64,
                    case[after_key].as_i64().unwrap()
                );
                drop(permit);
            } else {
                assert_eq!(
                    client
                        .inner
                        .acquire_admission(semaphore.clone(), Instant::now())
                        .await
                        .unwrap_err(),
                    TransportError::TooManyRequests
                );
                assert!(
                    client
                        .inner
                        .acquire_admission(
                            semaphore.clone(),
                            Instant::now() + std::time::Duration::from_millis(2),
                        )
                        .await
                        .unwrap_err()
                        .is_timeout()
                );
            }
        }
        client.shutdown().await;
    }
}

#[test]
fn client_remoting_defaults_follow_java_system_properties() {
    const CHILD_MARKER: &str = "ROCKETMQ_REMOTING_PROPERTY_CHILD";
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/runtime/client-config-properties.json"
    ))
    .unwrap();
    let async_property = config::JAVA_CLIENT_ASYNC_SEMAPHORE_VALUE_PROPERTY;
    let oneway_property = config::JAVA_CLIENT_ONEWAY_SEMAPHORE_VALUE_PROPERTY;
    let worker_property = config::JAVA_CLIENT_WORKER_SIZE_PROPERTY;
    let connect_timeout_property = config::JAVA_CLIENT_CONNECT_TIMEOUT_PROPERTY;
    let max_idle_time_property = config::JAVA_CLIENT_CHANNEL_MAX_IDLE_TIME_PROPERTY;
    let close_socket_property = config::JAVA_CLIENT_CLOSE_SOCKET_IF_TIMEOUT_PROPERTY;
    let send_buffer_property = config::JAVA_CLIENT_SOCKET_SNDBUF_SIZE_PROPERTY;
    let receive_buffer_property = config::JAVA_CLIENT_SOCKET_RCVBUF_SIZE_PROPERTY;
    let high_watermark_property = config::JAVA_CLIENT_WRITE_BUFFER_HIGH_WATER_MARK_PROPERTY;
    let low_watermark_property = config::JAVA_CLIENT_WRITE_BUFFER_LOW_WATER_MARK_PROPERTY;
    let configured = &fixture["remotingSemaphoreValues"];
    let worker_settings = &fixture["remotingWorkerSettings"];
    let timeout_settings = &fixture["remotingTimeoutSettings"];
    let bootstrap_settings = &fixture["remotingBootstrapSettings"];

    match std::env::var(CHILD_MARKER).as_deref() {
        Ok("valid") => {
            let defaults = ClientConfig::default();
            assert_eq!(
                i64::from(defaults.async_semaphore_value),
                configured["async"].as_i64().unwrap()
            );
            assert_eq!(
                i64::from(defaults.oneway_semaphore_value),
                configured["oneway"].as_i64().unwrap()
            );
            assert_eq!(
                i64::from(defaults.worker_threads),
                worker_settings["workerThreads"].as_i64().unwrap()
            );
            assert_eq!(
                defaults.close_socket_if_timeout,
                timeout_settings["closeSocketIfTimeout"]
            );
            assert_eq!(
                i64::from(defaults.connect_timeout),
                timeout_settings["connectTimeoutMillis"].as_i64().unwrap()
            );
            assert_eq!(
                i64::from(defaults.max_idle_time),
                timeout_settings["clientChannelMaxIdleTimeSeconds"]
                    .as_i64()
                    .unwrap()
            );
            assert_eq!(
                i64::from(defaults.client_socket_send_buffer_size),
                bootstrap_settings["clientSocketSndBufSize"]
                    .as_i64()
                    .unwrap()
            );
            assert_eq!(
                i64::from(defaults.client_socket_receive_buffer_size),
                bootstrap_settings["clientSocketRcvBufSize"]
                    .as_i64()
                    .unwrap()
            );
            assert_eq!(
                i64::from(defaults.write_buffer_high_water_mark),
                bootstrap_settings["writeBufferHighWaterMark"]
                    .as_i64()
                    .unwrap()
            );
            assert_eq!(
                i64::from(defaults.write_buffer_low_water_mark),
                bootstrap_settings["writeBufferLowWaterMark"]
                    .as_i64()
                    .unwrap()
            );
            return;
        }
        Ok("invalid") => {
            let error = std::panic::catch_unwind(ClientConfig::default).unwrap_err();
            let message = error.downcast_ref::<String>().unwrap();
            assert!(message.contains(async_property));
            return;
        }
        Ok("invalid-bootstrap") => {
            let error = std::panic::catch_unwind(ClientConfig::default).unwrap_err();
            let message = error.downcast_ref::<String>().unwrap();
            assert!(message.contains(send_buffer_property));
            return;
        }
        _ => {}
    }

    use std::process::Command;
    let executable = std::env::current_exe().unwrap();
    let property_values = &fixture["systemProperties"];
    let valid = Command::new(&executable)
        .arg("--exact")
        .arg("client::tests::client_remoting_defaults_follow_java_system_properties")
        .arg("--nocapture")
        .env(CHILD_MARKER, "valid")
        .env(
            async_property,
            property_values[async_property].as_str().unwrap(),
        )
        .env(
            oneway_property,
            property_values[oneway_property].as_str().unwrap(),
        )
        .env(
            worker_property,
            property_values[worker_property].as_str().unwrap(),
        )
        .env(
            close_socket_property,
            property_values[close_socket_property].as_str().unwrap(),
        )
        .env(
            connect_timeout_property,
            property_values[connect_timeout_property].as_str().unwrap(),
        )
        .env(
            max_idle_time_property,
            property_values[max_idle_time_property].as_str().unwrap(),
        )
        .env(
            send_buffer_property,
            property_values[send_buffer_property].as_str().unwrap(),
        )
        .env(
            receive_buffer_property,
            property_values[receive_buffer_property].as_str().unwrap(),
        )
        .env(
            high_watermark_property,
            property_values[high_watermark_property].as_str().unwrap(),
        )
        .env(
            low_watermark_property,
            property_values[low_watermark_property].as_str().unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        valid.status.success(),
        "valid child failed: {}{}",
        String::from_utf8_lossy(&valid.stdout),
        String::from_utf8_lossy(&valid.stderr)
    );

    let invalid = Command::new(&executable)
        .arg("--exact")
        .arg("client::tests::client_remoting_defaults_follow_java_system_properties")
        .arg("--nocapture")
        .env(CHILD_MARKER, "invalid")
        .env(async_property, " 1")
        .env(
            oneway_property,
            property_values[oneway_property].as_str().unwrap(),
        )
        .env(
            worker_property,
            property_values[worker_property].as_str().unwrap(),
        )
        .env(
            close_socket_property,
            property_values[close_socket_property].as_str().unwrap(),
        )
        .env(
            connect_timeout_property,
            property_values[connect_timeout_property].as_str().unwrap(),
        )
        .env(
            max_idle_time_property,
            property_values[max_idle_time_property].as_str().unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        invalid.status.success(),
        "invalid child failed: {}{}",
        String::from_utf8_lossy(&invalid.stdout),
        String::from_utf8_lossy(&invalid.stderr)
    );

    let invalid_bootstrap = Command::new(&executable)
        .arg("--exact")
        .arg("client::tests::client_remoting_defaults_follow_java_system_properties")
        .arg("--nocapture")
        .env(CHILD_MARKER, "invalid-bootstrap")
        .env(
            async_property,
            property_values[async_property].as_str().unwrap(),
        )
        .env(
            oneway_property,
            property_values[oneway_property].as_str().unwrap(),
        )
        .env(
            worker_property,
            property_values[worker_property].as_str().unwrap(),
        )
        .env(
            connect_timeout_property,
            property_values[connect_timeout_property].as_str().unwrap(),
        )
        .env(
            max_idle_time_property,
            property_values[max_idle_time_property].as_str().unwrap(),
        )
        .env(send_buffer_property, " 1")
        .env(
            receive_buffer_property,
            property_values[receive_buffer_property].as_str().unwrap(),
        )
        .env(
            high_watermark_property,
            property_values[high_watermark_property].as_str().unwrap(),
        )
        .env(
            low_watermark_property,
            property_values[low_watermark_property].as_str().unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        invalid_bootstrap.status.success(),
        "invalid bootstrap property child failed: {}{}",
        String::from_utf8_lossy(&invalid_bootstrap.stdout),
        String::from_utf8_lossy(&invalid_bootstrap.stderr)
    );
}

#[tokio::test]
async fn start_and_shutdown_race_leaves_no_owned_tasks_or_connections() {
    for _ in 0..16 {
        let client = RemotingClient::new(TransportMode::Plain);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let start_client = client.clone();
        let start_barrier = barrier.clone();
        let start = tokio::spawn(async move {
            start_barrier.wait().await;
            start_client.start()
        });
        let shutdown_client = client.clone();
        let shutdown_barrier = barrier.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_barrier.wait().await;
            shutdown_client.shutdown().await;
        });
        barrier.wait().await;
        let _ = start.await.unwrap();
        shutdown.await.unwrap();
        assert!(client.inner.tasks.lock().unwrap().is_empty());
        assert!(client.inner.connections.lock().unwrap().is_empty());
        assert!(client.inner.pending.lock().unwrap().is_empty());
        assert_eq!(client.start(), Err(TransportError::Shutdown));
    }
}

#[tokio::test]
async fn pending_terminal_race_and_opaque_wrap_never_replace_live_entries() {
    let client =
        RemotingClient::with_limits(TransportMode::Plain, DEFAULT_MAX_FRAME_SIZE, i32::MAX);
    let generation = 9;
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    let (first_id, first_rx) = client
        .inner
        .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
        .unwrap();
    let (second_id, second_rx) = client
        .inner
        .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
        .unwrap();
    assert_eq!(first_id, i32::MAX);
    assert_eq!(second_id, i32::MIN);

    let (barrier_tx, _) = tokio::sync::broadcast::channel(1);
    let mut response_barrier = barrier_tx.subscribe();
    let mut timeout_barrier = barrier_tx.subscribe();
    let response_inner = client.inner.clone();
    let response = tokio::spawn(async move {
        let _ = response_barrier.recv().await;
        response_inner.finish_pending(first_id, generation, Ok(test_command()))
    });
    let timeout_inner = client.inner.clone();
    let timeout = tokio::spawn(async move {
        let _ = timeout_barrier.recv().await;
        timeout_inner.finish_pending(first_id, generation, Err(TransportError::Timeout))
    });
    barrier_tx.send(()).unwrap();
    let response_won = response.await.unwrap();
    let timeout_won = timeout.await.unwrap();
    assert_ne!(response_won, timeout_won);
    assert_eq!(first_rx.await.is_ok(), response_won);
    assert_eq!(client.inner.pending.lock().unwrap().len(), 1);
    client.inner.cancel_pending(second_id, generation);
    assert!(matches!(second_rx.await, Ok(Err(TransportError::Closed))));
    assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
    client.shutdown().await;
    assert!(client.inner.tasks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn wire_response_timeout_cancellation_and_shutdown_race_leaves_no_pending() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).await.unwrap();
        let frame_length = u32::from_be_bytes(prefix) as usize;
        let mut frame_bytes = bytes::BytesMut::with_capacity(frame_length + 4);
        frame_bytes.extend_from_slice(&prefix);
        frame_bytes.resize(frame_length + 4, 0);
        stream.read_exact(&mut frame_bytes[4..]).await.unwrap();
        let request = crate::codec::frame::decode_next(&mut frame_bytes)
            .unwrap()
            .unwrap();
        request_tx.send(request.opaque).unwrap();
        let _ = release_rx.await;
        let mut response = test_command();
        response.opaque = request.opaque;
        response.mark_response();
        let _ = stream
            .write_all(&crate::codec::frame::encode(&response).unwrap())
            .await;
    });

    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let invoke_client = client.clone();
    let address_string = address.to_string();
    let invoke = tokio::spawn(async move {
        invoke_client
            .invoke(
                &address_string,
                test_command(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
    });
    let opaque = tokio::time::timeout(std::time::Duration::from_secs(2), request_rx)
        .await
        .expect("invoke frame was not written")
        .unwrap();

    let generation = client
        .inner
        .pending
        .lock()
        .unwrap()
        .get(&opaque)
        .expect("wire request must own a pending entry")
        .generation;
    let barrier = Arc::new(tokio::sync::Barrier::new(5));
    let cancel_barrier = barrier.clone();
    let cancel = tokio::spawn(async move {
        cancel_barrier.wait().await;
        invoke.abort();
    });
    let timeout_inner = client.inner.clone();
    let timeout_barrier = barrier.clone();
    let timeout = tokio::spawn(async move {
        timeout_barrier.wait().await;
        timeout_inner.expire_pending(opaque, generation);
    });
    let shutdown_client = client.clone();
    let shutdown_barrier = barrier.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_barrier.wait().await;
        shutdown_client.shutdown().await;
    });
    let release_barrier = barrier.clone();
    let release = tokio::spawn(async move {
        release_barrier.wait().await;
        let _ = release_tx.send(());
    });
    barrier.wait().await;
    cancel.await.unwrap();
    timeout.await.unwrap();
    release.await.unwrap();
    shutdown.await.unwrap();
    let _ = peer.await;
    assert!(client.inner.pending.lock().unwrap().is_empty());
    assert!(client.inner.tasks.lock().unwrap().is_empty());
    assert!(client.inner.connections.lock().unwrap().is_empty());
    assert!(!client.inner.pending.lock().unwrap().contains_key(&opaque));
}

#[tokio::test]
async fn connection_creation_racing_shutdown_closes_unpublished_or_published_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        let mut byte = [0; 1];
        assert!(
            matches!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    tokio::io::AsyncReadExt::read(&mut stream, &mut byte),
                )
                .await,
                Ok(Ok(0)) | Ok(Err(_))
            ),
            "shutdown left the accepted socket open"
        );
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let connect_client = client.clone();
    let connect = tokio::spawn(async move {
        connect_client
            .inner
            .connection(
                &address.to_string(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), accepted_rx)
        .await
        .expect("connection attempt did not reach the peer")
        .unwrap();
    tokio::task::yield_now().await;
    client.shutdown().await;
    if let Ok(connection) = connect.await.unwrap() {
        assert!(
            connection.is_closed(),
            "shutdown left a published connection open"
        );
    }
    peer.await.unwrap();
    assert!(client.inner.connections.lock().unwrap().is_empty());
    assert!(client.inner.pending.lock().unwrap().is_empty());
    assert!(client.inner.tasks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn inbound_dispatch_queue_accepts_more_than_legacy_capacity() {
    struct InlineExecutor;

    impl RequestExecutor for InlineExecutor {
        fn execute(&self, task: CallbackTask) -> Result<(), String> {
            task();
            Ok(())
        }
    }

    let client = RemotingClient::new(TransportMode::Plain);

    client.start().unwrap();
    let processed = Arc::new(AtomicUsize::new(0));
    let processed_by_handler = processed.clone();
    client.register_processor_with_executor(
        test_command().code,
        Arc::new(move |_| {
            processed_by_handler.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }),
        Some(Arc::new(InlineExecutor)),
    );
    for _ in 0..256 {
        client
            .inner
            .inbound
            .send(processor::InboundRequest {
                generation: 0,
                remote_addr: "test:0".into(),
                channel: ChannelHandle::pending("test:0"),
                command: test_command(),
            })
            .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while processed.load(Ordering::SeqCst) != 256 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unbounded inbound queue did not dispatch all accepted requests");
    client.shutdown().await;
}

#[tokio::test]
async fn accepted_inbound_requests_are_dispatched_when_shutdown_starts() {
    struct InlineExecutor;

    impl RequestExecutor for InlineExecutor {
        fn execute(&self, task: CallbackTask) -> Result<(), String> {
            task();
            Ok(())
        }
    }

    let client = RemotingClient::new(TransportMode::Plain);

    client.start().unwrap();
    let processed = Arc::new(AtomicUsize::new(0));
    let processed_by_handler = processed.clone();
    client.register_processor_with_executor(
        test_command().code,
        Arc::new(move |_| {
            processed_by_handler.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }),
        Some(Arc::new(InlineExecutor)),
    );
    client
        .inner
        .inbound
        .send(processor::InboundRequest {
            generation: 0,
            remote_addr: "test:0".into(),
            channel: ChannelHandle::pending("test:0"),
            command: test_command(),
        })
        .unwrap();
    client.inner.shutdown.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while processed.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted inbound request was dropped during shutdown");
    client.shutdown().await;
}

#[tokio::test]
async fn inbound_shutdown_go_away_precedes_rejection_above_java_551_version_cutoff() {
    struct RejectingProcessor {
        rejects: Arc<AtomicUsize>,
        processed: Arc<AtomicUsize>,
    }

    impl RequestProcessor for RejectingProcessor {
        fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
            self.processed.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        fn reject_request(&self) -> bool {
            self.rejects.fetch_add(1, Ordering::SeqCst);
            true
        }
    }

    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let rejects = Arc::new(AtomicUsize::new(0));
    let processed = Arc::new(AtomicUsize::new(0));
    client.register_processor(
        test_command().code,
        Arc::new(RejectingProcessor {
            rejects: rejects.clone(),
            processed: processed.clone(),
        }),
    );

    client.inner.shutdown.send_replace(true);
    for version in [476, 475] {
        let mut command = test_command();
        command.version = version;
        processor::process_one(
            &Arc::downgrade(&client.inner),
            processor::InboundRequest {
                generation: 0,
                remote_addr: "test:0".into(),
                channel: ChannelHandle::pending("test:0"),
                command,
            },
        );
        assert_eq!(rejects.load(Ordering::SeqCst), usize::from(version == 475));
    }

    client.inner.shutdown.send_replace(false);
    let mut running_command = test_command();
    running_command.version = 476;
    processor::process_one(
        &Arc::downgrade(&client.inner),
        processor::InboundRequest {
            generation: 0,
            remote_addr: "test:0".into(),
            channel: ChannelHandle::pending("test:0"),
            command: running_command,
        },
    );

    assert_eq!(rejects.load(Ordering::SeqCst), 2);
    assert_eq!(processed.load(Ordering::SeqCst), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn callback_workers_drain_late_tasks_after_client_shutdown_starts() {
    let (sender, receiver) = mpsc::unbounded_channel();
    let (shutdown, _shutdown_receiver) = tokio::sync::watch::channel(false);
    let (callback_shutdown, _) = tokio::sync::watch::channel(false);
    let completed = Arc::new(AtomicUsize::new(0));
    for _ in 0..32 {
        let completed = completed.clone();
        sender
            .send(Box::new(move || {
                completed.fetch_add(1, Ordering::SeqCst);
            }) as CallbackTask)
            .unwrap();
    }
    let workers = start_callback_workers(receiver, callback_shutdown.subscribe(), 2);
    shutdown.send(true).unwrap();
    let late_completed = completed.clone();
    sender
        .send(Box::new(move || {
            late_completed.fetch_add(1, Ordering::SeqCst);
        }) as CallbackTask)
        .unwrap();
    callback_shutdown.send(true).unwrap();
    for worker in workers {
        tokio::time::timeout(std::time::Duration::from_secs(2), worker)
            .await
            .expect("callback worker did not drain accepted tasks")
            .unwrap();
    }
    assert_eq!(completed.load(Ordering::SeqCst), 33);
}

#[test]
fn public_executor_rejects_submissions_after_client_shutdown() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let state = TokioExecutorState {
        closed: AtomicBool::new(false),
        admission: Mutex::new(()),
    };
    state
        .execute(&sender, Box::new(|| {}))
        .expect("running executor accepts tasks");
    assert!(receiver.try_recv().is_ok());
    state.shutdown();
    assert!(state.execute(&sender, Box::new(|| {})).is_err());
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn client_worker_threads_setting_is_read_when_starting() {
    let client = RemotingClient::with_config(ClientConfig {
        worker_threads: 1,
        callback_executor_threads: 1,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    assert_eq!(client.get_client_worker_threads(), 1);
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/runtime/client-config-properties.json"
    ))
    .unwrap();
    for case in fixture["remotingWorkerSettings"]["setterCases"]
        .as_array()
        .unwrap()
    {
        let input = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
        let expected = i32::try_from(case["getter"].as_i64().unwrap()).unwrap();
        client.set_client_worker_threads(input);
        assert_eq!(client.get_client_worker_threads(), expected);
    }

    client.set_client_worker_threads(-1);
    assert_eq!(client.get_client_worker_threads(), -1);
    assert!(matches!(
        client.start(),
        Err(TransportError::InvalidConfiguration(_))
    ));
    client.set_client_worker_threads(0);
    assert!(matches!(
        client.start(),
        Err(TransportError::InvalidConfiguration(_))
    ));
    client.set_client_worker_threads(3);
    client.start().unwrap();
    assert_eq!(client.get_client_worker_threads(), 3);
    // Processor start owns one dispatcher plus three workers. The remaining
    // tracked task is the timeout scheduler; shutdown joins the event worker separately.
    assert_eq!(client.inner.tasks.lock().unwrap().len(), 5);
    assert!(client.inner.channel_event_worker.lock().unwrap().is_some());
    assert_eq!(client.inner.callback_workers.lock().unwrap().len(), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn connect_timeout_setting_is_snapshotted_when_starting() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    assert_eq!(client.get_connect_timeout_millis(), 3000);
    client.set_connect_timeout_millis(25);
    assert_eq!(client.inner.active_connect_timeout_millis(), 3000);

    client.start().unwrap();
    assert_eq!(client.inner.active_connect_timeout_millis(), 25);
    let requested_deadline = Instant::now() + std::time::Duration::from_secs(1);
    let connect_deadline = ClientInner::deadline_for_connect_timeout(
        Some(requested_deadline),
        client.inner.active_connect_timeout_millis(),
    )
    .unwrap();
    assert!(connect_deadline <= requested_deadline);
    assert!(
        connect_deadline.saturating_duration_since(Instant::now())
            <= std::time::Duration::from_millis(25)
    );

    client.set_connect_timeout_millis(99);
    assert_eq!(client.get_connect_timeout_millis(), 99);
    assert_eq!(client.inner.active_connect_timeout_millis(), 25);
    client.shutdown().await;
}

#[tokio::test]
async fn proxy_connect_timeout_is_read_on_first_route_use_and_then_cached() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();

    client.set_connect_timeout_millis(125);
    let first = client
        .inner
        .connection_deadline_for_route(
            Some("127.0.0.0/8"),
            Some(Instant::now() + std::time::Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(
        client
            .inner
            .proxy_connect_timeout_millis
            .lock()
            .unwrap()
            .get("127.0.0.0/8"),
        Some(&125)
    );
    assert_eq!(client.inner.active_connect_timeout_millis(), 3000);

    client.set_connect_timeout_millis(250);
    let cached = client
        .inner
        .connection_deadline_for_route(
            Some("127.0.0.0/8"),
            Some(Instant::now() + std::time::Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(
        client
            .inner
            .proxy_connect_timeout_millis
            .lock()
            .unwrap()
            .get("127.0.0.0/8"),
        Some(&125)
    );
    assert!(first <= Instant::now() + std::time::Duration::from_millis(125));
    assert!(cached <= Instant::now() + std::time::Duration::from_millis(125));

    let second_route = client
        .inner
        .connection_deadline_for_route(
            Some("10.0.0.0/8"),
            Some(Instant::now() + std::time::Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(
        client
            .inner
            .proxy_connect_timeout_millis
            .lock()
            .unwrap()
            .get("10.0.0.0/8"),
        Some(&250)
    );
    assert!(second_route <= Instant::now() + std::time::Duration::from_millis(250));
    client.shutdown().await;
}

#[tokio::test]
async fn channel_pipeline_settings_are_read_from_live_values() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();
    assert_eq!(client.get_client_channel_max_idle_time_seconds(), 120);
    assert!(!client.get_disable_netty_worker_group());
    assert_eq!(
        client.inner.channel_pipeline_settings(),
        (std::time::Duration::from_secs(120), false)
    );

    client.set_client_channel_max_idle_time_seconds(-1);
    client.set_disable_netty_worker_group(true);
    assert_eq!(client.get_client_channel_max_idle_time_seconds(), -1);
    assert!(client.get_disable_netty_worker_group());
    assert_eq!(
        client.inner.channel_pipeline_settings(),
        (std::time::Duration::ZERO, true)
    );

    client.set_client_channel_max_idle_time_seconds(2);
    client.set_disable_netty_worker_group(false);
    assert_eq!(
        client.inner.channel_pipeline_settings(),
        (std::time::Duration::from_secs(2), false)
    );
    client.shutdown().await;
}

#[tokio::test]
async fn bootstrap_options_use_start_and_proxy_first_use_snapshots() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.set_client_socket_send_buffer_size(4096);
    client.set_client_socket_receive_buffer_size(8192);
    client.set_write_buffer_low_water_mark(16 * 1024);
    client.set_write_buffer_high_water_mark(48 * 1024);
    client.set_pooled_byte_buffer_allocator_enabled(true);
    client.start().unwrap();

    let direct = client.inner.direct_connection_settings().unwrap();
    assert_eq!(direct.send_buffer_size, 4096);
    assert_eq!(direct.receive_buffer_size, 8192);
    assert_eq!(direct.write_buffer_watermarks, (48 * 1024, 16 * 1024));
    assert!(direct.pooled_byte_buffer_allocator_enabled);

    client.set_client_socket_send_buffer_size(12 * 1024);
    client.set_client_socket_receive_buffer_size(24 * 1024);
    client.set_write_buffer_low_water_mark(4 * 1024);
    client.set_write_buffer_high_water_mark(12 * 1024);
    client.set_pooled_byte_buffer_allocator_enabled(true);

    let first_proxy = client
        .inner
        .connection_settings_for_route(Some("127.0.0.0/8"))
        .unwrap();
    assert_eq!(first_proxy.send_buffer_size, 12 * 1024);
    assert_eq!(first_proxy.receive_buffer_size, 24 * 1024);
    assert_eq!(first_proxy.write_buffer_watermarks, (64 * 1024, 32 * 1024));
    assert!(!first_proxy.pooled_byte_buffer_allocator_enabled);

    client.set_client_socket_send_buffer_size(32 * 1024);
    client.set_client_socket_receive_buffer_size(48 * 1024);
    assert_eq!(
        client
            .inner
            .connection_settings_for_route(Some("127.0.0.0/8"))
            .unwrap(),
        first_proxy
    );
    let second_proxy = client
        .inner
        .connection_settings_for_route(Some("10.0.0.0/8"))
        .unwrap();
    assert_eq!(second_proxy.send_buffer_size, 32 * 1024);
    assert_eq!(second_proxy.receive_buffer_size, 48 * 1024);
    assert_eq!(client.inner.direct_connection_settings().unwrap(), direct);
    client.shutdown().await;
}

#[tokio::test]
async fn signed_socket_settings_follow_java_direct_and_proxy_snapshots() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        socks_proxy_config: r#"{"127.0.0.0/8":{"addr":"127.0.0.1:1080"}}"#.into(),
        client_socket_send_buffer_size: -8192,
        client_socket_receive_buffer_size: 0,
        write_buffer_low_water_mark: -3,
        write_buffer_high_water_mark: 12,
        ..ClientConfig::default()
    })
    .unwrap();
    client.start().unwrap();

    let direct = client.inner.direct_connection_settings().unwrap();
    assert_eq!(direct.send_buffer_size, -8192);
    assert_eq!(direct.receive_buffer_size, 0);
    assert_eq!(direct.write_buffer_watermarks, (64 * 1024, 32 * 1024));

    let proxy = client
        .inner
        .connection_settings_for_route(Some("127.0.0.0/8"))
        .unwrap();
    assert_eq!(proxy.send_buffer_size, -8192);
    assert_eq!(proxy.receive_buffer_size, 0);
    assert_eq!(proxy.write_buffer_watermarks, (64 * 1024, 32 * 1024));
    client.shutdown().await;
}

#[tokio::test]
async fn signed_buffer_and_watermark_settings_match_java_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/runtime/client-config-properties.json"
    ))
    .unwrap();
    let settings = &fixture["remotingBootstrapSettings"]["setterCases"];
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();

    for (setting, cases) in settings.as_object().unwrap() {
        for case in cases.as_array().unwrap() {
            let input = i32::try_from(case["input"].as_i64().unwrap()).unwrap();
            let expected = i32::try_from(case["getter"].as_i64().unwrap()).unwrap();
            match setting.as_str() {
                "clientSocketSndBufSize" => {
                    client.set_client_socket_send_buffer_size(input);
                    assert_eq!(client.get_client_socket_send_buffer_size(), expected);
                }
                "clientSocketRcvBufSize" => {
                    client.set_client_socket_receive_buffer_size(input);
                    assert_eq!(client.get_client_socket_receive_buffer_size(), expected);
                }
                "writeBufferHighWaterMark" => {
                    client.set_write_buffer_high_water_mark(input);
                    assert_eq!(client.get_write_buffer_high_water_mark(), expected);
                }
                "writeBufferLowWaterMark" => {
                    client.set_write_buffer_low_water_mark(input);
                    assert_eq!(client.get_write_buffer_low_water_mark(), expected);
                }
                other => panic!("unexpected Java setting fixture: {other}"),
            }
        }
    }
    client.shutdown().await;
}

#[tokio::test]
async fn invalid_live_write_watermarks_leave_start_retryable() {
    let client = RemotingClient::with_config(ClientConfig {
        write_buffer_low_water_mark: 8192,
        write_buffer_high_water_mark: 4096,
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    assert!(matches!(
        client.start(),
        Err(TransportError::InvalidConfiguration(_))
    ));
    assert!(!client.inner.started.load(Ordering::Acquire));

    client.set_write_buffer_high_water_mark(16 * 1024);
    client.start().unwrap();
    assert_eq!(
        client
            .inner
            .direct_connection_settings()
            .unwrap()
            .write_buffer_watermarks,
        (16 * 1024, 8192)
    );
    client.shutdown().await;
}

#[tokio::test]
async fn worker_group_pool_is_available_for_live_channel_toggles() {
    let config = ClientConfig {
        disable_netty_worker_group: true,
        worker_threads: 2,
        ..ClientConfig::default()
    };
    let (inbound, receiver) = mpsc::unbounded_channel();
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let (callback_shutdown, _) = tokio::sync::watch::channel(false);
    let (channel_event_stopped, _) = tokio::sync::watch::channel(false);
    let (shutdown_cleanup_stopped, _) = tokio::sync::watch::channel(false);
    let (callback_sender, _callback_receiver) = mpsc::unbounded_channel();
    let (channel_event_sender, _channel_event_receiver) =
        mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
    let executor = Arc::new(TokioRequestExecutor {
        sender: callback_sender.clone(),
        state: Arc::new(TokioExecutorState {
            closed: AtomicBool::new(false),
            admission: Mutex::new(()),
        }),
    });
    let inner = Arc::new(ClientInner {
        connections: Mutex::new(HashMap::new()),
        connection_wrapper_locks: Mutex::new(HashMap::new()),
        retired_connections: Mutex::new(HashMap::new()),
        connecting: Mutex::new(HashMap::new()),
        pending: Mutex::new(HashMap::new()),
        opaque: AtomicI32::new(0),
        generation: AtomicU64::new(1),
        processors: Mutex::new(HashMap::new()),
        default_processor: Mutex::new(None),
        inbound,
        public_executor: executor,
        shutdown,
        callback_shutdown,
        callback_workers: Mutex::new(Vec::new()),
        channel_event_stopped,
        channel_event_worker: Mutex::new(None),
        shutdown_cleanup_stopped,
        shutdown_started: tokio::sync::OnceCell::new(),
        closed: AtomicBool::new(false),
        tasks: Mutex::new(Vec::new()),
        security: TransportMode::Plain,
        use_tls: AtomicBool::new(false),
        max_frame_size_override: Some(DEFAULT_MAX_FRAME_SIZE),
        close_socket_if_timeout: AtomicBool::new(config.close_socket_if_timeout),
        enable_reconnect_for_go_away: AtomicBool::new(config.enable_reconnect_for_go_away),
        disable_callback_executor: AtomicBool::new(config.disable_callback_executor),
        client_worker_threads: AtomicI32::new(config.worker_threads),
        disable_netty_worker_group: AtomicBool::new(config.disable_netty_worker_group),
        client_channel_max_idle_time_seconds: AtomicI32::new(config.max_idle_time),
        client_socket_send_buffer_size: AtomicI32::new(config.client_socket_send_buffer_size),
        client_socket_receive_buffer_size: AtomicI32::new(config.client_socket_receive_buffer_size),
        write_buffer_high_water_mark: AtomicI32::new(config.write_buffer_high_water_mark),
        write_buffer_low_water_mark: AtomicI32::new(config.write_buffer_low_water_mark),
        pooled_byte_buffer_allocator_enabled: AtomicBool::new(
            config.pooled_byte_buffer_allocator_enabled,
        ),
        active_connection_settings: Mutex::new(None),
        proxy_connection_settings: Mutex::new(HashMap::new()),
        scan_available_nameserver: AtomicBool::new(config.scan_available_nameserver),
        connect_timeout_millis: AtomicI32::new(config.connect_timeout),
        active_connect_timeout_millis: AtomicI32::new(config.connect_timeout),
        proxy_connect_timeout_millis: Mutex::new(HashMap::new()),
        config,
        socks_proxy_routes: config::ProxyRoutes::default(),
        startup: Mutex::new(None),
        runtime: Mutex::new(Some(tokio::runtime::Handle::current())),
        started: AtomicBool::new(true),
        async_admission: Arc::new(tokio::sync::Semaphore::new(8)),
        oneway_admission: Arc::new(tokio::sync::Semaphore::new(8)),
        callback_sender,
        callback_executor: Mutex::new(None),
        rpc_hooks: Mutex::new(Vec::new()),
        channel_listener: None,
        channel_event_sender,
        request_pipeline: Mutex::new(None),
        metrics_manager: RwLock::new(None),
        nameservers: Mutex::new(None),
        available_nameservers: Mutex::new(HashSet::new()),
        chosen_nameserver: Mutex::new(None),
        nameserver_index: AtomicU64::new(0),
    });
    let tasks = processor::start(
        Arc::downgrade(&inner),
        receiver,
        usize::try_from(inner.config.worker_threads).expect("validated worker count"),
    );
    assert_eq!(
        tasks.len(),
        usize::try_from(inner.config.worker_threads).unwrap() + 1
    );
    inner.shutdown.send(true).unwrap();
    for task in tasks {
        task.await.unwrap();
    }
    inner.public_executor.state.shutdown();
}

#[tokio::test]
async fn stale_generation_close_cannot_evict_replacement() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (_first, _) = listener.accept().await.unwrap();
        let (_replacement, _) = listener.accept().await.unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let first = client
        .inner
        .connection(
            &address.to_string(),
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    let stale_generation = first.generation;
    first.close();
    tokio::time::timeout(std::time::Duration::from_secs(2), first.join())
        .await
        .expect("first connection did not join");
    let replacement = client
        .inner
        .connection(
            &address.to_string(),
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_ne!(replacement.generation, stale_generation);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        client
            .inner
            .connection_closed(stale_generation, Some(TransportError::Closed));
    })
    .await
    .expect("stale close handler blocked");
    assert!(!replacement.is_closed());
    assert_eq!(
        client
            .inner
            .connections
            .lock()
            .unwrap()
            .get(&address.to_string())
            .unwrap()
            .generation,
        replacement.generation
    );
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn cancelled_invoke_guard_removes_only_its_pending_generation() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    let (opaque, receiver) = client
        .inner
        .insert_pending(5, deadline, "127.0.0.1:1".into(), 0)
        .unwrap();
    let guard = pending::PendingGuard::new(Arc::downgrade(&client.inner), opaque, 5);
    assert_eq!(client.inner.pending.lock().unwrap().len(), 1);
    drop(guard);
    assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
    assert!(matches!(receiver.await, Ok(Err(TransportError::Closed))));
    client.shutdown().await;
}

#[tokio::test]
async fn response_timeout_and_shutdown_race_has_one_terminal_completion() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let generation = 21;
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    let (opaque, receiver) = client
        .inner
        .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
        .unwrap();
    let guard = pending::PendingGuard::new(Arc::downgrade(&client.inner), opaque, generation);
    let barrier = Arc::new(tokio::sync::Barrier::new(5));

    let response_inner = client.inner.clone();
    let response_barrier = barrier.clone();
    let response = tokio::spawn(async move {
        response_barrier.wait().await;
        response_inner.finish_pending(opaque, generation, Ok(test_command()))
    });
    let timeout_inner = client.inner.clone();
    let timeout_barrier = barrier.clone();
    let timeout = tokio::spawn(async move {
        timeout_barrier.wait().await;
        timeout_inner.finish_pending(opaque, generation, Err(TransportError::Timeout))
    });
    let cancel_inner = client.inner.clone();
    let cancel_barrier = barrier.clone();
    let cancel = tokio::spawn(async move {
        cancel_barrier.wait().await;
        cancel_inner.finish_pending(opaque, generation, Err(TransportError::Closed))
    });
    let shutdown_inner = client.inner.clone();
    let shutdown_barrier = barrier.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_barrier.wait().await;
        shutdown_inner.fail_all_pending();
    });
    barrier.wait().await;
    let response_won = response.await.unwrap();
    let timeout_won = timeout.await.unwrap();
    let cancel_won = cancel.await.unwrap();
    shutdown.await.unwrap();
    assert!(usize::from(response_won) + usize::from(timeout_won) + usize::from(cancel_won) <= 1);
    assert!(matches!(receiver.await, Ok(Ok(_)) | Ok(Err(_))));
    assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
    drop(guard);
    client.shutdown().await;
}

#[tokio::test]
async fn pooled_allocator_reuses_frame_capacity_after_successful_write() {
    let (client_io, mut peer_io) = tokio::io::duplex(4096);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        30,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    let command = RemotingCommand {
        body: Some(vec![7; 128]),
        ..test_command()
    };
    let first = connection
        .encode_command(&command, DEFAULT_MAX_FRAME_SIZE)
        .unwrap();
    let first_capacity = first.capacity();
    connection
        .send(first, Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    let mut length = [0; 4];
    tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut length)
        .await
        .unwrap();
    let remaining = u32::from_be_bytes(length) as usize;
    let mut received = vec![0; remaining];
    tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut received)
        .await
        .unwrap();
    assert_eq!(connection.pooled_frame_count(), 1);
    assert!(connection.pooled_frame_capacity() >= first_capacity);

    let second = connection
        .encode_command(&command, DEFAULT_MAX_FRAME_SIZE)
        .unwrap();
    assert!(second.capacity() >= first_capacity);
    connection
        .send(second, Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut length)
        .await
        .unwrap();
    let remaining = u32::from_be_bytes(length) as usize;
    received.resize(remaining, 0);
    tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut received)
        .await
        .unwrap();
    connection.close();
    connection.join().await;
}

#[tokio::test]
async fn pooled_allocator_recycles_frame_after_encode_failure() {
    let (client_io, _peer_io) = tokio::io::duplex(64);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        23,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    assert!(matches!(
        connection.encode_command(&test_command(), 1),
        Err(TransportError::Codec(_))
    ));
    assert_eq!(connection.pooled_frame_count(), 1);
    connection.close();
    connection.join().await;
}

#[tokio::test]
async fn pooled_allocator_recycles_frame_after_saturated_write_timeout() {
    let (client_io, mut peer_io) = tokio::io::duplex(1);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        22,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    let active_sender = {
        let connection = connection.clone();
        tokio::spawn(async move {
            connection
                .send(
                    vec![0; 128],
                    Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while connection.queued_write_bytes() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("writer did not dequeue the active frame");
    let mut first_byte = [0];
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
    )
    .await
    .expect("writer did not begin sending the active frame")
    .unwrap();
    connection.try_send(vec![0; 128], Instant::now()).unwrap();
    let result = connection
        .send(
            vec![0; 128],
            Instant::now() + std::time::Duration::from_millis(10),
        )
        .await;
    assert!(matches!(result, Err(TransportError::Timeout)), "{result:?}");
    connection.close();
    connection.join().await;
    assert!(connection.pooled_frame_count() >= 1);
    active_sender.abort();
    let _ = active_sender.await;
}

#[test]
fn asynchronous_write_failure_matches_java_request_fail_classification() {
    let remote_addr = "127.0.0.1:10911";
    let error = classify_connection_error(remote_addr, TransportError::Connect("refused".into()));
    assert!(matches!(
        error,
        TransportError::ConnectRequest {
            remote_addr: address,
            cause: Some(cause),
        } if address == remote_addr && cause.to_string() == "refused"
    ));
    let error = classify_connection_error(remote_addr, TransportError::Tls("handshake".into()));
    assert!(matches!(
        error,
        TransportError::ConnectRequest {
            remote_addr: address,
            cause: Some(cause),
        } if address == remote_addr && cause.to_string() == "handshake"
    ));
    assert_eq!(
        classify_connection_error(remote_addr, TransportError::Timeout),
        TransportError::Timeout
    );
    let error = classify_write_error(remote_addr, TransportError::Send("broken pipe".into()));
    assert!(matches!(
        error,
        TransportError::SendRequest {
            remote_addr: address,
            cause: Some(cause),
        } if address == remote_addr && cause.to_string() == "broken pipe"
    ));
    assert_eq!(
        classify_write_error(remote_addr, TransportError::Closed),
        TransportError::SendRequest {
            remote_addr: remote_addr.into(),
            cause: None,
        }
    );
    assert_eq!(
        classify_write_error(
            remote_addr,
            TransportError::Codec(crate::error::CodecError::FrameTooLarge {
                length: 128,
                limit: 64,
            }),
        ),
        TransportError::SendRequest {
            remote_addr: remote_addr.into(),
            cause: None,
        }
    );
    assert_eq!(
        classify_write_error(
            remote_addr,
            TransportError::MalformedFrame(crate::error::CodecError::FrameTooLarge {
                length: 128,
                limit: 64,
            }),
        ),
        TransportError::SendRequest {
            remote_addr: remote_addr.into(),
            cause: None,
        }
    );
    assert_eq!(
        classify_write_error(remote_addr, TransportError::Timeout),
        TransportError::Timeout
    );
}

#[tokio::test]
async fn pooled_allocator_recycles_frames_after_write_error() {
    let (client_io, peer_io) = tokio::io::duplex(1);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        26,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    drop(peer_io);
    let result = connection
        .send(
            vec![0; 128],
            Instant::now() + std::time::Duration::from_secs(2),
        )
        .await;
    assert!(matches!(result, Err(TransportError::SendSource(_))));
    connection.join().await;
    assert_eq!(connection.pooled_frame_count(), 1);
}

#[tokio::test]
async fn pooled_allocator_recycles_frames_after_send_cancellation_and_shutdown() {
    for shutdown in [false, true] {
        let (client_io, mut peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            if shutdown { 25 } else { 24 },
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            (64 * 1024, 32 * 1024),
            "test:0".into(),
            true,
        );
        let sender = {
            let connection = connection.clone();
            tokio::spawn(async move {
                connection
                    .send(
                        vec![0; 128],
                        Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while connection.queued_write_bytes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writer did not dequeue the frame");
        let mut first_byte = [0];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
        )
        .await
        .expect("writer did not begin sending the frame")
        .unwrap();
        if shutdown {
            connection.close();
        } else {
            sender.abort();
        }
        let _ = sender.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), connection.join())
            .await
            .expect("connection did not stop");
        assert_eq!(connection.pooled_frame_count(), 1);
    }
}

#[tokio::test]
async fn pooled_allocator_keeps_at_most_four_buffers_per_connection() {
    let (client_io, _peer_io) = tokio::io::duplex(64);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        27,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    for _ in 0..8 {
        connection.recycle_frame(Vec::with_capacity(128));
    }
    assert_eq!(connection.pooled_frame_count(), 4);
    connection.close();
    connection.join().await;
}

#[tokio::test]
async fn unbounded_outbound_queue_accepts_more_than_legacy_capacity() {
    let (client_io, mut peer_io) = tokio::io::duplex(1);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        29,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    connection.try_send(vec![0; 128], deadline).unwrap();
    let mut first_byte = [0];
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
    )
    .await
    .expect("writer did not start the active frame")
    .unwrap();

    let queued_frames = super::LEGACY_OUTBOUND_QUEUE_CAPACITY + 32;
    for _ in 0..queued_frames {
        connection.try_send(vec![0; 128], deadline).unwrap();
    }
    assert!(connection.queued_write_bytes() >= queued_frames * 128);
    connection.close();
    connection.join().await;
    assert!(connection.pooled_frame_count() > 0);
}

#[tokio::test]
async fn write_watermarks_follow_queued_bytes_with_hysteresis() {
    let (client_io, mut peer_io) = tokio::io::duplex(64);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        32,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (8, 3),
        "test:0".into(),
        true,
    );
    assert!(connection.is_writable());
    connection.reserve_write_bytes(8);
    assert!(
        connection.is_writable(),
        "Java stays writable at the high mark"
    );
    connection.reserve_write_bytes(1);
    assert!(!connection.is_writable());
    connection.release_write_bytes(6);
    assert!(
        !connection.is_writable(),
        "Java stays unwritable at the low mark"
    );
    connection.release_write_bytes(1);
    assert!(connection.is_writable());

    let deadline = Instant::now() + std::time::Duration::from_secs(2);
    connection.try_send(vec![0; 9], deadline).unwrap();
    assert!(!connection.is_writable());
    let mut buffer = [0; 9];
    tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut buffer)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !connection.watermark_is_writable() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    connection.close();
    connection.join().await;
}

#[tokio::test]
async fn response_style_write_waits_until_connection_shutdown() {
    let (client_io, _peer_io) = tokio::io::duplex(1);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        33,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    let writer = {
        let connection = connection.clone();
        tokio::spawn(async move { connection.send_without_deadline(vec![0; 1024 * 1024]).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !writer.is_finished(),
        "unbounded response write must remain pending"
    );
    connection.close();
    assert_eq!(writer.await.unwrap(), Err(TransportError::Shutdown));
    connection.join().await;
}

#[tokio::test]
async fn saturated_writer_obeys_deadline_and_joins() {
    let (client_io, _peer_io) = tokio::io::duplex(1);
    let connection = connection::Connection::spawn(
        Box::new(client_io),
        31,
        Weak::new(),
        DEFAULT_MAX_FRAME_SIZE,
        (64 * 1024, 32 * 1024),
        "test:0".into(),
        true,
    );
    let deadline = Instant::now() + std::time::Duration::from_millis(100);
    let queued_frames = super::LEGACY_OUTBOUND_QUEUE_CAPACITY + 32;
    for _ in 0..queued_frames {
        connection.try_send(vec![0; 1024], deadline).unwrap();
    }
    assert!(connection.queued_write_bytes() >= queued_frames * 1024);
    assert!(matches!(
        connection
            .send(
                vec![0; 1024 * 1024],
                Instant::now() + std::time::Duration::from_millis(20),
            )
            .await,
        Err(TransportError::Timeout)
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), connection.join())
        .await
        .unwrap();
    assert!(connection.is_closed());
}

#[tokio::test]
async fn pending_calls_complete_on_shutdown_and_owned_tasks_join() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_seen_tx, request_seen_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut first_byte = [0; 1];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut first_byte)
            .await
            .unwrap();
        request_seen_tx.send(()).unwrap();
        let mut remaining = [0; 64];
        loop {
            if tokio::io::AsyncReadExt::read(&mut stream, &mut remaining)
                .await
                .unwrap()
                == 0
            {
                break;
            }
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let invoke_client = client.clone();
    let invoke = tokio::spawn(async move {
        invoke_client
            .invoke(
                &address.to_string(),
                test_command(),
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), request_seen_rx)
        .await
        .expect("peer did not receive the pending request")
        .unwrap();
    client.shutdown().await;
    assert!(matches!(
        invoke.await.unwrap(),
        Err(TransportError::Shutdown)
    ));
    assert!(client.inner.tasks.lock().unwrap().is_empty());
    assert!(client.inner.connections.lock().unwrap().is_empty());
    assert!(client.inner.pending.lock().unwrap().is_empty());
    peer.await.unwrap();
}

#[tokio::test]
async fn shutdown_returns_while_owned_blocking_callback_drains() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let callback_started = started.clone();
    let callback_finished = finished.clone();
    let callback_release = release_rx.clone();
    let future = ResponseFuture::from_result(
        None,
        test_command(),
        Err(TransportError::Shutdown),
        1000,
        Instant::now(),
        Arc::new(move |_: Result<RemotingCommand, TransportError>| {
            callback_started.store(true, Ordering::Release);
            callback_release.lock().unwrap().recv().unwrap();
            callback_finished.store(true, Ordering::Release);
        }),
        None,
    );
    client.inner.dispatch_callback(CallbackJob {
        future,
        channel_established: Arc::new(AtomicBool::new(true)),
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !started.load(Ordering::Acquire) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();

    let started_at = Instant::now();
    client.shutdown().await;
    assert!(started_at.elapsed() < std::time::Duration::from_millis(250));
    assert!(!finished.load(Ordering::Acquire));
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !finished.load(Ordering::Acquire) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let mut cleanup_stopped = client.inner.shutdown_cleanup_stopped.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !*cleanup_stopped.borrow() {
            cleanup_stopped.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_channel_event_worker_only_until_java_join_limit() {
    struct SlowListener {
        entered: Notify,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        finished: AtomicBool,
    }
    impl ChannelEventListener for SlowListener {
        fn on_connect(&self, _: &str, _: &ChannelHandle) {
            self.entered.notify_one();
            self.release.lock().unwrap().recv().unwrap();
            self.finished.store(true, Ordering::Release);
        }
    }

    let (release, release_rx) = std::sync::mpsc::channel();
    let listener = Arc::new(SlowListener {
        entered: Notify::new(),
        release: Mutex::new(release_rx),
        finished: AtomicBool::new(false),
    });
    let client = RemotingClient::with_config_and_channel_event_listener(
        ClientConfig::default(),
        Some(listener.clone()),
    )
    .unwrap();
    client.start().unwrap();
    client
        .inner
        .notify_channel(NettyEventType::Connect, ChannelHandle::pending("peer"));
    listener.entered.notified().await;

    let shutdown_client = client.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_client
            .shutdown_with_timeout(std::time::Duration::from_millis(30))
            .await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !client.inner.closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let accepted_task_finished = Arc::new(AtomicBool::new(false));
    let accepted_task_finished_in_task = accepted_task_finished.clone();
    client
        .inner
        .public_executor
        .execute(Box::new(move || {
            accepted_task_finished_in_task.store(true, Ordering::Release);
        }))
        .expect("public executor remains open until the channel event join ends");

    let started_at = Instant::now();
    shutdown.await.unwrap();
    assert!(started_at.elapsed() >= std::time::Duration::from_millis(20));
    assert!(started_at.elapsed() < std::time::Duration::from_millis(250));
    assert!(!listener.finished.load(Ordering::Acquire));
    assert!(!*client.inner.channel_event_stopped.subscribe().borrow());
    assert!(
        client
            .inner
            .public_executor
            .execute(Box::new(|| {}))
            .is_err()
    );

    release.send(()).unwrap();
    let mut stopped = client.inner.channel_event_stopped.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !*stopped.borrow() {
            stopped.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(listener.finished.load(Ordering::Acquire));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !accepted_task_finished.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut cleanup_stopped = client.inner.shutdown_cleanup_stopped.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !*cleanup_stopped.borrow() {
            cleanup_stopped.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_is_idempotent_and_new_calls_are_rejected() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    client.shutdown().await;
    client.shutdown().await;
    assert!(matches!(
        client
            .invoke(
                "127.0.0.1:1",
                test_command(),
                Instant::now() + std::time::Duration::from_secs(1)
            )
            .await,
        Err(TransportError::Shutdown)
    ));
}
