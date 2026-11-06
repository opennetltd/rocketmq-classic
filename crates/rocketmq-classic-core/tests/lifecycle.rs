use bytes::BytesMut;
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntime, ClientRuntimeManager, ProducerRuntime, RuntimeState},
};
use rocketmq_classic_protocol::codes::request;
use rocketmq_classic_remoting::{RemotingCommand, codec::frame};
use std::sync::{Arc, Weak};
use tokio::time::{Duration, Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Producer;
impl ProducerRuntime for Producer {}

fn explicit_config(ip: &str) -> ClientConfig {
    let mut config = ClientConfig::new(ip);
    config.set_namesrv_addr(Some(String::new()));
    config
}

#[tokio::test]
async fn shared_shutdown_uses_registration_identity_not_runtime_arc_counts() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&explicit_config("192.0.2.6"))
        .await
        .unwrap();
    let producer = Arc::new(Producer);
    let first = runtime
        .register_producer("first", producer.clone())
        .await
        .unwrap();
    let second = runtime
        .register_producer("second", producer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime.start().await.unwrap();
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert!(!runtime.shutdown().await.unwrap());
    let deadline = Instant::now() + Duration::from_secs(1);
    runtime.unregister_producer(&first, deadline).await.unwrap();
    assert!(!runtime.shutdown().await.unwrap());
    assert_eq!(runtime.state(), RuntimeState::Running);
    runtime
        .unregister_producer(&second, deadline)
        .await
        .unwrap();
    let extra_owner = runtime.clone();
    assert!(runtime.shutdown().await.unwrap());
    assert_eq!(extra_owner.state(), RuntimeState::Stopped);
    assert!(!runtime.shutdown().await.unwrap());
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["runtimeRestartAfterShutdown"];
    assert_eq!(java["stateBeforeShutdown"], "RUNNING");
    assert_eq!(java["shutdownCall"], "OK");
    assert_eq!(java["stateAfterShutdown"], "SHUTDOWN_ALREADY");
    assert_eq!(java["schedulerShutdownAfterShutdown"], true);
    assert_eq!(java["secondStart"], "OK");
    assert_eq!(java["stateAfterSecondStart"], "SHUTDOWN_ALREADY");
    assert_eq!(java["schedulerShutdownAfterSecondStart"], true);
    runtime.start().await.unwrap();
    assert_eq!(runtime.state(), RuntimeState::Stopped);
    let replacement = manager
        .get_or_create(&explicit_config("192.0.2.6"))
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&runtime, &replacement));
}

#[tokio::test]
async fn admin_registration_blocks_shutdown_until_owner_is_unregistered() {
    use std::any::Any;

    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let expected = &fixture["registryTables"]["adminRegistration"]["shutdownGate"];
    let mut config = explicit_config("admin-shutdown-gate");
    config.set_namesrv_addr(Some(String::new()));
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();

    struct AdminOwner(Weak<ClientRuntime>);
    let runtime_weak = Arc::downgrade(&runtime);
    let owner = Arc::new(AdminOwner(Arc::downgrade(&runtime)));
    assert!(owner.0.upgrade().is_some());
    let owner_weak = Arc::downgrade(&owner);
    let erased_owner: Arc<dyn Any + Send + Sync> = owner.clone();
    runtime.start().await.unwrap();
    assert_eq!(
        runtime
            .register_admin_ext(Some("shutdown-admin"), Some(erased_owner))
            .unwrap(),
        expected["registrationAccepted"].as_bool().unwrap()
    );
    drop(owner);
    assert!(owner_weak.upgrade().is_some());

    let stopped_while_registered = runtime.shutdown().await.unwrap();
    assert_eq!(
        !stopped_while_registered,
        expected["shutdownBlockedByAdmin"].as_bool().unwrap()
    );
    assert_eq!(runtime.state(), RuntimeState::Running);
    let same_runtime = manager.get_or_create(&config).await.unwrap();
    assert!(Arc::ptr_eq(&runtime, &same_runtime));

    runtime
        .unregister_admin_ext(Some("shutdown-admin"))
        .unwrap();
    assert!(owner_weak.upgrade().is_none());
    assert_eq!(
        runtime.shutdown().await.unwrap(),
        expected["shutdownAfterAdminRemoval"].as_bool().unwrap()
    );
    assert_eq!(runtime.state(), RuntimeState::Stopped);
    drop(same_runtime);
    drop(runtime);
    drop(manager);
    assert!(runtime_weak.upgrade().is_none());
}

#[tokio::test]
async fn many_concurrent_start_calls_observe_one_serialized_transition() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&explicit_config("192.0.2.6"))
        .await
        .unwrap();
    let mut calls = Vec::new();
    for _ in 0..64 {
        let runtime = runtime.clone();
        calls.push(tokio::spawn(async move {
            runtime.start().await.unwrap();
        }));
    }
    for call in calls {
        call.await.unwrap();
    }
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert!(runtime.shutdown().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdown_callers_join_the_same_terminal_control_task() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&explicit_config("shutdown-race"))
        .await
        .unwrap();
    runtime.start().await.unwrap();
    let mut calls = Vec::new();
    for _ in 0..64 {
        let runtime = runtime.clone();
        calls.push(tokio::spawn(
            async move { runtime.shutdown().await.unwrap() },
        ));
    }
    let mut actual_stops = 0;
    for call in calls {
        actual_stops += usize::from(call.await.unwrap());
    }
    assert_eq!(actual_stops, 1);
    assert_eq!(runtime.state(), RuntimeState::Stopped);
}

async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn respond(stream: &mut TcpStream, command: &RemotingCommand, body: Option<Vec<u8>>) {
    let mut response = RemotingCommand {
        opaque: command.opaque,
        body,
        ..Default::default()
    };
    response.mark_response();
    stream
        .write_all(&frame::encode(&response).unwrap())
        .await
        .unwrap();
}

async fn read_unregister(stream: &mut TcpStream) -> RemotingCommand {
    loop {
        let command = read_command(stream).await;
        if command.code != request::HEART_BEAT {
            return command;
        }
        let heartbeat: rocketmq_classic_protocol::heartbeat::HeartbeatData =
            serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(heartbeat.client_id.as_deref(), Some("192.0.2.6@DEFAULT"));
        assert!(
            heartbeat
                .consumer_data_set
                .as_ref()
                .unwrap()
                .with(|consumers| consumers.is_empty())
        );
        // A newly established unregister channel now emits Java's optional
        // active heartbeat. Acknowledge it without weakening unregister order.
        respond(stream, &command, None).await;
    }
}

#[tokio::test]
async fn cancelled_unregister_finishes_before_replacement_registration_and_last_shutdown_closes_after_unregister()
 {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap().to_string();
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("192.0.2.6");
    config.set_namesrv_addr(Some(nameserver.local_addr().unwrap().to_string()));
    let (observed, started) = tokio::sync::oneshot::channel();
    let (release, reply_allowed) = tokio::sync::oneshot::channel();
    let (last_observed, last_started) = tokio::sync::oneshot::channel();
    let (last_release, last_reply_allowed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let command = read_unregister(&mut stream).await;
        assert_eq!(command.code, request::UNREGISTER_CLIENT);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["producerGroup"],
            "producer"
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["clientID"],
            "192.0.2.6@DEFAULT"
        );
        assert!(
            !command
                .ext_fields
                .as_ref()
                .unwrap()
                .contains_key("consumerGroup")
        );
        observed.send(()).unwrap();
        reply_allowed.await.unwrap();
        respond(&mut stream, &command, None).await;
        let command = read_unregister(&mut stream).await;
        assert_eq!(command.code, request::UNREGISTER_CLIENT);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["producerGroup"],
            "producer"
        );
        respond(&mut stream, &command, None).await;
        let command = read_unregister(&mut stream).await;
        assert_eq!(command.code, request::UNREGISTER_CLIENT);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["producerGroup"],
            "CLIENT_INNER_PRODUCER"
        );
        last_observed.send(()).unwrap();
        last_reply_allowed.await.unwrap();
        respond(&mut stream, &command, None).await;
        let mut extra = [0; 1];
        assert_eq!(stream.read(&mut extra).await.unwrap(), 0);
    });
    let name_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        let route = serde_json::json!({"queueDatas":[],"brokerDatas":[{"brokerName":"broker","cluster":"cluster","brokerAddrs":{"0":broker_address}}],"filterServerTable":{}});
        respond(
            &mut stream,
            &command,
            Some(serde_json::to_vec(&route).unwrap()),
        )
        .await;
    });
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();
    let producer = Arc::new(Producer);
    let old = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    name_peer.await.unwrap();
    let cleanup_runtime = runtime.clone();
    let old_token = old.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_runtime
            .unregister_producer(&old_token, Instant::now() + Duration::from_secs(5))
            .await
    });
    started.await.unwrap();
    cleanup.abort();
    assert!(matches!(cleanup.await, Err(error) if error.is_cancelled()));
    let reset_runtime = runtime.clone();
    let mut resetting = tokio::spawn(async move {
        reset_runtime
            .reset_offset(None, Some("missing"), None)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut resetting)
            .await
            .is_err(),
        "reset must wait until the owned unregister command completes after caller cancellation"
    );
    let registering_runtime = runtime.clone();
    let registering_producer = producer.clone();
    let mut registering = tokio::spawn(async move {
        registering_runtime
            .register_producer("producer", registering_producer)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut registering)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    resetting.await.unwrap().unwrap();
    let replacement = registering.await.unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    assert!(!runtime.unregister_producer(&old, deadline).await.unwrap());
    assert!(
        runtime
            .unregister_producer(&replacement, deadline)
            .await
            .unwrap()
    );
    let stopping_runtime = runtime.clone();
    let stopping = tokio::spawn(async move { stopping_runtime.shutdown().await });
    last_started.await.unwrap();
    stopping.abort();
    assert!(matches!(stopping.await, Err(error) if error.is_cancelled()));
    last_release.send(()).unwrap();
    assert!(!runtime.shutdown().await.unwrap());
    peer.await.unwrap();
    assert_eq!(runtime.state(), RuntimeState::Stopped);
}
