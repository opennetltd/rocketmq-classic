use bytes::BytesMut;
use rocketmq_classic_core::{config::ClientConfig, runtime::ClientRuntimeManager};
use rocketmq_classic_protocol::{
    codes::{LanguageCode, request},
    header::UpdateConsumerOffsetRequestHeader,
    heartbeat::HeartbeatData,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RpcHook, RpcHookError},
    codec::frame,
};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Duration, Instant},
};

struct Signer;

#[derive(Default)]
struct NullableLanguageHook {
    before: Mutex<Vec<Option<LanguageCode>>>,
    after: Mutex<Vec<Option<LanguageCode>>>,
}

impl RpcHook for NullableLanguageHook {
    fn before_request_mut_result(
        &self,
        _: &str,
        command: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        let mut seen = self.before.lock().unwrap();
        seen.push(command.language);
        if seen.len() == 2 {
            command.language = Some(LanguageCode::Rust);
        }
        Ok(())
    }

    fn after_response(&self, _: &str, request: &RemotingCommand, _: &RemotingCommand) {
        self.after.lock().unwrap().push(request.language);
    }
}

#[tokio::test]
async fn nullable_heartbeat_language_reaches_hooks_and_json_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut config = ClientConfig::new("nullable-language-hook");
    config.set_namesrv_addr(Some(address.clone()));
    config.set_language(None);
    let hook = Arc::new(NullableLanguageHook::default());
    let runtime = ClientRuntimeManager::new()
        .get_or_create_with_hook(&config, Some(hook.clone()))
        .await
        .unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for expected in [None, Some("RUST")] {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let header_length = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) & 0x00ff_ffff;
            let header: serde_json::Value =
                serde_json::from_slice(&bytes[8..8 + header_length as usize]).unwrap();
            assert_eq!(
                header.get("language").and_then(serde_json::Value::as_str),
                expected
            );
            let command = frame::decode_next(&mut bytes).unwrap().unwrap();
            assert_eq!(command.code, request::HEART_BEAT);
            let mut reply = RemotingCommand {
                opaque: command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        }
    });
    runtime.remoting().start().unwrap();
    for _ in 0..2 {
        runtime
            .api()
            .send_heartbeat(
                &address,
                &HeartbeatData::default(),
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
    }
    peer.await.unwrap();
    assert_eq!(*hook.before.lock().unwrap(), [None, None]);
    assert_eq!(
        *hook.after.lock().unwrap(),
        [None, Some(LanguageCode::Rust)]
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn runtime_config_mutations_reach_heartbeat_language_and_namespace_hook() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut config = ClientConfig::new("live-runtime-config");
    config.set_namesrv_addr(Some(address.clone()));
    config.set_namespace_v2(Some("before".into()));
    config.set_language(Some(LanguageCode::Java));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let shared = runtime.config();
    {
        let mut config = shared.write().unwrap();
        config.set_namespace_v2(Some("after".into()));
        config.set_language(Some(LanguageCode::Cpp));
    }
    assert_eq!(config.namespace_v2(), Some("before"));

    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let command = frame::decode_next(&mut bytes).unwrap().unwrap();
        assert_eq!(command.code, request::HEART_BEAT);
        assert_eq!(command.language, Some(LanguageCode::Cpp));
        let fields = command.ext_fields.as_ref().unwrap();
        assert_eq!(fields["ns"], "after");
        assert_eq!(fields["nsd"], "true");
        let mut reply = RemotingCommand {
            opaque: command.opaque,
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
            .await
            .unwrap();
    });
    runtime.remoting().start().unwrap();
    runtime
        .api()
        .send_heartbeat(
            &address,
            &HeartbeatData::default(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

impl RpcHook for Signer {
    fn before_request_mut_result(
        &self,
        _: &str,
        command: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        let fields = command.ext_fields.as_mut().unwrap();
        assert_eq!(fields["ns"], "v2");
        assert_eq!(fields["nsd"], "true");
        assert_eq!(fields["ReqT"], "0");
        fields.insert("signature".into(), "ns=v2;ReqT=0".into());
        Ok(())
    }
}

#[tokio::test]
async fn runtime_hooks_reach_nameserver_heartbeat_v1_v2_and_oneway_with_source_specific_language() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut config = ClientConfig::new("192.0.2.6");
    config.set_namesrv_addr(Some(address.clone()));
    config.set_namespace_v2(Some("v2".into()));
    config.set_enable_stream_request_type(true);
    config.set_language(Some(LanguageCode::Rust));
    let runtime = ClientRuntimeManager::new()
        .get_or_create_with_hook(&config, Some(Arc::new(Signer)))
        .await
        .unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for code in [
            request::GET_ROUTEINFO_BY_TOPIC,
            request::HEART_BEAT,
            request::HEART_BEAT,
            request::UPDATE_CONSUMER_OFFSET,
        ] {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes).unwrap().unwrap();
            assert_eq!(command.code, code);
            assert_eq!(
                command.language,
                Some(if code == request::HEART_BEAT {
                    LanguageCode::Rust
                } else {
                    LanguageCode::Java
                })
            );
            let fields = command.ext_fields.as_ref().unwrap();
            assert_eq!(fields["signature"], "ns=v2;ReqT=0");
            if command.is_oneway() {
                continue;
            }
            let mut reply = RemotingCommand {
                opaque: command.opaque,
                version: 551,
                body: (code == request::GET_ROUTEINFO_BY_TOPIC).then(|| {
                    br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#.to_vec()
                }),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        }
    });
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    runtime
        .api()
        .send_heartbeat(
            &address,
            &HeartbeatData::default(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    runtime
        .api()
        .send_heartbeat_v2(
            &address,
            &HeartbeatData::default(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    runtime
        .api()
        .update_consumer_offset(
            &address,
            &UpdateConsumerOffsetRequestHeader::default(),
            true,
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}
