use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::auth::{AclClientRpcHook, AclSigner, AclUtils, SessionCredentials};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let frame_length = u32::from_be_bytes(length) as usize;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(frame_length + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn write_response(stream: &mut TcpStream, request: &RemotingCommand, accepted: bool) {
    let mut response = RemotingCommand {
        code: if accepted { 0 } else { 1 },
        opaque: request.opaque,
        remark: (!accepted).then(|| "ACL signature rejected".to_owned()),
        ..RemotingCommand::default()
    };
    response.mark_response();
    stream
        .write_all(&frame::encode(&response).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn scripted_acl_profile_rejects_wrong_secret_and_tampered_body() {
    const ACCESS_KEY_VALUE: &str = "profile-access-key";
    const SERVER_SECRET: &str = "profile-server-secret";

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let mut decisions = Vec::new();
        for case_index in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = read_command(&mut stream).await;
            assert_eq!(
                request.body.as_deref(),
                Some(b"signed request body".as_slice())
            );

            // Model a body changed after signing but before the auth profile checks it.
            if case_index == 2 {
                request.body = Some(b"tampered request body".to_vec());
            }
            let fields = request.ext_fields.as_ref().unwrap();
            let actual_signature = fields
                .get(SessionCredentials::SIGNATURE)
                .map(String::as_str);
            let ordered_fields = fields
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>();
            let canonical_content =
                AclUtils::combine_request_content_with_fields(&request, ordered_fields);
            let expected_signature = AclSigner::cal_signature(&canonical_content, SERVER_SECRET);
            let accepted = fields
                .get(SessionCredentials::ACCESS_KEY)
                .map(String::as_str)
                == Some(ACCESS_KEY_VALUE)
                && actual_signature == Some(expected_signature.as_str());
            decisions.push(accepted);
            write_response(&mut stream, &request, accepted).await;
        }
        decisions
    });

    for (secret, expected_code) in [(SERVER_SECRET, 0), ("wrong-secret", 1), (SERVER_SECRET, 1)] {
        let client = RemotingClient::new(TransportMode::Plain);
        client.add_rpc_hook(Arc::new(AclClientRpcHook::new(SessionCredentials::new(
            ACCESS_KEY_VALUE,
            secret,
        ))));
        client.start().unwrap();
        let response = client
            .invoke_sync(
                &address,
                RemotingCommand {
                    code: 900,
                    body: Some(b"signed request body".to_vec()),
                    ..RemotingCommand::default()
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(response.code, expected_code);
        client.shutdown().await;
    }

    assert_eq!(peer.await.unwrap(), [true, false, false]);
}
