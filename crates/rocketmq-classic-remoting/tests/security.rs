use std::{collections::HashMap, fs, process::Command, sync::Arc, time::Duration};

use bytes::BytesMut;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, KeyPair, date_time_ymd};
use rocketmq_classic_protocol::{codes::request, header::HeaderMap};
use rocketmq_classic_remoting::{
    RemotingCommand, TransportError,
    client::{RemotingClient, TransportMode, tls::TlsConfig},
    codec::frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};
use tokio_rustls::{TlsAcceptor, rustls};

async fn read_tls_command(stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>) {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    let request = frame::decode_next(&mut bytes).unwrap().unwrap();
    let mut response = RemotingCommand {
        code: 0,
        opaque: request.opaque,
        ..RemotingCommand::default()
    };
    response.mark_response();
    stream
        .write_all(&frame::encode(&response).unwrap())
        .await
        .unwrap();
}

fn server_config(certificate: &Certificate, key: &KeyPair) -> rustls::ServerConfig {
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
                key.serialize_der(),
            )),
        )
        .unwrap()
}

fn command() -> RemotingCommand {
    RemotingCommand {
        code: request::GET_ROUTEINFO_BY_TOPIC,
        ext_fields: Some(HeaderMap::from([("topic".into(), "orders".into())]).into()),
        ..RemotingCommand::default()
    }
}

async fn assert_tls_connection_rejected(tls_config: TlsConfig, acceptor: TlsAcceptor) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        if let Ok(Ok(mut stream)) = timeout(Duration::from_secs(2), acceptor.accept(socket)).await {
            if timeout(Duration::from_millis(250), read_tls_command(&mut stream))
                .await
                .is_ok()
            {}
        }
    });
    let client = RemotingClient::new(TransportMode::Tls(tls_config));
    client.start().unwrap();
    let address = format!("localhost:{port}");
    let result = client
        .invoke(&address, command(), Instant::now() + Duration::from_secs(2))
        .await;
    assert!(
        matches!(result, Err(TransportError::ConnectRequest { .. }))
            || matches!(result, Err(TransportError::SendRequest { .. }))
            || matches!(result, Err(TransportError::Tls(_)))
            || matches!(result, Err(TransportError::TlsSource(_))),
        "expected TLS failure, got {result:?}"
    );
    client.shutdown().await;
    peer.await.unwrap();
}

#[test]
fn tls_config_file_path_uses_initial_system_property_snapshot() {
    const PROBE: &str = "ROCKETMQ_TLS_CONFIG_PATH_SNAPSHOT_PROBE";
    const FIRST_FILE: &str = "ROCKETMQ_TLS_CONFIG_PATH_SNAPSHOT_FIRST";
    const SECOND_FILE: &str = "ROCKETMQ_TLS_CONFIG_PATH_SNAPSHOT_SECOND";

    if std::env::var_os(PROBE).is_some() {
        let first_file = std::env::var(FIRST_FILE).unwrap();
        let second_file = std::env::var(SECOND_FILE).unwrap();
        let initial_properties = HashMap::from([("tls.config.file".to_owned(), first_file)]);
        let first = TlsConfig::from_java_properties(&initial_properties, None).unwrap();
        assert!(!first.test_mode, "the first config file must be loaded");

        let later_properties = HashMap::from([("tls.config.file".to_owned(), second_file)]);
        let later = TlsConfig::from_java_properties(&later_properties, None).unwrap();
        let java_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/security/tls-settings-defaults.json"
        ))
        .unwrap();
        assert!(
            !later.test_mode && java_fixture["configFilePathSnapshot"].as_bool() == Some(true),
            "changing tls.config.file after initial property capture must keep using the first file"
        );
        return;
    }

    let directory = std::env::temp_dir().join(format!(
        "rocketmq-tls-config-path-snapshot-{}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let first_file = directory.join("first.properties");
    let second_file = directory.join("second.properties");
    fs::write(&first_file, "tls.test.mode.enable=false\n").unwrap();
    fs::write(&second_file, "tls.test.mode.enable=true\n").unwrap();

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("tls_config_file_path_uses_initial_system_property_snapshot")
        .arg("--nocapture")
        .env(PROBE, "1")
        .env(FIRST_FILE, &first_file)
        .env(SECOND_FILE, &second_file)
        .output()
        .unwrap();
    fs::remove_dir_all(&directory).unwrap();
    assert!(
        output.status.success(),
        "isolated TLS config path snapshot probe failed:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[tokio::test]
async fn expired_server_certificate_is_rejected_with_java_chain_validation_enabled() {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    server_params.not_before = date_time_ymd(1990, 1, 1);
    server_params.not_after = date_time_ymd(1991, 1, 1);
    let server_certificate = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config(&server_certificate, &server_key)));
    let mut config = TlsConfig::default();
    config.ca_pem = Some(ca.pem().into_bytes());
    config.test_mode = false;
    config.auth_server = true;
    assert_tls_connection_rejected(config, acceptor).await;
}

#[test]
fn tls_property_file_settings_are_process_wide_across_config_sources() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/security/tls-settings-defaults.json"
    ))
    .unwrap();
    let expected = &fixture["propertyFileAcrossSources"];

    let directory = std::env::temp_dir().join(format!(
        "rocketmq-tls-process-properties-{}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let first_file = directory.join("first.properties");
    let second_file = directory.join("second.properties");
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let ca_file = directory.join("trust-ca.pem");
    fs::write(&ca_file, ca.pem()).unwrap();
    fs::write(
        &first_file,
        format!(
            "tls.test.mode.enable=false\ntls.enable=true\ntls.client.trustCertPath={}\n",
            ca_file.display()
        ),
    )
    .unwrap();

    let initial_system_properties = HashMap::from([
        ("tls.enable".to_owned(), "false".to_owned()),
        ("tls.client.authServer".to_owned(), "true".to_owned()),
    ]);
    assert!(matches!(
        TransportMode::from_java_properties(&initial_system_properties, None).unwrap(),
        TransportMode::Plain
    ));
    let first = TlsConfig::from_java_properties(&HashMap::new(), Some(&first_file)).unwrap();
    assert!(!first.test_mode);
    assert!(first.auth_server);
    let first_test_mode = first.test_mode;
    let first_auth_server = first.auth_server;
    let _first_client =
        RemotingClient::with_config(rocketmq_classic_remoting::client::ClientConfig {
            transport: TransportMode::Tls(first),
            ..rocketmq_classic_remoting::client::ClientConfig::default()
        })
        .unwrap();

    fs::write(&second_file, "tls.test.mode.enable=false\n").unwrap();
    let later_system_property = expected["laterSystemPropertyAuthServer"]
        .as_bool()
        .unwrap()
        .to_string();
    let later_tls_enable = expected["laterSystemPropertyTlsEnable"]
        .as_bool()
        .unwrap()
        .to_string();
    let second_properties = HashMap::from([
        ("tls.client.authServer".to_owned(), later_system_property),
        ("tls.enable".to_owned(), later_tls_enable),
    ]);
    let second = TlsConfig::from_java_properties(&second_properties, Some(&second_file)).unwrap();
    assert!(
        expected["systemPropertyTlsEnableRetained"]
            .as_bool()
            .unwrap()
    );
    assert!(matches!(
        TransportMode::from_java_properties(&second_properties, None).unwrap(),
        TransportMode::Plain
    ));
    let observed_test_modes = vec![first_test_mode, second.test_mode];
    let observed_auth_server = vec![first_auth_server, second.auth_server];

    assert_eq!(
        observed_test_modes,
        expected["observedTestModes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_bool().unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        observed_auth_server,
        expected["observedAuthServer"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_bool().unwrap())
            .collect::<Vec<_>>()
    );

    let _second_client =
        RemotingClient::with_config(rocketmq_classic_remoting::client::ClientConfig {
            transport: TransportMode::Tls(second),
            ..rocketmq_classic_remoting::client::ClientConfig::default()
        })
        .unwrap();
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn plaintext_peer_is_rejected_when_client_expects_tls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut client_hello = [0; 1];
        let _ = timeout(Duration::from_secs(1), socket.read_exact(&mut client_hello)).await;
    });
    let client = RemotingClient::new(TransportMode::Tls(TlsConfig::default()));
    client.start().unwrap();
    let address = format!("localhost:{port}");
    let result = client
        .invoke(&address, command(), Instant::now() + Duration::from_secs(2))
        .await;
    assert!(matches!(result, Err(TransportError::ConnectRequest { .. })));
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn missing_client_certificate_is_rejected_by_a_required_client_auth_peer() {
    let server_key = KeyPair::generate().unwrap();
    let server_certificate = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&server_key)
        .unwrap();
    let client_ca_key = KeyPair::generate().unwrap();
    let mut client_ca_params = CertificateParams::new(Vec::new()).unwrap();
    client_ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let client_ca = client_ca_params.self_signed(&client_ca_key).unwrap();
    let mut client_roots = rustls::RootCertStore::empty();
    client_roots.add(client_ca.der().clone()).unwrap();
    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .unwrap();
    let server_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(
            vec![server_certificate.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
                server_key.serialize_der(),
            )),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    assert_tls_connection_rejected(TlsConfig::default(), acceptor).await;
}

#[test]
fn malformed_tls_identity_is_rejected_before_remoting_client_is_created() {
    let mut tls = TlsConfig::default();
    tls.test_mode = false;
    tls.client_cert_pem = Some(b"not a certificate".to_vec());
    tls.client_key_pem = Some(b"not a private key".to_vec());
    let result = RemotingClient::with_config(rocketmq_classic_remoting::client::ClientConfig {
        transport: TransportMode::Tls(tls),
        ..rocketmq_classic_remoting::client::ClientConfig::default()
    });
    assert!(matches!(result, Err(TransportError::Tls(_))));
}

#[test]
fn unreadable_tls_identity_is_rejected_while_loading_properties() {
    let directory = std::env::temp_dir().join(format!(
        "rocketmq-security-missing-identity-{}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let mut config = TlsConfig::default();
    config.test_mode = false;
    let properties = std::collections::HashMap::from([
        ("tls.client.certPath".into(), "missing-client.pem".into()),
        ("tls.client.keyPath".into(), "missing-client.key".into()),
    ]);
    let result = config.apply_properties(&properties, &directory);
    fs::remove_dir_all(&directory).unwrap();
    assert!(matches!(result, Err(TransportError::Tls(_))));
}
