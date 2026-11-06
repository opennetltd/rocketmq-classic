use crate::config::{SharedClientConfig, java_whitespace, read_shared_client_config};
use rocketmq_classic_remoting::{
    RemotingCommand, RequestType,
    client::{RemotingClient, RpcHook, RpcHookError},
};
use std::sync::Arc;

struct NamespaceFields(SharedClientConfig);
impl RpcHook for NamespaceFields {
    fn before_request_mut_result(
        &self,
        _: &str,
        command: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        let namespace = read_shared_client_config(&self.0).namespace_v2.clone();
        if let Some(namespace) = namespace.as_deref().filter(|value| !value.is_empty()) {
            let fields = command.ext_fields.get_or_insert_with(Default::default);
            fields.insert("nsd".into(), "true".into());
            fields.insert("ns".into(), namespace.into());
        }
        Ok(())
    }
}

struct StreamFields;
impl RpcHook for StreamFields {
    fn before_request_mut_result(
        &self,
        _: &str,
        command: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        command
            .ext_fields
            .get_or_insert_with(Default::default)
            .insert("ReqT".into(), RequestType::Stream.get_code().to_string());
        Ok(())
    }
}

type PropertyReader = dyn Fn(&str) -> Option<String> + Send + Sync;

struct DynamicFields {
    read: Arc<PropertyReader>,
}
impl RpcHook for DynamicFields {
    fn before_request_mut_result(
        &self,
        _: &str,
        command: &mut RemotingCommand,
    ) -> Result<(), RpcHookError> {
        for (property, environment, field) in [
            ("rocketmq.zone", "ROCKETMQ_ZONE", "__ZONE_NAME"),
            ("rocketmq.zone.mode", "ROCKETMQ_ZONE_MODE", "__ZONE_MODE"),
        ] {
            if let Some(value) = (self.read)(property)
                .or_else(|| (self.read)(environment))
                .filter(|value| !value.chars().all(java_whitespace))
            {
                command
                    .ext_fields
                    .get_or_insert_with(Default::default)
                    .insert(field.into(), value);
            }
        }
        Ok(())
    }
}

pub(super) fn register(
    client: &RemotingClient,
    config: SharedClientConfig,
    user: Option<Arc<dyn RpcHook>>,
) {
    register_with_dynamic(
        client,
        config,
        user,
        DynamicFields {
            read: Arc::new(|key| std::env::var(key).ok()),
        },
    );
}

fn register_with_dynamic(
    client: &RemotingClient,
    config: SharedClientConfig,
    user: Option<Arc<dyn RpcHook>>,
    dynamic: DynamicFields,
) {
    let initial_config = read_shared_client_config(&config).clone();
    client.add_rpc_hook(Arc::new(NamespaceFields(config)));
    if initial_config.enable_stream_request_type {
        client.add_rpc_hook(Arc::new(StreamFields));
    }
    if let Some(user) = user {
        client.add_rpc_hook(user);
    }
    client.add_rpc_hook(Arc::new(dynamic));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;
    use rocketmq_classic_remoting::{
        RemotingCommand,
        client::{RemotingClient, RpcHook, RpcHookError, TransportMode},
    };
    use std::sync::Arc;

    #[test]
    fn dynamic_properties_are_read_each_call_with_property_precedence_and_blank_skip() {
        use std::collections::HashMap;
        use std::sync::Mutex;
        let values = Arc::new(Mutex::new(HashMap::from([
            ("ROCKETMQ_ZONE", "fallback"),
            ("ROCKETMQ_ZONE_MODE", "env-mode"),
        ])));
        let source = values.clone();
        let dynamic = DynamicFields {
            read: Arc::new(move |key| source.lock().unwrap().get(key).map(|value| (*value).into())),
        };
        let mut command = RemotingCommand::default();
        dynamic
            .before_request_mut_result("peer", &mut command)
            .unwrap();
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["__ZONE_NAME"],
            "fallback"
        );
        values.lock().unwrap().insert("rocketmq.zone", "property");
        values.lock().unwrap().insert("rocketmq.zone.mode", "");
        dynamic
            .before_request_mut_result("peer", &mut command)
            .unwrap();
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["__ZONE_NAME"],
            "property"
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["__ZONE_MODE"],
            "env-mode"
        );
        values.lock().unwrap().insert("rocketmq.zone", "\u{2003}");
        dynamic
            .before_request_mut_result("peer", &mut command)
            .unwrap();
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["__ZONE_NAME"],
            "property"
        );
        values.lock().unwrap().remove("rocketmq.zone");
        values.lock().unwrap().insert("ROCKETMQ_ZONE", "changed");
        dynamic
            .before_request_mut_result("peer", &mut command)
            .unwrap();
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["__ZONE_NAME"],
            "changed"
        );
    }

    #[tokio::test]
    async fn namespace_v2_and_stream_hook_collisions_reuse_the_first_runtime() {
        let manager = crate::runtime::ClientRuntimeManager::new();
        let mut config = ClientConfig::new("ip");
        let first = manager.get_or_create(&config).await.unwrap();
        config.namespace_v2 = Some("v2".into());
        assert!(Arc::ptr_eq(
            &first,
            &manager.get_or_create(&config).await.unwrap()
        ));
        config.namespace_v2 = None;
        config.namespace = Some("resource-only".into());
        assert!(Arc::ptr_eq(
            &first,
            &manager.get_or_create(&config).await.unwrap()
        ));
        config.unit_name = Some("unit@STREAM".into());
        let collision = manager.get_or_create(&config).await.unwrap();
        config.unit_name = Some("unit".into());
        config.enable_stream_request_type = true;
        assert_eq!(config.build_client_id(), collision.client_id());
        assert!(Arc::ptr_eq(
            &collision,
            &manager.get_or_create(&config).await.unwrap()
        ));
    }

    struct Signer(serde_json::Value);
    impl RpcHook for Signer {
        fn before_request_mut_result(
            &self,
            _: &str,
            request: &mut RemotingCommand,
        ) -> Result<(), RpcHookError> {
            assert_eq!(serde_json::to_value(&request.ext_fields).unwrap(), self.0);
            let fields = request.ext_fields.as_mut().unwrap();
            for (key, value) in [
                ("ns", "signed-ns"),
                ("nsd", "signed-flag"),
                ("ReqT", "signature-type"),
                ("__ZONE_NAME", "user-zone"),
                ("__ZONE_MODE", "user-mode"),
            ] {
                fields.insert(key.into(), value.into());
            }
            Ok(())
        }
    }

    #[test]
    fn runtime_hook_chain_matches_actual_java_before_signing_and_after_dynamic_fields() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/hooks.json")).unwrap();
        assert_eq!(fixture["cases"].as_array().unwrap().len(), 36);
        for case in fixture["cases"].as_array().unwrap() {
            let mut config = ClientConfig::new("ip");
            config.namespace_v2 = case["namespaceV2"].as_str().map(str::to_owned);
            config.enable_stream_request_type = case["stream"].as_bool().unwrap();
            let client = RemotingClient::new(TransportMode::Plain);
            let zone = case["zone"].as_str().unwrap().to_owned();
            register_with_dynamic(
                &client,
                Arc::new(std::sync::RwLock::new(config.clone())),
                Some(Arc::new(Signer(case["beforeUser"].clone()))),
                DynamicFields {
                    read: Arc::new(move |_| Some(zone.clone())),
                },
            );
            let mut command = RemotingCommand {
                ext_fields: Some(
                    [
                        ("ns", "seed-ns"),
                        ("nsd", "false"),
                        ("ReqT", "seed-type"),
                        ("__ZONE_NAME", "seed-zone"),
                        ("__ZONE_MODE", "seed-mode"),
                    ]
                    .into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
                ),
                ..Default::default()
            };
            let hooks = client.get_rpc_hooks();
            assert_eq!(
                hooks.len(),
                3 + usize::from(config.enable_stream_request_type)
            );
            for hook in hooks {
                hook.before_request_mut_result("peer", &mut command)
                    .unwrap();
            }
            assert_eq!(
                serde_json::to_value(command.ext_fields).unwrap(),
                case["afterDynamic"]
            );
        }
    }
}
