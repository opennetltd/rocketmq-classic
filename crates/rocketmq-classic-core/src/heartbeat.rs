use crate::runtime::{ClientRuntime, RuntimeError};
use rocketmq_classic_protocol::{SharedValue, heartbeat::HeartbeatData};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::Poll,
};

#[derive(Default)]
pub(crate) struct HeartbeatChannelListener(OnceLock<Weak<ClientRuntime>>);
impl HeartbeatChannelListener {
    pub fn bind(&self, runtime: &Arc<ClientRuntime>) {
        let _ = self.0.set(Arc::downgrade(runtime));
    }
}
impl rocketmq_classic_remoting::client::ChannelEventListener for HeartbeatChannelListener {
    fn on_active_async<'a>(
        &'a self,
        address: &'a str,
        _: &'a rocketmq_classic_remoting::client::ChannelHandle,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let Some(runtime) = self.0.get().and_then(Weak::upgrade) else {
                return;
            };
            if matches!(
                runtime.state(),
                crate::runtime::RuntimeState::ShuttingDown
                    | crate::runtime::RuntimeState::Stopped
                    | crate::runtime::RuntimeState::Failed
            ) {
                return;
            }
            let Ok(routes) = runtime.route_snapshot() else {
                return;
            };
            for (broker, addresses) in &routes.broker_addresses {
                if let Some((id, _)) = addresses
                    .iter()
                    .find(|(_, value)| value.as_deref() == Some(address))
                {
                    let Some(id) = id else {
                        continue;
                    };
                    if runtime
                        .send_heartbeat_to_broker_with_lock_mode(*id, broker, address, false)
                        .await
                        .unwrap_or(false)
                    {
                        runtime.rebalance_immediately();
                    }
                }
            }
        })
    }
}

#[derive(Default)]
pub(crate) struct HeartbeatState {
    gate: tokio::sync::Mutex<()>,
    peers: Mutex<PeerState>,
}

#[derive(Default)]
struct PeerState {
    support_v2: HashSet<String>,
    fingerprints: HashMap<String, i32>,
}

type PeerFuture<'a> = Pin<Box<dyn Future<Output = Result<bool, RuntimeError>> + Send + 'a>>;

fn shared_set_is_empty<T>(values: &Option<SharedValue<HashSet<T>>>) -> bool {
    values
        .as_ref()
        .is_none_or(|values| values.with(HashSet::is_empty))
}

fn clear_heartbeat_subscriptions(data: &HeartbeatData) {
    let Some(consumers) = data.consumer_data_set.as_ref() else {
        return;
    };
    for consumer in consumers.snapshot().into_iter().flatten() {
        consumer.with_mut(|consumer| {
            if let Some(subscriptions) = consumer.subscription_data_set.as_ref() {
                subscriptions.with_mut(HashSet::clear);
            }
        });
    }
}

/// The fingerprint is client-local change detection, not a server-computed
/// checksum. Canonical set order is Rust-native; Java HashSet iteration is not
/// part of the wire contract. Preserve Java's reset fields and UTF-16 hash.
fn fingerprint(data: &HeartbeatData) -> Result<i32, RuntimeError> {
    data.compute_heartbeat_fingerprint()
        .map_err(RuntimeError::HeartbeatEncoding)
}

impl ClientRuntime {
    fn prepare_heartbeat_for_send(&self) -> Result<Option<HeartbeatData>, RuntimeError> {
        // Java's public heartbeat wrappers contain an owner's exception from
        // prepareHeartbeatData and return false. Keep the directly exposed
        // preparation capability unchanged; contain failure at the send entry.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.prepare_heartbeat_data(false)
        }))
        .map_or(Ok(None), |result| match result {
            Err(RuntimeError::InvalidHeartbeatData(_)) => Ok(None),
            other => other.map(Some),
        })
    }

    pub async fn send_heartbeat_to_broker(
        &self,
        id: i64,
        broker: &str,
        address: &str,
    ) -> Result<bool, RuntimeError> {
        self.send_heartbeat_to_broker_with_lock_mode(id, broker, address, true)
            .await
    }

    pub async fn send_heartbeat_to_broker_with_lock_mode(
        &self,
        _id: i64,
        broker: &str,
        address: &str,
        _strict_lock_mode: bool,
    ) -> Result<bool, RuntimeError> {
        let Ok(_guard) = self.heartbeat.gate.try_lock() else {
            return Ok(false);
        };
        let Some(mut full) = self.prepare_heartbeat_for_send()? else {
            return Ok(false);
        };
        if shared_set_is_empty(&full.producer_data_set)
            && shared_set_is_empty(&full.consumer_data_set)
        {
            return Ok(false);
        }
        let v2 = self.config_read().use_heartbeat_v2;
        if v2 {
            full.heartbeat_fingerprint = fingerprint(&full)?;
        }
        let mut simple = full.clone();
        simple.is_without_sub = true;
        clear_heartbeat_subscriptions(&simple);
        // Unlike the all-Broker round, the explicit single-peer method does not
        // skip a slave when only producers are registered (Java 5.5.1).
        self.send_heartbeat_peer(broker, address, &full, &simple, v2)
            .await
    }
    pub async fn send_heartbeat_to_all_brokers(&self) -> Result<bool, RuntimeError> {
        self.send_heartbeat_round(false, true).await
    }

    pub async fn send_heartbeat_to_all_brokers_with_rebalance(
        &self,
        is_rebalance: bool,
    ) -> Result<bool, RuntimeError> {
        // Java WithLockV2 uses serial V1 as its fallback, even when the
        // ordinary WithLock entry has concurrent V1 configured.
        self.send_heartbeat_round(is_rebalance, false).await
    }

    async fn send_heartbeat_round(
        &self,
        is_rebalance: bool,
        allow_concurrent_v1: bool,
    ) -> Result<bool, RuntimeError> {
        let Ok(_guard) = self.heartbeat.gate.try_lock() else {
            return Ok(false);
        };
        let Some(mut full) = self.prepare_heartbeat_for_send()? else {
            return Ok(false);
        };
        if shared_set_is_empty(&full.producer_data_set)
            && shared_set_is_empty(&full.consumer_data_set)
        {
            return Ok(false);
        }
        let routes = self.route_snapshot()?;
        if routes.broker_addresses.is_empty() {
            return Ok(false);
        }
        let v2 = self.config_read().use_heartbeat_v2;
        if v2 {
            full.heartbeat_fingerprint = fingerprint(&full)?;
            if is_rebalance {
                self.heartbeat
                    .peers
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?
                    .fingerprints
                    .clear();
            }
        }
        let mut simple = full.clone();
        simple.is_without_sub = true;
        clear_heartbeat_subscriptions(&simple);
        let has_consumers = !shared_set_is_empty(&full.consumer_data_set);
        let mut peers = Vec::new();
        for (broker, addresses) in &routes.broker_addresses {
            for (id, address) in addresses {
                let Some(address) = address.as_deref() else {
                    continue;
                };
                // Java skips null address values first, then unboxes the Long
                // key for its producer-only check and primitive send call.
                let id = id.ok_or(RuntimeError::InvalidRoute("null broker address id"))?;
                // Java compares the boxed Long with the primitive master ID
                // only for producer-only heartbeats, which unboxes the key.
                if !has_consumers && id != 0 {
                    continue;
                }
                peers.push((broker.as_str(), address));
            }
        }
        if !v2 && allow_concurrent_v1 && self.config_read().enable_concurrent_heartbeat {
            if peers.is_empty() {
                return Ok(false);
            }
            let Some(width) = self.concurrent_heartbeat_width else {
                // Java checks the mutable enable flag per heartbeat, but only
                // creates its executor in the constructor when it was enabled.
                return Ok(false);
            };
            let mut pending = peers.into_iter();
            let mut active: Vec<PeerFuture<'_>> = Vec::new();
            loop {
                while active.len() < width {
                    let Some((broker, address)) = pending.next() else {
                        break;
                    };
                    active.push(Box::pin(
                        self.send_heartbeat_peer(broker, address, &full, &simple, false),
                    ));
                }
                if active.is_empty() {
                    break;
                }
                let (index, result) = std::future::poll_fn(|context| {
                    for (index, operation) in active.iter_mut().enumerate() {
                        if let Poll::Ready(result) = operation.as_mut().poll(context) {
                            return Poll::Ready((index, result));
                        }
                    }
                    Poll::Pending
                })
                .await;
                drop(active.swap_remove(index));
                result?;
            }
        } else {
            for (broker, address) in peers {
                self.send_heartbeat_peer(broker, address, &full, &simple, v2)
                    .await?;
            }
        }
        Ok(true)
    }

    async fn send_heartbeat_peer(
        &self,
        broker: &str,
        address: &str,
        full: &HeartbeatData,
        simple: &HeartbeatData,
        v2: bool,
    ) -> Result<bool, RuntimeError> {
        let deadline = tokio::time::Instant::now() + self.rpc_timeout();
        let version = if v2 {
            let reduced = {
                let peers = self
                    .heartbeat
                    .peers
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?;
                peers.support_v2.contains(address)
                    && peers.fingerprints.get(address) == Some(&full.heartbeat_fingerprint)
            };
            let Ok(result) = self
                .api()
                .send_heartbeat_v2(address, if reduced { simple } else { full }, deadline)
                .await
            else {
                return Ok(false);
            };
            let mut peers = self
                .heartbeat
                .peers
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            if reduced {
                if result.is_sub_change {
                    peers.fingerprints.remove(address);
                }
            } else if result.is_support_heartbeat_v2 {
                peers.support_v2.insert(address.to_owned());
                if result.is_sub_change {
                    peers.fingerprints.remove(address);
                } else {
                    peers
                        .fingerprints
                        .insert(address.to_owned(), full.heartbeat_fingerprint);
                }
            }
            result.version
        } else {
            let Ok(version) = self.api().send_heartbeat(address, full, deadline).await else {
                return Ok(false);
            };
            version
        };
        self.record_broker_version(broker, address, version)
            .map(|_| true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ClientConfig,
        runtime::{ClientRuntimeManager, ConsumerRuntime},
    };
    use rocketmq_classic_protocol::heartbeat::{ConsumerData, SubscriptionData};

    #[tokio::test]
    async fn null_owner_subscription_set_matches_java_prepare_branch() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
        assert_eq!(
            fixture["nullOwnerSubscriptionPreparation"]["fullOutcome"],
            "NullPointerException"
        );
        assert_eq!(
            fixture["nullOwnerSubscriptionPreparation"]["simpleSubscriptionCount"],
            0
        );
        struct NullSubscriptions;
        impl ConsumerRuntime for NullSubscriptions {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData {
                    subscription_data_set: None,
                    ..Default::default()
                }
            }
        }
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&ClientConfig::new("null-subscription-set"))
            .await
            .unwrap();
        let owner = Arc::new(NullSubscriptions);
        let _lease = runtime
            .register_consumer("group", owner.clone())
            .await
            .unwrap();
        assert!(matches!(
            runtime.prepare_heartbeat_data(false),
            Err(RuntimeError::InvalidHeartbeatData("null subscription set"))
        ));
        let simple = runtime.prepare_heartbeat_data(true).unwrap();
        assert!(
            simple
                .consumer_data_set
                .as_ref()
                .unwrap()
                .with(|consumers| {
                    consumers.iter().flatten().next().unwrap().with(|consumer| {
                        consumer
                            .subscription_data_set
                            .as_ref()
                            .unwrap()
                            .with(HashSet::is_empty)
                    })
                })
        );
        assert!(
            !runtime
                .send_heartbeat_to_broker(0, "broker", "127.0.0.1:1")
                .await
                .unwrap()
        );
    }

    #[test]
    fn singleton_fingerprints_match_invoked_java_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let data: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
            assert_eq!(
                i64::from(fingerprint(&data).unwrap()),
                case["fingerprint"].as_i64().unwrap(),
                "{}",
                case["case"]
            );
        }
    }

    #[test]
    fn null_collection_fingerprints_match_invoked_java_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
        for case in fixture["fingerprintNullCases"].as_array().unwrap() {
            let mut data: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
            match case["case"].as_str().unwrap() {
                "null-producers" => data.producer_data_set = None,
                "null-consumers" => data.consumer_data_set = None,
                "null-subscriptions" => {
                    let consumer = data
                        .consumer_data_set
                        .as_ref()
                        .unwrap()
                        .with(|consumers| consumers.iter().flatten().next().unwrap().clone());
                    consumer.with_mut(|consumer| consumer.subscription_data_set = None);
                }
                _ => {}
            }
            assert_eq!(
                i64::from(fingerprint(&data).unwrap()),
                case["fingerprint"],
                "{}",
                case["case"]
            );
        }
    }

    #[test]
    fn fingerprint_ignores_identity_subscription_version_and_simple_flag() {
        let mut data = HeartbeatData::default();
        let subscription = SharedValue::from(SubscriptionData {
            topic: Some("topic".into()),
            sub_string: Some("*".into()),
            sub_version: 1,
            ..Default::default()
        });
        let consumer = SharedValue::from(ConsumerData {
            group_name: Some("consumer".into()),
            subscription_data_set: Some(SharedValue::from(HashSet::from([Some(
                subscription.clone(),
            )]))),
            ..Default::default()
        });
        data.consumer_data_set = Some(SharedValue::from(HashSet::from([Some(consumer.clone())])));
        let expected = fingerprint(&data).unwrap();
        data.client_id = Some("another".into());
        data.heartbeat_fingerprint = 42;
        data.is_without_sub = true;
        subscription.with_mut(|subscription| subscription.sub_version = 99);
        assert_eq!(fingerprint(&data).unwrap(), expected);
        subscription.with_mut(|subscription| subscription.sub_string = Some("changed".into()));
        assert_ne!(fingerprint(&data).unwrap(), expected);
    }

    #[test]
    fn fingerprint_preserves_duplicate_multiplicity_without_registration_order() {
        let a = SharedValue::from(ConsumerData {
            group_name: Some("a".into()),
            ..Default::default()
        });
        let b = SharedValue::from(ConsumerData {
            group_name: Some("b".into()),
            ..Default::default()
        });
        let consumers = SharedValue::from(HashSet::from([Some(a.clone()), Some(b.clone())]));
        let data = HeartbeatData {
            consumer_data_set: Some(consumers.clone()),
            ..Default::default()
        };
        let expected = fingerprint(&data).unwrap();
        consumers.with_mut(|consumers| {
            consumers.insert(Some(SharedValue::from(a.snapshot())));
        });
        assert_ne!(fingerprint(&data).unwrap(), expected);
        consumers.with_mut(|consumers| {
            consumers.insert(Some(SharedValue::from(b.snapshot())));
        });
        let duplicated = fingerprint(&data).unwrap();
        assert_eq!(fingerprint(&data).unwrap(), duplicated);
    }
}
