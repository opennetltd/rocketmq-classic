use super::{ClientRuntime, RuntimeError};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::{Duration, Instant},
};

pub(super) const WAIT_PROPERTY: &str = "rocketmq.client.rebalance.waitInterval";
pub(super) const MIN_PROPERTY: &str = "rocketmq.client.rebalance.minInterval";

pub(super) struct RebalanceSignal {
    pub thread: super::service_thread::ServiceThreadControl,
    wake: Notify,
    wait: Duration,
    minimum: Duration,
    constructed: Instant,
    delayed: Mutex<(bool, BinaryHeap<Reverse<Instant>>)>,
    pub scheduled: Notify,
}
impl RebalanceSignal {
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, RuntimeError> {
        fn interval(
            properties: &HashMap<String, String>,
            key: &str,
            default: i64,
        ) -> Result<Duration, RuntimeError> {
            let value = match properties.get(key) {
                Some(value) => crate::java_number::parse_long(value).ok_or_else(|| {
                    rocketmq_classic_remoting::TransportError::InvalidConfiguration(format!(
                        "invalid Java long property {key}"
                    ))
                })?,
                None => default,
            };
            // ServiceThread treats a nonpositive wait as an immediate deadline.
            Ok(Duration::from_millis(value.max(0) as u64))
        }
        Ok(Self {
            thread: super::service_thread::ServiceThreadControl::default(),
            wake: Notify::new(),
            wait: interval(properties, WAIT_PROPERTY, 20_000)?,
            minimum: interval(properties, MIN_PROPERTY, 1_000)?,
            constructed: Instant::now(),
            delayed: Mutex::new((false, BinaryHeap::new())),
            scheduled: Notify::new(),
        })
    }
    pub fn wakeup(&self) {
        self.wake.notify_one();
    }
    pub fn schedule(&self, delay: Duration) -> Result<(), RuntimeError> {
        let mut delayed = self
            .delayed
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if delayed.0 {
            return Err(RuntimeError::InvalidState(super::RuntimeState::Stopped));
        }
        delayed.1.push(Reverse(Instant::now() + delay));
        self.scheduled.notify_one();
        Ok(())
    }
    pub fn next_due(&self) -> Option<Instant> {
        self.delayed.lock().ok()?.1.peek().map(|value| value.0)
    }
    pub fn fire_due(&self) {
        if let Ok(mut delayed) = self.delayed.lock() {
            if delayed.1.peek().is_some_and(|due| due.0 <= Instant::now()) {
                delayed.1.pop();
                self.wakeup();
            }
        }
    }
    pub fn close_delayed(&self) {
        if let Ok(mut delayed) = self.delayed.lock() {
            delayed.0 = true;
            delayed.1.clear();
        }
    }
}

/// A sticky/coalescing notification and one joined worker, not consumer tasks.
pub(super) struct OwnedRebalance {
    active: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl OwnedRebalance {
    pub fn new(runtime: Weak<ClientRuntime>, signal: Arc<RebalanceSignal>) -> Self {
        #[cfg(test)]
        let live = runtime.upgrade().map(|runtime| {
            let probe = runtime.rebalance_probe.clone();
            probe
                .created
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            probe.live.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            super::tasks::LiveTask(probe)
        });
        let (active, mut activation) = watch::channel(false);
        let (cancel, mut cancellation) = watch::channel(false);
        let task = tokio::spawn(async move {
            #[cfg(test)]
            let _live = live;
            tokio::select! { biased;
                _ = cancellation.changed() => return,
                _ = activation.changed() => {},
            }
            let mut last = signal.constructed;
            let mut wait = signal.wait;
            loop {
                if signal.thread.is_stopped() {
                    return;
                }
                tokio::select! { biased;
                    _ = cancellation.changed() => return,
                    _ = signal.wake.notified() => {},
                    _ = tokio::time::sleep(wait) => {},
                }
                let elapsed = last.elapsed();
                if elapsed < signal.minimum {
                    wait = signal.minimum - elapsed;
                    continue;
                }
                let Some(runtime) = runtime.upgrade() else {
                    return;
                };
                let balanced = tokio::select! { biased;
                    _ = cancellation.changed() => return,
                    result = runtime.do_rebalance() => result.unwrap_or(true),
                };
                wait = if balanced {
                    signal.wait
                } else {
                    signal.minimum
                };
                last = Instant::now();
                // Even Java's nonpositive intervals must not monopolize Tokio.
                tokio::task::yield_now().await;
            }
        });
        Self {
            active,
            cancel,
            task: Some(task),
        }
    }
    pub fn activate(&self) {
        self.active.send_replace(true);
    }
    pub async fn stop(&mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
    pub async fn join_with_timeout(&mut self, timeout: Duration) -> bool {
        let Some(task) = self.task.as_mut() else {
            return true;
        };
        if tokio::time::timeout(timeout, task).await.is_ok() {
            self.task.take();
            true
        } else {
            false
        }
    }
}
impl Drop for OwnedRebalance {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_protocol::heartbeat::ConsumerData;
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct Consumer(AtomicUsize);
    impl super::super::ConsumerRuntime for Consumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
        fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
            Some(Box::pin(async {
                self.0.fetch_add(1, Ordering::SeqCst);
                true
            }))
        }
    }
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn configured_intervals_and_prestart_sticky_wakeup_use_constructed_timestamp() {
        let mut config = crate::config::ClientConfig::new("custom-rebalance");
        config.namesrv_addr = Some(String::new());
        let properties = HashMap::from([
            (WAIT_PROPERTY.into(), "1234".into()),
            (MIN_PROPERTY.into(), "100".into()),
        ]);
        let runtime = Arc::new(ClientRuntime::construct(config, &properties, None).unwrap());
        let consumer = Arc::new(Consumer(AtomicUsize::new(0)));
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        runtime.rebalance_immediately(); // Permit is retained before worker creation.
        tokio::time::advance(Duration::from_millis(500)).await;
        let mut worker = OwnedRebalance::new(Arc::downgrade(&runtime), runtime.rebalance.clone());
        worker.activate();
        settle().await;
        assert_eq!(consumer.0.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(1233)).await;
        settle().await;
        assert_eq!(consumer.0.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        settle().await;
        assert_eq!(consumer.0.load(Ordering::SeqCst), 2);
        worker.stop().await;
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn rebalance_property_signed_longs_match_invoked_java() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["rebalanceLongs"].as_array().unwrap() {
            for key in [WAIT_PROPERTY, MIN_PROPERTY] {
                let properties =
                    HashMap::from([(key.into(), case["input"].as_str().unwrap().into())]);
                match RebalanceSignal::from_properties(&properties) {
                    Ok(signal) => {
                        assert_eq!(case["outcome"], "OK", "{case}");
                        let actual = if key == WAIT_PROPERTY {
                            signal.wait
                        } else {
                            signal.minimum
                        };
                        assert_eq!(
                            actual,
                            Duration::from_millis(case["value"].as_i64().unwrap().max(0) as u64),
                            "{case}"
                        );
                    }
                    Err(_) => assert_eq!(case["outcome"], "NumberFormatException", "{case}"),
                }
            }
        }
    }

    #[test]
    fn exact_java_rebalance_property_keys_defaults_and_numeric_boundaries() {
        let defaults = RebalanceSignal::from_properties(&HashMap::new()).unwrap();
        assert_eq!(defaults.wait, Duration::from_secs(20));
        assert_eq!(defaults.minimum, Duration::from_secs(1));
        let custom = RebalanceSignal::from_properties(&HashMap::from([
            (WAIT_PROPERTY.into(), "+1234".into()),
            (MIN_PROPERTY.into(), "-1".into()),
        ]))
        .unwrap();
        assert_eq!(custom.wait, Duration::from_millis(1234));
        assert_eq!(custom.minimum, Duration::ZERO);
        for key in [WAIT_PROPERTY, MIN_PROPERTY] {
            for invalid in ["", " 1", "1 ", "9223372036854775808", "NaN"] {
                assert!(
                    RebalanceSignal::from_properties(&HashMap::from([(
                        key.into(),
                        invalid.into()
                    )]))
                    .is_err()
                );
            }
        }
    }
}
