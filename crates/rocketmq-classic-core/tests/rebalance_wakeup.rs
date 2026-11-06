use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime},
};
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::time::Duration;

#[derive(Default)]
struct Consumer {
    calls: AtomicUsize,
    balanced: AtomicBool,
    panic: AtomicBool,
    block: AtomicBool,
    active: AtomicUsize,
    release: tokio::sync::Notify,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
    fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
        Some(Box::pin(async {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                !self.panic.swap(false, Ordering::SeqCst),
                "scripted rebalance failure"
            );
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = Active(&self.active);
            if self.block.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
            self.balanced.load(Ordering::SeqCst)
        }))
    }
}

#[tokio::test(start_paused = true)]
async fn stop_cancels_and_joins_pending_rebalance_capability_without_post_stop_ticks() {
    let mut config = ClientConfig::new("pending-rebalance");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let consumer = Arc::new(Consumer::default());
    consumer.block.store(true, Ordering::SeqCst);
    let lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    settle().await;
    advance(20_000).await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
    runtime
        .unregister_consumer(&lease, tokio::time::Instant::now())
        .await
        .unwrap();
    assert!(runtime.shutdown().await.unwrap());
    assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
    runtime.rebalance_immediately();
    advance(120_000).await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn delayed_wakeup_runs_on_owned_scheduler_and_nonpositive_delay_is_immediate() {
    let mut config = ClientConfig::new("delayed-rebalance");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let consumer = Arc::new(Consumer::default());
    consumer.balanced.store(true, Ordering::SeqCst);
    let lease = runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    settle().await;
    runtime.rebalance_later(5_000).unwrap();
    advance(4_999).await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 0);
    advance(1).await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 1);
    runtime.rebalance_later(-1).unwrap();
    settle().await;
    advance(1_000).await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 2);
    runtime.rebalance_later(100_000).unwrap();
    runtime
        .unregister_consumer(&lease, tokio::time::Instant::now())
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
    assert!(runtime.rebalance_later(1).is_err());
    advance(120_000).await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 2);
}
async fn settle() {
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
}
async fn advance(ms: u64) {
    tokio::time::advance(Duration::from_millis(ms)).await;
    settle().await;
}

#[tokio::test(start_paused = true)]
async fn coalesced_wakeup_respects_minimum_interval_and_visits_all_registered_consumers() {
    let mut config = ClientConfig::new("rebalance-wakeup");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let first = Arc::new(Consumer::default());
    let second = Arc::new(Consumer::default());
    first.balanced.store(true, Ordering::SeqCst);
    second.balanced.store(true, Ordering::SeqCst);
    let a = runtime.register_consumer("a", first.clone()).await.unwrap();
    let b = runtime
        .register_consumer("b", second.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    for _ in 0..100 {
        runtime.rebalance_immediately();
    }
    settle().await;
    advance(999).await;
    assert_eq!(first.calls.load(Ordering::SeqCst), 0);
    advance(1).await;
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    advance(19_999).await;
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    advance(1).await;
    assert_eq!(first.calls.load(Ordering::SeqCst), 2);
    second.balanced.store(false, Ordering::SeqCst);
    runtime.rebalance_immediately();
    advance(1_000).await;
    assert_eq!(second.calls.load(Ordering::SeqCst), 3);
    advance(1_000).await;
    assert_eq!(second.calls.load(Ordering::SeqCst), 4);
    first.panic.store(true, Ordering::SeqCst);
    assert!(!runtime.do_rebalance().await.unwrap());
    assert_eq!(second.calls.load(Ordering::SeqCst), 5);
    runtime
        .unregister_consumer(&a, tokio::time::Instant::now())
        .await
        .unwrap();
    runtime
        .unregister_consumer(&b, tokio::time::Instant::now())
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
    advance(120_000).await;
    assert_eq!(second.calls.load(Ordering::SeqCst), 5);
}
