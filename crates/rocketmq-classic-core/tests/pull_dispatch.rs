use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, MessageRequest},
};
use rocketmq_classic_protocol::{body::MessageRequestMode, heartbeat::ConsumerData};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

struct Request {
    group: &'static str,
    mode: MessageRequestMode,
    id: usize,
}
impl MessageRequest for Request {
    fn consumer_group(&self) -> Option<&str> {
        Some(self.group)
    }
    fn mode(&self) -> MessageRequestMode {
        self.mode
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
struct Consumer(Mutex<Vec<(MessageRequestMode, usize)>>);
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
    fn pull_message(
        &self,
        request: Arc<dyn MessageRequest>,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
        Some(Box::pin(async move {
            let request = request.as_any().downcast_ref::<Request>().unwrap();
            self.0
                .lock()
                .unwrap()
                .push((MessageRequestMode::Pull, request.id));
        }))
    }
    fn pop_message(
        &self,
        request: Arc<dyn MessageRequest>,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
        Some(Box::pin(async move {
            let request = request.as_any().downcast_ref::<Request>().unwrap();
            self.0
                .lock()
                .unwrap()
                .push((MessageRequestMode::Pop, request.id));
        }))
    }
}
async fn settle() {
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
}
#[tokio::test(start_paused = true)]
async fn fifo_mode_dispatch_delay_and_group_selection_use_current_registration() {
    let mut config = ClientConfig::new("pull-dispatch");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let consumer = Arc::new(Consumer(Mutex::new(Vec::new())));
    let lease = runtime
        .register_consumer("group", consumer.clone())
        .await
        .unwrap();
    let request =
        |group, mode, id| Arc::new(Request { group, mode, id }) as Arc<dyn MessageRequest>;
    runtime
        .execute_pull_request_immediately(request("group", MessageRequestMode::Pull, 1))
        .unwrap();
    runtime
        .execute_pop_pull_request_immediately(request("group", MessageRequestMode::Pop, 2))
        .unwrap();
    runtime
        .execute_pull_request_immediately(request("missing", MessageRequestMode::Pull, 3))
        .unwrap();
    runtime
        .execute_pull_request_later(request("group", MessageRequestMode::Pull, 4), 5_000)
        .unwrap();
    runtime.start().await.unwrap();
    settle().await;
    assert_eq!(
        *consumer.0.lock().unwrap(),
        [(MessageRequestMode::Pull, 1), (MessageRequestMode::Pop, 2)]
    );
    tokio::time::advance(tokio::time::Duration::from_millis(4_999)).await;
    settle().await;
    assert_eq!(consumer.0.lock().unwrap().len(), 2);
    tokio::time::advance(tokio::time::Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(consumer.0.lock().unwrap()[2], (MessageRequestMode::Pull, 4));
    runtime
        .execute_pop_pull_request_later(request("group", MessageRequestMode::Pop, 5), 5_000)
        .unwrap();
    runtime
        .unregister_consumer(&lease, tokio::time::Instant::now())
        .await
        .unwrap();
    let replacement = Arc::new(Consumer(Mutex::new(Vec::new())));
    let new_lease = runtime
        .register_consumer("group", replacement.clone())
        .await
        .unwrap();
    tokio::time::advance(tokio::time::Duration::from_millis(5_000)).await;
    settle().await;
    assert_eq!(
        *replacement.0.lock().unwrap(),
        [(MessageRequestMode::Pop, 5)]
    );
    runtime
        .unregister_consumer(&new_lease, tokio::time::Instant::now())
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
    assert!(
        runtime
            .execute_pull_request_immediately(request("group", MessageRequestMode::Pull, 6))
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn scheduled_tasks_serialize_but_fifo_dispatch_is_independent_and_panic_isolated() {
    let mut config = ClientConfig::new("pull-task-serialization");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let consumer = Arc::new(Consumer(Mutex::new(Vec::new())));
    let lease = runtime
        .register_consumer("group", consumer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Notify::new());
    let first_events = events.clone();
    let first_release = release.clone();
    runtime
        .execute_task(async move {
            first_events.lock().unwrap().push(1);
            first_release.notified().await;
            first_events.lock().unwrap().push(2);
        })
        .unwrap();
    settle().await;
    let later_events = events.clone();
    runtime
        .execute_task_later(
            async move {
                later_events.lock().unwrap().push(3);
            },
            -1,
        )
        .unwrap();
    runtime
        .execute_pull_request_immediately(Arc::new(Request {
            group: "group",
            mode: MessageRequestMode::Pull,
            id: 10,
        }))
        .unwrap();
    runtime
        .execute_pop_pull_request_later(
            Arc::new(Request {
                group: "group",
                mode: MessageRequestMode::Pop,
                id: 11,
            }),
            1_000,
        )
        .unwrap();
    settle().await;
    tokio::time::advance(tokio::time::Duration::from_millis(1_000)).await;
    settle().await;
    assert_eq!(*events.lock().unwrap(), [1]);
    assert_eq!(
        *consumer.0.lock().unwrap(),
        [(MessageRequestMode::Pull, 10)]
    );
    release.notify_one();
    settle().await;
    assert_eq!(*events.lock().unwrap(), [1, 2, 3]);
    assert_eq!(consumer.0.lock().unwrap()[1], (MessageRequestMode::Pop, 11));
    runtime
        .execute_task(async {
            panic!("scripted scheduled failure");
        })
        .unwrap();
    let final_events = events.clone();
    runtime
        .execute_task(async move {
            final_events.lock().unwrap().push(4);
        })
        .unwrap();
    settle().await;
    assert_eq!(*events.lock().unwrap(), [1, 2, 3, 4]);
    runtime
        .unregister_consumer(&lease, tokio::time::Instant::now())
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
}

struct Active(Arc<std::sync::atomic::AtomicUsize>);
impl Active {
    fn new(count: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(count)
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
struct BlockingConsumer {
    active: Arc<std::sync::atomic::AtomicUsize>,
    calls: std::sync::atomic::AtomicUsize,
    runtime: std::sync::Weak<rocketmq_classic_core::runtime::ClientRuntime>,
}
impl ConsumerRuntime for BlockingConsumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
    fn pull_message(
        &self,
        request: Arc<dyn MessageRequest>,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
        Some(Box::pin(async move {
            let runtime = self.runtime.upgrade().unwrap();
            assert!(
                runtime
                    .select_consumer_nullable(request.consumer_group())
                    .unwrap()
                    .is_some()
            );
            runtime.route_snapshot().unwrap();
            let id = request.as_any().downcast_ref::<Request>().unwrap().id;
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_ne!(id, 0, "scripted consumer failure");
            let _active = Active::new(self.active.clone());
            std::future::pending::<()>().await;
        }))
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_both_inflight_workers_and_drops_pending_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut config = ClientConfig::new("pull-cancellation");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let consumer = Arc::new(BlockingConsumer {
        active: active.clone(),
        calls: AtomicUsize::new(0),
        runtime: Arc::downgrade(&runtime),
    });
    let lease = runtime
        .register_consumer("group", consumer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    for id in [0, 1, 2] {
        runtime
            .execute_pull_request_immediately(Arc::new(Request {
                group: "group",
                mode: MessageRequestMode::Pull,
                id,
            }))
            .unwrap();
    }
    let task_active = active.clone();
    runtime
        .execute_task(async move {
            let _active = Active::new(task_active);
            std::future::pending::<()>().await;
        })
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let queued = Active::new(dropped.clone());
    runtime
        .execute_task_later(
            async move {
                let _queued = queued;
            },
            100_000,
        )
        .unwrap();
    settle().await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 2); // Panic does not prevent next request.
    assert_eq!(active.load(Ordering::SeqCst), 2);
    runtime
        .unregister_consumer(&lease, tokio::time::Instant::now())
        .await
        .unwrap();
    assert!(runtime.shutdown().await.unwrap());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(runtime.execute_task(async {}).is_ok()); // Stopped PullMessageService ignores the task.
    tokio::time::advance(tokio::time::Duration::from_millis(120_000)).await;
    settle().await;
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn normal_shutdown_preserves_java_one_second_scheduled_executor_grace() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut config = ClientConfig::new("pull-graceful-stop");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.start().await.unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    let fast = completed.clone();
    runtime
        .execute_task_later(
            async move {
                fast.fetch_add(1, Ordering::SeqCst);
            },
            500,
        )
        .unwrap();
    let slow = completed.clone();
    runtime
        .execute_task_later(
            async move {
                slow.fetch_add(100, Ordering::SeqCst);
            },
            100_000,
        )
        .unwrap();
    let start = tokio::time::Instant::now();
    assert!(runtime.shutdown().await.unwrap());
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert!(start.elapsed() >= tokio::time::Duration::from_secs(1));
    assert!(start.elapsed() < tokio::time::Duration::from_secs(2));
    tokio::time::advance(tokio::time::Duration::from_secs(120)).await;
    settle().await;
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scheduled_executor_can_run_before_transport_start_without_starting_request_dispatch() {
    let mut config = ClientConfig::new("pull-prestart-executor");
    config.set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let (sent, mut received) = tokio::sync::oneshot::channel();
    runtime
        .execute_task(async move {
            sent.send(()).unwrap();
        })
        .unwrap();
    tokio::time::timeout(tokio::time::Duration::from_secs(1), &mut received)
        .await
        .expect("pre-start scheduled executor should run the task")
        .unwrap();
    assert_eq!(
        runtime.state(),
        rocketmq_classic_core::runtime::RuntimeState::Created
    );
    runtime.start().await.unwrap();
    runtime.shutdown().await.unwrap();
}
