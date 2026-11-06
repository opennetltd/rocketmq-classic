use std::future::Future;
use std::sync::{Arc, RwLock};

use crate::{push::PushConsumer, rebalance::strategy::AllocateMessageQueueStrategy};
use rocketmq_classic_protocol::heartbeat::MessageModel;

/// Serializes assignment work for one Push consumer. Wakeups and periodic
/// timing remain owned by the shared client runtime.
pub(crate) struct RebalanceService {
    strategy: RwLock<Arc<dyn AllocateMessageQueueStrategy>>,
    message_model: RwLock<MessageModel>,
    execution: tokio::sync::Mutex<()>,
}

impl RebalanceService {
    pub(crate) fn new(
        strategy: Arc<dyn AllocateMessageQueueStrategy>,
        message_model: MessageModel,
    ) -> Self {
        Self {
            strategy: RwLock::new(strategy),
            message_model: RwLock::new(message_model),
            execution: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn set_strategy(&self, strategy: Arc<dyn AllocateMessageQueueStrategy>) {
        *self
            .strategy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = strategy;
    }

    pub(crate) fn set_message_model(&self, message_model: MessageModel) {
        *self
            .message_model
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = message_model;
    }

    pub(crate) fn message_model(&self) -> MessageModel {
        *self
            .message_model
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) async fn rebalance(&self, consumer: &Arc<PushConsumer>) -> bool {
        self.rebalance_with_order(consumer, None).await
    }

    pub(crate) async fn rebalance_with_order(
        &self,
        consumer: &Arc<PushConsumer>,
        is_order: Option<bool>,
    ) -> bool {
        let strategy = self
            .strategy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        self.execute_serialized(consumer.rebalance_once(strategy.as_ref(), is_order))
            .await
    }

    #[cfg(test)]
    pub(crate) fn strategy_name(&self) -> String {
        self.strategy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .name()
            .to_owned()
    }

    #[cfg(test)]
    pub(crate) fn message_model_for_test(&self) -> MessageModel {
        self.message_model()
    }

    async fn execute_serialized<T>(&self, work: impl Future<Output = T>) -> T {
        let _execution = self.execution.lock().await;
        work.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rebalance::AllocateMessageQueueAveragely;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn concurrent_rebalance_triggers_run_one_at_a_time() {
        let service = Arc::new(RebalanceService::new(
            Arc::new(AllocateMessageQueueAveragely),
            MessageModel::Clustering,
        ));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let run = |service: Arc<RebalanceService>| {
            let active = active.clone();
            let maximum = maximum.clone();
            tokio::spawn(async move {
                service
                    .execute_serialized(async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(current, Ordering::SeqCst);
                        sleep(Duration::from_millis(5)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await;
            })
        };

        let first = run(service.clone());
        let second = run(service);
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }
}
