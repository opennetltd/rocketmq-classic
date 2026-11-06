mod concurrent;
mod hooks;
mod orderly;
mod service;
mod trace;

pub(crate) use concurrent::{ConcurrentConsumeService, ConsumeRequest, ConsumeSubmitError};
pub(crate) use hooks::ConsumeMessageHookRegistry;
pub use hooks::{ConsumeMessageContext, ConsumeMessageHook};
pub(crate) use orderly::{
    ORDERLY_LOCK_RETRY_DELAY_MILLIS, ORDERLY_LOCK_RPC_TIMEOUT_MILLIS, OrderlyConsumeRequest,
    OrderlyConsumeService, OrderlyDisposition, OrderlyLockSettings, OrderlySubmitError,
    orderly_disposition, suspend_delay_millis,
};
pub use service::{ConsumeMessageService, ConsumeMessageServiceFuture};
pub(crate) use trace::ConsumeTraceHook;

use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum ConsumeService {
    Concurrent(Arc<ConcurrentConsumeService>),
    Orderly(Arc<OrderlyConsumeService>),
}

impl ConsumeService {
    pub(crate) fn listener(&self) -> crate::push::PushMessageListener {
        match self {
            Self::Concurrent(service) => {
                crate::push::PushMessageListener::Concurrently(service.listener())
            }
            Self::Orderly(service) => crate::push::PushMessageListener::Orderly(service.listener()),
        }
    }

    pub(crate) async fn start(
        &self,
        consumer: std::sync::Weak<crate::push::PushConsumer>,
        orderly_lock_settings: Option<OrderlyLockSettings>,
        renew_orderly_locks: bool,
    ) {
        match self {
            Self::Concurrent(service) => service.start(consumer).await,
            Self::Orderly(service) => {
                service
                    .start(
                        consumer,
                        orderly_lock_settings.expect("orderly settings"),
                        renew_orderly_locks,
                    )
                    .await;
            }
        }
    }

    pub(crate) async fn shutdown(&self, await_termination_millis: i64) {
        match self {
            Self::Concurrent(service) => service.shutdown(await_termination_millis).await,
            Self::Orderly(service) => service.shutdown(await_termination_millis).await,
        }
    }

    pub(crate) fn update_core_pool_size(&self, core_pool_size: i32) -> bool {
        match self {
            Self::Concurrent(service) => service.update_core_pool_size(core_pool_size),
            Self::Orderly(service) => service.update_core_pool_size(core_pool_size),
        }
    }

    pub(crate) fn maximum_threads(&self) -> i32 {
        match self {
            Self::Concurrent(service) => service.maximum_threads(),
            Self::Orderly(service) => service.maximum_threads(),
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        match self {
            Self::Concurrent(service) => service.worker_count(),
            Self::Orderly(service) => service.worker_count(),
        }
    }
}
