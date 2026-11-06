use std::{future::Future, pin::Pin, sync::Arc};

use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_protocol::body::ConsumeMessageDirectlyResult;

use crate::{pop::PopProcessQueue, process_queue::ProcessQueue};

/// Boxed asynchronous result used by the consumer service compatibility API.
pub type ConsumeMessageServiceFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Rust extension point corresponding to Java's `ConsumeMessageService`.
///
/// A service can be installed on a [`PushConsumer`](crate::push::PushConsumer)
/// through its implementation accessors. Queue submission transfers message
/// snapshots and the live queue owner to the service; the service is then
/// responsible for completing or retrying the submitted work. Replacing the
/// primary service affects normal Pull and direct-consume work; Push POP
/// dispatch remains on the separate internal POP service.
pub trait ConsumeMessageService: Send + Sync {
    fn start(&self) -> ConsumeMessageServiceFuture<'_, ()>;

    fn shutdown(&self, await_termination_millis: i64) -> ConsumeMessageServiceFuture<'_, ()>;

    fn update_core_pool_size(&self, core_pool_size: i32);

    fn inc_core_pool_size(&self);

    fn dec_core_pool_size(&self);

    fn core_pool_size(&self) -> i32;

    fn consume_message_directly(
        &self,
        message: MessageExt,
        broker_name: Option<String>,
    ) -> ConsumeMessageServiceFuture<'_, ConsumeMessageDirectlyResult>;

    fn submit_consume_request(
        &self,
        messages: Vec<Arc<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    );

    fn submit_pop_consume_request(
        &self,
        messages: Vec<MessageExt>,
        process_queue: Arc<PopProcessQueue>,
        message_queue: MessageQueue,
    );
}
