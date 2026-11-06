use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};

use rocketmq_classic_common::message::{MessageExt, MessageQueue};

/// Java `MessageQueueListener` callback. `None` for `mq_all` represents
/// broker-driven assignment, where Java passes a null set.
pub type MessageQueueListener = Arc<
    dyn Fn(&str, Option<&HashSet<MessageQueue>>, &HashSet<MessageQueue>) + Send + Sync + 'static,
>;

pub type ConcurrentListenerFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    Option<ConsumeConcurrentlyStatus>,
                    ConsumeConcurrentlyContext,
                ),
            > + Send
            + 'static,
    >,
>;

pub type OrderlyListenerFuture = Pin<
    Box<
        dyn Future<Output = (Option<ConsumeOrderlyStatus>, ConsumeOrderlyContext)> + Send + 'static,
    >,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeConcurrentlyStatus {
    ConsumeSuccess,
    ReconsumeLater,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeOrderlyStatus {
    Success,
    /// Deprecated by Java 5.5.1; accepted for source compatibility.
    Rollback,
    /// Deprecated by Java 5.5.1; accepted for source compatibility.
    Commit,
    SuspendCurrentQueueAMoment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeConcurrentlyContext {
    message_queue: MessageQueue,
    ack_index: i32,
    delay_level_when_next_consume: i32,
}

impl ConsumeConcurrentlyContext {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue,
            ack_index: i32::MAX,
            delay_level_when_next_consume: 0,
        }
    }

    pub fn message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub const fn ack_index(&self) -> i32 {
        self.ack_index
    }

    pub fn set_ack_index(&mut self, value: i32) {
        self.ack_index = value;
    }

    pub const fn delay_level_when_next_consume(&self) -> i32 {
        self.delay_level_when_next_consume
    }

    pub fn set_delay_level_when_next_consume(&mut self, value: i32) {
        self.delay_level_when_next_consume = value;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeOrderlyContext {
    message_queue: MessageQueue,
    auto_commit: bool,
    suspend_current_queue_time_millis: i64,
}

impl ConsumeOrderlyContext {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue,
            auto_commit: true,
            suspend_current_queue_time_millis: -1,
        }
    }

    pub fn message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub const fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn set_auto_commit(&mut self, value: bool) {
        self.auto_commit = value;
    }

    pub const fn suspend_current_queue_time_millis(&self) -> i64 {
        self.suspend_current_queue_time_millis
    }

    pub fn set_suspend_current_queue_time_millis(&mut self, value: i64) {
        self.suspend_current_queue_time_millis = value;
    }
}

pub trait MessageListenerConcurrently: Send + Sync + 'static {
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
        context: ConsumeConcurrentlyContext,
    ) -> ConcurrentListenerFuture;
}

pub trait MessageListenerOrderly: Send + Sync + 'static {
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
        context: ConsumeOrderlyContext,
    ) -> OrderlyListenerFuture;
}

#[derive(Clone)]
pub enum PushMessageListener {
    Concurrently(std::sync::Arc<dyn MessageListenerConcurrently>),
    Orderly(std::sync::Arc<dyn MessageListenerOrderly>),
}

impl PushMessageListener {
    pub fn concurrently(listener: impl MessageListenerConcurrently) -> Self {
        Self::Concurrently(std::sync::Arc::new(listener))
    }

    pub fn orderly(listener: impl MessageListenerOrderly) -> Self {
        Self::Orderly(std::sync::Arc::new(listener))
    }
}

impl<F, Fut> MessageListenerConcurrently for F
where
    F: Fn(Vec<MessageExt>, ConsumeConcurrentlyContext) -> Fut + Send + Sync + 'static,
    Fut: Future<
            Output = (
                Option<ConsumeConcurrentlyStatus>,
                ConsumeConcurrentlyContext,
            ),
        > + Send
        + 'static,
{
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
        context: ConsumeConcurrentlyContext,
    ) -> ConcurrentListenerFuture {
        Box::pin(self(messages, context))
    }
}

impl<F, Fut> MessageListenerOrderly for F
where
    F: Fn(Vec<MessageExt>, ConsumeOrderlyContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (Option<ConsumeOrderlyStatus>, ConsumeOrderlyContext)> + Send + 'static,
{
    fn consume_message(
        &self,
        messages: Vec<MessageExt>,
        context: ConsumeOrderlyContext,
    ) -> OrderlyListenerFuture {
        Box::pin(self(messages, context))
    }
}
