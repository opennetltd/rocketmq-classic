use std::{future::Future, pin::Pin};

/// An owned future submitted to a producer's configured asynchronous send executor.
pub type AsyncSenderTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Schedules producer async-send work, corresponding to Java's
/// `DefaultMQProducer.setAsyncSenderExecutor`.
///
/// Return `Err(task)` without polling or dropping the task when the executor
/// rejects submission. If async backpressure is enabled, the producer runs a
/// rejected task inline; otherwise it returns `ProducerError::AsyncSenderExecutorRejected`.
/// Tasks use Tokio I/O and must be polled inside an active Tokio runtime.
pub trait AsyncSenderExecutor: Send + Sync + 'static {
    fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask>;
}

pub(crate) struct TokioAsyncSenderExecutor;

impl AsyncSenderExecutor for TokioAsyncSenderExecutor {
    fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
        tokio::spawn(task);
        Ok(())
    }
}
