use std::{
    future::Future,
    sync::{Arc, Weak},
};

use rocketmq_classic_protocol::codes::response;
use tokio::sync::mpsc;

use crate::{command::RemotingCommand, error::TransportError};

use super::{ChannelHandle, ClientInner, ProcessorRegistration};

/// Java 5.5.1's `MQVersion.Version.V5_3_1.ordinal()` shutdown cutoff.
const SHUTDOWN_GO_AWAY_VERSION: i32 = 475;

/// Request-scoped access to the channel that delivered an inbound command.
///
/// The channel handle preserves connection identity and supports address inspection, closing,
/// and outbound command writes. `write_and_flush` enqueues immediately and returns a future for
/// the local write result; it has no operation deadline and does not imply remote receipt.
#[derive(Clone, Debug)]
pub struct RequestContext {
    channel: ChannelHandle,
}

impl RequestContext {
    pub(super) fn new(channel: ChannelHandle) -> Self {
        Self { channel }
    }

    /// Returns the identity-preserving channel handle for the inbound connection.
    pub fn channel(&self) -> &ChannelHandle {
        &self.channel
    }

    /// Returns the peer address recorded for this channel, with URI credentials redacted.
    pub fn remote_addr(&self) -> &str {
        self.channel.remote_addr()
    }

    /// Closes this channel and lets the connection owner complete pending requests.
    pub fn close(&self) -> bool {
        self.channel.close()
    }

    /// Enqueues a command on this channel without an operation deadline.
    ///
    /// The returned future reports the local socket write result. Dropping it does not cancel the
    /// already-enqueued write, matching the fire-and-forget use of Java's `writeAndFlush` future.
    pub fn write_and_flush(
        &self,
        command: &RemotingCommand,
    ) -> Result<impl Future<Output = Result<(), TransportError>> + Send + 'static, TransportError>
    {
        self.channel.write_and_flush_without_deadline(command)
    }
}

pub trait RequestProcessor: Send + Sync + 'static {
    fn process(&self, request: RemotingCommand) -> Result<Option<RemotingCommand>, String>;

    /// Channel identity needed by transaction checks and other broker callbacks.
    fn process_with_context(
        &self,
        _remote_addr: &str,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        self.process(request)
    }

    /// Processes a command with its inbound channel context.
    ///
    /// The default preserves implementations of `process_with_context` by forwarding the peer
    /// address. Implementations that need channel operations can override this method.
    fn process_with_request_context(
        &self,
        context: &RequestContext,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        self.process_with_context(context.remote_addr(), request)
    }

    fn reject_request(&self) -> bool {
        false
    }
}

impl<F> RequestProcessor for F
where
    F: Fn(RemotingCommand) -> Result<Option<RemotingCommand>, String> + Send + Sync + 'static,
{
    fn process(&self, request: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        self(request)
    }
}

#[derive(Clone)]
pub(super) struct InboundRequest {
    pub generation: u64,
    pub remote_addr: String,
    pub channel: ChannelHandle,
    pub command: RemotingCommand,
}

pub(super) fn start(
    inner: Weak<ClientInner>,
    mut receiver: mpsc::UnboundedReceiver<InboundRequest>,
    worker_count: usize,
) -> Vec<tokio::task::JoinHandle<()>> {
    let Some(client) = inner.upgrade() else {
        return Vec::new();
    };
    let shutdown_sender = client.shutdown.clone();
    let (work, worker_rx) = mpsc::unbounded_channel();
    let worker_rx = Arc::new(tokio::sync::Mutex::new(worker_rx));
    let mut tasks = Vec::with_capacity(worker_count + 1);
    let mut shutdown = shutdown_sender.subscribe();
    tasks.push(tokio::spawn(async move {
        loop {
            tokio::select! {
                request = receiver.recv() => match request {
                    Some(request) => {
                        let _ = work.send(request);
                    }
                    None => return,
                },
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        receiver.close();
                        while let Some(request) = receiver.recv().await {
                            if work.send(request).is_err() {
                                return;
                            }
                        }
                        return;
                    }
                },
            }
        }
    }));
    for _ in 0..worker_count {
        let inner = inner.clone();
        let receiver = worker_rx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let request = receiver.lock().await.recv().await;
                match request {
                    Some(request) => process_one(&inner, request),
                    None => return,
                }
            }
        }));
    }
    tasks
}

pub(super) fn process_one(inner: &Weak<ClientInner>, request: InboundRequest) {
    let Some(client) = inner.upgrade() else {
        return;
    };
    let registration = client
        .processors
        .lock()
        .unwrap()
        .get(&request.command.code)
        .cloned()
        .or_else(|| client.default_processor.lock().unwrap().clone());
    let Some(registration) = registration else {
        let remark = format!(" request type {} not supported", request.command.code);
        send_response(
            inner,
            request,
            error_response(response::REQUEST_CODE_NOT_SUPPORTED, &remark),
        );
        return;
    };
    if let Some(response) =
        shutdown_go_away_response(*client.shutdown.borrow(), request.command.version)
    {
        send_response(inner, request, response);
        return;
    }
    if registration.processor.reject_request() {
        send_response(
            inner,
            request,
            error_response(
                response::SYSTEM_BUSY,
                "[REJECTREQUEST]system busy, start flow control for a while",
            ),
        );
        return;
    }

    let executor = registration
        .executor
        .clone()
        .unwrap_or_else(|| client.public_executor.clone());
    let request_for_rejection = request.clone();
    let task_inner = inner.clone();
    let task: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
        process_registered(&task_inner, request, registration);
    });
    {
        let task = Arc::new(std::sync::Mutex::new(Some(task)));
        let task_for_executor = task.clone();
        let dispatched: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
            if let Some(task) = task_for_executor.lock().unwrap().take() {
                task();
            }
        });
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.execute(dispatched)
        })) {
            Ok(Ok(())) => {}
            Ok(Err(_)) if task.lock().unwrap().take().is_some() => {
                send_response(
                    inner,
                    request_for_rejection,
                    error_response(
                        response::SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control for a while",
                    ),
                );
            }
            Ok(Err(_)) => {}
            Err(_) => {
                if let Some(manager) = client.metrics_manager.read().unwrap().clone() {
                    crate::metrics::record_process_request_failed(
                        &manager,
                        &request_for_rejection.command,
                    );
                }
            }
        }
    }
}

fn process_registered(
    inner: &Weak<ClientInner>,
    mut request: InboundRequest,
    registration: ProcessorRegistration,
) {
    let request_context = RequestContext::new(request.channel.clone());
    let mut response = None;
    let mut run_after_hooks = false;
    let mut request_for_hooks = request.command.clone();
    let mut before_failure = None;
    if let Some(client) = inner.upgrade() {
        let hooks = client.rpc_hooks.lock().unwrap().clone();
        let before_result = hooks.iter().try_for_each(|hook| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.before_request_mut_result(&request.remote_addr, &mut request_for_hooks)
            }))
            .unwrap_or_else(|payload| Err(super::RpcHookError::Failed(panic_message(payload))))
        });
        before_failure = match before_result {
            Err(super::RpcHookError::Abort {
                response_code,
                remark,
            }) => {
                response = Some(error_response(
                    response_code,
                    remark.as_deref().unwrap_or("RPC hook aborted request"),
                ));
                None
            }
            Err(super::RpcHookError::Failed(message)) => Some(message),
            Ok(()) => None,
        };
        if response.is_none() {
            let pipeline = client.request_pipeline.lock().unwrap().clone();
            let pipeline_result = if let Some(pipeline) = pipeline {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    pipeline
                        .execute_detailed_with_context_mut(&request_context, &mut request_for_hooks)
                }))
                .unwrap_or_else(|payload| {
                    Err(super::RequestPipelineError::Failed(panic_message(payload)))
                })
            } else {
                Ok(())
            };
            match pipeline_result {
                Err(super::RequestPipelineError::Abort {
                    response_code,
                    remark,
                }) => {
                    before_failure = None;
                    response = Some(error_response(
                        response_code,
                        remark.as_deref().unwrap_or("request pipeline aborted"),
                    ));
                }
                Err(super::RequestPipelineError::Failed(message)) => {
                    before_failure = None;
                    response = Some(error_response(response::SYSTEM_ERROR, &message));
                }
                Ok(()) if before_failure.is_some() => {
                    response = Some(
                        RemotingCommand::create_response_command(response::SYSTEM_ERROR, None)
                            .expect("remoting version configuration is valid"),
                    );
                    run_after_hooks = true;
                }
                Ok(()) => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        registration.processor.process_with_request_context(
                            &request_context,
                            request_for_hooks.clone(),
                        )
                    }));
                    match result {
                        Ok(Ok(processed_response)) => {
                            response = processed_response;
                            run_after_hooks = true;
                        }
                        Ok(Err(message)) => {
                            response = Some(error_response(response::SYSTEM_ERROR, &message));
                        }
                        Err(payload) => {
                            response = Some(error_response(
                                response::SYSTEM_ERROR,
                                &panic_message(payload),
                            ));
                        }
                    }
                }
            }
        }
    }
    if run_after_hooks {
        if let Some(client) = inner.upgrade() {
            for hook in client.rpc_hooks.lock().unwrap().clone() {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.after_response_optional_mut_result(
                        &request.remote_addr,
                        &mut request_for_hooks,
                        response.as_mut(),
                    )
                }))
                .unwrap_or_else(|payload| Err(super::RpcHookError::Failed(panic_message(payload))));
                match result {
                    Ok(()) => {}
                    Err(super::RpcHookError::Abort {
                        response_code,
                        remark,
                    }) => {
                        before_failure = None;
                        response = Some(error_response(
                            response_code,
                            remark.as_deref().unwrap_or("RPC hook aborted response"),
                        ));
                        break;
                    }
                    Err(super::RpcHookError::Failed(message)) => {
                        before_failure = None;
                        response = Some(error_response(response::SYSTEM_ERROR, &message));
                        break;
                    }
                }
            }
        }
        if let Some(message) = before_failure {
            response = Some(error_response(response::SYSTEM_ERROR, &message));
        }
    }
    request.command = request_for_hooks;
    if let Some(response) = response {
        send_response(inner, request, response);
    }
}

fn send_response(
    inner: &Weak<ClientInner>,
    request: InboundRequest,
    mut response: RemotingCommand,
) {
    let Some(client) = inner.upgrade() else {
        return;
    };
    let metrics_manager = client.metrics_manager.read().unwrap().clone();
    let attributes = metrics_manager.as_ref().map(|manager| {
        crate::metrics::response_attributes_builder(manager, &request.command, response.code)
    });
    let process_timer = request.command.get_process_timer().cloned();
    if request.command.is_oneway() {
        record_response_metric(
            metrics_manager.as_ref(),
            process_timer.as_ref(),
            attributes,
            crate::metrics::RESULT_ONEWAY,
        );
        return;
    }
    response.opaque = request.command.opaque;
    response.mark_response();
    let connection = client
        .connections
        .lock()
        .unwrap()
        .values()
        .find(|connection| connection.generation == request.generation)
        .cloned();
    let Some(connection) = connection else {
        record_response_metric(
            metrics_manager.as_ref(),
            process_timer.as_ref(),
            attributes,
            crate::metrics::RESULT_WRITE_CHANNEL_FAILED,
        );
        return;
    };
    match connection.encode_command(&response, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE) {
        Ok(encoded) => {
            let Some(runtime) = client.runtime.lock().unwrap().clone() else {
                record_response_metric(
                    metrics_manager.as_ref(),
                    process_timer.as_ref(),
                    attributes,
                    crate::metrics::RESULT_WRITE_CHANNEL_FAILED,
                );
                return;
            };
            let write_metric =
                match (metrics_manager, attributes) {
                    (Some(manager), Some(attributes)) => Some(
                        crate::metrics::PendingWriteMetric::new(manager, process_timer, attributes),
                    ),
                    _ => None,
                };
            let task = runtime.spawn(async move {
                let result = connection.send_without_deadline(encoded).await;
                let status = if result.is_ok() {
                    crate::metrics::WriteAndFlushStatus::Success
                } else {
                    crate::metrics::WriteAndFlushStatus::Failed
                };
                if let Some(write_metric) = write_metric {
                    write_metric.finish(status);
                }
            });
            client.spawn(task);
        }
        Err(error) => {
            record_response_metric(
                metrics_manager.as_ref(),
                process_timer.as_ref(),
                attributes,
                crate::metrics::RESULT_WRITE_CHANNEL_FAILED,
            );
            client.connection_closed(request.generation, Some(error));
        }
    }
}

fn record_response_metric(
    manager: Option<&Arc<crate::metrics::RemotingMetricsManager>>,
    timer: Option<&crate::command::ProcessTimer>,
    attributes: Option<crate::metrics::AttributesBuilder>,
    result: &'static str,
) {
    if let (Some(manager), Some(attributes)) = (manager, attributes) {
        crate::metrics::record_request_latency(manager, timer, attributes, result);
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).into()
    } else {
        "request processing panicked".into()
    }
}

fn error_response(code: i32, remark: &str) -> RemotingCommand {
    RemotingCommand::create_response_command(code, Some(remark.into()))
        .expect("remoting version configuration is valid")
}

fn shutdown_go_away_response(
    is_shutting_down: bool,
    request_version: i32,
) -> Option<RemotingCommand> {
    (is_shutting_down && request_version > SHUTDOWN_GO_AWAY_VERSION)
        .then(|| error_response(response::GO_AWAY, "please go away"))
}

#[cfg(test)]
mod shutdown_compatibility_tests {
    use super::*;

    #[test]
    fn java_551_shutdown_version_boundary_returns_go_away_only_above_v531() {
        assert!(shutdown_go_away_response(false, 476).is_none());
        assert!(shutdown_go_away_response(true, 475).is_none());

        let response = shutdown_go_away_response(true, 476).unwrap();
        assert_eq!(SHUTDOWN_GO_AWAY_VERSION, 475);
        assert_eq!(response.code, response::GO_AWAY);
        assert_eq!(response.remark.as_deref(), Some("please go away"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LegacyProcessor;

    impl RequestProcessor for LegacyProcessor {
        fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
            panic!("the legacy context method should handle this call")
        }

        fn process_with_context(
            &self,
            remote_addr: &str,
            _: RemotingCommand,
        ) -> Result<Option<RemotingCommand>, String> {
            Ok(Some(RemotingCommand {
                remark: Some(remote_addr.to_owned()),
                ..RemotingCommand::default()
            }))
        }
    }

    #[test]
    fn request_context_adapts_existing_remote_address_processors() {
        let context = RequestContext::new(ChannelHandle::pending("127.0.0.1:10911"));
        let response = LegacyProcessor
            .process_with_request_context(&context, RemotingCommand::default())
            .unwrap()
            .unwrap();

        assert_eq!(response.remark.as_deref(), Some("127.0.0.1:10911"));
    }

    #[test]
    fn request_context_exposes_channel_state_and_closes_its_channel() {
        let channel = ChannelHandle::pending("127.0.0.1:10911");
        let context = RequestContext::new(channel.clone());

        assert_eq!(context.channel(), &channel);
        assert_eq!(context.remote_addr(), "127.0.0.1:10911");
        assert!(channel.is_open());
        assert!(!channel.is_active());
        assert!(!channel.is_writable());

        assert!(context.close());
        assert!(!context.channel().is_open());
        assert!(!context.close());
    }
}
