use super::*;

impl RemotingClient {
    /// Selects the next configured NameServer. A successful invoke marks it available;
    /// connection failures remove it from the available set.
    pub async fn invoke_name_server(
        &self,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        self.invoke_name_server_with_peer(command, deadline)
            .await
            .map(|(response, _)| response)
    }

    /// Invokes a NameServer and returns the address that supplied the response.
    pub(super) async fn invoke_name_server_with_peer(
        &self,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<(RemotingCommand, String), TransportError> {
        self.invoke_name_server_mode(command, deadline, false).await
    }

    /// Java invokeSync(NameServer): retain the original absolute wait deadline.
    pub async fn invoke_name_server_sync_with_peer(
        &self,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<(RemotingCommand, String), TransportError> {
        self.invoke_name_server_mode(command, deadline, true).await
    }

    async fn invoke_name_server_mode(
        &self,
        command: RemotingCommand,
        deadline: Instant,
        sync: bool,
    ) -> Result<(RemotingCommand, String), TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        let addresses = self.get_name_server_address_list().unwrap_or_default();
        if addresses.is_empty() {
            return Err(TransportError::InvalidConfiguration(
                "no NameServer addresses configured".into(),
            ));
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        let chosen = self.inner.chosen_nameserver.lock().unwrap().clone();
        let start = chosen
            .as_ref()
            .and_then(|address| addresses.iter().position(|candidate| candidate == address))
            .unwrap_or_else(|| {
                self.inner
                    .nameserver_index
                    .fetch_add(1, Ordering::Relaxed)
                    .wrapping_add(1) as usize
                    % addresses.len()
            });
        let mut last_error = None;
        for offset in 0..addresses.len() {
            if Instant::now() >= deadline {
                return Err(TransportError::Timeout);
            }
            let address = &addresses[(start + offset) % addresses.len()];
            let address_is_configured = self
                .inner
                .nameservers
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|configured| configured.iter().any(|entry| entry == address));
            if !address_is_configured {
                continue;
            }
            let connection = match self.inner.connection(address, deadline).await {
                Ok(connection) => connection,
                Err(error) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let replacement_exists = self
                        .inner
                        .connections
                        .lock()
                        .unwrap()
                        .get(address)
                        .is_some_and(|current| !current.is_closed());
                    if nameservers
                        .as_ref()
                        .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                        && !replacement_exists
                    {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .remove(address);
                    }
                    last_error = Some(error);
                    continue;
                }
            };
            let selected = {
                let nameservers = self.inner.nameservers.lock().unwrap();
                if nameservers
                    .as_ref()
                    .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                {
                    *self.inner.chosen_nameserver.lock().unwrap() = Some(address.clone());
                    true
                } else {
                    false
                }
            };
            if !selected {
                continue;
            }
            let result = if sync {
                self.invoke_sync_with_deadline(address, command, deadline)
                    .await
            } else {
                self.invoke(address, command, deadline).await
            };
            match result {
                Ok(response) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let connection_is_current = nameservers
                        .as_ref()
                        .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                        && !connection.is_closed()
                        && self
                            .inner
                            .connections
                            .lock()
                            .unwrap()
                            .get(address)
                            .is_some_and(|current| current.generation == connection.generation);
                    if connection_is_current {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .insert(address.clone());
                    }
                    drop(nameservers);
                    return Ok((response, address.clone()));
                }
                Err(error) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let connection_is_current = nameservers
                        .as_ref()
                        .is_some_and(|configured| configured.iter().any(|entry| entry == address))
                        && !connection.is_closed()
                        && self
                            .inner
                            .connections
                            .lock()
                            .unwrap()
                            .get(address)
                            .is_some_and(|current| current.generation == connection.generation);
                    if connection_is_current {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .remove(address);
                        if matches!(
                            error,
                            TransportError::Connect(_)
                                | TransportError::Send(_)
                                | TransportError::Closed
                                | TransportError::Tls(_)
                        ) {
                            let mut chosen = self.inner.chosen_nameserver.lock().unwrap();
                            if chosen.as_deref() == Some(address.as_str()) {
                                *chosen = None;
                            }
                        }
                    }
                    drop(nameservers);
                    return Err(error);
                }
            }
        }
        Err(last_error.unwrap_or(TransportError::Timeout))
    }

    pub fn invoke_async(
        &self,
        addr: impl Into<String>,
        mut command: RemotingCommand,
        deadline: Instant,
        callback: Arc<dyn InvokeCallback>,
    ) -> Result<(), TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let addr = addr.into();
        let started = Instant::now();
        let timeout_millis = remaining_timeout_millis(deadline, started);
        let callback_timing = Arc::new(Mutex::new(CallbackTiming {
            wrapper_timeout_millis: timeout_millis,
            response_begin: started,
            response_timeout_millis: timeout_millis,
        }));
        let client = self.clone();
        let callback_request_state = Arc::new(Mutex::new(command.clone()));
        let channel_established = Arc::new(AtomicBool::new(false));
        let callback_opaque = Arc::new(AtomicI32::new(command.opaque));
        let task = tokio::spawn(async move {
            let mut channel = None;
            let (result, permit) = match client.inner.connection(&addr, deadline).await {
                Ok(connection) => {
                    channel = Some(connection.channel_handle());
                    channel_established.store(true, Ordering::Release);
                    let wrapper_timeout_millis = remaining_timeout_millis(deadline, Instant::now());
                    callback_timing.lock().unwrap().wrapper_timeout_millis = wrapper_timeout_millis;
                    let hooks = client.inner.rpc_hooks.lock().unwrap().clone();
                    let hook_result = run_before_rpc_hooks(&hooks, &addr, &mut command);
                    *callback_request_state.lock().unwrap() = command.clone();
                    if let Err(error) = hook_result {
                        let future = ResponseFuture::from_result(
                            channel,
                            callback_request_state.lock().unwrap().clone(),
                            Err(rpc_hook_transport_error(error)),
                            wrapper_timeout_millis,
                            Instant::now(),
                            callback.clone(),
                            None,
                        );
                        client.inner.dispatch_callback(CallbackJob {
                            future,
                            channel_established: channel_established.clone(),
                        });
                        return;
                    }
                    match client
                        .inner
                        .acquire_admission(client.inner.async_admission.clone(), deadline)
                        .await
                    {
                        Ok(permit) => {
                            let mut admission = Some(permit);
                            let result = client
                                .invoke_inner(
                                    &addr,
                                    command,
                                    deadline,
                                    InvokeOptions {
                                        admitted: true,
                                        retried_go_away: false,
                                        run_rpc_hooks: true,
                                        close_socket_on_timeout: false,
                                        timeout_millis: std::time::Duration::ZERO,
                                    },
                                    Some(CallbackTracking {
                                        channel_established: channel_established.clone(),
                                        opaque: callback_opaque.clone(),
                                        request: callback_request_state.clone(),
                                        timing: callback_timing.clone(),
                                    }),
                                    &mut admission,
                                )
                                .await;
                            (result, admission)
                        }
                        Err(error) => {
                            let error = match error {
                                TransportError::TimeoutWithContext { phase, timeout, .. } => {
                                    TransportError::timeout(phase, Some(addr.clone()), timeout)
                                }
                                error => error,
                            };
                            (Err(error), None)
                        }
                    }
                }
                Err(error) => (Err(classify_connection_error(&addr, error)), None),
            };
            let (future_timeout_millis, future_begin) = {
                let timing = *callback_timing.lock().unwrap();
                if result.is_ok() {
                    (timing.response_timeout_millis, timing.response_begin)
                } else {
                    (timing.wrapper_timeout_millis, Instant::now())
                }
            };
            let future = ResponseFuture::from_result(
                channel,
                callback_request_state.lock().unwrap().clone(),
                result,
                future_timeout_millis,
                future_begin,
                callback,
                permit,
            );
            client.inner.dispatch_callback(CallbackJob {
                future,
                channel_established: channel_established.clone(),
            });
        });
        self.inner.spawn(task);
        Ok(())
    }

    /// Sends one command and completes with the matching opaque response.
    pub async fn invoke(
        &self,
        addr: &str,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        let mut admission = None;
        self.invoke_inner(
            addr,
            command,
            deadline,
            InvokeOptions {
                admitted: false,
                retried_go_away: false,
                run_rpc_hooks: true,
                close_socket_on_timeout: false,
                timeout_millis: std::time::Duration::ZERO,
            },
            None,
            &mut admission,
        )
        .await
    }

    pub async fn invoke_sync(
        &self,
        addr: &str,
        command: RemotingCommand,
        timeout: std::time::Duration,
    ) -> Result<RemotingCommand, TransportError> {
        let timeout_millis = timeout.as_millis().min(i64::MAX as u128) as i64;
        self.invoke_sync_with_timeout_millis(addr, command, timeout_millis)
            .await
    }

    /// Absolute-deadline form of Java synchronous invocation for typed RPCs.
    pub async fn invoke_sync_with_deadline(
        &self,
        addr: &str,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        let timeout = deadline.saturating_duration_since(Instant::now());
        self.invoke_sync_deadline_inner(addr, command, deadline, timeout)
            .await
    }

    /// Synchronous Java-compatible call with a signed millisecond timeout.
    ///
    /// Java's `NettyRemotingClient.invokeSync` acquires or creates the channel
    /// before it checks whether the request timeout has elapsed. This preserves
    /// that ordering for RocketMQ client methods which receive a signed timeout
    /// value directly. A non-positive timeout can therefore establish a channel,
    /// but it never sends a request frame.
    #[doc(hidden)]
    pub async fn invoke_sync_with_timeout_millis(
        &self,
        addr: &str,
        command: RemotingCommand,
        timeout_millis: i64,
    ) -> Result<RemotingCommand, TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }

        let begin = Instant::now();
        self.inner
            .connection_for_scan(addr)
            .await
            .map_err(|error| match error {
                TransportError::Timeout => TransportError::ConnectRequest {
                    remote_addr: addr.to_owned(),
                    cause: None,
                },
                error => classify_connection_error(addr, error),
            })?;

        // Java subtracts whole elapsed milliseconds from its signed long
        // timeout after the channel future completes.
        let elapsed_millis = begin.elapsed().as_millis().min(i64::MAX as u128) as i64;
        let remaining_millis = timeout_millis.saturating_sub(elapsed_millis);
        if remaining_millis <= 0 {
            return Err(TransportError::timeout(
                crate::error::TimeoutPhase::Operation,
                Some(addr.to_owned()),
                (timeout_millis > 0)
                    .then(|| std::time::Duration::from_millis(timeout_millis as u64)),
            ));
        }

        let remaining = std::time::Duration::from_millis(remaining_millis as u64);
        self.invoke_sync_deadline_inner(addr, command, Instant::now() + remaining, remaining)
            .await
    }

    async fn invoke_sync_deadline_inner(
        &self,
        addr: &str,
        command: RemotingCommand,
        deadline: Instant,
        timeout: std::time::Duration,
    ) -> Result<RemotingCommand, TransportError> {
        let mut admission = None;
        self.invoke_inner(
            addr,
            command,
            deadline,
            InvokeOptions {
                admitted: false,
                retried_go_away: false,
                run_rpc_hooks: true,
                close_socket_on_timeout: true,
                timeout_millis: timeout,
            },
            None,
            &mut admission,
        )
        .await
    }

    async fn invoke_inner(
        &self,
        addr: &str,
        mut command: RemotingCommand,
        deadline: Instant,
        options: InvokeOptions,
        callback: Option<CallbackTracking>,
        admission: &mut Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<RemotingCommand, TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let timeout = (!options.timeout_millis.is_zero()).then_some(options.timeout_millis);
        if Instant::now() >= deadline {
            return Err(TransportError::TimeoutWithContext {
                phase: crate::error::TimeoutPhase::Operation,
                remote_addr: Some(addr.to_owned()),
                timeout,
                cause: None,
            });
        }
        let connection = self
            .inner
            .connection(addr, deadline)
            .await
            .map_err(|error| classify_connection_error(addr, error))
            .map_err(|error| match error {
                TransportError::Timeout => TransportError::TimeoutWithContext {
                    phase: crate::error::TimeoutPhase::Connection,
                    remote_addr: Some(addr.to_owned()),
                    timeout,
                    cause: None,
                },
                error => error,
            })?;
        if let Some(callback) = &callback {
            callback.channel_established.store(true, Ordering::Release);
        }
        let timeout_budget = deadline.saturating_duration_since(Instant::now());
        if Instant::now() >= deadline {
            return Err(TransportError::TimeoutWithContext {
                phase: crate::error::TimeoutPhase::Admission,
                remote_addr: Some(addr.to_owned()),
                timeout,
                cause: None,
            });
        }
        if options.run_rpc_hooks && !options.retried_go_away && !options.admitted {
            let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
            run_before_rpc_hooks(&hooks, addr, &mut command).map_err(rpc_hook_transport_error)?;
        }
        let mut request_for_hooks = command.clone();
        if !options.admitted {
            match self
                .inner
                .acquire_admission(self.inner.async_admission.clone(), deadline)
                .await
            {
                Ok(permit) => *admission = Some(permit),
                Err(error) if error.is_timeout() || error == TransportError::TooManyRequests => {
                    let error = if error.is_timeout() {
                        TransportError::TimeoutWithContext {
                            phase: crate::error::TimeoutPhase::Admission,
                            remote_addr: Some(addr.to_owned()),
                            timeout,
                            cause: None,
                        }
                    } else {
                        error
                    };
                    if options.close_socket_on_timeout {
                        connection.close();
                        return Err(TransportError::SendRequest {
                            remote_addr: addr.to_owned(),
                            cause: Some(crate::error::ErrorSource::message(error.to_string())),
                        });
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(callback) = &callback {
            let response_begin = Instant::now();
            let mut timing = callback.timing.lock().unwrap();
            timing.response_begin = response_begin;
            timing.response_timeout_millis = remaining_timeout_millis(deadline, response_begin);
        }
        let pending_deadline = deadline + std::time::Duration::from_secs(1);
        let (opaque, receiver) = self.inner.insert_pending(
            connection.generation,
            pending_deadline,
            connection.address().to_owned(),
            command.code,
        )?;
        command.opaque = opaque;
        if let Some(callback) = &callback {
            callback.opaque.store(opaque, Ordering::Release);
            if !options.retried_go_away {
                callback.request.lock().unwrap().opaque = opaque;
            }
        }
        let mut pending =
            pending::PendingGuard::new(Arc::downgrade(&self.inner), opaque, connection.generation);
        let frame = connection
            .encode_command(&command, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE)
            .map_err(|error| classify_write_error(connection.address(), error))?;
        connection
            .enqueue_without_deadline(frame)
            .map_err(|error| classify_write_error(connection.address(), error))?;
        let response_deadline = if options.close_socket_on_timeout {
            deadline
        } else {
            pending_deadline
        };
        match time::timeout_at(response_deadline, receiver).await {
            Ok(Ok(Ok(mut response))) => {
                pending.disarm();
                if response.code == rocketmq_classic_protocol::codes::response::GO_AWAY {
                    if self
                        .inner
                        .enable_reconnect_for_go_away
                        .load(Ordering::Acquire)
                        && !options.retried_go_away
                        && self.inner.retire_for_go_away(addr, connection.clone())
                    {
                        let retry_request = RemotingCommand {
                            code: request_for_hooks.code,
                            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                            ext_fields: request_for_hooks.ext_fields.clone(),
                            body: request_for_hooks.body.clone(),
                            ..RemotingCommand::default()
                        };
                        admission.take();
                        let mut retry = Box::pin(self.invoke_inner(
                            addr,
                            retry_request.clone(),
                            deadline,
                            InvokeOptions {
                                admitted: false,
                                retried_go_away: true,
                                run_rpc_hooks: false,
                                close_socket_on_timeout: options.close_socket_on_timeout,
                                timeout_millis: options.timeout_millis,
                            },
                            callback.clone(),
                            admission,
                        ));
                        let mut result = retry.as_mut().await;
                        if let Ok(response) = &mut result {
                            let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
                            run_after_rpc_hooks(&hooks, addr, &mut request_for_hooks, response)
                                .map_err(rpc_hook_transport_error)?;
                            if let Some(callback) = &callback {
                                let mut request = retry_request;
                                request.opaque = callback.opaque.load(Ordering::Acquire);
                                *callback.request.lock().unwrap() = request;
                            }
                        }
                        return result;
                    }
                    return Err(TransportError::SendRequest {
                        remote_addr: connection.address().to_owned(),
                        cause: Some(if options.retried_go_away {
                            crate::error::ErrorSource::new(crate::error::GoAwayCause {
                                remote_addr: connection.address().to_owned(),
                                retried: true,
                            })
                        } else {
                            crate::error::ErrorSource::new(crate::error::GoAwayCause {
                                remote_addr: connection.address().to_owned(),
                                retried: false,
                            })
                        }),
                    });
                }
                if options.run_rpc_hooks && !options.retried_go_away {
                    let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
                    run_after_rpc_hooks(&hooks, addr, &mut request_for_hooks, &mut response)
                        .map_err(rpc_hook_transport_error)?;
                }
                Ok(response)
            }
            Ok(Ok(Err(error))) => {
                pending.disarm();
                Err(error)
            }
            Ok(Err(_)) => {
                pending.disarm();
                if Instant::now() >= deadline {
                    Err(TransportError::TimeoutWithContext {
                        phase: crate::error::TimeoutPhase::Response,
                        remote_addr: Some(addr.to_owned()),
                        timeout,
                        cause: None,
                    })
                } else {
                    Err(TransportError::Closed)
                }
            }
            Err(_) => {
                if options.close_socket_on_timeout {
                    let mut entries = self.inner.pending.lock().unwrap();
                    if let Some(entry) = entries
                        .get_mut(&opaque)
                        .filter(|entry| entry.generation == connection.generation)
                    {
                        entry.retained_admission = admission.take();
                    }
                } else {
                    self.inner.cancel_pending(opaque, connection.generation);
                }
                if options.close_socket_on_timeout
                    && self.inner.close_socket_if_timeout.load(Ordering::Acquire)
                    && (timeout_budget > std::time::Duration::from_millis(100)
                        || timeout_budget > options.timeout_millis / 4)
                {
                    connection.close();
                }
                pending.disarm();
                Err(TransportError::TimeoutWithContext {
                    phase: crate::error::TimeoutPhase::Response,
                    remote_addr: Some(addr.to_owned()),
                    timeout,
                    cause: None,
                })
            }
        }
    }

    /// Starts connection acquisition at call time and returns after the client-owned operation is
    /// scheduled. As in Java, the caller receives only call-boundary errors; it does not observe
    /// later connection, admission, or socket-write completion, and this call never waits for a
    /// Broker response.
    pub fn invoke_oneway(
        &self,
        addr: &str,
        mut command: RemotingCommand,
        deadline: Instant,
    ) -> Result<(), TransportError> {
        if !self.inner.started.load(Ordering::Acquire) {
            return Err(TransportError::NotStarted);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        connection_manager::Address::parse(addr)?;
        let client = self.clone();
        let addr = addr.to_owned();
        let (connection_tx, connection_rx) = tokio::sync::oneshot::channel();
        let connection_client = client.clone();
        let connection_addr = addr.clone();
        let connection_task = async move {
            let result = connection_client
                .inner
                .connection(&connection_addr, deadline)
                .await;
            let _ = connection_tx.send(result);
        };
        if let Some(connection_task) = spawn_after_initial_poll(connection_task) {
            self.inner.spawn(connection_task);
        }
        let task = tokio::spawn(async move {
            let _ = async {
                let connection = connection_rx
                    .await
                    .map_err(|_| TransportError::Closed)?
                    .map_err(|error| classify_connection_error(&addr, error))?;
                let hooks = client.inner.rpc_hooks.lock().unwrap().clone();
                run_before_rpc_hooks(&hooks, &addr, &mut command)
                    .map_err(rpc_hook_transport_error)?;
                command.mark_oneway();
                let _admission = client
                    .inner
                    .acquire_admission(client.inner.oneway_admission.clone(), deadline)
                    .await
                    .map_err(|error| match error {
                        TransportError::TimeoutWithContext { phase, timeout, .. } => {
                            TransportError::timeout(phase, Some(addr.clone()), timeout)
                        }
                        error => error,
                    })?;
                let frame = connection
                    .encode_command(&command, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE)?;
                let written = connection
                    .enqueue_without_deadline(frame)
                    .map_err(|error| classify_write_error(&addr, error))?;
                written
                    .await
                    .map_err(|_| TransportError::Closed)?
                    .map_err(|error| classify_write_error(&addr, error))
            }
            .await;
        });
        self.inner.spawn(task);
        Ok(())
    }
}
