use std::sync::{Arc, Weak, atomic::AtomicBool};

use bytes::BytesMut;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpSocket,
    sync::mpsc,
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_socks::tcp::Socks5Stream;

use crate::{
    codec::frame,
    error::{ErrorSource, TransportError},
};

use super::response_future::ChannelHandle;
use super::{ClientInner, config::SocksProxyConfig, connection_manager};

pub use super::tls::TransportMode;

pub(super) type BoxedStream = Box<dyn AsyncIo>;

pub(super) trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncIo for T {}

struct OutboundFrame {
    bytes: Option<Vec<u8>>,
    frame_pool: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    pooled: bool,
    deadline: Option<Instant>,
    cancelled: Option<tokio::sync::oneshot::Receiver<()>>,
    written: Option<tokio::sync::oneshot::Sender<Result<(), TransportError>>>,
}

impl Drop for OutboundFrame {
    fn drop(&mut self) {
        if self.pooled {
            if let Some(mut bytes) = self.bytes.take() {
                bytes.clear();
                let mut pool = self.frame_pool.lock().unwrap();
                if pool.len() < 4 {
                    pool.push(bytes);
                }
            }
        }
    }
}

pub(crate) struct Connection {
    pub(crate) generation: u64,
    channel: ChannelHandle,
    sender: mpsc::UnboundedSender<OutboundFrame>,
    address: String,
    pooled_byte_buffer_allocator_enabled: bool,
    closed: Arc<AtomicBool>,
    retired: AtomicBool,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    queued_write_bytes: Arc<std::sync::atomic::AtomicUsize>,
    write_watermarks: (usize, usize),
    writable: Arc<AtomicBool>,
    frame_pool: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

struct FrameGuard<'a> {
    connection: &'a Connection,
    bytes: Option<Vec<u8>>,
}

impl FrameGuard<'_> {
    fn take(&mut self) -> Vec<u8> {
        self.bytes.take().expect("frame is present until queued")
    }
}

impl Drop for FrameGuard<'_> {
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes.take() {
            self.connection.recycle_frame(bytes);
        }
    }
}

impl Connection {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        stream: BoxedStream,
        generation: u64,
        inner: Weak<ClientInner>,
        max_frame_size: usize,
        write_watermarks: (usize, usize),
        address: String,
        pooled_byte_buffer_allocator_enabled: bool,
    ) -> Arc<Self> {
        let (max_idle_time, disable_worker_group) = inner
            .upgrade()
            .map(|client| {
                (
                    client.config.max_idle_time_duration(),
                    client.config.disable_netty_worker_group,
                )
            })
            .unwrap_or_default();
        let channel = ChannelHandle::pending(&address);
        Self::spawn_with_channel_settings(
            stream,
            generation,
            inner,
            channel,
            max_frame_size,
            write_watermarks,
            address,
            pooled_byte_buffer_allocator_enabled,
            max_idle_time,
            disable_worker_group,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_with_channel_settings(
        stream: BoxedStream,
        generation: u64,
        inner: Weak<ClientInner>,
        channel: ChannelHandle,
        max_frame_size: usize,
        write_watermarks: (usize, usize),
        address: String,
        pooled_byte_buffer_allocator_enabled: bool,
        max_idle_time: std::time::Duration,
        disable_worker_group: bool,
    ) -> Arc<Self> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let closed = channel.closed_flag();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let connection = Arc::new(Self {
            generation,
            channel: channel.clone(),
            sender,
            address: address.clone(),
            pooled_byte_buffer_allocator_enabled,
            closed: closed.clone(),
            retired: AtomicBool::new(false),
            task: tokio::sync::Mutex::new(None),
            shutdown,
            queued_write_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            write_watermarks,
            writable: channel.writable_flag(),
            frame_pool: Arc::new(std::sync::Mutex::new(Vec::new())),
        });
        channel.attach(&connection);
        let task = tokio::spawn(run_connection(
            stream,
            receiver,
            inner,
            ConnectionTask {
                generation,
                channel,
                closed,
                shutdown: shutdown_rx,
                max_frame_size,
                address,
                queued_write_bytes: connection.queued_write_bytes.clone(),
                write_watermarks,
                writable: connection.writable.clone(),
                pooled_byte_buffer_allocator_enabled,
                max_idle_time,
                frame_pool: connection.frame_pool.clone(),
                disable_worker_group,
            },
        ));
        connection
            .task
            .try_lock()
            .expect("new connection task lock")
            .replace(task);
        connection
    }

    pub(crate) fn encode_command(
        &self,
        command: &crate::command::RemotingCommand,
        max_frame_size: usize,
    ) -> Result<Vec<u8>, TransportError> {
        let mut bytes = self.take_frame_buffer();
        if let Err(error) = frame::encode_into_with_limit(command, max_frame_size, &mut bytes) {
            self.recycle_frame(bytes);
            return Err(TransportError::Codec(error));
        }
        Ok(bytes)
    }

    fn take_frame_buffer(&self) -> Vec<u8> {
        if self.pooled_byte_buffer_allocator_enabled {
            self.frame_pool.lock().unwrap().pop().unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn recycle_frame(&self, mut bytes: Vec<u8>) {
        if self.pooled_byte_buffer_allocator_enabled {
            bytes.clear();
            let mut pool = self.frame_pool.lock().unwrap();
            if pool.len() < 4 {
                pool.push(bytes);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn queued_write_bytes(&self) -> usize {
        self.queued_write_bytes
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn pooled_frame_count(&self) -> usize {
        self.frame_pool.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(super) fn pooled_frame_capacity(&self) -> usize {
        self.frame_pool
            .lock()
            .unwrap()
            .iter()
            .map(Vec::capacity)
            .max()
            .unwrap_or_default()
    }

    pub(crate) async fn send(
        &self,
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<(), TransportError> {
        self.send_with_deadline(bytes, Some(deadline)).await
    }

    pub(crate) async fn send_without_deadline(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        self.send_with_deadline(bytes, None).await
    }

    pub(crate) fn enqueue_without_deadline(
        &self,
        bytes: Vec<u8>,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), TransportError>>, TransportError> {
        let (written, response) = tokio::sync::oneshot::channel();
        let length = bytes.len();
        self.reserve_write_bytes(length);
        if self
            .sender
            .send(OutboundFrame {
                bytes: Some(bytes),
                frame_pool: self.frame_pool.clone(),
                pooled: self.pooled_byte_buffer_allocator_enabled,
                deadline: None,
                cancelled: None,
                written: Some(written),
            })
            .is_err()
        {
            self.release_write_bytes(length);
            return Err(TransportError::Closed);
        }
        Ok(response)
    }

    async fn send_with_deadline(
        &self,
        bytes: Vec<u8>,
        deadline: Option<Instant>,
    ) -> Result<(), TransportError> {
        let mut frame = FrameGuard {
            connection: self,
            bytes: Some(bytes),
        };
        let (written, response) = tokio::sync::oneshot::channel();
        let length = frame.bytes.as_ref().unwrap().len();
        self.reserve_write_bytes(length);
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let mut cancel = Some(cancel);
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            self.release_write_bytes(length);
            return Err(TransportError::Timeout);
        }
        if let Err(error) = self.sender.send(OutboundFrame {
            bytes: Some(frame.take()),
            frame_pool: self.frame_pool.clone(),
            pooled: self.pooled_byte_buffer_allocator_enabled,
            deadline,
            cancelled: Some(cancelled),
            written: Some(written),
        }) {
            self.release_write_bytes(length);
            drop(error.0);
            return Err(TransportError::Closed);
        }
        let write_result = match deadline {
            Some(deadline) => time::timeout_at(deadline, response).await.map_err(|_| {
                drop(cancel.take());
                TransportError::Timeout
            })?,
            None => response.await,
        };
        let result = write_result.map_err(|_| TransportError::Closed)?;
        drop(cancel.take());
        result
    }

    #[cfg(test)]
    pub(crate) fn try_send(&self, bytes: Vec<u8>, deadline: Instant) -> Result<(), TransportError> {
        let (written, _response) = tokio::sync::oneshot::channel();
        self.reserve_write_bytes(bytes.len());
        let length = bytes.len();
        match self.sender.send(OutboundFrame {
            bytes: Some(bytes),
            frame_pool: self.frame_pool.clone(),
            pooled: self.pooled_byte_buffer_allocator_enabled,
            deadline: Some(deadline),
            cancelled: None,
            written: Some(written),
        }) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.release_write_bytes(length);
                match error {
                    mpsc::error::SendError(_) => Err(TransportError::Closed),
                }
            }
        }
    }

    pub(crate) fn mark_retired(&self) {
        self.retired
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        self.channel.mark_closed();
        let _ = self.shutdown.send(true);
    }

    pub(crate) fn channel_handle(&self) -> ChannelHandle {
        self.channel.clone()
    }

    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn is_writable(&self) -> bool {
        !self.is_closed() && self.writable.load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn watermark_is_writable(&self) -> bool {
        self.writable.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(super) fn reserve_write_bytes(&self, length: usize) {
        let queued = self
            .queued_write_bytes
            .fetch_add(length, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(length);
        let (high, _) = self.write_watermarks;
        if queued > high {
            self.writable
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }

    pub(super) fn release_write_bytes(&self, length: usize) {
        let queued = self
            .queued_write_bytes
            .fetch_sub(length, std::sync::atomic::Ordering::AcqRel)
            .saturating_sub(length);
        let (_, low) = self.write_watermarks;
        if queued < low {
            self.writable
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    pub(crate) async fn join(&self) {
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }

    pub(crate) async fn join_until(&self, deadline: Instant) -> bool {
        let mut task = self.task.lock().await;
        let Some(join) = task.as_mut() else {
            return true;
        };
        if tokio::time::timeout_at(deadline, join).await.is_err() {
            return false;
        }
        task.take();
        true
    }

    pub(crate) async fn abort_task(&self) {
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
        }
    }
}

struct ConnectionTask {
    generation: u64,
    channel: ChannelHandle,
    closed: Arc<AtomicBool>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    max_frame_size: usize,
    address: String,
    queued_write_bytes: Arc<std::sync::atomic::AtomicUsize>,
    write_watermarks: (usize, usize),
    writable: Arc<AtomicBool>,
    pooled_byte_buffer_allocator_enabled: bool,
    max_idle_time: std::time::Duration,
    frame_pool: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    disable_worker_group: bool,
}

async fn run_connection(
    stream: BoxedStream,
    mut outbound: mpsc::UnboundedReceiver<OutboundFrame>,
    inner: Weak<ClientInner>,
    task: ConnectionTask,
) {
    let ConnectionTask {
        generation,
        channel,
        closed,
        mut shutdown,
        max_frame_size,
        address,
        queued_write_bytes,
        write_watermarks,
        writable,
        pooled_byte_buffer_allocator_enabled,
        max_idle_time,
        frame_pool,
        disable_worker_group,
    } = task;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let idle_timer = time::sleep(max_idle_time);
    tokio::pin!(idle_timer);
    let mut input = BytesMut::with_capacity(8192);
    let mut read_buffer = vec![0; 8192];
    let mut active_frame: Option<OutboundFrame> = None;
    let mut active_offset = 0;
    let complete_frame = |mut frame: OutboundFrame, result: Result<(), TransportError>| {
        let write_length = frame.bytes.as_ref().expect("queued frame bytes").len();
        let queued = queued_write_bytes
            .fetch_sub(write_length, std::sync::atomic::Ordering::AcqRel)
            .saturating_sub(write_length);
        let (_, low) = write_watermarks;
        if queued < low {
            writable.store(true, std::sync::atomic::Ordering::Release);
        }
        if let Some(bytes) = frame.bytes.take() {
            if pooled_byte_buffer_allocator_enabled {
                let mut bytes = bytes;
                bytes.clear();
                let mut pool = frame_pool.lock().unwrap();
                if pool.len() < 4 {
                    pool.push(bytes);
                }
            }
        }
        if let Some(written) = frame.written.take() {
            let _ = written.send(result);
        }
    };
    let failure = 'connection: loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => if result.is_err() || *shutdown.borrow() { break None },
            _ = &mut idle_timer, if !max_idle_time.is_zero() => {
                if let Some(inner) = inner.upgrade() {
                    inner.notify_channel(super::NettyEventType::Idle, channel.clone());
                }
                break None;
            },
            result = async {
                let frame = active_frame.as_mut().expect("active frame branch");
                let bytes = frame.bytes.as_ref().expect("queued frame bytes");
                let remaining = &bytes[active_offset..];
                tokio::select! {
                    result = async {
                        match frame.deadline {
                            Some(deadline) => time::timeout_at(deadline, writer.write(remaining))
                                .await
                                .map_err(|_| TransportError::Timeout)?
                                .map_err(|error| TransportError::SendSource(ErrorSource::new(error))),
                            None => writer.write(remaining)
                                .await
                                .map_err(|error| TransportError::SendSource(ErrorSource::new(error))),
                        }
                    } => result,
                    _ = async {
                        match frame.cancelled.as_mut() {
                            Some(cancelled) => { let _ = cancelled.await; }
                            None => std::future::pending().await,
                        }
                    } => Err(TransportError::Closed),
                }
            }, if active_frame.is_some() => {
                match result {
                    Ok(0) => {
                        let frame = active_frame.take().expect("completed active frame");
                        let error = TransportError::SendSource(ErrorSource::new(
                            std::io::Error::new(
                                std::io::ErrorKind::WriteZero,
                                "failed to write the complete remoting frame",
                            ),
                        ));
                        complete_frame(frame, Err(error.clone()));
                        break Some(error);
                    }
                    Ok(written) => {
                        active_offset += written;
                        if active_offset == active_frame
                            .as_ref()
                            .expect("active frame after partial write")
                            .bytes
                            .as_ref()
                            .expect("queued frame bytes")
                            .len()
                        {
                            let frame = active_frame.take().expect("completed active frame");
                            if !max_idle_time.is_zero() {
                                idle_timer
                                    .as_mut()
                                    .reset(Instant::now() + max_idle_time);
                            }
                            complete_frame(frame, Ok(()));
                        }
                    }
                    Err(error) => {
                        let frame = active_frame.take().expect("failed active frame");
                        complete_frame(frame, Err(error.clone()));
                        break if error == TransportError::Shutdown {
                            None
                        } else {
                            Some(error)
                        };
                    }
                }
            },
            frame = outbound.recv(), if active_frame.is_none() => match frame {
                Some(frame) => {
                    active_frame = Some(frame);
                    active_offset = 0;
                },
                None if closed.load(std::sync::atomic::Ordering::Acquire) => break None,
                None => break Some(TransportError::Closed),
            },
            result = reader.read(&mut read_buffer) => match result {
                Ok(0) => break Some(TransportError::Closed),
                Ok(length) => {
                    if !max_idle_time.is_zero() {
                        idle_timer
                            .as_mut()
                            .reset(Instant::now() + max_idle_time);
                    }
                    input.extend_from_slice(&read_buffer[..length]);
                    loop {
                        let process_timer = crate::command::ProcessTimer::started();
                        match frame::decode_next_with_limit(&mut input, max_frame_size) {
                            Ok(Some(mut command)) => {
                                command.set_process_timer(Some(process_timer));
                                if command.is_response() {
                                    if let Some(inner) = inner.upgrade() {
                                        inner.complete_response(generation, command);
                                    }
                                } else if let Some(inner) = inner.upgrade() {
                                    let request = super::processor::InboundRequest {
                                        generation,
                                        remote_addr: address.clone(),
                                        channel: channel.clone(),
                                        command,
                                    };
                                    if disable_worker_group {
                                        super::processor::process_one(&Arc::downgrade(&inner), request);
                                    } else {
                                        let _ = inner.inbound.send(request);
                                    }
                                } else {
                                    break 'connection None;
                                }
                            }
                            Ok(None) => break,
                            Err(error) => break 'connection Some(TransportError::MalformedFrame(error)),
                        }
                    }
                }
                Err(_) => break Some(TransportError::Closed),
            },
        }
    };
    if let Some(frame) = active_frame.take() {
        complete_frame(
            frame,
            Err(failure.clone().unwrap_or(TransportError::Shutdown)),
        );
    }
    closed.store(true, std::sync::atomic::Ordering::Release);
    if let Some(inner) = inner.upgrade() {
        inner.connection_closed(generation, failure);
    }
}

pub(super) async fn connect(
    address: &str,
    mode: &TransportMode,
    use_tls: &AtomicBool,
    proxy: Option<&SocksProxyConfig>,
    send_buffer_size: i32,
    receive_buffer_size: i32,
    deadline: Option<Instant>,
) -> Result<BoxedStream, TransportError> {
    let (host, port) = split_address(address)?;
    let host = connect_host(&host);
    let dial = async {
        if let Some(proxy) = proxy {
            // Java's proxy Bootstrap always installs both SO_*BUF options,
            // including zero and negative config values. The JDK rejects
            // nonpositive sizes when the channel applies those options.
            if send_buffer_size <= 0 || receive_buffer_size <= 0 {
                return Err(TransportError::InvalidConfiguration(
                    "SO_SNDBUF and SO_RCVBUF must be positive for proxy connections".into(),
                ));
            }
            let stream = match (&proxy.username, &proxy.password) {
                (Some(username), Some(password)) => {
                    Socks5Stream::connect_with_password(
                        proxy.address.as_str(),
                        (host.as_str(), port),
                        username,
                        password,
                    )
                    .await
                }
                _ => Socks5Stream::connect(proxy.address.as_str(), (host.as_str(), port)).await,
            }
            .map_err(TransportError::connect_source)?
            .into_inner();
            let socket = socket2::SockRef::from(&stream);
            socket
                .set_send_buffer_size(send_buffer_size as usize)
                .map_err(TransportError::connect_source)?;
            socket
                .set_recv_buffer_size(receive_buffer_size as usize)
                .map_err(TransportError::connect_source)?;
            Ok(Box::new(stream) as BoxedStream)
        } else {
            let addresses = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(TransportError::connect_source)?
                .collect::<Vec<_>>();
            if addresses.is_empty() {
                return Err(TransportError::Connect(
                    "address resolved to no socket addresses".into(),
                ));
            }
            let mut connected = None;
            let mut last_error = None;
            for address in addresses {
                let socket = if address.is_ipv6() {
                    TcpSocket::new_v6()
                } else {
                    TcpSocket::new_v4()
                }
                .map_err(TransportError::connect_source)?;
                let socket_ref = socket2::SockRef::from(&socket);
                if send_buffer_size > 0 {
                    socket_ref
                        .set_send_buffer_size(send_buffer_size as usize)
                        .map_err(TransportError::connect_source)?;
                }
                if receive_buffer_size > 0 {
                    socket_ref
                        .set_recv_buffer_size(receive_buffer_size as usize)
                        .map_err(TransportError::connect_source)?;
                }
                match socket.connect(address).await {
                    Ok(stream) => {
                        connected = Some(stream);
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            let stream = connected.ok_or_else(|| match last_error {
                Some(error) => TransportError::connect_source(error),
                None => TransportError::Connect("connection failed".into()),
            })?;
            stream
                .set_nodelay(true)
                .map_err(TransportError::connect_source)?;
            Ok(Box::new(stream) as BoxedStream)
        }
    };
    let stream = match deadline {
        Some(deadline) => time::timeout_at(deadline, dial)
            .await
            .map_err(|_| TransportError::Timeout)??,
        None => dial.await?,
    };
    if !use_tls.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(stream);
    }
    match mode {
        TransportMode::Plain => Ok(stream),
        TransportMode::Tls(config) => {
            let server_name = ServerName::try_from(host).map_err(TransportError::tls_source)?;
            let connector = config.connector()?;
            let handshake = connector.connect(server_name, stream);
            let tls = match deadline {
                Some(deadline) => time::timeout_at(deadline, handshake)
                    .await
                    .map_err(|_| TransportError::Timeout)?,
                None => handshake.await,
            }
            .map_err(TransportError::tls_source)?;
            Ok(Box::new(tls) as BoxedStream)
        }
    }
}

pub(super) fn split_address(address: &str) -> Result<(String, u16), TransportError> {
    let (raw_host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| TransportError::Connect("missing address port".into()))?;
    let host = raw_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(raw_host);
    let port = connection_manager::parse_socket_port(port)?;
    Ok((host.to_owned(), port))
}

pub(super) fn connect_host(host: &str) -> String {
    if host.is_empty() {
        // The pinned JDK resolves InetSocketAddress("", port) to IPv4 loopback.
        "127.0.0.1".to_owned()
    } else {
        host.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{SocksProxyConfig, TransportMode, connect};
    use crate::error::TransportError;
    use std::sync::atomic::AtomicBool;

    #[tokio::test]
    async fn proxy_nonpositive_socket_buffers_fail_before_dial() {
        let proxy = SocksProxyConfig::new("127.0.0.1:1");
        for (send_buffer_size, receive_buffer_size) in
            [(0, 8192), (8192, 0), (-1, 8192), (8192, -1)]
        {
            let result = connect(
                "127.0.0.1:10911",
                &TransportMode::Plain,
                &AtomicBool::new(false),
                Some(&proxy),
                send_buffer_size,
                receive_buffer_size,
                None,
            )
            .await;
            assert!(matches!(
                result,
                Err(TransportError::InvalidConfiguration(_))
            ));
        }
    }
}
