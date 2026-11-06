use std::{error::Error, fmt, sync::Arc, time::Duration};

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    #[error("frame length {0} is smaller than the required length prefix")]
    InvalidFrameLength(i32),
    #[error("frame length {length} exceeds configured limit {limit}")]
    FrameTooLarge { length: usize, limit: usize },
    #[error("header length {header} exceeds frame payload length {payload}")]
    HeaderTooLarge { header: usize, payload: usize },
    #[error("unsupported serialization type {0}")]
    UnsupportedSerialization(u8),
    #[error("ROCKETMQ binary header requires a language")]
    MissingLanguage,
    #[error("malformed {0} header: {1}")]
    MalformedHeader(&'static str, String),
    #[error("field `{0}` does not fit Java wire width")]
    FieldTooWide(&'static str),
    #[error("JSON header error: {0}")]
    Json(String),
    #[error("invalid UTF-8 in RocketMQ header")]
    InvalidUtf8,
}

/// A cloneable, typed error source retained by transport errors and callback results.
///
/// Clones preserve the wrapped error identity, and equality compares that identity like Java's
/// `Throwable.equals` inherited from `Object`.
#[derive(Clone)]
pub struct ErrorSource(Arc<dyn Error + Send + Sync + 'static>);

impl ErrorSource {
    pub fn new(error: impl Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(error))
    }

    pub fn with_context(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::new(ContextError {
            message: message.into(),
            source: Box::new(source),
        })
    }

    pub fn message(message: impl Into<String>) -> Self {
        Self::new(MessageError(message.into()))
    }

    pub fn downcast_ref<T: Error + 'static>(&self) -> Option<&T> {
        let mut current: &(dyn Error + 'static) = self.0.as_ref();
        loop {
            if let Some(error) = current.downcast_ref() {
                return Some(error);
            }
            current = current.source()?;
        }
    }
}

impl fmt::Debug for ErrorSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ErrorSource")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for ErrorSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for ErrorSource {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl PartialEq for ErrorSource {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ErrorSource {}

impl From<String> for ErrorSource {
    fn from(message: String) -> Self {
        Self::message(message)
    }
}

impl From<&str> for ErrorSource {
    fn from(message: &str) -> Self {
        Self::message(message)
    }
}

#[derive(Debug)]
struct MessageError(String);

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for MessageError {}

#[derive(Debug)]
struct ContextError {
    message: String,
    source: Box<dyn Error + Send + Sync>,
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.message, self.source)
    }
}

impl Error for ContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoAwayCause {
    pub remote_addr: String,
    pub retried: bool,
}

impl fmt::Display for GoAwayCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.retried {
            write!(
                formatter,
                "Receive GO_AWAY twice in request from channelId={}",
                self.remote_addr
            )
        } else {
            write!(
                formatter,
                "Receive GO_AWAY from channelId={}",
                self.remote_addr
            )
        }
    }
}

impl Error for GoAwayCause {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TimeoutPhase {
    #[error("operation")]
    Operation,
    #[error("connection acquisition")]
    Connection,
    #[error("request admission")]
    Admission,
    #[error("socket write")]
    Write,
    #[error("response wait")]
    Response,
}

/// Typed transport outcomes corresponding to the Java remoting exception paths.
///
/// Error construction helpers used by the transport remain internal; callers can inspect or
/// construct the public variants directly.
///
/// ```compile_fail
/// use rocketmq_classic_remoting::{TimeoutPhase, TransportError};
/// TransportError::timeout(TimeoutPhase::Response, None, None);
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_remoting::{TimeoutPhase, TransportError};
/// TransportError::timeout_with_cause(TimeoutPhase::Response, None, None, None);
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_remoting::TransportError;
/// TransportError::connect_source(std::io::Error::other("refused"));
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_remoting::TransportError;
/// TransportError::tls_source(std::io::Error::other("handshake"));
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_remoting::TransportError;
/// TransportError::send_source(std::io::Error::other("broken pipe"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connect(String),
    #[error("connection failed: {0}")]
    ConnectSource(#[source] ErrorSource),
    #[error("connect to {remote_addr} failed: {cause:?}")]
    ConnectRequest {
        remote_addr: String,
        #[source]
        cause: Option<ErrorSource>,
    },
    #[error("request send failed: {0}")]
    Send(String),
    #[error("request send failed: {0}")]
    SendSource(#[source] ErrorSource),
    #[error("operation deadline expired")]
    Timeout,
    #[error("{phase} deadline expired for {remote_addr:?} after {timeout:?}")]
    TimeoutWithContext {
        phase: TimeoutPhase,
        remote_addr: Option<String>,
        timeout: Option<Duration>,
        #[source]
        cause: Option<ErrorSource>,
    },
    #[error("invalid client configuration: {0}")]
    InvalidConfiguration(String),
    #[error("request admission limit reached")]
    TooManyRequests,
    #[error("request send failed for {remote_addr}: {cause:?}")]
    SendRequest {
        remote_addr: String,
        #[source]
        cause: Option<ErrorSource>,
    },
    #[error("connection is closed")]
    Closed,
    #[error("request was interrupted because its channel was closed")]
    Interrupted,
    #[error("remoting callback failed: {0}")]
    Remoting(String),
    #[error("remoting callback failed for {request}: {cause}")]
    RemotingWithCause {
        request: String,
        #[source]
        cause: ErrorSource,
    },
    #[error("RPC hook failed: {0}")]
    RpcHook(String),
    #[error("TLS setup or handshake failed: {0}")]
    Tls(String),
    #[error("TLS setup or handshake failed: {0}")]
    TlsSource(#[source] ErrorSource),
    #[error("malformed frame: {0}")]
    MalformedFrame(CodecError),
    #[error("codec: {0}")]
    Codec(CodecError),
    #[error("client has not been started")]
    NotStarted,
    #[error("client has shut down")]
    Shutdown,
    #[error("opaque identifiers are exhausted")]
    OpaqueExhausted,
}

impl TransportError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout | Self::TimeoutWithContext { .. })
    }

    pub(crate) fn timeout(
        phase: TimeoutPhase,
        remote_addr: Option<String>,
        timeout: Option<Duration>,
    ) -> Self {
        Self::TimeoutWithContext {
            phase,
            remote_addr,
            timeout,
            cause: None,
        }
    }

    pub(crate) fn timeout_with_cause(
        phase: TimeoutPhase,
        remote_addr: Option<String>,
        timeout: Option<Duration>,
        cause: Option<ErrorSource>,
    ) -> Self {
        Self::TimeoutWithContext {
            phase,
            remote_addr,
            timeout,
            cause,
        }
    }

    pub(crate) fn connect_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::ConnectSource(ErrorSource::new(error))
    }

    pub(crate) fn tls_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::TlsSource(ErrorSource::new(error))
    }
}

impl From<CodecError> for TransportError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_source_preserves_typed_cause_and_context_chain() {
        let source = ErrorSource::with_context(
            "dial context",
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
        );
        assert_eq!(source.to_string(), "dial context: refused");
        assert_eq!(
            source
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::ConnectionRefused)
        );
    }

    #[test]
    fn error_source_equality_matches_java_throwable_identity() {
        let source = ErrorSource::message("same message");
        let cloned_source = source.clone();
        let distinct_source = ErrorSource::message("same message");

        assert_eq!(source, cloned_source);
        assert_ne!(source, distinct_source);

        let cloned_error = TransportError::SendRequest {
            remote_addr: "127.0.0.1:10911".into(),
            cause: Some(cloned_source),
        };
        let distinct_error = TransportError::SendRequest {
            remote_addr: "127.0.0.1:10911".into(),
            cause: Some(distinct_source),
        };
        assert_ne!(cloned_error, distinct_error);
    }

    #[test]
    fn go_away_cause_retains_address_and_retry_state_with_java_message_shape() {
        let cause = ErrorSource::new(GoAwayCause {
            remote_addr: "127.0.0.1:10911".into(),
            retried: true,
        });
        let go_away = cause.downcast_ref::<GoAwayCause>().unwrap();
        assert_eq!(go_away.remote_addr, "127.0.0.1:10911");
        assert!(go_away.retried);
        assert_eq!(
            cause.to_string(),
            "Receive GO_AWAY twice in request from channelId=127.0.0.1:10911"
        );

        let first_go_away = GoAwayCause {
            remote_addr: "127.0.0.1:10911".into(),
            retried: false,
        };
        assert_eq!(
            first_go_away.to_string(),
            "Receive GO_AWAY from channelId=127.0.0.1:10911"
        );
    }
}
