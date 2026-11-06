use rocketmq_classic_common::permission::PermissionParseError;
use rocketmq_classic_protocol::header::ProtocolError;
use rocketmq_classic_remoting::TransportError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("transport failure: {0}")]
    Transport(#[from] TransportError),
    #[error("invalid RPC header: {0}")]
    Header(#[from] ProtocolError),
    #[error("unsupported RPC feature: {0}")]
    Unsupported(&'static str),
    #[error("failed to serialize RPC request {request_code}: {source}")]
    RequestSerialization {
        request_code: i32,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid response for request {request_code} from {peer}: {reason}")]
    MalformedResponse {
        request_code: i32,
        peer: String,
        reason: String,
    },
    #[error("request {request_code} rejected by {peer} with code {code}: {remark:?}")]
    Response {
        request_code: i32,
        peer: String,
        code: i32,
        remark: Option<String>,
    },
    #[error("consumer offset is not found at {peer}: {remark:?}")]
    OffsetNotFound {
        request_code: i32,
        peer: String,
        code: i32,
        remark: Option<String>,
    },
    #[error(
        "CODE: {response_code}  DESC: {message}\nFor more information, please visit the url, https://rocketmq.apache.org/docs/faq/"
    )]
    ClientValidation {
        request_code: i32,
        response_code: i32,
        message: String,
    },
    #[error(transparent)]
    PermissionParse(#[from] PermissionParseError),
    #[error("runtime failure: {0}")]
    Runtime(#[source] Box<crate::runtime::RuntimeError>),
}

impl From<crate::runtime::RuntimeError> for RpcError {
    fn from(source: crate::runtime::RuntimeError) -> Self {
        Self::Runtime(Box::new(source))
    }
}
