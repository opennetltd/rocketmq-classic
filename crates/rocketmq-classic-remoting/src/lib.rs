//! RocketMQ remoting client and wire codecs.
//!
//! The frame helpers mirror the pinned Java client's command codec while the
//! configured inbound frame limit remains a transport setting.
//!
//! The old caller-selected frame bound is not part of the public codec API:
//!
//! ```compile_fail
//! use rocketmq_classic_remoting::{codec::frame, RemotingCommand};
//! let command = RemotingCommand::default();
//! let _ = frame::encode(&command, 4096);
//! ```
//!
//! ```compile_fail
//! use bytes::BytesMut;
//! use rocketmq_classic_remoting::codec::frame;
//! let mut input = BytesMut::new();
//! let _ = frame::decode_next(&mut input, 4096);
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_remoting::{codec::json_header, RemotingCommand};
//! let command = RemotingCommand::default();
//! let _ = json_header::encode(&command, 4096);
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_remoting::{codec::rocketmq_header, RemotingCommand};
//! let command = RemotingCommand::default();
//! let _ = rocketmq_header::encode(&command, 4096);
//! ```
//!
//! Java's decoder default is an internal transport detail, not a public
//! constant:
//!
//! ```compile_fail
//! use rocketmq_classic_remoting::codec::frame::DEFAULT_MAX_FRAME_SIZE;
//! let _ = DEFAULT_MAX_FRAME_SIZE;
//! ```

pub mod client;
pub mod codec;
pub mod command;
pub mod error;
pub mod ext_fields;
pub mod metrics;
pub mod remoting_helper;
pub mod route;

pub use command::{
    CommandCallback, ProcessTimer, ProcessTimerError, RemotingCommand, RemotingCommandType,
    RequestSource, RequestType, SharedCommandHeader,
};
pub use error::{CodecError, ErrorSource, GoAwayCause, TimeoutPhase, TransportError};
pub use ext_fields::ExtFields;
pub use route::MessageQueueRouteState;
