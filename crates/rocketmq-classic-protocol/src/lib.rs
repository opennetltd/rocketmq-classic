//! Typed RocketMQ protocol models and codecs.
//!
//! The Fastjson coercion layer used by the DTO implementations is crate-private.
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::serde_fastjson;
//! # fn main() {}
//! ```
//!
//! Header field parsers and synthetic proof headers are implementation/test
//! helpers, not part of the public protocol API.
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::header::{
//!     java_boolean, optional_i32, optional_i64, optional_java_boolean,
//!     optional_string, required_i32, required_i64, required_string, ProofHeader,
//! };
//! # fn main() {}
//! ```

pub mod body;
pub mod codes;
pub mod header;
pub mod heartbeat;
pub mod namespace;
pub mod route;
pub(crate) mod serde_fastjson;
pub mod shared;
pub mod subscription;
pub mod writer_feature;

pub use namespace::NamespaceUtil;
pub use shared::SharedValue;
pub use writer_feature::JavaJsonWriterFeature;

/// Decodes one Java `RemotingSerializable.decode(byte[], Class)` body with
/// Fastjson input semantics, including EOF markers and permissive JSON syntax.
pub fn decode_java_remoting_serializable<T: serde::de::DeserializeOwned>(
    input: &[u8],
) -> Result<T, serde_json::Error> {
    serde_fastjson::from_fastjson_slice(input)
}

/// Decodes a nullable Java `RemotingSerializable.decode` result, preserving a
/// JSON `null` root as `None`.
pub fn decode_java_remoting_serializable_nullable<T: serde::de::DeserializeOwned>(
    input: &[u8],
) -> Result<Option<T>, serde_json::Error> {
    serde_fastjson::from_fastjson_slice_nullable(Some(input))
}
