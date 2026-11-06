//! Shared RocketMQ message and utility types.
//!
//! JSON compatibility adapters are implementation details of the common and
//! protocol crates, not public helpers for downstream users.
//!
//! ```compile_fail
//! use rocketmq_classic_common::serde_fastjson::from_fastjson_str;
//! # fn main() {}
//! ```
//!
//! Bit masks use Java's `MessageSysFlag.check(flag, expectedFlag)` counterpart.
//!
//! ```compile_fail
//! use rocketmq_classic_common::sysflag::message::has_compressed_flag;
//! # fn main() {}
//! ```
//!
//! `PullSysFlag` mask constants are private, as in Java; callers use its helpers.
//!
//! ```compile_fail
//! use rocketmq_classic_common::sysflag::pull::FLAG_COMMIT_OFFSET;
//! # fn main() {}
//! ```

pub mod compression;
pub mod message;
pub mod permission;
mod serde_fastjson;
pub mod sysflag;
pub mod topic;
pub mod utils;

pub use permission::{PermName, PermissionParseError};
