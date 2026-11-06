//! RocketMQ response codes from Java's `RemotingSysResponseCode` and
//! `ResponseCode`. `ResponseCode` inherits the base values, so the shared
//! Java field names are exposed once here.
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::codes::response::REMOTING_SUCCESS;
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::codes::response::REMOTING_SYSTEM_ERROR;
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::codes::response::REMOTING_SYSTEM_BUSY;
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::codes::response::REMOTING_REQUEST_CODE_NOT_SUPPORTED;
//! # fn main() {}
//! ```
//!
//! ```compile_fail
//! use rocketmq_classic_protocol::codes::response::REMOTING_TRANSACTION_FAILED;
//! # fn main() {}
//! ```

pub const SUCCESS: i32 = 0;
pub const SYSTEM_ERROR: i32 = 1;
pub const SYSTEM_BUSY: i32 = 2;
pub const REQUEST_CODE_NOT_SUPPORTED: i32 = 3;
pub const TRANSACTION_FAILED: i32 = 4;
pub const FLUSH_DISK_TIMEOUT: i32 = 10;
pub const SLAVE_NOT_AVAILABLE: i32 = 11;
pub const FLUSH_SLAVE_TIMEOUT: i32 = 12;
pub const MESSAGE_ILLEGAL: i32 = 13;
pub const SERVICE_NOT_AVAILABLE: i32 = 14;
pub const VERSION_NOT_SUPPORTED: i32 = 15;
pub const NO_PERMISSION: i32 = 16;
pub const TOPIC_NOT_EXIST: i32 = 17;
pub const TOPIC_EXIST_ALREADY: i32 = 18;
pub const PULL_NOT_FOUND: i32 = 19;
pub const PULL_RETRY_IMMEDIATELY: i32 = 20;
pub const PULL_OFFSET_MOVED: i32 = 21;
pub const QUERY_NOT_FOUND: i32 = 22;
pub const SUBSCRIPTION_PARSE_FAILED: i32 = 23;
pub const SUBSCRIPTION_NOT_EXIST: i32 = 24;
pub const SUBSCRIPTION_NOT_LATEST: i32 = 25;
pub const SUBSCRIPTION_GROUP_NOT_EXIST: i32 = 26;
pub const FILTER_DATA_NOT_EXIST: i32 = 27;
pub const FILTER_DATA_NOT_LATEST: i32 = 28;
pub const INVALID_PARAMETER: i32 = 29;
pub const TRANSACTION_SHOULD_COMMIT: i32 = 200;
pub const TRANSACTION_SHOULD_ROLLBACK: i32 = 201;
pub const TRANSACTION_STATE_UNKNOW: i32 = 202;
pub const TRANSACTION_STATE_GROUP_WRONG: i32 = 203;
pub const NO_BUYER_ID: i32 = 204;
pub const NOT_IN_CURRENT_UNIT: i32 = 205;
pub const CONSUMER_NOT_ONLINE: i32 = 206;
pub const CONSUME_MSG_TIMEOUT: i32 = 207;
pub const NO_MESSAGE: i32 = 208;
pub const POLLING_FULL: i32 = 209;
pub const POLLING_TIMEOUT: i32 = 210;
pub const BROKER_NOT_EXIST: i32 = 211;
pub const BROKER_DISPATCH_NOT_COMPLETE: i32 = 212;
pub const BROADCAST_CONSUMPTION: i32 = 213;
pub const FLOW_CONTROL: i32 = 215;
pub const NOT_LEADER_FOR_QUEUE: i32 = 501;
pub const ILLEGAL_OPERATION: i32 = 604;
pub const RPC_UNKNOWN: i32 = -1000;
pub const RPC_ADDR_IS_NULL: i32 = -1002;
pub const RPC_SEND_TO_CHANNEL_FAILED: i32 = -1004;
pub const RPC_TIME_OUT: i32 = -1006;
pub const GO_AWAY: i32 = 1500;
pub const CONTROLLER_FENCED_MASTER_EPOCH: i32 = 2000;
pub const CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH: i32 = 2001;
pub const CONTROLLER_INVALID_MASTER: i32 = 2002;
pub const CONTROLLER_INVALID_REPLICAS: i32 = 2003;
pub const CONTROLLER_MASTER_NOT_AVAILABLE: i32 = 2004;
pub const CONTROLLER_INVALID_REQUEST: i32 = 2005;
pub const CONTROLLER_BROKER_NOT_ALIVE: i32 = 2006;
pub const CONTROLLER_NOT_LEADER: i32 = 2007;
pub const CONTROLLER_BROKER_METADATA_NOT_EXIST: i32 = 2008;
pub const CONTROLLER_INVALID_CLEAN_BROKER_METADATA: i32 = 2009;
pub const CONTROLLER_BROKER_NEED_TO_BE_REGISTERED: i32 = 2010;
pub const CONTROLLER_MASTER_STILL_EXIST: i32 = 2011;
pub const CONTROLLER_ELECT_MASTER_FAILED: i32 = 2012;
pub const CONTROLLER_ALTER_SYNC_STATE_SET_FAILED: i32 = 2013;
pub const CONTROLLER_BROKER_ID_INVALID: i32 = 2014;
pub const CONTROLLER_JRAFT_INTERNAL_ERROR: i32 = 2015;
pub const CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS: i32 = 2016;
pub const LMQ_QUOTA_EXCEEDED: i32 = 2017;
pub const LITE_SUBSCRIPTION_QUOTA_EXCEEDED: i32 = 2018;
pub const USER_NOT_EXIST: i32 = 3001;
pub const POLICY_NOT_EXIST: i32 = 3002;
