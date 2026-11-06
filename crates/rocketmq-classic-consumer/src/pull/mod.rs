//! Pull result processing and subscription support.
//!
//! Class-filter subscription data is created through the Push consumer API;
//! the builder is an internal implementation detail.
//!
//! ```compile_fail
//! use rocketmq_classic_consumer::pull::build_class_filter_subscription_data;
//! ```

mod consumer;
mod result;

pub(crate) use consumer::build_subscription;
pub use consumer::{MessageSelector, PullConsumer};
pub(crate) use result::build_class_filter_subscription_data;
pub(crate) use result::resolve_pull_broker_address;
pub use result::{
    FilterMessageContext, FilterMessageHook, ProcessPullError, ProcessedPullResult, PullAPIWrapper,
    SharedFilterMessageHooks,
};
pub use rocketmq_classic_protocol::header::BoundaryType;
