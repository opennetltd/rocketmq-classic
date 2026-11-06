use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use rocketmq_classic_common::message::{Message, MessageQueue};
use rocketmq_classic_core::api::producer::{SendMode, SendResult};
use rocketmq_classic_protocol::codes::JavaEnumLookupError;

use crate::{LocalTransactionState, Producer, ProducerError};

/// Borrowed error object exposed to a synchronous send hook, preserving its
/// concrete Rust error type for inspection or downcasting.
pub type HookException<'a> = &'a (dyn std::error::Error + Send + Sync + 'static);

pub use rocketmq_classic_core::trace::MessageType as SendMessageType;

/// Producer communication mode matching Java's `CommunicationMode` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommunicationMode {
    Sync,
    Async,
    Oneway,
}

impl CommunicationMode {
    pub const fn values() -> [Self; 3] {
        [Self::Sync, Self::Async, Self::Oneway]
    }

    pub const fn java_name(self) -> &'static str {
        match self {
            Self::Sync => "SYNC",
            Self::Async => "ASYNC",
            Self::Oneway => "ONEWAY",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Sync => 0,
            Self::Async => 1,
            Self::Oneway => 2,
        }
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        Self::values()
            .into_iter()
            .find(|mode| Some(mode.java_name()) == name)
            .ok_or_else(|| match name {
                None => JavaEnumLookupError::NullName,
                Some(name) => JavaEnumLookupError::UnknownName {
                    enum_class: "org.apache.rocketmq.client.impl.CommunicationMode",
                    name: name.to_owned(),
                },
            })
    }
}

impl From<SendMode> for CommunicationMode {
    fn from(mode: SendMode) -> Self {
        match mode {
            SendMode::Sync => Self::Sync,
            SendMode::Future => Self::Async,
            SendMode::Oneway => Self::Oneway,
        }
    }
}

/// Mutable context passed to producer hooks before and after each send attempt.
pub struct SendMessageContext<'a> {
    pub producer_group: Option<String>,
    pub producer: &'a Producer,
    pub message: &'a mut Message,
    pub queue: MessageQueue,
    pub broker_addr: String,
    pub born_host: Option<String>,
    pub communication_mode: CommunicationMode,
    pub send_result: Option<SendResult>,
    pub exception: Option<HookException<'a>>,
    pub message_type: SendMessageType,
    pub namespace: Option<String>,
    pub trace_context: Option<Box<dyn Any + Send + Sync>>,
    pub properties: Option<HashMap<String, String>>,
}

/// Mutable pre-send context corresponding to Java `CheckForbiddenContext`.
pub struct CheckForbiddenContext<'a> {
    pub name_srv_addr: Option<String>,
    pub group: Option<String>,
    pub message: &'a mut Message,
    pub queue: &'a mut MessageQueue,
    pub broker_addr: String,
    pub communication_mode: CommunicationMode,
    pub send_result: Option<SendResult>,
    pub exception: Option<HookException<'a>>,
    pub arg: Option<Box<dyn Any + Send + Sync>>,
    pub unit_mode: bool,
}

/// Rejects or adjusts a producer request before send-message hooks and RPC encoding.
pub trait CheckForbiddenHook: Send + Sync + 'static {
    fn hook_name(&self) -> &str;

    fn check_forbidden(
        &self,
        _context: &mut CheckForbiddenContext<'_>,
    ) -> Result<(), ProducerError> {
        Ok(())
    }
}

pub(crate) type CheckForbiddenHookList = RwLock<Vec<Arc<dyn CheckForbiddenHook>>>;

/// Java `SendMessageHook` callbacks adapted to the Rust send operation lifecycle.
/// Future-mode sends call after once when the async request is submitted and
/// again when its final retry/result is known, matching Java's two callback sites.
pub trait SendMessageHook: Send + Sync + 'static {
    fn hook_name(&self) -> &str;

    fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {}

    fn send_message_after(&self, _context: &mut SendMessageContext<'_>) {}
}

pub(crate) type SendMessageHookList = RwLock<Vec<Arc<dyn SendMessageHook>>>;

/// Transaction completion context corresponding to Java's
/// `EndTransactionContext`. Hooks run immediately before the end-transaction
/// request is sent. The message is mutable, while the already prepared RPC
/// header retains the values captured before hook execution, as in Java.
pub struct EndTransactionContext<'a> {
    pub producer_group: Option<String>,
    pub message: &'a mut Message,
    pub broker_addr: String,
    pub message_id: String,
    pub transaction_id: Option<String>,
    pub transaction_state: LocalTransactionState,
    pub from_transaction_check: bool,
}

/// Observes a local transaction result or a Broker transaction check before
/// the client sends its end-transaction request.
pub trait EndTransactionHook: Send + Sync + 'static {
    fn hook_name(&self) -> &str;

    fn end_transaction(&self, _context: &mut EndTransactionContext<'_>) {}
}

pub(crate) type EndTransactionHookList = RwLock<Vec<Arc<dyn EndTransactionHook>>>;

#[cfg(test)]
mod tests {
    use super::{CommunicationMode, SendMessageType};
    use rocketmq_classic_protocol::codes::JavaEnumLookupError;

    #[test]
    fn message_types_match_java_names_ordinals_and_short_name_fallback() {
        let values = SendMessageType::values();
        assert_eq!(
            values.map(SendMessageType::java_name),
            [
                "Normal_Msg",
                "Trans_Msg_Half",
                "Trans_msg_Commit",
                "Delay_Msg",
                "Order_Msg"
            ]
        );
        assert_eq!(
            values.map(SendMessageType::short_name),
            ["Normal", "Trans", "TransCommit", "Delay", "Order"]
        );
        assert_eq!(values.map(SendMessageType::ordinal), [0, 1, 2, 3, 4]);
        assert_eq!(
            SendMessageType::get_by_short_name(Some("TransCommit")),
            SendMessageType::TransactionCommit
        );
        assert_eq!(
            SendMessageType::get_by_short_name(Some("unknown")),
            SendMessageType::Normal
        );
        assert_eq!(
            SendMessageType::value_of_name(Some("Order_Msg")),
            Ok(SendMessageType::Order)
        );
        assert_eq!(
            SendMessageType::value_of_name(None),
            Err(JavaEnumLookupError::NullName)
        );
    }

    #[test]
    fn communication_modes_match_java_order_names_ordinals_and_lookup() {
        let modes = CommunicationMode::values();
        assert_eq!(
            modes.map(CommunicationMode::java_name),
            ["SYNC", "ASYNC", "ONEWAY"]
        );
        assert_eq!(modes.map(CommunicationMode::ordinal), [0, 1, 2]);
        assert_eq!(
            CommunicationMode::value_of_name(Some("ASYNC")),
            Ok(CommunicationMode::Async)
        );
        assert!(CommunicationMode::value_of_name(None).is_err());
        assert!(CommunicationMode::value_of_name(Some("async")).is_err());
    }
}
