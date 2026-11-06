use std::{
    any::Any,
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{Arc, RwLock},
};

use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_core::config::AccessChannel;

/// Context passed to Java-compatible consumer hooks.
///
/// The same context is supplied to both callbacks, so values placed in
/// `properties` or `trace_context` during `consume_message_before` remain
/// available in `consume_message_after`.
pub struct ConsumeMessageContext {
    pub consumer_group: Option<String>,
    pub messages: Option<Vec<MessageExt>>,
    pub message_queue: MessageQueue,
    pub success: bool,
    pub status: Option<String>,
    pub trace_context: Option<Box<dyn Any + Send + Sync>>,
    /// Java initializes this map only for Push consume callbacks. Pull and Lite
    /// contexts leave it absent unless a hook installs one.
    pub properties: Option<HashMap<String, String>>,
    pub namespace: Option<String>,
    pub access_channel: Option<AccessChannel>,
}

impl ConsumeMessageContext {
    pub fn new(
        consumer_group: Option<String>,
        messages: Option<Vec<MessageExt>>,
        message_queue: MessageQueue,
        namespace: Option<String>,
    ) -> Self {
        Self {
            consumer_group,
            messages,
            message_queue,
            success: false,
            status: None,
            trace_context: None,
            properties: None,
            namespace,
            // Java sets accessChannel after consumeMessageBefore and before
            // consumeMessageAfter.
            access_channel: None,
        }
    }

    pub(crate) fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = Some(properties);
        self
    }
}

/// Java `ConsumeMessageHook` adapted to synchronous Rust callback boundaries.
pub trait ConsumeMessageHook: Send + Sync + 'static {
    fn hook_name(&self) -> &str;

    fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {}

    fn consume_message_after(&self, _context: &mut ConsumeMessageContext) {}
}

#[derive(Default)]
pub(crate) struct ConsumeMessageHookRegistry {
    hooks: RwLock<Vec<Arc<dyn ConsumeMessageHook>>>,
}

impl ConsumeMessageHookRegistry {
    pub(crate) fn register(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.hooks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(hook);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    pub(crate) fn execute_before(&self, context: &mut ConsumeMessageContext) {
        self.execute(context, true);
    }

    pub(crate) fn execute_after(&self, context: &mut ConsumeMessageContext) {
        self.execute(context, false);
    }

    fn execute(&self, context: &mut ConsumeMessageContext, before: bool) {
        let hooks = self
            .hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for hook in hooks {
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                if before {
                    hook.consume_message_before(context);
                } else {
                    hook.consume_message_after(context);
                }
            }));
            if result.is_err() {
                tracing::warn!("consumer hook panicked; continuing with remaining hooks");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_panics_do_not_skip_later_java_style_callbacks() {
        struct PanickingHook;

        impl ConsumeMessageHook for PanickingHook {
            fn hook_name(&self) -> &str {
                "panicking-consume-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                panic!("before hook failure");
            }

            fn consume_message_after(&self, _context: &mut ConsumeMessageContext) {
                panic!("after hook failure");
            }
        }

        struct RecordingHook(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl ConsumeMessageHook for RecordingHook {
            fn hook_name(&self) -> &str {
                "recording-consume-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before");
            }

            fn consume_message_after(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("after");
            }
        }

        let registry = ConsumeMessageHookRegistry::default();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        registry.register(Arc::new(PanickingHook));
        registry.register(Arc::new(RecordingHook(calls.clone())));
        let mut context = ConsumeMessageContext::new(
            Some("group".to_owned()),
            None,
            MessageQueue::new("topic", "broker", 0),
            None,
        );

        registry.execute_before(&mut context);
        registry.execute_after(&mut context);

        assert_eq!(calls.lock().unwrap().as_slice(), ["before", "after"]);
    }
}
