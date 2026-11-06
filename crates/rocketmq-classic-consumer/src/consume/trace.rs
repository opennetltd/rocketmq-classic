use std::sync::Arc;

use rocketmq_classic_common::message::{MessageExt, constants::*};
use rocketmq_classic_core::trace::{TraceBean, TraceContext, TraceType};
use rocketmq_classic_producer::TraceDispatcher;
use rocketmq_classic_protocol::NamespaceUtil;

use super::{ConsumeMessageContext, ConsumeMessageHook};

pub(crate) struct ConsumeTraceHook {
    dispatcher: Arc<TraceDispatcher>,
}

impl ConsumeTraceHook {
    pub(crate) fn new(dispatcher: Arc<TraceDispatcher>) -> Self {
        Self { dispatcher }
    }
}

impl ConsumeMessageHook for ConsumeTraceHook {
    fn hook_name(&self) -> &str {
        "ConsumeMessageTraceHook"
    }

    fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
        let Some(messages) = context.messages.as_deref() else {
            return;
        };
        if messages.is_empty() {
            return;
        }
        let mut before = TraceContext::new(TraceType::SubBefore);
        before.group_name = context.consumer_group.as_deref().map(without_namespace);
        let mut region_id = None;
        let mut trace_beans = Vec::with_capacity(messages.len());
        for message in messages {
            let properties = message.properties();
            if properties
                .get(PROPERTY_TRACE_SWITCH)
                .is_some_and(|trace_switch| trace_switch == "false")
            {
                continue;
            }
            region_id = properties.get(PROPERTY_MSG_REGION).cloned();
            trace_beans.push(trace_bean(message));
        }
        if !trace_beans.is_empty() {
            before.region_id = region_id;
            before.timestamp_millis = epoch_millis();
            before.trace_beans = Some(trace_beans);
            let _ = self.dispatcher.append(before.clone());
        }
        context.trace_context = Some(Box::new(before));
    }

    fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
        let Some(before) = context
            .trace_context
            .as_ref()
            .and_then(|value| value.downcast_ref::<TraceContext>())
        else {
            return;
        };
        if let Some(after) = consume_after_trace_context(before, context) {
            let _ = self.dispatcher.append(after);
        }
    }
}

fn consume_after_trace_context(
    before: &TraceContext,
    context: &ConsumeMessageContext,
) -> Option<TraceContext> {
    let before_beans = before.trace_beans.as_ref()?;
    if before_beans.is_empty() {
        return None;
    }
    let messages = context
        .messages
        .as_ref()
        .filter(|messages| !messages.is_empty())?;
    let denominator = i64::try_from(messages.len()).unwrap_or(i64::MAX).max(1);
    let cost = epoch_millis()
        .saturating_sub(before.timestamp_millis)
        .checked_div(denominator)
        .unwrap_or_default();
    Some(TraceContext {
        trace_type: Some(TraceType::SubAfter),
        timestamp_millis: epoch_millis(),
        region_id: before.region_id.clone(),
        region_name: before.region_name.clone(),
        group_name: before
            .group_name
            .clone()
            .map(|group| without_namespace(&group)),
        cost_time_millis: i32::try_from(cost).unwrap_or(i32::MAX),
        success: context.success,
        request_id: before.request_id.clone(),
        context_code: context
            .properties
            .as_ref()
            .and_then(|properties| properties.get("CONSUME_CONTEXT_TYPE"))
            .and_then(|value| consume_return_type_ordinal(value))
            .unwrap_or_default(),
        access_channel: context.access_channel,
        trace_beans: Some(before_beans.clone()),
    })
}

fn trace_bean(message: &MessageExt) -> TraceBean {
    let base_message = &**message;
    TraceBean {
        topic: Some(without_namespace(base_message.topic())),
        message_id: message.message_id().map(str::to_owned),
        offset_message_id: None,
        tags: base_message.tags(),
        keys: base_message.keys(),
        body_length: message.store_size(),
        store_time_millis: message.store_timestamp(),
        retry_times: message.reconsume_times(),
        message_type: None,
        transaction_id: None,
        transaction_state: None,
        from_transaction_check: false,
        ..TraceBean::default()
    }
}

fn consume_return_type_ordinal(value: &str) -> Option<i32> {
    match value {
        "SUCCESS" => Some(0),
        "TIME_OUT" => Some(1),
        "EXCEPTION" => Some(2),
        "RETURNNULL" => Some(3),
        "FAILED" => Some(4),
        _ => None,
    }
}

fn without_namespace(value: &str) -> String {
    NamespaceUtil::without_namespace(Some(value)).unwrap_or_default()
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_classic_common::message::{Message, MessageQueue};
    use rocketmq_classic_core::config::ClientConfig;
    use rocketmq_classic_core::runtime::ClientRuntimeManager;
    use rocketmq_classic_producer::TraceProducerKind;

    use super::*;

    #[test]
    fn consume_trace_hook_builds_java_sub_before_context_and_skips_trace_off() {
        let mut client_config = ClientConfig::default();
        client_config.set_namespace(Some("tenant".into()));
        let dispatcher = Arc::new(
            TraceDispatcher::new(
                &client_config,
                "workers",
                TraceProducerKind::Consume,
                None,
                ClientRuntimeManager::new(),
                None,
            )
            .unwrap(),
        );
        let hook = ConsumeTraceHook::new(dispatcher);
        let mut message =
            crate::test_support::message_ext_from(Message::new("other%orders", b"body".to_vec()));
        message.set_message_id(Some("message-1".into()));
        message.set_store_size(4);
        message.set_store_timestamp(1_700);
        message.set_tags("paid").unwrap();
        message.set_keys("key-1 key-2").unwrap();
        message
            .set_system_property(PROPERTY_MSG_REGION, "region-a")
            .unwrap();
        let mut context = ConsumeMessageContext::new(
            Some("tenant%tenant%workers".to_owned()),
            Some(vec![message]),
            MessageQueue::new("other%orders", "broker-a", 0),
            Some("tenant".into()),
        );

        hook.consume_message_before(&mut context);
        let before = context
            .trace_context
            .as_ref()
            .and_then(|value| value.downcast_ref::<TraceContext>())
            .unwrap();
        assert_eq!(before.trace_type, Some(TraceType::SubBefore));
        assert!(
            before
                .request_id
                .as_deref()
                .is_some_and(|id| !id.is_empty())
        );
        assert_eq!(before.group_name.as_deref(), Some("tenant%workers"));
        assert_eq!(before.region_id.as_deref(), Some("region-a"));
        assert_eq!(
            before.trace_beans.as_ref().unwrap()[0].topic.as_deref(),
            Some("orders")
        );
        assert_eq!(
            before.trace_beans.as_ref().unwrap()[0]
                .message_id
                .as_deref(),
            Some("message-1")
        );
        assert_eq!(
            before.trace_beans.as_ref().unwrap()[0].keys.as_deref(),
            Some("key-1 key-2")
        );
        assert_eq!(
            before.trace_beans.as_ref().unwrap()[0].store_time_millis,
            1_700
        );
        let bean = &before.trace_beans.as_ref().unwrap()[0];
        assert_eq!(bean.store_host, bean.client_host);
        assert!(bean.store_host.as_deref().is_some_and(|host| {
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok()
        }));
        let before_snapshot = before.clone();
        context.consumer_group = Some("changed-between-callbacks".to_owned());
        context.success = true;
        let after = consume_after_trace_context(&before_snapshot, &context)
            .expect("valid before context creates a SubAfter trace");
        assert_eq!(after.trace_type, Some(TraceType::SubAfter));
        assert_eq!(after.group_name.as_deref(), Some("workers"));
        assert_eq!(
            after.trace_beans.as_ref().unwrap()[0].topic.as_deref(),
            Some("orders")
        );
        assert!(after.success);

        let mut trace_off =
            crate::test_support::message_ext_from(Message::new("orders", b"body".to_vec()));
        trace_off
            .set_system_property(PROPERTY_TRACE_SWITCH, "false")
            .unwrap();
        context.messages = Some(vec![trace_off]);
        context.trace_context = None;
        hook.consume_message_before(&mut context);
        let trace_off_context = context
            .trace_context
            .as_ref()
            .and_then(|value| value.downcast_ref::<TraceContext>())
            .expect("Java retains the empty before-trace context for trace-off messages");
        assert_eq!(trace_off_context.trace_type, Some(TraceType::SubBefore));
        assert_eq!(trace_off_context.region_id.as_deref(), Some(""));
        assert!(trace_off_context.trace_beans.is_none());

        let live_group_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        context.consumer_group = Some("phase18-consume-retry-live".to_owned());
        context.trace_context = None;
        hook.consume_message_before(&mut context);
        let live_group_context = context
            .trace_context
            .as_ref()
            .and_then(|value| value.downcast_ref::<TraceContext>())
            .expect("trace hook creates a before context for live group");
        assert_eq!(
            live_group_fixture["consumeTraceHookGroup"].as_str(),
            live_group_context.group_name.as_deref()
        );
    }
}
