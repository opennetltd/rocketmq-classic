use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    sync::{Arc, Mutex, RwLock},
};

use rand::Rng;
use rocketmq_classic_common::{
    message::{
        DecodeError, DecodeOptions, MessageExt, MessageQueue, decode_many_as_client_compatible,
        decode_properties,
    },
    sysflag::message::{INNER_BATCH_FLAG, NEED_UNWRAP_FLAG},
};
use rocketmq_classic_core::api::consumer::{PullResultExt, PullStatus};
use rocketmq_classic_protocol::{heartbeat::SubscriptionData, route::TopicRouteData};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProcessPullError {
    #[error("FOUND pull result has no message binary")]
    MissingMessageBinary,
    #[error(transparent)]
    MessageDecode(#[from] DecodeError),
    #[error("inner batch is malformed at byte {offset}: {reason}")]
    MalformedInnerBatch { offset: usize, reason: &'static str },
    #[error("inner batch decoded size exceeds safety limit {limit}")]
    InnerBatchLimitExceeded { limit: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessedPullResult {
    pub result: PullResultExt,
    /// Java keeps this unset for every status except FOUND.
    pub messages: Option<Vec<MessageExt>>,
}

pub struct FilterMessageContext {
    pub unit_mode: bool,
    pub messages: Vec<MessageExt>,
}

pub trait FilterMessageHook: Send + Sync {
    fn hook_name(&self) -> &str;
    fn filter_message(&self, context: &mut FilterMessageContext);
}

pub type SharedFilterMessageHooks = Arc<RwLock<Vec<Arc<dyn FilterMessageHook>>>>;

/// Pull-result processing state shared by pull consumers.
#[derive(Default)]
pub struct PullAPIWrapper {
    suggested_broker_ids: Mutex<HashMap<MessageQueue, i64>>,
    hooks: RwLock<SharedFilterMessageHooks>,
    unit_mode: AtomicBool,
    connect_broker_by_user: AtomicBool,
    default_broker_id: AtomicI64,
}

impl PullAPIWrapper {
    pub fn new(unit_mode: bool) -> Self {
        Self {
            suggested_broker_ids: Mutex::new(HashMap::new()),
            hooks: RwLock::new(Arc::new(RwLock::new(Vec::new()))),
            unit_mode: AtomicBool::new(unit_mode),
            connect_broker_by_user: AtomicBool::new(false),
            default_broker_id: AtomicI64::new(0),
        }
    }

    pub(crate) fn set_unit_mode(&self, unit_mode: bool) {
        self.unit_mode.store(unit_mode, Ordering::Release);
    }

    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        self.hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(hook);
    }

    /// Replaces the wrapper's live hook list. Sharing this handle matches Java's
    /// `registerFilterMessageHook(ArrayList)` alias behavior.
    pub fn set_filter_message_hooks(&self, hooks: SharedFilterMessageHooks) {
        *self
            .hooks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = hooks;
    }

    pub fn has_hook(&self) -> bool {
        !self
            .hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }

    pub fn execute_hook(&self, context: &mut FilterMessageContext) {
        let hooks = self
            .hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for hook in hooks {
            let _ = catch_unwind(AssertUnwindSafe(|| hook.filter_message(context)));
        }
    }

    pub fn update_pull_from_which_node(&self, queue: &MessageQueue, broker_id: i64) {
        self.suggested_broker_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(queue.clone(), broker_id);
    }

    pub fn is_connect_broker_by_user(&self) -> bool {
        self.connect_broker_by_user.load(Ordering::Acquire)
    }

    pub fn default_broker_id(&self) -> i64 {
        self.default_broker_id.load(Ordering::Acquire)
    }

    /// Returns a nonnegative random value, matching Java's `randomNum()` rule
    /// for `Integer.MIN_VALUE` as well as ordinary negative values.
    pub fn random_num(&self) -> i32 {
        let value = rand::rng().random::<i32>();
        if value == i32::MIN { 0 } else { value.abs() }
    }

    /// Returns the Broker ID most recently suggested for this queue, defaulting
    /// to the master before the first pull response.
    pub fn recalculate_pull_from_which_node(&self, queue: &MessageQueue) -> i64 {
        if self.connect_broker_by_user.load(Ordering::Acquire) {
            return self.default_broker_id.load(Ordering::Acquire);
        }
        self.suggested_broker_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(queue)
            .copied()
            .unwrap_or_else(|| self.default_broker_id.load(Ordering::Acquire))
    }

    pub fn set_connect_broker_by_user(&self, connect: bool) {
        self.connect_broker_by_user
            .store(connect, Ordering::Release);
    }

    pub fn set_default_broker_id(&self, broker_id: i64) {
        self.default_broker_id.store(broker_id, Ordering::Release);
    }

    pub fn process_pull_result(
        &self,
        queue: &MessageQueue,
        mut result: PullResultExt,
        subscription: &SubscriptionData,
        options: DecodeOptions,
    ) -> Result<ProcessedPullResult, ProcessPullError> {
        self.suggested_broker_ids
            .lock()
            .unwrap()
            .insert(queue.clone(), result.suggest_which_broker_id);

        let messages = if result.status == PullStatus::Found {
            let raw = result
                .raw_messages
                .take()
                .ok_or(ProcessPullError::MissingMessageBinary)?;
            let mut messages = decode_many_as_client_compatible(&raw, options);
            if messages.iter().any(|message| {
                message.sys_flag() & INNER_BATCH_FLAG != 0
                    && message.sys_flag() & NEED_UNWRAP_FLAG != 0
            }) {
                if let Ok(unwrapped) = unwrap_messages(&messages, options) {
                    messages = unwrapped;
                }
            }

            attach_client_message_ids(&mut messages);
            filter_tags(&mut messages, subscription);
            let mut context = FilterMessageContext {
                unit_mode: self.unit_mode.load(Ordering::Acquire),
                messages,
            };
            if self.has_hook() {
                self.execute_hook(&mut context);
            }
            enrich_messages(&mut context.messages, queue, &result);
            Some(context.messages)
        } else {
            None
        };

        // Match PullAPIWrapper: the raw broker buffer is released for every status.
        result.raw_messages = None;
        Ok(ProcessedPullResult { result, messages })
    }
}

pub(crate) fn build_class_filter_subscription_data(
    topic: impl Into<String>,
    class_name: impl Into<String>,
    filter_class_source: impl Into<String>,
) -> SubscriptionData {
    let mut subscription = SubscriptionData::new(Some(topic.into()), Some("*".to_owned()));
    subscription.set_sub_string(Some(class_name.into()));
    subscription.set_class_filter_mode(true);
    subscription.set_filter_class_source(Some(filter_class_source.into()));
    subscription
}

#[derive(Debug, Error, PartialEq, Eq)]
#[allow(dead_code)] // Used by the pull request path added in the following consumer phase.
pub(crate) enum FilterServerError {
    #[error("Find Filter Server Failed, Broker Addr: {broker_address} topic: {topic}")]
    NotFound {
        broker_address: String,
        topic: String,
    },
}

/// Choose a filter server using the broker address keys carried by route data.
#[allow(dead_code)] // Kept crate-internal for the pull request path added in the following consumer phase.
pub(crate) fn select_filter_server(
    route: &TopicRouteData,
    topic: &str,
    broker_address: &str,
) -> Result<Option<String>, FilterServerError> {
    let not_found = || FilterServerError::NotFound {
        broker_address: broker_address.to_owned(),
        topic: topic.to_owned(),
    };
    let table = route.filter_server_table_snapshot().ok_or_else(not_found)?;
    let list = table
        .get(broker_address)
        .and_then(Option::as_ref)
        .ok_or_else(not_found)?;
    if list.is_empty() {
        return Err(not_found());
    }
    let index = rand::rng().random_range(0..list.len());
    Ok(list[index].clone())
}

/// Apply class-filter routing only when the Java pull class-filter bit is set.
#[allow(dead_code)] // Kept crate-internal for the pull request path added in the following consumer phase.
pub(crate) fn resolve_pull_broker_address(
    route: &TopicRouteData,
    topic: &str,
    broker_address: &str,
    pull_sys_flag: i32,
) -> Result<Option<String>, FilterServerError> {
    if rocketmq_classic_common::sysflag::pull::has_class_filter_flag(pull_sys_flag) {
        select_filter_server(route, topic, broker_address)
    } else {
        Ok(Some(broker_address.to_owned()))
    }
}

fn filter_tags(messages: &mut Vec<MessageExt>, subscription: &SubscriptionData) {
    if subscription.is_class_filter_mode() {
        return;
    }
    let tags = subscription
        .get_tags_set()
        .map(|set| set.snapshot())
        .unwrap_or_default();
    if tags.is_empty() {
        return;
    }
    messages.retain(|message| {
        message
            .tags()
            .is_some_and(|tag| tags.contains(&Some(tag.to_owned())))
    });
}

fn attach_client_message_ids(messages: &mut [MessageExt]) {
    for message in messages {
        let offset_message_id = if message.is_message_client_ext() {
            message.offset_message_id().map(str::to_owned)
        } else {
            message.message_id().map(str::to_owned)
        };
        let message_id = message
            .properties()
            .get(
                rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            )
            .cloned()
            .or_else(|| offset_message_id.clone());
        message.apply_client_decode_ids(offset_message_id, message_id);
    }
}

fn enrich_messages(messages: &mut [MessageExt], queue: &MessageQueue, result: &PullResultExt) {
    for message in messages {
        let is_prepared = message
            .properties()
            .get(rocketmq_classic_common::message::constants::PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        if is_prepared {
            let transaction_id = message
                .properties()
                .get(rocketmq_classic_common::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .cloned();
            message.set_transaction_id(transaction_id);
        }
        let _ = message.set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_MIN_OFFSET,
            result.min_offset.to_string(),
        );
        let _ = message.set_system_property(
            rocketmq_classic_common::message::constants::PROPERTY_MAX_OFFSET,
            result.max_offset.to_string(),
        );
        message.set_broker_name(queue.broker_name().map(str::to_owned));
        message.set_queue_id(queue.queue_id());
        if let Some(delta) = result.offset_delta {
            message.set_queue_offset(message.queue_offset().wrapping_add(delta));
        }
    }
}

fn unwrap_messages(
    messages: &[MessageExt],
    options: DecodeOptions,
) -> Result<Vec<MessageExt>, ProcessPullError> {
    if !options.read_body {
        return Err(ProcessPullError::MalformedInnerBatch {
            offset: 0,
            reason: "message bodies were not read",
        });
    }
    let mut unwrapped = Vec::new();
    for outer in messages {
        if outer.sys_flag() & INNER_BATCH_FLAG != 0 && outer.sys_flag() & NEED_UNWRAP_FLAG != 0 {
            unwrapped.extend(decode_inner_batch(outer)?);
        } else {
            unwrapped.push(outer.clone());
        }
    }
    Ok(unwrapped)
}

const MAX_INNER_BATCH_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_INNER_BATCH_DECODED_BYTES: usize = 64 * 1024 * 1024;

fn decode_inner_batch(outer: &MessageExt) -> Result<Vec<MessageExt>, ProcessPullError> {
    let bytes = outer.body();
    let mut offset = 0usize;
    let mut total_decoded = 0usize;
    let mut messages = Vec::new();
    while offset < bytes.len() {
        let size = read_i32(&bytes, offset).ok_or(ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "missing record size",
        })?;
        let size = usize::try_from(size).map_err(|_| ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "negative record size",
        })?;
        if size > MAX_INNER_BATCH_RECORD_BYTES {
            return Err(ProcessPullError::InnerBatchLimitExceeded {
                limit: MAX_INNER_BATCH_RECORD_BYTES,
            });
        }
        if size < 22 || offset.checked_add(size).is_none_or(|end| end > bytes.len()) {
            return Err(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "record size is truncated or smaller than its header",
            });
        }
        let record = &bytes[offset..offset + size];
        let flag = read_i32(record, 12).ok_or(ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "missing flag",
        })?;
        let body_size = usize::try_from(read_i32(record, 16).ok_or(
            ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "missing body size",
            },
        )?)
        .map_err(|_| ProcessPullError::MalformedInnerBatch {
            offset,
            reason: "negative body size",
        })?;
        let body_start = 20usize;
        let body_end = body_start
            .checked_add(body_size)
            .filter(|end| *end <= record.len())
            .ok_or(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "body exceeds its record",
            })?;
        let properties_size =
            read_i16(record, body_end).ok_or(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "missing properties size",
            })?;
        let properties_size = usize::try_from(properties_size).map_err(|_| {
            ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "negative properties size",
            }
        })?;
        let properties_start = body_end + 2;
        let properties_end = properties_start
            .checked_add(properties_size)
            .filter(|end| *end == record.len())
            .ok_or(ProcessPullError::MalformedInnerBatch {
                offset,
                reason: "properties length does not match record size",
            })?;
        total_decoded = total_decoded
            .checked_add(body_size)
            .and_then(|size| size.checked_add(properties_size))
            .ok_or(ProcessPullError::InnerBatchLimitExceeded {
                limit: MAX_INNER_BATCH_DECODED_BYTES,
            })?;
        if total_decoded > MAX_INNER_BATCH_DECODED_BYTES {
            return Err(ProcessPullError::InnerBatchLimitExceeded {
                limit: MAX_INNER_BATCH_DECODED_BYTES,
            });
        }
        let properties = String::from_utf8_lossy(&record[properties_start..properties_end]);
        let mut inner = MessageExt::new();
        inner.set_topic(outer.topic().to_owned());
        inner.set_body(record[body_start..body_end].to_vec());
        inner.set_flag(flag);
        for (key, value) in decode_properties(&properties) {
            let _ = inner.set_system_property(key, value);
        }
        inner.set_queue_id(outer.queue_id());
        inner.set_queue_offset(outer.queue_offset().wrapping_add(messages.len() as i64));
        inner.set_sys_flag(outer.sys_flag());
        inner.set_born_timestamp(outer.born_timestamp());
        inner.set_born_host(outer.born_host().cloned());
        inner.set_store_timestamp(outer.store_timestamp());
        inner.set_store_host(outer.store_host().cloned());
        inner.set_commit_log_offset(outer.commit_log_offset());
        messages.push(inner);
        offset += size;
    }
    Ok(messages)
}

fn read_i16(bytes: &[u8], offset: usize) -> Option<i16> {
    Some(i16::from_be_bytes(
        bytes.get(offset..offset.checked_add(2)?)?.try_into().ok()?,
    ))
}

fn read_i32(bytes: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_be_bytes(
        bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;
    use rocketmq_classic_common::message::{MessageHostAddress, encode_message_ext};

    fn empty_pull_result(status: PullStatus) -> PullResultExt {
        PullResultExt {
            status,
            next_begin_offset: 100,
            min_offset: 20,
            max_offset: 160,
            suggest_which_broker_id: 7,
            offset_delta: None,
            raw_messages: None,
        }
    }

    #[test]
    fn pull_result_keeps_all_24_valid_compressed_records() {
        const BODY_SIZE: usize = 3 * 1024 * 1024;

        let mut message = MessageExt::new();
        message.set_topic("orders");
        message.set_body(vec![0x5a; BODY_SIZE]);
        message.set_sys_flag(rocketmq_classic_common::sysflag::message::COMPRESSED_FLAG);
        message.set_born_host(Some(MessageHostAddress::from_ip_address(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            10_911,
        )));
        message.set_store_host(Some(MessageHostAddress::from_ip_address(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            10_912,
        )));
        let record = encode_message_ext(&message, true).expect("compressed message encodes");
        let mut raw_messages = Vec::new();
        for _ in 0..24 {
            raw_messages.extend_from_slice(&record);
        }

        let mut result = empty_pull_result(PullStatus::Found);
        result.next_begin_offset = 24;
        result.raw_messages = Some(raw_messages);
        let processed = PullAPIWrapper::default()
            .process_pull_result(
                &MessageQueue::new("orders", "broker-a", 0),
                result,
                &SubscriptionData::default(),
                DecodeOptions::default(),
            )
            .expect("valid compressed pull result decodes");

        assert_eq!(processed.result.next_begin_offset, 24);
        let messages = processed.messages.expect("FOUND returns messages");
        assert_eq!(messages.len(), 24);
        assert!(
            messages
                .iter()
                .all(|message| message.body().len() == BODY_SIZE)
        );
    }

    #[test]
    fn pull_processing_retains_suggested_broker_for_non_found_statuses() {
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let wrapper = PullAPIWrapper::default();
        let processed = wrapper
            .process_pull_result(
                &queue,
                empty_pull_result(PullStatus::NoNewMessage),
                &SubscriptionData::default(),
                DecodeOptions::default(),
            )
            .unwrap();
        assert_eq!(processed.result.next_begin_offset, 100);
        assert_eq!(
            wrapper.suggested_broker_ids.lock().unwrap().get(&queue),
            Some(&7)
        );
    }

    #[test]
    fn pull_api_wrapper_mutators_and_live_hook_list_match_java_entrypoints() {
        use std::sync::atomic::AtomicBool;

        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/pull-implementation-accessor.json"
        ))
        .unwrap();

        struct UnitModeHook(Arc<AtomicBool>);

        impl FilterMessageHook for UnitModeHook {
            fn hook_name(&self) -> &str {
                "pull-api-wrapper-test"
            }

            fn filter_message(&self, context: &mut FilterMessageContext) {
                context.unit_mode = true;
                self.0.store(true, Ordering::Release);
            }
        }

        let wrapper = PullAPIWrapper::new(false);
        let queue = MessageQueue::new("orders", "broker-a", 2);
        assert_eq!(wrapper.default_broker_id(), 0);
        assert_eq!(
            wrapper.default_broker_id(),
            expected["pullAPIWrapperDefaultBrokerId"].as_i64().unwrap()
        );
        assert!(!wrapper.is_connect_broker_by_user());
        assert_eq!(
            wrapper.is_connect_broker_by_user(),
            expected["pullAPIWrapperConnectByUserBeforeMutation"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(wrapper.recalculate_pull_from_which_node(&queue), 0);
        wrapper.update_pull_from_which_node(&queue, 7);
        assert_eq!(wrapper.recalculate_pull_from_which_node(&queue), 7);
        assert_eq!(
            wrapper.recalculate_pull_from_which_node(&queue),
            expected["pullAPIWrapperSuggestedBrokerId"]
                .as_i64()
                .unwrap()
        );
        wrapper.set_default_broker_id(11);
        wrapper.set_connect_broker_by_user(true);
        assert_eq!(wrapper.default_broker_id(), 11);
        assert!(wrapper.is_connect_broker_by_user());
        assert_eq!(wrapper.recalculate_pull_from_which_node(&queue), 11);
        assert_eq!(
            wrapper.recalculate_pull_from_which_node(&queue),
            expected["pullAPIWrapperBrokerIdAfterUserSelection"]
                .as_i64()
                .unwrap()
        );
        assert!(wrapper.random_num() >= 0);
        assert!(
            expected["pullAPIWrapperRandomNumNonnegative"]
                .as_bool()
                .unwrap()
        );
        assert!(!wrapper.has_hook());
        assert!(
            !expected["pullAPIWrapperHasHookBeforeRegistration"]
                .as_bool()
                .unwrap()
        );

        let hook_called = Arc::new(AtomicBool::new(false));
        let hooks = Arc::new(RwLock::new(Vec::<Arc<dyn FilterMessageHook>>::new()));
        wrapper.set_filter_message_hooks(hooks.clone());
        hooks
            .write()
            .unwrap()
            .push(Arc::new(UnitModeHook(hook_called.clone())));
        assert!(wrapper.has_hook());
        let mut context = FilterMessageContext {
            unit_mode: false,
            messages: Vec::new(),
        };
        wrapper.execute_hook(&mut context);
        assert!(context.unit_mode);
        assert!(hook_called.load(Ordering::Acquire));
        assert!(
            expected["pullAPIWrapperHasHookAfterRegistration"]
                .as_bool()
                .unwrap()
        );
        assert!(
            expected["pullAPIWrapperHookMutatedContext"]
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn found_decode_error_still_retains_the_suggested_broker() {
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let wrapper = PullAPIWrapper::default();
        assert!(matches!(
            wrapper.process_pull_result(
                &queue,
                empty_pull_result(PullStatus::Found),
                &SubscriptionData::default(),
                DecodeOptions::default(),
            ),
            Err(ProcessPullError::MissingMessageBinary)
        ));
        assert_eq!(
            wrapper.suggested_broker_ids.lock().unwrap().get(&queue),
            Some(&7)
        );
    }

    #[test]
    fn class_filter_routing_uses_the_broker_address_route_entry() {
        let mut route = TopicRouteData::default();
        route.set_filter_server_table_owned(Some(BTreeMap::from([(
            "192.0.2.9:10911".to_owned(),
            Some(vec![
                Some("192.0.2.20:10911".to_owned()),
                Some("192.0.2.21:10911".to_owned()),
            ]),
        )])));
        let selected = select_filter_server(&route, "orders", "192.0.2.9:10911").unwrap();
        assert!(matches!(
            selected.as_deref(),
            Some("192.0.2.20:10911" | "192.0.2.21:10911")
        ));
        assert!(matches!(
            select_filter_server(&route, "orders", "missing"),
            Err(FilterServerError::NotFound { .. })
        ));
        assert_eq!(
            resolve_pull_broker_address(&route, "orders", "192.0.2.9:10911", 0).unwrap(),
            Some("192.0.2.9:10911".to_owned())
        );
        let filter_flag =
            rocketmq_classic_common::sysflag::pull::build_pull_sys_flag(false, false, false, true);
        let routed = resolve_pull_broker_address(&route, "orders", "192.0.2.9:10911", filter_flag)
            .unwrap()
            .unwrap();
        assert!(matches!(
            routed.as_str(),
            "192.0.2.20:10911" | "192.0.2.21:10911"
        ));
        route.set_filter_server_table_owned(Some(BTreeMap::from([(
            "empty".to_owned(),
            Some(Vec::new()),
        )])));
        assert!(matches!(
            select_filter_server(&route, "orders", "empty"),
            Err(FilterServerError::NotFound { .. })
        ));
    }
}
