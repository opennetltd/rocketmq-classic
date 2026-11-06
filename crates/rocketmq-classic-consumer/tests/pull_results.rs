use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use base64::Engine;
use rocketmq_classic_common::message::{DecodeOptions, MessageQueue};
use rocketmq_classic_consumer::pull::{FilterMessageContext, FilterMessageHook, PullAPIWrapper};
use rocketmq_classic_consumer::{PushConsumer, PushConsumerConfig};
use rocketmq_classic_core::api::consumer::{PullResultExt, PullStatus};
use rocketmq_classic_protocol::heartbeat::SubscriptionData;
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap()
}

fn record(name: &str) -> Vec<u8> {
    let case = fixture()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap()
        .clone();
    base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap()
}

fn client_id(name: &str, key: &str) -> String {
    fixture()["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap()["clientDecoded"][key]
        .as_str()
        .unwrap()
        .to_owned()
}

fn result(status: PullStatus, raw_messages: Option<Vec<u8>>) -> PullResultExt {
    PullResultExt {
        status,
        next_begin_offset: 100,
        min_offset: 20,
        max_offset: 160,
        suggest_which_broker_id: 7,
        offset_delta: Some(5),
        raw_messages,
    }
}

async fn class_filter_subscription(
    topic: &str,
    class_name: &str,
    source: &str,
) -> SubscriptionData {
    let consumer = PushConsumer::new(PushConsumerConfig::new("pull-results-class-filter").unwrap());
    consumer
        .subscribe_with_class_filter(topic, class_name, source)
        .await
        .unwrap();
    consumer
        .subscriptions()
        .into_iter()
        .find(|subscription| subscription.get_topic() == Some(topic))
        .expect("public class-filter subscription is stored")
}

struct MarkHook(Arc<AtomicBool>);

impl FilterMessageHook for MarkHook {
    fn hook_name(&self) -> &str {
        "mark-hook"
    }

    fn filter_message(&self, context: &mut FilterMessageContext) {
        self.0.store(context.unit_mode, Ordering::SeqCst);
        if let Some(message) = context.messages.first_mut() {
            message.set_system_property("HOOK_MARK", "seen").unwrap();
        }
    }
}

#[test]
fn found_results_filter_enrich_run_hooks_and_drop_raw_bytes() {
    let queue = MessageQueue::new("orders", "broker-a", 9);
    let wrapper = PullAPIWrapper::new(true);
    let hook_ran_in_unit_mode = Arc::new(AtomicBool::new(false));
    wrapper.register_filter_message_hook(Arc::new(MarkHook(hook_ran_in_unit_mode.clone())));
    let mut subscription = SubscriptionData::new(Some("orders".into()), Some("green".into()));
    subscription.set_tags_set_owned(Some([Some("green".to_owned())].into_iter().collect()));
    let processed = wrapper
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(record("ipv4-v1"))),
            &subscription,
            DecodeOptions::default(),
        )
        .unwrap();
    assert!(processed.result.raw_messages.is_none());
    assert_eq!(processed.result.next_begin_offset, 100);
    let messages = processed.messages.unwrap();
    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert_eq!(
        message.message_id(),
        Some(client_id("ipv4-v1", "msgId").as_str())
    );
    assert_eq!(
        message.offset_message_id(),
        Some(client_id("ipv4-v1", "offsetMsgId").as_str())
    );
    assert_eq!(message.broker_name(), Some("broker-a"));
    assert_eq!(message.queue_id(), 9);
    assert_eq!(message.queue_offset(), 46);
    assert_eq!(
        message.properties().get("MIN_OFFSET").map(String::as_str),
        Some("20")
    );
    assert_eq!(
        message.properties().get("MAX_OFFSET").map(String::as_str),
        Some("160")
    );
    assert_eq!(
        message.properties().get("HOOK_MARK").map(String::as_str),
        Some("seen")
    );
    assert!(hook_ran_in_unit_mode.load(Ordering::SeqCst));
}

#[test]
fn found_results_keep_the_decoded_prefix_when_the_batch_tail_is_malformed() {
    let queue = MessageQueue::new("orders", "broker-a", 9);
    let mut raw = record("ipv4-v1");
    raw.push(0);
    let processed = PullAPIWrapper::default()
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(raw)),
            &SubscriptionData::default(),
            DecodeOptions::default(),
        )
        .unwrap();

    assert!(processed.result.raw_messages.is_none());
    let messages = processed.messages.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].topic(), "orders");
    assert_eq!(
        messages[0].message_id(),
        Some(client_id("ipv4-v1", "msgId").as_str())
    );
    assert_eq!(
        messages[0].offset_message_id(),
        Some(client_id("ipv4-v1", "offsetMsgId").as_str())
    );
}

#[test]
fn prepared_transactions_get_the_producer_unique_id_as_transaction_id() {
    let queue = MessageQueue::new("orders", "broker-a", 3);
    let wrapper = PullAPIWrapper::new(false);
    let subscription = SubscriptionData::new(None, None);
    let processed = wrapper
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(record("store-ipv6-v2"))),
            &subscription,
            DecodeOptions::default(),
        )
        .unwrap();
    let messages = processed.messages.unwrap();
    assert_eq!(messages[0].transaction_id(), Some("producer-unique-2"));
    assert_eq!(messages[0].message_id(), Some("producer-unique-2"));
    assert_eq!(
        messages[0].offset_message_id(),
        Some(client_id("store-ipv6-v2", "offsetMsgId").as_str())
    );
    assert_eq!(messages[0].queue_offset(), 48);
}

#[test]
fn tag_filter_discards_messages_without_a_tag_and_empty_tag_filters_keep_them() {
    let queue = MessageQueue::new("orders", "broker-a", 3);
    let bytes = record("no-tags-v1");
    let mut tagged_subscription = SubscriptionData::default();
    tagged_subscription.set_tags_set_owned(Some([Some("green".to_owned())].into_iter().collect()));
    let wrapper = PullAPIWrapper::default();
    let filtered = wrapper
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(bytes.clone())),
            &tagged_subscription,
            DecodeOptions::default(),
        )
        .unwrap();
    assert!(filtered.messages.unwrap().is_empty());
    let unfiltered = wrapper
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(bytes)),
            &SubscriptionData::default(),
            DecodeOptions::default(),
        )
        .unwrap();
    let unfiltered_message = unfiltered.messages.unwrap().into_iter().next().unwrap();
    assert_eq!(
        unfiltered_message.message_id(),
        Some(client_id("no-tags-v1", "msgId").as_str())
    );
    assert_eq!(
        unfiltered_message.offset_message_id(),
        Some(client_id("no-tags-v1", "offsetMsgId").as_str())
    );
}

#[tokio::test]
async fn class_filter_subscription_keeps_source_and_skips_tag_filtering() {
    let mut subscription = class_filter_subscription(
        "orders",
        "example.OrderFilter",
        "package example; public class OrderFilter {}",
    )
    .await;
    assert!(subscription.is_class_filter_mode());
    assert_eq!(subscription.get_topic(), Some("orders"));
    assert_eq!(subscription.get_sub_string(), Some("example.OrderFilter"));
    assert_eq!(
        subscription.get_filter_class_source(),
        Some("package example; public class OrderFilter {}")
    );
    let heartbeat_value = serde_json::to_value(&subscription).unwrap();
    assert_eq!(heartbeat_value["classFilterMode"], true);
    assert_eq!(heartbeat_value["topic"], "orders");
    assert_eq!(heartbeat_value["subString"], "example.OrderFilter");
    assert!(heartbeat_value.get("filterClassSource").is_none());
    let tags = subscription.get_tags_set().unwrap().snapshot();
    subscription.set_tags_set_owned(Some(
        [Some("does-not-match".to_owned())].into_iter().collect(),
    ));

    let queue = MessageQueue::new("orders", "broker-a", 3);
    let processed = PullAPIWrapper::default()
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(record("ipv4-v1"))),
            &subscription,
            DecodeOptions::default(),
        )
        .unwrap();
    assert_eq!(processed.messages.unwrap().len(), 1);
    assert!(tags.is_empty());
}

#[tokio::test]
async fn inner_batches_unwrap_when_both_flags_are_set_and_fall_back_as_a_unit() {
    let queue = MessageQueue::new("orders", "broker-a", 5);
    let subscription = class_filter_subscription("orders", "Filter", "source").await;
    let mut outer = record("inner-batch");
    // Use a tag constraint; class-filter mode suppresses this client-side pass.
    let mut class_filter = subscription;
    class_filter.set_tags_set_owned(Some([Some("no-match".to_owned())].into_iter().collect()));
    let processed = PullAPIWrapper::default()
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(outer.clone())),
            &class_filter,
            DecodeOptions::default(),
        )
        .unwrap();
    let messages = processed.messages.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].body().as_ref(), b"inner-one");
    assert_eq!(messages[1].body().as_ref(), b"inner-two");
    assert_eq!(messages[0].queue_offset(), 53);
    assert_eq!(messages[1].queue_offset(), 54);
    assert_eq!(messages[0].flag(), 301);
    assert_eq!(messages[0].message_id(), Some("inner-unique-1"));
    assert_eq!(messages[0].offset_message_id(), None);
    assert_eq!(messages[1].message_id(), None);
    assert_eq!(messages[1].offset_message_id(), None);
    assert_eq!(
        messages[0].properties().get("TAGS").map(String::as_str),
        Some("green")
    );

    outer = record("inner-batch-malformed");
    let fallback = PullAPIWrapper::default()
        .process_pull_result(
            &queue,
            result(PullStatus::Found, Some(outer)),
            &SubscriptionData::default(),
            DecodeOptions::default(),
        )
        .unwrap()
        .messages
        .unwrap();
    assert_eq!(fallback.len(), 1);
    assert_eq!(fallback[0].body().as_ref(), &[0, 0, 0, 8]);
}

#[test]
fn non_found_statuses_preserve_offsets_update_broker_and_release_raw_bytes() {
    let queue = MessageQueue::new("orders", "broker-a", 2);
    let wrapper = PullAPIWrapper::default();
    for status in [
        PullStatus::NoNewMessage,
        PullStatus::NoMatchedMessage,
        PullStatus::OffsetIllegal,
    ] {
        let processed = wrapper
            .process_pull_result(
                &queue,
                result(status, Some(vec![1, 2, 3])),
                &SubscriptionData::default(),
                DecodeOptions::default(),
            )
            .unwrap();
        assert_eq!(processed.result.status, status);
        assert_eq!(processed.result.next_begin_offset, 100);
        assert_eq!(processed.result.min_offset, 20);
        assert_eq!(processed.result.max_offset, 160);
        assert_eq!(processed.result.suggest_which_broker_id, 7);
        assert!(processed.result.raw_messages.is_none());
        assert!(processed.messages.is_none());
    }
}

#[test]
fn found_without_message_binary_returns_an_error_after_suggested_broker_update() {
    let queue = MessageQueue::new("orders", "broker-a", 2);
    let wrapper = PullAPIWrapper::default();
    let error = wrapper
        .process_pull_result(
            &queue,
            result(PullStatus::Found, None),
            &SubscriptionData::default(),
            DecodeOptions::default(),
        )
        .unwrap_err();
    assert_eq!(error.to_string(), "FOUND pull result has no message binary");
}
