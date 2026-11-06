use std::collections::HashSet;

use rocketmq_classic_protocol::SharedValue;
use rocketmq_classic_protocol::body::{
    LiteSubscription, LiteSubscriptionAction, LiteSubscriptionCtlRequestBody, LiteSubscriptionDTO,
    OffsetOption, OffsetOptionType,
};

#[test]
#[allow(clippy::mutable_key_type)] // Java DTO Set elements use stable object-identity hashes.
fn lite_subscription_java_accessors_are_public_and_chain_like_java() {
    let mut subscription = LiteSubscription::new();
    subscription
        .set_group(Some("workers".into()))
        .set_topic(Some("orders".into()))
        .set_lite_topic_set(["lite-a"]);
    assert_eq!(subscription.group(), Some("workers"));
    assert_eq!(subscription.topic(), Some("orders"));
    let lite_topics = subscription.lite_topic_set();
    assert!(lite_topics.ptr_eq(&subscription.lite_topic_set()));
    assert!(subscription.add_lite_topic("lite-b"));
    assert!(lite_topics.with(|topics| topics.contains("lite-b")));
    lite_topics.with_mut(|topics| {
        topics.insert("external-topic".into());
    });
    assert!(
        subscription
            .lite_topic_set()
            .with(|topics| topics.contains("external-topic"))
    );
    assert!(subscription.remove_lite_topic("lite-b"));

    let mut dto = LiteSubscriptionDTO::default();
    dto.set_action(Some(LiteSubscriptionAction::CompleteAdd))
        .set_client_id(Some("client".into()))
        .set_group(Some("workers".into()))
        .set_topic(Some("orders".into()))
        .set_lite_topic_set(Some(SharedValue::new(std::collections::BTreeSet::from([
            Some("lite-a".into()),
        ]))))
        .set_version(9);
    dto.set_offset_option(Some(SharedValue::new(OffsetOption::new(
        Some(OffsetOptionType::Policy),
        OffsetOption::POLICY_MIN_VALUE,
    ))));
    assert_eq!(dto.action(), Some(LiteSubscriptionAction::CompleteAdd));
    assert_eq!(dto.client_id(), Some("client"));
    assert_eq!(dto.version(), 9);

    let mut control = LiteSubscriptionCtlRequestBody::default();
    let shared_dto = SharedValue::new(dto);
    let subscriptions = SharedValue::new(HashSet::from([Some(shared_dto.clone())]));
    control.set_subscription_set(Some(subscriptions.clone()));
    let getter_subscriptions = control.subscription_set().unwrap();
    assert!(subscriptions.ptr_eq(&getter_subscriptions));
    assert!(getter_subscriptions.with(|set| set.contains(&Some(shared_dto))));
    assert_eq!(getter_subscriptions.with(HashSet::len), 1);

    let equal_options = HashSet::from([
        OffsetOption::new(Some(OffsetOptionType::TailN), 23),
        OffsetOption::new(Some(OffsetOptionType::TailN), 23),
    ]);
    assert_eq!(equal_options.len(), 1);
}
