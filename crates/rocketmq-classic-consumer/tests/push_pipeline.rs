use std::collections::HashMap;

use rocketmq_classic_common::message::MessageExt;
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, PushConsumer, PushConsumerConfig, PushConsumerError, PushMessageListener,
};

#[tokio::test]
async fn public_push_controls_match_configured_listener_and_subscription_state() {
    let mut config = PushConsumerConfig::new("push-pipeline-api").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = PushConsumer::new(config);
    let listener = PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    );
    consumer.set_message_listener(listener).await.unwrap();
    assert!(consumer.get_message_listener().is_some());
    assert!(!consumer.is_consume_orderly());

    let mut configured_subscriptions = HashMap::new();
    configured_subscriptions.insert("orders".to_owned(), "gold || silver".to_owned());
    consumer
        .set_subscription(configured_subscriptions)
        .await
        .unwrap();
    assert_eq!(
        consumer.get_subscription().get("tenant%orders"),
        Some(&"gold || silver".to_owned())
    );

    assert!(matches!(
        consumer.update_core_pool_size(2).await,
        Err(PushConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer.send_message_back(&MessageExt::default(), 0).await,
        Err(PushConsumerError::NotStarted)
    ));
    assert!(matches!(
        consumer
            .send_message_back_to_broker(&MessageExt::default(), 0, Some("broker-a"))
            .await,
        Err(PushConsumerError::NotStarted)
    ));
    consumer.suspend().await;
    assert!(consumer.is_pause());
    consumer.resume().await;
    assert!(!consumer.is_pause());

    let orderly = PushConsumer::new(PushConsumerConfig::new("push-orderly-api").unwrap());
    orderly
        .set_message_listener(PushMessageListener::orderly(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        ))
        .await
        .unwrap();
    assert!(orderly.get_message_listener().is_some());
    assert!(!orderly.is_consume_orderly());
}
