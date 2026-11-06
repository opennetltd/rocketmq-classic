use std::{env, sync::Arc, time::Duration};

use rocketmq_classic::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageExt, PushConsumer,
    PushConsumerConfig, PushMessageListener,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = env::var("ROCKETMQ_EXAMPLE_TOPIC").unwrap_or_else(|_| "example-topic".into());
    let run_seconds = env::var("ROCKETMQ_EXAMPLE_RUN_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60);

    let mut config = PushConsumerConfig::new("example-push-consumer")?;
    if let Ok(address) = env::var("ROCKETMQ_NAMESRV_ADDR") {
        config.client_config_mut().set_namesrv_addr(Some(address));
    }
    let consumer = Arc::new(PushConsumer::new(config));
    consumer
        .set_message_listener(PushMessageListener::concurrently(
            |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                let queue = context.message_queue().clone();
                for message in messages {
                    println!(
                        "consumed topic={} queue={} offset={} id={:?}",
                        message.topic(),
                        queue.queue_id(),
                        message.queue_offset(),
                        message.message_id()
                    );
                }
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        ))
        .await?;
    consumer.subscribe(&topic, "*").await?;
    consumer.start().await?;

    tokio::time::sleep(Duration::from_secs(run_seconds)).await;
    consumer.shutdown().await?;
    Ok(())
}
