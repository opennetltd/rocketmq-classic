use std::env;

use rocketmq_classic::{BoundaryType, MessageQueue, PullConsumer, PullConsumerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = env::var("ROCKETMQ_EXAMPLE_TOPIC").unwrap_or_else(|_| "example-topic".into());
    let broker = env::var("ROCKETMQ_EXAMPLE_BROKER").unwrap_or_else(|_| "broker-a".into());
    let queue_id = env::var("ROCKETMQ_EXAMPLE_QUEUE_ID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let start_timestamp_millis = env::var("ROCKETMQ_EXAMPLE_START_TIMESTAMP_MS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok());
    let boundary = if env::var("ROCKETMQ_EXAMPLE_BOUNDARY")
        .is_ok_and(|value| value.eq_ignore_ascii_case("upper"))
    {
        BoundaryType::Upper
    } else {
        BoundaryType::Lower
    };

    let mut config = PullConsumerConfig::new("example-pull-consumer")?;
    if let Ok(address) = env::var("ROCKETMQ_NAMESRV_ADDR") {
        config.client_config_mut().set_namesrv_addr(Some(address));
    }
    let consumer = std::sync::Arc::new(PullConsumer::new(config));
    consumer.register_message_queue_listener(&topic, None)?;
    consumer.start().await?;

    let queue = MessageQueue::new(topic, broker, queue_id);
    let mut next_offset = consumer.fetch_consume_offset(&queue, true).await?;
    if next_offset < 0 {
        next_offset = match start_timestamp_millis {
            Some(timestamp) => {
                consumer
                    .search_offset_with_boundary(&queue, timestamp, boundary)
                    .await?
            }
            None => consumer.get_min_offset(&queue).await?,
        }
        .max(0);
    }

    let pulled = consumer.pull(&queue, None, next_offset, 16).await?;
    match pulled.result.status.name() {
        "FOUND" => {
            for message in pulled.messages.unwrap_or_default() {
                // Replace this with the application's durable side effect.
                println!(
                    "processed topic={} queue={} offset={} id={:?}",
                    message.topic(),
                    queue.queue_id(),
                    message.queue_offset(),
                    message.message_id()
                );
            }
            next_offset = pulled.result.next_begin_offset;
            consumer.update_consume_offset(&queue, next_offset).await?;
            consumer.persist_offset(&queue).await?;
        }
        status => eprintln!("pull returned {status}; next offset remains {next_offset}"),
    }

    consumer.shutdown().await?;
    Ok(())
}
