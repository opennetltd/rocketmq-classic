use std::{env, sync::Arc};

use rocketmq_classic::{CompressionType, Message, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ProducerConfig::new("example-producer")?;
    config.set_compress_type(CompressionType::Zlib);
    if let Ok(address) = env::var("ROCKETMQ_NAMESRV_ADDR") {
        config.client_config_mut().set_namesrv_addr(Some(address));
    }
    let producer = Arc::new(Producer::new(config));
    producer.start().await?;

    let topic = env::var("ROCKETMQ_EXAMPLE_TOPIC").unwrap_or_else(|_| "example-topic".into());
    let mut message = Message::new(topic, b"example payload".to_vec());
    let send = producer.send(&mut message).await;
    let shutdown = producer.shutdown().await;

    let result = send?;
    shutdown?;
    println!("send result: {result:?}");
    Ok(())
}
