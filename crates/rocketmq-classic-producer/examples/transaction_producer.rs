use std::{any::Any, error::Error, sync::Arc};

use rocketmq_classic_common::message::{Message, MessageExt};
use rocketmq_classic_producer::{
    LocalTransactionState, ProducerConfig, TransactionCallbackError, TransactionListener,
    TransactionProducer,
};

struct OrderListener;

impl TransactionListener for OrderListener {
    fn execute_local_transaction(
        &self,
        _message: &mut Message,
        _argument: Option<&(dyn Any + Send + Sync)>,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        // Replace this with the durable order operation and its recorded outcome.
        Ok(Some(LocalTransactionState::Commit))
    }

    fn check_local_transaction(
        &self,
        _message: &mut MessageExt,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        // Return the same demo decision; real applications should query their
        // durable business state before deciding.
        Ok(Some(LocalTransactionState::Commit))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let producer = TransactionProducer::new(ProducerConfig::new("transaction-example")?);
    producer.set_transaction_listener(Some(Arc::new(OrderListener)));
    producer.start().await?;

    let mut message = Message::new("orders", b"order-123".to_vec());
    let send = producer
        .send_message_in_transaction(&mut message, None)
        .await;
    let shutdown = producer.shutdown().await;

    let result = send?;
    shutdown?;
    println!("transaction outcome: {:?}", result.local_transaction_state);
    Ok(())
}
