use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use rocketmq_classic_common::message::{Message, MessageExt};
use rocketmq_classic_producer::{
    EndTransactionContext, EndTransactionHook, LocalTransactionState, Producer, ProducerConfig,
    ProducerError, RpcHook, TransactionCallbackError, TransactionListener, TransactionProducer,
};

struct NoopListener;

struct CaptureEndTransactionHook(Arc<Mutex<Vec<String>>>);

impl EndTransactionHook for CaptureEndTransactionHook {
    fn hook_name(&self) -> &str {
        "public-api-capture"
    }

    fn end_transaction(&self, context: &mut EndTransactionContext<'_>) {
        self.0.lock().unwrap().push(format!(
            "{}:{}:{}:{}:{}:{}",
            context.producer_group.as_deref().unwrap_or("<null>"),
            context.message_id,
            context.broker_addr,
            context.transaction_id.as_deref().unwrap_or("<null>"),
            context.transaction_state.java_name(),
            context.from_transaction_check,
        ));
        let _ = context.message.set_system_property("HOOKED", "true");
    }
}

impl TransactionListener for NoopListener {
    fn execute_local_transaction(
        &self,
        _message: &mut Message,
        _argument: Option<&(dyn Any + Send + Sync)>,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        Ok(Some(LocalTransactionState::Unknown))
    }

    fn check_local_transaction(
        &self,
        _message: &mut MessageExt,
    ) -> Result<Option<LocalTransactionState>, TransactionCallbackError> {
        Ok(Some(LocalTransactionState::Unknown))
    }
}

fn producer() -> TransactionProducer {
    let mut config = ProducerConfig::new("transaction-public-api").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tx-fixture".into()));
    TransactionProducer::new(config)
}

#[test]
fn java_rpc_hook_constructor_is_available_from_the_public_crate_api() {
    struct Hook;
    impl RpcHook for Hook {}

    let producer = TransactionProducer::new_with_rpc_hook(
        ProducerConfig::new("transaction-public-rpc-hook").unwrap(),
        Some(Arc::new(Hook)),
    );
    assert!(producer.executor_service().is_none());
}

#[test]
fn end_transaction_hook_context_and_methods_are_available_from_the_public_crate_api() {
    let producer = Producer::new(ProducerConfig::new("end-hook-public-api").unwrap());
    let observed = Arc::new(Mutex::new(Vec::new()));
    assert!(!producer.has_end_transaction_hook());
    producer.register_end_transaction_hook(Arc::new(CaptureEndTransactionHook(observed.clone())));
    assert!(producer.has_end_transaction_hook());

    let mut direct_message = Message::new("orders", b"direct".to_vec());
    let mut direct_context = EndTransactionContext {
        producer_group: Some("direct-group".into()),
        message: &mut direct_message,
        broker_addr: "192.0.2.10:10911".into(),
        message_id: "direct-id".into(),
        transaction_id: None,
        transaction_state: LocalTransactionState::Rollback,
        from_transaction_check: true,
    };
    producer.execute_end_transaction_hook(&mut direct_context);

    let mut automatic_message = Message::new("orders", b"automatic".to_vec());
    automatic_message.set_transaction_id(Some("transaction-id".into()));
    producer.do_execute_end_transaction_hook(
        &mut automatic_message,
        "automatic-id",
        "192.0.2.11:10911",
        LocalTransactionState::Commit,
        false,
    );

    assert_eq!(
        observed.lock().unwrap().as_slice(),
        [
            "direct-group:direct-id:192.0.2.10:10911:<null>:ROLLBACK_MESSAGE:true",
            "end-hook-public-api:automatic-id:192.0.2.11:10911:transaction-id:COMMIT_MESSAGE:false",
        ]
    );
    assert_eq!(
        direct_message
            .properties()
            .get("HOOKED")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        automatic_message
            .properties()
            .get("HOOKED")
            .map(String::as_str),
        Some("true")
    );
}

#[test]
#[allow(deprecated)]
fn java_check_executor_defaults_and_settings_are_exposed() {
    let producer = producer();
    assert_eq!(producer.check_thread_pool_min_size(), 1);
    assert_eq!(producer.check_thread_pool_max_size(), 1);
    assert_eq!(producer.check_request_hold_max(), 2_000);
    assert!(producer.executor_service().is_none());
    assert!(producer.transaction_listener().is_none());
    assert!(producer.transaction_check_listener().is_none());

    let listener = Arc::new(NoopListener);
    let listener_api: Arc<dyn TransactionListener> = listener.clone();
    producer.set_transaction_listener(Some(listener_api.clone()));
    assert!(Arc::ptr_eq(
        &listener_api,
        &producer.transaction_listener().unwrap()
    ));

    producer.set_check_thread_pool_min_size(2);
    producer.set_check_thread_pool_max_size(5);
    producer.set_check_request_hold_max(17);
    assert_eq!(producer.check_thread_pool_min_size(), 2);
    assert_eq!(producer.check_thread_pool_max_size(), 5);
    assert_eq!(producer.check_request_hold_max(), 17);
}

#[tokio::test]
#[allow(deprecated)]
async fn check_pool_settings_validate_java_boundaries_at_start() {
    for (min, max, hold) in [(-1, 1, 2_000), (1, 0, 2_000), (2, 1, 2_000), (1, 1, 0)] {
        let producer = producer();
        producer.set_check_thread_pool_min_size(min);
        producer.set_check_thread_pool_max_size(max);
        producer.set_check_request_hold_max(hold);

        assert!(matches!(
            producer.start().await,
            Err(ProducerError::InvalidTransactionPoolSettings {
                min: actual_min,
                max: actual_max,
                hold: actual_hold,
            }) if (actual_min, actual_max, actual_hold) == (min, max, hold)
        ));
    }
}

#[tokio::test]
async fn missing_listener_fails_before_message_mutation_and_does_not_block_start() {
    let producer = producer();
    producer.start().await.unwrap();
    let mut message = Message::new("orders", b"body".to_vec());

    assert!(matches!(
        producer
            .send_message_in_transaction(&mut message, None)
            .await,
        Err(rocketmq_classic_producer::ProducerError::TransactionListenerMissing)
    ));
    assert_eq!(message.topic(), "orders");
    assert!(!message.properties().contains_key("TRAN_MSG"));
    assert!(!message.properties().contains_key("PGROUP"));

    producer.shutdown().await.unwrap();
}

#[tokio::test]
async fn half_message_properties_remain_on_caller_message_after_prestart_failure() {
    let producer = producer();
    producer.set_transaction_listener(Some(Arc::new(NoopListener)));
    let mut message = Message::new("orders", b"body".to_vec());

    assert!(matches!(
        producer
            .send_message_in_transaction(&mut message, None)
            .await,
        Err(rocketmq_classic_producer::ProducerError::TransactionSend(_))
    ));
    assert_eq!(message.topic(), "tx-fixture%orders");
    assert_eq!(
        message.properties().get("TRAN_MSG").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        message.properties().get("PGROUP").map(String::as_str),
        Some("transaction-public-api")
    );
}

#[tokio::test]
async fn delayed_transaction_is_rejected_after_java_namespace_wrapping() {
    let producer = producer();
    producer.set_transaction_listener(Some(Arc::new(NoopListener)));
    let mut message = Message::new("orders", b"body".to_vec());
    message.set_system_property("TIMER_DELAY_MS", "10").unwrap();

    assert!(matches!(
        producer
            .send_message_in_transaction(&mut message, None)
            .await,
        Err(rocketmq_classic_producer::ProducerError::DelayedTransactionalMessage)
    ));
    assert_eq!(message.topic(), "tx-fixture%orders");
    assert!(!message.properties().contains_key("TRAN_MSG"));
}
