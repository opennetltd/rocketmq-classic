use rocketmq_classic::{
    Message, ProducerError, ReplyMessageError, RequestCallback, create_reply_message,
};
use rocketmq_classic::{
    RequestFutureError, RequestFutureHolder, RequestFutureTable, RequestResponseFuture,
};
use std::sync::Arc;

#[test]
fn request_reply_api_is_available_from_the_facade_crate() {
    let callback: RequestCallback = Box::new(|result| {
        let _: Result<Message, ProducerError> = result;
    });
    let _: RequestCallback = callback;

    let mut request = Message::new("orders", b"request".to_vec());
    request
        .set_system_property("CLUSTER", "cluster-east")
        .unwrap();
    request
        .set_system_property("CORRELATION_ID", "correlation-01")
        .unwrap();
    let reply = create_reply_message(Some(&request), b"reply".to_vec()).unwrap();

    assert_eq!(reply.topic(), "cluster-east_REPLY_TOPIC");
    assert_eq!(
        reply.get_property("CORRELATION_ID").as_deref(),
        Some("correlation-01")
    );
    assert!(matches!(
        create_reply_message(None, b"reply".to_vec()),
        Err(ReplyMessageError::NullRequest)
    ));
}

#[test]
fn request_future_holder_returns_the_live_runtime_correlation_table() {
    let holder = RequestFutureHolder::get_instance();
    let table = holder.get_request_future_table();
    let runtime_table = RequestFutureTable::shared();
    let correlation_id = "facade-request-future-table";
    let _ = table.remove(correlation_id).unwrap();
    let future = Arc::new(RequestResponseFuture::new(correlation_id, 30_000, None));

    assert!(
        table
            .insert(correlation_id, future.clone())
            .unwrap()
            .is_none()
    );
    let mapped = runtime_table.get(correlation_id).unwrap().unwrap();
    assert!(Arc::ptr_eq(&mapped, &future));
    assert_eq!(mapped.correlation_id(), correlation_id);
    assert!(Arc::ptr_eq(
        &holder
            .get_request_future_table()
            .get(correlation_id)
            .unwrap()
            .unwrap(),
        &future
    ));
    assert!(Arc::ptr_eq(
        &table.remove(correlation_id).unwrap().unwrap(),
        &future
    ));
    assert!(table.is_empty().unwrap());
}

#[test]
fn nullable_java_count_down_latch_is_represented_in_the_public_rust_api() {
    let future = RequestResponseFuture::new("nullable-latch-api", 1_000, None);
    future.set_response_msg(Some(Message::new("plain-response", b"body".to_vec())));
    assert_eq!(
        future.response_msg().unwrap().topic(),
        "plain-response",
        "Java RequestResponseFuture accepts and returns the base Message type"
    );
    future.set_count_down_latch(None);
    assert!(future.count_down_latch().is_none());
    assert!(matches!(
        future.put_response_message(None),
        Err(RequestFutureError::MissingCountDownLatch)
    ));
}

#[test]
fn pinned_java_request_reply_callback_surface_is_inventoried() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/producer/request-reply.json"
    ))
    .unwrap();

    assert_eq!(
        fixture["publicRequestCallbackMethods"],
        serde_json::json!([
            "void onException(java.lang.Throwable)",
            "void onSuccess(org.apache.rocketmq.common.message.Message)"
        ])
    );
    assert_eq!(
        fixture["publicRequestFutureHolderMethods"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        fixture["publicRequestResponseFutureMethods"]
            .as_array()
            .unwrap()
            .len(),
        18
    );
}

#[test]
fn pinned_java_request_future_mutation_and_holder_table_behavior_is_recorded() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/producer/request-reply.json"
    ))
    .unwrap();

    let future = &fixture["requestResponseFutureBehavior"];
    assert_eq!(future["correlationId"], "future-fixture-01");
    assert_eq!(future["initialLatchCount"], 1);
    assert_eq!(future["replacementLatchInitialCount"], 2);
    assert_eq!(future["putResponseMessageCount"], 1);
    assert_eq!(future["directLatchCountDownCount"], 0);
    assert_eq!(future["negativeTimeoutIsExpired"], true);
    assert_eq!(future["responseSetterCanClear"], true);
    assert_eq!(future["sendRequestOkSetter"], false);
    assert_eq!(future["causeSameInstance"], true);
    assert_eq!(future["nullLatchGetter"], true);
    assert_eq!(
        future["nullLatchPutResponseException"],
        "java.lang.NullPointerException"
    );
    assert_eq!(future["nullLatchPutResponseStored"], true);
    assert_eq!(
        future["nullLatchWaitResponseException"],
        "java.lang.NullPointerException"
    );
    assert_eq!(
        future["callbackEvents"],
        serde_json::json!([
            "exception:java.lang.IllegalStateException:fixture-cause",
            "success:future-response"
        ])
    );
    assert_eq!(future["nullCallbackExecution"], "completed");

    let holder = &fixture["requestFutureHolderBehavior"];
    assert_eq!(holder["singletonInstance"], true);
    assert_eq!(holder["tableSameInstance"], true);
    assert_eq!(holder["insertVisibleThroughSecondView"], true);
    assert_eq!(holder["removedSameInstance"], true);
    assert_eq!(holder["sizeAfterInsert"], 1);
    assert_eq!(holder["sizeAfterRemove"], 0);
    assert_eq!(holder["waiterLatchCountAfterTableRemove"], 1);
    assert_eq!(holder["waiterResponseNullAfterTableRemove"], true);
    assert_eq!(holder["waiterLatchStillUnsignaledAfterTableRemove"], true);
}
