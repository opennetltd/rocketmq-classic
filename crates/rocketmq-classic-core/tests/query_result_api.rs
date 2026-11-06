use rocketmq_classic_common::message::MessageExt;
use rocketmq_classic_core::api::admin::{QueryMessageList, QueryResult};
use rocketmq_classic_protocol::SharedValue;
use serde_json::{Value, json};

fn java_query_result_fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/admin/query-result.json")).unwrap()
}

#[test]
fn query_result_surface_and_behavior_match_pinned_java_fixture() {
    let fixture = java_query_result_fixture();
    assert_eq!(fixture["publicFields"], json!([]));
    assert_eq!(
        fixture["publicConstructors"],
        json!(["org.apache.rocketmq.client.QueryResult(long,java.util.List)"])
    );
    assert_eq!(
        fixture["publicMethods"],
        json!([
            "java.lang.String toString()",
            "java.util.List getMessageList()",
            "long getIndexLastUpdateTimestamp()"
        ])
    );

    let null_list = QueryResult::new(17, None);
    assert_eq!(
        null_list.get_index_last_update_timestamp(),
        fixture["nullListTimestamp"].as_i64().unwrap()
    );
    assert!(null_list.get_message_list().is_none());
    assert_eq!(
        null_list.to_string(),
        fixture["nullListToString"].as_str().unwrap()
    );

    let message = SharedValue::from(MessageExt::default());
    let messages: QueryMessageList = SharedValue::from(vec![Some(message.clone()), None]);
    let result = QueryResult::new(18, Some(messages.clone()));
    let returned = result.get_message_list().unwrap();
    assert!(returned.ptr_eq(&messages));
    assert!(returned.snapshot()[0].as_ref().unwrap().ptr_eq(&message));
    assert_eq!(
        result.to_string(),
        fixture["listToString"].as_str().unwrap()
    );

    messages.with_mut(|messages| messages.push(Some(SharedValue::from(MessageExt::default()))));
    assert_eq!(
        result.to_string(),
        fixture["toStringAfterCallerMutation"].as_str().unwrap()
    );
    assert_eq!(
        returned.snapshot().len(),
        fixture["messageCountAfterCallerMutation"].as_u64().unwrap() as usize
    );
}
