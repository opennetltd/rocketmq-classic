use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use rocketmq_classic_common::{
    message::{Message, MessageQueue},
    sysflag::message::CompressionType,
};
use rocketmq_classic_core::route::TopicRouteDataHandle;
use rocketmq_classic_producer::{
    AsyncSenderExecutor, AsyncSenderTask, CheckForbiddenContext, CheckForbiddenHook,
    CommunicationMode, DEFAULT_PRODUCER_GROUP, FaultItem, FaultItemCompareError,
    ProduceAccumulator, Producer, ProducerConfig, ProducerError, ProducerPublishQueueList,
    ProducerQueueIndex, ProducerServiceState, ProducerTopicList, PublishInfoQueueFilter,
    SendMessageContext, SendMessageHook, SendMessageType, TopicPublishInfo,
    TopicPublishInfoTableError,
    validation::{
        check_broker_config, check_topic_config, validate_group, validate_message,
        validate_send_topic, validate_system_topic, validate_topic,
    },
};
use rocketmq_classic_protocol::route::{QueueData, TopicRouteData};

type ProducerConstructorCase<'a> = (
    &'a str,
    &'a str,
    Option<&'a str>,
    Option<Vec<&'a str>>,
    bool,
    Option<&'a str>,
    bool,
);

#[test]
fn java_default_settings_are_present() {
    let config = ProducerConfig::new("producer-test").unwrap();
    assert_eq!(config.producer_group(), Some("producer-test"));
    assert_eq!(config.create_topic_key(), "TBW102");
    assert_eq!(config.default_topic_queue_nums(), 4);
    assert_eq!(config.send_timeout_millis(), 3_000);
    assert_eq!(config.send_msg_max_timeout_per_request(), -1);
    assert_eq!(config.compress_threshold(), 4_096);
    assert_eq!(config.compress_type(), CompressionType::Zlib);
    assert_eq!(config.compress_level(), 5);
    assert_eq!(config.retry_times_when_send_failed(), 2);
    assert_eq!(config.retry_times_when_send_async_failed(), 2);
    assert!(!config.retry_another_broker_when_not_store_ok());
    assert_eq!(config.max_message_size(), 4 * 1024 * 1024);
}

#[test]
fn latency_fault_item_public_surface_and_behavior_match_invoked_java() {
    use std::{
        any::Any,
        cmp::Ordering,
        hash::{Hash, Hasher},
        time::{SystemTime, UNIX_EPOCH},
    };

    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let api = &fixture["faultItemApi"];
    assert_eq!(
        api["className"],
        "org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem"
    );
    assert_eq!(api["declaredPublicMethodCount"], 15);
    assert_eq!(api["publicMethodCount"], 15);
    let java_methods = api["declaredPublicMethods"].as_array().unwrap();
    for signature in [
        "boolean org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.equals(java.lang.Object)",
        "boolean org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.isAvailable()",
        "boolean org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.isReachable()",
        "int org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.compareTo(java.lang.Object)",
        "int org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.compareTo(org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem)",
        "int org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.hashCode()",
        "java.lang.String org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.getName()",
        "java.lang.String org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.toString()",
        "long org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.getCurrentLatency()",
        "long org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.getStartTimestamp()",
        "void org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.setCheckStamp(long)",
        "void org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.setCurrentLatency(long)",
        "void org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.setReachable(boolean)",
        "void org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.setStartTimestamp(long)",
        "void org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem.updateNotAvailableDuration(long)",
    ] {
        assert!(
            java_methods.iter().any(|method| method == signature),
            "missing pinned Java FaultItem method {signature}"
        );
    }
    assert_eq!(
        api["publicConstructors"][0],
        "org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl$FaultItem(org.apache.rocketmq.client.latency.LatencyFaultToleranceImpl,java.lang.String)"
    );

    let expected = &fixture["faultItemBehavior"];
    let item = FaultItem::new(Some("broker-a".to_owned()));
    assert_eq!(item.get_name(), expected["defaultName"].as_str());
    assert_eq!(
        item.get_current_latency(),
        expected["defaultCurrentLatency"]
    );
    assert_eq!(
        item.get_start_timestamp(),
        expected["defaultStartTimestamp"]
    );
    assert_eq!(item.is_available(), expected["defaultAvailable"]);
    assert_eq!(item.is_reachable(), expected["defaultReachable"]);
    assert_eq!(item.java_to_string(), expected["defaultToString"]);

    let epoch_millis = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    let before_update = epoch_millis();
    item.update_not_available_duration(10_000);
    assert_eq!(
        item.get_start_timestamp() > before_update,
        expected["positiveDurationMovedStartForward"]
    );
    assert_eq!(
        !item.is_available(),
        expected["positiveDurationMakesUnavailable"]
    );
    let unavailable_until = item.get_start_timestamp();
    item.update_not_available_duration(0);
    assert_eq!(
        item.get_start_timestamp() == unavailable_until,
        expected["zeroDurationRetainsStart"]
    );
    item.update_not_available_duration(-1);
    assert_eq!(
        item.get_start_timestamp() == unavailable_until,
        expected["negativeDurationRetainsStart"]
    );
    let overflow_duration = FaultItem::new(Some("overflow".to_owned()));
    overflow_duration.update_not_available_duration(i64::MAX);
    assert_eq!(
        overflow_duration.get_start_timestamp() == 0,
        expected["overflowDurationRetainsStart"]
    );
    item.set_start_timestamp(i64::MAX);
    item.update_not_available_duration(1);
    assert_eq!(
        item.get_start_timestamp() == i64::MAX,
        expected["shorterDurationDoesNotMoveStart"]
    );

    let equal = FaultItem::new(Some("same".to_owned()));
    equal.set_current_latency(50);
    equal.set_start_timestamp(700);
    equal.set_reachable(true);
    equal.set_check_stamp(1);
    let equal_different_flags = FaultItem::new(Some("same".to_owned()));
    equal_different_flags.set_current_latency(50);
    equal_different_flags.set_start_timestamp(700);
    equal_different_flags.set_reachable(false);
    equal_different_flags.set_check_stamp(i64::MAX);
    assert!(equal.java_equals(Some(&equal_different_flags as &dyn Any)));
    assert!(equal.java_equals(Some(&equal as &dyn Any)));
    assert!(!equal.java_equals(None));
    assert!(!equal.java_equals(Some(&"same" as &dyn Any)));
    assert!(equal == equal_different_flags);
    assert_eq!(
        equal == equal_different_flags,
        expected["equalAcrossOwners"]
    );
    assert_eq!(
        equal.java_hash_code(),
        expected["equalHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        equal.java_hash_code() == equal_different_flags.java_hash_code(),
        expected["equalHashCodes"]
    );
    assert_eq!(
        equal == equal_different_flags,
        expected["equalityIgnoresReachableAndCheckStamp"]
    );
    assert_eq!(
        !equal.java_equals(Some(
            &FaultItem::new(Some("different".to_owned())) as &dyn Any
        )),
        expected["differentNameNotEqual"]
    );
    let different_latency = FaultItem::new(Some("same".to_owned()));
    different_latency.set_current_latency(51);
    different_latency.set_start_timestamp(700);
    assert_eq!(
        !equal.java_equals(Some(&different_latency as &dyn Any)),
        expected["differentLatencyNotEqual"]
    );
    let different_start = FaultItem::new(Some("same".to_owned()));
    different_start.set_current_latency(50);
    different_start.set_start_timestamp(701);
    assert_eq!(
        !equal.java_equals(Some(&different_start as &dyn Any)),
        expected["differentStartNotEqual"]
    );
    assert_eq!(equal.java_to_string(), expected["equalToString"]);
    assert_eq!(
        equal.java_compare_to_nullable(Some(&equal_different_flags)),
        Ok(Ordering::Equal)
    );
    let mut equal_hasher = std::collections::hash_map::DefaultHasher::new();
    let mut other_hasher = std::collections::hash_map::DefaultHasher::new();
    equal.hash(&mut equal_hasher);
    equal_different_flags.hash(&mut other_hasher);
    assert_eq!(equal_hasher.finish(), other_hasher.finish());

    let available = FaultItem::new(Some("available".to_owned()));
    available.set_current_latency(10_000);
    available.set_start_timestamp(i64::MIN);
    let unavailable = FaultItem::new(Some("unavailable".to_owned()));
    unavailable.set_current_latency(0);
    unavailable.set_start_timestamp(i64::MAX);
    let order_code = |ordering| match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };
    assert_eq!(
        order_code(
            available
                .java_compare_to_nullable(Some(&unavailable))
                .unwrap()
        ),
        expected["availabilityPrecedesLatency"]
    );
    assert_eq!(
        order_code(
            unavailable
                .java_compare_to_nullable(Some(&available))
                .unwrap()
        ),
        expected["unavailableFollowsAvailable"]
    );

    let low_latency = FaultItem::new(Some("low-latency".to_owned()));
    low_latency.set_current_latency(-5);
    low_latency.set_start_timestamp(i64::MIN);
    let high_latency = FaultItem::new(Some("high-latency".to_owned()));
    high_latency.set_current_latency(5);
    high_latency.set_start_timestamp(i64::MIN);
    assert_eq!(
        order_code(
            low_latency
                .java_compare_to_nullable(Some(&high_latency))
                .unwrap()
        ),
        expected["signedLatencyOrder"]
    );
    let tie_different_name = FaultItem::new(Some("different-name".to_owned()));
    tie_different_name.set_current_latency(5);
    tie_different_name.set_start_timestamp(i64::MIN);
    assert_eq!(
        order_code(
            low_latency
                .java_compare_to_nullable(Some(&tie_different_name))
                .unwrap()
        ),
        expected["comparisonIgnoresName"]
    );

    let early_start = FaultItem::new(Some("early".to_owned()));
    early_start.set_current_latency(5);
    early_start.set_start_timestamp(10);
    let late_start = FaultItem::new(Some("late".to_owned()));
    late_start.set_current_latency(5);
    late_start.set_start_timestamp(20);
    assert_eq!(
        order_code(
            early_start
                .java_compare_to_nullable(Some(&late_start))
                .unwrap()
        ),
        expected["startTimestampOrder"]
    );

    assert_eq!(
        item.java_compare_to_nullable(None),
        Err(FaultItemCompareError::NullOther)
    );
    assert_eq!(
        item.java_compare_to_object(None),
        Err(FaultItemCompareError::NullOther)
    );
    assert_eq!(
        item.java_compare_to_object(Some(&"wrong type" as &dyn Any)),
        Err(FaultItemCompareError::WrongType)
    );
    assert_eq!(
        FaultItemCompareError::NullOther.to_string(),
        expected["nullCompareFailure"].as_str().unwrap()
    );
    assert_eq!(
        FaultItemCompareError::WrongType.to_string(),
        expected["wrongTypeComparableFailure"].as_str().unwrap()
    );
    assert_eq!(
        FaultItemCompareError::NullOther.java_exception_name(),
        "java.lang.NullPointerException"
    );
    assert_eq!(
        FaultItemCompareError::WrongType.java_exception_name(),
        "java.lang.ClassCastException"
    );
    assert_eq!(
        FaultItem::new(None).get_name(),
        expected["nullName"].as_str()
    );
    let null_name = FaultItem::new(None);
    let other_null_name = FaultItem::new(None);
    assert_eq!(null_name, other_null_name);
    assert_eq!(
        null_name.java_hash_code(),
        expected["nullNameHash"].as_i64().unwrap() as i32
    );
    assert_eq!(
        FaultItem::new(Some("A😀".to_owned())).java_hash_code(),
        expected["unicodeNameHash"].as_i64().unwrap() as i32
    );
    let extreme_hash = FaultItem::new(Some("hash".to_owned()));
    extreme_hash.set_current_latency(i64::MIN);
    extreme_hash.set_start_timestamp(i64::MAX);
    assert_eq!(
        extreme_hash.java_hash_code(),
        expected["extremeLongHash"].as_i64().unwrap() as i32
    );
}

#[test]
fn pinned_java_default_producer_impl_public_surface_is_recorded() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let inventory = &fixture["producerImplementationApi"];
    let methods = inventory["publicMethods"].as_array().unwrap();
    let constructors = inventory["publicConstructors"].as_array().unwrap();

    assert_eq!(
        inventory["className"],
        "org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl"
    );
    assert_eq!(inventory["publicMethodCount"].as_u64(), Some(82));
    assert_eq!(methods.len(), 82);
    assert_eq!(constructors.len(), 2);
    assert!(constructors.iter().any(|constructor| {
        constructor
            == "org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl(org.apache.rocketmq.client.producer.DefaultMQProducer,org.apache.rocketmq.remoting.RPCHook)"
    }));
    for method in [
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.hasCheckForbiddenHook()",
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.hasSendMessageHook()",
        "java.util.concurrent.ConcurrentMap org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getTopicPublishInfoTable()",
        "org.apache.rocketmq.common.ServiceState org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getServiceState()",
        "void org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.setServiceState(org.apache.rocketmq.common.ServiceState)",
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.isUnitMode()",
        "org.apache.rocketmq.common.message.MessageQueue org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.invokeMessageQueueSelector(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.client.producer.MessageQueueSelector,java.lang.Object,long)",
        "java.util.Set org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getPublishTopicList()",
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.isPublishTopicNeedUpdate(java.lang.String)",
        "void org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.updateTopicPublishInfo(java.lang.String,org.apache.rocketmq.client.impl.producer.TopicPublishInfo)",
        "org.apache.rocketmq.client.producer.TransactionCheckListener org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.checkListener()",
        "org.apache.rocketmq.client.producer.TransactionListener org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getCheckListener()",
        "void org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.checkTransactionState(java.lang.String,org.apache.rocketmq.common.message.MessageExt,org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getDefaultMQProducer()",
        "org.apache.rocketmq.client.latency.MQFaultStrategy org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getMqFaultStrategy()",
        "org.apache.rocketmq.client.impl.factory.MQClientInstance org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getMqClientFactory()",
        "org.apache.rocketmq.client.impl.factory.MQClientInstance org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getmQClientFactory()",
    ] {
        assert!(
            methods.iter().any(|candidate| candidate == method),
            "missing {method}"
        );
    }
}

#[test]
fn pinned_java_topic_publish_info_surface_has_phase_crosswalks() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let inventory = &fixture["topicPublishInfoApi"];
    let methods = inventory["publicMethods"].as_array().unwrap();
    let crosswalks = include_str!("../../../docs/compatibility/PHASE_7_PRODUCER_MATRIX.md");

    assert_eq!(
        inventory["className"],
        "org.apache.rocketmq.client.impl.producer.TopicPublishInfo"
    );
    assert_eq!(inventory["publicMethodCount"].as_u64(), Some(17));
    assert_eq!(methods.len(), 17);
    assert_eq!(inventory["declaredPublicMethodCount"].as_u64(), Some(17));
    let constructors = inventory["publicConstructors"].as_array().unwrap();
    assert_eq!(constructors.len(), 1);
    assert_eq!(
        constructors[0].as_str(),
        Some("org.apache.rocketmq.client.impl.producer.TopicPublishInfo()")
    );
    for expected_signature in [
        "boolean org.apache.rocketmq.client.impl.producer.TopicPublishInfo.isHaveTopicRouterInfo()",
        "boolean org.apache.rocketmq.client.impl.producer.TopicPublishInfo.isOrderTopic()",
        "boolean org.apache.rocketmq.client.impl.producer.TopicPublishInfo.ok()",
        "int org.apache.rocketmq.client.impl.producer.TopicPublishInfo.getWriteQueueNumsByBroker(java.lang.String)",
        "java.lang.String org.apache.rocketmq.client.impl.producer.TopicPublishInfo.toString()",
        "java.util.List org.apache.rocketmq.client.impl.producer.TopicPublishInfo.getMessageQueueList()",
        "org.apache.rocketmq.client.common.ThreadLocalIndex org.apache.rocketmq.client.impl.producer.TopicPublishInfo.getSendWhichQueue()",
        "org.apache.rocketmq.common.message.MessageQueue org.apache.rocketmq.client.impl.producer.TopicPublishInfo.selectOneMessageQueue()",
        "org.apache.rocketmq.common.message.MessageQueue org.apache.rocketmq.client.impl.producer.TopicPublishInfo.selectOneMessageQueue([Lorg.apache.rocketmq.client.impl.producer.TopicPublishInfo$QueueFilter;)",
        "org.apache.rocketmq.common.message.MessageQueue org.apache.rocketmq.client.impl.producer.TopicPublishInfo.selectOneMessageQueue(java.lang.String)",
        "org.apache.rocketmq.remoting.protocol.route.TopicRouteData org.apache.rocketmq.client.impl.producer.TopicPublishInfo.getTopicRouteData()",
        "void org.apache.rocketmq.client.impl.producer.TopicPublishInfo.resetIndex()",
        "void org.apache.rocketmq.client.impl.producer.TopicPublishInfo.setHaveTopicRouterInfo(boolean)",
        "void org.apache.rocketmq.client.impl.producer.TopicPublishInfo.setMessageQueueList(java.util.List)",
        "void org.apache.rocketmq.client.impl.producer.TopicPublishInfo.setOrderTopic(boolean)",
        "void org.apache.rocketmq.client.impl.producer.TopicPublishInfo.setSendWhichQueue(org.apache.rocketmq.client.common.ThreadLocalIndex)",
        "void org.apache.rocketmq.client.impl.producer.TopicPublishInfo.setTopicRouteData(org.apache.rocketmq.remoting.protocol.route.TopicRouteData)",
    ] {
        assert!(
            methods
                .iter()
                .any(|candidate| candidate.as_str() == Some(expected_signature)),
            "missing pinned Java TopicPublishInfo signature: {expected_signature}"
        );
    }
    for signature in methods {
        let signature = signature.as_str().unwrap();
        let method_name = signature
            .split('(')
            .next()
            .unwrap()
            .rsplit('.')
            .next()
            .unwrap();
        assert!(
            crosswalks.contains(&format!("TopicPublishInfo.{method_name}")),
            "Java TopicPublishInfo method has no Phase 7 crosswalk: {signature}"
        );
    }
}

#[test]
fn pinned_java_topic_publish_info_queue_filter_surface_has_phase_crosswalks() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let inventory = &fixture["topicPublishInfoQueueFilterApi"];
    let methods = inventory["publicMethods"].as_array().unwrap();
    let matrix = include_str!("../../../docs/compatibility/PHASE_7_PRODUCER_MATRIX.md");

    assert_eq!(
        inventory["className"],
        "org.apache.rocketmq.client.impl.producer.TopicPublishInfo$QueueFilter"
    );
    assert_eq!(inventory["publicMethodCount"].as_u64(), Some(1));
    assert_eq!(methods.len(), 1);
    assert!(
        inventory["publicConstructors"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let signature = "boolean org.apache.rocketmq.client.impl.producer.TopicPublishInfo$QueueFilter.filter(org.apache.rocketmq.common.message.MessageQueue)";
    assert!(
        methods
            .iter()
            .any(|candidate| candidate.as_str() == Some(signature))
    );
    assert!(matrix.contains("TopicPublishInfo.QueueFilter.filter"));
}

#[test]
fn pinned_java_thread_local_index_surface_and_format_match_rust() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["threadLocalIndexApi"];
    let inventory = &expected["publicApi"];
    let methods = inventory["publicMethods"].as_array().unwrap();
    let matrix = include_str!("../../../docs/compatibility/PHASE_7_PRODUCER_MATRIX.md");

    assert_eq!(
        inventory["className"],
        "org.apache.rocketmq.client.common.ThreadLocalIndex"
    );
    assert_eq!(inventory["publicMethodCount"].as_u64(), Some(3));
    assert_eq!(methods.len(), 3);
    let constructors = inventory["publicConstructors"].as_array().unwrap();
    assert_eq!(constructors.len(), 1);
    assert_eq!(
        constructors[0].as_str(),
        Some("org.apache.rocketmq.client.common.ThreadLocalIndex()")
    );
    for expected_signature in [
        "int org.apache.rocketmq.client.common.ThreadLocalIndex.incrementAndGet()",
        "java.lang.String org.apache.rocketmq.client.common.ThreadLocalIndex.toString()",
        "void org.apache.rocketmq.client.common.ThreadLocalIndex.reset()",
    ] {
        assert!(
            methods
                .iter()
                .any(|candidate| candidate.as_str() == Some(expected_signature)),
            "missing pinned Java ThreadLocalIndex signature: {expected_signature}"
        );
    }
    for signature in methods {
        let signature = signature.as_str().unwrap();
        let method_name = signature
            .split('(')
            .next()
            .unwrap()
            .rsplit('.')
            .next()
            .unwrap();
        assert!(
            matrix.contains(&format!("ThreadLocalIndex.{method_name}")),
            "Java ThreadLocalIndex method has no Phase 7 crosswalk: {signature}"
        );
    }

    let parse_index = |value: &str| {
        value
            .strip_prefix("ThreadLocalIndex{threadLocalIndex=")
            .unwrap()
            .strip_suffix('}')
            .unwrap()
            .parse::<i32>()
            .unwrap()
    };
    let index = ProducerQueueIndex::default();
    assert_eq!(
        index.to_string(),
        expected["initialString"].as_str().unwrap()
    );
    assert!(expected["incrementedNonnegative"].as_bool().unwrap());
    assert!(expected["incrementMatchesMask"].as_bool().unwrap());
    let incremented = index.increment_and_get();
    assert!(incremented >= 0);
    assert_eq!(parse_index(&index.to_string()) & i32::MAX, incremented);
    index.reset();
    assert!(expected["resetValueNonnegative"].as_bool().unwrap());
    assert!(parse_index(&index.to_string()) >= 0);

    assert_eq!(expected["overflowFirst"].as_i64(), Some(0));
    assert_eq!(expected["overflowSecond"].as_i64(), Some(1));
    let java_overflow_value = parse_index(expected["afterOverflowString"].as_str().unwrap());
    assert_eq!(java_overflow_value & i32::MAX, 1);
}

#[test]
fn pinned_java_producer_impl_method_names_have_phase_crosswalks() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let methods = fixture["producerImplementationApi"]["publicMethods"]
        .as_array()
        .unwrap();
    let crosswalks = [
        include_str!("../../../docs/compatibility/PHASE_7_PRODUCER_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_14_TRANSACTION_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_15_REQUEST_REPLY_MATRIX.md"),
        include_str!(
            "../../../docs/compatibility/PHASE_16_NAMESPACE_SECURITY_OPERATIONS_MATRIX.md"
        ),
        include_str!("../../../docs/compatibility/PHASE_17_ADMIN_MATRIX.md"),
    ]
    .join("\n");

    for signature in methods {
        let signature = signature.as_str().unwrap();
        let method_name = signature
            .split('(')
            .next()
            .unwrap()
            .rsplit('.')
            .next()
            .unwrap();
        assert!(
            crosswalks.contains(method_name),
            "Java implementation method has no phase crosswalk: {signature}"
        );
    }
}

#[test]
fn pinned_java_producer_callable_method_names_have_phase_crosswalks() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let surfaces = &fixture["producerFacadeApi"];
    let crosswalks = [
        include_str!("../../../docs/compatibility/PHASE_7_PRODUCER_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_14_TRANSACTION_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_15_REQUEST_REPLY_MATRIX.md"),
        include_str!(
            "../../../docs/compatibility/PHASE_16_NAMESPACE_SECURITY_OPERATIONS_MATRIX.md"
        ),
        include_str!("../../../docs/compatibility/PHASE_17_ADMIN_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_6_API_CONFIG_MAP.md"),
    ]
    .join("\n");

    for (owner, expected_count) in [
        ("DefaultMQProducer", 183),
        ("MQProducer", 42),
        ("MQAdmin", 8),
    ] {
        let methods = surfaces[owner]["publicMethods"].as_array().unwrap();
        assert_eq!(
            methods.len(),
            expected_count,
            "unexpected {owner} method count"
        );
        for signature in methods {
            let signature = signature.as_str().unwrap();
            let method_name = signature
                .split('(')
                .next()
                .unwrap()
                .rsplit('.')
                .next()
                .unwrap();
            assert!(
                crosswalks.contains(method_name),
                "Java facade/inherited method has no phase crosswalk: {signature}"
            );
        }
    }
}

#[test]
fn rust_publish_info_method_names_have_java_crosswalks() {
    const RUST_TO_JAVA: &[(&str, &str)] = &[
        (
            "TopicPublishInfo::is_order_topic",
            "TopicPublishInfo.isOrderTopic",
        ),
        (
            "TopicPublishInfo::set_order_topic",
            "TopicPublishInfo.setOrderTopic",
        ),
        ("TopicPublishInfo::ok", "TopicPublishInfo.ok"),
        (
            "TopicPublishInfo::message_queue_list",
            "TopicPublishInfo.getMessageQueueList",
        ),
        (
            "TopicPublishInfo::set_message_queue_list",
            "TopicPublishInfo.setMessageQueueList",
        ),
        (
            "TopicPublishInfo::send_which_queue",
            "TopicPublishInfo.getSendWhichQueue",
        ),
        (
            "TopicPublishInfo::set_send_which_queue",
            "TopicPublishInfo.setSendWhichQueue",
        ),
        (
            "TopicPublishInfo::have_topic_router_info",
            "TopicPublishInfo.isHaveTopicRouterInfo",
        ),
        (
            "TopicPublishInfo::set_have_topic_router_info",
            "TopicPublishInfo.setHaveTopicRouterInfo",
        ),
        (
            "TopicPublishInfo::select_one_message_queue",
            "TopicPublishInfo.selectOneMessageQueue()",
        ),
        (
            "TopicPublishInfo::select_one_message_queue_with_nullable_filters",
            "TopicPublishInfo.selectOneMessageQueue(QueueFilter...)",
        ),
        (
            "TopicPublishInfo::select_one_message_queue_avoiding",
            "TopicPublishInfo.selectOneMessageQueue(String lastBrokerName)",
        ),
        (
            "TopicPublishInfo::reset_index",
            "TopicPublishInfo.resetIndex",
        ),
        (
            "TopicPublishInfo::get_write_queue_nums_by_broker",
            "TopicPublishInfo.getWriteQueueNumsByBroker",
        ),
        (
            "Display for TopicPublishInfo",
            "TopicPublishInfo.toString()",
        ),
        (
            "TopicPublishInfo::topic_route_data",
            "TopicPublishInfo.getTopicRouteData",
        ),
        (
            "TopicPublishInfo::set_topic_route_data",
            "TopicPublishInfo.setTopicRouteData",
        ),
        (
            "PublishInfoQueueFilter",
            "TopicPublishInfo.QueueFilter.filter(MessageQueue)",
        ),
        (
            "NullablePublishInfoQueueFilters",
            "TopicPublishInfo.selectOneMessageQueue(QueueFilter...)",
        ),
        (
            "ProducerQueueIndex::increment_and_get",
            "ThreadLocalIndex.incrementAndGet()",
        ),
        ("ProducerQueueIndex::reset", "ThreadLocalIndex.reset()"),
        (
            "Display for ProducerQueueIndex",
            "ThreadLocalIndex.toString()",
        ),
        (
            "TopicPublishInfoTable::replace_all_with_nullable_callback",
            "Map.replaceAll(BiFunction)",
        ),
        (
            "TopicPublishInfoKeySetView::remove_if_with_nullable_callback",
            "Map.keySet().removeIf(Predicate)",
        ),
        (
            "TopicPublishInfoValuesView::remove_if_with_nullable_callback",
            "Map.values().removeIf(Predicate)",
        ),
        (
            "TopicPublishInfoEntrySetView::remove_if_with_nullable_callback",
            "Map.entrySet().removeIf(Predicate)",
        ),
        (
            "TopicPublishInfoValuesView::remove_all_iter",
            "Map.values().removeAll(Collection)",
        ),
    ];
    let matrix = include_str!("../../../docs/compatibility/PHASE_7_PRODUCER_MATRIX.md");

    for (rust_api, java_api) in RUST_TO_JAVA {
        assert!(
            matrix
                .lines()
                .any(|line| line.contains(rust_api) && line.contains(java_api)),
            "Rust Producer API has no Java crosswalk: {rust_api} -> {java_api}"
        );
    }
}

#[test]
fn implementation_raw_semaphore_setters_match_pinned_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationSemaphoreSettings"];
    let producer =
        Producer::new(ProducerConfig::new("producer-implementation-semaphores").unwrap());

    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        expected["initialCountAvailable"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        expected["initialByteAvailable"].as_i64().unwrap() as i32
    );
    producer.set_semaphore_async_send_num(7);
    assert_eq!(producer.semaphore_async_send_num_available_permits(), 7);
    assert_eq!(producer.backpressure_async_send_num(), 1024);
    producer.set_semaphore_async_send_num(-3);
    assert_eq!(
        producer.semaphore_async_send_num_available_permits(),
        expected["negativeCountAvailable"].as_i64().unwrap() as i32
    );
    producer.set_semaphore_async_send_size(4096);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        expected["byteAvailableAfterRawReplace"].as_i64().unwrap() as i32
    );
    producer.set_semaphore_async_send_size(-8192);
    assert_eq!(
        producer.semaphore_async_send_size_available_permits(),
        expected["negativeByteAvailable"].as_i64().unwrap() as i32
    );
    assert_eq!(producer.backpressure_async_send_size(), 100 * 1024 * 1024);
}

#[test]
fn pinned_java_default_producer_facade_and_inherited_interfaces_are_recorded() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let surfaces = &fixture["producerFacadeApi"];
    let producer = &surfaces["DefaultMQProducer"];
    let producer_methods = producer["publicMethods"].as_array().unwrap();

    assert_eq!(
        producer["className"],
        "org.apache.rocketmq.client.producer.DefaultMQProducer"
    );
    assert_eq!(producer["publicMethodCount"].as_u64(), Some(183));
    assert_eq!(producer_methods.len(), 183);
    assert_eq!(producer["declaredPublicMethodCount"].as_u64(), Some(107));
    assert_eq!(
        producer["declaredPublicMethods"].as_array().unwrap().len(),
        107
    );
    let constructors = producer["publicConstructors"].as_array().unwrap();
    assert_eq!(constructors.len(), 11);
    for constructor in [
        "org.apache.rocketmq.client.producer.DefaultMQProducer()",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,java.lang.String,org.apache.rocketmq.remoting.RPCHook)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,java.lang.String,org.apache.rocketmq.remoting.RPCHook,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook,java.util.List)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook,java.util.List,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(org.apache.rocketmq.remoting.RPCHook)",
    ] {
        assert!(
            constructors
                .iter()
                .any(|candidate| candidate.as_str() == Some(constructor)),
            "missing constructor {constructor}"
        );
    }
    for method in [
        "void org.apache.rocketmq.client.ClientConfig.setNamesrvAddr(java.lang.String)",
        "org.apache.rocketmq.client.producer.SendResult org.apache.rocketmq.client.producer.DefaultMQProducer.send(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.client.producer.MessageQueueSelector,java.lang.Object)",
    ] {
        assert!(
            producer_methods
                .iter()
                .any(|candidate| candidate.as_str() == Some(method)),
            "missing {method}"
        );
    }
    let declared_methods = producer["declaredPublicMethods"].as_array().unwrap();
    for method in [
        "org.apache.rocketmq.client.producer.SendResult org.apache.rocketmq.client.producer.DefaultMQProducer.sendDirect(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.common.message.MessageQueue,org.apache.rocketmq.client.producer.SendCallback)",
        "org.apache.rocketmq.client.producer.SendResult org.apache.rocketmq.client.producer.DefaultMQProducer.sendByAccumulator(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.common.message.MessageQueue,org.apache.rocketmq.client.producer.SendCallback)",
    ] {
        assert!(
            declared_methods
                .iter()
                .any(|candidate| candidate.as_str() == Some(method)),
            "missing {method}"
        );
    }

    let mq_producer = &surfaces["MQProducer"];
    assert_eq!(
        mq_producer["className"],
        "org.apache.rocketmq.client.producer.MQProducer"
    );
    assert_eq!(mq_producer["publicMethodCount"].as_u64(), Some(42));
    assert_eq!(mq_producer["publicMethods"].as_array().unwrap().len(), 42);
    assert_eq!(mq_producer["declaredPublicMethodCount"].as_u64(), Some(34));
    assert_eq!(
        mq_producer["declaredPublicMethods"]
            .as_array()
            .unwrap()
            .len(),
        34
    );
    assert!(
        mq_producer["publicConstructors"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(mq_producer["publicMethods"].as_array().unwrap().iter().any(|candidate| {
        candidate.as_str()
            == Some("long org.apache.rocketmq.client.MQAdmin.maxOffset(org.apache.rocketmq.common.message.MessageQueue)")
    }));
    let mq_admin = &surfaces["MQAdmin"];
    assert_eq!(mq_admin["className"], "org.apache.rocketmq.client.MQAdmin");
    assert_eq!(mq_admin["publicMethodCount"].as_u64(), Some(8));
    assert_eq!(mq_admin["publicMethods"].as_array().unwrap().len(), 8);
    assert_eq!(mq_admin["declaredPublicMethodCount"].as_u64(), Some(8));
    assert_eq!(
        mq_admin["declaredPublicMethods"].as_array().unwrap().len(),
        8
    );
    assert!(
        mq_admin["publicConstructors"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn pinned_java_producer_constructor_values_map_to_rust_configuration() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let java_cases = fixture["producerConstructorCases"]["cases"]
        .as_array()
        .unwrap();
    assert_eq!(
        fixture["producerConstructorCases"]["count"].as_u64(),
        Some(11)
    );
    assert_eq!(java_cases.len(), 11);

    let cases: [ProducerConstructorCase<'_>; 11] = [
        (
            "no-arg",
            DEFAULT_PRODUCER_GROUP,
            None,
            None,
            false,
            None,
            false,
        ),
        (
            "rpc-hook",
            DEFAULT_PRODUCER_GROUP,
            None,
            None,
            false,
            None,
            true,
        ),
        ("group", "constructor-group", None, None, false, None, false),
        (
            "group-rpc-hook",
            "constructor-group-hook",
            None,
            None,
            false,
            None,
            true,
        ),
        (
            "group-rpc-hook-topics",
            "constructor-group-topics",
            None,
            Some(vec!["constructor-topic-a", "constructor-topic-b"]),
            false,
            None,
            true,
        ),
        (
            "group-trace",
            "constructor-group-trace",
            None,
            None,
            true,
            Some("constructor-trace-topic"),
            false,
        ),
        (
            "group-rpc-hook-trace",
            "constructor-group-hook-trace",
            None,
            None,
            true,
            Some("constructor-hook-trace-topic"),
            true,
        ),
        (
            "group-rpc-hook-topics-trace",
            "constructor-group-full",
            None,
            Some(vec!["constructor-topic-a", "constructor-topic-b"]),
            true,
            Some("constructor-full-trace-topic"),
            true,
        ),
        (
            "namespace-group",
            "constructor-legacy-group",
            Some("constructor-namespace"),
            None,
            false,
            None,
            false,
        ),
        (
            "namespace-group-rpc-hook",
            "constructor-legacy-hook-group",
            Some("constructor-namespace-hook"),
            None,
            false,
            None,
            true,
        ),
        (
            "namespace-group-rpc-hook-trace",
            "constructor-legacy-trace-group",
            Some("constructor-namespace-trace"),
            None,
            true,
            Some("constructor-legacy-trace-topic"),
            true,
        ),
    ];

    for ((name, group, namespace, topics, trace_enabled, trace_topic, rpc_hook_supplied), java) in
        cases.into_iter().zip(java_cases)
    {
        assert_eq!(java["name"].as_str(), Some(name));
        assert_eq!(java["producerGroupIsNull"].as_bool(), Some(false));
        assert_eq!(java["producerGroup"].as_str(), Some(group));
        assert_eq!(java["namespaceIsNull"].as_bool(), Some(namespace.is_none()));
        assert_eq!(
            java["namespace"].as_str(),
            Some(namespace.unwrap_or_default())
        );
        assert_eq!(java["rpcHookSupplied"].as_bool(), Some(rpc_hook_supplied));
        assert_eq!(
            java["rpcHookRetainedByFacade"].as_bool(),
            Some(rpc_hook_supplied)
        );
        assert_eq!(
            java["rpcHookRetainedByImplementation"].as_bool(),
            Some(rpc_hook_supplied)
        );
        assert_eq!(java["topicsIsNull"].as_bool(), Some(topics.is_none()));
        assert_eq!(
            java["topicsInputIdentityRetained"].as_bool(),
            Some(topics.is_some())
        );
        assert_eq!(java["enableTrace"].as_bool(), Some(trace_enabled));
        assert_eq!(
            java["traceTopicIsNull"].as_bool(),
            Some(trace_topic.is_none())
        );
        assert_eq!(
            java["traceTopic"].as_str(),
            Some(trace_topic.unwrap_or_default())
        );

        let mut config = ProducerConfig::new(group).unwrap();
        config
            .client_config_mut()
            .set_namespace(namespace.map(str::to_owned));
        let config_topics = topics.clone().map(|topics| {
            ProducerTopicList::from(topics.into_iter().map(str::to_owned).collect::<Vec<_>>())
        });
        let caller_topics_alias = config_topics.clone();
        config.set_topics(config_topics);
        config.client_config_mut().set_enable_trace(trace_enabled);
        config
            .client_config_mut()
            .set_trace_topic(trace_topic.map(str::to_owned));

        assert_eq!(config.producer_group(), Some(group));
        assert_eq!(
            config.client_config_mut().get_namespace().unwrap(),
            namespace
        );
        assert_eq!(
            config.topics().map(|topics| topics.snapshot()),
            topics.map(|topics| topics.into_iter().map(str::to_owned).collect::<Vec<_>>())
        );
        assert_eq!(config.client_config().enable_trace(), trace_enabled);
        assert_eq!(config.client_config().trace_topic(), trace_topic);
        let producer = Producer::new(config);
        if let Some(caller_topics_alias) = caller_topics_alias {
            caller_topics_alias.push("caller-added-topic");
            assert_eq!(
                producer
                    .topics()
                    .unwrap()
                    .snapshot()
                    .last()
                    .map(String::as_str),
                Some("caller-added-topic")
            );
        }
    }

    let nullable_cases = fixture["producerConstructorCases"]["nullableCases"]
        .as_array()
        .unwrap();
    assert_eq!(
        fixture["producerConstructorCases"]["nullableCaseCount"].as_u64(),
        Some(6)
    );
    assert_eq!(nullable_cases.len(), 6);
    let null_group = &nullable_cases[0];
    assert_eq!(null_group["name"].as_str(), Some("null-producer-group"));
    assert_eq!(null_group["producerGroupIsNull"].as_bool(), Some(true));
    assert_eq!(null_group["namespaceIsNull"].as_bool(), Some(true));
    assert_eq!(null_group["topicsIsNull"].as_bool(), Some(true));
    assert_eq!(null_group["enableTrace"].as_bool(), Some(false));
    assert_eq!(null_group["traceTopicIsNull"].as_bool(), Some(true));

    let mut null_group_config = ProducerConfig::new_nullable(None).unwrap();
    assert_eq!(null_group_config.producer_group(), None);
    assert_eq!(null_group_config.clone().producer_group(), None);

    let namespaced_null_group = &nullable_cases[1];
    assert_eq!(
        namespaced_null_group["name"].as_str(),
        Some("null-producer-group-with-namespace")
    );
    assert_eq!(
        namespaced_null_group["producerGroupIsNull"].as_bool(),
        Some(true)
    );
    null_group_config
        .client_config_mut()
        .set_namespace(Some("constructor-null-group-namespace".to_owned()));
    assert_eq!(null_group_config.producer_group(), None);
    assert_eq!(
        null_group_config
            .client_config_mut()
            .get_namespace()
            .unwrap(),
        Some("constructor-null-group-namespace")
    );

    let null_group_setter = &nullable_cases[2];
    assert_eq!(
        null_group_setter["name"].as_str(),
        Some("null-producer-group-setter")
    );
    assert_eq!(
        null_group_setter["producerGroupBeforeSetter"].as_str(),
        Some("constructor-group-before-null")
    );
    let mut setter_config = ProducerConfig::new("constructor-group-before-null").unwrap();
    assert_eq!(
        setter_config.producer_group(),
        null_group_setter["producerGroupBeforeSetter"].as_str()
    );
    setter_config.set_producer_group(None);
    assert_eq!(setter_config.producer_group(), None);

    let nullable_rust_cases = [
        (
            "null-namespace",
            "constructor-null-namespace-group",
            None,
            None,
            false,
            None,
            false,
        ),
        (
            "null-initial-topics",
            "constructor-null-topics",
            None,
            None,
            false,
            None,
            true,
        ),
        (
            "null-trace-topic",
            "constructor-null-trace-topic",
            None,
            None,
            true,
            None,
            true,
        ),
    ];
    for ((name, group, namespace, topics, trace_enabled, trace_topic, rpc_hook_supplied), java) in
        nullable_rust_cases.into_iter().zip(&nullable_cases[3..])
    {
        assert_eq!(java["name"].as_str(), Some(name));
        assert_eq!(java["producerGroupIsNull"].as_bool(), Some(false));
        assert_eq!(java["producerGroup"].as_str(), Some(group));
        assert_eq!(java["namespaceIsNull"].as_bool(), Some(namespace.is_none()));
        assert_eq!(java["rpcHookSupplied"].as_bool(), Some(rpc_hook_supplied));
        assert_eq!(
            java["rpcHookRetainedByFacade"].as_bool(),
            Some(rpc_hook_supplied)
        );
        assert_eq!(
            java["rpcHookRetainedByImplementation"].as_bool(),
            Some(rpc_hook_supplied)
        );
        assert_eq!(java["topicsIsNull"].as_bool(), Some(topics.is_none()));
        assert_eq!(java["enableTrace"].as_bool(), Some(trace_enabled));
        assert_eq!(
            java["traceTopicIsNull"].as_bool(),
            Some(trace_topic.is_none())
        );

        let mut config = ProducerConfig::new(group).unwrap();
        config
            .client_config_mut()
            .set_namespace(namespace.map(str::to_owned));
        config.set_topics(topics.map(|topics: Vec<&str>| {
            ProducerTopicList::from(topics.into_iter().map(str::to_owned).collect::<Vec<_>>())
        }));
        config.client_config_mut().set_enable_trace(trace_enabled);
        config
            .client_config_mut()
            .set_trace_topic(trace_topic.map(str::to_owned));
        assert_eq!(
            config.client_config_mut().get_namespace().unwrap(),
            namespace
        );
        assert_eq!(config.topics(), None);
        assert_eq!(config.client_config().enable_trace(), trace_enabled);
        assert_eq!(config.client_config().trace_topic(), trace_topic);
    }
}

#[test]
fn pinned_java_returned_runtime_and_fault_strategy_surfaces_are_recorded() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let client_factory = &fixture["producerClientFactoryApi"];
    assert_eq!(
        client_factory["className"],
        "org.apache.rocketmq.client.impl.factory.MQClientInstance"
    );
    assert_eq!(
        client_factory["publicConstructors"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(client_factory["publicMethodCount"].as_u64(), Some(51));
    assert_eq!(
        client_factory["publicMethods"].as_array().unwrap().len(),
        51
    );
    for method in [
        "java.lang.String org.apache.rocketmq.client.impl.factory.MQClientInstance.getClientId()",
        "org.apache.rocketmq.remoting.protocol.route.TopicRouteData org.apache.rocketmq.client.impl.factory.MQClientInstance.queryTopicRouteData(java.lang.String)",
        "boolean org.apache.rocketmq.client.impl.factory.MQClientInstance.sendHeartbeatToAllBrokerWithLock()",
        "org.apache.rocketmq.client.impl.MQClientAPIImpl org.apache.rocketmq.client.impl.factory.MQClientInstance.getMQClientAPIImpl()",
    ] {
        assert!(
            client_factory["publicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|candidate| candidate.as_str() == Some(method)),
            "missing {method}"
        );
    }

    let strategy = &fixture["mqFaultStrategyApi"];
    assert_eq!(
        strategy["className"],
        "org.apache.rocketmq.client.latency.MQFaultStrategy"
    );
    assert_eq!(strategy["publicConstructors"].as_array().unwrap().len(), 2);
    assert_eq!(strategy["declaredPublicMethodCount"].as_u64(), Some(16));
    assert_eq!(strategy["publicMethodCount"].as_u64(), Some(17));
    assert!(
        strategy["publicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|method| {
                method.as_str()
                    == Some("void org.apache.rocketmq.client.latency.MQFaultStrategy.start()")
            })
    );
    assert!(
        strategy["publicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|method| {
                method.as_str()
                    == Some("void org.apache.rocketmq.common.utils.StartAndShutdown.preShutdown()")
            })
    );

    let broker_filter = &fixture["mqFaultStrategyBrokerFilterApi"];
    assert_eq!(
        broker_filter["className"],
        "org.apache.rocketmq.client.latency.MQFaultStrategy$BrokerFilter"
    );
    assert_eq!(
        broker_filter["publicConstructors"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(broker_filter["publicMethodCount"].as_u64(), Some(2));
}

#[test]
fn implementation_hook_helpers_match_pinned_java_fixture() {
    use std::sync::Mutex;

    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationHookExecution"];
    let producer = Producer::new(ProducerConfig::new("producer-implementation-hooks").unwrap());

    assert_eq!(
        producer.has_check_forbidden_hook(),
        expected["checkForbiddenInitiallyPresent"]
    );
    assert_eq!(
        producer.has_send_message_hook(),
        expected["sendMessageInitiallyPresent"]
    );

    let check_events = Arc::new(Mutex::new(Vec::<String>::new()));
    struct CheckFirst(Arc<Mutex<Vec<String>>>);
    impl CheckForbiddenHook for CheckFirst {
        fn hook_name(&self) -> &str {
            "fixture-check-first"
        }

        fn check_forbidden(
            &self,
            context: &mut CheckForbiddenContext<'_>,
        ) -> Result<(), ProducerError> {
            self.0.lock().unwrap().push("first".into());
            context
                .message
                .set_system_property("DIRECT_CHECK_HOOK", "mutated")
                .unwrap();
            context.queue.set_queue_id(4);
            Ok(())
        }
    }
    struct CheckReject(Arc<Mutex<Vec<String>>>);
    impl CheckForbiddenHook for CheckReject {
        fn hook_name(&self) -> &str {
            "fixture-check-reject"
        }

        fn check_forbidden(
            &self,
            context: &mut CheckForbiddenContext<'_>,
        ) -> Result<(), ProducerError> {
            self.0.lock().unwrap().push(format!(
                "reject:{}",
                context
                    .message
                    .properties()
                    .get("DIRECT_CHECK_HOOK")
                    .map(String::as_str)
                    .unwrap_or("<null>")
            ));
            Err(ProducerError::ForbiddenTopic)
        }
    }
    struct CheckLast(Arc<Mutex<Vec<String>>>);
    impl CheckForbiddenHook for CheckLast {
        fn hook_name(&self) -> &str {
            "fixture-check-last"
        }

        fn check_forbidden(
            &self,
            _context: &mut CheckForbiddenContext<'_>,
        ) -> Result<(), ProducerError> {
            self.0.lock().unwrap().push("unreached".into());
            Ok(())
        }
    }

    producer.register_check_forbidden_hook(Arc::new(CheckFirst(check_events.clone())));
    producer.register_check_forbidden_hook(Arc::new(CheckReject(check_events.clone())));
    producer.register_check_forbidden_hook(Arc::new(CheckLast(check_events.clone())));
    assert_eq!(
        producer.has_check_forbidden_hook(),
        expected["checkForbiddenPresentAfterRegistration"]
    );

    let mut check_message = Message::new("orders", [1]);
    let mut check_queue = MessageQueue::new("orders", "broker-a", 1);
    let check_result = {
        let mut context = CheckForbiddenContext {
            name_srv_addr: None,
            group: Some("producer-implementation-hooks".into()),
            message: &mut check_message,
            queue: &mut check_queue,
            broker_addr: "127.0.0.1:10911".into(),
            communication_mode: CommunicationMode::Sync,
            send_result: None,
            exception: None,
            arg: None,
            unit_mode: false,
        };
        producer.execute_check_forbidden_hook(&mut context)
    };
    assert!(check_result.is_err());
    assert!(expected["checkFailure"].is_string());
    assert_eq!(
        *check_events.lock().unwrap(),
        expected["checkEvents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        check_message
            .properties()
            .get("DIRECT_CHECK_HOOK")
            .map(String::as_str),
        expected["checkMessageMarker"].as_str()
    );
    assert_eq!(
        check_queue.queue_id(),
        expected["checkQueueId"].as_i64().unwrap() as i32
    );

    let send_events = Arc::new(Mutex::new(Vec::<String>::new()));
    struct SendFirst(Arc<Mutex<Vec<String>>>);
    impl SendMessageHook for SendFirst {
        fn hook_name(&self) -> &str {
            "fixture-send-first"
        }

        fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("before:first".into());
            context
                .message
                .set_system_property("DIRECT_SEND_HOOK", "before")
                .unwrap();
        }

        fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("after:first".into());
            context
                .message
                .set_system_property("DIRECT_SEND_HOOK", "after")
                .unwrap();
        }
    }
    struct SendPanics(Arc<Mutex<Vec<String>>>);
    impl SendMessageHook for SendPanics {
        fn hook_name(&self) -> &str {
            "fixture-send-throws"
        }

        fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("before:throws".into());
            panic!("fixture before failure");
        }

        fn send_message_after(&self, _context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("after:throws".into());
            panic!("fixture after failure");
        }
    }
    struct SendLast(Arc<Mutex<Vec<String>>>);
    impl SendMessageHook for SendLast {
        fn hook_name(&self) -> &str {
            "fixture-send-last"
        }

        fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
            let marker = context
                .message
                .get_property("DIRECT_SEND_HOOK")
                .unwrap_or_else(|| "<null>".to_owned());
            self.0.lock().unwrap().push(format!("before:last:{marker}"));
        }

        fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
            let marker = context
                .message
                .get_property("DIRECT_SEND_HOOK")
                .unwrap_or_else(|| "<null>".to_owned());
            self.0.lock().unwrap().push(format!("after:last:{marker}"));
        }
    }

    producer.register_send_message_hook(Arc::new(SendFirst(send_events.clone())));
    producer.register_send_message_hook(Arc::new(SendPanics(send_events.clone())));
    producer.register_send_message_hook(Arc::new(SendLast(send_events.clone())));
    assert_eq!(
        producer.has_send_message_hook(),
        expected["sendMessagePresentAfterRegistration"]
    );
    let mut send_message = Message::new("orders", [2]);
    let mut send_context = SendMessageContext {
        producer_group: Some("producer-implementation-hooks".into()),
        producer: &producer,
        message: &mut send_message,
        queue: MessageQueue::new("orders", "broker-a", 2),
        broker_addr: "127.0.0.1:10911".into(),
        born_host: None,
        communication_mode: CommunicationMode::Sync,
        send_result: None,
        exception: None,
        message_type: SendMessageType::Normal,
        namespace: None,
        trace_context: None,
        properties: None,
    };
    producer.execute_send_message_hook_before(&mut send_context);
    producer.execute_send_message_hook_after(&mut send_context);

    assert_eq!(
        *send_events.lock().unwrap(),
        expected["sendEvents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        send_message
            .properties()
            .get("DIRECT_SEND_HOOK")
            .map(String::as_str),
        expected["sendMessageMarker"].as_str()
    );
}

#[test]
fn implementation_state_accessors_match_pinned_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationStateAccessors"];
    let mut config = ProducerConfig::new("producer-implementation-state").unwrap();
    config.client_config_mut().set_unit_mode(true);
    let producer = Producer::new(config);

    assert_eq!(
        producer.service_state().unwrap().as_java_name(),
        expected["initialServiceState"]
    );
    assert_eq!(
        producer.config().producer_group(),
        expected["defaultProducerGroup"].as_str()
    );
    assert_eq!(producer.is_unit_mode(), expected["unitMode"]);
    assert!(expected["defaultProducerIdentity"].as_bool().unwrap());
    assert!(expected["defaultProducerImplIdentity"].as_bool().unwrap());
    let implementation = producer.default_mq_producer_impl();
    assert!(std::ptr::eq(&producer, implementation));
    assert!(std::ptr::eq(
        &producer,
        implementation.default_mq_producer()
    ));
    assert!(expected["clientFactoryInitiallyNull"].as_bool().unwrap());

    let states = ProducerServiceState::values();
    let expected_states = expected["serviceStateValues"].as_array().unwrap();
    let expected_ordinals = expected["serviceStateOrdinals"].as_array().unwrap();
    assert_eq!(states.len(), expected_states.len());
    assert_eq!(states.len(), expected_ordinals.len());
    for ((state, expected_name), expected_ordinal) in states
        .into_iter()
        .zip(expected_states)
        .zip(expected_ordinals)
    {
        assert_eq!(state.as_java_name(), expected_name.as_str().unwrap());
        assert_eq!(state.name(), expected_name.as_str().unwrap());
        assert_eq!(state.ordinal(), expected_ordinal.as_i64().unwrap() as i32);
        assert_eq!(
            ProducerServiceState::value_of_name(expected_name.as_str()),
            Ok(state)
        );
        producer.set_service_state(Some(state));
        assert_eq!(producer.service_state(), Some(state));
    }
    assert_eq!(
        ProducerServiceState::value_of_name(None),
        Err(rocketmq_classic_protocol::codes::JavaEnumLookupError::NullName)
    );
    producer.set_service_state(None);
    assert!(expected["nullServiceState"].as_bool().unwrap());
    assert_eq!(producer.service_state(), None);
}

#[tokio::test]
async fn null_producer_group_start_matches_pinned_java_validation() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let constructor_cases = &fixture["producerConstructorCases"];
    let cases = [
        ("nullProducerGroupStart", None),
        (
            "nullNamespacedProducerGroupStart",
            Some("constructor-null-group-namespace"),
        ),
    ];

    for (fixture_case, namespace) in cases {
        let expected = &constructor_cases[fixture_case];
        let mut config = ProducerConfig::new_nullable(None).unwrap();
        config
            .client_config_mut()
            .set_namespace(namespace.map(str::to_owned));
        let producer = Arc::new(Producer::new(config));
        assert_eq!(producer.producer_group(), None);

        let error = producer.start().await.unwrap_err();
        assert_eq!(
            error.to_string(),
            expected["failureMessage"].as_str().unwrap()
        );
        assert_eq!(
            producer.service_state().unwrap().as_java_name(),
            expected["serviceState"].as_str().unwrap()
        );
        assert_eq!(producer.producer_group(), None);
    }
}

#[test]
fn implementation_publish_info_table_and_nested_aliases_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationPublishInfoTable"];
    let producer = Producer::new(ProducerConfig::new("producer-publish-info-table").unwrap());
    let table = producer.get_topic_publish_info_table();
    let info = TopicPublishInfo::default();
    let original = MessageQueue::new("orders", "broker-a", 0);
    let replacement = MessageQueue::new("orders", "broker-b", 1);
    let original_list = ProducerPublishQueueList::new(vec![original.clone()]);
    info.set_message_queue_list(Some(original_list.clone()));

    let mut route = TopicRouteData::default();
    route.set_queue_datas_owned(Some(vec![Some(QueueData {
        broker_name: Some("broker-b".to_owned()),
        write_queue_nums: 3,
        ..QueueData::default()
    })]));
    let route = TopicRouteDataHandle::new(route);
    info.set_topic_route_data(Some(route.clone()));
    info.set_order_topic(true);
    info.set_have_topic_router_info(true);
    let send_index = ProducerQueueIndex::default();
    info.set_send_which_queue(send_index.clone());

    assert!(table.put("orders", info.clone()).is_none());
    let from_table = producer
        .get_topic_publish_info_table()
        .get("orders")
        .unwrap();
    assert_eq!(
        Some(from_table.is_order_topic()),
        expected["tableValueRetainsIdentity"].as_bool()
    );
    from_table.set_order_topic(false);
    assert!(!info.is_order_topic());
    from_table.set_order_topic(true);

    let table_queue_list = from_table.message_queue_list().unwrap();
    table_queue_list.with_mut(|queues| queues.push(replacement.clone()));
    assert_eq!(
        original_list.snapshot(),
        [original.clone(), replacement.clone()]
    );
    original_list.with_mut(|queues| queues.retain(|queue| queue == &original));

    route
        .with_mut(|route| {
            route.set_queue_datas_owned(Some(vec![
                Some(QueueData {
                    broker_name: Some("broker-b".to_owned()),
                    write_queue_nums: 3,
                    ..QueueData::default()
                }),
                Some(QueueData {
                    broker_name: Some("broker-c".to_owned()),
                    write_queue_nums: 4,
                    ..QueueData::default()
                }),
            ]));
        })
        .unwrap();
    assert_eq!(
        from_table
            .get_write_queue_nums_by_broker("broker-c")
            .unwrap(),
        4
    );

    let table_send_index = from_table.send_which_queue();
    let initial_index = send_index.to_string();
    table_send_index.increment_and_get();
    assert_ne!(send_index.to_string(), initial_index);
    assert_eq!(send_index.to_string(), table_send_index.to_string());

    let retained_list = from_table.message_queue_list().unwrap();
    retained_list.with_mut(|queues| {
        queues.clear();
        queues.push(replacement.clone());
    });
    assert_eq!(
        from_table.ok(),
        expected["queueListMutationAffectsOk"].as_bool().unwrap()
    );
    assert_eq!(
        from_table.select_one_message_queue().unwrap(),
        replacement,
        "the same live queue-list mutation must change subsequent selection"
    );
    assert_eq!(
        from_table.is_order_topic(),
        expected["orderTopic"].as_bool().unwrap()
    );
    assert_eq!(
        from_table.have_topic_router_info(),
        expected["haveTopicRouterInfo"].as_bool().unwrap()
    );
    assert_eq!(
        from_table
            .get_write_queue_nums_by_broker("broker-b")
            .unwrap(),
        expected["writeQueueNumsByBroker"].as_i64().unwrap() as i32
    );
    assert!(from_table.to_string().contains("haveTopicRouterInfo=true"));

    let stale_list = from_table.message_queue_list().unwrap();
    from_table.set_message_queue_list(Some(ProducerPublishQueueList::new(vec![original.clone()])));
    stale_list.with_mut(Vec::clear);
    let current_list = from_table.message_queue_list().unwrap();
    assert_eq!(current_list.snapshot(), [original]);

    let replacement_info = TopicPublishInfo::default();
    replacement_info.set_message_queue_list(Some(ProducerPublishQueueList::new(vec![
        replacement.clone(),
    ])));
    let previous = table.put("orders", replacement_info.clone()).unwrap();
    previous.set_order_topic(false);
    assert!(!info.is_order_topic());
    assert_eq!(
        table
            .get("orders")
            .unwrap()
            .message_queue_list()
            .unwrap()
            .snapshot(),
        [replacement]
    );
    assert!(table.remove("orders").is_some());
    assert!(table.is_empty());
    assert!(expected["getterReturnsLiveMap"].as_bool().unwrap());
    assert!(expected["queueListRetainsIdentity"].as_bool().unwrap());
    assert!(
        expected["queueListMutationAffectsSelection"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["sendIndexRetainsIdentity"].as_bool().unwrap());
    assert!(expected["routeDataRetainsIdentity"].as_bool().unwrap());
    assert!(expected["listSetterReplacesAlias"].as_bool().unwrap());
    assert!(
        expected["tablePutReturnsPreviousIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["replacementValueIsVisible"].as_bool().unwrap());
    assert!(expected["removeAffectsRetainedMap"].as_bool().unwrap());
    assert!(expected["toStringContainsState"].as_bool().unwrap());
}

#[test]
fn publish_info_table_remapping_and_live_views_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationPublishInfoTableAdvanced"];
    let table = Producer::new(ProducerConfig::new("publish-info-table-advanced").unwrap())
        .get_topic_publish_info_table();
    let first = TopicPublishInfo::default();
    let second = TopicPublishInfo::default();
    let third = TopicPublishInfo::default();

    let computed = table
        .compute_if_absent("compute", |_| Some(first.clone()))
        .unwrap();
    computed.set_order_topic(true);
    assert!(first.is_order_topic());
    let replaced = table
        .compute_if_present("compute", |_, _| Some(second.clone()))
        .unwrap();
    replaced.set_order_topic(true);
    assert!(second.is_order_topic());
    assert_eq!(
        expected["computeIfPresentIdentity"].as_bool(),
        Some(table.get("compute").unwrap().is_order_topic())
    );
    second.set_order_topic(false);
    let merged = table
        .merge("compute", third.clone(), |current, _| Some(current.clone()))
        .unwrap();
    merged.set_order_topic(true);
    assert!(second.is_order_topic());
    assert_eq!(
        expected["mergeKeepsCurrentIdentity"].as_bool(),
        Some(table.get("compute").unwrap().is_order_topic())
    );
    assert!(table.compute("compute", |_, _| None).is_none());
    assert!(!table.contains_key("compute"));
    assert!(expected["computeIfAbsentIdentity"].as_bool().unwrap());
    assert!(expected["computeNullRemoves"].as_bool().unwrap());

    table.put("replace", first.clone());
    first.set_order_topic(false);
    let previous = table.replace("replace", second.clone()).unwrap();
    previous.set_order_topic(true);
    assert!(first.is_order_topic());
    assert!(
        expected["replaceReturnsPreviousIdentity"]
            .as_bool()
            .unwrap()
    );
    let different_value = TopicPublishInfo::default();
    assert!(!table.replace_if_equal("replace", &different_value, third.clone()));
    assert!(table.replace_if_equal("replace", &second, third.clone()));
    table.replace_all(|_, _| second.clone());
    table.get("replace").unwrap().set_order_topic(true);
    assert!(second.is_order_topic());
    assert!(
        expected["conditionalReplaceUsesIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["replaceAllUsesLiveValues"].as_bool().unwrap());

    let keys = table.key_set_view();
    table.put("key-view", first.clone());
    assert!(keys.remove("key-view"));
    assert!(!table.contains_key("key-view"));
    table.put("key-bulk-remove", first.clone());
    table.put("key-bulk-retain", second.clone());
    assert_eq!(
        keys.remove_all(&BTreeSet::from(["key-bulk-remove".to_owned()])),
        1
    );
    assert_eq!(
        keys.retain_all(&BTreeSet::from(["key-bulk-retain".to_owned()])),
        1
    );
    assert!(matches!(
        table.put_all_nullable_rows(Some(&[
            (Some("put-all-before".to_owned()), Some(first.clone())),
            (Some("put-all-null".to_owned()), None),
            (Some("put-all-after".to_owned()), Some(third.clone())),
        ])),
        Err(TopicPublishInfoTableError::NullValue)
    ));
    assert!(table.contains_key("put-all-before"));
    assert!(!table.contains_key("put-all-after"));
    table.remove("put-all-before");
    assert!(expected["keyViewBulkMutation"].as_bool().unwrap());
    assert!(expected["putAllNullValueKeepsEarlier"].as_bool().unwrap());

    let values = table.values_view();
    table.put("value-view", first.clone());
    assert!(values.contains(&first));
    assert!(values.remove(&first));
    assert!(!table.contains_key("value-view"));
    table.put("value-bulk-a", first.clone());
    table.put("value-bulk-b", first.clone());
    table.put("value-bulk-c", second.clone());
    assert_eq!(values.remove_all(&[first.clone()]), 2);
    assert!(table.contains_key("value-bulk-c"));
    table.put("value-retain-a", first.clone());
    table.put("value-retain-b", second.clone());
    assert_eq!(values.retain_all(&[first.clone()]), 3);
    assert!(table.contains_key("value-retain-a"));
    assert!(expected["valuesViewBulkMutation"].as_bool().unwrap());

    let entries = table.entry_set_view();
    table.put("entry-view", first.clone());
    let entry = entries
        .snapshot()
        .into_iter()
        .find(|entry| entry.key() == "entry-view")
        .unwrap();
    let previous = entry.set_value(third.clone());
    previous.set_order_topic(true);
    assert!(first.is_order_topic());
    entry.value().set_order_topic(true);
    assert!(third.is_order_topic());
    table.get("entry-view").unwrap().set_order_topic(false);
    assert!(!third.is_order_topic());
    assert!(entries.remove("entry-view", &third));
    assert!(!table.contains_key("entry-view"));
    table.put("entry-bulk-remove", first.clone());
    table.put("entry-bulk-retain", third.clone());
    assert_eq!(
        entries.remove_all(&[("entry-bulk-remove".to_owned(), first.clone())]),
        1
    );
    assert_eq!(
        entries.retain_all(&[("entry-bulk-retain".to_owned(), third.clone())]),
        1
    );
    assert_eq!(table.len(), 1);
    table
        .get("entry-bulk-retain")
        .unwrap()
        .set_order_topic(true);
    assert!(third.is_order_topic());
    assert!(expected["entryViewBulkMutation"].as_bool().unwrap());

    table.put("key-remove-if", first.clone());
    assert_eq!(keys.remove_if(|topic| topic == "key-remove-if"), 1);
    assert!(!table.contains_key("key-remove-if"));
    first.set_order_topic(true);
    second.set_order_topic(false);
    third.set_order_topic(false);
    table.put("value-remove-if-match", first.clone());
    table.put("value-remove-if-keep", second.clone());
    assert_eq!(values.remove_if(TopicPublishInfo::is_order_topic), 1);
    assert!(!table.contains_key("value-remove-if-match"));
    assert!(!table.get("value-remove-if-keep").unwrap().is_order_topic());
    table.put("entry-remove-if-match", first.clone());
    table.put("entry-remove-if-keep", third.clone());
    assert_eq!(
        entries.remove_if(|topic, _| topic == "entry-remove-if-match"),
        1
    );
    assert!(!table.contains_key("entry-remove-if-match"));
    table
        .get("entry-remove-if-keep")
        .unwrap()
        .set_order_topic(true);
    assert!(third.is_order_topic());
    assert!(expected["keyViewRemoveIfMutation"].as_bool().unwrap());
    assert!(expected["valuesViewRemoveIfMutation"].as_bool().unwrap());
    assert!(expected["entryViewRemoveIfMutation"].as_bool().unwrap());

    table.clear();
    assert!(keys.is_empty());
    assert!(values.is_empty());
    assert!(entries.is_empty());
    assert!(expected["keyViewRemovesFromMap"].as_bool().unwrap());
    assert!(expected["valuesViewRemovesByIdentity"].as_bool().unwrap());
    assert!(
        expected["entryViewSetValueWritesThrough"]
            .as_bool()
            .unwrap()
    );
    assert!(
        expected["entryViewRemoveUsesKeyAndIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["clearAffectsRetainedViews"].as_bool().unwrap());

    let missing_default = table.get_or_default("missing", second.clone());
    missing_default.set_order_topic(true);
    assert!(second.is_order_topic());
    table.put("map-op-present", first.clone());
    let present_value = table.get_or_default("map-op-present", second.clone());
    present_value.set_order_topic(true);
    assert!(first.is_order_topic());
    assert!(table.contains_value(&first));
    let current_value = table
        .put_if_absent("map-op-present", second.clone())
        .unwrap();
    current_value.set_order_topic(false);
    assert!(!first.is_order_topic());
    assert!(
        table
            .put_if_absent("map-op-absent", second.clone())
            .is_none()
    );
    table.get("map-op-absent").unwrap().set_order_topic(true);
    assert!(second.is_order_topic());
    assert!(table.remove_if_equal("map-op-absent", &second));
    assert!(!table.contains_key("map-op-absent"));
    first.set_order_topic(true);
    second.set_order_topic(false);
    table.put("for-each-a", first.clone());
    table.put("for-each-b", second.clone());
    let mut for_each_rows = Vec::new();
    table.for_each(|key, value| {
        for_each_rows.push(format!(
            "{key}:{}",
            if value.is_order_topic() {
                "first"
            } else {
                "other"
            }
        ));
    });
    assert_eq!(for_each_rows.len(), 3);
    assert!(for_each_rows.contains(&"map-op-present:first".to_owned()));
    assert!(for_each_rows.contains(&"for-each-a:first".to_owned()));
    assert!(for_each_rows.contains(&"for-each-b:other".to_owned()));
    assert!(expected["getOrDefaultMissingIdentity"].as_bool().unwrap());
    assert!(expected["getOrDefaultPresentIdentity"].as_bool().unwrap());
    assert!(expected["containsValueIdentity"].as_bool().unwrap());
    assert!(
        expected["putIfAbsentKeepsCurrentIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["putIfAbsentInserts"].as_bool().unwrap());
    assert!(expected["conditionalRemoveUsesIdentity"].as_bool().unwrap());
    assert!(expected["forEachVisitsLiveEntries"].as_bool().unwrap());
    assert!(expected["sizeAndEmptyMatch"].as_bool().unwrap());
    table.clear();
    assert!(table.is_empty());
    assert!(expected["clearEmptiesLiveMap"].as_bool().unwrap());
}

#[test]
fn publish_info_table_nullable_arguments_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationPublishInfoTableAdvanced"];
    let table = Producer::new(ProducerConfig::new("publish-info-table-nullable").unwrap())
        .get_topic_publish_info_table();
    let first = TopicPublishInfo::default();
    let second = TopicPublishInfo::default();
    table.put("nullable", first.clone());

    macro_rules! assert_table_error {
        ($actual:expr, $expected:pat) => {
            assert!(matches!($actual, Err($expected)));
        };
    }

    for name in [
        "nullGetKey",
        "nullGetOrDefaultKey",
        "nullContainsKey",
        "nullContainsValue",
        "nullPutKey",
        "nullPutValue",
        "nullPutIfAbsentKey",
        "nullPutIfAbsentValue",
        "nullPutAllMap",
        "nullRemoveKey",
        "nullReplaceKey",
        "nullReplaceValue",
        "nullConditionalReplaceExpected",
        "nullConditionalReplaceValue",
        "nullComputeIfAbsentKey",
        "nullComputeIfPresentKey",
        "nullComputeKey",
        "nullMergeKey",
        "nullMergeValue",
        "nullComputeIfAbsentCallback",
        "nullComputeIfPresentCallback",
        "nullComputeCallback",
        "nullMergeCallback",
        "nullForEachCallback",
        "nullReplaceAllCallback",
        "nullKeySetContains",
        "nullKeySetRemove",
        "nullValuesContains",
        "nullKeySetRemoveAll",
        "nullKeySetRetainAll",
        "nullValuesRemoveAll",
        "nullValuesRetainAll",
        "nullEntrySetRemoveAll",
        "nullEntrySetRetainAll",
        "nullKeySetRemoveIf",
        "nullValuesRemoveIf",
        "nullEntrySetRemoveIf",
        "nullEntrySetValue",
        "nullReplaceAllResult",
    ] {
        assert_eq!(
            expected[name].as_str(),
            Some("NullPointerException"),
            "{name}"
        );
    }

    assert_eq!(expected["nullConditionalRemoveValue"], true);
    assert_eq!(expected["nullGetOrDefaultFallbackAllowed"], true);
    assert_eq!(expected["nullValuesRemove"], true);
    assert_eq!(expected["nullEntrySetContains"], true);
    assert_eq!(expected["nullEntrySetRemove"], true);
    assert_eq!(expected["nullKeyEntrySetContains"], true);
    assert_eq!(expected["nullValueEntrySetContains"], true);
    assert_eq!(expected["nullKeyEntrySetRemove"], true);
    assert_eq!(expected["nullValueEntrySetRemove"], true);
    assert_eq!(expected["nullArgumentsPreserveMapping"], true);
    assert_eq!(expected["nullEntrySetValuePreservesMapping"], true);
    assert_eq!(expected["nullReplaceAllResultPreservesMapping"], true);
    assert_eq!(expected["computeIfAbsentNullResultAllowed"], true);
    assert_eq!(expected["computeIfPresentNullResultRemoves"], true);
    assert_eq!(expected["computeNullResultRemoves"], true);
    assert_eq!(expected["mergeNullResultRemoves"], true);

    assert_table_error!(
        table.get_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert!(matches!(
        table.get_or_default_nullable(Some("missing"), None),
        Ok(None)
    ));
    assert_table_error!(
        table.contains_key_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.contains_value_nullable(None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.put_nullable(None, Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.put_nullable(Some("nullable-value"), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.put_if_absent_nullable(None, Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.put_if_absent_nullable(Some("nullable"), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.get_or_default_nullable(None, None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.put_all_nullable_rows(None),
        TopicPublishInfoTableError::NullMap
    );
    assert_table_error!(
        table.remove_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert!(
        !table
            .remove_if_equal_nullable(Some("nullable"), None)
            .unwrap()
    );
    assert_table_error!(
        table.remove_if_equal_nullable(None, None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.replace_nullable(None, Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.replace_nullable(Some("nullable"), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.replace_if_equal_nullable(Some("nullable"), None, Some(second.clone())),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.replace_if_equal_nullable(None, None, None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.replace_if_equal_nullable(Some("nullable"), Some(&first), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.compute_if_absent_nullable(None, |_| Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.compute_if_present_nullable(None, |_, _| Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.compute_nullable(None, |_, _| Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.merge_nullable(
            None,
            Some(second.clone()),
            None::<&mut fn(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>>
        ),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.merge_nullable(
            Some("nullable"),
            None,
            None::<&mut fn(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>>
        ),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.merge_nullable(
            Some("nullable"),
            Some(second.clone()),
            None::<&mut fn(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.compute_if_absent_with_nullable_callback(
            Some("nullable"),
            None::<&mut fn(&str) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.compute_if_present_with_nullable_callback(
            Some("nullable"),
            None::<&mut fn(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.compute_with_nullable_callback(
            Some("nullable"),
            None::<&mut fn(&str, Option<&TopicPublishInfo>) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.replace_all_with_nullable_callback(
            None::<&mut fn(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.for_each_nullable(None::<&mut fn(&str, &TopicPublishInfo)>),
        TopicPublishInfoTableError::NullCallback
    );

    let keys = table.key_set_view();
    let values = table.values_view();
    let entries = table.entry_set_view();
    assert_table_error!(
        keys.contains_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        keys.remove_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        keys.remove_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        keys.retain_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        keys.remove_if_with_nullable_callback(None::<&mut fn(&str) -> bool>),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        values.contains_nullable(None),
        TopicPublishInfoTableError::NullValue
    );
    assert!(matches!(values.remove_nullable(None), Ok(false)));
    assert_table_error!(
        values.remove_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        values.retain_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        values.remove_if_with_nullable_callback(None::<&mut fn(&TopicPublishInfo) -> bool>),
        TopicPublishInfoTableError::NullCallback
    );
    assert!(!entries.contains_nullable(None));
    assert!(!entries.contains_entry_nullable(None, Some(&first)));
    assert!(!entries.contains_entry_nullable(Some("nullable"), None));
    assert!(!entries.remove_nullable(None));
    assert!(!entries.remove_entry_nullable(None, Some(&first)));
    assert!(!entries.remove_entry_nullable(Some("nullable"), None));
    assert_table_error!(
        entries.remove_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        entries.retain_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        entries.remove_if_with_nullable_callback(None::<&mut fn(&str, &TopicPublishInfo) -> bool>),
        TopicPublishInfoTableError::NullCallback
    );
    let entry = entries.snapshot().into_iter().next().unwrap();
    assert_table_error!(
        entry.set_value_nullable(None),
        TopicPublishInfoTableError::NullValue
    );
    entry.value().set_order_topic(true);
    assert!(first.is_order_topic());
    table.get("nullable").unwrap().set_order_topic(false);
    assert!(!first.is_order_topic());

    assert!(
        table
            .compute_if_absent_nullable(Some("compute-absent-null"), |_| None)
            .unwrap()
            .is_none()
    );
    table.put("compute-present-null", second.clone());
    assert!(
        table
            .compute_if_present_nullable(Some("compute-present-null"), |_, _| None)
            .unwrap()
            .is_none()
    );
    assert!(!table.contains_key("compute-present-null"));
    table.put("compute-null-result", second.clone());
    assert!(
        table
            .compute_nullable(Some("compute-null-result"), |_, _| None)
            .unwrap()
            .is_none()
    );
    assert!(!table.contains_key("compute-null-result"));
    table.put("merge-null-result", second);
    let mut drop_merge = |_: &TopicPublishInfo, _: &TopicPublishInfo| None;
    assert!(
        table
            .merge_nullable(
                Some("merge-null-result"),
                Some(first.clone()),
                Some(&mut drop_merge)
            )
            .unwrap()
            .is_none()
    );
    assert!(!table.contains_key("merge-null-result"));

    table.clear();
    table.put("replace-all-null-result", first.clone());
    assert_table_error!(
        table.replace_all_nullable(|_, _| None),
        TopicPublishInfoTableError::NullRemappingResult
    );
    table
        .get("replace-all-null-result")
        .unwrap()
        .set_order_topic(true);
    assert!(first.is_order_topic());
}

#[test]
fn publish_info_queue_filter_nullability_and_order_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationQueueFilterSelection"];
    let info = TopicPublishInfo::default();
    let queue = MessageQueue::new("orders", "broker-a", 2);
    info.set_message_queue_list(Some(ProducerPublishQueueList::new(vec![queue.clone()])));

    assert_eq!(
        info.select_one_message_queue_with_nullable_filters(None)
            .unwrap(),
        Some(queue.clone())
    );
    assert_eq!(
        info.select_one_message_queue_with_nullable_filters(Some(&[]))
            .unwrap(),
        Some(queue.clone())
    );
    assert_eq!(
        expected["nullFilterArraySelection"].as_i64(),
        Some(i64::from(queue.queue_id()))
    );
    assert_eq!(
        expected["emptyFilterArraySelection"].as_i64(),
        Some(i64::from(queue.queue_id()))
    );

    let null_filter = [None];
    assert!(matches!(
        info.select_one_message_queue_with_nullable_filters(Some(&null_filter)),
        Err(ProducerError::NullPublishInfoQueueFilter)
    ));
    assert_eq!(
        expected["nullFilterWithQueue"].as_str(),
        Some("NullPointerException")
    );

    info.set_message_queue_list(Some(ProducerPublishQueueList::default()));
    assert!(
        info.select_one_message_queue_with_nullable_filters(Some(&null_filter))
            .unwrap()
            .is_none()
    );
    assert!(expected["nullFilterWithEmptyQueueList"].is_null());

    info.set_message_queue_list(None);
    assert!(
        info.select_one_message_queue_with_nullable_filters(Some(&null_filter))
            .unwrap()
            .is_none()
    );
    assert!(expected["nullFilterWithNullQueueList"].is_null());

    let calls = std::sync::Mutex::new(Vec::new());
    let reject = |_: &MessageQueue| {
        calls.lock().unwrap().push("reject");
        false
    };
    let accept = |_: &MessageQueue| {
        calls.lock().unwrap().push("accept");
        true
    };
    let filters: [Option<PublishInfoQueueFilter<'_>>; 2] = [Some(&reject), Some(&accept)];
    info.set_message_queue_list(Some(ProducerPublishQueueList::new(vec![queue.clone()])));
    assert!(
        info.select_one_message_queue_with_nullable_filters(Some(&filters))
            .unwrap()
            .is_none()
    );
    assert_eq!(*calls.lock().unwrap(), ["reject", "accept"]);
    assert!(expected["nonShortCircuitSelection"].is_null());
    assert_eq!(
        *calls.lock().unwrap(),
        expected["nonShortCircuitFilterOrder"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry.as_str().unwrap())
            .collect::<Vec<_>>()
    );
}

#[test]
fn producer_compressor_snapshot_and_operations_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let producer = Producer::new(ProducerConfig::new("compressor-fixture").unwrap());
    let input = [0, 1, 2, 3, 127, 255];

    for row in fixture["compressorSettings"]["types"].as_array().unwrap() {
        let kind = CompressionType::of(Some(row["type"].as_str().unwrap())).unwrap();
        producer.set_compress_type(kind);
        let compressor = producer.compressor();
        assert_eq!(compressor.compression_type(), kind, "{row}");
        assert!(row["roundTrip"].as_bool().unwrap(), "{row}");

        let compressed = compressor.compress(&input, 5).unwrap();
        assert_eq!(compressor.decompress(&compressed).unwrap(), input, "{row}");
        let expected_class = match kind {
            CompressionType::Lz4 => "Lz4Compressor",
            CompressionType::Zstd => "ZstdCompressor",
            CompressionType::Zlib => "ZlibCompressor",
        };
        assert_eq!(row["class"], expected_class, "{row}");
    }

    producer.set_compress_type(CompressionType::Zlib);
    let retained = producer.compressor();
    producer.set_compress_type(CompressionType::Lz4);
    assert_eq!(retained.compression_type(), CompressionType::Zlib);
    assert!(
        fixture["compressorSettings"]["retainedRoundTripAfterUpdate"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        retained
            .decompress(&retained.compress(&input, 5).unwrap())
            .unwrap(),
        input
    );
    let current = producer.compressor();
    assert_eq!(
        current.compression_type().name(),
        fixture["compressorSettings"]["currentTypeAfterUpdate"]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        fixture["compressorSettings"]["retainedClassAfterUpdate"],
        "ZlibCompressor"
    );
    assert_eq!(
        fixture["compressorSettings"]["currentClassAfterUpdate"],
        "Lz4Compressor"
    );
}

#[test]
fn producer_auto_batch_getter_and_setter_follow_accumulator_lifecycle() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let mut config = ProducerConfig::new("auto-batch-lifecycle-fixture").unwrap();
    config.set_auto_batch(true);
    let producer = Producer::new(config);

    assert!(
        !producer.auto_batch(),
        "Java returns false before accumulator initialization"
    );
    producer.init_produce_accumulator().unwrap();
    assert_eq!(
        producer.auto_batch(),
        fixture["autoBatchLifecycle"]["enabledAfterAccumulator"]
    );
    producer.set_auto_batch(false);
    assert_eq!(
        producer.auto_batch(),
        fixture["autoBatchLifecycle"]["disabledAfterRuntimeToggle"]
    );
    producer.set_auto_batch(true);
    assert_eq!(
        producer.auto_batch(),
        fixture["autoBatchLifecycle"]["enabledAfterRuntimeToggle"]
    );
}

fn accumulator_fixture_config(group: &str, instance_name: &str) -> ProducerConfig {
    let mut config = ProducerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_instance_name(Some(instance_name.to_owned()));
    config
}

#[test]
fn producer_runtime_accumulator_settings_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["runtimeAccumulatorSettings"];
    let producer = Producer::new(accumulator_fixture_config(
        "runtime-accumulator-fixture",
        "runtime-accumulator-fixture",
    ));

    assert_eq!(producer.batch_max_delay_millis(), 0);
    assert_eq!(producer.batch_max_bytes(), 0);
    assert_eq!(producer.total_batch_max_bytes(), 0);
    producer.init_produce_accumulator().unwrap();
    assert_eq!(
        producer.batch_max_delay_millis(),
        expected["initializedDelay"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.batch_max_bytes(),
        expected["initializedBytes"].as_i64().unwrap()
    );
    assert_eq!(
        producer.total_batch_max_bytes(),
        expected["initializedTotalGetterBytes"].as_i64().unwrap()
    );

    producer.set_batch_max_delay_millis(25).unwrap();
    producer.set_batch_max_bytes(65_536).unwrap();
    producer.set_total_batch_max_bytes(1_048_576).unwrap();
    assert_eq!(
        producer.batch_max_delay_millis(),
        expected["updatedDelay"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.batch_max_bytes(),
        expected["updatedBytes"].as_i64().unwrap()
    );
    assert_eq!(
        producer.total_batch_max_bytes(),
        expected["updatedTotalGetterBytes"].as_i64().unwrap()
    );

    assert!(matches!(
        producer.set_batch_max_delay_millis(0),
        Err(ProducerError::InvalidBatchMaxDelay(0))
    ));
    assert_eq!(
        producer.batch_max_delay_millis(),
        expected["delayAfterInvalidUpdate"].as_i64().unwrap() as i32
    );
    assert!(matches!(
        producer.set_batch_max_bytes(0),
        Err(ProducerError::InvalidBatchMaxBytes(0))
    ));
    assert_eq!(
        producer.batch_max_bytes(),
        expected["bytesAfterInvalidUpdate"].as_i64().unwrap()
    );
    assert!(matches!(
        producer.set_total_batch_max_bytes(0),
        Err(ProducerError::InvalidTotalBatchMaxBytes(0))
    ));
    assert_eq!(
        producer.total_batch_max_bytes(),
        expected["updatedTotalGetterBytes"].as_i64().unwrap()
    );
    assert!(matches!(
        producer.init_produce_accumulator(),
        Err(ProducerError::InvalidBatchMaxDelay(0))
    ));
    assert_eq!(
        producer.batch_max_delay_millis(),
        expected["delayAfterReinitializationFailure"]
            .as_i64()
            .unwrap() as i32
    );

    let configured = Producer::new(accumulator_fixture_config(
        "configured-before-accumulator",
        "configured-before-accumulator",
    ));
    configured.set_batch_max_delay_millis(25).unwrap();
    configured.set_batch_max_bytes(65_536).unwrap();
    configured.set_total_batch_max_bytes(1_048_576).unwrap();
    assert_eq!(configured.batch_max_delay_millis(), 0);
    assert_eq!(configured.batch_max_bytes(), 0);
    assert_eq!(configured.total_batch_max_bytes(), 0);
    configured.init_produce_accumulator().unwrap();
    assert_eq!(
        configured.batch_max_delay_millis(),
        expected["configuredAfterInitializationDelay"]
            .as_i64()
            .unwrap() as i32
    );
    assert_eq!(
        configured.batch_max_bytes(),
        expected["configuredAfterInitializationBytes"]
            .as_i64()
            .unwrap()
    );
    assert_eq!(
        configured.total_batch_max_bytes(),
        expected["configuredAfterInitializationTotalGetterBytes"]
            .as_i64()
            .unwrap()
    );

    let invalid = Producer::new(accumulator_fixture_config(
        "invalid-before-accumulator",
        "invalid-before-accumulator",
    ));
    assert!(invalid.set_batch_max_delay_millis(0).is_ok());
    assert!(matches!(
        invalid.init_produce_accumulator(),
        Err(ProducerError::InvalidBatchMaxDelay(0))
    ));
    assert_eq!(
        invalid.batch_max_delay_millis(),
        expected["invalidBeforeInitializationEffectiveDelay"]
            .as_i64()
            .unwrap() as i32
    );
}

#[test]
fn pinned_java_produce_accumulator_declared_surface_is_constructor_only() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let api = &fixture["produceAccumulatorApi"];

    assert_eq!(
        api["className"],
        "org.apache.rocketmq.client.producer.ProduceAccumulator"
    );
    assert_eq!(
        api["publicConstructors"],
        serde_json::json!([
            "org.apache.rocketmq.client.producer.ProduceAccumulator(java.lang.String)"
        ])
    );
    assert_eq!(api["declaredPublicMethods"], serde_json::json!([]));
    assert_eq!(api["declaredPublicMethodCount"], 0);
    assert_eq!(api["publicMethods"], serde_json::json!([]));
    assert_eq!(api["publicMethodCount"], 0);

    drop(ProduceAccumulator::new(Some(
        "producer-instance".to_owned(),
    )));
    drop(ProduceAccumulator::new(None));
}

#[test]
fn java_validation_boundaries_and_reserved_topics_are_preserved() {
    let config = ProducerConfig::new("producer-test").unwrap();
    assert!(validate_group(&"g".repeat(120)).is_ok());
    assert!(matches!(
        validate_group(&"g".repeat(121)),
        Err(ProducerError::GroupTooLong)
    ));
    assert!(validate_topic(&"t".repeat(127)).is_ok());
    assert!(matches!(
        validate_topic(&"t".repeat(128)),
        Err(ProducerError::TopicTooLong)
    ));
    assert!(matches!(
        validate_topic(&format!("%RETRY%{}", "t".repeat(121))),
        Err(ProducerError::TopicTooLong)
    ));
    assert!(validate_system_topic("rmq_sys_private").is_err());
    assert!(validate_send_topic("RMQ_SYS_TRACE_TOPIC").is_ok());
    assert!(validate_send_topic("SCHEDULE_TOPIC_XXXX").is_err());

    let empty = Message::new("orders", Vec::new());
    assert!(matches!(
        validate_message(Some(&empty), &config),
        Err(ProducerError::EmptyBody)
    ));
    let max_body = Message::new("orders", vec![0; 4 * 1024 * 1024]);
    assert!(validate_message(Some(&max_body), &config).is_ok());
    let oversized = Message::new("orders", vec![0; 4 * 1024 * 1024 + 1]);
    assert!(matches!(
        validate_message(Some(&oversized), &config),
        Err(ProducerError::BodyTooLarge { .. })
    ));
    assert!(matches!(
        validate_message(None, &config),
        Err(ProducerError::NullMessage)
    ));
}

#[test]
fn pinned_java_producer_core_fixture_matches_defaults_and_validation() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let defaults = &fixture["defaults"];
    let mut config = ProducerConfig::new(defaults["producerGroup"].as_str().unwrap()).unwrap();
    config.set_create_topic_key(defaults["createTopicKey"].as_str().unwrap());
    config.set_default_topic_queue_nums(defaults["defaultTopicQueueNums"].as_i64().unwrap() as i32);
    config.set_send_timeout_millis(defaults["sendMsgTimeout"].as_i64().unwrap() as i32);
    config.set_send_msg_max_timeout_per_request(
        defaults["sendMsgMaxTimeoutPerRequest"].as_i64().unwrap() as i32,
    );
    config.set_compress_threshold(defaults["compressMsgBodyOverHowmuch"].as_i64().unwrap() as i32);
    config.set_compress_type(
        CompressionType::of(Some(defaults["compressType"].as_str().unwrap())).unwrap(),
    );
    config.set_compress_level(defaults["compressLevel"].as_i64().unwrap() as i32);
    config.set_retry_times_when_send_failed(
        defaults["retryTimesWhenSendFailed"].as_i64().unwrap() as i32
    );
    config.set_retry_times_when_send_async_failed(
        defaults["retryTimesWhenSendAsyncFailed"].as_i64().unwrap() as i32,
    );
    for code in defaults["retryResponseCodes"].as_array().unwrap() {
        config.add_retry_response_code(code.as_i64().unwrap() as i32);
    }
    config.set_retry_another_broker_when_not_store_ok(
        defaults["retryAnotherBrokerWhenNotStoreOK"]
            .as_bool()
            .unwrap(),
    );
    config.set_max_message_size(defaults["maxMessageSize"].as_i64().unwrap() as i32);
    assert_eq!(config.producer_group(), Some("DEFAULT_PRODUCER"));
    assert_eq!(config.create_topic_key(), "TBW102");
    assert_eq!(config.default_topic_queue_nums(), 4);
    assert_eq!(config.send_timeout_millis(), 3_000);
    assert_eq!(
        config.send_msg_max_timeout_per_request(),
        defaults["sendMsgMaxTimeoutPerRequest"].as_i64().unwrap() as i32
    );
    assert_eq!(config.compress_threshold(), 4_096);
    assert_eq!(config.compress_type(), CompressionType::Zlib);
    assert_eq!(config.compress_level(), 5);
    assert_eq!(config.retry_times_when_send_failed(), 2);
    assert_eq!(config.retry_times_when_send_async_failed(), 2);
    assert_eq!(
        config
            .retry_response_codes()
            .snapshot()
            .into_iter()
            .collect::<Vec<_>>(),
        defaults["retryResponseCodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|code| code.as_i64().unwrap() as i32)
            .collect::<Vec<_>>()
    );
    assert!(!config.retry_another_broker_when_not_store_ok());
    assert_eq!(config.max_message_size(), 4 * 1024 * 1024);

    let runtime = &fixture["runtimeSettings"];
    let producer = Producer::new(ProducerConfig::new("runtime-settings-fixture").unwrap());
    let runtime_config_snapshot = producer.config().clone();
    producer.set_producer_group(Some(
        runtime["producerGroupAfterSetter"]
            .as_str()
            .unwrap()
            .to_owned(),
    ));
    producer.set_send_message_with_vip_channel(
        runtime["sendMessageWithVIPChannelAfterSetter"]
            .as_bool()
            .unwrap(),
    );
    producer.set_topics(Some(ProducerTopicList::from(
        runtime["topicsAfterSetter"]
            .as_array()
            .unwrap()
            .iter()
            .map(|topic| topic.as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
    )));
    assert_eq!(
        producer.producer_group().as_deref(),
        runtime["producerGroupAfterSetter"].as_str()
    );
    assert_eq!(
        producer.send_message_with_vip_channel(),
        runtime["sendMessageWithVIPChannelAfterSetter"]
    );
    assert_eq!(
        producer.topics().unwrap().snapshot(),
        runtime["topicsAfterSetter"]
            .as_array()
            .unwrap()
            .iter()
            .map(|topic| topic.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    );
    producer.set_topics(None);
    assert_eq!(
        producer.topics().is_none(),
        runtime["topicsIsNullAfterSetter"]
    );
    producer.set_create_topic_key(runtime["createTopicKey"].as_str().unwrap());
    producer
        .set_default_topic_queue_nums(runtime["defaultTopicQueueNums"].as_i64().unwrap() as i32);
    producer.set_send_timeout_millis(runtime["sendMsgTimeout"].as_i64().unwrap() as i32);
    producer.set_send_msg_max_timeout_per_request(
        runtime["sendMsgMaxTimeoutPerRequest"].as_i64().unwrap() as i32,
    );
    producer.set_retry_times_when_send_failed(
        runtime["retryTimesWhenSendFailed"].as_i64().unwrap() as i32,
    );
    producer.set_retry_times_when_send_async_failed(
        runtime["retryTimesWhenSendAsyncFailed"].as_i64().unwrap() as i32,
    );
    producer.set_retry_another_broker_when_not_store_ok(
        runtime["retryAnotherBrokerWhenNotStoreOK"]
            .as_bool()
            .unwrap(),
    );
    producer.set_max_message_size(runtime["maxMessageSize"].as_i64().unwrap() as i32);
    assert_eq!(producer.create_topic_key(), runtime["createTopicKey"]);
    assert_eq!(
        producer.default_topic_queue_nums(),
        runtime["defaultTopicQueueNums"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.send_timeout_millis(),
        runtime["sendMsgTimeout"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.send_msg_max_timeout_per_request(),
        runtime["sendMsgMaxTimeoutPerRequest"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.retry_times_when_send_failed(),
        runtime["retryTimesWhenSendFailed"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.retry_times_when_send_async_failed(),
        runtime["retryTimesWhenSendAsyncFailed"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.retry_another_broker_when_not_store_ok(),
        runtime["retryAnotherBrokerWhenNotStoreOK"]
    );
    assert_eq!(
        producer.max_message_size(),
        runtime["maxMessageSize"].as_i64().unwrap() as i32
    );
    assert_eq!(runtime_config_snapshot.default_topic_queue_nums(), 4);
    assert_eq!(runtime_config_snapshot.create_topic_key(), "TBW102");
    assert_eq!(runtime_config_snapshot.send_timeout_millis(), 3_000);
    assert_eq!(
        runtime_config_snapshot.send_msg_max_timeout_per_request(),
        -1
    );
    assert_eq!(runtime_config_snapshot.retry_times_when_send_failed(), 2);
    assert_eq!(
        runtime_config_snapshot.retry_times_when_send_async_failed(),
        2
    );
    assert!(!runtime_config_snapshot.retry_another_broker_when_not_store_ok());
    assert_eq!(runtime_config_snapshot.max_message_size(), 4 * 1024 * 1024);

    producer.set_enable_backpressure_for_async_mode(true);
    producer
        .set_backpressure_async_send_num(runtime["sendNumAfterUpdate"].as_i64().unwrap() as i32);
    producer
        .set_backpressure_async_send_size(runtime["sendSizeAfterUpdate"].as_i64().unwrap() as i32);
    assert_eq!(
        producer.enable_backpressure_for_async_mode(),
        runtime["backpressureEnabledAfterToggle"]
    );
    assert_eq!(
        producer.backpressure_async_send_num(),
        runtime["sendNumAfterUpdate"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.backpressure_async_send_size(),
        runtime["sendSizeAfterUpdate"].as_i64().unwrap() as i32
    );
    producer.set_backpressure_async_send_num(1);
    producer.set_backpressure_async_send_size(1);
    assert_eq!(
        producer.backpressure_async_send_num(),
        runtime["clampedSendNumAfterUpdate"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.backpressure_async_send_size(),
        runtime["clampedSendSizeAfterUpdate"].as_i64().unwrap() as i32
    );
    let live_codes = producer.retry_response_codes();
    assert!(live_codes.insert(9001));
    assert_eq!(
        live_codes.contains(&9001),
        runtime["setMutationVisibleThroughGetter"]
    );
    producer.add_retry_response_code(9002);
    assert_eq!(
        live_codes.contains(&9002),
        runtime["addMethodVisibleThroughExistingSet"]
    );
    live_codes.remove(&9001);
    assert_eq!(
        !producer.retry_response_codes().contains(&9001),
        runtime["setRemovalVisibleThroughGetter"]
    );

    for row in fixture["groups"].as_array().unwrap() {
        assert_eq!(
            validate_group(row["input"].as_str().unwrap()).is_ok(),
            row["valid"].as_bool().unwrap(),
            "group {:?}",
            row["input"]
        );
    }
    for row in fixture["topics"].as_array().unwrap() {
        assert_eq!(
            validate_topic(row["input"].as_str().unwrap()).is_ok(),
            row["valid"].as_bool().unwrap(),
            "topic {:?}",
            row["input"]
        );
    }
    for row in fixture["systemTopics"].as_array().unwrap() {
        let topic = row["topic"].as_str().unwrap();
        assert_eq!(validate_system_topic(topic).is_err(), row["system"]);
        assert_eq!(validate_send_topic(topic).is_err(), row["sendForbidden"]);
        assert_eq!(validate_send_topic(topic).is_ok(), row["sendAllowed"]);
    }
    let validators_api = &fixture["validatorsApi"];
    assert_eq!(
        validators_api["className"],
        "org.apache.rocketmq.client.Validators"
    );
    for method in [
        "void org.apache.rocketmq.client.Validators.checkTopicConfig(org.apache.rocketmq.common.TopicConfig)",
        "void org.apache.rocketmq.client.Validators.checkBrokerConfig(java.util.Properties)",
    ] {
        assert!(
            validators_api["publicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|candidate| candidate == method)
        );
    }

    for row in fixture["topicPermissions"].as_array().unwrap() {
        let permission = row["input"].as_i64().unwrap() as i32;
        let actual = check_topic_config(permission);
        assert_eq!(
            actual.is_ok(),
            row["accepted"],
            "topic permission {permission}"
        );
        if let Err(error) = actual {
            assert!(matches!(
                &error,
                ProducerError::ClientValidation {
                    response_code: 16,
                    ..
                }
            ));
            assert_eq!(error.to_string(), row["message"].as_str().unwrap());
        }
    }

    for row in fixture["brokerPermissions"].as_array().unwrap() {
        let permission = row["input"].as_str().unwrap();
        let properties = if permission == "missing" {
            HashMap::new()
        } else {
            HashMap::from([("brokerPermission".to_owned(), permission.to_owned())])
        };
        let actual = check_broker_config(&properties);
        assert_eq!(
            actual.is_ok(),
            row["accepted"],
            "broker permission {permission:?}"
        );
        match actual {
            Ok(()) => {}
            Err(error @ ProducerError::ClientValidation { response_code, .. }) => {
                assert_eq!(response_code, row["responseCode"].as_i64().unwrap() as i32);
                assert_eq!(error.to_string(), row["message"].as_str().unwrap());
            }
            Err(ProducerError::PermissionParse(source)) => {
                assert_eq!(row["errorClass"], "NumberFormatException");
                assert_eq!(source.to_string(), row["message"].as_str().unwrap());
            }
            other => panic!("unexpected broker permission result: {other:?}"),
        }
    }

    let perm_name_api = &fixture["permNameApi"];
    assert_eq!(
        perm_name_api["className"],
        "org.apache.rocketmq.common.constant.PermName"
    );
    for method_name in [
        ".perm2String(",
        ".isReadable(",
        ".isWriteable(",
        ".isInherited(",
        ".isValid(",
        ".isPriority(",
        ".isAccessible(",
    ] {
        assert!(
            perm_name_api["publicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|candidate| candidate.as_str().unwrap().contains(method_name))
        );
    }
    assert!(fixture["permNamePublicFields"].as_array().unwrap().len() >= 8);
    for row in fixture["messages"].as_array().unwrap() {
        let case = row["input"].as_str().unwrap();
        let valid = match case {
            "null-message" => validate_message(None, &config).is_ok(),
            // Message owns a non-null Vec in Rust. Java's null and empty body
            // cases therefore map to the same empty-body rejection.
            "null-body" | "empty-body" => {
                validate_message(Some(&Message::new("orders", Vec::new())), &config).is_ok()
            }
            "body-max" => validate_message(
                Some(&Message::new(
                    "orders",
                    vec![0; config.max_message_size() as usize],
                )),
                &config,
            )
            .is_ok(),
            "body-max-plus-one" => validate_message(
                Some(&Message::new(
                    "orders",
                    vec![0; config.max_message_size() as usize + 1],
                )),
                &config,
            )
            .is_ok(),
            "multi-dispatch-separator" => {
                let mut message = Message::new("orders", [1]);
                message
                    .set_system_property("INNER_MULTI_DISPATCH", "a/b")
                    .unwrap();
                validate_message(Some(&message), &config).is_ok()
            }
            other => panic!("unknown Java producer fixture case: {other}"),
        };
        assert_eq!(valid, row["valid"], "message case {case}");
    }

    let executor_fixture = &fixture["executorSettings"];
    assert_eq!(executor_fixture["defaultAsyncExecutorStable"], true);
    assert_eq!(executor_fixture["customAsyncExecutorRetained"], true);
    assert_eq!(executor_fixture["nullAsyncExecutorRestoresDefault"], true);
    assert_eq!(
        executor_fixture["callbackExecutorBeforeStart"],
        "NullPointerException"
    );

    struct UnusedExecutor;
    impl AsyncSenderExecutor for UnusedExecutor {
        fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
            Err(task)
        }
    }

    let producer = Producer::new(ProducerConfig::new("executor-fixture").unwrap());
    let default_executor = producer.async_sender_executor();
    assert!(Arc::ptr_eq(
        &default_executor,
        &producer.async_sender_executor()
    ));
    let executor: Arc<dyn AsyncSenderExecutor> = Arc::new(UnusedExecutor);
    producer.set_async_sender_executor(Some(executor.clone()));
    assert!(Arc::ptr_eq(&producer.async_sender_executor(), &executor));
    producer.set_async_sender_executor(None);
    assert!(Arc::ptr_eq(
        &producer.async_sender_executor(),
        &default_executor
    ));
}

#[tokio::test]
async fn producer_callback_executor_requires_an_initialized_client() {
    let producer = Producer::new(ProducerConfig::new("callback-before-start").unwrap());
    assert!(matches!(
        producer.set_callback_executor(None).await,
        Err(ProducerError::NotStarted)
    ));
}

#[test]
fn check_forbidden_context_and_modes_match_pinned_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["checkForbiddenContext"];
    let mut message = Message::new("orders", b"body".to_vec());
    let mut queue = rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 7);
    let context = CheckForbiddenContext {
        name_srv_addr: Some("namesrv.example:9876".into()),
        group: Some("producer-group".into()),
        message: &mut message,
        queue: &mut queue,
        broker_addr: "broker.example:10911".into(),
        communication_mode: CommunicationMode::Async,
        send_result: None,
        exception: None,
        arg: Some(Box::new("opaque-argument".to_owned())),
        unit_mode: true,
    };
    let mut null_group_message = Message::new("orders", b"body".to_vec());
    let mut null_group_queue =
        rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 7);
    let null_group_context = CheckForbiddenContext {
        name_srv_addr: None,
        group: None,
        message: &mut null_group_message,
        queue: &mut null_group_queue,
        broker_addr: String::new(),
        communication_mode: CommunicationMode::Async,
        send_result: None,
        exception: None,
        arg: None,
        unit_mode: false,
    };

    assert_eq!(
        context.name_srv_addr.as_deref(),
        expected["nameSrvAddr"].as_str()
    );
    assert_eq!(
        context.group.as_deref(),
        expected["group"].as_str().map(Some).unwrap()
    );
    assert_eq!(
        null_group_context.group.is_none(),
        expected["nullGroupIsNull"].as_bool().unwrap()
    );
    assert_eq!(context.message.topic(), expected["messageTopic"]);
    assert_eq!(
        context.message.body().as_ref(),
        expected["messageBody"].as_str().unwrap().as_bytes()
    );
    assert_eq!(
        context.queue.topic(),
        Some(expected["queueTopic"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.broker_name(),
        Some(expected["queueBroker"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        context.broker_addr,
        expected["brokerAddr"].as_str().unwrap()
    );
    assert_eq!(
        context.communication_mode.java_name(),
        expected["communicationMode"].as_str().unwrap()
    );
    assert_eq!(
        context.send_result.is_none(),
        expected["sendResultIsNull"].as_bool().unwrap()
    );
    assert_eq!(
        context.exception.is_none(),
        expected["exceptionIsNull"].as_bool().unwrap()
    );
    assert_eq!(
        context
            .arg
            .as_deref()
            .and_then(|value| value.downcast_ref::<String>())
            .map(String::as_str),
        expected["arg"].as_str()
    );
    assert_eq!(context.unit_mode, expected["unitMode"].as_bool().unwrap());
    assert_eq!(
        CommunicationMode::values().map(CommunicationMode::java_name),
        expected["communicationModes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|mode| mode.as_str().unwrap())
            .collect::<Vec<_>>()
            .as_slice()
    );
}

#[test]
fn send_message_context_surface_matches_pinned_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["sendMessageContext"]["values"];
    let producer = Producer::new(ProducerConfig::new("producer-group").unwrap());
    let mut message = Message::new("orders", b"body".to_vec());
    let hook_failure = std::io::Error::other("hook failure");
    let context = SendMessageContext {
        producer_group: Some("producer-group".into()),
        producer: &producer,
        message: &mut message,
        queue: MessageQueue::new("orders", "broker-a", 7),
        broker_addr: "broker.example:10911".into(),
        born_host: Some("192.0.2.1".into()),
        communication_mode: CommunicationMode::Async,
        send_result: None,
        exception: Some(&hook_failure),
        message_type: SendMessageType::Delay,
        namespace: Some("tenant".into()),
        trace_context: Some(Box::new("trace-context".to_owned())),
        properties: Some(HashMap::from([("source".to_owned(), "hook".to_owned())])),
    };
    let mut null_group_message = Message::new("orders", b"body".to_vec());
    let null_group_context = SendMessageContext {
        producer_group: None,
        producer: &producer,
        message: &mut null_group_message,
        queue: MessageQueue::new("orders", "broker-a", 7),
        broker_addr: String::new(),
        born_host: None,
        communication_mode: CommunicationMode::Async,
        send_result: None,
        exception: None,
        message_type: SendMessageType::Normal,
        namespace: None,
        trace_context: None,
        properties: None,
    };

    assert_eq!(
        context.producer_group.as_deref(),
        expected["producerGroup"].as_str()
    );
    assert_eq!(
        null_group_context.producer_group.is_none(),
        fixture["sendMessageContext"]["defaults"]["producerGroupIsNull"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        context.producer.producer_group().as_deref(),
        Some("producer-group")
    );
    assert_eq!(context.message.topic(), expected["messageTopic"]);
    assert_eq!(
        context.message.body().as_ref(),
        expected["messageBody"].as_str().unwrap().as_bytes()
    );
    assert_eq!(
        context.queue.topic(),
        Some(expected["queueTopic"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.broker_name(),
        Some(expected["queueBroker"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(context.broker_addr, expected["brokerAddr"]);
    assert_eq!(context.born_host.as_deref(), expected["bornHost"].as_str());
    assert_eq!(
        context.communication_mode.java_name(),
        expected["communicationMode"]
    );
    assert!(context.send_result.is_none());
    let exception_message = context.exception.map(ToString::to_string);
    assert_eq!(
        exception_message.as_deref(),
        expected["exceptionMessage"].as_str()
    );
    assert!(
        context
            .exception
            .is_some_and(|exception| exception.is::<std::io::Error>())
    );
    assert_eq!(
        context
            .trace_context
            .as_deref()
            .and_then(|value| value.downcast_ref::<String>())
            .map(String::as_str),
        expected["traceContext"].as_str()
    );
    assert_eq!(
        context.properties.as_ref().unwrap()["source"],
        expected["props"]["source"]
    );
    assert_eq!(context.message_type.java_name(), expected["messageType"]);
    assert_eq!(context.namespace.as_deref(), expected["namespace"].as_str());
    assert_eq!(
        CommunicationMode::values().map(CommunicationMode::java_name),
        fixture["sendMessageContext"]["communicationModes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|mode| mode.as_str().unwrap())
            .collect::<Vec<_>>()
            .as_slice()
    );
    assert_eq!(
        SendMessageType::values().map(SendMessageType::java_name),
        [
            "Normal_Msg",
            "Trans_Msg_Half",
            "Trans_msg_Commit",
            "Delay_Msg",
            "Order_Msg"
        ]
    );
}
