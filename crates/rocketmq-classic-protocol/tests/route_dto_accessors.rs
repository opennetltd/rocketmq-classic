use rocketmq_classic_protocol::{
    body::{
        ClusterInfo, ClusterInfoLookupError, SharedClusterAddrTable, SharedClusterBrokerAddrTable,
        SharedClusterBrokerNames,
    },
    route::{
        BrokerData, QueueData, SharedBrokerAddressMap, SharedBrokerDataHandle,
        SharedFilterServerList, SharedFilterServerTable, SharedQueueDataHandle,
        SharedRouteBrokerDataList, SharedRouteQueueDataList, SharedTopicQueueMappingIds,
        SharedTopicQueueMappingInfoHandle, SharedTopicQueueMappingTable, TopicQueueMappingInfo,
        TopicRouteData,
    },
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

fn route_fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap()
}

#[test]
fn route_dto_public_fields_match_invoked_java_getter_setter_values() {
    let fixture = route_fixture();
    let java = &fixture["routeDtoAccessors"];

    let broker: BrokerData = serde_json::from_value(java["brokerData"].clone()).unwrap();
    let broker_addrs = broker.broker_addrs.as_ref().map(|values| {
        values.with(|values| {
            values
                .iter()
                .map(|(key, value)| {
                    (
                        key.map_or_else(|| "null".to_owned(), |id| id.to_string()),
                        value.clone(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        })
    });
    assert_eq!(
        json!({
            "cluster": broker.cluster.as_deref(),
            "brokerName": broker.broker_name.as_deref(),
            "brokerAddrs": broker_addrs,
            "zoneName": broker.zone_name.as_deref(),
            "enableActingMaster": broker.enable_acting_master,
        }),
        java["brokerData"]
    );

    let broker_nulls: BrokerData = serde_json::from_value(java["brokerDataNulls"].clone()).unwrap();
    assert_eq!(
        json!({
            "cluster": broker_nulls.cluster.as_deref(),
            "brokerName": broker_nulls.broker_name.as_deref(),
            "brokerAddrs": broker_nulls.broker_addrs,
            "zoneName": broker_nulls.zone_name.as_deref(),
            "enableActingMaster": broker_nulls.enable_acting_master,
        }),
        java["brokerDataNulls"]
    );

    let queue: QueueData = serde_json::from_value(java["queueData"].clone()).unwrap();
    assert_eq!(
        json!({
            "brokerName": queue.broker_name.as_deref(),
            "readQueueNums": queue.read_queue_nums,
            "writeQueueNums": queue.write_queue_nums,
            "perm": queue.perm,
            "topicSysFlag": queue.topic_sys_flag,
        }),
        java["queueData"]
    );
    let queue_nulls: QueueData = serde_json::from_value(java["queueDataNulls"].clone()).unwrap();
    assert_eq!(
        json!({
            "brokerName": queue_nulls.broker_name.as_deref(),
            "readQueueNums": queue_nulls.read_queue_nums,
            "writeQueueNums": queue_nulls.write_queue_nums,
            "perm": queue_nulls.perm,
            "topicSysFlag": queue_nulls.topic_sys_flag,
        }),
        java["queueDataNulls"]
    );

    let mapping: TopicQueueMappingInfo =
        serde_json::from_value(java["topicQueueMappingInfo"].clone()).unwrap();
    let mapping_ids = mapping.curr_id_map.as_ref().map(|values| {
        values.with(|values| {
            values
                .iter()
                .map(|(key, value)| (key.to_string(), *value))
                .collect::<BTreeMap<_, _>>()
        })
    });
    assert_eq!(
        json!({
            "topic": mapping.topic.as_deref(),
            "scope": mapping.scope.as_deref(),
            "totalQueues": mapping.total_queues,
            "bname": mapping.bname.as_deref(),
            "epoch": mapping.epoch,
            "dirty": mapping.dirty,
            "currIdMap": mapping_ids,
        }),
        java["topicQueueMappingInfo"]
    );
    let mapping_nulls: TopicQueueMappingInfo =
        serde_json::from_value(java["topicQueueMappingInfoNulls"].clone()).unwrap();
    assert_eq!(
        json!({
            "topic": mapping_nulls.topic.as_deref(),
            "scope": mapping_nulls.scope.as_deref(),
            "totalQueues": mapping_nulls.total_queues,
            "bname": mapping_nulls.bname.as_deref(),
            "epoch": mapping_nulls.epoch,
            "dirty": mapping_nulls.dirty,
            "currIdMap": mapping_nulls.curr_id_map,
        }),
        java["topicQueueMappingInfoNulls"]
    );

    let route: TopicRouteData = TopicRouteData {
        order_topic_conf: Some(
            java["topicRouteData"]["orderTopicConf"]
                .as_str()
                .unwrap()
                .to_owned(),
        ),
        queue_datas: Some(
            vec![Some(
                serde_json::from_value::<QueueData>(java["queueData"].clone()).unwrap(),
            )]
            .into(),
        ),
        broker_datas: Some(
            vec![Some(BrokerData {
                broker_name: Some(
                    java["topicRouteData"]["brokerDatas"][0]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                ),
                ..Default::default()
            })]
            .into(),
        ),
        filter_server_table: Some(
            serde_json::from_value(java["topicRouteData"]["filterServerTable"].clone()).unwrap(),
        ),
        topic_queue_mapping_by_broker: Some(
            BTreeMap::<String, Option<TopicQueueMappingInfo>>::from([(
                java["topicRouteData"]["topicQueueMappingByBroker"][0]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                None,
            )])
            .into(),
        ),
    };
    let queue_names = route.queue_datas_snapshot().map(|values| {
        values
            .iter()
            .map(|value| value.as_ref().and_then(|queue| queue.broker_name.clone()))
            .collect::<Vec<_>>()
    });
    let broker_names = route.broker_datas_snapshot().map(|values| {
        values
            .iter()
            .map(|value| value.as_ref().and_then(|broker| broker.broker_name.clone()))
            .collect::<Vec<_>>()
    });
    let mapping_brokers = route
        .topic_queue_mapping_by_broker_snapshot()
        .as_ref()
        .map(|values| values.keys().cloned().collect::<Vec<_>>());
    assert_eq!(
        json!({
            "orderTopicConf": route.order_topic_conf.as_deref(),
            "queueDatas": queue_names,
            "brokerDatas": broker_names,
            "filterServerTable": route.filter_server_table_snapshot(),
            "topicQueueMappingByBroker": mapping_brokers,
        }),
        java["topicRouteData"]
    );
    let route_nulls = TopicRouteData {
        order_topic_conf: None,
        queue_datas: None,
        broker_datas: None,
        filter_server_table: None,
        topic_queue_mapping_by_broker: None,
    };
    assert_eq!(
        json!({
            "orderTopicConf": route_nulls.order_topic_conf.as_deref(),
            "queueDatas": route_nulls.queue_datas_snapshot(),
            "brokerDatas": route_nulls.broker_datas_snapshot(),
            "filterServerTable": route_nulls.filter_server_table_snapshot(),
            "topicQueueMappingByBroker": route_nulls.topic_queue_mapping_by_broker_snapshot(),
        }),
        java["topicRouteDataNulls"]
    );
}

#[test]
fn route_dto_mutable_map_accessors_preserve_java_aliases() {
    let fixture = route_fixture();
    let java = &fixture["routeDtoReferenceAliases"];

    let mut broker = BrokerData::default();
    let caller_addresses: SharedBrokerAddressMap =
        BTreeMap::from([(Some(0), Some("master:10911".to_owned()))]).into();
    broker.set_broker_addrs(Some(caller_addresses.clone()));
    let getter_addresses = broker.get_broker_addrs().unwrap();
    assert_eq!(
        caller_addresses.ptr_eq(&getter_addresses),
        java["brokerSetterRetainsMap"]
    );
    assert_eq!(
        getter_addresses.ptr_eq(&broker.get_broker_addrs().unwrap()),
        java["brokerGetterIsStable"]
    );
    caller_addresses.with_mut(|addresses| {
        addresses.insert(Some(2), Some("caller-slave:10911".to_owned()));
    });
    assert_eq!(
        getter_addresses.with(|addresses| addresses.get(&Some(2)).cloned().flatten()),
        Some("caller-slave:10911".to_owned())
    );
    assert_eq!(java["brokerCallerMutationVisible"], true);
    getter_addresses.with_mut(|addresses| {
        addresses.insert(Some(3), Some("getter-slave:10911".to_owned()));
    });
    assert_eq!(
        caller_addresses.with(|addresses| addresses.get(&Some(3)).cloned().flatten()),
        Some("getter-slave:10911".to_owned())
    );
    assert_eq!(java["brokerGetterMutationVisible"], true);

    let mut mapping = TopicQueueMappingInfo::default();
    let caller_ids: SharedTopicQueueMappingIds = BTreeMap::from([(0, 4)]).into();
    mapping.set_curr_id_map(Some(caller_ids.clone()));
    let getter_ids = mapping.get_curr_id_map().unwrap();
    assert_eq!(
        caller_ids.ptr_eq(&getter_ids),
        java["mappingSetterRetainsMap"]
    );
    assert_eq!(
        getter_ids.ptr_eq(&mapping.get_curr_id_map().unwrap()),
        java["mappingGetterIsStable"]
    );
    caller_ids.with_mut(|ids| {
        ids.insert(1, 7);
    });
    assert_eq!(getter_ids.with(|ids| ids.get(&1).copied()), Some(7));
    assert_eq!(java["mappingCallerMutationVisible"], true);
    getter_ids.with_mut(|ids| {
        ids.insert(2, 9);
    });
    assert_eq!(caller_ids.with(|ids| ids.get(&2).copied()), Some(9));
    assert_eq!(java["mappingGetterMutationVisible"], true);
}

#[test]
fn topic_route_data_collection_accessors_preserve_java_aliases() {
    let fixture = route_fixture();
    let java = &fixture["routeDtoReferenceAliases"];
    let mut route = TopicRouteData::default();

    let caller_queue = SharedQueueDataHandle::new(QueueData {
        broker_name: Some("queue-broker".to_owned()),
        read_queue_nums: 1,
        write_queue_nums: 2,
        perm: 6,
        topic_sys_flag: 0,
    });
    let caller_queues: SharedRouteQueueDataList = vec![Some(caller_queue.clone())].into();
    route.set_queue_datas(Some(caller_queues.clone()));
    let getter_queues = route.get_queue_datas().unwrap();
    assert_eq!(
        caller_queues.ptr_eq(&getter_queues),
        java["routeQueueSetterRetainsList"]
    );
    assert_eq!(
        getter_queues.ptr_eq(&route.get_queue_datas().unwrap()),
        java["routeQueueGetterIsStable"]
    );
    assert_eq!(
        caller_queue.ptr_eq(&getter_queues.with(|queues| queues[0].as_ref().unwrap().clone())),
        java["routeQueueElementIdentityRetained"]
    );
    caller_queues.with_mut(|queues| {
        queues.push(Some(SharedQueueDataHandle::new(QueueData::default())));
    });
    assert_eq!(getter_queues.with(|queues| queues.len()), 2);
    assert_eq!(java["routeQueueCallerMutationVisible"], true);
    getter_queues.with(|queues| {
        queues[0].as_ref().unwrap().with_mut(|queue| queue.perm = 5);
    });
    assert_eq!(caller_queue.with(|queue| queue.perm), 5);
    assert_eq!(java["routeQueueGetterElementMutationVisible"], true);
    getter_queues.with_mut(|queues| {
        queues.push(Some(SharedQueueDataHandle::new(QueueData::default())));
    });
    assert_eq!(caller_queues.with(|queues| queues.len()), 3);
    assert_eq!(java["routeQueueGetterMutationVisible"], true);

    let caller_broker = SharedBrokerDataHandle::new(BrokerData {
        broker_name: Some("route-broker".to_owned()),
        ..Default::default()
    });
    let caller_brokers: SharedRouteBrokerDataList = vec![Some(caller_broker.clone())].into();
    route.set_broker_datas(Some(caller_brokers.clone()));
    let getter_brokers = route.get_broker_datas().unwrap();
    assert_eq!(
        caller_brokers.ptr_eq(&getter_brokers),
        java["routeBrokerSetterRetainsList"]
    );
    assert_eq!(
        getter_brokers.ptr_eq(&route.get_broker_datas().unwrap()),
        java["routeBrokerGetterIsStable"]
    );
    assert_eq!(
        caller_broker.ptr_eq(&getter_brokers.with(|brokers| brokers[0].as_ref().unwrap().clone())),
        java["routeBrokerElementIdentityRetained"]
    );
    caller_brokers.with_mut(|brokers| {
        brokers.push(Some(SharedBrokerDataHandle::new(BrokerData::default())));
    });
    assert_eq!(getter_brokers.with(|brokers| brokers.len()), 2);
    assert_eq!(java["routeBrokerCallerMutationVisible"], true);
    getter_brokers.with(|brokers| {
        brokers[0]
            .as_ref()
            .unwrap()
            .with_mut(|broker| broker.broker_name = Some("getter-broker".to_owned()));
    });
    assert_eq!(
        caller_broker.with(|broker| broker.broker_name.clone()),
        Some("getter-broker".to_owned())
    );
    assert_eq!(java["routeBrokerGetterElementMutationVisible"], true);
    getter_brokers.with_mut(|brokers| {
        brokers.push(Some(SharedBrokerDataHandle::new(BrokerData::default())));
    });
    assert_eq!(caller_brokers.with(|brokers| brokers.len()), 3);
    assert_eq!(java["routeBrokerGetterMutationVisible"], true);

    let caller_filter_list: SharedFilterServerList = vec![Some("filter-a".to_owned())].into();
    let caller_filters: SharedFilterServerTable =
        BTreeMap::from([("filter-broker".to_owned(), Some(caller_filter_list.clone()))]).into();
    route.set_filter_server_table(Some(caller_filters.clone()));
    let getter_filters = route.get_filter_server_table().unwrap();
    assert_eq!(
        caller_filters.ptr_eq(&getter_filters),
        java["routeFilterSetterRetainsMap"]
    );
    assert_eq!(
        getter_filters.ptr_eq(&route.get_filter_server_table().unwrap()),
        java["routeFilterGetterIsStable"]
    );
    assert_eq!(
        caller_filter_list.ptr_eq(
            &getter_filters.with(|filters| filters["filter-broker"].as_ref().unwrap().clone())
        ),
        java["routeFilterNestedListIdentityRetained"]
    );
    caller_filter_list.with_mut(|filters| filters.push(Some("caller-filter".to_owned())));
    assert_eq!(
        getter_filters.with(|filters| filters["filter-broker"]
            .as_ref()
            .unwrap()
            .with(|filters| filters.len())),
        2
    );
    assert_eq!(java["routeFilterCallerMutationVisible"], true);
    getter_filters.with_mut(|filters| {
        filters.insert(
            "getter-broker".to_owned(),
            Some(vec![Some("getter-filter".to_owned())].into()),
        );
    });
    assert!(caller_filters.with(|filters| filters.contains_key("getter-broker")));
    assert_eq!(java["routeFilterGetterMutationVisible"], true);
    getter_filters.with(|filters| {
        filters["filter-broker"]
            .as_ref()
            .unwrap()
            .with_mut(|filters| filters.push(Some("getter-filter".to_owned())));
    });
    assert_eq!(caller_filter_list.with(|filters| filters.len()), 3);
    assert_eq!(java["routeFilterGetterNestedMutationVisible"], true);

    let caller_mapping = SharedTopicQueueMappingInfoHandle::new(TopicQueueMappingInfo {
        topic: Some("route-topic".to_owned()),
        total_queues: 2,
        bname: Some("mapping-broker".to_owned()),
        epoch: 4,
        ..Default::default()
    });
    let caller_mappings: SharedTopicQueueMappingTable =
        BTreeMap::from([("mapping-broker".to_owned(), Some(caller_mapping.clone()))]).into();
    route.set_topic_queue_mapping_by_broker(Some(caller_mappings.clone()));
    let getter_mappings = route.get_topic_queue_mapping_by_broker().unwrap();
    assert_eq!(
        caller_mappings.ptr_eq(&getter_mappings),
        java["routeMappingSetterRetainsMap"]
    );
    assert_eq!(
        getter_mappings.ptr_eq(&route.get_topic_queue_mapping_by_broker().unwrap()),
        java["routeMappingGetterIsStable"]
    );
    assert_eq!(
        caller_mapping.ptr_eq(
            &getter_mappings.with(|mappings| mappings["mapping-broker"].as_ref().unwrap().clone())
        ),
        java["routeMappingElementIdentityRetained"]
    );
    caller_mappings.with_mut(|mappings| {
        mappings.insert("caller-broker".to_owned(), None);
    });
    assert_eq!(getter_mappings.with(|mappings| mappings.len()), 2);
    assert_eq!(java["routeMappingCallerMutationVisible"], true);
    getter_mappings.with(|mappings| {
        mappings["mapping-broker"]
            .as_ref()
            .unwrap()
            .with_mut(|mapping| mapping.dirty = true);
    });
    assert!(caller_mapping.with(|mapping| mapping.dirty));
    assert_eq!(java["routeMappingGetterElementMutationVisible"], true);
    getter_mappings.with_mut(|mappings| {
        mappings.insert(
            "getter-broker".to_owned(),
            Some(SharedTopicQueueMappingInfoHandle::new(
                TopicQueueMappingInfo::default(),
            )),
        );
    });
    assert_eq!(caller_mappings.with(|mappings| mappings.len()), 3);
    assert_eq!(java["routeMappingGetterMutationVisible"], true);
}

#[test]
fn cluster_info_accessors_match_invoked_java_surface_and_nullability() {
    let fixture = route_fixture();
    let java = &fixture["clusterInfoAccessors"];
    let methods = java["declaredPublicMethods"].as_array().unwrap();
    let method_names = [
        "getBrokerAddrTable",
        "setBrokerAddrTable",
        "getClusterAddrTable",
        "setClusterAddrTable",
        "retrieveAllAddrByCluster",
        "retrieveAllClusterNames",
        "equals",
        "hashCode",
    ];
    for name in method_names {
        assert!(
            methods
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(name)),
            "Java ClusterInfo is missing {name}"
        );
    }
    assert_eq!(methods.len(), method_names.len());

    let mut info = ClusterInfo::default();
    assert!(info.get_broker_addr_table().is_none());
    assert!(info.get_cluster_addr_table().is_none());
    assert_eq!(java["defaultBrokerAddrTableIsNull"], true);
    assert_eq!(java["defaultClusterAddrTableIsNull"], true);

    let broker = SharedBrokerDataHandle::new(BrokerData {
        broker_name: Some("broker-a".to_owned()),
        broker_addrs: Some(BTreeMap::from([(Some(0), Some("master:10911".to_owned()))]).into()),
        ..Default::default()
    });
    let brokers: SharedClusterBrokerAddrTable =
        HashMap::from([(Some("broker-a".to_owned()), Some(broker.clone()))]).into();
    let names: SharedClusterBrokerNames = HashSet::from([Some("broker-a".to_owned())]).into();
    let clusters: SharedClusterAddrTable =
        HashMap::from([(Some("cluster-a".to_owned()), Some(names))]).into();
    info.set_broker_addr_table(Some(brokers));
    info.set_cluster_addr_table(Some(clusters));
    assert_eq!(
        info.get_broker_addr_table()
            .unwrap()
            .with(|table| table[&Some("broker-a".to_owned())].is_some()),
        java["brokerGetterReturnsValue"]
    );
    assert_eq!(
        info.get_cluster_addr_table().unwrap().with(|table| {
            table[&Some("cluster-a".to_owned())]
                .as_ref()
                .unwrap()
                .with(|names| names.contains(&Some("broker-a".to_owned())))
        }),
        java["clusterGetterReturnsValue"]
    );
    info.set_broker_addr_table(None);
    info.set_cluster_addr_table(None);
    assert!(info.get_broker_addr_table().is_none());
    assert!(info.get_cluster_addr_table().is_none());
    assert_eq!(java["brokerSetterPreservesNull"], true);
    assert_eq!(java["clusterSetterPreservesNull"], true);
}

#[test]
fn cluster_info_mutable_tables_and_nested_values_preserve_java_aliases() {
    let fixture = route_fixture();
    let java = &fixture["clusterInfoReferenceAliases"];
    let mut info = ClusterInfo::default();

    let caller_broker = SharedBrokerDataHandle::new(BrokerData::default());
    let caller_brokers: SharedClusterBrokerAddrTable =
        HashMap::from([(Some("broker-a".to_owned()), Some(caller_broker.clone()))]).into();
    info.set_broker_addr_table(Some(caller_brokers.clone()));
    let getter_brokers = info.get_broker_addr_table().unwrap();
    assert_eq!(
        caller_brokers.ptr_eq(&getter_brokers),
        java["brokerSetterRetainsMap"]
    );
    assert_eq!(
        getter_brokers.ptr_eq(&info.get_broker_addr_table().unwrap()),
        java["brokerGetterIsStable"]
    );
    assert_eq!(
        caller_broker.ptr_eq(&getter_brokers.with(|table| {
            table[&Some("broker-a".to_owned())]
                .as_ref()
                .unwrap()
                .clone()
        })),
        java["brokerElementIdentityRetained"]
    );
    caller_brokers.with_mut(|table| {
        table.insert(
            Some("caller-broker".to_owned()),
            Some(SharedBrokerDataHandle::new(BrokerData::default())),
        );
    });
    assert_eq!(getter_brokers.with(|table| table.len()), 2);
    assert_eq!(java["brokerCallerMutationVisible"], true);
    getter_brokers.with_mut(|table| {
        table.insert(
            Some("getter-broker".to_owned()),
            Some(SharedBrokerDataHandle::new(BrokerData::default())),
        );
    });
    assert!(caller_brokers.with(|table| table.contains_key(&Some("getter-broker".to_owned()))));
    assert_eq!(java["brokerGetterMutationVisible"], true);
    caller_broker.with_mut(|broker| broker.zone_name = Some("caller-zone".to_owned()));
    assert_eq!(
        caller_broker.with(|broker| broker.zone_name.clone()),
        Some("caller-zone".to_owned())
    );
    assert_eq!(java["brokerElementCallerMutationVisible"], true);
    getter_brokers.with(|table| {
        table[&Some("broker-a".to_owned())]
            .as_ref()
            .unwrap()
            .with_mut(|broker| broker.zone_name = Some("getter-zone".to_owned()));
    });
    assert_eq!(
        caller_broker.with(|broker| broker.zone_name.clone()),
        Some("getter-zone".to_owned())
    );
    assert_eq!(java["brokerElementGetterMutationVisible"], true);

    let caller_names: SharedClusterBrokerNames =
        HashSet::from([Some("broker-a".to_owned())]).into();
    let caller_clusters: SharedClusterAddrTable =
        HashMap::from([(Some("cluster-a".to_owned()), Some(caller_names.clone()))]).into();
    info.set_cluster_addr_table(Some(caller_clusters.clone()));
    let getter_clusters = info.get_cluster_addr_table().unwrap();
    assert_eq!(
        caller_clusters.ptr_eq(&getter_clusters),
        java["clusterSetterRetainsMap"]
    );
    assert_eq!(
        getter_clusters.ptr_eq(&info.get_cluster_addr_table().unwrap()),
        java["clusterGetterIsStable"]
    );
    assert_eq!(
        caller_names.ptr_eq(&getter_clusters.with(|table| {
            table[&Some("cluster-a".to_owned())]
                .as_ref()
                .unwrap()
                .clone()
        })),
        java["clusterSetIdentityRetained"]
    );
    caller_clusters.with_mut(|table| {
        table.insert(
            Some("caller-cluster".to_owned()),
            Some(HashSet::from([Some("broker-b".to_owned())]).into()),
        );
    });
    assert_eq!(getter_clusters.with(|table| table.len()), 2);
    assert_eq!(java["clusterCallerMutationVisible"], true);
    getter_clusters.with_mut(|table| {
        table.insert(
            Some("getter-cluster".to_owned()),
            Some(HashSet::from([Some("broker-c".to_owned())]).into()),
        );
    });
    assert!(caller_clusters.with(|table| table.contains_key(&Some("getter-cluster".to_owned()))));
    assert_eq!(java["clusterGetterMutationVisible"], true);
    caller_names.with_mut(|names| {
        names.insert(Some("caller-broker".to_owned()));
    });
    assert!(getter_clusters.with(|table| {
        table[&Some("cluster-a".to_owned())]
            .as_ref()
            .unwrap()
            .with(|names| names.contains(&Some("caller-broker".to_owned())))
    }));
    assert_eq!(java["clusterSetCallerMutationVisible"], true);
    getter_clusters.with(|table| {
        table[&Some("cluster-a".to_owned())]
            .as_ref()
            .unwrap()
            .with_mut(|names| {
                names.insert(Some("getter-broker".to_owned()));
            });
    });
    assert!(caller_names.with(|names| names.contains(&Some("getter-broker".to_owned()))));
    assert_eq!(java["clusterSetGetterMutationVisible"], true);
}

#[test]
fn cluster_info_retrieval_matches_java_null_and_missing_boundaries() {
    let fixture = route_fixture();
    let java = &fixture["clusterInfoRetrieval"];

    let mut empty = ClusterInfo::default();
    empty.set_broker_addr_table(Some(HashMap::new().into()));
    empty.set_cluster_addr_table(Some(HashMap::new().into()));
    assert_eq!(
        serde_json::json!(empty.retrieve_all_cluster_names().unwrap()),
        java["emptyClusterNames"]
    );
    assert_eq!(
        serde_json::json!(empty.retrieve_all_addr_by_cluster(Some("missing")).unwrap()),
        java["missingClusterAddresses"]
    );

    let broker = SharedBrokerDataHandle::new(BrokerData {
        broker_addrs: Some(BTreeMap::from([(Some(0), Some("master:10911".to_owned()))]).into()),
        ..Default::default()
    });
    let mut populated = ClusterInfo::default();
    populated.set_broker_addr_table(Some(
        HashMap::from([(Some("broker-a".to_owned()), Some(broker))]).into(),
    ));
    populated.set_cluster_addr_table(Some(
        HashMap::from([(
            Some("cluster-a".to_owned()),
            Some(HashSet::from([Some("broker-a".to_owned())]).into()),
        )])
        .into(),
    ));
    assert_eq!(
        serde_json::json!(populated.retrieve_all_cluster_names().unwrap()),
        java["clusterNames"]
    );
    assert_eq!(
        serde_json::json!(
            populated
                .retrieve_all_addr_by_cluster(Some("cluster-a"))
                .unwrap()
        ),
        java["clusterAddresses"]
    );

    let mut missing_broker = ClusterInfo::default();
    missing_broker.set_broker_addr_table(Some(HashMap::new().into()));
    missing_broker.set_cluster_addr_table(Some(
        HashMap::from([(
            Some("cluster-a".to_owned()),
            Some(HashSet::from([Some("missing".to_owned())]).into()),
        )])
        .into(),
    ));
    assert_eq!(
        serde_json::json!(
            missing_broker
                .retrieve_all_addr_by_cluster(Some("cluster-a"))
                .unwrap()
        ),
        java["missingBrokerAddresses"]
    );

    let mut null_broker = ClusterInfo::default();
    null_broker.set_broker_addr_table(Some(
        HashMap::from([(Some("broker-a".to_owned()), None)]).into(),
    ));
    null_broker.set_cluster_addr_table(Some(
        HashMap::from([(
            Some("cluster-a".to_owned()),
            Some(HashSet::from([Some("broker-a".to_owned())]).into()),
        )])
        .into(),
    ));
    assert_eq!(
        serde_json::json!(
            null_broker
                .retrieve_all_addr_by_cluster(Some("cluster-a"))
                .unwrap()
        ),
        java["nullBrokerDataAddresses"]
    );

    let null_address = SharedBrokerDataHandle::new(BrokerData {
        broker_addrs: Some(BTreeMap::from([(Some(0), None)]).into()),
        ..Default::default()
    });
    let mut null_address_info = ClusterInfo::default();
    null_address_info.set_broker_addr_table(Some(
        HashMap::from([(Some("broker-a".to_owned()), Some(null_address))]).into(),
    ));
    null_address_info.set_cluster_addr_table(Some(
        HashMap::from([(
            Some("cluster-a".to_owned()),
            Some(HashSet::from([Some("broker-a".to_owned())]).into()),
        )])
        .into(),
    ));
    assert_eq!(
        serde_json::json!(
            null_address_info
                .retrieve_all_addr_by_cluster(Some("cluster-a"))
                .unwrap()
        ),
        java["nullBrokerAddressValue"]
    );

    let null_key_broker = SharedBrokerDataHandle::new(BrokerData {
        broker_addrs: Some(BTreeMap::from([(Some(0), Some("null-key:10911".to_owned()))]).into()),
        ..Default::default()
    });
    let mut null_keys = ClusterInfo::default();
    null_keys.set_broker_addr_table(Some(HashMap::from([(None, Some(null_key_broker))]).into()));
    null_keys.set_cluster_addr_table(Some(
        HashMap::from([(None, Some(HashSet::from([None]).into()))]).into(),
    ));
    assert_eq!(
        serde_json::json!(null_keys.retrieve_all_cluster_names().unwrap()),
        java["nullClusterName"]
    );
    assert_eq!(
        serde_json::json!(null_keys.retrieve_all_addr_by_cluster(None).unwrap()),
        java["nullClusterBrokerAddress"]
    );

    let mut null_tables = ClusterInfo::default();
    assert_eq!(
        null_tables.retrieve_all_cluster_names(),
        Err(ClusterInfoLookupError::NullClusterAddrTable)
    );
    assert_eq!(
        null_tables.retrieve_all_addr_by_cluster(Some("cluster-a")),
        Err(ClusterInfoLookupError::NullClusterAddrTable)
    );
    assert_eq!(java["nullClusterTableNamesOutcome"], "NullPointerException");
    assert_eq!(
        java["nullClusterTableAddressesOutcome"],
        "NullPointerException"
    );

    null_tables.set_cluster_addr_table(Some(
        HashMap::from([(Some("cluster-a".to_owned()), None)]).into(),
    ));
    assert_eq!(
        null_tables.retrieve_all_addr_by_cluster(Some("cluster-a")),
        Err(ClusterInfoLookupError::NullClusterBrokerNames)
    );
    assert_eq!(java["nullBrokerNameSetOutcome"], "NullPointerException");

    null_tables.set_cluster_addr_table(Some(
        HashMap::from([(
            Some("cluster-a".to_owned()),
            Some(HashSet::from([Some("broker-a".to_owned())]).into()),
        )])
        .into(),
    ));
    assert_eq!(
        null_tables.retrieve_all_addr_by_cluster(Some("cluster-a")),
        Err(ClusterInfoLookupError::NullBrokerAddrTable)
    );
    assert_eq!(java["nullBrokerTableOutcome"], "NullPointerException");

    let no_address_table = SharedBrokerDataHandle::new(BrokerData::default());
    null_tables.set_broker_addr_table(Some(
        HashMap::from([(Some("broker-a".to_owned()), Some(no_address_table))]).into(),
    ));
    assert_eq!(
        null_tables.retrieve_all_addr_by_cluster(Some("cluster-a")),
        Err(ClusterInfoLookupError::NullBrokerAddressMap)
    );
    assert_eq!(
        java["nullBrokerAddressTableOutcome"],
        "NullPointerException"
    );

    null_tables.set_cluster_addr_table(Some(
        HashMap::from([(Some("cluster-a".to_owned()), Some(HashSet::new().into()))]).into(),
    ));
    null_tables.set_broker_addr_table(None);
    assert_eq!(
        null_tables.retrieve_all_addr_by_cluster(Some("cluster-a")),
        Ok(Vec::new())
    );
}

#[test]
fn cluster_info_equality_and_hash_match_java_implementations() {
    let fixture = route_fixture();
    let java = &fixture["clusterInfoValueMethods"];
    let defaults = ClusterInfo::default();
    assert_eq!(defaults, ClusterInfo::default());
    assert_eq!(
        defaults.java_hash_code(),
        java["defaultHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(java["defaultEquals"], true);

    let mut empty = ClusterInfo::default();
    empty.set_broker_addr_table(Some(HashMap::new().into()));
    assert_ne!(defaults, empty);
    assert_eq!(
        empty.java_hash_code(),
        java["emptyHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(java["nullVersusEmptyEquals"], false);

    let left = cluster_info_for_value_method("cluster-a", "master");
    let right = cluster_info_for_value_method("cluster-b", "master");
    assert_eq!(left, right);
    assert_eq!(
        left.java_hash_code(),
        java["metadataOnlyBrokerLeftHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        right.java_hash_code(),
        java["metadataOnlyBrokerRightHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(java["metadataOnlyBrokerChangeEquals"], true);

    let changed = cluster_info_for_value_method("cluster-a", "slave");
    assert_ne!(left, changed);
    assert_eq!(
        changed.java_hash_code(),
        java["changedBrokerAddressHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(java["changedBrokerAddressEquals"], false);
}

fn cluster_info_for_value_method(broker_cluster: &str, address: &str) -> ClusterInfo {
    let broker = SharedBrokerDataHandle::new(BrokerData {
        cluster: Some(broker_cluster.to_owned()),
        broker_name: Some("broker-a".to_owned()),
        broker_addrs: Some(BTreeMap::from([(Some(0), Some(address.to_owned()))]).into()),
        enable_acting_master: broker_cluster == "cluster-a",
        ..Default::default()
    });
    let mut info = ClusterInfo::default();
    info.set_broker_addr_table(Some(
        HashMap::from([(Some("broker-a".to_owned()), Some(broker))]).into(),
    ));
    info.set_cluster_addr_table(Some(
        HashMap::from([(
            Some("cluster-a".to_owned()),
            Some(HashSet::from([Some("broker-a".to_owned())]).into()),
        )])
        .into(),
    ));
    info
}
