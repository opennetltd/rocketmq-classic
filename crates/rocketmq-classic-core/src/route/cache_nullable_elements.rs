use super::RouteCache;
use crate::runtime::RuntimeError;
use rocketmq_classic_protocol::route::TopicRouteData;

#[test]
fn null_nested_route_values_match_invoked_runtime_failure_without_publication() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
    for case in fixture["runtimeNullNestedRouteElements"]
        .as_array()
        .unwrap()
    {
        assert_eq!(case["requests"], 1, "{}", case["id"]);

        let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
        let cache = RouteCache::default();
        if case["outcome"] == "OK" {
            assert_eq!(cache.replace("topic", route).unwrap(), case["changed"]);
            assert_eq!(case["cachedRouteExists"], true, "{}", case["id"]);
            let snapshot = cache.snapshot().unwrap();
            let filter_table = snapshot.topics["topic"]
                .route
                .filter_server_table_snapshot()
                .unwrap();
            assert!(filter_table.contains_key("broker"), "{}", case["id"]);
            assert!(filter_table["broker"].is_none(), "{}", case["id"]);
        } else {
            assert_eq!(case["outcome"], "NullPointerException", "{}", case["id"]);
            assert_eq!(case["cachedRouteExists"], false, "{}", case["id"]);
            assert!(matches!(
                cache.replace("topic", route),
                Err(RuntimeError::InvalidRoute(_))
            ));
            let snapshot = cache.snapshot().unwrap();
            assert!(!snapshot.raw_routes.contains_key("topic"), "{}", case["id"]);
            assert!(!snapshot.topics.contains_key("topic"), "{}", case["id"]);
        }
    }
}
