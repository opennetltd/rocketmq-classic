use rocketmq_classic_core::route::TopicRouteDataHandle;
use rocketmq_classic_protocol::route::{QueueData, TopicRouteData};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_IDENTITY_PROBE: AtomicUsize = AtomicUsize::new(0);

pub fn same_route_handle(left: &TopicRouteDataHandle, right: &TopicRouteDataHandle) -> bool {
    let original = right.snapshot().unwrap();
    let marker = identity_probe_route();
    let marker_value = serde_json::to_value(&marker).unwrap();

    right.with_mut(|route| *route = marker).unwrap();
    let aliases = serde_json::to_value(left.snapshot().unwrap()).unwrap() == marker_value;
    right.with_mut(|route| *route = original).unwrap();

    aliases
}

pub fn same_route_option(
    value: Option<TopicRouteDataHandle>,
    expected: &TopicRouteDataHandle,
) -> bool {
    value.is_some_and(|value| same_route_handle(&value, expected))
}

fn identity_probe_route() -> TopicRouteData {
    let id = NEXT_IDENTITY_PROBE.fetch_add(1, Ordering::Relaxed);
    let mut route = TopicRouteData::default();
    route.set_queue_datas_owned(Some(vec![Some(QueueData {
        broker_name: Some(format!("__identity_probe_{id}__")),
        write_queue_nums: 1,
        ..QueueData::default()
    })]));
    route
}
