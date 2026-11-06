use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    rc::Rc,
    sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, Weak},
    thread::ThreadId,
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::route::TopicRouteData;

use crate::runtime::RuntimeError;

use super::table::TopicRouteDataHandle;

#[derive(Clone, Debug)]
pub struct TopicSnapshot {
    pub route: Arc<TopicRouteData>,
    pub publish_queues: Vec<MessageQueue>,
    pub subscribe_queues: BTreeSet<MessageQueue>,
    pub endpoints: BTreeMap<MessageQueue, String>,
    pub order_topic: bool,
}

impl TopicSnapshot {
    /// Converts a route DTO into the immutable publish, subscribe, and static
    /// endpoint view used by the client runtime.
    ///
    /// This is the native counterpart to the route conversion methods on
    /// Java's `MQClientInstance`. Invalid Java route states return a typed
    /// error before any runtime cache is mutated.
    pub fn from_route_data(topic: &str, route: &TopicRouteData) -> Result<Self, RuntimeError> {
        Self::from_nullable_route_data(Some(topic), Some(route))
    }

    /// Converts nullable Java route-converter arguments. A null topic is
    /// retained in the generated queues; a null route returns a typed error.
    pub fn from_nullable_route_data(
        topic: Option<&str>,
        route: Option<&TopicRouteData>,
    ) -> Result<Self, RuntimeError> {
        convert_topic_route_data(topic, route)
    }
}

#[derive(Clone, Debug, Default)]
pub struct RouteSnapshot {
    /// Source raw topic table; explicit map writes do not rebuild `topics`.
    pub raw_routes: BTreeMap<String, TopicRouteDataHandle>,
    /// Last converted publication, independent of later raw map mutations.
    pub topics: BTreeMap<String, Arc<TopicSnapshot>>,
    pub broker_addresses: BTreeMap<String, BTreeMap<Option<i64>, Option<String>>>,
    pub(crate) broker_versions: BTreeMap<(String, String), i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerAddress {
    pub address: String,
    pub slave: bool,
    pub version: i32,
}

impl RouteSnapshot {
    /// Java selects a random BrokerData from this topic's raw cached route,
    /// then its master or a random available slave. Global broker metadata may
    /// have since been overwritten by another topic and must not be used here.
    pub fn broker_address_by_topic(&self, topic: &str) -> Result<Option<String>, RuntimeError> {
        self.select_topic_broker(topic, |length| rand::random_range(0..length))
    }

    fn select_topic_broker(
        &self,
        topic: &str,
        mut select: impl FnMut(usize) -> usize,
    ) -> Result<Option<String>, RuntimeError> {
        let Some(route) = self.raw_routes.get(topic) else {
            return Ok(None);
        };
        let route = route.snapshot()?;
        let broker_snapshot = route.broker_datas_snapshot();
        let brokers = broker_snapshot
            .as_deref()
            .ok_or(RuntimeError::InvalidRoute("null broker data list"))?;
        if brokers.is_empty() {
            return Ok(None);
        }
        let broker = brokers[select(brokers.len())]
            .as_ref()
            .ok_or(RuntimeError::InvalidRoute("selected broker is null"))?;
        let addresses = broker
            .broker_addrs
            .as_ref()
            .ok_or(RuntimeError::InvalidRoute(
                "selected broker has null address map",
            ))?;
        addresses.with(|addresses| {
            if let Some(master) = addresses.get(&Some(0)).and_then(Option::as_deref) {
                return Ok(Some(master.to_owned()));
            }
            if addresses.is_empty() {
                return Err(RuntimeError::InvalidRoute(
                    "selected broker has no addresses",
                ));
            }
            Ok(addresses
                .values()
                .nth(select(addresses.len()))
                .and_then(Option::clone))
        })
    }

    pub fn publish_address(&self, broker: &str) -> Option<&str> {
        self.broker_addresses
            .get(broker)?
            .get(&Some(0))
            .and_then(Option::as_deref)
    }

    /// Nullable Java entry point for `findBrokerAddressInPublish`; a null
    /// broker name returns null before the address table is read.
    pub fn publish_address_nullable(&self, broker: Option<&str>) -> Option<&str> {
        self.publish_address(broker?)
    }

    pub fn subscribe_address(
        &self,
        broker: &str,
        id: i64,
        only_this_broker: bool,
    ) -> Result<Option<BrokerAddress>, RuntimeError> {
        let Some(addresses) = self.broker_addresses.get(broker) else {
            return Ok(None);
        };
        let exact_or_adjacent = addresses
            .get_key_value(&Some(id))
            .filter(|(_, address)| address.is_some())
            .or_else(|| {
                (id != 0)
                    .then(|| addresses.get_key_value(&Some(id.wrapping_add(1))))
                    .flatten()
                    .filter(|(_, address)| address.is_some())
            });
        let is_exact_or_adjacent = exact_or_adjacent.is_some();
        let Some(selected) = exact_or_adjacent.or_else(|| {
            (!only_this_broker)
                .then(|| addresses.first_key_value())
                .flatten()
        }) else {
            return Ok(None);
        };
        let slave = if is_exact_or_adjacent {
            id != 0
        } else {
            selected
                .0
                .ok_or(RuntimeError::InvalidRoute("null broker address id"))?
                != 0
        };
        let Some(address) = selected.1.as_ref() else {
            return Ok(None);
        };
        Ok(Some(BrokerAddress {
            address: address.clone(),
            // Java changes the slave flag only for the arbitrary-entry fallback,
            // not for brokerId + 1 (including signed overflow/negative IDs).
            slave,
            version: self
                .broker_versions
                .get(&(broker.to_owned(), address.clone()))
                .copied()
                .unwrap_or(0),
        }))
    }

    /// Nullable Java entry point for `findBrokerAddressInSubscribe`; a null
    /// broker name returns null before broker ID/address selection.
    pub fn subscribe_address_nullable(
        &self,
        broker: Option<&str>,
        id: i64,
        only_this_broker: bool,
    ) -> Result<Option<BrokerAddress>, RuntimeError> {
        let Some(broker) = broker else {
            return Ok(None);
        };
        self.subscribe_address(broker, id, only_this_broker)
    }

    pub fn admin_address(&self, broker: &str) -> Result<Option<BrokerAddress>, RuntimeError> {
        // Java iterates until it finds a nonnull value. HashMap order is not
        // observable when several valid entries exist. Long IDs are unboxed
        // only after a nonnull address is found.
        let Some(addresses) = self.broker_addresses.get(broker) else {
            return Ok(None);
        };
        for (id, address) in addresses {
            let Some(address) = address else {
                continue;
            };
            let id = id.ok_or(RuntimeError::InvalidRoute("null broker address id"))?;
            return Ok(Some(BrokerAddress {
                address: address.clone(),
                slave: id != 0,
                version: self
                    .broker_versions
                    .get(&(broker.to_owned(), address.clone()))
                    .copied()
                    .unwrap_or(0),
            }));
        }
        Ok(None)
    }

    /// Nullable Java entry point for `findBrokerAddressInAdmin`; a null
    /// broker name returns null before address-table traversal.
    pub fn admin_address_nullable(
        &self,
        broker: Option<&str>,
    ) -> Result<Option<BrokerAddress>, RuntimeError> {
        let Some(broker) = broker else {
            return Ok(None);
        };
        self.admin_address(broker)
    }

    /// Resolves a queue's broker through the cached static endpoints, falling
    /// back to the queue broker only when no endpoint map is present or empty.
    /// A null Java topic causes a route error before that fallback can run.
    pub fn broker_name<'a>(
        &'a self,
        queue: &'a MessageQueue,
    ) -> Result<Option<&'a str>, RuntimeError> {
        let topic = queue
            .topic()
            .ok_or(RuntimeError::InvalidRoute("null message queue topic"))?;
        let endpoints = self.topics.get(topic).map(|topic| &topic.endpoints);
        Ok(match endpoints.filter(|endpoints| !endpoints.is_empty()) {
            Some(endpoints) => endpoints.get(queue).map(String::as_str),
            None => queue.broker_name(),
        })
    }

    /// Null-tolerant entry for Java calls that may pass a null MessageQueue.
    /// Java dereferences the argument immediately, so a null queue is an error.
    pub fn broker_name_nullable<'a>(
        &'a self,
        queue: Option<&'a MessageQueue>,
    ) -> Result<Option<&'a str>, RuntimeError> {
        let queue = queue.ok_or(RuntimeError::InvalidRoute("null message queue"))?;
        self.broker_name(queue)
    }
}

#[derive(Default)]
struct RawRouteMutationState {
    owner: Option<ThreadId>,
}

#[derive(Default)]
struct RawRouteMutationGate {
    state: Mutex<RawRouteMutationState>,
    available: Condvar,
}

impl RawRouteMutationGate {
    fn lock(self: &Arc<Self>) -> Result<RawRouteMutationGuard, RuntimeError> {
        let owner = std::thread::current().id();
        let mut state = self.lock_state();
        loop {
            match state.owner.as_ref() {
                Some(current) if *current == owner => {
                    return Err(RuntimeError::RecursiveRouteTableMutation);
                }
                Some(_) => {
                    state = match self.available.wait(state) {
                        Ok(state) => state,
                        Err(poisoned) => {
                            self.state.clear_poison();
                            poisoned.into_inner()
                        }
                    };
                }
                None => {
                    state.owner = Some(owner);
                    break;
                }
            }
        }
        drop(state);
        Ok(RawRouteMutationGuard {
            gate: self.clone(),
            owner,
            _not_send: PhantomData,
        })
    }

    fn lock_state(&self) -> MutexGuard<'_, RawRouteMutationState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                self.state.clear_poison();
                poisoned.into_inner()
            }
        }
    }
}

pub(crate) struct RawRouteMutationGuard {
    gate: Arc<RawRouteMutationGate>,
    owner: ThreadId,
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for RawRouteMutationGuard {
    fn drop(&mut self) {
        let mut state = self.gate.lock_state();
        if state.owner.as_ref() == Some(&self.owner) {
            state.owner = None;
            self.gate.available.notify_one();
        }
    }
}

#[derive(Default)]
struct RawRouteMutationGates {
    gates: Mutex<HashMap<String, Weak<RawRouteMutationGate>>>,
}

impl RawRouteMutationGates {
    fn gate(&self, key: &str) -> Result<Arc<RawRouteMutationGate>, RuntimeError> {
        let mut gates = self
            .gates
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if let Some(gate) = gates.get(key).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(RawRouteMutationGate::default());
        gates.insert(key.to_owned(), Arc::downgrade(&gate));
        if gates.len() >= 256 {
            gates.retain(|_, gate| gate.strong_count() > 0);
        }
        Ok(gate)
    }
}

#[derive(Clone, Default)]
pub(crate) struct RouteCache {
    snapshot: Arc<RwLock<Arc<RouteSnapshot>>>,
    raw_route_mutation_gates: Arc<RawRouteMutationGates>,
}

impl RouteCache {
    pub(super) fn update_raw<R>(
        &self,
        update: impl FnOnce(&mut BTreeMap<String, TopicRouteDataHandle>) -> (bool, R),
    ) -> Result<R, RuntimeError> {
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let mut next = current.as_ref().clone();
        let (changed, result) = update(&mut next.raw_routes);
        if changed {
            *current = Arc::new(next);
        }
        Ok(result)
    }

    pub(crate) fn lock_raw_route_key(
        &self,
        key: &str,
    ) -> Result<RawRouteMutationGuard, RuntimeError> {
        self.raw_route_mutation_gates.gate(key)?.lock()
    }

    pub(crate) fn record_version(
        &self,
        broker: &str,
        address: &str,
        version: i32,
    ) -> Result<(), RuntimeError> {
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let mut next = current.as_ref().clone();
        next.broker_versions
            .insert((broker.to_owned(), address.to_owned()), version);
        *current = Arc::new(next);
        Ok(())
    }

    pub(crate) fn clean_offline(&self) -> Result<(), RuntimeError> {
        let source = self.snapshot()?;
        let routes = source.raw_routes.values().cloned().collect::<Vec<_>>();
        let mut deferred_updates = Vec::new();

        for (broker_name, addresses) in &source.broker_addresses {
            if addresses.is_empty() {
                self.remove_broker_address_if_same(broker_name, addresses)?;
                continue;
            }

            let mut retained_addresses = addresses.clone();
            for (broker_id, address) in addresses {
                if !broker_address_exists_in_raw_routes(&routes, address.as_deref())? {
                    retained_addresses.remove(broker_id);
                }
            }

            if retained_addresses.is_empty() {
                // Java removes an empty outer broker row through the live-map
                // iterator immediately; nonempty inner-map replacements are
                // held until the full traversal succeeds.
                self.remove_broker_address_if_same(broker_name, addresses)?;
            } else {
                deferred_updates.push((broker_name.clone(), addresses.clone(), retained_addresses));
            }
        }

        if !deferred_updates.is_empty() {
            let mut current = self
                .snapshot
                .write()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            let mut next = current.as_ref().clone();
            let mut changed = false;
            for (broker_name, expected, retained) in deferred_updates {
                if current.broker_addresses.get(&broker_name) == Some(&expected) {
                    next.broker_addresses.insert(broker_name, retained);
                    changed = true;
                }
            }
            if changed {
                *current = Arc::new(next);
            }
        }
        Ok(())
    }

    fn remove_broker_address_if_same(
        &self,
        broker_name: &str,
        expected: &BTreeMap<Option<i64>, Option<String>>,
    ) -> Result<(), RuntimeError> {
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if current.broker_addresses.get(broker_name) == Some(expected) {
            let mut next = current.as_ref().clone();
            next.broker_addresses.remove(broker_name);
            *current = Arc::new(next);
        }
        Ok(())
    }
    pub(crate) fn snapshot(&self) -> Result<Arc<RouteSnapshot>, RuntimeError> {
        self.snapshot
            .read()
            .map(|value| value.clone())
            .map_err(|_| RuntimeError::RegistryPoisoned)
    }

    pub(crate) fn replace(&self, topic: &str, route: TopicRouteData) -> Result<bool, RuntimeError> {
        self.replace_for_owner_update(topic, route, false)
    }

    pub(crate) fn route_changed(
        &self,
        topic: &str,
        route: &TopicRouteData,
    ) -> Result<bool, RuntimeError> {
        let old = self.snapshot()?.raw_routes.get(topic).cloned();
        match old {
            Some(old) => Ok(!routes_equal(&old.snapshot()?, route)?),
            None => Ok(true),
        }
    }

    pub(crate) fn replace_for_owner_update(
        &self,
        topic: &str,
        route: TopicRouteData,
        force: bool,
    ) -> Result<bool, RuntimeError> {
        // A raw map write can insert data that normal refresh cannot convert.
        // Java compares cached raw data before entering conversion/publication.
        if !force && !self.route_changed(topic, &route)? {
            return Ok(false);
        }
        let mut topic_snapshot = TopicSnapshot::from_route_data(topic, &route)?;
        let _gate = self.lock_raw_route_key(topic)?;
        loop {
            let observed = self.snapshot()?.raw_routes.get(topic).cloned();
            let unchanged = match &observed {
                Some(old) => routes_equal(&old.snapshot()?, &topic_snapshot.route)?,
                None => false,
            };
            if unchanged && !force {
                return Ok(false);
            }
            let mut current = self
                .snapshot
                .write()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            let still_same = match (current.raw_routes.get(topic), observed.as_ref()) {
                (None, None) => true,
                (Some(current), Some(observed)) => current.ptr_eq(observed),
                _ => false,
            };
            if !still_same {
                drop(current);
                continue;
            }
            // Java retains existing endpoints when a new conversion is empty.
            if topic_snapshot.endpoints.is_empty() {
                if let Some(old) = current.topics.get(topic) {
                    topic_snapshot.endpoints.clone_from(&old.endpoints);
                }
            }
            let mut next = current.as_ref().clone();
            next.raw_routes.insert(
                topic.to_owned(),
                TopicRouteDataHandle::new(topic_snapshot.route.java_copy_constructed()),
            );
            for broker in topic_snapshot
                .route
                .broker_datas_snapshot()
                .unwrap_or_default()
                .iter()
                .flatten()
            {
                let Some(name) = broker.broker_name.clone() else {
                    continue;
                };
                next.broker_addresses.insert(
                    name,
                    broker
                        .broker_addrs
                        .as_ref()
                        .map(|addresses| addresses.snapshot())
                        .unwrap_or_default(),
                );
            }
            next.topics
                .insert(topic.to_owned(), Arc::new(topic_snapshot));
            *current = Arc::new(next);
            return Ok(!unchanged);
        }
    }

    pub(crate) fn replace_raw_if_same(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
        value: TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        self.update_raw(|map| {
            let matched = map.get(key).is_some_and(|current| current.ptr_eq(expected));
            if matched {
                map.insert(key.to_owned(), value);
            }
            (matched, matched)
        })
    }

    pub(crate) fn remove_raw_if_same(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        self.update_raw(|map| {
            let matched = map.get(key).is_some_and(|current| current.ptr_eq(expected));
            if matched {
                map.remove(key);
            }
            (matched, matched)
        })
    }
}

fn convert_topic_route_data(
    topic: Option<&str>,
    route: Option<&TopicRouteData>,
) -> Result<TopicSnapshot, RuntimeError> {
    let route = route.ok_or(RuntimeError::InvalidRoute("null topic route data"))?;
    let queue_topic = topic.unwrap_or_default();
    let broker_datas = route.broker_datas_snapshot();
    let queue_datas = route.queue_datas_snapshot();
    let topic_queue_mapping_by_broker = route.topic_queue_mapping_by_broker_snapshot();
    // Java's brokerAddrTable is a ConcurrentHashMap: publishing a null
    // address map fails, it is not a valid empty Broker. Validate before
    // any conversion/publication to preserve the coherent native snapshot.
    if broker_datas.is_none() {
        return Err(RuntimeError::InvalidRoute("null broker data list"));
    }
    if broker_datas
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .any(Option::is_none)
    {
        return Err(RuntimeError::InvalidRoute("null broker data"));
    }
    if broker_datas
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .flatten()
        .any(|broker| broker.broker_name.is_none())
    {
        return Err(RuntimeError::InvalidRoute("null broker name"));
    }
    let ordered = route
        .order_topic_conf
        .as_deref()
        .is_some_and(|value| !value.is_empty());
    let static_route = route.order_topic_conf.is_none()
        && topic_queue_mapping_by_broker
            .as_ref()
            .is_some_and(|mappings| !mappings.is_empty());
    if queue_datas.is_none() && !ordered && !static_route {
        return Err(RuntimeError::InvalidRoute("null queue data list"));
    }
    if !static_route
        && queue_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .any(Option::is_none)
    {
        return Err(RuntimeError::InvalidRoute("null queue data"));
    }
    if !ordered
        && !static_route
        && queue_datas.as_deref().unwrap_or(&[]).len() > 1
        && queue_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .flatten()
            .any(|queue| queue.broker_name.is_none())
    {
        return Err(RuntimeError::InvalidRoute(
            "null queue broker name during sort",
        ));
    }
    if let Some(mappings) = &topic_queue_mapping_by_broker {
        if mappings.values().any(Option::is_none) {
            return Err(RuntimeError::InvalidRoute("null static mapping"));
        }
        for mapping in mappings
            .values()
            .flatten()
            .filter(|mapping| mapping.scope.is_some())
        {
            let ids = mapping
                .curr_id_map
                .as_ref()
                .ok_or(RuntimeError::InvalidRoute("null static mapping id map"))?;
            if mapping.bname.is_none()
                && ids.with(|ids| ids.keys().any(|id| *id >= 0 && *id < mapping.total_queues))
            {
                return Err(RuntimeError::InvalidRoute(
                    "null static mapping broker name",
                ));
            }
        }
    }
    if broker_datas
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .flatten()
        .any(|broker| broker.broker_addrs.is_none())
    {
        return Err(RuntimeError::InvalidRoute("null broker address map"));
    }

    let mut conversion_route = route.clone();
    if let Some(ordered) = route
        .order_topic_conf
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        let mut normalized = Vec::new();
        for entry in ordered
            .trim_end_matches(';')
            .split(';')
            .filter(|_| !ordered.trim_end_matches(';').is_empty())
        {
            let count = entry.split(':').nth(1).and_then(java_integer);
            let count = count.ok_or(RuntimeError::InvalidRoute("invalid ordered queue count"))?;
            normalized.push(format!(
                "{}:{count}",
                entry.split(':').next().unwrap_or_default()
            ));
        }
        conversion_route.order_topic_conf = Some(normalized.join(";"));
    }

    // Raw order presence controls branch selection, even when a separator-
    // only Java split normalizes to an empty conversion string.
    validate_conversion_size(queue_topic, route)?;
    let mut publish_queues = if route
        .order_topic_conf
        .as_deref()
        .is_some_and(|value| !value.is_empty() && value.trim_end_matches(';').is_empty())
    {
        Vec::new()
    } else {
        conversion_route.publish_queues(queue_topic)
    };
    for queue in &mut publish_queues {
        queue.set_topic(topic.map(str::to_owned));
    }
    let subscribe_queues: Vec<MessageQueue> = route
        .subscribe_queues(queue_topic)
        .into_iter()
        .map(|mut queue| {
            queue.set_topic(topic.map(str::to_owned));
            queue
        })
        .collect();
    let endpoints = route
        .static_queue_endpoints(queue_topic)
        .unwrap_or_default()
        .into_iter()
        .map(|(mut queue, endpoint)| {
            queue.set_topic(topic.map(str::to_owned));
            (queue, endpoint)
        })
        .collect();
    Ok(TopicSnapshot {
        publish_queues,
        subscribe_queues: subscribe_queues.into_iter().collect(),
        endpoints,
        order_topic: route
            .order_topic_conf
            .as_ref()
            .is_some_and(|value| !value.is_empty()),
        // Java publishes a TopicRouteData copy whose constructor
        // normalizes null collections to empty containers.
        route: Arc::new(route.java_copy_constructed()),
    })
}

fn broker_address_exists_in_raw_routes(
    routes: &[TopicRouteDataHandle],
    address: Option<&str>,
) -> Result<bool, RuntimeError> {
    for route in routes {
        let route = route.snapshot()?;
        let broker_snapshot = route.broker_datas_snapshot();
        let broker_datas = broker_snapshot
            .as_deref()
            .ok_or(RuntimeError::InvalidRoute(
                "null broker data list during cleanup",
            ))?;
        for broker in broker_datas {
            let broker = broker.as_ref().ok_or(RuntimeError::InvalidRoute(
                "null broker data during cleanup",
            ))?;
            if broker.broker_addrs.as_ref().is_some_and(|addresses| {
                addresses.with(|addresses| {
                    addresses
                        .values()
                        .any(|candidate| candidate.as_deref() == address)
                })
            }) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

// Bound expansion of compact routes before invoking collection converters.
const MAX_ROUTE_EXPANSION_BYTES: usize = 16 * 1024 * 1024;

fn validate_conversion_size(topic: &str, route: &TopicRouteData) -> Result<(), RuntimeError> {
    let broker_datas = route.broker_datas_snapshot();
    let queue_datas = route.queue_datas_snapshot();
    let topic_queue_mapping_by_broker = route.topic_queue_mapping_by_broker_snapshot();
    let mut estimated = 0usize;
    let mut add = |count: i32, broker: &str| -> Result<(), RuntimeError> {
        let size = std::mem::size_of::<MessageQueue>()
            .checked_add(topic.len())
            .and_then(|size| size.checked_add(broker.len()))
            .and_then(|size| size.checked_add(64))
            .and_then(|size| size.checked_mul(count.max(0) as usize));
        estimated = size
            .and_then(|size| estimated.checked_add(size))
            .filter(|size| *size <= MAX_ROUTE_EXPANSION_BYTES)
            .ok_or(RuntimeError::InvalidRoute(
                "route queue expansion exceeds bounded memory budget",
            ))?;
        Ok(())
    };
    let ordered = route
        .order_topic_conf
        .as_deref()
        .filter(|value| !value.is_empty());
    let mappings = topic_queue_mapping_by_broker
        .as_ref()
        .filter(|value| !value.is_empty());
    if let Some(ordered) = ordered {
        for item in ordered.split(';') {
            let mut parts = item.split(':');
            let broker = parts.next().unwrap_or_default();
            let count = parts.next().and_then(java_integer).unwrap_or(0);
            add(count, broker)?;
        }
    }
    // Static metadata suppresses ordinary subscription conversion; it also
    // suppresses ordinary publish conversion only for null orderTopicConf.
    if let Some(mappings) = mappings {
        let mut totals = BTreeMap::<String, i32>::new();
        for mapping in mappings.values().flatten() {
            if let Some(scope) = mapping.scope.as_deref().filter(|_| mapping.epoch >= -1) {
                let broker = format!(
                    "__syslo__{}",
                    if scope == "__global__" {
                        &scope[2..]
                    } else {
                        scope
                    }
                );
                let total = totals.entry(broker).or_default();
                *total = (*total).max(mapping.total_queues);
            }
        }
        for (broker, total) in totals {
            add(total, &broker)?; // subscribe queues
            add(total, &broker)?; // endpoints
            if route.order_topic_conf.is_none() {
                add(total, &broker)?;
            } // publish queues
        }
    }
    let mut read_counts = BTreeMap::<Option<&str>, i32>::new();
    for queue in queue_datas.as_deref().unwrap_or(&[]).iter().flatten() {
        if mappings.is_none() && queue.perm & 4 != 0 {
            let count = read_counts.entry(queue.broker_name.as_deref()).or_default();
            *count = (*count).max(queue.read_queue_nums);
        }
        if ordered.is_none()
            && !(route.order_topic_conf.is_none() && mappings.is_some())
            && queue.perm & 2 != 0
        {
            let master = broker_datas
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .flatten()
                .find(|broker| broker.broker_name == queue.broker_name)
                .and_then(|broker| broker.broker_addrs.as_ref())
                .is_some_and(|addresses| {
                    addresses.with(|addresses| addresses.contains_key(&Some(0)))
                });
            if master {
                add(
                    queue.write_queue_nums,
                    queue.broker_name.as_deref().unwrap_or_default(),
                )?;
            }
        }
    }
    for (broker, count) in read_counts {
        add(count, broker.unwrap_or_default())?;
    }
    Ok(())
}

// Integer.parseInt iterates Java char/UTF-16 units and Character.digit(10),
// accepting BMP decimal scripts but not supplementary mathematical digits.
// Accumulate negatively so i32::MIN is representable without widening/range loss.
fn java_integer(value: &str) -> Option<i32> {
    crate::java_number::parse_long(value)?.try_into().ok()
}

#[cfg(test)]
fn java_decimal_digit(unit: u16) -> Option<u16> {
    crate::java_number::decimal_digit(unit)
}

fn routes_equal(left: &TopicRouteData, right: &TopicRouteData) -> Result<bool, RuntimeError> {
    let changed = left
        .java_topic_route_data_changed(Some(right))
        .map_err(|error| {
            use rocketmq_classic_protocol::route::TopicRouteDataChangedError;
            RuntimeError::InvalidRoute(match error {
                TopicRouteDataChangedError::NullQueueData => "null queue data during sort",
                TopicRouteDataChangedError::NullQueueBrokerName => {
                    "null queue broker name during sort"
                }
                TopicRouteDataChangedError::NullBrokerData => "null broker data during sort",
                TopicRouteDataChangedError::NullBrokerName => "null broker name during sort",
            })
        })?;
    Ok(!changed)
}

// TopicRouteData.equals delegates BrokerData.equals, which compares only name
// and addresses. BrokerData::PartialEq carries that source behavior.
pub(super) fn route_values_equal(left: &TopicRouteData, right: &TopicRouteData) -> bool {
    left.order_topic_conf == right.order_topic_conf
        && left.queue_datas_snapshot() == right.queue_datas_snapshot()
        && left.filter_server_table_snapshot() == right.filter_server_table_snapshot()
        && left.topic_queue_mapping_by_broker_snapshot()
            == right.topic_queue_mapping_by_broker_snapshot()
        && left.broker_datas_snapshot() == right.broker_datas_snapshot()
}

/// Java MixAll.brokerVIPChannel's last-colon split and signed-int port offset.
pub fn broker_vip_channel(enabled: bool, address: &str) -> Result<String, RuntimeError> {
    if !enabled {
        return Ok(address.to_owned());
    }
    let (host, port) = address
        .rsplit_once(':')
        .ok_or(RuntimeError::InvalidBrokerAddress)?;
    let port = crate::java_number::parse_long(port)
        .and_then(|port| i32::try_from(port).ok())
        .ok_or(RuntimeError::InvalidBrokerPort)?;
    Ok(format!("{host}:{}", port.wrapping_sub(2)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_broker_name_queue_matches_invoked_java_failure() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        assert_eq!(
            fixture["runtimeNullBrokerQueue"]["outcome"],
            "NullPointerException"
        );
        assert!(matches!(
            RouteSnapshot::default().broker_name_nullable(None),
            Err(RuntimeError::InvalidRoute("null message queue"))
        ));
    }
    use rocketmq_classic_protocol::route::{
        BrokerData, QueueData, SharedBrokerDataHandle, SharedQueueDataHandle,
    };

    fn broker_handle(route: &TopicRouteData, index: usize) -> SharedBrokerDataHandle {
        route
            .broker_datas
            .as_ref()
            .expect("broker list")
            .with(|brokers| brokers[index].clone().expect("broker row"))
    }

    fn queue_handle(route: &TopicRouteData, index: usize) -> SharedQueueDataHandle {
        route
            .queue_datas
            .as_ref()
            .expect("queue list")
            .with(|queues| queues[index].clone().expect("queue row"))
    }

    #[test]
    fn nullable_route_collection_refresh_matches_invoked_unstarted_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["runtimeNullableCollections"].as_array().unwrap() {
            if case["registeredConsumer"] == true {
                continue; // The registered-consumer branch is exercised through ClientRuntime.
            }
            let cache = RouteCache::default();
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let result = cache.replace("topic", route);
            assert_eq!(result.is_ok(), case["outcome"] == "OK");
            let snapshot = cache.snapshot().unwrap();
            assert_eq!(snapshot.raw_routes.contains_key("topic"), case["cached"]);
            if let Some(cached) = snapshot.raw_routes.get("topic") {
                let cached = cached.snapshot().unwrap();
                assert_eq!(cached.queue_datas.is_none(), case["cachedQueueNull"]);
                assert_eq!(cached.broker_datas.is_none(), case["cachedBrokerNull"]);
                assert_eq!(
                    cached.filter_server_table.is_none(),
                    case["cachedFilterNull"]
                );
            }
        }
    }

    #[test]
    fn route_collection_state_product_matches_invoked_unstarted_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["runtimeRouteCollectionMatrix"].as_array().unwrap();
        assert_eq!(cases.len(), 256);
        for (index, case) in cases.iter().enumerate() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let cache = RouteCache::default();
            let changed = cache.replace("topic", route);
            assert!(
                changed.is_ok(),
                "case {index} failed for {}: {changed:?}",
                case["states"]
            );
            assert_eq!(changed.unwrap(), case["changed"], "case {index}");

            let snapshot = cache.snapshot().unwrap();
            assert_eq!(
                snapshot.raw_routes.contains_key("topic"),
                case["cached"],
                "case {index}"
            );
            let cached = snapshot.raw_routes["topic"].snapshot().unwrap();
            assert_eq!(
                cached
                    .queue_datas_snapshot()
                    .map_or(-1, |rows| rows.len() as i64),
                case["cachedQueueCount"],
                "case {index}"
            );
            assert_eq!(
                cached
                    .broker_datas_snapshot()
                    .map_or(-1, |rows| rows.len() as i64),
                case["cachedBrokerCount"],
                "case {index}"
            );
            assert_eq!(
                cached
                    .filter_server_table_snapshot()
                    .map_or(-1, |rows| rows.len() as i64),
                case["cachedFilterCount"],
                "case {index}"
            );
            assert_eq!(
                cached
                    .topic_queue_mapping_by_broker_snapshot()
                    .map_or(-1, |rows| rows.len() as i64),
                case["cachedMappingCount"],
                "case {index}"
            );
        }
    }

    #[test]
    fn route_nested_collection_state_product_matches_invoked_unstarted_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["runtimeRouteNestedCollectionMatrix"]
            .as_array()
            .unwrap();
        assert_eq!(cases.len(), 625);
        for (index, case) in cases.iter().enumerate() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let cache = RouteCache::default();
            let changed = cache.replace("topic", route);
            assert_eq!(
                changed.is_ok(),
                case["outcome"] == "OK",
                "case {index} failed for {}: {changed:?}",
                case["states"]
            );
            if let Ok(changed) = changed {
                assert_eq!(changed, case["changed"], "case {index}");
            }

            let snapshot = cache.snapshot().unwrap();
            assert_eq!(
                snapshot.raw_routes.contains_key("topic"),
                case["cached"],
                "case {index}"
            );
            if let Some(cached) = snapshot.raw_routes.get("topic") {
                let cached = cached.snapshot().unwrap();
                assert_eq!(
                    cached
                        .queue_datas_snapshot()
                        .map_or(-1, |rows| rows.len() as i64),
                    case["cachedQueueCount"],
                    "case {index}"
                );
                assert_eq!(
                    cached
                        .broker_datas_snapshot()
                        .map_or(-1, |rows| rows.len() as i64),
                    case["cachedBrokerCount"],
                    "case {index}"
                );
                assert_eq!(
                    cached
                        .filter_server_table_snapshot()
                        .map_or(-1, |rows| rows.len() as i64),
                    case["cachedFilterCount"],
                    "case {index}"
                );
                assert_eq!(
                    cached
                        .topic_queue_mapping_by_broker_snapshot()
                        .map_or(-1, |rows| rows.len() as i64),
                    case["cachedMappingCount"],
                    "case {index}"
                );
            }
        }
    }

    #[test]
    fn nullable_static_mapping_conversion_matches_invoked_java_failures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullStaticMappingConversion"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let result = RouteCache::default().replace("topic", route);
            assert_eq!(result.is_err(), case["outcome"] == "NullPointerException");
        }
    }

    #[test]
    fn nullable_route_collections_preserve_raw_values_and_copy_comparison() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableRouteCollections"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let copy = route.java_copy_constructed();
            assert_eq!(
                copy.queue_datas_snapshot().unwrap().is_empty(),
                case["copyQueueEmpty"]
            );
            assert_eq!(
                copy.broker_datas_snapshot().unwrap().is_empty(),
                case["copyBrokerEmpty"]
            );
            assert_eq!(
                copy.filter_server_table_snapshot().unwrap().is_empty(),
                case["copyFilterEmpty"]
            );
            assert!(routes_equal(&route, &copy).unwrap());
            if route != copy {
                assert!(!route_values_equal(&route, &copy));
            }
            let cache = RouteCache::default();
            if route.queue_datas.is_none() || route.broker_datas.is_none() {
                assert!(matches!(
                    cache.replace("topic", route),
                    Err(RuntimeError::InvalidRoute(_))
                ));
            } else {
                assert!(cache.replace("topic", route.clone()).unwrap());
                assert!(!cache.route_changed("topic", &copy).unwrap());
                assert_eq!(
                    cache.snapshot().unwrap().raw_routes["topic"]
                        .snapshot()
                        .unwrap(),
                    copy
                );
            }
        }
    }

    #[test]
    fn offline_cleanup_skips_null_raw_broker_maps_like_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["runtimeRawOfflineCleanup"].as_array().unwrap() {
            let cache = RouteCache::default();
            if case["priorBroker"].as_bool().unwrap() {
                cache
                    .replace(
                        "topic",
                        serde_json::from_value(case["initialInput"].clone()).unwrap(),
                    )
                    .unwrap();
            }
            assert_eq!(
                cache.snapshot().unwrap().publish_address("broker"),
                case["publishAddressBefore"].as_str()
            );
            let table = super::super::TopicRouteTable::new(cache.clone());
            table.clear().unwrap();
            let raw = TopicRouteDataHandle::new(
                serde_json::from_value(case["rawInput"].clone()).unwrap(),
            );
            table.put("raw", raw.clone()).unwrap();
            let before = cache.snapshot().unwrap();
            assert_eq!(case["outcome"], "OK");
            cache.clean_offline().unwrap();
            let after = cache.snapshot().unwrap();
            assert_eq!(
                after.publish_address("broker"),
                case["publishAddressAfter"].as_str()
            );
            assert_eq!(
                after.raw_routes["raw"].ptr_eq(&raw),
                case["rawObjectRetained"]
            );
            assert_eq!(
                after.raw_routes["raw"]
                    .snapshot()
                    .unwrap()
                    .broker_datas_snapshot()
                    .unwrap()[0]
                    .as_ref()
                    .unwrap()
                    .broker_addrs
                    .is_none(),
                case["rawNullMapRetained"]
            );
            assert_eq!(
                before.publish_address("broker"),
                case["publishAddressBefore"].as_str()
            );
        }
    }

    #[test]
    fn malformed_raw_route_cleanup_shapes_match_invoked_java_runtime() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["runtimeRawCleanupShapes"].as_array().unwrap();
        assert_eq!(cases.len(), 21);
        for required in [
            "null-route-collections",
            "null-broker-row",
            "null-address-map",
            "empty-address-map",
            "null-address-only",
            "null-and-live-addresses",
            "null-row-and-live-address",
            "partial-candidate-before-null-row",
            "missing-route-fields",
            "null-broker-list",
            "empty-broker-list",
            "missing-address-map",
            "null-broker-name-live-address",
            "ignored-null-collections-live-address",
            "same-address-under-renamed-broker",
            "address-retained-by-sibling-topic",
            "candidate-before-null-address-map",
            "null-address-map-before-candidate",
            "null-broker-id-live-address",
            "empty-address-row-removed-before-later-failure",
            "live-null-address-retained",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            if case["id"] == "partial-candidate-before-null-row" {
                assert_eq!(
                    case["cleanupAddressTraversal"],
                    serde_json::json!(["master:10911", "slave:10911"])
                );
                assert_eq!(case["masterAddressVisibleBeforeFailure"], true);
                assert_eq!(case["laterStaleAddressOutcome"], "NullPointerException");
            }
            if case["id"] == "same-address-under-renamed-broker" {
                assert_eq!(case["outcome"], "OK");
                assert_eq!(
                    case["brokerAddressTable"],
                    serde_json::json!({"broker": {"0": "master:10911"}})
                );
            }
            if case["id"] == "candidate-before-null-address-map" {
                assert_eq!(case["outcome"], "OK");
                assert_eq!(
                    case["brokerAddressTable"],
                    serde_json::json!({"broker": {"0": "master:10911"}})
                );
            }
            if case["id"] == "null-address-map-before-candidate" {
                assert_eq!(case["outcome"], "OK");
                assert_eq!(
                    case["brokerAddressTable"],
                    serde_json::json!({"broker": {"0": "master:10911"}})
                );
            }
            if case["id"] == "null-broker-id-live-address" {
                assert_eq!(case["outcome"], "OK");
                assert_eq!(
                    case["brokerAddressTable"],
                    serde_json::json!({"broker": {"0": "master:10911"}})
                );
            }
            if case["id"] == "address-retained-by-sibling-topic" {
                assert_eq!(case["outcome"], "OK");
                assert_eq!(
                    case["brokerAddressTable"],
                    serde_json::json!({"broker": {"0": "master:10911"}})
                );
                assert_eq!(case["brokerVersionBeforeCleanup"], 551);
                assert_eq!(case["brokerVersionAfterCleanup"], 551);
            }
            if case["id"] == "empty-address-row-removed-before-later-failure" {
                assert_eq!(
                    case["brokerAddressTraversal"],
                    serde_json::json!(["a", "b"])
                );
                assert_eq!(case["outcome"], "NullPointerException");
                assert_eq!(
                    case["brokerAddressTable"],
                    serde_json::json!({"b": {"0": "stale:10911"}})
                );
            }
            let cache = RouteCache::default();
            let initial: TopicRouteData =
                serde_json::from_value(case["initialInput"].clone()).unwrap();
            cache.replace("topic", initial).unwrap();
            if case["id"] == "address-retained-by-sibling-topic" {
                cache.record_version("broker", "master:10911", 551).unwrap();
            }
            let table = super::super::TopicRouteTable::new(cache.clone());
            table.clear().unwrap();
            let raw: TopicRouteData = serde_json::from_value(case["rawInput"].clone()).unwrap();
            let raw = TopicRouteDataHandle::new(raw);
            table.put("raw", raw.clone()).unwrap();
            if case["id"] == "address-retained-by-sibling-topic" {
                let sibling: TopicRouteData =
                    serde_json::from_value(case["siblingInput"].clone()).unwrap();
                table
                    .put("sibling", TopicRouteDataHandle::new(sibling))
                    .unwrap();
            }

            assert_eq!(case["fetchCount"], 1, "{}", case["id"]);
            let before_cleanup = cache.snapshot().unwrap();
            let cleanup = cache.clean_offline();
            match case["outcome"].as_str().unwrap() {
                "OK" => assert!(cleanup.is_ok(), "{}: {cleanup:?}", case["id"]),
                "NullPointerException" => {
                    assert!(
                        matches!(cleanup, Err(RuntimeError::InvalidRoute(_))),
                        "{}: expected the typed invalid-route mapping of Java's null dereference, got {cleanup:?}",
                        case["id"]
                    )
                }
                outcome => panic!("unexpected Java cleanup outcome {outcome}"),
            }
            let snapshot = cache.snapshot().unwrap();
            if case["id"] == "address-retained-by-sibling-topic" {
                assert_eq!(
                    snapshot
                        .broker_versions
                        .get(&("broker".to_owned(), "master:10911".to_owned())),
                    Some(&551)
                );
            }
            if case["outcome"] == "NullPointerException" {
                if case["id"] == "empty-address-row-removed-before-later-failure" {
                    assert!(!Arc::ptr_eq(&before_cleanup, &snapshot), "{}", case["id"]);
                    assert_eq!(before_cleanup.broker_addresses.len(), 2);
                } else {
                    assert!(Arc::ptr_eq(&before_cleanup, &snapshot), "{}", case["id"]);
                }
            }
            let broker_address_table = snapshot
                .broker_addresses
                .iter()
                .map(|(broker, addresses)| {
                    (
                        broker.clone(),
                        addresses
                            .iter()
                            .map(|(id, address)| {
                                (
                                    id.map_or_else(|| "null".to_owned(), |id| id.to_string()),
                                    address,
                                )
                            })
                            .collect::<BTreeMap<_, _>>(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                serde_json::to_value(broker_address_table).unwrap(),
                case["brokerAddressTable"],
                "{}",
                case["id"]
            );
            assert!(snapshot.raw_routes["raw"].ptr_eq(&raw), "{}", case["id"]);
        }
    }

    #[test]
    fn raw_map_mutations_keep_derived_publication_and_offline_cleanup_matches_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeTopicRouteTable"];
        let first: TopicRouteData = serde_json::from_value(case["initialInput"].clone()).unwrap();
        let second = TopicRouteDataHandle::new(
            serde_json::from_value(case["replacementInput"].clone()).unwrap(),
        );
        let cache = RouteCache::default();
        let table = super::super::TopicRouteTable::new(cache.clone());
        assert_eq!(
            cache.replace("topic", first.clone()).unwrap(),
            case["refreshChanged"]
        );
        let before = cache.snapshot().unwrap();
        assert!(
            table
                .get("topic")
                .unwrap()
                .unwrap()
                .ptr_eq(&before.raw_routes["topic"])
        );
        assert_eq!(
            before.publish_address("broker"),
            case["initialPublishAddress"].as_str()
        );
        assert!(
            table
                .put("topic", second)
                .unwrap()
                .unwrap()
                .ptr_eq(&before.raw_routes["topic"])
        );
        let direct = cache.snapshot().unwrap();
        assert_eq!(
            direct.broker_address_by_topic("topic").unwrap().as_deref(),
            case["topicAddressAfterDirectPut"].as_str()
        );
        assert_eq!(
            direct.publish_address("broker"),
            case["publishAddressAfterDirectPut"].as_str()
        );
        assert!(Arc::ptr_eq(
            &direct.topics["topic"],
            &before.topics["topic"]
        ));
        assert_eq!(
            before.broker_address_by_topic("topic").unwrap().as_deref(),
            Some("master:10911")
        );
        table.clear().unwrap();
        let cleared = cache.snapshot().unwrap();
        assert!(table.get("topic").unwrap().is_none());
        assert_eq!(
            cleared.broker_address_by_topic("topic").unwrap().as_deref(),
            case["topicAddressAfterClear"].as_str()
        );
        assert_eq!(
            cleared.publish_address("broker"),
            case["publishAddressAfterClear"].as_str()
        );
        cache.clean_offline().unwrap();
        assert_eq!(
            cache.snapshot().unwrap().publish_address("broker"),
            case["publishAddressAfterOfflineClean"].as_str()
        );
        // Raw absence must force publication even when converted data is equal.
        assert!(cache.route_changed("topic", &first).unwrap());
        assert!(cache.replace("topic", first).unwrap());
        assert!(table.contains_key("topic").unwrap());
        assert_eq!(before.publish_address("broker"), Some("master:10911"));
        let invalid = before.raw_routes["topic"].snapshot().unwrap();
        broker_handle(&invalid, 0).with_mut(|broker| broker.broker_addrs = None);
        let invalid = TopicRouteDataHandle::new(invalid);
        table.put("invalid", invalid.clone()).unwrap();
        assert_eq!(
            cache
                .replace("invalid", invalid.snapshot().unwrap())
                .unwrap(),
            case["unchangedInvalidRawRefreshChanged"]
        );
        assert_eq!(
            table.get("invalid").unwrap().unwrap().ptr_eq(&invalid),
            case["unchangedInvalidRawRefreshKeepsCachedObject"]
        );
    }

    #[test]
    fn metadata_only_broker_changes_match_invoked_java_route_equality_and_refresh() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeTopicRouteTable"];
        let initial: TopicRouteData = serde_json::from_value(case["initialInput"].clone()).unwrap();
        let metadata: TopicRouteData =
            serde_json::from_value(case["metadataOnlyInput"].clone()).unwrap();
        assert_eq!(initial, metadata); // TopicRouteData delegates to Java BrokerData.equals.
        assert_eq!(
            routes_equal(&initial, &metadata).unwrap(),
            case["brokerMetadataIgnoredByRouteEquals"]
        );
        let cache = RouteCache::default();
        assert!(cache.replace("topic", initial.clone()).unwrap());
        let before = cache.snapshot().unwrap();
        assert_eq!(
            cache.route_changed("topic", &metadata).unwrap(),
            case["metadataOnlyRefreshChanged"]
        );
        assert_eq!(
            cache.replace("topic", metadata).unwrap(),
            case["metadataOnlyRefreshChanged"]
        );
        let after = cache.snapshot().unwrap();
        assert_eq!(
            Arc::ptr_eq(&before, &after),
            case["metadataOnlyRefreshKeepsCachedObject"]
        );
        assert_eq!(after.topics["topic"].route.as_ref(), &initial);
    }

    #[test]
    fn route_change_sort_matches_invoked_java_broker_comparison_errors() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataChanged"].as_array().unwrap() {
            let current: TopicRouteData = serde_json::from_value(case["current"].clone()).unwrap();
            let cache = RouteCache::default();
            if !case["old"].is_null() {
                let old: TopicRouteData = serde_json::from_value(case["old"].clone()).unwrap();
                super::super::TopicRouteTable::new(cache.clone())
                    .put("topic", TopicRouteDataHandle::new(old))
                    .unwrap();
            }
            let result = cache.route_changed("topic", &current);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["changed"]);
            } else {
                assert!(matches!(result, Err(RuntimeError::InvalidRoute(_))));
            }
        }
    }

    #[test]
    fn ordered_integer_parser_matches_invoked_java_digits_and_signed_boundaries() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let zeros = fixture["integerDigitZeroCodeUnits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap() as u16)
            .collect::<Vec<_>>();
        for unit in 0..=u16::MAX {
            let expected = zeros
                .iter()
                .find_map(|zero| unit.checked_sub(*zero).filter(|digit| *digit < 10));
            assert_eq!(java_decimal_digit(unit), expected, "UTF-16 unit {unit:x}");
        }
        for case in fixture["integers"].as_array().unwrap() {
            let expected = case["value"].as_i64().map(|value| value as i32);
            assert_eq!(
                java_integer(case["input"].as_str().unwrap()),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn nullable_broker_address_values_match_invoked_java_refresh_and_lookups() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["runtimeNullBrokerValues"].as_array().unwrap() {
            assert_eq!(case["outcome"], "OK");
            assert_eq!(case["decodedMasterKeyPresent"], true);
            assert_eq!(case["decodedMasterNull"], true);
            assert_eq!(case["cachedMasterKeyPresent"], true);
            assert_eq!(case["cachedMasterNull"], true);
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("Java accepts null address values");
            let cache = RouteCache::default();
            assert_eq!(cache.replace("topic", route).unwrap(), case["changed"]);
            let snapshot = cache.snapshot().unwrap();
            let topic = &snapshot.topics["topic"];
            let raw = serde_json::to_value(&*topic.route).unwrap();
            assert!(raw["brokerDatas"][0]["brokerAddrs"].get("0").is_some());
            assert!(raw["brokerDatas"][0]["brokerAddrs"]["0"].is_null());
            assert_eq!(
                topic.publish_queues.len() as u64,
                case["publishQueueCount"].as_u64().unwrap()
            );
            assert_eq!(
                topic.subscribe_queues.len() as u64,
                case["subscribeQueueCount"].as_u64().unwrap()
            );
            assert_eq!(
                snapshot.publish_address("broker"),
                case["publishAddress"].as_str()
            );
            assert_eq!(
                snapshot
                    .admin_address("broker")
                    .unwrap()
                    .map(|peer| peer.address),
                case["adminAddress"].as_str().map(str::to_owned)
            );
            assert_eq!(
                snapshot
                    .subscribe_address("broker", 0, true)
                    .unwrap()
                    .map(|peer| peer.address),
                case["subscribeExactMasterAddress"]
                    .as_str()
                    .map(str::to_owned)
            );
            assert_eq!(case["topicSelectionOnlyAllowedChoices"], true);
            assert_eq!(snapshot.select_topic_broker("topic", |_| 0).unwrap(), None);
            if case["adminAddress"].is_string() {
                assert_eq!(
                    snapshot
                        .select_topic_broker("topic", |length| usize::from(length > 1))
                        .unwrap(),
                    case["adminAddress"].as_str().map(str::to_owned)
                );
            }
            for _ in 0..128 {
                let selected = snapshot.broker_address_by_topic("topic").unwrap();
                assert!(selected.is_none() || selected.as_deref() == case["adminAddress"].as_str());
            }
        }
    }

    #[test]
    fn null_exact_subscribe_address_tries_adjacent_then_first_entry() {
        let mut snapshot = RouteSnapshot::default();
        snapshot.broker_addresses.insert(
            "broker".into(),
            BTreeMap::from([
                (Some(0), Some("master:10911".into())),
                (Some(1), None),
                (Some(2), Some("slave:10911".into())),
            ]),
        );
        assert_eq!(
            snapshot
                .subscribe_address("broker", 1, true)
                .unwrap()
                .unwrap()
                .address,
            "slave:10911"
        );
        snapshot
            .broker_addresses
            .get_mut("broker")
            .unwrap()
            .remove(&Some(2));
        assert!(
            snapshot
                .subscribe_address("broker", 1, true)
                .unwrap()
                .is_none()
        );
        let fallback = snapshot
            .subscribe_address("broker", 1, false)
            .unwrap()
            .unwrap();
        assert_eq!(fallback.address, "master:10911");
        assert!(!fallback.slave);
        snapshot
            .broker_addresses
            .get_mut("broker")
            .unwrap()
            .insert(Some(0), None);
        assert!(
            snapshot
                .subscribe_address("broker", 1, false)
                .unwrap()
                .is_none()
        );
        assert!(snapshot.admin_address("broker").unwrap().is_none());
    }

    #[test]
    fn broker_address_resolution_matches_invoked_java_runtime_cases() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["runtimeBrokerAddressResolution"]
            .as_array()
            .unwrap();
        assert_eq!(cases.len(), 9);

        for case in cases {
            let input = &case["input"];
            let mut snapshot = RouteSnapshot::default();
            let mut addresses = BTreeMap::new();
            for row in input["addresses"].as_array().unwrap() {
                let id = row["id"].as_i64();
                let address = row["address"].as_str().map(str::to_owned);
                addresses.insert(id, address);
            }
            if input["tableEntry"].as_bool().unwrap() {
                snapshot.broker_addresses.insert("broker".into(), addresses);
            }
            let versions = input["versions"].as_object().unwrap();
            for (address, version) in versions {
                snapshot.broker_versions.insert(
                    ("broker".into(), address.clone()),
                    version.as_i64().unwrap().try_into().unwrap(),
                );
            }

            assert_eq!(
                snapshot.publish_address("broker"),
                case["publishAddress"].as_str(),
                "{} publish lookup",
                case["id"]
            );

            let candidate_rows = input["addresses"].as_array().unwrap();
            let expected_admin = &case["admin"];
            let actual_admin = snapshot.admin_address("broker");
            if expected_admin["outcome"] != "OK" {
                assert!(actual_admin.is_err(), "{} admin lookup", case["id"]);
                assert!(matches!(actual_admin, Err(RuntimeError::InvalidRoute(_))));
            } else if expected_admin["isNull"] == true {
                assert!(
                    actual_admin.unwrap().is_none(),
                    "{} admin lookup",
                    case["id"]
                );
            } else {
                let actual = actual_admin.unwrap().expect("Rust admin lookup result");
                let java_address = expected_admin["address"].as_str().unwrap();
                assert!(
                    candidate_rows
                        .iter()
                        .any(|row| row["address"].as_str() == Some(java_address)),
                    "{} Java admin result is one of the advertised addresses",
                    case["id"]
                );
                let (_, expected_id) = candidate_rows
                    .iter()
                    .filter_map(|row| Some((row["address"].as_str()?, row["id"].as_i64())))
                    .find(|(address, _)| *address == actual.address)
                    .expect("Rust admin result is an advertised address");
                assert_eq!(
                    actual.slave,
                    expected_id != Some(0),
                    "{} admin slave flag",
                    case["id"]
                );
                let expected_version = versions
                    .get(&actual.address)
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0) as i32;
                assert_eq!(
                    actual.version, expected_version,
                    "{} admin version",
                    case["id"]
                );
                let java_version = versions
                    .get(java_address)
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0) as i32;
                assert_eq!(
                    expected_admin["version"].as_i64(),
                    Some(i64::from(java_version)),
                    "{} Java admin version oracle",
                    case["id"]
                );
            }

            for query in case["subscribe"].as_array().unwrap() {
                let expected = &query["result"];
                let broker_id = query["brokerId"].as_i64().unwrap();
                let only_this_broker = query["onlyThisBroker"].as_bool().unwrap();
                let actual = snapshot.subscribe_address("broker", broker_id, only_this_broker);
                if expected["outcome"] != "OK" {
                    assert!(actual.is_err(), "{} / {}", case["id"], query["id"]);
                    assert!(matches!(actual, Err(RuntimeError::InvalidRoute(_))));
                } else if expected["isNull"] == true {
                    assert!(
                        actual.unwrap().is_none(),
                        "{} / {}",
                        case["id"],
                        query["id"]
                    );
                } else {
                    let actual = actual.unwrap().expect("Rust subscribe lookup result");
                    assert_eq!(
                        actual.address,
                        expected["address"].as_str().unwrap(),
                        "{} / {} address",
                        case["id"],
                        query["id"]
                    );
                    assert_eq!(
                        actual.slave,
                        expected["slave"].as_bool().unwrap(),
                        "{} / {} slave flag",
                        case["id"],
                        query["id"]
                    );
                    assert_eq!(
                        actual.version,
                        expected["version"].as_i64().unwrap() as i32,
                        "{} / {} version",
                        case["id"],
                        query["id"]
                    );
                }
            }
        }
    }

    #[test]
    fn offline_cleanup_preserves_live_null_values_but_removes_stale_ones() {
        let cache = RouteCache::default();
        let route = route(1);
        broker_handle(&route, 0).with_mut(|broker| {
            broker.broker_addrs = Some(BTreeMap::from([(Some(0), None)]).into());
        });
        cache.replace("topic", route).unwrap();
        cache.clean_offline().unwrap();
        assert_eq!(
            cache.snapshot().unwrap().broker_addresses["broker"][&Some(0)],
            None
        );
        cache.update_raw(|raw| (true, raw.clear())).unwrap();
        cache.clean_offline().unwrap();
        assert!(cache.snapshot().unwrap().broker_addresses.is_empty());
    }

    #[test]
    fn null_broker_address_map_fails_refresh_without_replacing_coherent_cache() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeNullBrokerMap"];
        assert_eq!(case["outcome"], "NullPointerException");
        assert_eq!(case["initialChanged"], true);
        assert_eq!(case["cachedRouteUnchanged"], true);
        let cache = RouteCache::default();
        cache
            .replace(
                "topic",
                serde_json::from_value(case["initialInput"].clone()).unwrap(),
            )
            .unwrap();
        let before = cache.snapshot().unwrap();
        let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
        assert!(matches!(
            cache.replace("topic", route.clone()),
            Err(RuntimeError::InvalidRoute(_))
        ));
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
        // A legitimate empty map remains distinct from null and is accepted.
        let empty = route;
        broker_handle(&empty, 0).with_mut(|broker| {
            broker.broker_addrs = Some(BTreeMap::new().into());
        });
        assert!(cache.replace("topic", empty).unwrap());
    }

    #[test]
    fn static_to_ordinary_refresh_retains_endpoints_like_invoked_java_runtime() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeEmptyEndpointTransition"];
        assert_eq!(case["initialChanged"], true);
        assert_eq!(case["changed"], true);
        assert_eq!(case["retained"], true);
        assert_eq!(case["rawRouteHasStaticMappings"], false);
        let unmapped_queue: MessageQueue =
            serde_json::from_value(case["unmappedQueue"].clone()).unwrap();
        let null_topic_queue: MessageQueue =
            serde_json::from_value(case["nullTopicQueue"].clone()).unwrap();
        assert!(case["unmappedBrokerBefore"].is_null());
        assert!(case["unmappedBrokerAfter"].is_null());
        assert_eq!(case["nullTopicBefore"]["outcome"], "NullPointerException");
        assert_eq!(case["nullTopicAfter"]["outcome"], "NullPointerException");
        let cache = RouteCache::default();
        cache
            .replace(
                "topic",
                serde_json::from_value(case["initialInput"].clone()).unwrap(),
            )
            .unwrap();
        let before = cache.snapshot().unwrap();
        assert!(!before.topics["topic"].endpoints.is_empty());
        let ordinary: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
        assert!(cache.replace("topic", ordinary.clone()).unwrap());
        let after = cache.snapshot().unwrap();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.topics["topic"].route.as_ref(), &ordinary);
        assert_eq!(
            after.topics["topic"].endpoints,
            before.topics["topic"].endpoints
        );
        assert_eq!(before.broker_name(&unmapped_queue).unwrap(), None);
        assert_eq!(after.broker_name(&unmapped_queue).unwrap(), None);
        assert!(matches!(
            before.broker_name(&null_topic_queue),
            Err(RuntimeError::InvalidRoute("null message queue topic"))
        ));
        assert!(matches!(
            after.broker_name(&null_topic_queue),
            Err(RuntimeError::InvalidRoute("null message queue topic"))
        ));
        assert_eq!(after.topics["topic"].publish_queues.len(), 2);
        assert_eq!(after.topics["topic"].subscribe_queues.len(), 3);
        for (snapshot, rows) in [(&before, &case["before"]), (&after, &case["after"])] {
            assert_eq!(
                snapshot.topics["topic"].endpoints.len(),
                rows.as_array().unwrap().len()
            );
            for row in rows.as_array().unwrap() {
                let queue: MessageQueue = serde_json::from_value(row["queue"].clone()).unwrap();
                assert_eq!(
                    snapshot.broker_name(&queue).unwrap(),
                    row["broker"].as_str()
                );
            }
        }
        assert!(
            before.topics["topic"]
                .route
                .topic_queue_mapping_by_broker
                .is_some()
        );
        assert!(
            after.topics["topic"]
                .route
                .topic_queue_mapping_by_broker
                .is_none()
        );
    }
    #[test]
    fn topic_selection_uses_raw_route_and_selected_empty_broker_is_not_skipped() {
        let cache = RouteCache::default();
        let original = route(1);
        broker_handle(&original, 0).with_mut(|broker| {
            broker.broker_addrs = Some(
                BTreeMap::from([
                    (Some(2), Some("slave-a:10911".into())),
                    (Some(3), Some("slave-b:10911".into())),
                ])
                .into(),
            );
        });
        let other_broker = route(2)
            .broker_datas
            .unwrap()
            .with_mut(|brokers| brokers.remove(0).unwrap());
        original
            .broker_datas
            .as_ref()
            .unwrap()
            .with_mut(|brokers| brokers.push(Some(other_broker)));
        cache.replace("original", original.clone()).unwrap();
        cache.replace("overwriter", route(9)).unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(snapshot.publish_address("broker"), Some("master-9:10911"));
        let mut choices = [0, 1].into_iter();
        assert_eq!(
            snapshot
                .select_topic_broker("original", |length| {
                    assert_eq!(length, 2);
                    choices.next().unwrap()
                })
                .unwrap(),
            Some("slave-b:10911".to_owned())
        );
        assert_eq!(
            snapshot.select_topic_broker("original", |_| 1).unwrap(),
            Some("master-2:10911".to_owned())
        );
        broker_handle(&original, 0)
            .with_mut(|broker| broker.broker_addrs = Some(BTreeMap::new().into()));
        cache.replace("original", original).unwrap();
        assert!(matches!(
            cache
                .snapshot()
                .unwrap()
                .select_topic_broker("original", |_| 0),
            Err(RuntimeError::InvalidRoute(_))
        ));
    }

    #[test]
    fn topic_broker_lookup_matches_invoked_java_candidates_and_failures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 14);
        for required in [
            "null-broker-id-single-address",
            "null-broker-id-with-master",
            "null-broker-id-with-slave",
        ] {
            let case = cases
                .iter()
                .find(|case| case["name"] == required)
                .unwrap_or_else(|| panic!("missing Java broker lookup case {required}"));
            assert_eq!(case["outcome"], "OK", "{required}");
            assert!(case["input"]["brokerDatas"][0]["brokerAddrs"]["null"].is_string());
        }
        for required in ["null-broker-list", "null-broker-row"] {
            let case = cases
                .iter()
                .find(|case| case["name"] == required)
                .unwrap_or_else(|| panic!("missing Java broker lookup case {required}"));
            assert_eq!(case["outcome"], "NullPointerException", "{required}");
        }
        let missing_list = cases
            .iter()
            .find(|case| case["name"] == "missing-broker-list")
            .expect("missing Java broker lookup case missing-broker-list");
        assert_eq!(missing_list["outcome"], "NULL");
        assert!(missing_list["input"].get("brokerDatas").is_none());
        for case in cases {
            let mut snapshot = RouteSnapshot::default();
            if !case["input"].is_null() {
                // The Java lookup oracle reflects directly into its raw route
                // table, including states normal refresh cannot publish. Match
                // that test setup without bypassing production cache validation.
                snapshot.raw_routes.insert(
                    "topic".into(),
                    TopicRouteDataHandle::new(
                        serde_json::from_value(case["input"].clone()).unwrap(),
                    ),
                );
            }
            for _ in 0..128 {
                match case["outcome"].as_str().unwrap() {
                    "NULL" => assert_eq!(snapshot.broker_address_by_topic("topic").unwrap(), None),
                    "OK" => {
                        let selected = snapshot.broker_address_by_topic("topic").unwrap().unwrap();
                        assert!(
                            case["candidates"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|value| value.as_str() == Some(selected.as_str()))
                        );
                    }
                    "IllegalArgumentException" | "NullPointerException" => {
                        assert!(matches!(
                            snapshot.broker_address_by_topic("topic"),
                            Err(RuntimeError::InvalidRoute(_))
                        ));
                    }
                    outcome => panic!("unexpected Java outcome {outcome}"),
                }
            }
        }
    }

    fn route(count: i32) -> TopicRouteData {
        TopicRouteData {
            queue_datas: Some(
                vec![Some(QueueData {
                    broker_name: Some("broker".into()),
                    read_queue_nums: count,
                    write_queue_nums: count,
                    perm: 6,
                    ..Default::default()
                })]
                .into(),
            ),
            broker_datas: Some(
                vec![Some(BrokerData {
                    broker_name: Some("broker".into()),
                    broker_addrs: Some(
                        BTreeMap::from([
                            (Some(0), Some(format!("master-{count}:10911"))),
                            (Some(2), Some("slave:10911".into())),
                        ])
                        .into(),
                    ),
                    ..Default::default()
                })]
                .into(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn admin_resolution_reports_selected_peer_version_and_subscription_preserves_adjacent_slave_flag()
     {
        let mut snapshot = RouteSnapshot::default();
        assert!(snapshot.admin_address("missing").unwrap().is_none());
        snapshot
            .broker_addresses
            .insert("empty".into(), BTreeMap::new());
        assert!(snapshot.admin_address("empty").unwrap().is_none());
        snapshot.broker_addresses.insert(
            "broker".into(),
            BTreeMap::from([(Some(2), Some("slave".into()))]),
        );
        assert_eq!(
            snapshot.admin_address("broker").unwrap(),
            Some(BrokerAddress {
                address: "slave".into(),
                slave: true,
                version: 0
            })
        );
        snapshot
            .broker_versions
            .insert(("broker".into(), "slave".into()), 551);
        assert_eq!(
            snapshot.admin_address("broker").unwrap().unwrap().version,
            551
        );
        snapshot
            .broker_addresses
            .get_mut("broker")
            .unwrap()
            .insert(Some(0), Some("master".into()));
        // Java's admin method chooses an available map entry, not a portable
        // master-first contract. Assert metadata for the actually selected peer.
        let admin = snapshot.admin_address("broker").unwrap().unwrap();
        assert_eq!(admin.slave, admin.address != "master");
        assert_eq!(admin.version, if admin.slave { 551 } else { 0 });
        let adjacent = snapshot
            .subscribe_address("broker", -1, true)
            .unwrap()
            .unwrap();
        assert_eq!(adjacent.address, "master");
        assert!(adjacent.slave); // Java keeps the originally requested nonzero-ID flag.
        assert!(
            !snapshot
                .subscribe_address("broker", 0, true)
                .unwrap()
                .unwrap()
                .slave
        );
        assert!(
            snapshot
                .subscribe_address("broker", 7, true)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn nullable_broker_ids_fail_only_on_java_long_unboxing_paths() {
        let mut snapshot = RouteSnapshot::default();
        snapshot.broker_addresses.insert(
            "null-id".into(),
            BTreeMap::from([(None, Some("null-id:10911".into()))]),
        );
        assert!(matches!(
            snapshot.admin_address("null-id"),
            Err(RuntimeError::InvalidRoute("null broker address id"))
        ));
        assert!(matches!(
            snapshot.subscribe_address("null-id", 7, false),
            Err(RuntimeError::InvalidRoute("null broker address id"))
        ));
        assert_eq!(
            snapshot.subscribe_address("null-id", 0, true).unwrap(),
            None,
            "a direct Long-key lookup does not unbox map entries"
        );

        snapshot.broker_addresses.insert(
            "null-id-null-address".into(),
            BTreeMap::from([(None, None)]),
        );
        assert_eq!(
            snapshot.admin_address("null-id-null-address").unwrap(),
            None,
            "admin lookup skips null address values before unboxing"
        );
        assert!(matches!(
            snapshot.subscribe_address("null-id-null-address", 7, false),
            Err(RuntimeError::InvalidRoute("null broker address id"))
        ));
    }

    #[test]
    fn heartbeat_versions_are_coherent_and_offline_cleanup_checks_all_topics() {
        let cache = RouteCache::default();
        cache.replace("first", route(1)).unwrap();
        cache
            .record_version("broker", "master-1:10911", 551)
            .unwrap();
        let old = cache.snapshot().unwrap();
        assert_eq!(
            old.subscribe_address("broker", 0, true)
                .unwrap()
                .unwrap()
                .version,
            551
        );
        cache.replace("second", route(2)).unwrap();
        cache.replace("second", TopicRouteData::default()).unwrap();
        cache.clean_offline().unwrap();
        let next = cache.snapshot().unwrap();
        assert!(next.publish_address("broker").is_none());
        assert!(next.subscribe_address("broker", 2, true).unwrap().is_some()); // first topic still references this slave
        assert_eq!(
            next.broker_versions
                .get(&("broker".into(), "master-1:10911".into())),
            Some(&551)
        );
        assert_eq!(old.publish_address("broker"), Some("master-1:10911"));
        cache.replace("first", TopicRouteData::default()).unwrap();
        cache.clean_offline().unwrap();
        assert!(cache.snapshot().unwrap().broker_addresses.is_empty());
    }

    #[test]
    fn unchanged_route_requested_by_owner_republishes_its_broker_table() {
        let cache = RouteCache::default();
        cache.replace("first", route(1)).unwrap();
        cache.replace("second", route(2)).unwrap();
        let before = cache.snapshot().unwrap();
        assert_eq!(before.publish_address("broker"), Some("master-2:10911"));
        assert!(!cache.replace("first", route(1)).unwrap());
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
        assert!(
            !cache
                .replace_for_owner_update("first", route(1), true)
                .unwrap()
        );
        let after = cache.snapshot().unwrap();
        assert_eq!(after.publish_address("broker"), Some("master-1:10911"));
        assert_eq!(before.publish_address("broker"), Some("master-2:10911"));
        assert!(after.topics.contains_key("second"));
    }

    #[test]
    fn coherent_snapshot_keeps_raw_broker_publish_and_subscribe_state_together() {
        let cache = Arc::new(RouteCache::default());
        cache.replace("topic", route(1)).unwrap();
        let old = cache.snapshot().unwrap();
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let snapshot = cache.snapshot().unwrap();
                        let topic = &snapshot.topics["topic"];
                        let count = topic.route.queue_datas_snapshot().unwrap()[0]
                            .as_ref()
                            .unwrap()
                            .read_queue_nums as usize;
                        assert_eq!(topic.publish_queues.len(), count);
                        assert_eq!(topic.subscribe_queues.len(), count);
                        assert_eq!(
                            snapshot.publish_address("broker"),
                            Some(format!("master-{count}:10911").as_str())
                        );
                    }
                })
            })
            .collect();
        for _ in 0..100 {
            cache.replace("topic", route(2)).unwrap();
            cache.replace("topic", route(1)).unwrap();
        }
        for reader in readers {
            reader.join().unwrap();
        }
        assert_eq!(old.topics["topic"].publish_queues.len(), 1);
        assert!(!cache.replace("topic", route(1)).unwrap());
        let mut malformed = route(3);
        malformed.order_topic_conf = Some("broker:invalid".into());
        assert!(cache.replace("topic", malformed).is_err());
        assert_eq!(
            cache.snapshot().unwrap().topics["topic"]
                .publish_queues
                .len(),
            1
        );
    }

    #[test]
    fn ordered_conversion_uses_java_first_count_and_ignores_trailing_fields() {
        let cache = RouteCache::default();
        let mut ordered = route(1);
        ordered.order_topic_conf = Some("broker:2:ignored;".into());
        cache.replace("topic", ordered).unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(snapshot.topics["topic"].publish_queues.len(), 2);
        assert!(snapshot.topics["topic"].order_topic);
        assert_eq!(
            snapshot.topics["topic"].route.order_topic_conf.as_deref(),
            Some("broker:2:ignored;")
        );
    }

    #[test]
    fn subscribe_resolves_exact_adjacent_slave_and_optional_fallback() {
        let cache = RouteCache::default();
        cache.replace("topic", route(1)).unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert!(
            !snapshot
                .subscribe_address("broker", 0, true)
                .unwrap()
                .unwrap()
                .slave
        );
        assert_eq!(
            snapshot
                .subscribe_address("broker", 1, true)
                .unwrap()
                .unwrap()
                .address,
            "slave:10911"
        );
        assert!(
            snapshot
                .subscribe_address("broker", 3, true)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            snapshot
                .subscribe_address("broker", 3, false)
                .unwrap()
                .unwrap()
                .address,
            "master-1:10911"
        );
        assert!(snapshot.publish_address("missing").is_none());
        assert_eq!(
            snapshot
                .broker_name(&MessageQueue::new("topic", "broker", 0))
                .unwrap(),
            Some("broker")
        );
    }

    #[test]
    fn hostile_queue_counts_are_rejected_before_expanding_collections() {
        let cache = RouteCache::default();
        cache.replace("topic", route(1)).unwrap();
        let before = cache.snapshot().unwrap();
        assert!(cache.replace("topic", route(i32::MAX)).is_err());
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
    }

    #[test]
    fn budget_counts_effective_static_space_and_separator_only_order_not_unused_metadata() {
        use rocketmq_classic_protocol::route::TopicQueueMappingInfo;
        let cache = RouteCache::default();
        let mut ordered = route(0);
        queue_handle(&ordered, 0).with_mut(|queue| queue.write_queue_nums = i32::MAX);
        ordered.order_topic_conf = Some(";;".into());
        cache.replace("topic", ordered).unwrap();
        assert!(
            cache.snapshot().unwrap().topics["topic"]
                .publish_queues
                .is_empty()
        );
        let mut repeated = route(i32::MAX);
        repeated.topic_queue_mapping_by_broker = Some(
            (0..1_000)
                .map(|index| {
                    (
                        index.to_string(),
                        Some(TopicQueueMappingInfo {
                            total_queues: 1_000,
                            epoch: 0,
                            scope: Some("__global__".into()),
                            curr_id_map: Some(BTreeMap::new().into()),
                            ..Default::default()
                        }),
                    )
                })
                .collect::<BTreeMap<_, _>>()
                .into(),
        );
        cache.replace("topic", repeated.clone()).unwrap();
        assert_eq!(
            cache.snapshot().unwrap().topics["topic"]
                .publish_queues
                .len(),
            1_000
        );
        assert_eq!(
            cache.snapshot().unwrap().topics["topic"].endpoints.len(),
            1_000
        );
        repeated
            .topic_queue_mapping_by_broker
            .as_ref()
            .unwrap()
            .with(|mappings| mappings["0"].as_ref().unwrap().clone())
            .with_mut(|mapping| mapping.total_queues = i32::MAX);
        let before = cache.snapshot().unwrap();
        assert!(cache.replace("topic", repeated).is_err());
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
    }

    #[test]
    fn vip_transformation_preserves_valid_ipv6_and_rejects_java_parse_failures() {
        for (input, expected) in [
            ("broker:10911", "broker:10909"),
            ("[2001:db8::1]:10911", "[2001:db8::1]:10909"),
            ("2001:db8::1:10911", "2001:db8::1:10909"),
        ] {
            assert_eq!(broker_vip_channel(true, input).unwrap(), expected);
            assert_eq!(broker_vip_channel(false, input).unwrap(), input);
        }
        for input in ["missing", "broker: 2", "broker:", "broker:999999999999"] {
            assert!(broker_vip_channel(true, input).is_err(), "{input}");
        }
    }

    #[test]
    fn vip_helper_matches_invoked_java_int_and_split_semantics() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["vipCases"].as_array().unwrap() {
            let address = case["address"].as_str().unwrap();
            let actual = broker_vip_channel(true, address);
            let java_accepts = case["enabledOutcome"] == "OK";
            assert_eq!(actual.is_ok(), java_accepts, "{address}: {actual:?}");
            match actual {
                Ok(actual) => {
                    assert_eq!(actual, case["enabledValue"].as_str().unwrap(), "{address}");
                }
                Err(actual) => match case["enabledOutcome"].as_str().unwrap() {
                    "StringIndexOutOfBoundsException" => {
                        assert!(matches!(actual, RuntimeError::InvalidBrokerAddress));
                    }
                    "NumberFormatException" => {
                        assert!(matches!(actual, RuntimeError::InvalidBrokerPort));
                    }
                    other => panic!("unmapped Java VIP exception {other} for {address}"),
                },
            }
            assert_eq!(
                broker_vip_channel(false, address).unwrap(),
                case["disabled"].as_str().unwrap(),
                "disabled {address}"
            );
        }
    }

    #[test]
    fn snapshots_and_vip_compare_actual_pinned_java_conversion_results() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 36);
        for (order, expected_outcome) in [
            (";broker:2", "ArrayIndexOutOfBoundsException"),
            ("broker:2;;broker:1", "ArrayIndexOutOfBoundsException"),
            ("broker:2;;;broker:1", "ArrayIndexOutOfBoundsException"),
            ("broker:2;;;", "OK"),
            (";", "OK"),
            (":1", "OK"),
            ("broker:2;:1", "OK"),
            ("broker:2;broker", "ArrayIndexOutOfBoundsException"),
            ("broker:2;broker:1", "OK"),
        ] {
            assert!(
                cases.iter().any(|case| {
                    case["input"]["orderTopicConf"] == order && case["outcome"] == expected_outcome
                }),
                "missing Java ordered-route case {order:?} => {expected_outcome}"
            );
        }
        let repeated_broker = cases
            .iter()
            .find(|case| case["input"]["orderTopicConf"] == "broker:2;broker:1")
            .unwrap();
        let repeated_queues = repeated_broker["publish"].as_array().unwrap();
        assert_eq!(repeated_queues.len(), 3);
        assert_eq!(repeated_queues[0], repeated_queues[2]);
        for case in cases {
            let cache = RouteCache::default();
            let route = serde_json::from_value(case["input"].clone()).unwrap();
            if case["outcome"] == "OK" {
                let converted = TopicSnapshot::from_route_data("topic", &route).unwrap();
                assert_eq!(
                    serde_json::to_value(&converted.publish_queues).unwrap(),
                    case["publish"]
                );
                assert_eq!(
                    serde_json::to_value(&converted.subscribe_queues).unwrap(),
                    case["subscribe"]
                );
                assert_eq!(converted.order_topic, case["ordered"].as_bool().unwrap());
                assert_eq!(
                    serde_json::to_value(
                        converted
                            .endpoints
                            .iter()
                            .map(
                                |(queue, broker)| serde_json::json!({"queue":queue,"broker":broker})
                            )
                            .collect::<Vec<_>>()
                    )
                    .unwrap(),
                    case["endpoints"]
                );
                cache.replace("topic", route).unwrap();
                let snapshot = cache.snapshot().unwrap();
                let topic = &snapshot.topics["topic"];
                assert_eq!(topic.publish_queues, converted.publish_queues);
                assert_eq!(topic.subscribe_queues, converted.subscribe_queues);
                assert_eq!(topic.endpoints, converted.endpoints);
                assert_eq!(topic.order_topic, converted.order_topic);
                assert_eq!(
                    serde_json::to_value(&topic.publish_queues).unwrap(),
                    case["publish"]
                );
                assert_eq!(
                    serde_json::to_value(&topic.subscribe_queues).unwrap(),
                    case["subscribe"]
                );
                assert_eq!(topic.order_topic, case["ordered"].as_bool().unwrap());
                assert_eq!(
                    serde_json::to_value(
                        topic
                            .endpoints
                            .iter()
                            .map(
                                |(queue, broker)| serde_json::json!({"queue":queue,"broker":broker})
                            )
                            .collect::<Vec<_>>()
                    )
                    .unwrap(),
                    case["endpoints"]
                );
            } else {
                assert!(TopicSnapshot::from_route_data("topic", &route).is_err());
                assert!(cache.replace("topic", route).is_err());
                assert!(cache.snapshot().unwrap().topics.is_empty());
            }
        }
        for (input, output) in fixture["vip"].as_object().unwrap() {
            assert_eq!(
                broker_vip_channel(true, input).unwrap(),
                output.as_str().unwrap()
            );
        }
    }
}

#[cfg(test)]
#[path = "cache_nullable_elements.rs"]
mod nullable_element_tests;
