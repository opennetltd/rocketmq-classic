use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicI32, Ordering},
    },
    thread::ThreadId,
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::{
    route::{TopicRouteDataHandle, TopicSnapshot},
    runtime::RuntimeError,
};
use rocketmq_classic_protocol::route::TopicRouteData;

use crate::ProducerError;

/// One Java-compatible queue-filter callback.
pub type PublishInfoQueueFilter<'a> = &'a dyn Fn(&MessageQueue) -> bool;

/// Nullable `QueueFilter...` input, including Java's nullable filter elements.
pub type NullablePublishInfoQueueFilters<'a> = Option<&'a [Option<PublishInfoQueueFilter<'a>>]>;

thread_local! {
    static ACTIVE_PUBLISH_INFO_REMAPS: RefCell<BTreeSet<(usize, String)>> = const {
        RefCell::new(BTreeSet::new())
    };
}

struct PublishInfoRemapGuard {
    table_id: usize,
    topic: String,
}

impl PublishInfoRemapGuard {
    fn enter(table_id: usize, topic: &str) -> Self {
        let key = (table_id, topic.to_owned());
        let inserted =
            ACTIVE_PUBLISH_INFO_REMAPS.with(|active| active.borrow_mut().insert(key.clone()));
        assert!(
            inserted,
            "recursive update to TopicPublishInfoTable key `{topic}`"
        );
        Self {
            table_id,
            topic: topic.to_owned(),
        }
    }

    fn assert_not_active(table_id: usize, topic: &str) {
        let active = ACTIVE_PUBLISH_INFO_REMAPS
            .with(|active| active.borrow().contains(&(table_id, topic.to_owned())));
        assert!(
            !active,
            "recursive update to TopicPublishInfoTable key `{topic}`"
        );
    }
}

impl Drop for PublishInfoRemapGuard {
    fn drop(&mut self) {
        ACTIVE_PUBLISH_INFO_REMAPS.with(|active| {
            active
                .borrow_mut()
                .remove(&(self.table_id, self.topic.clone()));
        });
    }
}

/// Shared mutable queue-list handle corresponding to
/// `TopicPublishInfo.getMessageQueueList()`.
///
/// ```compile_fail
/// use rocketmq_classic_producer::publish_info::ProducerPublishQueueList;
/// let queues = ProducerPublishQueueList::default();
/// queues.ptr_eq(&queues);
/// ```
#[derive(Clone, Default)]
pub struct ProducerPublishQueueList(Arc<RwLock<Vec<MessageQueue>>>);

impl ProducerPublishQueueList {
    pub fn new(queues: Vec<MessageQueue>) -> Self {
        Self(Arc::new(RwLock::new(queues)))
    }

    pub fn snapshot(&self) -> Vec<MessageQueue> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn len(&self) -> usize {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mutates the Java list object retained by every alias to this handle.
    pub fn with_mut<R>(&self, update: impl FnOnce(&mut Vec<MessageQueue>) -> R) -> R {
        update(
            &mut self
                .0
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Shared mutable counterpart to Java's `ThreadLocalIndex`.
///
/// ```compile_fail
/// use rocketmq_classic_producer::publish_info::ProducerQueueIndex;
/// let index = ProducerQueueIndex::default();
/// index.ptr_eq(&index);
/// ```
#[derive(Clone, Default)]
pub struct ProducerQueueIndex(Arc<Mutex<HashMap<ThreadId, AtomicI32>>>);

impl ProducerQueueIndex {
    pub fn increment_and_get(&self) -> i32 {
        let thread = std::thread::current().id();
        let mut indexes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = indexes
            .entry(thread)
            .or_insert_with(|| AtomicI32::new(random_i32()));
        index.fetch_add(1, Ordering::Relaxed).wrapping_add(1) & i32::MAX
    }

    pub fn reset(&self) {
        let thread = std::thread::current().id();
        let mut indexes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        indexes.insert(thread, AtomicI32::new(random_i32() & i32::MAX));
    }

    pub(crate) fn current(&self) -> Option<i32> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&std::thread::current().id())
            .map(|index| index.load(Ordering::Relaxed))
    }
}

impl std::fmt::Display for ProducerQueueIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.current() {
            Some(index) => write!(formatter, "ThreadLocalIndex{{threadLocalIndex={index}}}"),
            None => write!(formatter, "ThreadLocalIndex{{threadLocalIndex=null}}"),
        }
    }
}

fn random_i32() -> i32 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish() as i32
}

#[derive(Clone)]
struct PublishInfoData {
    order_topic: bool,
    have_topic_router_info: bool,
    message_queue_list: Option<ProducerPublishQueueList>,
    send_which_queue: ProducerQueueIndex,
    topic_route_data: Option<TopicRouteDataHandle>,
    template: TopicSnapshot,
}

/// Mutable producer route value corresponding to Java's `TopicPublishInfo`.
/// Cloned handles share the same mutable value, queue list, route data, and
/// per-thread queue cursor.
///
/// ```compile_fail
/// use rocketmq_classic_producer::publish_info::TopicPublishInfo;
/// let info = TopicPublishInfo::default();
/// info.ptr_eq(&info);
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_producer::publish_info::TopicPublishInfo;
/// let info = TopicPublishInfo::default();
/// info.set_message_queues(None);
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageQueue;
/// use rocketmq_classic_producer::publish_info::TopicPublishInfo;
/// let info = TopicPublishInfo::default();
/// let filters = [|_: &MessageQueue| true];
/// info.select_one_message_queue_with_filter(&filters).unwrap();
/// ```
#[derive(Clone)]
pub struct TopicPublishInfo(Arc<RwLock<PublishInfoData>>);

impl Default for TopicPublishInfo {
    fn default() -> Self {
        Self::from_snapshot(
            TopicSnapshot {
                route: Arc::new(TopicRouteData::default()),
                publish_queues: Vec::new(),
                subscribe_queues: Default::default(),
                endpoints: Default::default(),
                order_topic: false,
            },
            false,
        )
    }
}

impl TopicPublishInfo {
    pub(crate) fn from_snapshot(snapshot: TopicSnapshot, have_topic_router_info: bool) -> Self {
        let topic_route_data = Some(TopicRouteDataHandle::new((*snapshot.route).clone()));
        let message_queue_list = Some(ProducerPublishQueueList::new(
            snapshot.publish_queues.clone(),
        ));
        Self(Arc::new(RwLock::new(PublishInfoData {
            order_topic: snapshot.order_topic,
            have_topic_router_info,
            message_queue_list,
            send_which_queue: ProducerQueueIndex::default(),
            topic_route_data,
            template: snapshot,
        })))
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn is_order_topic(&self) -> bool {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .order_topic
    }

    pub fn set_order_topic(&self, value: bool) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .order_topic = value;
    }

    pub fn ok(&self) -> bool {
        self.message_queue_list()
            .is_some_and(|queues| !queues.is_empty())
    }

    pub fn message_queue_list(&self) -> Option<ProducerPublishQueueList> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .message_queue_list
            .clone()
    }

    pub fn set_message_queue_list(&self, queues: Option<ProducerPublishQueueList>) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .message_queue_list = queues;
    }

    pub fn send_which_queue(&self) -> ProducerQueueIndex {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .send_which_queue
            .clone()
    }

    pub fn set_send_which_queue(&self, index: ProducerQueueIndex) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .send_which_queue = index;
    }

    pub fn have_topic_router_info(&self) -> bool {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .have_topic_router_info
    }

    pub fn set_have_topic_router_info(&self, value: bool) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .have_topic_router_info = value;
    }

    pub fn topic_route_data(&self) -> Option<TopicRouteDataHandle> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .topic_route_data
            .clone()
    }

    pub fn set_topic_route_data(&self, route: Option<TopicRouteDataHandle>) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .topic_route_data = route;
    }

    pub fn reset_index(&self) {
        self.send_which_queue().reset();
    }

    pub(crate) fn next_index(&self, queue_count: usize) -> usize {
        if queue_count == 0 {
            return 0;
        }
        (self.send_which_queue().increment_and_get() & i32::MAX) as usize % queue_count
    }

    /// Advances the retained ThreadLocalIndex and selects from the current
    /// live queue list using Java's nonnegative mask and increment-first order.
    pub fn select_one_message_queue(&self) -> Result<MessageQueue, ProducerError> {
        let data = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queues = data
            .message_queue_list
            .as_ref()
            .ok_or(ProducerError::NullPublishInfoQueueList)?;
        let queue_count = queues.len();
        if queue_count == 0 {
            return Err(ProducerError::EmptyPublishInfoQueueList);
        }
        let position =
            (data.send_which_queue.increment_and_get() & i32::MAX) as usize % queue_count;
        Ok(queues.snapshot()[position].clone())
    }

    /// Maps Java's nullable `QueueFilter...` input and nullable filter entries.
    /// A null filter array acts like no filters; a null element fails only when
    /// a nonempty queue list causes Java to evaluate the filters.
    pub fn select_one_message_queue_with_nullable_filters(
        &self,
        filters: NullablePublishInfoQueueFilters<'_>,
    ) -> Result<Option<MessageQueue>, ProducerError> {
        let filters = filters.filter(|filters| !filters.is_empty());
        let (queues, send_which_queue) = {
            let data = self
                .0
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                data.message_queue_list.clone(),
                data.send_which_queue.clone(),
            )
        };
        let Some(queues) = queues else {
            return Ok(None);
        };
        let queues = queues.snapshot();
        if queues.is_empty() {
            return Ok(None);
        }
        for _ in 0..queues.len() {
            let index = (send_which_queue.increment_and_get() & i32::MAX) as usize % queues.len();
            let queue = &queues[index];
            let mut accepted = true;
            for filter in filters.into_iter().flatten() {
                let Some(filter) = filter else {
                    return Err(ProducerError::NullPublishInfoQueueFilter);
                };
                accepted &= filter(queue);
            }
            if accepted {
                return Ok(Some(queue.clone()));
            }
        }
        Ok(None)
    }

    pub fn select_one_message_queue_avoiding(
        &self,
        last_broker: Option<&str>,
    ) -> Result<MessageQueue, ProducerError> {
        let Some(last_broker) = last_broker else {
            return self.select_one_message_queue();
        };
        let attempts = self.message_queue_list().map_or(0, |queues| queues.len());
        for _ in 0..attempts {
            let queue = self.select_one_message_queue()?;
            if queue.broker_name() != Some(last_broker) {
                return Ok(queue);
            }
        }
        self.select_one_message_queue()
    }

    pub fn get_write_queue_nums_by_broker(&self, broker_name: &str) -> Result<i32, ProducerError> {
        let route = self
            .topic_route_data()
            .ok_or(ProducerError::NullPublishInfoRouteData)?
            .snapshot()?;
        let Some(queue_data) = route.queue_datas_snapshot() else {
            return Err(ProducerError::NullPublishInfoQueueData);
        };
        for queue in queue_data {
            let Some(queue) = queue else {
                return Err(ProducerError::NullPublishInfoQueueData);
            };
            let Some(name) = queue.broker_name.as_deref() else {
                return Err(ProducerError::NullPublishInfoBrokerName);
            };
            if name == broker_name {
                return Ok(queue.write_queue_nums);
            }
        }
        Ok(-1)
    }

    pub(crate) fn snapshot(&self) -> Result<TopicSnapshot, RuntimeError> {
        let data = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let route = data
            .topic_route_data
            .as_ref()
            .map(TopicRouteDataHandle::snapshot)
            .transpose()?
            .unwrap_or_else(|| (*data.template.route).clone());
        Ok(TopicSnapshot {
            route: Arc::new(route),
            publish_queues: data
                .message_queue_list
                .map_or_else(Vec::new, |queues| queues.snapshot()),
            subscribe_queues: data.template.subscribe_queues,
            endpoints: data.template.endpoints,
            order_topic: data.order_topic,
        })
    }
}

impl std::fmt::Display for TopicPublishInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let queues = data.message_queue_list.map_or_else(
            || "null".to_owned(),
            |queues| {
                let queues = queues.snapshot();
                format!(
                    "[{}]",
                    queues
                        .iter()
                        .map(java_queue_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            },
        );
        write!(
            formatter,
            "TopicPublishInfo [orderTopic={}, messageQueueList={}, sendWhichQueue=ThreadLocalIndex{{threadLocalIndex={}}}, haveTopicRouterInfo={}]",
            data.order_topic,
            queues,
            data.send_which_queue
                .current()
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
            data.have_topic_router_info
        )
    }
}

fn java_queue_string(queue: &MessageQueue) -> String {
    format!(
        "MessageQueue [topic={}, brokerName={}, queueId={}]",
        queue.topic().unwrap_or("null"),
        queue.broker_name().unwrap_or("null"),
        queue.queue_id()
    )
}

/// Shared live topic-to-publish-info table corresponding to
/// `DefaultMQProducerImpl.getTopicPublishInfoTable()`.
#[derive(Clone, Default)]
pub struct TopicPublishInfoTable {
    entries: Arc<RwLock<HashMap<String, TopicPublishInfo>>>,
    mutation_gates: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopicPublishInfoTableError {
    #[error("TopicPublishInfoTable key is null")]
    NullKey,
    #[error("TopicPublishInfoTable value is null")]
    NullValue,
    #[error("TopicPublishInfoTable putAll source is null")]
    NullMap,
    #[error("TopicPublishInfoTable view collection is null")]
    NullCollection,
    #[error("TopicPublishInfoTable callback is null")]
    NullCallback,
    #[error("TopicPublishInfoTable remapping callback returned null")]
    NullRemappingResult,
}

impl TopicPublishInfoTable {
    fn table_id(&self) -> usize {
        Arc::as_ptr(&self.entries) as usize
    }

    fn remap_scope(&self, topic: &str) -> PublishInfoRemapGuard {
        PublishInfoRemapGuard::enter(self.table_id(), topic)
    }

    fn key_gate(&self, key: &str) -> Arc<Mutex<()>> {
        let mut gates = self
            .mutation_gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(gate) = gates.get(key).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        gates.insert(key.to_owned(), Arc::downgrade(&gate));
        gate
    }

    fn with_key_gate<R>(
        &self,
        key: &str,
        update: impl FnOnce(&mut HashMap<String, TopicPublishInfo>) -> R,
    ) -> R {
        PublishInfoRemapGuard::assert_not_active(self.table_id(), key);
        let gate = self.key_gate(key);
        let _guard = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        update(
            &mut self
                .entries
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    pub fn get(&self, topic: &str) -> Option<TopicPublishInfo> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(topic)
            .cloned()
    }

    pub fn get_nullable(
        &self,
        topic: Option<&str>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.get(topic))
    }

    pub fn get_or_default(&self, topic: &str, default: TopicPublishInfo) -> TopicPublishInfo {
        self.get(topic).unwrap_or(default)
    }

    pub fn get_or_default_nullable(
        &self,
        topic: Option<&str>,
        default: Option<TopicPublishInfo>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.get(topic).or(default))
    }

    pub fn contains_key(&self, topic: &str) -> bool {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(topic)
    }

    pub fn contains_key_nullable(
        &self,
        topic: Option<&str>,
    ) -> Result<bool, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.contains_key(topic))
    }

    pub fn contains_value(&self, value: &TopicPublishInfo) -> bool {
        self.snapshot()
            .values()
            .any(|current| current.ptr_eq(value))
    }

    pub fn contains_value_nullable(
        &self,
        value: Option<&TopicPublishInfo>,
    ) -> Result<bool, TopicPublishInfoTableError> {
        let Some(value) = value else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        Ok(self.contains_value(value))
    }

    pub fn len(&self) -> usize {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn put(
        &self,
        topic: impl Into<String>,
        info: TopicPublishInfo,
    ) -> Option<TopicPublishInfo> {
        let topic = topic.into();
        self.with_key_gate(&topic, |entries| entries.insert(topic.clone(), info))
    }

    pub fn put_nullable(
        &self,
        topic: Option<&str>,
        info: Option<TopicPublishInfo>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        let Some(info) = info else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        Ok(self.put(topic, info))
    }

    pub fn put_if_absent(
        &self,
        topic: impl Into<String>,
        info: TopicPublishInfo,
    ) -> Option<TopicPublishInfo> {
        let topic = topic.into();
        self.with_key_gate(&topic, |entries| {
            use std::collections::hash_map::Entry;
            match entries.entry(topic.clone()) {
                Entry::Occupied(entry) => Some(entry.get().clone()),
                Entry::Vacant(entry) => {
                    entry.insert(info);
                    None
                }
            }
        })
    }

    pub fn put_if_absent_nullable(
        &self,
        topic: Option<&str>,
        info: Option<TopicPublishInfo>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        let Some(info) = info else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        Ok(self.put_if_absent(topic, info))
    }

    pub fn remove(&self, topic: &str) -> Option<TopicPublishInfo> {
        self.with_key_gate(topic, |entries| entries.remove(topic))
    }

    pub fn remove_nullable(
        &self,
        topic: Option<&str>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.remove(topic))
    }

    pub fn remove_if_equal(&self, topic: &str, expected: &TopicPublishInfo) -> bool {
        self.with_key_gate(topic, |entries| {
            if entries
                .get(topic)
                .is_some_and(|current| current.ptr_eq(expected))
            {
                entries.remove(topic);
                true
            } else {
                false
            }
        })
    }

    pub fn remove_if_equal_nullable(
        &self,
        topic: Option<&str>,
        expected: Option<&TopicPublishInfo>,
    ) -> Result<bool, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        let Some(expected) = expected else {
            // ConcurrentHashMap.remove(key, null) returns false after checking
            // the key; null values can never match a stored mapping.
            return Ok(false);
        };
        Ok(self.remove_if_equal(topic, expected))
    }

    pub fn replace(&self, topic: &str, info: TopicPublishInfo) -> Option<TopicPublishInfo> {
        self.with_key_gate(topic, |entries| {
            entries
                .get_mut(topic)
                .map(|current| std::mem::replace(current, info))
        })
    }

    pub fn replace_nullable(
        &self,
        topic: Option<&str>,
        info: Option<TopicPublishInfo>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        let Some(info) = info else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        Ok(self.replace(topic, info))
    }

    pub fn replace_if_equal(
        &self,
        topic: &str,
        expected: &TopicPublishInfo,
        replacement: TopicPublishInfo,
    ) -> bool {
        self.with_key_gate(topic, |entries| {
            let Some(current) = entries.get_mut(topic) else {
                return false;
            };
            if !current.ptr_eq(expected) {
                return false;
            }
            *current = replacement;
            true
        })
    }

    pub fn replace_if_equal_nullable(
        &self,
        topic: Option<&str>,
        expected: Option<&TopicPublishInfo>,
        replacement: Option<TopicPublishInfo>,
    ) -> Result<bool, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        let Some(expected) = expected else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        let Some(replacement) = replacement else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        Ok(self.replace_if_equal(topic, expected, replacement))
    }

    pub fn put_all<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (String, TopicPublishInfo)>,
    {
        for (topic, info) in entries {
            self.put(topic, info);
        }
    }

    /// Applies nullable source rows in iteration order. Earlier writes remain
    /// visible if a later Java-incompatible null key/value is encountered.
    pub fn put_all_nullable_rows(
        &self,
        rows: Option<&[(Option<String>, Option<TopicPublishInfo>)]>,
    ) -> Result<(), TopicPublishInfoTableError> {
        let Some(rows) = rows else {
            return Err(TopicPublishInfoTableError::NullMap);
        };
        for (topic, info) in rows {
            self.put_nullable(topic.as_deref(), info.clone())?;
        }
        Ok(())
    }

    pub fn compute_if_absent(
        &self,
        topic: &str,
        mapping: impl FnOnce(&str) -> Option<TopicPublishInfo>,
    ) -> Option<TopicPublishInfo> {
        let _remap = self.remap_scope(topic);
        let gate = self.key_gate(topic);
        let _guard = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(current) = self.get(topic) {
            return Some(current);
        }
        let value = mapping(topic)?;
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic.to_owned(), value.clone());
        Some(value)
    }

    pub fn compute_if_absent_nullable(
        &self,
        topic: Option<&str>,
        mapping: impl FnOnce(&str) -> Option<TopicPublishInfo>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.compute_if_absent(topic, mapping))
    }

    pub fn compute_if_absent_with_nullable_callback<F>(
        &self,
        topic: Option<&str>,
        mapping: Option<&mut F>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError>
    where
        F: FnMut(&str) -> Option<TopicPublishInfo>,
    {
        let Some(mapping) = mapping else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        self.compute_if_absent_nullable(topic, |key| mapping(key))
    }

    pub fn compute_if_present(
        &self,
        topic: &str,
        remapping: impl FnOnce(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>,
    ) -> Option<TopicPublishInfo> {
        let _remap = self.remap_scope(topic);
        let gate = self.key_gate(topic);
        let _guard = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.get(topic)?;
        let replacement = remapping(topic, &current);
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match replacement {
            Some(value) => {
                entries.insert(topic.to_owned(), value.clone());
                Some(value)
            }
            None => {
                entries.remove(topic);
                None
            }
        }
    }

    pub fn compute_if_present_nullable(
        &self,
        topic: Option<&str>,
        remapping: impl FnOnce(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.compute_if_present(topic, remapping))
    }

    pub fn compute_if_present_with_nullable_callback<F>(
        &self,
        topic: Option<&str>,
        remapping: Option<&mut F>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError>
    where
        F: FnMut(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>,
    {
        let Some(remapping) = remapping else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        self.compute_if_present_nullable(topic, |key, current| remapping(key, current))
    }

    pub fn compute(
        &self,
        topic: &str,
        remapping: impl FnOnce(&str, Option<&TopicPublishInfo>) -> Option<TopicPublishInfo>,
    ) -> Option<TopicPublishInfo> {
        let _remap = self.remap_scope(topic);
        let gate = self.key_gate(topic);
        let _guard = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.get(topic);
        let replacement = remapping(topic, current.as_ref());
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match replacement {
            Some(value) => {
                entries.insert(topic.to_owned(), value.clone());
                Some(value)
            }
            None => {
                entries.remove(topic);
                None
            }
        }
    }

    pub fn compute_nullable(
        &self,
        topic: Option<&str>,
        remapping: impl FnOnce(&str, Option<&TopicPublishInfo>) -> Option<TopicPublishInfo>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.compute(topic, remapping))
    }

    pub fn compute_with_nullable_callback<F>(
        &self,
        topic: Option<&str>,
        remapping: Option<&mut F>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError>
    where
        F: FnMut(&str, Option<&TopicPublishInfo>) -> Option<TopicPublishInfo>,
    {
        let Some(remapping) = remapping else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        self.compute_nullable(topic, |key, current| remapping(key, current))
    }

    pub fn merge(
        &self,
        topic: &str,
        value: TopicPublishInfo,
        remapping: impl FnOnce(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>,
    ) -> Option<TopicPublishInfo> {
        let _remap = self.remap_scope(topic);
        let gate = self.key_gate(topic);
        let _guard = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.get(topic);
        let replacement = current
            .as_ref()
            .map_or_else(|| Some(value.clone()), |current| remapping(current, &value));
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match replacement {
            Some(info) => {
                entries.insert(topic.to_owned(), info.clone());
                Some(info)
            }
            None => {
                entries.remove(topic);
                None
            }
        }
    }

    pub fn merge_nullable<F>(
        &self,
        topic: Option<&str>,
        value: Option<TopicPublishInfo>,
        remapping: Option<&mut F>,
    ) -> Result<Option<TopicPublishInfo>, TopicPublishInfoTableError>
    where
        F: FnMut(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>,
    {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        let Some(value) = value else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        let Some(remapping) = remapping else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        Ok(self.merge(topic, value, |current, incoming| {
            remapping(current, incoming)
        }))
    }

    pub fn replace_all(
        &self,
        mut remapping: impl FnMut(&str, &TopicPublishInfo) -> TopicPublishInfo,
    ) {
        for (topic, _) in self.snapshot() {
            self.compute_if_present(&topic, |key, current| Some(remapping(key, current)));
        }
    }

    pub fn replace_all_nullable(
        &self,
        mut remapping: impl FnMut(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>,
    ) -> Result<(), TopicPublishInfoTableError> {
        for (topic, _) in self.snapshot() {
            let _remap = self.remap_scope(&topic);
            let gate = self.key_gate(&topic);
            let _guard = gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(current) = self.get(&topic) else {
                continue;
            };
            let Some(value) = remapping(&topic, &current) else {
                return Err(TopicPublishInfoTableError::NullRemappingResult);
            };
            self.entries
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(topic, value);
        }
        Ok(())
    }

    pub fn replace_all_with_nullable_callback<F>(
        &self,
        remapping: Option<&mut F>,
    ) -> Result<(), TopicPublishInfoTableError>
    where
        F: FnMut(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>,
    {
        let Some(remapping) = remapping else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        self.replace_all_nullable(|key, current| remapping(key, current))
    }

    pub fn for_each(&self, mut action: impl FnMut(&str, &TopicPublishInfo)) {
        for (topic, value) in self.snapshot() {
            action(&topic, &value);
        }
    }

    pub fn for_each_nullable<F>(
        &self,
        action: Option<&mut F>,
    ) -> Result<(), TopicPublishInfoTableError>
    where
        F: FnMut(&str, &TopicPublishInfo),
    {
        let Some(action) = action else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        self.for_each(|topic, value| action(topic, value));
        Ok(())
    }

    pub fn clear(&self) {
        for topic in self.keys() {
            self.remove(&topic);
        }
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<String, TopicPublishInfo> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(topic, info)| (topic.clone(), info.clone()))
            .collect()
    }

    pub(crate) fn keys(&self) -> BTreeSet<String> {
        self.snapshot().into_keys().collect()
    }

    pub fn key_set_view(&self) -> TopicPublishInfoKeySetView {
        TopicPublishInfoKeySetView(self.clone())
    }

    pub fn values_view(&self) -> TopicPublishInfoValuesView {
        TopicPublishInfoValuesView(self.clone())
    }

    pub fn entry_set_view(&self) -> TopicPublishInfoEntrySetView {
        TopicPublishInfoEntrySetView(self.clone())
    }
}

/// Live `Map.keySet()` counterpart. Snapshot iteration is weakly consistent;
/// key removals mutate the shared backing table.
#[derive(Clone)]
pub struct TopicPublishInfoKeySetView(TopicPublishInfoTable);

impl TopicPublishInfoKeySetView {
    pub fn contains(&self, topic: &str) -> bool {
        self.0.contains_key(topic)
    }

    pub fn contains_nullable(
        &self,
        topic: Option<&str>,
    ) -> Result<bool, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.contains(topic))
    }

    pub fn snapshot(&self) -> BTreeSet<String> {
        self.0.keys()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn remove(&self, topic: &str) -> bool {
        self.0.remove(topic).is_some()
    }

    pub fn remove_nullable(&self, topic: Option<&str>) -> Result<bool, TopicPublishInfoTableError> {
        let Some(topic) = topic else {
            return Err(TopicPublishInfoTableError::NullKey);
        };
        Ok(self.remove(topic))
    }

    pub fn clear(&self) {
        self.0.clear();
    }

    pub fn remove_if(&self, mut predicate: impl FnMut(&str) -> bool) -> usize {
        self.snapshot()
            .into_iter()
            .filter(|topic| predicate(topic) && self.remove(topic))
            .count()
    }

    pub fn remove_if_with_nullable_callback<F>(
        &self,
        predicate: Option<&mut F>,
    ) -> Result<usize, TopicPublishInfoTableError>
    where
        F: FnMut(&str) -> bool,
    {
        let Some(predicate) = predicate else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        Ok(self.remove_if(|topic| predicate(topic)))
    }

    pub fn remove_all(&self, topics: &BTreeSet<String>) -> usize {
        topics.iter().filter(|topic| self.remove(topic)).count()
    }

    pub fn remove_all_nullable(
        &self,
        topics: Option<&BTreeSet<String>>,
    ) -> Result<usize, TopicPublishInfoTableError> {
        let Some(topics) = topics else {
            return Err(TopicPublishInfoTableError::NullCollection);
        };
        Ok(self.remove_all(topics))
    }

    pub fn retain_all(&self, topics: &BTreeSet<String>) -> usize {
        self.snapshot()
            .into_iter()
            .filter(|topic| !topics.contains(topic) && self.remove(topic))
            .count()
    }

    pub fn retain_all_nullable(
        &self,
        topics: Option<&BTreeSet<String>>,
    ) -> Result<usize, TopicPublishInfoTableError> {
        let Some(topics) = topics else {
            return Err(TopicPublishInfoTableError::NullCollection);
        };
        Ok(self.retain_all(topics))
    }
}

/// Live `Map.values()` counterpart. Values are shared handles; removing a
/// value removes one mapping with the same Java object identity.
#[derive(Clone)]
pub struct TopicPublishInfoValuesView(TopicPublishInfoTable);

impl TopicPublishInfoValuesView {
    pub fn snapshot(&self) -> Vec<TopicPublishInfo> {
        self.0.snapshot().into_values().collect()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains(&self, value: &TopicPublishInfo) -> bool {
        self.0.contains_value(value)
    }

    pub fn contains_nullable(
        &self,
        value: Option<&TopicPublishInfo>,
    ) -> Result<bool, TopicPublishInfoTableError> {
        let Some(value) = value else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        Ok(self.contains(value))
    }

    pub fn remove(&self, value: &TopicPublishInfo) -> bool {
        self.0
            .snapshot()
            .into_iter()
            .find_map(|(topic, current)| {
                current
                    .ptr_eq(value)
                    .then(|| self.0.remove_if_equal(&topic, value))
            })
            .unwrap_or(false)
    }

    pub fn remove_nullable(
        &self,
        value: Option<&TopicPublishInfo>,
    ) -> Result<bool, TopicPublishInfoTableError> {
        // ConcurrentHashMap's values view returns false for remove(null).
        Ok(value.is_some_and(|value| self.remove(value)))
    }

    pub fn clear(&self) {
        self.0.clear();
    }

    pub fn remove_if(&self, mut predicate: impl FnMut(&TopicPublishInfo) -> bool) -> usize {
        self.0
            .snapshot()
            .into_iter()
            .filter(|(topic, current)| predicate(current) && self.0.remove_if_equal(topic, current))
            .count()
    }

    pub fn remove_if_with_nullable_callback<F>(
        &self,
        predicate: Option<&mut F>,
    ) -> Result<usize, TopicPublishInfoTableError>
    where
        F: FnMut(&TopicPublishInfo) -> bool,
    {
        let Some(predicate) = predicate else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        Ok(self.remove_if(|value| predicate(value)))
    }

    pub fn remove_all(&self, values: &[TopicPublishInfo]) -> usize {
        self.0
            .snapshot()
            .into_iter()
            .filter(|(topic, current)| {
                values.iter().any(|value| value.ptr_eq(current))
                    && self.0.remove_if_equal(topic, current)
            })
            .count()
    }

    pub fn remove_all_nullable(
        &self,
        values: Option<&[TopicPublishInfo]>,
    ) -> Result<usize, TopicPublishInfoTableError> {
        let Some(values) = values else {
            return Err(TopicPublishInfoTableError::NullCollection);
        };
        Ok(self.remove_all(values))
    }

    pub fn remove_all_iter<I>(&self, values: I) -> usize
    where
        I: IntoIterator<Item = TopicPublishInfo>,
    {
        let values = values.into_iter().collect::<Vec<_>>();
        self.remove_all(&values)
    }

    pub fn retain_all(&self, values: &[TopicPublishInfo]) -> usize {
        let retained = self
            .0
            .snapshot()
            .into_iter()
            .filter(|(_, current)| values.iter().any(|value| value.ptr_eq(current)))
            .collect::<BTreeMap<_, _>>();
        let mut removed = 0;
        for (topic, current) in self.0.snapshot() {
            if !retained
                .get(&topic)
                .is_some_and(|retained| retained.ptr_eq(&current))
                && self.0.remove_if_equal(&topic, &current)
            {
                removed += 1;
            }
        }
        removed
    }

    pub fn retain_all_nullable(
        &self,
        values: Option<&[TopicPublishInfo]>,
    ) -> Result<usize, TopicPublishInfoTableError> {
        let Some(values) = values else {
            return Err(TopicPublishInfoTableError::NullCollection);
        };
        Ok(self.retain_all(values))
    }
}

/// Detached entry carrying its captured value. `set_value` updates the entry
/// capture and unconditionally writes the new value to the shared table.
#[derive(Clone)]
pub struct TopicPublishInfoEntryHandle {
    table: TopicPublishInfoTable,
    topic: String,
    value: Arc<RwLock<TopicPublishInfo>>,
}

impl TopicPublishInfoEntryHandle {
    pub fn key(&self) -> &str {
        &self.topic
    }

    pub fn value(&self) -> TopicPublishInfo {
        self.value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_value(&self, value: TopicPublishInfo) -> TopicPublishInfo {
        let previous = {
            let mut captured = self
                .value
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut *captured, value.clone())
        };
        self.table.put(self.topic.clone(), value);
        previous
    }

    pub fn set_value_nullable(
        &self,
        value: Option<TopicPublishInfo>,
    ) -> Result<TopicPublishInfo, TopicPublishInfoTableError> {
        let Some(value) = value else {
            return Err(TopicPublishInfoTableError::NullValue);
        };
        Ok(self.set_value(value))
    }
}

/// Live `Map.entrySet()` counterpart. Traversal produces detached entries;
/// each entry's `set_value` still writes through to the backing table.
#[derive(Clone)]
pub struct TopicPublishInfoEntrySetView(TopicPublishInfoTable);

impl TopicPublishInfoEntrySetView {
    pub fn snapshot(&self) -> Vec<TopicPublishInfoEntryHandle> {
        self.0
            .snapshot()
            .into_iter()
            .map(|(topic, value)| TopicPublishInfoEntryHandle {
                table: self.0.clone(),
                topic,
                value: Arc::new(RwLock::new(value)),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains_nullable(&self, entry: Option<&(String, TopicPublishInfo)>) -> bool {
        entry.is_some_and(|(topic, value)| self.contains(topic, value))
    }

    pub fn contains(&self, topic: &str, value: &TopicPublishInfo) -> bool {
        self.0
            .get(topic)
            .is_some_and(|current| current.ptr_eq(value))
    }

    pub fn contains_entry_nullable(
        &self,
        topic: Option<&str>,
        value: Option<&TopicPublishInfo>,
    ) -> bool {
        topic
            .zip(value)
            .is_some_and(|(topic, value)| self.contains(topic, value))
    }

    pub fn remove(&self, topic: &str, value: &TopicPublishInfo) -> bool {
        self.0.remove_if_equal(topic, value)
    }

    pub fn remove_nullable(&self, entry: Option<&(String, TopicPublishInfo)>) -> bool {
        entry.is_some_and(|(topic, value)| self.remove(topic, value))
    }

    pub fn remove_entry_nullable(
        &self,
        topic: Option<&str>,
        value: Option<&TopicPublishInfo>,
    ) -> bool {
        topic
            .zip(value)
            .is_some_and(|(topic, value)| self.remove(topic, value))
    }

    pub fn remove_all(&self, entries: &[(String, TopicPublishInfo)]) -> usize {
        entries
            .iter()
            .filter(|(topic, value)| self.remove(topic, value))
            .count()
    }

    pub fn remove_all_nullable(
        &self,
        entries: Option<&[(String, TopicPublishInfo)]>,
    ) -> Result<usize, TopicPublishInfoTableError> {
        let Some(entries) = entries else {
            return Err(TopicPublishInfoTableError::NullCollection);
        };
        Ok(self.remove_all(entries))
    }

    pub fn retain_all(&self, entries: &[(String, TopicPublishInfo)]) -> usize {
        let keep = entries
            .iter()
            .map(|(topic, value)| (topic.as_str(), value))
            .collect::<Vec<_>>();
        self.0
            .snapshot()
            .into_iter()
            .filter(|(topic, value)| {
                !keep.iter().any(|(keep_topic, keep_value)| {
                    *keep_topic == topic && keep_value.ptr_eq(value)
                }) && self.0.remove_if_equal(topic, value)
            })
            .count()
    }

    pub fn retain_all_nullable(
        &self,
        entries: Option<&[(String, TopicPublishInfo)]>,
    ) -> Result<usize, TopicPublishInfoTableError> {
        let Some(entries) = entries else {
            return Err(TopicPublishInfoTableError::NullCollection);
        };
        Ok(self.retain_all(entries))
    }

    pub fn clear(&self) {
        self.0.clear();
    }

    pub fn remove_if(&self, mut predicate: impl FnMut(&str, &TopicPublishInfo) -> bool) -> usize {
        self.0
            .snapshot()
            .into_iter()
            .filter(|(topic, value)| predicate(topic, value) && self.remove(topic, value))
            .count()
    }

    pub fn remove_if_with_nullable_callback<F>(
        &self,
        predicate: Option<&mut F>,
    ) -> Result<usize, TopicPublishInfoTableError>
    where
        F: FnMut(&str, &TopicPublishInfo) -> bool,
    {
        let Some(predicate) = predicate else {
            return Err(TopicPublishInfoTableError::NullCallback);
        };
        Ok(self.remove_if(|topic, value| predicate(topic, value)))
    }
}

#[cfg(test)]
mod table_tests {
    use super::*;

    #[test]
    fn producer_queue_index_overflow_matches_pinned_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let expected = &fixture["threadLocalIndexApi"];
        let index = ProducerQueueIndex::default();
        let thread = std::thread::current().id();
        index
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(thread, AtomicI32::new(i32::MAX));

        assert_eq!(
            index.increment_and_get(),
            expected["overflowFirst"].as_i64().unwrap() as i32
        );
        assert_eq!(
            index.increment_and_get(),
            expected["overflowSecond"].as_i64().unwrap() as i32
        );
        assert_eq!(
            index.to_string(),
            expected["afterOverflowString"].as_str().unwrap()
        );
    }

    #[test]
    fn remapping_callback_can_update_a_different_topic() {
        let table = TopicPublishInfoTable::default();
        let first = TopicPublishInfo::default();
        let second = TopicPublishInfo::default();

        let computed = table.compute_if_absent("first", |_| {
            table.put("second", second.clone());
            Some(first.clone())
        });

        assert!(computed.unwrap().ptr_eq(&first));
        assert!(table.get("second").unwrap().ptr_eq(&second));
    }

    #[test]
    fn recursive_same_topic_mutation_fails_instead_of_deadlocking() {
        let table = TopicPublishInfoTable::default();
        let nested_table = table.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            table.compute_if_absent("same", |_| {
                nested_table.put("same", TopicPublishInfo::default());
                Some(TopicPublishInfo::default())
            });
        }));

        assert!(result.is_err());
        assert!(table.is_empty());
    }

    #[test]
    fn replace_all_null_result_keeps_prior_updates() {
        let table = TopicPublishInfoTable::default();
        let old_a = TopicPublishInfo::default();
        let old_b = TopicPublishInfo::default();
        let replacement = TopicPublishInfo::default();
        table.put("a", old_a.clone());
        table.put("b", old_b.clone());

        let result =
            table.replace_all_nullable(|topic, _| (topic == "a").then(|| replacement.clone()));

        assert_eq!(result, Err(TopicPublishInfoTableError::NullRemappingResult));
        assert!(table.get("a").unwrap().ptr_eq(&replacement));
        assert!(table.get("b").unwrap().ptr_eq(&old_b));
    }
}
