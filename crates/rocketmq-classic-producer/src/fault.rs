use std::{
    any::Any,
    cmp::Ordering as CmpOrdering,
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex, PoisonError, RwLock,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::config::{DEFAULT_LATENCY_MAX_MILLIS, DEFAULT_NOT_AVAILABLE_DURATION_MILLIS};
use rocketmq_classic_common::message::MessageQueue;

/// A shared, safely mutable producer fault-strategy array.
///
/// Cloned handles refer to the same values. This matches Java's producer API,
/// where setters retain the supplied `long[]` and getters return that same
/// array for later fault updates to read.
#[derive(Clone)]
pub struct ProducerLongArray {
    values: Option<Arc<RwLock<Vec<i64>>>>,
}

impl ProducerLongArray {
    pub fn new(values: Vec<i64>) -> Self {
        Self {
            values: Some(Arc::new(RwLock::new(values))),
        }
    }

    /// Returns a detached copy of the current array values, or `None` when
    /// this handle represents Java's null array value.
    pub fn snapshot(&self) -> Option<Vec<i64>> {
        Some(
            self.values
                .as_ref()?
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
    }

    /// Returns whether this handle represents Java's null array value.
    pub fn is_null(&self) -> bool {
        self.values.is_none()
    }

    /// Returns one value, or `None` when this is a null handle or `index` is
    /// outside the fixed array.
    pub fn get(&self, index: usize) -> Option<i64> {
        self.values
            .as_ref()?
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(index)
            .copied()
    }

    /// Replaces one value and returns its previous value, or `None` when this
    /// is a null handle or `index` is outside the fixed array.
    pub fn set(&self, index: usize, value: i64) -> Option<i64> {
        let mut values = self
            .values
            .as_ref()?
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let slot = values.get_mut(index)?;
        Some(std::mem::replace(slot, value))
    }

    pub fn len(&self) -> usize {
        self.values.as_ref().map_or(0, |values| {
            values.read().unwrap_or_else(PoisonError::into_inner).len()
        })
    }

    pub fn is_empty(&self) -> bool {
        self.values.as_ref().is_none_or(|values| {
            values
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        })
    }
}

impl From<Vec<i64>> for ProducerLongArray {
    fn from(values: Vec<i64>) -> Self {
        Self::new(values)
    }
}

impl From<Option<ProducerLongArray>> for ProducerLongArray {
    fn from(values: Option<ProducerLongArray>) -> Self {
        values.unwrap_or(Self { values: None })
    }
}

impl fmt::Debug for ProducerLongArray {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ProducerLongArray")
            .field(&self.snapshot())
            .finish()
    }
}

impl PartialEq for ProducerLongArray {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for ProducerLongArray {}

/// Java-compatible value surface for
/// `LatencyFaultToleranceImpl.FaultItem`.
///
/// The Java class is a non-static inner class. Its public behavior does not
/// read the enclosing strategy, so Rust owns the value directly instead of
/// retaining an otherwise-unused strategy reference.
pub struct FaultItem {
    name: Option<String>,
    current_latency: AtomicI64,
    start_timestamp: AtomicI64,
    check_stamp: AtomicI64,
    reachable: AtomicBool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultItemCompareError {
    NullOther,
    WrongType,
}

impl FaultItemCompareError {
    pub fn java_exception_name(self) -> &'static str {
        match self {
            Self::NullOther => "java.lang.NullPointerException",
            Self::WrongType => "java.lang.ClassCastException",
        }
    }
}

impl fmt::Display for FaultItemCompareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NullOther => "NullPointerException",
            Self::WrongType => "ClassCastException",
        })
    }
}

impl std::error::Error for FaultItemCompareError {}

impl FaultItem {
    pub fn new(name: Option<String>) -> Self {
        Self {
            name,
            current_latency: AtomicI64::new(0),
            start_timestamp: AtomicI64::new(0),
            check_stamp: AtomicI64::new(0),
            reachable: AtomicBool::new(false),
        }
    }

    pub fn update_not_available_duration(&self, not_available_duration_millis: i64) {
        let comparison_time = java_current_time_millis();
        if not_available_duration_millis > 0
            && comparison_time.wrapping_add(not_available_duration_millis)
                > self.start_timestamp.load(Ordering::SeqCst)
        {
            let start_timestamp =
                java_current_time_millis().wrapping_add(not_available_duration_millis);
            self.start_timestamp
                .store(start_timestamp, Ordering::SeqCst);
            tracing::info!(
                target: "org.apache.rocketmq.client.latency.MQFaultStrategy",
                "{} will be isolated for {} ms.",
                self.name.as_deref().unwrap_or("null"),
                not_available_duration_millis,
            );
        }
    }

    pub fn java_compare_to_nullable(
        &self,
        other: Option<&Self>,
    ) -> Result<CmpOrdering, FaultItemCompareError> {
        let other = other.ok_or(FaultItemCompareError::NullOther)?;
        let self_available = self.is_available();
        let other_available = other.is_available();
        if self_available != other_available {
            if self.is_available() {
                return Ok(CmpOrdering::Less);
            }
            if other.is_available() {
                return Ok(CmpOrdering::Greater);
            }
        }

        let latency_order = self.get_current_latency().cmp(&other.get_current_latency());
        if latency_order != CmpOrdering::Equal {
            return Ok(latency_order);
        }

        Ok(self.get_start_timestamp().cmp(&other.get_start_timestamp()))
    }

    /// Models the erased `Comparable.compareTo(Object)` bridge, including its
    /// cast failure for a non-`FaultItem` argument.
    pub fn java_compare_to_object(
        &self,
        other: Option<&dyn Any>,
    ) -> Result<CmpOrdering, FaultItemCompareError> {
        let other = other.ok_or(FaultItemCompareError::NullOther)?;
        self.java_compare_to_nullable(
            other
                .downcast_ref::<Self>()
                .ok_or(FaultItemCompareError::WrongType)
                .map(Some)?,
        )
    }

    pub fn set_reachable(&self, reachable: bool) {
        self.reachable.store(reachable, Ordering::SeqCst);
    }

    pub fn set_check_stamp(&self, check_stamp: i64) {
        self.check_stamp.store(check_stamp, Ordering::SeqCst);
    }

    pub fn is_available(&self) -> bool {
        java_current_time_millis() >= self.get_start_timestamp()
    }

    pub fn is_reachable(&self) -> bool {
        self.reachable.load(Ordering::SeqCst)
    }

    pub fn get_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn get_current_latency(&self) -> i64 {
        self.current_latency.load(Ordering::SeqCst)
    }

    pub fn set_current_latency(&self, current_latency: i64) {
        self.current_latency
            .store(current_latency, Ordering::SeqCst);
    }

    pub fn get_start_timestamp(&self) -> i64 {
        self.start_timestamp.load(Ordering::SeqCst)
    }

    pub fn set_start_timestamp(&self, start_timestamp: i64) {
        self.start_timestamp
            .store(start_timestamp, Ordering::SeqCst);
    }

    pub fn java_equals(&self, other: Option<&dyn Any>) -> bool {
        let Some(other) = other.and_then(|value| value.downcast_ref::<Self>()) else {
            return false;
        };
        std::ptr::eq(self, other)
            || (self.name == other.name
                && self.get_current_latency() == other.get_current_latency()
                && self.get_start_timestamp() == other.get_start_timestamp())
    }

    pub fn java_hash_code(&self) -> i32 {
        let mut hash = self.name.as_deref().map_or(0, java_fault_string_hash);
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(java_fault_long_hash(self.get_current_latency()));
        hash.wrapping_mul(31)
            .wrapping_add(java_fault_long_hash(self.get_start_timestamp()))
    }

    pub fn java_to_string(&self) -> String {
        self.to_string()
    }
}

impl PartialEq for FaultItem {
    fn eq(&self, other: &Self) -> bool {
        self.java_equals(Some(other as &dyn Any))
    }
}

impl Eq for FaultItem {}

impl Hash for FaultItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.java_hash_code().hash(state);
    }
}

impl fmt::Display for FaultItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "FaultItem{{name='{}', currentLatency={}, startTimestamp={}, reachableFlag={}}}",
            self.name.as_deref().unwrap_or("null"),
            self.get_current_latency(),
            self.get_start_timestamp(),
            self.is_reachable(),
        )
    }
}

impl fmt::Debug for FaultItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

fn java_current_time_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(error) => -(error.duration().as_millis() as i64),
    }
}

fn java_fault_string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, code_unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(code_unit))
    })
}

fn java_fault_long_hash(value: i64) -> i32 {
    let bits = value as u64;
    ((bits >> 32) as u32 ^ bits as u32) as i32
}

#[derive(Clone, Copy, Debug)]
struct FaultRecord {
    current_latency_millis: u64,
    unavailable_until: Instant,
    check_after: Instant,
    reachable: bool,
}

/// Producer-side copy of the pinned Java latency and reachability selection
/// tables. The clock is passed into pure methods so boundary cases can be
/// checked without sleeping.
pub(crate) struct FaultStrategy {
    items: Mutex<HashMap<String, FaultRecord>>,
    latency_max_millis: RwLock<ProducerLongArray>,
    not_available_duration_millis: RwLock<ProducerLongArray>,
    send_latency_fault_enable: AtomicBool,
    start_detector_enable: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FaultStrategyUpdateError {
    NullLatencyMax,
    NullNotAvailableDuration,
    DurationIndexOutOfBounds,
}

impl FaultStrategyUpdateError {
    pub(crate) fn java_exception_name(self) -> &'static str {
        match self {
            Self::NullLatencyMax | Self::NullNotAvailableDuration => {
                "java.lang.NullPointerException"
            }
            Self::DurationIndexOutOfBounds => "java.lang.ArrayIndexOutOfBoundsException",
        }
    }
}

impl Default for FaultStrategy {
    fn default() -> Self {
        Self::new(
            DEFAULT_LATENCY_MAX_MILLIS.to_vec(),
            DEFAULT_NOT_AVAILABLE_DURATION_MILLIS.to_vec(),
        )
    }
}

impl FaultStrategy {
    pub(crate) fn new(
        latency_max_millis: Vec<i64>,
        not_available_duration_millis: Vec<i64>,
    ) -> Self {
        Self {
            items: Mutex::new(HashMap::new()),
            latency_max_millis: RwLock::new(ProducerLongArray::new(latency_max_millis)),
            not_available_duration_millis: RwLock::new(ProducerLongArray::new(
                not_available_duration_millis,
            )),
            send_latency_fault_enable: AtomicBool::new(false),
            start_detector_enable: AtomicBool::new(false),
        }
    }

    pub(crate) fn latency_max_millis(&self) -> ProducerLongArray {
        self.latency_max_millis
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_latency_max_millis(&self, value: ProducerLongArray) {
        *self
            .latency_max_millis
            .write()
            .unwrap_or_else(PoisonError::into_inner) = value;
    }

    pub(crate) fn not_available_duration_millis(&self) -> ProducerLongArray {
        self.not_available_duration_millis
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_not_available_duration_millis(&self, value: ProducerLongArray) {
        *self
            .not_available_duration_millis
            .write()
            .unwrap_or_else(PoisonError::into_inner) = value;
    }

    pub(crate) fn send_latency_fault_enable(&self) -> bool {
        self.send_latency_fault_enable.load(Ordering::SeqCst)
    }

    pub(crate) fn set_send_latency_fault_enable(&self, value: bool) {
        self.send_latency_fault_enable
            .store(value, Ordering::SeqCst);
    }

    pub(crate) fn start_detector_enable(&self) -> bool {
        self.start_detector_enable.load(Ordering::SeqCst)
    }

    pub(crate) fn set_start_detector_enable(&self, value: bool) {
        self.start_detector_enable.store(value, Ordering::SeqCst);
    }

    pub(crate) fn update(
        &self,
        broker: &str,
        latency: Duration,
        isolation: bool,
        reachable: bool,
        now: Instant,
    ) -> Result<(), FaultStrategyUpdateError> {
        let duration_key = if isolation {
            10_000
        } else {
            latency.as_millis().min(u128::from(u64::MAX)) as u64
        };
        let latency_max_millis = self
            .latency_max_millis()
            .snapshot()
            .ok_or(FaultStrategyUpdateError::NullLatencyMax)?;
        let not_available_duration_millis = self.not_available_duration_millis().snapshot();
        let duration = unavailable_duration(
            duration_key,
            &latency_max_millis,
            not_available_duration_millis.as_deref(),
        )?
        .max(0) as u64;
        let mut items = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = items.entry(broker.to_owned()).or_insert(FaultRecord {
            current_latency_millis: 0,
            unavailable_until: now,
            check_after: now,
            reachable: true,
        });
        entry.current_latency_millis = latency.as_millis().min(u128::from(u64::MAX)) as u64;
        entry.unavailable_until = entry
            .unavailable_until
            .max(now + Duration::from_millis(duration));
        entry.reachable = reachable;
        Ok(())
    }

    pub(crate) fn take_due_for_detection(&self, now: Instant, interval: Duration) -> Vec<String> {
        let mut items = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        items
            .iter_mut()
            .filter_map(|(broker, item)| {
                if item.check_after <= now {
                    item.check_after = now + interval;
                    Some(broker.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn mark_reachable(&self, broker: &str) {
        if let Some(item) = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(broker)
        {
            item.reachable = true;
        }
    }

    pub(crate) fn remove(&self, broker: &str) {
        self.items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(broker);
    }

    pub(crate) fn select(
        &self,
        queues: &[MessageQueue],
        last_broker: Option<&str>,
        latency_fault_enabled: bool,
        mut next_index: impl FnMut(usize) -> usize,
        now: Instant,
    ) -> Option<MessageQueue> {
        if queues.is_empty() {
            return None;
        }
        let items = self
            .items
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let acceptable = |queue: &MessageQueue, avoid_last: bool| {
            let Some(broker) = queue.broker_name() else {
                return !avoid_last;
            };
            if avoid_last && last_broker == Some(broker) {
                return false;
            }
            if !latency_fault_enabled {
                return true;
            }
            items
                .get(broker)
                .is_none_or(|item| item.unavailable_until <= now && item.reachable)
        };

        if latency_fault_enabled {
            if let Some(queue) = choose_matching(queues, &acceptable, true, &mut next_index) {
                return Some(queue);
            }
            let reachable = |queue: &MessageQueue, avoid_last: bool| {
                let Some(broker) = queue.broker_name() else {
                    return !avoid_last;
                };
                if avoid_last && last_broker == Some(broker) {
                    return false;
                }
                items.get(broker).is_none_or(|item| item.reachable)
            };
            if let Some(queue) = choose_matching(queues, &reachable, true, &mut next_index) {
                return Some(queue);
            }
        }

        choose_matching(
            queues,
            &|queue, avoid_last| {
                if avoid_last && last_broker.is_some() && queue.broker_name() == last_broker {
                    return false;
                }
                true
            },
            true,
            &mut next_index,
        )
        .or_else(|| choose_matching(queues, &|_, _| true, false, &mut next_index))
    }

    #[cfg(test)]
    fn item(&self, broker: &str) -> Option<(u64, Instant, bool)> {
        self.items.lock().unwrap().get(broker).map(|item| {
            (
                item.current_latency_millis,
                item.unavailable_until,
                item.reachable,
            )
        })
    }
}

fn choose_matching(
    queues: &[MessageQueue],
    filter: &impl Fn(&MessageQueue, bool) -> bool,
    avoid_last: bool,
    next_index: &mut impl FnMut(usize) -> usize,
) -> Option<MessageQueue> {
    for _ in 0..queues.len() {
        let index = next_index(queues.len()) % queues.len();
        let queue = &queues[index];
        if filter(queue, avoid_last) {
            return Some(queue.clone());
        }
    }
    None
}

fn unavailable_duration(
    latency_millis: u64,
    latency_max_millis: &[i64],
    not_available_duration_millis: Option<&[i64]>,
) -> Result<i64, FaultStrategyUpdateError> {
    let Some(index) = (0..latency_max_millis.len()).rev().find(|index| {
        let threshold = latency_max_millis[*index];
        threshold <= 0
            || u64::try_from(threshold).is_ok_and(|threshold| latency_millis >= threshold)
    }) else {
        return Ok(0);
    };
    let not_available_duration_millis =
        not_available_duration_millis.ok_or(FaultStrategyUpdateError::NullNotAvailableDuration)?;
    not_available_duration_millis
        .get(index)
        .copied()
        .ok_or(FaultStrategyUpdateError::DurationIndexOutOfBounds)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(broker: &str, queue_id: i32) -> MessageQueue {
        MessageQueue::new("orders", broker, queue_id)
    }

    #[test]
    fn unavailable_duration_matches_java_latency_thresholds() {
        let cases = [
            (49, 0),
            (50, 0),
            (100, 0),
            (549, 0),
            (550, 2_000),
            (1_800, 5_000),
            (3_000, 6_000),
            (5_000, 10_000),
            (15_000, 30_000),
        ];
        for (latency, expected) in cases {
            assert_eq!(
                unavailable_duration(
                    latency,
                    &DEFAULT_LATENCY_MAX_MILLIS,
                    Some(&DEFAULT_NOT_AVAILABLE_DURATION_MILLIS),
                ),
                Ok(expected),
                "{latency}"
            );
        }
    }

    #[test]
    fn fault_defaults_and_detector_contract_match_pinned_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let java = &fixture["faultStrategy"];
        let as_i64 = |values: &serde_json::Value| {
            values
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_i64().unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            as_i64(&java["latencyMaxMillis"]),
            DEFAULT_LATENCY_MAX_MILLIS
        );
        assert_eq!(
            as_i64(&java["notAvailableDurationMillis"]),
            DEFAULT_NOT_AVAILABLE_DURATION_MILLIS
        );
        assert_eq!(java["sendLatencyFaultEnable"], false);
        assert_eq!(java["startDetectorEnable"], false);
        assert_eq!(java["detectTimeoutMillis"], 200);
        assert_eq!(java["detectIntervalMillis"], 2_000);
        assert_eq!(java["scheduleInitialDelaySeconds"], 3);
        assert_eq!(java["schedulePeriodSeconds"], 3);
        assert_eq!(
            java["probeRequestCode"],
            rocketmq_classic_protocol::codes::request::GET_MAX_OFFSET
        );
        assert_eq!(java["probeQueueId"], 0);
        assert_eq!(java["probeCommitted"], true);
    }

    #[test]
    fn configured_latency_tables_control_unavailable_duration() {
        let now = Instant::now();
        let fault = FaultStrategy::new(vec![10, 20], vec![100, 300]);
        fault
            .update("broker-a", Duration::from_millis(25), false, true, now)
            .unwrap();
        assert_eq!(
            fault.item("broker-a").unwrap().1,
            now + Duration::from_millis(300)
        );
    }

    #[test]
    fn same_shared_handle_can_supply_both_java_latency_arrays() {
        let now = Instant::now();
        let values = ProducerLongArray::new(vec![10, 30, 100]);
        let fault = FaultStrategy::new(Vec::new(), Vec::new());
        fault.set_latency_max_millis(values.clone());
        fault.set_not_available_duration_millis(values);

        fault
            .update("broker-a", Duration::from_millis(20), false, true, now)
            .unwrap();

        assert_eq!(
            fault.item("broker-a").unwrap().1,
            now + Duration::from_millis(10)
        );
    }

    #[test]
    fn latency_selection_prefers_available_and_skips_the_previous_broker() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault
            .update("broker-a", Duration::from_millis(3_000), true, true, now)
            .unwrap();
        let queues = [queue("broker-a", 0), queue("broker-b", 0)];
        let picked = fault
            .select(&queues, Some("broker-b"), true, |_| 0, now)
            .unwrap();
        assert_eq!(picked.broker_name(), Some("broker-a"));
    }

    #[test]
    fn unavailable_reachable_brokers_are_fallbacks_before_all_routes() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault
            .update("broker-a", Duration::from_millis(3_000), true, true, now)
            .unwrap();
        let queues = [queue("broker-a", 0), queue("broker-b", 0)];
        let picked = fault
            .select(&queues, Some("broker-b"), true, |_| 0, now)
            .unwrap();
        assert_eq!(picked.broker_name(), Some("broker-a"));
        assert_eq!(fault.item("broker-a").unwrap().0, 3_000);
    }

    #[test]
    fn broker_becomes_available_at_the_isolation_deadline() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault
            .update("broker-a", Duration::from_millis(600), false, true, now)
            .unwrap();
        let queues = [queue("broker-a", 0), queue("broker-b", 0)];
        let mut index = 0;
        assert_eq!(
            fault
                .select(
                    &queues,
                    None,
                    true,
                    |length| {
                        let value = index;
                        index += 1;
                        value % length
                    },
                    now,
                )
                .unwrap()
                .broker_name(),
            Some("broker-b")
        );
        index = 0;
        assert_eq!(
            fault
                .select(
                    &queues,
                    None,
                    true,
                    |length| {
                        let value = index;
                        index += 1;
                        value % length
                    },
                    now + Duration::from_millis(1_999)
                )
                .unwrap()
                .broker_name(),
            Some("broker-b")
        );
        assert_eq!(
            fault
                .select(
                    &queues,
                    None,
                    true,
                    |_| 0,
                    now + Duration::from_millis(2_000)
                )
                .unwrap()
                .broker_name(),
            Some("broker-a")
        );
    }

    #[test]
    fn detector_candidates_are_rate_limited_and_success_restores_reachability() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault
            .update("broker-a", Duration::from_millis(600), true, false, now)
            .unwrap();
        assert_eq!(
            fault.take_due_for_detection(now, Duration::from_secs(2)),
            ["broker-a"]
        );
        assert!(
            fault
                .take_due_for_detection(now + Duration::from_millis(1_999), Duration::from_secs(2))
                .is_empty()
        );
        assert_eq!(
            fault.take_due_for_detection(now + Duration::from_secs(2), Duration::from_secs(2)),
            ["broker-a"]
        );
        fault.mark_reachable("broker-a");
        assert!(fault.item("broker-a").unwrap().2);
        fault.remove("broker-a");
        assert!(fault.item("broker-a").is_none());
    }

    #[test]
    fn unreachable_flag_is_preserved_until_a_success_or_detector_update() {
        let now = Instant::now();
        let fault = FaultStrategy::default();
        fault
            .update("broker-a", Duration::ZERO, true, false, now)
            .unwrap();
        assert!(!fault.item("broker-a").unwrap().2);
        fault
            .update("broker-a", Duration::ZERO, false, true, now)
            .unwrap();
        assert!(fault.item("broker-a").unwrap().2);
    }
}
