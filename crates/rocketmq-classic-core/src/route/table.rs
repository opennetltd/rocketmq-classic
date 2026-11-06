use super::cache::{RouteCache, route_values_equal};
use crate::runtime::RuntimeError;
use rocketmq_classic_protocol::route::TopicRouteData;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock, Weak},
};

/// Shared, safely mutable route DTO, matching the object identity Java stores
/// in its raw topic route table.
///
/// ```compile_fail
/// use rocketmq_classic_core::route::TopicRouteDataHandle;
/// let route = TopicRouteDataHandle::new(Default::default());
/// route.ptr_eq(&route);
/// ```
#[derive(Clone, Debug)]
pub struct TopicRouteDataHandle(Arc<RwLock<TopicRouteData>>);

#[derive(Clone)]
pub struct WeakTopicRouteDataHandle(Weak<RwLock<TopicRouteData>>);

/// Detached `Map.Entry`-like view over one raw route-table mapping.
///
/// The cached value stays detached from later map replacements. Calling
/// `set_value` updates this entry's cached value and unconditionally writes
/// the same value under its key in the backing route table.
#[derive(Clone)]
pub struct TopicRouteEntryHandle {
    table: TopicRouteTable,
    key: String,
    value: Arc<RwLock<TopicRouteDataHandle>>,
}

impl TopicRouteEntryHandle {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Result<TopicRouteDataHandle, RuntimeError> {
        self.value
            .read()
            .map(|value| value.clone())
            .map_err(|_| RuntimeError::RegistryPoisoned)
    }

    /// Replaces this entry's cached value and the current table mapping,
    /// returning the value captured by this entry before replacement.
    pub fn set_value(
        &self,
        value: TopicRouteDataHandle,
    ) -> Result<TopicRouteDataHandle, RuntimeError> {
        let mut current = self
            .value
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let previous = std::mem::replace(&mut *current, value.clone());
        self.table.put(self.key.clone(), value)?;
        Ok(previous)
    }

    /// Applies Java `Map.Entry.setValue` null rejection without changing the
    /// captured entry or its backing route-table mapping.
    pub fn set_value_nullable(
        &self,
        value: Option<TopicRouteDataHandle>,
    ) -> Result<TopicRouteDataHandle, RuntimeError> {
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        self.set_value(value)
    }
}

impl WeakTopicRouteDataHandle {
    pub fn upgrade(&self) -> Option<TopicRouteDataHandle> {
        self.0.upgrade().map(TopicRouteDataHandle)
    }
}

impl TopicRouteDataHandle {
    pub fn new(route: TopicRouteData) -> Self {
        Self(Arc::new(RwLock::new(route)))
    }

    /// Returns an owned DTO copy so callers can inspect it without holding a
    /// lock or borrowing data that another alias may mutate.
    pub fn snapshot(&self) -> Result<TopicRouteData, RuntimeError> {
        self.0
            .read()
            .map(|route| route.clone())
            .map_err(|_| RuntimeError::RegistryPoisoned)
    }

    /// Mutates the shared DTO observed by every alias to this handle.
    /// The update closure runs while holding the write lock and must not call
    /// back into snapshot or mutation methods on this same handle.
    pub fn with_mut<R>(
        &self,
        update: impl FnOnce(&mut TopicRouteData) -> R,
    ) -> Result<R, RuntimeError> {
        self.0
            .write()
            .map(|mut route| update(&mut route))
            .map_err(|_| RuntimeError::RegistryPoisoned)
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn downgrade(&self) -> WeakTopicRouteDataHandle {
        WeakTopicRouteDataHandle(Arc::downgrade(&self.0))
    }
}

/// Live raw topic map. Writes neither fetch nor validate/convert a route.
/// Values use Java route content equality, not owner registration identity.
/// The handle retains cache data only, never runtime workers or transport.
#[derive(Clone)]
pub struct TopicRouteTable(RouteCache);

impl TopicRouteTable {
    pub(crate) fn new(cache: RouteCache) -> Self {
        Self(cache)
    }
    pub fn get(&self, key: &str) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.get(key).cloned())
    }
    /// Preserves Java `ConcurrentMap.get`'s null-key rejection.
    pub fn get_nullable(
        &self,
        key: Option<&str>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        self.get(key)
    }
    /// Map.getOrDefault counterpart using raw-table presence only.
    pub fn get_or_default(
        &self,
        key: &str,
        default: TopicRouteDataHandle,
    ) -> Result<TopicRouteDataHandle, RuntimeError> {
        Ok(self.get(key)?.unwrap_or(default))
    }

    /// Preserves Java `Map.getOrDefault`'s nullable default value while
    /// rejecting a null route key like `ConcurrentHashMap`.
    pub fn get_or_default_nullable(
        &self,
        key: Option<&str>,
        default: Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        Ok(self.get(key)?.or(default))
    }

    pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.contains_key(key))
    }
    /// Preserves Java `ConcurrentMap.containsKey`'s null-key rejection.
    pub fn contains_key_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        self.contains_key(key)
    }
    /// Uses the source route-content equality used by map-value comparisons.
    pub fn contains_value(&self, value: &TopicRouteData) -> Result<bool, RuntimeError> {
        for current in self.0.snapshot()?.raw_routes.values() {
            if route_values_equal(&current.snapshot()?, value) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Preserves Java `ConcurrentMap.containsValue`'s null-value rejection.
    pub fn contains_value_nullable(
        &self,
        value: Option<&TopicRouteData>,
    ) -> Result<bool, RuntimeError> {
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        self.contains_value(value)
    }
    pub fn len(&self) -> Result<usize, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.len())
    }
    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        Ok(self.len()? == 0)
    }
    /// Detached owned iteration copy; retaining this table handle stays live.
    pub fn snapshot(&self) -> Result<BTreeMap<String, TopicRouteDataHandle>, RuntimeError> {
        Ok(self.0.snapshot()?.raw_routes.clone())
    }
    /// Visits a detached map snapshot without holding the route-cache lock.
    /// Java's weakly consistent live traversal is not represented here.
    pub fn for_each(
        &self,
        mut action: impl FnMut(&str, &TopicRouteDataHandle),
    ) -> Result<(), RuntimeError> {
        let snapshot = self.snapshot()?;
        for (key, value) in &snapshot {
            action(key, value);
        }
        Ok(())
    }

    /// Preserves Java `ConcurrentMap.forEach(null)` rejection.
    pub fn for_each_nullable(
        &self,
        action: Option<&mut crate::runtime::TableForEach<'_, TopicRouteDataHandle>>,
    ) -> Result<(), RuntimeError> {
        let Some(action) = action else {
            return Err(RuntimeError::InvalidRoute("null topic route callback"));
        };
        self.for_each(|key, value| action(key, value))
    }

    fn replace_if_handle_same_locked(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
        value: TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        self.0.replace_raw_if_same(key, expected, value)
    }

    fn replace_if_java_value_matches(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
        value: TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        let _gate = self.0.lock_raw_route_key(key)?;
        let Some(current) = self.get(key)? else {
            return Ok(false);
        };
        if !route_values_equal(&current.snapshot()?, &expected.snapshot()?) {
            return Ok(false);
        }
        self.replace_if_handle_same_locked(key, &current, value)
    }

    fn remove_if_handle_same(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        let _gate = self.0.lock_raw_route_key(key)?;
        self.remove_if_handle_same_locked(key, expected)
    }

    fn remove_if_handle_same_locked(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        self.0.remove_raw_if_same(key, expected)
    }
    pub fn key_set_view(&self) -> TopicRouteKeySetView {
        TopicRouteKeySetView(self.clone())
    }
    pub fn values_view(&self) -> TopicRouteValuesView {
        TopicRouteValuesView(self.clone())
    }
    pub fn entry_set_view(&self) -> TopicRouteEntrySetView {
        TopicRouteEntrySetView(self.clone())
    }
    pub fn put(
        &self,
        key: impl Into<String>,
        value: TopicRouteDataHandle,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        self.0.update_raw(|map| (true, map.insert(key, value)))
    }
    /// Preserves Java `ConcurrentMap.put`'s null-key/value rejection.
    pub fn put_nullable(
        &self,
        key: Option<&str>,
        value: Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        self.put(key, value)
    }
    /// Writes entries in source iteration order without converting route snapshots.
    pub fn put_all<I>(&self, entries: I) -> Result<(), RuntimeError>
    where
        I: IntoIterator<Item = (String, TopicRouteDataHandle)>,
    {
        for (key, value) in entries {
            let _gate = self.0.lock_raw_route_key(&key)?;
            self.0.update_raw(|map| {
                map.insert(key, value);
                (true, ())
            })?;
        }
        Ok(())
    }
    /// Applies nullable source-map rows in iterator order, leaving prior rows
    /// applied when Java `ConcurrentMap.putAll` would reject a null key/value.
    pub fn put_all_nullable_rows(
        &self,
        entries: Option<&[(Option<String>, Option<TopicRouteDataHandle>)]>,
    ) -> Result<(), RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null topic route putAll map"));
        };
        self.put_all_nullable_iter(entries.iter().cloned())
    }

    /// Applies nullable route rows from any source iterator, preserving its
    /// traversal order and `ConcurrentMap.putAll` partial-write behavior.
    pub fn put_all_nullable_iter<I>(&self, entries: I) -> Result<(), RuntimeError>
    where
        I: IntoIterator<Item = (Option<String>, Option<TopicRouteDataHandle>)>,
    {
        for (key, value) in entries {
            let Some(key) = key else {
                return Err(RuntimeError::InvalidRoute("null topic route key"));
            };
            let Some(value) = value else {
                return Err(RuntimeError::InvalidRoute("null topic route value"));
            };
            self.put(key, value)?;
        }
        Ok(())
    }
    pub fn put_if_absent(
        &self,
        key: impl Into<String>,
        value: TopicRouteDataHandle,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        self.0.update_raw(|map| {
            if let Some(previous) = map.get(&key) {
                (false, Some(previous.clone()))
            } else {
                map.insert(key, value);
                (true, None)
            }
        })
    }
    /// Preserves Java `ConcurrentMap.putIfAbsent`'s null-key/value rejection.
    pub fn put_if_absent_nullable(
        &self,
        key: Option<&str>,
        value: Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        self.put_if_absent(key, value)
    }
    /// Applies a Java-style ConcurrentMap remapping function. Conditional
    /// Updates serialize by exact key while the callback runs, so it is called
    /// once and ordinary same-key mutations wait.
    pub fn compute_if_absent(
        &self,
        key: impl Into<String>,
        mut mapping: impl FnMut(&str) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        if let Some(current) = self.get(&key)? {
            return Ok(Some(current));
        }
        let Some(value) = mapping(&key) else {
            return Ok(None);
        };
        let previous = self.0.update_raw(|map| {
            if let Some(current) = map.get(&key) {
                (false, Some(current.clone()))
            } else {
                map.insert(key.clone(), value.clone());
                (true, None)
            }
        })?;
        Ok(Some(previous.unwrap_or(value)))
    }
    /// Preserves Java `computeIfAbsent`'s null-key rejection before callback.
    pub fn compute_if_absent_nullable(
        &self,
        key: Option<&str>,
        mapping: impl FnMut(&str) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        self.compute_if_absent(key, mapping)
    }

    /// Preserves Java's nullable mapping-function boundary in addition to its key boundary.
    pub fn compute_if_absent_with_nullable_callback(
        &self,
        key: Option<&str>,
        mapping: Option<&mut crate::runtime::TableComputeIfAbsent<'_, TopicRouteDataHandle>>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(mapping) = mapping else {
            return Err(RuntimeError::InvalidRoute("null topic route callback"));
        };
        self.compute_if_absent_nullable(key, |key| mapping(key))
    }

    pub fn compute_if_present(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, &TopicRouteDataHandle) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        let Some(current) = self.get(&key)? else {
            return Ok(None);
        };
        Ok(match remapping(&key, &current) {
            Some(value) => {
                self.replace_if_handle_same_locked(&key, &current, value.clone())?;
                Some(value)
            }
            None => {
                self.remove_if_handle_same_locked(&key, &current)?;
                None
            }
        })
    }
    /// Preserves Java `computeIfPresent`'s null-key rejection before callback.
    pub fn compute_if_present_nullable(
        &self,
        key: Option<&str>,
        remapping: impl FnMut(&str, &TopicRouteDataHandle) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        self.compute_if_present(key, remapping)
    }

    /// Preserves Java's nullable remapping-function boundary in addition to its key boundary.
    pub fn compute_if_present_with_nullable_callback(
        &self,
        key: Option<&str>,
        remapping: Option<&mut crate::runtime::TableComputeIfPresent<'_, TopicRouteDataHandle>>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute("null topic route callback"));
        };
        self.compute_if_present_nullable(key, |key, value| remapping(key, value))
    }

    pub fn compute(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, Option<&TopicRouteDataHandle>) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        let current = self.get(&key)?;
        match (current.as_ref(), remapping(&key, current.as_ref())) {
            (None, None) => Ok(None),
            (None, Some(value)) => {
                let previous = self.0.update_raw(|map| {
                    if let Some(current) = map.get(&key) {
                        (false, Some(current.clone()))
                    } else {
                        map.insert(key.clone(), value.clone());
                        (true, None)
                    }
                })?;
                Ok(Some(previous.unwrap_or(value)))
            }
            (Some(current), Some(value)) => {
                self.replace_if_handle_same_locked(&key, current, value.clone())?;
                Ok(Some(value))
            }
            (Some(current), None) => {
                self.remove_if_handle_same_locked(&key, current)?;
                Ok(None)
            }
        }
    }
    /// Preserves Java `compute`'s null-key rejection before callback.
    pub fn compute_nullable(
        &self,
        key: Option<&str>,
        remapping: impl FnMut(&str, Option<&TopicRouteDataHandle>) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        self.compute(key, remapping)
    }

    /// Preserves Java's nullable remapping-function boundary in addition to its key boundary.
    pub fn compute_with_nullable_callback(
        &self,
        key: Option<&str>,
        remapping: Option<&mut crate::runtime::TableCompute<'_, TopicRouteDataHandle>>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute("null topic route callback"));
        };
        self.compute_nullable(key, |key, old_value| remapping(key, old_value))
    }

    pub fn merge(
        &self,
        key: impl Into<String>,
        value: TopicRouteDataHandle,
        mut remapping: impl FnMut(
            &TopicRouteDataHandle,
            &TopicRouteDataHandle,
        ) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        let Some(current) = self.get(&key)? else {
            let previous = self.0.update_raw(|map| {
                if let Some(current) = map.get(&key) {
                    (false, Some(current.clone()))
                } else {
                    map.insert(key.clone(), value.clone());
                    (true, None)
                }
            })?;
            return Ok(Some(previous.unwrap_or(value)));
        };
        Ok(match remapping(&current, &value) {
            Some(next) => {
                self.replace_if_handle_same_locked(&key, &current, next.clone())?;
                Some(next)
            }
            None => {
                self.remove_if_handle_same_locked(&key, &current)?;
                None
            }
        })
    }
    /// Preserves Java `merge`'s null-key/value rejection before callback.
    pub fn merge_nullable(
        &self,
        key: Option<&str>,
        value: Option<TopicRouteDataHandle>,
        remapping: impl FnMut(
            &TopicRouteDataHandle,
            &TopicRouteDataHandle,
        ) -> Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        self.merge(key, value, remapping)
    }

    /// Preserves Java's nullable remapping-function boundary in addition to key/value checks.
    pub fn merge_with_nullable_callback(
        &self,
        key: Option<&str>,
        value: Option<TopicRouteDataHandle>,
        remapping: Option<&mut crate::runtime::TableMerge<'_, TopicRouteDataHandle>>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute("null topic route callback"));
        };
        self.merge_nullable(key, value, |old_value, new_value| {
            remapping(old_value, new_value)
        })
    }

    /// Replaces values for keys in one detached key snapshot. The callback
    /// runs without a cache lock and may be retried if a concurrent write
    /// changes that key before its conditional replacement.
    pub fn replace_all(
        &self,
        mut remapping: impl FnMut(&str, &TopicRouteDataHandle) -> TopicRouteDataHandle,
    ) -> Result<(), RuntimeError> {
        self.replace_all_with_nullable_results(|key, observed| Some(remapping(key, observed)))
    }

    /// Preserves Java `ConcurrentMap.replaceAll(null)` rejection.
    pub fn replace_all_nullable(
        &self,
        remapping: Option<&mut crate::runtime::TableReplaceAll<'_, TopicRouteDataHandle>>,
    ) -> Result<(), RuntimeError> {
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute("null topic route callback"));
        };
        self.replace_all(|key, value| remapping(key, value))
    }

    /// Applies a Java-style `replaceAll` callback that can return null.
    /// Earlier replacements remain applied if a later callback returns `None`.
    pub fn replace_all_with_nullable_results(
        &self,
        mut remapping: impl FnMut(&str, &TopicRouteDataHandle) -> Option<TopicRouteDataHandle>,
    ) -> Result<(), RuntimeError> {
        for key in self.snapshot()?.into_keys() {
            let Some(mut observed) = self.get(&key)? else {
                continue;
            };
            loop {
                let Some(value) = remapping(&key, &observed) else {
                    return Err(RuntimeError::InvalidRoute("null topic route value"));
                };
                if self.replace_if_java_value_matches(&key, &observed, value)? {
                    break;
                }
                let Some(current) = self.get(&key)? else {
                    break;
                };
                observed = current;
            }
        }
        Ok(())
    }

    pub fn replace(
        &self,
        key: impl Into<String>,
        value: TopicRouteDataHandle,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        self.0.update_raw(|map| {
            if let std::collections::btree_map::Entry::Occupied(mut entry) = map.entry(key) {
                (true, Some(entry.insert(value)))
            } else {
                (false, None)
            }
        })
    }
    /// Preserves Java `ConcurrentMap.replace(key, value)` null checks.
    pub fn replace_nullable(
        &self,
        key: Option<&str>,
        value: Option<TopicRouteDataHandle>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        self.replace(key, value)
    }
    pub fn replace_if_equal(
        &self,
        key: impl Into<String>,
        expected: &TopicRouteData,
        value: TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        let key = key.into();
        let _gate = self.0.lock_raw_route_key(&key)?;
        loop {
            let Some(current) = self.get(&key)? else {
                return Ok(false);
            };
            if !route_values_equal(&current.snapshot()?, expected) {
                return Ok(false);
            }
            if self.replace_if_handle_same_locked(&key, &current, value.clone())? {
                return Ok(true);
            }
        }
    }
    /// Preserves Java `ConcurrentMap.replace(key, oldValue, newValue)` null checks.
    pub fn replace_if_equal_nullable(
        &self,
        key: Option<&str>,
        expected: Option<&TopicRouteData>,
        value: Option<TopicRouteDataHandle>,
    ) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        let Some(expected) = expected else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null topic route value"));
        };
        self.replace_if_equal(key, expected, value)
    }
    pub fn remove(&self, key: &str) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let _gate = self.0.lock_raw_route_key(key)?;
        self.0.update_raw(|map| {
            let previous = map.remove(key);
            (previous.is_some(), previous)
        })
    }
    /// Preserves Java `ConcurrentMap.remove`'s null-key rejection.
    pub fn remove_nullable(
        &self,
        key: Option<&str>,
    ) -> Result<Option<TopicRouteDataHandle>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        self.remove(key)
    }
    pub fn remove_if_equal(
        &self,
        key: &str,
        expected: &TopicRouteData,
    ) -> Result<bool, RuntimeError> {
        let _gate = self.0.lock_raw_route_key(key)?;
        loop {
            let Some(current) = self.get(key)? else {
                return Ok(false);
            };
            if !route_values_equal(&current.snapshot()?, expected) {
                return Ok(false);
            }
            if self.remove_if_handle_same_locked(key, &current)? {
                return Ok(true);
            }
        }
    }
    /// Preserves Java `ConcurrentMap.remove(key, value)` null-key and
    /// null-expected-value behavior.
    pub fn remove_if_equal_nullable(
        &self,
        key: Option<&str>,
        expected: Option<&TopicRouteData>,
    ) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null topic route key"));
        };
        let Some(expected) = expected else {
            return Ok(false);
        };
        self.remove_if_equal(key, expected)
    }
    /// Removes the keys observed at traversal start one at a time. Per-key
    /// gates order clear with compute-style mutations without making a
    /// whole-table transaction.
    pub fn clear(&self) -> Result<(), RuntimeError> {
        for key in self.snapshot()?.into_keys() {
            // Java's ConcurrentHashMap.clear can remove the key currently
            // being remapped by this thread. Other threads still wait for
            // the per-key operation to finish before clear removes it.
            let _gate = match self.0.lock_raw_route_key(&key) {
                Ok(gate) => Some(gate),
                Err(RuntimeError::RecursiveRouteTableMutation) => None,
                Err(error) => return Err(error),
            };
            self.0.update_raw(|map| (map.remove(&key).is_some(), ()))?;
        }
        Ok(())
    }
}

/// Retained live key-set view of a raw topic route table.
#[derive(Clone)]
pub struct TopicRouteKeySetView(TopicRouteTable);

impl TopicRouteKeySetView {
    pub fn contains(&self, key: &str) -> Result<bool, RuntimeError> {
        self.0.contains_key(key)
    }
    /// Preserves Java key-view null-key rejection.
    pub fn contains_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
        self.0.contains_key_nullable(key)
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached key copy from the live view.
    pub fn snapshot(&self) -> Result<BTreeSet<String>, RuntimeError> {
        Ok(self.0.snapshot()?.into_keys().collect())
    }

    pub fn remove(&self, key: &str) -> Result<bool, RuntimeError> {
        self.0.remove(key).map(|value| value.is_some())
    }
    /// Preserves Java key-view null-key rejection.
    pub fn remove_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
        self.0.remove_nullable(key).map(|value| value.is_some())
    }

    /// Removes matching keys while visiting a detached entry snapshot.
    pub fn remove_if(
        &self,
        mut predicate: impl FnMut(&str) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, value) in self.0.snapshot()? {
            if predicate(&key) && self.0.remove_if_handle_same(&key, &value)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Preserves Java collection-view `removeIf(null)` rejection.
    pub fn remove_if_nullable(
        &self,
        predicate: Option<&mut crate::runtime::TableKeyViewPredicate<'_>>,
    ) -> Result<usize, RuntimeError> {
        let Some(predicate) = predicate else {
            return Err(RuntimeError::InvalidRoute("null topic route predicate"));
        };
        self.remove_if(|key| predicate(key))
    }

    pub fn remove_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
        self.remove_if(|key| keys.contains(key))
    }
    /// Preserves Java collection-view `removeAll` null-collection rejection.
    pub fn remove_all_nullable(
        &self,
        keys: Option<&BTreeSet<String>>,
    ) -> Result<usize, RuntimeError> {
        let Some(keys) = keys else {
            return Err(RuntimeError::InvalidRoute("null topic route collection"));
        };
        self.remove_all(keys)
    }

    pub fn retain_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
        self.remove_if(|key| !keys.contains(key))
    }
    /// Preserves Java collection-view `retainAll` null-collection rejection.
    pub fn retain_all_nullable(
        &self,
        keys: Option<&BTreeSet<String>>,
    ) -> Result<usize, RuntimeError> {
        let Some(keys) = keys else {
            return Err(RuntimeError::InvalidRoute("null topic route collection"));
        };
        self.retain_all(keys)
    }

    pub fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear()
    }
}

/// Retained live values view of a raw topic route table.
#[derive(Clone)]
pub struct TopicRouteValuesView(TopicRouteTable);

impl TopicRouteValuesView {
    pub fn contains(&self, value: &TopicRouteData) -> Result<bool, RuntimeError> {
        self.0.contains_value(value)
    }
    /// Preserves Java values-view null-value rejection.
    pub fn contains_nullable(&self, value: Option<&TopicRouteData>) -> Result<bool, RuntimeError> {
        self.0.contains_value_nullable(value)
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached values copy from the live view.
    pub fn snapshot(&self) -> Result<Vec<TopicRouteDataHandle>, RuntimeError> {
        Ok(self.0.snapshot()?.into_values().collect())
    }

    /// Removes one mapping whose route matches by source route equality.
    pub fn remove(&self, value: &TopicRouteData) -> Result<bool, RuntimeError> {
        for (key, current) in self.0.snapshot()? {
            if route_values_equal(&current.snapshot()?, value)
                && self.0.remove_if_handle_same(&key, &current)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Java's values view rejects null for `contains`, while `remove(null)`
    /// returns false because ConcurrentHashMap cannot contain null values.
    pub fn remove_nullable(&self, value: Option<&TopicRouteData>) -> Result<bool, RuntimeError> {
        let Some(value) = value else {
            return Ok(false);
        };
        self.remove(value)
    }

    /// Removes matching values while visiting a detached entry snapshot.
    pub fn remove_if(
        &self,
        mut predicate: impl FnMut(&TopicRouteData) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, value) in self.0.snapshot()? {
            let route = value.snapshot()?;
            if predicate(&route) && self.0.remove_if_handle_same(&key, &value)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Preserves Java collection-view `removeIf(null)` rejection.
    pub fn remove_if_nullable(
        &self,
        predicate: Option<&mut crate::runtime::TableValueViewPredicate<'_, TopicRouteData>>,
    ) -> Result<usize, RuntimeError> {
        let Some(predicate) = predicate else {
            return Err(RuntimeError::InvalidRoute("null topic route predicate"));
        };
        self.remove_if(|value| predicate(value))
    }

    pub fn remove_all(&self, values: &[TopicRouteDataHandle]) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, current) in self.0.snapshot()? {
            let current_value = current.snapshot()?;
            let mut matches = false;
            for candidate in values {
                if route_values_equal(&candidate.snapshot()?, &current_value) {
                    matches = true;
                    break;
                }
            }
            if matches && self.0.remove_if_handle_same(&key, &current)? {
                removed += 1;
            }
        }
        Ok(removed)
    }
    /// Preserves Java collection-view `removeAll` null-collection rejection.
    pub fn remove_all_nullable(
        &self,
        values: Option<&[TopicRouteDataHandle]>,
    ) -> Result<usize, RuntimeError> {
        let Some(values) = values else {
            return Err(RuntimeError::InvalidRoute("null topic route collection"));
        };
        self.remove_all(values)
    }

    pub fn retain_all(&self, values: &[TopicRouteDataHandle]) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, current) in self.0.snapshot()? {
            let current_value = current.snapshot()?;
            let mut matches = false;
            for candidate in values {
                if route_values_equal(&candidate.snapshot()?, &current_value) {
                    matches = true;
                    break;
                }
            }
            if !matches && self.0.remove_if_handle_same(&key, &current)? {
                removed += 1;
            }
        }
        Ok(removed)
    }
    /// Preserves Java collection-view `retainAll` null-collection rejection.
    pub fn retain_all_nullable(
        &self,
        values: Option<&[TopicRouteDataHandle]>,
    ) -> Result<usize, RuntimeError> {
        let Some(values) = values else {
            return Err(RuntimeError::InvalidRoute("null topic route collection"));
        };
        self.retain_all(values)
    }

    pub fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear()
    }
}

/// Retained live entry-set view of a raw topic route table.
#[derive(Clone)]
pub struct TopicRouteEntrySetView(TopicRouteTable);

impl TopicRouteEntrySetView {
    pub fn contains(&self, key: &str, value: &TopicRouteData) -> Result<bool, RuntimeError> {
        match self.0.get(key)? {
            Some(current) => Ok(route_values_equal(&current.snapshot()?, value)),
            None => Ok(false),
        }
    }
    /// Preserves Java entry-view null-entry mismatch behavior.
    pub fn contains_nullable(
        &self,
        key: Option<&str>,
        value: Option<&TopicRouteData>,
    ) -> Result<bool, RuntimeError> {
        let (Some(key), Some(value)) = (key, value) else {
            return Ok(false);
        };
        self.contains(key, value)
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached entry copy from the live view.
    pub fn snapshot(&self) -> Result<BTreeMap<String, TopicRouteDataHandle>, RuntimeError> {
        self.0.snapshot()
    }

    /// Returns detached entry handles in deterministic key order. Entry value
    /// replacement follows Java `ConcurrentHashMap.MapEntry.setValue`: it
    /// updates both the captured entry and the backing map without refreshing
    /// other entry snapshots.
    pub fn entry_snapshots(&self) -> Result<Vec<TopicRouteEntryHandle>, RuntimeError> {
        Ok(self
            .0
            .snapshot()?
            .into_iter()
            .map(|(key, value)| TopicRouteEntryHandle {
                table: self.0.clone(),
                key,
                value: Arc::new(RwLock::new(value)),
            })
            .collect())
    }

    pub fn remove(&self, key: &str, value: &TopicRouteData) -> Result<bool, RuntimeError> {
        self.0.remove_if_equal(key, value)
    }
    /// Preserves Java entry-view null-entry mismatch behavior.
    pub fn remove_nullable(
        &self,
        key: Option<&str>,
        value: Option<&TopicRouteData>,
    ) -> Result<bool, RuntimeError> {
        let (Some(key), Some(value)) = (key, value) else {
            return Ok(false);
        };
        self.remove(key, value)
    }

    /// Removes matching entries while visiting a detached entry snapshot.
    pub fn remove_if(
        &self,
        mut predicate: impl FnMut(&str, &TopicRouteData) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, value) in self.0.snapshot()? {
            let route = value.snapshot()?;
            if predicate(&key, &route) && self.0.remove_if_handle_same(&key, &value)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Preserves Java collection-view `removeIf(null)` rejection.
    pub fn remove_if_nullable(
        &self,
        predicate: Option<&mut crate::runtime::TableEntryViewPredicate<'_, TopicRouteData>>,
    ) -> Result<usize, RuntimeError> {
        let Some(predicate) = predicate else {
            return Err(RuntimeError::InvalidRoute("null topic route predicate"));
        };
        self.remove_if(|key, value| predicate(key, value))
    }

    pub fn remove_all(
        &self,
        entries: &BTreeMap<String, TopicRouteDataHandle>,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, current) in self.0.snapshot()? {
            let Some(candidate) = entries.get(&key) else {
                continue;
            };
            let current_value = current.snapshot()?;
            if route_values_equal(&candidate.snapshot()?, &current_value)
                && self.0.remove_if_handle_same(&key, &current)?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }
    /// Preserves Java collection-view `removeAll` null-collection rejection.
    pub fn remove_all_nullable(
        &self,
        entries: Option<&BTreeMap<String, TopicRouteDataHandle>>,
    ) -> Result<usize, RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null topic route collection"));
        };
        self.remove_all(entries)
    }

    pub fn retain_all(
        &self,
        entries: &BTreeMap<String, TopicRouteDataHandle>,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, current) in self.0.snapshot()? {
            let current_value = current.snapshot()?;
            let matches = match entries.get(&key) {
                Some(candidate) => route_values_equal(&candidate.snapshot()?, &current_value),
                None => false,
            };
            if !matches && self.0.remove_if_handle_same(&key, &current)? {
                removed += 1;
            }
        }
        Ok(removed)
    }
    /// Preserves Java collection-view `retainAll` null-collection rejection.
    pub fn retain_all_nullable(
        &self,
        entries: Option<&BTreeMap<String, TopicRouteDataHandle>>,
    ) -> Result<usize, RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null topic route collection"));
        };
        self.retain_all(entries)
    }

    pub fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear()
    }
}
