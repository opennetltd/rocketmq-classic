use super::{
    ClientRuntime, ConsumerRuntime, NullableTableEntry, ProducerRuntime, RuntimeError,
    RuntimeState, TableCompute, TableComputeIfAbsent, TableComputeIfPresent,
    TableEntryViewPredicate, TableForEach, TableKeyViewPredicate, TableMerge, TableReplaceAll,
    TableReplaceAllOptionalResult, TableValueViewPredicate, registry::GroupRegistry,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) enum TableKind {
    Producer,
    Consumer,
}
#[derive(Clone)]
pub(super) enum TableValue {
    Producer(Arc<dyn ProducerRuntime>),
    Consumer(Arc<dyn ConsumerRuntime>),
}
#[derive(Clone)]
pub(super) enum ValueComparison {
    Identity,
}
#[derive(Clone)]
pub(super) enum TableMutation {
    Put {
        key: String,
        value: TableValue,
        only_absent: bool,
    },
    Replace {
        key: String,
        value: TableValue,
        expected: Option<TableValue>,
        comparison: Option<ValueComparison>,
    },
    Remove {
        key: String,
        expected: Option<TableValue>,
        comparison: Option<ValueComparison>,
    },
    Clear,
}
pub(super) struct TableChange {
    pub previous: Option<TableValue>,
    pub matched: bool,
}

#[derive(Clone)]
struct TableHandle {
    kind: TableKind,
    groups: Arc<GroupRegistry>,
    runtime: Weak<ClientRuntime>,
}
impl TableHandle {
    async fn key_gate(&self, key: &str) -> Result<OwnedMutexGuard<()>, RuntimeError> {
        Ok(self
            .groups
            .table_mutation_gate(self.kind, key)?
            .lock_owned()
            .await)
    }

    async fn mutate(&self, mutation: TableMutation) -> Result<TableChange, RuntimeError> {
        let gate = match &mutation {
            TableMutation::Put { key, .. }
            | TableMutation::Replace { key, .. }
            | TableMutation::Remove { key, .. } => Some(self.key_gate(key).await?),
            TableMutation::Clear => None,
        };
        self.mutate_with_gate(mutation, gate).await
    }

    async fn mutate_with_gate(
        &self,
        mutation: TableMutation,
        gate: Option<OwnedMutexGuard<()>>,
    ) -> Result<TableChange, RuntimeError> {
        if let Some(runtime) = self.runtime.upgrade() {
            // Starting can invoke addressing capabilities that reenter the raw
            // maps; no unregister transaction is active in that state. All
            // other live actor states serialize against pending unregisters.
            if runtime.state() != RuntimeState::Starting {
                return runtime
                    .mutate_registry_table(self.kind, mutation, gate)
                    .await;
            }
        }
        // Raw maps remain available before start and after stop. Do not create
        // a control worker or revive transport for these primitive writes.
        self.groups.mutate_table(self.kind, &mutation)
    }
}

macro_rules! table {
    ($name:ident, $owner:ident, $kind:ident, $select:ident, $null_group:literal, $null_value:literal, $key_set:ident, $values_view:ident, $entry_set:ident) => {
        /// Live registry mapping that strongly retains each mapped owner until
        /// replacement, removal, or clear, matching Java ConcurrentMap values.
        /// Owner implementations that refer back to `ClientRuntime` must use a
        /// weak runtime reference to avoid an `Arc` cycle. Primitive writes
        /// neither unregister from Brokers nor stop owners.
        #[derive(Clone)]
        pub struct $name(TableHandle);
        impl $name {
            fn owners_equal(expected: &Arc<dyn $owner>, current: &Arc<dyn $owner>) -> bool {
                Arc::ptr_eq(expected, current)
                    || expected
                        .map_value_equality_key()
                        .zip(current.map_value_equality_key())
                        .is_some_and(|(expected, current)| expected == current)
            }
            pub(super) fn new(runtime: &Arc<ClientRuntime>) -> Self {
                Self(TableHandle {
                    kind: TableKind::$kind,
                    groups: runtime.groups.clone(),
                    runtime: Arc::downgrade(runtime),
                })
            }
            pub fn get(&self, key: &str) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.0.groups.$select(key)
            }
            /// Nullable Java-compatible `ConcurrentMap.get` boundary.
            pub fn get_nullable(
                &self,
                key: Option<&str>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                self.get(key)
            }
            /// Nullable Java-compatible `ConcurrentMap.getOrDefault` boundary.
            pub fn get_or_default_nullable(
                &self,
                key: Option<&str>,
                default: Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                Ok(self.get(key)?.or(default))
            }
            /// Map.getOrDefault counterpart; the default is used only when the key is absent.
            pub fn get_or_default(
                &self,
                key: &str,
                default: Arc<dyn $owner>,
            ) -> Result<Arc<dyn $owner>, RuntimeError> {
                Ok(self.get(key)?.unwrap_or(default))
            }
            pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
                self.0.groups.table_contains_key(self.0.kind, key)
            }
            /// Nullable Java-compatible `ConcurrentMap.containsKey` boundary.
            pub fn contains_key_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                self.contains_key(key)
            }
            pub fn contains_value(
                &self,
                value: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                Ok(self
                    .snapshot()?
                    .values()
                    .any(|current| Self::owners_equal(value, current)))
            }
            /// Nullable Java-compatible `ConcurrentMap.containsValue` boundary.
            pub fn contains_value_nullable(
                &self,
                value: Option<&Arc<dyn $owner>>,
            ) -> Result<bool, RuntimeError> {
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                self.contains_value(value)
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Live handle for the backing map's key-set view.
            pub fn key_set_view(&self) -> $key_set {
                $key_set(self.0.clone())
            }
            /// Live handle for the backing map's values collection view.
            pub fn values_view(&self) -> $values_view {
                $values_view(self.0.clone())
            }
            /// Live handle for the backing map's entry-set view.
            pub fn entry_set_view(&self) -> $entry_set {
                $entry_set(self.0.clone())
            }
            /// Detached owned iteration copy; this handle itself remains live.
            pub fn snapshot(&self) -> Result<BTreeMap<String, Arc<dyn $owner>>, RuntimeError> {
                Ok(self
                    .0
                    .groups
                    .table_snapshot(self.0.kind)?
                    .into_iter()
                    .filter_map(|(key, value)| match value {
                        TableValue::$kind(owner) => Some((key, owner)),
                        _ => None,
                    })
                    .collect())
            }
            /// Detached key view corresponding to Java's keySet iteration;
            /// mutations require explicit table methods.
            pub fn keys(&self) -> Result<BTreeSet<String>, RuntimeError> {
                Ok(self.snapshot()?.into_keys().collect())
            }
            /// Detached value view corresponding to Java's values iteration.
            pub fn values(&self) -> Result<Vec<Arc<dyn $owner>>, RuntimeError> {
                Ok(self.snapshot()?.into_values().collect())
            }
            /// Visits a detached snapshot. Entry order is unspecified by this API.
            pub fn for_each(&self, mut action: impl FnMut(&str, &Arc<dyn $owner>)) -> Result<(), RuntimeError> {
                for (key, value) in self.snapshot()? {
                    action(&key, &value);
                }
                Ok(())
            }
            /// Nullable Java-compatible `ConcurrentHashMap.forEach` callback boundary.
            pub fn for_each_nullable(
                &self,
                action: Option<&mut TableForEach<'_, Arc<dyn $owner>>>,
            ) -> Result<(), RuntimeError> {
                let Some(action) = action else {
                    return Err(RuntimeError::InvalidRoute("null table forEach action"));
                };
                self.for_each(|key, value| action(key, value))
            }
            pub async fn put(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.write(TableMutation::Put {
                    key: key.into(),
                    value: TableValue::$kind(value),
                    only_absent: false,
                })
                .await
            }
            /// Nullable Java-compatible `ConcurrentMap.put` boundary.
            pub async fn put_nullable(
                &self,
                key: Option<&str>,
                value: Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                self.put(key, value).await
            }
            pub async fn put_if_absent(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.write(TableMutation::Put {
                    key: key.into(),
                    value: TableValue::$kind(value),
                    only_absent: true,
                })
                .await
            }
            /// Nullable Java-compatible `ConcurrentMap.putIfAbsent` boundary.
            pub async fn put_if_absent_nullable(
                &self,
                key: Option<&str>,
                value: Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                self.put_if_absent(key, value).await
            }
            /// Applies mappings in source iteration order, matching Map.putAll's
            /// independent per-entry writes.
            pub async fn put_all<I>(&self, entries: I) -> Result<(), RuntimeError>
            where
                I: IntoIterator<Item = (String, Arc<dyn $owner>)>,
            {
                for (key, value) in entries {
                    self.put(key, value).await?;
                }
                Ok(())
            }
            /// Nullable Java-compatible `Map.putAll` source-map boundary.
            pub async fn put_all_nullable(
                &self,
                entries: Option<&BTreeMap<String, Arc<dyn $owner>>>,
            ) -> Result<(), RuntimeError> {
                let Some(entries) = entries else {
                    return Err(RuntimeError::InvalidRoute("null table putAll map"));
                };
                for (key, value) in entries {
                    self.put(key, value.clone()).await?;
                }
                Ok(())
            }
            /// Applies ordered nullable source-map rows with Java `putAll`
            /// failure behavior; writes before an invalid row remain visible.
            pub async fn put_all_nullable_rows(
                &self,
                entries: Option<&[NullableTableEntry<dyn $owner>]>,
            ) -> Result<(), RuntimeError> {
                let Some(entries) = entries else {
                    return Err(RuntimeError::InvalidRoute("null table putAll map"));
                };
                self.put_all_nullable_iter(entries.iter().cloned()).await
            }
            /// Applies nullable entries from any source iterator in its
            /// traversal order, preserving Java `Map.putAll` partial writes.
            pub async fn put_all_nullable_iter<I>(
                &self,
                entries: I,
            ) -> Result<(), RuntimeError>
            where
                I: IntoIterator<Item = NullableTableEntry<dyn $owner>>,
            {
                for (key, value) in entries {
                    let Some(key) = key.as_deref() else {
                        return Err(RuntimeError::InvalidRoute($null_group));
                    };
                    let Some(value) = value else {
                        return Err(RuntimeError::InvalidRoute($null_value));
                    };
                    self.put(key, value.clone()).await?;
                }
                Ok(())
            }
            pub async fn replace(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let change = self
                    .0
                    .mutate(TableMutation::Replace {
                        key: key.into(),
                        value: TableValue::$kind(value),
                        expected: None,
                        comparison: None,
                    })
                    .await?;
                Ok(if change.matched {
                    Self::previous(change)
                } else {
                    None
                })
            }
            /// Nullable Java-compatible `ConcurrentMap.replace(key, value)` boundary.
            pub async fn replace_nullable(
                &self,
                key: Option<&str>,
                value: Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                self.replace(key, value).await
            }
            pub async fn replace_if_same(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
                value: Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                Ok(self
                    .0
                    .mutate(TableMutation::Replace {
                        key: key.into(),
                        value: TableValue::$kind(value),
                        expected: Some(TableValue::$kind(expected.clone())),
                        comparison: Some(ValueComparison::Identity),
                    })
                    .await?
                    .matched)
            }
            /// Java ConcurrentMap conditional replacement using owner value
            /// equality, with identity as the default owner equality.
            pub async fn replace_if_equal(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
                value: Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                let key = key.into();
                loop {
                    let Some(current) = self.get(&key)? else {
                        return Ok(false);
                    };
                    if !Self::owners_equal(expected, &current) {
                        return Ok(false);
                    }
                    if self.replace_if_same(&key, &current, value.clone()).await? {
                        return Ok(true);
                    }
                }
            }
            /// Nullable Java-compatible conditional replacement using owner value equality.
            pub async fn replace_if_equal_nullable(
                &self,
                key: Option<&str>,
                expected: Option<&Arc<dyn $owner>>,
                value: Option<Arc<dyn $owner>>,
            ) -> Result<bool, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(expected) = expected else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                self.replace_if_equal(key, expected, value).await
            }
            pub async fn remove(
                &self,
                key: impl Into<String>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.write(TableMutation::Remove {
                    key: key.into(),
                    expected: None,
                    comparison: None,
                })
                .await
            }
            /// Nullable Java-compatible `ConcurrentMap.remove(key)` boundary.
            pub async fn remove_nullable(
                &self,
                key: Option<&str>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                self.remove(key).await
            }
            pub async fn remove_if_same(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                Ok(self
                    .0
                    .mutate(TableMutation::Remove {
                        key: key.into(),
                        expected: Some(TableValue::$kind(expected.clone())),
                        comparison: Some(ValueComparison::Identity),
                    })
                    .await?
                    .matched)
            }
            /// Java ConcurrentMap conditional removal using owner value
            /// equality, with identity as the default owner equality.
            pub async fn remove_if_equal(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                let key = key.into();
                loop {
                    let Some(current) = self.get(&key)? else {
                        return Ok(false);
                    };
                    if !Self::owners_equal(expected, &current) {
                        return Ok(false);
                    }
                    if self.remove_if_same(&key, &current).await? {
                        return Ok(true);
                    }
                }
            }
            /// Nullable Java-compatible conditional removal using owner value equality.
            pub async fn remove_if_equal_nullable(
                &self,
                key: Option<&str>,
                expected: Option<&Arc<dyn $owner>>,
            ) -> Result<bool, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(expected) = expected else {
                    return Ok(false);
                };
                self.remove_if_equal(key, expected).await
            }
            /// ConcurrentMap-style remapping operations serialize same-key
            /// table mutations while the closure runs, so the closure is called
            /// once for this operation.
            pub async fn compute_if_absent(
                &self,
                key: impl Into<String>,
                mut mapping: impl FnMut(&str) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                let gate = self.0.key_gate(&key).await?;
                if let Some(current) = self.get(&key)? {
                    return Ok(Some(current));
                }
                let Some(value) = mapping(&key) else {
                    return Ok(None);
                };
                let change = self
                    .0
                    .mutate_with_gate(
                        TableMutation::Put {
                            key,
                            value: TableValue::$kind(value.clone()),
                            only_absent: true,
                        },
                        Some(gate),
                    )
                    .await?;
                Ok(Some(Self::previous(change).unwrap_or(value)))
            }
            /// Nullable Java-compatible `computeIfAbsent` key/function boundary.
            pub async fn compute_if_absent_nullable(
                &self,
                key: Option<&str>,
                mapping: Option<&mut TableComputeIfAbsent<'_, Arc<dyn $owner>>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(mapping) = mapping else {
                    return Err(RuntimeError::InvalidRoute("null table remapping function"));
                };
                self.compute_if_absent(key, |key| mapping(key)).await
            }
            pub async fn compute_if_present(
                &self,
                key: impl Into<String>,
                mut remapping: impl FnMut(&str, &Arc<dyn $owner>) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                let gate = self.0.key_gate(&key).await?;
                let Some(current) = self.get(&key)? else {
                    return Ok(None);
                };
                let (mutation, result) = match remapping(&key, &current) {
                    Some(value) => (
                        TableMutation::Replace {
                            key,
                            value: TableValue::$kind(value.clone()),
                            expected: Some(TableValue::$kind(current)),
                            comparison: Some(ValueComparison::Identity),
                        },
                        Some(value),
                    ),
                    None => (
                        TableMutation::Remove {
                            key,
                            expected: Some(TableValue::$kind(current)),
                            comparison: Some(ValueComparison::Identity),
                        },
                        None,
                    ),
                };
                self.0.mutate_with_gate(mutation, Some(gate)).await?;
                Ok(result)
            }
            /// Nullable Java-compatible `computeIfPresent` key/function boundary.
            pub async fn compute_if_present_nullable(
                &self,
                key: Option<&str>,
                remapping: Option<&mut TableComputeIfPresent<'_, Arc<dyn $owner>>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(remapping) = remapping else {
                    return Err(RuntimeError::InvalidRoute("null table remapping function"));
                };
                self.compute_if_present(key, |key, value| remapping(key, value))
                    .await
            }
            pub async fn compute(
                &self,
                key: impl Into<String>,
                mut remapping: impl FnMut(
                    &str,
                    Option<&Arc<dyn $owner>>,
                ) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                let gate = self.0.key_gate(&key).await?;
                let current = self.get(&key)?;
                let value = remapping(&key, current.as_ref());
                let (mutation, result) = match (current, value) {
                    (None, None) => return Ok(None),
                    (None, Some(value)) => (
                        TableMutation::Put {
                            key,
                            value: TableValue::$kind(value.clone()),
                            only_absent: true,
                        },
                        Some(value),
                    ),
                    (Some(current), Some(value)) => (
                        TableMutation::Replace {
                            key,
                            value: TableValue::$kind(value.clone()),
                            expected: Some(TableValue::$kind(current)),
                            comparison: Some(ValueComparison::Identity),
                        },
                        Some(value),
                    ),
                    (Some(current), None) => (
                        TableMutation::Remove {
                            key,
                            expected: Some(TableValue::$kind(current)),
                            comparison: Some(ValueComparison::Identity),
                        },
                        None,
                    ),
                };
                self.0.mutate_with_gate(mutation, Some(gate)).await?;
                Ok(result)
            }
            /// Nullable Java-compatible `compute` key/function boundary.
            pub async fn compute_nullable(
                &self,
                key: Option<&str>,
                remapping: Option<&mut TableCompute<'_, Arc<dyn $owner>>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(remapping) = remapping else {
                    return Err(RuntimeError::InvalidRoute("null table remapping function"));
                };
                self.compute(key, |key, current| remapping(key, current))
                    .await
            }
            pub async fn merge(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
                mut remapping: impl FnMut(
                    &Arc<dyn $owner>,
                    &Arc<dyn $owner>,
                ) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                let gate = self.0.key_gate(&key).await?;
                let current = self.get(&key)?;
                let (mutation, result) = match current {
                    None => (
                        TableMutation::Put {
                            key,
                            value: TableValue::$kind(value.clone()),
                            only_absent: true,
                        },
                        Some(value.clone()),
                    ),
                    Some(current) => match remapping(&current, &value) {
                        Some(next) => (
                            TableMutation::Replace {
                                key,
                                value: TableValue::$kind(next.clone()),
                                expected: Some(TableValue::$kind(current)),
                                comparison: Some(ValueComparison::Identity),
                            },
                            Some(next),
                        ),
                        None => (
                            TableMutation::Remove {
                                key,
                                expected: Some(TableValue::$kind(current)),
                                comparison: Some(ValueComparison::Identity),
                            },
                            None,
                        ),
                    },
                };
                self.0.mutate_with_gate(mutation, Some(gate)).await?;
                Ok(result)
            }
            /// Nullable Java-compatible `merge` key/value/function boundary.
            pub async fn merge_nullable(
                &self,
                key: Option<&str>,
                value: Option<Arc<dyn $owner>>,
                remapping: Option<&mut TableMerge<'_, Arc<dyn $owner>>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                let Some(remapping) = remapping else {
                    return Err(RuntimeError::InvalidRoute("null table remapping function"));
                };
                self.merge(key, value, |current, next| remapping(current, next))
                    .await
            }
            /// Visits the keys in a detached snapshot and applies the
            /// ConcurrentHashMap.replaceAll callback contract. The callback
            /// runs outside the key gate and may be retried if a concurrent
            /// write changes the mapping before the conditional update.
            pub async fn replace_all(
                &self,
                mut remapping: impl FnMut(&str, &Arc<dyn $owner>) -> Arc<dyn $owner>,
            ) -> Result<(), RuntimeError> {
                for key in self.keys()? {
                    loop {
                        let Some(current) = self.get(&key)? else {
                            break;
                        };
                        // ConcurrentHashMap.replaceAll invokes the function
                        // outside its bin lock and retries after a lost CAS.
                        let value = remapping(&key, &current);
                        let gate = self.0.key_gate(&key).await?;
                        let Some(latest) = self.get(&key)? else {
                            break;
                        };
                        // ConcurrentHashMap's conditional replace compares
                        // the old value with equals(), not reference identity.
                        // Read the equal current value under the per-key gate,
                        // then commit against that stable mapping by identity.
                        if !Self::owners_equal(&current, &latest) {
                            continue;
                        }
                        let change = self
                            .0
                            .mutate_with_gate(
                                TableMutation::Replace {
                                    key: key.clone(),
                                    value: TableValue::$kind(value),
                                    expected: Some(TableValue::$kind(latest)),
                                    comparison: Some(ValueComparison::Identity),
                                },
                                Some(gate),
                            )
                            .await?;
                        if change.matched {
                            break;
                        }
                    }
                }
                Ok(())
            }
            /// Nullable Java-compatible `ConcurrentHashMap.replaceAll` callback boundary.
            pub async fn replace_all_nullable(
                &self,
                remapping: Option<&mut TableReplaceAll<'_, Arc<dyn $owner>>>,
            ) -> Result<(), RuntimeError> {
                let Some(remapping) = remapping else {
                    return Err(RuntimeError::InvalidRoute("null table replaceAll function"));
                };
                self.replace_all(|key, value| remapping(key, value)).await
            }
            /// Applies `replaceAll` callbacks with nullable Java results. A
            /// null result fails at that key while prior updates remain.
            pub async fn replace_all_with_nullable_results(
                &self,
                mut remapping: Option<&mut TableReplaceAllOptionalResult<'_, Arc<dyn $owner>>>,
            ) -> Result<(), RuntimeError> {
                let Some(remapping) = remapping.as_mut() else {
                    return Err(RuntimeError::InvalidRoute("null table replaceAll function"));
                };
                for key in self.keys()? {
                    loop {
                        let Some(current) = self.get(&key)? else {
                            break;
                        };
                        let Some(value) = remapping(&key, &current) else {
                            return Err(RuntimeError::InvalidRoute(
                                "null table replaceAll result",
                            ));
                        };
                        let gate = self.0.key_gate(&key).await?;
                        let Some(latest) = self.get(&key)? else {
                            break;
                        };
                        if !Self::owners_equal(&current, &latest) {
                            continue;
                        }
                        let change = self
                            .0
                            .mutate_with_gate(
                                TableMutation::Replace {
                                    key: key.clone(),
                                    value: TableValue::$kind(value),
                                    expected: Some(TableValue::$kind(latest)),
                                    comparison: Some(ValueComparison::Identity),
                                },
                                Some(gate),
                            )
                            .await?;
                        if change.matched {
                            break;
                        }
                    }
                }
                Ok(())
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
            async fn write(
                &self,
                mutation: TableMutation,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                Ok(Self::previous(self.0.mutate(mutation).await?))
            }
            fn previous(change: TableChange) -> Option<Arc<dyn $owner>> {
                change.previous.and_then(|value| match value {
                    TableValue::$kind(owner) => Some(owner),
                    _ => None,
                })
            }
        }
    };
}
macro_rules! table_views {
    ($table:ident, $owner:ident, $kind:ident, $null_group:literal, $null_value:literal, $key_set:ident, $values_view:ident, $entry_set:ident, $entry_handle:ident) => {
        #[derive(Clone)]
        pub struct $key_set(TableHandle);
        impl $key_set {
            pub fn contains(&self, key: &str) -> Result<bool, RuntimeError> {
                self.0.groups.table_contains_key(self.0.kind, key)
            }
            /// Nullable Java-compatible key-set membership boundary.
            pub fn contains_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                self.contains(key)
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Detached iteration copy from this live view.
            pub fn snapshot(&self) -> Result<BTreeSet<String>, RuntimeError> {
                Ok(self
                    .0
                    .groups
                    .table_snapshot(self.0.kind)?
                    .into_keys()
                    .collect())
            }
            pub async fn remove(&self, key: impl Into<String>) -> Result<bool, RuntimeError> {
                self.0
                    .mutate(TableMutation::Remove {
                        key: key.into(),
                        expected: None,
                        comparison: None,
                    })
                    .await
                    .map(|change| change.matched)
            }
            /// Nullable Java-compatible key-set removal boundary.
            pub async fn remove_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
                let Some(key) = key else {
                    return Err(RuntimeError::InvalidRoute($null_group));
                };
                self.remove(key).await
            }
            /// Removes keys accepted by the predicate, visiting a detached key copy.
            pub async fn remove_if(
                &self,
                mut predicate: impl FnMut(&str) -> bool,
            ) -> Result<usize, RuntimeError> {
                let mut removed = 0;
                for key in self.snapshot()? {
                    if predicate(&key) && self.remove(key).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            /// Nullable Java-compatible `removeIf` predicate boundary.
            pub async fn remove_if_nullable(
                &self,
                predicate: Option<&mut TableKeyViewPredicate<'_>>,
            ) -> Result<usize, RuntimeError> {
                let Some(predicate) = predicate else {
                    return Err(RuntimeError::InvalidRoute("null table view predicate"));
                };
                self.remove_if(|key| predicate(key)).await
            }
            /// Removes keys rejected by the predicate, visiting a detached key copy.
            pub async fn retain(
                &self,
                mut predicate: impl FnMut(&str) -> bool,
            ) -> Result<usize, RuntimeError> {
                let mut removed = 0;
                for key in self.snapshot()? {
                    if !predicate(&key) && self.remove(key).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn remove_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
                self.remove_if(|key| keys.contains(key)).await
            }
            /// Nullable Java-compatible collection argument boundary.
            pub async fn remove_all_nullable(
                &self,
                keys: Option<&BTreeSet<String>>,
            ) -> Result<usize, RuntimeError> {
                let Some(keys) = keys else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                self.remove_all(keys).await
            }
            /// Java key-set bulk removal processes collection entries in order;
            /// a null key fails at its position and earlier removals remain.
            pub async fn remove_all_with_nullable_elements(
                &self,
                keys: Option<&[Option<String>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(keys) = keys else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                let mut removed = 0;
                for key in keys {
                    if self.remove_nullable(key.as_deref()).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn retain_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
                self.retain(|key| keys.contains(key)).await
            }
            /// Nullable Java-compatible collection argument boundary.
            pub async fn retain_all_nullable(
                &self,
                keys: Option<&BTreeSet<String>>,
            ) -> Result<usize, RuntimeError> {
                let Some(keys) = keys else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                self.retain_all(keys).await
            }
            /// Null collection elements do not match keys during Java `retainAll`.
            pub async fn retain_all_with_nullable_elements(
                &self,
                keys: Option<&[Option<String>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(keys) = keys else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                self.retain(|key| keys.iter().flatten().any(|candidate| candidate == key))
                    .await
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
        }

        #[derive(Clone)]
        pub struct $values_view(TableHandle);
        impl $values_view {
            fn table(&self) -> $table {
                $table(self.0.clone())
            }
            pub fn contains(&self, value: &Arc<dyn $owner>) -> Result<bool, RuntimeError> {
                self.table().contains_value(value)
            }
            /// Nullable Java-compatible values-view membership boundary.
            pub fn contains_nullable(
                &self,
                value: Option<&Arc<dyn $owner>>,
            ) -> Result<bool, RuntimeError> {
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                self.contains(value)
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Detached iteration copy from this live view.
            pub fn snapshot(&self) -> Result<Vec<Arc<dyn $owner>>, RuntimeError> {
                self.table().values()
            }
            pub async fn remove(&self, value: &Arc<dyn $owner>) -> Result<bool, RuntimeError> {
                for key in self.table().keys()? {
                    if self.table().remove_if_equal(key, value).await? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            /// Java `ConcurrentHashMap.values().remove(null)` returns false.
            pub async fn remove_nullable(
                &self,
                value: Option<&Arc<dyn $owner>>,
            ) -> Result<bool, RuntimeError> {
                let Some(value) = value else {
                    return Ok(false);
                };
                self.remove(value).await
            }
            /// Removes values accepted by the predicate from a detached entry copy.
            pub async fn remove_if(
                &self,
                mut predicate: impl FnMut(&Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if predicate(&value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            /// Nullable Java-compatible `removeIf` predicate boundary.
            pub async fn remove_if_nullable(
                &self,
                predicate: Option<&mut TableValueViewPredicate<'_, Arc<dyn $owner>>>,
            ) -> Result<usize, RuntimeError> {
                let Some(predicate) = predicate else {
                    return Err(RuntimeError::InvalidRoute("null table view predicate"));
                };
                self.remove_if(|value| predicate(value)).await
            }
            /// Removes values rejected by the predicate from a detached entry copy.
            pub async fn retain(
                &self,
                mut predicate: impl FnMut(&Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if !predicate(&value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn remove_all(
                &self,
                values: &[Arc<dyn $owner>],
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(values, false).await
            }
            /// Nullable Java-compatible collection argument boundary.
            pub async fn remove_all_nullable(
                &self,
                values: Option<&[Arc<dyn $owner>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(values) = values else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                self.remove_all(values).await
            }
            /// Null candidates are ignored by the Java values view.
            pub async fn remove_all_with_nullable_elements(
                &self,
                values: Option<&[Option<Arc<dyn $owner>>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(values) = values else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                let mut removed = 0;
                for value in values.iter().flatten() {
                    if self.remove(value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn retain_all(
                &self,
                values: &[Arc<dyn $owner>],
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(values, true).await
            }
            /// Nullable Java-compatible collection argument boundary.
            pub async fn retain_all_nullable(
                &self,
                values: Option<&[Arc<dyn $owner>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(values) = values else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                self.retain_all(values).await
            }
            /// Null candidates do not match a non-null ConcurrentHashMap value.
            pub async fn retain_all_with_nullable_elements(
                &self,
                values: Option<&[Option<Arc<dyn $owner>>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(values) = values else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                let candidates: Vec<_> = values.iter().flatten().cloned().collect();
                self.retain_all(&candidates).await
            }
            async fn mutate_membership(
                &self,
                values: &[Arc<dyn $owner>],
                retain_matches: bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let entry_set = table.entry_set_view();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    let mut matches = false;
                    for candidate in values {
                        if entry_set.contains(&key, candidate)? {
                            matches = true;
                            break;
                        }
                    }
                    if matches != retain_matches && table.remove_if_equal(&key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
        }

        /// Detached `Map.Entry`-like snapshot whose value can be replaced in
        /// both the entry and its backing live table.
        #[derive(Clone)]
        pub struct $entry_handle {
            table: $table,
            key: String,
            value: Arc<RwLock<Arc<dyn $owner>>>,
            update_lock: Arc<AsyncMutex<()>>,
        }
        impl $entry_handle {
            pub fn key(&self) -> &str {
                &self.key
            }
            pub fn value(&self) -> Result<Arc<dyn $owner>, RuntimeError> {
                self.value
                    .read()
                    .map(|value| value.clone())
                    .map_err(|_| RuntimeError::RegistryPoisoned)
            }
            pub async fn set_value(
                &self,
                value: Arc<dyn $owner>,
            ) -> Result<Arc<dyn $owner>, RuntimeError> {
                let _update = self.update_lock.lock().await;
                let previous = self.value()?;
                self.table.put(self.key.clone(), value.clone()).await?;
                *self
                    .value
                    .write()
                    .map_err(|_| RuntimeError::RegistryPoisoned)? = value;
                Ok(previous)
            }
            /// Nullable Java-compatible `Map.Entry.setValue` boundary.
            pub async fn set_value_nullable(
                &self,
                value: Option<Arc<dyn $owner>>,
            ) -> Result<Arc<dyn $owner>, RuntimeError> {
                let Some(value) = value else {
                    return Err(RuntimeError::InvalidRoute($null_value));
                };
                self.set_value(value).await
            }
        }

        #[derive(Clone)]
        pub struct $entry_set(TableHandle);
        impl $entry_set {
            fn table(&self) -> $table {
                $table(self.0.clone())
            }
            pub fn contains(
                &self,
                key: &str,
                value: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                Ok(self
                    .table()
                    .get(key)?
                    .is_some_and(|current| $table::owners_equal(value, &current)))
            }
            /// A null Java Map.Entry is not a member of this entry-set view.
            pub fn contains_nullable(
                &self,
                entry: Option<(&str, &Arc<dyn $owner>)>,
            ) -> Result<bool, RuntimeError> {
                let Some((key, value)) = entry else {
                    return Ok(false);
                };
                self.contains(key, value)
            }
            /// Null keys or values inside a Map.Entry do not match a CHM entry.
            pub fn contains_entry_nullable(
                &self,
                key: Option<&str>,
                value: Option<&Arc<dyn $owner>>,
            ) -> Result<bool, RuntimeError> {
                match (key, value) {
                    (Some(key), Some(value)) => self.contains(key, value),
                    _ => Ok(false),
                }
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Detached iteration copy from this live view.
            pub fn snapshot(&self) -> Result<BTreeMap<String, Arc<dyn $owner>>, RuntimeError> {
                self.table().snapshot()
            }
            /// Returns detached entry snapshots in deterministic key order.
            pub fn entry_snapshots(&self) -> Result<Vec<$entry_handle>, RuntimeError> {
                let table = self.table();
                Ok(table
                    .snapshot()?
                    .into_iter()
                    .map(|(key, value)| $entry_handle {
                        table: table.clone(),
                        key,
                        value: Arc::new(RwLock::new(value)),
                        update_lock: Arc::new(AsyncMutex::new(())),
                    })
                    .collect())
            }
            pub async fn remove(
                &self,
                key: impl Into<String>,
                value: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                self.table().remove_if_equal(key, value).await
            }
            /// A null Java Map.Entry is not removed from this entry-set view.
            pub async fn remove_nullable(
                &self,
                entry: Option<(&str, &Arc<dyn $owner>)>,
            ) -> Result<bool, RuntimeError> {
                let Some((key, value)) = entry else {
                    return Ok(false);
                };
                self.remove(key, value).await
            }
            /// Null keys or values inside a Map.Entry do not remove a CHM entry.
            pub async fn remove_entry_nullable(
                &self,
                key: Option<&str>,
                value: Option<&Arc<dyn $owner>>,
            ) -> Result<bool, RuntimeError> {
                match (key, value) {
                    (Some(key), Some(value)) => self.remove(key, value).await,
                    _ => Ok(false),
                }
            }
            /// Removes entries accepted by the predicate from a detached entry copy.
            pub async fn remove_if(
                &self,
                mut predicate: impl FnMut(&str, &Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if predicate(&key, &value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            /// Nullable Java-compatible `removeIf` predicate boundary.
            pub async fn remove_if_nullable(
                &self,
                predicate: Option<&mut TableEntryViewPredicate<'_, Arc<dyn $owner>>>,
            ) -> Result<usize, RuntimeError> {
                let Some(predicate) = predicate else {
                    return Err(RuntimeError::InvalidRoute("null table view predicate"));
                };
                self.remove_if(|key, value| predicate(key, value)).await
            }
            /// Removes entries rejected by the predicate from a detached entry copy.
            pub async fn retain(
                &self,
                mut predicate: impl FnMut(&str, &Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if !predicate(&key, &value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn remove_all(
                &self,
                entries: &BTreeMap<String, Arc<dyn $owner>>,
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(entries, false).await
            }
            /// Nullable Java-compatible collection argument boundary.
            pub async fn remove_all_nullable(
                &self,
                entries: Option<&BTreeMap<String, Arc<dyn $owner>>>,
            ) -> Result<usize, RuntimeError> {
                let Some(entries) = entries else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                self.remove_all(entries).await
            }
            /// Null entries and entries with null fields do not match a CHM entry.
            pub async fn remove_all_with_nullable_elements(
                &self,
                entries: Option<&[Option<NullableTableEntry<dyn $owner>>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(entries) = entries else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                let mut removed = 0;
                for entry in entries.iter().flatten() {
                    if let (Some(key), Some(value)) = entry {
                        if self.remove(key, value).await? {
                            removed += 1;
                        }
                    }
                }
                Ok(removed)
            }
            pub async fn retain_all(
                &self,
                entries: &BTreeMap<String, Arc<dyn $owner>>,
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(entries, true).await
            }
            /// Nullable Java-compatible collection argument boundary.
            pub async fn retain_all_nullable(
                &self,
                entries: Option<&BTreeMap<String, Arc<dyn $owner>>>,
            ) -> Result<usize, RuntimeError> {
                let Some(entries) = entries else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                self.retain_all(entries).await
            }
            /// Null entries and entries with null fields do not match a CHM entry.
            pub async fn retain_all_with_nullable_elements(
                &self,
                entries: Option<&[Option<NullableTableEntry<dyn $owner>>]>,
            ) -> Result<usize, RuntimeError> {
                let Some(entries) = entries else {
                    return Err(RuntimeError::InvalidRoute("null table view collection"));
                };
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    let matches = entries.iter().flatten().any(|entry| match entry {
                        (Some(candidate_key), Some(candidate_value)) => {
                            candidate_key == &key && $table::owners_equal(candidate_value, &value)
                        }
                        _ => false,
                    });
                    if !matches && table.remove_if_equal(&key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            async fn mutate_membership(
                &self,
                candidates: &BTreeMap<String, Arc<dyn $owner>>,
                retain_matches: bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    let matches = match candidates.get(&key) {
                        Some(candidate) => self.contains(&key, candidate)?,
                        None => false,
                    };
                    if matches != retain_matches && table.remove_if_equal(&key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
        }
    };
}
table!(
    ProducerTable,
    ProducerRuntime,
    Producer,
    select_producer,
    "null producer group",
    "null producer value",
    ProducerKeySetView,
    ProducerValuesView,
    ProducerEntrySetView
);
table!(
    ConsumerTable,
    ConsumerRuntime,
    Consumer,
    select_consumer,
    "null consumer group",
    "null consumer value",
    ConsumerKeySetView,
    ConsumerValuesView,
    ConsumerEntrySetView
);
table_views!(
    ProducerTable,
    ProducerRuntime,
    Producer,
    "null producer group",
    "null producer value",
    ProducerKeySetView,
    ProducerValuesView,
    ProducerEntrySetView,
    ProducerEntryHandle
);
table_views!(
    ConsumerTable,
    ConsumerRuntime,
    Consumer,
    "null consumer group",
    "null consumer value",
    ConsumerKeySetView,
    ConsumerValuesView,
    ConsumerEntrySetView,
    ConsumerEntryHandle
);

#[cfg(test)]
mod tests {
    use super::super::{ClientRuntimeManager, registry::ConsumerRuntime};
    use super::*;
    use crate::config::ClientConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn startup_addressing_capability_can_reenter_live_table_without_waiting_on_itself() {
        use std::{future::Future, pin::Pin, sync::Mutex};
        struct Producer;
        impl ProducerRuntime for Producer {}
        struct Addressing {
            table: Mutex<Option<ProducerTable>>,
            owner: Arc<dyn ProducerRuntime>,
        }
        impl crate::namesrv::TopAddressing for Addressing {
            fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
                Box::pin(async move {
                    let table = self.table.lock().unwrap().clone().unwrap();
                    table
                        .put("from-addressing", self.owner.clone())
                        .await
                        .unwrap();
                    Some("127.0.0.1:1".into()) // Only an address result; no peer is contacted.
                })
            }
        }
        let provider = Arc::new(Addressing {
            table: Mutex::new(None),
            owner: Arc::new(Producer),
        });
        let manager = ClientRuntimeManager::with_top_addressing(provider.clone());
        let mut config = ClientConfig::new("table-addressing-reentry");
        config.namesrv_addr = None;
        let runtime = manager.get_or_create(&config).await.unwrap();
        let table = runtime.producer_table();
        *provider.table.lock().unwrap() = Some(table.clone());
        tokio::time::timeout(tokio::time::Duration::from_secs(5), runtime.start())
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(
            &table.get("from-addressing").unwrap().unwrap(),
            &provider.owner
        ));
        table.clear().await.unwrap();
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn raw_maps_create_no_worker_put_if_absent_is_atomic_and_owner_drop_reenters() {
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&ClientConfig::new("table-atomic"))
            .await
            .unwrap();
        let table = runtime.producer_table();
        struct Producer;
        impl ProducerRuntime for Producer {}
        let owner: Arc<dyn ProducerRuntime> = Arc::new(Producer);
        let mut calls = Vec::new();
        for _ in 0..64 {
            let table = table.clone();
            let owner = owner.clone();
            calls.push(tokio::spawn(async move {
                table.put_if_absent("group", owner).await.unwrap().is_none()
            }));
        }
        let mut installed = 0;
        for call in calls {
            installed += usize::from(call.await.unwrap());
        }
        assert_eq!(installed, 1);
        let detached = table.snapshot().unwrap();
        table.clear().await.unwrap();
        assert_eq!(detached.len(), 1);
        assert!(table.is_empty().unwrap());
        assert!(runtime.control.existing_sender().unwrap().is_none());
        struct ReentrantOwner {
            table: ConsumerTable,
            drops: Arc<AtomicUsize>,
        }
        impl ConsumerRuntime for ReentrantOwner {
            fn heartbeat_data(&self) -> rocketmq_classic_protocol::heartbeat::ConsumerData {
                Default::default()
            }
        }
        impl Drop for ReentrantOwner {
            fn drop(&mut self) {
                assert!(self.table.get("drop").unwrap().is_none());
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let consumer = runtime.consumer_table();
        let drops = Arc::new(AtomicUsize::new(0));
        consumer
            .put(
                "drop",
                Arc::new(ReentrantOwner {
                    table: consumer.clone(),
                    drops: drops.clone(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(consumer.contains_key("drop").unwrap());
        consumer.clear().await.unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!consumer.contains_key("drop").unwrap());
        assert!(runtime.control.existing_sender().unwrap().is_none());
    }
}
