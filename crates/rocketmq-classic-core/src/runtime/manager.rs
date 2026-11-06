use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, Weak},
};
use tokio::sync::Mutex as AsyncMutex;

use rocketmq_classic_remoting::client::{RpcHook, TlsConfig};

use super::{
    ClientRuntime, NullableTableEntry, RuntimeError, TableCompute, TableComputeIfAbsent,
    TableComputeIfPresent, TableEntryViewPredicate, TableForEach, TableKeyViewPredicate,
    TableMerge, TableReplaceAll, TableReplaceAllOptionalResult, TableValueViewPredicate,
};
use crate::config::ClientConfig;

type Entries = HashMap<String, Arc<Entry>>;

struct Entry {
    config: ClientConfig,
    properties: HashMap<String, String>,
    hook: Option<Arc<dyn RpcHook>>,
    tls_config: Option<TlsConfig>,
    initialization: AsyncMutex<()>,
    runtime: OnceLock<Arc<ClientRuntime>>,
}

struct KeyMutationGate {
    asynchronous: Arc<AsyncMutex<()>>,
    synchronous: SynchronousMutationGate,
}

#[derive(Default)]
struct SynchronousMutationGate {
    state: Mutex<SynchronousMutationState>,
    available: Condvar,
}

#[derive(Default)]
struct SynchronousMutationState {
    owner: Option<std::thread::ThreadId>,
    depth: usize,
}

impl SynchronousMutationGate {
    fn lock(&self, gate: Arc<KeyMutationGate>) -> SynchronousMutationGuard {
        let owner = std::thread::current().id();
        let mut state = self.lock_state();
        loop {
            match state.owner {
                Some(current) if current == owner => {
                    state.depth += 1;
                    break;
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
                    state.depth = 1;
                    break;
                }
            }
        }
        drop(state);
        SynchronousMutationGuard { gate, owner }
    }

    fn lock_state(&self) -> MutexGuard<'_, SynchronousMutationState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                self.state.clear_poison();
                poisoned.into_inner()
            }
        }
    }
}

struct SynchronousMutationGuard {
    gate: Arc<KeyMutationGate>,
    owner: std::thread::ThreadId,
}

impl Drop for SynchronousMutationGuard {
    fn drop(&mut self) {
        let mut state = self.gate.synchronous.lock_state();
        debug_assert_eq!(state.owner, Some(self.owner));
        state.depth -= 1;
        if state.depth == 0 {
            state.owner = None;
            self.gate.synchronous.available.notify_one();
        }
    }
}

impl Default for KeyMutationGate {
    fn default() -> Self {
        Self {
            asynchronous: Arc::new(AsyncMutex::new(())),
            synchronous: SynchronousMutationGate::default(),
        }
    }
}

struct KeyMutationGuard {
    gate: Arc<KeyMutationGate>,
    _asynchronous: tokio::sync::OwnedMutexGuard<()>,
}

impl KeyMutationGuard {
    fn lock_synchronous(&self) -> SynchronousMutationGuard {
        self.gate.synchronous.lock(self.gate.clone())
    }
}

#[derive(Default)]
struct KeyMutationLocks {
    gates: Mutex<HashMap<String, Weak<KeyMutationGate>>>,
}

impl KeyMutationLocks {
    fn gate(&self, key: &str) -> Result<Arc<KeyMutationGate>, RuntimeError> {
        let mut gates = self
            .gates
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if gates.len() >= 256 {
            gates.retain(|_, gate| gate.strong_count() > 0);
        }
        match gates.get(key).and_then(Weak::upgrade) {
            Some(gate) => Ok(gate),
            None => {
                let gate = Arc::new(KeyMutationGate::default());
                gates.insert(key.to_owned(), Arc::downgrade(&gate));
                Ok(gate)
            }
        }
    }

    async fn lock(&self, key: &str) -> Result<KeyMutationGuard, RuntimeError> {
        let gate = self.gate(key)?;
        let asynchronous = gate.asynchronous.clone().lock_owned().await;
        Ok(KeyMutationGuard {
            gate,
            _asynchronous: asynchronous,
        })
    }

    fn lock_synchronous(&self, key: &str) -> Result<SynchronousMutationGuard, RuntimeError> {
        let gate = self.gate(key)?;
        Ok(gate.synchronous.lock(gate.clone()))
    }
}

/// Explicit sharing boundary corresponding to MQClientManager's factory table.
/// Keys are installed before any resource construction. Each key initializes
/// independently within its mutation shard; a failed entry may be retried by a
/// subsequent lookup.
#[derive(Clone, Default)]
pub struct ClientRuntimeManager {
    entries: Arc<Mutex<Entries>>,
    mutation_locks: Arc<KeyMutationLocks>,
    cache_token: Arc<()>,
    top_addressing: Option<Arc<dyn crate::namesrv::TopAddressing>>,
}

#[derive(Clone)]
pub(super) struct ManagerLink {
    entries: Weak<Mutex<Entries>>,
    mutation_locks: Weak<KeyMutationLocks>,
}

impl ManagerLink {
    pub fn same_table(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.entries, &other.entries)
    }
    pub fn remove(&self, runtime: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        let Some(entries) = self.entries.upgrade() else {
            return Ok(false);
        };
        let Some(mutation_locks) = self.mutation_locks.upgrade() else {
            return Ok(false);
        };
        ClientRuntimeManager {
            entries,
            mutation_locks,
            cache_token: Arc::new(()),
            top_addressing: None,
        }
        .remove_client_factory(runtime.client_id(), runtime)
    }
}

impl ClientRuntimeManager {
    /// Shared default manager used by the public Producer and Consumer facades.
    #[doc(hidden)]
    pub fn shared_default() -> Self {
        static MANAGER: OnceLock<ClientRuntimeManager> = OnceLock::new();
        MANAGER.get_or_init(Self::new).clone()
    }

    /// A retained live view, corresponding to MQClientManager.getFactoryTable.
    /// Reads observe published runtimes; mutations wait for their key's shard.
    pub fn factory_table(&self) -> FactoryTable {
        FactoryTable {
            manager: Self {
                entries: self.entries.clone(),
                mutation_locks: self.mutation_locks.clone(),
                cache_token: self.cache_token.clone(),
                top_addressing: None,
            },
        }
    }
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the retained identity token for internal manager-scoped caches.
    #[doc(hidden)]
    pub fn cache_identity_token(&self) -> Arc<()> {
        self.cache_token.clone()
    }

    pub fn with_top_addressing(provider: Arc<dyn crate::namesrv::TopAddressing>) -> Self {
        Self {
            entries: Arc::default(),
            mutation_locks: Arc::default(),
            cache_token: Arc::new(()),
            top_addressing: Some(provider),
        }
    }

    pub async fn get_or_create(
        &self,
        config: &ClientConfig,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        self.get_or_create_with_hook(config, None).await
    }

    pub async fn get_or_create_with_hook(
        &self,
        config: &ClientConfig,
        hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        self.get_or_create_with_hook_and_tls_config(config, hook, None)
            .await
    }

    /// Creates a shared runtime with an explicit Rust TLS configuration.
    /// The first configuration used for a Java-compatible client identity owns
    /// that shared runtime, including its transport settings.
    #[doc(hidden)]
    pub async fn get_or_create_with_hook_and_tls_config(
        &self,
        config: &ClientConfig,
        hook: Option<Arc<dyn RpcHook>>,
        tls_config: Option<TlsConfig>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        let provider = self.top_addressing.clone();
        self.get_or_create_inner_with_tls_config(config, hook, tls_config, move |entry| {
            let provider = provider.clone();
            async move {
                ClientRuntime::construct_with_top_addressing_and_tls_config(
                    entry.config.clone_client_config(),
                    &entry.properties,
                    entry.hook.clone(),
                    provider,
                    entry.tls_config.clone(),
                )
            }
        })
        .await
    }

    /// Nullable Java-compatible `MQClientManager.getOrCreateMQClientInstance`
    /// configuration boundary.
    pub async fn get_or_create_nullable(
        &self,
        config: Option<&ClientConfig>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        self.get_or_create_with_hook_nullable(config, None).await
    }

    /// Nullable Java-compatible configuration boundary for the hook overload.
    pub async fn get_or_create_with_hook_nullable(
        &self,
        config: Option<&ClientConfig>,
        hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        let Some(config) = config else {
            return Err(RuntimeError::InvalidRoute("null client config"));
        };
        self.get_or_create_with_hook(config, hook).await
    }

    #[cfg(test)]
    async fn get_or_create_inner<F, Fut>(
        &self,
        config: &ClientConfig,
        hook: Option<Arc<dyn RpcHook>>,
        construct: F,
    ) -> Result<Arc<ClientRuntime>, RuntimeError>
    where
        F: Fn(Arc<Entry>) -> Fut,
        Fut: Future<Output = Result<ClientRuntime, RuntimeError>>,
    {
        self.get_or_create_inner_with_tls_config(config, hook, None, construct)
            .await
    }

    async fn get_or_create_inner_with_tls_config<F, Fut>(
        &self,
        config: &ClientConfig,
        hook: Option<Arc<dyn RpcHook>>,
        tls_config: Option<TlsConfig>,
        construct: F,
    ) -> Result<Arc<ClientRuntime>, RuntimeError>
    where
        F: Fn(Arc<Entry>) -> Fut,
        Fut: Future<Output = Result<ClientRuntime, RuntimeError>>,
    {
        let client_id = config.build_client_id();
        let _key_mutation = self.mutation_locks.lock(&client_id).await?;
        loop {
            let entry = {
                let mut entries = self
                    .entries
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?;
                let entry = entries.entry(client_id.clone()).or_insert_with(|| {
                    let properties: HashMap<_, _> = std::env::vars()
                        .filter(|(key, _)| {
                            key.starts_with("tls.")
                                || key == "rocketmq.namesrv.domain"
                                || key == "rocketmq.namesrv.domain.subgroup"
                                || key == super::rebalance::WAIT_PROPERTY
                                || key == super::rebalance::MIN_PROPERTY
                        })
                        .collect();
                    Arc::new(Entry {
                        config: config.clone(),
                        properties,
                        hook: hook.clone(),
                        tls_config: tls_config.clone(),
                        initialization: AsyncMutex::new(()),
                        runtime: OnceLock::new(),
                    })
                });
                entry.clone()
            };
            let _initialization = entry.initialization.lock().await;
            // A waiter on a failed/removed entry must not initialize an orphan
            // after another lookup installed a replacement for the same ID.
            let current = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?
                .get(&client_id)
                .is_some_and(|value| Arc::ptr_eq(value, &entry));
            if !current {
                continue;
            }
            if let Some(runtime) = entry.runtime.get() {
                return Ok(runtime.clone());
            }
            match construct(entry.clone()).await {
                Ok(created) => {
                    let created = Arc::new(created);
                    let _synchronous_mutation = _key_mutation.lock_synchronous();
                    created.attach_manager(ManagerLink {
                        entries: Arc::downgrade(&self.entries),
                        mutation_locks: Arc::downgrade(&self.mutation_locks),
                    })?;
                    // The key lock guarantees a sole publisher. Removal reads
                    // this immutable cell without contending with lookup locks.
                    let _ = entry.runtime.set(created.clone());
                    return Ok(created);
                }
                Err(error) => {
                    self.remove_entry(&client_id, &entry)?;
                    return Err(error);
                }
            }
        }
    }

    fn remove_entry(&self, client_id: &str, expected: &Arc<Entry>) -> Result<bool, RuntimeError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if entries
            .get(client_id)
            .is_some_and(|value| Arc::ptr_eq(value, expected))
        {
            entries.remove(client_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Identity-aware cleanup, corresponding to removeClientFactory(id, instance).
    pub fn remove_client_factory(
        &self,
        client_id: &str,
        expected: &Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        let _synchronous_mutation = self.mutation_locks.lock_synchronous(client_id)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let matches = entries.get(client_id).is_some_and(|entry| {
            entry
                .runtime
                .get()
                .is_some_and(|value| Arc::ptr_eq(value, expected))
        });
        if matches {
            entries.remove(client_id);
        }
        Ok(matches)
    }

    /// Nullable Java-compatible identity-aware removal boundary.
    pub fn remove_client_factory_nullable(
        &self,
        client_id: Option<&str>,
        expected: Option<&Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        let Some(client_id) = client_id else {
            return Err(RuntimeError::InvalidRoute("null client id"));
        };
        let Some(expected) = expected else {
            return Ok(false);
        };
        self.remove_client_factory(client_id, expected)
    }

    /// Unconditional removal corresponding to Java removeClientFactory(id).
    /// Does not shut down the removed runtime. Lifecycle cleanup must continue
    /// to use the identity-aware overload to avoid removing replacements.
    /// Await this key's initializer, as ConcurrentHashMap removal does for an
    /// in-progress mapping. Keys in different shards remain independently
    /// available; hash-colliding keys share a shard.
    pub async fn remove_client_factory_by_id(&self, client_id: &str) -> Result<bool, RuntimeError> {
        let _key_mutation = self.mutation_locks.lock(client_id).await?;
        loop {
            let entry = {
                let entries = self
                    .entries
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?;
                let Some(entry) = entries.get(client_id) else {
                    return Ok(false);
                };
                entry.clone()
            };
            let _initialization = entry.initialization.lock().await;
            let _synchronous_mutation = _key_mutation.lock_synchronous();
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            match entries.get(client_id) {
                None => return Ok(false),
                Some(current) if Arc::ptr_eq(current, &entry) => {
                    let published = entry.runtime.get().is_some();
                    entries.remove(client_id);
                    return Ok(published);
                }
                // A concurrent identity-safe cleanup/replacement overtook our
                // wait: unconditional removal targets the current mapping.
                Some(_) => continue,
            }
        }
    }

    /// Nullable Java-compatible unconditional removal boundary.
    pub async fn remove_client_factory_by_id_nullable(
        &self,
        client_id: Option<&str>,
    ) -> Result<bool, RuntimeError> {
        let Some(client_id) = client_id else {
            return Err(RuntimeError::InvalidRoute("null client id"));
        };
        self.remove_client_factory_by_id(client_id).await
    }
}

/// Live factory-map access with owned reads and atomic per-key mutations.
/// Removing/replacing mappings does not shut down their runtime. Key aliases
/// are permitted, as in the Java map; lifecycle cleanup uses the runtime's ID.
#[derive(Clone)]
pub struct FactoryTable {
    manager: ClientRuntimeManager,
}

enum TableMutation {
    Put(Arc<ClientRuntime>),
    PutIfAbsent(Arc<ClientRuntime>),
    Replace(Arc<ClientRuntime>),
    ReplaceIfSame(Arc<ClientRuntime>, Arc<ClientRuntime>),
    Remove,
    RemoveIfSame(Arc<ClientRuntime>),
}

impl FactoryTable {
    pub fn get(&self, key: &str) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .get(key)
            .and_then(|entry| entry.runtime.get())
            .cloned())
    }

    /// Nullable Java-compatible `ConcurrentMap.get` boundary.
    pub fn get_nullable(
        &self,
        key: Option<&str>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        self.get(key)
    }

    pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
        self.get(key).map(|value| value.is_some())
    }

    /// Nullable Java-compatible `ConcurrentMap.containsKey` boundary.
    pub fn contains_key_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        self.contains_key(key)
    }

    /// Uses MQClientInstance's inherited Java Object identity equality.
    pub fn contains_value(&self, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .values()
            .filter_map(|entry| entry.runtime.get())
            .any(|current| Arc::ptr_eq(current, value)))
    }

    /// Nullable Java-compatible `ConcurrentMap.containsValue` boundary.
    pub fn contains_value_nullable(
        &self,
        value: Option<&Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        self.contains_value(value)
    }

    /// Returns the mapped runtime, or the supplied value when the key is absent.
    pub fn get_or_default(
        &self,
        key: &str,
        default: Arc<ClientRuntime>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        Ok(self.get(key)?.unwrap_or(default))
    }

    /// Nullable Java-compatible `ConcurrentMap.getOrDefault` boundary.
    pub fn get_or_default_nullable(
        &self,
        key: Option<&str>,
        default: Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        Ok(self.get(key)?.or(default))
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .values()
            .filter(|entry| entry.runtime.get().is_some())
            .count())
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.len().map(|len| len == 0)
    }

    /// An explicitly detached copy for iteration. The FactoryTable handle
    /// itself stays live and every subsequent read sees the current mapping.
    pub fn snapshot(&self) -> Result<HashMap<String, Arc<ClientRuntime>>, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .runtime
                    .get()
                    .map(|runtime| (key.clone(), runtime.clone()))
            })
            .collect())
    }

    /// Visits one detached map snapshot. The callback runs without a manager
    /// lock and may safely reenter this table; Java's weakly consistent live
    /// traversal is not represented by this Rust convenience method.
    pub fn for_each(
        &self,
        mut action: impl FnMut(&str, &Arc<ClientRuntime>),
    ) -> Result<(), RuntimeError> {
        let snapshot = self.snapshot()?;
        for (key, runtime) in &snapshot {
            action(key, runtime);
        }
        Ok(())
    }

    /// Nullable Java-compatible `ConcurrentHashMap.forEach` callback boundary.
    pub fn for_each_nullable(
        &self,
        action: Option<&mut TableForEach<'_, Arc<ClientRuntime>>>,
    ) -> Result<(), RuntimeError> {
        let Some(action) = action else {
            return Err(RuntimeError::InvalidRoute("null factory forEach action"));
        };
        self.for_each(|key, value| action(key, value))
    }

    pub fn key_set_view(&self) -> FactoryKeySetView {
        FactoryKeySetView(self.clone())
    }

    pub fn values_view(&self) -> FactoryValuesView {
        FactoryValuesView(self.clone())
    }

    pub fn entry_set_view(&self) -> FactoryEntrySetView {
        FactoryEntrySetView(self.clone())
    }

    pub async fn put(
        &self,
        key: &str,
        value: Arc<ClientRuntime>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::Put(value))
            .await
            .map(|(previous, _)| previous)
    }

    /// Nullable Java-compatible `ConcurrentMap.put` boundary.
    pub async fn put_nullable(
        &self,
        key: Option<&str>,
        value: Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        self.put(key, value).await
    }

    /// Applies mappings in source iteration order, matching Map.putAll's
    /// independent per-key writes.
    pub async fn put_all<I>(&self, entries: I) -> Result<(), RuntimeError>
    where
        I: IntoIterator<Item = (String, Arc<ClientRuntime>)>,
    {
        for (key, value) in entries {
            self.put(&key, value).await?;
        }
        Ok(())
    }

    /// Nullable Java-compatible `Map.putAll` source-map boundary.
    pub async fn put_all_nullable(
        &self,
        entries: Option<&BTreeMap<String, Arc<ClientRuntime>>>,
    ) -> Result<(), RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null factory putAll map"));
        };
        for (key, value) in entries {
            self.put(key, value.clone()).await?;
        }
        Ok(())
    }

    /// Applies ordered nullable source-map rows with Java `putAll` failure
    /// behavior; writes before an invalid key/value are not rolled back.
    pub async fn put_all_nullable_rows(
        &self,
        entries: Option<&[NullableTableEntry<ClientRuntime>]>,
    ) -> Result<(), RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null factory putAll map"));
        };
        self.put_all_nullable_iter(entries.iter().cloned()).await
    }

    /// Applies nullable entries from any source iterator in its traversal
    /// order, preserving Java `Map.putAll` partial writes before null errors.
    pub async fn put_all_nullable_iter<I>(&self, entries: I) -> Result<(), RuntimeError>
    where
        I: IntoIterator<Item = NullableTableEntry<ClientRuntime>>,
    {
        for (key, value) in entries {
            let Some(key) = key.as_deref() else {
                return Err(RuntimeError::InvalidRoute("null factory key"));
            };
            let Some(value) = value else {
                return Err(RuntimeError::InvalidRoute("null factory value"));
            };
            self.put(key, value.clone()).await?;
        }
        Ok(())
    }

    pub async fn put_if_absent(
        &self,
        key: &str,
        value: Arc<ClientRuntime>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::PutIfAbsent(value))
            .await
            .map(|(previous, _)| previous)
    }

    /// Nullable Java-compatible `ConcurrentMap.putIfAbsent` boundary.
    pub async fn put_if_absent_nullable(
        &self,
        key: Option<&str>,
        value: Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        self.put_if_absent(key, value).await
    }

    /// Runs the mapping callback at most once for this invocation. A per-key
    /// gate serializes its read/callback/write sequence with table mutations;
    /// hash-colliding keys share a shard.
    pub async fn compute_if_absent(
        &self,
        key: impl Into<String>,
        mut mapping: impl FnMut(&str) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        let _key_mutation = self.manager.mutation_locks.lock(&key).await?;
        let _synchronous_mutation = _key_mutation.lock_synchronous();
        if let Some(current) = self.get(&key)? {
            return Ok(Some(current));
        }
        let Some(value) = mapping(&key) else {
            return Ok(None);
        };
        let (previous, _) = self.mutate_locked(&key, TableMutation::PutIfAbsent(value.clone()))?;
        Ok(Some(previous.unwrap_or(value)))
    }

    /// Nullable Java-compatible `computeIfAbsent` key/function boundary.
    pub async fn compute_if_absent_nullable(
        &self,
        key: Option<&str>,
        mapping: Option<&mut TableComputeIfAbsent<'_, Arc<ClientRuntime>>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(mapping) = mapping else {
            return Err(RuntimeError::InvalidRoute(
                "null factory remapping function",
            ));
        };
        self.compute_if_absent(key, |key| mapping(key)).await
    }

    pub async fn compute_if_present(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, &Arc<ClientRuntime>) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        let _key_mutation = self.manager.mutation_locks.lock(&key).await?;
        let _synchronous_mutation = _key_mutation.lock_synchronous();
        let Some(current) = self.get(&key)? else {
            return Ok(None);
        };
        match remapping(&key, &current) {
            Some(value) => {
                self.mutate_locked(&key, TableMutation::ReplaceIfSame(current, value.clone()))?;
                Ok(Some(value))
            }
            None => {
                self.mutate_locked(&key, TableMutation::RemoveIfSame(current))?;
                Ok(None)
            }
        }
    }

    /// Nullable Java-compatible `computeIfPresent` key/function boundary.
    pub async fn compute_if_present_nullable(
        &self,
        key: Option<&str>,
        remapping: Option<&mut TableComputeIfPresent<'_, Arc<ClientRuntime>>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute(
                "null factory remapping function",
            ));
        };
        self.compute_if_present(key, |key, value| remapping(key, value))
            .await
    }

    pub async fn compute(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, Option<&Arc<ClientRuntime>>) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        let _key_mutation = self.manager.mutation_locks.lock(&key).await?;
        let _synchronous_mutation = _key_mutation.lock_synchronous();
        let current = self.get(&key)?;
        let value = remapping(&key, current.as_ref());
        match (current, value) {
            (None, None) => Ok(None),
            (None, Some(value)) => {
                self.mutate_locked(&key, TableMutation::PutIfAbsent(value.clone()))?;
                Ok(Some(value))
            }
            (Some(current), Some(value)) => {
                self.mutate_locked(&key, TableMutation::ReplaceIfSame(current, value.clone()))?;
                Ok(Some(value))
            }
            (Some(current), None) => {
                self.mutate_locked(&key, TableMutation::RemoveIfSame(current))?;
                Ok(None)
            }
        }
    }

    /// Nullable Java-compatible `compute` key/function boundary.
    pub async fn compute_nullable(
        &self,
        key: Option<&str>,
        remapping: Option<&mut TableCompute<'_, Arc<ClientRuntime>>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute(
                "null factory remapping function",
            ));
        };
        self.compute(key, |key, current| remapping(key, current))
            .await
    }

    pub async fn merge(
        &self,
        key: impl Into<String>,
        value: Arc<ClientRuntime>,
        mut remapping: impl FnMut(
            &Arc<ClientRuntime>,
            &Arc<ClientRuntime>,
        ) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        let _key_mutation = self.manager.mutation_locks.lock(&key).await?;
        let _synchronous_mutation = _key_mutation.lock_synchronous();
        let Some(current) = self.get(&key)? else {
            self.mutate_locked(&key, TableMutation::PutIfAbsent(value.clone()))?;
            return Ok(Some(value));
        };
        match remapping(&current, &value) {
            Some(next) => {
                self.mutate_locked(&key, TableMutation::ReplaceIfSame(current, next.clone()))?;
                Ok(Some(next))
            }
            None => {
                self.mutate_locked(&key, TableMutation::RemoveIfSame(current))?;
                Ok(None)
            }
        }
    }

    /// Nullable Java-compatible `merge` key/value/function boundary.
    pub async fn merge_nullable(
        &self,
        key: Option<&str>,
        value: Option<Arc<ClientRuntime>>,
        remapping: Option<&mut TableMerge<'_, Arc<ClientRuntime>>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute(
                "null factory remapping function",
            ));
        };
        self.merge(key, value, |current, next| remapping(current, next))
            .await
    }

    /// Visits the keys in a detached snapshot with Java
    /// ConcurrentHashMap.replaceAll semantics. Callbacks run outside the key
    /// gate and may be retried after a concurrent mapping change.
    pub async fn replace_all(
        &self,
        mut remapping: impl FnMut(&str, &Arc<ClientRuntime>) -> Arc<ClientRuntime>,
    ) -> Result<(), RuntimeError> {
        for key in self.snapshot()?.into_keys() {
            loop {
                let Some(current) = self.get(&key)? else {
                    break;
                };
                // ConcurrentHashMap.replaceAll invokes the function outside
                // its bin lock and retries after a lost conditional replace.
                let value = remapping(&key, &current);
                let _key_mutation = self.manager.mutation_locks.lock(&key).await?;
                let _synchronous_mutation = _key_mutation.lock_synchronous();
                let (_, changed) =
                    self.mutate_locked(&key, TableMutation::ReplaceIfSame(current, value))?;
                if changed {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Nullable Java-compatible `ConcurrentHashMap.replaceAll` callback boundary.
    pub async fn replace_all_nullable(
        &self,
        remapping: Option<&mut TableReplaceAll<'_, Arc<ClientRuntime>>>,
    ) -> Result<(), RuntimeError> {
        let Some(remapping) = remapping else {
            return Err(RuntimeError::InvalidRoute(
                "null factory replaceAll function",
            ));
        };
        self.replace_all(|key, value| remapping(key, value)).await
    }

    /// Applies Java-compatible `replaceAll` callbacks whose result may be
    /// null. A null result fails at that key; earlier replacements remain.
    pub async fn replace_all_with_nullable_results(
        &self,
        mut remapping: Option<&mut TableReplaceAllOptionalResult<'_, Arc<ClientRuntime>>>,
    ) -> Result<(), RuntimeError> {
        let Some(remapping) = remapping.as_mut() else {
            return Err(RuntimeError::InvalidRoute(
                "null factory replaceAll function",
            ));
        };
        for key in self.snapshot()?.into_keys() {
            loop {
                let Some(current) = self.get(&key)? else {
                    break;
                };
                let Some(value) = remapping(&key, &current) else {
                    return Err(RuntimeError::InvalidRoute("null factory replaceAll result"));
                };
                let _key_mutation = self.manager.mutation_locks.lock(&key).await?;
                let _synchronous_mutation = _key_mutation.lock_synchronous();
                let (_, changed) =
                    self.mutate_locked(&key, TableMutation::ReplaceIfSame(current, value))?;
                if changed {
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn replace(
        &self,
        key: &str,
        value: Arc<ClientRuntime>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::Replace(value))
            .await
            .map(|(previous, _)| previous)
    }

    /// Nullable Java-compatible `ConcurrentMap.replace(key, value)` boundary.
    pub async fn replace_nullable(
        &self,
        key: Option<&str>,
        value: Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        self.replace(key, value).await
    }

    pub async fn replace_if_same(
        &self,
        key: &str,
        expected: &Arc<ClientRuntime>,
        value: Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        self.mutate(key, TableMutation::ReplaceIfSame(expected.clone(), value))
            .await
            .map(|(_, changed)| changed)
    }

    /// Nullable Java-compatible conditional replacement. Runtime equality is
    /// identity equality, matching `MQClientInstance`'s inherited `Object.equals`.
    pub async fn replace_if_equal_nullable(
        &self,
        key: Option<&str>,
        expected: Option<&Arc<ClientRuntime>>,
        value: Option<Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(expected) = expected else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        self.replace_if_same(key, expected, value).await
    }

    pub async fn remove(&self, key: &str) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::Remove)
            .await
            .map(|(previous, _)| previous)
    }

    /// Nullable Java-compatible `ConcurrentMap.remove(key)` boundary.
    pub async fn remove_nullable(
        &self,
        key: Option<&str>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        self.remove(key).await
    }

    pub async fn remove_if_same(
        &self,
        key: &str,
        expected: &Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        self.mutate(key, TableMutation::RemoveIfSame(expected.clone()))
            .await
            .map(|(_, changed)| changed)
    }

    /// Nullable Java-compatible conditional removal using runtime identity equality.
    pub async fn remove_if_equal_nullable(
        &self,
        key: Option<&str>,
        expected: Option<&Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        let Some(expected) = expected else {
            return Ok(false);
        };
        self.remove_if_same(key, expected).await
    }

    /// Like ConcurrentMap.clear, not a transaction against concurrent inserts.
    pub async fn clear(&self) -> Result<(), RuntimeError> {
        let keys: Vec<_> = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?
            .keys()
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key).await?;
        }
        Ok(())
    }

    async fn mutate(
        &self,
        key: &str,
        operation: TableMutation,
    ) -> Result<(Option<Arc<ClientRuntime>>, bool), RuntimeError> {
        let _key_mutation = self.manager.mutation_locks.lock(key).await?;
        let _synchronous_mutation = _key_mutation.lock_synchronous();
        self.mutate_locked(key, operation)
    }

    fn mutate_locked(
        &self,
        key: &str,
        operation: TableMutation,
    ) -> Result<(Option<Arc<ClientRuntime>>, bool), RuntimeError> {
        let mut entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let current = entries.get(key);
        let previous = current.and_then(|entry| entry.runtime.get()).cloned();
        let same = |expected: &Arc<ClientRuntime>| {
            previous
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, expected))
        };
        let replacement = match &operation {
            TableMutation::Put(value) => Some(value),
            TableMutation::PutIfAbsent(value) if previous.is_none() => Some(value),
            TableMutation::Replace(value) if previous.is_some() => Some(value),
            TableMutation::ReplaceIfSame(expected, value) if same(expected) => Some(value),
            TableMutation::Remove => {
                entries.remove(key);
                return Ok((previous.clone(), previous.is_some()));
            }
            TableMutation::RemoveIfSame(expected) if same(expected) => {
                entries.remove(key);
                return Ok((previous, true));
            }
            _ => return Ok((previous, false)),
        };
        if let Some(value) = replacement {
            // Retain the actual transport's sharing inputs, not today's
            // process properties or a fabricated fresh hook identity.
            let entry = Entry {
                config: value.config_snapshot(),
                properties: value.sharing_properties.clone(),
                hook: value.sharing_hook.clone(),
                tls_config: value.sharing_tls_config.clone(),
                initialization: AsyncMutex::new(()),
                runtime: OnceLock::from(value.clone()),
            };
            if key == value.client_id() {
                value.attach_manager(ManagerLink {
                    entries: Arc::downgrade(&self.manager.entries),
                    mutation_locks: Arc::downgrade(&self.manager.mutation_locks),
                })?;
            }
            entries.insert(key.into(), Arc::new(entry));
            return Ok((previous, true));
        }
        unreachable!("every table mutation returns or selects a replacement")
    }
}

/// Retained live key-set view of a `FactoryTable`.
#[derive(Clone)]
pub struct FactoryKeySetView(FactoryTable);

impl FactoryKeySetView {
    pub fn contains(&self, key: &str) -> Result<bool, RuntimeError> {
        self.0.contains_key(key)
    }

    /// Nullable Java-compatible key-set membership boundary.
    pub fn contains_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        self.contains(key)
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

    pub async fn remove(&self, key: &str) -> Result<bool, RuntimeError> {
        self.0.remove(key).await.map(|value| value.is_some())
    }

    /// Nullable Java-compatible key-set removal boundary.
    pub async fn remove_nullable(&self, key: Option<&str>) -> Result<bool, RuntimeError> {
        let Some(key) = key else {
            return Err(RuntimeError::InvalidRoute("null factory key"));
        };
        self.remove(key).await
    }

    /// Removes keys accepted by the predicate from a detached key snapshot.
    pub async fn remove_if(
        &self,
        mut predicate: impl FnMut(&str) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for key in self.snapshot()? {
            if predicate(&key) && self.remove(&key).await? {
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
            return Err(RuntimeError::InvalidRoute("null factory view predicate"));
        };
        self.remove_if(|key| predicate(key)).await
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
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.remove_all(keys).await
    }

    /// Java `KeySetView.removeAll` removes candidates in collection order;
    /// a null key fails at its position and earlier removals remain visible.
    pub async fn remove_all_with_nullable_elements(
        &self,
        keys: Option<&[Option<String>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(keys) = keys else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
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
        self.remove_if(|key| !keys.contains(key)).await
    }

    /// Nullable Java-compatible collection argument boundary.
    pub async fn retain_all_nullable(
        &self,
        keys: Option<&BTreeSet<String>>,
    ) -> Result<usize, RuntimeError> {
        let Some(keys) = keys else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.retain_all(keys).await
    }

    /// Null collection elements do not match keys during Java `retainAll`.
    pub async fn retain_all_with_nullable_elements(
        &self,
        keys: Option<&[Option<String>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(keys) = keys else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.remove_if(|key| !keys.iter().flatten().any(|candidate| candidate == key))
            .await
    }

    pub async fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear().await
    }
}

/// Retained live values collection view of a `FactoryTable`.
#[derive(Clone)]
pub struct FactoryValuesView(FactoryTable);

impl FactoryValuesView {
    pub fn contains(&self, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        self.0.contains_value(value)
    }

    /// Nullable Java-compatible values-view membership boundary.
    pub fn contains_nullable(
        &self,
        value: Option<&Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        self.contains(value)
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached values copy from the live view.
    pub fn snapshot(&self) -> Result<Vec<Arc<ClientRuntime>>, RuntimeError> {
        Ok(self.0.snapshot()?.into_values().collect())
    }

    /// Removes one matching runtime mapping without shutting the runtime down.
    pub async fn remove(&self, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        for (key, current) in self.0.snapshot()? {
            if Arc::ptr_eq(&current, value) && self.0.remove_if_same(&key, value).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Java `ConcurrentHashMap.values().remove(null)` returns false.
    pub async fn remove_nullable(
        &self,
        value: Option<&Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        let Some(value) = value else {
            return Ok(false);
        };
        self.remove(value).await
    }

    /// Removes values accepted by the predicate from a detached entry snapshot.
    pub async fn remove_if(
        &self,
        mut predicate: impl FnMut(&Arc<ClientRuntime>) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, value) in self.0.snapshot()? {
            if predicate(&value) && self.0.remove_if_same(&key, &value).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Nullable Java-compatible `removeIf` predicate boundary.
    pub async fn remove_if_nullable(
        &self,
        predicate: Option<&mut TableValueViewPredicate<'_, Arc<ClientRuntime>>>,
    ) -> Result<usize, RuntimeError> {
        let Some(predicate) = predicate else {
            return Err(RuntimeError::InvalidRoute("null factory view predicate"));
        };
        self.remove_if(|value| predicate(value)).await
    }

    pub async fn remove_all(&self, values: &[Arc<ClientRuntime>]) -> Result<usize, RuntimeError> {
        self.remove_if(|value| values.iter().any(|candidate| Arc::ptr_eq(candidate, value)))
            .await
    }

    /// Nullable Java-compatible collection argument boundary.
    pub async fn remove_all_nullable(
        &self,
        values: Option<&[Arc<ClientRuntime>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(values) = values else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.remove_all(values).await
    }

    /// Null candidates are ignored by the Java values view.
    pub async fn remove_all_with_nullable_elements(
        &self,
        values: Option<&[Option<Arc<ClientRuntime>>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(values) = values else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        let mut removed = 0;
        for value in values.iter().flatten() {
            if self.remove(value).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub async fn retain_all(&self, values: &[Arc<ClientRuntime>]) -> Result<usize, RuntimeError> {
        self.remove_if(|value| !values.iter().any(|candidate| Arc::ptr_eq(candidate, value)))
            .await
    }

    /// Nullable Java-compatible collection argument boundary.
    pub async fn retain_all_nullable(
        &self,
        values: Option<&[Arc<ClientRuntime>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(values) = values else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.retain_all(values).await
    }

    /// Null candidates do not match a non-null ConcurrentHashMap value.
    pub async fn retain_all_with_nullable_elements(
        &self,
        values: Option<&[Option<Arc<ClientRuntime>>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(values) = values else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.remove_if(|value| {
            !values
                .iter()
                .flatten()
                .any(|candidate| Arc::ptr_eq(candidate, value))
        })
        .await
    }

    pub async fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear().await
    }
}

/// Retained live entry-set view of a `FactoryTable`.
#[derive(Clone)]
pub struct FactoryEntrySetView(FactoryTable);

/// Detached `Map.Entry`-like snapshot over one factory-table mapping.
#[derive(Clone)]
pub struct FactoryEntryHandle {
    table: FactoryTable,
    key: String,
    value: Arc<RwLock<Arc<ClientRuntime>>>,
    update_lock: Arc<AsyncMutex<()>>,
}

impl FactoryEntryHandle {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Result<Arc<ClientRuntime>, RuntimeError> {
        self.value
            .read()
            .map(|value| value.clone())
            .map_err(|_| RuntimeError::RegistryPoisoned)
    }

    pub async fn set_value(
        &self,
        value: Arc<ClientRuntime>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        let _update = self.update_lock.lock().await;
        let previous = self.value()?;
        self.table.put(&self.key, value.clone()).await?;
        *self
            .value
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)? = value;
        Ok(previous)
    }

    /// Nullable Java-compatible `Map.Entry.setValue` boundary.
    pub async fn set_value_nullable(
        &self,
        value: Option<Arc<ClientRuntime>>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        let Some(value) = value else {
            return Err(RuntimeError::InvalidRoute("null factory value"));
        };
        self.set_value(value).await
    }
}

impl FactoryEntrySetView {
    pub fn contains(&self, key: &str, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        Ok(self
            .0
            .get(key)?
            .is_some_and(|current| Arc::ptr_eq(&current, value)))
    }

    /// A null Java Map.Entry is not a member of this entry-set view.
    pub fn contains_nullable(
        &self,
        entry: Option<(&str, &Arc<ClientRuntime>)>,
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
        value: Option<&Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        match (key, value) {
            (Some(key), Some(value)) => self.contains(key, value),
            _ => Ok(false),
        }
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached entry copy from the live view.
    pub fn snapshot(&self) -> Result<BTreeMap<String, Arc<ClientRuntime>>, RuntimeError> {
        Ok(self.0.snapshot()?.into_iter().collect())
    }

    /// Returns detached entry snapshots in deterministic key order.
    pub fn entry_snapshots(&self) -> Result<Vec<FactoryEntryHandle>, RuntimeError> {
        Ok(self
            .0
            .snapshot()?
            .into_iter()
            .map(|(key, value)| FactoryEntryHandle {
                table: self.0.clone(),
                key,
                value: Arc::new(RwLock::new(value)),
                update_lock: Arc::new(AsyncMutex::new(())),
            })
            .collect())
    }

    pub async fn remove(
        &self,
        key: &str,
        value: &Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        self.0.remove_if_same(key, value).await
    }

    /// A null Java Map.Entry is not removed from this entry-set view.
    pub async fn remove_nullable(
        &self,
        entry: Option<(&str, &Arc<ClientRuntime>)>,
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
        value: Option<&Arc<ClientRuntime>>,
    ) -> Result<bool, RuntimeError> {
        match (key, value) {
            (Some(key), Some(value)) => self.remove(key, value).await,
            _ => Ok(false),
        }
    }

    /// Removes entries accepted by the predicate from a detached entry snapshot.
    pub async fn remove_if(
        &self,
        mut predicate: impl FnMut(&str, &Arc<ClientRuntime>) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, value) in self.0.snapshot()? {
            if predicate(&key, &value) && self.0.remove_if_same(&key, &value).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Nullable Java-compatible `removeIf` predicate boundary.
    pub async fn remove_if_nullable(
        &self,
        predicate: Option<&mut TableEntryViewPredicate<'_, Arc<ClientRuntime>>>,
    ) -> Result<usize, RuntimeError> {
        let Some(predicate) = predicate else {
            return Err(RuntimeError::InvalidRoute("null factory view predicate"));
        };
        self.remove_if(|key, value| predicate(key, value)).await
    }

    pub async fn remove_all(
        &self,
        entries: &BTreeMap<String, Arc<ClientRuntime>>,
    ) -> Result<usize, RuntimeError> {
        self.remove_if(|key, value| {
            entries
                .get(key)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, value))
        })
        .await
    }

    /// Nullable Java-compatible collection argument boundary.
    pub async fn remove_all_nullable(
        &self,
        entries: Option<&BTreeMap<String, Arc<ClientRuntime>>>,
    ) -> Result<usize, RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.remove_all(entries).await
    }

    /// Null entries and entries with null fields do not match a CHM entry.
    pub async fn remove_all_with_nullable_elements(
        &self,
        entries: Option<&[Option<NullableTableEntry<ClientRuntime>>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
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
        entries: &BTreeMap<String, Arc<ClientRuntime>>,
    ) -> Result<usize, RuntimeError> {
        self.remove_if(|key, value| {
            !entries
                .get(key)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, value))
        })
        .await
    }

    /// Nullable Java-compatible collection argument boundary.
    pub async fn retain_all_nullable(
        &self,
        entries: Option<&BTreeMap<String, Arc<ClientRuntime>>>,
    ) -> Result<usize, RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.retain_all(entries).await
    }

    /// Null entries and entries with null fields do not match a CHM entry.
    pub async fn retain_all_with_nullable_elements(
        &self,
        entries: Option<&[Option<NullableTableEntry<ClientRuntime>>]>,
    ) -> Result<usize, RuntimeError> {
        let Some(entries) = entries else {
            return Err(RuntimeError::InvalidRoute("null factory view collection"));
        };
        self.remove_if(|key, value| {
            !entries.iter().flatten().any(|entry| match entry {
                (Some(candidate_key), Some(candidate_value)) => {
                    candidate_key == key && Arc::ptr_eq(candidate_value, value)
                }
                _ => false,
            })
        })
        .await
    }

    pub async fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_remoting::TransportError;
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn explicit_tls_config_reaches_runtime_construction_before_workers_start() {
        let manager = ClientRuntimeManager::new();
        let mut config = ClientConfig::new("explicit-client-tls");
        config.set_use_tls(true);
        let mut tls = TlsConfig::default();
        tls.test_mode = false;
        tls.client_cert_pem = Some(b"malformed client certificate".to_vec());
        tls.client_key_pem = Some(b"malformed client key".to_vec());

        let result = manager
            .get_or_create_with_hook_and_tls_config(&config, None, Some(tls))
            .await;
        assert!(matches!(
            result,
            Err(RuntimeError::Transport(TransportError::Tls(_)))
        ));
        assert!(
            !manager
                .entries
                .lock()
                .unwrap()
                .contains_key(&config.build_client_id())
        );
    }

    #[tokio::test]
    async fn unconditional_removal_matches_java_without_stale_identity_cleanup_regression() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let java = &fixture["managerRemoval"];
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("unconditional-remove");
        let id = config.build_client_id();
        assert!(!manager.remove_client_factory_by_id(&id).await.unwrap());
        let first = manager.get_or_create(&config).await.unwrap();
        assert_eq!(
            manager.entries.lock().unwrap()[&id]
                .runtime
                .get()
                .is_some_and(|value| Arc::ptr_eq(value, &first)),
            java["liveSeesFirst"]
        );
        assert!(manager.remove_client_factory_by_id(&id).await.unwrap());
        assert_eq!(
            !manager.entries.lock().unwrap().contains_key(&id),
            java["absentAfterUnconditional"]
        );
        assert_eq!(first.state(), super::super::RuntimeState::Created);
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert_eq!(
            !Arc::ptr_eq(&first, &replacement),
            java["replacementIdentity"]
        );
        assert!(!manager.remove_client_factory(&id, &first).unwrap());
        assert_eq!(
            Arc::ptr_eq(&replacement, &manager.get_or_create(&config).await.unwrap()),
            java["staleIdentityKeepsReplacement"]
        );
        assert!(manager.remove_client_factory_by_id(&id).await.unwrap());
        assert_eq!(
            !manager.entries.lock().unwrap().contains_key(&id),
            java["unconditionalRemovesReplacement"]
        );
        assert!(!manager.remove_client_factory_by_id(&id).await.unwrap());
        assert_eq!(
            !manager.entries.lock().unwrap().contains_key(&id),
            java["absentRemovalStaysAbsent"]
        );
    }

    #[tokio::test]
    async fn unconditional_removal_waits_only_for_its_key_and_cancellation_preserves_mapping() {
        use tokio::time::{Duration, timeout};
        for cancel in [false, true] {
            let manager = ClientRuntimeManager::new();
            let config = ClientConfig::new("in-progress-removal");
            let ready = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let task_manager = manager.clone();
            let task_config = config.clone();
            let task_ready = ready.clone();
            let task_release = release.clone();
            let creation = tokio::spawn(async move {
                task_manager
                    .get_or_create_inner(&task_config, None, |entry| {
                        let ready = task_ready.clone();
                        let release = task_release.clone();
                        async move {
                            ready.notify_one();
                            release.notified().await;
                            ClientRuntime::construct(entry.config.clone(), &entry.properties, None)
                        }
                    })
                    .await
            });
            ready.notified().await;
            let task_manager = manager.clone();
            let id = config.build_client_id();
            let mut removal =
                tokio::spawn(async move { task_manager.remove_client_factory_by_id(&id).await });
            assert!(
                timeout(Duration::from_millis(20), &mut removal)
                    .await
                    .is_err()
            );
            let other = timeout(
                Duration::from_secs(1),
                manager.get_or_create(&ClientConfig::new("other-key")),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(other.client_id(), "other-key@DEFAULT");
            if cancel {
                removal.abort();
                assert!((&mut removal).await.unwrap_err().is_cancelled());
            } else {
                // Still waiting after unrelated construction, not serializing
                // the whole table or prematurely removing a reserved entry.
                assert!(!removal.is_finished());
            }
            release.notify_one();
            let first = creation.await.unwrap().unwrap();
            if cancel {
                assert!(Arc::ptr_eq(
                    &first,
                    &manager.get_or_create(&config).await.unwrap()
                ));
            } else {
                assert!(removal.await.unwrap().unwrap());
                let replacement = manager.get_or_create(&config).await.unwrap();
                assert!(!Arc::ptr_eq(&first, &replacement));
                assert!(
                    !manager
                        .remove_client_factory(first.client_id(), &first)
                        .unwrap()
                );
            }
        }
    }

    #[tokio::test]
    async fn live_table_waits_for_publication_and_cancelled_mutation_has_no_effect() {
        use tokio::time::{Duration, timeout};
        for action in ["put", "remove", "clear", "cancel"] {
            let manager = ClientRuntimeManager::new();
            let live = manager.factory_table();
            let mut config = ClientConfig::new("reserved-table-key");
            config.namesrv_addr = Some(String::new());
            let source = ClientRuntimeManager::new();
            let replacement = source.get_or_create(&config).await.unwrap();
            let id = config.build_client_id();
            let ready = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let task_manager = manager.clone();
            let task_config = config.clone();
            let task_ready = ready.clone();
            let task_release = release.clone();
            let creation = tokio::spawn(async move {
                task_manager
                    .get_or_create_inner(&task_config, None, |entry| {
                        let ready = task_ready.clone();
                        let release = task_release.clone();
                        async move {
                            ready.notify_one();
                            release.notified().await;
                            ClientRuntime::construct(entry.config.clone(), &entry.properties, None)
                        }
                    })
                    .await
            });
            ready.notified().await;
            assert!(live.get(&id).unwrap().is_none());
            assert_eq!(live.len().unwrap(), 0);
            assert!(live.snapshot().unwrap().is_empty());
            let task_live = live.clone();
            let task_id = id.clone();
            let task_value = replacement.clone();
            let mut mutation = tokio::spawn(async move {
                match action {
                    "put" | "cancel" => task_live.put(&task_id, task_value).await,
                    "remove" => task_live.remove(&task_id).await,
                    _ => task_live.clear().await.map(|()| None),
                }
            });
            assert!(
                timeout(Duration::from_millis(20), &mut mutation)
                    .await
                    .is_err()
            );
            let mut unrelated = ClientConfig::new("unrelated-table-key");
            unrelated.namesrv_addr = Some(String::new());
            timeout(Duration::from_secs(1), manager.get_or_create(&unrelated))
                .await
                .unwrap()
                .unwrap();
            if action == "cancel" {
                mutation.abort();
                assert!(matches!((&mut mutation).await, Err(error) if error.is_cancelled()));
            }
            release.notify_one();
            let created = creation.await.unwrap().unwrap();
            if action == "cancel" {
                assert!(Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &created));
            } else {
                let previous = mutation.await.unwrap().unwrap();
                if action == "put" || action == "remove" {
                    assert!(Arc::ptr_eq(&previous.unwrap(), &created));
                }
                if action == "put" {
                    assert!(Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &replacement));
                } else {
                    assert!(live.get(&id).unwrap().is_none());
                }
            }
        }
    }

    #[tokio::test]
    async fn table_insertion_retains_original_property_snapshot_not_current_environment() {
        let mut config = ClientConfig::new("property-table-key");
        config.namesrv_addr = Some(String::new());
        let properties = HashMap::from([(
            "rocketmq.namesrv.domain".into(),
            "captured-table-fixture.invalid".into(),
        )]);
        let runtime =
            Arc::new(ClientRuntime::construct(config.clone(), &properties, None).unwrap());
        let manager = ClientRuntimeManager::new();
        manager
            .factory_table()
            .put(runtime.client_id(), runtime.clone())
            .await
            .unwrap();
        assert_eq!(
            manager.entries.lock().unwrap()[runtime.client_id()].properties,
            properties
        );
        manager.factory_table().clear().await.unwrap();
    }

    #[tokio::test]
    async fn unrelated_keys_construct_concurrently_and_same_key_constructs_once() {
        let manager = ClientRuntimeManager::new();
        let barrier = Arc::new(Barrier::new(2));
        let count = AtomicUsize::new(0);
        let construct = |entry: Arc<Entry>| {
            let barrier = barrier.clone();
            let count = &count;
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                ClientRuntime::construct(entry.config.clone(), &entry.properties, None)
            }
        };
        // Choose IDs that collide in the prior 256-shard scheme. Exact-key
        // gates must still let these distinct runtimes initialize together.
        let mut old_shards = HashMap::<usize, ClientConfig>::new();
        let mut collision = None;
        for index in 0..=256 {
            let config = ClientConfig::new(format!("same-old-shard-{index}"));
            let mut hasher = DefaultHasher::new();
            config.build_client_id().hash(&mut hasher);
            let shard = (hasher.finish() as usize) & 255;
            if let Some(previous) = old_shards.insert(shard, config.clone()) {
                collision = Some((previous, config));
                break;
            }
        }
        let (first, second) = collision.expect("257 IDs must collide in the old 256-shard scheme");
        assert_ne!(first.build_client_id(), second.build_client_id());
        let same_key_config = first.clone();
        let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(
                manager.get_or_create_inner(&first, None, &construct),
                manager.get_or_create_inner(&second, None, &construct)
            )
        })
        .await
        .unwrap();
        assert!(!Arc::ptr_eq(&first.unwrap(), &second.unwrap()));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        manager
            .get_or_create_inner(&same_key_config, None, &construct)
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn stale_failed_entry_cannot_remove_a_replacement_entry() {
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("first");
        let first = manager.get_or_create(&config).await.unwrap();
        let old_entry = manager.entries.lock().unwrap()[first.client_id()].clone();
        manager
            .remove_client_factory(first.client_id(), &first)
            .unwrap();
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert!(!manager.remove_entry(first.client_id(), &old_entry).unwrap());
        assert!(Arc::ptr_eq(
            &replacement,
            &manager.get_or_create(&config).await.unwrap()
        ));
    }

    #[tokio::test]
    async fn identity_cleanup_does_not_depend_on_a_concurrent_lookup_lock() {
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("first");
        let first = manager.get_or_create(&config).await.unwrap();
        let entry = manager.entries.lock().unwrap()[first.client_id()].clone();
        let _lookup_guard = entry.initialization.lock().await;
        assert!(
            manager
                .remove_client_factory(first.client_id(), &first)
                .unwrap()
        );
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &replacement));
    }

    #[tokio::test]
    async fn cancelled_initialization_releases_key_for_retry_without_publishing_resources() {
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("first");
        let (ready, started) = tokio::sync::oneshot::channel();
        let sender = Arc::new(Mutex::new(Some(ready)));
        let task_manager = manager.clone();
        let task_config = config.clone();
        let task = tokio::spawn(async move {
            task_manager
                .get_or_create_inner(&task_config, None, |entry| {
                    let sender = sender.clone();
                    async move {
                        let _unpublished = ClientRuntime::construct(
                            entry.config.clone(),
                            &entry.properties,
                            None,
                        )?;
                        sender.lock().unwrap().take().unwrap().send(()).unwrap();
                        std::future::pending::<Result<ClientRuntime, RuntimeError>>().await
                    }
                })
                .await
        });
        started.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert!(Arc::ptr_eq(
            &replacement,
            &manager.get_or_create(&config).await.unwrap()
        ));
    }
}
