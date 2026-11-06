use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeMap};
use std::{
    cell::Cell,
    fmt,
    hash::Hash,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

pub(crate) const SHARED_VALUE_REFERENCE_MARKER_KEY: &str =
    "\0rocketmq_classic_shared_value_identity";

thread_local! {
    static SHARED_VALUE_REFERENCE_MARKERS_ENABLED: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn shared_value_reference_markers_enabled() -> bool {
    SHARED_VALUE_REFERENCE_MARKERS_ENABLED.with(Cell::get)
}

pub(crate) fn serialize_shared_value_identity<S, T>(
    identity: usize,
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    if !shared_value_reference_markers_enabled() {
        return value.serialize(serializer);
    }
    let payload = (identity, value);
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry(SHARED_VALUE_REFERENCE_MARKER_KEY, &payload)?;
    map.end()
}

pub(crate) fn with_shared_value_reference_markers<R>(action: impl FnOnce() -> R) -> R {
    struct RestoreMarkerState(bool);

    impl Drop for RestoreMarkerState {
        fn drop(&mut self) {
            SHARED_VALUE_REFERENCE_MARKERS_ENABLED.with(|enabled| enabled.set(self.0));
        }
    }

    let previous = SHARED_VALUE_REFERENCE_MARKERS_ENABLED.with(|enabled| enabled.replace(true));
    let restore = RestoreMarkerState(previous);
    let result = action();
    drop(restore);
    result
}

/// A cloneable, shared mutable DTO value.
///
/// Java DTO setters commonly retain an object reference and return that same
/// object from their getter. `SharedValue` provides that identity behavior for
/// Rust DTOs while keeping access synchronized and snapshots owned.
pub struct SharedValue<T>(Arc<RwLock<T>>);

impl<T> SharedValue<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(RwLock::new(value)))
    }

    pub fn snapshot(&self) -> T
    where
        T: Clone,
    {
        self.read().clone()
    }

    pub fn with<R>(&self, read: impl FnOnce(&T) -> R) -> R {
        read(&self.read())
    }

    /// Runs a mutation visible to every clone of this handle.
    ///
    /// The closure must not re-enter this same handle. Poisoned locks recover
    /// the contained value, matching Java's continued access after an
    /// exception has interrupted a mutation.
    pub fn with_mut<R>(&self, update: impl FnOnce(&mut T) -> R) -> R {
        update(&mut self.write())
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Feeds this handle's stable process-local identity into a hash builder.
    pub(crate) fn hash_identity<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }

    pub(crate) fn reference_identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<T> Clone for SharedValue<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> From<T> for SharedValue<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Default> Default for SharedValue<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: fmt::Debug> fmt::Debug for SharedValue<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.read().fmt(formatter)
    }
}

impl<T: Serialize> Serialize for SharedValue<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if SHARED_VALUE_REFERENCE_MARKERS_ENABLED.with(Cell::get) {
            let value = self.read();
            serialize_shared_value_identity(self.reference_identity(), &*value, serializer)
        } else {
            self.read().serialize(serializer)
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedValue<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::new)
    }
}
