use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, PoisonError, RwLock},
};

/// A live retry response-code set shared with the producer configuration.
/// Mutations affect later synchronous send retry decisions.
pub struct RetryResponseCodes {
    values: Arc<RwLock<BTreeSet<i32>>>,
}

impl RetryResponseCodes {
    pub(crate) fn new(values: BTreeSet<i32>) -> Self {
        Self {
            values: Arc::new(RwLock::new(values)),
        }
    }

    pub(crate) fn shared_handle(&self) -> Self {
        Self {
            values: self.values.clone(),
        }
    }

    pub(crate) fn detached_clone(&self) -> Self {
        Self::new(self.snapshot())
    }

    pub fn contains(&self, value: &i32) -> bool {
        self.values
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(value)
    }

    pub fn insert(&self, value: i32) -> bool {
        self.values
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(value)
    }

    pub fn remove(&self, value: &i32) -> bool {
        self.values
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(value)
    }

    pub fn clear(&self) {
        self.values
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    pub fn len(&self) -> usize {
        self.values
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.values
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    pub fn extend(&self, values: impl IntoIterator<Item = i32>) {
        self.values
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(values);
    }

    pub fn retain(&self, mut keep: impl FnMut(&i32) -> bool) {
        self.values
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|value| keep(value));
    }

    pub fn snapshot(&self) -> BTreeSet<i32> {
        self.values
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Clone for RetryResponseCodes {
    fn clone(&self) -> Self {
        self.shared_handle()
    }
}

impl fmt::Debug for RetryResponseCodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.snapshot()).finish()
    }
}

impl PartialEq for RetryResponseCodes {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for RetryResponseCodes {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_handles_alias_and_detached_clones_are_independent() {
        let codes = RetryResponseCodes::new([1, 2].into());
        let independent = codes.detached_clone();
        let cloned_handle = codes.clone();
        let live = codes.shared_handle();

        assert!(live.insert(3));
        assert!(codes.contains(&3));
        assert!(cloned_handle.contains(&3));
        assert!(!independent.contains(&3));

        assert!(independent.remove(&1));
        assert!(codes.contains(&1));
    }
}
