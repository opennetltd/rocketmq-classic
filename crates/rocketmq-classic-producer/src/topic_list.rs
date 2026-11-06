use std::sync::{Arc, RwLock};

/// A shared mutable producer topic list.
///
/// Java's `DefaultMQProducer.setTopics` retains the supplied list and
/// `getTopics` returns that same list. Cloning this handle keeps the alias;
/// [`snapshot`](Self::snapshot) provides an owned point-in-time copy.
#[derive(Clone, Default)]
pub struct ProducerTopicList(Arc<RwLock<Vec<String>>>);

impl ProducerTopicList {
    pub fn new(topics: Vec<String>) -> Self {
        Self(Arc::new(RwLock::new(topics)))
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.with_read(<[String]>::to_vec)
    }

    pub fn len(&self) -> usize {
        self.with_read(<[String]>::len)
    }

    pub fn is_empty(&self) -> bool {
        self.with_read(<[String]>::is_empty)
    }

    pub fn contains(&self, topic: &str) -> bool {
        self.with_read(|topics| topics.iter().any(|value| value == topic))
    }

    pub fn push(&self, topic: impl Into<String>) {
        self.with_mut(|topics| topics.push(topic.into()));
    }

    pub fn insert(&self, index: usize, topic: impl Into<String>) -> bool {
        self.with_mut(|topics| {
            if index > topics.len() {
                return false;
            }
            topics.insert(index, topic.into());
            true
        })
    }

    pub fn set(&self, index: usize, topic: impl Into<String>) -> Option<String> {
        self.with_mut(|topics| {
            let value = topics.get_mut(index)?;
            Some(std::mem::replace(value, topic.into()))
        })
    }

    pub fn remove(&self, index: usize) -> Option<String> {
        self.with_mut(|topics| (index < topics.len()).then(|| topics.remove(index)))
    }

    pub fn clear(&self) {
        self.with_mut(Vec::clear);
    }

    pub fn with_read<R>(&self, read: impl FnOnce(&[String]) -> R) -> R {
        let topics = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        read(&topics)
    }

    pub fn with_mut<R>(&self, update: impl FnOnce(&mut Vec<String>) -> R) -> R {
        let mut topics = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        update(&mut topics)
    }

    pub(crate) fn detached_clone(&self) -> Self {
        Self::new(self.snapshot())
    }
}

impl From<Vec<String>> for ProducerTopicList {
    fn from(topics: Vec<String>) -> Self {
        Self::new(topics)
    }
}

impl std::fmt::Debug for ProducerTopicList {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.with_read(|topics| formatter.debug_list().entries(topics).finish())
    }
}

impl PartialEq for ProducerTopicList {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for ProducerTopicList {}

#[cfg(test)]
mod tests {
    use super::ProducerTopicList;

    #[test]
    fn clones_share_mutations_and_snapshots_are_independent() {
        let topics = ProducerTopicList::from(vec!["startup".to_owned()]);
        let alias = topics.clone();
        let snapshot = topics.snapshot();

        alias.push("later");

        assert_eq!(
            topics.snapshot(),
            vec!["startup".to_owned(), "later".to_owned()]
        );
        assert_eq!(snapshot, vec!["startup".to_owned()]);
    }

    #[test]
    fn list_mutators_observe_java_index_boundaries() {
        let topics = ProducerTopicList::from(vec!["one".to_owned(), "two".to_owned()]);

        assert!(!topics.insert(3, "bad-index"));
        assert!(topics.insert(1, "inserted"));
        assert_eq!(topics.set(2, "updated"), Some("two".to_owned()));
        assert_eq!(topics.remove(0), Some("one".to_owned()));
        assert_eq!(
            topics.snapshot(),
            vec!["inserted".to_owned(), "updated".to_owned()]
        );
    }
}
