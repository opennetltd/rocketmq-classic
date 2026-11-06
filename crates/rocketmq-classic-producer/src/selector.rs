use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Per-topic overflow-safe queue cursor matching Java's increment-then-modulo
/// selection order without inheriting `abs(i32::MIN)` overflow behavior.
#[derive(Default)]
pub(crate) struct RoundRobinSelector {
    cursors: Mutex<HashMap<String, AtomicUsize>>,
}

impl RoundRobinSelector {
    pub(crate) fn next_index(&self, topic: &str, queue_count: usize) -> usize {
        if queue_count == 0 {
            return 0;
        }
        let mut cursors = self
            .cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cursors
            .entry(topic.to_owned())
            .or_insert_with(|| AtomicUsize::new(random_seed()))
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
            % queue_count
    }

    pub(crate) fn reset(&self, topic: &str) {
        let mut cursors = self
            .cursors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cursors
            .entry(topic.to_owned())
            .or_insert_with(|| AtomicUsize::new(random_seed()))
            .store(random_seed(), Ordering::Relaxed);
    }
}

fn random_seed() -> usize {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::MessageQueue;

    #[test]
    fn round_robin_is_per_topic_and_wraps_unsigned_indices() {
        let selector = RoundRobinSelector::default();
        let orders = (0..4)
            .map(|_| selector.next_index("orders", 4))
            .collect::<std::collections::BTreeSet<_>>();
        let payments = (0..4)
            .map(|_| selector.next_index("payments", 4))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(orders, [0, 1, 2, 3].into());
        assert_eq!(payments, [0, 1, 2, 3].into());
        let before_reset = selector.next_index("orders", 4);
        selector.reset("orders");
        let after_reset = selector.next_index("orders", 4);
        assert!(before_reset < 4);
        assert!(after_reset < 4);
        assert_eq!(selector.next_index("empty", 0), 0);
        let queue = MessageQueue::new("orders", "broker-a", 0);
        assert_eq!(queue.topic(), Some("orders"));
    }
}
