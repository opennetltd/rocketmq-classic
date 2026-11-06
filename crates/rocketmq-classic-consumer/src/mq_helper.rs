use std::{collections::BTreeSet, future::Future, sync::Arc};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::heartbeat::MessageModel;

use crate::{PullConsumerConfig, PullConsumerError, pull::PullConsumer};

/// Utilities corresponding to the pinned Java `org.apache.rocketmq.client.MQHelper`.
pub struct MqHelper;

impl MqHelper {
    /// Resets a topic's consumer offsets from a timestamp using the default instance name.
    ///
    /// This is Java's deprecated four-argument `resetOffsetByTimestamp` overload.
    #[deprecated(note = "use reset_offset_by_timestamp_with_instance")]
    pub async fn reset_offset_by_timestamp(
        message_model: MessageModel,
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        timestamp_millis: i64,
    ) -> Result<(), PullConsumerError> {
        Self::reset_offset_by_timestamp_with_instance(
            message_model,
            "DEFAULT",
            consumer_group,
            topic,
            timestamp_millis,
        )
        .await
    }

    /// Resets each queue's consume offset to the first offset at or after a timestamp.
    ///
    /// Queues are processed in `MessageQueue` order, matching Java's `TreeSet` pass.
    /// Offsets below zero are ignored. The fetched queue set is persisted even when
    /// a search or update fails, and the consumer is shut down after persistence.
    pub async fn reset_offset_by_timestamp_with_instance(
        message_model: MessageModel,
        instance_name: impl Into<String>,
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        timestamp_millis: i64,
    ) -> Result<(), PullConsumerError> {
        let mut config = PullConsumerConfig::new(consumer_group)?;
        config.set_message_model(message_model);
        config
            .client_config_mut()
            .set_instance_name(Some(instance_name.into()));

        let consumer = Arc::new(PullConsumer::new(config));
        // Java calls start before entering its try/finally block.
        consumer.start().await?;

        let topic = topic.into();
        let fetched = consumer.fetch_subscribe_message_queues(&topic).await;
        let (queues, mut result) = match fetched {
            Ok(queues) => (Some(queues), Ok(())),
            // Java's admin facade returns an empty set for an empty NameServer route.
            Err(PullConsumerError::EmptyMessageQueues(_)) => (Some(BTreeSet::new()), Ok(())),
            Err(error) => (None, Err(error)),
        };

        if let Some(queues) = queues {
            if result.is_ok() {
                result = update_offsets_by_timestamp(
                    &queues,
                    timestamp_millis,
                    |queue, timestamp| {
                        let consumer = Arc::clone(&consumer);
                        async move { consumer.search_offset(&queue, timestamp).await }
                    },
                    |queue, offset| {
                        let consumer = Arc::clone(&consumer);
                        async move { consumer.update_consume_offset(&queue, offset).await }
                    },
                )
                .await;
            }

            // Java persists the original fetched set, not just queues with a valid result.
            consumer
                .persist_offsets(&queues.iter().cloned().collect::<Vec<MessageQueue>>())
                .await?;
        }

        // This is reached after fetch/search/update errors, matching Java's finally block.
        consumer.shutdown().await?;
        result
    }
}

async fn update_offsets_by_timestamp<E, Search, SearchFuture, Update, UpdateFuture>(
    queues: &BTreeSet<MessageQueue>,
    timestamp_millis: i64,
    mut search: Search,
    mut update: Update,
) -> Result<(), E>
where
    Search: FnMut(MessageQueue, i64) -> SearchFuture,
    SearchFuture: Future<Output = Result<i64, E>>,
    Update: FnMut(MessageQueue, i64) -> UpdateFuture,
    UpdateFuture: Future<Output = Result<(), E>>,
{
    for queue in queues.iter().cloned() {
        let offset = search(queue.clone(), timestamp_millis).await?;
        if offset >= 0 {
            update(queue, offset).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Mutex};

    use rocketmq_classic_common::message::MessageQueue;

    use super::update_offsets_by_timestamp;
    use crate::PullConsumerError;

    #[tokio::test]
    async fn timestamp_reset_sorts_queues_and_skips_negative_offsets() {
        let queues = BTreeSet::from([
            MessageQueue::new("orders", "broker-b", 0),
            MessageQueue::new("orders", "broker-a", 2),
            MessageQueue::new("orders", "broker-a", 0),
        ]);
        let searched = Mutex::new(Vec::new());
        let updated = Mutex::new(Vec::new());

        update_offsets_by_timestamp(
            &queues,
            456,
            |queue, timestamp| {
                searched.lock().unwrap().push((queue.clone(), timestamp));
                async move {
                    Ok::<_, PullConsumerError>(match (queue.broker_name(), queue.queue_id()) {
                        (Some("broker-a"), 0) => 12,
                        (Some("broker-a"), 2) => -1,
                        (Some("broker-b"), 0) => 8,
                        _ => unreachable!(),
                    })
                }
            },
            |queue, offset| {
                updated.lock().unwrap().push((queue, offset));
                async { Ok::<_, PullConsumerError>(()) }
            },
        )
        .await
        .unwrap();

        let searched = searched.into_inner().unwrap();
        assert_eq!(
            searched.iter().map(|(_, time)| *time).collect::<Vec<_>>(),
            [456; 3]
        );
        assert_eq!(
            searched
                .into_iter()
                .map(|(queue, _)| queue)
                .collect::<Vec<_>>(),
            [
                MessageQueue::new("orders", "broker-a", 0),
                MessageQueue::new("orders", "broker-a", 2),
                MessageQueue::new("orders", "broker-b", 0),
            ]
        );
        assert_eq!(
            updated.into_inner().unwrap(),
            [
                (MessageQueue::new("orders", "broker-a", 0), 12),
                (MessageQueue::new("orders", "broker-b", 0), 8),
            ]
        );
    }

    #[tokio::test]
    async fn timestamp_reset_stops_after_first_search_error() {
        let queues = BTreeSet::from([
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-a", 1),
        ]);
        let searched = Mutex::new(Vec::new());

        let result = update_offsets_by_timestamp(
            &queues,
            456,
            |queue, _| {
                searched.lock().unwrap().push(queue);
                async { Err::<i64, _>(PullConsumerError::InvalidOffset) }
            },
            |_, _| async { Ok::<_, PullConsumerError>(()) },
        )
        .await;

        assert!(matches!(result, Err(PullConsumerError::InvalidOffset)));
        assert_eq!(
            searched.into_inner().unwrap(),
            [MessageQueue::new("orders", "broker-a", 0)]
        );
    }

    #[tokio::test]
    async fn timestamp_reset_stops_after_first_update_error() {
        let queues = BTreeSet::from([
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-a", 1),
        ]);
        let searched = Mutex::new(Vec::new());
        let updated = Mutex::new(Vec::new());

        let result = update_offsets_by_timestamp(
            &queues,
            456,
            |queue, _| {
                searched.lock().unwrap().push(queue);
                async { Ok::<i64, PullConsumerError>(5) }
            },
            |queue, _| {
                updated.lock().unwrap().push(queue);
                async { Err::<(), _>(PullConsumerError::InvalidOffset) }
            },
        )
        .await;

        assert!(matches!(result, Err(PullConsumerError::InvalidOffset)));
        assert_eq!(
            searched.into_inner().unwrap(),
            [MessageQueue::new("orders", "broker-a", 0)]
        );
        assert_eq!(
            updated.into_inner().unwrap(),
            [MessageQueue::new("orders", "broker-a", 0)]
        );
    }
}
