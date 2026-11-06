use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::{route::BrokerAddress, runtime::ClientRuntime};
use rocketmq_classic_protocol::header::{
    QueryConsumerOffsetRequestHeader, UpdateConsumerOffsetRequestHeader,
};
use tokio::time::Instant;

use super::{
    OffsetRead, OffsetStore, OffsetStoreError, OffsetStoreFuture, OffsetTable, ReadOffsetType,
};

const OFFSET_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const MASTER_BROKER_ID: i64 = 0;

/// Stores offsets in the shared per-client table and persists them to a
/// Broker master, matching Java `RemoteBrokerOffsetStore`.
pub struct RemoteBrokerOffsetStore {
    runtime: Arc<ClientRuntime>,
    group_name: String,
    offsets: OffsetTable,
}

impl RemoteBrokerOffsetStore {
    pub fn new(runtime: Arc<ClientRuntime>, group_name: impl Into<String>) -> Self {
        Self {
            runtime,
            group_name: group_name.into(),
            offsets: OffsetTable::default(),
        }
    }

    pub fn group_name(&self) -> &str {
        &self.group_name
    }

    async fn resolve_broker_address(
        &self,
        queue: &MessageQueue,
        only_this_broker: bool,
    ) -> Result<Option<BrokerAddress>, OffsetStoreError> {
        let topic = queue
            .topic()
            .ok_or_else(|| OffsetStoreError::Runtime("message queue has no topic".into()))?;
        let broker = queue
            .broker_name()
            .ok_or_else(|| OffsetStoreError::Runtime("message queue has no broker name".into()))?;
        let find = |only_this_broker| {
            self.runtime
                .route_snapshot()
                .map_err(|error| OffsetStoreError::Runtime(error.to_string()))?
                .subscribe_address(broker, MASTER_BROKER_ID, only_this_broker)
                .map_err(|error| OffsetStoreError::Runtime(error.to_string()))
        };

        if let Some(address) = find(only_this_broker)? {
            return Ok(Some(address));
        }

        let config = self.runtime.config();
        let timeout_millis = config
            .read()
            .map_err(|_| OffsetStoreError::Runtime("client config lock poisoned".into()))?
            .mq_client_api_timeout()
            .max(0) as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis);
        self.runtime
            .update_topic_route(topic, deadline)
            .await
            .map_err(|error| OffsetStoreError::Runtime(error.to_string()))?;
        find(false)
    }

    async fn address_or_error(
        &self,
        queue: &MessageQueue,
        only_this_broker: bool,
    ) -> Result<String, OffsetStoreError> {
        self.resolve_broker_address(queue, only_this_broker)
            .await?
            .map(|address| address.address)
            .ok_or_else(|| {
                OffsetStoreError::BrokerNotFound(queue.broker_name().unwrap_or("null").to_owned())
            })
    }

    async fn fetch_from_broker(
        &self,
        queue: &MessageQueue,
    ) -> Result<OffsetRead, OffsetStoreError> {
        let topic = queue
            .topic()
            .ok_or_else(|| OffsetStoreError::Runtime("message queue has no topic".into()))?;
        let address = self.address_or_error(queue, true).await?;
        let mut header = QueryConsumerOffsetRequestHeader::default();
        header.set_consumer_group(Some(self.group_name.clone()));
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_broker_name(queue.broker_name().map(str::to_owned));
        let offset = self
            .runtime
            .api()
            .query_consumer_offset(&address, &header, Instant::now() + OFFSET_RPC_TIMEOUT)
            .await?;
        if let Some(offset) = offset {
            self.offsets.update(queue, offset, false);
            Ok(OffsetRead::Found(offset))
        } else {
            Ok(OffsetRead::NotFound)
        }
    }

    async fn send_to_broker(
        &self,
        queue: &MessageQueue,
        offset: i64,
        oneway: bool,
    ) -> Result<(), OffsetStoreError> {
        let topic = queue
            .topic()
            .ok_or_else(|| OffsetStoreError::Runtime("message queue has no topic".into()))?;
        let address = self.address_or_error(queue, false).await?;
        let mut header = UpdateConsumerOffsetRequestHeader::default();
        header.set_consumer_group(Some(self.group_name.clone()));
        header.set_topic(Some(topic.to_owned()));
        header.set_queue_id(Some(queue.queue_id()));
        header.set_commit_offset(Some(offset));
        header.set_broker_name(queue.broker_name().map(str::to_owned));
        self.runtime
            .api()
            .update_consumer_offset(
                &address,
                &header,
                oneway,
                Instant::now() + OFFSET_RPC_TIMEOUT,
            )
            .await?;
        Ok(())
    }
}

impl OffsetStore for RemoteBrokerOffsetStore {
    fn load(&self) -> OffsetStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn update_offset(&self, queue: &MessageQueue, offset: i64, increase_only: bool) {
        self.offsets.update(queue, offset, increase_only);
    }

    fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
        self.offsets.update_and_freeze(queue, offset);
    }

    fn read_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        mode: ReadOffsetType,
    ) -> OffsetStoreFuture<'a, OffsetRead> {
        Box::pin(async move {
            match mode {
                ReadOffsetType::ReadFromMemory => Ok(self
                    .offsets
                    .read(queue)
                    .map_or(OffsetRead::NotFound, OffsetRead::Found)),
                ReadOffsetType::ReadFromStore => self.fetch_from_broker(queue).await,
                ReadOffsetType::MemoryFirstThenStore => {
                    if let Some(offset) = self.offsets.read(queue) {
                        Ok(OffsetRead::Found(offset))
                    } else {
                        self.fetch_from_broker(queue).await
                    }
                }
            }
        })
    }

    fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            if let Some(offset) = self.offsets.read(queue) {
                self.send_to_broker(queue, offset, true).await?;
            }
            Ok(())
        })
    }

    fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            if queues.is_empty() {
                return Ok(());
            }
            let selected = queues.iter().cloned().collect::<HashSet<_>>();
            let snapshot = self.offsets.snapshot(None);
            let mut first_error = None;
            for (queue, offset) in snapshot {
                if selected.contains(&queue) {
                    if let Err(error) = self.send_to_broker(&queue, offset, true).await {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            self.offsets.remove_unselected(&selected);
            if let Some(error) = first_error {
                return Err(error);
            }
            Ok(())
        })
    }

    fn remove_offset(&self, queue: &MessageQueue) {
        self.offsets.remove(queue);
    }

    fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
        self.offsets.snapshot(topic)
    }

    fn update_consume_offset_to_broker<'a>(
        &'a self,
        queue: &'a MessageQueue,
        offset: i64,
        oneway: bool,
    ) -> OffsetStoreFuture<'a, ()> {
        Box::pin(self.send_to_broker(queue, offset, oneway))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn clone_topic_filter_matches_pinned_java_for_every_bmp_scalar() {
        let mut config = rocketmq_classic_core::config::ClientConfig::new("offset-clone-oracle");
        config.set_namesrv_addr(None);
        let runtime = rocketmq_classic_core::runtime::ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let store = RemoteBrokerOffsetStore::new(runtime.clone(), "group");
        super::super::assert_clone_topic_filter_matches_java(&store);
        runtime.shutdown().await.unwrap();
    }
}
