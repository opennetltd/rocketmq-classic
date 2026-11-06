use crate::{
    api::admin::{AdminQueryError, AdminQueryRequest, QueryResult},
    error::RpcError,
};
use rocketmq_classic_protocol::codes::{request, response};
use tokio::time::Instant;

use super::{ClientRuntime, RuntimeError};
use rocketmq_classic_protocol::body::ConsumerRunningInfoBody;

impl ClientRuntime {
    /// Java's synchronized `MQClientInstance.resetOffset(topic, group, table)`.
    /// Resets for this runtime serialize across groups. The shared table is
    /// observed by the owner at each Java lookup boundary, including after the
    /// drain wait. Missing/non-Push groups ignore the nullable topic/table.
    pub async fn reset_offset(
        &self,
        topic: Option<&str>,
        group: Option<&str>,
        offsets: Option<super::ConsumerOffsetTable>,
    ) -> Result<(), RuntimeError> {
        let _gate = self.monitor_gate.lock().await;
        let Some(consumer) = self.select_consumer_nullable(group)? else {
            return Ok(());
        };
        match consumer.reset_offset(topic.map(str::to_owned), offsets) {
            Some(operation) => operation.await,
            None => Ok(()),
        }
    }

    /// Java `MQClientInstance.consumeMessageDirectly(msg, group, brokerName)`.
    /// Only Push consumers invoke the service; other/missing groups return
    /// null before the message is inspected. No offset is advanced.
    pub async fn consume_message_directly(
        &self,
        message: Option<rocketmq_classic_common::message::MessageExt>,
        group: Option<&str>,
        broker_name: Option<&str>,
    ) -> Result<Option<rocketmq_classic_protocol::body::ConsumeMessageDirectlyResult>, RuntimeError>
    {
        let Some(consumer) = self.select_consumer_nullable(group)? else {
            return Ok(None);
        };
        match consumer.consume_message_directly(message, broker_name.map(str::to_owned)) {
            Some(operation) => operation.await,
            None => Ok(None),
        }
    }

    /// Java `MQClientInstance.consumerRunningInfo(group)`. The snapshot's
    /// shared properties are enriched in place, in Java's observable order.
    pub fn consumer_running_info(
        &self,
        group: Option<&str>,
    ) -> Result<Option<ConsumerRunningInfoBody>, RuntimeError> {
        let Some(consumer) = self.select_consumer_nullable(group)? else {
            return Ok(None);
        };
        let info = consumer.consumer_running_info()?;
        let addresses = self
            .remoting()
            .get_name_server_address_list()
            .unwrap_or_default();
        let mut nameservers = String::new();
        for address in addresses {
            nameservers.push_str(&address);
            nameservers.push(';');
        }
        let properties = info.get_properties().ok_or_else(|| {
            RuntimeError::ConsumerOperation("null consumer running info properties".into())
        })?;
        properties.with_mut(|properties| {
            properties.insert(
                ConsumerRunningInfoBody::PROP_NAMESERVER_ADDR.into(),
                nameservers,
            );
        });
        let consume_type = consumer
            .consume_type()
            .ok_or_else(|| RuntimeError::ConsumerOperation("null consumer consume type".into()))?;
        properties.with_mut(|properties| {
            properties.insert(
                ConsumerRunningInfoBody::PROP_CONSUME_TYPE.into(),
                consume_type.name().into(),
            );
            properties.insert(
                ConsumerRunningInfoBody::PROP_CLIENT_VERSION.into(),
                "V5_5_1".into(),
            );
        });
        Ok(Some(info))
    }

    /// Queries the route's Brokers with one shared deadline, using Java's
    /// MQAdmin filtering and partial-failure behavior.
    pub async fn query_message(
        &self,
        query: &AdminQueryRequest,
        deadline: Instant,
    ) -> Result<QueryResult, AdminQueryError> {
        let route = match self.get_existing_topic_route(&query.route_topic)? {
            Some(route) => Some(route),
            None => {
                self.update_topic_route(&query.route_topic, deadline)
                    .await?;
                self.get_existing_topic_route(&query.route_topic)?
            }
        };
        let Some(route) = route else {
            return Err(RpcError::Response {
                request_code: request::QUERY_MESSAGE,
                peer: query.topic.clone(),
                code: response::NO_MESSAGE,
                remark: Some("query message by key finished, but no message.".into()),
            }
            .into());
        };
        let route = route.snapshot()?;
        self.api()
            .query_message_across_brokers(&route, query, deadline)
            .await
            .map_err(Into::into)
    }
}
