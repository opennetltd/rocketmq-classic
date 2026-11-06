use super::{ClientRuntime, RuntimeError};
use crate::error::RpcError;
use rocketmq_classic_protocol::body::CheckClientRequestBody;

fn is_tag(expression: Option<&str>) -> bool {
    matches!(expression, None | Some("" | "TAG"))
}

impl ClientRuntime {
    /// Consumer-start check, not a runtime-start side effect. No route fetch is
    /// attempted here: absent Broker metadata skips the subscription check.
    pub async fn check_client_in_broker(&self) -> Result<(), RuntimeError> {
        for (group, consumer) in self.groups.consumer_entries()? {
            let subscriptions = consumer
                .heartbeat_data()
                .subscription_data_set
                .map(|subscriptions| subscriptions.snapshot());
            let Some(subscriptions) = subscriptions else {
                return Ok(());
            };
            if subscriptions.is_empty() {
                return Ok(()); // Java returns from the entire round, not continue.
            }
            for subscription in subscriptions {
                let Some(subscription) = subscription else {
                    return Err(RuntimeError::InvalidHeartbeatData("null subscription data"));
                };
                let subscription = subscription.snapshot();
                if is_tag(subscription.expression_type.as_deref()) {
                    continue;
                }
                let topic = subscription
                    .topic
                    .as_deref()
                    .ok_or(RuntimeError::InvalidRoute("null subscription topic"))?;
                let Some(address) = self.find_broker_address_by_topic(topic)? else {
                    continue;
                };
                let expression_type = subscription.expression_type.clone().unwrap_or_default();
                let result = async {
                    let address = crate::route::broker_vip_channel(
                        self.config_read().vip_channel_enabled,
                        &address,
                    )?;
                    let mut body = CheckClientRequestBody::default();
                    body.set_client_id(Some(self.client_id().into()));
                    body.set_group(Some(group.clone()));
                    body.set_subscription_data(Some(subscription.into()));
                    body.set_namespace(None);
                    self.api()
                        .check_client_in_broker(
                            &address,
                            &body,
                            tokio::time::Instant::now() + self.rpc_timeout(),
                        )
                        .await?;
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => {}
                    Err(error @ RuntimeError::Rpc(RpcError::Response { .. })) => return Err(error),
                    Err(error) => {
                        return Err(RuntimeError::ClientCheck {
                            expression_type,
                            source: Box::new(error),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tag_classification_matches_actual_java_including_null_and_case() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-check.json"
        ))
        .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            assert_eq!(
                is_tag(case["expression"].as_str()),
                case["isTag"].as_bool().unwrap()
            );
        }
    }
}
