use std::{collections::HashSet, sync::OnceLock};

use rocketmq_classic_protocol::{
    body::TopicList,
    codes::{request, response},
    header::{
        CommandHeader, GetRouteInfoRequestHeader, GetTopicsByClusterRequestHeader, HeaderMap,
    },
    route::TopicRouteData,
};
use rocketmq_classic_remoting::RemotingCommand;
use tokio::time::Instant;

use super::MQClientApi;
use crate::error::RpcError;

const MISSING_ROUTE_BODY: &str = "missing route body";
static EMPTY_CLUSTER_LIST: OnceLock<HashSet<String>> = OnceLock::new();

impl MQClientApi {
    /// Returns Java's shared immutable empty result for
    /// `MQClientAPIImpl.getClusterList`. The pinned Java method does not use
    /// either input and does not issue a request.
    pub fn get_cluster_list(
        &self,
        _topic: Option<&str>,
        _deadline: Instant,
    ) -> &'static HashSet<String> {
        EMPTY_CLUSTER_LIST.get_or_init(HashSet::new)
    }

    pub(crate) fn route_error_is_client_exception(error: &RpcError) -> bool {
        matches!(error, RpcError::Response { .. })
            || matches!(error, RpcError::MalformedResponse { request_code: request::GET_ROUTEINFO_BY_TOPIC, reason, .. } if reason == MISSING_ROUTE_BODY)
    }

    /// Java's default-route method queries the fixed auto-create topic.
    /// allowTopicNotExist changes diagnostics only, not the wire or error.
    pub async fn get_default_topic_route(
        &self,
        deadline: Instant,
    ) -> Result<Option<TopicRouteData>, RpcError> {
        self.get_topic_route("TBW102", deadline).await
    }

    pub async fn get_topic_route(
        &self,
        topic: &str,
        deadline: Instant,
    ) -> Result<Option<TopicRouteData>, RpcError> {
        self.get_topic_route_nullable(Some(topic), deadline).await
    }

    /// Retrieves the NameServer topic list through Java
    /// `MQClientAPIImpl.getTopicListFromNameServer`.
    pub async fn get_topic_list_from_nameserver(
        &self,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        let code = request::GET_ALL_TOPIC_LIST_FROM_NAMESERVER;
        let (reply, peer) = self
            .remoting
            .invoke_name_server_sync_with_peer(
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer,
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
            request_code: code,
            peer: peer.clone(),
            reason: "successful topic-list reply has no body".into(),
        })?;
        TopicList::java_decode(&body).map_err(|error| RpcError::MalformedResponse {
            request_code: code,
            peer,
            reason: error.to_string(),
        })
    }

    /// Retrieves every topic assigned to a cluster through Java
    /// `MQClientAPIImpl.getTopicsByCluster`.
    pub async fn get_topics_by_cluster(
        &self,
        cluster: &str,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        self.get_topics_by_cluster_nullable(Some(cluster), deadline)
            .await
    }

    /// Nullable adapter for Java `getTopicsByCluster(String,long)`. A null
    /// cluster is accepted by Java's request header and omitted from the wire.
    pub async fn get_topics_by_cluster_nullable(
        &self,
        cluster: Option<&str>,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        let code = request::GET_TOPICS_BY_CLUSTER;
        let mut header = GetTopicsByClusterRequestHeader::default();
        header.set_cluster(cluster.map(str::to_owned));
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        let (reply, peer) = self
            .remoting
            .invoke_name_server_sync_with_peer(
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields.into()),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer,
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
            request_code: code,
            peer: peer.clone(),
            reason: "successful cluster-topic-list reply has no body".into(),
        })?;
        TopicList::java_decode(&body).map_err(|error| RpcError::MalformedResponse {
            request_code: code,
            peer,
            reason: error.to_string(),
        })
    }

    /// Retrieves the unit-topic list through Java
    /// `MQClientAPIImpl.getUnitTopicList`.
    pub async fn get_unit_topic_list(
        &self,
        is_unit_mode: bool,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        self.get_unit_topic_list_with_code(request::GET_UNIT_TOPIC_LIST, is_unit_mode, deadline)
            .await
    }

    /// Retrieves the topics with unit subscriptions through Java
    /// `MQClientAPIImpl.getHasUnitSubTopicList`.
    pub async fn get_has_unit_sub_topic_list(
        &self,
        is_unit_mode: bool,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        self.get_unit_topic_list_with_code(
            request::GET_HAS_UNIT_SUB_TOPIC_LIST,
            is_unit_mode,
            deadline,
        )
        .await
    }

    /// Retrieves the topics with and without unit subscriptions through Java
    /// `MQClientAPIImpl.getHasUnitSubUnUnitTopicList`.
    pub async fn get_has_unit_sub_ununit_topic_list(
        &self,
        is_unit_mode: bool,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        self.get_unit_topic_list_with_code(
            request::GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
            is_unit_mode,
            deadline,
        )
        .await
    }

    async fn get_unit_topic_list_with_code(
        &self,
        code: i32,
        is_unit_mode: bool,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        let (reply, peer) = self
            .remoting
            .invoke_name_server_sync_with_peer(
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer,
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
            request_code: code,
            peer: peer.clone(),
            reason: "successful unit-topic-list reply has no body".into(),
        })?;
        let topic_list =
            TopicList::java_decode(&body).map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer,
                reason: error.to_string(),
            })?;
        if !is_unit_mode {
            if let Some(topics) = topic_list.get_topic_list() {
                topics.with_mut(|topics| topics.retain(|topic| !topic.starts_with("%RETRY%")));
            }
        }
        Ok(topic_list)
    }

    /// Retrieves and merges the NameServer and Broker system-topic lists like
    /// Java `MQClientAPIImpl.getSystemTopicList`.
    pub async fn get_system_topic_list(&self, deadline: Instant) -> Result<TopicList, RpcError> {
        let code = request::GET_SYSTEM_TOPIC_LIST_FROM_NS;
        let (reply, peer) = self
            .remoting
            .invoke_name_server_sync_with_peer(
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer,
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
            request_code: code,
            peer: peer.clone(),
            reason: "successful system-topic-list reply has no body".into(),
        })?;
        let topic_list =
            TopicList::java_decode(&body).map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer,
                reason: error.to_string(),
            })?;

        // Java only queries the Broker for a nonempty NameServer set and a
        // nonblank address. Mutate the retained set handle to preserve aliases.
        if let (Some(name_server_topics), Some(broker_addr)) = (
            topic_list.get_topic_list(),
            topic_list.get_broker_addr().map(str::to_owned),
        ) {
            if !name_server_topics.snapshot().is_empty() && !broker_addr.trim().is_empty() {
                let broker_topics = self
                    .get_system_topic_list_from_broker(&broker_addr, deadline)
                    .await?
                    .get_topic_list();
                if let Some(broker_topics) = broker_topics {
                    let broker_topics = broker_topics.snapshot();
                    if !broker_topics.is_empty() {
                        name_server_topics.with_mut(|topics| topics.extend(broker_topics));
                    }
                }
            }
        }
        Ok(topic_list)
    }

    pub(crate) async fn get_topic_route_nullable(
        &self,
        topic: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<TopicRouteData>, RpcError> {
        let mut fields = HeaderMap::new();
        if let Some(topic) = topic {
            {
                let mut __phase18_header_0 = GetRouteInfoRequestHeader::default();
                __phase18_header_0.set_topic(topic.to_owned());
                __phase18_header_0.set_accept_standard_json_only(None);
                __phase18_header_0
            }
            .encode_fields(&mut fields);
        }
        let (reply, peer) = self
            .remoting
            .invoke_name_server_sync_with_peer(
                RemotingCommand {
                    code: request::GET_ROUTEINFO_BY_TOPIC,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields.into()),
                    ..super::java_client_request_command(request::GET_ROUTEINFO_BY_TOPIC)?
                },
                deadline,
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: request::GET_ROUTEINFO_BY_TOPIC,
                peer,
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
            request_code: request::GET_ROUTEINFO_BY_TOPIC,
            peer: peer.clone(),
            reason: MISSING_ROUTE_BODY.into(),
        })?;
        TopicRouteData::java_decode_nullable(Some(&body)).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: request::GET_ROUTEINFO_BY_TOPIC,
                peer,
                reason: error.to_string(),
            }
        })
    }
}
