use std::collections::{BTreeMap, HashMap, HashSet};

use rocketmq_classic_common::{
    message::{DecodeOptions, MessageExt, constants, decode_many_as_client_compatible, decode_one},
    permission::PermName,
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        AclInfo, BrokerStatsData, CheckRocksdbCqWriteResult, ClusterInfo,
        ConsumeMessageDirectlyResult, ConsumeStats, ConsumeStatsList, ConsumerConnection,
        ConsumerRunningInfoBody, CreateTopicListRequestBody, DataVersion,
        DeleteSubscriptionGroupListRequestBody, DeleteTopicListRequestBody, GroupList,
        HaRuntimeInfo, KvTable, ProducerConnection, ProducerTableInfo,
        QueryConsumeQueueResponseBody, QueryCorrectionOffsetBody, QuerySubscriptionResponseBody,
        QueueTimeSpan, ResetOffsetBody, SharedQueueOffsetEntries, SubscriptionGroupList,
        SubscriptionGroupWrapper, TopicConfig, TopicConfigAndQueueMapping, TopicConfigFilterType,
        TopicConfigSerializeWrapper, TopicList, TopicQueueMappingDetail, TopicStatsTable, UserInfo,
    },
    codes::{request, response},
    header::{
        AddBrokerRequestHeader, AddWritePermOfBrokerRequestHeader,
        AddWritePermOfBrokerResponseHeader, CheckRocksdbCqWriteProgressRequestHeader,
        CloneGroupOffsetRequestHeader, CommandHeader, ConsumeMessageDirectlyResultRequestHeader,
        CreateAclRequestHeader, CreateTopicRequestHeader, CreateUserRequestHeader,
        DeleteAclRequestHeader, DeleteKvConfigRequestHeader, DeleteSubscriptionGroupRequestHeader,
        DeleteTopicFromNamesrvRequestHeader, DeleteTopicRequestHeader, DeleteUserRequestHeader,
        ExportRocksDBConfigToJsonRequestHeader, ExportRocksDBConfigType, GetAclRequestHeader,
        GetAllSubscriptionGroupRequestHeader, GetAllSubscriptionGroupResponseHeader,
        GetAllTopicConfigRequestHeader, GetAllTopicConfigResponseHeader,
        GetConsumeStatsInBrokerRequestHeader, GetConsumeStatsRequestHeader,
        GetConsumerConnectionListRequestHeader, GetConsumerRunningInfoRequestHeader,
        GetKvConfigRequestHeader, GetKvListByNamespaceRequestHeader,
        GetSubscriptionGroupConfigRequestHeader, GetTopicConfigRequestHeader,
        GetTopicStatsInfoRequestHeader, GetUserRequestHeader, HeaderMap, ListAclsRequestHeader,
        ListUsersRequestHeader, PutKvConfigRequestHeader, QueryConsumeQueueRequestHeader,
        QueryConsumeTimeSpanRequestHeader, QueryCorrectionOffsetRequestHeader,
        QueryMessageRequestHeader, QueryMessageResponseHeader,
        QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
        QueryTopicsByConsumerRequestHeader, RemoveBrokerRequestHeader,
        ResetMasterFlushOffsetHeader, ResetOffsetRequestHeader, UpdateAclRequestHeader,
        UpdateGroupForbiddenRequestHeader, UpdateUserRequestHeader,
        ViewBrokerStatsDataRequestHeader, ViewMessageRequestHeader,
        WipeWritePermOfBrokerRequestHeader, WipeWritePermOfBrokerResponseHeader,
    },
    heartbeat::SubscriptionData,
    route::TopicRouteData,
    subscription::{GroupForbidden, SubscriptionGroupConfig},
};
use rocketmq_classic_remoting::RemotingCommand;
use tokio::{
    task::JoinSet,
    time::{Duration, Instant, timeout_at},
};

use super::{MQClientApi, classify, decode_body, decode_header};
use crate::config::read_shared_client_config;
use crate::error::RpcError;

#[derive(Default)]
struct InvokeBrokerToGetConsumerStatusHeader {
    topic: Option<String>,
    group: Option<String>,
    client_addr: Option<String>,
}

impl CommandHeader for InvokeBrokerToGetConsumerStatusHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        if let Some(value) = &self.topic {
            fields.insert("topic".to_owned(), value.clone());
        }
        if let Some(value) = &self.group {
            fields.insert("group".to_owned(), value.clone());
        }
        if let Some(value) = &self.client_addr {
            fields.insert("clientAddr".to_owned(), value.clone());
        }
    }

    fn decode_fields(
        fields: &HeaderMap,
    ) -> Result<Self, rocketmq_classic_protocol::header::ProtocolError> {
        Ok(Self {
            topic: fields.get("topic").cloned(),
            group: fields.get("group").cloned(),
            client_addr: fields.get("clientAddr").cloned(),
        })
    }
}

/// Java `getNameServerConfig` results keyed by NameServer address. Each value
/// is optional because Java returns a null `Properties` for malformed content.
pub type NameServerConfigs = HashMap<String, Option<HashMap<String, String>>>;

fn deadline_after_timeout_millis(timeout_millis: i64) -> Instant {
    let now = Instant::now();
    if timeout_millis <= 0 {
        return now;
    }
    now.checked_add(Duration::from_millis(timeout_millis as u64))
        .unwrap_or(now)
}

/// The index watermark and matching messages returned by a Broker query.
/// Its read-only result fields match Java's constructor/getter surface; the
/// optional list and shared element handles preserve Java null and alias
/// behavior.
///
/// ```compile_fail
/// use rocketmq_classic_core::api::admin::QueryResult;
/// let mut result = QueryResult::new(17, None);
/// result.index_last_update_timestamp = 18;
/// ```
#[derive(Debug)]
pub struct QueryResult {
    pub(crate) index_last_update_timestamp: i64,
    pub(crate) message_list: Option<QueryMessageList>,
}

/// Direct asynchronous admin operations from Java's `MqClientAdmin` surface.
/// The object shares the underlying remoting client with `MQClientApi`.
#[derive(Clone)]
pub struct MqClientAdmin {
    api: MQClientApi,
}

/// A mutable list handle corresponding to Java's retained `List<MessageExt>`.
pub type QueryMessageList = SharedValue<Vec<Option<SharedValue<MessageExt>>>>;

/// One Java MQAdmin key or unique-key query across a topic route.
/// The route topic and logical-topic flag are derived from Java's query inputs
/// and stay internal. Builder methods retain the nullable cluster, index-type,
/// and pagination inputs of Java's full `MQAdminImpl.queryMessage` overload:
///
/// ```compile_fail
/// use rocketmq_classic_core::api::admin::AdminQueryRequest;
/// let mut request = AdminQueryRequest::by_key("orders", "k", 32, 0, 1);
/// request.logical_message_queue = true;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminQueryRequest {
    pub(crate) route_topic: String,
    pub(crate) topic: String,
    pub(crate) key: String,
    pub(crate) max_num: i32,
    pub(crate) begin_timestamp: i64,
    pub(crate) end_timestamp: i64,
    pub(crate) index_type: Option<String>,
    pub(crate) last_key: Option<String>,
    pub(crate) unique_key: bool,
    pub(crate) cluster_name: Option<String>,
    pub(crate) logical_message_queue: bool,
}

impl AdminQueryRequest {
    pub fn by_key(
        topic: impl Into<String>,
        key: impl Into<String>,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Self {
        let topic = topic.into();
        Self {
            route_topic: topic.clone(),
            logical_message_queue: topic.starts_with("%LMQ%"),
            topic,
            key: key.into(),
            max_num,
            begin_timestamp,
            end_timestamp,
            index_type: Some(constants::INDEX_KEY_TYPE.to_owned()),
            last_key: None,
            unique_key: false,
            cluster_name: None,
        }
    }

    pub fn by_unique_key(
        topic: impl Into<String>,
        unique_key: impl Into<String>,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Self {
        let topic = topic.into();
        Self {
            route_topic: topic.clone(),
            logical_message_queue: topic.starts_with("%LMQ%"),
            topic,
            key: unique_key.into(),
            max_num,
            begin_timestamp,
            end_timestamp,
            index_type: Some(constants::INDEX_UNIQUE_TYPE.to_owned()),
            last_key: None,
            unique_key: true,
            cluster_name: None,
        }
    }

    /// Applies Java's optional cluster selector. Logical topics and the wheel
    /// timer use the supplied cluster as their route topic; ordinary topics
    /// keep their own route and use the cluster only to filter Brokers.
    pub fn in_cluster(self, cluster_name: impl Into<String>) -> Self {
        self.with_cluster_name(Some(cluster_name.into()))
    }

    /// Applies Java's nullable `clusterName` query input.
    pub fn with_cluster_name(mut self, cluster_name: Option<String>) -> Self {
        self.route_topic = self.topic.clone();
        if self.logical_message_queue || self.topic == "rmq_sys_wheel_timer" {
            if let Some(cluster) = cluster_name
                .as_deref()
                .filter(|cluster| !cluster.is_empty())
            {
                self.route_topic = cluster.to_owned();
            }
        }
        self.cluster_name = cluster_name;
        self
    }

    /// Applies Java's nullable `indexType` query input. A null or empty value
    /// causes the Java query-result filter to return no key matches.
    pub fn with_index_type(mut self, index_type: Option<String>) -> Self {
        self.index_type = index_type;
        self
    }

    /// Applies Java's nullable `lastKey` pagination input.
    pub fn with_last_key(mut self, last_key: Option<String>) -> Self {
        self.last_key = last_key;
        self
    }
}

/// Errors from the route lookup or single-Broker calls used by an admin query.
#[derive(Debug, thiserror::Error)]
pub enum AdminQueryError {
    #[error(transparent)]
    Runtime(#[from] crate::runtime::RuntimeError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
}

impl QueryResult {
    /// Constructs a query result from Java's nullable message-list input.
    pub fn new(index_last_update_timestamp: i64, message_list: Option<QueryMessageList>) -> Self {
        Self {
            index_last_update_timestamp,
            message_list,
        }
    }

    pub fn get_index_last_update_timestamp(&self) -> i64 {
        self.index_last_update_timestamp
    }

    /// Returns a shared mutable list handle, preserving Java's getter alias.
    pub fn get_message_list(&self) -> Option<QueryMessageList> {
        self.message_list.clone()
    }
}

impl std::fmt::Display for QueryResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "QueryResult [indexLastUpdateTimestamp={}, messageList=",
            self.index_last_update_timestamp
        )?;
        match &self.message_list {
            None => formatter.write_str("null")?,
            Some(messages) => {
                formatter.write_str("[")?;
                for (index, message) in messages.snapshot().iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    match message {
                        Some(message) => message.with(|message| write!(formatter, "{message}"))?,
                        None => formatter.write_str("null")?,
                    }
                }
                formatter.write_str("]")?;
            }
        }
        formatter.write_str("]")
    }
}

impl MQClientApi {
    /// Updates configuration on each selected NameServer through Java
    /// `MQClientAPIImpl.updateNameServerConfig`.
    ///
    /// An empty property map is a no-op. A missing or empty address slice uses
    /// the NameServer list configured on the remoting client. The same
    /// absolute deadline is applied to calls in list order.
    pub async fn update_name_server_config(
        &self,
        properties: &HashMap<String, String>,
        name_server_addrs: Option<&[String]>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        if properties.is_empty() {
            return Ok(());
        }

        let mut body = String::new();
        for (key, value) in properties {
            body.push_str(key);
            body.push('=');
            body.push_str(value);
            body.push('\n');
        }
        let body = body.into_bytes();
        let addresses = self.name_server_addresses(name_server_addrs);
        let code = request::UPDATE_NAMESRV_CONFIG;
        let mut last_error = None;
        for addr in addresses {
            let reply = self
                .remoting
                .invoke_sync_with_deadline(
                    &addr,
                    RemotingCommand {
                        code,
                        language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                        body: Some(body.clone()),
                        ..super::java_client_request_command(code)?
                    },
                    deadline,
                )
                .await?;
            if reply.code != response::SUCCESS {
                last_error = Some(RpcError::Response {
                    request_code: code,
                    peer: addr,
                    code: reply.code,
                    remark: reply.remark,
                });
            }
        }
        last_error.map_or(Ok(()), Err)
    }

    /// Retrieves NameServer configuration through Java
    /// `MQClientAPIImpl.getNameServerConfig`.
    ///
    /// A missing or empty address slice uses the remoting client's configured
    /// NameServers. No configured servers returns `None`; malformed Java
    /// Properties are retained as null values in the returned map.
    pub async fn get_name_server_config(
        &self,
        name_server_addrs: Option<&[String]>,
        deadline: Instant,
    ) -> Result<Option<NameServerConfigs>, RpcError> {
        let addresses = self.name_server_addresses(name_server_addrs);
        if addresses.is_empty() {
            return Ok(None);
        }

        let code = request::GET_NAMESRV_CONFIG;
        let mut configurations = HashMap::with_capacity(addresses.len());
        for addr in addresses {
            let reply = self
                .remoting
                .invoke_sync_with_deadline(
                    &addr,
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
                    peer: addr,
                    code: reply.code,
                    remark: reply.remark,
                });
            }
            let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.clone(),
                reason: "successful NameServer-config reply has no body".into(),
            })?;
            let decoded = String::from_utf8_lossy(&body);
            configurations.insert(addr, crate::auth::parse_java_properties(decoded.as_bytes()));
        }
        Ok(Some(configurations))
    }

    /// Reads a NameServer KV value through Java
    /// `MQClientAPIImpl.getKVConfigValue`.
    pub async fn get_kv_config_value(
        &self,
        namespace: Option<&str>,
        key: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<String>, RpcError> {
        let code = request::GET_KV_CONFIG;
        let mut header = GetKvConfigRequestHeader::default();
        header.set_namespace(namespace.map(str::to_owned));
        header.set_key(key.map(str::to_owned));
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
        let reply = classify(reply, code, &peer)?;
        Ok(reply
            .ext_fields
            .as_ref()
            .and_then(|fields| fields.get("value"))
            .cloned())
    }

    /// Writes a KV value to every configured NameServer, following Java
    /// `MQClientAPIImpl.putKVConfigValue`. As in Java, a null or empty
    /// NameServer list makes the operation a no-op, and the last non-success
    /// response is returned after all servers have been contacted.
    pub async fn put_kv_config_value(
        &self,
        namespace: Option<&str>,
        key: Option<&str>,
        value: Option<&str>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let mut header = PutKvConfigRequestHeader::default();
        header.set_namespace(namespace.map(str::to_owned));
        header.set_key(key.map(str::to_owned));
        header.set_value(value.map(str::to_owned));
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        self.broadcast_name_server_kv_request(request::PUT_KV_CONFIG, fields, deadline)
            .await
    }

    /// Deletes a KV value from every configured NameServer, following Java
    /// `MQClientAPIImpl.deleteKVConfigValue` and retaining nullable inputs.
    pub async fn delete_kv_config_value(
        &self,
        namespace: Option<&str>,
        key: Option<&str>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let mut header = DeleteKvConfigRequestHeader::default();
        header.set_namespace(namespace.map(str::to_owned));
        header.set_key(key.map(str::to_owned));
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        self.broadcast_name_server_kv_request(request::DELETE_KV_CONFIG, fields, deadline)
            .await
    }

    /// Lists one NameServer namespace through Java
    /// `MQClientAPIImpl.getKVListByNamespace`.
    pub async fn get_kv_list_by_namespace(
        &self,
        namespace: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<KvTable>, RpcError> {
        let code = request::GET_KVLIST_BY_NAMESPACE;
        let mut header = GetKvListByNamespaceRequestHeader::default();
        header.set_namespace(namespace.map(str::to_owned));
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
        let reply = classify(reply, code, &peer)?;
        KvTable::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer,
                reason: error.to_string(),
            }
        })
    }

    /// Wipes write permission from every topic on a Broker through Java
    /// `MQClientAPIImpl.wipeWritePermOfBroker`'s direct NameServer RPC.
    pub async fn wipe_write_perm_of_broker(
        &self,
        addr: &str,
        broker_name: Option<&str>,
        deadline: Instant,
    ) -> Result<i32, RpcError> {
        let code = request::WIPE_WRITE_PERM_OF_BROKER;
        let mut header = WipeWritePermOfBrokerRequestHeader::default();
        header.set_broker_name(broker_name.map(str::to_owned));
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields.into()),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        let response_header =
            decode_header::<WipeWritePermOfBrokerResponseHeader>(&reply, code, addr)?;
        response_header
            .get_wipe_topic_count()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: rocketmq_classic_protocol::header::ProtocolError::MissingField(
                    "wipeTopicCount",
                )
                .to_string(),
            })
    }

    /// Adds write permission to every topic on a Broker through Java
    /// `MQClientAPIImpl.addWritePermOfBroker`'s direct NameServer RPC.
    pub async fn add_write_perm_of_broker(
        &self,
        addr: &str,
        broker_name: Option<&str>,
        deadline: Instant,
    ) -> Result<i32, RpcError> {
        let code = request::ADD_WRITE_PERM_OF_BROKER;
        let mut header = AddWritePermOfBrokerRequestHeader::default();
        header.set_broker_name(broker_name.map(str::to_owned));
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields.into()),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        let response_header =
            decode_header::<AddWritePermOfBrokerResponseHeader>(&reply, code, addr)?;
        response_header
            .get_add_topic_count()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: rocketmq_classic_protocol::header::ProtocolError::MissingField(
                    "addTopicCount",
                )
                .to_string(),
            })
    }

    /// Registers a Broker through Java `MQClientAPIImpl.addBroker`. The
    /// supplied address and timeout are used directly, without VIP rewriting.
    pub async fn add_broker(
        &self,
        addr: &str,
        config_path: Option<&str>,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::ADD_BROKER;
        let mut header = AddBrokerRequestHeader::default();
        header.set_config_path(config_path.map(str::to_owned));
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Removes a Broker through Java `MQClientAPIImpl.removeBroker`. The
    /// nullable header values are preserved and the supplied address is used
    /// directly, without VIP rewriting.
    pub async fn remove_broker(
        &self,
        addr: &str,
        cluster_name: Option<&str>,
        broker_name: Option<&str>,
        broker_id: i64,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::REMOVE_BROKER;
        let mut header = RemoveBrokerRequestHeader::default();
        header.set_broker_cluster_name(cluster_name.map(str::to_owned));
        header.set_broker_name(broker_name.map(str::to_owned));
        header.set_broker_id(Some(broker_id));
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Clones one consumer group's offsets through Java
    /// `MQClientAPIImpl.cloneGroupOffset`. It retains nullable Java string
    /// inputs, configured VIP address conversion, and the caller's timeout.
    pub async fn clone_group_offset(
        &self,
        addr: &str,
        src_group: Option<&str>,
        dest_group: Option<&str>,
        topic: Option<&str>,
        offline: bool,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::CLONE_GROUP_OFFSET;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut header = CloneGroupOffsetRequestHeader::default();
        header.set_src_group(src_group.map(str::to_owned));
        header.set_dest_group(dest_group.map(str::to_owned));
        header.set_topic(topic.map(str::to_owned));
        header.set_offline(offline);
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Gets a consumer's queue offsets through Java
    /// `MQClientAPIImpl.invokeBrokerToGetConsumerStatus`. Nullable header
    /// strings are omitted exactly as Java's FastCodesHeader does, and a
    /// successful response without a body follows Java's client-error path.
    pub async fn invoke_broker_to_get_consumer_status(
        &self,
        addr: &str,
        topic: Option<&str>,
        group: Option<&str>,
        client_addr: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<rocketmq_classic_protocol::body::SharedConsumerQueueOffsets>, RpcError> {
        let code = request::INVOKE_BROKER_TO_GET_CONSUMER_STATUS;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let header = InvokeBrokerToGetConsumerStatusHeader {
            topic: topic.map(str::to_owned),
            group: group.map(str::to_owned),
            client_addr: client_addr.map(str::to_owned),
        };
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        let Some(body) = reply.body else {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        };
        let body = std::str::from_utf8(&body).map_err(|error| RpcError::MalformedResponse {
            request_code: code,
            peer: addr.to_owned(),
            reason: error.to_string(),
        })?;
        let status = rocketmq_classic_protocol::body::GetConsumerStatusBody::decode_java_json(body)
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })?;
        Ok(status.get_consumer_table())
    }

    async fn broadcast_name_server_kv_request(
        &self,
        code: i32,
        fields: HeaderMap,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            ext_fields: Some(fields.into()),
            ..super::java_client_request_command(code)?
        };
        let mut last_error = None;
        for address in self.name_server_addresses(None) {
            let reply = self
                .remoting
                .invoke_sync_with_deadline(&address, command.clone(), deadline)
                .await?;
            if reply.code != response::SUCCESS {
                last_error = Some(RpcError::Response {
                    request_code: code,
                    peer: address,
                    code: reply.code,
                    remark: reply.remark,
                });
            }
        }
        last_error.map_or(Ok(()), Err)
    }

    fn name_server_addresses(&self, addresses: Option<&[String]>) -> Vec<String> {
        addresses
            .filter(|addresses| !addresses.is_empty())
            .map(<[_]>::to_vec)
            .unwrap_or_else(|| {
                self.remoting
                    .get_name_server_address_list()
                    .unwrap_or_default()
            })
    }

    /// Cleans expired consume queues through Java
    /// `MQClientAPIImpl.cleanExpiredConsumeQueue`.
    pub async fn clean_expired_consume_queue(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        self.clean_broker_data(addr, request::CLEAN_EXPIRED_CONSUMEQUEUE, deadline)
            .await
    }

    /// Deletes expired commit-log data through Java
    /// `MQClientAPIImpl.deleteExpiredCommitLog`.
    pub async fn delete_expired_commit_log(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        self.clean_broker_data(addr, request::DELETE_EXPIRED_COMMITLOG, deadline)
            .await
    }

    /// Cleans unused topics through Java `MQClientAPIImpl.cleanUnusedTopicByAddr`.
    pub async fn clean_unused_topic_by_addr(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        self.clean_broker_data(addr, request::CLEAN_UNUSED_TOPIC, deadline)
            .await
    }

    /// Resets a Broker's master flush offset through Java
    /// `MQClientAPIImpl.resetMasterFlushOffset`. Java uses the supplied
    /// address unchanged and a fixed 3000 ms timeout.
    pub async fn reset_master_flush_offset(
        &self,
        addr: &str,
        master_flush_offset: i64,
    ) -> Result<(), RpcError> {
        let mut header = ResetMasterFlushOffsetHeader::default();
        header.set_master_flush_offset(Some(master_flush_offset));
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        let code = request::RESET_MASTER_FLUSH_OFFSET;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields.into()),
                    ..super::java_client_request_command(code)?
                },
                Instant::now() + Duration::from_millis(3000),
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        Ok(())
    }

    /// Checks RocksDB consume-queue write progress through Java
    /// `MQClientAPIImpl.checkRocksdbCqWriteProgress`.
    ///
    /// A successful response body may be JSON `null` or absent; both produce
    /// `Ok(None)`, matching Fastjson's nullable parse result.
    pub async fn check_rocksdb_cq_write_progress(
        &self,
        addr: &str,
        topic: Option<&str>,
        check_store_time: i64,
        deadline: Instant,
    ) -> Result<Option<CheckRocksdbCqWriteResult>, RpcError> {
        let mut header = CheckRocksdbCqWriteProgressRequestHeader::default();
        header.set_topic(topic.map(str::to_owned));
        header.set_check_store_time(check_store_time);
        let code = request::CHECK_ROCKSDB_CQ_WRITE_PROGRESS;
        let reply = self.request(addr, code, &header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        CheckRocksdbCqWriteResult::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Exports the selected RocksDB configuration through Java
    /// `MQClientAPIImpl.exportRocksDBConfigToJson`.
    pub async fn export_rocksdb_config_to_json(
        &self,
        addr: &str,
        config_types: &[ExportRocksDBConfigType],
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.export_rocksdb_config_to_json_nullable(addr, Some(config_types), deadline)
            .await
    }

    /// Nullable-list adapter for Java's `List<ConfigType>` input. A null list
    /// fails before sending any request, matching Java's header update path.
    pub async fn export_rocksdb_config_to_json_nullable(
        &self,
        addr: &str,
        config_types: Option<&[ExportRocksDBConfigType]>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let config_types = config_types.ok_or_else(|| {
            RpcError::Header(
                rocketmq_classic_protocol::header::ProtocolError::MissingField("configType"),
            )
        })?;
        let mut header = ExportRocksDBConfigToJsonRequestHeader::default();
        header.update_config_type(config_types);
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        let code = request::EXPORT_ROCKSDB_CONFIG_TO_JSON;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                addr,
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
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        Ok(())
    }

    async fn clean_broker_data(
        &self,
        addr: &str,
        code: i32,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(true)
    }

    /// Retrieves one Broker's Java `ClusterInfo` payload.
    pub async fn get_broker_cluster_info(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<Option<ClusterInfo>, RpcError> {
        let code = request::GET_BROKER_CLUSTER_INFO;
        let reply = self
            .request_future_without_header(addr, code, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        ClusterInfo::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Retrieves one Broker's Java `ConsumerConnection` response.
    pub async fn get_consumer_connection_list(
        &self,
        addr: &str,
        header: &GetConsumerConnectionListRequestHeader,
        deadline: Instant,
    ) -> Result<Option<ConsumerConnection>, RpcError> {
        let code = request::GET_CONSUMER_CONNECTION_LIST;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        ConsumerConnection::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Retrieves one Broker's Java `ProducerConnection` response.
    ///
    /// Java `MQClientAPIImpl.getProducerConnectionList` applies the configured
    /// VIP address transform and sends only `producerGroup` in the request
    /// header. The source header's `checkFields` is a no-op, so the nullable
    /// adapter preserves a null group by omitting the wire field.
    pub async fn get_producer_connection_list(
        &self,
        addr: &str,
        producer_group: &str,
        deadline: Instant,
    ) -> Result<Option<ProducerConnection>, RpcError> {
        self.get_producer_connection_list_nullable(addr, Some(producer_group), deadline)
            .await
    }

    /// Nullable-input adapter for Java's `getProducerConnectionList`.
    pub async fn get_producer_connection_list_nullable(
        &self,
        addr: &str,
        producer_group: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<ProducerConnection>, RpcError> {
        let code = request::GET_PRODUCER_CONNECTION_LIST;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| super::read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut fields = HeaderMap::new();
        if let Some(producer_group) = producer_group {
            fields.insert("producerGroup".into(), producer_group.to_owned());
        }
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields.into()),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        ProducerConnection::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Retrieves every producer group and its current connections from one
    /// Broker through Java `MQClientAPIImpl.getAllProducerInfo`.
    pub async fn get_all_producer_info(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<Option<ProducerTableInfo>, RpcError> {
        let code = request::GET_ALL_PRODUCER_INFO;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| super::read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        ProducerTableInfo::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Retrieves the Java `KVTable` runtime snapshot from one Broker.
    pub async fn get_broker_runtime_info(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<Option<KvTable>, RpcError> {
        let code = request::GET_BROKER_RUNTIME_INFO;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| super::read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        KvTable::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Retrieves one Broker's Java `ConsumeStats` response.
    pub async fn get_consume_stats(
        &self,
        addr: &str,
        header: &GetConsumeStatsRequestHeader,
        deadline: Instant,
    ) -> Result<Option<ConsumeStats>, RpcError> {
        let code = request::GET_CONSUME_STATS;
        let reply = self
            .request_future(addr, code, header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        ConsumeStats::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Performs Java `MQClientAPIImpl.getConsumeStats` through synchronous
    /// remoting. `MqClientAdmin::getConsumeStats` remains future-based and uses
    /// [`Self::get_consume_stats`].
    pub async fn get_consume_stats_sync(
        &self,
        addr: &str,
        header: &GetConsumeStatsRequestHeader,
        deadline: Instant,
    ) -> Result<Option<ConsumeStats>, RpcError> {
        let code = request::GET_CONSUME_STATS;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        ConsumeStats::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Queries a Broker's correction offsets through Java
    /// `MQClientAPIImpl.queryCorrectionOffset`. The filter set is serialized
    /// as Java's comma-separated `filterGroups` header value; its iteration
    /// order is unspecified just as it is for the Java `Set` input.
    pub async fn query_correction_offset(
        &self,
        addr: &str,
        topic: Option<&str>,
        compare_group: Option<&str>,
        filter_groups: Option<&HashSet<String>>,
        timeout_millis: i64,
    ) -> Result<Option<HashMap<i32, Option<i64>>>, RpcError> {
        let code = request::QUERY_CORRECTION_OFFSET;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut header = QueryCorrectionOffsetRequestHeader::default();
        header.set_topic(topic.map(str::to_owned));
        header.set_compare_group(compare_group.map(str::to_owned));
        header.set_filter_groups(filter_groups.map(|groups| {
            groups
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(",")
        }));
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        let Some(body) = reply.body else {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        };
        let body = QueryCorrectionOffsetBody::java_decode(&body).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })?;
        Ok(body
            .get_correction_offsets()
            .map(|correction_offsets| correction_offsets.snapshot()))
    }

    /// Returns the direct admin adapter exposed by Java's `MQClientAPIExt`.
    pub fn mq_client_admin(&self) -> MqClientAdmin {
        MqClientAdmin { api: self.clone() }
    }

    /// Gets one Broker's topic statistics and offset table.
    pub async fn get_topic_stats_info(
        &self,
        addr: &str,
        header: &GetTopicStatsInfoRequestHeader,
        deadline: Instant,
    ) -> Result<Option<TopicStatsTable>, RpcError> {
        let code = request::GET_TOPIC_STATS_INFO;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        TopicStatsTable::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Queries one Broker for the consumer groups subscribed to a topic.
    pub async fn query_topic_consume_by_who(
        &self,
        addr: &str,
        header: &QueryTopicConsumeByWhoRequestHeader,
        deadline: Instant,
    ) -> Result<GroupList, RpcError> {
        let code = request::QUERY_TOPIC_CONSUME_BY_WHO;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        decode_body(&reply, code, addr, GroupList::java_decode)
    }

    /// Queries one Broker for a group's subscription to a topic.
    pub async fn query_subscription_by_consumer(
        &self,
        addr: &str,
        header: &QuerySubscriptionByConsumerRequestHeader,
        deadline: Instant,
    ) -> Result<Option<SharedValue<SubscriptionData>>, RpcError> {
        let code = request::QUERY_SUBSCRIPTION_BY_CONSUMER;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        let body: QuerySubscriptionResponseBody = decode_body(
            &reply,
            code,
            addr,
            QuerySubscriptionResponseBody::java_decode,
        )?;
        Ok(body.get_subscription_data())
    }

    /// Queries one Broker for the topics consumed by a group.
    pub async fn query_topics_by_consumer(
        &self,
        addr: &str,
        header: &QueryTopicsByConsumerRequestHeader,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        let code = request::QUERY_TOPICS_BY_CONSUMER;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        decode_body(&reply, code, addr, TopicList::java_decode)
    }

    /// Fans one Java MQAdmin query out to the selected Brokers on a route.
    /// Individual Broker errors and malformed responses are ignored when
    /// another Broker returns matching messages, as in Java's callback path.
    pub async fn query_message_across_brokers(
        &self,
        route: &TopicRouteData,
        query: &AdminQueryRequest,
        deadline: Instant,
    ) -> Result<QueryResult, RpcError> {
        let mut addresses = Vec::new();
        for broker in route.broker_datas_snapshot().unwrap_or_default() {
            let Some(broker) = broker else {
                continue;
            };
            if !query.logical_message_queue
                && query.cluster_name.as_deref().is_some_and(|cluster| {
                    !cluster.is_empty() && broker.cluster.as_deref() != Some(cluster)
                })
            {
                continue;
            }
            match broker.java_select_broker_addr() {
                Ok(Some(address)) => addresses.push(address),
                Ok(None) => {}
                Err(error) => {
                    return Err(RpcError::MalformedResponse {
                        request_code: request::QUERY_MESSAGE,
                        peer: query.route_topic.clone(),
                        reason: error.to_string(),
                    });
                }
            }
        }
        if addresses.is_empty() {
            return Err(no_message_error(&query.topic));
        }

        let request_topic = if query.logical_message_queue {
            query
                .cluster_name
                .as_deref()
                .filter(|cluster| !cluster.is_empty())
                .unwrap_or(&query.topic)
        } else {
            &query.topic
        };
        let header = {
            let mut __phase18_header_0 = QueryMessageRequestHeader::default();
            __phase18_header_0.set_topic(Some(request_topic.to_owned()));
            __phase18_header_0.set_key(Some(query.key.clone()));
            __phase18_header_0.set_max_num(Some(query.max_num));
            __phase18_header_0.set_begin_timestamp(Some(query.begin_timestamp));
            __phase18_header_0.set_end_timestamp(Some(query.end_timestamp));
            __phase18_header_0.set_index_type(query.index_type.clone());
            __phase18_header_0.set_last_key(query.last_key.clone());
            __phase18_header_0
        };
        let mut tasks = JoinSet::new();
        for address in addresses {
            let api = self.clone();
            let header = header.clone();
            let query_is_unique = query.unique_key;
            tasks.spawn(async move {
                api.query_message(&address, &header, query_is_unique, deadline)
                    .await
            });
        }

        let mut watermark = 0;
        let mut messages = Vec::new();
        loop {
            let joined = match timeout_at(deadline, tasks.join_next()).await {
                Ok(joined) => joined,
                Err(_) => {
                    tasks.abort_all();
                    break;
                }
            };
            match joined {
                None => break,
                Some(Ok(Ok(result))) => {
                    watermark = watermark.max(result.index_last_update_timestamp);
                    if let Some(result_messages) = result.message_list {
                        for message in result_messages.snapshot().into_iter().flatten() {
                            if message.with(|message| query_message_matches(message, query)) {
                                messages.push(Some(message));
                            }
                        }
                    }
                }
                Some(_) => continue,
            }
        }

        if messages.is_empty() {
            return Err(RpcError::Response {
                request_code: request::QUERY_MESSAGE,
                peer: query.topic.clone(),
                code: response::NO_MESSAGE,
                remark: Some("query message by key finished, but no message.".into()),
            });
        }
        Ok(QueryResult::new(
            watermark,
            Some(SharedValue::from(messages)),
        ))
    }

    /// Sends Java `MQClientAPIImpl.createTopic` to one Broker.
    pub async fn create_topic(
        &self,
        addr: &str,
        header: &CreateTopicRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_TOPIC;
        if let Some(permission) = header.get_perm() {
            if !PermName::is_valid(permission) {
                return Err(RpcError::ClientValidation {
                    request_code: code,
                    response_code: response::NO_PERMISSION,
                    message: format!("topicPermission value: {permission} is invalid."),
                });
            }
        }
        let reply = self.request(addr, code, header, None, deadline).await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Creates or updates a static topic through Java
    /// `MQClientAPIImpl.createStaticTopic` (request 513). The static mapping
    /// detail is encoded as the request body; this path intentionally does not
    /// add `createTopic`'s local permission validation.
    pub async fn create_static_topic(
        &self,
        addr: &str,
        default_topic: Option<&str>,
        topic_config: &TopicConfig,
        mapping_detail: &TopicQueueMappingDetail,
        force: bool,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_STATIC_TOPIC;
        let mut header = CreateTopicRequestHeader::default();
        header.set_topic(topic_config.topic_name().map(str::to_owned));
        header.set_default_topic(default_topic.map(str::to_owned));
        header.set_read_queue_nums(Some(topic_config.read_queue_nums()));
        header.set_write_queue_nums(Some(topic_config.write_queue_nums()));
        header.set_perm(Some(topic_config.perm()));
        header.set_topic_filter_type(topic_config.topic_filter_type().map(|filter_type| {
            match filter_type {
                TopicConfigFilterType::SingleTag => "SINGLE_TAG".to_owned(),
                TopicConfigFilterType::MultiTag => "MULTI_TAG".to_owned(),
            }
        }));
        header.set_topic_sys_flag(Some(topic_config.topic_sys_flag()));
        header.set_order(Some(topic_config.is_order()));
        header.set_force(Some(force));
        header.check_fields()?;

        let body =
            mapping_detail
                .java_encode()
                .map_err(|source| RpcError::RequestSerialization {
                    request_code: code,
                    source,
                })?;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                Some(body),
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Creates a Broker auth user through Java
    /// `MQClientAPIImpl.createUser` (request 3001). The Broker address is sent
    /// unchanged, even when VIP channels are enabled.
    pub async fn create_user(
        &self,
        addr: &str,
        user_info: &UserInfo,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::AUTH_CREATE_USER;
        let mut header = CreateUserRequestHeader::default();
        header.set_username(user_info.get_username().map(str::to_owned));
        header.check_fields()?;
        let body = user_info
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                Some(body),
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Updates a Broker auth user through Java
    /// `MQClientAPIImpl.updateUser` (request 3002). The Broker address is sent
    /// unchanged, even when VIP channels are enabled.
    pub async fn update_user(
        &self,
        addr: &str,
        user_info: &UserInfo,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::AUTH_UPDATE_USER;
        let mut header = UpdateUserRequestHeader::default();
        header.set_username(user_info.get_username().map(str::to_owned));
        header.check_fields()?;
        let body = user_info
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                Some(body),
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Deletes a Broker auth user through Java
    /// `MQClientAPIImpl.deleteUser` (request 3003). Java accepts a nullable
    /// username and sends the Broker address unchanged, including with VIP enabled.
    pub async fn delete_user(
        &self,
        addr: &str,
        username: Option<&str>,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::AUTH_DELETE_USER;
        let mut header = DeleteUserRequestHeader::default();
        header.set_username(username.map(str::to_owned));
        header.check_fields()?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Retrieves a Broker auth user through Java `MQClientAPIImpl.getUser`
    /// (request 3004). Java accepts a nullable username and response body; the
    /// supplied Broker address is preserved even when VIP channels are enabled.
    pub async fn get_user(
        &self,
        addr: &str,
        username: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<UserInfo>, RpcError> {
        let code = request::AUTH_GET_USER;
        let mut header = GetUserRequestHeader::default();
        header.set_username(username.map(str::to_owned));
        header.check_fields()?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        let decoded = UserInfo::java_decode_nullable(reply.body.as_deref());
        decoded.map_err(|error| RpcError::MalformedResponse {
            request_code: code,
            peer: addr.to_owned(),
            reason: error.to_string(),
        })
    }

    /// Retrieves a Broker ACL through Java `MQClientAPIImpl.getAcl`
    /// (request 3009). Java accepts a nullable subject and response body; the
    /// supplied Broker address is preserved even when VIP channels are enabled.
    pub async fn get_acl(
        &self,
        addr: &str,
        subject: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<AclInfo>, RpcError> {
        let code = request::AUTH_GET_ACL;
        let mut header = GetAclRequestHeader::default();
        header.set_subject(subject.map(str::to_owned));
        header.check_fields()?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        AclInfo::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Creates a Broker ACL through Java `MQClientAPIImpl.createAcl`
    /// (request 3006). The ACL body and nullable subject use the pinned Java
    /// DTO shape; the supplied Broker address is unchanged with VIP enabled.
    pub async fn create_acl(
        &self,
        addr: &str,
        acl_info: &AclInfo,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::AUTH_CREATE_ACL;
        let mut header = CreateAclRequestHeader::default();
        header.set_subject(acl_info.get_subject().map(str::to_owned));
        header.check_fields()?;
        let body = acl_info
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                Some(body),
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Updates a Broker ACL through Java `MQClientAPIImpl.updateAcl`
    /// (request 3007). It reuses the Java-compatible `AclInfo` body and keeps
    /// the supplied Broker address unchanged when VIP channels are enabled.
    pub async fn update_acl(
        &self,
        addr: &str,
        acl_info: &AclInfo,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::AUTH_UPDATE_ACL;
        let mut header = UpdateAclRequestHeader::default();
        header.set_subject(acl_info.get_subject().map(str::to_owned));
        header.check_fields()?;
        let body = acl_info
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                Some(body),
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Deletes a Broker ACL through Java `MQClientAPIImpl.deleteAcl`
    /// (request 3008). Java accepts nullable subject/resource fields, leaves
    /// `policyType` unset, and sends the supplied Broker address unchanged.
    pub async fn delete_acl(
        &self,
        addr: &str,
        subject: Option<&str>,
        resource: Option<&str>,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let code = request::AUTH_DELETE_ACL;
        let mut header = DeleteAclRequestHeader::default();
        header.set_subject(subject.map(str::to_owned));
        header.set_resource(resource.map(str::to_owned));
        header.check_fields()?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Lists Broker auth users through Java `MQClientAPIImpl.listUser`
    /// (request 3005). Java accepts a nullable filter and preserves a null
    /// response list and null list members.
    pub async fn list_user(
        &self,
        addr: &str,
        filter: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<Vec<Option<UserInfo>>>, RpcError> {
        let code = request::AUTH_LIST_USER;
        let mut header = ListUsersRequestHeader::default();
        header.set_filter(filter.map(str::to_owned));
        header.check_fields()?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        UserInfo::java_decode_list_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Lists Broker ACLs through Java `MQClientAPIImpl.listAcl` (request
    /// 3010). Java accepts nullable subject/resource filters and returns a
    /// nullable list with nullable `AclInfo` members.
    pub async fn list_acl(
        &self,
        addr: &str,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<Vec<Option<AclInfo>>>, RpcError> {
        let code = request::AUTH_LIST_ACL;
        let mut header = ListAclsRequestHeader::default();
        header.set_subject_filter(subject_filter.map(str::to_owned));
        header.set_resource_filter(resource_filter.map(str::to_owned));
        header.check_fields()?;
        let reply = self
            .request(
                addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        AclInfo::java_decode_list_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Reads one Broker's topic configuration and static-queue mapping through
    /// Java `MQClientAPIImpl.getTopicConfig`.
    pub async fn get_topic_config(
        &self,
        addr: &str,
        topic: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<TopicConfigAndQueueMapping>, RpcError> {
        let code = request::GET_TOPIC_CONFIG;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut header = GetTopicConfigRequestHeader::default();
        header.set_topic(topic.map(str::to_owned));
        header.set_lo(Some(true));
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        TopicConfigAndQueueMapping::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Reads a Broker's consume-queue entries through Java
    /// `MQClientAPIImpl.queryConsumeQueue` (request 321).
    #[allow(clippy::too_many_arguments)]
    pub async fn query_consume_queue(
        &self,
        addr: &str,
        topic: Option<&str>,
        queue_id: i32,
        index: i64,
        count: i32,
        consumer_group: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<QueryConsumeQueueResponseBody>, RpcError> {
        let code = request::QUERY_CONSUME_QUEUE;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut header = QueryConsumeQueueRequestHeader::default();
        header.set_topic(topic.map(str::to_owned));
        header.set_queue_id(queue_id);
        header.set_index(index);
        header.set_count(count);
        header.set_consumer_group(consumer_group.map(str::to_owned));
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        QueryConsumeQueueResponseBody::java_decode_nullable(reply.body.as_deref()).map_err(
            |error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            },
        )
    }

    /// Reads one Broker's complete topic configuration snapshot through Java
    /// `MQClientAPIImpl.getAllTopicConfig`, including its version-checked pages.
    pub async fn get_all_topic_config(
        &self,
        addr: &str,
        timeout_millis: i64,
    ) -> Result<TopicConfigSerializeWrapper, RpcError> {
        let code = request::GET_ALL_TOPIC_CONFIG;
        let deadline = deadline_after_timeout_millis(timeout_millis);
        let mut current_data_version: Option<DataVersion> = None;
        let mut topic_seq = 0_i32;
        let mut topic_config_table = HashMap::new();
        let (vip_enabled, max_topic_num) = self
            .config
            .as_ref()
            .map(|config| {
                let config = read_shared_client_config(config);
                (
                    config.vip_channel_enabled(),
                    config.max_page_size_in_get_metadata(),
                )
            })
            .unwrap_or((false, 2_000));
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;

        loop {
            let data_version_json = match current_data_version.as_ref() {
                Some(data_version) => data_version.java_to_json(false).map_err(|source| {
                    RpcError::RequestSerialization {
                        request_code: code,
                        source,
                    }
                })?,
                None => String::new(),
            };
            let mut header = GetAllTopicConfigRequestHeader::default();
            header.set_topic_seq(Some(topic_seq));
            header.set_data_version(Some(data_version_json));
            header.set_max_topic_num(Some(max_topic_num));

            let reply = self
                .request(&broker_addr, code, &header, None, deadline)
                .await?;
            if reply.code != response::SUCCESS {
                return Err(RpcError::Response {
                    request_code: code,
                    peer: addr.to_owned(),
                    code: reply.code,
                    remark: reply.remark,
                });
            }
            let body = reply
                .body
                .as_deref()
                .ok_or_else(|| RpcError::MalformedResponse {
                    request_code: code,
                    peer: addr.to_owned(),
                    reason: "missing response body".into(),
                })?;
            let page = TopicConfigSerializeWrapper::java_decode(body).map_err(|error| {
                RpcError::MalformedResponse {
                    request_code: code,
                    peer: addr.to_owned(),
                    reason: error.to_string(),
                }
            })?;
            let page_table =
                page.topic_config_table()
                    .ok_or_else(|| RpcError::MalformedResponse {
                        request_code: code,
                        peer: addr.to_owned(),
                        reason: "response field `topicConfigTable` is null".into(),
                    })?;
            topic_config_table.extend(
                page_table
                    .iter()
                    .map(|(topic, config)| (topic.clone(), config.clone())),
            );
            topic_seq = topic_seq.wrapping_add(page_table.len() as i32);

            let new_data_version = page.data_version().cloned();
            if current_data_version.is_none() {
                // Java captures the first version before checking the optional
                // response total for compatibility with older Brokers.
                current_data_version = new_data_version.clone();
            }
            let response_header = reply
                .ext_fields
                .as_ref()
                .and_then(|fields| GetAllTopicConfigResponseHeader::decode_fields(fields).ok())
                .unwrap_or_default();
            let Some(total_topic_num) = response_header.get_total_topic_num() else {
                // Java 5.5.1 treats a response without totalTopicNum as a
                // pre-pagination Broker and accepts this complete body.
                break;
            };

            if current_data_version != new_data_version {
                current_data_version = new_data_version;
                topic_seq = 0;
                topic_config_table.clear();
                continue;
            }

            if topic_seq >= total_topic_num.wrapping_sub(1) {
                break;
            }
        }

        Ok(TopicConfigSerializeWrapper::new(
            current_data_version,
            Some(topic_config_table.into_iter().collect()),
        ))
    }

    /// Reads one Broker's complete subscription-group configuration snapshot
    /// through Java `MQClientAPIImpl.getAllSubscriptionGroup`, including its
    /// version-checked pages and forbidden-topic settings.
    pub async fn get_all_subscription_group(
        &self,
        addr: &str,
        timeout_millis: i64,
    ) -> Result<SubscriptionGroupWrapper, RpcError> {
        let code = request::GET_ALL_SUBSCRIPTIONGROUP_CONFIG;
        let deadline = deadline_after_timeout_millis(timeout_millis);
        let mut current_data_version: Option<DataVersion> = None;
        let mut group_seq = 0_i32;
        let mut subscription_group_table = BTreeMap::new();
        let mut forbidden_table = BTreeMap::new();
        let (vip_enabled, max_group_num) = self
            .config
            .as_ref()
            .map(|config| {
                let config = read_shared_client_config(config);
                (
                    config.vip_channel_enabled(),
                    config.max_page_size_in_get_metadata(),
                )
            })
            .unwrap_or((false, 2_000));
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;

        loop {
            let data_version_json = match current_data_version.as_ref() {
                Some(data_version) => data_version.java_to_json(false).map_err(|source| {
                    RpcError::RequestSerialization {
                        request_code: code,
                        source,
                    }
                })?,
                None => String::new(),
            };
            let mut header = GetAllSubscriptionGroupRequestHeader::default();
            header.set_group_seq(Some(group_seq));
            header.set_data_version(Some(data_version_json));
            header.set_max_group_num(Some(max_group_num));

            let reply = self
                .request(&broker_addr, code, &header, None, deadline)
                .await?;
            if reply.code != response::SUCCESS {
                return Err(RpcError::Response {
                    request_code: code,
                    peer: addr.to_owned(),
                    code: reply.code,
                    remark: reply.remark,
                });
            }
            let body = reply
                .body
                .as_deref()
                .ok_or_else(|| RpcError::MalformedResponse {
                    request_code: code,
                    peer: addr.to_owned(),
                    reason: "missing response body".into(),
                })?;
            let page = SubscriptionGroupWrapper::java_decode(body).map_err(|error| {
                RpcError::MalformedResponse {
                    request_code: code,
                    peer: addr.to_owned(),
                    reason: error.to_string(),
                }
            })?;
            let page_group_table =
                page.subscription_group_table()
                    .ok_or_else(|| RpcError::MalformedResponse {
                        request_code: code,
                        peer: addr.to_owned(),
                        reason: "response field `subscriptionGroupTable` is null".into(),
                    })?;
            let page_forbidden_table =
                page.forbidden_table()
                    .ok_or_else(|| RpcError::MalformedResponse {
                        request_code: code,
                        peer: addr.to_owned(),
                        reason: "response field `forbiddenTable` is null".into(),
                    })?;
            subscription_group_table.extend(
                page_group_table
                    .iter()
                    .map(|(group, config)| (group.clone(), config.clone())),
            );
            forbidden_table.extend(
                page_forbidden_table
                    .iter()
                    .map(|(group, values)| (group.clone(), values.clone())),
            );
            group_seq = group_seq.wrapping_add(page_group_table.len() as i32);

            let new_data_version = page.data_version().cloned();
            if current_data_version.is_none() {
                // Java captures the first version before checking the optional
                // response total for compatibility with older Brokers.
                current_data_version = new_data_version.clone();
            }
            let response_header = reply
                .ext_fields
                .as_ref()
                .and_then(|fields| {
                    GetAllSubscriptionGroupResponseHeader::decode_fields(fields).ok()
                })
                .unwrap_or_default();
            let Some(total_group_num) = response_header.get_total_group_num() else {
                // Java 5.5.1 treats a response without totalGroupNum as a
                // pre-pagination Broker and accepts this complete body.
                break;
            };

            if current_data_version != new_data_version {
                current_data_version = new_data_version;
                group_seq = 0;
                subscription_group_table.clear();
                forbidden_table.clear();
                continue;
            }

            if group_seq >= total_group_num.wrapping_sub(1) {
                break;
            }
        }

        Ok(SubscriptionGroupWrapper::new(
            current_data_version,
            Some(subscription_group_table),
            Some(forbidden_table),
        ))
    }

    /// Reads a Broker's subscription-group configuration through Java
    /// `MQClientAPIImpl.getSubscriptionGroupConfig`.
    pub async fn get_subscription_group_config(
        &self,
        addr: &str,
        group: &str,
        deadline: Instant,
    ) -> Result<Option<SubscriptionGroupConfig>, RpcError> {
        self.get_subscription_group_config_nullable(addr, Some(group), deadline)
            .await
    }

    /// Nullable-input adapter for Java's `getSubscriptionGroupConfig`.
    /// Java's no-op header validation accepts a null or empty group.
    pub async fn get_subscription_group_config_nullable(
        &self,
        addr: &str,
        group: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<SubscriptionGroupConfig>, RpcError> {
        let code = request::GET_SUBSCRIPTIONGROUP_CONFIG;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut header = GetSubscriptionGroupConfigRequestHeader::default();
        header.set_group(group.map(str::to_owned));
        let reply = self
            .request(&broker_addr, code, &header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        reply
            .body
            .as_deref()
            .map(SubscriptionGroupConfig::java_decode)
            .transpose()
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })
    }

    /// Updates and reads a Broker's group-forbidden flag through Java
    /// `MQClientAPIImpl.updateAndGetGroupForbidden`. The request preserves the
    /// nullable header fields and applies the configured VIP address mapping.
    pub async fn update_and_get_group_forbidden(
        &self,
        addr: &str,
        header: &UpdateGroupForbiddenRequestHeader,
        timeout_millis: i64,
    ) -> Result<Option<GroupForbidden>, RpcError> {
        let code = request::UPDATE_AND_GET_GROUP_FORBIDDEN;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .request(
                &broker_addr,
                code,
                header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        GroupForbidden::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Reads a Broker's HA runtime status through Java
    /// `MQClientAPIImpl.getBrokerHAStatus`. Java sends this request without a
    /// custom header, request body, or VIP address transformation.
    pub async fn get_broker_ha_status(
        &self,
        addr: &str,
        timeout_millis: i64,
    ) -> Result<Option<HaRuntimeInfo>, RpcError> {
        let code = request::GET_BROKER_HA_STATUS;
        let reply = self
            .request_without_header(
                addr,
                code,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        HaRuntimeInfo::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Fetches the aggregate consumer offsets from a Broker through Java
    /// `MQClientAPIImpl.fetchConsumeStatsInBroker`. The request preserves the
    /// primitive order flag and uses configured VIP conversion.
    pub async fn fetch_consume_stats_in_broker(
        &self,
        addr: &str,
        is_order: bool,
        timeout_millis: i64,
    ) -> Result<ConsumeStatsList, RpcError> {
        let code = request::GET_BROKER_CONSUME_STATS;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut header = GetConsumeStatsInBrokerRequestHeader::default();
        header.set_is_order(is_order);
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        if reply.code == response::SUCCESS {
            if let Some(body) = reply.body.as_deref() {
                return ConsumeStatsList::decode_java_json(&String::from_utf8_lossy(body)).map_err(
                    |error| RpcError::MalformedResponse {
                        request_code: code,
                        peer: addr.to_owned(),
                        reason: error.to_string(),
                    },
                );
            }
        }
        Err(RpcError::Response {
            request_code: code,
            peer: addr.to_owned(),
            code: reply.code,
            remark: reply.remark,
        })
    }

    /// Reads one Broker's minute, hour, and day statistics through Java
    /// `MQClientAPIImpl.viewBrokerStatsData` with configured VIP conversion.
    pub async fn view_broker_stats_data(
        &self,
        addr: &str,
        stats_name: Option<&str>,
        stats_key: Option<&str>,
        timeout_millis: i64,
    ) -> Result<Option<BrokerStatsData>, RpcError> {
        let code = request::VIEW_BROKER_STATS_DATA;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let mut header = ViewBrokerStatsDataRequestHeader::default();
        header.set_stats_name(stats_name.map(str::to_owned));
        header.set_stats_key(stats_key.map(str::to_owned));
        let reply = self
            .request(
                &broker_addr,
                code,
                &header,
                None,
                deadline_after_timeout_millis(timeout_millis),
            )
            .await?;
        if reply.code == response::SUCCESS {
            if let Some(body) = reply.body.as_deref() {
                return BrokerStatsData::java_decode_nullable(Some(body)).map_err(|error| {
                    RpcError::MalformedResponse {
                        request_code: code,
                        peer: addr.to_owned(),
                        reason: error.to_string(),
                    }
                });
            }
        }
        Err(RpcError::Response {
            request_code: code,
            peer: addr.to_owned(),
            code: reply.code,
            remark: reply.remark,
        })
    }

    /// Creates or updates one Broker's subscription group through Java
    /// `MQClientAPIImpl.createSubscriptionGroup`. This synchronous low-level
    /// API applies the configured VIP address transform; the separate
    /// `MqClientAdmin.updateOrCreateSubscriptionGroup` operation does not.
    pub async fn create_subscription_group(
        &self,
        addr: &str,
        config: Option<&SubscriptionGroupConfig>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP;
        let body = config
            .map(|config| config.java_encode())
            .transpose()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    body,
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Creates a batch of subscription groups through Java
    /// `MQClientAPIImpl.createSubscriptionGroupList`.
    pub async fn create_subscription_group_list(
        &self,
        addr: &str,
        configs: &[SubscriptionGroupConfig],
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST;
        let body = SubscriptionGroupList::new(configs.to_vec())
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    body: Some(body),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Creates a batch of topics through Java `MQClientAPIImpl.createTopicList`.
    pub async fn create_topic_list(
        &self,
        addr: &str,
        configs: &[TopicConfig],
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.create_topic_list_body(
            addr,
            CreateTopicListRequestBody::new(configs.to_vec()),
            deadline,
        )
        .await
    }

    /// Nullable adapter for Java `createTopicList(String,List,long)`. Java
    /// accepts a null list and null elements and serializes them as omitted
    /// `topicConfigList` and JSON null array entries, respectively.
    pub async fn create_topic_list_nullable(
        &self,
        addr: &str,
        configs: Option<&[Option<TopicConfig>]>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.create_topic_list_body(
            addr,
            CreateTopicListRequestBody::new_nullable(configs.map(<[_]>::to_vec)),
            deadline,
        )
        .await
    }

    async fn create_topic_list_body(
        &self,
        addr: &str,
        request_body: CreateTopicListRequestBody,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_TOPIC_LIST;
        let body = request_body
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    body: Some(body),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Sends Java `MQClientAPIImpl.updateBrokerConfig` to one Broker.
    ///
    /// Java validates an optional `brokerPermission` property before encoding
    /// Properties as UTF-8 `key=value\n` lines. An empty Properties object is
    /// a successful no-op.
    pub async fn update_broker_config(
        &self,
        addr: &str,
        properties: &HashMap<String, String>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_BROKER_CONFIG;
        if let Some(value) = properties.get("brokerPermission") {
            let valid = PermName::is_valid_string(value)?;
            if !valid {
                return Err(RpcError::ClientValidation {
                    request_code: code,
                    response_code: response::NO_PERMISSION,
                    message: format!("brokerPermission value: {value} is invalid."),
                });
            }
        }

        if properties.is_empty() {
            return Ok(());
        }

        let mut body = String::new();
        for (key, value) in properties {
            body.push_str(key);
            body.push('=');
            body.push_str(value);
            body.push('\n');
        }
        let body = body.into_bytes();
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            body: Some(body),
            ..super::java_client_request_command(code)?
        };
        let reply = self
            .remoting
            .invoke_sync_with_deadline(&broker_addr, command, deadline)
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Fetches Broker Properties through Java `MQClientAPIImpl.getBrokerConfig`.
    ///
    /// Java returns an empty `Properties` for an empty body and null when the
    /// Properties stream is malformed. A missing body fails before parsing.
    pub async fn get_broker_config(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<Option<HashMap<String, String>>, RpcError> {
        let code = request::GET_BROKER_CONFIG;
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            ..super::java_client_request_command(code)?
        };
        let reply = self
            .remoting
            .invoke_sync_with_deadline(addr, command, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        let Some(body) = reply.body else {
            return Err(RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "successful broker-config reply has no body".into(),
            });
        };
        let decoded = String::from_utf8_lossy(&body);
        Ok(crate::auth::parse_java_properties(decoded.as_bytes()))
    }

    /// Updates Java `MQClientAPIImpl` cold-data flow control Properties.
    ///
    /// An empty map is a no-op. Non-empty values are encoded as UTF-8
    /// `key=value\n` lines and sent to the Broker's VIP address when enabled.
    pub async fn update_cold_data_flow_ctr_group_config(
        &self,
        addr: &str,
        properties: &HashMap<String, String>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        if properties.is_empty() {
            return Ok(());
        }
        let mut body = String::new();
        for (key, value) in properties {
            body.push_str(key);
            body.push('=');
            body.push_str(value);
            body.push('\n');
        }
        let code = request::UPDATE_COLD_DATA_FLOW_CTR_CONFIG;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            body: Some(body.into_bytes()),
            ..super::java_client_request_command(code)?
        };
        let reply = self
            .remoting
            .invoke_sync_with_deadline(&broker_addr, command, deadline)
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Removes one Java cold-data flow control group configuration.
    ///
    /// Java does nothing for a null or empty group. A non-empty group is sent
    /// as a UTF-8 body to the configured Broker VIP address.
    pub async fn remove_cold_data_flow_ctr_group_config(
        &self,
        addr: &str,
        consumer_group: Option<&str>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let Some(consumer_group) = consumer_group.filter(|group| !group.is_empty()) else {
            return Ok(());
        };
        let code = request::REMOVE_COLD_DATA_FLOW_CTR_CONFIG;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            body: Some(consumer_group.as_bytes().to_vec()),
            ..super::java_client_request_command(code)?
        };
        let reply = self
            .remoting
            .invoke_sync_with_deadline(&broker_addr, command, deadline)
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Reads the Broker's cold-data flow control status text.
    ///
    /// Java sends request 2003 to the supplied address without VIP
    /// transformation and returns null for an absent or empty success body.
    pub async fn get_cold_data_flow_ctr_info(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<Option<String>, RpcError> {
        let code = request::GET_COLD_DATA_FLOW_CTR_INFO;
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            ..super::java_client_request_command(code)?
        };
        let reply = self
            .remoting
            .invoke_sync_with_deadline(addr, command, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        Ok(reply
            .body
            .filter(|body| !body.is_empty())
            .map(|body| String::from_utf8_lossy(&body).into_owned()))
    }

    /// Retrieves a Broker's system-topic list through Java
    /// `MQClientAPIImpl.getSystemTopicListFromBroker`.
    pub async fn get_system_topic_list_from_broker(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        let code = request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        let body = reply
            .body
            .as_deref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "successful system-topic-list reply has no body".into(),
            })?;
        TopicList::java_decode(body).map_err(|error| RpcError::MalformedResponse {
            request_code: code,
            peer: addr.to_owned(),
            reason: error.to_string(),
        })
    }

    /// Sets the CommitLog read-ahead mode and returns Java's non-empty remark.
    ///
    /// The request uses the `READ_AHEAD_MODE` extension field and the supplied
    /// Broker address directly. Java's RocketMQ serializer omits a null mode
    /// value from the wire, so `None` produces no extension fields.
    pub async fn set_commit_log_read_ahead_mode(
        &self,
        addr: &str,
        mode: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<String>, RpcError> {
        let code = request::SET_COMMITLOG_READ_MODE;
        let command = RemotingCommand {
            code,
            language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
            ext_fields: mode.map(|mode| [("READ_AHEAD_MODE".to_owned(), mode.to_owned())].into()),
            ..super::java_client_request_command(code)?
        };
        let reply = self
            .remoting
            .invoke_sync_with_deadline(addr, command, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        Ok(reply.remark.filter(|remark| !remark.is_empty()))
    }

    /// Deletes one topic from a Broker through Java `MqClientAdmin`'s RPC.
    pub async fn delete_topic_in_broker(
        &self,
        addr: &str,
        header: &DeleteTopicRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::DELETE_TOPIC_IN_BROKER;
        let reply = self.request(addr, code, header, None, deadline).await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Deletes multiple topics from a Broker through Java
    /// `MQClientAPIImpl.deleteTopicInBrokerList`.
    pub async fn delete_topic_in_broker_list(
        &self,
        addr: &str,
        topics: &[String],
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let nullable_topics = topics.iter().cloned().map(Some).collect::<Vec<_>>();
        self.delete_topic_in_broker_list_nullable(addr, Some(&nullable_topics), deadline)
            .await
    }

    /// Nullable adapter preserving Java's null list and null topic entries.
    pub async fn delete_topic_in_broker_list_nullable(
        &self,
        addr: &str,
        topics: Option<&[Option<String>]>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::DELETE_TOPIC_IN_BROKER_LIST;
        let body = DeleteTopicListRequestBody::new_nullable(topics.map(<[_]>::to_vec))
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        self.delete_batch_request(addr, code, body, deadline).await
    }

    /// Deletes one topic from a NameServer through Java `MqClientAdmin`'s RPC.
    pub async fn delete_topic_in_nameserver(
        &self,
        addr: &str,
        header: &DeleteTopicFromNamesrvRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::DELETE_TOPIC_IN_NAMESRV;
        let reply = self.request(addr, code, header, None, deadline).await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Deletes a NameServer KV config entry through Java `MqClientAdmin`'s RPC.
    pub async fn delete_kv_config(
        &self,
        addr: &str,
        header: &DeleteKvConfigRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::DELETE_KV_CONFIG;
        let reply = self.request(addr, code, header, None, deadline).await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Deletes a subscription group from a Broker through Java `MqClientAdmin`'s RPC.
    pub async fn delete_subscription_group(
        &self,
        addr: &str,
        header: &DeleteSubscriptionGroupRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::DELETE_SUBSCRIPTIONGROUP;
        let reply = self.request(addr, code, header, None, deadline).await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Deletes multiple subscription groups through Java
    /// `MQClientAPIImpl.deleteSubscriptionGroupList`.
    pub async fn delete_subscription_group_list(
        &self,
        addr: &str,
        group_names: &[String],
        clean_offset: bool,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let nullable_groups = group_names.iter().cloned().map(Some).collect::<Vec<_>>();
        self.delete_subscription_group_list_nullable(
            addr,
            Some(&nullable_groups),
            clean_offset,
            deadline,
        )
        .await
    }

    /// Nullable adapter preserving Java's null group list and null entries.
    pub async fn delete_subscription_group_list_nullable(
        &self,
        addr: &str,
        group_names: Option<&[Option<String>]>,
        clean_offset: bool,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::DELETE_SUBSCRIPTION_GROUP_LIST;
        let body = DeleteSubscriptionGroupListRequestBody::new_nullable(
            group_names.map(<[_]>::to_vec),
            clean_offset,
        )
        .java_encode()
        .map_err(|source| RpcError::RequestSerialization {
            request_code: code,
            source,
        })?;
        self.delete_batch_request(addr, code, body, deadline).await
    }

    async fn delete_batch_request(
        &self,
        addr: &str,
        code: i32,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                &broker_addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    body: Some(body),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Invokes one Broker's Java `MqClientAdmin.invokeBrokerToResetOffset` RPC.
    pub async fn invoke_broker_to_reset_offset(
        &self,
        addr: &str,
        header: &ResetOffsetRequestHeader,
        deadline: Instant,
    ) -> Result<Option<SharedQueueOffsetEntries>, RpcError> {
        self.invoke_broker_to_reset_offset_with_language(
            addr,
            header,
            rocketmq_classic_protocol::codes::LanguageCode::Java,
            deadline,
        )
        .await
    }

    /// Invokes Java `MQClientAPIImpl.invokeBrokerToResetOffset(..., isC)` with
    /// the request language selected by the caller. `LanguageCode::Cpp` maps
    /// the Java overload's `isC = true` branch.
    pub async fn invoke_broker_to_reset_offset_with_language(
        &self,
        addr: &str,
        header: &ResetOffsetRequestHeader,
        language: rocketmq_classic_protocol::codes::LanguageCode,
        deadline: Instant,
    ) -> Result<Option<SharedQueueOffsetEntries>, RpcError> {
        let code = request::INVOKE_BROKER_TO_RESET_OFFSET;
        let reply = self
            .request_with_mode_and_language(
                addr,
                code,
                header,
                None,
                deadline,
                true,
                Some(language),
            )
            .await?;
        let reply = classify(reply, code, addr)?;
        let body_bytes = reply.body.as_deref().ok_or_else(|| RpcError::Response {
            request_code: code,
            peer: addr.to_owned(),
            code: reply.code,
            remark: reply.remark.clone(),
        })?;
        let response_body =
            std::str::from_utf8(body_bytes).map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })?;
        let body: ResetOffsetBody =
            ResetOffsetBody::decode_java_json(response_body).map_err(|error| {
                RpcError::MalformedResponse {
                    request_code: code,
                    peer: addr.to_owned(),
                    reason: error.to_string(),
                }
            })?;
        Ok(body.get_offset_table())
    }

    /// Adapts Java `MQClientAPIImpl.invokeBrokerToResetOffset(addr, topic,
    /// group, timestamp, isForce, timeout, isC)`, including its default
    /// `queueId = -1` and `offset = -1` request-header fields.
    #[allow(clippy::too_many_arguments)]
    pub async fn invoke_broker_to_reset_offset_for_client(
        &self,
        addr: &str,
        topic: &str,
        group: &str,
        timestamp: i64,
        is_force: bool,
        is_cpp: bool,
        deadline: Instant,
    ) -> Result<Option<SharedQueueOffsetEntries>, RpcError> {
        let mut header = ResetOffsetRequestHeader::default();
        header.set_topic(topic.to_owned());
        header.set_group(group.to_owned());
        header.set_queue_id(-1);
        header.set_timestamp(timestamp);
        header.set_force(is_force);
        header.set_offset(Some(-1));
        let language = if is_cpp {
            rocketmq_classic_protocol::codes::LanguageCode::Cpp
        } else {
            rocketmq_classic_protocol::codes::LanguageCode::Java
        };
        self.invoke_broker_to_reset_offset_with_language(addr, &header, language, deadline)
            .await
    }

    /// Adapts Java `MQClientAPIImpl.invokeBrokerToResetOffset(addr, topic,
    /// group, timestamp, queueId, offset, timeoutMillis)`.
    #[allow(clippy::too_many_arguments)]
    pub async fn invoke_broker_to_reset_offset_at_offset(
        &self,
        addr: &str,
        topic: &str,
        group: &str,
        timestamp: i64,
        queue_id: i32,
        offset: Option<i64>,
        deadline: Instant,
    ) -> Result<Option<SharedQueueOffsetEntries>, RpcError> {
        let mut header = ResetOffsetRequestHeader::default();
        header.set_topic(topic.to_owned());
        header.set_group(group.to_owned());
        header.set_queue_id(queue_id);
        header.set_timestamp(timestamp);
        header.set_offset(offset);
        self.invoke_broker_to_reset_offset_with_language(
            addr,
            &header,
            rocketmq_classic_protocol::codes::LanguageCode::Java,
            deadline,
        )
        .await
    }

    /// Queries one Broker using Java's `_UNIQUE_KEY_QUERY` extension field.
    /// The admin facade is responsible for route selection, fan-out and result
    /// merge; this method performs exactly one Broker request.
    pub async fn query_message(
        &self,
        addr: &str,
        header: &QueryMessageRequestHeader,
        unique_key: bool,
        deadline: Instant,
    ) -> Result<QueryResult, RpcError> {
        let code = request::QUERY_MESSAGE;
        let reply = self
            .query_message_response(addr, header, unique_key, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        let response_header: QueryMessageResponseHeader = decode_header(&reply, code, addr)?;
        let body = reply
            .body
            .as_ref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "missing response body".into(),
            })?;
        let message_list = decode_many_as_client_compatible(body, DecodeOptions::default());
        let index_last_update_timestamp = response_header
            .get_index_last_update_timestamp()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "missing or invalid response header field `indexLastUpdateTimestamp`"
                    .into(),
            })?;
        Ok(QueryResult::new(
            index_last_update_timestamp,
            Some(SharedValue::from(
                message_list
                    .into_iter()
                    .map(|message| Some(SharedValue::from(message)))
                    .collect::<Vec<_>>(),
            )),
        ))
    }

    async fn query_message_response(
        &self,
        addr: &str,
        header: &QueryMessageRequestHeader,
        unique_key: bool,
        deadline: Instant,
    ) -> Result<RemotingCommand, RpcError> {
        let mut fields = rocketmq_classic_protocol::header::HeaderMap::new();
        header.encode_fields(&mut fields);
        fields.insert("_UNIQUE_KEY_QUERY".into(), unique_key.to_string());
        self.request_fields_with_mode(addr, request::QUERY_MESSAGE, fields, None, deadline, false)
            .await
    }

    /// Views one physical message by the Broker's commit-log offset.
    pub async fn view_message(
        &self,
        addr: &str,
        header: &ViewMessageRequestHeader,
        deadline: Instant,
    ) -> Result<MessageExt, RpcError> {
        let code = request::VIEW_MESSAGE_BY_ID;
        let reply = self
            .request_future(addr, code, header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        let body = reply
            .body
            .as_ref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "missing response body".into(),
            })?;
        decode_one(body, DecodeOptions::default())
            .map(|(mut message, _)| {
                client_decode_message(&mut message);
                message
            })
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })
    }

    /// Invokes Java `MqClientAdmin.consumeMessageDirectly` on one Broker.
    pub async fn consume_message_directly(
        &self,
        addr: &str,
        header: &ConsumeMessageDirectlyResultRequestHeader,
        deadline: Instant,
    ) -> Result<ConsumeMessageDirectlyResult, RpcError> {
        let code = request::CONSUME_MESSAGE_DIRECTLY;
        let reply = self
            .request_future(addr, code, header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        decode_body(
            &reply,
            code,
            addr,
            ConsumeMessageDirectlyResult::java_decode,
        )
    }
}

impl MqClientAdmin {
    /// Retrieves one Broker's consumer runtime details through Java
    /// `MqClientAdmin.getConsumerRunningInfo`.
    pub async fn get_consumer_running_info(
        &self,
        addr: &str,
        header: &GetConsumerRunningInfoRequestHeader,
        deadline: Instant,
    ) -> Result<Option<ConsumerRunningInfoBody>, RpcError> {
        let code = request::GET_CONSUMER_RUNNING_INFO;
        let reply = self
            .api
            .request_future(addr, code, header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        let Some(body) = reply.body.as_deref() else {
            return Ok(None);
        };
        let body = std::str::from_utf8(body).map_err(|error| RpcError::MalformedResponse {
            request_code: code,
            peer: addr.to_owned(),
            reason: error.to_string(),
        })?;
        ConsumerRunningInfoBody::decode_java_json_body(body)
            .map(Some)
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })
    }

    /// Creates or updates one Broker's subscription group through Java
    /// `MqClientAdmin.updateOrCreateSubscriptionGroup`.
    pub async fn update_or_create_subscription_group(
        &self,
        addr: &str,
        config: &SubscriptionGroupConfig,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP;
        let body = config
            .java_encode()
            .map_err(|source| RpcError::RequestSerialization {
                request_code: code,
                source,
            })?;
        let reply = self
            .api
            .request_future_without_header(addr, code, Some(body), deadline)
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Retrieves one Broker's cluster metadata through Java `MqClientAdmin`.
    pub async fn get_broker_cluster_info(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<Option<ClusterInfo>, RpcError> {
        self.api.get_broker_cluster_info(addr, deadline).await
    }

    /// Retrieves one Broker's consumer connections through Java `MqClientAdmin`.
    pub async fn get_consumer_connection_list(
        &self,
        addr: &str,
        header: &GetConsumerConnectionListRequestHeader,
        deadline: Instant,
    ) -> Result<Option<ConsumerConnection>, RpcError> {
        self.api
            .get_consumer_connection_list(addr, header, deadline)
            .await
    }

    /// Retrieves one Broker's consume offsets and rate through Java `MqClientAdmin`.
    pub async fn get_consume_stats(
        &self,
        addr: &str,
        header: &GetConsumeStatsRequestHeader,
        deadline: Instant,
    ) -> Result<Option<ConsumeStats>, RpcError> {
        self.api.get_consume_stats(addr, header, deadline).await
    }

    /// Queries one Broker and applies Java's topic/key filter to returned
    /// messages. A Broker `QUERY_NOT_FOUND` response produces an empty list.
    pub async fn query_message(
        &self,
        addr: &str,
        unique_key_flag: bool,
        decompress_body: bool,
        header: &QueryMessageRequestHeader,
        deadline: Instant,
    ) -> Result<Vec<MessageExt>, RpcError> {
        let code = request::QUERY_MESSAGE;
        let reply = self
            .api
            .query_message_response(addr, header, unique_key_flag, deadline)
            .await?;
        if reply.code == response::QUERY_NOT_FOUND {
            return Ok(Vec::new());
        }
        let reply = classify(reply, code, addr)?;
        let body = reply
            .body
            .as_deref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "missing response body".into(),
            })?;
        let mut messages = decode_many_as_client_compatible(
            body,
            DecodeOptions {
                decompress_body,
                ..DecodeOptions::default()
            },
        );

        let Some(topic) = header.get_topic() else {
            return Ok(Vec::new());
        };
        let Some(key) = header.get_key() else {
            return Ok(Vec::new());
        };
        messages.retain(|message| {
            if message.topic() != topic {
                return false;
            }
            if unique_key_flag {
                return message.message_id() == Some(key);
            }
            message
                .keys()
                .is_some_and(|keys| java_query_message_keys_match(&keys, key))
        });
        Ok(messages)
    }

    /// Views one Broker message by its physical commit-log offset.
    pub async fn view_message(
        &self,
        addr: &str,
        header: &ViewMessageRequestHeader,
        deadline: Instant,
    ) -> Result<MessageExt, RpcError> {
        self.api.view_message(addr, header, deadline).await
    }

    /// Invokes one Broker's direct-consume admin RPC.
    pub async fn consume_message_directly(
        &self,
        addr: &str,
        header: &ConsumeMessageDirectlyResultRequestHeader,
        deadline: Instant,
    ) -> Result<ConsumeMessageDirectlyResult, RpcError> {
        self.api
            .consume_message_directly(addr, header, deadline)
            .await
    }

    /// Queries one Broker's topic statistics through Java
    /// `MqClientAdmin.getTopicStatsInfo`.
    pub async fn get_topic_stats_info(
        &self,
        addr: &str,
        header: &GetTopicStatsInfoRequestHeader,
        deadline: Instant,
    ) -> Result<Option<TopicStatsTable>, RpcError> {
        self.api.get_topic_stats_info(addr, header, deadline).await
    }

    /// Queries one Broker's consume time spans through Java
    /// `MqClientAdmin.queryConsumeTimeSpan`.
    pub async fn query_consume_time_span(
        &self,
        addr: &str,
        header: &QueryConsumeTimeSpanRequestHeader,
        deadline: Instant,
    ) -> Result<Option<Vec<Option<QueueTimeSpan>>>, RpcError> {
        self.api
            .query_consume_time_span(addr, header, deadline)
            .await
    }

    /// Creates or updates one Broker topic through Java
    /// `MqClientAdmin.updateOrCreateTopic`.
    pub async fn update_or_create_topic(
        &self,
        addr: &str,
        header: &CreateTopicRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_TOPIC;
        let reply = self
            .api
            .request_future(addr, code, header, None, deadline)
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Deletes one Broker topic through Java `MqClientAdmin.deleteTopicInBroker`.
    pub async fn delete_topic_in_broker(
        &self,
        addr: &str,
        header: &DeleteTopicRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.api
            .delete_topic_in_broker(addr, header, deadline)
            .await
    }

    /// Deletes one NameServer topic through Java
    /// `MqClientAdmin.deleteTopicInNameserver`.
    pub async fn delete_topic_in_nameserver(
        &self,
        addr: &str,
        header: &DeleteTopicFromNamesrvRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.api
            .delete_topic_in_nameserver(addr, header, deadline)
            .await
    }

    /// Deletes one NameServer KV entry through Java `MqClientAdmin.deleteKvConfig`.
    pub async fn delete_kv_config(
        &self,
        addr: &str,
        header: &DeleteKvConfigRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.api.delete_kv_config(addr, header, deadline).await
    }

    /// Deletes one Broker subscription group through Java
    /// `MqClientAdmin.deleteSubscriptionGroup`.
    pub async fn delete_subscription_group(
        &self,
        addr: &str,
        header: &DeleteSubscriptionGroupRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.api
            .delete_subscription_group(addr, header, deadline)
            .await
    }

    /// Invokes one Broker's reset-offset operation through Java
    /// `MqClientAdmin.invokeBrokerToResetOffset`.
    pub async fn invoke_broker_to_reset_offset(
        &self,
        addr: &str,
        header: &ResetOffsetRequestHeader,
        deadline: Instant,
    ) -> Result<Option<SharedQueueOffsetEntries>, RpcError> {
        self.api
            .invoke_broker_to_reset_offset(addr, header, deadline)
            .await
    }

    /// Queries the topics consumed by one group through Java
    /// `MqClientAdmin.queryTopicsByConsumer`.
    pub async fn query_topics_by_consumer(
        &self,
        addr: &str,
        header: &QueryTopicsByConsumerRequestHeader,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        self.api
            .query_topics_by_consumer(addr, header, deadline)
            .await
    }

    /// Queries one group's subscription through Java
    /// `MqClientAdmin.querySubscriptionByConsumer`.
    pub async fn query_subscription_by_consumer(
        &self,
        addr: &str,
        header: &QuerySubscriptionByConsumerRequestHeader,
        deadline: Instant,
    ) -> Result<Option<SharedValue<SubscriptionData>>, RpcError> {
        self.api
            .query_subscription_by_consumer(addr, header, deadline)
            .await
    }

    /// Queries the groups consuming one topic through Java
    /// `MqClientAdmin.queryTopicConsumeByWho`.
    pub async fn query_topic_consume_by_who(
        &self,
        addr: &str,
        header: &QueryTopicConsumeByWhoRequestHeader,
        deadline: Instant,
    ) -> Result<GroupList, RpcError> {
        self.api
            .query_topic_consume_by_who(addr, header, deadline)
            .await
    }
}

fn no_message_error(topic: &str) -> RpcError {
    RpcError::Response {
        request_code: request::QUERY_MESSAGE,
        peer: topic.to_owned(),
        code: response::NO_MESSAGE,
        remark: Some("query message by key finished, but no message.".into()),
    }
}

fn query_message_matches(message: &MessageExt, query: &AdminQueryRequest) -> bool {
    if query.unique_key {
        return message.message_id() == Some(query.key.as_str());
    }

    let topic_matches = query.logical_message_queue || message.topic() == query.topic.as_str();
    if !topic_matches {
        return false;
    }
    match query.index_type.as_deref() {
        Some(constants::INDEX_KEY_TYPE) => message
            .keys()
            .is_some_and(|keys| java_query_message_keys_match(&keys, &query.key)),
        Some(constants::INDEX_TAG_TYPE) => message.tags().is_some_and(|tags| tags == query.key),
        _ => false,
    }
}

/// Matches Java `MqClientAdminImpl.filterMessages` key handling: Commons
/// `StringUtils.isNotBlank` delegates to `Character.isWhitespace`, then Java's
/// `String.split(" ")` drops trailing empty tokens but retains interior ones.
fn java_query_message_keys_match(keys: &str, query_key: &str) -> bool {
    if !keys
        .chars()
        .any(|character| !java_character_is_whitespace(character))
    {
        return false;
    }

    let mut tokens = keys.split(constants::KEY_SEPARATOR).collect::<Vec<_>>();
    while tokens.last() == Some(&"") {
        tokens.pop();
    }
    tokens
        .into_iter()
        .any(|message_key| message_key == query_key)
}

fn java_character_is_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{001c}'..='\u{0020}'
            | '\u{1680}'
            | '\u{2000}'..='\u{2006}'
            | '\u{2008}'..='\u{200a}'
            | '\u{2028}'..='\u{2029}'
            | '\u{205f}'
            | '\u{3000}'
    )
}

/// Applies the ID fields exposed by Java's `MessageDecoder.clientDecode`.
/// The record decoder creates the physical ID; client decode exposes that as
/// `offsetMsgId` and prefers the `UNIQ_KEY` property for `msgId`.
fn client_decode_message(message: &mut MessageExt) {
    let offset_message_id = message.message_id().map(str::to_owned);
    let client_message_id = message
        .properties()
        .get(constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .cloned()
        .or_else(|| offset_message_id.clone());
    message.apply_client_decode_ids(offset_message_id, client_message_id);
}

#[cfg(test)]
mod tests {
    use super::{
        AdminQueryRequest, MessageExt, QueryResult, SharedValue, java_query_message_keys_match,
        query_message_matches,
    };

    #[test]
    fn query_message_keys_match_java_whitespace_and_split_semantics() {
        assert!(java_query_message_keys_match("\u{00a0}", "\u{00a0}"));
        assert!(!java_query_message_keys_match("\u{2003}", "\u{2003}"));
        assert!(java_query_message_keys_match("\u{0085}", "\u{0085}"));
        assert!(!java_query_message_keys_match("\u{001c}", "\u{001c}"));
        assert!(java_query_message_keys_match("alpha  beta", ""));
        assert!(!java_query_message_keys_match("alpha ", ""));
        assert!(!java_query_message_keys_match("  ", ""));

        let mut message = MessageExt::new();
        message.set_topic_nullable(Some("orders".to_owned()));
        message.set_keys_nullable(Some("alpha  beta".to_owned()));
        assert!(query_message_matches(
            &message,
            &AdminQueryRequest::by_key("orders", "", 1, 0, 0)
        ));
    }

    #[test]
    fn broker_properties_parser_matches_java_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        for case in fixture["brokerConfigProperties"].as_array().unwrap() {
            let input = case["input"].as_str().unwrap();
            let actual = crate::auth::parse_java_properties(input.as_bytes());
            assert_eq!(actual.is_none(), case["isNull"], "input {input:?}");
            let expected = case["entries"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                .collect::<std::collections::HashMap<_, _>>();
            if let Some(actual) = actual {
                assert_eq!(actual, expected, "input {input:?}");
            }
        }
    }

    #[tokio::test]
    async fn direct_broker_operations_apply_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            for (expected_code, expected_fields) in [
                (
                    request::CLONE_GROUP_OFFSET,
                    serde_json::json!({
                        "srcGroup": "source",
                        "destGroup": "destination",
                        "topic": "orders",
                        "offline": "true"
                    }),
                ),
                (
                    request::INVOKE_BROKER_TO_GET_CONSUMER_STATUS,
                    serde_json::json!({
                        "topic": "orders",
                        "group": "workers",
                        "clientAddr": "consumer-a"
                    }),
                ),
                (
                    request::QUERY_CONSUME_QUEUE,
                    serde_json::json!({
                        "topic": "orders",
                        "queueId": "2",
                        "index": "13",
                        "count": "4",
                        "consumerGroup": "workers"
                    }),
                ),
            ] {
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.unwrap();
                let mut encoded = bytes::BytesMut::from(&length[..]);
                encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
                stream.read_exact(&mut encoded[4..]).await.unwrap();
                let command = frame::decode_next(&mut encoded).unwrap().unwrap();
                assert_eq!(command.code, expected_code);
                assert_eq!(command.language, Some(LanguageCode::Java));
                assert!(command.body.is_none());
                let actual_fields = command
                    .ext_fields
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({}));
                assert_eq!(actual_fields, expected_fields);
                let mut reply = RemotingCommand {
                    code: response::SUCCESS,
                    opaque: command.opaque,
                    body: match expected_code {
                        request::INVOKE_BROKER_TO_GET_CONSUMER_STATUS => {
                            Some(br#"{"consumerTable":{"consumer-a":{}}}"#.to_vec())
                        }
                        request::QUERY_CONSUME_QUEUE => Some(b"{}".to_vec()),
                        _ => None,
                    },
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
            }
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("direct-broker-operations-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let address = format!("127.0.0.1:{advertised_port}");
        api.clone_group_offset(
            &address,
            Some("source"),
            Some("destination"),
            Some("orders"),
            true,
            2000,
        )
        .await
        .unwrap();
        let status = api
            .invoke_broker_to_get_consumer_status(
                &address,
                Some("orders"),
                Some("workers"),
                Some("consumer-a"),
                2000,
            )
            .await
            .unwrap();
        assert!(status.is_some());
        let query_consume_queue_case = serde_json::from_str::<serde_json::Value>(include_str!(
            "../../../../fixtures/producer/core.json"
        ))
        .unwrap();
        let vip_fixture_case = query_consume_queue_case["queryConsumeQueueOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "queue-vip-address")
            .unwrap();
        assert_eq!(vip_fixture_case["vipEnabled"], true);
        assert_eq!(
            vip_fixture_case["requests"][0]["requestCode"],
            request::QUERY_CONSUME_QUEUE
        );
        assert_eq!(vip_fixture_case["requests"][0]["address"], "broker-a:10909");
        let queue = api
            .query_consume_queue(&address, Some("orders"), 2, 13, 4, Some("workers"), 2000)
            .await
            .unwrap();
        assert_eq!(queue.unwrap().get_max_queue_index(), 0);
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn create_static_topic_applies_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::{
            body::{TopicConfig, TopicQueueMappingDetail},
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["createStaticTopicOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-address")
            .unwrap();
        assert_eq!(case["vipEnabled"], true);
        let expected_request = &case["requests"][0];
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request_command.code,
                request::UPDATE_AND_CREATE_STATIC_TOPIC
            );
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            assert_eq!(
                request_command.body.as_deref(),
                Some(expected_body.as_slice())
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let topic_config =
            serde_json::from_str::<TopicConfig>(case["topicConfigJson"].as_str().unwrap()).unwrap();
        let mapping_detail = TopicQueueMappingDetail::java_decode(
            case["mappingDetailJson"].as_str().unwrap().as_bytes(),
        )
        .unwrap();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("create-static-topic-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let address = format!("127.0.0.1:{advertised_port}");
        api.create_static_topic(
            &address,
            case["defaultTopic"].as_str(),
            &topic_config,
            &mapping_detail,
            case["force"].as_bool().unwrap(),
            case["timeoutMillis"].as_i64().unwrap(),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn create_user_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::{
            body::UserInfo,
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["createUserOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_CREATE_USER);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            assert_eq!(
                request_command.body.as_deref(),
                Some(expected_body.as_slice())
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let user_info =
            serde_json::from_str::<UserInfo>(case["userInfoJson"].as_str().unwrap()).unwrap();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("create-user-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.create_user(
            &address,
            &user_info,
            case["timeoutMillis"].as_i64().unwrap(),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn update_user_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::{
            body::UserInfo,
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["updateUserOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_UPDATE_USER);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            assert_eq!(
                request_command.body.as_deref(),
                Some(expected_body.as_slice())
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let user_info =
            serde_json::from_str::<UserInfo>(case["userInfoJson"].as_str().unwrap()).unwrap();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("update-user-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.update_user(
            &address,
            &user_info,
            case["timeoutMillis"].as_i64().unwrap(),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn delete_user_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["deleteUserOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_DELETE_USER);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.body.is_none());
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let username = case["username"].as_str();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("delete-user-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.delete_user(&address, username, case["timeoutMillis"].as_i64().unwrap())
            .await
            .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn list_user_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["listUserOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(case["responseBodyBase64"].as_str().unwrap())
            .unwrap();
        let expected_result: serde_json::Value =
            serde_json::from_str(case["resultJson"].as_str().unwrap()).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_LIST_USER);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.body.is_none());
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                body: Some(expected_body),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let filter = case["filter"].as_str();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("list-user-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let result = api
            .list_user(&address, filter, case["timeoutMillis"].as_i64().unwrap())
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(result).unwrap(), expected_result);
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn list_acl_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["listAclOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(case["responseBodyBase64"].as_str().unwrap())
            .unwrap();
        let expected_result: serde_json::Value =
            serde_json::from_str(case["resultJson"].as_str().unwrap()).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_LIST_ACL);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.body.is_none());
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                body: Some(expected_body),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("list-acl-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let result = api
            .list_acl(
                &address,
                case["subjectFilter"].as_str(),
                case["resourceFilter"].as_str(),
                case["timeoutMillis"].as_i64().unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(result).unwrap(), expected_result);
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn get_user_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["getUserOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(case["responseBodyBase64"].as_str().unwrap())
            .unwrap();
        let expected_result: serde_json::Value =
            serde_json::from_str(case["resultJson"].as_str().unwrap()).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_GET_USER);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.body.is_none());
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                body: Some(expected_body),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let username = case["username"].as_str();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("get-user-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let result = api
            .get_user(&address, username, case["timeoutMillis"].as_i64().unwrap())
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(result).unwrap(), expected_result);
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn get_acl_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["getAclOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(case["responseBodyBase64"].as_str().unwrap())
            .unwrap();
        let expected_result: serde_json::Value =
            serde_json::from_str(case["resultJson"].as_str().unwrap()).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_GET_ACL);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.body.is_none());
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                body: Some(expected_body),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let subject = case["subject"].as_str();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("get-acl-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let result = api
            .get_acl(&address, subject, case["timeoutMillis"].as_i64().unwrap())
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(result).unwrap(), expected_result);
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn create_acl_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::{
            body::AclInfo,
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["createAclOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_CREATE_ACL);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert_eq!(
                request_command.body.as_deref(),
                Some(expected_body.as_slice())
            );
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let acl_info: AclInfo =
            serde_json::from_str(case["aclInfoJson"].as_str().unwrap()).unwrap();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("create-acl-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.create_acl(&address, &acl_info, case["timeoutMillis"].as_i64().unwrap())
            .await
            .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn update_acl_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use base64::Engine as _;
        use rocketmq_classic_protocol::{
            body::AclInfo,
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["updateAclOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        let expected_fields = expected_request["extFields"].clone();
        let expected_body = base64::engine::general_purpose::STANDARD
            .decode(expected_request["requestBodyBase64"].as_str().unwrap())
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_UPDATE_ACL);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert_eq!(
                request_command.body.as_deref(),
                Some(expected_body.as_slice())
            );
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let acl_info: AclInfo =
            serde_json::from_str(case["aclInfoJson"].as_str().unwrap()).unwrap();
        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("update-acl-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.update_acl(&address, &acl_info, case["timeoutMillis"].as_i64().unwrap())
            .await
            .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn delete_acl_keeps_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let case = fixture["deleteAclOperations"]["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "vip-enabled-address-unchanged")
            .unwrap();
        let expected_request = &case["requests"][0];
        assert_eq!(case["vipEnabled"], true);
        assert_eq!(expected_request["address"], case["address"]);
        assert_eq!(expected_request["requestBodyPresent"], false);
        let expected_fields = expected_request["extFields"].clone();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::AUTH_DELETE_ACL);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.body.is_none());
            assert_eq!(
                request_command
                    .ext_fields
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or_else(|| serde_json::json!({})),
                expected_fields
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("delete-acl-vip-enabled");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.delete_acl(
            &address,
            case["subject"].as_str(),
            case["resource"].as_str(),
            case["timeoutMillis"].as_i64().unwrap(),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn update_broker_config_applies_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            collections::HashMap,
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request.code,
                rocketmq_classic_protocol::codes::request::UPDATE_BROKER_CONFIG
            );
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.ext_fields.is_none());
            assert_eq!(
                request.body.as_deref(),
                Some(b"brokerPermission=7\n".as_slice())
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("update-broker-config-vip");
        config.set_vip_channel_enabled(true);
        let config = Arc::new(RwLock::new(config));
        let api = MQClientApi::with_config(remoting.clone(), config);
        api.update_broker_config(
            &format!("127.0.0.1:{advertised_port}"),
            &HashMap::from([("brokerPermission".to_owned(), "7".to_owned())]),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn clean_expired_consume_queue_applies_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request.code,
                rocketmq_classic_protocol::codes::request::CLEAN_EXPIRED_CONSUMEQUEUE
            );
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.ext_fields.is_none());
            assert!(request.body.is_none());
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("clean-expired-consume-queue-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.clean_expired_consume_queue(
            &format!("127.0.0.1:{advertised_port}"),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn get_subscription_group_config_applies_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request.code,
                rocketmq_classic_protocol::codes::request::GET_SUBSCRIPTIONGROUP_CONFIG
            );
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert_eq!(
                request
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("group"))
                    .map(String::as_str),
                Some("workers")
            );
            assert!(request.body.is_none());
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("get-subscription-group-config-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        assert!(
            api.get_subscription_group_config(
                &format!("127.0.0.1:{advertised_port}"),
                "workers",
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .is_none()
        );
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn get_broker_config_uses_supplied_address_with_vip_enabled() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            collections::HashMap,
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request.code,
                rocketmq_classic_protocol::codes::request::GET_BROKER_CONFIG
            );
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.ext_fields.is_none());
            assert!(request.body.is_none());
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request.opaque,
                body: Some(b"brokerPermission=7\n".to_vec()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("get-broker-config-no-vip");
        config.set_vip_channel_enabled(true);
        let config = Arc::new(RwLock::new(config));
        let api = MQClientApi::with_config(remoting.clone(), config);
        let properties = api
            .get_broker_config(&addr, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(
            properties,
            Some(HashMap::from([(
                "brokerPermission".to_owned(),
                "7".to_owned()
            )]))
        );
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn producer_diagnostic_rpcs_apply_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            for (expected_code, expected_fields, body) in [
                (
                    request::GET_PRODUCER_CONNECTION_LIST,
                    &[("producerGroup", "workers")][..],
                    b"{\"connectionSet\":[]}".as_slice(),
                ),
                (
                    request::GET_ALL_PRODUCER_INFO,
                    &[][..],
                    b"{\"data\":{}}".as_slice(),
                ),
                (
                    request::GET_BROKER_RUNTIME_INFO,
                    &[][..],
                    b"{\"table\":{}}".as_slice(),
                ),
                (
                    request::VIEW_BROKER_STATS_DATA,
                    &[("statsName", "SNDBYTES"), ("statsKey", "orders@0")][..],
                    b"{\"statsMinute\":{\"sum\":1,\"tps\":1.0,\"avgpt\":1.0}}".as_slice(),
                ),
                (
                    request::GET_TOPIC_CONFIG,
                    &[("topic", "orders"), ("lo", "true")][..],
                    b"{}".as_slice(),
                ),
                (
                    request::GET_ALL_TOPIC_CONFIG,
                    &[
                        ("topicSeq", "0"),
                        ("dataVersion", ""),
                        ("maxTopicNum", "77"),
                    ][..],
                    b"{\"dataVersion\":{\"counter\":0,\"stateVersion\":0,\"timestamp\":0},\"topicConfigTable\":{}}"
                        .as_slice(),
                ),
                (
                    request::GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
                    &[
                        ("groupSeq", "0"),
                        ("dataVersion", ""),
                        ("maxGroupNum", "77"),
                    ][..],
                    b"{}".as_slice(),
                ),
            ] {
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.unwrap();
                let mut encoded = bytes::BytesMut::from(&length[..]);
                encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
                stream.read_exact(&mut encoded[4..]).await.unwrap();
                let request = frame::decode_next(&mut encoded).unwrap().unwrap();
                assert_eq!(request.code, expected_code);
                assert_eq!(request.language, Some(LanguageCode::Java));
                assert!(request.body.is_none());
                let fields = request.ext_fields.unwrap_or_default();
                assert_eq!(fields.len(), expected_fields.len());
                for (key, value) in expected_fields {
                    assert_eq!(fields.get(key).map(String::as_str), Some(*value));
                }
                let mut reply = RemotingCommand {
                    code: response::SUCCESS,
                    opaque: request.opaque,
                    body: Some(body.to_vec()),
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
            }
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("producer-diagnostics-vip");
        config.set_vip_channel_enabled(true);
        config.set_max_page_size_in_get_metadata(77);
        let config = Arc::new(RwLock::new(config));
        let api = MQClientApi::with_config(remoting.clone(), config);
        let advertised_addr = format!("127.0.0.1:{advertised_port}");
        let deadline = Instant::now() + Duration::from_secs(2);
        api.get_producer_connection_list(&advertised_addr, "workers", deadline)
            .await
            .unwrap();
        api.get_all_producer_info(&advertised_addr, deadline)
            .await
            .unwrap();
        api.get_broker_runtime_info(&advertised_addr, deadline)
            .await
            .unwrap();
        api.view_broker_stats_data(&advertised_addr, Some("SNDBYTES"), Some("orders@0"), 2000)
            .await
            .unwrap();
        api.get_topic_config(&advertised_addr, Some("orders"), 2000)
            .await
            .unwrap();
        api.get_all_topic_config(&advertised_addr, 2000)
            .await
            .unwrap();
        api.get_all_subscription_group(&advertised_addr, 2000)
            .await
            .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn system_topic_list_from_broker_applies_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::{
            body::TopicList,
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let response_body = TopicList::default().java_encode().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request_command.code,
                request::GET_SYSTEM_TOPIC_LIST_FROM_BROKER
            );
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.ext_fields.is_none());
            assert!(request_command.body.is_none());
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                body: Some(response_body),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("system-topic-list-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let topics = api
            .get_system_topic_list_from_broker(
                &format!("127.0.0.1:{advertised_port}"),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert!(topics.get_topic_list().unwrap().snapshot().is_empty());
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn create_subscription_group_applies_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::{
            codes::{LanguageCode, request, response},
            subscription::SubscriptionGroupConfig,
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::sync::{Arc, RwLock};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::{Duration, Instant},
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request_command.code,
                request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP
            );
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.ext_fields.is_none());
            let body =
                SubscriptionGroupConfig::java_decode(request_command.body.as_deref().unwrap())
                    .unwrap();
            assert_eq!(body.get_group_name(), Some("workers"));
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut client_config = ClientConfig::new("create-subscription-group-vip");
        client_config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(client_config)));
        let mut group = SubscriptionGroupConfig::default();
        group.set_group_name(Some("workers".into()));
        api.create_subscription_group(
            &format!("127.0.0.1:{advertised_port}"),
            Some(&group),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn create_subscription_group_list_applies_configured_vip_port_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::{
            body::SubscriptionGroupList,
            codes::{LanguageCode, request, response},
            subscription::SubscriptionGroupConfig,
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(
                request_command.code,
                request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST
            );
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.ext_fields.is_none());
            let body = SubscriptionGroupList::java_decode(request_command.body.as_deref().unwrap())
                .unwrap();
            assert_eq!(body.get_group_config_list().len(), 1);
            assert_eq!(
                body.get_group_config_list()[0].get_group_name(),
                Some("workers")
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("create-subscription-group-list-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let mut group = SubscriptionGroupConfig::default();
        group.set_group_name(Some("workers".into()));
        api.create_subscription_group_list(
            &format!("127.0.0.1:{advertised_port}"),
            &[group],
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn create_topic_list_applies_configured_vip_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::{
            body::{CreateTopicListRequestBody, TopicConfig},
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request_command.code, request::UPDATE_AND_CREATE_TOPIC_LIST);
            assert_eq!(request_command.language, Some(LanguageCode::Java));
            assert!(request_command.ext_fields.is_none());
            let body =
                CreateTopicListRequestBody::java_decode(request_command.body.as_deref().unwrap())
                    .unwrap();
            assert_eq!(
                body.topic_config_list().unwrap()[0]
                    .as_ref()
                    .unwrap()
                    .topic_name(),
                Some("orders")
            );
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request_command.opaque,
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("create-topic-list-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        api.create_topic_list(
            &format!("127.0.0.1:{advertised_port}"),
            &[TopicConfig::new("orders")],
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn batch_delete_apis_apply_configured_vip_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::{
            body::{DeleteSubscriptionGroupListRequestBody, DeleteTopicListRequestBody},
            codes::{LanguageCode, request, response},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            for expected_code in [
                request::DELETE_TOPIC_IN_BROKER_LIST,
                request::DELETE_SUBSCRIPTION_GROUP_LIST,
            ] {
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.unwrap();
                let mut encoded = bytes::BytesMut::from(&length[..]);
                encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
                stream.read_exact(&mut encoded[4..]).await.unwrap();
                let request_command = frame::decode_next(&mut encoded).unwrap().unwrap();
                assert_eq!(request_command.code, expected_code);
                assert_eq!(request_command.language, Some(LanguageCode::Java));
                assert!(request_command.ext_fields.is_none());
                if expected_code == request::DELETE_TOPIC_IN_BROKER_LIST {
                    let body = DeleteTopicListRequestBody::java_decode(
                        request_command.body.as_deref().unwrap(),
                    )
                    .unwrap();
                    assert_eq!(body.topic_list().unwrap()[0].as_deref(), Some("orders"));
                } else {
                    let body = DeleteSubscriptionGroupListRequestBody::java_decode(
                        request_command.body.as_deref().unwrap(),
                    )
                    .unwrap();
                    assert_eq!(
                        body.group_name_list().unwrap()[0].as_deref(),
                        Some("workers")
                    );
                    assert!(body.clean_offset());
                }
                let mut reply = RemotingCommand {
                    code: response::SUCCESS,
                    opaque: request_command.opaque,
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
            }
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("batch-delete-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let addr = format!("127.0.0.1:{advertised_port}");
        api.delete_topic_in_broker_list(
            &addr,
            &["orders".to_owned()],
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        api.delete_subscription_group_list(
            &addr,
            &["workers".to_owned()],
            true,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn cold_data_flow_mutations_apply_the_configured_vip_offset() {
        use super::MQClientApi;
        use crate::config::ClientConfig;
        use rocketmq_classic_protocol::codes::{LanguageCode, request, response};
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::{
            collections::HashMap,
            sync::{Arc, RwLock},
            time::Duration,
        };
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            time::Instant,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let advertised_port = peer_addr.port().checked_add(2).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut requests = Vec::new();
            for _ in 0..2 {
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.unwrap();
                let mut encoded = bytes::BytesMut::from(&length[..]);
                encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
                stream.read_exact(&mut encoded[4..]).await.unwrap();
                let request = frame::decode_next(&mut encoded).unwrap().unwrap();
                let mut reply = RemotingCommand {
                    code: response::SUCCESS,
                    opaque: request.opaque,
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
                requests.push(request);
            }
            requests
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let mut config = ClientConfig::new("cold-flow-vip");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(remoting.clone(), Arc::new(RwLock::new(config)));
        let addr = format!("127.0.0.1:{advertised_port}");
        api.update_cold_data_flow_ctr_group_config(
            &addr,
            &HashMap::from([("group".to_owned(), "workers".to_owned())]),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        api.remove_cold_data_flow_ctr_group_config(
            &addr,
            Some("workers"),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        let requests = peer.await.unwrap();
        assert_eq!(requests[0].code, request::UPDATE_COLD_DATA_FLOW_CTR_CONFIG);
        assert_eq!(requests[0].language, Some(LanguageCode::Java));
        assert_eq!(
            requests[0].body.as_deref(),
            Some(b"group=workers\n".as_slice())
        );
        assert_eq!(requests[1].code, request::REMOVE_COLD_DATA_FLOW_CTR_CONFIG);
        assert_eq!(requests[1].language, Some(LanguageCode::Java));
        assert_eq!(requests[1].body.as_deref(), Some(b"workers".as_slice()));
        remoting.shutdown().await;
    }

    #[test]
    fn query_result_java_getters_expose_the_stored_values() {
        let messages = SharedValue::from(Vec::new());
        let result = QueryResult::new(17, Some(messages.clone()));
        assert_eq!(result.get_index_last_update_timestamp(), 17);
        let returned = result.get_message_list().unwrap();
        assert!(returned.ptr_eq(&messages));
        returned.with_mut(|messages| messages.push(Some(SharedValue::from(MessageExt::default()))));
        assert_eq!(messages.snapshot().len(), 1);
    }

    #[test]
    fn query_result_preserves_java_null_list_elements_and_to_string() {
        let null_list = QueryResult::new(17, None);
        assert!(null_list.get_message_list().is_none());
        assert_eq!(
            null_list.to_string(),
            "QueryResult [indexLastUpdateTimestamp=17, messageList=null]"
        );

        let null_element = QueryResult::new(18, Some(SharedValue::from(vec![None])));
        assert_eq!(
            null_element.to_string(),
            "QueryResult [indexLastUpdateTimestamp=18, messageList=[null]]"
        );
    }

    #[test]
    fn query_cluster_inputs_derive_java_route_topic() {
        let logical = AdminQueryRequest::by_key("%LMQ%child", "k", 32, 0, 1)
            .with_cluster_name(Some("parent".into()));
        assert!(logical.logical_message_queue);
        assert_eq!(logical.route_topic, "parent");

        let logical_without_cluster = logical.with_cluster_name(None);
        assert_eq!(logical_without_cluster.route_topic, "%LMQ%child");

        let wheel_timer = AdminQueryRequest::by_key("rmq_sys_wheel_timer", "k", 32, 0, 1)
            .with_cluster_name(Some("cluster-a".into()));
        assert!(!wheel_timer.logical_message_queue);
        assert_eq!(wheel_timer.route_topic, "cluster-a");

        let ordinary = AdminQueryRequest::by_key("orders", "k", 32, 0, 1)
            .with_cluster_name(Some("cluster-a".into()));
        assert_eq!(ordinary.route_topic, "orders");
    }

    #[tokio::test]
    async fn query_topics_by_consumer_sends_java_header_and_decodes_topic_list() {
        use crate::api::MQClientApi;
        use rocketmq_classic_protocol::{
            codes::{request, response},
            header::{CommandHeader, HeaderMap, QueryTopicsByConsumerRequestHeader},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::time::Duration;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::QUERY_TOPICS_BY_CONSUMER);
            assert_eq!(
                request
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("group"))
                    .map(String::as_str),
                Some("workers")
            );
            let mut expected_fields = HeaderMap::new();
            let mut expected_header = QueryTopicsByConsumerRequestHeader::default();
            expected_header.set_group(Some("workers".into()));
            expected_header.set_namespace(Some("tenant".into()));
            expected_header.set_namespaced(Some(true));
            expected_header.set_broker_name(Some("broker-a".into()));
            expected_header.set_oneway(Some(false));
            CommandHeader::encode_fields(&expected_header, &mut expected_fields);
            assert_eq!(
                request
                    .ext_fields
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(key, value)| { (key.clone(), value.clone()) })
                    .collect::<HeaderMap>(),
                expected_fields
            );

            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request.opaque,
                body: Some(
                    br#"{"topicList":["orders","billing"],"brokerAddr":"broker-a"}"#.to_vec(),
                ),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let api = MQClientApi::new(remoting.clone());
        let mut header = QueryTopicsByConsumerRequestHeader::default();
        header.set_group(Some("workers".into()));
        header.set_namespace(Some("tenant".into()));
        header.set_namespaced(Some(true));
        header.set_broker_name(Some("broker-a".into()));
        header.set_oneway(Some(false));
        let result = api
            .query_topics_by_consumer(
                &address,
                &header,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(result.get_broker_addr(), Some("broker-a"));
        assert_eq!(
            result.get_topic_list().unwrap().snapshot(),
            std::collections::HashSet::from(["orders".to_owned(), "billing".to_owned()])
        );
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn query_topics_by_consumer_preserves_broker_error() {
        use crate::{api::MQClientApi, error::RpcError};
        use rocketmq_classic_protocol::{
            codes::{request, response},
            header::QueryTopicsByConsumerRequestHeader,
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::time::Duration;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::QUERY_TOPICS_BY_CONSUMER);

            let mut reply = RemotingCommand {
                code: response::TOPIC_NOT_EXIST,
                opaque: request.opaque,
                remark: Some("topic not exist".into()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let api = MQClientApi::new(remoting.clone());
        let mut header = QueryTopicsByConsumerRequestHeader::default();
        header.set_group(Some("workers".into()));
        let error = api
            .query_topics_by_consumer(
                &address,
                &header,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RpcError::Response {
                request_code: request::QUERY_TOPICS_BY_CONSUMER,
                peer,
                code: response::TOPIC_NOT_EXIST,
                remark: Some(remark),
            } if peer == address && remark == "topic not exist"
        ));
        peer.await.unwrap();
        remoting.shutdown().await;
    }
}
