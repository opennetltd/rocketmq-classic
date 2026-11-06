//! One-attempt lifecycle and extension requests. Message decoding belongs to the consumer layer.
use rocketmq_classic_protocol::{
    body::{
        BatchAckMessageRequestBody, GetBrokerLiteInfoResponseBody, GetLiteClientInfoResponseBody,
        GetLiteGroupInfoResponseBody, GetLiteTopicInfoResponseBody, GetParentTopicInfoResponseBody,
        LiteSubscriptionCtlRequestBody, QueryAssignmentRequestBody, QueryAssignmentResponseBody,
        SetMessageRequestModeRequestBody,
    },
    codes::{request, response},
    header::{
        AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
        ChangeInvisibleTimeResponseHeader, GetLiteClientInfoRequestHeader,
        GetLiteGroupInfoRequestHeader, GetLiteTopicInfoRequestHeader,
        GetParentTopicInfoRequestHeader, HeaderMap, HeartbeatRequestHeader,
        LiteSubscriptionCtlRequestHeader, NotificationRequestHeader, NotificationResponseHeader,
        PopLiteMessageRequestHeader, PopLiteMessageResponseHeader, PopMessageRequestHeader,
        PopMessageResponseHeader, ProtocolError, UnregisterClientRequestHeader,
    },
    heartbeat::HeartbeatData,
};
use rocketmq_classic_remoting::RemotingCommand;
use tokio::time::Instant;

use super::{MQClientApi, classify, decode_body, decode_header};
use crate::error::RpcError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatV2Result {
    pub version: i32,
    pub is_sub_change: bool,
    pub is_support_heartbeat_v2: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PopStatus {
    Found,
    NoNewMsg,
    PollingNotFound,
    PollingFull,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PopStatusLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.client.consumer.PopStatus.{0}")]
    UnknownName(String),
}

impl PopStatus {
    pub const fn values() -> [Self; 4] {
        [
            Self::Found,
            Self::NoNewMsg,
            Self::PollingFull,
            Self::PollingNotFound,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, PopStatusLookupError> {
        match name {
            None => Err(PopStatusLookupError::NullName),
            Some("FOUND") => Ok(Self::Found),
            Some("NO_NEW_MSG") => Ok(Self::NoNewMsg),
            Some("POLLING_FULL") => Ok(Self::PollingFull),
            Some("POLLING_NOT_FOUND") => Ok(Self::PollingNotFound),
            Some(other) => Err(PopStatusLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::Found => "FOUND",
            Self::NoNewMsg => "NO_NEW_MSG",
            Self::PollingFull => "POLLING_FULL",
            Self::PollingNotFound => "POLLING_NOT_FOUND",
        }
    }

    pub const fn ordinal(&self) -> i32 {
        match self {
            Self::Found => 0,
            Self::NoNewMsg => 1,
            Self::PollingFull => 2,
            Self::PollingNotFound => 3,
        }
    }

    pub const fn java_compare_to(&self, other: &Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for PopStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopReply<H> {
    pub status: PopStatus,
    pub header: Option<H>,
    pub raw_messages: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    Ok,
    NoExist,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AckStatusLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.client.consumer.AckStatus.{0}")]
    UnknownName(String),
}

impl AckStatus {
    pub const fn values() -> [Self; 2] {
        [Self::Ok, Self::NoExist]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, AckStatusLookupError> {
        match name {
            None => Err(AckStatusLookupError::NullName),
            Some("OK") => Ok(Self::Ok),
            Some("NO_EXIST") => Ok(Self::NoExist),
            Some(other) => Err(AckStatusLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::NoExist => "NO_EXIST",
        }
    }

    pub const fn ordinal(&self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::NoExist => 1,
        }
    }

    pub const fn java_compare_to(&self, other: &Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for AckStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeInvisibleTimeResult {
    pub status: AckStatus,
    pub pop_time: Option<i64>,
    pub invisible_time: Option<i64>,
    pub revive_qid: Option<i32>,
    pub extra_info: Option<String>,
}

/// Result returned by Java `MQClientAPIExt.notificationWithPollingStats`.
///
/// Java's `NotifyResult` has private fields, a public no-argument constructor,
/// accessors, and a stable `toString()` representation. Rust keeps the fields
/// private and exposes the same state through idiomatic accessors.
///
/// ```compile_fail
/// use rocketmq_classic_core::api::extensions::NotifyResult;
/// let _ = NotifyResult::default().has_msg;
/// ```
#[derive(Debug, Default)]
pub struct NotifyResult {
    has_msg: bool,
    polling_full: bool,
}

impl NotifyResult {
    pub fn is_has_msg(&self) -> bool {
        self.has_msg
    }

    pub fn set_has_msg(&mut self, value: bool) {
        self.has_msg = value;
    }

    pub fn is_polling_full(&self) -> bool {
        self.polling_full
    }

    pub fn set_polling_full(&mut self, value: bool) {
        self.polling_full = value;
    }
}

impl std::fmt::Display for NotifyResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "NotifyResult{{hasMsg={}, pollingFull={}}}",
            self.has_msg, self.polling_full
        )
    }
}

fn json_body<T: serde::Serialize>(value: &T, request_code: i32) -> Result<Vec<u8>, RpcError> {
    serde_json::to_vec(value).map_err(|source| RpcError::RequestSerialization {
        request_code,
        source,
    })
}

fn heartbeat_json_body(data: &HeartbeatData) -> Result<Vec<u8>, RpcError> {
    data.java_encode()
        .map_err(|source| RpcError::RequestSerialization {
            request_code: request::HEART_BEAT,
            source,
        })
}

fn pop_status(reply: &RemotingCommand, code: i32, addr: &str) -> Result<PopStatus, RpcError> {
    match reply.code {
        response::SUCCESS => Ok(PopStatus::Found),
        response::PULL_NOT_FOUND => Ok(PopStatus::PollingNotFound),
        response::POLLING_TIMEOUT => Ok(PopStatus::PollingNotFound),
        response::POLLING_FULL => Ok(PopStatus::PollingFull),
        _ => Err(RpcError::Response {
            request_code: code,
            peer: addr.into(),
            code: reply.code,
            remark: reply.remark.clone(),
        }),
    }
}

// Other POP response headers still use Java's reflective primitive defaults.
fn primitive<T: std::str::FromStr + Default>(reply: &RemotingCommand, key: &str) -> T {
    reply
        .ext_fields
        .as_ref()
        .and_then(|fields| fields.get(key))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn optional_field(reply: &RemotingCommand, key: &str) -> Option<String> {
    reply
        .ext_fields
        .as_ref()
        .and_then(|fields| fields.get(key))
        .cloned()
}

impl MQClientApi {
    /// Ask a Broker whether the specified POP queue currently has messages.
    /// This maps Java `MQClientAPIExt.notification` and uses the async invoke
    /// path even though the result is awaited here.
    pub async fn notification(
        &self,
        addr: &str,
        header: &NotificationRequestHeader,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        Ok(self
            .notification_with_polling_stats(addr, header, deadline)
            .await?
            .is_has_msg())
    }

    /// Ask a Broker for the POP notification flags, including polling capacity.
    /// This maps Java `MQClientAPIExt.notificationWithPollingStats`.
    pub async fn notification_with_polling_stats(
        &self,
        addr: &str,
        header: &NotificationRequestHeader,
        deadline: Instant,
    ) -> Result<NotifyResult, RpcError> {
        let reply = self
            .request_future(addr, request::NOTIFICATION, header, None, deadline)
            .await?;
        let reply = classify(reply, request::NOTIFICATION, addr)?;
        let response_header =
            decode_header::<NotificationResponseHeader>(&reply, request::NOTIFICATION, addr)?;
        let mut result = NotifyResult::default();
        result.set_has_msg(response_header.is_has_msg());
        result.set_polling_full(response_header.is_polling_full());
        Ok(result)
    }

    /// Gets Lite-specific Broker metadata. This mirrors the synchronous Java
    /// `MQClientAPIImpl.getBrokerLiteInfo` call, including its VIP address
    /// transform and nullable response body.
    pub async fn get_broker_lite_info(
        &self,
        addr: &str,
        deadline: Instant,
    ) -> Result<Option<GetBrokerLiteInfoResponseBody>, RpcError> {
        let code = request::GET_BROKER_LITE_INFO;
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| crate::config::read_shared_client_config(config).vip_channel_enabled())
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
        GetBrokerLiteInfoResponseBody::java_decode_nullable(reply.body.as_deref()).map_err(
            |error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            },
        )
    }

    /// Queries a Lite parent topic's metadata through Java's synchronous
    /// `MQClientAPIImpl.getParentTopicInfo` operation. A missing response body
    /// is a successful null result in Java.
    pub async fn get_parent_topic_info(
        &self,
        addr: &str,
        topic: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<GetParentTopicInfoResponseBody>, RpcError> {
        let code = request::GET_PARENT_TOPIC_INFO;
        let mut header = GetParentTopicInfoRequestHeader::default();
        header.set_topic(topic.map(str::to_owned));
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| crate::config::read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .request(&broker_addr, code, &header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        GetParentTopicInfoResponseBody::java_decode_nullable(reply.body.as_deref()).map_err(
            |error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            },
        )
    }

    /// Queries a Lite client's metadata through Java's synchronous
    /// `MQClientAPIImpl.getLiteClientInfo` operation. Java sends the request
    /// header's default `maxCount` of 1000; an absent response body is null.
    pub async fn get_lite_client_info(
        &self,
        addr: &str,
        parent_topic: Option<&str>,
        group: Option<&str>,
        client_id: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<GetLiteClientInfoResponseBody>, RpcError> {
        let code = request::GET_LITE_CLIENT_INFO;
        let mut header = GetLiteClientInfoRequestHeader::default();
        header.set_parent_topic(parent_topic.map(str::to_owned));
        header.set_group(group.map(str::to_owned));
        header.set_client_id(client_id.map(str::to_owned));
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| crate::config::read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .request(&broker_addr, code, &header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        GetLiteClientInfoResponseBody::java_decode_nullable(reply.body.as_deref()).map_err(
            |error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            },
        )
    }

    /// Queries Lite consumer-group lag details through Java's synchronous
    /// `MQClientAPIImpl.getLiteGroupInfo` operation. Java accepts nullable
    /// group/topic values and sends the supplied primitive `top_k` unchanged.
    pub async fn get_lite_group_info(
        &self,
        addr: &str,
        group: Option<&str>,
        lite_topic: Option<&str>,
        top_k: i32,
        deadline: Instant,
    ) -> Result<Option<GetLiteGroupInfoResponseBody>, RpcError> {
        let code = request::GET_LITE_GROUP_INFO;
        let mut header = GetLiteGroupInfoRequestHeader::default();
        header.set_group(group.map(str::to_owned));
        header.set_lite_topic(lite_topic.map(str::to_owned));
        header.set_top_k(top_k);
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| crate::config::read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .request(&broker_addr, code, &header, None, deadline)
            .await?;
        let reply = classify(reply, code, addr)?;
        GetLiteGroupInfoResponseBody::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Queries Lite topic details through Java's asynchronous
    /// `MQClientAPIExt.getLiteTopicInfoAsync` operation. A missing response
    /// body is a successful null result in the Java implementation.
    pub async fn get_lite_topic_info(
        &self,
        addr: &str,
        parent_topic: Option<&str>,
        lite_topic: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<GetLiteTopicInfoResponseBody>, RpcError> {
        let mut header = GetLiteTopicInfoRequestHeader::default();
        header.set_parent_topic(parent_topic.map(str::to_owned));
        header.set_lite_topic(lite_topic.map(str::to_owned));
        let reply = classify(
            self.request_future(addr, request::GET_LITE_TOPIC_INFO, &header, None, deadline)
                .await?,
            request::GET_LITE_TOPIC_INFO,
            addr,
        )?;
        GetLiteTopicInfoResponseBody::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: request::GET_LITE_TOPIC_INFO,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    pub async fn send_heartbeat(
        &self,
        addr: &str,
        data: &HeartbeatData,
        deadline: Instant,
    ) -> Result<i32, RpcError> {
        let body = heartbeat_json_body(data)?;
        let reply = self
            .request(
                addr,
                request::HEART_BEAT,
                &HeartbeatRequestHeader::default(),
                Some(body),
                deadline,
            )
            .await?;
        Ok(classify(reply, request::HEART_BEAT, addr)?.version)
    }

    /// Sends the synchronous Java `MQClientAPIImpl.registerClient` heartbeat
    /// request and returns whether the Broker replied with `SUCCESS`.
    pub async fn register_client(
        &self,
        addr: &str,
        data: &HeartbeatData,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        let reply = self
            .request(
                addr,
                request::HEART_BEAT,
                &HeartbeatRequestHeader::default(),
                Some(heartbeat_json_body(data)?),
                deadline,
            )
            .await?;
        Ok(reply.code == response::SUCCESS)
    }

    /// Future-based counterpart of Java `MQClientAPIExt.sendHeartbeatAsync`.
    pub async fn send_heartbeat_async(
        &self,
        addr: &str,
        data: &HeartbeatData,
        deadline: Instant,
    ) -> Result<i32, RpcError> {
        let body = heartbeat_json_body(data)?;
        let reply = self
            .request_future(
                addr,
                request::HEART_BEAT,
                &HeartbeatRequestHeader::default(),
                Some(body),
                deadline,
            )
            .await?;
        Ok(classify(reply, request::HEART_BEAT, addr)?.version)
    }

    /// One-way counterpart of Java `MQClientAPIExt.sendHeartbeatOneway`.
    pub async fn send_heartbeat_oneway(
        &self,
        addr: &str,
        data: &HeartbeatData,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.oneway(
            addr,
            request::HEART_BEAT,
            &HeartbeatRequestHeader::default(),
            Some(heartbeat_json_body(data)?),
            deadline,
        )
        .await
    }

    pub async fn send_heartbeat_v2(
        &self,
        addr: &str,
        data: &HeartbeatData,
        deadline: Instant,
    ) -> Result<HeartbeatV2Result, RpcError> {
        let body = heartbeat_json_body(data)?;
        let reply = classify(
            self.request(
                addr,
                request::HEART_BEAT,
                &HeartbeatRequestHeader::default(),
                Some(body),
                deadline,
            )
            .await?,
            request::HEART_BEAT,
            addr,
        )?;
        let fields = reply.ext_fields.as_ref();
        Ok(HeartbeatV2Result {
            version: reply.version,
            is_sub_change: fields
                .and_then(|f| f.get("IS_SUB_CHANGE"))
                .is_some_and(|v| v.eq_ignore_ascii_case("true")),
            is_support_heartbeat_v2: fields
                .and_then(|f| f.get("IS_SUPPORT_HEART_BEAT_V2"))
                .is_some_and(|v| v.eq_ignore_ascii_case("true")),
        })
    }

    pub async fn unregister_client(
        &self,
        addr: &str,
        header: &UnregisterClientRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        classify(
            self.request(addr, request::UNREGISTER_CLIENT, header, None, deadline)
                .await?,
            request::UNREGISTER_CLIENT,
            addr,
        )?;
        Ok(())
    }

    pub async fn query_assignment(
        &self,
        addr: &str,
        body: &QueryAssignmentRequestBody,
        deadline: Instant,
    ) -> Result<QueryAssignmentResponseBody, RpcError> {
        let reply = classify(
            self.request(
                addr,
                request::QUERY_ASSIGNMENT,
                &LiteSubscriptionCtlRequestHeader::default(),
                Some(json_body(body, request::QUERY_ASSIGNMENT)?),
                deadline,
            )
            .await?,
            request::QUERY_ASSIGNMENT,
            addr,
        )?;
        decode_body(
            &reply,
            request::QUERY_ASSIGNMENT,
            addr,
            QueryAssignmentResponseBody::java_decode,
        )
    }

    /// Uses the signed timeout passed by `MQClientInstance.queryAssignment`.
    /// Channel acquisition precedes timeout expiry, as in Java's
    /// `NettyRemotingClient.invokeSync`.
    pub(crate) async fn query_assignment_with_timeout_millis(
        &self,
        addr: &str,
        body: &QueryAssignmentRequestBody,
        timeout_millis: i32,
    ) -> Result<QueryAssignmentResponseBody, RpcError> {
        let reply = classify(
            self.request_with_timeout_millis(
                addr,
                request::QUERY_ASSIGNMENT,
                &LiteSubscriptionCtlRequestHeader::default(),
                Some(json_body(body, request::QUERY_ASSIGNMENT)?),
                i64::from(timeout_millis),
            )
            .await?,
            request::QUERY_ASSIGNMENT,
            addr,
        )?;
        decode_body(
            &reply,
            request::QUERY_ASSIGNMENT,
            addr,
            QueryAssignmentResponseBody::java_decode,
        )
    }

    pub async fn set_message_request_mode(
        &self,
        addr: &str,
        body: &SetMessageRequestModeRequestBody,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        classify(
            self.request(
                addr,
                request::SET_MESSAGE_REQUEST_MODE,
                &LiteSubscriptionCtlRequestHeader::default(),
                Some(json_body(body, request::SET_MESSAGE_REQUEST_MODE)?),
                deadline,
            )
            .await?,
            request::SET_MESSAGE_REQUEST_MODE,
            addr,
        )?;
        Ok(())
    }

    /// Exports Broker POP records through Java's `MQClientAPIImpl.exportPopRecord`.
    /// This operation sends code 200056 without a header or body and uses the
    /// supplied address unchanged, including when VIP mode is enabled.
    pub async fn export_pop_record(&self, addr: &str, deadline: Instant) -> Result<(), RpcError> {
        let code = request::POP_ROLLBACK;
        let reply = self
            .remoting
            .invoke_sync_with_deadline(
                addr,
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..super::java_client_request_command(code)?
                },
                deadline,
            )
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Switches the Broker timer engine through Java's
    /// `MQClientAPIImpl.switchTimerEngine`. The `E_T` value is sent as a
    /// command extension field; the supplied address is used unchanged.
    pub async fn switch_timer_engine(
        &self,
        addr: &str,
        engine_type: &str,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::SWITCH_TIMER_ENGINE;
        let mut fields = rocketmq_classic_protocol::header::HeaderMap::new();
        fields.insert("E_T".into(), engine_type.into());
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
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Triggers dispatch of a Lite consumer group through Java's
    /// `MQClientAPIImpl.triggerLiteDispatch`. The Broker address uses the
    /// configured VIP port transform. Java's request header has a no-op
    /// `checkFields()`, so nullable group and client ID values are preserved
    /// as omitted extension fields instead of being rejected locally.
    pub async fn trigger_lite_dispatch(
        &self,
        addr: &str,
        group: Option<&str>,
        client_id: Option<&str>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::TRIGGER_LITE_DISPATCH;
        let mut fields = HeaderMap::new();
        if let Some(group) = group {
            fields.insert("group".into(), group.into());
        }
        if let Some(client_id) = client_id {
            fields.insert("clientId".into(), client_id.into());
        }
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| crate::config::read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .request_fields_with_mode(&broker_addr, code, fields, None, deadline, true)
            .await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    pub async fn lite_subscription_ctl(
        &self,
        addr: &str,
        body: &LiteSubscriptionCtlRequestBody,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        classify(
            self.request_future(
                addr,
                request::LITE_SUBSCRIPTION_CTL,
                &LiteSubscriptionCtlRequestHeader::default(),
                Some(json_body(body, request::LITE_SUBSCRIPTION_CTL)?),
                deadline,
            )
            .await?,
            request::LITE_SUBSCRIPTION_CTL,
            addr,
        )?;
        Ok(())
    }

    pub async fn pop_message(
        &self,
        addr: &str,
        header: &PopMessageRequestHeader,
        deadline: Instant,
    ) -> Result<PopReply<PopMessageResponseHeader>, RpcError> {
        let reply = self
            .request_future(addr, request::POP_MESSAGE, header, None, deadline)
            .await?;
        let status = pop_status(&reply, request::POP_MESSAGE, addr)?;
        if status == PopStatus::Found && reply.body.is_none() {
            return Err(RpcError::MalformedResponse {
                request_code: request::POP_MESSAGE,
                peer: addr.into(),
                reason: "missing pop message body".into(),
            });
        }
        let mut parsed = PopMessageResponseHeader::default();
        parsed.set_pop_time(primitive(&reply, "popTime"));
        parsed.set_invisible_time(primitive(&reply, "invisibleTime"));
        parsed.set_revive_qid(primitive(&reply, "reviveQid"));
        parsed.set_rest_num(primitive(&reply, "restNum"));
        parsed.set_start_offset_info(optional_field(&reply, "startOffsetInfo"));
        parsed.set_msg_offset_info(optional_field(&reply, "msgOffsetInfo"));
        parsed.set_order_count_info(optional_field(&reply, "orderCountInfo"));
        Ok(PopReply {
            status,
            header: Some(parsed),
            raw_messages: reply.body,
        })
    }

    pub async fn pop_lite_message(
        &self,
        addr: &str,
        header: &PopLiteMessageRequestHeader,
        deadline: Instant,
    ) -> Result<PopReply<PopLiteMessageResponseHeader>, RpcError> {
        let reply = self
            .request_future(addr, request::POP_LITE_MESSAGE, header, None, deadline)
            .await?;
        let status = pop_status(&reply, request::POP_LITE_MESSAGE, addr)?;
        if status == PopStatus::Found && reply.body.is_none() {
            return Err(RpcError::MalformedResponse {
                request_code: request::POP_LITE_MESSAGE,
                peer: addr.into(),
                reason: "missing pop lite message body".into(),
            });
        }
        let mut parsed = PopLiteMessageResponseHeader::default();
        parsed.set_pop_time(primitive(&reply, "popTime"));
        parsed.set_invisible_time(primitive(&reply, "invisibleTime"));
        parsed.set_revive_qid(primitive(&reply, "reviveQid"));
        parsed.set_start_offset_info(optional_field(&reply, "startOffsetInfo"));
        parsed.set_msg_offset_info(optional_field(&reply, "msgOffsetInfo"));
        parsed.set_order_count_info(optional_field(&reply, "orderCountInfo"));
        Ok(PopReply {
            status,
            header: Some(parsed),
            raw_messages: reply.body,
        })
    }

    pub async fn ack_message(
        &self,
        addr: &str,
        header: &AckMessageRequestHeader,
        deadline: Instant,
    ) -> Result<AckStatus, RpcError> {
        let reply = self
            .request_future(addr, request::ACK_MESSAGE, header, None, deadline)
            .await?;
        Ok(if reply.code == response::SUCCESS {
            AckStatus::Ok
        } else {
            AckStatus::NoExist
        })
    }

    pub async fn batch_ack_message(
        &self,
        addr: &str,
        body: &BatchAckMessageRequestBody,
        deadline: Instant,
    ) -> Result<AckStatus, RpcError> {
        let reply = self
            .request_future(
                addr,
                request::BATCH_ACK_MESSAGE,
                &LiteSubscriptionCtlRequestHeader::default(),
                Some(
                    body.java_encode()
                        .map_err(|source| RpcError::RequestSerialization {
                            request_code: request::BATCH_ACK_MESSAGE,
                            source,
                        })?,
                ),
                deadline,
            )
            .await?;
        Ok(if reply.code == response::SUCCESS {
            AckStatus::Ok
        } else {
            AckStatus::NoExist
        })
    }

    pub async fn change_invisible_time(
        &self,
        addr: &str,
        broker_name: &str,
        header: &ChangeInvisibleTimeRequestHeader,
        deadline: Instant,
    ) -> Result<ChangeInvisibleTimeResult, RpcError> {
        let reply = self
            .request_future(
                addr,
                request::CHANGE_MESSAGE_INVISIBLETIME,
                header,
                None,
                deadline,
            )
            .await?;
        let response_header = reply
            .decode_command_custom_header_directly::<ChangeInvisibleTimeResponseHeader>(true)?;
        let pop_time = response_header.get_pop_time();
        let invisible_time = response_header.get_invisible_time();
        let revive_qid = response_header.get_revive_qid();
        if reply.code != response::SUCCESS {
            return Ok(ChangeInvisibleTimeResult {
                status: AckStatus::NoExist,
                pop_time: None,
                invisible_time: None,
                revive_qid: None,
                extra_info: None,
            });
        }
        let offset = header
            .get_offset()
            .ok_or(ProtocolError::MissingField("offset"))?;
        let topic = header.get_topic();
        let queue_id = header
            .get_queue_id()
            .ok_or(ProtocolError::MissingField("queueId"))?;
        let topic = topic.ok_or(ProtocolError::MissingField("topic"))?;
        Ok(ChangeInvisibleTimeResult {
            status: AckStatus::Ok,
            pop_time: Some(pop_time),
            invisible_time: Some(invisible_time),
            revive_qid: Some(revive_qid),
            extra_info: Some(change_invisible_extra_info(
                offset,
                pop_time,
                invisible_time,
                revive_qid,
                topic,
                broker_name,
                queue_id,
            )),
        })
    }
}

fn change_invisible_extra_info(
    offset: i64,
    pop_time: i64,
    invisible_time: i64,
    revive_qid: i32,
    topic: &str,
    broker_name: &str,
    queue_id: i32,
) -> String {
    let retry = if topic.starts_with("%RETRY%") && topic.contains('+') {
        2
    } else if topic.starts_with("%RETRY%") {
        1
    } else {
        0
    };
    format!(
        "{offset} {pop_time} {invisible_time} {revive_qid} {retry} {broker_name} {queue_id} {offset}"
    )
}

#[cfg(test)]
mod serialization_tests {
    use super::*;

    #[test]
    fn serializer_failure_returns_typed_request_serialization_error() {
        struct FailingValue;

        impl serde::Serialize for FailingValue {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("fixture serialization failure"))
            }
        }

        let error = json_body(&FailingValue, request::HEART_BEAT).unwrap_err();
        match error {
            RpcError::RequestSerialization {
                request_code,
                source,
            } => {
                assert_eq!(request_code, request::HEART_BEAT);
                assert!(!source.to_string().is_empty());
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}

#[cfg(test)]
mod change_invisible_tests {
    use super::change_invisible_extra_info;

    #[test]
    fn extra_info_tracks_java_retry_topic_classification() {
        for (topic, retry) in [
            ("oracle-topic", "0"),
            ("%RETRY%consumer", "1"),
            ("%RETRY%consumer+P1", "2"),
        ] {
            assert_eq!(
                change_invisible_extra_info(17, 123, 456, 2, topic, "broker", 2),
                format!("17 123 456 2 {retry} broker 2 17")
            );
        }
    }
}

#[cfg(test)]
mod export_pop_record_tests {
    use super::MQClientApi;
    use crate::config::ClientConfig;
    use bytes::BytesMut;
    use rocketmq_classic_protocol::codes::{LanguageCode, request};
    use rocketmq_classic_remoting::{
        RemotingCommand,
        client::{RemotingClient, TransportMode},
        codec::frame,
    };
    use serde_json::Value;
    use std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::Instant,
    };

    #[tokio::test]
    async fn export_pop_record_matches_java_fixture_and_keeps_supplied_address_with_vip_enabled() {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let signatures = fixture["exportPopRecordApi"].as_array().unwrap();
        assert!(signatures.iter().any(|signature| signature
            == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.exportPopRecord(java.lang.String,long)"));

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("export-pop-record");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        for case in fixture["exportPopRecordOperations"]["cases"]
            .as_array()
            .unwrap()
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let expected_code = case["responseCode"].as_i64().unwrap() as i32;
            let expected_remark = case["responseRemark"].as_str().map(str::to_owned);
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.unwrap();
                let mut encoded = BytesMut::from(&length[..]);
                encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
                stream.read_exact(&mut encoded[4..]).await.unwrap();
                let request = frame::decode_next(&mut encoded).unwrap().unwrap();

                let mut reply = RemotingCommand {
                    code: expected_code,
                    opaque: request.opaque,
                    remark: expected_remark,
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
                request
            });

            let result = api
                .export_pop_record(&address, Instant::now() + Duration::from_secs(2))
                .await;
            if case["outcome"] == "returned" {
                result.unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(crate::error::RpcError::Response {
                        request_code: request::POP_ROLLBACK,
                        peer,
                        code,
                        remark,
                    }) if peer == address
                        && code == expected_code
                        && remark.as_deref() == case["errorRemark"].as_str()
                ));
            }

            let request = peer.await.unwrap();
            assert_eq!(request.code, request::POP_ROLLBACK);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.ext_fields.is_none());
            assert!(request.body.is_none());
        }
        client.shutdown().await;
    }

    #[tokio::test]
    async fn switch_timer_engine_matches_java_fixture_and_keeps_supplied_address_with_vip_enabled()
    {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let signatures = fixture["switchTimerEngineApi"].as_array().unwrap();
        assert!(signatures.iter().any(|signature| signature
            == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.switchTimerEngine(java.lang.String,java.lang.String,long)"));

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("switch-timer-engine");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        for case in fixture["switchTimerEngineOperations"]["cases"]
            .as_array()
            .unwrap()
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let expected_code = case["responseCode"].as_i64().unwrap() as i32;
            let expected_remark = case["responseRemark"].as_str().map(str::to_owned);
            let expected_fields: BTreeMap<String, String> = case["requests"][0]["extFields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                .collect();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.unwrap();
                let mut encoded = BytesMut::from(&length[..]);
                encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
                stream.read_exact(&mut encoded[4..]).await.unwrap();
                let request = frame::decode_next(&mut encoded).unwrap().unwrap();

                let mut reply = RemotingCommand {
                    code: expected_code,
                    opaque: request.opaque,
                    remark: expected_remark,
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
                request
            });

            let result = api
                .switch_timer_engine(
                    &address,
                    case["engineType"].as_str().unwrap(),
                    Instant::now() + Duration::from_secs(2),
                )
                .await;
            if case["outcome"] == "returned" {
                result.unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(crate::error::RpcError::Response {
                        request_code: request::SWITCH_TIMER_ENGINE,
                        peer,
                        code,
                        remark,
                    }) if peer == address
                        && code == expected_code
                        && remark.as_deref() == case["errorRemark"].as_str()
                ));
            }

            let request = peer.await.unwrap();
            assert_eq!(request.code, request::SWITCH_TIMER_ENGINE);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.body.is_none());
            assert_eq!(request.ext_fields.unwrap_or_default(), expected_fields);
        }
        client.shutdown().await;
    }
}

#[cfg(test)]
mod trigger_lite_dispatch_tests {
    use super::MQClientApi;
    use crate::config::ClientConfig;
    use bytes::BytesMut;
    use rocketmq_classic_protocol::codes::{LanguageCode, request};
    use rocketmq_classic_remoting::{
        RemotingCommand,
        client::{RemotingClient, TransportMode},
        codec::frame,
    };
    use serde_json::Value;
    use std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::Instant,
    };

    #[tokio::test]
    async fn trigger_lite_dispatch_matches_java_fixture_vip_and_nullable_header_fields() {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let signatures = fixture["triggerLiteDispatchApi"].as_array().unwrap();
        assert!(signatures.iter().any(|signature| signature
            == "void org.apache.rocketmq.client.impl.MQClientAPIImpl.triggerLiteDispatch(java.lang.String,java.lang.String,java.lang.String,long)"));

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("trigger-lite-dispatch");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        for case in fixture["triggerLiteDispatchOperations"]["cases"]
            .as_array()
            .unwrap()
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server_addr = listener.local_addr().unwrap();
            let address = format!("127.0.0.1:{}", server_addr.port() + 2);
            let expected_code = case["responseCode"].as_i64().unwrap() as i32;
            let expected_remark = case["responseRemark"].as_str().map(str::to_owned);
            let expected_fields: BTreeMap<String, String> = case["expectedExtFields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                .collect();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.unwrap();
                let mut encoded = BytesMut::from(&length[..]);
                encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
                stream.read_exact(&mut encoded[4..]).await.unwrap();
                let request = frame::decode_next(&mut encoded).unwrap().unwrap();

                let mut reply = RemotingCommand {
                    code: expected_code,
                    opaque: request.opaque,
                    remark: expected_remark,
                    ..Default::default()
                };
                reply.mark_response();
                stream
                    .write_all(&frame::encode(&reply).unwrap())
                    .await
                    .unwrap();
                request
            });

            let result = api
                .trigger_lite_dispatch(
                    &address,
                    case["group"].as_str(),
                    case["clientId"].as_str(),
                    Instant::now() + Duration::from_secs(2),
                )
                .await;
            if case["outcome"] == "returned" {
                result.unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(crate::error::RpcError::Response {
                        request_code: request::TRIGGER_LITE_DISPATCH,
                        peer,
                        code,
                        remark,
                    }) if peer == address
                        && code == expected_code
                        && remark.as_deref() == case["errorRemark"].as_str()
                ));
            }

            let request = peer.await.unwrap();
            assert_eq!(request.code, request::TRIGGER_LITE_DISPATCH);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.body.is_none());
            assert_eq!(request.ext_fields.unwrap_or_default(), expected_fields);
        }
        client.shutdown().await;
    }
}

#[cfg(test)]
mod broker_lite_info_tests {
    use super::MQClientApi;
    use crate::config::ClientConfig;
    use bytes::BytesMut;
    use rocketmq_classic_protocol::codes::{LanguageCode, request};
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

    #[tokio::test]
    async fn get_broker_lite_info_uses_vip_address_and_decodes_nullable_body() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("get-broker-lite-info");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let address = format!("127.0.0.1:{}", server_addr.port() + 2);
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::GET_BROKER_LITE_INFO);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.ext_fields.is_none());
            assert!(request.body.is_none());

            let mut reply = RemotingCommand {
                code: 0,
                opaque: request.opaque,
                body: Some(br#"{"storeType":"rocksdb","maxLmqNum":12}"#.to_vec()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let result = api
            .get_broker_lite_info(&address, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.get_store_type(), Some("rocksdb"));
        assert_eq!(result.get_max_lmq_num(), 12);
        peer.await.unwrap();
        client.shutdown().await;
    }
}

#[cfg(test)]
mod parent_topic_info_tests {
    use super::MQClientApi;
    use crate::config::ClientConfig;
    use bytes::BytesMut;
    use rocketmq_classic_protocol::codes::{LanguageCode, request};
    use rocketmq_classic_remoting::{
        RemotingCommand,
        client::{RemotingClient, TransportMode},
        codec::frame,
    };
    use std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::Instant,
    };

    #[tokio::test]
    async fn get_parent_topic_info_uses_vip_address_and_typed_header() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("get-parent-topic-info");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let address = format!("127.0.0.1:{}", server_addr.port() + 2);
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::GET_PARENT_TOPIC_INFO);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert_eq!(
                request.ext_fields,
                Some(BTreeMap::from([("topic".into(), "lite-parent".into())]).into())
            );
            assert!(request.body.is_none());

            let mut reply = RemotingCommand {
                code: 0,
                opaque: request.opaque,
                body: Some(
                    br#"{"topic":"lite-parent","ttl":3600,"groups":["workers"],"lmqNum":3,"liteTopicCount":12}"#
                        .to_vec(),
                ),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let result = api
            .get_parent_topic_info(
                &address,
                Some("lite-parent"),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.get_topic(), Some("lite-parent"));
        assert_eq!(result.get_ttl(), 3600);
        assert_eq!(result.get_groups().unwrap().with(|groups| groups.len()), 1);
        assert_eq!(result.get_lmq_num(), 3);
        assert_eq!(result.get_lite_topic_count(), 12);
        peer.await.unwrap();
        client.shutdown().await;
    }

    #[tokio::test]
    async fn get_lite_client_info_uses_vip_address_and_default_max_count() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("get-lite-client-info");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let address = format!("127.0.0.1:{}", server_addr.port() + 2);
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::GET_LITE_CLIENT_INFO);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert_eq!(
                request.ext_fields,
                Some(
                    BTreeMap::from([
                        ("parentTopic".into(), "lite-parent".into()),
                        ("group".into(), "workers".into()),
                        ("clientId".into(), "client-a".into()),
                        ("maxCount".into(), "1000".into()),
                    ])
                    .into()
                )
            );
            assert!(request.body.is_none());

            let mut reply = RemotingCommand {
                code: 0,
                opaque: request.opaque,
                body: Some(
                    br#"{"parentTopic":"lite-parent","group":"workers","clientId":"client-a","lastAccessTime":1000,"lastConsumeTime":500,"liteTopicCount":2,"liteTopicSet":["%LMQ%lite-parent%orders"]}"#.to_vec(),
                ),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let result = api
            .get_lite_client_info(
                &address,
                Some("lite-parent"),
                Some("workers"),
                Some("client-a"),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.get_parent_topic(), Some("lite-parent"));
        assert_eq!(result.get_group(), Some("workers"));
        assert_eq!(result.get_client_id(), Some("client-a"));
        assert_eq!(result.get_last_access_time(), 1000);
        assert_eq!(result.get_last_consume_time(), 500);
        assert_eq!(result.get_lite_topic_count(), 2);
        assert_eq!(
            result
                .get_lite_topic_set()
                .unwrap()
                .with(|topics| topics.len()),
            1
        );
        peer.await.unwrap();
        client.shutdown().await;
    }

    #[tokio::test]
    async fn get_lite_group_info_accepts_null_inputs_and_negative_top_k() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("get-lite-group-info");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let address = format!("127.0.0.1:{}", server_addr.port() + 2);
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::GET_LITE_GROUP_INFO);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert_eq!(
                request.ext_fields,
                Some(BTreeMap::from([("topK".into(), "-3".into())]).into())
            );
            assert!(request.body.is_none());

            let mut reply = RemotingCommand {
                code: 0,
                opaque: request.opaque,
                body: Some(br#"{"earliestUnconsumedTimestamp":-1,"totalLagCount":0}"#.to_vec()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let result = api
            .get_lite_group_info(
                &address,
                None,
                None,
                -3,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.get_group(), None);
        assert_eq!(result.get_earliest_unconsumed_timestamp(), -1);
        assert_eq!(result.get_total_lag_count(), 0);
        assert!(result.get_lag_count_top_k().is_none());
        peer.await.unwrap();
        client.shutdown().await;
    }
}
