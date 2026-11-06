use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{
        CommandHeader, EndTransactionRequestHeader, HeaderMap, RecallMessageRequestHeader,
        RecallMessageResponseHeader, SendMessageRequestHeaderV1, SendMessageRequestHeaderV2,
        SendMessageResponseHeader,
    },
};
use rocketmq_classic_remoting::RemotingCommand;
use std::sync::OnceLock;
use tokio::time::Instant;

use super::{MQClientApi, decode_header};
use crate::config::read_shared_client_config;
use crate::error::RpcError;

const SEND_SMART_MSG_PROPERTY: &str = "org.apache.rocketmq.client.sendSmartMsg";

fn java_send_smart_msg_enabled() -> bool {
    static SEND_SMART_MSG_ENABLED: OnceLock<bool> = OnceLock::new();
    *SEND_SMART_MSG_ENABLED.get_or_init(|| match std::env::var(SEND_SMART_MSG_PROPERTY) {
        Ok(value) => value.eq_ignore_ascii_case("true"),
        Err(std::env::VarError::NotPresent) => true,
        Err(std::env::VarError::NotUnicode(_)) => false,
    })
}

pub(crate) fn initialize_java_send_smart_msg() {
    let _ = java_send_smart_msg_enabled();
}

fn send_kind_for_message(batch: bool, reply: bool, smart: bool) -> SendKind {
    if reply {
        if smart {
            SendKind::ReplyV2
        } else {
            SendKind::ReplyV1
        }
    } else if batch {
        SendKind::Batch
    } else if smart {
        SendKind::V2
    } else {
        SendKind::V1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendKind {
    V1,
    V2,
    Batch,
    ReplyV1,
    ReplyV2,
}

impl SendKind {
    fn code(self) -> i32 {
        match self {
            Self::V1 => request::SEND_MESSAGE,
            Self::V2 => request::SEND_MESSAGE_V2,
            Self::Batch => request::SEND_BATCH_MESSAGE,
            Self::ReplyV1 => request::SEND_REPLY_MESSAGE,
            Self::ReplyV2 => request::SEND_REPLY_MESSAGE_V2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SendMode {
    Sync,
    Future,
    Oneway,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendStatus {
    SendOk,
    FlushDiskTimeout,
    FlushSlaveTimeout,
    SlaveNotAvailable,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SendStatusLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.client.producer.SendStatus.{0}")]
    UnknownName(String),
}

impl SendStatus {
    pub const fn values() -> [Self; 4] {
        [
            Self::SendOk,
            Self::FlushDiskTimeout,
            Self::FlushSlaveTimeout,
            Self::SlaveNotAvailable,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, SendStatusLookupError> {
        match name {
            None => Err(SendStatusLookupError::NullName),
            Some("SEND_OK") => Ok(Self::SendOk),
            Some("FLUSH_DISK_TIMEOUT") => Ok(Self::FlushDiskTimeout),
            Some("FLUSH_SLAVE_TIMEOUT") => Ok(Self::FlushSlaveTimeout),
            Some("SLAVE_NOT_AVAILABLE") => Ok(Self::SlaveNotAvailable),
            Some(other) => Err(SendStatusLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::SendOk => "SEND_OK",
            Self::FlushDiskTimeout => "FLUSH_DISK_TIMEOUT",
            Self::FlushSlaveTimeout => "FLUSH_SLAVE_TIMEOUT",
            Self::SlaveNotAvailable => "SLAVE_NOT_AVAILABLE",
        }
    }

    pub const fn ordinal(&self) -> i32 {
        match self {
            Self::SendOk => 0,
            Self::FlushDiskTimeout => 1,
            Self::FlushSlaveTimeout => 2,
            Self::SlaveNotAvailable => 3,
        }
    }

    pub const fn java_compare_to(&self, other: &Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for SendStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResult {
    pub status: SendStatus,
    pub msg_id: String,
    pub offset_msg_id: Option<String>,
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub transaction_id: Option<String>,
    pub recall_handle: Option<String>,
    pub raw_resp_body: Option<Vec<u8>>,
    pub region_id: String,
    pub trace_on: bool,
}

impl MQClientApi {
    /// Resolves the producer's Java `MQClientAPIImpl.sendSmartMsg` request
    /// code selection for a prepared message.
    #[doc(hidden)]
    pub fn send_kind_for_message(batch: bool, reply: bool) -> SendKind {
        send_kind_for_message(batch, reply, java_send_smart_msg_enabled())
    }

    /// Calls Java's synchronous `MQClientAPIImpl.resumeCheckHalfMessage` operation.
    /// A non-success Broker response is reported as `false`, matching Java.
    pub async fn resume_check_half_message(
        &self,
        addr: &str,
        topic: &str,
        msg_id: &str,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        self.resume_check_half_message_nullable(addr, Some(topic), Some(msg_id), deadline)
            .await
    }

    /// Nullable form of `resume_check_half_message`; Java's custom header
    /// accepts null values and omits them from the wire fields.
    pub async fn resume_check_half_message_nullable(
        &self,
        addr: &str,
        topic: Option<&str>,
        msg_id: Option<&str>,
        deadline: Instant,
    ) -> Result<bool, RpcError> {
        let mut fields = HeaderMap::new();
        if let Some(topic) = topic {
            fields.insert("topic".into(), topic.into());
        }
        if let Some(msg_id) = msg_id {
            fields.insert("msgId".into(), msg_id.into());
        }
        let vip_enabled = self
            .config
            .as_ref()
            .map(|config| read_shared_client_config(config).vip_channel_enabled())
            .unwrap_or(false);
        let broker_addr = crate::route::broker_vip_channel(vip_enabled, addr)?;
        let reply = self
            .request_fields_with_mode(
                &broker_addr,
                request::RESUME_CHECK_HALF_MESSAGE,
                fields,
                None,
                deadline,
                true,
            )
            .await?;
        Ok(reply.code == response::SUCCESS)
    }

    /// Calls Java's synchronous `MQClientAPIImpl.recallMessage` operation.
    pub async fn recall_message(
        &self,
        addr: &str,
        header: &RecallMessageRequestHeader,
        deadline: Instant,
    ) -> Result<Option<String>, RpcError> {
        let code = request::RECALL_MESSAGE;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = super::classify(reply, code, addr)?;
        let response: RecallMessageResponseHeader = if reply.ext_fields.is_some() {
            super::decode_header(&reply, code, addr)?
        } else {
            RecallMessageResponseHeader::default()
        };
        Ok(response.get_msg_id().map(str::to_owned))
    }

    /// Java's optional latency detector probes a broker with GET_MAX_OFFSET
    /// for queue zero of one cached producer topic. The caller only needs the
    /// result's validity to restore broker reachability.
    #[doc(hidden)]
    pub async fn probe_broker(
        &self,
        addr: &str,
        topic: &str,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let mut fields = HeaderMap::new();
        fields.insert("topic".into(), topic.into());
        fields.insert("queueId".into(), "0".into());
        fields.insert("committed".into(), "true".into());
        let code = request::GET_MAX_OFFSET;
        let reply = self
            .request_fields_with_mode(addr, code, fields, None, deadline, true)
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer: addr.to_owned(),
                code: reply.code,
                remark: reply.remark,
            });
        }
        let offset = reply
            .ext_fields
            .as_ref()
            .and_then(|fields| fields.get("offset"))
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "missing max offset".into(),
            })?;
        offset
            .parse::<i64>()
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: format!("invalid max offset: {error}"),
            })?;
        Ok(())
    }

    /// Performs exactly one Broker attempt; queue selection and retries belong to the producer.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_message(
        &self,
        addr: &str,
        broker_name: &str,
        header: &SendMessageRequestHeaderV1,
        body: Vec<u8>,
        uniq_msg_id: &str,
        kind: SendKind,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, RpcError> {
        let code = kind.code();
        if mode == SendMode::Oneway {
            match kind {
                SendKind::V1 | SendKind::ReplyV1 => {
                    self.oneway(addr, code, header, Some(body), deadline)
                        .await?
                }
                _ => {
                    self.oneway(
                        addr,
                        code,
                        &SendMessageRequestHeaderV2::from_v1(header),
                        Some(body),
                        deadline,
                    )
                    .await?
                }
            }
            return Ok(None);
        }
        let reply = match kind {
            SendKind::V1 | SendKind::ReplyV1 => {
                self.request_with_mode(
                    addr,
                    code,
                    header,
                    Some(body),
                    deadline,
                    mode == SendMode::Sync,
                )
                .await?
            }
            _ => {
                self.request_with_mode(
                    addr,
                    code,
                    &SendMessageRequestHeaderV2::from_v1(header),
                    Some(body),
                    deadline,
                    mode == SendMode::Sync,
                )
                .await?
            }
        };
        let status = match reply.code {
            response::SUCCESS => SendStatus::SendOk,
            response::FLUSH_DISK_TIMEOUT => SendStatus::FlushDiskTimeout,
            response::FLUSH_SLAVE_TIMEOUT => SendStatus::FlushSlaveTimeout,
            response::SLAVE_NOT_AVAILABLE => SendStatus::SlaveNotAvailable,
            code => {
                return Err(RpcError::Response {
                    request_code: kind.code(),
                    peer: addr.to_owned(),
                    code,
                    remark: reply.remark,
                });
            }
        };
        let result: SendMessageResponseHeader = decode_header(&reply, code, addr)?;
        let queue_id = result
            .get_queue_id()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: kind.code(),
                peer: addr.to_owned(),
                reason: "the custom field <queueId> is null".into(),
            })?;
        let queue_offset =
            result
                .get_queue_offset()
                .ok_or_else(|| RpcError::MalformedResponse {
                    request_code: kind.code(),
                    peer: addr.to_owned(),
                    reason: "the custom field <queueOffset> is null".into(),
                })?;
        let ext = reply
            .ext_fields
            .as_ref()
            .expect("decoded response header exists");
        Ok(Some(SendResult {
            status,
            msg_id: uniq_msg_id.to_owned(),
            offset_msg_id: result.get_msg_id().map(str::to_owned),
            topic: header.get_topic().to_owned(),
            broker_name: broker_name.to_owned(),
            queue_id,
            queue_offset,
            transaction_id: result.get_transaction_id().map(str::to_owned),
            recall_handle: result.get_recall_handle().map(str::to_owned),
            raw_resp_body: None,
            region_id: ext
                .get("MSG_REGION")
                .filter(|s| !s.is_empty())
                .cloned()
                .unwrap_or_else(|| "DefaultRegion".into()),
            trace_on: ext.get("TRACE_ON").is_none_or(|value| value != "false"),
        }))
    }

    /// Java's endTransactionOneway sends a remark but never waits for a Broker reply.
    pub async fn end_transaction_oneway(
        &self,
        addr: &str,
        header: &EndTransactionRequestHeader,
        remark: Option<String>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        self.remoting.invoke_oneway(
            addr,
            RemotingCommand {
                code: request::END_TRANSACTION,
                language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                ext_fields: Some(fields.into()),
                remark,
                ..super::java_client_request_command(request::END_TRANSACTION)?
            },
            deadline,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod send_smart_tests {
    use super::*;

    #[test]
    fn send_smart_switch_matches_java_message_reply_and_batch_branches() {
        assert_eq!(send_kind_for_message(false, false, true), SendKind::V2);
        assert_eq!(send_kind_for_message(false, true, true), SendKind::ReplyV2);
        assert_eq!(send_kind_for_message(true, false, true), SendKind::Batch);
        assert_eq!(send_kind_for_message(true, true, true), SendKind::ReplyV2);

        assert_eq!(send_kind_for_message(false, false, false), SendKind::V1);
        assert_eq!(send_kind_for_message(false, true, false), SendKind::ReplyV1);
        assert_eq!(send_kind_for_message(true, false, false), SendKind::Batch);
        assert_eq!(send_kind_for_message(true, true, false), SendKind::ReplyV1);
    }
}

#[cfg(test)]
mod tests {
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
    async fn resume_check_half_message_matches_java_fixture_and_vip_address() {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../../fixtures/producer/core.json")).unwrap();
        let signatures = fixture["resumeCheckHalfMessageApi"].as_array().unwrap();
        assert!(signatures.iter().any(|signature| signature
            == "boolean org.apache.rocketmq.client.impl.MQClientAPIImpl.resumeCheckHalfMessage(java.lang.String,java.lang.String,java.lang.String,long)"));

        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let mut config = ClientConfig::new("resume-check-half-message");
        config.set_vip_channel_enabled(true);
        let api = MQClientApi::with_config(client.clone(), Arc::new(RwLock::new(config)));

        for case in fixture["resumeCheckHalfMessageOperations"]["cases"]
            .as_array()
            .unwrap()
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let actual_address = listener.local_addr().unwrap();
            let actual_port = actual_address.port();
            let advertised_port = actual_port.checked_add(2).unwrap();
            let advertised_address = format!("127.0.0.1:{advertised_port}");
            let expected_code = case["responseCode"].as_i64().unwrap() as i32;
            let expected_remark = case["responseRemark"].as_str().map(str::to_owned);
            let expected_fields: BTreeMap<String, String> = case["requestExtFields"]
                .as_object()
                .map(|fields| {
                    fields
                        .iter()
                        .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                        .collect()
                })
                .unwrap_or_default();
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

            let topic = case["topic"].as_str();
            let msg_id = case["msgId"].as_str();
            let actual = if topic.is_some() && msg_id.is_some() {
                api.resume_check_half_message(
                    &advertised_address,
                    topic.unwrap(),
                    msg_id.unwrap(),
                    Instant::now() + Duration::from_secs(2),
                )
                .await
            } else {
                api.resume_check_half_message_nullable(
                    &advertised_address,
                    topic,
                    msg_id,
                    Instant::now() + Duration::from_secs(2),
                )
                .await
            };
            assert_eq!(actual.unwrap(), case["result"].as_bool().unwrap());

            let request = peer.await.unwrap();
            assert_eq!(request.code, request::RESUME_CHECK_HALF_MESSAGE);
            assert_eq!(request.language, Some(LanguageCode::Java));
            assert!(request.body.is_none());
            assert_eq!(request.ext_fields.unwrap_or_default(), expected_fields);
            assert_eq!(
                crate::route::broker_vip_channel(true, &advertised_address).unwrap(),
                actual_address.to_string()
            );
        }
        client.shutdown().await;
    }
}
