//! POP message receipt data and ACK helpers used by the push consumer.

use std::collections::HashMap;

use rocketmq_classic_common::message::{
    DecodeError, DecodeOptions, MessageExt, decode_many_as_client_compatible,
};
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        extensions::{AckStatus, PopReply},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::header::{
    AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader,
};
use rocketmq_classic_protocol::header::{PopMessageRequestHeader, PopMessageResponseHeader};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{BatchAck, BatchAckMessageRequestBody, BitSet},
};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use tokio::time::Instant;

pub use rocketmq_classic_core::api::extensions::PopStatus;

const PROPERTY_POP_CK: &str = rocketmq_classic_common::message::constants::PROPERTY_POP_CK;
const PROPERTY_FIRST_POP_TIME: &str =
    rocketmq_classic_common::message::constants::PROPERTY_FIRST_POP_TIME;

/// Public POP queue state exposed through the Push rebalance implementation.
///
/// `new()` matches Java's public no-argument constructor. Runtime assignments
/// use the same state with their queue key and generation attached internally.
pub struct PopProcessQueue {
    queue: rocketmq_classic_common::message::MessageQueue,
    generation: u64,
    dropped: AtomicBool,
    wait_ack_count: AtomicI32,
    last_pop_timestamp: AtomicI64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PopProcessQueueError {
    #[error("java.lang.NullPointerException")]
    NullInfo,
}

impl PopProcessQueue {
    pub fn new() -> Self {
        Self::new_for_assignment(rocketmq_classic_common::message::MessageQueue::default(), 0)
    }

    pub(crate) fn new_for_assignment(
        queue: rocketmq_classic_common::message::MessageQueue,
        generation: u64,
    ) -> Self {
        pop_process_queue_pull_max_idle_time().unwrap_or_else(|error| panic!("{error}"));
        Self {
            queue,
            generation,
            dropped: AtomicBool::new(false),
            wait_ack_count: AtomicI32::new(0),
            last_pop_timestamp: AtomicI64::new(epoch_millis()),
        }
    }

    pub(crate) fn queue(&self) -> &rocketmq_classic_common::message::MessageQueue {
        &self.queue
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::Acquire)
    }

    pub fn set_dropped(&self, dropped: bool) {
        self.dropped.store(dropped, Ordering::Release);
    }

    pub(crate) fn drop_assignment(&self) {
        self.set_dropped(true);
    }

    pub fn get_last_pop_timestamp(&self) -> i64 {
        self.last_pop_timestamp()
    }

    pub fn get_wai_ack_msg_count(&self) -> i32 {
        self.wait_ack_count()
    }

    pub fn inc_found_msg(&self, count: i32) {
        self.wait_ack_count.fetch_add(count, Ordering::AcqRel);
    }

    /// Applies Java `decFoundMsg(int)`, whose signed argument is added to the
    /// current counter. The Push consume service passes a negative count.
    pub fn dec_found_msg(&self, delta: i32) {
        self.wait_ack_count.fetch_add(delta, Ordering::AcqRel);
    }

    pub fn fill_pop_process_queue_info(
        &self,
        info: &mut rocketmq_classic_protocol::body::ConsumerPopProcessQueueInfo,
    ) {
        info.set_wait_ack_count(self.get_wai_ack_msg_count());
        info.set_droped(self.is_dropped());
        info.set_last_pop_timestamp(self.get_last_pop_timestamp());
    }

    /// Maps Java's `fillPopProcessQueueInfo(null)` failure.
    pub fn fill_pop_process_queue_info_nullable(
        &self,
        info: Option<&mut rocketmq_classic_protocol::body::ConsumerPopProcessQueueInfo>,
    ) -> Result<(), PopProcessQueueError> {
        let Some(info) = info else {
            return Err(PopProcessQueueError::NullInfo);
        };
        self.fill_pop_process_queue_info(info);
        Ok(())
    }

    pub fn set_last_pop_timestamp(&self, timestamp: i64) {
        self.last_pop_timestamp.store(timestamp, Ordering::Release);
    }

    pub fn ack(&self) -> i32 {
        self.wait_ack_count.fetch_sub(1, Ordering::AcqRel)
    }

    pub fn is_pull_expired(&self) -> bool {
        self.is_pull_expired_with_timeout(java_pull_max_idle_time())
    }

    pub(crate) fn inc_found_messages(&self, count: usize) {
        self.wait_ack_count
            .fetch_add(i32::try_from(count).unwrap_or(i32::MAX), Ordering::AcqRel);
    }

    pub(crate) fn dec_found_messages(&self, count: usize) {
        self.wait_ack_count
            .fetch_sub(i32::try_from(count).unwrap_or(i32::MAX), Ordering::AcqRel);
    }

    pub(crate) fn wait_ack_count(&self) -> i32 {
        self.wait_ack_count.load(Ordering::Acquire)
    }

    pub(crate) fn last_pop_timestamp(&self) -> i64 {
        self.last_pop_timestamp.load(Ordering::Acquire)
    }

    pub(crate) fn is_pull_expired_with_timeout(&self, max_idle_time_millis: i64) -> bool {
        crate::process_queue::pull_idle_time_expired(
            epoch_millis(),
            self.last_pop_timestamp.load(Ordering::Acquire),
            max_idle_time_millis,
        )
    }
}

impl Default for PopProcessQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PopProcessQueue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "PopProcessQueue[waitAckCounter:{}, lastPopTimestamp:{}, drop:{}]",
            self.get_wai_ack_msg_count(),
            self.get_last_pop_timestamp(),
            self.is_dropped()
        )
    }
}

fn java_pull_max_idle_time() -> i64 {
    pop_process_queue_pull_max_idle_time().unwrap_or_else(|error| panic!("{error}"))
}

pub(crate) fn pop_process_queue_pull_max_idle_time() -> Result<i64, String> {
    static JAVA_PULL_MAX_IDLE_TIME: OnceLock<Result<i64, String>> = OnceLock::new();
    crate::process_queue::cached_java_property(
        &JAVA_PULL_MAX_IDLE_TIME,
        crate::process_queue::pull_max_idle_time_from_environment,
    )
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PopProcessError {
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error("FOUND POP response is missing its response header or message body")]
    MissingResponseData,
    #[error("POP response metadata is malformed: {0}")]
    MalformedMetadata(String),
}

/// Java-compatible result passed from POP request execution to its consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopResult {
    messages: Option<Vec<rocketmq_classic_common::message::MessageExt>>,
    status: PopStatus,
    pop_time: i64,
    invisible_time: i64,
    rest_num: i64,
}

impl PopResult {
    pub fn new(
        status: PopStatus,
        messages: Option<Vec<rocketmq_classic_common::message::MessageExt>>,
    ) -> Self {
        Self {
            messages,
            status,
            pop_time: 0,
            invisible_time: 0,
            rest_num: 0,
        }
    }

    pub fn messages(&self) -> Option<&[rocketmq_classic_common::message::MessageExt]> {
        self.messages.as_deref()
    }

    pub fn set_messages(
        &mut self,
        messages: Option<Vec<rocketmq_classic_common::message::MessageExt>>,
    ) {
        self.messages = messages;
    }

    pub const fn status(&self) -> &PopStatus {
        &self.status
    }

    pub fn set_status(&mut self, status: PopStatus) {
        self.status = status;
    }

    pub const fn pop_time(&self) -> i64 {
        self.pop_time
    }

    pub fn set_pop_time(&mut self, pop_time: i64) {
        self.pop_time = pop_time;
    }

    pub const fn invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub fn set_invisible_time(&mut self, invisible_time: i64) {
        self.invisible_time = invisible_time;
    }

    pub const fn rest_num(&self) -> i64 {
        self.rest_num
    }

    pub fn set_rest_num(&mut self, rest_num: i64) {
        self.rest_num = rest_num;
    }
}

/// Applies the pinned Java POP response decoration rules. Receipt checkpoints
/// are mapped by retry category and queue ID, so one response may safely
/// contain messages from multiple queues.
pub(crate) fn process_pop_reply(
    mut reply: PopReply<PopMessageResponseHeader>,
    request: &PopMessageRequestHeader,
    broker_name: &str,
    options: DecodeOptions,
) -> Result<PopResult, PopProcessError> {
    let status = reply.status.clone();
    let mut result = PopResult::new(status.clone(), None);
    let header = reply.header.ok_or(PopProcessError::MissingResponseData)?;
    result.set_rest_num(header.get_rest_num());
    if status != PopStatus::Found {
        return Ok(result);
    }

    result.set_pop_time(header.get_pop_time());
    result.set_invisible_time(header.get_invisible_time());
    let raw = reply
        .raw_messages
        .take()
        .ok_or(PopProcessError::MissingResponseData)?;
    let mut messages = decode_many_as_client_compatible(&raw, options);
    add_receipts(&mut messages, &header, request, broker_name)?;
    result.set_messages(Some(messages));
    Ok(result)
}

fn add_receipts(
    messages: &mut [MessageExt],
    header: &PopMessageResponseHeader,
    request: &PopMessageRequestHeader,
    broker_name: &str,
) -> Result<(), PopProcessError> {
    let start_offsets = header
        .get_start_offset_info()
        .filter(|value| !value.is_empty())
        .map(parse_start_offset_info)
        .transpose()?;
    let message_offsets = header
        .get_msg_offset_info()
        .filter(|value| !value.is_empty())
        .map(parse_message_offset_info)
        .transpose()?;

    let mut sorted_offsets: HashMap<String, Vec<i64>> = HashMap::new();
    if start_offsets.is_some() {
        for message in messages.iter() {
            let retry = retry_category(message.topic(), message.properties().get(PROPERTY_POP_CK))?;
            let key = format!("{retry}@{}", message.queue_id());
            sorted_offsets
                .entry(key)
                .or_default()
                .push(message.queue_offset());
        }
        for offsets in sorted_offsets.values_mut() {
            offsets.sort_unstable();
        }
    }

    let fallback_offsets = if start_offsets.is_none() {
        let mut checkpoints = HashMap::new();
        for message in messages.iter() {
            let retry = retry_category(message.topic(), message.properties().get(PROPERTY_POP_CK))?;
            checkpoints
                .entry(format!("{retry}@{}", message.queue_id()))
                .or_insert(message.queue_offset());
        }
        Some(checkpoints)
    } else {
        None
    };

    for message in messages.iter_mut() {
        if message.properties().contains_key(PROPERTY_POP_CK) {
            continue;
        }
        let retry = retry_category(message.topic(), None)?;
        let key = format!("{retry}@{}", message.queue_id());
        let raw = if let Some(start_offsets) = &start_offsets {
            let start_offset = start_offsets.get(&key).ok_or_else(|| {
                PopProcessError::MalformedMetadata(format!(
                    "startOffsetInfo has no entry for {key}"
                ))
            })?;
            let available_offsets = sorted_offsets.get(&key).ok_or_else(|| {
                PopProcessError::MalformedMetadata(format!(
                    "message offsets have no entry for {key}"
                ))
            })?;
            let index = available_offsets
                .iter()
                .position(|offset| *offset == message.queue_offset())
                .ok_or_else(|| {
                    PopProcessError::MalformedMetadata(format!(
                        "queue offset {} is absent from {key}",
                        message.queue_offset()
                    ))
                })?;
            let queue_offset = message_offsets
                .as_ref()
                .and_then(|offsets| offsets.get(&key))
                .and_then(|offsets| offsets.get(index))
                .copied()
                .ok_or_else(|| {
                    PopProcessError::MalformedMetadata(format!(
                        "msgOffsetInfo has no offset {index} for {key}"
                    ))
                })?;
            format!(
                "{} {} {} {} {} {} {} {}",
                start_offset,
                header.get_pop_time(),
                header.get_invisible_time(),
                header.get_revive_qid(),
                retry,
                broker_name,
                message.queue_id(),
                queue_offset
            )
        } else {
            let checkpoint = fallback_offsets
                .as_ref()
                .and_then(|offsets| offsets.get(&key))
                .copied()
                .unwrap_or(message.queue_offset());
            format!(
                "{} {} {} {} {} {} {} {}",
                checkpoint,
                header.get_pop_time(),
                header.get_invisible_time(),
                header.get_revive_qid(),
                retry,
                broker_name,
                message.queue_id(),
                message.queue_offset()
            )
        };
        let _ = message.set_system_property(PROPERTY_POP_CK, raw);
    }

    let topic = request.get_topic().unwrap_or_default().to_owned();
    for message in messages.iter_mut() {
        message.set_broker_name(Some(broker_name.to_owned()));
        message.set_topic(topic.clone());
        if !message.properties().contains_key(PROPERTY_FIRST_POP_TIME) {
            let _ = message
                .set_system_property(PROPERTY_FIRST_POP_TIME, header.get_pop_time().to_string());
        }
    }
    Ok(())
}

fn retry_category(topic: &str, pop_ck: Option<&String>) -> Result<&'static str, PopProcessError> {
    if let Some(pop_ck) = pop_ck {
        let receipt = PopReceipt::parse(pop_ck.clone())
            .map_err(|error| PopProcessError::MalformedMetadata(error.to_string()))?;
        return match receipt.retry() {
            "0" => Ok("0"),
            "1" => Ok("1"),
            "2" => Ok("2"),
            other => Err(PopProcessError::MalformedMetadata(format!(
                "unknown retry category {other}"
            ))),
        };
    }
    if topic.starts_with("%RETRY%") {
        Ok(if topic.contains('+') { "2" } else { "1" })
    } else {
        Ok("0")
    }
}

fn parse_start_offset_info(value: &str) -> Result<HashMap<String, i64>, PopProcessError> {
    let mut result = HashMap::new();
    for entry in value.split(';') {
        let fields = entry.split(' ').collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(PopProcessError::MalformedMetadata(format!(
                "invalid startOffsetInfo entry {entry:?}"
            )));
        }
        let key = format!("{}@{}", fields[0], fields[1]);
        let offset = fields[2].parse::<i64>().map_err(|_| {
            PopProcessError::MalformedMetadata(format!("invalid start offset in {entry:?}"))
        })?;
        if result.insert(key.clone(), offset).is_some() {
            return Err(PopProcessError::MalformedMetadata(format!(
                "duplicate startOffsetInfo entry {key}"
            )));
        }
    }
    Ok(result)
}

fn parse_message_offset_info(value: &str) -> Result<HashMap<String, Vec<i64>>, PopProcessError> {
    let mut result = HashMap::new();
    for entry in value.split(';') {
        let fields = entry.split(' ').collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(PopProcessError::MalformedMetadata(format!(
                "invalid msgOffsetInfo entry {entry:?}"
            )));
        }
        let key = format!("{}@{}", fields[0], fields[1]);
        let offsets = fields[2]
            .split(',')
            .map(|offset| {
                offset.parse::<i64>().map_err(|_| {
                    PopProcessError::MalformedMetadata(format!(
                        "invalid message offset in {entry:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if result.insert(key.clone(), offsets).is_some() {
            return Err(PopProcessError::MalformedMetadata(format!(
                "duplicate msgOffsetInfo entry {key}"
            )));
        }
    }
    Ok(result)
}

impl std::fmt::Display for PopResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "PopResult [popStatus={},msgFoundList={},restNum={}]",
            self.status,
            self.messages.as_ref().map_or(0, Vec::len),
            self.rest_num
        )
    }
}

/// A validated POP receipt. The original extra-info string is retained because the
/// Broker uses it as an opaque token when acknowledging or extending invisibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopReceipt {
    raw: String,
    checkpoint_queue_offset: i64,
    pop_time: i64,
    invisible_time: i64,
    revive_queue_id: i32,
    retry: String,
    broker_name: String,
    queue_id: i32,
    queue_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PopReceiptError {
    #[error("POP receipt is missing required fields (found {actual}, need at least 8)")]
    MissingFields { actual: usize },
    #[error("POP receipt field {field} is not a valid {kind}")]
    InvalidNumber {
        field: &'static str,
        kind: &'static str,
    },
    #[error("POP receipt queue offset is outside the Java batch ACK bitset range")]
    BatchAckOffsetOutOfRange,
}

#[derive(Debug, thiserror::Error)]
pub enum PopBatchAckError {
    #[error("Java batch ACK input extraInfoList was null")]
    NullExtraInfoList,
    #[error("Java batch ACK input extraInfoList[{index}] was null")]
    NullExtraInfo { index: usize },
    #[error(transparent)]
    Receipt(#[from] PopReceiptError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
}

impl PopReceipt {
    /// Parses the eight fields consumed by Java `ExtraInfoUtil` and retains the
    /// unmodified source string, including any fields added by newer Brokers.
    pub fn parse(raw: impl Into<String>) -> Result<Self, PopReceiptError> {
        let raw = raw.into();
        let mut fields = raw.split(' ').collect::<Vec<_>>();
        while fields.last().is_some_and(|field| field.is_empty()) {
            fields.pop();
        }
        if fields.len() < 8 {
            return Err(PopReceiptError::MissingFields {
                actual: fields.len(),
            });
        }

        Ok(Self {
            checkpoint_queue_offset: parse_i64(fields[0], "checkpoint queue offset", "long")?,
            pop_time: parse_i64(fields[1], "pop time", "long")?,
            invisible_time: parse_i64(fields[2], "invisibility time", "long")?,
            revive_queue_id: parse_i32(fields[3], "revive queue id", "int")?,
            retry: fields[4].to_owned(),
            broker_name: fields[5].to_owned(),
            queue_id: parse_i32(fields[6], "queue id", "int")?,
            queue_offset: parse_i64(fields[7], "queue offset", "long")?,
            raw,
        })
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    pub const fn checkpoint_queue_offset(&self) -> i64 {
        self.checkpoint_queue_offset
    }

    pub const fn pop_time(&self) -> i64 {
        self.pop_time
    }

    pub const fn invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub const fn revive_queue_id(&self) -> i32 {
        self.revive_queue_id
    }

    pub fn retry(&self) -> &str {
        &self.retry
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub const fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub const fn queue_offset(&self) -> i64 {
        self.queue_offset
    }

    /// Resolves the Broker topic encoded by Java's retry discriminator.
    pub fn real_topic(&self, topic: &str, consumer_group: &str) -> String {
        match self.retry.as_str() {
            "1" => format!("%RETRY%{consumer_group}_{topic}"),
            "2" => format!("%RETRY%{consumer_group}+{topic}"),
            _ => topic.to_owned(),
        }
    }

    pub fn ack_header(&self, topic: &str, consumer_group: &str) -> AckMessageRequestHeader {
        {
            let mut header = AckMessageRequestHeader::default();
            header.set_consumer_group(consumer_group.to_owned());
            header.set_topic(self.real_topic(topic, consumer_group));
            header.set_queue_id(self.queue_id);
            header.set_extra_info(self.raw.clone());
            header.set_offset(self.queue_offset);
            header.set_lite_topic(None);
            header
        }
    }

    pub fn change_invisible_header(
        &self,
        topic: &str,
        consumer_group: &str,
        invisible_time: i64,
    ) -> ChangeInvisibleTimeRequestHeader {
        {
            let mut header = ChangeInvisibleTimeRequestHeader::default();
            header.set_consumer_group(Some(consumer_group.to_owned()));
            header.set_topic(Some(self.real_topic(topic, consumer_group)));
            header.set_queue_id(Some(self.queue_id));
            header.set_extra_info(Some(self.raw.clone()));
            header.set_offset(Some(self.queue_offset));
            header.set_invisible_time(Some(invisible_time));
            header.set_lite_topic(None);
            header.set_suspend(false);
            header
        }
    }
}

/// Builds the Java `BatchAckMessageRequestBody` from parsed POP receipts.
/// Receipts with the same retry kind, queue, checkpoint offset and pop time
/// share one ACK with Java-compatible bit positions.
fn build_batch_ack_body_nullable(
    receipts: &[PopReceipt],
    topic: Option<&str>,
    consumer_group: Option<&str>,
) -> Result<BatchAckMessageRequestBody, PopReceiptError> {
    type BatchKey = (String, i32, i64, i64);
    let mut broker_name = None;
    let mut batches = HashMap::<BatchKey, BatchAck>::new();
    let mut insertion_order = Vec::<BatchKey>::new();

    for receipt in receipts {
        broker_name.get_or_insert_with(|| receipt.broker_name.clone());
        let key = (
            receipt.retry.clone(),
            receipt.queue_id,
            receipt.checkpoint_queue_offset,
            receipt.pop_time,
        );
        let batch = batches.entry(key.clone()).or_insert_with(|| {
            insertion_order.push(key);
            let mut batch = BatchAck::default();
            batch.set_consumer_group(consumer_group.map(str::to_owned));
            batch.set_topic(topic.map(str::to_owned));
            batch.set_retry(Some(receipt.retry.clone()));
            batch.set_start_offset(receipt.checkpoint_queue_offset);
            batch.set_queue_id(receipt.queue_id);
            batch.set_revive_queue_id(receipt.revive_queue_id);
            batch.set_pop_time(receipt.pop_time);
            batch.set_invisible_time(receipt.invisible_time);
            batch.set_bit_set(Some(SharedValue::new(BitSet::new())));
            batch
        });
        // Java performs signed-long subtraction followed by an int cast before
        // BitSet.set, so preserve both overflow wraparound and narrowing.
        let bit_index = receipt
            .queue_offset
            .wrapping_sub(receipt.checkpoint_queue_offset) as i32;
        if bit_index < 0 {
            return Err(PopReceiptError::BatchAckOffsetOutOfRange);
        }
        let bit_set = batch
            .get_bit_set()
            .ok_or(PopReceiptError::BatchAckOffsetOutOfRange)?;
        bit_set
            .with_mut(|bit_set| bit_set.set(bit_index))
            .map_err(|_| PopReceiptError::BatchAckOffsetOutOfRange)?;
    }

    let capacity = java_hash_map_capacity(insertion_order.len());
    let mut ordered_keys = insertion_order.into_iter().enumerate().collect::<Vec<_>>();
    ordered_keys.sort_by_key(|(index, key)| (java_batch_key_bucket(key, capacity), *index));

    let mut body = BatchAckMessageRequestBody::default();
    body.set_broker_name(broker_name);
    body.set_acks(Some(SharedValue::new(
        ordered_keys
            .into_iter()
            .map(|(_, key)| {
                Some(SharedValue::new(
                    batches.remove(&key).expect("batch key was collected once"),
                ))
            })
            .collect(),
    )));
    Ok(body)
}

fn java_hash_map_capacity(entries: usize) -> usize {
    let mut capacity = 16usize;
    while entries > capacity - capacity / 4 {
        capacity *= 2;
    }
    capacity
}

fn java_batch_key_bucket(key: &(String, i32, i64, i64), capacity: usize) -> usize {
    let text = format!("{}@{}@{}@{}", key.0, key.1, key.2, key.3);
    let hash = text.encode_utf16().fold(0u32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    ((hash ^ (hash >> 16)) as usize) & (capacity - 1)
}

/// Maps Java's `MQClientAPIImpl.batchAckMessageAsync` overload that accepts
/// `topic`, `consumerGroup`, and POP receipt `extraInfoList` values. The list
/// is grouped into the request body before the asynchronous Broker request.
pub async fn batch_ack_message_from_extra_info(
    api: &MQClientApi,
    addr: &str,
    topic: Option<&str>,
    consumer_group: Option<&str>,
    extra_info_list: Option<&[Option<String>]>,
    deadline: Instant,
) -> Result<AckStatus, PopBatchAckError> {
    let extra_info_list = extra_info_list.ok_or(PopBatchAckError::NullExtraInfoList)?;
    let receipts = extra_info_list
        .iter()
        .enumerate()
        .map(|(index, raw)| {
            let raw = raw
                .as_deref()
                .ok_or(PopBatchAckError::NullExtraInfo { index })?;
            PopReceipt::parse(raw.to_owned()).map_err(PopBatchAckError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let body = build_batch_ack_body_nullable(&receipts, topic, consumer_group)?;
    api.batch_ack_message(addr, &body, deadline)
        .await
        .map_err(PopBatchAckError::from)
}

fn parse_i64(value: &str, field: &'static str, kind: &'static str) -> Result<i64, PopReceiptError> {
    value
        .parse()
        .map_err(|_| PopReceiptError::InvalidNumber { field, kind })
}

fn parse_i32(value: &str, field: &'static str, kind: &'static str) -> Result<i32, PopReceiptError> {
    value
        .parse()
        .map_err(|_| PopReceiptError::InvalidNumber { field, kind })
}

#[cfg(test)]
fn build_batch_ack_body(
    receipts: &[PopReceipt],
    topic: &str,
    consumer_group: &str,
) -> Result<BatchAckMessageRequestBody, PopReceiptError> {
    build_batch_ack_body_nullable(receipts, Some(topic), Some(consumer_group))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use rocketmq_classic_common::message::{Message, MessageHostAddress, encode_message_ext};
    use std::net::{IpAddr, Ipv4Addr};

    fn java_message_record(name: &str) -> Vec<u8> {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == name)
            .unwrap();
        base64::engine::general_purpose::STANDARD
            .decode(case["recordBase64"].as_str().unwrap())
            .unwrap()
    }

    fn message(topic: &str, queue_id: i32, queue_offset: i64) -> MessageExt {
        let mut message = crate::test_support::message_ext_from(Message::new(topic, Vec::new()));
        message.set_queue_id(queue_id);
        message.set_queue_offset(queue_offset);
        message
    }

    #[test]
    fn pop_process_queue_pull_idle_property_keeps_its_first_java_class_value() {
        let property = OnceLock::new();
        assert_eq!(
            crate::process_queue::cached_java_property(&property, || Ok::<_, String>(120_000)),
            Ok(120_000)
        );
        assert_eq!(
            crate::process_queue::cached_java_property(&property, || Ok::<_, String>(5)),
            Ok(120_000)
        );
    }

    #[test]
    fn receipt_matches_pinned_java_fields_and_keeps_the_original_token() {
        let raw = "17 123 456 2 1 broker-a 4 19";
        let receipt = PopReceipt::parse(raw).unwrap();
        assert_eq!(receipt.raw(), raw);
        assert_eq!(receipt.checkpoint_queue_offset(), 17);
        assert_eq!(receipt.pop_time(), 123);
        assert_eq!(receipt.invisible_time(), 456);
        assert_eq!(receipt.revive_queue_id(), 2);
        assert_eq!(receipt.retry(), "1");
        assert_eq!(receipt.broker_name(), "broker-a");
        assert_eq!(receipt.queue_id(), 4);
        assert_eq!(receipt.queue_offset(), 19);
    }

    #[test]
    fn ack_and_invisibility_headers_use_real_topic_and_preserve_receipt() {
        let receipt = PopReceipt::parse("17 123 456 2 2 broker-a 4 19 extra-field").unwrap();
        let ack = receipt.ack_header("orders", "workers");
        assert_eq!(ack.get_topic(), "%RETRY%workers+orders");
        assert_eq!(ack.get_offset(), 19);
        assert_eq!(ack.get_extra_info(), receipt.raw());
        let change = receipt.change_invisible_header("orders", "workers", 9_000);
        assert_eq!(change.get_consumer_group(), Some("workers"));
        assert_eq!(change.get_topic(), Some(ack.get_topic()));
        assert_eq!(change.get_queue_id(), Some(4));
        assert_eq!(change.get_offset(), Some(19));
        assert_eq!(change.get_invisible_time(), Some(9_000));
        assert_eq!(change.get_extra_info(), Some(receipt.raw()));
    }

    #[test]
    fn retry_topic_names_match_the_pinned_java_key_builder() {
        let v1 = PopReceipt::parse("17 123 456 2 1 broker-a 4 19").unwrap();
        let v2 = PopReceipt::parse("17 123 456 2 2 broker-a 4 19").unwrap();
        assert_eq!(v1.real_topic("orders", "workers"), "%RETRY%workers_orders");
        assert_eq!(v2.real_topic("orders", "workers"), "%RETRY%workers+orders");
    }

    #[test]
    fn pop_response_metadata_maps_receipts_by_retry_kind_and_queue() {
        let request = {
            let mut header = PopMessageRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header
        };
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        header.set_revive_qid(2);
        header.set_rest_num(8);
        header.set_start_offset_info(Some("0 1 10;0 2 20".into()));
        header.set_msg_offset_info(Some("0 1 100,101;0 2 200".into()));
        let mut messages = vec![
            message("orders", 1, 5),
            message("orders", 1, 6),
            message("orders", 2, 9),
        ];
        add_receipts(&mut messages, &header, &request, "broker-a").unwrap();

        let receipts = messages
            .iter()
            .map(|message| {
                PopReceipt::parse(message.properties().get(PROPERTY_POP_CK).unwrap().clone())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(receipts[0].checkpoint_queue_offset(), 10);
        assert_eq!(receipts[0].queue_offset(), 100);
        assert_eq!(receipts[1].queue_offset(), 101);
        assert_eq!(receipts[2].checkpoint_queue_offset(), 20);
        assert_eq!(receipts[2].queue_offset(), 200);
        assert_eq!(messages[0].broker_name(), Some("broker-a"));
        assert_eq!(messages[0].topic(), "orders");
        assert_eq!(
            messages[0]
                .properties()
                .get(PROPERTY_FIRST_POP_TIME)
                .map(String::as_str),
            Some("123")
        );
    }

    #[test]
    fn pop_response_without_checkpoint_metadata_uses_first_queue_offset() {
        let request = {
            let mut header = PopMessageRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header
        };
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        header.set_revive_qid(2);
        header.set_rest_num(0);
        header.set_start_offset_info(Some(String::new()));
        header.set_msg_offset_info(Some(String::new()));
        let mut messages = vec![message("orders", 1, 5), message("orders", 1, 6)];
        add_receipts(&mut messages, &header, &request, "broker-a").unwrap();
        let first =
            PopReceipt::parse(messages[0].properties().get(PROPERTY_POP_CK).unwrap()).unwrap();
        let second =
            PopReceipt::parse(messages[1].properties().get(PROPERTY_POP_CK).unwrap()).unwrap();
        assert_eq!(first.checkpoint_queue_offset(), 5);
        assert_eq!(second.checkpoint_queue_offset(), 5);
        assert_eq!(second.queue_offset(), 6);
    }

    #[test]
    fn pop_reply_keeps_decoded_prefix_when_batch_tail_is_malformed() {
        let mut raw = java_message_record("ipv4-v1");
        raw.push(0);
        let request = {
            let mut request = PopMessageRequestHeader::default();
            request.set_topic(Some("orders".to_owned()));
            request
        };
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        let reply = PopReply {
            status: PopStatus::Found,
            header: Some(header),
            raw_messages: Some(raw),
        };

        let result =
            process_pop_reply(reply, &request, "broker-a", DecodeOptions::default()).unwrap();
        let messages = result.messages().unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].is_message_client_ext());
        assert_eq!(messages[0].topic(), "orders");
        assert_eq!(messages[0].message_id(), Some("producer-unique-1"));
        assert!(messages[0].offset_message_id().is_some());
        assert_eq!(
            messages[0]
                .properties()
                .get(PROPERTY_FIRST_POP_TIME)
                .map(String::as_str),
            Some("123")
        );
    }

    #[test]
    fn pop_reply_keeps_all_24_valid_compressed_messages() {
        const BODY_SIZE: usize = 3 * 1024 * 1024;

        let mut message = MessageExt::new();
        message.set_topic("orders");
        message.set_body(vec![0x5a; BODY_SIZE]);
        message.set_sys_flag(rocketmq_classic_common::sysflag::message::COMPRESSED_FLAG);
        message.set_born_host(Some(MessageHostAddress::from_ip_address(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            10_911,
        )));
        message.set_store_host(Some(MessageHostAddress::from_ip_address(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            10_912,
        )));
        let record = encode_message_ext(&message, true).expect("compressed message encodes");
        let mut raw = Vec::new();
        for _ in 0..24 {
            raw.extend_from_slice(&record);
        }
        let mut request = PopMessageRequestHeader::default();
        request.set_topic(Some("orders".to_owned()));
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        header.set_revive_qid(2);
        let reply = PopReply {
            status: PopStatus::Found,
            header: Some(header),
            raw_messages: Some(raw),
        };

        let result = process_pop_reply(reply, &request, "broker-a", DecodeOptions::default())
            .expect("valid compressed POP response decodes");
        let messages = result.messages().unwrap();
        assert_eq!(messages.len(), 24);
        assert!(messages.iter().all(|message| {
            message.body().len() == BODY_SIZE && message.properties().contains_key(PROPERTY_POP_CK)
        }));
    }

    #[test]
    fn malformed_mixed_queue_metadata_is_rejected_before_dispatch() {
        let request = {
            let mut header = PopMessageRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header
        };
        let mut header = PopMessageResponseHeader::default();
        header.set_pop_time(123);
        header.set_invisible_time(456);
        header.set_revive_qid(2);
        header.set_rest_num(0);
        header.set_start_offset_info(Some("0 1 10".into()));
        header.set_msg_offset_info(Some("0 2 100".into()));
        let mut messages = vec![message("orders", 1, 5)];
        assert!(matches!(
            add_receipts(&mut messages, &header, &request, "broker-a"),
            Err(PopProcessError::MalformedMetadata(_))
        ));
    }

    #[test]
    fn batch_ack_groups_receipts_and_sets_java_bit_positions() {
        let receipts = [
            PopReceipt::parse("10 123 456 2 0 broker-a 1 10").unwrap(),
            PopReceipt::parse("10 123 456 2 0 broker-a 1 12").unwrap(),
            PopReceipt::parse("20 200 300 3 2 broker-a 2 21").unwrap(),
        ];
        let body = build_batch_ack_body(&receipts, "orders", "workers").unwrap();
        assert_eq!(body.get_broker_name(), Some("broker-a"));
        let batches = body
            .get_acks()
            .unwrap()
            .snapshot()
            .into_iter()
            .flatten()
            .map(|ack| ack.snapshot())
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 2);
        let first = batches
            .iter()
            .find(|batch| batch.get_start_offset() == 10)
            .unwrap();
        let bits = first.get_bit_set().unwrap().snapshot();
        assert!(bits.get(0).unwrap());
        assert!(bits.get(2).unwrap());
        let retry = batches
            .iter()
            .find(|batch| batch.get_start_offset() == 20)
            .unwrap();
        assert_eq!(retry.get_retry(), Some("2"));
        assert!(
            retry
                .get_bit_set()
                .unwrap()
                .with(|bits| bits.get(1).unwrap())
        );
    }

    #[test]
    fn batch_ack_rejects_offsets_that_overflow_the_java_bitset_index() {
        for raw in [
            "10 123 456 2 1 broker-a 4 9",
            "0 123 456 2 1 broker-a 4 2147483648",
        ] {
            let receipt = PopReceipt::parse(raw).unwrap();
            assert!(matches!(
                build_batch_ack_body(&[receipt], "orders", "workers"),
                Err(PopReceiptError::BatchAckOffsetOutOfRange)
            ));
        }

        let wrapped =
            PopReceipt::parse("9223372036854775807 123 456 2 1 broker-a 4 -9223372036854775808")
                .unwrap();
        let body = build_batch_ack_body(&[wrapped], "orders", "workers").unwrap();
        let ack = body
            .get_acks()
            .unwrap()
            .snapshot()
            .into_iter()
            .next()
            .unwrap()
            .unwrap()
            .snapshot();
        assert!(ack.get_bit_set().unwrap().with(|bits| bits.get(1).unwrap()));
    }

    #[test]
    fn receipt_rejects_missing_fields_and_numeric_overflow() {
        assert!(matches!(
            PopReceipt::parse("17 123 456 2 1 broker-a 4"),
            Err(PopReceiptError::MissingFields { .. })
        ));
        assert!(matches!(
            PopReceipt::parse("17 123 456 999999999999999999999999 1 broker-a 4 19"),
            Err(PopReceiptError::InvalidNumber {
                field: "revive queue id",
                ..
            })
        ));
        assert!(matches!(
            PopReceipt::parse("17 123 456 2 1 broker-a 4 9223372036854775808"),
            Err(PopReceiptError::InvalidNumber {
                field: "queue offset",
                ..
            })
        ));
    }
}
