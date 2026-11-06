use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
    time::{SystemTime, UNIX_EPOCH},
};

use rocketmq_classic_protocol::{
    codes::{LanguageCode, request, response},
    header::{CommandHeader, HeaderMap, ProtocolError},
};
use rocketmq_classic_remoting::RemotingCommand;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{MapAccess, Visitor},
    ser::SerializeMap,
};
use serde_json::value::RawValue;

use super::{MQClientApi, classify, java_client_request_command};
use crate::error::RpcError;

/// Typed null-dereference outcome for Java controller DTO methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ControllerDtoError {
    #[error("{operation} dereferenced null `{field}`")]
    NullDereference {
        operation: &'static str,
        field: &'static str,
    },
}

fn null_dereference(operation: &'static str, field: &'static str) -> ControllerDtoError {
    ControllerDtoError::NullDereference { operation, field }
}

/// Java `GetMetaDataResponseHeader` returned by a controller.
#[derive(Debug, Clone, Default)]
pub struct GetMetaDataResponseHeader {
    pub group: Option<String>,
    pub controller_leader_id: Option<String>,
    pub controller_leader_address: Option<String>,
    pub is_leader: bool,
    pub peers: Option<String>,
}

impl GetMetaDataResponseHeader {
    /// Builds the Java full-argument constructor equivalent.
    pub fn new(
        group: Option<String>,
        controller_leader_id: Option<String>,
        controller_leader_address: Option<String>,
        is_leader: bool,
        peers: Option<String>,
    ) -> Self {
        Self {
            group,
            controller_leader_id,
            controller_leader_address,
            is_leader,
            peers,
        }
    }
}

impl Display for GetMetaDataResponseHeader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "GetMetaDataResponseHeader{{group='{}', controllerLeaderId='{}', controllerLeaderAddress='{}', isLeader={}, peers='{}'}}",
            java_nullable_string(self.group.as_deref()),
            java_nullable_string(self.controller_leader_id.as_deref()),
            java_nullable_string(self.controller_leader_address.as_deref()),
            self.is_leader,
            java_nullable_string(self.peers.as_deref())
        )
    }
}

impl CommandHeader for GetMetaDataResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        insert_optional(fields, "group", self.group.as_deref());
        insert_optional(
            fields,
            "controllerLeaderId",
            self.controller_leader_id.as_deref(),
        );
        insert_optional(
            fields,
            "controllerLeaderAddress",
            self.controller_leader_address.as_deref(),
        );
        fields.insert("isLeader".into(), self.is_leader.to_string());
        insert_optional(fields, "peers", self.peers.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            group: fields.get("group").cloned(),
            controller_leader_id: fields.get("controllerLeaderId").cloned(),
            controller_leader_address: fields.get("controllerLeaderAddress").cloned(),
            // Java's primitive boolean defaults to false when the ext field is absent
            // or is not equal to "true", ignoring case.
            is_leader: fields
                .get("isLeader")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            peers: fields.get("peers").cloned(),
        })
    }
}

/// Java `ElectMasterResponseHeader` returned by a successful election request.
#[derive(Debug, Clone, Default)]
pub struct ElectMasterResponseHeader {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<String>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
}

impl ElectMasterResponseHeader {
    /// Builds the Java full-argument constructor equivalent.
    pub fn new(
        master_broker_id: Option<i64>,
        master_address: Option<String>,
        master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
    ) -> Self {
        Self {
            master_broker_id,
            master_address,
            master_epoch,
            sync_state_set_epoch,
        }
    }
}

impl Display for ElectMasterResponseHeader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ElectMasterResponseHeader{{masterBrokerId={}, masterAddress='{}', masterEpoch={}, syncStateSetEpoch={}}}",
            java_nullable_number(self.master_broker_id),
            java_nullable_string(self.master_address.as_deref()),
            java_nullable_number(self.master_epoch),
            java_nullable_number(self.sync_state_set_epoch)
        )
    }
}

impl CommandHeader for ElectMasterResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        insert_optional_number(fields, "masterBrokerId", self.master_broker_id);
        insert_optional(fields, "masterAddress", self.master_address.as_deref());
        insert_optional_number(fields, "masterEpoch", self.master_epoch);
        insert_optional_number(fields, "syncStateSetEpoch", self.sync_state_set_epoch);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            master_broker_id: parse_optional(fields, "masterBrokerId")?,
            master_address: fields.get("masterAddress").cloned(),
            master_epoch: parse_optional(fields, "masterEpoch")?,
            sync_state_set_epoch: parse_optional(fields, "syncStateSetEpoch")?,
        })
    }
}

struct RawObjectEntries(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for RawObjectEntries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntriesVisitor;

        impl<'de> Visitor<'de> for EntriesVisitor {
            type Value = RawObjectEntries;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<M>(self, mut entries: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(entry) = entries.next_entry::<String, Box<RawValue>>()? {
                    values.push(entry);
                }
                Ok(RawObjectEntries(values))
            }
        }

        deserializer.deserialize_map(EntriesVisitor)
    }
}

fn serialize_optional_replicas_info_table<S>(
    value: &Option<HashMap<Option<String>, Option<ReplicasInfo>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(entries) = value else {
        return serializer.serialize_none();
    };
    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        map.serialize_entry(key.as_deref().unwrap_or("null"), value)?;
    }
    map.end()
}

#[allow(clippy::type_complexity)]
fn deserialize_optional_replicas_info_table<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<Option<String>, Option<ReplicasInfo>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let RawObjectEntries(entries) =
        serde_json::from_str(raw.get()).map_err(<D::Error as serde::de::Error>::custom)?;
    let mut replicas = HashMap::with_capacity(entries.len());
    for (key, value) in entries {
        let key = (key != "null").then_some(key);
        let value = if value.get().trim() == "null" {
            None
        } else {
            Some(
                serde_json::from_str(value.get())
                    .map_err(<D::Error as serde::de::Error>::custom)?,
            )
        };
        replicas.insert(key, value);
    }
    Ok(Some(replicas))
}

fn serialize_optional_broker_addresses<S>(
    value: &Option<HashMap<Option<i64>, Option<String>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(entries) = value else {
        return serializer.serialize_none();
    };
    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        let key = key.map_or_else(|| "null".to_owned(), |key| key.to_string());
        map.serialize_entry(&key, value)?;
    }
    map.end()
}

#[allow(clippy::type_complexity)]
fn deserialize_optional_broker_addresses<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<Option<i64>, Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<RawValue>::deserialize(deserializer)?;
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let RawObjectEntries(entries) =
        serde_json::from_str(raw.get()).map_err(<D::Error as serde::de::Error>::custom)?;
    let mut addresses = HashMap::with_capacity(entries.len());
    for (key, raw_value) in entries {
        let key = if key == "null" {
            None
        } else {
            Some(
                key.parse::<i64>()
                    .map_err(<D::Error as serde::de::Error>::custom)?,
            )
        };
        let value = if raw_value.get().trim() == "null" {
            None
        } else {
            let value: serde_json::Value = serde_json::from_str(raw_value.get())
                .map_err(<D::Error as serde::de::Error>::custom)?;
            Some(match value {
                serde_json::Value::String(value) => value,
                serde_json::Value::Null => unreachable!("null values were handled above"),
                value => value.to_string(),
            })
        };
        addresses.insert(key, value);
    }
    Ok(Some(addresses))
}

/// Java `BrokerReplicasInfo` returned by `getInSyncStateData`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerReplicasInfo {
    #[serde(
        default = "empty_optional_map",
        serialize_with = "serialize_optional_replicas_info_table",
        deserialize_with = "deserialize_optional_replicas_info_table"
    )]
    pub replicas_info_table: Option<HashMap<Option<String>, Option<ReplicasInfo>>>,
}

impl Default for BrokerReplicasInfo {
    fn default() -> Self {
        Self {
            replicas_info_table: Some(HashMap::new()),
        }
    }
}

impl BrokerReplicasInfo {
    /// Adds or replaces one entry as Java `addReplicaInfo` does.
    pub fn add_replica_info(
        &mut self,
        broker_name: Option<String>,
        replicas_info: Option<ReplicasInfo>,
    ) -> Result<(), ControllerDtoError> {
        self.replicas_info_table
            .as_mut()
            .ok_or_else(|| {
                null_dereference("BrokerReplicasInfo.addReplicaInfo", "replicasInfoTable")
            })?
            .insert(broker_name, replicas_info);
        Ok(())
    }
}

/// Replica state for one broker, corresponding to Java's nested
/// `BrokerReplicasInfo.ReplicasInfo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicasInfo {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<String>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
    pub in_sync_replicas: Option<Vec<Option<ReplicaIdentity>>>,
    pub not_in_sync_replicas: Option<Vec<Option<ReplicaIdentity>>>,
}

impl ReplicasInfo {
    /// Builds the Java `ReplicasInfo` constructor equivalent.
    pub fn new(
        master_broker_id: Option<i64>,
        master_address: Option<String>,
        master_epoch: i32,
        sync_state_set_epoch: i32,
        in_sync_replicas: Option<Vec<Option<ReplicaIdentity>>>,
        not_in_sync_replicas: Option<Vec<Option<ReplicaIdentity>>>,
    ) -> Self {
        Self {
            master_broker_id,
            master_address,
            master_epoch: Some(master_epoch),
            sync_state_set_epoch: Some(sync_state_set_epoch),
            in_sync_replicas,
            not_in_sync_replicas,
        }
    }

    /// Checks the Java in-sync replica membership path.
    pub fn is_exist_in_sync(
        &self,
        broker_name: Option<&str>,
        broker_id: Option<i64>,
        broker_address: Option<&str>,
    ) -> Result<bool, ControllerDtoError> {
        self.contains_replica(
            self.in_sync_replicas.as_deref(),
            "inSyncReplicas",
            broker_name,
            broker_id,
            broker_address,
        )
    }

    /// Checks the Java not-in-sync replica membership path.
    pub fn is_exist_in_not_sync(
        &self,
        broker_name: Option<&str>,
        broker_id: Option<i64>,
        broker_address: Option<&str>,
    ) -> Result<bool, ControllerDtoError> {
        self.contains_replica(
            self.not_in_sync_replicas.as_deref(),
            "notInSyncReplicas",
            broker_name,
            broker_id,
            broker_address,
        )
    }

    /// Checks both Java replica lists and preserves its left-to-right
    /// short-circuit behavior.
    pub fn is_exist_in_all_replicas(
        &self,
        broker_name: Option<&str>,
        broker_id: Option<i64>,
        broker_address: Option<&str>,
    ) -> Result<bool, ControllerDtoError> {
        if self.is_exist_in_sync(broker_name, broker_id, broker_address)? {
            return Ok(true);
        }
        self.is_exist_in_not_sync(broker_name, broker_id, broker_address)
    }

    fn contains_replica(
        &self,
        replicas: Option<&[Option<ReplicaIdentity>]>,
        list_field: &'static str,
        broker_name: Option<&str>,
        broker_id: Option<i64>,
        broker_address: Option<&str>,
    ) -> Result<bool, ControllerDtoError> {
        let replicas =
            replicas.ok_or_else(|| null_dereference("ReplicasInfo.isExistIn*", list_field))?;
        let needle = ReplicaIdentity::new(
            broker_name.map(str::to_owned),
            broker_id,
            broker_address.map(str::to_owned),
        );
        for replica in replicas {
            if needle.java_equals(
                replica
                    .as_ref()
                    .map(|replica| replica as &dyn std::any::Any),
            )? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// One replica identity in a Java `BrokerReplicasInfo` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaIdentity {
    pub broker_name: Option<String>,
    pub broker_id: Option<i64>,
    pub broker_address: Option<String>,
    pub alive: Option<bool>,
}

impl ReplicaIdentity {
    /// Builds Java's three-argument constructor, which defaults `alive` to false.
    pub fn new(
        broker_name: Option<String>,
        broker_id: Option<i64>,
        broker_address: Option<String>,
    ) -> Self {
        Self::with_alive(broker_name, broker_id, broker_address, Some(false))
    }

    /// Builds Java's four-argument constructor, preserving nullable `alive`.
    pub fn with_alive(
        broker_name: Option<String>,
        broker_id: Option<i64>,
        broker_address: Option<String>,
        alive: Option<bool>,
    ) -> Self {
        Self {
            broker_name,
            broker_id,
            broker_address,
            alive,
        }
    }

    /// Maps Java `equals(Object)`; Java compares only broker name, ID, and address.
    pub fn java_equals(
        &self,
        other: Option<&dyn std::any::Any>,
    ) -> Result<bool, ControllerDtoError> {
        let Some(other) = other else {
            return Ok(false);
        };
        let Some(other) = other.downcast_ref::<Self>() else {
            return Ok(false);
        };
        if std::ptr::eq(self, other) {
            return Ok(true);
        }

        let broker_name = self
            .broker_name
            .as_deref()
            .ok_or_else(|| null_dereference("ReplicaIdentity.equals", "brokerName"))?;
        if Some(broker_name) != other.broker_name.as_deref() {
            return Ok(false);
        }

        let broker_id = self
            .broker_id
            .ok_or_else(|| null_dereference("ReplicaIdentity.equals", "brokerId"))?;
        if Some(broker_id) != other.broker_id {
            return Ok(false);
        }

        let broker_address = self
            .broker_address
            .as_deref()
            .ok_or_else(|| null_dereference("ReplicaIdentity.equals", "brokerAddress"))?;
        Ok(Some(broker_address) == other.broker_address.as_deref())
    }

    /// Returns Java `Objects.hash(brokerName, brokerId, brokerAddress)`.
    pub fn java_hash_code(&self) -> i32 {
        java_objects_hash_code(&[
            java_optional_string_hash(self.broker_name.as_deref()),
            java_optional_long_hash(self.broker_id),
            java_optional_string_hash(self.broker_address.as_deref()),
        ])
    }
}

impl Display for ReplicaIdentity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ReplicaIdentity{{brokerName='{}', brokerId={}, brokerAddress='{}', alive={}}}",
            java_nullable_string(self.broker_name.as_deref()),
            java_nullable_number(self.broker_id),
            java_nullable_string(self.broker_address.as_deref()),
            java_nullable_number(self.alive)
        )
    }
}

/// Java `EpochEntryCache` returned by `getBrokerEpochCache`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochEntryCache {
    pub cluster_name: Option<String>,
    pub broker_name: Option<String>,
    #[serde(default)]
    pub broker_id: i64,
    pub epoch_list: Option<Vec<Option<EpochEntry>>>,
    #[serde(default)]
    pub max_offset: i64,
}

impl EpochEntryCache {
    /// Builds the Java `EpochEntryCache` constructor equivalent.
    pub fn new(
        cluster_name: Option<String>,
        broker_name: Option<String>,
        broker_id: i64,
        epoch_list: Option<Vec<Option<EpochEntry>>>,
        max_offset: i64,
    ) -> Self {
        Self {
            cluster_name,
            broker_name,
            broker_id,
            epoch_list,
            max_offset,
        }
    }
}

impl Display for EpochEntryCache {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "EpochEntryCache{{clusterName='{}', brokerName='{}', brokerId={}, epochList={}, maxOffset={}}}",
            java_nullable_string(self.cluster_name.as_deref()),
            java_nullable_string(self.broker_name.as_deref()),
            self.broker_id,
            java_nullable_list(self.epoch_list.as_deref()),
            self.max_offset
        )
    }
}

/// One commit-log epoch range returned in an `EpochEntryCache`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochEntry {
    #[serde(default)]
    pub epoch: i32,
    #[serde(default)]
    pub start_offset: i64,
    #[serde(default = "last_epoch_end_offset")]
    pub end_offset: i64,
}

impl EpochEntry {
    /// Copies the three epoch offsets from another Java `EpochEntry`.
    pub fn from_entry(entry: &Self) -> Self {
        Self {
            epoch: entry.epoch,
            start_offset: entry.start_offset,
            end_offset: entry.end_offset,
        }
    }

    /// Builds Java's two-argument constructor, defaulting the end to
    /// `Long.MAX_VALUE`.
    pub fn new(epoch: i32, start_offset: i64) -> Self {
        Self {
            epoch,
            start_offset,
            end_offset: i64::MAX,
        }
    }

    /// Builds Java's three-argument constructor.
    pub fn with_end_offset(epoch: i32, start_offset: i64, end_offset: i64) -> Self {
        Self {
            epoch,
            start_offset,
            end_offset,
        }
    }

    /// Returns Java `Objects.hash(epoch, startOffset, endOffset)`.
    pub fn java_hash_code(&self) -> i32 {
        java_objects_hash_code(&[
            self.epoch,
            java_long_hash(self.start_offset),
            java_long_hash(self.end_offset),
        ])
    }
}

impl Display for EpochEntry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "EpochEntry{{epoch={}, startOffset={}, endOffset={}}}",
            self.epoch, self.start_offset, self.end_offset
        )
    }
}

fn last_epoch_end_offset() -> i64 {
    i64::MAX
}

/// Java `BrokerMemberGroup` returned by a successful master election.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerMemberGroup {
    pub cluster: Option<String>,
    pub broker_name: Option<String>,
    #[serde(
        default = "empty_optional_map",
        serialize_with = "serialize_optional_broker_addresses",
        deserialize_with = "deserialize_optional_broker_addresses"
    )]
    pub broker_addrs: Option<HashMap<Option<i64>, Option<String>>>,
}

impl Default for BrokerMemberGroup {
    fn default() -> Self {
        Self {
            cluster: None,
            broker_name: None,
            broker_addrs: Some(HashMap::new()),
        }
    }
}

impl BrokerMemberGroup {
    /// Builds Java's `(cluster, brokerName)` constructor, initializing an empty
    /// broker-address map.
    pub fn new(cluster: Option<String>, broker_name: Option<String>) -> Self {
        Self {
            cluster,
            broker_name,
            broker_addrs: Some(HashMap::new()),
        }
    }

    /// Returns the smallest broker ID, matching Java `minimumBrokerId()`.
    pub fn minimum_broker_id(&self) -> Result<i64, ControllerDtoError> {
        let broker_addrs = self
            .broker_addrs
            .as_ref()
            .ok_or_else(|| null_dereference("BrokerMemberGroup.minimumBrokerId", "brokerAddrs"))?;
        if broker_addrs.is_empty() {
            return Ok(0);
        }
        broker_addrs
            .keys()
            .try_fold(None::<i64>, |minimum, broker_id| {
                let broker_id = broker_id.ok_or_else(|| {
                    null_dereference("BrokerMemberGroup.minimumBrokerId", "brokerAddrs key")
                })?;
                Ok::<_, ControllerDtoError>(Some(
                    minimum.map_or(broker_id, |min| min.min(broker_id)),
                ))
            })
            .map(|minimum| minimum.unwrap_or_default())
    }

    /// Returns Java Guava `Objects.hashCode(cluster, brokerName, brokerAddrs)`.
    pub fn java_hash_code(&self) -> i32 {
        java_objects_hash_code(&[
            java_optional_string_hash(self.cluster.as_deref()),
            java_optional_string_hash(self.broker_name.as_deref()),
            self.broker_addrs.as_ref().map_or(0, |addresses| {
                addresses
                    .iter()
                    .map(|(broker_id, address)| {
                        java_optional_long_hash(*broker_id)
                            ^ java_optional_string_hash(address.as_deref())
                    })
                    .fold(0, i32::wrapping_add)
            }),
        ])
    }
}

impl Display for BrokerMemberGroup {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let broker_addrs = self.broker_addrs.as_ref().map_or_else(
            || "null".to_owned(),
            |addresses| {
                let mut entries = addresses.iter().collect::<Vec<_>>();
                entries.sort_by_key(|(broker_id, _)| **broker_id);
                let entries = entries
                    .into_iter()
                    .map(|(broker_id, address)| {
                        format!(
                            "{}={}",
                            java_nullable_number(*broker_id),
                            java_nullable_string(address.as_deref())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        write!(
            formatter,
            "BrokerMemberGroup{{cluster='{}', brokerName='{}', brokerAddrs={broker_addrs}}}",
            java_nullable_string(self.cluster.as_deref()),
            java_nullable_string(self.broker_name.as_deref())
        )
    }
}

fn java_nullable_string(value: Option<&str>) -> &str {
    value.unwrap_or("null")
}

fn java_nullable_number<T: Display>(value: Option<T>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn java_nullable_list<T: Display>(values: Option<&[Option<T>]>) -> String {
    values.map_or_else(
        || "null".to_owned(),
        |values| {
            let values = values
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map_or_else(|| "null".to_owned(), ToString::to_string)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{values}]")
        },
    )
}

fn empty_optional_map<K, V>() -> Option<HashMap<K, V>> {
    Some(HashMap::new())
}

fn java_objects_hash_code(values: &[i32]) -> i32 {
    values
        .iter()
        .fold(1, |hash, value| hash.wrapping_mul(31).wrapping_add(*value))
}

fn java_long_hash(value: i64) -> i32 {
    let value = value as u64;
    (value ^ (value >> 32)) as i32
}

fn java_optional_long_hash(value: Option<i64>) -> i32 {
    value.map_or(0, java_long_hash)
}

fn java_optional_string_hash(value: Option<&str>) -> i32 {
    value.map_or(0, |value| {
        value.encode_utf16().fold(0, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        })
    })
}

fn insert_optional(fields: &mut HeaderMap, name: &str, value: Option<&str>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value.to_owned());
    }
}

fn insert_optional_number<T: ToString>(fields: &mut HeaderMap, name: &str, value: Option<T>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value.to_string());
    }
}

fn parse_optional<T: std::str::FromStr>(
    fields: &HeaderMap,
    name: &'static str,
) -> Result<Option<T>, ProtocolError>
where
    T::Err: std::fmt::Debug,
{
    fields
        .get(name)
        .map(|value| {
            value
                .parse()
                .map_err(|error: T::Err| ProtocolError::InvalidField {
                    field: name,
                    reason: format!("{error:?}"),
                })
        })
        .transpose()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn malformed_response(code: i32, peer: &str, reason: impl Into<String>) -> RpcError {
    RpcError::MalformedResponse {
        request_code: code,
        peer: peer.to_owned(),
        reason: reason.into(),
    }
}

fn decode_nullable_body<T>(
    body: Option<&[u8]>,
    code: i32,
    peer: &str,
    decoder: impl FnOnce(&[u8]) -> Result<Option<T>, serde_json::Error>,
) -> Result<Option<T>, RpcError> {
    let Some(body) = body.filter(|body| !body.is_empty()) else {
        return Ok(None);
    };
    decoder(body).map_err(|error| malformed_response(code, peer, error.to_string()))
}

impl MQClientApi {
    /// Reads controller metadata through Java `MQClientAPIImpl.getControllerMetaData`.
    /// The request uses the Java method's fixed 3000 millisecond timeout.
    pub async fn get_controller_meta_data(
        &self,
        controller_address: &str,
    ) -> Result<GetMetaDataResponseHeader, RpcError> {
        let code = request::CONTROLLER_GET_METADATA_INFO;
        let reply = self
            .invoke_controller(controller_address, code, None, None, 3000)
            .await?;
        let reply = classify(reply, code, controller_address)?;
        let fields = reply.ext_fields.as_ref().cloned().unwrap_or_default();
        GetMetaDataResponseHeader::decode_fields(&fields)
            .map_err(|error| malformed_response(code, controller_address, error.to_string()))
    }

    /// Reads replication state through Java `MQClientAPIImpl.getInSyncStateData`.
    /// It first asks the supplied controller for metadata, then sends the broker
    /// list to the returned leader. A null broker list is serialized as JSON null.
    pub async fn get_in_sync_state_data(
        &self,
        controller_address: &str,
        brokers: Option<&[String]>,
    ) -> Result<Option<BrokerReplicasInfo>, RpcError> {
        let metadata = self.get_controller_meta_data(controller_address).await?;
        let leader = controller_leader_address(
            metadata.controller_leader_address.as_deref(),
            request::CONTROLLER_GET_SYNC_STATE_DATA,
        )?;
        let body =
            serde_json::to_vec(&brokers).map_err(|source| RpcError::RequestSerialization {
                request_code: request::CONTROLLER_GET_SYNC_STATE_DATA,
                source,
            })?;
        let reply = self
            .invoke_controller(
                leader,
                request::CONTROLLER_GET_SYNC_STATE_DATA,
                None,
                Some(body),
                3000,
            )
            .await?;
        let reply = classify(reply, request::CONTROLLER_GET_SYNC_STATE_DATA, leader)?;
        decode_nullable_body(
            reply.body.as_deref(),
            request::CONTROLLER_GET_SYNC_STATE_DATA,
            leader,
            rocketmq_classic_protocol::decode_java_remoting_serializable_nullable::<
                BrokerReplicasInfo,
            >,
        )
    }

    /// Reads the Broker's epoch cache through Java
    /// `MQClientAPIImpl.getBrokerEpochCache`, using its fixed 3000 millisecond
    /// timeout and nullable response body.
    pub async fn get_broker_epoch_cache(
        &self,
        broker_address: &str,
    ) -> Result<Option<EpochEntryCache>, RpcError> {
        let code = request::GET_BROKER_EPOCH_CACHE;
        let reply = self
            .invoke_controller(broker_address, code, None, None, 3000)
            .await?;
        let reply = classify(reply, code, broker_address)?;
        decode_nullable_body(
            reply.body.as_deref(),
            code,
            broker_address,
            rocketmq_classic_protocol::decode_java_remoting_serializable_nullable::<EpochEntryCache>,
        )
    }

    /// Reads Java `MQClientAPIImpl.getControllerConfig` from the supplied
    /// controllers, or from the remoting client's NameServer list when the
    /// controller list is null or empty. A missing list after fallback returns
    /// `None`; malformed Properties values are represented as `None` per entry.
    pub async fn get_controller_config(
        &self,
        controllers: Option<&[String]>,
        timeout_millis: i64,
    ) -> Result<Option<HashMap<String, Option<HashMap<String, String>>>>, RpcError> {
        let addresses = controllers
            .filter(|addresses| !addresses.is_empty())
            .map(<[_]>::to_vec)
            .or_else(|| self.remoting.get_name_server_address_list())
            .unwrap_or_default();
        if addresses.is_empty() {
            return Ok(None);
        }

        let code = request::GET_CONTROLLER_CONFIG;
        let mut configurations = HashMap::with_capacity(addresses.len());
        for address in addresses {
            let reply = self
                .invoke_controller(&address, code, None, None, timeout_millis)
                .await?;
            let reply = classify(reply, code, &address)?;
            let body = reply.body.as_deref().ok_or_else(|| {
                malformed_response(code, &address, "successful reply has no body")
            })?;
            let properties =
                crate::auth::parse_java_properties(String::from_utf8_lossy(body).as_bytes());
            configurations.insert(address, properties);
        }
        Ok(Some(configurations))
    }

    /// Updates Java controller configuration. Empty Properties or an absent
    /// controller list is a no-op; non-success replies are accumulated and the
    /// last error is returned after all controllers have been called.
    pub async fn update_controller_config(
        &self,
        properties: Option<&HashMap<String, String>>,
        controllers: Option<&[String]>,
        timeout_millis: i64,
    ) -> Result<(), RpcError> {
        let properties =
            properties.ok_or(RpcError::Header(ProtocolError::MissingField("properties")))?;
        if properties.is_empty() || controllers.is_none_or(<[_]>::is_empty) {
            return Ok(());
        }

        // Java Properties iteration order is unspecified. Keep one body for
        // every controller, while matching its key=value newline encoding.
        let mut body = String::new();
        for (key, value) in properties {
            body.push_str(key);
            body.push('=');
            body.push_str(value);
            body.push('\n');
        }
        let code = request::UPDATE_CONTROLLER_CONFIG;
        let mut last_error = None;
        for address in controllers.unwrap_or_default() {
            let reply = self
                .invoke_controller(
                    address,
                    code,
                    None,
                    Some(body.as_bytes().to_vec()),
                    timeout_millis,
                )
                .await?;
            if reply.code != response::SUCCESS {
                last_error = Some(RpcError::Response {
                    request_code: code,
                    peer: address.clone(),
                    code: reply.code,
                    remark: reply.remark,
                });
            }
        }
        last_error.map_or(Ok(()), Err)
    }

    /// Elects a Broker master through Java `MQClientAPIImpl.electMaster`.
    /// Controller metadata is read first and the request is sent to its leader.
    pub async fn elect_master(
        &self,
        controller_address: &str,
        cluster_name: &str,
        broker_name: &str,
        broker_id: Option<i64>,
    ) -> Result<(ElectMasterResponseHeader, Option<BrokerMemberGroup>), RpcError> {
        let metadata = self.get_controller_meta_data(controller_address).await?;
        let leader = controller_leader_address(
            metadata.controller_leader_address.as_deref(),
            request::CONTROLLER_ELECT_MASTER,
        )?;
        let mut fields = HeaderMap::new();
        fields.insert("clusterName".into(), cluster_name.to_owned());
        fields.insert("brokerName".into(), broker_name.to_owned());
        insert_optional_number(&mut fields, "brokerId", broker_id);
        fields.insert("designateElect".into(), "true".into());
        fields.insert("invokeTime".into(), now_millis().to_string());

        let reply = self
            .invoke_controller(
                leader,
                request::CONTROLLER_ELECT_MASTER,
                Some(fields),
                None,
                3000,
            )
            .await?;
        let reply = classify(reply, request::CONTROLLER_ELECT_MASTER, leader)?;
        let fields = reply.ext_fields.as_ref().cloned().unwrap_or_default();
        let header = ElectMasterResponseHeader::decode_fields(&fields).map_err(|error| {
            malformed_response(request::CONTROLLER_ELECT_MASTER, leader, error.to_string())
        })?;
        let broker_member_group = decode_nullable_body(
            reply.body.as_deref(),
            request::CONTROLLER_ELECT_MASTER,
            leader,
            rocketmq_classic_protocol::decode_java_remoting_serializable_nullable::<
                BrokerMemberGroup,
            >,
        )?;
        Ok((header, broker_member_group))
    }

    /// Cleans controller Broker metadata through Java
    /// `MQClientAPIImpl.cleanControllerBrokerData`, after resolving the current
    /// controller leader.
    pub async fn clean_controller_broker_data(
        &self,
        controller_address: &str,
        cluster_name: Option<&str>,
        broker_name: &str,
        broker_controller_ids_to_clean: Option<&str>,
        clean_living_broker: bool,
    ) -> Result<(), RpcError> {
        let metadata = self.get_controller_meta_data(controller_address).await?;
        let leader = controller_leader_address(
            metadata.controller_leader_address.as_deref(),
            request::CLEAN_BROKER_DATA,
        )?;
        let mut fields = HeaderMap::new();
        insert_optional(&mut fields, "clusterName", cluster_name);
        fields.insert("brokerName".into(), broker_name.to_owned());
        insert_optional(
            &mut fields,
            "brokerControllerIdsToClean",
            broker_controller_ids_to_clean,
        );
        fields.insert(
            "isCleanLivingBroker".into(),
            clean_living_broker.to_string(),
        );
        fields.insert("invokeTime".into(), now_millis().to_string());

        let reply = self
            .invoke_controller(leader, request::CLEAN_BROKER_DATA, Some(fields), None, 3000)
            .await?;
        classify(reply, request::CLEAN_BROKER_DATA, leader)?;
        Ok(())
    }

    async fn invoke_controller(
        &self,
        address: &str,
        code: i32,
        ext_fields: Option<HeaderMap>,
        body: Option<Vec<u8>>,
        timeout_millis: i64,
    ) -> Result<RemotingCommand, RpcError> {
        let reply = self
            .remoting
            .invoke_sync_with_timeout_millis(
                address,
                RemotingCommand {
                    code,
                    language: Some(LanguageCode::Java),
                    ext_fields: ext_fields.map(Into::into),
                    body,
                    ..java_client_request_command(code)?
                },
                timeout_millis,
            )
            .await?;
        Ok(reply)
    }
}

fn controller_leader_address(address: Option<&str>, request_code: i32) -> Result<&str, RpcError> {
    address.ok_or_else(|| {
        RpcError::Transport(
            rocketmq_classic_remoting::TransportError::InvalidConfiguration(format!(
                "controller metadata for request {request_code} has no leader address"
            )),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_dtos_decode_java_camel_case_and_nullable_fields() {
        let state: BrokerReplicasInfo = serde_json::from_str(
            r#"{"replicasInfoTable":{"broker-a":{"masterBrokerId":null,"masterAddress":"broker-a:10911","masterEpoch":7,"syncStateSetEpoch":8,"inSyncReplicas":[{"brokerName":"broker-a","brokerId":0,"brokerAddress":"broker-a:10911","alive":true},null],"notInSyncReplicas":null},"null":null}}"#,
        )
        .unwrap();
        let state_round_trip: BrokerReplicasInfo =
            serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        assert!(
            state_round_trip
                .replicas_info_table
                .as_ref()
                .unwrap()
                .contains_key(&None)
        );
        assert!(
            state_round_trip.replicas_info_table.as_ref().unwrap()[&Some("broker-a".to_owned())]
                .as_ref()
                .unwrap()
                .in_sync_replicas
                .as_ref()
                .unwrap()[1]
                .is_none()
        );
        let replicas = state.replicas_info_table.unwrap();
        assert!(replicas.contains_key(&None));
        assert!(replicas[&None].is_none());
        let replica = replicas[&Some("broker-a".to_owned())].as_ref().unwrap();
        assert_eq!(replica.master_broker_id, None);
        assert_eq!(replica.master_epoch, Some(7));
        assert_eq!(
            replica.in_sync_replicas.as_ref().unwrap()[0]
                .as_ref()
                .unwrap()
                .alive,
            Some(true)
        );
        assert!(replica.in_sync_replicas.as_ref().unwrap()[1].is_none());
        assert!(replica.not_in_sync_replicas.is_none());

        let group: BrokerMemberGroup = serde_json::from_str(
            r#"{"cluster":"cluster-a","brokerName":"broker-a","brokerAddrs":{"5":"slave","0":"master","null":"null-address"}}"#,
        )
        .unwrap();
        assert_eq!(
            group.minimum_broker_id(),
            Err(null_dereference(
                "BrokerMemberGroup.minimumBrokerId",
                "brokerAddrs key"
            ))
        );
        let group_round_trip: BrokerMemberGroup =
            serde_json::from_slice(&serde_json::to_vec(&group).unwrap()).unwrap();
        assert!(
            group_round_trip
                .broker_addrs
                .as_ref()
                .unwrap()
                .contains_key(&None)
        );
        let group: BrokerMemberGroup = serde_json::from_str(
            r#"{"cluster":"cluster-a","brokerName":"broker-a","brokerAddrs":{"5":"slave","0":"master"}}"#,
        )
        .unwrap();
        assert_eq!(group.minimum_broker_id(), Ok(0));
        assert_eq!(BrokerMemberGroup::default().minimum_broker_id(), Ok(0));

        let epoch: EpochEntry = serde_json::from_str(r#"{"epoch":4,"startOffset":10}"#).unwrap();
        assert_eq!(epoch.end_offset, i64::MAX);
    }

    #[test]
    fn controller_dto_methods_preserve_java_null_equality_and_short_circuits() {
        let alive = ReplicaIdentity::with_alive(
            Some("broker-a".into()),
            Some(7),
            Some("broker-a:10911".into()),
            Some(true),
        );
        let dead = ReplicaIdentity::with_alive(
            Some("broker-a".into()),
            Some(7),
            Some("broker-a:10911".into()),
            Some(false),
        );
        assert!(alive.java_equals(Some(&dead)).unwrap());
        assert_eq!(alive.java_hash_code(), dead.java_hash_code());

        let matching = ReplicaIdentity::new(
            Some("broker-a".into()),
            Some(7),
            Some("broker-a:10911".into()),
        );
        let replicas = ReplicasInfo::new(
            Some(7),
            Some("broker-a:10911".into()),
            1,
            2,
            Some(vec![Some(matching)]),
            Some(vec![]),
        );
        assert_eq!(
            replicas.is_exist_in_sync(Some("broker-a"), Some(7), Some("broker-a:10911")),
            Ok(true)
        );
        assert_eq!(
            replicas.is_exist_in_not_sync(Some("broker-a"), Some(7), Some("broker-a:10911")),
            Ok(false)
        );
        assert_eq!(
            replicas.is_exist_in_sync(Some("other"), None, None),
            Ok(false)
        );
        assert_eq!(
            replicas.is_exist_in_sync(None, Some(7), Some("broker-a:10911")),
            Err(null_dereference("ReplicaIdentity.equals", "brokerName"))
        );

        let missing_second = ReplicasInfo::new(Some(7), None, 1, 2, Some(vec![]), None);
        assert_eq!(
            missing_second.is_exist_in_all_replicas(Some("broker-a"), Some(7), None),
            Err(null_dereference(
                "ReplicasInfo.isExistIn*",
                "notInSyncReplicas"
            ))
        );
        let short_circuit = ReplicasInfo::new(
            Some(7),
            None,
            1,
            2,
            Some(vec![Some(ReplicaIdentity::new(
                Some("broker-a".into()),
                Some(7),
                Some("addr".into()),
            ))]),
            None,
        );
        assert_eq!(
            short_circuit.is_exist_in_all_replicas(Some("broker-a"), Some(7), Some("addr")),
            Ok(true)
        );
        let missing_first = ReplicasInfo::new(Some(7), None, 1, 2, None, Some(vec![]));
        assert_eq!(
            missing_first.is_exist_in_sync(Some("broker-a"), Some(7), None),
            Err(null_dereference(
                "ReplicasInfo.isExistIn*",
                "inSyncReplicas"
            ))
        );
    }

    #[test]
    fn controller_dto_mutators_minimum_id_and_java_display_match() {
        let mut broker_info = BrokerReplicasInfo::default();
        broker_info.add_replica_info(None, None).unwrap();
        assert!(broker_info.replicas_info_table.as_ref().unwrap()[&None].is_none());
        let mut null_table = BrokerReplicasInfo {
            replicas_info_table: None,
        };
        assert_eq!(
            null_table.add_replica_info(Some("broker-a".into()), None),
            Err(null_dereference(
                "BrokerReplicasInfo.addReplicaInfo",
                "replicasInfoTable"
            ))
        );

        let mut group = BrokerMemberGroup::new(Some("cluster-a".into()), Some("broker-a".into()));
        group
            .broker_addrs
            .as_mut()
            .unwrap()
            .insert(Some(8), Some("broker-8:10911".into()));
        group
            .broker_addrs
            .as_mut()
            .unwrap()
            .insert(Some(2), Some("broker-2:10911".into()));
        assert_eq!(group.minimum_broker_id(), Ok(2));
        assert!(
            group
                .to_string()
                .contains("brokerAddrs={2=broker-2:10911, 8=broker-8:10911}")
        );
        assert_eq!(
            BrokerMemberGroup {
                broker_addrs: None,
                ..BrokerMemberGroup::default()
            }
            .minimum_broker_id(),
            Err(null_dereference(
                "BrokerMemberGroup.minimumBrokerId",
                "brokerAddrs"
            ))
        );

        let epoch = EpochEntry::new(4, 10);
        assert_eq!(
            epoch.to_string(),
            format!(
                "EpochEntry{{epoch=4, startOffset=10, endOffset={}}}",
                i64::MAX
            )
        );
        assert_eq!(
            ReplicaIdentity::with_alive(
                Some("broker-a".into()),
                Some(0),
                Some("broker-a:10911".into()),
                Some(true)
            )
            .to_string(),
            "ReplicaIdentity{brokerName='broker-a', brokerId=0, brokerAddress='broker-a:10911', alive=true}"
        );
        assert_eq!(
            GetMetaDataResponseHeader::new(Some("c".into()), None, None, true, None).to_string(),
            "GetMetaDataResponseHeader{group='c', controllerLeaderId='null', controllerLeaderAddress='null', isLeader=true, peers='null'}"
        );
    }

    #[test]
    fn response_headers_match_java_optional_and_primitive_defaults() {
        let metadata = GetMetaDataResponseHeader::decode_fields(&HeaderMap::new()).unwrap();
        assert!(!metadata.is_leader);
        assert_eq!(metadata.controller_leader_address, None);

        let election = ElectMasterResponseHeader::decode_fields(&HeaderMap::from([
            ("masterBrokerId".into(), "17".into()),
            ("masterEpoch".into(), "not-an-int".into()),
        ]));
        assert!(matches!(
            election,
            Err(ProtocolError::InvalidField {
                field: "masterEpoch",
                ..
            })
        ));
    }
}
