use std::any::{Any, type_name};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use super::host_address::{
    MessageHostAddress, MessageHostAddressBytesError, parse_java_i32, parse_java_i64,
};

pub const NAME_VALUE_SEPARATOR: char = '\u{1}';
pub const PROPERTY_SEPARATOR: char = '\u{2}';

pub type MessageProperties = BTreeMap<String, String>;
/// Java-style property map capable of representing null keys and values.
pub type NullableMessageProperties = BTreeMap<Option<String>, Option<String>>;

#[derive(Debug, Clone, Default)]
struct MessagePropertyStorage {
    values: MessageProperties,
    null_values: BTreeSet<String>,
    null_key_value: Option<Option<String>>,
}

/// A cloneable, mutable handle to one message property map.
///
/// Java's `MessageAccessor.setProperties` retains the supplied `Map` object,
/// so updates through either the caller's map or `Message.getProperties()` are
/// visible through the other reference. This Rust handle preserves that
/// behavior while keeping property access synchronized and ownership-safe.
#[derive(Clone, Default)]
pub struct SharedMessageProperties {
    storage: Arc<RwLock<MessagePropertyStorage>>,
}

impl SharedMessageProperties {
    /// Creates a handle to a present, empty property map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a shared handle initialized from non-null string entries.
    pub fn from_properties(properties: MessageProperties) -> Self {
        Self {
            storage: Arc::new(RwLock::new(MessagePropertyStorage {
                values: properties,
                ..MessagePropertyStorage::default()
            })),
        }
    }

    /// Creates a shared handle that can retain Java-style null keys and values.
    pub fn from_nullable_properties(properties: NullableMessageProperties) -> Self {
        let handle = Self::new();
        handle.extend(properties);
        handle
    }

    /// Returns the value for a key, preserving the distinction between an
    /// absent entry and an entry whose value is null.
    pub fn get(&self, key: Option<&str>) -> Option<Option<String>> {
        let properties = read_message_state(&self.storage);
        match key {
            Some(key) => properties
                .values
                .get(key)
                .cloned()
                .map(Some)
                .or_else(|| properties.null_values.contains(key).then_some(None)),
            None => properties.null_key_value.clone(),
        }
    }

    /// Inserts or replaces a Java-style map entry and returns its old value.
    pub fn insert(&self, key: Option<String>, value: Option<String>) -> Option<Option<String>> {
        let mut properties = write_message_state(&self.storage);
        match key {
            Some(key) => {
                let previous = properties
                    .values
                    .remove(&key)
                    .map(Some)
                    .or_else(|| properties.null_values.remove(&key).then_some(None));
                match value {
                    Some(value) => {
                        properties.values.insert(key, value);
                    }
                    None => {
                        properties.null_values.insert(key);
                    }
                }
                previous
            }
            None => properties.null_key_value.replace(value),
        }
    }

    /// Returns whether the map contains the supplied key, including null keys.
    pub fn contains_key(&self, key: Option<&str>) -> bool {
        let properties = read_message_state(&self.storage);
        match key {
            Some(key) => {
                properties.values.contains_key(key) || properties.null_values.contains(key)
            }
            None => properties.null_key_value.is_some(),
        }
    }

    /// Removes an entry and returns its old value, if present.
    pub fn remove(&self, key: Option<&str>) -> Option<Option<String>> {
        let mut properties = write_message_state(&self.storage);
        match key {
            Some(key) => properties
                .values
                .remove(key)
                .map(Some)
                .or_else(|| properties.null_values.remove(key).then_some(None)),
            None => properties.null_key_value.take(),
        }
    }

    /// Removes all entries without detaching this handle from attached messages.
    pub fn clear(&self) {
        *write_message_state(&self.storage) = MessagePropertyStorage::default();
    }

    /// Returns the number of entries, including null keys and values.
    pub fn len(&self) -> usize {
        let properties = read_message_state(&self.storage);
        properties.values.len()
            + properties.null_values.len()
            + usize::from(properties.null_key_value.is_some())
    }

    /// Returns true when the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Adds or replaces entries from an owned set of Java-style map entries.
    pub fn extend<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (Option<String>, Option<String>)>,
    {
        for (key, value) in entries {
            self.insert(key, value);
        }
    }

    /// Returns an owned snapshot of all entries, including null keys/values.
    pub fn snapshot(&self) -> NullableMessageProperties {
        let properties = read_message_state(&self.storage);
        properties
            .values
            .iter()
            .map(|(key, value)| (Some(key.clone()), Some(value.clone())))
            .chain(
                properties
                    .null_values
                    .iter()
                    .filter(|key| !properties.values.contains_key(*key))
                    .map(|key| (Some(key.clone()), None)),
            )
            .chain(
                properties
                    .null_key_value
                    .as_ref()
                    .map(|value| (None, value.clone())),
            )
            .collect()
    }

    /// Iterates over an owned snapshot so the map lock is not held by the iterator.
    pub fn iter(&self) -> impl Iterator<Item = (Option<String>, Option<String>)> {
        self.snapshot().into_iter()
    }
}

impl std::fmt::Debug for SharedMessageProperties {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_map().entries(self.snapshot()).finish()
    }
}

/// A cloneable handle to one mutable message body byte array.
///
/// Java's `Message.setBody(byte[])` retains the supplied array, and
/// `Message.getBody()` returns that same array. This handle preserves the
/// caller/message alias while exposing synchronized byte-slice access.
#[derive(Clone, Default)]
pub struct SharedMessageBody {
    storage: Arc<RwLock<Vec<u8>>>,
}

impl SharedMessageBody {
    /// Creates a shared body handle initialized with the supplied bytes.
    pub fn new(body: impl Into<Vec<u8>>) -> Self {
        Self {
            storage: Arc::new(RwLock::new(body.into())),
        }
    }

    /// Reads the current bytes while holding a shared lock for the callback.
    pub fn with_bytes<R>(&self, read: impl FnOnce(&[u8]) -> R) -> R {
        let body = read_message_state(&self.storage);
        read(&body)
    }

    /// Mutates the current bytes while holding an exclusive lock for the callback.
    pub fn with_bytes_mut<R>(&self, update: impl FnOnce(&mut [u8]) -> R) -> R {
        let mut body = write_message_state(&self.storage);
        update(&mut body)
    }

    /// Returns an owned snapshot of the current bytes.
    pub fn snapshot(&self) -> Vec<u8> {
        read_message_state(&self.storage).clone()
    }

    /// Returns the fixed byte-array length.
    pub fn len(&self) -> usize {
        read_message_state(&self.storage).len()
    }

    /// Returns whether the byte array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether two handles refer to the same body array.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.storage, &other.storage)
    }
}

impl std::fmt::Debug for SharedMessageBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SharedMessageBody")
            .field(&self.snapshot())
            .finish()
    }
}

struct MessageBodyReadView<'a>(Option<RwLockReadGuard<'a, Vec<u8>>>);

impl Deref for MessageBodyReadView<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().map(|body| &body[..]).unwrap_or_default()
    }
}

impl AsRef<[u8]> for MessageBodyReadView<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

enum MessageExtBodyReadSource<'a> {
    Stored(MessageBodyReadView<'a>),
    Runtime(Vec<u8>),
}

/// Read view for Java's virtual `MessageExt.getBody()` method.
struct MessageExtBodyReadView<'a>(MessageExtBodyReadSource<'a>);

impl Deref for MessageExtBodyReadView<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            MessageExtBodyReadSource::Stored(body) => body,
            MessageExtBodyReadSource::Runtime(body) => body,
        }
    }
}

impl AsRef<[u8]> for MessageExtBodyReadView<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for MessageExtBodyReadView<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("MessageExtBodyReadView")
            .field(&&self[..])
            .finish()
    }
}

impl PartialEq for MessageExtBodyReadView<'_> {
    fn eq(&self, other: &Self) -> bool {
        self[..] == other[..]
    }
}

impl Eq for MessageExtBodyReadView<'_> {}

impl std::fmt::Debug for MessageBodyReadView<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, formatter)
    }
}

impl PartialEq for MessageBodyReadView<'_> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for MessageBodyReadView<'_> {
    fn eq(&self, other: &&[u8; N]) -> bool {
        **self == other[..]
    }
}

impl PartialEq<Vec<u8>> for MessageBodyReadView<'_> {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_ref() == other.as_slice()
    }
}

impl PartialEq<&[u8]> for MessageBodyReadView<'_> {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_ref() == *other
    }
}

struct MessageBodyWriteView<'a>(RwLockWriteGuard<'a, Vec<u8>>);

impl Deref for MessageBodyWriteView<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MessageBodyWriteView<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

enum MessagePropertiesReadSource<'a> {
    Shared(RwLockReadGuard<'a, MessagePropertyStorage>),
    Runtime(MessageProperties),
    Empty,
}

struct MessagePropertiesReadView<'a>(MessagePropertiesReadSource<'a>);

impl Deref for MessagePropertiesReadView<'_> {
    type Target = MessageProperties;

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            MessagePropertiesReadSource::Shared(properties) => &properties.values,
            MessagePropertiesReadSource::Runtime(properties) => properties,
            MessagePropertiesReadSource::Empty => empty_message_properties(),
        }
    }
}

impl std::fmt::Debug for MessagePropertiesReadView<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, formatter)
    }
}

impl PartialEq for MessagePropertiesReadView<'_> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Serialize for MessagePropertiesReadView<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}

struct MessagePropertiesWriteView<'a>(RwLockWriteGuard<'a, MessagePropertyStorage>);

impl Deref for MessagePropertiesWriteView<'_> {
    type Target = MessageProperties;

    fn deref(&self) -> &Self::Target {
        &self.0.values
    }
}

impl DerefMut for MessagePropertiesWriteView<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.values
    }
}

fn read_message_state<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_message_state<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicFilterType {
    SingleTag,
    MultiTag,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessagePropertyError {
    #[error("property key cannot be empty or blank")]
    EmptyKey,
    #[error("property value cannot be empty or blank")]
    EmptyValue,
    #[error("user property name is reserved by RocketMQ")]
    ReservedProperty,
    #[error("The priority must be greater than or equal to 0")]
    NegativePriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MessageKeyCollectionError {
    #[error("Java Message.setKeys(Collection) rejects a null collection")]
    NullCollection,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid Java integer in message property {property}: {value:?}")]
pub struct MessagePropertyParseError {
    property: &'static str,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid Java integer value: {value:?}")]
pub struct MessageSysFlagParseError {
    value: String,
}

/// RocketMQ message value corresponding to Java's `Message` API.
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// let _body = message.take_body();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// let _body = message.take_body_nullable();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// message.set_property("key", "value");
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// message.properties_mut();
/// ```
///
/// Java `Message.toString()` is represented by Rust's `Display`/`ToString`;
/// its formatter helper is not a separate public method:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let _ = Message::default().java_to_string();
/// ```
///
/// Java `Message` inherits identity-based `Object.equals`/`hashCode`; Rust
/// `PartialEq`/`Eq`/`Hash` use an identity token rather than message fields.
/// Rust `Clone` preserves the token; `clone_message()` creates the distinct
/// object represented by Java `MessageAccessor.cloneMessage`:
///
/// ```
/// use rocketmq_classic_common::message::Message;
/// let message = Message::default();
/// assert_eq!(message, message);
/// assert_eq!(message, message.clone());
/// assert_ne!(message, message.clone_message());
/// ```
pub struct Message {
    topic: Option<String>,
    body: Option<Arc<RwLock<Vec<u8>>>>,
    flag: i32,
    properties: Option<Arc<RwLock<MessagePropertyStorage>>>,
    properties_read_initialized: AtomicBool,
    transaction_id: Option<String>,
    identity: Arc<()>,
}

#[derive(Serialize, Deserialize)]
struct NullablePropertyValue {
    value: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct MessageSerde {
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    body: Option<Vec<u8>>,
    #[serde(default)]
    flag: i32,
    #[serde(default)]
    properties: Option<BTreeMap<String, Option<String>>>,
    #[serde(
        default,
        rename = "nullKeyProperty",
        skip_serializing_if = "Option::is_none"
    )]
    null_key_property: Option<NullablePropertyValue>,
    #[serde(
        default,
        rename = "transactionId",
        skip_serializing_if = "Option::is_none"
    )]
    transaction_id: Option<String>,
}

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let property_snapshot = match self.properties.as_ref() {
            Some(properties) => {
                let properties = read_message_state(properties);
                let mut nullable = properties
                    .values
                    .iter()
                    .map(|(key, value)| (key.clone(), Some(value.clone())))
                    .collect::<BTreeMap<_, _>>();
                for key in &properties.null_values {
                    nullable.entry(key.clone()).or_insert(None);
                }
                Some(nullable)
            }
            None if self
                .properties_read_initialized
                .load(AtomicOrdering::Relaxed) =>
            {
                Some(BTreeMap::new())
            }
            None => None,
        };
        MessageSerde {
            topic: self.topic.clone(),
            body: self
                .body
                .as_ref()
                .map(|body| read_message_state(body).clone()),
            flag: self.flag,
            properties: property_snapshot,
            null_key_property: self
                .properties
                .as_ref()
                .and_then(|properties| read_message_state(properties).null_key_value.clone())
                .clone()
                .map(|value| NullablePropertyValue { value }),
            transaction_id: self.transaction_id.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MessageSerde::deserialize(deserializer)?;
        let properties = match wire.properties {
            Some(values) => {
                let mut properties = BTreeMap::new();
                let mut null_values = BTreeSet::new();
                for (key, value) in values {
                    if let Some(value) = value {
                        properties.insert(key, value);
                    } else {
                        null_values.insert(key);
                    }
                }
                Some(Arc::new(RwLock::new(MessagePropertyStorage {
                    values: properties,
                    null_values,
                    null_key_value: None,
                })))
            }
            None => None,
        };
        if let (Some(properties), Some(null_key_property)) = (
            &properties,
            wire.null_key_property.map(|property| property.value),
        ) {
            write_message_state(properties).null_key_value = Some(null_key_property);
        }
        Ok(Self {
            topic: wire.topic,
            body: wire.body.map(|body| Arc::new(RwLock::new(body))),
            flag: wire.flag,
            properties,
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: wire.transaction_id,
            identity: Arc::new(()),
        })
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Message")
            .field("topic", &self.topic)
            .field("body", &self.body)
            .field("flag", &self.flag)
            .field("properties", &self.properties)
            .field(
                "properties_read_initialized",
                &self.properties_read_initialized,
            )
            .field("transaction_id", &self.transaction_id)
            .finish()
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}

impl Eq for Message {}

impl Hash for Message {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.identity).hash(state);
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            body: self
                .body
                .as_ref()
                .map(|body| Arc::new(RwLock::new(read_message_state(body).clone()))),
            flag: self.flag,
            properties: self
                .properties
                .as_ref()
                .map(|properties| Arc::new(RwLock::new(read_message_state(properties).clone()))),
            properties_read_initialized: AtomicBool::new(
                self.properties_read_initialized
                    .load(AtomicOrdering::Relaxed),
            ),
            transaction_id: self.transaction_id.clone(),
            identity: self.identity.clone(),
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            topic: None,
            body: None,
            flag: 0,
            properties: None,
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
            identity: Arc::new(()),
        }
    }
}

fn empty_message_properties() -> &'static MessageProperties {
    static EMPTY: OnceLock<MessageProperties> = OnceLock::new();
    EMPTY.get_or_init(MessageProperties::new)
}

fn string_properties(properties: NullableMessageProperties) -> MessageProperties {
    properties
        .into_iter()
        .filter_map(|(key, value)| key.zip(value))
        .collect()
}

impl Message {
    pub(crate) fn from_decoded(
        topic: String,
        body: Option<Vec<u8>>,
        properties: Option<MessageProperties>,
    ) -> Self {
        Self {
            topic: Some(topic),
            body: body.map(|body| Arc::new(RwLock::new(body))),
            flag: 0,
            properties: properties.map(|values| {
                Arc::new(RwLock::new(MessagePropertyStorage {
                    values,
                    ..MessagePropertyStorage::default()
                }))
            }),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
            identity: Arc::new(()),
        }
    }

    pub(crate) fn from_compact(body: Vec<u8>, flag: i32, properties: MessageProperties) -> Self {
        Self {
            topic: None,
            body: Some(Arc::new(RwLock::new(body))),
            flag,
            properties: Some(Arc::new(RwLock::new(MessagePropertyStorage {
                values: properties,
                ..MessagePropertyStorage::default()
            }))),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
            identity: Arc::new(()),
        }
    }

    pub fn new(topic: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: Some(topic.into()),
            body: Some(Arc::new(RwLock::new(body.into()))),
            flag: 0,
            properties: Some(Arc::new(RwLock::new(MessagePropertyStorage {
                values: BTreeMap::from([(
                    crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK.to_owned(),
                    "true".to_owned(),
                )]),
                ..MessagePropertyStorage::default()
            }))),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
            identity: Arc::new(()),
        }
    }

    /// Creates a message while retaining the caller's shared body array.
    pub fn new_shared(topic: impl Into<String>, body: SharedMessageBody) -> Self {
        Self::new_nullable_shared(Some(topic.into()), Some(body))
    }

    /// Creates the `MessageAccessor.cloneMessage` projection. Body bytes and
    /// the property map are shared with the source until either message
    /// replaces that field; the transaction ID is left unset.
    pub fn clone_message(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            body: self.body.clone(),
            flag: self.flag,
            properties: self.properties.clone(),
            properties_read_initialized: AtomicBool::new(
                self.properties_read_initialized
                    .load(AtomicOrdering::Relaxed),
            ),
            transaction_id: None,
            identity: Arc::new(()),
        }
    }

    /// Java's two-argument constructor, preserving null topic and body values.
    pub fn new_nullable(topic: Option<String>, body: Option<Vec<u8>>) -> Self {
        Self::with_nullable_fields(topic, None, None, 0, body, true)
    }

    /// Java's two-argument constructor with nullable topic/body and a shared body array.
    pub fn new_nullable_shared(topic: Option<String>, body: Option<SharedMessageBody>) -> Self {
        Self::with_nullable_fields_shared(topic, None, None, 0, body, true)
    }

    /// Java's `Message(topic, tags, body)` constructor, including null inputs.
    pub fn with_tags(topic: Option<String>, tags: Option<String>, body: Option<Vec<u8>>) -> Self {
        Self::with_nullable_fields(topic, tags, None, 0, body, true)
    }

    /// Java's `Message(topic, tags, body)` constructor with a shared body array.
    pub fn with_tags_shared(
        topic: Option<String>,
        tags: Option<String>,
        body: Option<SharedMessageBody>,
    ) -> Self {
        Self::with_nullable_fields_shared(topic, tags, None, 0, body, true)
    }

    /// Java's `Message(topic, tags, keys, body)` constructor, including null inputs.
    pub fn with_tags_and_keys(
        topic: Option<String>,
        tags: Option<String>,
        keys: Option<String>,
        body: Option<Vec<u8>>,
    ) -> Self {
        Self::with_nullable_fields(topic, tags, keys, 0, body, true)
    }

    /// Java's `Message(topic, tags, keys, body)` constructor with a shared body array.
    pub fn with_tags_and_keys_shared(
        topic: Option<String>,
        tags: Option<String>,
        keys: Option<String>,
        body: Option<SharedMessageBody>,
    ) -> Self {
        Self::with_nullable_fields_shared(topic, tags, keys, 0, body, true)
    }

    /// Java's six-argument `Message(topic, tags, keys, flag, body, waitStoreMsgOK)`
    /// constructor, with Rust-owned values and optional tag/key properties.
    pub fn with_fields(
        topic: impl Into<String>,
        tags: Option<&str>,
        keys: Option<&str>,
        flag: i32,
        body: impl Into<Vec<u8>>,
        wait_store_msg_ok: bool,
    ) -> Self {
        Self::with_nullable_fields(
            Some(topic.into()),
            tags.map(str::to_owned),
            keys.map(str::to_owned),
            flag,
            Some(body.into()),
            wait_store_msg_ok,
        )
    }

    /// Java's six-argument constructor while retaining the caller's body array.
    pub fn with_fields_shared(
        topic: impl Into<String>,
        tags: Option<&str>,
        keys: Option<&str>,
        flag: i32,
        body: SharedMessageBody,
        wait_store_msg_ok: bool,
    ) -> Self {
        Self::with_nullable_fields_shared(
            Some(topic.into()),
            tags.map(str::to_owned),
            keys.map(str::to_owned),
            flag,
            Some(body),
            wait_store_msg_ok,
        )
    }

    /// Java's six-argument constructor, including nullable topic/body inputs.
    pub fn with_nullable_fields(
        topic: Option<String>,
        tags: Option<String>,
        keys: Option<String>,
        flag: i32,
        body: Option<Vec<u8>>,
        wait_store_msg_ok: bool,
    ) -> Self {
        Self::with_body_storage(
            topic,
            tags,
            keys,
            flag,
            body.map(|body| Arc::new(RwLock::new(body))),
            wait_store_msg_ok,
        )
    }

    /// Java's six-argument constructor with nullable fields and a retained body array.
    pub fn with_nullable_fields_shared(
        topic: Option<String>,
        tags: Option<String>,
        keys: Option<String>,
        flag: i32,
        body: Option<SharedMessageBody>,
        wait_store_msg_ok: bool,
    ) -> Self {
        Self::with_body_storage(
            topic,
            tags,
            keys,
            flag,
            body.map(|body| body.storage),
            wait_store_msg_ok,
        )
    }

    fn with_body_storage(
        topic: Option<String>,
        tags: Option<String>,
        keys: Option<String>,
        flag: i32,
        body: Option<Arc<RwLock<Vec<u8>>>>,
        wait_store_msg_ok: bool,
    ) -> Self {
        let mut message = Self {
            topic,
            body,
            flag,
            ..Self::default()
        };
        if let Some(tags) = tags.filter(|tags| !tags.is_empty()) {
            message.put_property_value(crate::message::constants::PROPERTY_TAGS, Some(tags));
        }
        if let Some(keys) = keys.filter(|keys| !keys.is_empty()) {
            message.put_property_value(crate::message::constants::PROPERTY_KEYS, Some(keys));
        }
        message.put_property_value(
            crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK,
            Some(wait_store_msg_ok.to_string()),
        );
        message
    }

    pub fn topic(&self) -> &str {
        self.topic.as_deref().unwrap_or_default()
    }
    pub fn topic_nullable(&self) -> Option<&str> {
        self.topic.as_deref()
    }
    pub fn set_topic(&mut self, topic: impl Into<String>) {
        self.topic = Some(topic.into());
    }
    pub fn set_topic_nullable(&mut self, topic: Option<String>) {
        self.topic = topic;
    }
    pub fn body(
        &self,
    ) -> impl Deref<Target = [u8]> + AsRef<[u8]> + std::fmt::Debug + PartialEq + '_ {
        MessageBodyReadView(self.body.as_ref().map(|body| read_message_state(body)))
    }
    pub fn body_nullable(
        &self,
    ) -> Option<impl Deref<Target = [u8]> + AsRef<[u8]> + std::fmt::Debug + PartialEq + '_> {
        self.body
            .as_ref()
            .map(|body| MessageBodyReadView(Some(read_message_state(body))))
    }
    /// Mutably borrows the Java `getBody()` byte array when one is present.
    /// Messages returned by `clone_message()` share this array until either
    /// message calls `set_body` or `set_body_nullable`.
    pub fn body_mut(&mut self) -> Option<impl DerefMut<Target = [u8]> + '_> {
        self.body
            .as_ref()
            .map(|body| MessageBodyWriteView(write_message_state(body)))
    }
    pub fn set_body(&mut self, body: impl Into<Vec<u8>>) {
        self.body = Some(Arc::new(RwLock::new(body.into())));
    }
    pub fn set_body_nullable(&mut self, body: Option<Vec<u8>>) {
        self.body = body.map(|body| Arc::new(RwLock::new(body)));
    }
    /// Replaces the body with a caller-held handle, preserving byte-array identity.
    pub fn set_body_shared(&mut self, body: Option<SharedMessageBody>) {
        self.body = body.map(|body| body.storage);
    }
    /// Returns a handle to the live body bytes, when a body is present.
    pub fn shared_body(&self) -> Option<SharedMessageBody> {
        self.body.as_ref().map(|storage| SharedMessageBody {
            storage: storage.clone(),
        })
    }

    pub fn flag(&self) -> i32 {
        self.flag
    }
    pub fn properties(
        &self,
    ) -> impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_ {
        MessagePropertiesReadView(match self.properties.as_ref() {
            Some(properties) => MessagePropertiesReadSource::Shared(read_message_state(properties)),
            None => MessagePropertiesReadSource::Empty,
        })
    }
    pub fn properties_nullable(
        &self,
    ) -> Option<impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_>
    {
        match self.properties.as_ref() {
            Some(properties) => Some(MessagePropertiesReadView(
                MessagePropertiesReadSource::Shared(read_message_state(properties)),
            )),
            None if self
                .properties_read_initialized
                .load(AtomicOrdering::Relaxed) =>
            {
                Some(MessagePropertiesReadView(
                    MessagePropertiesReadSource::Empty,
                ))
            }
            None => None,
        }
    }
    pub fn get_properties(
        &self,
    ) -> Option<impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_>
    {
        self.properties_nullable()
    }
    /// Replace the message property map, corresponding to Java's
    /// `MessageAccessor.setProperties`. Rust takes ownership of the supplied
    /// map; `None` preserves Java's null-map state.
    pub fn set_properties(&mut self, properties: Option<MessageProperties>) {
        self.properties = properties.map(|values| {
            Arc::new(RwLock::new(MessagePropertyStorage {
                values,
                ..MessagePropertyStorage::default()
            }))
        });
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
    }

    /// Replaces the property map with a shared handle, matching Java's live
    /// `MessageAccessor.setProperties` map-reference behavior.
    pub fn set_properties_shared(&mut self, properties: Option<SharedMessageProperties>) {
        self.properties = properties.map(|properties| properties.storage);
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
    }

    /// Returns a cloneable handle to the live property map when it is present.
    pub fn shared_properties(&self) -> Option<SharedMessageProperties> {
        self.properties
            .as_ref()
            .map(|storage| SharedMessageProperties {
                storage: storage.clone(),
            })
    }

    /// Replace the property map while preserving Java `HashMap` null keys and
    /// values. `None` represents a null map; `Some(empty)` represents an empty
    /// map.
    pub fn set_nullable_properties(&mut self, properties: Option<NullableMessageProperties>) {
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
        self.properties = properties.map(|properties| {
            let mut storage = MessagePropertyStorage::default();
            for (key, value) in properties {
                match key {
                    Some(key) => {
                        if let Some(value) = value {
                            storage.values.insert(key, value);
                        } else {
                            storage.null_values.insert(key);
                        }
                    }
                    None => {
                        storage.null_key_value = Some(value);
                    }
                }
            }
            Arc::new(RwLock::new(storage))
        });
    }
    /// Returns stored property entries, including Java-style null values, in
    /// deterministic key order. `properties()` and `get_properties()` retain
    /// the Rust string-to-string map view; this iterator exposes the entries
    /// that Java callers can observe through `Message.getProperties()`.
    pub fn property_entries(&self) -> impl Iterator<Item = (String, Option<String>)> {
        self.nullable_property_entries()
            .filter_map(|(key, value)| key.map(|key| (key, value)))
    }
    /// Returns stored Java-style entries, including a null key or null value,
    /// in deterministic key order. A null key is returned as `None`.
    pub fn nullable_property_entries(
        &self,
    ) -> impl Iterator<Item = (Option<String>, Option<String>)> {
        let mut entries = self
            .properties
            .as_ref()
            .map_or_else(Vec::new, |properties| {
                let properties = read_message_state(properties);
                properties
                    .values
                    .iter()
                    .map(|(key, value)| (Some(key.clone()), Some(value.clone())))
                    .chain(
                        properties
                            .null_values
                            .iter()
                            .filter(|key| !properties.values.contains_key(*key))
                            .map(|key| (Some(key.clone()), None)),
                    )
                    .chain(
                        properties
                            .null_key_value
                            .as_ref()
                            .map(|value| (None, value.clone())),
                    )
                    .collect::<Vec<_>>()
            });
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries.into_iter()
    }
    pub fn get_properties_mut(&mut self) -> Option<impl DerefMut<Target = MessageProperties> + '_> {
        if self.properties.is_none()
            && !self
                .properties_read_initialized
                .swap(false, AtomicOrdering::Relaxed)
        {
            return None;
        }
        let properties = self
            .properties
            .get_or_insert_with(|| Arc::new(RwLock::new(MessagePropertyStorage::default())));
        Some(MessagePropertiesWriteView(write_message_state(properties)))
    }
    pub fn get_property(&self, key: &str) -> Option<String> {
        if self.properties.is_none() {
            self.properties_read_initialized
                .store(true, AtomicOrdering::Relaxed);
        }
        self.properties
            .as_ref()
            .and_then(|properties| read_message_state(properties).values.get(key).cloned())
    }
    pub fn get_nullable_property(&self, key: Option<&str>) -> Option<String> {
        match key {
            Some(key) => self.get_property(key),
            None => {
                if self.properties.is_none() {
                    self.properties_read_initialized
                        .store(true, AtomicOrdering::Relaxed);
                }
                self.properties.as_ref().and_then(|properties| {
                    read_message_state(properties)
                        .null_key_value
                        .as_ref()
                        .and_then(Clone::clone)
                })
            }
        }
    }
    pub fn get_user_property(&self, key: &str) -> Option<String> {
        self.get_property(key)
    }
    pub fn get_nullable_user_property(&self, key: Option<&str>) -> Option<String> {
        self.get_nullable_property(key)
    }
    pub fn has_property(&self, key: &str) -> bool {
        self.properties.as_ref().is_some_and(|properties| {
            let properties = read_message_state(properties);
            properties.values.contains_key(key) || properties.null_values.contains(key)
        })
    }
    pub fn has_nullable_property(&self, key: Option<&str>) -> bool {
        match key {
            Some(key) => self.has_property(key),
            None => self
                .properties
                .as_ref()
                .is_some_and(|properties| read_message_state(properties).null_key_value.is_some()),
        }
    }
    pub fn tags(&self) -> Option<String> {
        self.get_property(crate::message::constants::PROPERTY_TAGS)
    }
    pub fn keys(&self) -> Option<String> {
        self.get_property(crate::message::constants::PROPERTY_KEYS)
    }
    pub fn is_wait_store_msg_ok(&self) -> bool {
        self.get_property(crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK)
            .is_none_or(|value| value.eq_ignore_ascii_case("true"))
    }
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }
    pub fn set_flag(&mut self, flag: i32) {
        self.flag = flag;
    }
    pub fn buyer_id(&self) -> Option<String> {
        self.get_property(crate::message::constants::PROPERTY_BUYER_ID)
    }
    pub fn set_buyer_id(&mut self, buyer_id: impl Into<String>) {
        self.put_property_value(
            crate::message::constants::PROPERTY_BUYER_ID,
            Some(buyer_id.into()),
        );
    }
    pub fn set_buyer_id_nullable(&mut self, buyer_id: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_BUYER_ID, buyer_id);
    }
    pub fn set_instance_id(&mut self, instance_id: impl Into<String>) {
        self.put_property_value(
            crate::message::constants::PROPERTY_INSTANCE_ID,
            Some(instance_id.into()),
        );
    }
    pub fn set_instance_id_nullable(&mut self, instance_id: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_INSTANCE_ID, instance_id);
    }
    pub fn delay_time_level(&self) -> Result<i32, MessagePropertyParseError> {
        self.parse_i32_property(crate::message::constants::PROPERTY_DELAY_TIME_LEVEL)
    }
    pub fn set_delay_time_level(&mut self, level: i32) {
        self.put_property_value(
            crate::message::constants::PROPERTY_DELAY_TIME_LEVEL,
            Some(level.to_string()),
        );
    }
    pub fn priority(&self) -> i32 {
        self.get_property(crate::message::constants::PROPERTY_PRIORITY)
            .and_then(|value| parse_java_i32(&value))
            .unwrap_or(-1)
    }
    pub fn set_priority(&mut self, priority: i32) -> Result<(), MessagePropertyError> {
        if priority < 0 {
            return Err(MessagePropertyError::NegativePriority);
        }
        self.put_property_value(
            crate::message::constants::PROPERTY_PRIORITY,
            Some(priority.to_string()),
        );
        Ok(())
    }
    pub fn delay_time_sec(&self) -> Result<i64, MessagePropertyParseError> {
        self.parse_i64_property(crate::message::constants::PROPERTY_TIMER_DELAY_SEC)
    }
    pub fn set_delay_time_sec(&mut self, seconds: i64) {
        self.put_property_value(
            crate::message::constants::PROPERTY_TIMER_DELAY_SEC,
            Some(seconds.to_string()),
        );
    }
    pub fn delay_time_ms(&self) -> Result<i64, MessagePropertyParseError> {
        self.parse_i64_property(crate::message::constants::PROPERTY_TIMER_DELAY_MS)
    }
    pub fn set_delay_time_ms(&mut self, milliseconds: i64) {
        self.put_property_value(
            crate::message::constants::PROPERTY_TIMER_DELAY_MS,
            Some(milliseconds.to_string()),
        );
    }
    pub fn deliver_time_ms(&self) -> Result<i64, MessagePropertyParseError> {
        self.parse_i64_property(crate::message::constants::PROPERTY_TIMER_DELIVER_MS)
    }
    pub fn set_deliver_time_ms(&mut self, milliseconds: i64) {
        self.put_property_value(
            crate::message::constants::PROPERTY_TIMER_DELIVER_MS,
            Some(milliseconds.to_string()),
        );
    }
    pub fn set_transaction_id(&mut self, transaction_id: Option<String>) {
        self.transaction_id = transaction_id;
    }
    pub fn put_user_property_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        if crate::message::constants::is_reserved_user_property(key.as_deref()) {
            return Err(MessagePropertyError::ReservedProperty);
        }
        let key = key
            .filter(|key| !java_trim_is_empty(key))
            .ok_or(MessagePropertyError::EmptyKey)?;
        let value = value
            .filter(|value| !java_trim_is_empty(value))
            .ok_or(MessagePropertyError::EmptyValue)?;
        self.put_user_property(key, value)
    }

    pub fn put_user_property(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let key = key.into();
        let value = value.into();
        if crate::message::constants::is_reserved_user_property(Some(&key)) {
            return Err(MessagePropertyError::ReservedProperty);
        }
        validate_property(&key, &value)?;
        Ok(self.put_property_value(key, Some(value)))
    }

    pub fn set_system_property(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let key = key.into();
        let value = value.into();
        Ok(self.put_property_value(key, Some(value)))
    }

    /// Stores a system property whose Java `HashMap` value may be null.
    pub fn set_system_property_nullable(&mut self, key: impl Into<String>, value: Option<String>) {
        self.put_property_value(key, value);
    }

    /// Set a property through Java's nullable `MessageAccessor.putProperty`
    /// boundary. The nested return value distinguishes an absent key from a
    /// key whose previous value was null.
    pub fn set_property_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Option<Option<String>> {
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
        match key {
            Some(key) => {
                let previous = self.has_property(&key).then(|| self.get_property(&key));
                self.put_property_value(key, value);
                previous
            }
            None => {
                let properties = self.properties.get_or_insert_with(|| {
                    Arc::new(RwLock::new(MessagePropertyStorage::default()))
                });
                write_message_state(properties)
                    .null_key_value
                    .replace(value)
            }
        }
    }

    /// Removes a system property, corresponding to Java `MessageAccessor.clearProperty`.
    pub fn clear_system_property(&mut self, key: &str) -> Option<String> {
        self.properties.as_mut().and_then(|properties| {
            let mut properties = write_message_state(properties);
            properties.null_values.remove(key);
            properties.values.remove(key)
        })
    }

    /// Clear a nullable property key through Java's `MessageAccessor` API.
    /// The nested return value distinguishes an absent key from a key whose
    /// value was null.
    pub fn clear_property_nullable(&mut self, key: Option<&str>) -> Option<Option<String>> {
        match key {
            Some(key) => {
                let was_present = self.has_property(key);
                let value = self.clear_system_property(key);
                was_present.then_some(value)
            }
            None => self
                .properties
                .as_ref()
                .and_then(|properties| write_message_state(properties).null_key_value.take()),
        }
    }

    pub fn set_tags(
        &mut self,
        tags: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(crate::message::constants::PROPERTY_TAGS, tags)
    }
    pub fn set_tags_nullable(&mut self, tags: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_TAGS, tags);
    }

    pub fn set_keys(
        &mut self,
        keys: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(crate::message::constants::PROPERTY_KEYS, keys)
    }
    pub fn set_keys_nullable(&mut self, keys: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_KEYS, keys);
    }

    /// Java's `setKeys(Collection<String>)` overload; collection order is kept.
    pub fn set_keys_from<I, S>(&mut self, keys: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let joined = keys
            .into_iter()
            .map(|key| key.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join(crate::message::constants::KEY_SEPARATOR);
        self.put_property_value(crate::message::constants::PROPERTY_KEYS, Some(joined));
    }

    /// Java's `String.join` collection overload: null elements become `"null"`;
    /// a null collection is reported as an error instead of panicking.
    pub fn set_keys_from_nullable<I, S>(
        &mut self,
        keys: Option<I>,
    ) -> Result<(), MessageKeyCollectionError>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let keys = keys.ok_or(MessageKeyCollectionError::NullCollection)?;
        let joined = keys
            .into_iter()
            .map(|key| {
                key.map(|key| key.as_ref().to_owned())
                    .unwrap_or_else(|| "null".to_owned())
            })
            .collect::<Vec<_>>()
            .join(crate::message::constants::KEY_SEPARATOR);
        self.put_property_value(crate::message::constants::PROPERTY_KEYS, Some(joined));
        Ok(())
    }

    pub fn set_wait_store_msg_ok(
        &mut self,
        wait: bool,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(
            crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK,
            wait.to_string(),
        )
    }

    fn parse_i32_property(&self, property: &'static str) -> Result<i32, MessagePropertyParseError> {
        parse_i32_property_value(property, self.get_property(property))
    }

    fn parse_i64_property(&self, property: &'static str) -> Result<i64, MessagePropertyParseError> {
        parse_i64_property_value(property, self.get_property(property))
    }

    fn put_property_value(
        &mut self,
        key: impl Into<String>,
        value: Option<String>,
    ) -> Option<String> {
        let key = key.into();
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
        let properties = self
            .properties
            .get_or_insert_with(|| Arc::new(RwLock::new(MessagePropertyStorage::default())));
        let mut properties = write_message_state(properties);
        match value {
            Some(value) => {
                properties.null_values.remove(&key);
                properties.values.insert(key, value)
            }
            None => {
                let previous = properties.values.remove(&key);
                properties.null_values.insert(key);
                previous
            }
        }
    }

    /// Java-style diagnostic representation. Java's `HashMap` property order
    /// is unspecified; Rust emits the `BTreeMap` order for reproducibility.
    fn java_to_string(&self) -> String {
        let properties = if self.properties_nullable().is_none() {
            "null".to_owned()
        } else {
            format!(
                "{{{}}}",
                self.nullable_property_entries()
                    .map(|(key, value)| {
                        format!(
                            "{}={}",
                            key.as_deref().unwrap_or("null"),
                            value.as_deref().unwrap_or("null")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let body = self
            .body
            .as_ref()
            .map(|body| {
                let body = read_message_state(body);
                format!(
                    "[{}]",
                    body.iter()
                        .map(|byte| (*byte as i8).to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .unwrap_or_else(|| "null".to_owned());
        let transaction_id = self.transaction_id.as_deref().unwrap_or("null");
        format!(
            "Message{{topic='{}', flag={}, properties={}, body={}, transactionId='{}'}}",
            self.topic.as_deref().unwrap_or("null"),
            self.flag,
            properties,
            body,
            transaction_id
        )
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.java_to_string())
    }
}

impl Deref for MessageExt {
    type Target = Message;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl DerefMut for MessageExt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
    }
}

/// Extended RocketMQ message data corresponding to Java's `MessageExt`.
///
/// Java `MessageExt.toString()` is exposed through Rust's `Display`/`ToString`;
/// its formatter helper is not a separate public method:
///
/// Rust's `DerefMut` exposes inherited `Message` setters in place, so there is
/// no Java-equivalent operation that replaces the whole inherited message:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// let mut extended = MessageExt::default();
/// extended.set_message(Default::default());
/// ```
///
/// The Java class inherits `Message` directly; it has no composition accessors:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// let _ = MessageExt::default().message();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// MessageExt::default().message_mut();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// let _ = MessageExt::default().java_to_string();
/// ```
///
/// Java `MessageExt` has a no-argument constructor, not a constructor that
/// wraps an existing `Message`:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::{Message, MessageExt};
/// let _ = MessageExt::new(Message::default());
/// ```
///
/// The internal wrapper helper is not part of the public API:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::{Message, MessageExt};
/// let _ = MessageExt::from_message(Message::default());
/// ```
///
/// Java inheritance exposes the `Message` surface through `MessageExt`; Rust
/// uses `Deref` for that surface and does not export lossy `From` conversions:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::{Message, MessageExt};
/// let _: Message = MessageExt::default().into();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::{Message, MessageExt};
/// let extended = MessageExt::default();
/// let _: Message = (&extended).into();
/// ```
///
/// Java `MessageExt` inherits identity-based `Object.equals`/`hashCode`;
/// Rust equality and hashing use its `Message` identity token. Rust `Clone`
/// preserves that token; constructors, decoding, and deserialization create
/// new identities:
///
/// ```
/// use rocketmq_classic_common::message::MessageExt;
/// let message = MessageExt::default();
/// assert_eq!(message, message.clone());
/// assert_ne!(message, MessageExt::default());
/// ```
///
/// Caller-defined subtypes retain shared fields and selected Java-virtual
/// method overrides when passed through an API typed as `MessageExt`:
///
/// ```
/// use std::{any::Any, sync::Arc};
/// use rocketmq_classic_common::message::{MessageExt, MessageExtSubtype};
///
/// struct CustomMessage { id: String }
/// impl MessageExtSubtype for CustomMessage {
///     fn as_any(&self) -> &dyn Any { self }
///     fn message_id(&self) -> Option<Option<String>> {
///         Some(Some(self.id.clone()))
///     }
/// }
/// let subtype = Arc::new(CustomMessage { id: "custom-id".into() });
/// let mut message = MessageExt::default();
/// message.set_runtime_subtype(subtype.clone());
/// assert_eq!(message.message_id_for_runtime_type().as_deref(), Some("custom-id"));
/// assert_eq!(message.runtime_subtype::<CustomMessage>().unwrap().id, "custom-id");
/// assert_eq!(message.clone().runtime_subtype::<CustomMessage>().unwrap().id, "custom-id");
/// ```
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct MessageExt {
    #[serde(flatten)]
    message: Message,
    #[serde(rename = "brokerName", skip_serializing_if = "Option::is_none")]
    broker_name: Option<String>,
    #[serde(rename = "queueId")]
    queue_id: i32,
    #[serde(rename = "storeSize")]
    store_size: i32,
    #[serde(rename = "queueOffset")]
    queue_offset: i64,
    #[serde(rename = "sysFlag")]
    sys_flag: i32,
    #[serde(rename = "bornTimestamp")]
    born_timestamp: i64,
    #[serde(rename = "bornHost", skip_serializing_if = "Option::is_none")]
    born_host: Option<MessageHostAddress>,
    #[serde(rename = "storeTimestamp")]
    store_timestamp: i64,
    #[serde(rename = "storeHost", skip_serializing_if = "Option::is_none")]
    store_host: Option<MessageHostAddress>,
    #[serde(rename = "msgId", skip_serializing_if = "Option::is_none")]
    message_id: Option<String>,
    #[serde(rename = "offsetMsgId", skip_serializing_if = "Option::is_none")]
    offset_message_id: Option<String>,
    /// Retains Java's runtime `MessageClientExt` subtype when client-decoded
    /// values cross an API typed as `MessageExt`.
    #[serde(skip)]
    client_ext: bool,
    /// Retains caller-defined subtype data and its overrides for Java-virtual
    /// `MessageExt` methods when a value crosses a base-typed API.
    #[serde(skip)]
    runtime_subtype: Option<Arc<dyn MessageExtSubtype>>,
    #[serde(rename = "commitLogOffset")]
    commit_log_offset: i64,
    #[serde(rename = "bodyCRC")]
    body_crc: i32,
    #[serde(rename = "reconsumeTimes")]
    reconsume_times: i32,
    #[serde(rename = "preparedTransactionOffset")]
    prepared_transaction_offset: i64,
}

impl PartialEq for MessageExt {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.message.identity, &other.message.identity)
    }
}

impl Eq for MessageExt {}

impl Hash for MessageExt {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.message.hash(state);
    }
}

impl std::fmt::Debug for MessageExt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MessageExt")
            .field("identity", &self.message.identity)
            .field("message", &self.message)
            .field("broker_name", &self.broker_name)
            .field("queue_id", &self.queue_id)
            .field("store_size", &self.store_size)
            .field("queue_offset", &self.queue_offset)
            .field("sys_flag", &self.sys_flag)
            .field("born_timestamp", &self.born_timestamp)
            .field("born_host", &self.born_host)
            .field("store_timestamp", &self.store_timestamp)
            .field("store_host", &self.store_host)
            .field("message_id", &self.message_id)
            .field("offset_message_id", &self.offset_message_id)
            .field("client_ext", &self.client_ext)
            .field(
                "runtime_subtype",
                &self
                    .runtime_subtype
                    .as_ref()
                    .map(|subtype| subtype.runtime_type_name()),
            )
            .field("commit_log_offset", &self.commit_log_offset)
            .field("body_crc", &self.body_crc)
            .field("reconsume_times", &self.reconsume_times)
            .field(
                "prepared_transaction_offset",
                &self.prepared_transaction_offset,
            )
            .finish()
    }
}

impl MessageExt {
    /// Java's public no-argument `MessageExt` constructor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a caller-defined runtime subtype to this base-typed message.
    /// The shared subtype value survives `MessageExt::clone`, matching the
    /// aliasing observed when Java passes a subclass through a `MessageExt`
    /// parameter.
    pub fn set_runtime_subtype<T>(&mut self, subtype: Arc<T>)
    where
        T: MessageExtSubtype + 'static,
    {
        self.runtime_subtype = Some(subtype);
    }

    /// Return the retained caller-defined subtype value, if it has type `T`.
    pub fn runtime_subtype<T>(&self) -> Option<&T>
    where
        T: MessageExtSubtype + 'static,
    {
        self.runtime_subtype.as_ref()?.as_any().downcast_ref::<T>()
    }

    /// The Rust type name of an attached caller-defined runtime subtype.
    pub fn runtime_subtype_name(&self) -> Option<&'static str> {
        self.runtime_subtype
            .as_ref()
            .map(|subtype| subtype.runtime_type_name())
    }

    /// Java's virtual `Message.getProperties()` view. Subtype maps are exposed
    /// as owned snapshots; base messages retain their live read guard.
    pub fn properties(
        &self,
    ) -> impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_ {
        if let Some(properties) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.live_properties())
        {
            return MessagePropertiesReadView(match properties {
                Some(properties) => {
                    MessagePropertiesReadSource::Shared(read_message_state(&properties.storage))
                }
                None => MessagePropertiesReadSource::Empty,
            });
        }
        if let Some(properties) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.properties())
        {
            return MessagePropertiesReadView(MessagePropertiesReadSource::Runtime(
                string_properties(properties.unwrap_or_default()),
            ));
        }
        MessagePropertiesReadView(match self.message.properties.as_ref() {
            Some(properties) => MessagePropertiesReadSource::Shared(read_message_state(properties)),
            None => MessagePropertiesReadSource::Empty,
        })
    }

    /// Nullable Java `Message.getProperties()` view, including subtype
    /// overrides. A subtype map is an owned snapshot.
    pub fn properties_nullable(
        &self,
    ) -> Option<impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_>
    {
        if let Some(properties) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.live_properties())
        {
            return properties.map(|properties| {
                MessagePropertiesReadView(MessagePropertiesReadSource::Shared(read_message_state(
                    &properties.storage,
                )))
            });
        }
        if let Some(properties) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.properties())
        {
            return properties.map(|properties| {
                MessagePropertiesReadView(MessagePropertiesReadSource::Runtime(string_properties(
                    properties,
                )))
            });
        }
        match self.message.properties.as_ref() {
            Some(properties) => Some(MessagePropertiesReadView(
                MessagePropertiesReadSource::Shared(read_message_state(properties)),
            )),
            None if self
                .message
                .properties_read_initialized
                .load(AtomicOrdering::Relaxed) =>
            {
                Some(MessagePropertiesReadView(
                    MessagePropertiesReadSource::Empty,
                ))
            }
            None => None,
        }
    }

    /// Java `Message.getProperties()` nullable alias.
    pub fn get_properties(
        &self,
    ) -> Option<impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_>
    {
        self.properties_nullable()
    }

    /// Returns a mutable handle to the live property map, including a
    /// caller-defined subtype map when the subtype exposes one.
    pub fn shared_properties(&self) -> Option<SharedMessageProperties> {
        match self.runtime_subtype.as_ref() {
            Some(subtype) => match subtype.live_properties() {
                Some(properties) => properties.cloned(),
                None if subtype.properties().is_some() => None,
                None => self.message.shared_properties(),
            },
            None => self.message.shared_properties(),
        }
    }

    /// Java-style properties returned by a subtype's `getProperties()`;
    /// base messages retain their stored null keys and values.
    pub fn nullable_property_entries(
        &self,
    ) -> impl Iterator<Item = (Option<String>, Option<String>)> {
        let mut entries = match self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.live_properties())
        {
            Some(Some(properties)) => properties.snapshot().into_iter().collect(),
            Some(None) => Vec::new(),
            None => match self
                .runtime_subtype
                .as_ref()
                .and_then(|subtype| subtype.properties())
            {
                Some(properties) => properties
                    .unwrap_or_default()
                    .into_iter()
                    .collect::<Vec<_>>(),
                None => self.message.nullable_property_entries().collect::<Vec<_>>(),
            },
        };
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries.into_iter()
    }

    /// String-keyed entries from Java's virtual `Message.getProperties()`.
    pub fn property_entries(&self) -> impl Iterator<Item = (String, Option<String>)> {
        self.nullable_property_entries()
            .filter_map(|(key, value)| key.map(|key| (key, value)))
    }

    /// Java's virtual `Message.getProperty(String)` view, including
    /// caller-defined `MessageExt` subtype overrides.
    pub fn get_property(&self, key: &str) -> Option<String> {
        self.get_nullable_property(Some(key))
    }

    /// Nullable Java `Message.getProperty(String)` view. `None` from the
    /// subtype hook falls back to the stored message; `Some(None)` represents
    /// an explicit Java null result.
    pub fn get_nullable_property(&self, key: Option<&str>) -> Option<String> {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.property(key))
            .unwrap_or_else(|| self.message.get_nullable_property(key))
    }

    /// Java's virtual `Message.getUserProperty(String)` view. The default
    /// implementation delegates to `getProperty`, while subtypes may override
    /// this method independently.
    pub fn get_user_property(&self, key: &str) -> Option<String> {
        self.get_nullable_user_property(Some(key))
    }

    pub fn get_nullable_user_property(&self, key: Option<&str>) -> Option<String> {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.user_property(key))
            .unwrap_or_else(|| self.get_nullable_property(key))
    }

    /// Java's virtual `Message.hasProperty(String)` view. Its base
    /// implementation checks the stored map directly rather than calling
    /// `getProperty`.
    pub fn has_property(&self, key: &str) -> bool {
        self.has_nullable_property(Some(key))
    }

    pub fn has_nullable_property(&self, key: Option<&str>) -> bool {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.has_property(key))
            .unwrap_or_else(|| self.message.has_nullable_property(key))
    }

    /// Java's virtual `Message.getTags()` view. A direct subtype override takes
    /// precedence; the inherited getter reads through virtual `getProperty`.
    pub fn tags(&self) -> Option<String> {
        if let Some(tags) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.tags())
        {
            return tags;
        }
        self.get_property(crate::message::constants::PROPERTY_TAGS)
    }

    /// Java's virtual `Message.getKeys()` view. A direct subtype override takes
    /// precedence; the inherited getter reads through virtual `getProperty`.
    pub fn keys(&self) -> Option<String> {
        if let Some(keys) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.keys())
        {
            return keys;
        }
        self.get_property(crate::message::constants::PROPERTY_KEYS)
    }

    /// Java `Message.isWaitStoreMsgOK()`, including direct subtype and
    /// subtype-`getProperty` overrides.
    pub fn is_wait_store_msg_ok(&self) -> bool {
        if let Some(wait) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.wait_store_msg_ok())
        {
            return wait;
        }
        self.get_property(crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK)
            .is_none_or(|value| value.eq_ignore_ascii_case("true"))
    }

    /// Java `Message.getBuyerId()`, including direct subtype and
    /// subtype-`getProperty` overrides.
    pub fn buyer_id(&self) -> Option<String> {
        if let Some(buyer_id) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.buyer_id())
        {
            return buyer_id;
        }
        self.get_property(crate::message::constants::PROPERTY_BUYER_ID)
    }

    /// Java `Message.getDelayTimeLevel()`, including direct subtype overrides
    /// and the inherited `getProperty` parsing path.
    pub fn delay_time_level(&self) -> Result<i32, MessagePropertyParseError> {
        if let Some(level) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.delay_time_level())
        {
            return Ok(level);
        }
        let property = crate::message::constants::PROPERTY_DELAY_TIME_LEVEL;
        parse_i32_property_value(property, self.get_property(property))
    }

    /// Java `Message.getDelayTimeSec()`, including direct subtype overrides
    /// and the inherited `getProperty` parsing path.
    pub fn delay_time_sec(&self) -> Result<i64, MessagePropertyParseError> {
        if let Some(seconds) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.delay_time_sec())
        {
            return Ok(seconds);
        }
        let property = crate::message::constants::PROPERTY_TIMER_DELAY_SEC;
        parse_i64_property_value(property, self.get_property(property))
    }

    /// Java `Message.getDelayTimeMs()`, including direct subtype overrides and
    /// the inherited `getProperty` parsing path.
    pub fn delay_time_ms(&self) -> Result<i64, MessagePropertyParseError> {
        if let Some(milliseconds) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.delay_time_ms())
        {
            return Ok(milliseconds);
        }
        let property = crate::message::constants::PROPERTY_TIMER_DELAY_MS;
        parse_i64_property_value(property, self.get_property(property))
    }

    /// Java `Message.getDeliverTimeMs()`, including direct subtype overrides
    /// and the inherited `getProperty` parsing path.
    pub fn deliver_time_ms(&self) -> Result<i64, MessagePropertyParseError> {
        if let Some(milliseconds) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.deliver_time_ms())
        {
            return Ok(milliseconds);
        }
        let property = crate::message::constants::PROPERTY_TIMER_DELIVER_MS;
        parse_i64_property_value(property, self.get_property(property))
    }

    /// Java's virtual `Message.getTransactionId()` field view.
    pub fn transaction_id(&self) -> Option<&str> {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.transaction_id())
            .unwrap_or_else(|| self.message.transaction_id())
    }

    /// Java's virtual `Message.setTransactionId(String)` view.
    pub fn set_transaction_id(&mut self, transaction_id: Option<String>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_transaction_id(transaction_id.clone()))
        {
            self.message.set_transaction_id(transaction_id);
        }
    }

    pub fn put_user_property_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        if self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.put_user_property(key.clone(), value.clone()))
        {
            Ok(None)
        } else {
            self.message.put_user_property_nullable(key, value)
        }
    }

    pub fn put_user_property(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let key = key.into();
        let value = value.into();
        self.put_user_property_nullable(Some(key), Some(value))
    }

    pub fn set_tags(
        &mut self,
        tags: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let tags = tags.into();
        if self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_tags(Some(tags.clone())))
        {
            Ok(None)
        } else {
            self.message.set_tags(tags)
        }
    }

    pub fn set_tags_nullable(&mut self, tags: Option<String>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_tags(tags.clone()))
        {
            self.message.set_tags_nullable(tags);
        }
    }

    pub fn set_keys(
        &mut self,
        keys: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let keys = keys.into();
        if self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_keys(Some(keys.clone())))
        {
            Ok(None)
        } else {
            self.message.set_keys(keys)
        }
    }

    pub fn set_keys_nullable(&mut self, keys: Option<String>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_keys(keys.clone()))
        {
            self.message.set_keys_nullable(keys);
        }
    }

    /// Java's `setKeys(Collection<String>)` overload joins the collection and
    /// then dynamically calls `setKeys(String)`.
    pub fn set_keys_from<I, S>(&mut self, keys: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let joined = keys
            .into_iter()
            .map(|key| key.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join(crate::message::constants::KEY_SEPARATOR);
        self.set_keys_nullable(Some(joined));
    }

    pub fn set_keys_from_nullable<I, S>(
        &mut self,
        keys: Option<I>,
    ) -> Result<(), MessageKeyCollectionError>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let keys = keys.ok_or(MessageKeyCollectionError::NullCollection)?;
        let joined = keys
            .into_iter()
            .map(|key| {
                key.map(|key| key.as_ref().to_owned())
                    .unwrap_or_else(|| "null".to_owned())
            })
            .collect::<Vec<_>>()
            .join(crate::message::constants::KEY_SEPARATOR);
        self.set_keys_nullable(Some(joined));
        Ok(())
    }

    pub fn set_wait_store_msg_ok(
        &mut self,
        wait: bool,
    ) -> Result<Option<String>, MessagePropertyError> {
        if self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_wait_store_msg_ok(wait))
        {
            Ok(None)
        } else {
            self.message.set_wait_store_msg_ok(wait)
        }
    }

    pub fn set_buyer_id(&mut self, buyer_id: impl Into<String>) {
        self.set_buyer_id_nullable(Some(buyer_id.into()));
    }

    pub fn set_buyer_id_nullable(&mut self, buyer_id: Option<String>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_buyer_id(buyer_id.clone()))
        {
            self.message.set_buyer_id_nullable(buyer_id);
        }
    }

    pub fn set_instance_id(&mut self, instance_id: impl Into<String>) {
        self.set_instance_id_nullable(Some(instance_id.into()));
    }

    pub fn set_instance_id_nullable(&mut self, instance_id: Option<String>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_instance_id(instance_id.clone()))
        {
            self.message.set_instance_id_nullable(instance_id);
        }
    }

    pub fn set_delay_time_level(&mut self, level: i32) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_delay_time_level(level))
        {
            self.message.set_delay_time_level(level);
        }
    }

    pub fn set_delay_time_sec(&mut self, seconds: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_delay_time_sec(seconds))
        {
            self.message.set_delay_time_sec(seconds);
        }
    }

    pub fn set_delay_time_ms(&mut self, milliseconds: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_delay_time_ms(milliseconds))
        {
            self.message.set_delay_time_ms(milliseconds);
        }
    }

    pub fn set_deliver_time_ms(&mut self, milliseconds: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_deliver_time_ms(milliseconds))
        {
            self.message.set_deliver_time_ms(milliseconds);
        }
    }

    /// Java's virtual `Message.getTopic()` view, including caller-defined
    /// `MessageExt` subtype overrides.
    pub fn topic(&self) -> &str {
        self.topic_nullable().unwrap_or_default()
    }

    /// Java's virtual nullable `Message.getTopic()` view.
    pub fn topic_nullable(&self) -> Option<&str> {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.topic())
            .unwrap_or(self.message.topic_nullable())
    }

    /// Java's virtual `Message.setTopic(String)` view.
    pub fn set_topic(&mut self, topic: impl Into<String>) {
        self.set_topic_nullable(Some(topic.into()));
    }

    /// Java's virtual nullable `Message.setTopic(String)` view.
    pub fn set_topic_nullable(&mut self, topic: Option<String>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_topic(topic.clone()))
        {
            self.message.set_topic_nullable(topic);
        }
    }

    /// Java's virtual `Message.getBody()` view, including caller-defined
    /// `MessageExt` subtype overrides.
    pub fn body(
        &self,
    ) -> impl Deref<Target = [u8]> + AsRef<[u8]> + std::fmt::Debug + PartialEq + '_ {
        if let Some(subtype) = self.runtime_subtype.as_ref() {
            if let Some(body) = subtype.shared_body() {
                return MessageExtBodyReadView(MessageExtBodyReadSource::Runtime(
                    body.map(|body| body.snapshot()).unwrap_or_default(),
                ));
            }
            if let Some(body) = subtype.body() {
                return MessageExtBodyReadView(MessageExtBodyReadSource::Runtime(
                    body.unwrap_or_default(),
                ));
            }
        }
        let body = self
            .message
            .body
            .as_ref()
            .map(|body| read_message_state(body));
        MessageExtBodyReadView(MessageExtBodyReadSource::Stored(MessageBodyReadView(body)))
    }

    /// Returns the live body handle exposed by Java's virtual `getBody()`.
    /// A caller-defined subtype can provide its own shared byte array; a
    /// snapshot-only subtype getter has no shared handle and returns `None`.
    pub fn shared_body(&self) -> Option<SharedMessageBody> {
        if let Some(subtype) = self.runtime_subtype.as_ref() {
            if let Some(body) = subtype.shared_body() {
                return body;
            }
            if subtype.body().is_some() {
                return None;
            }
        }
        self.message.shared_body()
    }

    /// Nullable Java `Message.getBody()` view, including subtype overrides.
    pub fn body_nullable(
        &self,
    ) -> Option<impl Deref<Target = [u8]> + AsRef<[u8]> + std::fmt::Debug + PartialEq + '_> {
        if let Some(subtype) = self.runtime_subtype.as_ref() {
            if let Some(body) = subtype.shared_body() {
                return body.map(|body| {
                    MessageExtBodyReadView(MessageExtBodyReadSource::Runtime(body.snapshot()))
                });
            }
            if let Some(body) = subtype.body() {
                return body
                    .map(|body| MessageExtBodyReadView(MessageExtBodyReadSource::Runtime(body)));
            }
        }
        self.message.body.as_ref().map(|body| {
            MessageExtBodyReadView(MessageExtBodyReadSource::Stored(MessageBodyReadView(Some(
                read_message_state(body),
            ))))
        })
    }

    /// Java's virtual `Message.getFlag()` view, including caller-defined
    /// `MessageExt` subtype overrides.
    pub fn flag(&self) -> i32 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.flag())
            .unwrap_or(self.message.flag())
    }

    /// Java's virtual `Message.setBody(byte[])` view.
    pub fn set_body(&mut self, body: impl Into<Vec<u8>>) {
        self.set_body_nullable(Some(body.into()));
    }

    /// Sets the virtual Java body while retaining the caller's shared byte
    /// array. A subtype that handles `setBody` and needs caller aliasing should
    /// override `MessageExtSubtype::set_shared_body`; otherwise its ordinary
    /// `set_body` hook receives a byte snapshot.
    pub fn set_body_shared(&mut self, body: Option<SharedMessageBody>) {
        if let Some(subtype) = self.runtime_subtype.as_ref() {
            if subtype.set_shared_body(body.clone()) {
                return;
            }
            if subtype.set_body(body.as_ref().map(SharedMessageBody::snapshot)) {
                return;
            }
        }
        self.message.set_body_shared(body);
    }

    /// Java's nullable virtual `Message.setBody(byte[])` view.
    pub fn set_body_nullable(&mut self, body: Option<Vec<u8>>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_body(body.clone()))
        {
            self.message.set_body_nullable(body);
        }
    }

    /// Java's virtual `Message.setFlag(int)` view.
    pub fn set_flag(&mut self, flag: i32) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_flag(flag))
        {
            self.message.set_flag(flag);
        }
    }

    pub(crate) fn from_message(message: Message) -> Self {
        Self {
            message,
            ..Self::default()
        }
    }
    /// Java `MessageExt.parseTopicFilterType(int)`.
    pub fn parse_topic_filter_type(sys_flag: i32) -> TopicFilterType {
        if sys_flag & crate::sysflag::message::MULTI_TAGS_FLAG
            == crate::sysflag::message::MULTI_TAGS_FLAG
        {
            TopicFilterType::MultiTag
        } else {
            TopicFilterType::SingleTag
        }
    }
    /// Constructor equivalent to Java's
    /// `MessageExt(int, long, SocketAddress, long, SocketAddress, String)`.
    pub fn with_socket_addresses(
        queue_id: i32,
        born_timestamp: i64,
        born_host: Option<MessageHostAddress>,
        store_timestamp: i64,
        store_host: Option<MessageHostAddress>,
        message_id: Option<String>,
    ) -> Self {
        Self {
            queue_id,
            born_timestamp,
            born_host,
            store_timestamp,
            store_host,
            message_id,
            ..Self::default()
        }
    }
    pub fn broker_name(&self) -> Option<&str> {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.broker_name())
            .unwrap_or(self.broker_name.as_deref())
    }
    /// Java's virtual `Message.getPriority()`, including subtype overrides.
    pub fn priority(&self) -> i32 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.priority())
            .unwrap_or_else(|| {
                self.get_property(crate::message::constants::PROPERTY_PRIORITY)
                    .and_then(|value| parse_java_i32(&value))
                    .unwrap_or(-1)
            })
    }
    /// Java's virtual `Message.setPriority(int)`, including subtype overrides.
    pub fn set_priority(&mut self, priority: i32) -> Result<(), MessagePropertyError> {
        if self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_priority(priority))
        {
            return Ok(());
        }
        self.message.set_priority(priority)
    }
    pub fn queue_id(&self) -> i32 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.queue_id())
            .unwrap_or(self.queue_id)
    }
    pub fn queue_offset(&self) -> i64 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.queue_offset())
            .unwrap_or(self.queue_offset)
    }
    pub fn commit_log_offset(&self) -> i64 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.commit_log_offset())
            .unwrap_or(self.commit_log_offset)
    }
    pub fn sys_flag(&self) -> i32 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.sys_flag())
            .unwrap_or(self.sys_flag)
    }
    pub fn born_timestamp(&self) -> i64 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.born_timestamp())
            .unwrap_or(self.born_timestamp)
    }
    pub fn store_timestamp(&self) -> i64 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.store_timestamp())
            .unwrap_or(self.store_timestamp)
    }
    pub fn reconsume_times(&self) -> i32 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.reconsume_times())
            .unwrap_or(self.reconsume_times)
    }
    pub fn store_size(&self) -> i32 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.store_size())
            .unwrap_or(self.store_size)
    }
    pub fn body_crc(&self) -> i32 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.body_crc())
            .unwrap_or(self.body_crc)
    }
    /// Java `MessageExt.getBornHost()`, represented as a resolved or unresolved
    /// Rust `MessageHostAddress` value.
    pub fn born_host(&self) -> Option<&MessageHostAddress> {
        if let Some(subtype) = self.runtime_subtype.as_ref() {
            if let Some(born_host) = subtype.born_host() {
                return born_host;
            }
        }
        self.born_host.as_ref()
    }
    /// Java `MessageExt.getStoreHost()`, represented as a resolved or unresolved
    /// Rust `MessageHostAddress` value.
    pub fn store_host(&self) -> Option<&MessageHostAddress> {
        if let Some(subtype) = self.runtime_subtype.as_ref() {
            if let Some(store_host) = subtype.store_host() {
                return store_host;
            }
        }
        self.store_host.as_ref()
    }
    /// Java `MessageExt.getBornHostString()`; unresolved hosts return `None`.
    /// The Java base implementation reads its stored field directly, like its
    /// corresponding host-name and host-bytes helpers. A retained subtype may
    /// override this virtual getter.
    pub fn born_host_string(&self) -> Option<String> {
        if let Some(born_host_string) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.born_host_string())
        {
            return born_host_string;
        }
        self.born_host
            .as_ref()
            .and_then(MessageHostAddress::host_address_string)
    }
    /// Java `MessageExt.getBornHostNameString()` without reverse DNS lookup.
    /// The Java base implementation reads its stored field directly; a
    /// retained subtype may override this virtual getter.
    pub fn born_host_name_string(&self) -> Option<&str> {
        if let Some(born_host_name_string) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.born_host_name_string())
        {
            return born_host_name_string;
        }
        self.born_host.as_ref().map(MessageHostAddress::host_name)
    }
    /// Java `MessageExt.getBornHostBytes()` in network byte order. The Java
    /// base implementation reads its stored field directly; a retained
    /// subtype may override it with `MessageExtSubtype::born_host_bytes`.
    pub fn born_host_bytes(&self) -> Result<Vec<u8>, MessageHostAddressBytesError> {
        if let Some(bytes) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.born_host_bytes())
        {
            return Ok(bytes);
        }
        self.born_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .network_bytes()
            .ok_or(MessageHostAddressBytesError::UnresolvedAddress)
    }
    /// Java `MessageExt.getStoreHostBytes()` in network byte order. The Java
    /// base implementation reads its stored field directly; a retained
    /// subtype may override it with `MessageExtSubtype::store_host_bytes`.
    pub fn store_host_bytes(&self) -> Result<Vec<u8>, MessageHostAddressBytesError> {
        if let Some(bytes) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.store_host_bytes())
        {
            return Ok(bytes);
        }
        self.store_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .network_bytes()
            .ok_or(MessageHostAddressBytesError::UnresolvedAddress)
    }
    /// Fill a Rust byte slice with the Java `getBornHostBytes(ByteBuffer)` layout.
    /// Subtypes may override the write with their `MessageExtSubtype` hook.
    pub fn write_born_host_bytes(
        &self,
        output: &mut [u8],
    ) -> Result<usize, MessageHostAddressBytesError> {
        if let Some(result) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.write_born_host_bytes(output))
        {
            return result;
        }
        self.born_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .write_network_bytes(output)
    }
    /// Fill a Rust byte slice with the Java `getStoreHostBytes(ByteBuffer)` layout.
    /// Subtypes may override the write with their `MessageExtSubtype` hook.
    pub fn write_store_host_bytes(
        &self,
        output: &mut [u8],
    ) -> Result<usize, MessageHostAddressBytesError> {
        if let Some(result) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.write_store_host_bytes(output))
        {
            return result;
        }
        self.store_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .write_network_bytes(output)
    }
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }
    /// Returns the value Java exposes through virtual `MessageExt.getMsgId()`.
    /// For a retained `MessageClientExt` subtype this reads `UNIQ_KEY` live,
    /// then falls back to its physical offset ID.
    pub fn message_id_for_runtime_type(&self) -> Option<String> {
        if let Some(message_id) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.message_id())
        {
            message_id
        } else if self.client_ext {
            self.get_property(crate::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .or_else(|| self.offset_message_id_for_runtime_type())
                .or_else(|| self.message_id().map(str::to_owned))
        } else {
            self.message_id().map(str::to_owned)
        }
    }
    /// Whether this value retains Java's `MessageClientExt` runtime type.
    pub const fn is_message_client_ext(&self) -> bool {
        self.client_ext
    }
    pub fn offset_message_id(&self) -> Option<&str> {
        self.offset_message_id.as_deref()
    }
    /// Returns Java `MessageClientExt.getOffsetMsgId()` through the retained
    /// runtime subtype, falling back to the stored physical ID for the base
    /// client extension type.
    pub fn offset_message_id_for_runtime_type(&self) -> Option<String> {
        if let Some(offset_message_id) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.offset_message_id())
        {
            return offset_message_id;
        }
        if self.client_ext {
            self.offset_message_id
                .clone()
                .or_else(|| self.message_id.clone())
        } else {
            self.offset_message_id.clone()
        }
    }
    pub fn prepared_transaction_offset(&self) -> i64 {
        self.runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.prepared_transaction_offset())
            .unwrap_or(self.prepared_transaction_offset)
    }
    pub fn set_broker_name(&mut self, value: Option<String>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_broker_name(value.clone()))
        {
            self.broker_name = value;
        }
    }
    pub fn set_queue_id(&mut self, value: i32) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_queue_id(value))
        {
            self.queue_id = value;
        }
    }
    pub fn set_queue_offset(&mut self, value: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_queue_offset(value))
        {
            self.queue_offset = value;
        }
    }
    pub fn set_commit_log_offset(&mut self, value: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_commit_log_offset(value))
        {
            self.commit_log_offset = value;
        }
    }
    pub fn set_sys_flag(&mut self, value: i32) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_sys_flag(value))
        {
            self.sys_flag = value;
        }
    }
    /// Set the Java `BORNHOST_V6_FLAG` bit without changing other system flags.
    pub fn set_born_host_v6_flag(&mut self) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_born_host_v6_flag())
        {
            self.sys_flag |= crate::sysflag::message::BORNHOST_V6_FLAG;
        }
    }
    /// Set the Java `STOREHOSTADDRESS_V6_FLAG` bit without changing other flags.
    pub fn set_store_host_address_v6_flag(&mut self) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_store_host_address_v6_flag())
        {
            self.sys_flag |= crate::sysflag::message::STOREHOSTADDRESS_V6_FLAG;
        }
    }
    pub fn set_born_timestamp(&mut self, value: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_born_timestamp(value))
        {
            self.born_timestamp = value;
        }
    }
    pub fn set_store_timestamp(&mut self, value: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_store_timestamp(value))
        {
            self.store_timestamp = value;
        }
    }
    pub fn set_reconsume_times(&mut self, value: i32) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_reconsume_times(value))
        {
            self.reconsume_times = value;
        }
    }
    pub fn set_store_size(&mut self, value: i32) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_store_size(value))
        {
            self.store_size = value;
        }
    }
    pub fn set_body_crc(&mut self, value: i32) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_body_crc(value))
        {
            self.body_crc = value;
        }
    }
    /// Java `MessageExt.setBornHost(SocketAddress)` using Rust value semantics.
    pub fn set_born_host(&mut self, value: Option<MessageHostAddress>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_born_host(value.clone()))
        {
            self.born_host = value;
        }
    }
    /// Java `MessageExt.setStoreHost(SocketAddress)` using Rust value semantics.
    pub fn set_store_host(&mut self, value: Option<MessageHostAddress>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_store_host(value.clone()))
        {
            self.store_host = value;
        }
    }
    pub fn set_message_id(&mut self, value: Option<String>) {
        if self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_message_id(value.clone()))
        {
            return;
        }
        if !self.client_ext {
            self.message_id = value;
        }
    }
    /// Apply the ID state produced by Java `MessageDecoder.clientDecode` while
    /// retaining its `MessageClientExt` runtime subtype.
    pub fn apply_client_decode_ids(
        &mut self,
        offset_message_id: Option<String>,
        client_message_id: Option<String>,
    ) {
        self.mark_client_ext();
        self.offset_message_id = offset_message_id;
        self.message_id = client_message_id;
    }
    pub(crate) fn mark_client_ext(&mut self) {
        self.client_ext = true;
    }
    pub fn set_offset_message_id(&mut self, value: Option<String>) {
        if self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_offset_message_id(value.clone()))
        {
            return;
        }
        self.offset_message_id = value.clone();
        if self.client_ext {
            self.message_id = value;
        }
    }
    pub fn set_prepared_transaction_offset(&mut self, value: i64) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_prepared_transaction_offset(value))
        {
            self.prepared_transaction_offset = value;
        }
    }
    /// Read Java `MessageExt.getTopicSysFlag()`. A direct subtype override has
    /// precedence; the inherited property path treats empty text as absent and
    /// reports malformed decimal input as a parse error.
    pub fn topic_sys_flag(&self) -> Result<Option<i32>, MessageSysFlagParseError> {
        if let Some(value) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.topic_sys_flag())
        {
            return Ok(value);
        }
        parse_transient_sys_flag(
            self.get_property(crate::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG)
                .as_deref(),
        )
    }
    /// Set or clear Java's transient topic system flag property.
    pub fn set_topic_sys_flag(&mut self, value: Option<i32>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_topic_sys_flag(value))
        {
            set_transient_sys_flag(
                &mut self.message,
                crate::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
                value,
            );
        }
    }
    /// Read Java `MessageExt.getGroupSysFlag()`. A direct subtype override has
    /// precedence; the inherited property path treats empty text as absent and
    /// reports malformed decimal input as a parse error.
    pub fn group_sys_flag(&self) -> Result<Option<i32>, MessageSysFlagParseError> {
        if let Some(value) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.group_sys_flag())
        {
            return Ok(value);
        }
        parse_transient_sys_flag(
            self.get_property(crate::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG)
                .as_deref(),
        )
    }
    /// Set or clear Java's transient group system flag property.
    pub fn set_group_sys_flag(&mut self, value: Option<i32>) {
        if !self
            .runtime_subtype
            .as_ref()
            .is_some_and(|subtype| subtype.set_group_sys_flag(value))
        {
            set_transient_sys_flag(
                &mut self.message,
                crate::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
                value,
            );
        }
    }

    /// Java-style diagnostic string. Property entries are emitted in Rust's
    /// deterministic map order; Java's backing `HashMap` does not promise an
    /// iteration order when multiple properties are present.
    fn java_to_string(&self) -> String {
        if let Some(value) = self
            .runtime_subtype
            .as_ref()
            .and_then(|subtype| subtype.display_value())
        {
            return value;
        }
        let born_host = self
            .born_host
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_owned());
        let store_host = self
            .store_host
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_owned());
        let message_id = self.message_id.as_deref().unwrap_or("null");
        let broker_name = self.broker_name.as_deref().unwrap_or("null");
        format!(
            "MessageExt [brokerName={broker_name}, queueId={}, storeSize={}, queueOffset={}, sysFlag={}, bornTimestamp={}, bornHost={born_host}, storeTimestamp={}, storeHost={store_host}, msgId={message_id}, commitLogOffset={}, bodyCRC={}, reconsumeTimes={}, preparedTransactionOffset={}, toString()={}]",
            self.queue_id,
            self.store_size,
            self.queue_offset,
            self.sys_flag,
            self.born_timestamp,
            self.store_timestamp,
            self.commit_log_offset,
            self.body_crc,
            self.reconsume_times,
            self.prepared_transaction_offset,
            self.message.java_to_string(),
        )
    }
}

/// Dynamic Rust adapter for application-defined `MessageExt` subtypes.
/// Implement `as_any` to retain custom fields for typed downcasts; override
/// the base methods whose Java implementations are virtual overrides.
/// A hook returns `None` to inherit the stored `MessageExt` value. For nullable
/// getters, `Some(None)` explicitly overrides the value with Java `null`. The
/// adapter is runtime-only: cloning shares it, while serialization and
/// deserialization omit it.
pub trait MessageExtSubtype: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn runtime_type_name(&self) -> &'static str {
        type_name::<Self>()
    }

    fn topic(&self) -> Option<Option<&str>> {
        None
    }

    /// Override Java's virtual `Message.getTags()` independently of
    /// `getProperty(String)`.
    fn tags(&self) -> Option<Option<String>> {
        None
    }

    /// Override Java's virtual `Message.getKeys()` independently of
    /// `getProperty(String)`.
    fn keys(&self) -> Option<Option<String>> {
        None
    }

    /// Override Java's virtual `Message.isWaitStoreMsgOK()`.
    fn wait_store_msg_ok(&self) -> Option<bool> {
        None
    }

    /// Override Java's virtual `Message.getBuyerId()` independently of
    /// `getProperty(String)`.
    fn buyer_id(&self) -> Option<Option<String>> {
        None
    }

    /// Override Java's virtual property-derived delay getters.
    fn delay_time_level(&self) -> Option<i32> {
        None
    }

    fn delay_time_sec(&self) -> Option<i64> {
        None
    }

    fn delay_time_ms(&self) -> Option<i64> {
        None
    }

    fn deliver_time_ms(&self) -> Option<i64> {
        None
    }

    /// Override Java's virtual `MessageExt.getTopicSysFlag()` and
    /// `getGroupSysFlag()` accessors. `Some(None)` represents Java `null`.
    fn topic_sys_flag(&self) -> Option<Option<i32>> {
        None
    }

    fn group_sys_flag(&self) -> Option<Option<i32>> {
        None
    }

    /// Override Java's virtual `Message.getProperty(String)`.
    fn property(&self, _key: Option<&str>) -> Option<Option<String>> {
        None
    }

    /// Override Java's virtual `Message.getPriority()`.
    fn priority(&self) -> Option<i32> {
        None
    }

    /// Handle Java's virtual `Message.setPriority(int)`.
    fn set_priority(&self, _priority: i32) -> bool {
        false
    }

    /// Override Java's virtual `Message.getUserProperty(String)`.
    fn user_property(&self, _key: Option<&str>) -> Option<Option<String>> {
        None
    }

    /// Override Java's virtual `Message.hasProperty(String)`.
    fn has_property(&self, _key: Option<&str>) -> Option<bool> {
        None
    }

    /// Override Java's virtual `Message.getTransactionId()`.
    fn transaction_id(&self) -> Option<Option<&str>> {
        None
    }

    /// Override Java's virtual `Message.getProperties()` with a nullable map.
    /// Returned maps are snapshotted for Rust reads and retry-property copies.
    fn properties(&self) -> Option<Option<NullableMessageProperties>> {
        None
    }

    /// Override Java's virtual `Message.getProperties()` with a live mutable
    /// map. The returned handle is borrowed from the subtype and shared by
    /// Rust clones, preserving caller-visible map aliasing. This takes
    /// precedence over the snapshot-only `properties` hook.
    fn live_properties(&self) -> Option<Option<&SharedMessageProperties>> {
        None
    }

    /// Handle Java's virtual `MessageExt.setTopicSysFlag(Integer)`.
    fn set_topic_sys_flag(&self, _topic_sys_flag: Option<i32>) -> bool {
        false
    }

    /// Handle Java's virtual `MessageExt.setGroupSysFlag(Integer)`.
    fn set_group_sys_flag(&self, _group_sys_flag: Option<i32>) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setWaitStoreMsgOK(boolean)`.
    fn set_wait_store_msg_ok(&self, _wait: bool) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setBuyerId(String)`.
    fn set_buyer_id(&self, _buyer_id: Option<String>) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setInstanceId(String)`.
    fn set_instance_id(&self, _instance_id: Option<String>) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setDelayTimeLevel(int)`.
    fn set_delay_time_level(&self, _level: i32) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setDelayTimeSec(long)`.
    fn set_delay_time_sec(&self, _seconds: i64) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setDelayTimeMs(long)`.
    fn set_delay_time_ms(&self, _milliseconds: i64) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setDeliverTimeMs(long)`.
    fn set_deliver_time_ms(&self, _milliseconds: i64) -> bool {
        false
    }

    /// Handle Java's virtual `Message.putUserProperty(String, String)`.
    fn put_user_property(&self, _key: Option<String>, _value: Option<String>) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setTags(String)`.
    fn set_tags(&self, _tags: Option<String>) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setKeys(String)`.
    fn set_keys(&self, _keys: Option<String>) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setTransactionId(String)`.
    fn set_transaction_id(&self, _transaction_id: Option<String>) -> bool {
        false
    }

    fn set_topic(&self, _topic: Option<String>) -> bool {
        false
    }

    /// Override Java's virtual `Message.getBody()`.
    fn body(&self) -> Option<Option<Vec<u8>>> {
        None
    }

    /// Override Java's virtual `Message.getBody()` with its live mutable byte
    /// array. The owned handle preserves array identity across getter calls.
    /// This hook takes precedence over the snapshot-only `body` hook.
    fn shared_body(&self) -> Option<Option<SharedMessageBody>> {
        None
    }

    /// Handle Java's virtual `Message.setBody(byte[])`.
    fn set_body(&self, _body: Option<Vec<u8>>) -> bool {
        false
    }

    /// Handle Java's virtual `Message.setBody(byte[])` while retaining the
    /// caller's live byte-array identity.
    fn set_shared_body(&self, _body: Option<SharedMessageBody>) -> bool {
        false
    }

    /// Override Java's virtual `Message.getFlag()`.
    fn flag(&self) -> Option<i32> {
        None
    }

    /// Handle Java's virtual `Message.setFlag(int)`.
    fn set_flag(&self, _flag: i32) -> bool {
        false
    }

    fn broker_name(&self) -> Option<Option<&str>> {
        None
    }

    fn set_broker_name(&self, _broker_name: Option<String>) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getStoreHost()`.
    fn store_host(&self) -> Option<Option<&MessageHostAddress>> {
        None
    }

    /// Handle Java's virtual `MessageExt.setStoreHost(SocketAddress)`.
    fn set_store_host(&self, _store_host: Option<MessageHostAddress>) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getBornTimestamp()`.
    fn born_timestamp(&self) -> Option<i64> {
        None
    }

    /// Handle Java's virtual `MessageExt.setBornTimestamp(long)`.
    fn set_born_timestamp(&self, _born_timestamp: i64) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getBornHost()`.
    fn born_host(&self) -> Option<Option<&MessageHostAddress>> {
        None
    }

    /// Override Java's virtual `MessageExt.getBornHostString()`.
    fn born_host_string(&self) -> Option<Option<String>> {
        None
    }

    /// Override Java's virtual `MessageExt.getBornHostNameString()`.
    fn born_host_name_string(&self) -> Option<Option<&str>> {
        None
    }

    /// Override Java's virtual no-argument `MessageExt.getBornHostBytes()`.
    fn born_host_bytes(&self) -> Option<Vec<u8>> {
        None
    }

    /// Override Java's virtual no-argument `MessageExt.getStoreHostBytes()`.
    fn store_host_bytes(&self) -> Option<Vec<u8>> {
        None
    }

    /// Handle Java's virtual `MessageExt.getBornHostBytes(ByteBuffer)` using
    /// the caller-provided Rust output slice.
    fn write_born_host_bytes(
        &self,
        _output: &mut [u8],
    ) -> Option<Result<usize, MessageHostAddressBytesError>> {
        None
    }

    /// Handle Java's virtual `MessageExt.getStoreHostBytes(ByteBuffer)` using
    /// the caller-provided Rust output slice.
    fn write_store_host_bytes(
        &self,
        _output: &mut [u8],
    ) -> Option<Result<usize, MessageHostAddressBytesError>> {
        None
    }

    /// Handle Java's virtual `MessageExt.setBornHost(SocketAddress)`.
    fn set_born_host(&self, _born_host: Option<MessageHostAddress>) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getStoreTimestamp()`.
    fn store_timestamp(&self) -> Option<i64> {
        None
    }

    /// Handle Java's virtual `MessageExt.setStoreTimestamp(long)`.
    fn set_store_timestamp(&self, _store_timestamp: i64) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getSysFlag()`.
    fn sys_flag(&self) -> Option<i32> {
        None
    }

    /// Handle Java's virtual `MessageExt.setSysFlag(int)`.
    fn set_sys_flag(&self, _sys_flag: i32) -> bool {
        false
    }

    /// Handle Java's virtual `MessageExt.setBornHostV6Flag()`.
    fn set_born_host_v6_flag(&self) -> bool {
        false
    }

    /// Handle Java's virtual `MessageExt.setStoreHostAddressV6Flag()`.
    fn set_store_host_address_v6_flag(&self) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getBodyCRC()`.
    fn body_crc(&self) -> Option<i32> {
        None
    }

    /// Handle Java's virtual `MessageExt.setBodyCRC(int)`.
    fn set_body_crc(&self, _body_crc: i32) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getCommitLogOffset()`.
    fn commit_log_offset(&self) -> Option<i64> {
        None
    }

    /// Handle Java's virtual `MessageExt.setCommitLogOffset(long)`.
    fn set_commit_log_offset(&self, _commit_log_offset: i64) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getStoreSize()`.
    fn store_size(&self) -> Option<i32> {
        None
    }

    /// Handle Java's virtual `MessageExt.setStoreSize(int)`.
    fn set_store_size(&self, _store_size: i32) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getPreparedTransactionOffset()`.
    fn prepared_transaction_offset(&self) -> Option<i64> {
        None
    }

    /// Handle Java's virtual `MessageExt.setPreparedTransactionOffset(long)`.
    fn set_prepared_transaction_offset(&self, _prepared_transaction_offset: i64) -> bool {
        false
    }

    fn queue_id(&self) -> Option<i32> {
        None
    }

    fn set_queue_id(&self, _queue_id: i32) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getQueueOffset()`.
    fn queue_offset(&self) -> Option<i64> {
        None
    }

    /// Handle Java's virtual `MessageExt.setQueueOffset(long)`.
    fn set_queue_offset(&self, _queue_offset: i64) -> bool {
        false
    }

    fn message_id(&self) -> Option<Option<String>> {
        None
    }

    /// Override Java `MessageClientExt.getOffsetMsgId()`. This owned result
    /// supports subtype getters backed by interior mutable state.
    fn offset_message_id(&self) -> Option<Option<String>> {
        None
    }

    /// Handle Java `MessageClientExt.setOffsetMsgId(String)`.
    fn set_offset_message_id(&self, _offset_message_id: Option<String>) -> bool {
        false
    }

    fn set_message_id(&self, _message_id: Option<String>) -> bool {
        false
    }

    /// Override Java's virtual `MessageExt.getReconsumeTimes()`.
    fn reconsume_times(&self) -> Option<i32> {
        None
    }

    /// Handle Java's virtual `MessageExt.setReconsumeTimes(int)`.
    fn set_reconsume_times(&self, _reconsume_times: i32) -> bool {
        false
    }

    fn display_value(&self) -> Option<String> {
        None
    }
}

impl std::fmt::Display for MessageExt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.java_to_string())
    }
}

/// Client-decoded `MessageExt` view corresponding to Java's
/// `MessageClientExt`. The wrapped base message retains its physical ID;
/// `message_id()` resolves the live `UNIQ_KEY` property first, and the Java
/// subclass's `setMsgId` override remains a no-op. Java exposes an implicit
/// public no-argument constructor; Rust provides `new()` and `Default` for
/// that construction path.
///
/// ```
/// use rocketmq_classic_common::message::MessageClientExt;
/// let _by_constructor = MessageClientExt::new();
/// let _by_default = MessageClientExt::default();
/// ```
///
/// Convert the subclass into the base type accepted by APIs such as direct
/// consumption while retaining its runtime type and virtual ID behavior:
///
/// ```
/// use rocketmq_classic_common::message::{MessageClientExt, MessageExt};
/// let client_message = MessageClientExt::new();
/// let base_message: MessageExt = client_message.into();
/// assert!(base_message.is_message_client_ext());
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::{MessageClientExt, MessageExt};
/// let _: MessageClientExt = MessageExt::default().into();
/// ```
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct MessageClientExt(MessageExt);

impl Default for MessageClientExt {
    fn default() -> Self {
        Self::new()
    }
}

impl<'de> Deserialize<'de> for MessageClientExt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut message = MessageExt::deserialize(deserializer)?;
        message.client_ext = true;
        Ok(Self(message))
    }
}

impl Hash for MessageClientExt {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl MessageClientExt {
    pub fn new() -> Self {
        Self(MessageExt {
            client_ext: true,
            ..MessageExt::default()
        })
    }

    pub fn message_id(&self) -> Option<String> {
        self.0.message_id_for_runtime_type()
    }

    /// Java `MessageClientExt.setMsgId` intentionally does nothing.
    pub fn set_message_id(&mut self, value: Option<String>) {
        self.0.set_message_id(value);
    }

    /// Java `MessageClientExt.getOffsetMsgId` returns the inherited `msgId`
    /// through the runtime subtype's virtual getter.
    pub fn offset_message_id(&self) -> Option<String> {
        self.0.offset_message_id_for_runtime_type()
    }

    /// Java `MessageClientExt.setOffsetMsgId` writes the inherited `msgId`.
    pub fn set_offset_message_id(&mut self, value: Option<String>) {
        self.0.set_offset_message_id(value);
    }

    /// Convert to the base type accepted by Java APIs whose parameter is
    /// `MessageExt`; the runtime subtype remains observable on the Rust value.
    pub fn into_message_ext(self) -> MessageExt {
        self.0
    }
}

impl From<MessageClientExt> for MessageExt {
    fn from(message: MessageClientExt) -> Self {
        message.into_message_ext()
    }
}

impl Deref for MessageClientExt {
    type Target = MessageExt;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MessageClientExt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl PartialEq<MessageExt> for Message {
    fn eq(&self, other: &MessageExt) -> bool {
        Arc::ptr_eq(&self.identity, &other.message.identity)
    }
}

impl PartialEq<Message> for MessageExt {
    fn eq(&self, other: &Message) -> bool {
        Arc::ptr_eq(&self.message.identity, &other.identity)
    }
}

impl PartialEq<MessageClientExt> for Message {
    fn eq(&self, other: &MessageClientExt) -> bool {
        Arc::ptr_eq(&self.identity, &other.0.message.identity)
    }
}

impl PartialEq<Message> for MessageClientExt {
    fn eq(&self, other: &Message) -> bool {
        Arc::ptr_eq(&self.0.message.identity, &other.identity)
    }
}

impl PartialEq<MessageClientExt> for MessageExt {
    fn eq(&self, other: &MessageClientExt) -> bool {
        Arc::ptr_eq(&self.message.identity, &other.0.message.identity)
    }
}

impl PartialEq<MessageExt> for MessageClientExt {
    fn eq(&self, other: &MessageExt) -> bool {
        Arc::ptr_eq(&self.0.message.identity, &other.message.identity)
    }
}

fn parse_transient_sys_flag(value: Option<&str>) -> Result<Option<i32>, MessageSysFlagParseError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    parse_java_i32(value)
        .map(Some)
        .ok_or_else(|| MessageSysFlagParseError {
            value: value.to_owned(),
        })
}

fn parse_i32_property_value(
    property: &'static str,
    value: Option<String>,
) -> Result<i32, MessagePropertyParseError> {
    let Some(value) = value else {
        return Ok(0);
    };
    parse_java_i32(&value).ok_or(MessagePropertyParseError { property, value })
}

fn parse_i64_property_value(
    property: &'static str,
    value: Option<String>,
) -> Result<i64, MessagePropertyParseError> {
    let Some(value) = value else {
        return Ok(0);
    };
    parse_java_i64(&value).ok_or(MessagePropertyParseError { property, value })
}

fn set_transient_sys_flag(message: &mut Message, key: &str, value: Option<i32>) {
    match value {
        Some(value) => {
            message.put_property_value(key, Some(value.to_string()));
        }
        None => {
            message.clear_system_property(key);
        }
    }
}

/// Queue identity corresponding to Java's `MessageQueue` constructors.
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageQueue;
/// let _queue = MessageQueue::new_nullable_broker("topic", None, 0);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize)]
pub struct MessageQueue {
    #[serde(
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    topic: Option<String>,
    #[serde(
        rename = "brokerName",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    broker_name: Option<String>,
    #[serde(
        rename = "queueId",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_i32"
    )]
    queue_id: i32,
}

#[derive(Deserialize)]
struct MessageQueueFields {
    #[serde(
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        rename = "brokerName",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string"
    )]
    broker_name: Option<String>,
    #[serde(
        rename = "queueId",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_i32"
    )]
    queue_id: i32,
}

impl<'de> Deserialize<'de> for MessageQueue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: MessageQueueFields =
            crate::serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            broker_name: fields.broker_name,
            queue_id: fields.queue_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MessageQueueCompareError {
    #[error("Java NullPointerException: null MessageQueue compareTo argument")]
    NullOther,
    #[error("MessageQueue topic is null")]
    NullTopic,
    #[error("MessageQueue broker name is null")]
    NullBrokerName,
}

impl MessageQueue {
    pub fn new(topic: impl Into<String>, broker_name: impl Into<String>, queue_id: i32) -> Self {
        Self {
            topic: Some(topic.into()),
            broker_name: Some(broker_name.into()),
            queue_id,
        }
    }
    pub fn new_nullable(topic: Option<String>, broker_name: Option<String>, queue_id: i32) -> Self {
        Self {
            topic,
            broker_name,
            queue_id,
        }
    }
    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }
    pub fn broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }
    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }
    pub fn set_broker_name(&mut self, broker_name: Option<String>) {
        self.broker_name = broker_name;
    }
    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    /// Returns the pinned Java `MessageQueue.hashCode` value.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = 31i32.wrapping_add(java_optional_string_hash(&self.broker_name));
        result = 31i32.wrapping_mul(result).wrapping_add(self.queue_id);
        31i32
            .wrapping_mul(result)
            .wrapping_add(java_optional_string_hash(&self.topic))
    }

    /// Returns the exact pinned Java 5.5.1 `MessageQueue.compareTo` result.
    pub fn java_compare_to(&self, other: &Self) -> Result<i32, MessageQueueCompareError> {
        let topic = self
            .topic
            .as_deref()
            .ok_or(MessageQueueCompareError::NullTopic)?;
        let other_topic = other
            .topic
            .as_deref()
            .ok_or(MessageQueueCompareError::NullTopic)?;
        let result = java_string_compare_to(topic, other_topic);
        if result != 0 {
            return Ok(result);
        }
        let broker = self
            .broker_name
            .as_deref()
            .ok_or(MessageQueueCompareError::NullBrokerName)?;
        let other_broker = other
            .broker_name
            .as_deref()
            .ok_or(MessageQueueCompareError::NullBrokerName)?;
        let result = java_string_compare_to(broker, other_broker);
        if result != 0 {
            return Ok(result);
        }
        Ok(match self.queue_id.cmp(&other.queue_id) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        })
    }

    /// Compares with an optional value and maps Java's null-argument failure.
    pub fn java_compare_to_nullable(
        &self,
        other: Option<&Self>,
    ) -> Result<i32, MessageQueueCompareError> {
        other
            .map(|other| self.java_compare_to(other))
            .unwrap_or(Err(MessageQueueCompareError::NullOther))
    }
}

impl std::fmt::Display for MessageQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MessageQueue [topic={}, brokerName={}, queueId={}]",
            self.topic.as_deref().unwrap_or("null"),
            self.broker_name.as_deref().unwrap_or("null"),
            self.queue_id
        )
    }
}

/// Java 5.5.1 `MessageQueueForC` value used by the C-client offset response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageQueueForC {
    topic: Option<String>,
    broker_name: Option<String>,
    queue_id: i32,
    offset: i64,
}

/// Java `MessageQueueForC.compareTo` null failure branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MessageQueueForCCompareError {
    #[error("Java NullPointerException: null MessageQueueForC compareTo argument")]
    NullOther,
    #[error("MessageQueueForC topic is null")]
    NullTopic,
    #[error("MessageQueueForC broker name is null")]
    NullBrokerName,
}

impl MessageQueueForC {
    /// Maps Java's `(topic, brokerName, queueId, offset)` constructor.
    pub fn new(
        topic: Option<String>,
        broker_name: Option<String>,
        queue_id: i32,
        offset: i64,
    ) -> Self {
        Self {
            topic,
            broker_name,
            queue_id,
            offset,
        }
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub const fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub const fn offset(&self) -> i64 {
        self.offset
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn set_broker_name(&mut self, broker_name: Option<String>) {
        self.broker_name = broker_name;
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    pub fn set_offset(&mut self, offset: i64) {
        self.offset = offset;
    }

    /// Returns Java's `hashCode`; the source method intentionally omits offset.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = 1i32;
        result = 31i32
            .wrapping_mul(result)
            .wrapping_add(java_optional_string_hash(&self.broker_name));
        result = 31i32.wrapping_mul(result).wrapping_add(self.queue_id);
        31i32
            .wrapping_mul(result)
            .wrapping_add(java_optional_string_hash(&self.topic))
    }

    /// Returns the pinned Java 5.5.1 `compareTo` result, including subtraction wrap.
    pub fn java_compare_to(&self, other: &Self) -> Result<i32, MessageQueueForCCompareError> {
        let topic = self
            .topic
            .as_deref()
            .ok_or(MessageQueueForCCompareError::NullTopic)?;
        let other_topic = other
            .topic
            .as_deref()
            .ok_or(MessageQueueForCCompareError::NullTopic)?;
        let result = java_string_compare_to(topic, other_topic);
        if result != 0 {
            return Ok(result);
        }

        let broker = self
            .broker_name
            .as_deref()
            .ok_or(MessageQueueForCCompareError::NullBrokerName)?;
        let other_broker = other
            .broker_name
            .as_deref()
            .ok_or(MessageQueueForCCompareError::NullBrokerName)?;
        let result = java_string_compare_to(broker, other_broker);
        if result != 0 {
            return Ok(result);
        }

        let result = self.queue_id.wrapping_sub(other.queue_id);
        if result != 0 {
            return Ok(result);
        }
        let offset_difference = self.offset.wrapping_sub(other.offset);
        Ok(match offset_difference.cmp(&0) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        })
    }

    /// Maps Java's `NullPointerException` when `compareTo` receives null.
    pub fn java_compare_to_nullable(
        &self,
        other: Option<&Self>,
    ) -> Result<i32, MessageQueueForCCompareError> {
        other
            .map(|other| self.java_compare_to(other))
            .unwrap_or(Err(MessageQueueForCCompareError::NullOther))
    }
}

impl std::fmt::Display for MessageQueueForC {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "MessageQueueForC [topic={}, brokerName={}, queueId={}, offset={}]",
            self.topic.as_deref().unwrap_or("null"),
            self.broker_name.as_deref().unwrap_or("null"),
            self.queue_id,
            self.offset
        )
    }
}

impl Ord for MessageQueue {
    fn cmp(&self, other: &Self) -> Ordering {
        java_optional_string_cmp(&self.topic, &other.topic)
            .then_with(|| match (&self.broker_name, &other.broker_name) {
                (Some(left), Some(right)) => java_string_cmp(left, right),
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
            })
            .then_with(|| self.queue_id.cmp(&other.queue_id))
    }
}

impl PartialOrd for MessageQueue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn java_string_cmp(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn java_string_compare_to(left: &str, right: &str) -> i32 {
    for (a, b) in left.encode_utf16().zip(right.encode_utf16()) {
        if a != b {
            return i32::from(a) - i32::from(b);
        }
    }
    (left.encode_utf16().count() as i32) - (right.encode_utf16().count() as i32)
}

fn java_optional_string_hash(value: &Option<String>) -> i32 {
    value.as_deref().map_or(0, java_string_hash_code)
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn java_optional_string_cmp(left: &Option<String>, right: &Option<String>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => java_string_cmp(left, right),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}

fn validate_property(key: &str, value: &str) -> Result<(), MessagePropertyError> {
    if java_trim_is_empty(key) {
        return Err(MessagePropertyError::EmptyKey);
    }
    if java_trim_is_empty(value) {
        return Err(MessagePropertyError::EmptyValue);
    }
    Ok(())
}

fn java_trim_is_empty(value: &str) -> bool {
    value
        .trim_matches(|character: char| character <= '\u{20}')
        .is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_ext_inherits_java_message_surface_through_deref() {
        let mut extended = MessageExt::new();
        extended.set_topic("reply-topic");
        extended.set_body(b"reply-body".to_vec());

        let inherited: &Message = &extended;
        assert_eq!(inherited.topic(), "reply-topic");
        assert_eq!(inherited.body().as_ref(), b"reply-body");

        let owned_copy = Message::clone(inherited);
        assert_eq!(owned_copy.topic(), "reply-topic");
        assert_eq!(owned_copy.body().as_ref(), b"reply-body");
    }

    #[test]
    fn message_queue_fastjson_inputs_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["messageQueueInputCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            19,
            "Java oracle includes scalar, numeric and duplicate-field boundaries"
        );
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<MessageQueue>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    fn message_queue_for_c_from_row(row: &serde_json::Value) -> MessageQueueForC {
        MessageQueueForC::new(
            row["topic"].as_str().map(str::to_owned),
            row["brokerName"].as_str().map(str::to_owned),
            row["queueId"].as_i64().unwrap() as i32,
            row["offset"].as_i64().unwrap(),
        )
    }

    #[test]
    fn message_queue_for_c_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let java = &fixture["messageQueueForCApi"];
        let queue = message_queue_for_c_from_row(&java["values"]);

        assert_eq!(queue.topic(), Some("topic"));
        assert_eq!(queue.broker_name(), Some("broker"));
        assert_eq!(queue.queue_id(), -7);
        assert_eq!(queue.offset(), 42);
        assert_eq!(
            queue.java_hash_code(),
            java["hashCode"].as_i64().unwrap() as i32
        );
        assert_eq!(queue.to_string(), java["toString"].as_str().unwrap());
        assert_eq!(queue == queue.clone(), java["equalsCopy"]);
        assert!(
            queue != MessageQueueForC::new(Some("topic".into()), Some("broker".into()), -7, 43)
        );
        assert_eq!(java["equalsSelf"], true);
        assert_eq!(java["equalsNull"], false);
        assert_eq!(java["equalsWrongType"], false);
        assert_eq!(java["equalsOffsetChanged"], false);
        assert_eq!(java["hashIgnoresOffset"], true);
        assert_eq!(
            queue.java_compare_to_nullable(None),
            Err(MessageQueueForCCompareError::NullOther)
        );

        let mut updated =
            MessageQueueForC::new(Some("old-topic".into()), Some("old-broker".into()), 0, 0);
        updated.set_topic(Some("new-topic".into()));
        updated.set_broker_name(None);
        updated.set_queue_id(i32::MIN);
        updated.set_offset(i64::MIN);
        assert_eq!(updated.topic(), java["setterValues"]["topic"].as_str());
        assert_eq!(updated.broker_name(), None);
        assert_eq!(updated.queue_id(), i32::MIN);
        assert_eq!(updated.offset(), i64::MIN);

        for case in fixture["messageQueueForCCompareTo"].as_array().unwrap() {
            let left = message_queue_for_c_from_row(&case["left"]);
            let right = message_queue_for_c_from_row(&case["right"]);
            match case["outcome"].as_str().unwrap() {
                "OK" => assert_eq!(
                    left.java_compare_to(&right).unwrap(),
                    case["result"].as_i64().unwrap() as i32
                ),
                "NullPointerException" => assert!(left.java_compare_to(&right).is_err()),
                outcome => panic!("unexpected Java compareTo outcome: {outcome}"),
            }
        }
    }

    #[test]
    fn java_queue_order_uses_utf16_code_units() {
        let supplementary = MessageQueue::new("\u{10000}", "b", 0);
        let bmp = MessageQueue::new("\u{e000}", "b", 0);
        assert!(supplementary < bmp);
    }

    #[test]
    fn queue_equality_includes_all_java_fields_and_signed_id() {
        let a = MessageQueue::new("t", "b", i32::MIN);
        let b = MessageQueue::new("t", "b", i32::MIN);
        assert_eq!(a, b);
        assert_eq!(a.queue_id(), i32::MIN);
        assert_ne!(a, MessageQueue::new("t", "b", i32::MAX));
    }

    #[test]
    fn message_queue_setters_preserve_nullable_java_fields() {
        let mut queue = MessageQueue::new("topic", "broker", 1);
        queue.set_topic(None);
        queue.set_broker_name(None);
        queue.set_queue_id(i32::MIN);
        assert_eq!(queue.topic(), None);
        assert_eq!(queue.broker_name(), None);
        assert_eq!(queue.queue_id(), i32::MIN);
        queue.set_topic(Some("next".into()));
        queue.set_broker_name(Some("new-broker".into()));
        queue.set_queue_id(-17);
        assert_eq!(queue.topic(), Some("next"));
        assert_eq!(queue.broker_name(), Some("new-broker"));
        assert_eq!(queue.queue_id(), -17);
    }

    #[test]
    fn message_has_separate_user_and_broker_metadata() {
        let mut message = Message::new("topic", [0, 1, 255]);
        message.set_flag(-1);
        message.set_transaction_id(Some("tx".into()));
        assert!(message.is_wait_store_msg_ok());
        message.set_tags("tag").unwrap();
        assert_eq!(message.tags(), Some("tag".to_owned()));
        let mut ext = MessageExt::from_message(message.clone());
        ext.set_queue_offset(i64::MIN);
        ext.set_commit_log_offset(i64::MAX);
        assert_eq!(ext.topic_nullable(), message.topic_nullable());
        assert_eq!(
            ext.body_nullable().as_deref(),
            message.body_nullable().as_deref()
        );
        assert_eq!(&*ext.properties(), &*message.properties());
        assert_eq!(ext.queue_offset(), i64::MIN);
        assert_eq!(ext.commit_log_offset(), i64::MAX);
    }

    #[test]
    fn java_property_setters_accept_wire_separators() {
        let mut message = Message::new("topic", vec![]);
        message.put_user_property("key", "bad\u{2}value").unwrap();
        assert_eq!(message.properties().get("key").unwrap(), "bad\u{2}value");
        assert_eq!(
            message.put_user_property("", "value"),
            Err(MessagePropertyError::EmptyKey)
        );
        assert_eq!(
            message.put_user_property("KEYS", "user-value"),
            Err(MessagePropertyError::ReservedProperty)
        );
        assert_eq!(
            message.put_user_property("custom", "  "),
            Err(MessagePropertyError::EmptyValue)
        );
    }
}
