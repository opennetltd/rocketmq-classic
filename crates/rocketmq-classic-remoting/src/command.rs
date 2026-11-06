use std::any::Any;
use std::borrow::Cow;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use rocketmq_classic_protocol::{
    codes::{JavaEnumLookupError, LanguageCode, SerializeType},
    header::{CommandHeader, HeaderMap, ProtocolError},
};

use crate::ext_fields::ExtFields;

static REQUEST_ID: AtomicI32 = AtomicI32::new(0);
static COMMAND_VERSION: AtomicI32 = AtomicI32::new(-1);
static CLIENT_API_VERSION: AtomicI32 = AtomicI32::new(-1);
static SERIALIZE_TYPE_CONFIG: OnceLock<Result<SerializeType, String>> = OnceLock::new();

#[derive(Debug, Default)]
struct ProcessTimerState {
    accumulated: Duration,
    started_at: Option<Instant>,
}

/// A monotonic command-processing timer corresponding to Java's remoting
/// `Stopwatch` field. Cloning shares its mutable state, matching Java's
/// reference-valued `RemotingCommand.processTimer` field.
///
/// Java Guava `Stopwatch` inherits identity-based `Object.equals` and
/// `Object.hashCode`; Rust does not expose structural equality for timer
/// instances.
///
/// ```compile_fail
/// use rocketmq_classic_remoting::ProcessTimer;
/// let first = ProcessTimer::new();
/// let second = ProcessTimer::new();
/// let _ = first == second;
/// ```
#[derive(Clone, Debug, Default)]
pub struct ProcessTimer {
    state: Arc<Mutex<ProcessTimerState>>,
}

impl ProcessTimer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn started() -> Self {
        Self {
            state: Arc::new(Mutex::new(ProcessTimerState {
                accumulated: Duration::ZERO,
                started_at: Some(Instant::now()),
            })),
        }
    }

    pub fn is_running(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .started_at
            .is_some()
    }

    pub fn elapsed(&self) -> Duration {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.accumulated.saturating_add(
            state
                .started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or_default(),
        )
    }

    pub fn start(&mut self) -> Result<(), ProcessTimerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.started_at.is_some() {
            return Err(ProcessTimerError::AlreadyRunning);
        }
        state.started_at = Some(Instant::now());
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), ProcessTimerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(started_at) = state.started_at.take() else {
            return Err(ProcessTimerError::NotRunning);
        };
        state.accumulated = state.accumulated.saturating_add(started_at.elapsed());
        Ok(())
    }

    pub fn reset(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.accumulated = Duration::ZERO;
        state.started_at = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProcessTimerError {
    #[error("process timer is already running")]
    AlreadyRunning,
    #[error("process timer is not running")]
    NotRunning,
}

/// A command callback with the no-argument behavior of Java's
/// `org.apache.rocketmq.remoting.CommandCallback`.
#[derive(Clone)]
pub struct CommandCallback(Arc<dyn Fn() + Send + Sync + 'static>);

impl CommandCallback {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        Self(Arc::new(callback))
    }

    pub fn accept(&self) {
        (self.0)();
    }
}

impl std::fmt::Debug for CommandCallback {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandCallback(..)")
    }
}

impl PartialEq for CommandCallback {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CommandCallback {}

trait CustomHeaderValueDyn: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn clone_box(&self) -> Box<dyn CustomHeaderValueDyn>;
    fn encode_fields(&self, fields: &mut HeaderMap);
    fn concrete_type_name(&self) -> &'static str;
}

impl<T> CustomHeaderValueDyn for T
where
    T: CommandHeader + Clone + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn CustomHeaderValueDyn> {
        Box::new(self.clone())
    }

    fn encode_fields(&self, fields: &mut HeaderMap) {
        CommandHeader::encode_fields(self, fields);
    }

    fn concrete_type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

/// Type-erased storage for the typed custom header attached to a command.
///
/// This is a hidden field-level representation; use `read_custom_header`,
/// `read_custom_header_mut`, and `write_custom_header` to access it.
#[doc(hidden)]
pub struct CustomHeaderValue(Box<dyn CustomHeaderValueDyn>);

impl CustomHeaderValue {
    fn new<T>(header: T) -> Self
    where
        T: CommandHeader + Clone + Send + Sync + 'static,
    {
        Self(Box::new(header))
    }

    fn encoded_fields(&self) -> HeaderMap {
        let mut fields = HeaderMap::new();
        self.0.encode_fields(&mut fields);
        fields
    }
}

impl Clone for CustomHeaderValue {
    fn clone(&self) -> Self {
        Self(self.0.clone_box())
    }
}

impl std::fmt::Debug for CustomHeaderValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CustomHeaderValue")
            .field("type", &self.0.concrete_type_name())
            .field("fields", &self.encoded_fields())
            .finish()
    }
}

/// A typed custom header that preserves Java's shared-object behavior.
///
/// Clone this handle before attaching it when code must retain access to the
/// same header value after `write_custom_header`:
///
/// ```
/// use rocketmq_classic_protocol::header::GetRouteInfoRequestHeader;
/// use rocketmq_classic_remoting::{RemotingCommand, command::SharedCommandHeader};
///
/// let header = SharedCommandHeader::new(GetRouteInfoRequestHeader::default());
/// let mut command = RemotingCommand::create_request_command(1).unwrap();
/// command.write_custom_header(Some(header.clone()));
/// header.with_mut(|value| value.set_topic("orders".into()));
/// assert_eq!(
///     command
///         .read_custom_header::<SharedCommandHeader<GetRouteInfoRequestHeader>>()
///         .unwrap()
///         .with(|value| value.get_topic().to_owned()),
///     "orders"
/// );
/// ```
pub struct SharedCommandHeader<T>(Arc<RwLock<T>>);

impl<T> SharedCommandHeader<T> {
    pub fn new(header: T) -> Self {
        Self(Arc::new(RwLock::new(header)))
    }

    pub fn with<R>(&self, inspect: impl FnOnce(&T) -> R) -> R {
        let header = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inspect(&header)
    }

    pub fn with_mut<R>(&self, update: impl FnOnce(&mut T) -> R) -> R {
        let mut header = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        update(&mut header)
    }
}

impl<T> Clone for SharedCommandHeader<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> CommandHeader for SharedCommandHeader<T>
where
    T: CommandHeader + Send + Sync,
{
    fn encode_fields(&self, fields: &mut HeaderMap) {
        self.with(|header| header.encode_fields(fields));
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        T::decode_fields(fields).map(Self::new)
    }
}

impl PartialEq for CustomHeaderValue {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_any().type_id() == other.0.as_any().type_id()
            && self.encoded_fields() == other.encoded_fields()
    }
}

impl Eq for CustomHeaderValue {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RemotingCommandType {
    RequestCommand,
    ResponseCommand,
}

impl RemotingCommandType {
    pub const fn values() -> [Self; 2] {
        [Self::RequestCommand, Self::ResponseCommand]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("REQUEST_COMMAND") => Ok(Self::RequestCommand),
            Some("RESPONSE_COMMAND") => Ok(Self::ResponseCommand),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.RemotingCommandType",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::RequestCommand => "REQUEST_COMMAND",
            Self::ResponseCommand => "RESPONSE_COMMAND",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::RequestCommand => 0,
            Self::ResponseCommand => 1,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for RemotingCommandType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestType {
    Stream,
}

impl RequestType {
    pub const fn values() -> [Self; 1] {
        [Self::Stream]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("STREAM") => Ok(Self::Stream),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.RequestType",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn from_java_byte(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Stream),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        "STREAM"
    }

    pub const fn ordinal(self) -> i32 {
        0
    }

    pub const fn get_code(self) -> i8 {
        0
    }

    pub const fn java_compare_to(self, _other: Self) -> i32 {
        0
    }
}

impl std::fmt::Display for RequestType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestSource {
    Sdk,
    ProxyForOrder,
    ProxyForBroadcast,
    ProxyForStream,
}

impl RequestSource {
    pub const SYSTEM_PROPERTY_KEY: &'static str = "rocketmq.requestSource";

    pub const fn values() -> [Self; 4] {
        [
            Self::Sdk,
            Self::ProxyForOrder,
            Self::ProxyForBroadcast,
            Self::ProxyForStream,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("SDK") => Ok(Self::Sdk),
            Some("PROXY_FOR_ORDER") => Ok(Self::ProxyForOrder),
            Some("PROXY_FOR_BROADCAST") => Ok(Self::ProxyForBroadcast),
            Some("PROXY_FOR_STREAM") => Ok(Self::ProxyForStream),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.remoting.protocol.RequestSource",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Sdk => "SDK",
            Self::ProxyForOrder => "PROXY_FOR_ORDER",
            Self::ProxyForBroadcast => "PROXY_FOR_BROADCAST",
            Self::ProxyForStream => "PROXY_FOR_STREAM",
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Sdk => 0,
            Self::ProxyForOrder => 1,
            Self::ProxyForBroadcast => 2,
            Self::ProxyForStream => 3,
        }
    }

    pub const fn get_value(self) -> i32 {
        self.ordinal() - 1
    }

    pub const fn is_valid(value: Option<i32>) -> bool {
        matches!(value, Some(-1..=2))
    }

    pub const fn parse_integer(value: Option<i32>) -> Self {
        match value {
            Some(-1) => Self::Sdk,
            Some(0) => Self::ProxyForOrder,
            Some(1) => Self::ProxyForBroadcast,
            Some(2) => Self::ProxyForStream,
            _ => Self::Sdk,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for RequestSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

/// A RocketMQ remoting command.
///
/// Java 5.5.1 inherits identity-based `Object.equals` and `Object.hashCode` for
/// this type. Rust intentionally omits structural `PartialEq` and `Eq` so two
/// commands with equal fields do not gain Java-incompatible value equality.
///
/// ```compile_fail
/// use rocketmq_classic_remoting::RemotingCommand;
/// let first = RemotingCommand::default();
/// let second = RemotingCommand::default();
/// let _ = first == second;
/// ```
#[derive(Debug, Clone)]
pub struct RemotingCommand {
    pub code: i32,
    pub language: Option<LanguageCode>,
    pub version: i32,
    pub opaque: i32,
    pub flag: i32,
    pub remark: Option<String>,
    pub ext_fields: Option<ExtFields>,
    pub body: Option<Vec<u8>>,
    pub serialization: SerializeType,
    pub suspended: bool,
    /// Attached typed header, kept out of the wire fields until encoding.
    #[doc(hidden)]
    pub custom_header: Option<CustomHeaderValue>,
    /// Lazily decoded custom header cached by `decode_command_custom_header`.
    #[doc(hidden)]
    pub cached_header: Option<CustomHeaderValue>,
    /// Timer attached to inbound command processing; not serialized on the wire.
    #[doc(hidden)]
    pub process_timer: Option<ProcessTimer>,
    /// Command completion callbacks; not serialized on the wire.
    #[doc(hidden)]
    pub callback_list: Option<Vec<CommandCallback>>,
}

impl Default for RemotingCommand {
    fn default() -> Self {
        Self {
            code: 0,
            language: Some(LanguageCode::Java),
            version: 0,
            opaque: Self::create_new_request_id(),
            flag: 0,
            remark: None,
            ext_fields: None,
            body: None,
            serialization: Self::get_serialize_type_config_in_this_server()
                .unwrap_or_else(|error| panic!("{error}")),
            suspended: false,
            custom_header: None,
            cached_header: None,
            process_timer: None,
            callback_list: None,
        }
    }
}

impl RemotingCommand {
    pub const SERIALIZE_TYPE_PROPERTY: &'static str = "rocketmq.serialize.type";
    pub const SERIALIZE_TYPE_ENV: &'static str = "ROCKETMQ_SERIALIZE_TYPE";
    pub const REMOTING_VERSION_KEY: &'static str = "rocketmq.remoting.version";

    /// Records the Java client API version installed by
    /// `MQClientAPIImpl`'s static initializer. This is hidden because it is a
    /// cross-crate compatibility hook, not a user setting.
    #[doc(hidden)]
    pub fn initialize_client_api_version(version: i32) {
        CLIENT_API_VERSION.store(version, Ordering::SeqCst);
    }

    const RESPONSE_BIT: i32 = 1;
    const ONEWAY_BIT: i32 = 1 << 1;

    pub fn create_new_request_id() -> i32 {
        REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    }

    /// Creates a request command with Java's default command fields and a process-wide opaque ID.
    pub fn create_request_command(code: i32) -> Result<Self, String> {
        Self::create_command(code, false, None)
    }

    /// Creates a request command with an attached typed header.
    pub fn create_request_command_with_header<T: CommandHeader + Clone + Send + Sync + 'static>(
        code: i32,
        custom_header: Option<T>,
    ) -> Result<Self, String> {
        let mut command = Self::create_command(code, false, None)?;
        command.write_custom_header(custom_header);
        Ok(command)
    }

    /// Creates a response command with Java's response flag, remark, and process-wide opaque ID.
    pub fn create_response_command(code: i32, remark: Option<String>) -> Result<Self, String> {
        Self::create_command(code, true, remark)
    }

    /// Creates a response command with an attached typed header.
    pub fn create_response_command_with_header<T: CommandHeader + Clone + Send + Sync + 'static>(
        code: i32,
        custom_header: Option<T>,
    ) -> Result<Self, String> {
        let mut command = Self::create_command(code, true, None)?;
        command.write_custom_header(custom_header);
        Ok(command)
    }

    /// Creates a response with a default typed header, matching Java's class-header overload.
    pub fn create_response_command_with_default_header<T>(
        code: i32,
        remark: Option<String>,
    ) -> Result<Self, String>
    where
        T: CommandHeader + Default + Clone + Send + Sync + 'static,
    {
        let mut command = Self::create_response_command(code, remark)?;
        command.write_custom_header(Some(T::default()));
        Ok(command)
    }

    /// Creates a response with a typed header supplied by a fallible
    /// constructor. Returning `None` represents Java's class-header reflection
    /// failure, which returns a null command after consuming its request ID.
    pub fn create_response_command_with_header_factory<T>(
        code: i32,
        remark: Option<String>,
        header_factory: impl FnOnce() -> Option<T>,
    ) -> Result<Option<Self>, String>
    where
        T: CommandHeader + Clone + Send + Sync + 'static,
    {
        let mut command = Self::create_response_command(code, remark)?;
        let Some(header) = header_factory() else {
            return Ok(None);
        };
        command.write_custom_header(Some(header));
        Ok(Some(command))
    }

    /// Creates Java's default system-error response with a default typed header.
    pub fn create_response_command_for_header<T>() -> Result<Self, String>
    where
        T: CommandHeader + Default + Clone + Send + Sync + 'static,
    {
        Self::create_response_command_with_default_header::<T>(
            rocketmq_classic_protocol::codes::response::SYSTEM_ERROR,
            Some("not set any response code".into()),
        )
    }

    /// Creates an error response command.
    pub fn build_error_response(code: i32, remark: Option<String>) -> Result<Self, String> {
        Self::create_response_command(code, remark)
    }

    /// Creates an error response and attaches a default typed header.
    pub fn build_error_response_with_default_header<T>(
        code: i32,
        remark: Option<String>,
    ) -> Result<Self, String>
    where
        T: CommandHeader + Default + Clone + Send + Sync + 'static,
    {
        Self::create_response_command_with_default_header::<T>(code, remark)
    }

    /// Creates an error response with a fallible typed-header constructor.
    /// Returning `None` maps to Java's `NullPointerException` after its
    /// class-header response factory returns null.
    pub fn build_error_response_with_header_factory<T>(
        code: i32,
        remark: Option<String>,
        header_factory: impl FnOnce() -> Option<T>,
    ) -> Result<Self, String>
    where
        T: CommandHeader + Clone + Send + Sync + 'static,
    {
        Self::create_response_command_with_header_factory(code, remark, header_factory)?
            .ok_or_else(|| "NullPointerException".to_owned())
    }

    /// Reads the attached typed custom header, if it has the requested type.
    pub fn read_custom_header<T: 'static>(&self) -> Option<&T> {
        self.custom_header.as_ref()?.0.as_any().downcast_ref::<T>()
    }

    /// Mutably borrows the attached typed custom header, if it has the requested type.
    pub fn read_custom_header_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.custom_header
            .as_mut()?
            .0
            .as_any_mut()
            .downcast_mut::<T>()
    }

    /// Attaches or clears a typed header. Its fields are merged into
    /// `ext_fields` when `make_custom_header_to_net` or a codec encoder runs.
    pub fn write_custom_header<T>(&mut self, custom_header: Option<T>)
    where
        T: CommandHeader + Clone + Send + Sync + 'static,
    {
        self.custom_header = custom_header.map(CustomHeaderValue::new);
    }

    /// Decodes and caches a typed custom header, replacing the current cache.
    pub fn decode_command_custom_header<T>(&mut self) -> Result<&mut T, ProtocolError>
    where
        T: CommandHeader + Default + Clone + Send + Sync + 'static,
    {
        self.decode_command_custom_header_with_cache::<T>(false)
    }

    /// Decodes a typed header and optionally reuses the cached instance.
    pub fn decode_command_custom_header_with_cache<T>(
        &mut self,
        is_cached: bool,
    ) -> Result<&mut T, ProtocolError>
    where
        T: CommandHeader + Default + Clone + Send + Sync + 'static,
    {
        if is_cached {
            if let Some(cached) = self.cached_header.as_ref() {
                if !cached.0.as_any().is::<T>() {
                    return Err(ProtocolError::InvalidField {
                        field: "cachedHeader",
                        reason: "cached custom header has a different type".into(),
                    });
                }
                return Ok(self
                    .cached_header
                    .as_mut()
                    .unwrap()
                    .0
                    .as_any_mut()
                    .downcast_mut::<T>()
                    .unwrap());
            }
        }

        let decoded = self.decode_command_custom_header_directly::<T>(true)?;
        self.cached_header = Some(CustomHeaderValue::new(decoded));
        Ok(self
            .cached_header
            .as_mut()
            .unwrap()
            .0
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap())
    }

    /// Decodes a typed custom header. The `use_fast_encode` hint has no separate
    /// Rust path unless the header type defines distinct Java fast and reflective
    /// field-decoding behavior.
    pub fn decode_command_custom_header_directly<T: CommandHeader + Default>(
        &self,
        use_fast_encode: bool,
    ) -> Result<T, ProtocolError> {
        match self.ext_fields.as_ref() {
            Some(fields) => T::decode_fields_with_mode(fields.non_null_fields(), use_fast_encode),
            None => Ok(T::default()),
        }
    }

    /// Merges non-null typed header fields into the existing extension fields.
    pub fn make_custom_header_to_net(&mut self) {
        let Some(custom_header) = &self.custom_header else {
            return;
        };
        self.ext_fields
            .get_or_insert_with(ExtFields::new)
            .extend(custom_header.encoded_fields());
    }

    /// Adds or replaces an extension field, creating the map when needed.
    pub fn add_ext_field(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.ext_fields
            .get_or_insert_with(ExtFields::new)
            .insert(key.into(), value.into());
    }

    /// Adds an extension field with Java-compatible nullable key/value input.
    pub fn add_ext_field_nullable(&mut self, key: Option<String>, value: Option<String>) {
        self.ext_fields
            .get_or_insert_with(ExtFields::new)
            .insert_nullable(key, value);
    }

    /// Adds an extension field only when the map does not already contain its key.
    ///
    /// Java's method dereferences `extFields` without creating it; the Rust
    /// method reports that absent-map case as a typed error.
    pub fn add_ext_field_if_not_exist(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), ProtocolError> {
        let fields = self
            .ext_fields
            .as_mut()
            .ok_or(ProtocolError::MissingField("extFields"))?;
        fields.put_if_absent_nullable(Some(key.into()), Some(value.into()));
        Ok(())
    }

    /// Adds an extension field only if its Java-compatible nullable key has no
    /// non-null value. A present null value is replaced, matching `HashMap`.
    pub fn add_ext_field_if_not_exist_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Result<(), ProtocolError> {
        let fields = self
            .ext_fields
            .as_mut()
            .ok_or(ProtocolError::MissingField("extFields"))?;
        fields.put_if_absent_nullable(key, value);
        Ok(())
    }

    pub(crate) fn ext_fields_for_encoding(&self) -> Option<Cow<'_, ExtFields>> {
        match (&self.ext_fields, &self.custom_header) {
            (None, None) => None,
            (Some(fields), None) => Some(Cow::Borrowed(fields)),
            (existing, Some(custom_header)) => {
                let mut fields = existing.as_ref().cloned().unwrap_or_default();
                fields.extend(custom_header.encoded_fields());
                Some(Cow::Owned(fields))
            }
        }
    }

    fn create_command(code: i32, response: bool, remark: Option<String>) -> Result<Self, String> {
        let defaults = Self::default();
        let mut command = Self {
            code,
            remark,
            ..defaults
        };
        if response {
            command.mark_response();
        }
        command.set_configured_version()?;
        Ok(command)
    }

    fn set_configured_version(&mut self) -> Result<(), String> {
        let configured = COMMAND_VERSION.load(Ordering::SeqCst);
        if configured >= 0 {
            self.version = configured;
            return Ok(());
        }

        let client_api_version = CLIENT_API_VERSION.load(Ordering::SeqCst);
        if client_api_version >= 0 {
            COMMAND_VERSION.store(client_api_version, Ordering::SeqCst);
            self.version = client_api_version;
            return Ok(());
        }

        let Ok(value) = std::env::var(Self::REMOTING_VERSION_KEY) else {
            return Ok(());
        };
        let parsed = value
            .parse::<i32>()
            .map_err(|_| format!("For input string: \"{value}\""))?;
        if parsed >= 0 {
            COMMAND_VERSION.store(parsed, Ordering::SeqCst);
        }
        self.version = parsed;
        Ok(())
    }

    pub fn get_serialize_type_config_in_this_server() -> Result<SerializeType, String> {
        SERIALIZE_TYPE_CONFIG
            .get_or_init(|| {
                let configured = std::env::var(Self::SERIALIZE_TYPE_PROPERTY)
                    .ok()
                    .or_else(|| std::env::var(Self::SERIALIZE_TYPE_ENV).ok());
                match configured {
                    Some(value) if !value.trim().is_empty() => {
                        SerializeType::value_of_name(Some(&value)).map_err(|error| {
                            format!("parser specified protocol error. protocol={value}: {error}")
                        })
                    }
                    _ => Ok(SerializeType::Json),
                }
            })
            .clone()
    }

    pub const fn get_header_length(length: i32) -> i32 {
        length & 0x00ff_ffff
    }

    pub const fn get_protocol_type(source: i32) -> Option<SerializeType> {
        let code = ((source as u32 >> 24) as u8) as i8;
        SerializeType::from_java_byte(code)
    }

    pub const fn mark_protocol_type(source: i32, serialization: SerializeType) -> i32 {
        (((serialization.code() as u32) << 24) | (source as u32 & 0x00ff_ffff)) as i32
    }

    pub fn is_response(&self) -> bool {
        self.flag & Self::RESPONSE_BIT != 0
    }

    pub fn get_type(&self) -> RemotingCommandType {
        if self.is_response() {
            RemotingCommandType::ResponseCommand
        } else {
            RemotingCommandType::RequestCommand
        }
    }

    pub fn mark_response(&mut self) {
        self.flag |= Self::RESPONSE_BIT;
    }

    pub fn is_oneway(&self) -> bool {
        self.flag & Self::ONEWAY_BIT != 0
    }

    pub fn mark_oneway(&mut self) {
        self.flag |= Self::ONEWAY_BIT;
    }

    pub fn is_suspended(&self) -> bool {
        self.suspended
    }

    pub fn set_suspended(&mut self, suspended: bool) {
        self.suspended = suspended;
    }

    pub fn get_process_timer(&self) -> Option<&ProcessTimer> {
        self.process_timer.as_ref()
    }

    pub fn get_process_timer_mut(&mut self) -> Option<&mut ProcessTimer> {
        self.process_timer.as_mut()
    }

    pub fn set_process_timer(&mut self, process_timer: Option<ProcessTimer>) {
        self.process_timer = process_timer;
    }

    pub fn get_callback_list(&self) -> Option<&[CommandCallback]> {
        self.callback_list.as_deref()
    }

    pub fn get_callback_list_mut(&mut self) -> Option<&mut Vec<CommandCallback>> {
        self.callback_list.as_mut()
    }

    pub fn set_callback_list(&mut self, callback_list: Option<Vec<CommandCallback>>) {
        self.callback_list = callback_list;
    }
}

impl std::fmt::Display for RemotingCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let language = self
            .language
            .map(|language| language.to_string())
            .unwrap_or_else(|| "null".to_owned());
        let remark = self.remark.as_deref().unwrap_or("null");
        let ext_fields = self
            .ext_fields
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_owned());
        let flag_binary = format!("{:b}", self.flag as u32);
        write!(
            formatter,
            "RemotingCommand [code={}, language={}, version={}, opaque={}, flag(B)={}, remark={}, extFields={}, serializeTypeCurrentRPC={}]",
            self.code,
            language,
            self.version,
            self.opaque,
            flag_binary,
            remark,
            ext_fields,
            self.serialization
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn pull_header_fast_and_reflective_decode_modes_match_invoked_java() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/pull-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        let api_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/query-admin-header-decode-modes.json"
        ))
        .unwrap();

        for header_name in [
            "QueryTopicsByConsumerRequestHeader",
            "QuerySubscriptionByConsumerRequestHeader",
            "QueryTopicConsumeByWhoRequestHeader",
        ] {
            assert_eq!(
                api_fixture["apiInventories"][header_name]["publicFields"],
                serde_json::json!([]),
                "{header_name} public fields"
            );
        }

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();
            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let decoded = command.decode_command_custom_header_directly::<
                    rocketmq_classic_protocol::header::PullMessageRequestHeader,
                >(use_fast_encode);
                let expected = &case[mode];
                if expected["outcome"] == "OK" {
                    let decoded = decoded.unwrap_or_else(|error| {
                        panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                    });
                    let mut encoded = HeaderMap::new();
                    CommandHeader::encode_fields(&decoded, &mut encoded);
                    let expected_fields = expected["encodedFields"]
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                        .collect::<HeaderMap>();
                    assert_eq!(encoded, expected_fields, "{case_name}/{mode}");
                } else {
                    assert!(
                        decoded.is_err(),
                        "{case_name}/{mode}: Java returned {}, Rust accepted the header",
                        expected["outcome"]
                    );
                }
            }
        }
    }

    #[test]
    fn consumer_offset_header_reflection_modes_match_invoked_java() {
        use rocketmq_classic_protocol::header::{
            QueryConsumerOffsetRequestHeader, QueryConsumerOffsetResponseHeader,
            UpdateConsumerOffsetRequestHeader,
        };

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/offset-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        let api_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/offset-boundary-header-decode-modes.json"
        ))
        .unwrap();

        for header_name in [
            "SearchOffsetRequestHeader",
            "GetMaxOffsetRequestHeader",
            "GetMinOffsetRequestHeader",
            "GetEarliestMsgStoretimeRequestHeader",
            "SearchOffsetResponseHeader",
            "GetMaxOffsetResponseHeader",
            "GetMinOffsetResponseHeader",
            "GetEarliestMsgStoretimeResponseHeader",
        ] {
            assert_eq!(
                api_fixture["apiInventories"][header_name]["publicFields"],
                serde_json::json!([]),
                "{header_name} public fields"
            );
        }

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();

            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let mut encoded = HeaderMap::new();
                match case["headerType"].as_str().unwrap() {
                    "query" => {
                        let header = command
                            .decode_command_custom_header_directly::<QueryConsumerOffsetRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                            });
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "update" => {
                        let header = command
                            .decode_command_custom_header_directly::<UpdateConsumerOffsetRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                            });
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "response" => {
                        let header = command
                            .decode_command_custom_header_directly::<QueryConsumerOffsetResponseHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                            });
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    other => panic!("{case_name}: unknown header type {other}"),
                }

                let expected = case[mode]["encodedFields"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                    .collect::<HeaderMap>();
                assert_eq!(encoded, expected, "{case_name}/{mode}");
            }
        }
    }

    #[test]
    fn create_topic_header_reflection_modes_match_invoked_java() {
        use rocketmq_classic_protocol::header::{
            CommandHeader, CreateTopicRequestHeader, HeaderMap,
        };

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/create-topic-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();

            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let decoded = command
                    .decode_command_custom_header_directly::<CreateTopicRequestHeader>(
                        use_fast_encode,
                    );
                let expected = &case[mode];
                if expected["outcome"] == "OK" {
                    let header = decoded.unwrap_or_else(|error| {
                        panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                    });
                    let mut encoded = HeaderMap::new();
                    CommandHeader::encode_fields(&header, &mut encoded);
                    let expected_fields = expected["encodedFields"]
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                        .collect::<HeaderMap>();
                    assert_eq!(encoded, expected_fields, "{case_name}/{mode}");
                } else {
                    assert!(
                        decoded.is_err(),
                        "{case_name}/{mode}: Java returned {}, Rust accepted the header",
                        expected["outcome"]
                    );
                }
            }
        }
    }

    #[test]
    fn query_admin_header_reflection_modes_match_invoked_java_551() {
        use rocketmq_classic_protocol::header::{
            AddWritePermOfBrokerRequestHeader, AddWritePermOfBrokerResponseHeader,
            CheckRocksdbCqWriteProgressRequestHeader, CommandHeader, DeleteKvConfigRequestHeader,
            ExportRocksDBConfigToJsonRequestHeader, GetAllSubscriptionGroupRequestHeader,
            GetAllSubscriptionGroupResponseHeader, GetTopicStatsInfoRequestHeader, HeaderMap,
            QueryConsumeQueueRequestHeader, QueryConsumeTimeSpanRequestHeader,
            QueryMessageRequestHeader, QueryMessageResponseHeader,
            QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
            QueryTopicsByConsumerRequestHeader, ResetMasterFlushOffsetHeader,
            ViewMessageRequestHeader, WipeWritePermOfBrokerRequestHeader,
            WipeWritePermOfBrokerResponseHeader,
        };

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/query-admin-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for header_name in [
            "QueryTopicsByConsumerRequestHeader",
            "QuerySubscriptionByConsumerRequestHeader",
            "QueryTopicConsumeByWhoRequestHeader",
            "QueryConsumeTimeSpanRequestHeader",
            "GetTopicStatsInfoRequestHeader",
            "DeleteTopicRequestHeader",
            "DeleteTopicFromNamesrvRequestHeader",
            "DeleteSubscriptionGroupRequestHeader",
            "DeleteKVConfigRequestHeader",
            "QueryMessageResponseHeader",
        ] {
            assert_eq!(
                fixture["apiInventories"][header_name]["publicFields"],
                serde_json::json!([]),
                "{header_name} public fields"
            );
        }

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();

            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let expected = &case[mode];
                assert_eq!(expected["outcome"], "OK", "{case_name}/{mode}");

                let (decoded_fields, encoded_fields) = match case["headerType"].as_str().unwrap() {
                    "query-request" => {
                        let header = command
                            .decode_command_custom_header_directly::<QueryMessageRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "key": header.get_key(),
                                "maxNum": header.get_max_num(),
                                "beginTimestamp": header.get_begin_timestamp(),
                                "endTimestamp": header.get_end_timestamp(),
                                "indexType": header.get_index_type(),
                                "lastKey": header.get_last_key(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                                "lo": header.get_lo(),
                            }),
                            encoded,
                        )
                    }
                    "query-consume-time-span" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                QueryConsumeTimeSpanRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "group": header.get_group(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                                "lo": header.get_lo(),
                            }),
                            encoded,
                        )
                    }
                    "query-consume-queue" => {
                        let header = command
                            .decode_command_custom_header_directly::<QueryConsumeQueueRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "queueId": header.get_queue_id(),
                                "index": header.get_index(),
                                "count": header.get_count(),
                                "consumerGroup": header.get_consumer_group(),
                                "bname": header.get_broker_name(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "lo": header.get_lo(),
                                "oway": header.get_oneway(),
                            }),
                            encoded,
                        )
                    }
                    "get-all-subscription-group-request" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                GetAllSubscriptionGroupRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "groupSeq": header.get_group_seq(),
                                "dataVersion": header.get_data_version(),
                                "maxGroupNum": header.get_max_group_num(),
                            }),
                            encoded,
                        )
                    }
                    "get-all-subscription-group-response" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                GetAllSubscriptionGroupResponseHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "totalGroupNum": header.get_total_group_num(),
                            }),
                            encoded,
                        )
                    }
                    "delete-kv-config" => {
                        let header = command
                            .decode_command_custom_header_directly::<DeleteKvConfigRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "namespace": header.get_namespace(),
                                "key": header.get_key(),
                            }),
                            encoded,
                        )
                    }
                    "query-response" => {
                        let header = command
                            .decode_command_custom_header_directly::<QueryMessageResponseHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "indexLastUpdateTimestamp":
                                    header.get_index_last_update_timestamp(),
                                "indexLastUpdatePhyoffset":
                                    header.get_index_last_update_phyoffset(),
                            }),
                            encoded,
                        )
                    }
                    "view-request" => {
                        let header = command
                            .decode_command_custom_header_directly::<ViewMessageRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({"topic": header.get_topic(), "offset": header.get_offset()}),
                            encoded,
                        )
                    }
                    "query-topics" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                QueryTopicsByConsumerRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "group": header.get_group(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                            }),
                            encoded,
                        )
                    }
                    "query-subscription" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                QuerySubscriptionByConsumerRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "group": header.get_group(),
                                "topic": header.get_topic(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                                "lo": header.get_lo(),
                            }),
                            encoded,
                        )
                    }
                    "query-topic-consumers" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                QueryTopicConsumeByWhoRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                                "lo": header.get_lo(),
                            }),
                            encoded,
                        )
                    }
                    "topic-stats-info" => {
                        let header = command
                            .decode_command_custom_header_directly::<GetTopicStatsInfoRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                                "lo": header.get_lo(),
                            }),
                            encoded,
                        )
                    }
                    "add-write-perm-request" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                AddWritePermOfBrokerRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({"brokerName": header.get_broker_name()}),
                            encoded,
                        )
                    }
                    "add-write-perm-response" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                AddWritePermOfBrokerResponseHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({"addTopicCount": header.get_add_topic_count()}),
                            encoded,
                        )
                    }
                    "wipe-write-perm-request" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                WipeWritePermOfBrokerRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({"brokerName": header.get_broker_name()}),
                            encoded,
                        )
                    }
                    "wipe-write-perm-response" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                WipeWritePermOfBrokerResponseHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({"wipeTopicCount": header.get_wipe_topic_count()}),
                            encoded,
                        )
                    }
                    "check-rocksdb-cq-write-progress" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                CheckRocksdbCqWriteProgressRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "checkStoreTime": header.get_check_store_time(),
                            }),
                            encoded,
                        )
                    }
                    "export-rocksdb-config" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                ExportRocksDBConfigToJsonRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({"configType": header.get_config_type()}),
                            encoded,
                        )
                    }
                    "reset-master-flush-offset" => {
                        let header = command
                            .decode_command_custom_header_directly::<ResetMasterFlushOffsetHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        let mut encoded = HeaderMap::new();
                        CommandHeader::encode_fields(&header, &mut encoded);
                        (
                            serde_json::json!({
                                "masterFlushOffset": header.get_master_flush_offset(),
                            }),
                            encoded,
                        )
                    }
                    other => panic!("{case_name}: unknown header type {other}"),
                };

                assert_eq!(
                    expected["decodedFields"], decoded_fields,
                    "{case_name}/{mode} decoded values"
                );
                let expected_encoded = expected["encodedFields"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                    .collect::<HeaderMap>();
                assert_eq!(encoded_fields, expected_encoded, "{case_name}/{mode}");

                if case["headerType"] == "view-request" {
                    assert_eq!(expected["equalsDeclaredBy"], "java.lang.Object");
                    assert_eq!(expected["hashCodeDeclaredBy"], "java.lang.Object");
                    assert_eq!(expected["distinctDefaultObjectsEquals"], false);
                }
            }
        }
    }

    #[test]
    fn end_transaction_header_decode_modes_match_invoked_java_551() {
        use rocketmq_classic_protocol::header::{
            CommandHeader, EndTransactionRequestHeader, HeaderMap, ProtocolError,
        };

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/end-transaction-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();

            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let decoded = command
                    .decode_command_custom_header_directly::<EndTransactionRequestHeader>(
                        use_fast_encode,
                    );
                let expected = &case[mode];
                if expected["outcome"] == "OK" {
                    let header = decoded.unwrap_or_else(|error| {
                        panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                    });
                    let mut encoded = HeaderMap::new();
                    CommandHeader::encode_fields(&header, &mut encoded);
                    let expected_fields = expected["encodedFields"]
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                        .collect::<HeaderMap>();
                    assert_eq!(encoded, expected_fields, "{case_name}/{mode}");
                } else {
                    assert!(
                        matches!(
                            decoded,
                            Err(ProtocolError::InvalidField {
                                field: "commitOrRollback",
                                ..
                            })
                        ),
                        "{case_name}/{mode}: Java returned {}, Rust did not reject the commit state",
                        expected["outcome"]
                    );
                }
            }
        }
    }

    #[test]
    fn offset_boundary_response_header_reflection_modes_match_invoked_java() {
        use rocketmq_classic_protocol::header::{
            GetEarliestMsgStoretimeResponseHeader, GetMaxOffsetResponseHeader,
            GetMinOffsetResponseHeader, SearchOffsetResponseHeader,
        };

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/offset-boundary-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            if case["headerType"].as_str().unwrap().starts_with("request-") {
                continue;
            }
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();
            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let expected = &case[mode];
                let expected_value = expected["value"].as_i64();
                let mut encoded = HeaderMap::new();
                match case["headerType"].as_str().unwrap() {
                    "search" => {
                        let header = command
                            .decode_command_custom_header_directly::<SearchOffsetResponseHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        assert_eq!(header.get_offset(), expected_value, "{case_name}/{mode}");
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "max" => {
                        let header = command
                            .decode_command_custom_header_directly::<GetMaxOffsetResponseHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        assert_eq!(header.get_offset(), expected_value, "{case_name}/{mode}");
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "min" => {
                        let header = command
                            .decode_command_custom_header_directly::<GetMinOffsetResponseHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        assert_eq!(header.get_offset(), expected_value, "{case_name}/{mode}");
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "earliest" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                GetEarliestMsgStoretimeResponseHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                            });
                        assert_eq!(header.get_timestamp(), expected_value, "{case_name}/{mode}");
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    other => panic!("{case_name}: unknown header type {other}"),
                }

                let expected_fields = expected["encodedFields"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                    .collect::<HeaderMap>();
                assert_eq!(encoded, expected_fields, "{case_name}/{mode}");
                let expected_owner_outcome = if expected_value.is_some() {
                    "OK"
                } else {
                    "NullPointerException"
                };
                assert_eq!(
                    expected["ownerOutcome"], expected_owner_outcome,
                    "{case_name}/{mode}"
                );
                if let Some(value) = expected_value {
                    assert_eq!(
                        expected["ownerValue"].as_i64(),
                        Some(value),
                        "{case_name}/{mode}"
                    );
                }
            }
        }
    }

    #[test]
    fn change_invisible_time_response_header_matches_invoked_java_api() {
        use rocketmq_classic_protocol::header::ChangeInvisibleTimeResponseHeader;

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/change-invisible-time-response-header.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(
            fixture["headerType"],
            "org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader"
        );
        assert!(fixture["publicConstructor"].as_bool().unwrap());
        assert_eq!(
            fixture["declaredPublicMethods"],
            serde_json::json!([
                "checkFields():void",
                "getInvisibleTime():long",
                "getPopTime():long",
                "getReviveQid():int",
                "setInvisibleTime(long):void",
                "setPopTime(long):void",
                "setReviveQid(int):void"
            ])
        );
        assert_eq!(
            fixture["identity"]["equalsDeclaringClass"],
            "java.lang.Object"
        );
        assert_eq!(
            fixture["identity"]["hashCodeDeclaringClass"],
            "java.lang.Object"
        );
        assert!(fixture["identity"]["selfEquals"].as_bool().unwrap());
        assert!(
            !fixture["identity"]["distinctDefaultEquals"]
                .as_bool()
                .unwrap()
        );

        let defaults = &fixture["defaults"];
        let default_header = ChangeInvisibleTimeResponseHeader::default();
        assert_eq!(
            default_header.get_pop_time(),
            defaults["popTime"].as_i64().unwrap()
        );
        assert_eq!(
            default_header.get_invisible_time(),
            defaults["invisibleTime"].as_i64().unwrap()
        );
        assert_eq!(
            default_header.get_revive_qid(),
            defaults["reviveQid"].as_i64().unwrap() as i32
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .map(|fields| {
                    fields
                        .iter()
                        .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                        .collect::<HeaderMap>()
                })
                .unwrap_or_default();
            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: (!case["fields"].is_null()).then(|| fields.clone().into()),
                    ..Default::default()
                };
                let header = command
                    .decode_command_custom_header_directly::<ChangeInvisibleTimeResponseHeader>(
                        use_fast_encode,
                    )
                    .unwrap_or_else(|error| {
                        panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                    });
                let expected = &case[mode];
                assert_eq!(
                    header.get_pop_time(),
                    expected["popTime"].as_i64().unwrap(),
                    "{case_name}/{mode}"
                );
                assert_eq!(
                    header.get_invisible_time(),
                    expected["invisibleTime"].as_i64().unwrap(),
                    "{case_name}/{mode}"
                );
                assert_eq!(
                    header.get_revive_qid(),
                    expected["reviveQid"].as_i64().unwrap() as i32,
                    "{case_name}/{mode}"
                );
            }
        }

        let accessors = &fixture["accessors"];
        let encoded_fields = fixture["encodedFields"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
            .collect::<HeaderMap>();
        assert_eq!(fixture["publicFieldCount"], 0);
        let mut assigned = ChangeInvisibleTimeResponseHeader::default();
        assigned.set_pop_time(accessors["popTime"].as_i64().unwrap());
        assigned.set_invisible_time(accessors["invisibleTime"].as_i64().unwrap());
        assigned.set_revive_qid(accessors["reviveQid"].as_i64().unwrap() as i32);
        let mut actual_fields = HeaderMap::new();
        CommandHeader::encode_fields(&assigned, &mut actual_fields);
        assert_eq!(actual_fields, encoded_fields);
    }

    #[test]
    fn offset_boundary_request_header_reflection_modes_match_invoked_java() {
        use rocketmq_classic_protocol::header::{
            GetEarliestMsgStoretimeRequestHeader, GetMaxOffsetRequestHeader,
            GetMinOffsetRequestHeader, SearchOffsetRequestHeader,
        };

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/offset-boundary-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            if !case["headerType"].as_str().unwrap().starts_with("request-") {
                continue;
            }
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();
            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let expected = &case[mode];
                let mut encoded = HeaderMap::new();
                match case["headerType"].as_str().unwrap() {
                    "request-search" => {
                        let header = command
                            .decode_command_custom_header_directly::<SearchOffsetRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        assert_eq!(
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "liteTopic": header.get_lite_topic(),
                                "queueId": header.get_queue_id(),
                                "timestamp": header.get_timestamp(),
                                "boundaryType": fields.contains_key("boundaryType")
                                    .then(|| header.get_boundary_type().name()),
                                "lo": header.get_lo(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                            }),
                            expected["decodedFields"],
                            "{case_name}/{mode}"
                        );
                        assert_eq!(
                            expected["getterBoundaryType"],
                            header.get_boundary_type().name(),
                            "{case_name}/{mode}"
                        );
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "request-max" => {
                        let header = command
                            .decode_command_custom_header_directly::<GetMaxOffsetRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        assert_eq!(
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "queueId": header.get_queue_id(),
                                "committed": header.is_committed(),
                                "lo": header.get_lo(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                            }),
                            expected["decodedFields"],
                            "{case_name}/{mode}"
                        );
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "request-min" => {
                        let header = command
                            .decode_command_custom_header_directly::<GetMinOffsetRequestHeader>(
                                use_fast_encode,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{case_name}/{mode}: Java returned OK, Rust returned {error}"
                                )
                            });
                        assert_eq!(
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "queueId": header.get_queue_id(),
                                "lo": header.get_lo(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                            }),
                            expected["decodedFields"],
                            "{case_name}/{mode}"
                        );
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    "request-earliest" => {
                        let header = command
                            .decode_command_custom_header_directly::<
                                GetEarliestMsgStoretimeRequestHeader,
                            >(use_fast_encode)
                            .unwrap_or_else(|error| {
                                panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                            });
                        assert_eq!(
                            serde_json::json!({
                                "topic": header.get_topic(),
                                "queueId": header.get_queue_id(),
                                "lo": header.get_lo(),
                                "ns": header.get_namespace(),
                                "nsd": header.get_namespaced(),
                                "bname": header.get_broker_name(),
                                "oway": header.get_oneway(),
                            }),
                            expected["decodedFields"],
                            "{case_name}/{mode}"
                        );
                        CommandHeader::encode_fields(&header, &mut encoded);
                    }
                    other => panic!("{case_name}: unexpected request header type {other}"),
                }

                let expected_fields = expected["encodedFields"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                    .collect::<HeaderMap>();
                assert_eq!(encoded, expected_fields, "{case_name}/{mode}");
            }
        }
    }

    #[test]
    fn pull_response_header_fast_and_reflective_decode_modes_match_invoked_java() {
        use rocketmq_classic_protocol::header::PullMessageResponseHeader;

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/pull-response-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();
            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let decoded = command
                    .decode_command_custom_header_directly::<PullMessageResponseHeader>(
                        use_fast_encode,
                    );
                let expected = &case[mode];
                if expected["outcome"] == "OK" {
                    let decoded = decoded.unwrap_or_else(|error| {
                        panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                    });
                    let expected_header = &expected["header"];
                    assert_eq!(
                        decoded.get_suggest_which_broker_id(),
                        expected_header["suggestWhichBrokerId"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_next_begin_offset(),
                        expected_header["nextBeginOffset"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_min_offset(),
                        expected_header["minOffset"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_max_offset(),
                        expected_header["maxOffset"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_offset_delta(),
                        expected_header["offsetDelta"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_topic_sys_flag().map(i64::from),
                        expected_header["topicSysFlag"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_group_sys_flag().map(i64::from),
                        expected_header["groupSysFlag"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_forbidden_type().map(i64::from),
                        expected_header["forbiddenType"].as_i64()
                    );
                } else {
                    assert!(
                        decoded.is_err(),
                        "{case_name}/{mode}: Java returned {}, Rust decoded successfully",
                        expected["outcome"]
                    );
                }
            }
        }
    }

    #[test]
    fn send_message_response_header_decode_modes_match_invoked_java_551() {
        use rocketmq_classic_protocol::header::SendMessageResponseHeader;

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/send-message-response-header-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();
            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let decoded = command
                    .decode_command_custom_header_directly::<SendMessageResponseHeader>(
                        use_fast_encode,
                    );
                let expected = &case[mode];
                if expected["outcome"] == "OK" {
                    let decoded = decoded.unwrap_or_else(|error| {
                        panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                    });
                    let expected_header = &expected["header"];
                    assert_eq!(decoded.get_msg_id(), expected_header["msgId"].as_str());
                    assert_eq!(
                        decoded.get_queue_id().map(i64::from),
                        expected_header["queueId"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_queue_offset(),
                        expected_header["queueOffset"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_transaction_id(),
                        expected_header["transactionId"].as_str()
                    );
                    assert_eq!(
                        decoded.get_batch_uniq_id(),
                        expected_header["batchUniqId"].as_str()
                    );
                    assert_eq!(
                        decoded.get_recall_handle(),
                        expected_header["recallHandle"].as_str()
                    );
                } else {
                    assert!(
                        decoded.is_err(),
                        "{case_name}/{mode}: Java returned {}, Rust decoded successfully",
                        expected["outcome"]
                    );
                }
            }
        }
    }

    #[test]
    fn send_message_request_v2_decode_modes_match_invoked_java_551() {
        use rocketmq_classic_protocol::header::SendMessageRequestHeaderV2;

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/send-message-request-v2-decode-modes.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );

        for (case_name, case) in fixture["cases"].as_object().unwrap() {
            let fields = case["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
                .collect::<HeaderMap>();
            for (mode, use_fast_encode) in [("fast", true), ("reflection", false)] {
                let command = RemotingCommand {
                    ext_fields: Some(fields.clone().into()),
                    ..Default::default()
                };
                let decoded = command
                    .decode_command_custom_header_directly::<SendMessageRequestHeaderV2>(
                        use_fast_encode,
                    );
                let expected = &case[mode];
                if expected["outcome"] == "OK" {
                    let decoded = decoded.unwrap_or_else(|error| {
                        panic!("{case_name}/{mode}: Java returned OK, Rust returned {error}")
                    });
                    let expected_header = &expected["header"];
                    assert_eq!(decoded.get_a(), expected_header["a"].as_str());
                    assert_eq!(decoded.get_b(), expected_header["b"].as_str());
                    assert_eq!(decoded.get_c(), expected_header["c"].as_str());
                    assert_eq!(
                        decoded.get_d().map(i64::from),
                        expected_header["d"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_e().map(i64::from),
                        expected_header["e"].as_i64()
                    );
                    assert_eq!(
                        decoded.get_f().map(i64::from),
                        expected_header["f"].as_i64()
                    );
                    assert_eq!(decoded.get_g(), expected_header["g"].as_i64());
                    assert_eq!(
                        decoded.get_h().map(i64::from),
                        expected_header["h"].as_i64()
                    );
                    assert_eq!(decoded.get_i(), expected_header["i"].as_str());
                    assert_eq!(
                        decoded.get_j().map(i64::from),
                        expected_header["j"].as_i64()
                    );
                    assert_eq!(decoded.is_k(), expected_header["k"].as_bool());
                    assert_eq!(
                        decoded.get_l().map(i64::from),
                        expected_header["l"].as_i64()
                    );
                    assert_eq!(decoded.is_m(), expected_header["m"].as_bool());
                    let mut encoded = HeaderMap::new();
                    CommandHeader::encode_fields(&decoded, &mut encoded);
                    assert_eq!(
                        encoded.get("n").map(String::as_str),
                        expected_header["n"].as_str()
                    );
                    assert_eq!(decoded.get_namespace(), expected_header["ns"].as_str());
                    assert_eq!(decoded.get_namespaced(), expected_header["nsd"].as_bool());
                    assert_eq!(decoded.get_broker_name(), expected_header["bname"].as_str());
                    assert_eq!(decoded.get_oneway(), expected_header["oway"].as_bool());
                    assert_eq!(decoded.get_lo(), expected_header["lo"].as_bool());
                } else {
                    assert!(
                        decoded.is_err(),
                        "{case_name}/{mode}: Java returned {}, Rust decoded successfully",
                        expected["outcome"]
                    );
                }
            }
        }
    }

    #[test]
    fn send_message_request_v2_conversion_matches_invoked_java_551() {
        use rocketmq_classic_protocol::header::{
            SendMessageRequestHeaderV1, SendMessageRequestHeaderV2,
        };

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/send-message-request-v2-decode-modes.json"
        ))
        .unwrap();
        let v1 = {
            let mut __phase18_header_0 = SendMessageRequestHeaderV1::default();
            __phase18_header_0.set_producer_group(Some("producer".into()));
            __phase18_header_0.set_topic("orders".into());
            __phase18_header_0.set_default_topic("TBW102".into());
            __phase18_header_0.set_default_topic_queue_nums(4);
            __phase18_header_0.set_queue_id(2);
            __phase18_header_0.set_sys_flag(1);
            __phase18_header_0.set_born_timestamp(1_712_345_678_901);
            __phase18_header_0.set_flag(7);
            __phase18_header_0.set_properties(Some("TAGS\u{1}blue".into()));
            __phase18_header_0.set_reconsume_times(Some(0));
            __phase18_header_0.set_unit_mode(Some(false));
            __phase18_header_0.set_max_reconsume_times(Some(16));
            __phase18_header_0.set_batch(Some(false));
            __phase18_header_0.set_broker_name(Some("short-broker".into()));
            __phase18_header_0.set_namespace(Some("tenant".into()));
            __phase18_header_0.set_namespaced(Some(true));
            __phase18_header_0.set_lo(Some(true));
            __phase18_header_0.set_oneway(Some(false));
            __phase18_header_0
        };
        let header = SendMessageRequestHeaderV2::from_v1(&v1);
        let mut fields = HeaderMap::new();
        rocketmq_classic_protocol::header::CommandHeader::encode_fields(&header, &mut fields);
        assert_eq!(
            serde_json::to_value(&fields).unwrap(),
            fixture["conversion"]["encodedFields"]
        );
        assert_eq!(fields.get("n").map(String::as_str), Some("short-broker"));
        assert_eq!(header.get_broker_name(), None);
        assert_eq!(header.get_namespace(), None);
        assert_eq!(header.get_namespaced(), None);
        assert_eq!(header.get_lo(), None);
        assert_eq!(header.get_oneway(), None);
    }

    #[test]
    fn remoting_enum_methods_and_command_type_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let cases = &fixture["remotingEnumMethodCases"];

        macro_rules! check_enum_cases {
            ($enum_type:ty, $cases:expr) => {{
                let enum_cases = $cases.as_array().unwrap();
                let values = <$enum_type>::values();
                let expected_names = enum_cases
                    .iter()
                    .find(|case| case["case"] == "constant")
                    .unwrap()["values"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|name| name.as_str().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(
                    values.iter().map(|value| value.name()).collect::<Vec<_>>(),
                    expected_names
                );
                let mut mutated_values = <$enum_type>::values();
                mutated_values[0] = values[values.len() - 1];
                assert_eq!(<$enum_type>::values()[0], values[0]);

                for case in enum_cases {
                    match case["case"].as_str().unwrap() {
                        "constant" => {
                            let name = case["name"].as_str().unwrap();
                            let value = <$enum_type>::value_of_name(Some(name)).unwrap();
                            let ordinal = case["ordinal"].as_i64().unwrap() as i32;
                            assert_eq!(value.name(), name, "{case}");
                            assert_eq!(value.to_string(), case["text"].as_str().unwrap(), "{case}");
                            assert_eq!(value.ordinal(), ordinal, "{case}");
                            assert_eq!(value, values[ordinal as usize], "{case}");
                            assert_eq!(case["valueOfName"], name, "{case}");
                            assert_eq!(case["valuesDefensiveCopy"], true, "{case}");
                            let next = values[(ordinal as usize + 1) % values.len()];
                            assert_eq!(
                                value.java_compare_to(next),
                                case["compareNext"].as_i64().unwrap() as i32,
                                "{case}"
                            );
                        }
                        "lookup" => {
                            let result = <$enum_type>::value_of_name(case["input"].as_str());
                            match result {
                                Ok(value) => {
                                    assert_eq!(case["outcome"], "OK", "{case}");
                                    assert_eq!(case["decoded"], value.name(), "{case}");
                                }
                                Err(error) => {
                                    let expected = if case["input"].is_null() {
                                        "NullPointerException"
                                    } else {
                                        "IllegalArgumentException"
                                    };
                                    assert_eq!(case["outcome"], expected, "{case}");
                                    assert_eq!(case["message"], error.to_string(), "{case}");
                                }
                            }
                        }
                        other => panic!("unknown Java remoting enum case: {other}"),
                    }
                }
            }};
        }

        check_enum_cases!(RemotingCommandType, &cases["RemotingCommandType"]);
        check_enum_cases!(RequestType, &cases["RequestType"]);
        let request_source = &cases["RequestSource"];
        check_enum_cases!(RequestSource, &request_source["enum"]);
        assert_eq!(
            RequestSource::SYSTEM_PROPERTY_KEY,
            request_source["systemPropertyKey"].as_str().unwrap()
        );
        for case in request_source["values"].as_array().unwrap() {
            let source = RequestSource::value_of_name(case["name"].as_str()).unwrap();
            assert_eq!(source.get_value(), case["value"].as_i64().unwrap() as i32);
        }
        for case in request_source["integers"].as_array().unwrap() {
            let value = case["input"].as_i64().map(|input| input as i32);
            assert_eq!(
                RequestSource::is_valid(value),
                case["valid"].as_bool().unwrap()
            );
            assert_eq!(
                RequestSource::parse_integer(value).name(),
                case["parsed"].as_str().unwrap()
            );
        }

        for case in cases["requestTypeBytes"].as_array().unwrap() {
            let input = case["input"].as_i64().unwrap() as i8;
            let decoded = RequestType::from_java_byte(input);
            assert_eq!(case["outcome"], "OK", "{case}");
            assert_eq!(
                case["decoded"].as_str(),
                decoded.map(RequestType::name),
                "{case}"
            );
            assert_eq!(
                case["getCode"].as_i64().map(|code| code as i8),
                decoded.map(RequestType::get_code),
                "{case}"
            );
        }

        let request = RemotingCommand::default();
        let mut response = RemotingCommand::default();
        response.mark_response();
        let mut marked_request = RemotingCommand::default();
        marked_request.mark_response();
        let commands = [request, response, marked_request];
        for (case, command) in cases["commandGetType"]
            .as_array()
            .unwrap()
            .iter()
            .zip(commands)
        {
            assert_eq!(command.get_type().name(), case["type"].as_str().unwrap());
            assert_eq!(command.is_response(), case["isResponse"].as_bool().unwrap());
        }

        let default = RemotingCommand::default();
        let mut oneway = RemotingCommand::default();
        oneway.mark_oneway();
        let mut oneway_response = RemotingCommand::default();
        oneway_response.mark_oneway();
        oneway_response.mark_response();
        let mut repeated_marks = oneway_response.clone();
        repeated_marks.mark_oneway();
        repeated_marks.mark_response();
        let mut preserves_unknown_bits = RemotingCommand {
            flag: 1 << 8,
            ..RemotingCommand::default()
        };
        preserves_unknown_bits.mark_oneway();
        preserves_unknown_bits.mark_response();
        let commands = [
            default,
            oneway,
            oneway_response,
            repeated_marks,
            preserves_unknown_bits,
        ];
        let flag_cases = cases["commandFlags"].as_array().unwrap();
        assert_eq!(flag_cases.len(), commands.len());
        for (case, command) in flag_cases.iter().zip(commands) {
            assert_eq!(
                command.flag,
                case["flag"].as_i64().unwrap() as i32,
                "{case}"
            );
            assert_eq!(
                command.get_type().name(),
                case["type"].as_str().unwrap(),
                "{case}"
            );
            assert_eq!(
                command.is_oneway(),
                case["isOneway"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                command.is_response(),
                case["isResponse"].as_bool().unwrap(),
                "{case}"
            );
        }

        let suspended_cases = cases["commandSuspended"].as_array().unwrap();
        let mut command = RemotingCommand::default();
        assert_eq!(suspended_cases.len(), 3);
        for (case, next) in suspended_cases.iter().zip([None, Some(true), Some(false)]) {
            if let Some(value) = next {
                command.set_suspended(value);
            }
            assert_eq!(
                command.is_suspended(),
                case["suspended"].as_bool().unwrap(),
                "{case}"
            );
        }

        command.set_suspended(true);
        let encoded = crate::codec::json_header::encode(&command).unwrap();
        assert!(
            !String::from_utf8(encoded.clone())
                .unwrap()
                .contains("suspended")
        );
        assert!(
            !crate::codec::json_header::decode(&encoded)
                .unwrap()
                .is_suspended()
        );

        let helpers = &cases["commandProtocolHelpers"];
        assert_eq!(
            RemotingCommand::SERIALIZE_TYPE_PROPERTY,
            helpers["serializeTypeProperty"].as_str().unwrap()
        );
        assert_eq!(
            RemotingCommand::SERIALIZE_TYPE_ENV,
            helpers["serializeTypeEnv"].as_str().unwrap()
        );
        assert_eq!(
            RemotingCommand::REMOTING_VERSION_KEY,
            helpers["remotingVersionKey"].as_str().unwrap()
        );
        assert_eq!(
            RemotingCommand::get_serialize_type_config_in_this_server()
                .unwrap()
                .name(),
            helpers["serializeTypeConfig"].as_str().unwrap()
        );
        for case in helpers["headerLengths"].as_array().unwrap() {
            assert_eq!(
                RemotingCommand::get_header_length(case["input"].as_i64().unwrap() as i32),
                case["result"].as_i64().unwrap() as i32,
                "{case}"
            );
        }
        for case in helpers["protocolTypes"].as_array().unwrap() {
            let source = case["source"].as_i64().unwrap() as i32;
            let serialization = match case["type"].as_str().unwrap() {
                "JSON" => SerializeType::Json,
                "ROCKETMQ" => SerializeType::RocketMq,
                other => panic!("unexpected Java serialization type {other}"),
            };
            let marked = RemotingCommand::mark_protocol_type(source, serialization);
            assert_eq!(marked, case["marked"].as_i64().unwrap() as i32, "{case}");
            assert_eq!(
                RemotingCommand::get_protocol_type(marked).map(SerializeType::name),
                case["decoded"].as_str(),
                "{case}"
            );
        }
        for case in helpers["protocolTypeLookups"].as_array().unwrap() {
            assert_eq!(
                RemotingCommand::get_protocol_type(case["source"].as_i64().unwrap() as i32)
                    .map(SerializeType::name),
                case["decoded"].as_str(),
                "{case}"
            );
        }
        let first_id = RemotingCommand::create_new_request_id();
        let second_id = RemotingCommand::create_new_request_id();
        assert_eq!(
            second_id.wrapping_sub(first_id),
            helpers["requestIdDelta"].as_i64().unwrap() as i32
        );

        for case in cases["commandToString"].as_array().unwrap() {
            let fields = &case["fields"];
            let language = fields["language"]
                .as_str()
                .map(|name| LanguageCode::value_of_name(Some(name)).unwrap());
            let serialization =
                SerializeType::value_of_name(fields["serialization"].as_str()).unwrap();
            let ext_fields = fields["extFields"].as_object().map(|object| {
                object
                    .iter()
                    .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                    .collect()
            });
            let command = RemotingCommand {
                code: fields["code"].as_i64().unwrap() as i32,
                language,
                version: fields["version"].as_i64().unwrap() as i32,
                opaque: fields["opaque"].as_i64().unwrap() as i32,
                flag: fields["flag"].as_i64().unwrap() as i32,
                remark: fields["remark"].as_str().map(str::to_owned),
                ext_fields,
                serialization,
                ..RemotingCommand::default()
            };
            let java_result = case["result"].as_str().unwrap();
            let expected_result = if case["name"] == "multipleExtFields" {
                // Java HashMap does not guarantee iteration order; validate its entries as a map
                // and normalize only that segment before comparing the diagnostic string.
                let ext_fields_start = java_result.find("extFields=").unwrap() + "extFields=".len();
                let ext_fields_end = java_result[ext_fields_start..]
                    .find(", serializeTypeCurrentRPC=")
                    .unwrap()
                    + ext_fields_start;
                let java_ext_fields = &java_result[ext_fields_start..ext_fields_end];
                let parsed_java_ext_fields = parse_java_ext_fields_display(java_ext_fields);
                let command_ext_fields = command
                    .ext_fields
                    .as_ref()
                    .map(|fields| fields.non_null_fields().clone());
                assert_eq!(parsed_java_ext_fields, command_ext_fields, "{case}");
                normalize_ext_fields_display(
                    java_result,
                    command
                        .ext_fields
                        .as_ref()
                        .map(|fields| fields.non_null_fields()),
                )
            } else {
                java_result.to_owned()
            };
            assert_eq!(command.to_string(), expected_result, "{case}");
        }
    }

    fn parse_java_ext_fields_display(display: &str) -> Option<HeaderMap> {
        if display == "null" {
            return None;
        }
        let entries = display.strip_prefix('{')?.strip_suffix('}')?;
        let mut fields = HeaderMap::new();
        if entries.is_empty() {
            return Some(fields);
        }
        for entry in entries.split(", ") {
            let (key, value) = entry.split_once('=')?;
            fields.insert(key.to_owned(), value.to_owned());
        }
        Some(fields)
    }

    fn normalize_ext_fields_display(display: &str, fields: Option<&HeaderMap>) -> String {
        let start = display.find("extFields=").unwrap() + "extFields=".len();
        let end = display[start..].find(", serializeTypeCurrentRPC=").unwrap() + start;
        let ext_fields = fields.map_or_else(
            || "null".to_owned(),
            |fields| {
                let entries = fields
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{entries}}}")
            },
        );
        format!("{}{}{}", &display[..start], ext_fields, &display[end..])
    }

    #[test]
    fn nullable_ext_fields_match_invoked_java_map_and_codec_behavior() {
        use base64::Engine as _;

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let expected = &fixture["remotingEnumMethodCases"]["commandNullableExtFields"];

        let mut fields = ExtFields::new();
        fields.insert_nullable(None, Some("null-key-value".into()));
        fields.insert_nullable(Some("null-value".into()), None);
        fields.insert("kept".into(), "value".into());

        assert!(fields.contains_null_key());
        assert_eq!(fields.get_nullable(None), Some(Some("null-key-value")));
        assert!(fields.contains_key("null-value"));
        assert_eq!(fields.get_nullable(Some("null-value")), Some(None));
        assert_eq!(fields.get_nullable(Some("kept")), Some(Some("value")));
        assert_eq!(fields.len(), 3);
        assert_eq!(expected["nullKeyPresent"], true);
        assert_eq!(expected["nullKeyValue"], "null-key-value");
        assert_eq!(expected["nullValueKeyPresent"], true);
        assert!(expected["nullValue"].is_null());
        assert_eq!(expected["nonNullValue"], "value");

        let expected_json: serde_json::Value =
            serde_json::from_str(expected["jsonHeader"].as_str().unwrap()).unwrap();
        let json_command = RemotingCommand {
            code: expected_json["code"].as_i64().unwrap() as i32,
            language: Some(LanguageCode::Java),
            version: expected_json["version"].as_i64().unwrap() as i32,
            opaque: expected_json["opaque"].as_i64().unwrap() as i32,
            ext_fields: Some(fields.clone()),
            serialization: SerializeType::Json,
            ..RemotingCommand::default()
        };
        let java_display = expected["display"].as_str().unwrap();
        let rust_display = json_command.to_string();
        let display_fields = |display: &str| {
            let start = display.find("extFields=").unwrap() + "extFields=".len();
            let end = display[start..].find(", serializeTypeCurrentRPC=").unwrap() + start;
            parse_java_ext_fields_display(&display[start..end])
        };
        assert_eq!(display_fields(&rust_display), display_fields(java_display));

        let actual_json: serde_json::Value =
            serde_json::from_slice(&crate::codec::json_header::encode(&json_command).unwrap())
                .unwrap();
        assert_eq!(actual_json, expected_json);

        let expected_binary_map = base64::engine::general_purpose::STANDARD
            .decode(expected["rocketMqMapBase64"].as_str().unwrap())
            .unwrap();
        let binary_command = RemotingCommand {
            serialization: SerializeType::RocketMq,
            ..json_command
        };
        let binary_header = crate::codec::rocketmq_header::encode(&binary_command).unwrap();
        let binary_map_length =
            i32::from_be_bytes(binary_header[17..21].try_into().unwrap()) as usize;
        assert_eq!(binary_map_length, expected_binary_map.len());
        assert_eq!(&binary_header[21..], expected_binary_map.as_slice());

        let mut added = RemotingCommand::default();
        added.add_ext_field_nullable(None, Some("null-key-added".into()));
        added.add_ext_field_nullable(Some("null-value-added".into()), None);
        assert!(added.ext_fields.as_ref().unwrap().contains_null_key());
        assert_eq!(
            added.ext_fields.as_ref().unwrap().get_nullable(None),
            Some(Some("null-key-added"))
        );
        assert!(
            added
                .ext_fields
                .as_ref()
                .unwrap()
                .contains_key("null-value-added")
        );

        let mut put_if_absent = ExtFields::new();
        put_if_absent.insert_nullable(Some("field".into()), None);
        let mut command = RemotingCommand {
            ext_fields: Some(put_if_absent),
            ..RemotingCommand::default()
        };
        command
            .add_ext_field_if_not_exist_nullable(Some("field".into()), Some("replacement".into()))
            .unwrap();
        assert_eq!(
            command
                .ext_fields
                .as_ref()
                .unwrap()
                .get_nullable(Some("field")),
            Some(Some("replacement"))
        );
        assert_eq!(expected["addNullKeyPresent"], true);
        assert_eq!(expected["addNullKeyValue"], "null-key-added");
        assert_eq!(expected["addNullValueKeyPresent"], true);
        assert_eq!(expected["putIfAbsentReplacesNull"], true);
    }

    #[test]
    fn pinned_java_remoting_command_public_surface_is_present_in_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(
            fixture["remotingCommandPublicConstructors"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        let methods = fixture["remotingCommandPublicMethods"].as_array().unwrap();
        assert_eq!(methods.len(), 57);
        let method_names = methods
            .iter()
            .map(|signature| signature.as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(method_names.len() >= 40);
        for signature in [
            "void markResponseType()",
            "boolean isResponseType()",
            "void markOnewayRPC()",
            "boolean isOnewayRPC()",
            "boolean isSuspended()",
            "void setSuspended(boolean)",
            "java.lang.String toString()",
        ] {
            assert!(method_names.contains(&signature), "missing {signature}");
        }
    }

    #[test]
    fn remoting_command_identity_matches_pinned_java_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        let identity = &fixture["remotingCommandIdentityBehavior"];
        assert_eq!(identity["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(identity["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(identity["equalVisibleState"], true);
        assert_eq!(identity["sameInstanceEquals"], true);
        assert_eq!(identity["distinctEqualStateEquals"], false);
        assert_eq!(identity["sameInstanceHashStable"], true);
        assert_eq!(identity["distinctEqualStateHashSetSize"], 2);
    }

    #[test]
    fn process_timer_identity_matches_pinned_java_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/remoting/response-future-api.json"
        ))
        .unwrap();
        let identity = &fixture["processTimerIdentityBehavior"];
        assert_eq!(identity["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(identity["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(identity["equalStoppedState"], true);
        assert_eq!(identity["processTimerGetterReturnsSameObject"], true);
        assert_eq!(identity["sameInstanceEquals"], true);
        assert_eq!(identity["distinctEqualStateEquals"], false);
        assert_eq!(identity["distinctEqualStateHashSetSize"], 2);
    }

    #[test]
    fn cloned_process_timer_shares_java_stopwatch_state() {
        let mut timer = ProcessTimer::new();
        let mut cloned_timer = timer.clone();

        cloned_timer.start().unwrap();
        assert!(timer.is_running());
        assert_eq!(timer.start(), Err(ProcessTimerError::AlreadyRunning));

        timer.stop().unwrap();
        assert!(!cloned_timer.is_running());

        cloned_timer.start().unwrap();
        timer.reset();
        assert!(!cloned_timer.is_running());
        assert_eq!(timer.elapsed(), Duration::ZERO);
    }

    #[test]
    fn client_api_version_initializer_preserves_java_static_configuration_order() {
        const CHILD_CASE: &str = "ROCKETMQ_CLASSIC_CLIENT_API_VERSION_CASE";
        match std::env::var(CHILD_CASE).as_deref() {
            Ok("command-before-client") => {
                let before = RemotingCommand::create_request_command(70).unwrap();
                assert_eq!(before.version, 0);
                let before_response = RemotingCommand::create_response_command(70, None).unwrap();
                assert_eq!(before_response.version, 0);
                RemotingCommand::initialize_client_api_version(515);
                let after = RemotingCommand::create_request_command(71).unwrap();
                assert_eq!(after.version, 515);
                let after_response = RemotingCommand::create_response_command(71, None).unwrap();
                assert_eq!(after_response.version, 515);
                return;
            }
            Ok("cached-property-before-client") => {
                let before = RemotingCommand::create_request_command(70).unwrap();
                assert_eq!(before.version, 77);
                let before_response = RemotingCommand::create_response_command(70, None).unwrap();
                assert_eq!(before_response.version, 77);
                RemotingCommand::initialize_client_api_version(515);
                let after = RemotingCommand::create_request_command(71).unwrap();
                assert_eq!(after.version, 77);
                let after_response = RemotingCommand::create_response_command(71, None).unwrap();
                assert_eq!(after_response.version, 77);
                return;
            }
            Ok("client-before-command") => {
                RemotingCommand::initialize_client_api_version(515);
                let command = RemotingCommand::create_request_command(70).unwrap();
                assert_eq!(command.version, 515);
                let response = RemotingCommand::create_response_command(71, None).unwrap();
                assert_eq!(response.version, 515);
                return;
            }
            _ => {}
        }

        for (case, property) in [
            ("command-before-client", None),
            ("cached-property-before-client", Some("77")),
            ("client-before-command", Some("77")),
        ] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .arg("--exact")
                .arg("command::tests::client_api_version_initializer_preserves_java_static_configuration_order")
                .arg("--nocapture")
                .env(CHILD_CASE, case)
                .env_remove(RemotingCommand::SERIALIZE_TYPE_PROPERTY)
                .env_remove(RemotingCommand::SERIALIZE_TYPE_ENV);
            if let Some(value) = property {
                child.env(RemotingCommand::REMOTING_VERSION_KEY, value);
            } else {
                child.env_remove(RemotingCommand::REMOTING_VERSION_KEY);
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "child case {case} failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn command_factories_match_pinned_java_defaults_and_id_sequence() {
        const CHILD_CASE: &str = "ROCKETMQ_CLASSIC_COMMAND_FACTORY_CASE";
        if std::env::var(CHILD_CASE).as_deref() == Ok("invalid") {
            assert_eq!(
                RemotingCommand::create_request_command(71).unwrap_err(),
                "For input string: \"invalid\""
            );
            return;
        }
        if std::env::var(CHILD_CASE).as_deref() == Ok("child") {
            let fixture: serde_json::Value =
                serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                    .unwrap();
            let cases = &fixture["remotingEnumMethodCases"]["commandFactories"];
            let before_failed_factories =
                RemotingCommand::create_request_command(78).unwrap().opaque;
            assert!(
                RemotingCommand::create_response_command_with_header_factory::<
                    rocketmq_classic_protocol::header::UnregisterClientResponseHeader,
                >(79, Some("missing".into()), || None)
                .unwrap()
                .is_none()
            );
            assert!(
                RemotingCommand::create_response_command_with_header_factory::<
                    rocketmq_classic_protocol::header::UnregisterClientResponseHeader,
                >(80, Some("inaccessible".into()), || None)
                .unwrap()
                .is_none()
            );
            assert!(
                RemotingCommand::create_response_command_with_header_factory::<
                    rocketmq_classic_protocol::header::UnregisterClientResponseHeader,
                >(81, Some("throwing".into()), || None)
                .unwrap()
                .is_none()
            );
            assert_eq!(
                RemotingCommand::build_error_response_with_header_factory::<
                    rocketmq_classic_protocol::header::UnregisterClientResponseHeader,
                >(82, Some("error".into()), || None)
                .unwrap_err(),
                "NullPointerException"
            );
            let after_failed_factories =
                RemotingCommand::create_request_command(83).unwrap().opaque;
            assert_eq!(
                after_failed_factories.wrapping_sub(before_failed_factories),
                cases["failedHeaderFactoryOpaqueDelta"].as_i64().unwrap() as i32
            );
            assert_eq!(cases["missingDefaultHeaderReturnsNull"], true);
            assert_eq!(cases["inaccessibleDefaultHeaderReturnsNull"], true);
            assert_eq!(cases["throwingDefaultHeaderReturnsNull"], true);
            assert_eq!(cases["errorFactoryHeaderFailure"], "NullPointerException");
            let mut commands = [
                RemotingCommand::create_request_command(71).unwrap(),
                RemotingCommand::create_response_command(72, Some("accepted".into())).unwrap(),
                RemotingCommand::build_error_response(73, None).unwrap(),
                RemotingCommand::create_request_command_with_header(
                    74,
                    Some({
                        let mut __phase18_header_1 =
                            rocketmq_classic_protocol::header::GetRouteInfoRequestHeader::default();
                        __phase18_header_1.set_topic("orders".into());
                        __phase18_header_1
                    }),
                )
                .unwrap(),
                {
                    let mut header =
                        rocketmq_classic_protocol::header::SearchOffsetResponseHeader::default();
                    header.set_offset(Some(88));
                    RemotingCommand::create_response_command_with_header(75, Some(header)).unwrap()
                },
                RemotingCommand::create_response_command_for_header::<
                    rocketmq_classic_protocol::header::UnregisterClientResponseHeader,
                >()
                .unwrap(),
                RemotingCommand::create_response_command_with_default_header::<
                    rocketmq_classic_protocol::header::UnregisterClientResponseHeader,
                >(76, Some("class-header".into()))
                .unwrap(),
                RemotingCommand::build_error_response_with_default_header::<
                    rocketmq_classic_protocol::header::UnregisterClientResponseHeader,
                >(77, Some("built".into()))
                .unwrap(),
            ];
            for command in &mut commands {
                command.make_custom_header_to_net();
            }
            assert_eq!(cases["commands"].as_array().unwrap().len(), commands.len());
            for (case, command) in cases["commands"]
                .as_array()
                .unwrap()
                .iter()
                .zip(commands.iter())
            {
                assert_eq!(
                    command.code,
                    case["code"].as_i64().unwrap() as i32,
                    "{case}"
                );
                assert_eq!(
                    command.language.map(LanguageCode::name),
                    case["language"].as_str(),
                    "{case}"
                );
                assert_eq!(
                    command.version,
                    case["version"].as_i64().unwrap() as i32,
                    "{case}"
                );
                assert_eq!(
                    command.flag,
                    case["flag"].as_i64().unwrap() as i32,
                    "{case}"
                );
                assert_eq!(command.remark.as_deref(), case["remark"].as_str(), "{case}");
                let expected_ext_fields = case["extFields"].as_object().map(|object| {
                    object
                        .iter()
                        .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                        .collect::<BTreeMap<_, _>>()
                });
                assert_eq!(
                    command
                        .ext_fields
                        .as_ref()
                        .map(|fields| fields.non_null_fields().clone()),
                    expected_ext_fields,
                    "{case}"
                );
                assert_eq!(
                    command.serialization.name(),
                    case["serialization"].as_str().unwrap(),
                    "{case}"
                );
            }
            let expected_deltas = cases["requestIdDeltas"].as_array().unwrap();
            assert_eq!(expected_deltas.len(), commands.len() - 1);
            for index in 1..commands.len() {
                assert_eq!(
                    commands[index]
                        .opaque
                        .wrapping_sub(commands[index - 1].opaque),
                    expected_deltas[index - 1].as_i64().unwrap() as i32
                );
            }
            for (index, command) in commands.iter().enumerate() {
                assert_eq!(command.is_response(), !matches!(index, 0 | 3));
            }
            return;
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let expected_version = fixture["remotingEnumMethodCases"]["commandFactories"]["commands"]
            [0]["version"]
            .as_i64()
            .unwrap()
            .to_string();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("command::tests::command_factories_match_pinned_java_defaults_and_id_sequence")
            .arg("--nocapture")
            .env(CHILD_CASE, "child")
            .env(RemotingCommand::REMOTING_VERSION_KEY, expected_version)
            .env_remove(RemotingCommand::SERIALIZE_TYPE_PROPERTY)
            .env_remove(RemotingCommand::SERIALIZE_TYPE_ENV)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child factory comparison failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let invalid = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("command::tests::command_factories_match_pinned_java_defaults_and_id_sequence")
            .arg("--nocapture")
            .env(CHILD_CASE, "invalid")
            .env(RemotingCommand::REMOTING_VERSION_KEY, "invalid")
            .env_remove(RemotingCommand::SERIALIZE_TYPE_PROPERTY)
            .env_remove(RemotingCommand::SERIALIZE_TYPE_ENV)
            .output()
            .unwrap();
        assert!(
            invalid.status.success(),
            "invalid version property case failed: {}{}",
            String::from_utf8_lossy(&invalid.stdout),
            String::from_utf8_lossy(&invalid.stderr)
        );
    }

    #[test]
    fn command_custom_header_helpers_match_invoked_java_fields() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let cases = &fixture["remotingEnumMethodCases"]["commandCustomHeaders"];
        assert_eq!(cases["attachedHeaderPresent"], true);
        assert_eq!(cases["readReturnsSameInstance"], true);
        assert_eq!(cases["readReturnsNullWithoutAttachedHeader"], true);
        assert_eq!(cases["writeNullClearsAttachedHeader"], true);
        assert_eq!(cases["extFieldsBeforeMake"]["oracle"], "5.5.1");
        assert!(cases["extFieldsBeforeMake"]["topic"].is_null());

        let header = {
            let mut __phase18_header_2 =
                rocketmq_classic_protocol::header::GetRouteInfoRequestHeader::default();
            __phase18_header_2.set_topic("custom-topic".into());
            __phase18_header_2.set_accept_standard_json_only(Some(true));
            __phase18_header_2.set_namespace(Some("custom-ns".into()));
            __phase18_header_2.set_namespaced(Some(false));
            __phase18_header_2.set_broker_name(Some("broker-a".into()));
            __phase18_header_2.set_oneway(Some(true));
            __phase18_header_2
        };
        let mut command = RemotingCommand {
            ext_fields: Some(HeaderMap::from([("oracle".into(), "5.5.1".into())]).into()),
            ..Default::default()
        };
        let mut null_header = RemotingCommand::default();
        null_header.write_custom_header(Some(
            rocketmq_classic_protocol::header::GetRouteInfoRequestHeader::default(),
        ));
        null_header
            .write_custom_header::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>(
                None,
            );
        assert!(null_header.custom_header.is_none());
        command.write_custom_header(Some(header));
        assert!(command.custom_header.is_some());
        let attached_ref = command
            .read_custom_header::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>()
            .unwrap() as *const _;
        let repeated_attached_ref = command
            .read_custom_header::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>()
            .unwrap() as *const _;
        assert_eq!(attached_ref, repeated_attached_ref);
        assert_eq!(
            command
                .ext_fields
                .as_ref()
                .unwrap()
                .get("oracle")
                .map(String::as_str),
            Some("5.5.1")
        );
        assert_eq!(
            command
                .read_custom_header::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>(
                )
                .unwrap()
                .get_topic(),
            "custom-topic"
        );

        let before_make = crate::codec::json_header::encode(&command).unwrap();
        let before_make: serde_json::Value = serde_json::from_slice(&before_make).unwrap();
        assert_eq!(before_make["extFields"]["oracle"], "5.5.1");
        assert_eq!(before_make["extFields"]["topic"], "custom-topic");
        assert!(command.ext_fields.as_ref().unwrap().get("topic").is_none());

        command
            .read_custom_header_mut::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>(
            )
            .unwrap()
            .set_topic(
                cases["attachedHeaderTopicAfterMutation"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        let after_mutation = crate::codec::json_header::encode(&command).unwrap();
        let after_mutation: serde_json::Value = serde_json::from_slice(&after_mutation).unwrap();
        assert_eq!(
            after_mutation["extFields"]["topic"],
            cases["attachedHeaderTopicAfterMutation"]
        );
        let binary = crate::codec::rocketmq_header::encode(&command).unwrap();
        let binary = crate::codec::rocketmq_header::decode(&binary).unwrap();
        assert_eq!(
            binary
                .ext_fields
                .as_ref()
                .unwrap()
                .get("topic")
                .map(String::as_str),
            cases["attachedHeaderTopicAfterMutation"].as_str()
        );
        assert_eq!(
            command
                .read_custom_header::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>(
                )
                .unwrap()
                .get_topic(),
            cases["attachedHeaderTopicAfterMutation"]
        );

        let expected_ext_fields = cases["extFieldsAfterMake"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect::<BTreeMap<_, _>>();
        command.make_custom_header_to_net();
        assert_eq!(
            command
                .ext_fields
                .as_ref()
                .map(|fields| fields.non_null_fields()),
            Some(&expected_ext_fields)
        );
        assert_eq!(
            command
                .read_custom_header::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>(
                )
                .unwrap()
                .get_topic(),
            cases["cachedHeader"]["topic"]
        );

        let unattached = RemotingCommand {
            ext_fields: command.ext_fields.clone(),
            ..Default::default()
        };
        assert!(
            unattached
                .read_custom_header::<rocketmq_classic_protocol::header::GetRouteInfoRequestHeader>(
                )
                .is_none()
        );

        let first_cached = command
            .decode_command_custom_header_with_cache::<
                rocketmq_classic_protocol::header::GetRouteInfoRequestHeader,
            >(true)
            .unwrap() as *const _;
        let repeated_cached = command
            .decode_command_custom_header_with_cache::<
                rocketmq_classic_protocol::header::GetRouteInfoRequestHeader,
            >(true)
            .unwrap() as *const _;
        assert_eq!(first_cached, repeated_cached);
        assert_eq!(cases["cachedRepeatedSameInstance"], true);
        let decoded = command
            .decode_command_custom_header::<
                rocketmq_classic_protocol::header::GetRouteInfoRequestHeader,
            >()
            .unwrap()
            .clone();
        let direct = command
            .decode_command_custom_header_directly::<
                rocketmq_classic_protocol::header::GetRouteInfoRequestHeader,
            >(true)
            .unwrap();
        for decoded in [decoded, direct] {
            assert_eq!(
                decoded.get_topic(),
                cases["directHeader"]["topic"].as_str().unwrap()
            );
            assert_eq!(
                decoded.get_accept_standard_json_only(),
                cases["directHeader"]["acceptStandardJsonOnly"].as_bool()
            );
            assert_eq!(
                decoded.get_namespace(),
                cases["directHeader"]["ns"].as_str()
            );
            assert_eq!(
                decoded.get_namespaced(),
                cases["directHeader"]["nsd"].as_bool()
            );
            assert_eq!(
                decoded.get_broker_name(),
                cases["directHeader"]["bname"].as_str()
            );
            assert_eq!(
                decoded.get_oneway(),
                cases["directHeader"]["oway"].as_bool()
            );
        }

        let mut add_field = RemotingCommand::default();
        add_field.add_ext_field("added", "value");
        let expected_add_field = cases["addExtField"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            add_field
                .ext_fields
                .as_ref()
                .map(|fields| fields.non_null_fields()),
            Some(&expected_add_field)
        );

        let mut add_if_missing = RemotingCommand {
            ext_fields: Some(HeaderMap::from([("existing".into(), "original".into())]).into()),
            ..Default::default()
        };
        add_if_missing
            .add_ext_field_if_not_exist("existing", "original")
            .unwrap();
        add_if_missing
            .add_ext_field_if_not_exist("existing", "replacement")
            .unwrap();
        add_if_missing
            .add_ext_field_if_not_exist("new", "value")
            .unwrap();
        let expected_add_if_missing = cases["addExtFieldIfNotExist"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            add_if_missing
                .ext_fields
                .as_ref()
                .map(|fields| fields.non_null_fields()),
            Some(&expected_add_if_missing)
        );

        let missing_map_error = RemotingCommand::default()
            .add_ext_field_if_not_exist("missing-map", "value")
            .unwrap_err();
        assert!(matches!(
            missing_map_error,
            ProtocolError::MissingField("extFields")
        ));
        assert_eq!(
            cases["addExtFieldIfNotExistWithoutMap"],
            "NullPointerException"
        );
    }

    #[test]
    fn custom_header_check_fields_matches_invoked_java_551() {
        use rocketmq_classic_protocol::header::{
            CheckTransactionStateResponseHeader, CreateTopicRequestHeader,
            EndTransactionRequestHeader,
        };

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let validation = &fixture["remotingEnumMethodCases"]["commandCustomHeaders"]["checkFields"];

        let mut create_topic_base = HeaderMap::from([
            ("topic".into(), "topic".into()),
            ("defaultTopic".into(), "default".into()),
            ("readQueueNums".into(), "4".into()),
            ("writeQueueNums".into(), "4".into()),
            ("perm".into(), "6".into()),
        ]);
        for case in validation["createTopic"].as_array().unwrap() {
            create_topic_base.insert(
                "topicFilterType".into(),
                case["value"].as_str().unwrap().into(),
            );
            let command = RemotingCommand {
                ext_fields: Some(create_topic_base.clone().into()),
                ..Default::default()
            };
            assert_java_validation_outcome(
                command.decode_command_custom_header_directly::<CreateTopicRequestHeader>(false),
                case["outcome"].as_str().unwrap(),
            );
        }

        let transaction_base = HeaderMap::from([
            ("producerGroup".into(), "producer".into()),
            ("tranStateTableOffset".into(), "1".into()),
            ("commitLogOffset".into(), "2".into()),
            ("msgId".into(), "message".into()),
        ]);
        for (cases_key, header_kind) in
            [("endTransaction", 0), ("checkTransactionStateResponse", 1)]
        {
            for case in validation[cases_key].as_array().unwrap() {
                let mut fields = transaction_base.clone();
                fields.insert(
                    "commitOrRollback".into(),
                    case["value"].as_str().unwrap().into(),
                );
                let command = RemotingCommand {
                    ext_fields: Some(fields.into()),
                    ..Default::default()
                };
                let outcome = if header_kind == 0 {
                    command
                        .decode_command_custom_header_directly::<EndTransactionRequestHeader>(false)
                        .map(|_| ())
                } else {
                    command
                        .decode_command_custom_header_directly::<CheckTransactionStateResponseHeader>(true)
                        .map(|_| ())
                };
                assert_java_validation_outcome(outcome, case["outcome"].as_str().unwrap());
            }
        }

        #[derive(Default)]
        struct ValidatedCustomHeader {
            value: Option<i32>,
        }

        impl CommandHeader for ValidatedCustomHeader {
            fn encode_fields(&self, fields: &mut HeaderMap) {
                if let Some(value) = self.value {
                    fields.insert("value".into(), value.to_string());
                }
            }

            fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
                let value = fields
                    .get("value")
                    .map(|value| {
                        value
                            .parse::<i32>()
                            .map_err(|_| ProtocolError::InvalidField {
                                field: "value",
                                reason: "expected i32".into(),
                            })
                    })
                    .transpose()?;
                Ok(Self { value })
            }

            fn check_fields(&self) -> Result<(), ProtocolError> {
                if self.value.is_some_and(|value| value >= 0) {
                    Ok(())
                } else {
                    Err(ProtocolError::InvalidField {
                        field: "value",
                        reason: "value must be non-negative".into(),
                    })
                }
            }
        }

        let custom_cases = &validation["custom"];
        for (case_name, fields) in [
            ("zero", HeaderMap::from([("value".into(), "0".into())])),
            ("negative", HeaderMap::from([("value".into(), "-1".into())])),
            ("missing", HeaderMap::new()),
        ] {
            let command = RemotingCommand {
                ext_fields: Some(fields.into()),
                ..Default::default()
            };
            assert_java_validation_outcome(
                command.decode_command_custom_header_directly::<ValidatedCustomHeader>(true),
                custom_cases[case_name].as_str().unwrap(),
            );
        }
    }

    fn assert_java_validation_outcome<T>(result: Result<T, ProtocolError>, java_outcome: &str) {
        assert_eq!(
            result.is_ok(),
            java_outcome == "OK",
            "Java outcome: {java_outcome}"
        );
    }

    #[test]
    fn command_process_timer_and_callback_list_match_java_api_behavior() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let cases = &fixture["remotingEnumMethodCases"]["commandInfrastructure"];
        let mut command = RemotingCommand::default();
        assert_eq!(
            command.get_process_timer().is_none(),
            cases["processTimerInitiallyNull"]
        );
        assert_eq!(
            command.get_callback_list().is_none(),
            cases["callbackListInitiallyNull"]
        );

        command.set_process_timer(Some(ProcessTimer::new()));
        let first_timer = command.get_process_timer().unwrap() as *const _;
        let repeated_timer = command.get_process_timer().unwrap() as *const _;
        assert!(std::ptr::eq(first_timer, repeated_timer));
        assert!(!command.get_process_timer().unwrap().is_running());
        assert_eq!(cases["processTimerSameInstance"], true);
        assert_eq!(cases["processTimerInitiallyRunning"], false);

        let timer = command.get_process_timer_mut().unwrap();
        timer.start().unwrap();
        assert!(timer.is_running());
        assert!(timer.elapsed() >= Duration::ZERO);
        assert_eq!(timer.start(), Err(ProcessTimerError::AlreadyRunning));
        assert_eq!(cases["processTimerRunningAfterStart"], true);
        assert_eq!(cases["processTimerElapsedNonnegative"], true);
        assert_eq!(
            cases["processTimerStartWhileRunning"],
            "IllegalStateException"
        );

        timer.stop().unwrap();
        assert!(!timer.is_running());
        timer.reset();
        assert!(!timer.is_running());
        assert_eq!(timer.elapsed(), Duration::ZERO);
        assert_eq!(timer.stop(), Err(ProcessTimerError::NotRunning));
        assert_eq!(cases["processTimerRunningAfterStop"], false);
        assert_eq!(cases["processTimerRunningAfterReset"], false);
        assert_eq!(cases["processTimerElapsedAfterReset"], 0);
        assert_eq!(
            cases["processTimerStopWhileStopped"],
            "IllegalStateException"
        );
        command.set_process_timer(None);
        assert!(command.get_process_timer().is_none());
        assert_eq!(cases["processTimerNullAfterClear"], true);

        let callback_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callbacks = vec![{
            let callback_count = callback_count.clone();
            CommandCallback::new(move || {
                callback_count.fetch_add(1, Ordering::SeqCst);
            })
        }];
        let callbacks_ptr = callbacks.as_ptr();
        command.set_callback_list(Some(callbacks));
        assert_eq!(command.get_callback_list().unwrap().as_ptr(), callbacks_ptr);
        command.get_callback_list_mut().unwrap().push({
            let callback_count = callback_count.clone();
            CommandCallback::new(move || {
                callback_count.fetch_add(1, Ordering::SeqCst);
            })
        });
        assert_eq!(cases["callbackListSameInstance"], true);
        for callback in command.get_callback_list().unwrap() {
            callback.accept();
        }
        assert_eq!(command.get_callback_list().unwrap().len(), 2);
        assert_eq!(callback_count.load(Ordering::SeqCst), 2);
        assert_eq!(cases["callbackListSizeAfterMutation"], 2);
        assert_eq!(cases["callbackListInvocationCount"], 2);
        command.set_callback_list(None);
        assert!(command.get_callback_list().is_none());
        assert_eq!(cases["callbackListNullAfterClear"], true);
    }

    #[test]
    fn serialize_type_config_matches_java_property_and_environment_rules() {
        const CHILD_MARKER: &str = "ROCKETMQ_CLASSIC_SERIALIZE_TYPE_CASE";
        if let Ok(case) = std::env::var(CHILD_MARKER) {
            match case.as_str() {
                "property-precedence" => assert_eq!(
                    RemotingCommand::default().serialization,
                    SerializeType::RocketMq
                ),
                "environment-alias" => assert_eq!(
                    RemotingCommand::default().serialization,
                    SerializeType::RocketMq
                ),
                "blank-property" => assert_eq!(
                    RemotingCommand::default().serialization,
                    SerializeType::Json
                ),
                "invalid-property" => {
                    let error =
                        RemotingCommand::get_serialize_type_config_in_this_server().unwrap_err();
                    assert!(error.contains("parser specified protocol error"));
                    assert!(error.contains("protocol=rocketmq"));
                    let panic = std::panic::catch_unwind(RemotingCommand::default).unwrap_err();
                    assert!(
                        panic
                            .downcast_ref::<String>()
                            .unwrap()
                            .contains("protocol=rocketmq")
                    );
                }
                other => panic!("unknown child case {other}"),
            }
            return;
        }

        let executable = std::env::current_exe().unwrap();
        for (case, property, environment) in [
            ("property-precedence", Some("ROCKETMQ"), Some("JSON")),
            ("environment-alias", None, Some("ROCKETMQ")),
            ("blank-property", Some(" \t"), Some("ROCKETMQ")),
            ("invalid-property", Some("rocketmq"), None),
        ] {
            let mut child = std::process::Command::new(&executable);
            child
                .arg("--exact")
                .arg("command::tests::serialize_type_config_matches_java_property_and_environment_rules")
                .arg("--nocapture")
                .env(CHILD_MARKER, case)
                .env_remove(RemotingCommand::SERIALIZE_TYPE_PROPERTY)
                .env_remove(RemotingCommand::SERIALIZE_TYPE_ENV);
            if let Some(value) = property {
                child.env(RemotingCommand::SERIALIZE_TYPE_PROPERTY, value);
            }
            if let Some(value) = environment {
                child.env(RemotingCommand::SERIALIZE_TYPE_ENV, value);
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "child case {case} failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn shared_custom_header_preserves_java_caller_alias() {
        use crate::command::SharedCommandHeader;

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                .unwrap();
        let cases = &fixture["remotingEnumMethodCases"]["commandCustomHeaders"];
        let mut initial_header =
            rocketmq_classic_protocol::header::GetRouteInfoRequestHeader::default();
        initial_header.set_topic("custom-topic".into());
        let header = SharedCommandHeader::new(initial_header);
        let caller_header = header.clone();
        let mut command =
            RemotingCommand::create_request_command_with_header(78, Some(header)).unwrap();

        caller_header.with_mut(|header| {
            header.set_topic(
                cases["attachedHeaderTopicAfterMutation"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        });
        assert_eq!(cases["readReturnsSameInstance"], true);
        assert_eq!(cases["attachedMutationVisibleToCaller"], true);
        assert_eq!(
            command
                .read_custom_header::<SharedCommandHeader<
                    rocketmq_classic_protocol::header::GetRouteInfoRequestHeader,
                >>()
                .unwrap()
                .with(|header| header.get_topic().to_owned()),
            cases["attachedHeaderTopicAfterMutation"]
        );

        let before_make = crate::codec::json_header::encode(&command).unwrap();
        let before_make: serde_json::Value = serde_json::from_slice(&before_make).unwrap();
        assert_eq!(
            before_make["extFields"]["topic"],
            cases["attachedHeaderTopicAfterMutation"]
        );
        assert!(command.ext_fields.is_none());

        command.make_custom_header_to_net();
        command
            .read_custom_header::<SharedCommandHeader<
                rocketmq_classic_protocol::header::GetRouteInfoRequestHeader,
            >>()
            .unwrap()
            .with_mut(|header| header.set_topic("mutated-through-command".into()));
        assert_eq!(cases["commandMutationVisibleToCaller"], true);
        assert_eq!(
            caller_header.with(|header| header.get_topic().to_owned()),
            "mutated-through-command"
        );

        let json = crate::codec::json_header::encode(&command).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(json["extFields"]["topic"], "mutated-through-command");
        let binary = crate::codec::rocketmq_header::encode(&command).unwrap();
        let binary = crate::codec::rocketmq_header::decode(&binary).unwrap();
        assert_eq!(
            binary
                .ext_fields
                .as_ref()
                .unwrap()
                .get("topic")
                .map(String::as_str),
            Some("mutated-through-command")
        );
    }

    #[test]
    fn defaults_and_flag_helpers_match_java_bits() {
        const CHILD_CASE: &str = "ROCKETMQ_CLASSIC_COMMAND_DEFAULT_CASE";
        if std::env::var(CHILD_CASE).as_deref() == Ok("child") {
            let fixture: serde_json::Value =
                serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
                    .unwrap();
            let cases = &fixture["remotingEnumMethodCases"]["commandConstructorDefaults"];
            let command = RemotingCommand::default();
            let next = RemotingCommand::default();
            assert_eq!(command.code, cases["code"].as_i64().unwrap() as i32);
            assert_eq!(
                command.language.map(LanguageCode::name),
                cases["language"].as_str()
            );
            assert_eq!(command.version, cases["version"].as_i64().unwrap() as i32);
            assert_eq!(
                next.opaque.wrapping_sub(command.opaque),
                cases["opaqueIncrement"].as_i64().unwrap() as i32
            );
            assert_eq!(command.flag, cases["flag"].as_i64().unwrap() as i32);
            assert_eq!(command.remark.is_none(), cases["remarkNull"]);
            assert_eq!(command.ext_fields.is_none(), cases["extFieldsNull"]);
            assert_eq!(command.body.is_none(), cases["bodyNull"]);
            assert_eq!(
                command.serialization.name(),
                cases["serialization"].as_str().unwrap()
            );
            assert_eq!(command.suspended, cases["suspended"]);
            assert_eq!(command.process_timer.is_none(), cases["processTimerNull"]);
            assert_eq!(command.callback_list.is_none(), cases["callbackListNull"]);

            let mut command = RemotingCommand::default();
            assert!(!command.is_response());
            assert!(!command.is_oneway());
            command.flag = 1 << 8;
            command.mark_response();
            assert_eq!(command.flag, (1 << 8) | 1);
            command.mark_oneway();
            assert_eq!(command.flag, (1 << 8) | 3);
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("command::tests::defaults_and_flag_helpers_match_java_bits")
            .arg("--nocapture")
            .env(CHILD_CASE, "child")
            .env_remove(RemotingCommand::SERIALIZE_TYPE_PROPERTY)
            .env_remove(RemotingCommand::SERIALIZE_TYPE_ENV)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child default comparison failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let mut command = RemotingCommand::default();
        assert!(!command.is_response());
        assert!(!command.is_oneway());
        command.flag = 1 << 8;
        command.mark_response();
        assert_eq!(command.flag, (1 << 8) | 1);
        command.mark_oneway();
        assert_eq!(command.flag, (1 << 8) | 3);
    }
}
