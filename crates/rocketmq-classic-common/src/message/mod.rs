//! Shared RocketMQ message and utility types.
//!
//! Java exposes the V1 magic value as `MESSAGE_MAGIC_CODE`; the V1 label is
//! kept internal to the decoder.
//!
//! ```compile_fail
//! use rocketmq_classic_common::message::MESSAGE_MAGIC_CODE_V1;
//! # fn main() {}
//! ```

mod compact;
pub mod constants;
pub mod decoder;
mod encoder;
mod host_address;
mod message_id;
mod properties;
mod types;
mod utils;

pub use compact::{
    CompactMessageError, count_inner_messages, decode_compact_message, decode_compact_messages,
    decode_inner_batch, encode_compact_message, encode_compact_messages,
};
pub use decoder::{
    BLANK_MAGIC_CODE, CHARSET_UTF8, DecodeError, DecodeOptions, MESSAGE_FLAG_POSITION,
    MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_POSITION, MESSAGE_MAGIC_CODE_V2,
    MESSAGE_PHYSIC_OFFSET_POSITION, MESSAGE_STORE_TIMESTAMP_POSITION, MessageVersion,
    MessageVersionError, PHY_POS_POSITION, QUEUE_OFFSET_POSITION, SYSFLAG_POSITION,
    append_crc32_property, client_decode, client_decodes, client_decodes_with_body,
    decode_many_as_client_compatible, decode_many_compatible, decode_one, decode_one_as_client,
    decode_record_properties,
};
pub use encoder::{MessageExtEncodeError, encode_message_ext, encode_message_ext_uniquely};
pub use host_address::{MessageHostAddress, MessageHostAddressBytesError, MessageHostAddressError};
pub use message_id::{
    MessageId, MessageIdError, create_message_id, create_message_id_from_buffers, decode_message_id,
};
pub use properties::{
    decode_properties, decode_properties_nullable, encode_nullable_property_entries,
    encode_properties, encode_properties_nullable,
};
pub use types::{
    Message, MessageClientExt, MessageExt, MessageExtSubtype, MessageKeyCollectionError,
    MessageProperties, MessagePropertyError, MessagePropertyParseError, MessageQueue,
    MessageQueueCompareError, MessageQueueForC, MessageQueueForCCompareError,
    MessageSysFlagParseError, NAME_VALUE_SEPARATOR, NullableMessageProperties, PROPERTY_SEPARATOR,
    SharedMessageBody, SharedMessageProperties, TopicFilterType,
};
pub use utils::{
    MessageUtilsError, delete_property, get_sharding_key_index, get_sharding_key_index_by_msg,
    get_sharding_key_indexes,
};
