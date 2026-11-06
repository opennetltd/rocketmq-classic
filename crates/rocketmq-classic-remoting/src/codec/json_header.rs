use std::io::{self, Write};

use rocketmq_classic_protocol::codes::{LanguageCode, SerializeType};
use serde_json::{Map, Value};

use crate::{command::RemotingCommand, error::CodecError, ext_fields::ExtFields};

/// Encode a JSON remoting header without applying a caller-selected size limit.
pub fn encode(command: &RemotingCommand) -> Result<Vec<u8>, CodecError> {
    encode_with_limit(command, 0x00ff_ffff)
}

pub(crate) fn encode_with_limit(
    command: &RemotingCommand,
    max_length: usize,
) -> Result<Vec<u8>, CodecError> {
    let mut fields = Map::new();
    fields.insert("code".into(), command.code.into());
    if let Some(language) = command.language {
        fields.insert("language".into(), language.name().into());
    }
    fields.insert("version".into(), command.version.into());
    fields.insert("opaque".into(), command.opaque.into());
    fields.insert("flag".into(), command.flag.into());
    if let Some(remark) = &command.remark {
        fields.insert("remark".into(), remark.clone().into());
    }
    if let Some(ext_fields) = command.ext_fields_for_encoding() {
        fields.insert(
            "extFields".into(),
            serde_json::to_value(ext_fields.as_ref()).unwrap(),
        );
    }
    fields.insert(
        "serializeTypeCurrentRPC".into(),
        command.serialization.name().into(),
    );
    let value = Value::Object(fields);
    let mut counter = CountingWriter {
        length: 0,
        limit: max_length,
    };
    serde_json::to_writer(&mut counter, &value)
        .map_err(|_| CodecError::FieldTooWide("JSON header"))?;
    let mut output = Vec::with_capacity(counter.length);
    serde_json::to_writer(&mut output, &value)
        .map_err(|error| CodecError::Json(error.to_string()))?;
    Ok(output)
}

pub fn decode(bytes: &[u8]) -> Result<RemotingCommand, CodecError> {
    let value: Value = rocketmq_classic_protocol::decode_java_remoting_serializable(bytes)
        .map_err(|error| CodecError::Json(error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| CodecError::Json("header must be a JSON object".into()))?;
    let code = integer(object.get("code"), "code")?.unwrap_or_default();
    let language = match object.get("language") {
        None => Some(LanguageCode::Java),
        Some(Value::Null) => None,
        Some(Value::String(name)) => LanguageCode::get_code_by_name(Some(name)),
        _ => return Err(CodecError::Json("language must be a string".into())),
    };
    let version = integer(object.get("version"), "version")?.unwrap_or_default();
    let opaque = integer(object.get("opaque"), "opaque")?.unwrap_or_default();
    let flag = integer(object.get("flag"), "flag")?.unwrap_or_default();
    let remark = match object.get("remark") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.clone()),
        _ => return Err(CodecError::Json("remark must be a string or null".into())),
    };
    let ext_fields = match object.get("extFields") {
        None | Some(Value::Null) => None,
        Some(Value::Object(values)) => {
            let mut fields = ExtFields::new();
            for (key, value) in values {
                let value = match value {
                    Value::String(value) => Some(value.clone()),
                    Value::Null => None,
                    _ => {
                        return Err(CodecError::Json(format!(
                            "extFields.{key} must be a string"
                        )));
                    }
                };
                fields.insert_nullable(Some(key.clone()), value);
            }
            Some(fields)
        }
        _ => {
            return Err(CodecError::Json(
                "extFields must be an object or null".into(),
            ));
        }
    };
    let serialization = match object.get("serializeTypeCurrentRPC") {
        None | Some(Value::Null) => SerializeType::Json,
        Some(Value::String(name)) if name == "JSON" => SerializeType::Json,
        Some(Value::String(name)) if name == "ROCKETMQ" => SerializeType::RocketMq,
        Some(Value::String(name)) => {
            return Err(CodecError::Json(format!("unknown serialization `{name}`")));
        }
        Some(_) => {
            return Err(CodecError::Json(
                "serializeTypeCurrentRPC must be a string".into(),
            ));
        }
    };
    Ok(RemotingCommand {
        code,
        language,
        version,
        opaque,
        flag,
        remark,
        ext_fields,
        serialization,
        ..RemotingCommand::default()
    })
}

struct CountingWriter {
    length: usize,
    limit: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .length
            .checked_add(bytes.len())
            .filter(|length| *length <= self.limit)
            .ok_or_else(|| io::Error::other("JSON header exceeds configured bound"))?;
        self.length = length;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn integer(value: Option<&Value>, field: &'static str) -> Result<Option<i32>, CodecError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_i64()
            .and_then(|number| i32::try_from(number).ok())
            .map(Some)
            .ok_or_else(|| CodecError::Json(format!("{field} must be a 32-bit integer"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn json_uses_java_field_names_and_language_enum_string() {
        let command = RemotingCommand {
            code: 105,
            ext_fields: Some(BTreeMap::from([("unknown".into(), "kept".into())]).into()),
            ..RemotingCommand::default()
        };
        let encoded = encode_with_limit(&command, 4096).unwrap();
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["language"], "JAVA");
        assert_eq!(value["extFields"]["unknown"], "kept");
        assert!(value.get("body").is_none());
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.code, command.code);
        assert_eq!(decoded.language, command.language);
        assert_eq!(decoded.version, command.version);
        assert_eq!(decoded.opaque, command.opaque);
        assert_eq!(decoded.flag, command.flag);
        assert_eq!(decoded.remark, command.remark);
        assert_eq!(decoded.ext_fields, command.ext_fields);
        assert_eq!(decoded.body, command.body);
        assert_eq!(decoded.serialization, command.serialization);
    }
}
