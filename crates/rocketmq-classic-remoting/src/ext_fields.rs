use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::iter::FromIterator;
use std::ops::Deref;

use rocketmq_classic_protocol::header::HeaderMap;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Extension fields from a Java `HashMap<String, String>`.
///
/// RocketMQ commands may contain a null key or value in memory. The ordinary
/// map view contains non-null entries; `nullable_entries` exposes every entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtFields {
    values: HeaderMap,
    null_values: BTreeSet<String>,
    null_key_value: Option<Option<String>>,
}

impl ExtFields {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.values.len() + self.null_values.len() + usize::from(self.null_key_value.is_some())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key) || self.null_values.contains(key)
    }

    pub fn contains_null_key(&self) -> bool {
        self.null_key_value.is_some()
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.values.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut String> {
        self.values.get_mut(key)
    }

    pub fn get_nullable(&self, key: Option<&str>) -> Option<Option<&str>> {
        match key {
            Some(key) => self
                .values
                .get(key)
                .map(|value| Some(value.as_str()))
                .or_else(|| self.null_values.contains(key).then_some(None)),
            None => self.null_key_value.as_ref().map(Option::as_deref),
        }
    }

    pub fn insert(&mut self, key: String, value: String) -> Option<String> {
        self.insert_nullable(Some(key), Some(value)).flatten()
    }

    /// Inserts a Java-compatible nullable key/value pair.
    ///
    /// The nested option distinguishes a missing key from a present key whose
    /// value is null.
    pub fn insert_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Option<Option<String>> {
        match key {
            Some(key) => {
                let previous = if let Some(value) = self.values.remove(&key) {
                    Some(Some(value))
                } else if self.null_values.remove(&key) {
                    Some(None)
                } else {
                    None
                };
                match value {
                    Some(value) => {
                        self.values.insert(key, value);
                    }
                    None => {
                        self.null_values.insert(key);
                    }
                }
                previous
            }
            None => self.null_key_value.replace(value),
        }
    }

    /// Java `HashMap.putIfAbsent`: a present null value is replaced.
    pub fn put_if_absent_nullable(&mut self, key: Option<String>, value: Option<String>) {
        if self.get_nullable(key.as_deref()).flatten().is_none() {
            self.insert_nullable(key, value);
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.remove_nullable(Some(key.to_owned())).flatten()
    }

    pub fn remove_nullable(&mut self, key: Option<String>) -> Option<Option<String>> {
        match key {
            Some(key) => {
                if let Some(value) = self.values.remove(&key) {
                    Some(Some(value))
                } else if self.null_values.remove(&key) {
                    Some(None)
                } else {
                    None
                }
            }
            None => self.null_key_value.take(),
        }
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.null_values.clear();
        self.null_key_value = None;
    }

    /// Iterates all fields in deterministic key order, including null entries.
    pub fn nullable_entries(&self) -> impl Iterator<Item = (Option<&str>, Option<&str>)> + '_ {
        let mut entries = Vec::with_capacity(self.len());
        entries.extend(
            self.values
                .iter()
                .map(|(key, value)| (Some(key.as_str()), Some(value.as_str()))),
        );
        entries.extend(
            self.null_values
                .iter()
                .map(|key| (Some(key.as_str()), None)),
        );
        if let Some(value) = &self.null_key_value {
            entries.push((None, value.as_deref()));
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries.into_iter()
    }

    pub(crate) fn non_null_fields(&self) -> &HeaderMap {
        &self.values
    }
}

impl Deref for ExtFields {
    type Target = HeaderMap;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl Extend<(String, String)> for ExtFields {
    fn extend<T: IntoIterator<Item = (String, String)>>(&mut self, iter: T) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl From<HeaderMap> for ExtFields {
    fn from(values: HeaderMap) -> Self {
        Self {
            values,
            ..Self::default()
        }
    }
}

impl<const N: usize> From<[(String, String); N]> for ExtFields {
    fn from(values: [(String, String); N]) -> Self {
        values.into_iter().collect()
    }
}

impl FromIterator<(String, String)> for ExtFields {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        Self {
            values: iter.into_iter().collect(),
            ..Self::default()
        }
    }
}

impl PartialEq<HeaderMap> for ExtFields {
    fn eq(&self, other: &HeaderMap) -> bool {
        self.null_values.is_empty() && self.null_key_value.is_none() && self.values == *other
    }
}

impl fmt::Display for ExtFields {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("{")?;
        for (index, (key, value)) in self.nullable_entries().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(
                formatter,
                "{}={}",
                key.unwrap_or("null"),
                value.unwrap_or("null")
            )?;
        }
        formatter.write_str("}")
    }
}

/// Fastjson's default map writer omits null values and stringifies a null key.
impl Serialize for ExtFields {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let entries = self
            .nullable_entries()
            .filter_map(|(key, value)| value.map(|value| (key.unwrap_or("null"), value)))
            .collect::<Vec<_>>();
        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, value) in entries {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for ExtFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = BTreeMap::<String, Option<String>>::deserialize(deserializer)?;
        let mut result = Self::new();
        for (key, value) in fields {
            result.insert_nullable(Some(key), value);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_entries_match_java_map_views_and_writer_rules() {
        let mut fields = ExtFields::new();
        fields.insert_nullable(None, Some("null-key".into()));
        fields.insert_nullable(Some("null-value".into()), None);
        fields.insert("kept".into(), "value".into());

        assert!(fields.contains_null_key());
        assert!(fields.contains_key("null-value"));
        assert_eq!(fields.get_nullable(None), Some(Some("null-key")));
        assert_eq!(fields.get_nullable(Some("null-value")), Some(None));
        assert_eq!(fields.get("null-value"), None);
        assert_eq!(fields.len(), 3);
        assert_eq!(
            fields.to_string(),
            "{null=null-key, kept=value, null-value=null}"
        );

        let json = serde_json::to_value(&fields).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"null": "null-key", "kept": "value"})
        );
        let decoded: ExtFields =
            serde_json::from_value(serde_json::json!({"field": null})).unwrap();
        assert_eq!(decoded.get_nullable(Some("field")), Some(None));
    }

    #[test]
    fn nullable_put_if_absent_replaces_only_null_values() {
        let mut fields = ExtFields::new();
        fields.insert_nullable(Some("null".into()), None);
        fields.insert_nullable(Some("value".into()), Some("first".into()));
        fields.put_if_absent_nullable(Some("null".into()), Some("replaced".into()));
        fields.put_if_absent_nullable(Some("value".into()), Some("ignored".into()));

        assert_eq!(fields.get("null").map(String::as_str), Some("replaced"));
        assert_eq!(fields.get("value").map(String::as_str), Some("first"));
    }

    #[test]
    fn replacing_nullable_entries_keeps_one_value_per_key() {
        let mut fields = ExtFields::new();
        fields.insert_nullable(Some("field".into()), None);
        fields.insert("field".into(), "value".into());
        fields.get_mut("field").unwrap().push_str("-updated");

        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields.get_nullable(Some("field")),
            Some(Some("value-updated"))
        );
        assert_eq!(
            fields.nullable_entries().collect::<Vec<_>>(),
            vec![(Some("field"), Some("value-updated"))]
        );
    }
}
