use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, RuntimeError},
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

#[tokio::test]
async fn live_table_keeps_sharing_hooks_and_identity_cleanup_across_explicit_managers() {
    use rocketmq_classic_remoting::client::RpcHook;
    struct Hook;
    impl RpcHook for Hook {}
    let origin = ClientRuntimeManager::new();
    let target = ClientRuntimeManager::new();
    let live = target.factory_table();
    let mut config = ClientConfig::new("cross-manager-table");
    config.set_namesrv_addr(Some(String::new()));
    let id = config.build_client_id();
    let hook: Arc<dyn RpcHook> = Arc::new(Hook);
    let first = origin
        .get_or_create_with_hook(&config, Some(hook.clone()))
        .await
        .unwrap();
    origin.remove_client_factory_by_id(&id).await.unwrap();
    let replacement = origin
        .get_or_create_with_hook(&config, Some(hook.clone()))
        .await
        .unwrap();
    live.put(&id, first.clone()).await.unwrap();
    assert!(Arc::ptr_eq(
        &first,
        &target
            .get_or_create_with_hook(&config, Some(hook.clone()))
            .await
            .unwrap()
    ));
    assert!(Arc::ptr_eq(
        &first,
        &target.get_or_create(&config).await.unwrap()
    ));
    assert!(Arc::ptr_eq(
        &first,
        &target
            .get_or_create_with_hook(&config, Some(Arc::new(Hook)))
            .await
            .unwrap()
    ));
    let mut incompatible = config.clone();
    incompatible.set_namespace_v2(Some("different".into()));
    assert!(Arc::ptr_eq(
        &first,
        &target
            .get_or_create_with_hook(&incompatible, Some(hook.clone()))
            .await
            .unwrap()
    ));
    first.start().await.unwrap();
    assert!(first.shutdown().await.unwrap());
    assert!(live.is_empty().unwrap());
    assert!(Arc::ptr_eq(
        &replacement,
        &origin
            .get_or_create_with_hook(&config, Some(hook.clone()))
            .await
            .unwrap()
    ));
    live.put(&id, replacement.clone()).await.unwrap();
    live.put("alias", replacement.clone()).await.unwrap();
    replacement.start().await.unwrap();
    assert!(replacement.shutdown().await.unwrap());
    assert!(!live.contains_key(&id).unwrap());
    assert!(Arc::ptr_eq(
        &live.get("alias").unwrap().unwrap(),
        &replacement
    ));
    live.clear().await.unwrap();
    assert!(live.is_empty().unwrap());
    assert!(origin.factory_table().is_empty().unwrap());
    drop(target);
    live.put(&id, first.clone()).await.unwrap();
    assert!(live.contains_key(&id).unwrap());
    live.clear().await.unwrap();
}

#[tokio::test]
async fn nullable_factory_table_reads_match_invoked_java_null_key_failures() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    assert_eq!(java["nullKeyGet"], "NullPointerException");
    assert_eq!(java["nullKeyContainsKey"], "NullPointerException");
    assert_eq!(java["nullKeyGetOrDefault"], "NullPointerException");
    assert_eq!(java["nullValueContainsValue"], "NullPointerException");
    assert_eq!(java["nullDefaultOnPresent"], "MAPPED");
    assert_eq!(java["nullDefaultOnMissing"], "NULL");

    let manager = ClientRuntimeManager::new();
    let table = manager.factory_table();
    assert!(matches!(
        table.get_nullable(None),
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert!(matches!(
        table.contains_key_nullable(None),
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert!(matches!(
        table.contains_value_nullable(None),
        Err(RuntimeError::InvalidRoute("null factory value"))
    ));
    let default_manager = ClientRuntimeManager::new();
    let default = default_manager
        .get_or_create(&ClientConfig::new("factory-null-read-default"))
        .await
        .unwrap();
    assert!(matches!(
        table.get_or_default_nullable(None, Some(default.clone())),
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert!(
        table
            .get_or_default_nullable(Some("missing"), None)
            .unwrap()
            .is_none()
    );
    table.put("present", default.clone()).await.unwrap();
    assert!(table.contains_value_nullable(Some(&default)).unwrap());
    assert!(
        table
            .get_or_default_nullable(Some("present"), None)
            .unwrap()
            .is_some_and(|mapped| Arc::ptr_eq(&mapped, &default))
    );
    assert!(table.get_nullable(Some("missing")).unwrap().is_none());
    assert!(!table.contains_key_nullable(Some("missing")).unwrap());
}

#[tokio::test]
async fn nullable_get_or_create_config_inputs_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableGetOrCreateInputs"];
    assert_eq!(java["nullConfig"], "NullPointerException");
    assert_eq!(java["nullConfigWithNullHook"], "NullPointerException");

    let manager = ClientRuntimeManager::new();
    let table = manager.factory_table();
    assert!(table.is_empty().unwrap());
    assert!(matches!(
        manager.get_or_create_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null client config"))
    ));
    assert!(matches!(
        manager.get_or_create_with_hook_nullable(None, None).await,
        Err(RuntimeError::InvalidRoute("null client config"))
    ));
    assert!(table.is_empty().unwrap());

    let config = ClientConfig::new("nullable-get-or-create-valid");
    let first = manager.get_or_create_nullable(Some(&config)).await.unwrap();
    let repeated = manager
        .get_or_create_with_hook_nullable(Some(&config), None)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &repeated));
    assert_eq!(table.len().unwrap(), 1);
}

#[tokio::test]
async fn nullable_factory_table_mutations_match_invoked_java_null_failures() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableTableMutations"];
    for key in [
        "factoryPutNullKey",
        "factoryPutNullValue",
        "factoryPutIfAbsentNullKey",
        "factoryPutIfAbsentNullValue",
        "factoryReplaceNullKey",
        "factoryReplaceNullValue",
        "factoryConditionalReplaceNullKey",
        "factoryConditionalReplaceNullExpected",
        "factoryConditionalReplaceNullValue",
        "factoryRemoveNullKey",
        "factoryConditionalRemoveNullKey",
    ] {
        assert_eq!(java[key], "NullPointerException", "{key}");
    }
    assert_eq!(java["factoryConditionalRemoveNullValue"], false);

    macro_rules! assert_null {
        ($operation:expr, $message:literal) => {
            assert!(matches!(
                $operation,
                Err(RuntimeError::InvalidRoute($message))
            ));
        };
    }

    let manager = ClientRuntimeManager::new();
    let table = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("nullable-factory-mutation-first"))
        .await
        .unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("nullable-factory-mutation-second"))
        .await
        .unwrap();
    let initial_keys: BTreeSet<_> = table.snapshot().unwrap().into_keys().collect();

    assert_null!(
        table.put_nullable(None, Some(first.clone())).await,
        "null factory key"
    );
    assert_null!(
        table.put_nullable(Some("put"), None).await,
        "null factory value"
    );
    assert_null!(
        table
            .put_if_absent_nullable(None, Some(first.clone()))
            .await,
        "null factory key"
    );
    assert_null!(
        table
            .put_if_absent_nullable(Some("put-if-absent"), None)
            .await,
        "null factory value"
    );
    assert_null!(
        table.replace_nullable(None, Some(first.clone())).await,
        "null factory key"
    );
    assert_null!(
        table.replace_nullable(Some("replace"), None).await,
        "null factory value"
    );
    assert_null!(
        table
            .replace_if_equal_nullable(None, Some(&first), Some(second.clone()))
            .await,
        "null factory key"
    );
    assert_null!(
        table
            .replace_if_equal_nullable(Some("replace-old"), None, Some(second.clone()))
            .await,
        "null factory value"
    );
    assert_null!(
        table
            .replace_if_equal_nullable(Some("replace-new"), Some(&first), None)
            .await,
        "null factory value"
    );
    assert_null!(table.remove_nullable(None).await, "null factory key");
    assert_null!(
        table.remove_if_equal_nullable(None, Some(&first)).await,
        "null factory key"
    );
    assert!(
        !table
            .remove_if_equal_nullable(Some("remove"), None)
            .await
            .unwrap()
    );
    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>(),
        initial_keys
    );
}

#[tokio::test]
async fn nullable_factory_table_views_match_invoked_java_null_inputs() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableTableViewInputs"];
    for key in [
        "factoryKeyContainsNull",
        "factoryKeyRemoveNull",
        "factoryValuesContainsNull",
        "factoryKeyRemoveIfNull",
        "factoryValuesRemoveIfNull",
        "factoryEntriesRemoveIfNull",
        "factoryKeyRemoveAllNull",
        "factoryValuesRemoveAllNull",
        "factoryEntriesRemoveAllNull",
        "factoryKeyRetainAllNull",
        "factoryValuesRetainAllNull",
        "factoryEntriesRetainAllNull",
    ] {
        assert_eq!(java[key], "NullPointerException", "{key}");
    }
    for key in [
        "factoryValuesRemoveNull",
        "factoryEntriesContainsNull",
        "factoryEntriesRemoveNull",
        "factoryEntriesContainsNullKey",
        "factoryEntriesRemoveNullKey",
        "factoryEntriesContainsNullValue",
        "factoryEntriesRemoveNullValue",
    ] {
        assert_eq!(java[key], false, "{key}");
    }

    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("nullable-factory-view-inputs"))
        .await
        .unwrap();
    let table = manager.factory_table();
    let key_set = table.key_set_view();
    let values = table.values_view();
    let entries = table.entry_set_view();
    let initial_keys: BTreeSet<_> = table.snapshot().unwrap().into_keys().collect();

    assert!(matches!(
        key_set.contains_nullable(None),
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert!(matches!(
        key_set.remove_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert!(matches!(
        key_set.remove_if_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view predicate"))
    ));
    assert!(matches!(
        key_set.remove_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view collection"))
    ));
    assert!(matches!(
        key_set.retain_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view collection"))
    ));

    assert!(matches!(
        values.contains_nullable(None),
        Err(RuntimeError::InvalidRoute("null factory value"))
    ));
    assert!(!values.remove_nullable(None).await.unwrap());
    assert!(matches!(
        values.remove_if_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view predicate"))
    ));
    assert!(matches!(
        values.remove_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view collection"))
    ));
    assert!(matches!(
        values.retain_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view collection"))
    ));

    assert!(!entries.contains_nullable(None).unwrap());
    assert!(!entries.remove_nullable(None).await.unwrap());
    assert!(
        !entries
            .contains_entry_nullable(None, Some(&runtime))
            .unwrap()
    );
    assert!(
        !entries
            .contains_entry_nullable(Some(runtime.client_id()), None)
            .unwrap()
    );
    assert!(
        !entries
            .remove_entry_nullable(None, Some(&runtime))
            .await
            .unwrap()
    );
    assert!(
        !entries
            .remove_entry_nullable(Some(runtime.client_id()), None)
            .await
            .unwrap()
    );
    assert!(matches!(
        entries.remove_if_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view predicate"))
    ));
    assert!(matches!(
        entries.remove_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view collection"))
    ));
    assert!(matches!(
        entries.retain_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory view collection"))
    ));

    let java = &fixture["managerTable"]["nullableEntrySetValueInputs"];
    assert_eq!(java["factoryEntrySetValueNull"], "NullPointerException");
    assert_eq!(java["factoryEntrySetValueNullEntryUnchanged"], true);
    assert_eq!(java["factoryEntrySetValueNullTableUnchanged"], true);
    let entry = entries
        .entry_snapshots()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let entry_key = entry.key().to_owned();
    let entry_value = entry.value().unwrap();
    assert!(matches!(
        entry.set_value_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory value"))
    ));
    assert!(Arc::ptr_eq(&entry.value().unwrap(), &entry_value));
    assert!(Arc::ptr_eq(
        &table.get(&entry_key).unwrap().unwrap(),
        &entry_value
    ));

    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>(),
        initial_keys
    );
}

#[tokio::test]
async fn nullable_factory_table_view_elements_match_invoked_java_collections() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableTableViewNullElements"];
    let manager = ClientRuntimeManager::new();
    let table = manager.factory_table();
    let config = ClientConfig::new("nullable-factory-view-elements");
    let key = config.build_client_id();
    let value = manager.get_or_create(&config).await.unwrap();
    table.put(&key, value.clone()).await.unwrap();
    let assert_keys = |fixture_field: &str| {
        let mut expected: Vec<String> =
            serde_json::from_value(java[fixture_field].clone()).unwrap();
        for expected_key in &mut expected {
            if expected_key == "manager-copy@DEFAULT" {
                *expected_key = key.clone();
            }
        }
        let actual: Vec<_> = table.snapshot().unwrap().into_keys().collect();
        assert_eq!(actual, expected, "{fixture_field}");
    };

    assert_eq!(
        java["factoryKeyRemoveAllNullElement"],
        "NullPointerException"
    );
    assert!(matches!(
        table
            .key_set_view()
            .remove_all_with_nullable_elements(Some(&[None]))
            .await,
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert_keys("factoryKeyRemoveAllNullElementKeysAfter");

    assert_eq!(
        java["factoryKeyRemoveAllNullThenMatch"],
        "NullPointerException"
    );
    assert!(matches!(
        table
            .key_set_view()
            .remove_all_with_nullable_elements(Some(&[None, Some(key.clone())]))
            .await,
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert_keys("factoryKeyRemoveAllNullThenMatchKeysAfter");

    assert_eq!(
        java["factoryKeyRemoveAllMatchThenNull"],
        "NullPointerException"
    );
    assert!(matches!(
        table
            .key_set_view()
            .remove_all_with_nullable_elements(Some(&[Some(key.clone()), None]))
            .await,
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert_keys("factoryKeyRemoveAllMatchThenNullKeysAfter");

    table.put(&key, value.clone()).await.unwrap();
    assert_eq!(java["factoryKeyRetainAllNullElement"], true);
    assert_eq!(
        table
            .key_set_view()
            .retain_all_with_nullable_elements(Some(&[None]))
            .await
            .unwrap(),
        1
    );
    assert_keys("factoryKeyRetainAllNullElementKeysAfter");
    table.put(&key, value.clone()).await.unwrap();
    assert_eq!(java["factoryKeyRetainAllNullAndMatch"], false);
    assert_eq!(
        table
            .key_set_view()
            .retain_all_with_nullable_elements(Some(&[None, Some(key.clone())]))
            .await
            .unwrap(),
        0
    );
    assert_keys("factoryKeyRetainAllNullAndMatchKeysAfter");

    let values = table.values_view();
    assert_eq!(java["factoryValuesRemoveAllNullElement"], false);
    assert_eq!(
        values
            .remove_all_with_nullable_elements(Some(&[None]))
            .await
            .unwrap(),
        0
    );
    assert_keys("factoryValuesRemoveAllNullElementKeysAfter");
    assert_eq!(java["factoryValuesRemoveAllNullThenMatch"], true);
    assert_eq!(
        values
            .remove_all_with_nullable_elements(Some(&[None, Some(value.clone())]))
            .await
            .unwrap(),
        1
    );
    assert_keys("factoryValuesRemoveAllNullThenMatchKeysAfter");

    table.put(&key, value.clone()).await.unwrap();
    assert_eq!(java["factoryValuesRetainAllNullElement"], true);
    assert_eq!(
        values
            .retain_all_with_nullable_elements(Some(&[None]))
            .await
            .unwrap(),
        1
    );
    assert_keys("factoryValuesRetainAllNullElementKeysAfter");
    table.put(&key, value.clone()).await.unwrap();
    assert_eq!(java["factoryValuesRetainAllNullAndMatch"], false);
    assert_eq!(
        values
            .retain_all_with_nullable_elements(Some(&[None, Some(value.clone())]))
            .await
            .unwrap(),
        0
    );
    assert_keys("factoryValuesRetainAllNullAndMatchKeysAfter");

    let entries = table.entry_set_view();
    let null_entry: Option<(Option<String>, Option<Arc<_>>)> = None;
    let matching_entry = Some((Some(key.clone()), Some(value.clone())));
    assert_eq!(java["factoryEntriesRemoveAllNullElement"], false);
    assert_eq!(
        entries
            .remove_all_with_nullable_elements(Some(&[null_entry.clone()]))
            .await
            .unwrap(),
        0
    );
    assert_keys("factoryEntriesRemoveAllNullElementKeysAfter");
    assert_eq!(java["factoryEntriesRemoveAllNullThenMatch"], true);
    assert_eq!(
        entries
            .remove_all_with_nullable_elements(Some(&[null_entry.clone(), matching_entry.clone()]))
            .await
            .unwrap(),
        1
    );
    assert_keys("factoryEntriesRemoveAllNullThenMatchKeysAfter");

    table.put(&key, value.clone()).await.unwrap();
    assert_eq!(java["factoryEntriesRetainAllNullElement"], true);
    assert_eq!(
        entries
            .retain_all_with_nullable_elements(Some(&[null_entry.clone()]))
            .await
            .unwrap(),
        1
    );
    assert_keys("factoryEntriesRetainAllNullElementKeysAfter");
    table.put(&key, value.clone()).await.unwrap();
    assert_eq!(java["factoryEntriesRetainAllNullAndMatch"], false);
    assert_eq!(
        entries
            .retain_all_with_nullable_elements(Some(&[null_entry, matching_entry]))
            .await
            .unwrap(),
        0
    );
    assert_keys("factoryEntriesRetainAllNullAndMatchKeysAfter");
    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>(),
        [key].into()
    );
}

#[tokio::test]
async fn nullable_factory_remapping_inputs_match_invoked_java_null_failures() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableRemappingInputs"];
    for key in [
        "factoryComputeIfAbsentNullKey",
        "factoryComputeIfAbsentNullFunction",
        "factoryComputeIfPresentNullKey",
        "factoryComputeIfPresentNullFunction",
        "factoryComputeNullKey",
        "factoryComputeNullFunction",
        "factoryMergeNullKey",
        "factoryMergeNullValue",
        "factoryMergeNullFunction",
    ] {
        assert_eq!(java[key], "NullPointerException", "{key}");
    }

    macro_rules! assert_null {
        ($operation:expr, $message:literal) => {
            assert!(matches!(
                $operation,
                Err(RuntimeError::InvalidRoute($message))
            ));
        };
    }

    let manager = ClientRuntimeManager::new();
    let table = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("nullable-factory-remapping-first"))
        .await
        .unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("nullable-factory-remapping-second"))
        .await
        .unwrap();
    let initial_keys: BTreeSet<_> = table.snapshot().unwrap().into_keys().collect();
    let mut if_absent = |_: &str| Some(first.clone());
    let mut if_present = |_: &str, _: &Arc<_>| Some(second.clone());
    let mut compute = |_: &str, _: Option<&Arc<_>>| Some(first.clone());
    let mut merge = |_: &Arc<_>, _: &Arc<_>| Some(second.clone());

    assert_null!(
        table
            .compute_if_absent_nullable(None, Some(&mut if_absent))
            .await,
        "null factory key"
    );
    assert_null!(
        table.compute_if_absent_nullable(Some("key"), None).await,
        "null factory remapping function"
    );
    assert_null!(
        table
            .compute_if_present_nullable(None, Some(&mut if_present))
            .await,
        "null factory key"
    );
    assert_null!(
        table.compute_if_present_nullable(Some("key"), None).await,
        "null factory remapping function"
    );
    assert_null!(
        table.compute_nullable(None, Some(&mut compute)).await,
        "null factory key"
    );
    assert_null!(
        table.compute_nullable(Some("key"), None).await,
        "null factory remapping function"
    );
    assert_null!(
        table
            .merge_nullable(None, Some(first.clone()), Some(&mut merge))
            .await,
        "null factory key"
    );
    assert_null!(
        table
            .merge_nullable(Some("key"), None, Some(&mut merge))
            .await,
        "null factory value"
    );
    assert_null!(
        table
            .merge_nullable(Some("key"), Some(first.clone()), None)
            .await,
        "null factory remapping function"
    );
    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>(),
        initial_keys
    );

    assert!(Arc::ptr_eq(
        &table
            .compute_if_absent_nullable(Some("remapping"), Some(&mut if_absent))
            .await
            .unwrap()
            .unwrap(),
        &first
    ));
    assert!(Arc::ptr_eq(
        &table
            .compute_if_present_nullable(Some("remapping"), Some(&mut if_present))
            .await
            .unwrap()
            .unwrap(),
        &second
    ));
    assert!(Arc::ptr_eq(
        &table
            .compute_nullable(Some("remapping"), Some(&mut compute))
            .await
            .unwrap()
            .unwrap(),
        &first
    ));
    assert!(Arc::ptr_eq(
        &table
            .merge_nullable(Some("remapping"), Some(second.clone()), Some(&mut merge))
            .await
            .unwrap()
            .unwrap(),
        &second
    ));
}

#[tokio::test]
async fn nullable_factory_bulk_and_traversal_callbacks_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableBulkInputs"];
    assert_eq!(java["factoryPutAllNullMap"], "NullPointerException");
    assert_eq!(java["factoryForEachNullAction"], "NullPointerException");
    assert_eq!(
        java["factoryReplaceAllNullFunction"],
        "NullPointerException"
    );

    let manager = ClientRuntimeManager::new();
    let table = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("manager-copy"))
        .await
        .unwrap();
    let second = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("nullable-factory-bulk-second"))
        .await
        .unwrap();
    table
        .put("manager-copy@DEFAULT", first.clone())
        .await
        .unwrap();
    let initial_keys: BTreeSet<_> = table.snapshot().unwrap().into_keys().collect();
    assert!(matches!(
        table.put_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null factory putAll map"))
    ));
    assert!(matches!(
        table.for_each_nullable(None),
        Err(RuntimeError::InvalidRoute("null factory forEach action"))
    ));
    assert!(matches!(
        table.replace_all_nullable(None).await,
        Err(RuntimeError::InvalidRoute(
            "null factory replaceAll function"
        ))
    ));
    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>(),
        initial_keys
    );

    assert_eq!(java["factoryPutAllNullKey"], "NullPointerException");
    let null_key_rows = [
        (
            Some("put-all-before-null-key".to_owned()),
            Some(first.clone()),
        ),
        (None, Some(first.clone())),
        (
            Some("put-all-after-null-key".to_owned()),
            Some(second.clone()),
        ),
    ];
    assert!(matches!(
        table.put_all_nullable_rows(Some(&null_key_rows)).await,
        Err(RuntimeError::InvalidRoute("null factory key"))
    ));
    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>(),
        serde_json::from_value(java["factoryPutAllNullKeyKeysAfter"].clone()).unwrap()
    );
    assert!(
        table
            .remove("put-all-before-null-key")
            .await
            .unwrap()
            .is_some()
    );

    assert_eq!(java["factoryPutAllNullValue"], "NullPointerException");
    let null_value_rows = [
        (
            Some("put-all-before-null-value".to_owned()),
            Some(first.clone()),
        ),
        (Some("put-all-null-value".to_owned()), None),
        (
            Some("put-all-after-null-value".to_owned()),
            Some(second.clone()),
        ),
    ];
    assert!(matches!(
        table.put_all_nullable_rows(Some(&null_value_rows)).await,
        Err(RuntimeError::InvalidRoute("null factory value"))
    ));
    assert_eq!(
        table
            .snapshot()
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>(),
        serde_json::from_value(java["factoryPutAllNullValueKeysAfter"].clone()).unwrap()
    );

    let mut visited = 0;
    let mut action = |_: &str, _: &Arc<_>| visited += 1;
    table.for_each_nullable(Some(&mut action)).unwrap();
    assert_eq!(visited, initial_keys.len() + 1);
    let entries = BTreeMap::from([("added".to_owned(), second.clone())]);
    table.put_all_nullable(Some(&entries)).await.unwrap();
    assert!(Arc::ptr_eq(&table.get("added").unwrap().unwrap(), &second));
    let mut replace = |_: &str, _: &Arc<_>| second.clone();
    table
        .replace_all_nullable(Some(&mut replace))
        .await
        .unwrap();
    assert!(
        table
            .snapshot()
            .unwrap()
            .values()
            .all(|value| Arc::ptr_eq(value, &second))
    );
}

#[tokio::test]
async fn hash_map_nullable_put_all_order_matches_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableBulkInputs"];
    assert_eq!(
        java["factoryHashMapPutAllNullValue"],
        "NullPointerException"
    );
    let visit_order: Vec<String> =
        serde_json::from_value(java["factoryHashMapPutAllNullValueVisitOrder"].clone()).unwrap();
    let null_value_key = java["factoryHashMapPutAllNullValueKey"].as_str().unwrap();
    assert_eq!(visit_order.len(), 3);
    assert_eq!(visit_order[1], null_value_key);

    let table = ClientRuntimeManager::new().factory_table();
    let value = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("hash-map-put-all-value"))
        .await
        .unwrap();
    table.put("seed", value.clone()).await.unwrap();
    let rows: Vec<_> = visit_order
        .iter()
        .map(|key| {
            (
                Some(key.clone()),
                (key != null_value_key).then(|| value.clone()),
            )
        })
        .collect();
    assert!(matches!(
        table.put_all_nullable_rows(Some(&rows)).await,
        Err(RuntimeError::InvalidRoute("null factory value"))
    ));
    let expected: Vec<String> =
        serde_json::from_value(java["factoryHashMapPutAllNullValueKeysAfter"].clone()).unwrap();
    let mut actual: Vec<_> = table.snapshot().unwrap().into_keys().collect();
    actual.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn tree_map_nullable_put_all_accepts_generic_source_iteration() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableBulkInputs"];
    assert_eq!(
        java["factoryTreeMapPutAllNullValue"],
        "NullPointerException"
    );
    let visit_order: Vec<String> =
        serde_json::from_value(java["factoryTreeMapPutAllNullValueVisitOrder"].clone()).unwrap();
    assert_eq!(
        visit_order,
        [
            "a-put-all-before-null-value",
            "m-put-all-null-value",
            "z-put-all-after-null-value"
        ]
    );
    let null_value_key = java["factoryTreeMapPutAllNullValueKey"].as_str().unwrap();

    let table_manager = ClientRuntimeManager::new();
    let table = table_manager.factory_table();
    let value = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("tree-map-put-all-value"))
        .await
        .unwrap();
    table.put("seed", value.clone()).await.unwrap();
    let source: BTreeMap<_, _> = visit_order
        .into_iter()
        .map(|key| {
            let mapped = (key != null_value_key).then(|| value.clone());
            (key, mapped)
        })
        .collect();
    let entries = source.into_iter().map(|(key, value)| (Some(key), value));
    assert!(matches!(
        table.put_all_nullable_iter(entries).await,
        Err(RuntimeError::InvalidRoute("null factory value"))
    ));
    let expected: Vec<String> =
        serde_json::from_value(java["factoryTreeMapPutAllNullValueKeysAfter"].clone()).unwrap();
    let mut actual: Vec<_> = table.snapshot().unwrap().into_keys().collect();
    actual.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn nullable_factory_replace_all_results_match_invoked_java_partial_updates() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"]["nullableReplaceAllResults"];
    assert_eq!(java["factoryReplaceAllNullResult"], "NullPointerException");
    let java_visited: Vec<String> =
        serde_json::from_value(java["factoryReplaceAllNullResultVisited"].clone()).unwrap();
    assert_eq!(java_visited.len(), 2);
    assert!(java_visited.contains(&"a".to_owned()));
    assert!(java_visited.contains(&"b".to_owned()));
    assert_eq!(
        java["factoryReplaceAllNullResultAUpdated"],
        java_visited[0] == "a"
    );
    assert_eq!(
        java["factoryReplaceAllNullResultBOriginal"],
        java_visited[0] == "a"
    );

    let table_manager = ClientRuntimeManager::new();
    let table = table_manager.factory_table();
    let first = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("replace-all-null-result-first"))
        .await
        .unwrap();
    let replacement = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("replace-all-null-result-replacement"))
        .await
        .unwrap();
    table.put("a", first.clone()).await.unwrap();
    table.put("b", first.clone()).await.unwrap();

    let mut visited = Vec::new();
    {
        let replacement_for_callback = replacement.clone();
        let mut remapping = |key: &str, _current: &Arc<_>| {
            visited.push(key.to_owned());
            (visited.len() == 1).then(|| replacement_for_callback.clone())
        };
        assert!(matches!(
            table
                .replace_all_with_nullable_results(Some(&mut remapping))
                .await,
            Err(RuntimeError::InvalidRoute("null factory replaceAll result"))
        ));
    }
    assert_eq!(visited.len(), 2);
    assert!(visited.contains(&"a".to_owned()));
    assert!(visited.contains(&"b".to_owned()));
    assert!(Arc::ptr_eq(
        &table.get(&visited[0]).unwrap().unwrap(),
        &replacement
    ));
    assert!(Arc::ptr_eq(
        &table.get(&visited[1]).unwrap().unwrap(),
        &first
    ));
}

#[tokio::test]
async fn nullable_manager_removal_overloads_match_invoked_java_null_key_failures() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerRemoval"];
    assert_eq!(java["removeByNullId"], "NullPointerException");
    assert_eq!(java["removeByNullIdAndInstance"], "NullPointerException");
    assert_eq!(java["removeByNullInstance"], "RETURNED");
    assert_eq!(
        java["removeByNullIdAndNullInstance"],
        "NullPointerException"
    );

    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("nullable-manager-removal"))
        .await
        .unwrap();
    assert!(matches!(
        manager.remove_client_factory_nullable(None, Some(&runtime)),
        Err(RuntimeError::InvalidRoute("null client id"))
    ));
    assert!(matches!(
        manager.remove_client_factory_nullable(None, None),
        Err(RuntimeError::InvalidRoute("null client id"))
    ));
    assert!(
        !manager
            .remove_client_factory_nullable(Some(runtime.client_id()), None)
            .unwrap()
    );
    assert!(matches!(
        manager.remove_client_factory_by_id_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null client id"))
    ));
    let selected = manager
        .factory_table()
        .get_nullable(Some(runtime.client_id()))
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&selected, &runtime));
}

#[tokio::test]
async fn concurrent_put_if_absent_publishes_one_identity_and_clear_releases_owners() {
    let source = ClientRuntimeManager::new();
    let first = source
        .get_or_create(&ClientConfig::new("first-table-value"))
        .await
        .unwrap();
    let second = source
        .get_or_create(&ClientConfig::new("second-table-value"))
        .await
        .unwrap();
    let target = ClientRuntimeManager::new();
    let live = target.factory_table();
    let mut tasks = Vec::new();
    for i in 0..64 {
        let live = live.clone();
        let value = if i % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        tasks.push(tokio::spawn(async move {
            live.put_if_absent("alias", value.clone())
                .await
                .map(|previous| (previous, value))
        }));
    }
    let mut inserted = 0;
    let mut observed = None;
    for task in tasks {
        let (previous, candidate) = task.await.unwrap().unwrap();
        match previous {
            None => {
                inserted += 1;
                observed = Some(candidate);
            }
            Some(previous) => {
                assert!(Arc::ptr_eq(&previous, &first) || Arc::ptr_eq(&previous, &second));
            }
        }
    }
    assert_eq!(inserted, 1);
    let selected = live.get("alias").unwrap().unwrap();
    assert!(Arc::ptr_eq(&selected, &observed.take().unwrap()));
    assert_eq!(live.len().unwrap(), 1);
    let weak_first = Arc::downgrade(&first);
    let weak_second = Arc::downgrade(&second);
    drop(source);
    drop(first);
    drop(second);
    drop(selected);
    live.clear().await.unwrap();
    assert!(weak_first.upgrade().is_none());
    assert!(weak_second.upgrade().is_none());
}

#[tokio::test]
async fn retained_factory_table_and_atomic_mutations_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let second_view = live.clone();
    let key_view = live.key_set_view();
    let values_view = live.values_view();
    let entry_view = live.entry_set_view();
    let config = ClientConfig::new("table-oracle");
    let id = config.build_client_id();
    assert!(live.is_empty().unwrap());
    let first = manager.get_or_create(&config).await.unwrap();
    manager.remove_client_factory_by_id(&id).await.unwrap();
    let second = manager.get_or_create(&config).await.unwrap();
    assert_eq!(
        Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &second),
        java["retainedViewSeesSecond"]
    );
    assert_eq!(
        live.contains_value(&second).unwrap(),
        java["containsValueSecond"]
    );
    assert_eq!(
        !live.contains_value(&first).unwrap(),
        java["containsValueAbsent"]
    );
    assert_eq!(
        Arc::ptr_eq(&live.get_or_default(&id, first.clone()).unwrap(), &second),
        java["getOrDefaultPresent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live.get_or_default("missing", first.clone()).unwrap(),
            &first
        ),
        java["getOrDefaultAbsent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live.put(&id, first.clone()).await.unwrap().unwrap(),
            &second
        ),
        java["putReturnsPrevious"]
    );
    assert_eq!(
        Arc::ptr_eq(&manager.get_or_create(&config).await.unwrap(), &first),
        java["managerSeesPut"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .put_if_absent(&id, second.clone())
                .await
                .unwrap()
                .unwrap(),
            &first
        ) && Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &first),
        java["putIfAbsentKeepsCurrent"]
    );
    assert_eq!(
        !live
            .replace_if_same(&id, &second, first.clone())
            .await
            .unwrap(),
        java["staleReplaceFails"]
    );
    assert_eq!(
        live.replace_if_same(&id, &first, second.clone())
            .await
            .unwrap(),
        java["matchingReplaceSucceeds"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live.replace(&id, first.clone()).await.unwrap().unwrap(),
            &second
        ),
        java["replaceReturnsPrevious"]
    );
    assert_eq!(
        !live.remove_if_same(&id, &second).await.unwrap(),
        java["staleRemoveFails"]
    );
    assert_eq!(
        live.remove_if_same(&id, &first).await.unwrap(),
        java["matchingRemoveSucceeds"]
    );
    assert_eq!(
        live.replace(&id, second.clone()).await.unwrap().is_none(),
        java["absentReplaceReturnsNull"]
    );
    assert_eq!(
        live.remove(&id).await.unwrap().is_none(),
        java["absentRemoveReturnsNull"]
    );
    assert_eq!(
        live.put_if_absent(&id, first.clone())
            .await
            .unwrap()
            .is_none(),
        java["absentPutIfAbsentReturnsNull"]
    );
    live.put("", second.clone()).await.unwrap();
    assert_eq!(
        live.len().unwrap(),
        java["sizeWithAlias"].as_u64().unwrap() as usize
    );
    assert!(live.contains_key("").unwrap());
    assert_eq!(
        Arc::ptr_eq(&live.get("").unwrap().unwrap(), &second),
        java["aliasHasSecond"]
    );
    let bulk_values = HashMap::from([
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    live.put_all(bulk_values).await.unwrap();
    assert_eq!(
        Arc::ptr_eq(&live.get("bulk-a").unwrap().unwrap(), &first)
            && Arc::ptr_eq(&live.get("bulk-b").unwrap().unwrap(), &second)
            && live.len().unwrap() == 4,
        java["putAllAdds"]
    );
    let expected = BTreeMap::from([
        (id.clone(), first.clone()),
        (String::new(), second.clone()),
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    let mut visited = BTreeMap::new();
    second_view
        .for_each(|key, value| {
            // A callback can reenter the table because traversal holds no manager lock.
            let _ = second_view.len().unwrap();
            visited.insert(
                key.to_owned(),
                expected
                    .get(key)
                    .is_some_and(|expected| Arc::ptr_eq(value, expected)),
            );
        })
        .unwrap();
    assert_eq!(
        visited.len() == expected.len() && visited.values().all(|matches| *matches),
        java["forEachVisitsCurrentMappings"]
    );
    assert_eq!(
        key_view.contains("bulk-a").unwrap() && key_view.len().unwrap() == 4,
        java["keyViewSeesBulk"]
    );
    assert_eq!(
        values_view.contains(&first).unwrap()
            && values_view.contains(&second).unwrap()
            && values_view.len().unwrap() == 4,
        java["valuesViewSeesBulk"]
    );
    assert_eq!(
        entry_view.contains("bulk-a", &first).unwrap() && entry_view.len().unwrap() == 4,
        java["entryViewSeesBulk"]
    );
    assert_eq!(
        key_view.remove_if(|key| key == "bulk-b").await.unwrap() > 0,
        java["keyViewRemovesBulk"]
    );
    assert_eq!(
        entry_view
            .remove_if(|key, value| key == "bulk-a" && Arc::ptr_eq(value, &first))
            .await
            .unwrap()
            > 0,
        java["entryViewRemovesBulk"]
    );
    assert_eq!(
        values_view
            .remove_if(|value| Arc::ptr_eq(value, &first))
            .await
            .unwrap()
            > 0,
        java["valuesViewRemovesCurrent"]
    );
    assert_eq!(
        !key_view.contains("bulk-b").unwrap()
            && !entry_view.contains("bulk-a", &first).unwrap()
            && !key_view.contains(&id).unwrap()
            && values_view.len().unwrap() == 1,
        java["viewsSeeRemovals"]
    );
    live.put("bulk-c", first.clone()).await.unwrap();
    live.put("bulk-d", second.clone()).await.unwrap();
    assert_eq!(
        key_view
            .remove_all(&BTreeSet::from(["bulk-c".to_owned()]))
            .await
            .unwrap()
            > 0,
        java["keyViewRemoveAll"]
    );
    assert_eq!(
        values_view.remove_all(&[second.clone()]).await.unwrap() > 0 && live.is_empty().unwrap(),
        java["valuesViewRemoveAll"]
    );
    live.put("keep", first.clone()).await.unwrap();
    live.put("drop", second.clone()).await.unwrap();
    let retained_entry = BTreeMap::from([("keep".to_owned(), first.clone())]);
    assert_eq!(
        entry_view.retain_all(&retained_entry).await.unwrap() > 0
            && entry_view.len().unwrap() == 1
            && entry_view.contains("keep", &first).unwrap(),
        java["entryViewRetainAll"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute_if_absent("compute-a", |_| Some(first.clone()))
                .await
                .unwrap()
                .unwrap(),
            &first
        ),
        java["computeIfAbsentMaps"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute_if_absent("compute-a", |_| Some(second.clone()))
                .await
                .unwrap()
                .unwrap(),
            &first
        ),
        java["computeIfAbsentKeepsCurrent"]
    );
    assert_eq!(
        live.compute_if_absent("compute-null", |_| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-null").unwrap(),
        java["computeIfAbsentNullLeavesAbsent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute_if_present("compute-a", |_, _| Some(second.clone()))
                .await
                .unwrap()
                .unwrap(),
            &second
        ) && entry_view.contains("compute-a", &second).unwrap(),
        java["computeIfPresentReplaces"]
    );
    assert_eq!(
        live.compute_if_present("compute-a", |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-a").unwrap(),
        java["computeIfPresentNullRemoves"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute("compute-a", |_, _| Some(first.clone()))
                .await
                .unwrap()
                .unwrap(),
            &first
        ) && entry_view.contains("compute-a", &first).unwrap(),
        java["computeReplaces"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute("compute-b", |_, _| Some(second.clone()))
                .await
                .unwrap()
                .unwrap(),
            &second
        ),
        java["computeAbsentInserts"]
    );
    assert_eq!(
        live.compute("compute-b", |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-b").unwrap(),
        java["computeNullRemoves"]
    );
    assert_eq!(
        live.compute("compute-null-absent", |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-null-absent").unwrap(),
        java["computeAbsentNullLeavesAbsent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .merge("compute-a", second.clone(), |_, added| Some(added.clone()))
                .await
                .unwrap()
                .unwrap(),
            &second
        ),
        java["mergeCombines"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .merge("merge-new", first.clone(), |_, _| None)
                .await
                .unwrap()
                .unwrap(),
            &first
        ),
        java["mergeAbsentInserts"]
    );
    live.put("merge-remove", second.clone()).await.unwrap();
    assert_eq!(
        live.merge("merge-remove", first.clone(), |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("merge-remove").unwrap(),
        java["mergeNullRemoves"]
    );
    // The Java fixture also retains the manager ID mapping from its preceding
    // identity-removal contention case, so replaceAll observes four entries.
    live.put(&id, first.clone()).await.unwrap();
    live.replace_all(|_, _| second.clone()).await.unwrap();
    assert_eq!(
        live.len().unwrap() == 3
            && live
                .snapshot()
                .unwrap()
                .values()
                .all(|value| Arc::ptr_eq(value, &second)),
        java["replaceAllUpdatesMappings"]
    );
    assert!(Arc::ptr_eq(
        &second_view.snapshot().unwrap()["keep"],
        &second
    ));
    live.clear().await.unwrap();
    assert_eq!(
        second_view.is_empty().unwrap(),
        java["retainedViewEmptyAfterClear"]
    );
    assert_eq!(
        key_view.is_empty().unwrap()
            && values_view.is_empty().unwrap()
            && entry_view.is_empty().unwrap(),
        java["collectionViewsEmptyAfterClear"]
    );
    live.put("iterator-key", first.clone()).await.unwrap();
    assert_eq!(
        key_view.remove("iterator-key").await.unwrap() && live.is_empty().unwrap(),
        java["keyIteratorRemove"]
    );
    live.put("iterator-value", first.clone()).await.unwrap();
    assert_eq!(
        values_view.remove(&first).await.unwrap() && live.is_empty().unwrap(),
        java["valuesIteratorRemove"]
    );
    live.put("iterator-entry", first.clone()).await.unwrap();
    assert_eq!(
        entry_view.remove("iterator-entry", &first).await.unwrap() && live.is_empty().unwrap(),
        java["entryIteratorRemove"]
    );
    live.put("entry-set-value", first.clone()).await.unwrap();
    let entry = entry_view.entry_snapshots().unwrap().pop().unwrap();
    assert_eq!(
        Arc::ptr_eq(&entry.set_value(second.clone()).await.unwrap(), &first),
        java["entrySetValueReturnsPrevious"]
    );
    assert_eq!(
        Arc::ptr_eq(&entry.value().unwrap(), &second),
        java["entrySetValueUpdatesEntry"]
    );
    assert_eq!(
        Arc::ptr_eq(&live.get("entry-set-value").unwrap().unwrap(), &second),
        java["entrySetValueUpdatesTable"]
    );
    live.put("entry-set-value", first.clone()).await.unwrap();
    assert_eq!(
        Arc::ptr_eq(&entry.value().unwrap(), &second),
        java["entrySetValueEntryIsSnapshot"]
    );
    assert_eq!(
        Arc::ptr_eq(&entry.set_value(second.clone()).await.unwrap(), &second),
        java["entrySetValueReturnsCapturedPrevious"]
    );
    assert_eq!(
        Arc::ptr_eq(&live.get("entry-set-value").unwrap().unwrap(), &second),
        java["entrySetValueReplacesCurrentMapping"]
    );
    live.clear().await.unwrap();
    assert_eq!(
        first.state(),
        rocketmq_classic_core::runtime::RuntimeState::Created
    );
    assert_eq!(
        second.state(),
        rocketmq_classic_core::runtime::RuntimeState::Created
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_factory_table_remapping_serializes_same_key_callback_like_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("factory-remap-race-current"))
        .await
        .unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("factory-remap-race-next"))
        .await
        .unwrap();
    live.put("same-key", first.clone()).await.unwrap();

    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_calls_for_task = callback_calls.clone();
    let callback_read_current = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_read_for_task = callback_read_current.clone();
    let replacement = second.clone();
    let current = first.clone();
    let table_for_compute = live.clone();
    let table_for_callback = live.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let computation = tokio::spawn(async move {
        table_for_compute
            .compute("same-key", move |key, _| {
                let observed = table_for_callback.get(key).unwrap();
                callback_read_for_task.store(
                    observed.is_some_and(|value| Arc::ptr_eq(&value, &current)),
                    Ordering::SeqCst,
                );
                if callback_calls_for_task.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Some(replacement.clone())
            })
            .await
            .unwrap()
            .unwrap()
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();

    let table_for_writer = live.clone();
    let writer_value = second.clone();
    let (writer_started_tx, writer_started_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        writer_started_tx.send(()).unwrap();
        table_for_writer
            .put("same-key", writer_value)
            .await
            .unwrap();
    });
    writer_started_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let writer_waits_for_callback = !writer.is_finished();
    release_tx.send(()).unwrap();

    let computed = computation.await.unwrap();
    writer.await.unwrap();
    assert_eq!(
        writer_waits_for_callback,
        java["remappingWriterWaitsForCallback"]
    );
    assert_eq!(
        callback_calls.load(Ordering::SeqCst) == 1,
        java["remappingCallbackRunsOnce"]
    );
    assert_eq!(
        callback_read_current.load(Ordering::SeqCst),
        java["remappingCallbackReadsCurrent"]
    );
    assert_eq!(
        Arc::ptr_eq(&computed, &second)
            && live
                .get("same-key")
                .unwrap()
                .is_some_and(|value| Arc::ptr_eq(&value, &second)),
        java["remappingReturnsAndKeepsSecond"]
    );
    live.remove("same-key").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn factory_table_replace_all_retries_after_concurrent_same_key_write() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("factory-replace-all-race"))
        .await
        .unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("factory-replace-all-race-next"))
        .await
        .unwrap();
    let key = first.client_id().to_owned();

    let callback_calls = Arc::new(AtomicUsize::new(0));
    let initial_callback_is_first = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let retry_callback_is_second = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let calls_for_task = callback_calls.clone();
    let initial_for_task = initial_callback_is_first.clone();
    let retry_for_task = retry_callback_is_second.clone();
    let callback_key = key.clone();
    let callback_first = first.clone();
    let callback_second = second.clone();
    let table_for_replace_all = live.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let replacing = tokio::spawn(async move {
        table_for_replace_all
            .replace_all(move |key, current| {
                if key != callback_key {
                    return current.clone();
                }
                let call = calls_for_task.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    initial_for_task.store(Arc::ptr_eq(current, &callback_first), Ordering::SeqCst);
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                } else if call == 1 {
                    retry_for_task.store(Arc::ptr_eq(current, &callback_second), Ordering::SeqCst);
                }
                callback_first.clone()
            })
            .await
            .unwrap()
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();

    let table_for_writer = live.clone();
    let writer_value = second.clone();
    let writer_key = key.clone();
    let (writer_started_tx, writer_started_rx) = tokio::sync::oneshot::channel();
    let mut writer = tokio::spawn(async move {
        writer_started_tx.send(()).unwrap();
        table_for_writer
            .put(&writer_key, writer_value)
            .await
            .unwrap()
    });
    writer_started_rx.await.unwrap();
    let writer_result = tokio::time::timeout(Duration::from_millis(100), &mut writer).await;
    let writer_runs_during_callback = writer_result.is_ok();
    release_tx.send(()).unwrap();

    replacing.await.unwrap();
    let writer_previous = match writer_result {
        Ok(result) => result.unwrap(),
        Err(_) => writer.await.unwrap(),
    };
    let callback_retries_after_writer = callback_calls.load(Ordering::SeqCst) == 2
        && initial_callback_is_first.load(Ordering::SeqCst)
        && retry_callback_is_second.load(Ordering::SeqCst);
    let writer_observed_old_value =
        writer_previous.is_some_and(|value| Arc::ptr_eq(&value, &first));
    let final_value_is_computed = live
        .get(&key)
        .unwrap()
        .is_some_and(|value| Arc::ptr_eq(&value, &first));
    assert_eq!(
        writer_runs_during_callback,
        java["replaceAllWriterRunsDuringCallback"]
    );
    assert_eq!(
        callback_retries_after_writer,
        java["replaceAllCallbackRetriesAfterWriter"]
    );
    assert_eq!(
        writer_observed_old_value,
        java["replaceAllWriterObservedOldValue"]
    );
    assert_eq!(
        final_value_is_computed,
        java["replaceAllFinalValueIsComputed"]
    );
    live.remove(&key).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn identity_safe_manager_removal_waits_for_factory_table_remapping() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let config = ClientConfig::new("factory-remap-manager-remove");
    let id = config.build_client_id();
    let first = manager.get_or_create(&config).await.unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("factory-remap-manager-remove-next"))
        .await
        .unwrap();

    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_calls_for_task = callback_calls.clone();
    let expected_first = first.clone();
    let replacement = second.clone();
    let table_for_compute = live.clone();
    let compute_key = id.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let computation = tokio::spawn(async move {
        table_for_compute
            .compute_if_present(compute_key, move |_, current| {
                assert!(Arc::ptr_eq(current, &expected_first));
                if callback_calls_for_task.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Some(replacement.clone())
            })
            .await
            .unwrap()
            .unwrap()
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();

    let manager_for_removal = manager.clone();
    let id_for_removal = id.clone();
    let expected_for_removal = first.clone();
    let (removal_started_tx, removal_started_rx) = tokio::sync::oneshot::channel();
    let removal = tokio::task::spawn_blocking(move || {
        removal_started_tx.send(()).unwrap();
        manager_for_removal
            .remove_client_factory(&id_for_removal, &expected_for_removal)
            .unwrap()
    });
    removal_started_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let removal_waits_for_remapping = !removal.is_finished();
    release_tx.send(()).unwrap();

    let computed = computation.await.unwrap();
    let removed_first = removal.await.unwrap();
    assert_eq!(
        removal_waits_for_remapping,
        java["managerIdentityRemovalWaitsForRemapping"]
    );
    assert_eq!(
        callback_calls.load(Ordering::SeqCst) == 1,
        java["managerIdentityRemovalRemappingRunsOnce"]
    );
    assert_eq!(
        !removed_first
            && Arc::ptr_eq(&computed, &second)
            && live
                .get(&id)
                .unwrap()
                .is_some_and(|value| Arc::ptr_eq(&value, &second)),
        java["managerIdentityRemovalKeepsRemappingResult"]
    );
    live.remove(&id).await.unwrap();
}

#[tokio::test]
async fn remapping_callback_panic_does_not_poison_factory_table_mutations() {
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("factory-remap-panic-current"))
        .await
        .unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("factory-remap-panic-next"))
        .await
        .unwrap();
    let key = first.client_id().to_owned();
    live.put(&key, first.clone()).await.unwrap();

    let table_for_compute = live.clone();
    let compute_key = key.clone();
    let compute = tokio::spawn(async move {
        table_for_compute
            .compute(&compute_key, |_, _| -> Option<Arc<_>> {
                panic!("mapping callback failed")
            })
            .await
    });
    let error = match compute.await {
        Ok(_) => panic!("mapping callback unexpectedly completed"),
        Err(error) => error,
    };
    assert!(error.is_panic());

    let previous = live.put(&key, second.clone()).await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&previous, &first));
    assert!(Arc::ptr_eq(&live.get(&key).unwrap().unwrap(), &second));
}
