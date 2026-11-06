use rocketmq_classic_common::utils::{
    name_server_address::get_name_server_addresses, system_properties::get_property,
};
use rocketmq_classic_core::{
    api::{MQClientApiFactory, MQClientApiFactoryError, NameserverAccessConfig},
    config::ClientConfig,
};

#[test]
fn factory_property_writes_are_visible_to_later_client_configs() {
    let java: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/api-factory.json")).unwrap();

    let null_access =
        MQClientApiFactory::new_nullable(None, Some("unused-".into()), 0, None, None, None);
    assert!(matches!(
        null_access,
        Err(MQClientApiFactoryError::NullNameServerAccessConfig)
    ));
    assert_eq!(
        java["nullNameserverAccessConfigError"],
        "NullPointerException"
    );
    assert_eq!(
        get_property(ClientConfig::SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY).as_deref(),
        java["vipPropertyAfterNullNameserverAccessConfig"].as_str()
    );

    let missing =
        MQClientApiFactory::new(NameserverAccessConfig::default(), "missing-", 0, None, None);
    assert!(matches!(
        missing,
        Err(MQClientApiFactoryError::MissingNameServerAddress)
    ));
    assert_eq!(java["missingStaticAddressError"], "RuntimeException");
    assert_eq!(
        get_property(ClientConfig::SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY).as_deref(),
        java["vipPropertyAfterMissingStaticAddress"].as_str()
    );
    assert!(!ClientConfig::new("127.0.0.1").vip_channel_enabled());

    let address = java["nameserverAccessConfig"][0].as_str().unwrap();
    let _static_factory = MQClientApiFactory::new(
        NameserverAccessConfig::new(Some(address.into()), Some(String::new()), None),
        "static-",
        0,
        None,
        None,
    )
    .unwrap();
    assert_eq!(get_name_server_addresses().as_deref(), Some(address));
    let later_client = ClientConfig::new("127.0.0.1");
    assert_eq!(later_client.get_namesrv_addr().as_deref(), Some(address));
    assert!(!later_client.vip_channel_enabled());

    let before_domain = "before.example";
    let before_subgroup = "before-subgroup";
    let _seed_domain_factory = MQClientApiFactory::new(
        NameserverAccessConfig::new(
            None,
            Some(before_domain.into()),
            Some(before_subgroup.into()),
        ),
        "seed-domain-",
        0,
        None,
        None,
    )
    .unwrap();

    let domain = "failed-domain.example";
    let missing_subgroup = MQClientApiFactory::new(
        NameserverAccessConfig::new(None, Some(domain.into()), None),
        "bad-domain-",
        0,
        None,
        None,
    );
    assert!(matches!(
        missing_subgroup,
        Err(MQClientApiFactoryError::NullNameServerDomainSubgroup)
    ));
    assert_eq!(java["missingDomainSubgroupError"], "NullPointerException");
    assert_eq!(
        get_property(ClientConfig::SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY).as_deref(),
        java["vipPropertyAfterMissingDomainSubgroup"].as_str()
    );
    assert_eq!(
        get_property("rocketmq.namesrv.domain").as_deref(),
        java["domainPropertyAfterMissingSubgroup"].as_str()
    );
    assert_eq!(
        get_property("rocketmq.namesrv.domain.subgroup").as_deref(),
        java["subgroupPropertyAfterMissingSubgroup"].as_str()
    );

    let domain = java["domainPropertyAfterInit"].as_str().unwrap();
    let subgroup = java["domainSubgroupPropertyAfterInit"].as_str().unwrap();
    let _domain_factory = MQClientApiFactory::new(
        NameserverAccessConfig::new(None, Some(domain.into()), Some(subgroup.into())),
        "domain-",
        0,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        get_property("rocketmq.namesrv.domain.subgroup").as_deref(),
        Some(subgroup)
    );
}
