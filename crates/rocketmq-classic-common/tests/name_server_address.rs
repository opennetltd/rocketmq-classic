use std::process::Command;

use rocketmq_classic_common::utils::name_server_address::{
    ENDPOINT_PREFIX, INST_ENDPOINT_PATTERN, INSTANCE_PREFIX, INSTANCE_REGEX,
    NAMESRV_ENDPOINT_PATTERN, NameServerAddressError, get_name_server_addresses,
    get_name_srv_addr_from_namesrv_endpoint, matches_namesrv_endpoint,
    parse_instance_id_from_endpoint, validate_instance_endpoint,
};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap()
}

#[test]
fn name_server_address_public_surface_and_inputs_match_invoked_java() {
    let fixture = fixture();
    let java = &fixture["nameServerAddressUtils"];

    assert_eq!(
        java["publicFields"],
        json!([
            "java.lang.String ENDPOINT_PREFIX",
            "java.lang.String INSTANCE_PREFIX",
            "java.lang.String INSTANCE_REGEX",
            "java.util.regex.Pattern INST_ENDPOINT_PATTERN",
            "java.util.regex.Pattern NAMESRV_ENDPOINT_PATTERN"
        ])
    );
    assert_eq!(
        java["publicMethods"],
        json!([
            "boolean validateInstanceEndpoint(java.lang.String)",
            "java.lang.String getNameServerAddresses()",
            "java.lang.String getNameSrvAddrFromNamesrvEndpoint(java.lang.String)",
            "java.lang.String parseInstanceIdFromEndpoint(java.lang.String)"
        ])
    );
    assert_eq!(
        java["publicValues"],
        json!({
            "INSTANCE_PREFIX": INSTANCE_PREFIX,
            "INSTANCE_REGEX": INSTANCE_REGEX,
            "ENDPOINT_PREFIX": ENDPOINT_PREFIX,
            "NAMESRV_ENDPOINT_PATTERN": NAMESRV_ENDPOINT_PATTERN,
            "INST_ENDPOINT_PATTERN": INST_ENDPOINT_PATTERN,
        })
    );

    for case in java["namesrvPatternCases"].as_array().unwrap() {
        match case["outcome"].as_str().unwrap() {
            "OK" => assert_eq!(
                matches_namesrv_endpoint(case["input"].as_str()).unwrap(),
                case["matches"].as_bool().unwrap(),
                "{case}"
            ),
            "NullPointerException" => assert_eq!(
                matches_namesrv_endpoint(None),
                Err(NameServerAddressError::NullEndpoint)
            ),
            outcome => panic!("unhandled Java outcome {outcome}: {case}"),
        }
    }

    for case in java["instanceEndpointCases"].as_array().unwrap() {
        match case["outcome"].as_str().unwrap() {
            "OK" => assert_eq!(
                validate_instance_endpoint(case["input"].as_str()).unwrap(),
                case["valid"].as_bool().unwrap(),
                "{case}"
            ),
            "NullPointerException" => assert_eq!(
                validate_instance_endpoint(None),
                Err(NameServerAddressError::NullEndpoint)
            ),
            outcome => panic!("unhandled Java outcome {outcome}: {case}"),
        }
    }

    for case in java["instanceIdCases"].as_array().unwrap() {
        match case["outcome"].as_str().unwrap() {
            "OK" => assert_eq!(
                parse_instance_id_from_endpoint(case["input"].as_str())
                    .unwrap()
                    .as_deref(),
                case["value"].as_str(),
                "{case}"
            ),
            "StringIndexOutOfBoundsException" => assert_eq!(
                parse_instance_id_from_endpoint(case["input"].as_str()),
                Err(NameServerAddressError::InvalidInstanceIdBounds),
                "{case}"
            ),
            outcome => panic!("unhandled Java outcome {outcome}: {case}"),
        }
    }

    for case in java["namesrvAddressCases"].as_array().unwrap() {
        assert_eq!(
            get_name_srv_addr_from_namesrv_endpoint(case["input"].as_str()),
            case["value"].as_str().map(str::to_owned),
            "{case}"
        );
    }

    assert_eq!(
        fixture["nameServerAddressCases"]["directGetterSystemPropertyValue"],
        "system:9876"
    );
    assert_eq!(
        fixture["nameServerAddressCases"]["directGetterExplicitBlankValue"],
        ""
    );
}

const PROBE_ENABLED: &str = "ROCKETMQ_CLASSIC_NAME_SERVER_ADDRESS_PROBE";
const PROBE_EXPECTED: &str = "ROCKETMQ_CLASSIC_NAME_SERVER_ADDRESS_EXPECTED";

#[test]
fn get_name_server_addresses_reads_java_property_then_environment_fallback() {
    if std::env::var_os(PROBE_ENABLED).is_some() {
        let expected = std::env::var(PROBE_EXPECTED).unwrap();
        assert_eq!(
            get_name_server_addresses().as_deref(),
            (expected != "<none>").then_some(expected.as_str())
        );
        return;
    }

    for (property, fallback, expected) in [
        (Some("system:9876"), Some("fallback:9876"), "system:9876"),
        (Some(""), Some("fallback:9876"), ""),
        (None, Some("fallback:9876"), "fallback:9876"),
        (None, None, "<none>"),
    ] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "get_name_server_addresses_reads_java_property_then_environment_fallback",
        ]);
        command.env(PROBE_ENABLED, "1");
        command.env(PROBE_EXPECTED, expected);
        command.env_remove("rocketmq.namesrv.addr");
        command.env_remove("NAMESRV_ADDR");
        if let Some(property) = property {
            command.env("rocketmq.namesrv.addr", property);
        }
        if let Some(fallback) = fallback {
            command.env("NAMESRV_ADDR", fallback);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "child probe failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
