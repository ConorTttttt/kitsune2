//! iroh transport module tests

use super::*;

mod close;
mod connect_failure;
mod connection_registry;
mod frame;
mod relay_lifecycle;
mod relay_qad;
mod simultaneous_open;
mod stream;
mod support;
mod url;

#[test]
fn validate_bad_relay_url() {
    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    };

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: Some("<bad-url>".into()),
                ..Default::default()
            },
        })
        .unwrap();

    assert!(builder.validate_config().is_err());
}

#[test]
fn ble_only_defaults_to_false_when_absent() {
    let config: IrohTransportModConfig =
        serde_json::from_value(serde_json::json!({
            "irohTransport": {
                "relayUrl": "https://relay.example/",
                "relayAllowPlainText": false,
                "maxFrameBytes": 1024,
                "connectTimeoutS": 60,
            }
        }))
        .unwrap();

    assert!(!config.iroh_transport.ble_only);
}

#[test]
fn validate_ble_only_requires_the_ble_feature() {
    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    };

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                ble_only: true,
                ..Default::default()
            },
        })
        .unwrap();

    assert_eq!(builder.validate_config().is_ok(), cfg!(feature = "ble"));
}

fn direct_config_builder(direct_bind_addr: &str, ble_only: bool) -> Builder {
    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    };
    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                direct_bind_addr: Some(direct_bind_addr.to_string()),
                ble_only,
                ..Default::default()
            },
        })
        .unwrap();
    builder
}

#[test]
fn direct_bind_addr_defaults_to_none_when_absent() {
    let config: IrohTransportModConfig =
        serde_json::from_value(serde_json::json!({
            "irohTransport": {
                "relayUrl": "https://relay.example/",
                "relayAllowPlainText": false,
                "maxFrameBytes": 1024,
                "connectTimeoutS": 60,
            }
        }))
        .unwrap();

    assert_eq!(config.iroh_transport.direct_bind_addr, None);
}

#[test]
fn validate_accepts_interface_direct_bind_addrs() {
    for addr in ["192.168.137.1:47001", "127.0.0.1:0", "[fd00::1]:47001"] {
        assert!(
            direct_config_builder(addr, false).validate_config().is_ok(),
            "{addr}"
        );
    }
}

#[test]
fn validate_rejects_unusable_direct_bind_addrs() {
    for addr in [
        "not an address",
        "192.168.137.1",
        "0.0.0.0:47001",
        "[::]:47001",
        "224.0.0.1:47001",
        "[fe80::1%3]:47001",
    ] {
        assert!(
            direct_config_builder(addr, false)
                .validate_config()
                .is_err(),
            "{addr}"
        );
    }
}

#[test]
fn validate_rejects_direct_bind_addr_with_ble_only() {
    assert!(
        direct_config_builder("192.168.137.1:47001", true)
            .validate_config()
            .is_err()
    );
}

#[test]
fn direct_link_addrs_get_a_loopback_anchor_of_their_own_family() {
    let v4 = direct_anchor_addr("192.168.137.1:47001".parse().unwrap());
    assert_eq!(v4, Some(("127.0.0.1:0".parse().unwrap(), 32)));
    let v6 = direct_anchor_addr("[fd00::1]:47001".parse().unwrap());
    assert_eq!(v6, Some(("[::1]:0".parse().unwrap(), 128)));
}

#[test]
fn loopback_direct_addrs_need_no_anchor() {
    assert_eq!(direct_anchor_addr("127.0.0.1:47001".parse().unwrap()), None);
    assert_eq!(direct_anchor_addr("127.0.0.2:47001".parse().unwrap()), None);
    assert_eq!(direct_anchor_addr("[::1]:47001".parse().unwrap()), None);
}

#[test]
fn local_addr_availability_follows_interface_addresses() {
    assert!(is_local_addr_available("127.0.0.1".parse().unwrap()));
    // TEST-NET-1 (RFC 5737) is never assigned to a local interface.
    assert!(!is_local_addr_available("192.0.2.1".parse().unwrap()));
}

#[test]
fn validate_allowed_plain_text_relay_url() {
    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    };

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: Some("http://test.url".into()),
                relay_allow_plain_text: true,
                ..Default::default()
            },
        })
        .unwrap();

    let result = builder.validate_config();
    assert!(result.is_ok());
}

#[test]
fn validate_zero_relay_keepalive_interval() {
    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    };

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_keepalive_interval_s: 0,
                ..Default::default()
            },
        })
        .unwrap();

    let result = builder.validate_config();
    assert!(result.is_err());
    assert!(
        format!("{result:?}")
            .contains("Relay keepalive interval must be greater than zero")
    );
}

/// Test that `configure_for_space` reads the per-space iroh transport
/// config and dynamically adds a relay to the transport.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn configure_for_space_adds_relay() {
    use crate::test_utils::MockTxHandler;
    use kitsune2_test_utils::bootstrap::TestBootstrapSrv;
    use std::sync::Arc;

    kitsune2_test_utils::enable_tracing();

    let bootstrap_server = TestBootstrapSrv::new(false).await;
    let relay_url = format!("{}/relay", bootstrap_server.addr());

    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    }
    .with_default_config()
    .unwrap();

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: None,
                relay_allow_plain_text: true,
                ..Default::default()
            },
        })
        .unwrap();

    let builder = Arc::new(builder);

    let got_address = Arc::new(tokio::sync::Notify::new());
    let got_address_clone = got_address.clone();
    let handler: DynTxHandler = Arc::new(MockTxHandler {
        new_listening_address: Arc::new(move |_url| {
            got_address_clone.notify_one();
        }),
        ..MockTxHandler::default()
    });

    let tx = builder
        .transport
        .create(builder.clone(), handler)
        .await
        .unwrap();

    let per_space_config = Config::default();
    per_space_config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: Some(relay_url),
                relay_allow_plain_text: true,
                ..Default::default()
            },
        })
        .unwrap();

    let space_id = SpaceId(Id(bytes::Bytes::from_static(b"test-space")));

    // configure_for_space is non-blocking — it spawns relay insertion
    // in the background and returns immediately.
    tx.configure_for_space(space_id, &per_space_config)
        .await
        .unwrap();

    // The transport should receive a relay-assigned listening address
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        got_address.notified(),
    )
    .await
    .expect(
        "Transport should receive a listening address after configure_for_space",
    );
}

/// Test that `configure_for_space` is a no-op when no per-space config
/// is provided.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn configure_for_space_noop_without_config() {
    use crate::test_utils::MockTxHandler;
    use std::sync::Arc;

    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    }
    .with_default_config()
    .unwrap();

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: None,
                relay_allow_plain_text: true,
                ..Default::default()
            },
        })
        .unwrap();

    let builder = Arc::new(builder);
    let handler: DynTxHandler = Arc::new(MockTxHandler::default());

    let tx = builder
        .transport
        .create(builder.clone(), handler)
        .await
        .unwrap();

    let empty_config = Config::default();
    let space_id = SpaceId(Id(bytes::Bytes::from_static(b"test-space")));

    // Should succeed as a no-op when no per-space relay is configured
    tx.configure_for_space(space_id, &empty_config)
        .await
        .unwrap();
}

#[test]
fn validate_disallowed_plain_text_relay_url() {
    let builder = Builder {
        transport: IrohTransportFactory::create(),
        ..kitsune2_core::default_test_builder()
    };

    builder
        .config
        .set_module_config(&IrohTransportModConfig {
            iroh_transport: IrohTransportConfig {
                relay_url: Some("http://test.url".into()),
                relay_allow_plain_text: false,
                ..Default::default()
            },
        })
        .unwrap();

    let result = builder.validate_config();
    assert!(result.is_err());
    assert!(format!("{result:?}").contains("Disallowed plaintext relay URL"));
}
