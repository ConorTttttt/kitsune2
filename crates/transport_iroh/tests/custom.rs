//! Transports whose only transport is a custom iroh transport the
//! application registered reach each other by endpoint ID alone, with no IP
//! socket, relay or bootstrap server.
//!
//! The custom transport here is iroh's in-memory test network, standing in
//! for a real one such as Bluetooth RFCOMM.
#![cfg(feature = "custom-transport")]

use bytes::Bytes;
use iroh::SecretKey;
use iroh::endpoint::Builder as EndpointBuilder;
use iroh::test_utils::test_transport::TestNetwork;
use kitsune2_api::{Builder, DynTransport, K2Error, K2Result, Url};
use kitsune2_test_utils::{enable_tracing, space::TEST_SPACE_ID};
use kitsune2_transport_iroh::{
    CustomTransportFactory, IrohTransportConfig, IrohTransportFactory,
    IrohTransportModConfig, register_custom_transport,
    test_utils::MockTxHandler,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// Puts every endpoint it is asked for on one in-memory network.
#[derive(Debug, Default)]
struct TestNetworkFactory {
    network: TestNetwork,
}

impl CustomTransportFactory for TestNetworkFactory {
    fn attach(
        &self,
        builder: EndpointBuilder,
        secret_key: &SecretKey,
    ) -> K2Result<EndpointBuilder> {
        let transport = self
            .network
            .create_transport(secret_key.public())
            .map_err(|err| K2Error::other_src("test transport", err))?;
        Ok(builder
            .add_custom_transport(transport)
            .address_lookup(self.network.address_lookup()))
    }
}

struct Node {
    transport: DynTransport,
    url: Url,
    received: UnboundedReceiver<(Url, Bytes)>,
}

fn builder(custom_transport: Option<&str>) -> Builder {
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
                custom_transport: custom_transport.map(str::to_string),
                // There is no relay, and none must be contacted.
                relay_url: Some("https://relay.invalid:443/".to_string()),
                ..Default::default()
            },
        })
        .unwrap();
    builder
}

async fn custom_node(name: &str) -> Node {
    let builder = builder(Some(name));
    builder.validate_config().unwrap();
    let builder = Arc::new(builder);

    let (sender, received) = unbounded_channel();
    let handler = Arc::new(MockTxHandler {
        recv_space_notify: Arc::new(move |peer, _space_id, data| {
            sender.send((peer, data)).unwrap();
            Ok(())
        }),
        ..Default::default()
    });
    let transport = builder
        .transport
        .create(builder.clone(), handler.clone())
        .await
        .unwrap();
    // A custom endpoint knows its URL as soon as it is bound.
    let url = transport
        .register_space_handler(TEST_SPACE_ID, handler)
        .expect("a custom endpoint has a URL once created");
    Node {
        transport,
        url,
        received,
    }
}

async fn expect_notify(node: &mut Node, from: &Url, message: &[u8]) {
    let (peer, data) =
        tokio::time::timeout(Duration::from_secs(10), node.received.recv())
            .await
            .expect("message was not received")
            .unwrap();
    assert_eq!(&peer, from);
    assert_eq!(&data[..], message);
}

#[tokio::test]
async fn custom_nodes_exchange_messages_both_ways_by_endpoint_id() {
    enable_tracing();
    register_custom_transport(
        "test-net",
        Arc::new(TestNetworkFactory::default()),
    )
    .unwrap();
    let mut node_1 = custom_node("test-net").await;
    let mut node_2 = custom_node("test-net").await;

    for node in [&node_1, &node_2] {
        let url = node.url.as_str();
        assert!(url.starts_with("https://test-net.invalid:443/"), "{url}");
        let stats = node
            .transport
            .dump_network_stats()
            .await
            .unwrap()
            .transport_stats;
        assert_eq!(stats.backend, "iroh-test-net");
        assert_eq!(stats.peer_urls, vec![node.url.clone()]);
    }

    node_1
        .transport
        .send_space_notify(
            node_2.url.clone(),
            TEST_SPACE_ID,
            Bytes::from_static(b"one to two"),
        )
        .await
        .unwrap();
    expect_notify(&mut node_2, &node_1.url, b"one to two").await;

    node_2
        .transport
        .send_space_notify(
            node_1.url.clone(),
            TEST_SPACE_ID,
            Bytes::from_static(b"two to one"),
        )
        .await
        .unwrap();
    expect_notify(&mut node_1, &node_2.url, b"two to one").await;
}

#[tokio::test]
async fn an_unregistered_custom_transport_fails_to_start() {
    enable_tracing();
    let builder = Arc::new(builder(Some("never-registered")));
    builder.validate_config().unwrap();
    let handler = Arc::new(MockTxHandler::default());
    let Err(err) = builder.transport.create(builder.clone(), handler).await
    else {
        panic!("no factory is registered under that name");
    };
    assert!(
        err.to_string().contains("no transport is registered"),
        "{err}"
    );
}

#[test]
fn custom_transport_names_must_be_host_name_labels() {
    for bad in [
        "",
        "RFCOMM",
        "has space",
        "-lead",
        "trail-",
        &"x".repeat(33),
    ] {
        assert!(
            builder(Some(bad)).validate_config().is_err(),
            "{bad:?} must be refused"
        );
    }
    assert!(builder(Some("rfcomm")).validate_config().is_ok());
}

#[test]
fn custom_transport_can_not_be_combined_with_direct_or_ble() {
    for config in [
        IrohTransportConfig {
            custom_transport: Some("rfcomm".into()),
            direct_bind_addr: Some("127.0.0.1:0".into()),
            ..Default::default()
        },
        IrohTransportConfig {
            custom_transport: Some("rfcomm".into()),
            ble_only: true,
            ..Default::default()
        },
    ] {
        let builder = builder(None);
        builder
            .config
            .set_module_config(&IrohTransportModConfig {
                iroh_transport: config,
            })
            .unwrap();
        assert!(builder.validate_config().is_err());
    }
}
