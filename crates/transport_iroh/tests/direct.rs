//! Transports in direct mode reach each other at their bound IP addresses,
//! with no relay and no bootstrap server anywhere.

use bytes::Bytes;
use kitsune2_api::{Builder, DynTransport, Url};
use kitsune2_test_utils::{enable_tracing, space::TEST_SPACE_ID};
use kitsune2_transport_iroh::{
    IrohTransportConfig, IrohTransportFactory, IrohTransportModConfig,
    test_utils::MockTxHandler,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// A direct-mode transport, its URL, and the space notifications it receives.
struct Node {
    transport: DynTransport,
    url: Url,
    received: UnboundedReceiver<(Url, Bytes)>,
}

async fn direct_node(bind_addr: &str) -> Node {
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
                direct_bind_addr: Some(bind_addr.to_string()),
                // There is no relay, and none must be contacted.
                relay_url: Some("https://relay.invalid:443/".to_string()),
                ..Default::default()
            },
        })
        .unwrap();
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
    // A direct endpoint knows its URL as soon as it is bound.
    let url = transport
        .register_space_handler(TEST_SPACE_ID, handler)
        .expect("a direct endpoint has a URL once created");

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
async fn direct_url_names_the_bound_socket() {
    enable_tracing();
    let node = direct_node("127.0.0.1:0").await;

    let url = node.url.as_str();
    assert!(url.starts_with("http://127.0.0.1:"), "{url}");
    assert!(url.contains("/direct/"), "{url}");
    let port: u16 =
        node.url.addr().rsplit_once(':').unwrap().1.parse().unwrap();
    assert_ne!(port, 0, "port 0 must be replaced by the bound port");

    let stats = node
        .transport
        .dump_network_stats()
        .await
        .unwrap()
        .transport_stats;
    assert_eq!(stats.backend, "iroh-direct");
    assert_eq!(stats.peer_urls, vec![node.url.clone()]);
}

#[tokio::test]
async fn direct_nodes_exchange_messages_both_ways_without_a_relay() {
    enable_tracing();
    let mut node_1 = direct_node("127.0.0.1:0").await;
    let mut node_2 = direct_node("127.0.0.1:0").await;

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

    // One connection, on a direct IP path, to the endpoint the URL names.
    let stats = node_1
        .transport
        .dump_network_stats()
        .await
        .unwrap()
        .transport_stats;
    assert_eq!(stats.connections.len(), 1);
    let connection = &stats.connections[0];
    assert!(connection.is_direct);
    assert_eq!(Some(connection.pub_key.as_str()), node_2.url.peer_id());
    assert!(connection.send_bytes > 0 && connection.recv_bytes > 0);
}

/// A peer that restarts on a new address and identity is reachable at its new
/// URL, and the old URL no longer delivers anything.
#[tokio::test]
async fn direct_peer_is_reachable_after_moving_to_a_new_address() {
    enable_tracing();
    let node_1 = direct_node("127.0.0.1:0").await;
    let mut node_2 = direct_node("127.0.0.1:0").await;

    node_1
        .transport
        .send_space_notify(
            node_2.url.clone(),
            TEST_SPACE_ID,
            Bytes::from_static(b"before"),
        )
        .await
        .unwrap();
    expect_notify(&mut node_2, &node_1.url, b"before").await;

    let old_url = node_2.url.clone();
    drop(node_2);

    let mut node_3 = direct_node("127.0.0.1:0").await;
    assert_ne!(node_3.url, old_url);

    node_1
        .transport
        .send_space_notify(
            node_3.url.clone(),
            TEST_SPACE_ID,
            Bytes::from_static(b"after"),
        )
        .await
        .unwrap();
    expect_notify(&mut node_3, &node_1.url, b"after").await;
}
