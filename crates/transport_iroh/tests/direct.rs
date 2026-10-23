//! Transports in direct mode reach each other at their bound IP addresses,
//! with no relay and no bootstrap server anywhere.

use bytes::Bytes;
use kitsune2_api::{Builder, DynTransport, Url};
use kitsune2_test_utils::{enable_tracing, space::TEST_SPACE_ID};
use kitsune2_transport_iroh::{
    IrohTransportConfig, IrohTransportFactory, IrohTransportModConfig,
    test_utils::MockTxHandler,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// A direct-mode transport, its URL, the space notifications it receives,
/// and the peers it marked unresponsive.
struct Node {
    transport: DynTransport,
    url: Url,
    received: UnboundedReceiver<(Url, Bytes)>,
    unresponsive: Arc<Mutex<Vec<Url>>>,
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
    let unresponsive = Arc::new(Mutex::new(Vec::new()));
    let marks = unresponsive.clone();
    let handler = Arc::new(MockTxHandler {
        recv_space_notify: Arc::new(move |peer, _space_id, data| {
            sender.send((peer, data)).unwrap();
            Ok(())
        }),
        set_unresponsive: Arc::new(move |peer, _when| {
            marks.lock().expect("poison").push(peer);
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
        unresponsive,
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

/// Run `ip` with these arguments, inside the test's network namespace.
fn ip(args: &str) {
    let status = std::process::Command::new("ip")
        .args(args.split_whitespace())
        .status()
        .expect("running ip");
    assert!(status.success(), "ip {args} failed");
}

/// Send until a message gets through, for up to `within`: after an outage,
/// the first sends may go to a connection that has not noticed yet.
async fn send_until_received(
    from: &Node,
    to: &mut Node,
    message: &'static [u8],
    within: Duration,
) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let _ = from
            .transport
            .send_space_notify(
                to.url.clone(),
                TEST_SPACE_ID,
                Bytes::from_static(message),
            )
            .await;
        let wait = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match to.received.recv().await {
                    Some((_, data)) if &data[..] == message => return,
                    Some(_) => continue,
                    None => panic!("receiver closed"),
                }
            }
        })
        .await;
        if wait.is_ok() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} was not delivered within {within:?}",
            String::from_utf8_lossy(message)
        );
    }
}

/// A direct endpoint outlives its link address going away and coming back,
/// as a Wi-Fi Direct address does when the link drops and is remade.
///
/// iroh rebinds its sockets on every network change, and a socket bound to
/// an address that is gone fails to rebind. Without a socket that can not
/// fail, iroh then shuts the endpoint down for good. And while its own
/// address is gone, the endpoint must not mark the peers it can not reach
/// unresponsive, or it would not dial them again once the link is back.
///
/// Both endpoints share one namespace here, so once the outage is over they
/// may also reach each other through their loopback sockets, which two
/// machines can not. The test pins down the two failures above; recovery of
/// the link path itself between separate machines is covered by
/// holochain-wifi-direct-demo's `scripts/outage-test.sh`, which gives each
/// node its own namespace.
///
/// Changing addresses needs its own network namespace, so the test is
/// ignored by default. Run it, as an unprivileged user, with:
///
/// ```text
/// unshare -rn sh -c 'ip link set lo up && ip link add d0 type dummy && \
///   ip link set d0 up && ip addr add 10.77.0.1/24 dev d0 && \
///   ip addr add 10.77.0.2/24 dev d0 && \
///   cargo test --offline -p kitsune2_transport_iroh --test direct -- \
///     --ignored --exact direct_endpoint_survives_its_link_address_going_away'
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs its own network namespace; see the doc comment"]
async fn direct_endpoint_survives_its_link_address_going_away() {
    enable_tracing();
    let mut node_1 = direct_node("10.77.0.1:0").await;
    let mut node_2 = direct_node("10.77.0.2:0").await;
    // A peer node 1 first tries to reach during its outage.
    ip("addr add 10.77.0.3/24 dev d0");
    let mut node_3 = direct_node("10.77.0.3:0").await;
    send_until_received(
        &node_1,
        &mut node_2,
        b"before 1-2",
        Duration::from_secs(10),
    )
    .await;
    send_until_received(
        &node_2,
        &mut node_1,
        b"before 2-1",
        Duration::from_secs(10),
    )
    .await;

    // The link drops: node 1's address goes away for a while, during which
    // both sides keep trying to send. Other addresses come and go too, as
    // they do while Windows tears a group adapter down: each change makes
    // iroh try, and fail, to rebind node 1's socket again.
    ip("addr del 10.77.0.1/24 dev d0");
    for _ in 0..10 {
        ip("addr add 10.78.0.1/24 dev d0");
        tokio::time::sleep(Duration::from_millis(200)).await;
        ip("addr del 10.78.0.1/24 dev d0");
        for (from, to) in [(&node_1, &node_2.url), (&node_2, &node_1.url)] {
            let _ = tokio::time::timeout(
                Duration::from_millis(500),
                from.transport.send_space_notify(
                    to.clone(),
                    TEST_SPACE_ID,
                    Bytes::from_static(b"during"),
                ),
            )
            .await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // A new connection while the link is down is not attempted, and the peer
    // is not blamed for it.
    let err = node_1
        .transport
        .send_space_notify(
            node_3.url.clone(),
            TEST_SPACE_ID,
            Bytes::from_static(b"while down"),
        )
        .await
        .expect_err("node 1 has no address to send from");
    assert!(
        format!("{err:?}").contains("own direct address is gone"),
        "{err:?}"
    );
    assert_eq!(
        *node_1.unresponsive.lock().expect("poison"),
        Vec::<Url>::new()
    );

    // The link comes back with the same address.
    ip("addr add 10.77.0.1/24 dev d0");
    send_until_received(
        &node_1,
        &mut node_2,
        b"after 1-2",
        Duration::from_secs(60),
    )
    .await;
    send_until_received(
        &node_2,
        &mut node_1,
        b"after 2-1",
        Duration::from_secs(60),
    )
    .await;
    send_until_received(
        &node_1,
        &mut node_3,
        b"after 1-3",
        Duration::from_secs(60),
    )
    .await;
}
