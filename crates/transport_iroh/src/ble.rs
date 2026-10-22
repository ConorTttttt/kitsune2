//! Bluetooth Low Energy as the only transport of the iroh endpoint.
//!
//! With [`bleOnly`](crate::IrohTransportConfig::ble_only) set, the endpoint
//! carries [`iroh_ble_transport`] as a custom transport and nothing else: no
//! IP sockets and no relay. A peer is reached by its endpoint ID alone, which
//! the BLE address lookup resolves from the GATT advertisements it scans, so
//! a BLE peer URL carries no address, only the endpoint ID.

use crate::url::ble_peer_url;
use iroh::SecretKey;
use iroh::endpoint::Builder;
use iroh_ble_transport::BleTransport;
use kitsune2_api::{K2Error, K2Result, Url};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::AbortHandle;
use tracing::info;

/// How often BLE peers and traffic counters are checked for changes to log.
const STATUS_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// The BLE transport carried by an iroh endpoint.
pub(crate) struct Ble {
    transport: Arc<BleTransport>,
    url: Url,
    status_log_task: AbortHandle,
}

impl std::fmt::Debug for Ble {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ble")
            .field("url", &self.url)
            .field("adapter_state", &self.transport.adapter_state())
            .field("metrics", &self.transport.metrics())
            .finish()
    }
}

impl Drop for Ble {
    fn drop(&mut self) {
        self.status_log_task.abort();
    }
}

impl Ble {
    /// Start BLE and make it the only transport of the endpoint being built.
    ///
    /// Returns the builder to bind the endpoint with.
    pub(crate) async fn attach(builder: Builder) -> K2Result<(Builder, Self)> {
        // BLE advertises the endpoint ID, so it must be known before the
        // endpoint is bound.
        let secret_key = SecretKey::generate();
        let endpoint_id = secret_key.public();

        let transport =
            BleTransport::builder().build(endpoint_id).await.map_err(
                |err| K2Error::other_src("Failed to start BLE transport", err),
            )?;

        let builder = builder
            .secret_key(secret_key)
            .hooks(transport.dedup_hook())
            .add_custom_transport(transport.as_custom_transport())
            .address_lookup(transport.address_lookup())
            .clear_ip_transports();

        let url = ble_peer_url(endpoint_id)?;
        info!(
            %url,
            adapter_state = ?transport.adapter_state(),
            "BLE is the only transport"
        );

        let status_log_task = spawn_status_log_task(transport.clone());

        Ok((
            builder,
            Self {
                transport,
                url,
                status_log_task,
            },
        ))
    }

    /// The peer URL the endpoint is reachable at once bound.
    pub(crate) fn url(&self) -> &Url {
        &self.url
    }
}

/// Log BLE peers and traffic counters whenever they change.
///
/// This is the evidence that kitsune2 traffic is carried over Bluetooth: the
/// endpoint has no other transport, and these counters only move when the
/// BLE transport sends or receives.
fn spawn_status_log_task(transport: Arc<BleTransport>) -> AbortHandle {
    tokio::spawn(async move {
        let mut last_metrics = Default::default();
        let mut last_peers = String::new();
        let mut interval = tokio::time::interval(STATUS_LOG_INTERVAL);
        loop {
            interval.tick().await;

            let peers = transport
                .snapshot_peers()
                .iter()
                .map(|peer| {
                    format!(
                        "{:?} {:?} via {:?} endpoint {:?}",
                        peer.device_id,
                        peer.phase,
                        peer.connect_path,
                        peer.verified_endpoint
                            .map(|id| id.fmt_short().to_string()),
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            if peers != last_peers {
                info!(%peers, "BLE peers changed");
                last_peers = peers;
            }

            let metrics = transport.metrics();
            if metrics != last_metrics {
                info!(
                    tx_bytes = metrics.tx_bytes,
                    rx_bytes = metrics.rx_bytes,
                    retransmits = metrics.retransmits,
                    "BLE traffic"
                );
                last_metrics = metrics;
            }
        }
    })
    .abort_handle()
}
