//! A custom iroh transport, supplied by the application, as the only
//! transport of the endpoint.
//!
//! An application registers a [`CustomTransportFactory`] under a name before
//! it builds kitsune2, and sets `customTransport` to that name in the iroh
//! transport config. The endpoint then carries that transport and nothing
//! else: no IP sockets and no relay. As with BLE, a peer is reached by its
//! endpoint ID alone, which the address lookup the factory installs resolves
//! to an address of the custom transport, so the peer URL carries no address:
//! `https://<name>.invalid:443/<endpoint-id>`.
//!
//! The factory is process-global because the transport config is plain data
//! that crosses the conductor's config, and a transport is not.

use crate::url::custom_peer_url;
use iroh::SecretKey;
use iroh::endpoint::Builder;
use kitsune2_api::{K2Error, K2Result, Url};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use tracing::info;

/// Supplies a custom iroh transport to an endpoint being built.
pub trait CustomTransportFactory:
    std::fmt::Debug + Send + Sync + 'static
{
    /// Add the custom transport, and the address lookup that resolves
    /// endpoint IDs to its addresses, to `builder`.
    ///
    /// `secret_key` is the key the endpoint will be bound with, so the
    /// transport can know its own endpoint ID before the endpoint exists.
    /// IP transports and relays are cleared after this returns, whatever
    /// the factory does.
    fn attach(
        &self,
        builder: Builder,
        secret_key: &SecretKey,
    ) -> K2Result<Builder>;
}

static FACTORIES: LazyLock<
    Mutex<HashMap<String, Arc<dyn CustomTransportFactory>>>,
> = LazyLock::new(Default::default);

/// Check a custom transport name: 1 to 32 lower-case letters, digits or `-`,
/// as it becomes a host name label in peer URLs.
pub(crate) fn check_name(name: &str) -> K2Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 32
        && !name.starts_with('-')
        && !name.ends_with('-')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if valid {
        Ok(())
    } else {
        Err(K2Error::other(format!(
            "customTransport name {name:?} must be 1 to 32 lower-case \
             letters, digits or '-'"
        )))
    }
}

/// Register `factory` under `name`, replacing any factory registered under
/// that name before. Endpoints built afterwards with `customTransport` set
/// to `name` use it.
pub fn register_custom_transport(
    name: &str,
    factory: Arc<dyn CustomTransportFactory>,
) -> K2Result<()> {
    check_name(name)?;
    FACTORIES
        .lock()
        .expect("poisoned")
        .insert(name.to_string(), factory);
    Ok(())
}

/// Remove the factory registered under `name`, if any.
pub fn unregister_custom_transport(name: &str) {
    FACTORIES.lock().expect("poisoned").remove(name);
}

fn factory(name: &str) -> K2Result<Arc<dyn CustomTransportFactory>> {
    FACTORIES
        .lock()
        .expect("poisoned")
        .get(name)
        .cloned()
        .ok_or_else(|| {
            K2Error::other(format!(
                "customTransport is {name:?}, but no transport is registered \
                 under that name"
            ))
        })
}

/// Make the custom transport registered under `name` the only transport of
/// the endpoint being built.
///
/// Returns the builder to bind the endpoint with, and the peer URL the
/// endpoint is reachable at once bound.
pub(crate) fn attach(name: &str, builder: Builder) -> K2Result<(Builder, Url)> {
    check_name(name)?;
    let factory = factory(name)?;
    // The transport may need to know its endpoint ID before the endpoint is
    // bound, as BLE does to advertise it.
    let secret_key = SecretKey::generate();
    let endpoint_id = secret_key.public();
    let builder = factory
        .attach(builder.secret_key(secret_key.clone()), &secret_key)?
        .clear_ip_transports()
        .clear_relay_transports();
    let url = custom_peer_url(name, endpoint_id)?;
    info!(%url, transport = name, "A custom transport is the only transport");
    Ok((builder, url))
}
