mod bootstrap;
mod coap;
mod config;
mod error;
mod housekeeping;
mod ipc;
mod model;
mod registry;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    bootstrap::BootstrapRegistry,
    coap::server::DispatchRequest,
    config::Config,
    registry::DeviceRegistry,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lwm2m_gateway=info".into()),
        )
        .init();

    let cfg = Config::from_args().map_err(|e| anyhow::anyhow!("{e}"))?;
    let registry = DeviceRegistry::new();
    let bootstrap_registry = BootstrapRegistry::new(cfg.network_key.clone(), Some(cfg.server_uri.clone()));
    let bootstrap_registry_hk = bootstrap_registry.clone();
    let cancel = CancellationToken::new();

    let socket = coap::bind(cfg.coap_bind_addr, cfg.coap_interface.as_deref())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (coap_dispatch_tx, coap_dispatch_rx) = mpsc::channel::<DispatchRequest>(256);

    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("shutdown signal received");
            cancel.cancel();
        });
    }

    info!("lwm2m-gateway starting");

    tokio::select! {
        r = coap::server::run(
            socket.clone(),
            registry.clone(),
            bootstrap_registry,
            coap_dispatch_tx,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap server: {e}"))? }

        r = coap::client::run(
            socket,
            registry.clone(),
            coap_dispatch_rx,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap dispatch: {e}"))? }

        r = housekeeping::run(
            registry,
            bootstrap_registry_hk,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("housekeeping: {e}"))? }

        r = ipc::run(cancel.clone()) => {
            r.map_err(|e| anyhow::anyhow!("ipc: {e}"))?
        }
    }

    info!("lwm2m-gateway stopped");
    Ok(())
}
