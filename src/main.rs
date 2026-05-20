use std::path::PathBuf;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use std::sync::Arc;

use lwm2m_gateway::{
    bootstrap::BootstrapRegistry,
    coap,
    coap::server::DispatchRequest,
    config::Config,
    event::{self, EventSender},
    housekeeping,
    ipc,
    ipso::IpsoModel,
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
    let ipso = Arc::new(IpsoModel::load_dirs(&cfg.ipso_directories));
    let registry = DeviceRegistry::new();
    let bootstrap_registry = BootstrapRegistry::new(cfg.network_key.clone(), Some(cfg.server_uri.clone()));
    let bootstrap_registry_hk = bootstrap_registry.clone();
    let bootstrap_registry_ipc = bootstrap_registry.clone();
    let event_sender = EventSender::new();
    let cancel = CancellationToken::new();

    let socket = coap::bind(cfg.coap_bind_addr, cfg.coap_interface.as_deref())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (coap_dispatch_tx, coap_dispatch_rx) = mpsc::channel::<DispatchRequest>(256);
    let coap_dispatch_tx_ipc = coap_dispatch_tx.clone();

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
            event_sender.clone(),
            ipso.clone(),
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap server: {e}"))? }

        r = coap::client::run(
            socket,
            registry.clone(),
            coap_dispatch_rx,
            event_sender.clone(),
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap dispatch: {e}"))? }

        r = housekeeping::run(
            registry.clone(),
            bootstrap_registry_hk,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("housekeeping: {e}"))? }

        r = ipc::run(
            PathBuf::from(ipc::DEFAULT_SOCKET_PATH),
            bootstrap_registry_ipc,
            registry,
            ipso,
            coap_dispatch_tx_ipc,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("ipc: {e}"))? }

        r = event::run(
            PathBuf::from(event::DEFAULT_EVENT_SOCKET_PATH),
            event_sender,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("event: {e}"))? }
    }

    info!("lwm2m-gateway stopped");
    Ok(())
}
