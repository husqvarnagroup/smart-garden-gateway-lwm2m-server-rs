use std::path::PathBuf;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{Layer as _, util::SubscriberInitExt as _, prelude::__tracing_subscriber_SubscriberExt as _};

use std::sync::Arc;

use lwm2mserver_rs::{
    bootstrap::BootstrapRegistry,
    coap,
    coap::server::DispatchRequest,
    config::Config,
    event::{self, EventSender},
    housekeeping,
    ipc,
    ipso::IpsoModel,
    persistence::PersistenceStore,
    registry::DeviceRegistry,
    console_fmt::PrefixedFields,
    syslog_layer::SyslogLayer,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "lwm2mserver_rs=info".into());

    let journal = std::env::var_os("JOURNAL_STREAM").is_some();
    let syslog = if journal { SyslogLayer::try_new() } else { None };
    let suppress_fmt = journal && syslog.is_some();
    let fmt = tracing_subscriber::fmt::layer()
        .event_format(PrefixedFields::default())
        .with_filter(if suppress_fmt {
            tracing_subscriber::filter::LevelFilter::OFF
        } else {
            tracing_subscriber::filter::LevelFilter::TRACE
        });

    tracing_subscriber::registry()
        .with(env_filter)
        .with(syslog)
        .with(fmt)
        .init();

    let cfg = Config::from_args().map_err(|e| anyhow::anyhow!("{e}"))?;
    let ipso = Arc::new(IpsoModel::load_dirs(&cfg.ipso_directories));
    let registry = DeviceRegistry::new();
    let bootstrap_registry = BootstrapRegistry::new(cfg.network_key.clone(), Some(cfg.server_uri.clone()));
    let bootstrap_registry_hk = bootstrap_registry.clone();
    let bootstrap_registry_ipc = bootstrap_registry.clone();
    let event_sender = EventSender::new();
    let cancel = CancellationToken::new();

    let persistence = Arc::new(PersistenceStore::new(
        PathBuf::from("/var/lib/lwm2mserver"),
        &cfg.server_uri,
    ));

    // Restore persisted state before starting the server.
    let snapshots = persistence.load_registry();
    if !snapshots.is_empty() {
        info!(count = snapshots.len(), "Restoring devices");
        registry.restore(snapshots).await;
    }
    let included = persistence.load_included();
    if !included.is_empty() {
        info!(count = included.len(), "Restoring included devices");
        bootstrap_registry.load_included(included).await;
    }
    for (ep, state) in persistence.load_all_device_states() {
        registry.restore_device_state(&ep, state).await;
    }

    let socket = coap::bind(cfg.coap_bind_addr, cfg.coap_interface.as_deref())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (coap_dispatch_tx, coap_dispatch_rx) = mpsc::channel::<DispatchRequest>(256);
    let coap_dispatch_tx_ipc = coap_dispatch_tx.clone();

    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("Shutdown signal received");
            cancel.cancel();
        });
    }

    info!("Server starting");

    tokio::select! {
        r = coap::server::run(
            socket.clone(),
            registry.clone(),
            bootstrap_registry,
            coap_dispatch_tx,
            event_sender.clone(),
            ipso.clone(),
            persistence,
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

    info!("Server stopped");
    Ok(())
}
