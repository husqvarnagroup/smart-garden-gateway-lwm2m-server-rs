use std::path::PathBuf;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt as _, util::SubscriberInitExt as _, Layer as _,
};

use std::sync::Arc;

use lwm2mserver_rs::{
    config::Config,
    housekeeping,
    ipc::{command, event},
    logging::{PrefixedFields, SyslogLayer},
    lwm2m::{
        self,
        bootstrap::BootstrapRegistry,
        ipso::{load_shared, IpsoModel},
        server::DispatchRequest,
    },
    persistence::PersistenceStore,
    registry::DeviceRegistry,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "lwm2mserver_rs=info".into());

    let journal = std::env::var_os("JOURNAL_STREAM").is_some();
    let syslog = if journal {
        SyslogLayer::try_new()
    } else {
        None
    };
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
    let ipso = load_shared(&cfg.ipso_directories);
    let registry = DeviceRegistry::new();
    let bootstrap_registry =
        BootstrapRegistry::new(cfg.network_key.clone(), Some(cfg.server_uri.clone()));
    let bootstrap_registry_hk = bootstrap_registry.clone();
    let bootstrap_registry_ipc = bootstrap_registry.clone();
    let event_sender = event::EventSender::new();
    let cancel = CancellationToken::new();

    let persistence = Arc::new(PersistenceStore::new(
        PathBuf::from("/var/lib/lwm2mserver"),
        &cfg.server_uri,
    ));

    // Restore persisted state before starting the server.
    let snapshots = persistence.load_registry();
    let included = persistence.load_included();

    // Consistency check: wakaama.json must be a subset of included_devices.json.
    // Drop any registry entry not in the included list and clean up its state file.
    let included_set: std::collections::HashSet<&str> =
        included.iter().map(String::as_str).collect();
    let (valid_snapshots, orphaned): (Vec<_>, Vec<_>) = snapshots
        .into_iter()
        .partition(|s| included_set.contains(s.endpoint.as_str()));
    if !orphaned.is_empty() {
        for s in &orphaned {
            warn!(device = %s.endpoint, "Dropping orphaned registry entry (not in included list)");
            persistence.delete_device_state(&s.endpoint);
        }
        persistence.save_registry(&valid_snapshots);
    }

    if !valid_snapshots.is_empty() {
        info!(count = valid_snapshots.len(), "Restoring devices");
        registry.restore(valid_snapshots).await;
    }
    if !included.is_empty() {
        info!(count = included.len(), "Restoring included devices");
        bootstrap_registry.load_included(included.clone()).await;
    }
    for (ep, state) in persistence.load_all_device_states() {
        if included_set.contains(ep.as_str()) {
            registry.restore_device_state(&ep, state).await;
        } else {
            warn!(device = %ep, "Deleting orphaned device state (not in included list)");
            persistence.delete_device_state(&ep);
        }
    }

    let socket = lwm2m::bind(cfg.coap_bind_addr, cfg.coap_interface.as_deref())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (coap_dispatch_tx, coap_dispatch_rx) = mpsc::channel::<DispatchRequest>(256);
    let coap_dispatch_tx_ipc = coap_dispatch_tx.clone();
    let coap_dispatch_tx_hk = coap_dispatch_tx.clone();
    let persistence_ipc = Arc::clone(&persistence);
    let block_acks = lwm2m::new_block_ack_map();

    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("Shutdown signal received");
            cancel.cancel();
        });
    }

    #[cfg(unix)]
    {
        let ipso = ipso.clone();
        let dirs = cfg.ipso_directories.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sighup = signal(SignalKind::hangup()).expect("SIGHUP handler");
            while sighup.recv().await.is_some() {
                info!("SIGHUP received, reloading IPSO definitions");
                let new_model = std::sync::Arc::new(IpsoModel::load_dirs(&dirs));
                *ipso.write().unwrap() = new_model;
            }
        });
    }

    info!("Server starting");

    tokio::select! {
        r = lwm2m::server::run(
            socket.clone(),
            registry.clone(),
            bootstrap_registry,
            coap_dispatch_tx,
            event_sender.clone(),
            ipso.clone(),
            persistence,
            block_acks.clone(),
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap server: {e}"))? }

        r = lwm2m::client::run(
            socket,
            registry.clone(),
            coap_dispatch_rx,
            event_sender.clone(),
            block_acks,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap dispatch: {e}"))? }

        r = housekeeping::run(
            registry.clone(),
            bootstrap_registry_hk,
            coap_dispatch_tx_hk,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("housekeeping: {e}"))? }

        r = command::run(
            PathBuf::from(command::DEFAULT_SOCKET_PATH),
            bootstrap_registry_ipc,
            registry,
            ipso,
            coap_dispatch_tx_ipc,
            persistence_ipc,
            event_sender.clone(),
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
