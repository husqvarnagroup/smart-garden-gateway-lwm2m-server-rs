mod bootstrap;
mod coap;
mod config;
mod error;
mod housekeeping;
mod model;
mod mqtt;
mod registry;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use rumqttc::AsyncClient;

use crate::{
    bootstrap::BootstrapRegistry,
    coap::server::DispatchRequest,
    config::Config,
    model::MqttResponse,
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

    let cfg = Config::from_env().map_err(|e| anyhow::anyhow!("{e}"))?;
    let registry = DeviceRegistry::new();
    let bootstrap_registry = BootstrapRegistry::new();
    let bootstrap_registry_hk = bootstrap_registry.clone();
    let cancel = CancellationToken::new();

    let socket = coap::bind(cfg.coap_bind_addr, cfg.coap_interface.as_deref())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let (coap_dispatch_tx, coap_dispatch_rx) = mpsc::channel::<DispatchRequest>(256);
    let (mqtt_out_tx, mqtt_out_rx) = mpsc::channel::<MqttResponse>(256);

    let (mqtt_client, event_loop): (AsyncClient, _) =
        mqtt::build_client(&cfg).map_err(|e| anyhow::anyhow!("{e}"))?;

    // Wire up SIGTERM / Ctrl-C to graceful shutdown.
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("shutdown signal received");
            cancel.cancel();
        });
    }

    let topic_prefix = cfg.mqtt_topic_prefix.clone();

    info!("lwm2m-gateway starting");

    tokio::select! {
        r = coap::server::run(
            socket.clone(),
            registry.clone(),
            bootstrap_registry,
            coap_dispatch_tx.clone(),
            mqtt_out_tx.clone(),
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap server: {e}"))? }

        r = coap::client::run(
            socket,
            registry.clone(),
            coap_dispatch_rx,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("coap dispatch: {e}"))? }

        r = mqtt::subscriber::run(
            mqtt_client.clone(),
            event_loop,
            registry.clone(),
            coap_dispatch_tx,
            mqtt_out_tx,
            topic_prefix,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("mqtt subscriber: {e}"))? }

        r = mqtt::publisher::run(
            mqtt_client,
            mqtt_out_rx,
            cancel.clone(),
        ) => { r.map_err(|e| anyhow::anyhow!("mqtt publisher: {e}"))? }

        r = housekeeping::run(
            registry,
            bootstrap_registry_hk,
            mpsc::channel(1).0,
            cancel,
        ) => { r.map_err(|e| anyhow::anyhow!("housekeeping: {e}"))? }
    }

    info!("lwm2m-gateway stopped");
    Ok(())
}
