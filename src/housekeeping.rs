use std::time::Duration;

use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{bootstrap::BootstrapRegistry, error::Result, model::MqttResponse, registry::DeviceRegistry};

/// Registration grace period before entries are purged (seconds beyond declared lifetime).
const GRACE_SECS: u32 = 30;
/// Maximum time an in-flight CoAP operation may wait for a response.
const IN_FLIGHT_TIMEOUT_SECS: u64 = 60;
/// How often housekeeping runs.
const INTERVAL_SECS: u64 = 60;

/// Bootstrap sessions older than this are considered dead (device never responded).
const BOOTSTRAP_TIMEOUT_SECS: u64 = 30;

pub async fn run(
    registry: DeviceRegistry,
    bootstrap_registry: BootstrapRegistry,
    _mqtt_out_tx: mpsc::Sender<MqttResponse>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(INTERVAL_SECS));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("housekeeping task shutting down");
                return Ok(());
            }
            _ = interval.tick() => {
                let expired = registry.expire_stale(GRACE_SECS).await;
                if !expired.is_empty() {
                    warn!(count = expired.len(), endpoints = ?expired, "expired device registrations purged");
                }
                registry.timeout_in_flight(IN_FLIGHT_TIMEOUT_SECS).await;
                bootstrap_registry.expire_stale(BOOTSTRAP_TIMEOUT_SECS).await;
            }
        }
    }
}
