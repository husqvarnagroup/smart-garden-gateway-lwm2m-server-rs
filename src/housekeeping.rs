use std::time::Duration;

use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{bootstrap::BootstrapRegistry, error::Result, registry::DeviceRegistry};

const GRACE_SECS: u32 = 30;
const IN_FLIGHT_TIMEOUT_SECS: u64 = 60;
const INTERVAL_SECS: u64 = 60;
const BOOTSTRAP_TIMEOUT_SECS: u64 = 30;

pub async fn run(
    registry: DeviceRegistry,
    bootstrap_registry: BootstrapRegistry,
    cancel: CancellationToken,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(INTERVAL_SECS));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Housekeeping task shutting down");
                return Ok(());
            }
            _ = interval.tick() => {
                let expired = registry.expire_stale(GRACE_SECS).await;
                if !expired.is_empty() {
                    warn!(count = expired.len(), endpoints = ?expired, "Expired device registrations purged");
                }
                registry.timeout_in_flight(IN_FLIGHT_TIMEOUT_SECS).await;
                bootstrap_registry.expire_stale(BOOTSTRAP_TIMEOUT_SECS).await;
            }
        }
    }
}
