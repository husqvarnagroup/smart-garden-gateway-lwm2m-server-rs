use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    error::Result,
    lwm2m::{
        bootstrap::BootstrapRegistry,
        ipso::{IpsoModel, SharedIpso},
        server::DispatchRequest,
    },
    model::{LwM2mCommand, PendingOperation, ResourcePath},
    registry::DeviceRegistry,
};

const GRACE_SECS: u32 = 30;
const IN_FLIGHT_TIMEOUT_SECS: u64 = 60;
const INTERVAL_SECS: u64 = 60;
const BOOTSTRAP_TIMEOUT_SECS: u64 = 30;

const OFFLINE_PING_INTERVAL: Duration = Duration::from_secs(15 * 60);
const ONLINE_PING_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const OFFLINE_PING_MAX_DURATION: Duration = Duration::from_secs(6 * 60 * 60);

/// Op-ID counter for housekeeping-originated pings. Uses the upper half of u32
/// to avoid collisions with IPC-originated op IDs (which start from 1).
static NEXT_PING_OP_ID: AtomicU32 = AtomicU32::new(0x8000_0000);

pub async fn run(
    registry: DeviceRegistry,
    bootstrap_registry: BootstrapRegistry,
    dispatch_tx: mpsc::Sender<DispatchRequest>,
    ipso: SharedIpso,
    cancel: CancellationToken,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(INTERVAL_SECS));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    // First tick fires immediately; use it for startup pings.
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

                let ping_path = resolve_ping_path(&ipso.read().unwrap());
                if let Some(ref path) = ping_path {
                    let candidates = registry
                        .take_ping_candidates(
                            OFFLINE_PING_INTERVAL,
                            ONLINE_PING_INTERVAL,
                            OFFLINE_PING_MAX_DURATION,
                        )
                        .await;

                    for (endpoint, addr) in candidates {
                        let op_id = NEXT_PING_OP_ID.fetch_add(1, Ordering::Relaxed);
                        let (response_tx, _response_rx) = tokio::sync::oneshot::channel();
                        let op = PendingOperation {
                            id: op_id,
                            command: LwM2mCommand::Execute {
                                path: path.clone(),
                                args: None,
                            },
                            response_tx,
                            first_ack_tx: None,
                            created_at: Instant::now(),
                            attempts: 0,
                        };
                        info!(device = %endpoint, activity = "connection-status", "Sending connectivity ping");
                        let _ = dispatch_tx.send(DispatchRequest { addr, ops: vec![op] }).await;
                    }
                }
            }
        }
    }
}

fn resolve_ping_path(ipso: &IpsoModel) -> Option<ResourcePath> {
    let obj_id = ipso.object_id_by_name("sg_common")?;
    let res_id = ipso.resource_id_by_name(obj_id, "measure_rf_link", None)?;
    Some(ResourcePath {
        object_id: obj_id as u16,
        instance_id: 0,
        resource_id: res_id as u16,
    })
}
