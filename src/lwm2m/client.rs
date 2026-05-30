//! CoAP client dispatch task — drives the outbound [`DispatchService`] from a channel.

use std::sync::Arc;

use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{error::Result, ipc::event::EventSender, registry::DeviceRegistry};

use super::{server::DispatchRequest, service::dedup::outbound_dedup_key, BlockAckMap};

pub use super::service::outbound::{DispatchService, BLOCK_THRESHOLD};

pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    mut dispatch_rx: mpsc::Receiver<DispatchRequest>,
    event_sender: EventSender,
    block_acks: BlockAckMap,
    cancel: CancellationToken,
) -> Result<()> {
    use tower::{Service, ServiceBuilder};
    use tower_resilience_coalesce::CoalesceLayer;

    let inner = DispatchService::new(socket, registry, event_sender, block_acks);

    // Deduplication layer: coalesces concurrent dispatch requests targeting the
    // same (destination address, operation ID).  Per RFC 7252 §4.5 / LwM2M 1.1,
    // this prevents redundant processing if the same operation is triggered
    // multiple times while already in flight.  CoAP-level retransmission with
    // exponential backoff (RFC 7252 §4.2) is handled per-operation inside the
    // service.
    let dedup_layer = CoalesceLayer::builder(outbound_dedup_key)
        .name("coap-dispatch-dedup")
        .build();

    let mut service = ServiceBuilder::new().layer(dedup_layer).service(inner);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("CoAP dispatch task shutting down");
                return Ok(());
            }
            Some(req) = dispatch_rx.recv() => {
                match service.call(req).await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Dispatch error: {e}");
                    }
                }
            }
        }
    }
}
