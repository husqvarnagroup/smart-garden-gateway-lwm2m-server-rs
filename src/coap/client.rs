use std::{net::SocketAddr, sync::Arc, time::Duration};

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method,
};
use tokio::{net::UdpSocket, sync::mpsc, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    error::Result,
    model::{LwM2mCommand, LwM2mError, PendingOperation},
    registry::DeviceRegistry,
};

use super::{server::DispatchRequest, set_tclass, TC_ENCRYPTED};

/// CoAP retransmission constants (RFC 7252 defaults).
const ACK_TIMEOUT_MS: u64 = 2_000;
const MAX_RETRANSMIT: u8 = 4;

pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    mut dispatch_rx: mpsc::Receiver<DispatchRequest>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut mid_counter: u16 = 1;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("CoAP dispatch task shutting down");
                return Ok(());
            }
            Some(req) = dispatch_rx.recv() => {
                for op in req.ops {
                    dispatch_op(&socket, &registry, req.addr, op, &mut mid_counter).await;
                }
            }
        }
    }
}

async fn dispatch_op(
    socket: &Arc<UdpSocket>,
    registry: &DeviceRegistry,
    addr: SocketAddr,
    mut op: PendingOperation,
    mid_counter: &mut u16,
) {
    // CoAP token: 8 bytes, lower 4 bytes = op id (u32), upper 4 bytes zero.
    // AtomicU64 is unavailable on 32-bit ARM, so op ids are u32.
    let token: [u8; 8] = {
        let mut arr = [0u8; 8];
        arr[..4].copy_from_slice(&op.id.to_le_bytes());
        arr
    };
    let mid = next_mid(mid_counter);

    let packet = match build_request(&op.command, token, mid) {
        Ok(p) => p,
        Err(e) => {
            warn!(op_id = op.id, "failed to build CoAP request: {e}");
            let _ = op.response_tx.send(Err(LwM2mError::BadRequest));
            return;
        }
    };

    let bytes = match packet.to_bytes() {
        Ok(b) => b,
        Err(e) => {
            warn!(op_id = op.id, "failed to encode CoAP packet: {e:?}");
            let _ = op.response_tx.send(Err(LwM2mError::BadRequest));
            return;
        }
    };

    op.attempts += 1;
    registry.place_in_flight(addr, token, op).await;

    // Send with retransmit loop (Confirmable).
    let socket = socket.clone();
    let registry = registry.clone();
    tokio::spawn(async move {
        let mut timeout_ms = ACK_TIMEOUT_MS;
        let mut attempts = 0u8;

        loop {
            set_tclass(&socket, TC_ENCRYPTED);
            if let Err(e) = socket.send_to(&bytes, addr).await {
                warn!(%addr, "send_to failed: {e}");
                break;
            }
            debug!(%addr, ?token, "CoAP CON sent (attempt {})", attempts + 1);

            sleep(Duration::from_millis(timeout_ms)).await;

            // Check if the op was already completed (ACK received).
            if registry.complete_in_flight(addr, &token).await.is_none() {
                // Already acked — nothing more to do.
                return;
            }

            attempts += 1;
            if attempts >= MAX_RETRANSMIT {
                // Final timeout: retrieve and fail the op.
                if let Some(op) = registry.complete_in_flight(addr, &token).await {
                    warn!(%addr, op_id = op.id, "CoAP retransmit exhausted, timing out op");
                    let _ = op.response_tx.send(Err(LwM2mError::Timeout));
                }
                return;
            }

            // Exponential backoff.
            timeout_ms *= 2;
        }
    });
}

fn build_request(command: &LwM2mCommand, token: [u8; 8], mid: u16) -> Result<Packet> {
    let mut packet = Packet::new();
    packet.header.set_type(MessageType::Confirmable);
    packet.header.message_id = mid;
    packet.set_token(token.to_vec());

    match command {
        LwM2mCommand::Read { path } => {
            packet.header.code = MessageClass::Request(Method::Get);
            add_uri_path(&mut packet, &path.as_uri_path());
        }
        LwM2mCommand::Write { path, value, content_format } => {
            packet.header.code = MessageClass::Request(Method::Put);
            add_uri_path(&mut packet, &path.as_uri_path());
            packet.add_option(
                CoapOption::ContentFormat,
                content_format.to_be_bytes().to_vec(),
            );
            packet.payload = value.clone();
        }
        LwM2mCommand::Execute { path, args } => {
            packet.header.code = MessageClass::Request(Method::Post);
            add_uri_path(&mut packet, &path.as_uri_path());
            if let Some(args) = args {
                packet.payload = args.clone();
            }
        }
    }
    Ok(packet)
}

fn add_uri_path(packet: &mut Packet, path: &str) {
    for segment in path.split('/') {
        packet.add_option(CoapOption::UriPath, segment.as_bytes().to_vec());
    }
}

fn next_mid(counter: &mut u16) -> u16 {
    let mid = *counter;
    *counter = counter.wrapping_add(1).max(1);
    mid
}
