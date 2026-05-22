use std::{net::SocketAddr, sync::Arc, time::Duration};

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    error::Result,
    ipc::event::EventSender,
    model::{LwM2mCommand, LwM2mError, PendingOperation, ResourceValue},
    registry::DeviceRegistry,
};

use super::{server::DispatchRequest, set_tclass, BlockAckMap, TC_ENCRYPTED};

/// CoAP retransmission constants (RFC 7252 defaults).
const ACK_TIMEOUT_MS: u64 = 2_000;
const MAX_RETRANSMIT: u8 = 3;

/// Initial block size for Block1: 512 bytes (SZX=5).
/// The device may negotiate down to a smaller SZX in its Continue responses.
const BLOCK_SZX: u8 = 5;

/// Minimum payload length that triggers block-wise transfer.
const BLOCK_THRESHOLD: usize = 1 << (BLOCK_SZX + 4); // 512

pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    mut dispatch_rx: mpsc::Receiver<DispatchRequest>,
    event_sender: EventSender,
    block_acks: BlockAckMap,
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
                    dispatch_op(&socket, &registry, req.addr, op, &mut mid_counter, &event_sender, &block_acks).await;
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
    event_sender: &EventSender,
    block_acks: &BlockAckMap,
) {
    // Large Write payloads require block-wise transfer (RFC 7959).
    if let LwM2mCommand::Write { ref value, .. } = op.command {
        if value.len() > BLOCK_THRESHOLD {
            // Pre-allocate enough MIDs for the worst case (all blocks at the initial SZX).
            let max_blocks = value.len().div_ceil(BLOCK_THRESHOLD);
            let start_mid = *mid_counter;
            *mid_counter = mid_counter.wrapping_add(max_blocks as u16);
            if *mid_counter == 0 {
                *mid_counter = 1;
            }
            tokio::spawn(dispatch_block_write(
                socket.clone(),
                registry.clone(),
                block_acks.clone(),
                addr,
                op,
                event_sender.clone(),
                start_mid,
            ));
            return;
        }
    }

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
            warn!(op_id = op.id, "Failed to build CoAP request: {e}");
            let _ = op.response_tx.send(Err(LwM2mError::BadRequest));
            return;
        }
    };

    let bytes = match packet.to_bytes() {
        Ok(b) => b,
        Err(e) => {
            warn!(op_id = op.id, "Failed to encode CoAP packet: {e:?}");
            let _ = op.response_tx.send(Err(LwM2mError::BadRequest));
            return;
        }
    };

    op.attempts += 1;
    registry.place_in_flight(addr, token, op).await;

    // Send with retransmit loop (Confirmable).
    let socket = socket.clone();
    let registry = registry.clone();
    let event_sender = event_sender.clone();
    tokio::spawn(async move {
        let mut timeout_ms = ACK_TIMEOUT_MS;
        let mut attempts = 0u8;

        loop {
            set_tclass(&socket, TC_ENCRYPTED);
            if let Err(e) = socket.send_to(&bytes, addr).await {
                warn!(%addr, "Failed to send: {e}");
                if let Some(endpoint) = registry.set_device_offline(addr).await {
                    info!(device = %endpoint, activity = "connection-status", "Device is offline");
                    event_sender.send_connection_status(&endpoint, false);
                }
                break;
            }
            debug!(%addr, ?token, "CoAP CON sent (attempt {})", attempts + 1);

            sleep(Duration::from_millis(timeout_ms)).await;

            // Check if the op was already completed (ACK received).
            if !registry.is_in_flight(addr, &token).await {
                // Already acked — nothing more to do.
                return;
            }

            attempts += 1;
            if attempts >= MAX_RETRANSMIT {
                // Final timeout: retrieve and fail the op.
                if let Some(op) = registry.complete_in_flight(addr, &token).await {
                    warn!(%addr, op_id = op.id, "Device unreachable, operation timed out");
                    let _ = op.response_tx.send(Err(LwM2mError::Timeout));
                }
                if let Some(endpoint) = registry.set_device_offline(addr).await {
                    info!(device = %endpoint, activity = "connection-status", "Device is offline");
                    event_sender.send_connection_status(&endpoint, false);
                }
                return;
            }

            // Exponential backoff.
            timeout_ms *= 2;
        }
    });
}

/// Send a large Write payload using CoAP Block1 option (RFC 7959).
/// Starts at SZX=5 (512 bytes); honors device SZX negotiation in Continue responses.
async fn dispatch_block_write(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    block_acks: BlockAckMap,
    addr: SocketAddr,
    op: PendingOperation,
    event_sender: EventSender,
    start_mid: u16,
) {
    let PendingOperation {
        id: op_id,
        command,
        response_tx,
        ..
    } = op;
    let (path, payload, content_format) = match command {
        LwM2mCommand::Write {
            path,
            value,
            content_format,
        } => (path, value, content_format),
        _ => unreachable!("dispatch_block_write called with non-Write command"),
    };

    let token: [u8; 8] = {
        let mut arr = [0u8; 8];
        arr[..4].copy_from_slice(&op_id.to_le_bytes());
        arr
    };

    let total = payload.len();
    let mut szx = BLOCK_SZX;
    let mut block_size = 1usize << (szx + 4);
    let mut mid = start_mid;
    let mut offset = 0usize;

    info!(%addr, op_id, szx, "Block write: {} bytes", total);

    while offset < total {
        let end = (offset + block_size).min(total);
        let chunk = &payload[offset..end];
        let more = end < total;
        let block_num = offset / block_size;

        let block1 = encode_block1(block_num, more, szx);
        let pkt_bytes = match build_block_put(
            &path.as_uri_path(),
            &token,
            mid,
            chunk,
            content_format,
            &block1,
        ) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    op_id,
                    "Block write: failed to encode block {block_num}: {e}"
                );
                let _ = response_tx.send(Err(LwM2mError::BadRequest));
                return;
            }
        };
        mid = mid.wrapping_add(1);
        if mid == 0 {
            mid = 1;
        }

        // Retransmit loop for this block.
        let mut timeout_ms = ACK_TIMEOUT_MS;
        let mut retransmits = 0u8;
        let ack_pkt = loop {
            let (tx, rx) = oneshot::channel::<Packet>();
            block_acks.lock().await.insert(token, tx);

            set_tclass(&socket, TC_ENCRYPTED);
            if socket.send_to(&pkt_bytes, addr).await.is_err() {
                block_acks.lock().await.remove(&token);
                let _ = response_tx.send(Err(LwM2mError::Timeout));
                if let Some(endpoint) = registry.set_device_offline(addr).await {
                    info!(device = %endpoint, activity = "connection-status", "Device is offline");
                    event_sender.send_connection_status(&endpoint, false);
                }
                return;
            }
            debug!(%addr, block_num, more, szx, "Block1 CON PUT sent");

            match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
                Ok(Ok(pkt)) => break pkt,
                Ok(Err(_)) => {
                    // Sender was dropped by RST handler.
                    let _ = response_tx.send(Err(LwM2mError::CoapError {
                        class: 5,
                        detail: 0,
                    }));
                    return;
                }
                Err(_) => {
                    // Timeout — remove stale sender, maybe retransmit.
                    block_acks.lock().await.remove(&token);
                    retransmits += 1;
                    if retransmits >= MAX_RETRANSMIT {
                        warn!(%addr, op_id, block_num, "Block write timed out");
                        let _ = response_tx.send(Err(LwM2mError::Timeout));
                        if let Some(endpoint) = registry.set_device_offline(addr).await {
                            info!(device = %endpoint, activity = "connection-status", "Device is offline");
                            event_sender.send_connection_status(&endpoint, false);
                        }
                        return;
                    }
                    timeout_ms *= 2;
                }
            }
        };

        // Decode the ACK response.
        match ack_pkt.header.code {
            MessageClass::Response(Status::Continue) => {
                if !more {
                    // Last block shouldn't elicit Continue — device error.
                    let _ = response_tx.send(Err(LwM2mError::CoapError {
                        class: 5,
                        detail: 0,
                    }));
                    return;
                }
                // Honor device's SZX negotiation (RFC 7959 §2.5).
                if let Some(dev_szx) = block1_szx_from_response(&ack_pkt) {
                    if dev_szx < szx {
                        let new_block_size = 1usize << (dev_szx + 4);
                        info!(%addr, op_id, old_szx = szx, new_szx = dev_szx, "Block write: device negotiated smaller block size");
                        szx = dev_szx;
                        block_size = new_block_size;
                        // Advance offset to end of current chunk; block_num recomputed from offset next iteration.
                    }
                }
                offset = end;
            }
            MessageClass::Response(status) => {
                let (class, detail) = super::response_type_to_class_detail(status);
                if class == 2 {
                    info!(%addr, op_id, "Block write complete");
                    let _ = response_tx.send(Ok(ResourceValue::CoapResponse { class, detail }));
                } else {
                    warn!(%addr, op_id, block_num, "Block write rejected: {class}.{detail:02}");
                    let _ = response_tx.send(Err(LwM2mError::CoapError { class, detail }));
                }
                return;
            }
            _ => {
                let _ = response_tx.send(Err(LwM2mError::CoapError {
                    class: 5,
                    detail: 0,
                }));
                return;
            }
        }
    }
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
        LwM2mCommand::Write {
            path,
            value,
            content_format,
        } => {
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

/// Extract the SZX from a Block1 option in a CoAP response (e.g. 2.31 Continue).
/// Returns `None` if the option is absent or malformed.
fn block1_szx_from_response(packet: &Packet) -> Option<u8> {
    let opts = packet.get_option(CoapOption::Block1)?;
    let bytes = opts.front()?;
    let val: u64 = bytes.iter().fold(0u64, |acc, &b| (acc << 8) | b as u64);
    Some((val & 0x07) as u8)
}

/// Encode the Block1 option value for the given block number, more flag, and SZX.
/// Returns the minimum-length big-endian representation (1–3 bytes).
fn encode_block1(num: usize, more: bool, szx: u8) -> Vec<u8> {
    let val = (num << 4) | ((more as usize) << 3) | (szx as usize);
    if val <= 0xFF {
        vec![val as u8]
    } else if val <= 0xFFFF {
        vec![(val >> 8) as u8, val as u8]
    } else {
        vec![(val >> 16) as u8, (val >> 8) as u8, val as u8]
    }
}

fn build_block_put(
    uri_path: &str,
    token: &[u8; 8],
    mid: u16,
    chunk: &[u8],
    content_format: u16,
    block1: &[u8],
) -> crate::error::Result<Vec<u8>> {
    let mut packet = Packet::new();
    packet.header.set_type(MessageType::Confirmable);
    packet.header.code = MessageClass::Request(Method::Put);
    packet.header.message_id = mid;
    packet.set_token(token.to_vec());
    add_uri_path(&mut packet, uri_path);
    packet.add_option(
        CoapOption::ContentFormat,
        content_format.to_be_bytes().to_vec(),
    );
    packet.add_option(CoapOption::Block1, block1.to_vec());
    packet.payload = chunk.to_vec();
    packet
        .to_bytes()
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))
}
