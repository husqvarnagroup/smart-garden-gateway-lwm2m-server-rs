use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tower::Service;
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
pub const BLOCK_THRESHOLD: usize = 1 << (BLOCK_SZX + 4); // 512

// ── Tower service ────────────────────────────────────────────────────────────

/// Tower service that dispatches outbound LwM2M CoAP operations to devices.
///
/// Each call processes a [`DispatchRequest`] containing one or more pending
/// operations destined for a single device address. The service handles CoAP
/// encoding, retransmission with exponential backoff, and block-wise transfer
/// for large payloads. Individual operation results are delivered via the
/// oneshot channels embedded in each [`PendingOperation`].
#[derive(Clone)]
pub struct DispatchService {
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    event_sender: EventSender,
    block_acks: BlockAckMap,
    mid_counter: Arc<AtomicU16>,
}

impl DispatchService {
    /// Create a new dispatch service.
    pub fn new(
        socket: Arc<UdpSocket>,
        registry: DeviceRegistry,
        event_sender: EventSender,
        block_acks: BlockAckMap,
    ) -> Self {
        Self {
            socket,
            registry,
            event_sender,
            block_acks,
            mid_counter: Arc::new(AtomicU16::new(1)),
        }
    }

    fn next_mid(&self) -> u16 {
        loop {
            let mid = self.mid_counter.fetch_add(1, Ordering::Relaxed);
            if mid != 0 {
                return mid;
            }
            // Skip 0, try again (wraps to 1).
            self.mid_counter
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                .ok();
        }
    }

    fn alloc_mid_range(&self, count: u16) -> u16 {
        let start = self.mid_counter.fetch_add(count, Ordering::Relaxed);
        if start == 0 {
            self.mid_counter
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                .ok();
            return 1;
        }
        start
    }
}

impl Service<DispatchRequest> for DispatchService {
    type Response = ();
    type Error = crate::error::Error;
    type Future = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DispatchRequest) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move {
            for op in req.ops {
                svc.dispatch_op(req.addr, op).await;
            }
            Ok(())
        })
    }
}

impl DispatchService {
    async fn dispatch_op(&self, addr: SocketAddr, mut op: PendingOperation) {
        // Large Write payloads require block-wise transfer (RFC 7959).
        if let LwM2mCommand::Write { ref value, .. } = op.command {
            if value.len() > BLOCK_THRESHOLD {
                let max_blocks = value.len().div_ceil(BLOCK_THRESHOLD);
                let start_mid = self.alloc_mid_range(max_blocks as u16);
                tokio::spawn(dispatch_block_write(
                    self.socket.clone(),
                    self.registry.clone(),
                    self.block_acks.clone(),
                    addr,
                    op,
                    self.event_sender.clone(),
                    start_mid,
                ));
                return;
            }
        }

        // CoAP token: 8 bytes, lower 4 bytes = op id (u32), upper 4 bytes zero.
        let token: [u8; 8] = {
            let mut arr = [0u8; 8];
            arr[..4].copy_from_slice(&op.id.to_le_bytes());
            arr
        };
        let mid = self.next_mid();

        let packet = match build_request(&op.command, token, mid) {
            Ok(p) => p,
            Err(e) => {
                warn!(op_id = op.id, "Failed to build CoAP request: {e}");
                let _ = op.response_tx.send(Err(LwM2mError::BadRequest));
                return;
            }
        };

        let coap_msg = if tracing::enabled!(tracing::Level::DEBUG) {
            super::coap_summary(&packet)
        } else {
            String::new()
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
        self.registry.place_in_flight(addr, token, op).await;

        // Send with retransmit loop (Confirmable).
        let socket = self.socket.clone();
        let registry = self.registry.clone();
        let event_sender = self.event_sender.clone();
        tokio::spawn(async move {
            let mut timeout_ms = ACK_TIMEOUT_MS;
            let mut attempts = 0u8;

            loop {
                set_tclass(&socket, TC_ENCRYPTED);
                if let Err(e) = socket.send_to(&bytes, addr).await {
                    warn!(%addr, "Failed to send: {e}");
                    mark_device_offline(&registry, addr, &event_sender).await;
                    break;
                }
                debug!(%addr, attempt = attempts + 1, coap = %coap_msg, "CoAP tx");

                sleep(Duration::from_millis(timeout_ms)).await;

                // Check if the op was already completed (ACK received).
                if !registry.is_in_flight(addr, &token).await {
                    return;
                }

                attempts += 1;
                if attempts >= MAX_RETRANSMIT {
                    if let Some(op) = registry.complete_in_flight(addr, &token).await {
                        warn!(%addr, op_id = op.id, "Device unreachable, operation timed out");
                        let _ = op.response_tx.send(Err(LwM2mError::Timeout));
                    }
                    mark_device_offline(&registry, addr, &event_sender).await;
                    return;
                }

                // Exponential backoff.
                timeout_ms *= 2;
            }
        });
    }
}

// ── Run loop ─────────────────────────────────────────────────────────────────

pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    mut dispatch_rx: mpsc::Receiver<DispatchRequest>,
    event_sender: EventSender,
    block_acks: BlockAckMap,
    cancel: CancellationToken,
) -> Result<()> {
    let mut service = DispatchService::new(socket, registry, event_sender, block_acks);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("CoAP dispatch task shutting down");
                return Ok(());
            }
            Some(req) = dispatch_rx.recv() => {
                if let Err(e) = service.call(req).await {
                    warn!("Dispatch error: {e}");
                }
            }
        }
    }
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
        first_ack_tx,
        ..
    } = op;
    let mut first_ack_tx = first_ack_tx;
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
                mark_device_offline(&registry, addr, &event_sender).await;
                return;
            }
            debug!(%addr, op_id, block_num, more, szx, bytes = chunk.len(), "CoAP tx Block1 PUT");

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
                        mark_device_offline(&registry, addr, &event_sender).await;
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
                // Signal the FOTA handler that the transfer has started.
                if let Some(tx) = first_ack_tx.take() {
                    let _ = tx.send(Ok(ResourceValue::CoapResponse {
                        class: 2,
                        detail: 31,
                    }));
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
                super::encode_content_format(*content_format),
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

async fn mark_device_offline(
    registry: &DeviceRegistry,
    addr: SocketAddr,
    event_sender: &EventSender,
) {
    if let Some(endpoint) = registry.set_device_offline(addr).await {
        info!(device = %endpoint, activity = "connection-status", "Device is offline");
        event_sender.send_connection_status(&endpoint, false);
    }
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
        super::encode_content_format(content_format),
    );
    packet.add_option(CoapOption::Block1, block1.to_vec());
    packet.payload = chunk.to_vec();
    packet
        .to_bytes()
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))
}
