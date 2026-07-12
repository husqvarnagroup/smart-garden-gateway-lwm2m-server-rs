//! CoAP client (downlink requests to devices) as a tower service stack.
//!
//! Stack (outer → inner):
//!   [`BlockWriteLayer`] — client side of RFC 7959 Block1: splits oversized
//!     PUT payloads into per-block exchanges through the inner service.
//!   [`ExchangeService`] — one confirmable request/ACK exchange with RFC 7252
//!     retransmission. ACKs are routed in by the server's receive loop via
//!     the shared [`AckMap`].
//!
//! [`run`] drives the stack: it turns each queued [`PendingOperation`] into
//! an [`Exchange`], calls the service and completes the operation's response
//! channel with the decoded result.

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
    block_handler::BlockValue, CoapOption, MessageClass, MessageType, Packet,
    RequestType as Method, ResponseType as Status,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tower::{util::BoxCloneService, Layer, Service, ServiceBuilder, ServiceExt};
use tracing::{debug, info, warn};

use crate::{
    error::Result,
    ipc::event::EventSender,
    model::{LwM2mCommand, LwM2mError, LwM2mResult, PendingOperation, ResourceValue},
    registry::DeviceRegistry,
};

use super::{server::DispatchRequest, set_tclass, token_array, AckMap, TC_ENCRYPTED};

/// CoAP retransmission constants (RFC 7252 defaults).
const ACK_TIMEOUT_MS: u64 = 2_000;
const MAX_RETRANSMIT: u8 = 3;

/// Initial block size for Block1: 512 bytes (SZX=5).
/// The device may negotiate down to a smaller SZX in its Continue responses.
const BLOCK_SZX: u8 = 5;

/// Minimum payload length that triggers block-wise transfer.
pub const BLOCK_THRESHOLD: usize = 1 << (BLOCK_SZX + 4); // 512

/// Errors produced by the CoAP client service stack.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    /// The request packet could not be encoded.
    #[error("CoAP encode error: {0}")]
    Encode(String),
    /// The UDP send itself failed; the device is treated as unreachable.
    #[error("UDP send failed: {0}")]
    Send(String),
    /// No ACK arrived within all retransmission attempts.
    #[error("No ACK after {MAX_RETRANSMIT} attempts")]
    Timeout,
    /// The device rejected the message with a RST.
    #[error("Message rejected by device (RST)")]
    Reset,
    /// The device answered the final Block1 block with 2.31 Continue.
    #[error("Unexpected 2.31 Continue on final block")]
    UnexpectedContinue,
}

/// One confirmable request/response exchange with a device.
pub struct Exchange {
    pub addr: SocketAddr,
    /// Request packet. The token must be set; the Message ID is assigned by
    /// [`ExchangeService`] (fresh per block for block-wise transfers).
    pub packet: Packet,
    /// Operation id, carried for logging only.
    pub op_id: u32,
    /// Fired after the first 2.31 Continue ACK in a block-wise write.
    /// Set only for FOTA uploads; None for all other operations.
    pub first_ack_tx: Option<oneshot::Sender<LwM2mResult>>,
}

/// Boxed CoAP client service: one downlink request in, its final ACK out.
pub type CoapClientService = BoxCloneService<Exchange, Packet, ClientError>;

/// Build the CoAP client service stack.
pub fn coap_client_service(socket: Arc<UdpSocket>, acks: AckMap) -> CoapClientService {
    ServiceBuilder::new()
        .boxed_clone()
        .layer(BlockWriteLayer)
        .service(ExchangeService::new(socket, acks))
}

// ── Exchange service ─────────────────────────────────────────────────────────

/// Tower service performing a single CON request/ACK exchange.
///
/// Retransmission lives here rather than in a `RetryLayer` because RFC 7252
/// §4.2 requires retransmits to be byte-identical (same Message ID) and the
/// ACK wait doubles per attempt — a retry layer would re-enter `call` and
/// allocate a fresh MID.
#[derive(Clone)]
pub struct ExchangeService {
    socket: Arc<UdpSocket>,
    acks: AckMap,
    mid_counter: Arc<AtomicU16>,
}

impl ExchangeService {
    pub fn new(socket: Arc<UdpSocket>, acks: AckMap) -> Self {
        Self {
            socket,
            acks,
            mid_counter: Arc::new(AtomicU16::new(1)),
        }
    }

    /// Allocate the next Message ID, skipping 0.
    fn next_mid(&self) -> u16 {
        loop {
            let mid = self.mid_counter.fetch_add(1, Ordering::Relaxed);
            if mid != 0 {
                return mid;
            }
        }
    }
}

impl Service<Exchange> for ExchangeService {
    type Response = Packet;
    type Error = ClientError;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<Packet, ClientError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), ClientError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut ex: Exchange) -> Self::Future {
        let socket = Arc::clone(&self.socket);
        let acks = Arc::clone(&self.acks);
        ex.packet.header.message_id = self.next_mid();

        Box::pin(async move {
            let token = token_array(ex.packet.get_token());
            let bytes = ex
                .packet
                .to_bytes()
                .map_err(|e| ClientError::Encode(format!("{e:?}")))?;
            let coap_msg = if tracing::enabled!(tracing::Level::DEBUG) {
                super::coap_summary(&ex.packet)
            } else {
                String::new()
            };

            let mut timeout_ms = ACK_TIMEOUT_MS;
            for attempt in 1..=MAX_RETRANSMIT {
                let (tx, rx) = oneshot::channel::<Packet>();
                acks.lock().await.insert(token, tx);

                set_tclass(&socket, TC_ENCRYPTED);
                if let Err(e) = socket.send_to(&bytes, ex.addr).await {
                    acks.lock().await.remove(&token);
                    return Err(ClientError::Send(e.to_string()));
                }
                debug!(addr = %ex.addr, attempt, coap = %coap_msg, "CoAP tx");

                match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
                    Ok(Ok(ack)) => return Ok(ack),
                    // Sender was dropped by the RST handler.
                    Ok(Err(_)) => return Err(ClientError::Reset),
                    Err(_) => {
                        // Timeout — remove the stale sender, then retransmit
                        // with exponential backoff.
                        acks.lock().await.remove(&token);
                        timeout_ms *= 2;
                    }
                }
            }
            Err(ClientError::Timeout)
        })
    }
}

// ── Block-wise write layer ────────────────────────────────────────────────────

/// Tower service implementing the client side of RFC 7959 Block1: PUT
/// payloads above [`BLOCK_THRESHOLD`] are sent as a sequence of exchanges,
/// honoring the device's SZX negotiation in its Continue responses (§2.5).
/// All other requests pass through unchanged.
#[derive(Clone)]
pub struct BlockWriteService<S> {
    inner: S,
}

impl<S> Service<Exchange> for BlockWriteService<S>
where
    S: Service<Exchange, Response = Packet, Error = ClientError> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Packet;
    type Error = ClientError;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<Packet, ClientError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), ClientError>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut ex: Exchange) -> Self::Future {
        let needs_block_transfer = ex.packet.header.code == MessageClass::Request(Method::Put)
            && ex.packet.payload.len() > BLOCK_THRESHOLD;
        if !needs_block_transfer {
            return Box::pin(self.inner.call(ex));
        }

        // Move the ready inner service into the future (tower clone idiom).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let addr = ex.addr;
            let op_id = ex.op_id;
            let mut first_ack_tx = ex.first_ack_tx.take();
            let payload = std::mem::take(&mut ex.packet.payload);
            let template = ex.packet;

            let total = payload.len();
            let mut block_size = BLOCK_THRESHOLD;
            let mut offset = 0usize;

            info!(%addr, op_id, szx = BLOCK_SZX, "Block write: {} bytes", total);

            loop {
                let end = (offset + block_size).min(total);
                let more = end < total;
                let block_num = offset / block_size;
                let block1 = BlockValue::new(block_num, more, block_size)
                    .map_err(|e| ClientError::Encode(format!("{e:?}")))?;

                let mut packet = template.clone();
                packet.add_option_as(CoapOption::Block1, block1);
                packet.payload = payload[offset..end].to_vec();
                debug!(%addr, op_id, block_num, more, bytes = end - offset, "Block1 PUT");

                let ack = inner
                    .ready()
                    .await?
                    .call(Exchange {
                        addr,
                        packet,
                        op_id,
                        first_ack_tx: None,
                    })
                    .await?;

                match ack.header.code {
                    MessageClass::Response(Status::Continue) => {
                        if !more {
                            // The last block must not elicit Continue — device error.
                            return Err(ClientError::UnexpectedContinue);
                        }
                        // Signal the FOTA handler that the transfer has started.
                        if let Some(tx) = first_ack_tx.take() {
                            let _ = tx.send(Ok(ResourceValue::CoapResponse {
                                class: 2,
                                detail: 31,
                            }));
                        }
                        // Honor the device's SZX negotiation (RFC 7959 §2.5).
                        if let Some(dev_block) = ack
                            .get_first_option_as::<BlockValue>(CoapOption::Block1)
                            .and_then(|x| x.ok())
                        {
                            if dev_block.size() < block_size {
                                info!(%addr, op_id, old_size = block_size, new_size = dev_block.size(),
                                    "Block write: device negotiated smaller block size");
                                block_size = dev_block.size();
                            }
                        }
                        offset = end;
                    }
                    MessageClass::Response(status) => {
                        let (class, detail) = super::response_type_to_class_detail(status);
                        if class == 2 {
                            info!(%addr, op_id, "Block write complete");
                        } else {
                            warn!(%addr, op_id, block_num, "Block write rejected: {class}.{detail:02}");
                        }
                        return Ok(ack);
                    }
                    // Non-response codes are decoded (to 5.00) by the caller.
                    _ => return Ok(ack),
                }
            }
        })
    }
}

/// Tower layer wrapping a service with [`BlockWriteService`].
#[derive(Clone, Copy, Default)]
pub struct BlockWriteLayer;

impl<S> Layer<S> for BlockWriteLayer {
    type Service = BlockWriteService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BlockWriteService { inner }
    }
}

// ── Dispatch driver ──────────────────────────────────────────────────────────

pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    mut dispatch_rx: mpsc::Receiver<DispatchRequest>,
    event_sender: EventSender,
    acks: AckMap,
    cancel: CancellationToken,
) -> Result<()> {
    let service = coap_client_service(socket, acks);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("CoAP dispatch task shutting down");
                return Ok(());
            }
            Some(req) = dispatch_rx.recv() => {
                for op in req.ops {
                    tokio::spawn(dispatch_op(
                        service.clone(),
                        registry.clone(),
                        req.addr,
                        op,
                        event_sender.clone(),
                    ));
                }
            }
        }
    }
}

/// Send one operation through the client service stack and complete its
/// response channel with the outcome.
async fn dispatch_op(
    service: CoapClientService,
    registry: DeviceRegistry,
    addr: SocketAddr,
    mut op: PendingOperation,
    event_sender: EventSender,
) {
    // CoAP token: 8 bytes, lower 4 bytes = op id (u32), upper 4 bytes zero.
    // AtomicU64 is unavailable on 32-bit ARM, so op ids are u32.
    let token: [u8; 8] = {
        let mut arr = [0u8; 8];
        arr[..4].copy_from_slice(&op.id.to_le_bytes());
        arr
    };

    let packet = match build_request(&op.command, token) {
        Ok(p) => p,
        Err(e) => {
            warn!(op_id = op.id, "Failed to build CoAP request: {e}");
            let _ = op.response_tx.send(Err(LwM2mError::BadRequest));
            return;
        }
    };

    let exchange = Exchange {
        addr,
        packet,
        op_id: op.id,
        first_ack_tx: op.first_ack_tx.take(),
    };

    let result = match service.oneshot(exchange).await {
        Ok(ack) => super::coap_response_to_result(&ack),
        Err(ClientError::Timeout) => {
            warn!(%addr, op_id = op.id, "Device unreachable, operation timed out");
            mark_device_offline(&registry, addr, &event_sender).await;
            Err(LwM2mError::Timeout)
        }
        Err(ClientError::Send(e)) => {
            warn!(%addr, op_id = op.id, "Failed to send: {e}");
            mark_device_offline(&registry, addr, &event_sender).await;
            Err(LwM2mError::Timeout)
        }
        Err(ClientError::Reset) | Err(ClientError::UnexpectedContinue) => {
            Err(LwM2mError::CoapError {
                class: 5,
                detail: 0,
            })
        }
        Err(ClientError::Encode(e)) => {
            warn!(op_id = op.id, "Failed to encode CoAP packet: {e}");
            Err(LwM2mError::BadRequest)
        }
    };
    let _ = op.response_tx.send(result);
}

fn build_request(command: &LwM2mCommand, token: [u8; 8]) -> Result<Packet> {
    let mut packet = Packet::new();
    packet.header.set_type(MessageType::Confirmable);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lwm2m::new_ack_map;
    use std::sync::Mutex as StdMutex;

    const TOKEN: [u8; 8] = [0x2A, 0, 0, 0, 0, 0, 0, 0];

    fn put_packet(payload: &[u8]) -> Packet {
        let mut packet = Packet::new();
        packet.header.set_type(MessageType::Confirmable);
        packet.header.code = MessageClass::Request(Method::Put);
        packet.set_token(TOKEN.to_vec());
        add_uri_path(&mut packet, "5/0/0");
        packet.add_option(CoapOption::ContentFormat, vec![42]);
        packet.payload = payload.to_vec();
        packet
    }

    fn exchange(packet: Packet, addr: SocketAddr) -> Exchange {
        Exchange {
            addr,
            packet,
            op_id: 42,
            first_ack_tx: None,
        }
    }

    fn any_peer() -> SocketAddr {
        "127.0.0.1:9".parse().unwrap()
    }

    // ── ExchangeService ──────────────────────────────────────────────────────

    async fn exchange_service() -> (ExchangeService, AckMap, UdpSocket) {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let acks = new_ack_map();
        (ExchangeService::new(socket, Arc::clone(&acks)), acks, peer)
    }

    /// Responder standing in for the server receive loop: waits until the
    /// exchange has registered its token, then routes an ACK (or drops the
    /// sender to simulate a RST).
    fn respond_via_ack_map(acks: AckMap, status: Option<Status>) {
        tokio::spawn(async move {
            loop {
                if let Some(tx) = acks.lock().await.remove(&TOKEN) {
                    if let Some(status) = status {
                        let mut ack = Packet::new();
                        ack.header.set_type(MessageType::Acknowledgement);
                        ack.header.code = MessageClass::Response(status);
                        ack.set_token(TOKEN.to_vec());
                        let _ = tx.send(ack);
                    }
                    // None: drop tx — the exchange must report Reset.
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }

    #[tokio::test]
    async fn exchange_resolves_with_routed_ack() {
        let (mut svc, acks, peer) = exchange_service().await;
        respond_via_ack_map(acks, Some(Status::Changed));

        let ack = svc
            .ready()
            .await
            .unwrap()
            .call(exchange(put_packet(b"x"), peer.local_addr().unwrap()))
            .await
            .unwrap();

        assert_eq!(ack.header.code, MessageClass::Response(Status::Changed));
    }

    #[tokio::test]
    async fn exchange_reports_reset_when_sender_dropped() {
        let (mut svc, acks, peer) = exchange_service().await;
        respond_via_ack_map(acks, None);

        let result = svc
            .ready()
            .await
            .unwrap()
            .call(exchange(put_packet(b"x"), peer.local_addr().unwrap()))
            .await;

        assert!(matches!(result, Err(ClientError::Reset)));
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_times_out_after_identical_retransmits() {
        let (mut svc, acks, peer) = exchange_service().await;
        let addr = peer.local_addr().unwrap();

        let result = svc
            .ready()
            .await
            .unwrap()
            .call(exchange(put_packet(b"x"), addr))
            .await;
        assert!(matches!(result, Err(ClientError::Timeout)));
        assert!(acks.lock().await.is_empty(), "stale sender must be removed");

        // Drain with real time: paused time plus socket IO races the timeout.
        tokio::time::resume();

        // RFC 7252 §4.2: all retransmits must be byte-identical (same MID).
        let mut datagrams = Vec::new();
        let mut buf = [0u8; 128];
        while let Ok(Ok((len, _))) =
            tokio::time::timeout(Duration::from_millis(50), peer.recv_from(&mut buf)).await
        {
            datagrams.push(buf[..len].to_vec());
        }
        assert_eq!(datagrams.len(), MAX_RETRANSMIT as usize);
        assert!(datagrams.windows(2).all(|w| w[0] == w[1]));
    }

    // ── BlockWriteService ────────────────────────────────────────────────────

    /// Inner service standing in for the exchange transport: records every
    /// request packet and answers like a device would.
    #[derive(Clone)]
    struct MockDevice {
        calls: Arc<StdMutex<Vec<Packet>>>,
        /// Block size the device negotiates down to in its Continue responses.
        negotiate: Option<usize>,
        final_status: Status,
        /// Answer every block — including the final one — with 2.31 Continue.
        always_continue: bool,
    }

    impl MockDevice {
        fn new(final_status: Status) -> Self {
            Self {
                calls: Arc::new(StdMutex::new(Vec::new())),
                negotiate: None,
                final_status,
                always_continue: false,
            }
        }
    }

    impl Service<Exchange> for MockDevice {
        type Response = Packet;
        type Error = ClientError;
        type Future =
            Pin<Box<dyn Future<Output = std::result::Result<Packet, ClientError>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), ClientError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, ex: Exchange) -> Self::Future {
            self.calls.lock().unwrap().push(ex.packet.clone());
            let block1 = ex
                .packet
                .get_first_option_as::<BlockValue>(CoapOption::Block1)
                .and_then(|x| x.ok());

            let mut ack = Packet::new();
            ack.header.set_type(MessageType::Acknowledgement);
            ack.set_token(ex.packet.get_token().to_vec());
            match block1 {
                Some(b) if b.more || self.always_continue => {
                    ack.header.code = MessageClass::Response(Status::Continue);
                    let size = self.negotiate.unwrap_or_else(|| b.size());
                    ack.add_option_as(
                        CoapOption::Block1,
                        BlockValue::new(b.num as usize, b.more, size).unwrap(),
                    );
                }
                _ => ack.header.code = MessageClass::Response(self.final_status),
            }
            Box::pin(std::future::ready(Ok(ack)))
        }
    }

    fn block1_of(packet: &Packet) -> Option<BlockValue> {
        packet
            .get_first_option_as::<BlockValue>(CoapOption::Block1)
            .and_then(|x| x.ok())
    }

    #[tokio::test]
    async fn small_put_passes_through_unsplit() {
        let mock = MockDevice::new(Status::Changed);
        let calls = Arc::clone(&mock.calls);
        let mut svc = BlockWriteLayer.layer(mock);

        let payload = vec![0xAB; BLOCK_THRESHOLD]; // at threshold: no split
        let ack = svc
            .ready()
            .await
            .unwrap()
            .call(exchange(put_packet(&payload), any_peer()))
            .await
            .unwrap();

        assert_eq!(ack.header.code, MessageClass::Response(Status::Changed));
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(block1_of(&calls[0]).is_none());
        assert_eq!(calls[0].payload, payload);
    }

    #[tokio::test]
    async fn large_put_is_split_and_reassembles() {
        let mock = MockDevice::new(Status::Changed);
        let calls = Arc::clone(&mock.calls);
        let mut svc = BlockWriteLayer.layer(mock);

        let payload: Vec<u8> = (0..1200u32).map(|i| i as u8).collect();
        let ack = svc
            .ready()
            .await
            .unwrap()
            .call(exchange(put_packet(&payload), any_peer()))
            .await
            .unwrap();

        assert_eq!(ack.header.code, MessageClass::Response(Status::Changed));
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 3); // 512 + 512 + 176

        let reassembled: Vec<u8> = calls.iter().flat_map(|p| p.payload.clone()).collect();
        assert_eq!(reassembled, payload);

        for (i, packet) in calls.iter().enumerate() {
            let b = block1_of(packet).expect("every block must carry Block1");
            assert_eq!(usize::from(b.num), i);
            assert_eq!(b.more, i < 2);
            assert_eq!(packet.get_token(), TOKEN);
        }
    }

    #[tokio::test]
    async fn block_write_fires_first_ack_after_first_continue() {
        let mut svc = BlockWriteLayer.layer(MockDevice::new(Status::Changed));

        let (tx, mut rx) = oneshot::channel();
        let mut ex = exchange(put_packet(&vec![1u8; 700]), any_peer());
        ex.first_ack_tx = Some(tx);

        svc.ready().await.unwrap().call(ex).await.unwrap();

        match rx.try_recv() {
            Ok(Ok(ResourceValue::CoapResponse {
                class: 2,
                detail: 31,
            })) => {}
            other => panic!("expected first-ack Continue signal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn block_write_honors_szx_negotiation() {
        let mut mock = MockDevice::new(Status::Changed);
        mock.negotiate = Some(128);
        let calls = Arc::clone(&mock.calls);
        let mut svc = BlockWriteLayer.layer(mock);

        let payload: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();
        svc.ready()
            .await
            .unwrap()
            .call(exchange(put_packet(&payload), any_peer()))
            .await
            .unwrap();

        let calls = calls.lock().unwrap();
        // First block at 512, remaining 512 bytes in 128-byte blocks.
        assert_eq!(calls[0].payload.len(), 512);
        assert!(calls[1..].iter().all(|p| p.payload.len() <= 128));

        let reassembled: Vec<u8> = calls.iter().flat_map(|p| p.payload.clone()).collect();
        assert_eq!(reassembled, payload);
    }

    #[tokio::test]
    async fn continue_on_final_block_is_an_error() {
        let mut mock = MockDevice::new(Status::Changed);
        mock.always_continue = true;
        let mut svc = BlockWriteLayer.layer(mock);

        let result = svc
            .ready()
            .await
            .unwrap()
            .call(exchange(put_packet(&vec![1u8; 700]), any_peer()))
            .await;

        assert!(matches!(result, Err(ClientError::UnexpectedContinue)));
    }

    #[tokio::test]
    async fn block_error_response_is_returned_to_caller() {
        let mut svc = BlockWriteLayer.layer(MockDevice::new(Status::NotFound));

        let ack = svc
            .ready()
            .await
            .unwrap()
            .call(exchange(put_packet(&vec![1u8; 700]), any_peer()))
            .await
            .unwrap();

        // The caller decodes the final response; 4.04 must survive the layer.
        assert_eq!(ack.header.code, MessageClass::Response(Status::NotFound));
        assert!(matches!(
            crate::lwm2m::coap_response_to_result(&ack),
            Err(LwM2mError::CoapError {
                class: 4,
                detail: 4
            })
        ));
    }
}
