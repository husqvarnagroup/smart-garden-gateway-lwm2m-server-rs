use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use coap_lite::Packet;
use tokio::{net::UdpSocket, sync::mpsc};
use tower::Service;

use crate::{
    ipc::event::EventSender, lwm2m::bootstrap::BootstrapRegistry, lwm2m::ipso::SharedIpso,
    persistence::PersistenceStore, registry::DeviceRegistry,
};

use super::{server::DispatchRequest, BlockAckMap};

/// An inbound CoAP request received from the UDP transport.
#[derive(Debug, Clone)]
pub struct CoapRequest {
    /// The parsed CoAP packet.
    pub packet: Packet,
    /// The source address of the UDP datagram.
    pub addr: SocketAddr,
}

/// A CoAP response to send back over UDP.
#[derive(Debug)]
pub struct CoapResponse {
    /// Serialised CoAP packet bytes to send.
    pub bytes: Vec<u8>,
    /// The destination address.
    pub addr: SocketAddr,
    /// IPv6 Traffic Class to set on the outgoing socket.
    pub traffic_class: u32,
}

// ── LwM2M 1.1 / RFC 7252 CoAP retransmission parameters ─────────────────────

/// ACK_TIMEOUT: The initial base timeout before retransmission (seconds).
const ACK_TIMEOUT: f64 = 2.0;
/// ACK_RANDOM_FACTOR: Multiplied with ACK_TIMEOUT to get the upper bound
/// of the initial random timeout window.
const ACK_RANDOM_FACTOR: f64 = 1.5;
/// MAX_RETRANSMIT: Maximum number of retransmissions before giving up.
const MAX_RETRANSMIT: u32 = 4;

/// Tower retry policy implementing LwM2M 1.1 (RFC 7252 §4.2) exponential
/// backoff for CoAP Confirmable message retransmission.
///
/// The initial timeout is chosen randomly from \[ACK_TIMEOUT, ACK_TIMEOUT × ACK_RANDOM_FACTOR\]
/// (i.e. 2–3 s). Each subsequent retransmission doubles the previous timeout.
/// After MAX_RETRANSMIT (4) failed attempts the request is abandoned.
#[derive(Debug, Clone)]
pub struct CoapRetryPolicy {
    /// Number of retries already attempted.
    attempts: u32,
    /// Current backoff duration (doubles each retry).
    current_backoff: Duration,
}

impl CoapRetryPolicy {
    /// Create a new policy with a randomised initial backoff per RFC 7252.
    pub fn new() -> Self {
        let jitter = rand_core::OsRng.next_u32() as f64 / u32::MAX as f64;
        let initial = ACK_TIMEOUT + jitter * (ACK_TIMEOUT * (ACK_RANDOM_FACTOR - 1.0));
        Self {
            attempts: 0,
            current_backoff: Duration::from_secs_f64(initial),
        }
    }
}

impl Default for CoapRetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

use rand_core::RngCore;

impl tower::retry::Policy<CoapRequest, Option<CoapResponse>, crate::error::Error>
    for CoapRetryPolicy
{
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        _req: &mut CoapRequest,
        result: &mut Result<Option<CoapResponse>, crate::error::Error>,
    ) -> Option<Self::Future> {
        match result {
            // Success — no retry needed.
            Ok(_) => None,
            Err(e) => {
                // Only retry transient I/O errors; protocol-level errors
                // (bad packets, bootstrap failures) are not retryable.
                if !is_transient(e) {
                    return None;
                }

                if self.attempts >= MAX_RETRANSMIT {
                    tracing::warn!(
                        attempts = self.attempts,
                        "CoAP retransmission limit reached, giving up"
                    );
                    return None;
                }

                let backoff = self.current_backoff;
                self.attempts += 1;
                self.current_backoff = self
                    .current_backoff
                    .saturating_mul(2)
                    .min(Duration::from_secs(32));

                tracing::debug!(
                    attempt = self.attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    "Retrying CoAP request after backoff"
                );

                Some(Box::pin(async move {
                    tokio::time::sleep(backoff).await;
                }))
            }
        }
    }

    fn clone_request(&mut self, req: &CoapRequest) -> Option<CoapRequest> {
        Some(req.clone())
    }
}

/// Determine whether an error is transient and worth retrying.
fn is_transient(e: &crate::error::Error) -> bool {
    matches!(e, crate::error::Error::Io(_))
}

// ── CoapService ──────────────────────────────────────────────────────────────

/// Tower service that processes inbound CoAP packets.
///
/// The service encapsulates the full server-side CoAP logic: bootstrap,
/// registration, device data push, and ACK/RST routing.  It returns an
/// optional `CoapResponse`; when `None` no reply is sent (e.g. for ACKs
/// that only update internal state).
#[derive(Clone)]
pub struct CoapService {
    pub(super) socket: Arc<UdpSocket>,
    pub(super) registry: DeviceRegistry,
    pub(super) bootstrap_registry: BootstrapRegistry,
    pub(super) coap_dispatch_tx: mpsc::Sender<DispatchRequest>,
    pub(super) event_sender: EventSender,
    pub(super) ipso: SharedIpso,
    pub(super) persistence: Arc<PersistenceStore>,
    pub(super) block_acks: BlockAckMap,
}

impl CoapService {
    /// Create a new `CoapService` with the given shared state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        socket: Arc<UdpSocket>,
        registry: DeviceRegistry,
        bootstrap_registry: BootstrapRegistry,
        coap_dispatch_tx: mpsc::Sender<DispatchRequest>,
        event_sender: EventSender,
        ipso: SharedIpso,
        persistence: Arc<PersistenceStore>,
        block_acks: BlockAckMap,
    ) -> Self {
        Self {
            socket,
            registry,
            bootstrap_registry,
            coap_dispatch_tx,
            event_sender,
            ipso,
            persistence,
            block_acks,
        }
    }
}

impl Service<CoapRequest> for CoapService {
    type Response = Option<CoapResponse>;
    type Error = crate::error::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CoapRequest) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move { svc.handle_request(req).await })
    }
}

impl CoapService {
    async fn handle_request(
        self,
        req: CoapRequest,
    ) -> Result<Option<CoapResponse>, crate::error::Error> {
        use super::server;

        let ctx = server::ServerCtx {
            socket: &self.socket,
            registry: &self.registry,
            bootstrap_registry: &self.bootstrap_registry,
            coap_dispatch_tx: &self.coap_dispatch_tx,
            event_sender: &self.event_sender,
            ipso: &self.ipso,
            persistence: &self.persistence,
            block_acks: &self.block_acks,
        };

        server::handle_packet_inner(&req.packet, req.addr, &ctx).await?;

        // Responses are sent directly by the handlers via the socket.
        // Returning None here signals the transport that no additional
        // response needs to be sent.
        Ok(None)
    }
}
