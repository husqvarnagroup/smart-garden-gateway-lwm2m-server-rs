//! Tower service for processing inbound CoAP packets (server side).

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::{net::UdpSocket, sync::mpsc};
use tower::Service;

use crate::{
    ipc::event::EventSender,
    lwm2m::{bootstrap::BootstrapRegistry, ipso::SharedIpso, server::DispatchRequest, BlockAckMap},
    persistence::PersistenceStore,
    registry::DeviceRegistry,
};

use super::{CoapRequest, CoapResponse, ServiceError};

/// Tower service that processes inbound CoAP packets.
///
/// Encapsulates the full server-side CoAP logic: bootstrap, registration,
/// device data push, and ACK/RST routing.  Returns an optional [`CoapResponse`];
/// when `None` no reply is sent (e.g. for ACKs that only update internal state).
#[derive(Clone)]
pub struct CoapService {
    pub(in crate::lwm2m) socket: Arc<UdpSocket>,
    pub(in crate::lwm2m) registry: DeviceRegistry,
    pub(in crate::lwm2m) bootstrap_registry: BootstrapRegistry,
    pub(in crate::lwm2m) coap_dispatch_tx: mpsc::Sender<DispatchRequest>,
    pub(in crate::lwm2m) event_sender: EventSender,
    pub(in crate::lwm2m) ipso: SharedIpso,
    pub(in crate::lwm2m) persistence: Arc<PersistenceStore>,
    pub(in crate::lwm2m) block_acks: BlockAckMap,
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
    type Error = ServiceError;
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
    async fn handle_request(self, req: CoapRequest) -> Result<Option<CoapResponse>, ServiceError> {
        use crate::lwm2m::server;

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

        server::handle_packet_inner(&req.packet, req.addr, &ctx)
            .await
            .map_err(Arc::new)?;

        Ok(None)
    }
}
