use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use tempfile::TempDir;
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;

use lwm2m_gateway::{
    bootstrap::BootstrapRegistry,
    coap,
    coap::server::DispatchRequest,
    event::{self, EventSender},
    housekeeping,
    ipc,
    ipso::IpsoModel,
    persistence::PersistenceStore,
    registry::DeviceRegistry,
};

#[allow(dead_code)]
pub struct TestGateway {
    /// Actual UDP address the CoAP server is listening on (loopback, OS-assigned port).
    pub coap_addr: SocketAddr,
    /// Path to the IPC command socket inside the temp dir.
    pub ipc_path: PathBuf,
    /// Path to the event socket inside the temp dir.
    pub event_path: PathBuf,
    pub cancel: CancellationToken,
    pub registry: DeviceRegistry,
    pub bootstrap_registry: BootstrapRegistry,
    /// Shared event sender — tests can emit events or inspect the broadcast channel.
    pub event_sender: EventSender,
    // Keeps the temp directory alive for the lifetime of the gateway.
    _tmp: TempDir,
}

impl TestGateway {
    pub async fn start() -> Self {
        let tmp = TempDir::new().expect("temp dir");
        let ipc_path = tmp.path().join("cmd.ipc");
        let event_path = tmp.path().join("event.ipc");

        // Bind CoAP UDP to loopback:0 — OS picks a free port.
        let bind_addr: SocketAddr = "[::1]:0".parse().unwrap();
        let socket: Arc<UdpSocket> = coap::bind(bind_addr, None).await.expect("coap bind");
        let coap_addr = socket.local_addr().expect("local addr");

        let registry = DeviceRegistry::new();
        let bootstrap_registry = BootstrapRegistry::new(
            vec![0u8; 16],
            Some("coap://[::1]".into()),
        );
        let event_sender = EventSender::new();
        let (dispatch_tx, dispatch_rx) = mpsc::channel::<DispatchRequest>(256);
        let dispatch_tx_ipc = dispatch_tx.clone();
        let cancel = CancellationToken::new();

        let persistence = Arc::new(PersistenceStore::new(
            tmp.path().join("persist"),
            "coap://[::1]",
        ));
        tokio::spawn(coap::server::run(
            socket.clone(),
            registry.clone(),
            bootstrap_registry.clone(),
            dispatch_tx,
            event_sender.clone(),
            Arc::new(IpsoModel::default()),
            persistence,
            cancel.clone(),
        ));
        tokio::spawn(coap::client::run(
            socket,
            registry.clone(),
            dispatch_rx,
            event_sender.clone(),
            cancel.clone(),
        ));
        tokio::spawn(housekeeping::run(
            registry.clone(),
            bootstrap_registry.clone(),
            cancel.clone(),
        ));
        tokio::spawn(ipc::run(
            ipc_path.clone(),
            bootstrap_registry.clone(),
            registry.clone(),
            Arc::new(IpsoModel::default()),
            dispatch_tx_ipc,
            cancel.clone(),
        ));
        tokio::spawn(event::run(event_path.clone(), event_sender.clone(), cancel.clone()));

        // Yield to let tasks reach their accept/recv loops.
        tokio::time::sleep(Duration::from_millis(10)).await;

        Self { coap_addr, ipc_path, event_path, cancel, registry, bootstrap_registry, event_sender, _tmp: tmp }
    }

    pub async fn stop(self) {
        self.cancel.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
