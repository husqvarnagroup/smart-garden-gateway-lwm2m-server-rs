use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use tokio::{io::AsyncWriteExt, net::UnixListener, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::Result;

pub const DEFAULT_EVENT_SOCKET_PATH: &str = "/tmp/lwm2mserver-event.ipc";

/// Cheap-to-clone handle for pushing newline-framed JSON events to all connected clients.
#[derive(Clone)]
pub struct EventSender {
    tx: broadcast::Sender<String>,
    seq: Arc<AtomicU32>,
}

impl EventSender {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { tx, seq: Arc::new(AtomicU32::new(0)) }
    }

    /// Emit an `includable_device/<id>` update to all connected clients.
    pub fn send_includable(
        &self,
        id: u32,
        identifier: &str,
        inclusion_started: bool,
        inclusion_completed: bool,
    ) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let ts = unix_ts();
        let msg = serde_json::json!([{
            "op": "update",
            "entity": {
                "service": "lwm2mserver",
                "path": format!("includable_device/{id}")
            },
            "payload": {
                "identifier":          {"vs": identifier, "ts": ts},
                "protocol":            {"vi": 1, "ts": ts},
                "inclusion_started":   {"vb": inclusion_started,   "ts": ts},
                "inclusion_completed": {"vb": inclusion_completed, "ts": ts},
                "inclusion_error":     {"vi": 0, "ts": ts},
                "_urn": "urn:oma:lwm2m:x:28170:0.1"
            },
            "metadata": {"source": "lwm2mserver", "sequence": seq}
        }]);
        let _ = self.tx.send(format!("{msg}\n"));
    }

    /// Emit a `device/<endpoint>` update event carrying the IPSO-translated /dp payload.
    pub fn send_device_data(&self, endpoint: &str, payload: serde_json::Value) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let msg = serde_json::json!([{
            "op": "update",
            "entity": {"device": endpoint, "path": ""},
            "payload": payload,
            "metadata": {"source": "lwm2mserver", "sequence": seq}
        }]);
        let _ = self.tx.send(format!("{msg}\n"));
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

pub async fn run(path: PathBuf, events: EventSender, cancel: CancellationToken) -> Result<()> {
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    info!(path = %path.display(), "event socket listening");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = std::fs::remove_file(&path);
                info!("event socket shutting down");
                return Ok(());
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        tokio::spawn(relay_to_client(stream, events.subscribe()));
                    }
                    Err(e) => warn!("event socket accept: {e}"),
                }
            }
        }
    }
}

async fn relay_to_client(stream: tokio::net::UnixStream, mut rx: broadcast::Receiver<String>) {
    let (_, mut writer) = tokio::io::split(stream);
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if writer.write_all(msg.as_bytes()).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

pub(crate) fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
