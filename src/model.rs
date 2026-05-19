use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    time::Instant,
};

use serde::Serialize;
use tokio::sync::oneshot;

pub type DeviceId = u32;

// ── Resource addressing ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResourcePath {
    pub object_id: u16,
    pub instance_id: u16,
    pub resource_id: u16,
}

impl ResourcePath {
    /// e.g. "3/0/0"
    pub fn as_uri_path(&self) -> String {
        format!("{}/{}/{}", self.object_id, self.instance_id, self.resource_id)
    }
}

// ── LWM2M commands & results ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum LwM2mCommand {
    Read { path: ResourcePath },
    Write { path: ResourcePath, value: Vec<u8>, content_format: u16 },
    Execute { path: ResourcePath, args: Option<Vec<u8>> },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ResourceValue {
    Text(String),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "code")]
pub enum LwM2mError {
    NotFound,
    BadRequest,
    Timeout,
    /// CoAP error class/detail code pair (e.g. 4/4 = Not Found).
    CoapError { class: u8, detail: u8 },
}

pub type LwM2mResult = Result<ResourceValue, LwM2mError>;

// ── Pending operations ───────────────────────────────────────────────────────

pub struct PendingOperation {
    pub id: u32,
    pub command: LwM2mCommand,
    /// Fired when a CoAP response arrives or when the operation times out.
    pub response_tx: oneshot::Sender<LwM2mResult>,
    pub created_at: Instant,
    pub attempts: u8,
}

// ── Device state ─────────────────────────────────────────────────────────────

pub struct Device {
    pub id: DeviceId,
    pub endpoint: String,
    /// Last known remote address. For PPP/IPv6 this is a SocketAddrV6 including scope_id.
    pub addr: SocketAddr,
    /// Lifetime declared by the device at registration (seconds).
    pub lifetime: u32,
    pub registered_at: Instant,
    pub last_contact: Instant,
    /// Object links reported at registration, e.g. ["3/0", "4/0"].
    pub objects: Vec<String>,
    /// Object versions from the link-format `ver` attribute, keyed by object id.
    pub object_versions: HashMap<u32, String>,
    /// Operations waiting to be sent on next device contact.
    pub pending_ops: VecDeque<PendingOperation>,
    /// Operations sent but awaiting CoAP ACK, keyed by 8-byte token.
    pub in_flight: HashMap<[u8; 8], PendingOperation>,
}

impl Device {
    pub fn new(
        id: DeviceId,
        endpoint: String,
        addr: SocketAddr,
        lifetime: u32,
        objects: Vec<String>,
        object_versions: HashMap<u32, String>,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            endpoint,
            addr,
            lifetime,
            registered_at: now,
            last_contact: now,
            objects,
            object_versions,
            pending_ops: VecDeque::new(),
            in_flight: HashMap::new(),
        }
    }

    /// True if the device registration has expired (plus a grace period).
    pub fn is_expired(&self, grace_secs: u32) -> bool {
        let total = self.lifetime as u64 + grace_secs as u64;
        self.last_contact.elapsed().as_secs() > total
    }
}

