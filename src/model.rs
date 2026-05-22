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
        format!(
            "{}/{}/{}",
            self.object_id, self.instance_id, self.resource_id
        )
    }
}

// ── LWM2M commands & results ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum LwM2mCommand {
    Read {
        path: ResourcePath,
    },
    Write {
        path: ResourcePath,
        value: Vec<u8>,
        content_format: u16,
    },
    Execute {
        path: ResourcePath,
        args: Option<Vec<u8>>,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ResourceValue {
    Text(String),
    CoapResponse { class: u8, detail: u8 },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "code")]
pub enum LwM2mError {
    NotFound,
    BadRequest,
    Timeout,
    /// CoAP error class/detail code pair (e.g. 4/4 = Not Found).
    CoapError {
        class: u8,
        detail: u8,
    },
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
    /// Reset on every POST /rd (new) and POST /rd/<id> (update). Drives expiry.
    pub last_registered_at: Instant,
    pub last_contact: Instant,
    /// Object links reported at registration, e.g. ["3/0", "4/0"].
    pub objects: Vec<String>,
    /// Object versions from the link-format `ver` attribute, keyed by object id.
    pub object_versions: HashMap<u32, String>,
    /// LWM2M version from /rd `lwm2m=` query param (e.g. "1.1").
    pub lwm2m_version: String,
    /// Binding mode from /rd `b=` query param (e.g. "U").
    pub binding_mode: String,
    /// Accumulated merged IPSO state from all /dp payloads + connection_status.
    pub state: serde_json::Value,
    /// None = unknown, Some(true) = online, Some(false) = offline.
    pub online: Option<bool>,
    /// When the device transitioned to offline; used to bound the 6-hour retry window.
    pub offline_since: Option<Instant>,
    /// When we last sent a connectivity ping; used to pace ping scheduling.
    pub last_ping_attempt: Option<Instant>,
    /// Operations waiting to be sent on next device contact.
    pub pending_ops: VecDeque<PendingOperation>,
    /// Operations sent but awaiting CoAP ACK, keyed by 8-byte token.
    pub in_flight: HashMap<[u8; 8], PendingOperation>,
}

impl Device {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: DeviceId,
        endpoint: String,
        addr: SocketAddr,
        lifetime: u32,
        objects: Vec<String>,
        object_versions: HashMap<u32, String>,
        lwm2m_version: String,
        binding_mode: String,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            endpoint,
            addr,
            lifetime,
            registered_at: now,
            last_registered_at: now,
            last_contact: now,
            objects,
            object_versions,
            lwm2m_version,
            binding_mode,
            state: serde_json::Value::Object(serde_json::Map::new()),
            online: Some(true),
            offline_since: None,
            last_ping_attempt: None,
            pending_ops: VecDeque::new(),
            in_flight: HashMap::new(),
        }
    }

    /// True if the device registration has expired (plus a grace period).
    pub fn is_expired(&self, grace_secs: u32) -> bool {
        let total = self.lifetime as u64 + grace_secs as u64;
        self.last_registered_at.elapsed().as_secs() > total
    }
}
