use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Instant,
};

use tokio::sync::Mutex;
use tracing::{info, warn};

/// State for one in-progress bootstrap exchange with a device.
pub struct BootstrapSession {
    pub endpoint: String,
    pub device_addr: SocketAddr,
    pub started_at: Instant,
    /// Raw SenML+CBOR payload from the device's GET /0/0 response.
    pub pubkey_payload: Option<Vec<u8>>,
}

struct RegistryInner {
    /// Active GET /0/0 requests keyed by the 4-byte token (padded to 8).
    by_token: HashMap<[u8; 8], BootstrapSession>,
    /// Reverse index: endpoint name → token, so duplicate /bs requests can be ignored.
    by_endpoint: HashMap<String, [u8; 8]>,
    /// Cached public-key payloads for devices that completed the GET /0/0 phase.
    /// Keyed by endpoint name; cleared once the bootstrap write phase is done.
    cert_cache: HashMap<String, Vec<u8>>,
    mid_counter: u16,
}

/// Thread-safe registry of active bootstrap sessions.
#[derive(Clone)]
pub struct BootstrapRegistry {
    inner: Arc<Mutex<RegistryInner>>,
    /// Token IDs start at 0x8000_0000 to avoid collisions with regular op IDs (1…).
    token_counter: Arc<AtomicU32>,
}

impl BootstrapRegistry {
    pub fn new() -> Self {
        // Seed from wall-clock nanoseconds so MID and token are different on every
        // service restart. The device uses (src-addr, MID) for duplicate detection;
        // a fixed MID causes the device to silently drop what it sees as a replay.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();

        // Keep high bit set on token so it never collides with regular op tokens (0…0x7FFF_FFFF).
        let token_start = 0x8000_0000u32 | (seed & 0x7FFF_FFFF);
        // MID is u16; use lower 14 bits of seed so it won't be the predictable 0xC000.
        let mid_start = (seed >> 2) as u16;

        Self {
            inner: Arc::new(Mutex::new(RegistryInner {
                by_token: HashMap::new(),
                by_endpoint: HashMap::new(),
                cert_cache: HashMap::new(),
                mid_counter: mid_start,
            })),
            token_counter: Arc::new(AtomicU32::new(token_start)),
        }
    }

    /// Begin a bootstrap session for the given endpoint.
    ///
    /// Returns `None` if a GET /0/0 is already in flight for this endpoint
    /// (duplicate /bs should be silently ignored).
    /// Returns `Some((token_bytes, mid))` to use for the outbound GET /0/0.
    pub async fn begin(&self, endpoint: String, device_addr: SocketAddr) -> Option<([u8; 8], u16)> {
        let mut inner = self.inner.lock().await;

        if inner.by_endpoint.contains_key(&endpoint) {
            return None; // GET /0/0 already in flight
        }

        let id = self.token_counter.fetch_add(1, Ordering::Relaxed);
        let mut token = [0u8; 8];
        token[..4].copy_from_slice(&id.to_le_bytes());

        let mid = inner.mid_counter;
        inner.mid_counter = inner.mid_counter.wrapping_add(1);

        inner.by_endpoint.insert(endpoint.clone(), token);
        inner.by_token.insert(token, BootstrapSession {
            endpoint: endpoint.clone(),
            device_addr,
            started_at: Instant::now(),
            pubkey_payload: None,
        });

        info!(%endpoint, %device_addr, token = hex(&token[..4]), "bootstrap session started");
        Some((token, mid))
    }

    /// Called when the ACK for GET /0/0 arrives.
    /// Caches the pubkey payload and returns the completed session.
    pub async fn complete(&self, token: &[u8; 8], payload: Vec<u8>) -> Option<BootstrapSession> {
        let mut inner = self.inner.lock().await;
        if let Some(mut session) = inner.by_token.remove(token) {
            inner.by_endpoint.remove(&session.endpoint);
            info!(
                endpoint = %session.endpoint,
                bytes = payload.len(),
                "bootstrap: received public key payload"
            );
            inner.cert_cache.insert(session.endpoint.clone(), payload.clone());
            session.pubkey_payload = Some(payload);
            Some(session)
        } else {
            None
        }
    }

    /// Returns true if a GET /0/0 with this token is pending.
    pub async fn is_pending(&self, token: &[u8; 8]) -> bool {
        self.inner.lock().await.by_token.contains_key(token)
    }

    /// Returns true if the device's public key has already been retrieved and cached.
    pub async fn has_cert(&self, endpoint: &str) -> bool {
        self.inner.lock().await.cert_cache.contains_key(endpoint)
    }

    /// Expire GET /0/0 sessions older than `max_secs` (device never responded).
    pub async fn expire_stale(&self, max_secs: u64) {
        let mut inner = self.inner.lock().await;
        let stale: Vec<[u8; 8]> = inner
            .by_token
            .iter()
            .filter(|(_, s)| s.started_at.elapsed().as_secs() > max_secs)
            .map(|(t, _)| *t)
            .collect();
        for token in stale {
            if let Some(s) = inner.by_token.remove(&token) {
                inner.by_endpoint.remove(&s.endpoint);
                warn!(endpoint = %s.endpoint, "bootstrap session timed out");
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join("")
}
