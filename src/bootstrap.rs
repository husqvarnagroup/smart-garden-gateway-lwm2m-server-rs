use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Instant,
};

use p256::{elliptic_curve::sec1::ToEncodedPoint, SecretKey};
use rand_core::OsRng;
use tokio::sync::{oneshot, Mutex};
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
    cert_cache: HashMap<String, Vec<u8>>,
    mid_counter: u16,
    /// Oneshot senders for pending write-phase CON requests, keyed by token.
    write_acks: HashMap<[u8; 8], oneshot::Sender<bool>>,
}

/// Thread-safe registry of active bootstrap sessions.
#[derive(Clone)]
pub struct BootstrapRegistry {
    inner: Arc<Mutex<RegistryInner>>,
    /// Token IDs start at 0x8000_0000 to avoid collisions with regular op IDs (1…).
    token_counter: Arc<AtomicU32>,
    /// Compressed P-256 public key (33 bytes) of the ephemeral server keypair.
    server_pubkey_bytes: Arc<Vec<u8>>,
    /// Raw network key loaded from --lb-key-file.
    network_key: Arc<Vec<u8>>,
    /// Server CoAP URI written to /0/1/0 during bootstrap write phase.
    server_uri: Option<Arc<String>>,
}

impl BootstrapRegistry {
    pub fn new(network_key: Vec<u8>, server_uri: Option<String>) -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();

        // Keep high bit set on token so it never collides with regular op tokens (0…0x7FFF_FFFF).
        let token_start = 0x8000_0000u32 | (seed & 0x7FFF_FFFF);
        // MID is u16; use lower 14 bits of seed so it won't be the predictable 0xC000.
        let mid_start = (seed >> 2) as u16;

        // Generate ephemeral P-256 keypair. The compressed public key is written
        // to /0/1/4 (Server Public Key) during bootstrap write phase.
        let secret = SecretKey::random(&mut OsRng);
        let pubkey_bytes = secret.public_key().to_encoded_point(true).as_bytes().to_vec();
        info!(pubkey = %hex(&pubkey_bytes), "bootstrap: generated ephemeral server P-256 keypair");

        Self {
            inner: Arc::new(Mutex::new(RegistryInner {
                by_token: HashMap::new(),
                by_endpoint: HashMap::new(),
                cert_cache: HashMap::new(),
                mid_counter: mid_start,
                write_acks: HashMap::new(),
            })),
            token_counter: Arc::new(AtomicU32::new(token_start)),
            server_pubkey_bytes: Arc::new(pubkey_bytes),
            network_key: Arc::new(network_key),
            server_uri: server_uri.map(Arc::new),
        }
    }

    /// Begin a bootstrap session for the given endpoint.
    ///
    /// Returns `None` if a GET /0/0 is already in flight for this endpoint.
    /// Returns `Some((token_bytes, mid))` to use for the outbound GET /0/0.
    pub async fn begin(&self, endpoint: String, device_addr: SocketAddr) -> Option<([u8; 8], u16)> {
        let mut inner = self.inner.lock().await;

        if inner.by_endpoint.contains_key(&endpoint) {
            return None; // GET /0/0 already in flight
        }

        let (token, mid) = self.alloc_inner(&mut inner);

        inner.by_endpoint.insert(endpoint.clone(), token);
        inner.by_token.insert(
            token,
            BootstrapSession {
                endpoint: endpoint.clone(),
                device_addr,
                started_at: Instant::now(),
                pubkey_payload: None,
            },
        );

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

    /// Returns a clone of the cached cert payload for the given endpoint, if present.
    pub async fn get_cert(&self, endpoint: &str) -> Option<Vec<u8>> {
        self.inner.lock().await.cert_cache.get(endpoint).cloned()
    }

    /// Allocate a fresh token + MID for a write-phase CON request.
    pub async fn alloc_token_mid(&self) -> ([u8; 8], u16) {
        let mut inner = self.inner.lock().await;
        self.alloc_inner(&mut inner)
    }

    /// Register a oneshot receiver that fires when the ACK for `token` arrives.
    /// `true` = 2.xx success; `false` = RST or error.
    pub async fn register_write_ack(&self, token: [u8; 8]) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        self.inner.lock().await.write_acks.insert(token, tx);
        rx
    }

    /// Called from the CoAP receive loop when an ACK matches a write-phase token.
    /// Returns true if the token was found (and the oneshot fired).
    pub async fn complete_write_ack(&self, token: &[u8; 8], success: bool) -> bool {
        if let Some(tx) = self.inner.lock().await.write_acks.remove(token) {
            let _ = tx.send(success);
            true
        } else {
            false
        }
    }

    /// Remove a pending write-ack entry after max retransmits.
    pub async fn cancel_write_ack(&self, token: &[u8; 8]) {
        self.inner.lock().await.write_acks.remove(token);
    }

    /// Compressed P-256 public key of the ephemeral server keypair (33 bytes).
    pub fn server_pubkey_bytes(&self) -> &[u8] {
        &self.server_pubkey_bytes
    }

    /// Raw network key from --lb-key-file.
    pub fn network_key(&self) -> &[u8] {
        &self.network_key
    }

    /// Server CoAP URI to write into /0/1/0 during bootstrap, if configured.
    pub fn server_uri(&self) -> Option<&str> {
        self.server_uri.as_deref().map(|s| s.as_str())
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

    fn alloc_inner(&self, inner: &mut RegistryInner) -> ([u8; 8], u16) {
        let id = self.token_counter.fetch_add(1, Ordering::Relaxed);
        let mut token = [0u8; 8];
        token[..4].copy_from_slice(&id.to_le_bytes());
        let mid = inner.mid_counter;
        inner.mid_counter = inner.mid_counter.wrapping_add(1);
        (token, mid)
    }
}

// ── CBOR encoding ─────────────────────────────────────────────────────────────
//
// Minimal subset of RFC 7049 needed for SenML+CBOR (RFC 8428) payloads.
// Labels used: -2=bn, 0=n, 2=v, 3=vs, 4=vb, 8=vd.

fn cbor_head(major: u8, val: u64) -> Vec<u8> {
    let m = major << 5;
    if val <= 23 {
        vec![m | val as u8]
    } else if val <= 0xFF {
        vec![m | 24, val as u8]
    } else if val <= 0xFFFF {
        vec![m | 25, (val >> 8) as u8, val as u8]
    } else {
        vec![
            m | 26,
            (val >> 24) as u8,
            (val >> 16) as u8,
            (val >> 8) as u8,
            val as u8,
        ]
    }
}

fn cbor_uint(v: u64) -> Vec<u8> {
    cbor_head(0, v)
}

fn cbor_text(s: &str) -> Vec<u8> {
    let mut v = cbor_head(3, s.len() as u64);
    v.extend_from_slice(s.as_bytes());
    v
}

fn cbor_bytes_val(b: &[u8]) -> Vec<u8> {
    let mut v = cbor_head(2, b.len() as u64);
    v.extend_from_slice(b);
    v
}

fn cbor_array_header(n: usize) -> Vec<u8> {
    cbor_head(4, n as u64)
}

fn cbor_map_header(n: usize) -> Vec<u8> {
    cbor_head(5, n as u64)
}

// Single-byte SenML CBOR label values (all fit in one byte).
const BN: u8 = 0x21; // -2  "bn" base name
const N: u8 = 0x00; //  0  "n"  name
const V: u8 = 0x02; //  2  "v"  numeric value
const VS: u8 = 0x03; //  3  "vs" string value
const VB: u8 = 0x04; //  4  "vb" boolean value
const VD: u8 = 0x08; //  8  "vd" data (bytes) value

/// Encode the LWM2M Server Object instance for PUT /1/1.
///
/// Fixed structure (Short Server ID=1, Lifetime=86400s, Binding="U"):
/// ```text
/// [
///   {bn:"/1/1/", n:"0", v:1},
///   {n:"1", v:86400},
///   {n:"6", vb:false},
///   {n:"7", vs:"U"},
/// ]
/// ```
pub fn encode_server_object() -> Vec<u8> {
    let mut out = cbor_array_header(4);

    // {bn: "/1/1/", n: "0", v: 1}  — Short Server ID = 1
    out.extend(cbor_map_header(3));
    out.push(BN);
    out.extend(cbor_text("/1/1/"));
    out.push(N);
    out.extend(cbor_text("0"));
    out.push(V);
    out.extend(cbor_uint(1));

    // {n: "1", v: 86400}  — Lifetime = 24 h
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("1"));
    out.push(V);
    out.extend(cbor_uint(86400));

    // {n: "6", vb: false}  — Notification Storing = false
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("6"));
    out.push(VB);
    out.push(0xf4);

    // {n: "7", vs: "U"}  — Binding = UDP
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("7"));
    out.push(VS);
    out.extend(cbor_text("U"));

    out
}

/// Encode the LWM2M Security Object instance for PUT /0/1.
///
/// ```text
/// [
///   {bn:"/0/1/", n:"0",  vs: server_uri},
///   {n:"1",  vb:false},
///   {n:"10", v:1},
///   {n:"2",  v:3},           // Security Mode = NoSec (DTLS off; radio handles it)
///   {n:"4",  vd: server_pubkey},
///   {n:"5",  vd: network_key},
/// ]
/// ```
pub fn encode_security_object(server_uri: &str, server_pubkey: &[u8], network_key: &[u8]) -> Vec<u8> {
    let mut out = cbor_array_header(6);

    // {bn: "/0/1/", n: "0", vs: server_uri}  — LWM2M Server URI
    out.extend(cbor_map_header(3));
    out.push(BN);
    out.extend(cbor_text("/0/1/"));
    out.push(N);
    out.extend(cbor_text("0"));
    out.push(VS);
    out.extend(cbor_text(server_uri));

    // {n: "1", vb: false}  — Bootstrap Server = false
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("1"));
    out.push(VB);
    out.push(0xf4);

    // {n: "10", v: 1}  — Short Server ID = 1
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("10"));
    out.push(V);
    out.extend(cbor_uint(1));

    // {n: "2", v: 3}  — Security Mode = 3 (NoSec)
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("2"));
    out.push(V);
    out.extend(cbor_uint(3));

    // {n: "4", vd: server_pubkey}  — Server Public Key (compressed P-256, 33 B)
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("4"));
    out.push(VD);
    out.extend(cbor_bytes_val(server_pubkey));

    // {n: "5", vd: network_key}  — Secret Key (network key from --lb-key-file)
    out.extend(cbor_map_header(2));
    out.push(N);
    out.extend(cbor_text("5"));
    out.push(VD);
    out.extend(cbor_bytes_val(network_key));

    out
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join("")
}
