use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Instant,
};

use aes::{
    cipher::{BlockEncrypt, KeyInit},
    Aes128,
};
use p256::{
    ecdh::diffie_hellman,
    elliptic_curve::sec1::ToEncodedPoint,
    PublicKey, SecretKey,
};
use rand_core::OsRng;
use tokio::sync::{oneshot, Mutex};
use std::collections::HashSet;
use tracing::{info, warn};


/// State for one in-progress bootstrap exchange with a device.
pub struct BootstrapSession {
    pub endpoint: String,
    pub started_at: Instant,
    /// Raw SenML+CBOR payload from the device's GET /0/0 response.
    pub pubkey_payload: Option<Vec<u8>>,
}

struct RegistryInner {
    /// Active GET /0/0 requests keyed by the 4-byte token (padded to 8).
    by_token: HashMap<[u8; 8], BootstrapSession>,
    /// Reverse index: endpoint name → token, so duplicate /bs requests can be deduplicated.
    by_endpoint: HashMap<String, [u8; 8]>,
    /// Cached public-key payloads — permanent, cert never changes per device.
    cert_cache: HashMap<String, Vec<u8>>,
    /// Endpoints the user has explicitly approved for inclusion on their next /bs.
    /// Consumed (removed) when the write phase starts.
    approved: HashSet<String>,
    /// Stable numeric ID assigned to each seen endpoint (for IPC path references).
    includable_ids: HashMap<String, u32>,
    /// Reverse index: id → endpoint.
    includable_by_id: HashMap<u32, String>,
    next_includable_id: u32,
    /// Endpoints that have successfully completed inclusion (persisted across restarts).
    included: HashSet<String>,
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
    /// Ephemeral server P-256 keypair — secret kept for ECDH during write phase.
    server_secret_key: Arc<SecretKey>,
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
        info!(pubkey = %hex(&pubkey_bytes), "Bootstrap: generated ephemeral server P-256 keypair");

        Self {
            inner: Arc::new(Mutex::new(RegistryInner {
                by_token: HashMap::new(),
                by_endpoint: HashMap::new(),
                cert_cache: HashMap::new(),
                approved: HashSet::new(),
                includable_ids: HashMap::new(),
                includable_by_id: HashMap::new(),
                next_includable_id: 1,
                included: HashSet::new(),
                mid_counter: mid_start,
                write_acks: HashMap::new(),
            })),
            token_counter: Arc::new(AtomicU32::new(token_start)),
            server_secret_key: Arc::new(secret),
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
                started_at: Instant::now(),
                pubkey_payload: None,
            },
        );

        tracing::debug!(device = %endpoint, %device_addr, token = hex(&token[..4]), activity = "inclusion", "Bootstrap session started");
        Some((token, mid))
    }

    /// Called when the ACK for GET /0/0 arrives.
    /// Caches the pubkey payload and returns the completed session.
    pub async fn complete(&self, token: &[u8; 8], payload: Vec<u8>) -> Option<BootstrapSession> {
        let mut inner = self.inner.lock().await;
        if let Some(mut session) = inner.by_token.remove(token) {
            inner.by_endpoint.remove(&session.endpoint);
            info!(
                device = %session.endpoint,
                bytes = payload.len(),
                activity = "inclusion",
                "Bootstrap read security object done"
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

    /// Ephemeral server P-256 secret key, needed for ECDH during write phase.
    pub fn server_secret_key(&self) -> Arc<SecretKey> {
        Arc::clone(&self.server_secret_key)
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
                warn!(device = %s.endpoint, activity = "inclusion", "Bootstrap session timed out");
            }
        }
    }

    /// Get or create a stable numeric ID for this endpoint (used in IPC path references).
    pub async fn ensure_includable_id(&self, endpoint: &str) -> u32 {
        let mut inner = self.inner.lock().await;
        if let Some(&id) = inner.includable_ids.get(endpoint) {
            return id;
        }
        let id = inner.next_includable_id;
        inner.next_includable_id += 1;
        inner.includable_ids.insert(endpoint.to_owned(), id);
        inner.includable_by_id.insert(id, endpoint.to_owned());
        id
    }

    /// Returns true if the user has pre-approved this endpoint for inclusion.
    pub async fn is_approved(&self, endpoint: &str) -> bool {
        self.inner.lock().await.approved.contains(endpoint)
    }

    /// Record user approval for the device with `id`.
    ///
    /// The approval is stored and consumed on the device's next /bs request.
    /// Returns the endpoint name if found, `None` if the id is unknown.
    pub async fn approve_inclusion(&self, id: u32) -> Option<String> {
        let mut inner = self.inner.lock().await;
        let endpoint = inner.includable_by_id.get(&id)?.clone();
        inner.approved.insert(endpoint.clone());
        info!(device = %endpoint, id, activity = "inclusion", "Bootstrap: inclusion approved, awaiting next /bs");
        Some(endpoint)
    }

    /// Consume the approval flag when the write phase starts.
    pub async fn consume_approval(&self, endpoint: &str) {
        self.inner.lock().await.approved.remove(endpoint);
    }

    /// Remove the includable ID mapping after the write phase completes or fails.
    pub async fn remove_includable_id(&self, endpoint: &str) {
        let mut inner = self.inner.lock().await;
        if let Some(id) = inner.includable_ids.remove(endpoint) {
            inner.includable_by_id.remove(&id);
        }
    }

    /// Record that this endpoint has completed inclusion. Persisted via included_devices.json.
    pub async fn mark_included(&self, endpoint: &str) {
        self.inner.lock().await.included.insert(endpoint.to_owned());
    }

    /// True if this endpoint has previously completed inclusion.
    pub async fn is_included(&self, endpoint: &str) -> bool {
        self.inner.lock().await.included.contains(endpoint)
    }

    /// Remove an endpoint from the included set (e.g. after device factory reset).
    pub async fn unmark_included(&self, endpoint: &str) {
        self.inner.lock().await.included.remove(endpoint);
    }

    /// Snapshot the included set for persistence.
    pub async fn included_list(&self) -> Vec<String> {
        self.inner.lock().await.included.iter().cloned().collect()
    }

    /// Restore the included set from a persisted list.
    pub async fn load_included(&self, endpoints: Vec<String>) {
        let mut inner = self.inner.lock().await;
        for ep in endpoints {
            inner.included.insert(ep);
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

// ── Bootstrap crypto ──────────────────────────────────────────────────────────

/// Extract the device's raw P-256 public key bytes from a SenML+CBOR GET /0/0 payload.
///
/// Looks for a record with SenML label `n="3"` (LWM2M Security Object resource 3 =
/// "Public Key or Identity") and returns its `vd` (bytes) field.
pub fn parse_device_pubkey(payload: &[u8]) -> crate::error::Result<Vec<u8>> {
    use ciborium::value::Value as Cbor;

    let top: Cbor = ciborium::from_reader(payload)
        .map_err(|e| crate::error::Error::Bootstrap(format!("CBOR decode: {e}")))?;
    let Cbor::Array(records) = top else {
        return Err(crate::error::Error::Bootstrap("expected CBOR array".into()));
    };

    for record in &records {
        let Cbor::Map(fields) = record else { continue };
        let mut name: Option<&str> = None;
        let mut vd: Option<&[u8]> = None;
        for (k, v) in fields {
            match (k, v) {
                (Cbor::Integer(i), Cbor::Text(s)) if i128::from(*i) == 0 => {
                    name = Some(s.as_str());
                }
                (Cbor::Integer(i), Cbor::Bytes(b)) if i128::from(*i) == 8 => {
                    vd = Some(b.as_slice());
                }
                _ => {}
            }
        }
        if name == Some("3") {
            return vd
                .map(|b| b.to_vec())
                .ok_or_else(|| crate::error::Error::Bootstrap("resource 3 has no bytes value".into()));
        }
    }
    Err(crate::error::Error::Bootstrap(
        "device public key (resource /0/0/3) not found in GET /0/0 payload".into(),
    ))
}

fn parse_p256_pubkey(bytes: &[u8]) -> crate::error::Result<PublicKey> {
    use p256::pkcs8::DecodePublicKey;
    use x509_cert::der::{Decode, Encode};

    let cert = x509_cert::Certificate::from_der(bytes)
        .map_err(|e| crate::error::Error::Bootstrap(format!("X.509 decode: {e}")))?;
    let spki_der = cert
        .tbs_certificate
        .subject_public_key_info
        .to_der()
        .map_err(|e| crate::error::Error::Bootstrap(format!("SPKI encode: {e}")))?;
    PublicKey::from_public_key_der(&spki_der)
        .map_err(|e| crate::error::Error::Bootstrap(format!("pubkey decode: {e}")))
}

/// Encrypt `network_key` for the device:
///
///   random_prefix  = urandom(14)
///   plaintext      = random_prefix(14 B) ‖ network_key(16 B) ‖ CRC16_XMODEM(30 B)(2 B)
///                  = 32 bytes (two AES blocks)
///   aes_key        = first 16 bytes of x-coordinate of ECDH(server_private, device_public)
///   output         = AES-128-ECB(aes_key, plaintext)                        — 32 bytes
pub fn encrypt_network_key(
    server_secret: &SecretKey,
    device_pubkey_bytes: &[u8],
    network_key: &[u8],
) -> crate::error::Result<Vec<u8>> {
    use rand_core::RngCore;

    let device_pubkey = parse_p256_pubkey(device_pubkey_bytes)?;

    let shared = diffie_hellman(
        server_secret.to_nonzero_scalar(),
        device_pubkey.as_affine(),
    );
    let shared_x = shared.raw_secret_bytes(); // 32-byte X coordinate; use first 16 as AES-128 key

    // Build the 32-byte plaintext.
    let mut plaintext = [0u8; 32];
    OsRng.fill_bytes(&mut plaintext[..14]);             // 14 random prefix bytes
    plaintext[14..30].copy_from_slice(&network_key[..16]);
    let crc = crc16_xmodem(&plaintext[..30]);
    plaintext[30] = (crc >> 8) as u8;                  // big-endian CRC
    plaintext[31] = crc as u8;

    // AES-128-ECB: encrypt each 16-byte block independently (no IV, no auth tag).
    let cipher = Aes128::new_from_slice(&shared_x[..16])
        .expect("16-byte AES-128 key");
    // GenericArray doesn't implement TryFrom<&[u8]>, so go through [u8; 16] first.
    let mut b0: aes::Block = (<[u8; 16]>::try_from(&plaintext[..16]).unwrap()).into();
    let mut b1: aes::Block = (<[u8; 16]>::try_from(&plaintext[16..]).unwrap()).into();
    cipher.encrypt_block(&mut b0);
    cipher.encrypt_block(&mut b1);

    let mut out = vec![0u8; 32];
    out[..16].copy_from_slice(&b0);
    out[16..].copy_from_slice(&b1);
    Ok(out)
}

/// CRC-16/XMODEM: poly=0x1021, init=0x0000, no reflection, no XOR output.
fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0x0000;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
        }
    }
    crc
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

#[cfg(test)]
mod tests {
    use super::*;
    use aes::{
        cipher::{BlockDecrypt, KeyInit},
        Aes128,
    };

    fn from_hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    // ── Test fixtures from lwm2m_crypto/tests/auxiliary/ ─────────────────────

    // device_descriptors_dk_2022-05-19_2.json — factory_settings
    const DK_CERT_DER_HEX: &str = "308201a030820146a003020102021100bcb87d92731ea1f8ba1c543bb038a6b5300a06082a8648ce3d04030230163114301206035504030c0b736d61727473797374656d3020170d3232303531393039303933385a180f32313232303432353039303933385a302f312d302b06035504030c2432373961323230632d663231302d343035382d383635302d3164303736383636326666633059301306072a8648ce3d020106082a8648ce3d0301070342000424e4f8db540fc12d61111c6f2c46a2bd6bad651c8b5e4c7d6557a10064523133d2695df8a52163179aac2efd8b92c9ee7c9a7fe26586251f02b3453cd631ab0ca35a305830090603551d130402300030130603551d25040c300a06082b06010505070302300b0603551d0f04040302078030290603551d1104223020861e736774696e3a333031386631363930303366666663303030303030303032300a06082a8648ce3d0403020348003045022100be6ccf6bcdc11185df285adf3c9322a7bb15ae1be7a54db4b067fa009d2ba07802205cb26e20476b8209cfcaf01da4f4fb5a08c23dc32b1c54cce8d5aa3a9c064f88";
    // raw 32-byte P-256 scalar extracted from factory_settings.device_private_key SEC1 DER
    const DK_PRIV_SCALAR_HEX: &str =
        "27bdafc69076243d65fe23c8203615751bdd3c42371f30b02e5f039079b35847";
    // uncompressed public key (04 || X || Y) extracted from the DK certificate
    const DK_PUBKEY_UNCOMPRESSED_HEX: &str = "0424e4f8db540fc12d61111c6f2c46a2bd6bad651c8b5e4c7d6557a10064523133d2695df8a52163179aac2efd8b92c9ee7c9a7fe26586251f02b3453cd631ab0c";

    // device_descriptor_dmd-live.json — factory_data
    const DMD_CERT_DER_HEX: &str = "308201b63082015ca00302010202147ff9e1a8d4d3eebd1fdd2c18cb974575bb4e24a7300a06082a8648ce3d04030230293110300e060355040a0c0747415244454e413115301306035504030c0c4465766963652043412047313020170d3234303930353133333433365a180f32313234303831323132303531325a302f312d302b06035504030c2433356631343634652d373938392d346362622d396637382d3032313062613439303337373059301306072a8648ce3d020106082a8648ce3d03010703420004f3a401bb86cd485998dc5a235c8a696356261ac3912589ca60b879dc117d795fb5d8aabd54ff3d8bdaeada5112733cafef19d6e8c8bd67bc57374ddd7ae28ce4a35a305830090603551d130402300030290603551d1104223020861e736774696e3a33303334463833313943303037353430303030313836413230130603551d25040c300a06082b06010505070302300b0603551d0f040403020780300a06082a8648ce3d040302034800304502206a008989e77e92dfe93fde5f17520e9586ee86e3b3454083e749a82104851815022100bb1428aa4d51b759712ac9e4fe4d3b9e939fe18878099ffa3c5e2c0154be7978";
    const DMD_PRIV_SCALAR_HEX: &str =
        "0f330d1ea9a9e3f661a3da0a4fb243b3aa71c449e6d4b09d85d2dd300b129bbd";
    const DMD_PUBKEY_UNCOMPRESSED_HEX: &str = "04f3a401bb86cd485998dc5a235c8a696356261ac3912589ca60b879dc117d795fb5d8aabd54ff3d8bdaeada5112733cafef19d6e8c8bd67bc57374ddd7ae28ce4";

    // ── CRC ──────────────────────────────────────────────────────────────────

    #[test]
    fn crc16_xmodem_check_value() {
        // Standard CRC-16/XMODEM check value: input "123456789" → 0x31C3
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
    }

    // ── Public key extraction ─────────────────────────────────────────────────

    fn assert_pubkey_from_cert(cert_der_hex: &str, expected_uncompressed_hex: &str) {
        let cert_der = from_hex(cert_der_hex);
        let pubkey = parse_p256_pubkey(&cert_der).expect("parse_p256_pubkey");
        let got = pubkey.to_encoded_point(false);
        let expected = from_hex(expected_uncompressed_hex);
        assert_eq!(got.as_bytes(), expected.as_slice());
    }

    #[test]
    fn parse_p256_pubkey_dk_device() {
        assert_pubkey_from_cert(DK_CERT_DER_HEX, DK_PUBKEY_UNCOMPRESSED_HEX);
    }

    #[test]
    fn parse_p256_pubkey_dmd_live_device() {
        assert_pubkey_from_cert(DMD_CERT_DER_HEX, DMD_PUBKEY_UNCOMPRESSED_HEX);
    }

    // ── Encryption round-trip ─────────────────────────────────────────────────

    /// Mirrors test_inclusion_session from the Python test suite:
    /// encrypt on the server side, decrypt on the device side using the device's
    /// own private key, then verify the plaintext structure and CRC.
    fn assert_encrypt_round_trip(cert_der_hex: &str, priv_scalar_hex: &str) {
        let cert_der = from_hex(cert_der_hex);
        let network_key = [0x42u8; 16];

        // Server side: generate ephemeral keypair and encrypt.
        let server_secret = SecretKey::random(&mut OsRng);
        let server_pubkey = server_secret.public_key();
        let ciphertext =
            encrypt_network_key(&server_secret, &cert_der, &network_key).expect("encrypt");
        assert_eq!(ciphertext.len(), 32);

        // Device side: derive the same shared secret using the device private key.
        let scalar_bytes: [u8; 32] = from_hex(priv_scalar_hex).try_into().unwrap();
        let device_secret =
            SecretKey::from_bytes(&scalar_bytes.into()).unwrap();
        let shared = diffie_hellman(device_secret.to_nonzero_scalar(), server_pubkey.as_affine());
        let aes_key = &shared.raw_secret_bytes()[..16];

        // AES-128-ECB decrypt.
        let cipher = Aes128::new_from_slice(aes_key).unwrap();
        let mut b0: aes::Block = (<[u8; 16]>::try_from(&ciphertext[..16]).unwrap()).into();
        let mut b1: aes::Block = (<[u8; 16]>::try_from(&ciphertext[16..]).unwrap()).into();
        cipher.decrypt_block(&mut b0);
        cipher.decrypt_block(&mut b1);
        let mut plaintext = [0u8; 32];
        plaintext[..16].copy_from_slice(&b0);
        plaintext[16..].copy_from_slice(&b1);

        // Bytes 14..30 must equal the original network key.
        assert_eq!(&plaintext[14..30], &network_key, "network key mismatch");

        // Bytes 30..32 must be CRC-16/XMODEM over the first 30 bytes.
        let expected_crc = crc16_xmodem(&plaintext[..30]);
        let actual_crc = u16::from_be_bytes([plaintext[30], plaintext[31]]);
        assert_eq!(actual_crc, expected_crc, "CRC mismatch");
    }

    #[test]
    fn encrypt_network_key_dk_device_round_trip() {
        assert_encrypt_round_trip(DK_CERT_DER_HEX, DK_PRIV_SCALAR_HEX);
    }

    #[test]
    fn encrypt_network_key_dmd_live_device_round_trip() {
        assert_encrypt_round_trip(DMD_CERT_DER_HEX, DMD_PRIV_SCALAR_HEX);
    }
}
