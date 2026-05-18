use std::{net::SocketAddr, sync::Arc, time::Duration};

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status,
};
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    bootstrap::{self, BootstrapRegistry},
    error::Result,
    model::{LwM2mError, MqttResponse, PendingOperation, ResourceValue},
    registry::DeviceRegistry,
};

use super::{content_format::SENML_CBOR, RD_PATH};

const BS_PATH: &str = "bs";
const DP_PATH: &str = "dp";
/// IPv6 Traffic Class used for all post-bootstrap server traffic.
/// Tells the radio module to apply MAC-layer encryption.
#[cfg(target_os = "linux")]
const TC_ENCRYPTED: u32 = 0x1c;
const MAX_PACKET: usize = 1500;
/// RFC 7252 retransmit defaults: up to 4 re-sends, 2 s initial backoff.
const MAX_RETRANSMIT: u8 = 4;
const ACK_TIMEOUT_SECS: u64 = 2;

pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    bootstrap_registry: BootstrapRegistry,
    coap_dispatch_tx: mpsc::Sender<DispatchRequest>,
    mqtt_out_tx: mpsc::Sender<MqttResponse>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut buf = vec![0u8; MAX_PACKET];
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("CoAP server shutting down");
                return Ok(());
            }
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((len, addr)) => {
                        info!(%addr, bytes = len, "CoAP packet received");
                        if let Err(e) = handle_packet(
                            &buf[..len],
                            addr,
                            &socket,
                            &registry,
                            &bootstrap_registry,
                            &coap_dispatch_tx,
                            &mqtt_out_tx,
                        )
                        .await
                        {
                            warn!(%addr, "error handling CoAP packet: {e}");
                        }
                    }
                    Err(e) => error!("UDP recv error: {e}"),
                }
            }
        }
    }
}

/// A set of operations ready to be sent to a device.
pub struct DispatchRequest {
    pub addr: SocketAddr,
    pub ops: Vec<PendingOperation>,
}

async fn handle_packet(
    data: &[u8],
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    registry: &DeviceRegistry,
    bootstrap_registry: &BootstrapRegistry,
    coap_dispatch_tx: &mpsc::Sender<DispatchRequest>,
    mqtt_out_tx: &mpsc::Sender<MqttResponse>,
) -> Result<()> {
    let packet = Packet::from_bytes(data)
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;

    registry.touch(addr).await;

    match packet.header.get_type() {
        // Device initiating a request to the server (registration, update).
        MessageType::Confirmable | MessageType::NonConfirmable => {
            match packet.header.code {
                MessageClass::Request(Method::Post) => {
                    handle_post(packet, addr, socket, registry, bootstrap_registry, coap_dispatch_tx).await?;
                }
                other => {
                    warn!(%addr, ?other, "unexpected CoAP request method");
                }
            }
        }
        // Device acknowledging one of our downlink requests.
        MessageType::Acknowledgement => {
            handle_ack(packet, addr, registry, bootstrap_registry, mqtt_out_tx).await?;
        }
        MessageType::Reset => {
            // Device rejected our message — treat as error for the in-flight op.
            let token = token_array(packet.get_token());
            if bootstrap_registry.is_pending(&token).await {
                warn!(%addr, "bootstrap GET /0/0 reset by device");
            } else if bootstrap_registry.complete_write_ack(&token, false).await {
                warn!(%addr, "bootstrap write op reset by device");
            } else if let Some(op) = registry.complete_in_flight(addr, &token).await {
                let _ = op.response_tx.send(Err(LwM2mError::CoapError { class: 5, detail: 0 }));
            }
        }
    }
    Ok(())
}

async fn handle_post(
    packet: Packet,
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    registry: &DeviceRegistry,
    bootstrap_registry: &BootstrapRegistry,
    coap_dispatch_tx: &mpsc::Sender<DispatchRequest>,
) -> Result<()> {
    let path = uri_path(&packet);
    let path_parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    match path_parts.as_slice() {
        // POST /bs?ep=<name>&pct=<fmt>  — bootstrap request
        [p] if *p == BS_PATH => {
            handle_bootstrap(packet, addr, socket, bootstrap_registry).await?;
        }
        // POST /rd?ep=<name>&lt=<lifetime>&b=U  — new registration
        [p] if *p == RD_PATH => {
            handle_registration(packet, addr, socket, registry).await?;
        }
        // POST /rd/<id>  — registration update (heartbeat)
        [p, _id] if *p == RD_PATH => {
            handle_update(packet, addr, socket, registry, coap_dispatch_tx).await?;
        }
        // POST /dp  — device data push (SenML+CBOR state report after registration)
        [p] if *p == DP_PATH => {
            handle_dp(packet, addr, socket).await?;
        }
        _ => {
            warn!(%addr, path, "POST to unknown path");
            send_response(socket, addr, &packet, Status::NotFound, None).await?;
        }
    }
    Ok(())
}

async fn handle_bootstrap(
    packet: Packet,
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    bootstrap_registry: &BootstrapRegistry,
) -> Result<()> {
    let query = uri_query(&packet);
    let params = parse_query(&query);

    let endpoint = params.get("ep").cloned().unwrap_or_default();
    if endpoint.is_empty() {
        warn!(%addr, "bootstrap request missing ep parameter");
        send_response(socket, addr, &packet, Status::BadRequest, None).await?;
        return Ok(());
    }

    // If the certificate is already cached this is the second /bs — ACK and proceed.
    if bootstrap_registry.has_cert(&endpoint).await {
        info!(%endpoint, %addr, "bootstrap: cert cached, ACKing second /bs for write phase");
        let bytes = make_response_bytes(&packet, Status::Changed, None)?;
        send_bootstrap_packet(socket, &bytes, addr).await?;

        let Some(server_uri) = bootstrap_registry.server_uri().map(str::to_owned) else {
            warn!(%endpoint, "bootstrap: SERVER_URI not configured — skipping write phase");
            return Ok(());
        };

        let socket_clone = socket.clone();
        let br = bootstrap_registry.clone();
        tokio::spawn(async move {
            if let Err(e) = bootstrap_write_phase(endpoint, addr, socket_clone, br, server_uri).await {
                tracing::error!("bootstrap write phase failed: {e}");
            }
        });
        return Ok(());
    }

    // First /bs: do NOT ACK — just trigger the CON GET /0/0 to read the device certificate.
    let Some((token, mid)) = bootstrap_registry.begin(endpoint.clone(), addr).await else {
        info!(%endpoint, "bootstrap: GET /0/0 already in flight, ignoring duplicate /bs");
        return Ok(());
    };

    // The device needs ~3 s after transmitting CON POST /bs to open its response socket.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut get = Packet::new();
    get.header.set_type(MessageType::Confirmable);
    get.header.code = MessageClass::Request(Method::Get);
    get.header.message_id = mid;
    get.set_token(token[..4].to_vec()); // 4-byte token — device echoes it back
    get.add_option(CoapOption::UriPath, b"0".to_vec());
    get.add_option(CoapOption::UriPath, b"0".to_vec());

    let bytes = get
        .to_bytes()
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;
    // TC=0x0c signals the radio module: no MAC-layer encryption yet.
    send_bootstrap_packet(socket, &bytes, addr).await?;

    info!(%endpoint, %addr, mid, "bootstrap: sent CON GET /0/0 (TC=0x0c, /bs not ACKed)");
    Ok(())
}

fn make_response_bytes(request: &Packet, status: Status, payload: Option<Vec<u8>>) -> Result<Vec<u8>> {
    let mut response = make_response(request, status);
    if let Some(body) = payload {
        response.payload = body;
    }
    response.to_bytes().map_err(|e| crate::error::Error::Coap(format!("{e:?}")))
}

/// Send a bootstrap-phase CoAP packet with IPv6 Traffic Class 0x0c.
/// TC=0x0c tells the radio module not to apply MAC-layer encryption,
/// which is required before the network key has been exchanged.
/// On macOS (dev builds) TC is not settable; the warning is expected.
async fn send_bootstrap_packet(socket: &UdpSocket, bytes: &[u8], addr: SocketAddr) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use socket2::SockRef;
        SockRef::from(socket).set_tclass_v6(0x0c)?;
    }
    socket.send_to(bytes, addr).await?;
    #[cfg(target_os = "linux")]
    {
        use socket2::SockRef;
        SockRef::from(socket).set_tclass_v6(0)?;
    }
    Ok(())
}

/// Send a post-bootstrap CoAP packet with IPv6 Traffic Class 0x1c.
/// TC=0x1c tells the radio module to apply MAC-layer encryption,
/// required for all traffic after the network key has been provisioned.
async fn send_encrypted_packet(socket: &UdpSocket, bytes: &[u8], addr: SocketAddr) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use socket2::SockRef;
        SockRef::from(socket).set_tclass_v6(TC_ENCRYPTED)?;
    }
    socket.send_to(bytes, addr).await?;
    #[cfg(target_os = "linux")]
    {
        use socket2::SockRef;
        SockRef::from(socket).set_tclass_v6(0)?;
    }
    Ok(())
}

async fn send_encrypted_response(
    socket: &UdpSocket,
    addr: SocketAddr,
    request: &Packet,
    status: Status,
    payload: Option<Vec<u8>>,
) -> Result<()> {
    let bytes = make_response_bytes(request, status, payload)?;
    send_encrypted_packet(socket, &bytes, addr).await
}

async fn handle_registration(
    packet: Packet,
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    registry: &DeviceRegistry,
) -> Result<()> {
    let query = uri_query(&packet);
    let params = parse_query(&query);

    let endpoint = params.get("ep").cloned().unwrap_or_default();
    let lifetime: u32 = params
        .get("lt")
        .and_then(|v| v.parse().ok())
        .unwrap_or(86400);

    if endpoint.is_empty() {
        warn!(%addr, "registration missing ep parameter");
        send_encrypted_response(socket, addr, &packet, Status::BadRequest, None).await?;
        return Ok(());
    }

    // Parse link-format body for registered objects.
    let body = std::str::from_utf8(packet.payload.as_slice()).unwrap_or("");
    let objects = parse_link_format(body);

    let id = registry.register(endpoint.clone(), addr, lifetime, objects).await;

    // 2.01 Created with Location-Path: rd / <id>
    let mut response = make_response(&packet, Status::Created);
    response.add_option(CoapOption::LocationPath, b"rd".to_vec());
    response.add_option(CoapOption::LocationPath, id.to_string().into_bytes());

    let bytes = response
        .to_bytes()
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;
    send_encrypted_packet(socket, &bytes, addr).await?;

    info!(%endpoint, id, %addr, "device registered");
    Ok(())
}

async fn handle_update(
    packet: Packet,
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    registry: &DeviceRegistry,
    coap_dispatch_tx: &mpsc::Sender<DispatchRequest>,
) -> Result<()> {
    send_encrypted_response(socket, addr, &packet, Status::Changed, None).await?;

    // Drain pending ops and hand them to the dispatch task.
    let ops = registry.drain_pending(addr).await;
    if !ops.is_empty() {
        info!(%addr, count = ops.len(), "dispatching pending ops on device update");
        let _ = coap_dispatch_tx.send(DispatchRequest { addr, ops }).await;
    }
    Ok(())
}

async fn handle_ack(
    packet: Packet,
    addr: SocketAddr,
    registry: &DeviceRegistry,
    bootstrap_registry: &BootstrapRegistry,
    mqtt_out_tx: &mpsc::Sender<MqttResponse>,
) -> Result<()> {
    let token = token_array(packet.get_token());

    // Write-phase ACKs (DELETE /1, PUT /1/1, DELETE /0, PUT /0/1, POST /bs finish).
    if bootstrap_registry.complete_write_ack(&token, true).await {
        return Ok(());
    }

    // Bootstrap GET /0/0 ACK (token IDs ≥ 0x8000_0000, no overlap with op tokens).
    if bootstrap_registry.is_pending(&token).await {
        if let Some(session) = bootstrap_registry.complete(&token, packet.payload.clone()).await {
            info!(
                endpoint = %session.endpoint,
                bytes = session.pubkey_payload.as_ref().map_or(0, |p| p.len()),
                "bootstrap: public key received — ready for write phase"
            );
        }
        return Ok(());
    }

    let Some(op) = registry.complete_in_flight(addr, &token).await else {
        // Spurious or duplicate ACK — ignore.
        return Ok(());
    };

    let result = coap_response_to_result(&packet);
    let _ = op.response_tx.send(result);
    let _ = mqtt_out_tx; // used via subscriber wrapper
    Ok(())
}

// ── Device data push (/dp) ───────────────────────────────────────────────────

async fn handle_dp(
    packet: Packet,
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
) -> Result<()> {
    send_encrypted_response(socket, addr, &packet, Status::Changed, None).await?;
    if !packet.payload.is_empty() {
        log_senml_cbor(addr, &packet.payload);
    }
    Ok(())
}

/// Decode a SenML+CBOR payload and emit one `info!` line per record.
///
/// SenML is a CBOR array of maps. Integer keys are the standard labels:
///   -2=bn (base name), 0=n (name), 2=v (number), 3=vs (string),
///   4=vb (bool), 8=vd (bytes).
fn log_senml_cbor(addr: SocketAddr, data: &[u8]) {
    use ciborium::value::Value as Cbor;

    let top: Cbor = match ciborium::from_reader(data) {
        Ok(v) => v,
        Err(e) => {
            warn!(%addr, bytes = data.len(), "dp: CBOR decode failed: {e}");
            return;
        }
    };

    let Cbor::Array(records) = top else {
        warn!(%addr, "dp: expected CBOR array at top level");
        return;
    };

    let mut base_name = String::new();

    for record in &records {
        let Cbor::Map(fields) = record else { continue };

        let mut rel_name = String::new();
        let mut value_str = String::new();

        for (key, val) in fields {
            let label = match key {
                Cbor::Integer(i) => i128::from(*i),
                _ => continue,
            };
            match label {
                -2 => {
                    if let Cbor::Text(s) = val {
                        base_name = s.clone();
                    }
                }
                0 => {
                    if let Cbor::Text(s) = val {
                        rel_name = s.clone();
                    }
                }
                2 | 3 | 4 | 8 => {
                    value_str = fmt_cbor(val);
                }
                _ => {}
            }
        }

        let path = format!("{}{}", base_name, rel_name);
        info!(%addr, %path, value = %value_str, "dp");
    }
}

fn fmt_cbor(v: &ciborium::value::Value) -> String {
    use ciborium::value::Value as Cbor;
    match v {
        Cbor::Integer(i) => format!("{}", i128::from(*i)),
        Cbor::Float(f) => format!("{f}"),
        Cbor::Text(s) => s.clone(),
        Cbor::Bool(b) => b.to_string(),
        Cbor::Bytes(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
        Cbor::Null => "null".into(),
        Cbor::Array(a) => format!("[{} items]", a.len()),
        Cbor::Map(m) => format!("{{{} fields}}", m.len()),
        Cbor::Tag(_, inner) => fmt_cbor(inner),
        _ => "?".into(),
    }
}

// ── Bootstrap write phase ─────────────────────────────────────────────────────

/// Execute the full bootstrap write sequence after ACKing the second POST /bs:
///   CON DELETE /1  → CON PUT /1/1  → CON DELETE /0  → CON PUT /0/1  → CON POST /bs
/// All packets are sent with TC=0x0c (no MAC-layer encryption).
async fn bootstrap_write_phase(
    endpoint: String,
    addr: SocketAddr,
    socket: Arc<UdpSocket>,
    registry: BootstrapRegistry,
    server_uri: String,
) -> Result<()> {
    info!(%endpoint, %addr, "bootstrap write phase starting");

    let server_pubkey = registry.server_pubkey_bytes().to_vec();
    let network_key = registry.network_key().to_vec();
    let server_secret = registry.server_secret_key();

    // Step 1 — DELETE /1 (clear existing Server Object instances)
    {
        let (token, mid) = registry.alloc_token_mid().await;
        let pkt = build_delete(mid, &token[..4], &["1"])?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(%endpoint, "bootstrap: DELETE /1 ACKed");
    }

    // Step 2 — PUT /1/1 (LWM2M Server Object: lifetime, binding, server ID)
    {
        let (token, mid) = registry.alloc_token_mid().await;
        let payload = bootstrap::encode_server_object();
        let pkt = build_put(mid, &token[..4], &["1", "1"], SENML_CBOR, payload)?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(%endpoint, "bootstrap: PUT /1/1 ACKed");
    }

    // Step 3 — DELETE /0 (clear existing Security Object instances)
    {
        let (token, mid) = registry.alloc_token_mid().await;
        let pkt = build_delete(mid, &token[..4], &["0"])?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(%endpoint, "bootstrap: DELETE /0 ACKed");
    }

    // Step 4 — PUT /0/1 (LWM2M Security Object: URI, server pubkey, encrypted network key)
    {
        let device_pubkey_payload = registry
            .get_cert(&endpoint)
            .await
            .ok_or_else(|| crate::error::Error::Bootstrap("device pubkey not in cache".into()))?;
        let device_pubkey_bytes = bootstrap::parse_device_pubkey(&device_pubkey_payload)?;
        let encrypted_key =
            bootstrap::encrypt_network_key(&server_secret, &device_pubkey_bytes, &network_key)?;

        let (token, mid) = registry.alloc_token_mid().await;
        let payload =
            bootstrap::encode_security_object(&server_uri, &server_pubkey, &encrypted_key);
        let pkt = build_put(mid, &token[..4], &["0", "1"], SENML_CBOR, payload)?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(%endpoint, "bootstrap: PUT /0/1 ACKed");
    }

    // Step 5 — POST /bs (bootstrap finish signal; device switches to encrypted traffic)
    {
        let (token, mid) = registry.alloc_token_mid().await;
        let pkt = build_post_bs(mid, &token[..4])?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(%endpoint, %addr, "bootstrap: write phase complete — device now encrypted");
    }

    Ok(())
}

/// Send a CON request and wait for the ACK, retransmitting up to MAX_RETRANSMIT times.
async fn send_con_write_step(
    socket: &UdpSocket,
    bytes: &[u8],
    addr: SocketAddr,
    token: [u8; 8],
    registry: &BootstrapRegistry,
) -> Result<()> {
    let mut rx = registry.register_write_ack(token).await;
    let mut delay = Duration::from_secs(ACK_TIMEOUT_SECS);

    for attempt in 0..=MAX_RETRANSMIT {
        if attempt > 0 {
            tracing::debug!(attempt, %addr, "bootstrap write: retransmitting");
        }
        send_bootstrap_packet(socket, bytes, addr).await?;

        tokio::select! {
            biased;
            result = &mut rx => {
                return match result {
                    Ok(true) => Ok(()),
                    _ => Err(crate::error::Error::Coap("bootstrap write op rejected (RST)".into())),
                };
            }
            _ = tokio::time::sleep(delay) => {
                delay = (delay * 2).min(Duration::from_secs(32));
            }
        }
    }

    registry.cancel_write_ack(&token).await;
    Err(crate::error::Error::Coap("bootstrap write: max retransmits exceeded".into()))
}

fn build_delete(mid: u16, token: &[u8], path: &[&str]) -> Result<Vec<u8>> {
    let mut pkt = Packet::new();
    pkt.header.set_type(MessageType::Confirmable);
    pkt.header.code = MessageClass::Request(Method::Delete);
    pkt.header.message_id = mid;
    pkt.set_token(token.to_vec());
    for part in path {
        pkt.add_option(CoapOption::UriPath, part.as_bytes().to_vec());
    }
    pkt.to_bytes().map_err(|e| crate::error::Error::Coap(format!("{e:?}")))
}

fn build_put(mid: u16, token: &[u8], path: &[&str], cf: u16, payload: Vec<u8>) -> Result<Vec<u8>> {
    let mut pkt = Packet::new();
    pkt.header.set_type(MessageType::Confirmable);
    pkt.header.code = MessageClass::Request(Method::Put);
    pkt.header.message_id = mid;
    pkt.set_token(token.to_vec());
    for part in path {
        pkt.add_option(CoapOption::UriPath, part.as_bytes().to_vec());
    }
    let cf_bytes = if cf <= 0xFF { vec![cf as u8] } else { vec![(cf >> 8) as u8, cf as u8] };
    pkt.add_option(CoapOption::ContentFormat, cf_bytes);
    pkt.payload = payload;
    pkt.to_bytes().map_err(|e| crate::error::Error::Coap(format!("{e:?}")))
}

fn build_post_bs(mid: u16, token: &[u8]) -> Result<Vec<u8>> {
    let mut pkt = Packet::new();
    pkt.header.set_type(MessageType::Confirmable);
    pkt.header.code = MessageClass::Request(Method::Post);
    pkt.header.message_id = mid;
    pkt.set_token(token.to_vec());
    pkt.add_option(CoapOption::UriPath, b"bs".to_vec());
    pkt.to_bytes().map_err(|e| crate::error::Error::Coap(format!("{e:?}")))
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn coap_response_to_result(packet: &Packet) -> crate::model::LwM2mResult {
    use coap_lite::ResponseType::*;
    match packet.header.code {
        MessageClass::Response(status) => {
            if matches!(status, Created | Deleted | Valid | Changed | Content) {
                let text = String::from_utf8_lossy(&packet.payload).into_owned();
                Ok(ResourceValue::Text(text))
            } else {
                Err(LwM2mError::NotFound)
            }
        }
        _ => Err(LwM2mError::CoapError { class: 5, detail: 0 }),
    }
}

fn make_response(request: &Packet, status: Status) -> Packet {
    let mut response = Packet::new();
    response.header.set_type(MessageType::Acknowledgement);
    response.header.code = MessageClass::Response(status);
    response.header.message_id = request.header.message_id;
    response.set_token(request.get_token().to_vec());
    response
}

async fn send_response(
    socket: &UdpSocket,
    addr: SocketAddr,
    request: &Packet,
    status: Status,
    payload: Option<Vec<u8>>,
) -> Result<()> {
    let mut response = make_response(request, status);
    if let Some(body) = payload {
        response.payload = body;
    }
    let bytes = response
        .to_bytes()
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;
    socket.send_to(&bytes, addr).await?;
    Ok(())
}

fn uri_path(packet: &Packet) -> String {
    packet
        .get_option(CoapOption::UriPath)
        .map(|opts: &std::collections::LinkedList<Vec<u8>>| {
            opts.iter()
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect::<Vec<_>>()
                .join("/")
        })
        .unwrap_or_default()
}

fn uri_query(packet: &Packet) -> String {
    packet
        .get_option(CoapOption::UriQuery)
        .map(|opts: &std::collections::LinkedList<Vec<u8>>| {
            opts.iter()
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect::<Vec<_>>()
                .join("&")
        })
        .unwrap_or_default()
}

fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    query
        .split('&')
        .filter_map(|kv| {
            let mut parts = kv.splitn(2, '=');
            Some((parts.next()?.to_owned(), parts.next().unwrap_or("").to_owned()))
        })
        .collect()
}

/// Parse LWM2M link-format: "</3/0>,</4/0>" → ["3/0", "4/0"]
fn parse_link_format(body: &str) -> Vec<String> {
    body.split(',')
        .filter_map(|link| {
            let link = link.trim();
            let inner = link.strip_prefix('<')?.strip_suffix('>')?;
            Some(inner.trim_start_matches('/').to_owned())
        })
        .collect()
}

fn token_array(token: &[u8]) -> [u8; 8] {
    let mut arr = [0u8; 8];
    let len = token.len().min(8);
    arr[..len].copy_from_slice(&token[..len]);
    arr
}
