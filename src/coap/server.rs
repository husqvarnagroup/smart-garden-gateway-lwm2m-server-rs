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
    event::EventSender,
    ipso::IpsoModel,
    model::{LwM2mError, PendingOperation, ResourceValue},
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
    event_sender: EventSender,
    ipso: Arc<IpsoModel>,
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
                            &event_sender,
                            &ipso,
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
    event_sender: &EventSender,
    ipso: &Arc<IpsoModel>,
) -> Result<()> {
    let packet = Packet::from_bytes(data)
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;

    registry.touch(addr).await;

    match packet.header.get_type() {
        // Device initiating a request to the server (registration, update).
        MessageType::Confirmable | MessageType::NonConfirmable => {
            match packet.header.code {
                MessageClass::Request(Method::Post) => {
                    handle_post(packet, addr, socket, registry, bootstrap_registry, coap_dispatch_tx, event_sender, ipso).await?;
                }
                other => {
                    warn!(%addr, ?other, "unexpected CoAP request method");
                }
            }
        }
        // Device acknowledging one of our downlink requests.
        MessageType::Acknowledgement => {
            handle_ack(packet, addr, socket, registry, bootstrap_registry, event_sender).await?;
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
    event_sender: &EventSender,
    ipso: &Arc<IpsoModel>,
) -> Result<()> {
    let path = uri_path(&packet);
    let path_parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    match path_parts.as_slice() {
        // POST /bs?ep=<name>&pct=<fmt>  — bootstrap request
        [p] if *p == BS_PATH => {
            handle_bootstrap(packet, addr, socket, bootstrap_registry, event_sender).await?;
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
            handle_dp(packet, addr, socket, registry, event_sender, ipso).await?;
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
    event_sender: &EventSender,
) -> Result<()> {
    let query = uri_query(&packet);
    let params = parse_query(&query);

    let ep_raw = params.get("ep").map(String::as_str).unwrap_or("");
    let endpoint = super::sgtin_from_ep(ep_raw).to_owned();
    if endpoint.is_empty() {
        warn!(%addr, "bootstrap request missing ep parameter");
        send_response(socket, addr, &packet, Status::BadRequest, None).await?;
        return Ok(());
    }

    // Case 1: cert received and device is pending user approval for inclusion.
    // ACK each /bs so the device stops retransmitting, and re-emit the pending event
    // so the app stays informed while the user decides.
    if let Some(id) = bootstrap_registry.includable_id(&endpoint).await {
        bootstrap_registry.update_includable_addr(&endpoint, addr).await;
        let bytes = make_response_bytes(&packet, Status::Changed, None)?;
        send_bootstrap_packet(socket, &bytes, addr).await?;
        event_sender.send_includable(id, &endpoint, false, false);
        info!(%endpoint, id, %addr, "bootstrap: pending inclusion, ACKed /bs");
        return Ok(());
    }

    // Case 2: cert cached but no pending includable entry — device is re-bootstrapping
    // after a previously completed inclusion.  Trigger the write phase directly.
    if bootstrap_registry.has_cert(&endpoint).await {
        info!(%endpoint, %addr, "bootstrap: re-bootstrap, triggering write phase");
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

    // Case 3: first /bs from this device — start GET /0/0 to read its certificate.
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

    let ep_raw = params.get("ep").map(String::as_str).unwrap_or("");
    let endpoint = super::sgtin_from_ep(ep_raw).to_owned();
    let lifetime: u32 = params
        .get("lt")
        .and_then(|v| v.parse().ok())
        .unwrap_or(86400);

    if endpoint.is_empty() {
        warn!(%addr, "registration missing ep parameter");
        send_encrypted_response(socket, addr, &packet, Status::BadRequest, None).await?;
        return Ok(());
    }

    // Parse link-format body for registered objects and their versions.
    let body = std::str::from_utf8(packet.payload.as_slice()).unwrap_or("");
    let (objects, object_versions) = parse_link_format(body);

    let id = registry.register(endpoint.clone(), addr, lifetime, objects, object_versions).await;

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
    socket: &Arc<UdpSocket>,
    registry: &DeviceRegistry,
    bootstrap_registry: &BootstrapRegistry,
    event_sender: &EventSender,
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
                "bootstrap: public key received — awaiting inclusion approval"
            );

            // Register as includable and emit the pending event.
            if let Some((id, approval_rx)) =
                bootstrap_registry.register_includable(session.endpoint.clone(), addr).await
            {
                event_sender.send_includable(id, &session.endpoint, false, false);

                // Spawn a task that blocks until the user approves (via IPC execute),
                // then runs the write phase and emits completion events.
                if let Some(server_uri) = bootstrap_registry.server_uri().map(str::to_owned) {
                    let socket_c = socket.clone();
                    let br = bootstrap_registry.clone();
                    let es = event_sender.clone();
                    let ep = session.endpoint.clone();
                    tokio::spawn(async move {
                        if approval_rx.await.is_err() {
                            return; // sender dropped — server shutting down
                        }
                        let Some(device_addr) = br.get_includable_addr(&ep).await else {
                            return;
                        };
                        es.send_includable(id, &ep, true, false);
                        match bootstrap_write_phase(ep.clone(), device_addr, socket_c, br.clone(), server_uri).await {
                            Ok(()) => {
                                es.send_includable(id, &ep, true, true);
                                br.remove_includable(&ep).await;
                                info!(endpoint = %ep, "inclusion completed");
                            }
                            Err(e) => {
                                error!(endpoint = %ep, "bootstrap write phase failed: {e}");
                                br.remove_includable(&ep).await;
                            }
                        }
                    });
                }
            }
        }
        return Ok(());
    }

    let Some(op) = registry.complete_in_flight(addr, &token).await else {
        // Spurious or duplicate ACK — ignore.
        return Ok(());
    };

    let _ = op.response_tx.send(coap_response_to_result(&packet));
    Ok(())
}

// ── Device data push (/dp) ───────────────────────────────────────────────────

async fn handle_dp(
    packet: Packet,
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    registry: &DeviceRegistry,
    event_sender: &EventSender,
    ipso: &IpsoModel,
) -> Result<()> {
    send_encrypted_response(socket, addr, &packet, Status::Changed, None).await?;

    if packet.payload.is_empty() {
        return Ok(());
    }

    let Some(endpoint) = registry.endpoint_by_addr(addr).await else {
        warn!(%addr, "dp: unknown device, ignoring payload");
        return Ok(());
    };

    let obj_versions = registry.object_versions_by_addr(addr).await.unwrap_or_default();

    match build_device_payload(&packet.payload, ipso, &obj_versions) {
        Some(payload) => {
            info!(%addr, endpoint = %endpoint, "dp: emitting device data event");
            event_sender.send_device_data(&endpoint, payload);
        }
        None => warn!(%addr, endpoint = %endpoint, "dp: no known objects in payload"),
    }

    Ok(())
}

// ── SenML+CBOR → event payload ────────────────────────────────────────────────

enum SenmlValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// Parse a SenML+CBOR payload and build the event `payload` object grouped by
/// IPSO object name → instance id → resource name.  Returns `None` if the
/// payload decodes but contains no objects known to the IPSO model.
fn build_device_payload(
    data: &[u8],
    ipso: &IpsoModel,
    obj_versions: &std::collections::HashMap<u32, String>,
) -> Option<serde_json::Value> {
    use ciborium::value::Value as Cbor;
    use std::collections::BTreeMap;

    let top: Cbor = match ciborium::from_reader(data) {
        Ok(v) => v,
        Err(e) => {
            warn!(bytes = data.len(), "dp: CBOR decode failed: {e}");
            return None;
        }
    };

    let Cbor::Array(records) = top else {
        warn!("dp: expected CBOR array at top level");
        return None;
    };

    let ts = crate::event::unix_ts();
    let mut base_name = String::new();

    // obj_id → inst_id → res_id → (values, is_array)
    // is_array = true when the SenML path included a resource-instance segment OR
    // when the IPSO definition marks the resource as MultipleInstances.
    let mut raw: BTreeMap<u32, BTreeMap<u32, BTreeMap<u32, (Vec<SenmlValue>, bool)>>> = BTreeMap::new();

    for record in &records {
        let Cbor::Map(fields) = record else { continue };

        let mut rel_name = String::new();
        let mut value: Option<SenmlValue> = None;

        for (key, val) in fields {
            let Cbor::Integer(i) = key else { continue };
            match i128::from(*i) {
                -2 => {
                    if let Cbor::Text(s) = val { base_name = s.clone(); }
                }
                0 => {
                    if let Cbor::Text(s) = val { rel_name = s.clone(); }
                }
                2 => match val {
                    Cbor::Integer(i) => { value = Some(SenmlValue::Int(i128::from(*i) as i64)); }
                    Cbor::Float(f)   => { value = Some(SenmlValue::Float(*f)); }
                    _ => {}
                },
                3 => { if let Cbor::Text(s) = val { value = Some(SenmlValue::Str(s.clone())); } }
                4 => { if let Cbor::Bool(b) = val { value = Some(SenmlValue::Bool(*b)); } }
                8 => { if let Cbor::Bytes(b) = val { value = Some(SenmlValue::Bytes(b.clone())); } }
                _ => {}
            }
        }

        let full_path = format!("{base_name}{rel_name}");
        let Some(v) = value else { continue };
        let Some((obj_id, inst_id, res_id, has_res_inst)) = parse_lwm2m_path(&full_path) else { continue };

        let entry = raw.entry(obj_id).or_default()
            .entry(inst_id).or_default()
            .entry(res_id)
            .or_insert_with(|| (Vec::new(), false));
        entry.0.push(v);
        entry.1 |= has_res_inst;
    }

    let mut payload = serde_json::Map::new();

    for (obj_id, instances) in &raw {
        let ver = obj_versions.get(obj_id).map(String::as_str);
        let Some(obj_def) = ipso.get_versioned(*obj_id, ver) else { continue };

        let mut obj_json = serde_json::Map::new();
        obj_json.insert("_urn".into(), serde_json::Value::String(obj_def.urn.clone()));

        for (inst_id, resources) in instances {
            let mut inst_json = serde_json::Map::new();

            for (res_id, (values, path_is_array)) in resources {
                let (res_name, res_type, def_is_array) = match obj_def.resources.get(res_id) {
                    Some(r) => (r.name.clone(), &r.resource_type, r.multiple_instances),
                    None    => (res_id.to_string(), &crate::ipso::ResourceType::Integer, false),
                };

                // Use array encoding when the IPSO def or the SenML path signals multi-instance.
                let is_array = *path_is_array || def_is_array;
                let value_json = if !is_array && values.len() == 1 {
                    encode_single_value(&values[0], res_type, ts)
                } else {
                    encode_array_value(values, res_type, ts)
                };

                inst_json.insert(res_name, value_json);
            }

            obj_json.insert(inst_id.to_string(), serde_json::Value::Object(inst_json));
        }

        payload.insert(obj_def.name.clone(), serde_json::Value::Object(obj_json));
    }

    if payload.is_empty() { None } else { Some(serde_json::Value::Object(payload)) }
}

/// Parse `/<obj>/<inst>/<res>[/<res_inst>]`.
///
/// Returns `(obj_id, inst_id, res_id, has_res_inst)` where `has_res_inst` is
/// `true` when a fourth segment (resource-instance ID) is present — indicating
/// that the resource is multi-instance and its value should be wrapped in an array.
fn parse_lwm2m_path(path: &str) -> Option<(u32, u32, u32, bool)> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.splitn(5, '/');
    let obj:  u32 = parts.next()?.parse().ok()?;
    let inst: u32 = parts.next()?.parse().ok()?;
    let res:  u32 = parts.next()?.parse().ok()?;
    let has_res_inst = parts.next().map_or(false, |p| p.parse::<u32>().is_ok());
    Some((obj, inst, res, has_res_inst))
}

fn encode_single_value(
    v: &SenmlValue,
    res_type: &crate::ipso::ResourceType,
    ts: u64,
) -> serde_json::Value {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use crate::ipso::ResourceType::*;
    match (v, res_type) {
        (SenmlValue::Int(i),   Time)    => serde_json::json!({"vt": *i, "ts": ts}),
        (SenmlValue::Int(i),   _)       => serde_json::json!({"vi": *i, "ts": ts}),
        (SenmlValue::Float(f), _)       => serde_json::json!({"vf": *f, "ts": ts}),
        (SenmlValue::Str(s),   _)       => serde_json::json!({"vs": s,  "ts": ts}),
        (SenmlValue::Bool(b),  _)       => serde_json::json!({"vb": *b, "ts": ts}),
        (SenmlValue::Bytes(b), _)       => serde_json::json!({"vo": STANDARD.encode(b), "ts": ts}),
    }
}

fn encode_array_value(
    values: &[SenmlValue],
    res_type: &crate::ipso::ResourceType,
    ts: u64,
) -> serde_json::Value {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use crate::ipso::ResourceType::*;
    match res_type {
        String => {
            let arr: Vec<_> = values.iter()
                .filter_map(|v| if let SenmlValue::Str(s) = v { Some(s.as_str()) } else { None })
                .map(serde_json::Value::from).collect();
            serde_json::json!({"as": arr, "ts": ts})
        }
        Boolean => {
            let arr: Vec<_> = values.iter()
                .filter_map(|v| if let SenmlValue::Bool(b) = v { Some(*b) } else { None })
                .map(serde_json::Value::from).collect();
            serde_json::json!({"ab": arr, "ts": ts})
        }
        Opaque => {
            let arr: Vec<_> = values.iter()
                .filter_map(|v| if let SenmlValue::Bytes(b) = v { Some(STANDARD.encode(b)) } else { None })
                .map(serde_json::Value::from).collect();
            serde_json::json!({"ao": arr, "ts": ts})
        }
        // Integer, Float, Time, UnsignedInteger, CoreLink → ai / af
        _ => {
            if values.iter().all(|v| matches!(v, SenmlValue::Int(_))) {
                let arr: Vec<i64> = values.iter()
                    .filter_map(|v| if let SenmlValue::Int(i) = v { Some(*i) } else { None })
                    .collect();
                serde_json::json!({"ai": arr, "ts": ts})
            } else {
                let arr: Vec<f64> = values.iter().filter_map(|v| match v {
                    SenmlValue::Int(i)   => Some(*i as f64),
                    SenmlValue::Float(f) => Some(*f),
                    _                    => None,
                }).collect();
                serde_json::json!({"af": arr, "ts": ts})
            }
        }
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

/// Parse LWM2M link-format into object paths and per-object version strings.
///
/// Input: `"</3/0>;ver=1.1,</4/0>;ver=1.2,</5/0>"`
/// Returns: `(["3/0", "4/0", "5/0"], {3 → "1.1", 4 → "1.2"})`
fn parse_link_format(body: &str) -> (Vec<String>, std::collections::HashMap<u32, String>) {
    let mut objects = Vec::new();
    let mut versions = std::collections::HashMap::new();

    for link in body.split(',') {
        let link = link.trim();
        let Some(angle_end) = link.find('>') else { continue };
        // Strip leading '<' and trailing '>'
        let path = link[1..angle_end].trim_start_matches('/').to_owned();

        // Parse semicolon-separated attributes after '>'
        let attrs = &link[angle_end + 1..];
        let ver = attrs
            .split(';')
            .filter_map(|attr| attr.strip_prefix("ver="))
            .next()
            .unwrap_or("");

        if let Some(obj_id_str) = path.split('/').next() {
            if let Ok(obj_id) = obj_id_str.parse::<u32>() {
                if !ver.is_empty() {
                    versions.insert(obj_id, ver.to_owned());
                }
            }
        }

        objects.push(path);
    }

    (objects, versions)
}

fn token_array(token: &[u8]) -> [u8; 8] {
    let mut arr = [0u8; 8];
    let len = token.len().min(8);
    arr[..len].copy_from_slice(&token[..len]);
    arr
}
