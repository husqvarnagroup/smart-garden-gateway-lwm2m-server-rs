use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status,
};
use tokio::{net::UdpSocket, sync::{mpsc, oneshot}};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    lwm2m::{
        bootstrap::{self, BootstrapRegistry},
        ipso::{IpsoModel, SharedIpso},
    },
    error::Result,
    ipc::event::EventSender,
    model::{LwM2mCommand, LwM2mError, PendingOperation, ResourcePath},
    persistence::PersistenceStore,
    registry::DeviceRegistry,
};

use super::{content_format::SENML_CBOR, set_tclass, BlockAckMap, TC_ENCRYPTED, TC_PLAIN};

/// Op-ID counter for server-internal operations (factory reset etc.).
/// Starts at 0x8000_0000 to stay well clear of IPC op IDs (which begin at 1).
static INTERNAL_OP_ID: AtomicU32 = AtomicU32::new(0x8000_0000);

const BS_PATH: &str = "bs";
const DP_PATH: &str = "dp";
const RD_PATH: &str = "rd";
const MAX_PACKET: usize = 1500;
/// RFC 7252 retransmit defaults: up to 4 re-sends, 2 s initial backoff.
const MAX_RETRANSMIT: u8 = 4;
const ACK_TIMEOUT_SECS: u64 = 2;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
    bootstrap_registry: BootstrapRegistry,
    coap_dispatch_tx: mpsc::Sender<DispatchRequest>,
    event_sender: EventSender,
    ipso: SharedIpso,
    persistence: Arc<PersistenceStore>,
    block_acks: BlockAckMap,
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
                        tracing::debug!(%addr, bytes = len, "CoAP packet received");
                        let ctx = ServerCtx {
                            socket: &socket,
                            registry: &registry,
                            bootstrap_registry: &bootstrap_registry,
                            coap_dispatch_tx: &coap_dispatch_tx,
                            event_sender: &event_sender,
                            ipso: &ipso,
                            persistence: &persistence,
                            block_acks: &block_acks,
                        };
                        if let Err(e) = handle_packet(&buf[..len], addr, &ctx).await {
                            warn!(%addr, "Error handling CoAP packet: {e}");
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

/// Shared server state threaded through packet handlers.
struct ServerCtx<'a> {
    socket: &'a Arc<UdpSocket>,
    registry: &'a DeviceRegistry,
    bootstrap_registry: &'a BootstrapRegistry,
    coap_dispatch_tx: &'a mpsc::Sender<DispatchRequest>,
    event_sender: &'a EventSender,
    ipso: &'a SharedIpso,
    persistence: &'a Arc<PersistenceStore>,
    block_acks: &'a BlockAckMap,
}

async fn handle_packet(data: &[u8], addr: SocketAddr, ctx: &ServerCtx<'_>) -> Result<()> {
    let packet = Packet::from_bytes(data)
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;

    if let Some(endpoint) = ctx.registry.touch(addr).await {
        info!(device = %endpoint, activity = "connection-status", "Device is online");
        ctx.event_sender.send_connection_status(&endpoint, true);
    }

    match packet.header.get_type() {
        // Device initiating a request to the server (registration, update).
        MessageType::Confirmable | MessageType::NonConfirmable => {
            match packet.header.code {
                MessageClass::Request(Method::Post) => {
                    handle_post(packet, addr, ctx).await?;
                }
                MessageClass::Request(Method::Delete) => {
                    handle_delete(packet, addr, ctx).await?;
                }
                other => {
                    warn!(%addr, ?other, "Unexpected CoAP request method");
                }
            }
        }
        // Device acknowledging one of our downlink requests.
        MessageType::Acknowledgement => {
            handle_ack(packet, addr, ctx.registry, ctx.bootstrap_registry, ctx.block_acks).await?;
        }
        MessageType::Reset => {
            // Device rejected our message — treat as error for the in-flight op.
            let token = token_array(packet.get_token());
            if ctx.bootstrap_registry.is_pending(&token).await {
                warn!(%addr, "Bootstrap GET /0/0 reset by device");
            } else if ctx.bootstrap_registry.complete_write_ack(&token, false).await {
                warn!(%addr, "Bootstrap write op reset by device");
            } else {
                // Drop the block-write sender (if any) — signals error to the block task.
                ctx.block_acks.lock().await.remove(&token);
                if let Some(op) = ctx.registry.complete_in_flight(addr, &token).await {
                    let _ = op.response_tx.send(Err(LwM2mError::CoapError { class: 5, detail: 0 }));
                }
            }
        }
    }
    Ok(())
}

async fn handle_post(packet: Packet, addr: SocketAddr, ctx: &ServerCtx<'_>) -> Result<()> {
    let path = uri_path(&packet);
    let path_parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    match path_parts.as_slice() {
        // POST /bs?ep=<name>&pct=<fmt>  — bootstrap request
        [p] if *p == BS_PATH => {
            handle_bootstrap(packet, addr, ctx.socket, ctx.bootstrap_registry, ctx.event_sender, ctx.persistence).await?;
        }
        // POST /rd?ep=<name>&lt=<lifetime>&b=U  — new registration
        [p] if *p == RD_PATH => {
            handle_registration(packet, addr, ctx.socket, ctx.registry, ctx.bootstrap_registry, ctx.coap_dispatch_tx, ctx.persistence).await?;
        }
        // POST /rd/<id>  — registration update (heartbeat)
        [p, _id] if *p == RD_PATH => {
            handle_update(packet, addr, ctx.socket, ctx.registry, ctx.coap_dispatch_tx).await?;
        }
        // POST /dp  — device data push (SenML+CBOR state report after registration)
        [p] if *p == DP_PATH => {
            let ipso = ctx.ipso.read().unwrap().clone();
            handle_dp(packet, addr, ctx.socket, ctx.registry, ctx.event_sender, &ipso, ctx.persistence).await?;
        }
        _ => {
            warn!(%addr, path, "POST to unknown path");
            send_encrypted_response(ctx.socket, addr, &packet, Status::NotFound, None).await?;
        }
    }
    Ok(())
}

async fn handle_delete(packet: Packet, addr: SocketAddr, ctx: &ServerCtx<'_>) -> Result<()> {
    send_encrypted_response(ctx.socket, addr, &packet, Status::Deleted, None).await?;

    let Some(endpoint) = ctx.registry.remove_by_addr(addr).await else {
        warn!(%addr, "DELETE from unknown device, ignoring");
        return Ok(());
    };

    // A device-initiated DELETE means the device is going offline temporarily (e.g. firmware
    // update reboot). It does NOT mean exclusion: the device stays included and its persisted
    // state is preserved so it can resume normally after reconnecting.
    info!(device = %endpoint, activity = "connection-status", "Device is offline");
    info!(device = %endpoint, activity = "registration", "Device deregistered");

    let snapshots = ctx.registry.snapshot().await;
    let ps = Arc::clone(ctx.persistence);
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || ps.save_registry(&snapshots)).await;
    });

    Ok(())
}

async fn handle_bootstrap(
    packet: Packet,
    addr: SocketAddr,
    socket: &Arc<UdpSocket>,
    bootstrap_registry: &BootstrapRegistry,
    event_sender: &EventSender,
    persistence: &Arc<PersistenceStore>,
) -> Result<()> {
    let query = uri_query(&packet);
    let params = parse_query(&query);

    let ep_raw = params.get("ep").map(String::as_str).unwrap_or("");
    let endpoint = super::sgtin_from_ep(ep_raw).to_owned();
    if endpoint.is_empty() {
        warn!(%addr, "Bootstrap request missing ep parameter");
        let bytes = make_response_bytes(&packet, Status::BadRequest, None)?;
        send_bootstrap_packet(socket, &bytes, addr).await?;
        return Ok(());
    }

    // Every /bs gets an event — assign a stable ID first.
    let id = bootstrap_registry.ensure_includable_id(&endpoint).await;
    event_sender.send_includable(id, &endpoint, false, false);
    info!(device = %endpoint, id, %addr, activity = "inclusion", "Received device inclusion request");

    // Case 1: user has approved — ACK, consume approval, start write phase.
    if bootstrap_registry.is_approved(&endpoint).await {
        bootstrap_registry.consume_approval(&endpoint).await;
        let bytes = make_response_bytes(&packet, Status::Changed, None)?;
        send_bootstrap_packet(socket, &bytes, addr).await?;
        info!(device = %endpoint, activity = "inclusion", "Start inclusion");

        let Some(server_uri) = bootstrap_registry.server_uri().map(str::to_owned) else {
            warn!(device = %endpoint, activity = "inclusion", "Bootstrap: SERVER_URI not configured — skipping write phase");
            return Ok(());
        };

        let socket_c = socket.clone();
        let br = bootstrap_registry.clone();
        let es = event_sender.clone();
        let ep = endpoint.clone();
        let ps = Arc::clone(persistence);
        tokio::spawn(async move {
            es.send_includable(id, &ep, true, false);
            match bootstrap_write_phase(ep.clone(), addr, socket_c, br.clone(), server_uri).await {
                Ok(()) => {
                    es.send_connection_status(&ep, true);
                    es.send_includable(id, &ep, true, true);
                    br.remove_includable_id(&ep).await;
                    br.mark_included(&ep).await;
                    let included = br.included_list().await;
                    let ps2 = Arc::clone(&ps);
                    tokio::spawn(async move {
                        let _ = tokio::task::spawn_blocking(move || ps2.save_included(&included)).await;
                    });
                    info!(device = %ep, activity = "inclusion", "Inclusion completed");
                }
                Err(e) => {
                    error!(device = %ep, activity = "inclusion", "Bootstrap write phase failed: {e}");
                    br.remove_includable_id(&ep).await;
                }
            }
        });
        return Ok(());
    }

    // Case 2: cert not yet cached — start GET /0/0 if not already in flight.
    if !bootstrap_registry.has_cert(&endpoint).await {
        let Some((token, mid)) = bootstrap_registry.begin(endpoint.clone(), addr).await else {
            info!(device = %endpoint, activity = "inclusion", "Bootstrap: GET /0/0 already in flight");
            return Ok(());
        };
        info!(device = %endpoint, activity = "inclusion", "Device needs authentication");

        // Device needs ~3 s after sending /bs to open its receive socket.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut get = Packet::new();
        get.header.set_type(MessageType::Confirmable);
        get.header.code = MessageClass::Request(Method::Get);
        get.header.message_id = mid;
        get.set_token(token[..4].to_vec());
        get.add_option(CoapOption::UriPath, b"0".to_vec());
        get.add_option(CoapOption::UriPath, b"0".to_vec());

        let bytes = get
            .to_bytes()
            .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;
        send_bootstrap_packet(socket, &bytes, addr).await?;
        info!(device = %endpoint, activity = "inclusion", "Bootstrap read security object");
    }
    // else: cert cached, awaiting user approval — event already emitted, nothing more to do.

    Ok(())
}

fn make_response_bytes(request: &Packet, status: Status, payload: Option<Vec<u8>>) -> Result<Vec<u8>> {
    let mut response = make_response(request, status);
    if let Some(body) = payload {
        response.payload = body;
    }
    response.to_bytes().map_err(|e| crate::error::Error::Coap(format!("{e:?}")))
}

/// Send a bootstrap-phase CoAP packet (TC=0x0c — no MAC-layer encryption).
async fn send_bootstrap_packet(socket: &UdpSocket, bytes: &[u8], addr: SocketAddr) -> Result<()> {
    set_tclass(socket, TC_PLAIN);
    socket.send_to(bytes, addr).await?;
    Ok(())
}

/// Send a post-bootstrap CoAP packet (TC=0x1c — MAC-layer encryption active).
async fn send_encrypted_packet(socket: &UdpSocket, bytes: &[u8], addr: SocketAddr) -> Result<()> {
    set_tclass(socket, TC_ENCRYPTED);
    socket.send_to(bytes, addr).await?;
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
    bootstrap_registry: &BootstrapRegistry,
    coap_dispatch_tx: &mpsc::Sender<DispatchRequest>,
    persistence: &Arc<PersistenceStore>,
) -> Result<()> {
    let query = uri_query(&packet);
    let params = parse_query(&query);

    let ep_raw = params.get("ep").map(String::as_str).unwrap_or("");
    let endpoint = super::sgtin_from_ep(ep_raw).to_owned();
    let lifetime: u32 = params
        .get("lt")
        .and_then(|v| v.parse().ok())
        .unwrap_or(86400);
    let lwm2m_version = params.get("lwm2m").cloned().unwrap_or_else(|| "1.0".to_owned());
    let binding_mode = params.get("b").cloned().unwrap_or_default();

    if endpoint.is_empty() {
        warn!(%addr, "Registration missing ep parameter");
        send_encrypted_response(socket, addr, &packet, Status::BadRequest, None).await?;
        return Ok(());
    }

    // Parse link-format body for registered objects and their versions.
    let body = std::str::from_utf8(packet.payload.as_slice()).unwrap_or("");
    let (objects, object_versions) = parse_link_format(body);

    let id = registry.register(endpoint.clone(), addr, lifetime, objects, object_versions, lwm2m_version, binding_mode).await;

    // 2.01 Created with Location-Path: rd / <id>
    let mut response = make_response(&packet, Status::Created);
    response.add_option(CoapOption::LocationPath, b"rd".to_vec());
    response.add_option(CoapOption::LocationPath, id.to_string().into_bytes());

    let bytes = response
        .to_bytes()
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;
    send_encrypted_packet(socket, &bytes, addr).await?;

    info!(device = %endpoint, id, %addr, activity = "registration", "Device registered");

    if !bootstrap_registry.is_included(&endpoint).await {
        warn!(device = %endpoint, activity = "registration", "Device not included — triggering factory reset");

        let (response_tx, _) = oneshot::channel();
        let op = PendingOperation {
            id: INTERNAL_OP_ID.fetch_add(1, Ordering::Relaxed),
            command: LwM2mCommand::Execute {
                path: ResourcePath { object_id: 3, instance_id: 0, resource_id: 5 },
                args: None,
            },
            response_tx,
            created_at: std::time::Instant::now(),
            attempts: 0,
        };
        let _ = coap_dispatch_tx.send(DispatchRequest { addr, ops: vec![op] }).await;

        let registry = registry.clone();
        let ps = Arc::clone(persistence);
        let ep = endpoint.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            registry.remove_by_addr(addr).await;
            info!(device = %ep, activity = "registration", "Device disconnected after factory reset");
            let snapshots = registry.snapshot().await;
            let _ = tokio::task::spawn_blocking(move || ps.save_registry(&snapshots)).await;
        });

        return Ok(());
    }

    let snapshots = registry.snapshot().await;
    let ps = Arc::clone(persistence);
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || ps.save_registry(&snapshots)).await;
    });

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

    // Always reset the expiry timer; apply a new lifetime if lt= was included.
    let query = uri_query(&packet);
    let new_lt = parse_query(&query).get("lt").and_then(|v| v.parse::<u32>().ok());
    registry.renew_registration(addr, new_lt).await;

    // Drain pending ops and hand them to the dispatch task.
    let ops = registry.drain_pending(addr).await;
    if !ops.is_empty() {
        info!(%addr, count = ops.len(), "Dispatching pending ops on device update");
        let _ = coap_dispatch_tx.send(DispatchRequest { addr, ops }).await;
    }
    Ok(())
}

async fn handle_ack(
    packet: Packet,
    addr: SocketAddr,
    registry: &DeviceRegistry,
    bootstrap_registry: &BootstrapRegistry,
    block_acks: &BlockAckMap,
) -> Result<()> {
    let token = token_array(packet.get_token());

    // Write-phase ACKs (DELETE /1, PUT /1/1, DELETE /0, PUT /0/1, POST /bs finish).
    if bootstrap_registry.complete_write_ack(&token, true).await {
        return Ok(());
    }

    // Bootstrap GET /0/0 ACK — validate cert, cache on success; write phase triggered on next /bs.
    if bootstrap_registry.is_pending(&token).await {
        if let Some(session) = bootstrap_registry.complete(&token, packet.payload.clone()).await {
            let payload = session.pubkey_payload.as_deref().unwrap_or(&[]);
            let valid = bootstrap::parse_device_pubkey(payload)
                .and_then(|cert_der| bootstrap::validate_device_certificate(&cert_der));
            match valid {
                Ok(()) => {
                    info!(
                        device = %session.endpoint,
                        bytes = payload.len(),
                        activity = "inclusion",
                        "Device available for inclusion"
                    );
                }
                Err(e) => {
                    warn!(
                        device = %session.endpoint,
                        activity = "inclusion",
                        "Device certificate rejected: {e}"
                    );
                    bootstrap_registry.remove_from_cert_cache(&session.endpoint).await;
                }
            }
        }
        return Ok(());
    }

    // Block-wise transfer ACK (2.31 Continue or final response for block writes).
    if let Some(tx) = block_acks.lock().await.remove(&token) {
        let _ = tx.send(packet);
        return Ok(());
    }

    let Some(op) = registry.complete_in_flight(addr, &token).await else {
        // Spurious or duplicate ACK — ignore.
        return Ok(());
    };

    let _ = op.response_tx.send(super::coap_response_to_result(&packet));
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
    persistence: &Arc<PersistenceStore>,
) -> Result<()> {
    send_encrypted_response(socket, addr, &packet, Status::Changed, None).await?;

    if packet.payload.is_empty() {
        return Ok(());
    }

    let Some(endpoint) = registry.endpoint_by_addr(addr).await else {
        warn!(%addr, "Unknown device, ignoring /dp payload");
        return Ok(());
    };

    let obj_versions = registry.object_versions_by_addr(addr).await.unwrap_or_default();

    match build_device_payload(&packet.payload, ipso, &obj_versions) {
        Some(payload) => {
            if let Some(objs) = payload.as_object() {
                let instances: Vec<String> = objs.iter()
                    .flat_map(|(obj, insts)| {
                        insts.as_object()
                            .map(|m| m.keys()
                                .filter(|k| *k != "_urn")
                                .map(|inst| format!("{obj}/{inst}"))
                                .collect::<Vec<_>>())
                            .unwrap_or_default()
                    })
                    .collect();
                info!(device = %endpoint, activity = "state", "Reported object instance(s): {}", instances.join(", "));
                for (obj, insts) in objs {
                    if let Some(inst_map) = insts.as_object() {
                        for (inst, resources) in inst_map {
                            if inst == "_urn" { continue; }
                            if let Some(res_map) = resources.as_object() {
                                for (res, val) in res_map {
                                    if res == "_urn" { continue; }
                                    let formatted = format_resource_value(val);
                                    info!(device = %endpoint, activity = "state", "Reported resource {obj}/{inst}/{res} as {formatted}");
                                }
                            }
                        }
                    }
                }
            }
            let state = registry.merge_device_state_by_addr(addr, payload.clone()).await;
            event_sender.send_device_data(&endpoint, payload);
            let ps = Arc::clone(persistence);
            let ep = endpoint.clone();
            tokio::spawn(async move {
                let _ = tokio::task::spawn_blocking(move || ps.save_device_state(&ep, &state)).await;
            });
        }
        None => warn!(%addr, device = %endpoint, activity = "state", "No known objects in /dp payload"),
    }

    Ok(())
}

// ── SenML+CBOR → event payload ────────────────────────────────────────────────

type RawPayload = std::collections::BTreeMap<
    u32,
    std::collections::BTreeMap<u32, std::collections::BTreeMap<u32, (Vec<SenmlValue>, bool)>>,
>;

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

    let top: Cbor = match ciborium::from_reader(data) {
        Ok(v) => v,
        Err(e) => {
            warn!(bytes = data.len(), "CBOR decode failed: {e}");
            return None;
        }
    };

    let Cbor::Array(records) = top else {
        warn!("Expected CBOR array at top level");
        return None;
    };

    let ts = crate::ipc::event::unix_ts();
    let mut base_name = String::new();

    // obj_id → inst_id → res_id → (values, is_array)
    // is_array = true when the SenML path included a resource-instance segment OR
    // when the IPSO definition marks the resource as MultipleInstances.
    let mut raw: RawPayload = RawPayload::new();

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
                    None    => (res_id.to_string(), &crate::lwm2m::ipso::ResourceType::Integer, false),
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
    let has_res_inst = parts.next().is_some_and(|p| p.parse::<u32>().is_ok());
    Some((obj, inst, res, has_res_inst))
}

fn encode_single_value(
    v: &SenmlValue,
    res_type: &crate::lwm2m::ipso::ResourceType,
    ts: u64,
) -> serde_json::Value {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use crate::lwm2m::ipso::ResourceType::*;
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
    res_type: &crate::lwm2m::ipso::ResourceType,
    ts: u64,
) -> serde_json::Value {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use crate::lwm2m::ipso::ResourceType::*;
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
    info!(device = %endpoint, activity = "inclusion", "Bootstrap start");

    let server_pubkey = registry.server_pubkey_bytes().to_vec();
    let network_key = registry.network_key().to_vec();
    let server_secret = registry.server_secret_key();

    // Step 1 — DELETE /1 (clear existing Server Object instances)
    {
        info!(device = %endpoint, activity = "inclusion", "Bootstrap delete server object");
        let (token, mid) = registry.alloc_token_mid().await;
        let pkt = build_delete(mid, &token[..4], &["1"])?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(device = %endpoint, activity = "inclusion", "Bootstrap server object deleted");
    }

    // Step 2 — PUT /1/1 (LWM2M Server Object: lifetime, binding, server ID)
    {
        info!(device = %endpoint, activity = "inclusion", "Bootstrap write server object");
        let (token, mid) = registry.alloc_token_mid().await;
        let payload = bootstrap::encode_server_object();
        let pkt = build_put(mid, &token[..4], &["1", "1"], SENML_CBOR, payload)?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(device = %endpoint, activity = "inclusion", "Bootstrap write server object done");
    }

    // Step 3 — DELETE /0 (clear existing Security Object instances)
    {
        info!(device = %endpoint, activity = "inclusion", "Bootstrap delete security object");
        let (token, mid) = registry.alloc_token_mid().await;
        let pkt = build_delete(mid, &token[..4], &["0"])?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
        info!(device = %endpoint, activity = "inclusion", "Bootstrap security object deleted");
    }

    // Step 4 — PUT /0/1 (LWM2M Security Object: URI, server pubkey, encrypted network key)
    {
        info!(device = %endpoint, activity = "inclusion", "Bootstrap write security object");
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
        info!(device = %endpoint, activity = "inclusion", "Bootstrap write security object done");
    }

    // Step 5 — POST /bs (bootstrap finish signal; device switches to encrypted traffic)
    {
        let (token, mid) = registry.alloc_token_mid().await;
        let pkt = build_post_bs(mid, &token[..4])?;
        send_con_write_step(&socket, &pkt, addr, token, &registry).await?;
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
            tracing::debug!(attempt, %addr, "Bootstrap write: retransmitting");
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

fn make_response(request: &Packet, status: Status) -> Packet {
    let mut response = Packet::new();
    response.header.set_type(MessageType::Acknowledgement);
    response.header.code = MessageClass::Response(status);
    response.header.message_id = request.header.message_id;
    response.set_token(request.get_token().to_vec());
    response
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

fn format_resource_value(val: &serde_json::Value) -> String {
    if let Some(v) = val.get("vs").and_then(|v| v.as_str()) { return format!("'{v}'"); }
    if let Some(v) = val.get("vi") { return v.to_string(); }
    if let Some(v) = val.get("vf") { return v.to_string(); }
    if let Some(v) = val.get("vb") { return v.to_string(); }
    if let Some(v) = val.get("vt") { return v.to_string(); }
    if let Some(v) = val.get("vo").and_then(|v| v.as_str()) { return format!("'{v}'"); }
    for key in ["ai", "ab", "as", "af", "ao"] {
        if let Some(v) = val.get(key) { return v.to_string(); }
    }
    val.to_string()
}

fn token_array(token: &[u8]) -> [u8; 8] {
    let mut arr = [0u8; 8];
    let len = token.len().min(8);
    arr[..len].copy_from_slice(&token[..len]);
    arr
}
