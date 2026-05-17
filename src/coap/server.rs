use std::{net::SocketAddr, sync::Arc};

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status,
};
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    bootstrap::BootstrapRegistry,
    error::Result,
    model::{LwM2mError, MqttResponse, PendingOperation, ResourceValue},
    registry::DeviceRegistry,
};

use super::RD_PATH;

const BS_PATH: &str = "bs";

const MAX_PACKET: usize = 1500;

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
        info!(%endpoint, %addr, "bootstrap: certificate cached, ACKing /bs for write phase");
        let bytes = make_response_bytes(&packet, Status::Changed, None)?;
        send_bootstrap_packet(socket, &bytes, addr).await?;
        // TODO: bootstrap write phase (send network key derived via ECDH)
        return Ok(());
    }

    // First /bs: do NOT ACK — just trigger the CON GET /0/0 to read the device certificate.
    let Some((token, mid)) = bootstrap_registry.begin(endpoint.clone(), addr).await else {
        info!(%endpoint, "bootstrap: GET /0/0 already in flight, ignoring duplicate /bs");
        return Ok(());
    };

    // The original server waited ~3 s before sending GET /0/0. The device appears to need
    // this window to open its response socket after transmitting the CON POST /bs.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

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
        send_response(socket, addr, &packet, Status::BadRequest, None).await?;
        return Ok(());
    }

    // Parse link-format body for registered objects.
    let body = std::str::from_utf8(packet.payload.as_slice()).unwrap_or("");
    let objects = parse_link_format(body);

    let id = registry.register(endpoint.clone(), addr, lifetime, objects).await;

    // 2.01 Created with Location-Path: rd / <id>
    let mut response = make_response(&packet, Status::Created);
    response
        .add_option(CoapOption::LocationPath, b"rd".to_vec());
    response
        .add_option(CoapOption::LocationPath, id.to_string().into_bytes());

    let bytes = response
        .to_bytes()
        .map_err(|e| crate::error::Error::Coap(format!("{e:?}")))?;
    socket.send_to(&bytes, addr).await?;

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
    send_response(socket, addr, &packet, Status::Changed, None).await?;

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

    // Check bootstrap sessions first (token IDs ≥ 0x8000_0000, no overlap with op tokens).
    if bootstrap_registry.is_pending(&token).await {
        if let Some(session) = bootstrap_registry.complete(&token, packet.payload.clone()).await {
            info!(
                endpoint = %session.endpoint,
                bytes = session.pubkey_payload.as_ref().map_or(0, |p| p.len()),
                "bootstrap: public key received — ready for ECDH"
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
