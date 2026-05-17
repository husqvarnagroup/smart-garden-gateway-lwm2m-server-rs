use std::{net::SocketAddr, sync::Arc};

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status,
};
use tokio::{net::UdpSocket, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    error::Result,
    model::{LwM2mError, MqttResponse, PendingOperation, ResourceValue},
    registry::DeviceRegistry,
};

use super::RD_PATH;

const MAX_PACKET: usize = 1500;

pub async fn run(
    socket: Arc<UdpSocket>,
    registry: DeviceRegistry,
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
                    handle_post(packet, addr, socket, registry, coap_dispatch_tx).await?;
                }
                other => {
                    warn!(%addr, ?other, "unexpected CoAP request method");
                }
            }
        }
        // Device acknowledging one of our downlink requests.
        MessageType::Acknowledgement => {
            handle_ack(packet, addr, registry, mqtt_out_tx).await?;
        }
        MessageType::Reset => {
            // Device rejected our message — treat as error for the in-flight op.
            let token = token_array(packet.get_token());
            if let Some(op) = registry.complete_in_flight(addr, &token).await {
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
    coap_dispatch_tx: &mpsc::Sender<DispatchRequest>,
) -> Result<()> {
    let path = uri_path(&packet);
    let path_parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    match path_parts.as_slice() {
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
    mqtt_out_tx: &mpsc::Sender<MqttResponse>,
) -> Result<()> {
    let token = token_array(packet.get_token());
    let Some(op) = registry.complete_in_flight(addr, &token).await else {
        // Spurious or duplicate ACK — ignore.
        return Ok(());
    };

    let result = coap_response_to_result(&packet);
    let path = match &op.command {
        crate::model::LwM2mCommand::Read { path }
        | crate::model::LwM2mCommand::Write { path, .. }
        | crate::model::LwM2mCommand::Execute { path, .. } => path.clone(),
    };

    // Fire the oneshot so the MQTT publisher gets the result.
    // The response_tx carries a LwM2mResult; we need to also route to MQTT.
    // We use a side-channel: build an MqttResponse directly here using
    // the endpoint name recovered from the op's context (stored in command path).
    // Since PendingOperation doesn't carry the endpoint/correlation_id, we use
    // a wrapper approach: the MQTT subscriber wraps the oneshot in a future
    // that maps the result to MqttResponse. See mqtt/subscriber.rs.
    let _ = op.response_tx.send(result.clone());

    // Also publish directly if result is available (belt-and-suspenders path
    // for operations queued without an MQTT correlation wrapper).
    // In normal flow the subscriber's spawned future handles publishing.
    let _ = mqtt_out_tx; // suppress unused warning; used via subscriber wrapper
    let _ = path;

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
