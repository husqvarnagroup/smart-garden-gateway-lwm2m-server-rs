pub mod bootstrap;
pub mod client;
pub mod ipso;
pub mod server;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use coap_lite::{CoapOption, MessageClass, MessageType, Packet, RequestType as Method};
use tokio::{
    net::UdpSocket,
    sync::{oneshot, Mutex},
};
use tracing::info;

use crate::{
    error::Result,
    model::{LwM2mError, ResourceValue},
};

/// Map from CoAP token to a waiting receiver for the next ACK packet.
/// Used to route intermediate 2.31 Continue ACKs to the block-write task
/// without involving the in-flight registry.
pub type BlockAckMap = Arc<Mutex<HashMap<[u8; 8], oneshot::Sender<Packet>>>>;

pub fn new_block_ack_map() -> BlockAckMap {
    Arc::new(Mutex::new(HashMap::new()))
}

pub(super) fn response_type_to_class_detail(status: coap_lite::ResponseType) -> (u8, u8) {
    use coap_lite::ResponseType::*;
    match status {
        Created => (2, 1),
        Deleted => (2, 2),
        Valid => (2, 3),
        Changed => (2, 4),
        Content => (2, 5),
        BadRequest => (4, 0),
        Unauthorized => (4, 1),
        BadOption => (4, 2),
        Forbidden => (4, 3),
        NotFound => (4, 4),
        MethodNotAllowed => (4, 5),
        NotAcceptable => (4, 6),
        PreconditionFailed => (4, 12),
        RequestEntityTooLarge => (4, 13),
        UnsupportedContentFormat => (4, 15),
        InternalServerError => (5, 0),
        NotImplemented => (5, 1),
        BadGateway => (5, 2),
        ServiceUnavailable => (5, 3),
        GatewayTimeout => (5, 4),
        ProxyingNotSupported => (5, 5),
        _ => (5, 0),
    }
}

pub(super) fn coap_response_to_result(packet: &Packet) -> crate::model::LwM2mResult {
    match packet.header.code {
        coap_lite::MessageClass::Response(status) => {
            let (class, detail) = response_type_to_class_detail(status);
            if class == 2 {
                Ok(ResourceValue::CoapResponse { class, detail })
            } else {
                Err(LwM2mError::CoapError { class, detail })
            }
        }
        _ => Err(LwM2mError::CoapError {
            class: 5,
            detail: 0,
        }),
    }
}

/// IPv6 Traffic Class: no MAC-layer encryption (bootstrap phase).
pub const TC_PLAIN: u32 = 0x0c;
/// IPv6 Traffic Class: MAC-layer encryption active (post-bootstrap).
pub const TC_ENCRYPTED: u32 = 0x1c;

/// Set the IPv6 Traffic Class on the socket before a send.
///
/// On Linux this controls MAC-layer encryption in the radio module.
/// On other platforms (macOS dev builds) the call is a no-op.
pub fn set_tclass(socket: &UdpSocket, tc: u32) {
    #[cfg(target_os = "linux")]
    {
        use socket2::SockRef;
        if let Err(e) = SockRef::from(socket).set_tclass_v6(tc) {
            tracing::warn!(tc, "Failed to set traffic class: {e}");
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (socket, tc);
}

pub async fn bind(addr: SocketAddr, interface: Option<&str>) -> Result<Arc<UdpSocket>> {
    use socket2::{Domain, Protocol, Socket, Type};

    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;

    if let Some(iface) = interface {
        // SO_BINDTODEVICE — forces all tx/rx through this interface.
        // This ensures the GET /0/0 leaves via ppp0 with the correct source address
        // so the device recognises the server and sends its ACK back.
        // bind_device is Linux-only; on macOS (dev builds) we skip it.
        #[cfg(target_os = "linux")]
        sock.bind_device(Some(iface.as_bytes()))?;
        #[cfg(not(target_os = "linux"))]
        tracing::warn!(iface, "SO_BINDTODEVICE not supported on this OS — ignored");
        info!(iface, "CoAP socket bound to interface");
    }

    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;

    let std_sock: std::net::UdpSocket = sock.into();
    let tokio_sock = UdpSocket::from_std(std_sock)?;

    info!(bind = %addr, "CoAP UDP socket bound");
    Ok(Arc::new(tokio_sock))
}

/// Returns a compact one-line description of a CoAP packet for debug logging.
pub(super) fn coap_summary(pkt: &Packet) -> String {
    let mtype = match pkt.header.get_type() {
        MessageType::Confirmable => "CON",
        MessageType::NonConfirmable => "NON",
        MessageType::Acknowledgement => "ACK",
        MessageType::Reset => "RST",
    };

    let code = match pkt.header.code {
        MessageClass::Empty => "0.00".to_owned(),
        MessageClass::Request(m) => match m {
            Method::Get => "GET".to_owned(),
            Method::Post => "POST".to_owned(),
            Method::Put => "PUT".to_owned(),
            Method::Delete => "DELETE".to_owned(),
            _ => "?".to_owned(),
        },
        MessageClass::Response(s) => {
            let (class, detail) = response_type_to_class_detail(s);
            format!("{class}.{detail:02}")
        }
        _ => "?".to_owned(),
    };

    let mid = pkt.header.message_id;
    let token: String = pkt.get_token().iter().map(|b| format!("{b:02x}")).collect();

    let path = pkt
        .get_option(CoapOption::UriPath)
        .map(|opts| {
            format!(
                "/{}",
                opts.iter()
                    .map(|v| String::from_utf8_lossy(v).into_owned())
                    .collect::<Vec<_>>()
                    .join("/")
            )
        })
        .unwrap_or_default();

    let size = if pkt.payload.is_empty() {
        String::new()
    } else {
        format!(" {}B", pkt.payload.len())
    };

    format!("{mtype} {code}{path} mid={mid} tok={token}{size}")
}

/// CoAP content-format values used by LWM2M 1.0.
pub mod content_format {
    pub const SENML_CBOR: u16 = 112;
}

/// Encode a CoAP Content-Format option value using the minimum number of bytes
/// (RFC 7252 §3.2: option values are compact unsigned integers).
pub(super) fn encode_content_format(cf: u16) -> Vec<u8> {
    if cf <= 0xFF {
        vec![cf as u8]
    } else {
        cf.to_be_bytes().to_vec()
    }
}

/// Extract the bare SGTIN from a CoAP `ep` parameter that may carry a full URN.
///
/// `urn:dev:sg:3034F8319C00754000000097` → `3034F8319C00754000000097`
/// `sgtin:3034F8319C00754000000097`      → `3034F8319C00754000000097`
/// `3034F8319C00754000000097`            → `3034F8319C00754000000097`
pub fn sgtin_from_ep(ep: &str) -> &str {
    ep.rfind(':').map(|i| &ep[i + 1..]).unwrap_or(ep)
}
