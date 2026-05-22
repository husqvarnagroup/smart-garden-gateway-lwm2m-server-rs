pub mod bootstrap;
pub mod client;
pub mod ipso;
pub mod server;

use std::{net::SocketAddr, sync::Arc};

use tokio::net::UdpSocket;
use tracing::info;

use crate::error::Result;

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

    let domain = if addr.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
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

/// CoAP content-format values used by LWM2M 1.0.
pub mod content_format {
    pub const SENML_CBOR: u16 = 112;
}


/// Extract the bare SGTIN from a CoAP `ep` parameter that may carry a full URN.
///
/// `urn:dev:sg:3034F8319C00754000000097` → `3034F8319C00754000000097`
/// `sgtin:3034F8319C00754000000097`      → `3034F8319C00754000000097`
/// `3034F8319C00754000000097`            → `3034F8319C00754000000097`
pub fn sgtin_from_ep(ep: &str) -> &str {
    ep.rfind(':').map(|i| &ep[i + 1..]).unwrap_or(ep)
}
