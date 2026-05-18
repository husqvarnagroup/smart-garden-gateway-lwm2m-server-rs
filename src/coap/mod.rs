pub mod client;
pub mod server;

use std::{net::SocketAddr, sync::Arc};

use tokio::net::UdpSocket;
use tracing::info;

use crate::error::Result;

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

/// LWM2M registration path prefix.
pub const RD_PATH: &str = "rd";
