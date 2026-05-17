pub mod client;
pub mod server;

use std::{net::SocketAddr, sync::Arc};

use tokio::net::UdpSocket;
use tracing::info;

use crate::error::Result;

pub async fn bind(addr: SocketAddr) -> Result<Arc<UdpSocket>> {
    let sock = UdpSocket::bind(addr).await?;
    info!(bind = %addr, "CoAP UDP socket bound");
    Ok(Arc::new(sock))
}

/// CoAP content-format values used by LWM2M 1.0.
pub mod content_format {
    pub const TEXT_PLAIN: u16 = 0;
    pub const LINK_FORMAT: u16 = 40;
    pub const TLV: u16 = 11542;
    pub const JSON: u16 = 11543;
}

/// LWM2M registration path prefix.
pub const RD_PATH: &str = "rd";
