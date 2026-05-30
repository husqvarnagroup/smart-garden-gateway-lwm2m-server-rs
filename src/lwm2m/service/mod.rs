//! Tower service layers for the LwM2M CoAP stack.
//!
//! Provides shared infrastructure used by both the inbound (server) and outbound
//! (client/dispatch) CoAP processing pipelines:
//!
//! - [`retry`] — LwM2M 1.1 / RFC 7252 exponential-backoff retry policy
//! - [`dedup`] — CoAP message deduplication key extractors
//! - [`inbound`] — Tower service for processing received CoAP packets
//! - [`outbound`] — Tower service for dispatching operations to devices

pub mod dedup;
pub mod inbound;
pub mod outbound;
pub mod retry;

use std::{net::SocketAddr, sync::Arc};

use coap_lite::Packet;

/// Service-level error type: wraps [`crate::error::Error`] in an `Arc` to
/// satisfy the `Clone` bound required by the coalesce deduplication layer,
/// without polluting the domain error type with `Clone` semantics.
pub type ServiceError = Arc<crate::error::Error>;

/// An inbound CoAP request received from the UDP transport.
#[derive(Debug, Clone)]
pub struct CoapRequest {
    /// The parsed CoAP packet.
    pub packet: Packet,
    /// The source address of the UDP datagram.
    pub addr: SocketAddr,
}

/// A CoAP response to send back over UDP.
#[derive(Debug, Clone)]
pub struct CoapResponse {
    /// Serialised CoAP packet bytes to send.
    pub bytes: Vec<u8>,
    /// The destination address.
    pub addr: SocketAddr,
    /// IPv6 Traffic Class to set on the outgoing socket.
    pub traffic_class: u32,
}
