//! CoAP message deduplication key extractors (RFC 7252 §4.5).

use std::net::SocketAddr;

use super::CoapRequest;
use crate::lwm2m::server::DispatchRequest;

// ── Server (inbound) deduplication ───────────────────────────────────────────

/// Deduplication key for inbound CoAP packets: (source address, message ID).
///
/// Per RFC 7252 §4.5, a server MUST detect duplicate Confirmable messages
/// based on the combination of the sender endpoint and the Message ID within
/// EXCHANGE_LIFETIME (247 s with default parameters).
pub type InboundDeduplicationKey = (SocketAddr, u16);

/// Extract the deduplication key from an inbound CoAP request.
pub fn inbound_dedup_key(req: &CoapRequest) -> InboundDeduplicationKey {
    (req.addr, req.packet.header.message_id)
}

// ── Client (outbound) deduplication ──────────────────────────────────────────

/// Deduplication key for outbound dispatch requests: (destination address, first op ID).
///
/// On the client/dispatch side, the operation ID serves as the logical message
/// identifier: if the same operation is dispatched again while still in flight,
/// the coalesce layer suppresses redundant processing.
pub type OutboundDeduplicationKey = (SocketAddr, u32);

/// Extract the deduplication key from a dispatch request.
pub fn outbound_dedup_key(req: &DispatchRequest) -> OutboundDeduplicationKey {
    let first_op_id = req.ops.first().map(|op| op.id).unwrap_or(0);
    (req.addr, first_op_id)
}
