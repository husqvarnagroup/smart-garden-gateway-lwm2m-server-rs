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

#[cfg(test)]
mod tests {
    use super::*;
    use coap_lite::{MessageClass, MessageType, Packet, RequestType as Method};
    use std::net::{Ipv6Addr, SocketAddrV6};
    use tokio::sync::oneshot;

    use crate::model::{LwM2mCommand, PendingOperation, ResourcePath};

    fn make_coap_request(addr: SocketAddr, message_id: u16) -> CoapRequest {
        let mut pkt = Packet::new();
        pkt.header.set_type(MessageType::Confirmable);
        pkt.header.code = MessageClass::Request(Method::Post);
        pkt.header.message_id = message_id;
        CoapRequest { packet: pkt, addr }
    }

    fn make_dispatch_request(addr: SocketAddr, op_ids: &[u32]) -> DispatchRequest {
        let ops = op_ids
            .iter()
            .map(|&id| {
                let (tx, _rx) = oneshot::channel();
                PendingOperation::new(
                    id,
                    LwM2mCommand::Read {
                        path: ResourcePath {
                            object_id: 3,
                            instance_id: 0,
                            resource_id: 0,
                        },
                    },
                    tx,
                )
            })
            .collect();
        DispatchRequest { addr, ops }
    }

    #[test]
    fn inbound_same_addr_same_mid_produces_same_key() {
        let addr: SocketAddr = "[::1]:5683".parse().unwrap();
        let req1 = make_coap_request(addr, 42);
        let req2 = make_coap_request(addr, 42);
        assert_eq!(inbound_dedup_key(&req1), inbound_dedup_key(&req2));
    }

    #[test]
    fn inbound_same_addr_different_mid_produces_different_key() {
        let addr: SocketAddr = "[::1]:5683".parse().unwrap();
        let req1 = make_coap_request(addr, 42);
        let req2 = make_coap_request(addr, 43);
        assert_ne!(inbound_dedup_key(&req1), inbound_dedup_key(&req2));
    }

    #[test]
    fn inbound_different_addr_same_mid_produces_different_key() {
        let addr1: SocketAddr = "[::1]:5683".parse().unwrap();
        let addr2: SocketAddr = "[::1]:5684".parse().unwrap();
        let req1 = make_coap_request(addr1, 42);
        let req2 = make_coap_request(addr2, 42);
        assert_ne!(inbound_dedup_key(&req1), inbound_dedup_key(&req2));
    }

    #[test]
    fn inbound_key_includes_scope_id() {
        let addr1 = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 5683, 0, 1));
        let addr2 = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 5683, 0, 2));
        let req1 = make_coap_request(addr1, 42);
        let req2 = make_coap_request(addr2, 42);
        assert_ne!(inbound_dedup_key(&req1), inbound_dedup_key(&req2));
    }

    #[test]
    fn outbound_same_addr_same_op_id_produces_same_key() {
        let addr: SocketAddr = "[::1]:5683".parse().unwrap();
        let req1 = make_dispatch_request(addr, &[100]);
        let req2 = make_dispatch_request(addr, &[100]);
        assert_eq!(outbound_dedup_key(&req1), outbound_dedup_key(&req2));
    }

    #[test]
    fn outbound_same_addr_different_op_id_produces_different_key() {
        let addr: SocketAddr = "[::1]:5683".parse().unwrap();
        let req1 = make_dispatch_request(addr, &[100]);
        let req2 = make_dispatch_request(addr, &[101]);
        assert_ne!(outbound_dedup_key(&req1), outbound_dedup_key(&req2));
    }

    #[test]
    fn outbound_different_addr_same_op_id_produces_different_key() {
        let addr1: SocketAddr = "[::1]:5683".parse().unwrap();
        let addr2: SocketAddr = "[::1]:5684".parse().unwrap();
        let req1 = make_dispatch_request(addr1, &[100]);
        let req2 = make_dispatch_request(addr2, &[100]);
        assert_ne!(outbound_dedup_key(&req1), outbound_dedup_key(&req2));
    }

    #[test]
    fn outbound_empty_ops_uses_zero_key() {
        let addr: SocketAddr = "[::1]:5683".parse().unwrap();
        let req = DispatchRequest { addr, ops: vec![] };
        assert_eq!(outbound_dedup_key(&req), (addr, 0));
    }

    #[test]
    fn outbound_uses_first_op_id_only() {
        let addr: SocketAddr = "[::1]:5683".parse().unwrap();
        let req = make_dispatch_request(addr, &[7, 8, 9]);
        assert_eq!(outbound_dedup_key(&req), (addr, 7));
    }
}
