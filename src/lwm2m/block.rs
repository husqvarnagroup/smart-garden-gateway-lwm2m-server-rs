//! CoAP block-wise transfer (RFC 7959) as a tower layer.
//!
//! Sits outermost in the CoAP service stack. Intermediate Block1 blocks are
//! answered with 2.31 Continue, oversized payloads without Block1 with 4.13,
//! and Block2 follow-ups are served from cache — all without calling the
//! inner service. The final Block1 block forwards the reassembled body to the
//! inner service and its response carries the Block1 echo option (§2.3).
//! Large responses are fragmented into Block2 sequences.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use coap_lite::{
    block_handler::{BlockHandler, BlockHandlerConfig, BlockValue},
    CoapOption, CoapRequest, Packet,
};
use tower::{Layer, Service};
use tracing::error;

use super::server::{InboundMessage, OutboundMessage, COAP_EXCHANGE_LIFETIME};

/// RFC 7252 §4.6 DEFAULT_MAX_MESSAGE_SIZE; yields 1024-byte blocks (SZX=6).
const MAX_TOTAL_MESSAGE_SIZE: usize = 1152;

pub fn block_handler_config() -> BlockHandlerConfig {
    BlockHandlerConfig {
        max_total_message_size: MAX_TOTAL_MESSAGE_SIZE,
        cache_expiry_duration: COAP_EXCHANGE_LIFETIME,
    }
}

/// Tower service implementing the server side of RFC 7959 block-wise
/// transfers. All clones share one `BlockHandler`, so concurrent packets from
/// the same peer contribute to the same assembly buffer and response cache.
pub struct BlockService<S> {
    inner: S,
    block_handler: Arc<Mutex<BlockHandler<SocketAddr>>>,
}

impl<S: Clone> Clone for BlockService<S> {
    fn clone(&self) -> Self {
        BlockService {
            inner: self.inner.clone(),
            block_handler: Arc::clone(&self.block_handler),
        }
    }
}

impl<S> BlockService<S> {
    pub fn new(inner: S, config: BlockHandlerConfig) -> Self {
        BlockService {
            inner,
            block_handler: Arc::new(Mutex::new(BlockHandler::new(config))),
        }
    }
}

impl<S> Service<InboundMessage> for BlockService<S>
where
    S: Service<InboundMessage, Response = Option<OutboundMessage>>,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Option<OutboundMessage>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, msg: InboundMessage) -> Self::Future {
        let addr = msg.addr;
        let block_handler = Arc::clone(&self.block_handler);

        // Unparsable bytes go to the inner service unchanged; it reports the
        // parse error itself.
        let Ok(packet) = Packet::from_bytes(&msg.bytes) else {
            return Box::pin(self.inner.call(msg));
        };
        let mut request = CoapRequest::from_packet(packet, addr);

        // true → consumed by the block handler (2.31 Continue, 4.13 Too
        // Large, or a cached Block2 chunk sits in request.response).
        // false → pass on; a final Block1 block has been replaced with the
        // reassembled body and the Block1 ack pre-set in request.response.
        let handled = {
            let mut h = block_handler.lock().expect("BlockHandler mutex poisoned");
            h.intercept_request(&mut request).unwrap_or(false)
        };

        if handled {
            let response = request
                .response
                .and_then(|r| r.message.to_bytes().ok())
                .map(|bytes| OutboundMessage {
                    addr,
                    bytes,
                    encrypted: true,
                });
            return Box::pin(async move { Ok(response) });
        }

        // Save the Block1 ack: the inner response replaces request.response
        // and the ack must be re-attached afterwards (RFC 7959 §2.3).
        let block1_ack = request.response.as_ref().and_then(|r| {
            r.message
                .get_first_option_as::<BlockValue>(CoapOption::Block1)
                .and_then(|x| x.ok())
        });

        // Re-serialise so the inner service sees the reassembled body.
        let Ok(fwd_bytes) = request.message.to_bytes() else {
            return Box::pin(self.inner.call(msg));
        };
        let inner_fut = self.inner.call(InboundMessage {
            addr,
            bytes: fwd_bytes,
        });

        Box::pin(async move {
            let Some(resp_msg) = inner_fut.await? else {
                return Ok(None);
            };

            // Let the block handler cache and fragment oversized responses
            // (Block2), then re-attach the Block1 ack.
            let Ok(resp_packet) = Packet::from_bytes(&resp_msg.bytes) else {
                return Ok(Some(resp_msg));
            };
            let Some(r) = request.response.as_mut() else {
                return Ok(Some(resp_msg));
            };
            r.message = resp_packet;

            {
                let mut h = block_handler.lock().expect("BlockHandler mutex poisoned");
                if let Err(e) = h.intercept_response(&mut request) {
                    error!("BlockHandler intercept_response error: {e:?}");
                }
            }

            if let (Some(ack), Some(r)) = (block1_ack, request.response.as_mut()) {
                r.message.add_option_as(CoapOption::Block1, ack);
            }

            match request.response.and_then(|r| r.message.to_bytes().ok()) {
                Some(bytes) => Ok(Some(OutboundMessage {
                    addr: resp_msg.addr,
                    bytes,
                    encrypted: resp_msg.encrypted,
                })),
                None => Ok(Some(resp_msg)),
            }
        })
    }
}

/// Tower layer wrapping a service with [`BlockService`]. Config fields are
/// stored individually because [`BlockHandlerConfig`] is not `Clone`.
#[derive(Clone, Copy)]
pub struct BlockLayer {
    max_total_message_size: usize,
    cache_expiry_duration: std::time::Duration,
}

impl BlockLayer {
    pub fn new(config: BlockHandlerConfig) -> Self {
        BlockLayer {
            max_total_message_size: config.max_total_message_size,
            cache_expiry_duration: config.cache_expiry_duration,
        }
    }
}

impl<S> Layer<S> for BlockLayer {
    type Service = BlockService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BlockService::new(
            inner,
            BlockHandlerConfig {
                max_total_message_size: self.max_total_message_size,
                cache_expiry_duration: self.cache_expiry_duration,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lwm2m::server::ServerError;
    use coap_lite::{MessageClass, MessageType, RequestType, ResponseType};
    use std::time::Duration;

    /// Inner service that returns 2.01 Created and captures the request
    /// payload it received.
    #[derive(Clone, Default)]
    struct CreatedService(Arc<Mutex<Vec<u8>>>);

    impl Service<InboundMessage> for CreatedService {
        type Response = Option<OutboundMessage>;
        type Error = ServerError;
        type Future =
            Pin<Box<dyn Future<Output = Result<Option<OutboundMessage>, ServerError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, msg: InboundMessage) -> Self::Future {
            let captured = Arc::clone(&self.0);
            Box::pin(async move {
                let packet =
                    Packet::from_bytes(&msg.bytes).map_err(|_| ServerError::CoapParsing)?;
                *captured.lock().unwrap() = packet.payload.clone();
                let mut req = CoapRequest::from_packet(packet, msg.addr);
                let response = req.response.as_mut().map(|resp| {
                    resp.set_status(ResponseType::Created);
                    resp.message.clear_all_options();
                    OutboundMessage {
                        addr: msg.addr,
                        bytes: resp.message.to_bytes().unwrap(),
                        encrypted: true,
                    }
                });
                Ok(response)
            })
        }
    }

    fn make_post(payload: &[u8], block1: Option<BlockValue>, mid: u16) -> InboundMessage {
        let mut packet = Packet::new();
        packet.header.code = MessageClass::Request(RequestType::Post);
        packet.header.message_id = mid;
        packet.header.set_type(MessageType::Confirmable);
        packet.add_option(CoapOption::UriPath, b"test".to_vec());
        packet.payload = payload.to_vec();
        if let Some(b) = block1 {
            packet.add_option_as(CoapOption::Block1, b);
        }
        InboundMessage {
            addr: "192.0.2.1:5683".parse().unwrap(),
            bytes: packet.to_bytes().unwrap(),
        }
    }

    fn make_service(max_msg_size: usize) -> BlockService<CreatedService> {
        BlockService::new(
            CreatedService::default(),
            BlockHandlerConfig {
                max_total_message_size: max_msg_size,
                cache_expiry_duration: Duration::from_secs(120),
            },
        )
    }

    fn response_code(resp: &OutboundMessage) -> String {
        Packet::from_bytes(&resp.bytes)
            .unwrap()
            .header
            .get_code()
            .to_string()
    }

    #[tokio::test]
    async fn non_block_request_passes_through() {
        let mut svc = make_service(1152);
        let resp = svc
            .call(make_post(b"</3>;ver=1.0", None, 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response_code(&resp), "2.01");
    }

    #[tokio::test]
    async fn too_large_without_block1_returns_413_with_hint() {
        let mut svc = make_service(32);
        let resp = svc
            .call(make_post(&b"A".repeat(64), None, 2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response_code(&resp), "4.13");

        let p = Packet::from_bytes(&resp.bytes).unwrap();
        assert!(p
            .get_first_option_as::<BlockValue>(CoapOption::Block1)
            .and_then(|x| x.ok())
            .is_some());
    }

    #[tokio::test]
    async fn intermediate_block1_returns_continue() {
        let mut svc = make_service(64);
        let blk = BlockValue::new(0, true, 16).unwrap();
        let resp = svc
            .call(make_post(b"0123456789abcdef", Some(blk), 3))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response_code(&resp), "2.31");
    }

    #[tokio::test]
    async fn final_block_response_echoes_block1_option() {
        let mut svc = make_service(64);

        let blk0 = BlockValue::new(0, true, 16).unwrap();
        let resp0 = svc
            .call(make_post(b"0123456789abcdef", Some(blk0), 10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response_code(&resp0), "2.31");

        let blk1 = BlockValue::new(1, false, 16).unwrap();
        let resp1 = svc
            .call(make_post(b"ghijklmnopqrstuv", Some(blk1), 11))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response_code(&resp1), "2.01");

        // RFC 7959 §2.3: the final response must acknowledge block 1, M=0.
        let ack = Packet::from_bytes(&resp1.bytes)
            .unwrap()
            .get_first_option_as::<BlockValue>(CoapOption::Block1)
            .unwrap()
            .unwrap();
        assert_eq!(ack.num, 1);
        assert!(!ack.more);
    }

    #[tokio::test]
    async fn multi_block_payload_is_reassembled_for_inner_service() {
        let inner = CreatedService::default();
        let captured = Arc::clone(&inner.0);
        let mut svc = BlockService::new(
            inner,
            BlockHandlerConfig {
                max_total_message_size: 64,
                cache_expiry_duration: Duration::from_secs(120),
            },
        );

        let chunks: &[&[u8]] = &[
            b"0123456789abcdef",
            b"ghijklmnopqrstuv",
            b"wxyz012345678901",
        ];
        let expected: Vec<u8> = chunks.concat();

        for (i, chunk) in chunks.iter().enumerate() {
            let is_last = i + 1 == chunks.len();
            let blk = BlockValue::new(i, !is_last, 16).unwrap();
            let resp = svc
                .call(make_post(chunk, Some(blk), (i + 1) as u16))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response_code(&resp), if is_last { "2.01" } else { "2.31" });
        }

        assert_eq!(*captured.lock().unwrap(), expected);
    }

    #[tokio::test]
    async fn inner_none_response_stays_none() {
        #[derive(Clone)]
        struct NoneService;
        impl Service<InboundMessage> for NoneService {
            type Response = Option<OutboundMessage>;
            type Error = ServerError;
            type Future =
                Pin<Box<dyn Future<Output = Result<Option<OutboundMessage>, ServerError>> + Send>>;
            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, _msg: InboundMessage) -> Self::Future {
                Box::pin(async { Ok(None) })
            }
        }

        let mut svc = BlockService::new(NoneService, block_handler_config());
        let resp = svc.call(make_post(b"x", None, 42)).await.unwrap();
        assert!(resp.is_none());
    }
}
