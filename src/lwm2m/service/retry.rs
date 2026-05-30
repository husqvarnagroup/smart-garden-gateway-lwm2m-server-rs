//! LwM2M 1.1 / RFC 7252 §4.2 exponential-backoff retry policy.

use std::{future::Future, pin::Pin, time::Duration};

use rand_core::RngCore;

use super::{CoapRequest, CoapResponse, ServiceError};

/// ACK_TIMEOUT: The initial base timeout before retransmission (seconds).
const ACK_TIMEOUT: f64 = 2.0;
/// ACK_RANDOM_FACTOR: Multiplied with ACK_TIMEOUT to get the upper bound
/// of the initial random timeout window.
const ACK_RANDOM_FACTOR: f64 = 1.5;
/// MAX_RETRANSMIT: Maximum number of retransmissions before giving up.
const MAX_RETRANSMIT: u32 = 4;

/// Tower retry policy implementing LwM2M 1.1 (RFC 7252 §4.2) exponential
/// backoff for CoAP Confirmable message retransmission.
///
/// The initial timeout is chosen randomly from \[ACK_TIMEOUT, ACK_TIMEOUT × ACK_RANDOM_FACTOR\]
/// (i.e. 2–3 s). Each subsequent retransmission doubles the previous timeout.
/// After MAX_RETRANSMIT (4) failed attempts the request is abandoned.
#[derive(Debug, Clone)]
pub struct CoapRetryPolicy {
    /// Number of retries already attempted.
    attempts: u32,
    /// Current backoff duration (doubles each retry).
    current_backoff: Duration,
}

impl CoapRetryPolicy {
    /// Create a new policy with a randomised initial backoff per RFC 7252.
    pub fn new() -> Self {
        let jitter = rand_core::OsRng.next_u32() as f64 / u32::MAX as f64;
        let initial = ACK_TIMEOUT + jitter * (ACK_TIMEOUT * (ACK_RANDOM_FACTOR - 1.0));
        Self {
            attempts: 0,
            current_backoff: Duration::from_secs_f64(initial),
        }
    }
}

impl Default for CoapRetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl tower::retry::Policy<CoapRequest, Option<CoapResponse>, ServiceError> for CoapRetryPolicy {
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        _req: &mut CoapRequest,
        result: &mut Result<Option<CoapResponse>, ServiceError>,
    ) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            Err(e) => {
                if !is_transient(e) {
                    return None;
                }

                if self.attempts >= MAX_RETRANSMIT {
                    tracing::warn!(
                        attempts = self.attempts,
                        "CoAP retransmission limit reached, giving up"
                    );
                    return None;
                }

                let backoff = self.current_backoff;
                self.attempts += 1;
                self.current_backoff = self
                    .current_backoff
                    .saturating_mul(2)
                    .min(Duration::from_secs(32));

                tracing::debug!(
                    attempt = self.attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    "Retrying CoAP request after backoff"
                );

                Some(Box::pin(async move {
                    tokio::time::sleep(backoff).await;
                }))
            }
        }
    }

    fn clone_request(&mut self, req: &CoapRequest) -> Option<CoapRequest> {
        Some(req.clone())
    }
}

/// Determine whether an error is transient and worth retrying.
fn is_transient(e: &crate::error::Error) -> bool {
    matches!(e, crate::error::Error::Io(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Instant;

    use coap_lite::{MessageClass, MessageType, Packet, RequestType as Method};
    use tower::retry::Policy;

    fn make_request() -> CoapRequest {
        let mut pkt = Packet::new();
        pkt.header.set_type(MessageType::Confirmable);
        pkt.header.code = MessageClass::Request(Method::Post);
        pkt.header.message_id = 1;
        CoapRequest {
            packet: pkt,
            addr: "[::1]:5683".parse().unwrap(),
        }
    }

    fn io_error() -> ServiceError {
        Arc::new(crate::error::Error::Io(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        )))
    }

    fn non_transient_error() -> ServiceError {
        Arc::new(crate::error::Error::Coap("bad packet".into()))
    }

    #[test]
    fn initial_backoff_within_rfc7252_range() {
        // RFC 7252: initial timeout ∈ [ACK_TIMEOUT, ACK_TIMEOUT * ACK_RANDOM_FACTOR]
        // = [2.0, 3.0] seconds
        for _ in 0..100 {
            let policy = CoapRetryPolicy::new();
            let ms = policy.current_backoff.as_millis();
            assert!(
                ms >= 2000 && ms <= 3000,
                "initial backoff {ms} ms outside [2000, 3000]"
            );
        }
    }

    #[test]
    fn no_retry_on_success() {
        let mut policy = CoapRetryPolicy::new();
        let mut req = make_request();
        let mut result: Result<Option<CoapResponse>, ServiceError> = Ok(None);
        assert!(policy.retry(&mut req, &mut result).is_none());
    }

    #[test]
    fn no_retry_on_non_transient_error() {
        let mut policy = CoapRetryPolicy::new();
        let mut req = make_request();
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(non_transient_error());
        assert!(policy.retry(&mut req, &mut result).is_none());
    }

    #[test]
    fn retries_on_io_error() {
        let mut policy = CoapRetryPolicy::new();
        let mut req = make_request();
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
        assert!(policy.retry(&mut req, &mut result).is_some());
    }

    #[test]
    fn stops_after_max_retransmit() {
        let mut policy = CoapRetryPolicy::new();
        let mut req = make_request();

        for i in 0..MAX_RETRANSMIT {
            let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
            assert!(
                policy.retry(&mut req, &mut result).is_some(),
                "expected retry on attempt {i}"
            );
        }

        // Next attempt should give up.
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
        assert!(
            policy.retry(&mut req, &mut result).is_none(),
            "expected no retry after MAX_RETRANSMIT ({MAX_RETRANSMIT}) attempts"
        );
    }

    #[test]
    fn backoff_doubles_each_retry() {
        let mut policy = CoapRetryPolicy::new();
        let initial = policy.current_backoff;
        let mut req = make_request();

        // First retry — backoff should be `initial`, next should be 2×initial.
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
        policy.retry(&mut req, &mut result);
        assert_eq!(policy.current_backoff, initial * 2);

        // Second retry — should double again.
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
        policy.retry(&mut req, &mut result);
        assert_eq!(policy.current_backoff, initial * 4);

        // Third retry — should double again.
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
        policy.retry(&mut req, &mut result);
        assert_eq!(policy.current_backoff, initial * 8);
    }

    #[test]
    fn backoff_capped_at_32_seconds() {
        let mut policy = CoapRetryPolicy {
            attempts: 0,
            current_backoff: Duration::from_secs(20),
        };
        let mut req = make_request();

        // After retry, 20*2 = 40 should be capped to 32.
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
        policy.retry(&mut req, &mut result);
        assert_eq!(policy.current_backoff, Duration::from_secs(32));

        // Already at cap — should stay at 32.
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
        policy.retry(&mut req, &mut result);
        assert_eq!(policy.current_backoff, Duration::from_secs(32));
    }

    #[test]
    fn clone_request_succeeds() {
        let mut policy = CoapRetryPolicy::new();
        let req = make_request();
        let cloned = policy.clone_request(&req);
        assert!(cloned.is_some());
        let cloned = cloned.unwrap();
        assert_eq!(cloned.addr, req.addr);
        assert_eq!(
            cloned.packet.header.message_id,
            req.packet.header.message_id
        );
    }

    #[test]
    fn attempts_counter_increments() {
        let mut policy = CoapRetryPolicy::new();
        let mut req = make_request();
        assert_eq!(policy.attempts, 0);

        for expected in 1..=MAX_RETRANSMIT {
            let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());
            policy.retry(&mut req, &mut result);
            assert_eq!(policy.attempts, expected);
        }
    }

    #[tokio::test]
    async fn retry_future_sleeps_for_backoff_duration() {
        let mut policy = CoapRetryPolicy {
            attempts: 0,
            current_backoff: Duration::from_millis(50),
        };
        let mut req = make_request();
        let mut result: Result<Option<CoapResponse>, ServiceError> = Err(io_error());

        let fut = policy.retry(&mut req, &mut result).expect("should retry");
        let start = Instant::now();
        fut.await;
        let elapsed = start.elapsed();

        // Should have slept ~50ms (allow some slack for scheduling).
        assert!(
            elapsed >= Duration::from_millis(40),
            "elapsed {elapsed:?} too short"
        );
        assert!(
            elapsed < Duration::from_millis(200),
            "elapsed {elapsed:?} too long"
        );
    }

    #[test]
    fn is_transient_classifies_io_errors() {
        let io_err = crate::error::Error::Io(io::Error::new(io::ErrorKind::Other, "test"));
        assert!(is_transient(&io_err));
    }

    #[test]
    fn is_transient_rejects_coap_errors() {
        let coap_err = crate::error::Error::Coap("decode failure".into());
        assert!(!is_transient(&coap_err));
    }

    #[test]
    fn is_transient_rejects_config_errors() {
        let cfg_err = crate::error::Error::Config("bad config".into());
        assert!(!is_transient(&cfg_err));
    }

    #[test]
    fn is_transient_rejects_bootstrap_errors() {
        let bs_err = crate::error::Error::Bootstrap("cert invalid".into());
        assert!(!is_transient(&bs_err));
    }

    #[test]
    fn is_transient_rejects_json_errors() {
        let json_err: crate::error::Error = serde_json::from_str::<serde_json::Value>("{{")
            .unwrap_err()
            .into();
        assert!(!is_transient(&json_err));
    }
}
