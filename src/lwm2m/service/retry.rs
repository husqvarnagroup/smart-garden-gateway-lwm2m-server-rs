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
