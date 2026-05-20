use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
};

use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::model::{Device, DeviceId, LwM2mError, PendingOperation};

struct RegistryInner {
    by_id: HashMap<DeviceId, Device>,
    by_endpoint: HashMap<String, DeviceId>,
    by_addr: HashMap<SocketAddr, DeviceId>,
    next_id: DeviceId,
}

#[derive(Clone)]
pub struct DeviceRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

impl Default for DeviceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DeviceRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                by_id: HashMap::new(),
                by_endpoint: HashMap::new(),
                by_addr: HashMap::new(),
                next_id: 1,
            })),
        }
    }

    /// Register a new device or refresh an existing registration.
    /// Returns the assigned DeviceId.
    pub async fn register(
        &self,
        endpoint: String,
        addr: SocketAddr,
        lifetime: u32,
        objects: Vec<String>,
        object_versions: HashMap<u32, String>,
    ) -> DeviceId {
        let mut inner = self.inner.write().await;

        if let Some(&id) = inner.by_endpoint.get(&endpoint) {
            // Address may have changed on PPP re-connect — update addr index first.
            let old_addr = inner.by_id[&id].addr;
            if old_addr != addr {
                inner.by_addr.remove(&old_addr);
                inner.by_addr.insert(addr, id);
            }
            let dev = inner.by_id.get_mut(&id).unwrap();
            dev.addr = addr;
            dev.lifetime = lifetime;
            dev.objects = objects;
            dev.object_versions = object_versions;
            let now = std::time::Instant::now();
            dev.registered_at = now;
            dev.last_contact = now;
            info!(endpoint, id, "device re-registered");
            return id;
        }

        let id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1).max(1);

        let dev = Device::new(id, endpoint.clone(), addr, lifetime, objects, object_versions);
        inner.by_id.insert(id, dev);
        inner.by_endpoint.insert(endpoint.clone(), id);
        inner.by_addr.insert(addr, id);

        info!(endpoint, id, "device registered");
        id
    }

    /// Update last-contact timestamp and address (called on any inbound CoAP).
    pub async fn touch(&self, addr: SocketAddr) {
        let mut inner = self.inner.write().await;
        if let Some(&id) = inner.by_addr.get(&addr) {
            let dev = inner.by_id.get_mut(&id).unwrap();
            dev.last_contact = std::time::Instant::now();
            // addr won't differ here since we looked up by addr, but guard for safety.
            dev.addr = addr;
        }
    }

    /// Drain all pending operations for the device at `addr`.
    /// Returns the ops and the device's current socket address.
    pub async fn drain_pending(&self, addr: SocketAddr) -> Vec<PendingOperation> {
        let mut inner = self.inner.write().await;
        let Some(&id) = inner.by_addr.get(&addr) else {
            return Vec::new();
        };
        let dev = inner.by_id.get_mut(&id).unwrap();
        dev.pending_ops.drain(..).collect()
    }

    /// Move an operation to the in-flight map (after it has been sent).
    pub async fn place_in_flight(&self, addr: SocketAddr, token: [u8; 8], op: PendingOperation) {
        let mut inner = self.inner.write().await;
        if let Some(&id) = inner.by_addr.get(&addr) {
            if let Some(dev) = inner.by_id.get_mut(&id) {
                dev.in_flight.insert(token, op);
            }
        }
    }

    /// Remove an in-flight operation by token (CoAP response received).
    /// Returns the op so the caller can fire its response channel.
    pub async fn complete_in_flight(&self, addr: SocketAddr, token: &[u8; 8]) -> Option<PendingOperation> {
        let mut inner = self.inner.write().await;
        let id = *inner.by_addr.get(&addr)?;
        inner.by_id.get_mut(&id)?.in_flight.remove(token)
    }

    /// Expire stale device registrations. Returns endpoints that were removed.
    /// In-flight and pending ops have their response channels fired with Timeout.
    pub async fn expire_stale(&self, grace_secs: u32) -> Vec<String> {
        let mut inner = self.inner.write().await;
        let expired: Vec<DeviceId> = inner
            .by_id
            .values()
            .filter(|d| d.is_expired(grace_secs))
            .map(|d| d.id)
            .collect();

        let mut removed = Vec::new();
        for id in expired {
            if let Some(dev) = inner.by_id.remove(&id) {
                warn!(endpoint = %dev.endpoint, "registration expired, removing device");
                inner.by_endpoint.remove(&dev.endpoint);
                inner.by_addr.remove(&dev.addr);

                for op in dev.pending_ops {
                    let _ = op.response_tx.send(Err(LwM2mError::Timeout));
                }
                for (_, op) in dev.in_flight {
                    let _ = op.response_tx.send(Err(LwM2mError::Timeout));
                }
                removed.push(dev.endpoint);
            }
        }
        removed
    }

    /// Return the endpoint name registered for this address, if any.
    pub async fn endpoint_by_addr(&self, addr: SocketAddr) -> Option<String> {
        let inner = self.inner.read().await;
        let id = inner.by_addr.get(&addr)?;
        Some(inner.by_id[id].endpoint.clone())
    }

    /// Return the object versions registered for the device at this address.
    pub async fn object_versions_by_addr(&self, addr: SocketAddr) -> Option<HashMap<u32, String>> {
        let inner = self.inner.read().await;
        let id = inner.by_addr.get(&addr)?;
        Some(inner.by_id[id].object_versions.clone())
    }

    /// Timeout in-flight operations that have exceeded `max_secs`.
    pub async fn timeout_in_flight(&self, max_secs: u64) {
        let mut inner = self.inner.write().await;
        for dev in inner.by_id.values_mut() {
            let timed_out: Vec<[u8; 8]> = dev
                .in_flight
                .iter()
                .filter(|(_, op)| op.created_at.elapsed().as_secs() > max_secs)
                .map(|(token, _)| *token)
                .collect();

            for token in timed_out {
                if let Some(op) = dev.in_flight.remove(&token) {
                    warn!(endpoint = %dev.endpoint, op_id = op.id, "in-flight op timed out");
                    let _ = op.response_tx.send(Err(LwM2mError::Timeout));
                }
            }
        }
    }


}
