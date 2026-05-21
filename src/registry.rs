use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::model::{Device, DeviceId, LwM2mError, PendingOperation};

pub struct DeviceSnapshot {
    pub id: DeviceId,
    pub endpoint: String,
    pub addr: SocketAddr,
    pub lifetime: u32,
    pub objects: Vec<String>,
    pub object_versions: HashMap<u32, String>,
    pub lwm2m_version: String,
    pub binding_mode: String,
    /// Unix timestamp when the registration expires.
    pub end_of_life: u64,
}

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
    #[allow(clippy::too_many_arguments)]
    pub async fn register(
        &self,
        endpoint: String,
        addr: SocketAddr,
        lifetime: u32,
        objects: Vec<String>,
        object_versions: HashMap<u32, String>,
        lwm2m_version: String,
        binding_mode: String,
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
            dev.lwm2m_version = lwm2m_version;
            dev.binding_mode = binding_mode;
            let now = std::time::Instant::now();
            dev.registered_at = now;
            dev.last_contact = now;
            info!(endpoint, id, "device re-registered");
            return id;
        }

        let id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1).max(1);

        let dev = Device::new(id, endpoint.clone(), addr, lifetime, objects, object_versions, lwm2m_version, binding_mode);
        inner.by_id.insert(id, dev);
        inner.by_endpoint.insert(endpoint.clone(), id);
        inner.by_addr.insert(addr, id);

        info!(endpoint, id, "device registered");
        id
    }

    /// Update last-contact timestamp. Returns the endpoint if the device just came online.
    pub async fn touch(&self, addr: SocketAddr) -> Option<String> {
        let mut inner = self.inner.write().await;
        let id = *inner.by_addr.get(&addr)?;
        let dev = inner.by_id.get_mut(&id).unwrap();
        dev.last_contact = std::time::Instant::now();
        dev.addr = addr;
        if dev.online != Some(true) {
            dev.online = Some(true);
            Some(dev.endpoint.clone())
        } else {
            None
        }
    }

    /// Mark device offline. Returns the endpoint if it was previously online.
    pub async fn set_device_offline(&self, addr: SocketAddr) -> Option<String> {
        let mut inner = self.inner.write().await;
        let id = *inner.by_addr.get(&addr)?;
        let dev = inner.by_id.get_mut(&id)?;
        if dev.online == Some(true) {
            dev.online = Some(false);
            Some(dev.endpoint.clone())
        } else {
            None
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

    /// Return true if an in-flight operation with this token is still pending (non-destructive).
    pub async fn is_in_flight(&self, addr: SocketAddr, token: &[u8; 8]) -> bool {
        let inner = self.inner.read().await;
        let Some(id) = inner.by_addr.get(&addr) else { return false; };
        inner.by_id[id].in_flight.contains_key(token)
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

    /// Look up a device by endpoint name, returning its address, registry ID, and object versions.
    pub async fn addr_and_id_by_endpoint(&self, endpoint: &str) -> Option<(SocketAddr, DeviceId, HashMap<u32, String>)> {
        let inner = self.inner.read().await;
        let &id = inner.by_endpoint.get(endpoint)?;
        let dev = &inner.by_id[&id];
        Some((dev.addr, id, dev.object_versions.clone()))
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

    /// Snapshot all registered devices for persistence.
    pub async fn snapshot(&self) -> Vec<DeviceSnapshot> {
        let inner = self.inner.read().await;
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        inner.by_id.values().map(|dev| {
            let elapsed = dev.last_contact.elapsed().as_secs();
            let remaining = dev.lifetime.saturating_sub(elapsed as u32);
            DeviceSnapshot {
                id: dev.id,
                endpoint: dev.endpoint.clone(),
                addr: dev.addr,
                lifetime: remaining,
                objects: dev.objects.clone(),
                object_versions: dev.object_versions.clone(),
                lwm2m_version: dev.lwm2m_version.clone(),
                binding_mode: dev.binding_mode.clone(),
                end_of_life: now_unix + remaining as u64,
            }
        }).collect()
    }

    /// Restore devices from persisted snapshots.
    /// Restored devices start as offline; the first contact triggers an online event.
    pub async fn restore(&self, snapshots: Vec<DeviceSnapshot>) {
        let mut inner = self.inner.write().await;
        for s in snapshots {
            if s.id >= inner.next_id {
                inner.next_id = s.id + 1;
            }
            let dev = crate::model::Device {
                id: s.id,
                endpoint: s.endpoint.clone(),
                addr: s.addr,
                lifetime: s.lifetime,
                registered_at: std::time::Instant::now(),
                last_contact: std::time::Instant::now(),
                objects: s.objects,
                object_versions: s.object_versions,
                lwm2m_version: s.lwm2m_version,
                binding_mode: s.binding_mode,
                state: serde_json::Value::Object(serde_json::Map::new()),
                online: Some(false),
                pending_ops: std::collections::VecDeque::new(),
                in_flight: std::collections::HashMap::new(),
            };
            inner.by_endpoint.insert(s.endpoint, s.id);
            inner.by_addr.insert(s.addr, s.id);
            inner.by_id.insert(s.id, dev);
        }
    }

    /// Deep-merge IPSO state into the device at `addr`. Returns the full merged state.
    pub async fn merge_device_state_by_addr(
        &self,
        addr: SocketAddr,
        new_state: serde_json::Value,
    ) -> serde_json::Value {
        let mut inner = self.inner.write().await;
        let Some(&id) = inner.by_addr.get(&addr) else { return new_state; };
        let dev = inner.by_id.get_mut(&id).unwrap();
        json_merge(&mut dev.state, new_state);
        dev.state.clone()
    }

    /// Remove a device by socket address. Returns the endpoint name if found.
    /// In-flight and pending ops have their response channels fired with Timeout.
    pub async fn remove_by_addr(&self, addr: SocketAddr) -> Option<String> {
        let mut inner = self.inner.write().await;
        let id = *inner.by_addr.get(&addr)?;
        let dev = inner.by_id.remove(&id)?;
        inner.by_endpoint.remove(&dev.endpoint);
        inner.by_addr.remove(&addr);
        for op in dev.pending_ops {
            let _ = op.response_tx.send(Err(LwM2mError::Timeout));
        }
        for (_, op) in dev.in_flight {
            let _ = op.response_tx.send(Err(LwM2mError::Timeout));
        }
        Some(dev.endpoint)
    }

    /// Return the accumulated IPSO state for a device identified by endpoint name.
    pub async fn device_state_by_endpoint(&self, endpoint: &str) -> Option<serde_json::Value> {
        let inner = self.inner.read().await;
        let &id = inner.by_endpoint.get(endpoint)?;
        Some(inner.by_id[&id].state.clone())
    }

    /// Restore persisted IPSO state for a device identified by endpoint name.
    pub async fn restore_device_state(&self, endpoint: &str, state: serde_json::Value) {
        let mut inner = self.inner.write().await;
        let Some(&id) = inner.by_endpoint.get(endpoint) else { return; };
        if let Some(dev) = inner.by_id.get_mut(&id) {
            dev.state = state;
        }
    }
}

fn json_merge(target: &mut serde_json::Value, source: serde_json::Value) {
    if let (Some(t), serde_json::Value::Object(s)) = (target.as_object_mut(), source) {
        for (k, v) in s {
            let entry = t.entry(k).or_insert(serde_json::Value::Null);
            if entry.is_object() && v.is_object() {
                json_merge(entry, v);
            } else {
                *entry = v;
            }
        }
    }
}
