use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::warn;

use crate::registry::DeviceSnapshot;

pub struct PersistenceStore {
    pub dir: PathBuf,
    pub session_to: String,
}

impl PersistenceStore {
    pub fn new(dir: PathBuf, server_uri: &str) -> Self {
        let session_to = extract_ipv6_from_uri(server_uri);
        Self { dir, session_to }
    }

    pub fn save_registry(&self, snapshots: &[DeviceSnapshot]) {
        if let Err(e) = std::fs::create_dir_all(&self.dir) {
            warn!(dir = %self.dir.display(), "persistence: create dir failed: {e}");
            return;
        }
        let clients: Vec<serde_json::Value> = snapshots
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let mut obj_map: HashMap<u32, String> = s.object_versions.clone();
                for obj_str in &s.objects {
                    if let Some(id_str) = obj_str.split('/').next() {
                        if let Ok(id) = id_str.parse::<u32>() {
                            obj_map.entry(id).or_default();
                        }
                    }
                }
                let mut obj_ids: Vec<u32> = obj_map.keys().copied().collect();
                obj_ids.sort();
                let objects: Vec<serde_json::Value> = obj_ids
                    .iter()
                    .map(|id| {
                        let ver = obj_map.get(id).cloned().unwrap_or_default();
                        serde_json::json!({"id": id, "ver": ver, "inst": []})
                    })
                    .collect();

                let (addr_str, port) = match s.addr {
                    std::net::SocketAddr::V6(a) => (a.ip().to_string(), a.port()),
                    std::net::SocketAddr::V4(a) => (a.ip().to_string(), a.port()),
                };

                serde_json::json!({
                    "id": i,
                    "reg_id": s.id,
                    "ep": format!("urn:dev:sg:{}", s.endpoint),
                    "lwm2m": s.lwm2m_version,
                    "b": s.binding_mode,
                    "ct": 112,
                    "lt": s.lifetime,
                    "end_of_life": s.end_of_life,
                    "objects": objects,
                    "session": {
                        "id": [addr_str, port],
                        "to": self.session_to,
                        "tc": 28,
                        "data": {}
                    }
                })
            })
            .collect();

        let doc = serde_json::json!({"file_version": 2, "clients": clients});
        match serde_json::to_vec_pretty(&doc) {
            Ok(json) => {
                let path = self.dir.join("wakaama.json");
                if let Err(e) = write_atomic(&path, &json) {
                    warn!(path = %path.display(), "persistence: save wakaama.json failed: {e}");
                }
            }
            Err(e) => warn!("persistence: serialize wakaama.json failed: {e}"),
        }
    }

    pub fn load_registry(&self) -> Vec<DeviceSnapshot> {
        let path = self.dir.join("wakaama.json");
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let doc: serde_json::Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(e) => {
                warn!("persistence: load wakaama.json failed: {e}");
                return Vec::new();
            }
        };

        let now = unix_now();
        let Some(clients) = doc["clients"].as_array() else {
            return Vec::new();
        };

        let mut snapshots = Vec::new();
        for client in clients {
            let Some(ep_full) = client["ep"].as_str() else { continue };
            let endpoint = ep_full.rfind(':').map(|i| &ep_full[i + 1..]).unwrap_or(ep_full).to_owned();
            let lwm2m_version = client["lwm2m"].as_str().unwrap_or("1.1").to_owned();
            let binding_mode = client["b"].as_str().unwrap_or("").to_owned();
            let end_of_life = client["end_of_life"].as_u64().unwrap_or(0);
            let id = client["reg_id"].as_u64().unwrap_or(0) as u32;

            if end_of_life <= now {
                continue;
            }
            let remaining = (end_of_life - now) as u32;

            let (objects, object_versions) = if let Some(objs) = client["objects"].as_array() {
                let mut obj_list = Vec::new();
                let mut ver_map: HashMap<u32, String> = HashMap::new();
                for obj in objs {
                    let obj_id = obj["id"].as_u64().unwrap_or(0) as u32;
                    let ver = obj["ver"].as_str().unwrap_or("").to_owned();
                    obj_list.push(format!("{}/0", obj_id));
                    if !ver.is_empty() {
                        ver_map.insert(obj_id, ver);
                    }
                }
                (obj_list, ver_map)
            } else {
                (Vec::new(), HashMap::new())
            };

            let addr = if let Some(arr) = client["session"]["id"].as_array() {
                let ip_str = arr.first().and_then(|v| v.as_str()).unwrap_or("::1");
                let port = arr.get(1).and_then(|v| v.as_u64()).unwrap_or(0) as u16;
                let ip: std::net::Ipv6Addr = ip_str.parse().unwrap_or(std::net::Ipv6Addr::LOCALHOST);
                std::net::SocketAddr::V6(std::net::SocketAddrV6::new(ip, port, 0, 0))
            } else {
                std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::LOCALHOST, 0, 0, 0,
                ))
            };

            snapshots.push(DeviceSnapshot {
                id,
                endpoint,
                addr,
                lifetime: remaining,
                objects,
                object_versions,
                lwm2m_version,
                binding_mode,
                end_of_life,
            });
        }
        snapshots
    }

    pub fn save_included(&self, endpoints: &[String]) {
        if let Err(e) = std::fs::create_dir_all(&self.dir) {
            warn!(dir = %self.dir.display(), "persistence: create dir failed: {e}");
            return;
        }
        let path = self.dir.join("included_devices.json");
        match serde_json::to_vec_pretty(endpoints) {
            Ok(json) => {
                if let Err(e) = write_atomic(&path, &json) {
                    warn!(path = %path.display(), "persistence: save included_devices.json failed: {e}");
                }
            }
            Err(e) => warn!("persistence: serialize included_devices.json failed: {e}"),
        }
    }

    pub fn load_included(&self) -> Vec<String> {
        let path = self.dir.join("included_devices.json");
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        serde_json::from_str::<Vec<String>>(&content).unwrap_or_default()
    }

    pub fn save_device_state(&self, endpoint: &str, state: &serde_json::Value) {
        let devices_dir = self.dir.join("devices");
        if let Err(e) = std::fs::create_dir_all(&devices_dir) {
            warn!("persistence: create devices dir failed: {e}");
            return;
        }
        let path = devices_dir.join(format!("{endpoint}.json"));
        match serde_json::to_vec_pretty(state) {
            Ok(json) => {
                if let Err(e) = write_atomic(&path, &json) {
                    warn!(endpoint, "persistence: save device state failed: {e}");
                }
            }
            Err(e) => warn!(endpoint, "persistence: serialize device state failed: {e}"),
        }
    }

    pub fn delete_device_state(&self, endpoint: &str) {
        let path = self.dir.join("devices").join(format!("{endpoint}.json"));
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(endpoint, "persistence: delete device state failed: {e}");
            }
        }
    }

    pub fn load_all_device_states(&self) -> HashMap<String, serde_json::Value> {
        let devices_dir = self.dir.join("devices");
        let mut states = HashMap::new();
        let entries = match std::fs::read_dir(&devices_dir) {
            Ok(e) => e,
            Err(_) => return states,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) if !s.is_empty() => s.to_owned(),
                _ => continue,
            };
            let content = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(_) => continue,
            };
            match serde_json::from_str::<serde_json::Value>(&content) {
                Ok(v) => { states.insert(stem, v); }
                Err(e) => warn!(path = %path.display(), "persistence: load device state failed: {e}"),
            }
        }
        states
    }
}

fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)
}

fn extract_ipv6_from_uri(uri: &str) -> String {
    // coap://[fc00::6:100:0:0] → fc00::6:100:0:0
    if let (Some(start), Some(end)) = (uri.find('['), uri.find(']')) {
        return uri[start + 1..end].to_owned();
    }
    uri.trim_start_matches("coap://").to_owned()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
    use tempfile::TempDir;

    fn make_store(tmp: &TempDir) -> PersistenceStore {
        PersistenceStore::new(tmp.path().to_path_buf(), "coap://[fc00::6:100:0:0]")
    }

    fn ipv6_addr(ip: &str, port: u16) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(ip.parse::<Ipv6Addr>().unwrap(), port, 0, 0))
    }

    #[test]
    fn registry_round_trip() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);
        let now = unix_now();

        let mut obj_versions = HashMap::new();
        obj_versions.insert(3u32, "1.0".to_owned());
        obj_versions.insert(4u32, "1.2".to_owned());

        let snapshot = DeviceSnapshot {
            id: 42,
            endpoint: "3034F8319C00754000000097".to_owned(),
            addr: ipv6_addr("fc00::6:94bb:aec0:1f6", 20192),
            lifetime: 86400,
            objects: vec!["3/0".to_owned(), "4/0".to_owned()],
            object_versions: obj_versions.clone(),
            lwm2m_version: "1.1".to_owned(),
            binding_mode: "U".to_owned(),
            end_of_life: now + 86400,
        };

        store.save_registry(&[snapshot]);
        let loaded = store.load_registry();

        assert_eq!(loaded.len(), 1);
        let s = &loaded[0];
        assert_eq!(s.id, 42);
        assert_eq!(s.endpoint, "3034F8319C00754000000097");
        assert_eq!(s.lwm2m_version, "1.1");
        assert_eq!(s.binding_mode, "U");
        assert_eq!(s.object_versions, obj_versions);
        // Remaining lifetime shrinks by at most a few seconds during the test.
        assert!(s.lifetime <= 86400 && s.lifetime >= 86395);
        let SocketAddr::V6(a) = s.addr else { panic!("expected V6 addr") };
        assert_eq!(a.port(), 20192);
        assert_eq!(a.ip().to_string(), "fc00::6:94bb:aec0:1f6");
    }

    #[test]
    fn registry_skips_expired() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);
        let now = unix_now();

        let snapshots = vec![
            DeviceSnapshot {
                id: 1,
                endpoint: "EXPIRED".to_owned(),
                addr: ipv6_addr("::1", 1),
                lifetime: 0,
                objects: vec![],
                object_versions: HashMap::new(),
                lwm2m_version: "1.0".to_owned(),
                binding_mode: "".to_owned(),
                end_of_life: now - 1,
            },
            DeviceSnapshot {
                id: 2,
                endpoint: "VALID".to_owned(),
                addr: ipv6_addr("::1", 2),
                lifetime: 3600,
                objects: vec![],
                object_versions: HashMap::new(),
                lwm2m_version: "1.0".to_owned(),
                binding_mode: "".to_owned(),
                end_of_life: now + 3600,
            },
        ];

        store.save_registry(&snapshots);
        let loaded = store.load_registry();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].endpoint, "VALID");
    }

    #[test]
    fn included_round_trip() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);

        let endpoints = vec![
            "3034F8319C00754000000097".to_owned(),
            "AABBCCDD00112233".to_owned(),
        ];
        store.save_included(&endpoints);
        let loaded = store.load_included();

        assert_eq!(loaded, endpoints);
    }

    #[test]
    fn device_state_round_trip() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);

        let state = serde_json::json!({
            "irrigation_control": {
                "_urn": "urn:oma:lwm2m:x:28152:0.2",
                "0": {
                    "error": {"vi": 0, "ts": 1234567890_u64}
                }
            }
        });
        store.save_device_state("3034F8319C00754000000097", &state);

        let all = store.load_all_device_states();
        assert_eq!(all.len(), 1);
        assert_eq!(all["3034F8319C00754000000097"], state);
    }

    #[test]
    fn device_state_multiple_devices() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);

        let s1 = serde_json::json!({"a": 1});
        let s2 = serde_json::json!({"b": 2});
        store.save_device_state("DEV1", &s1);
        store.save_device_state("DEV2", &s2);

        let all = store.load_all_device_states();
        assert_eq!(all.len(), 2);
        assert_eq!(all["DEV1"], s1);
        assert_eq!(all["DEV2"], s2);
    }

    #[test]
    fn session_to_extracted_from_uri() {
        assert_eq!(
            make_store(&TempDir::new().unwrap()).session_to,
            "fc00::6:100:0:0"
        );
        assert_eq!(
            PersistenceStore::new(PathBuf::from("/tmp"), "coap://[::1]").session_to,
            "::1"
        );
    }

    #[test]
    fn load_registry_missing_file_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);
        assert!(store.load_registry().is_empty());
    }

    #[test]
    fn load_included_missing_file_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);
        assert!(store.load_included().is_empty());
    }

    #[test]
    fn load_device_states_missing_dir_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp);
        assert!(store.load_all_device_states().is_empty());
    }
}
