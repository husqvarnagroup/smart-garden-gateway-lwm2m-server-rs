use lwm2m_registry::Registry;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};
use tracing::{info, warn};

/// A shared, hot-reloadable IPSO model. Inner `Arc<IpsoModel>` is swapped
/// atomically on SIGHUP; the write lock is held only during the swap.
pub type SharedIpso = Arc<RwLock<Arc<IpsoModel>>>;

/// Construct a freshly loaded [`SharedIpso`] from the given directories.
pub async fn load_shared(dirs: &[impl AsRef<Path>]) -> SharedIpso {
    Arc::new(RwLock::new(Arc::new(IpsoModel::load_dirs(dirs).await)))
}

// ── Public types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum ResourceType {
    Integer,
    Float,
    String,
    Boolean,
    Time,
    Opaque,
    CoreLink,
    UnsignedInteger,
}

#[derive(Debug, Clone)]
pub struct ResourceDef {
    pub name: String,
    pub resource_type: ResourceType,
    pub multiple_instances: bool,
}

#[derive(Debug, Clone)]
pub struct ObjectDef {
    pub name: String,
    pub urn: String,
    pub resources: HashMap<u32, ResourceDef>,
}

/// In-memory IPSO object model loaded from XML files at startup.
///
/// Keyed by `(object_id, version)` where `version` is the `<ObjectVersion>`
/// element value from the XML (e.g. `"1.1"`), defaulting to `"1.0"` when
/// absent.
#[derive(Debug, Clone, Default)]
pub struct IpsoModel {
    // object_id → version_string → ObjectDef
    objects: HashMap<u32, HashMap<String, ObjectDef>>,
}

impl IpsoModel {
    /// Load all `*.xml` files from each directory in `dirs`.
    /// Logs warnings for unreadable directories or unparsable files.
    pub async fn load_dirs(dirs: &[impl AsRef<Path>]) -> Self {
        let mut model = IpsoModel::default();
        for dir in dirs {
            let dir = dir.as_ref();
            match Registry::init(vec![dir.to_path_buf()]).await {
                Ok(registry) => {
                    for object in registry.objects {
                        let (id, version, def) = convert_object(object);
                        model.objects.entry(id).or_default().insert(version, def);
                    }
                }
                Err(e) => warn!(dir = %dir.display(), "Cannot read IPSO directory: {e}"),
            }
        }
        let count: usize = model.objects.values().map(|v| v.len()).sum();
        info!(objects = count, "IPSO model loaded");
        model
    }

    /// Look up an object definition by id and optional version string.
    ///
    /// Falls back to any known version of the object when `version` is
    /// `None` or has no exact match.
    pub fn get_versioned(&self, object_id: u32, version: Option<&str>) -> Option<&ObjectDef> {
        let versions = self.objects.get(&object_id)?;
        version
            .and_then(|ver| versions.get(ver))
            .or_else(|| versions.values().next())
    }

    /// Find the numeric object ID for a given snake_case object name.
    /// Scans all versions; returns the first match.
    pub fn object_id_by_name(&self, name: &str) -> Option<u32> {
        self.objects
            .iter()
            .find(|(_, versions)| versions.values().any(|def| def.name == name))
            .map(|(&id, _)| id)
    }

    /// Find the numeric resource ID for a resource name within a given object,
    /// using the version resolution order of `get_versioned`.
    pub fn resource_id_by_name(
        &self,
        object_id: u32,
        resource_name: &str,
        version: Option<&str>,
    ) -> Option<u32> {
        let def = self.get_versioned(object_id, version)?;
        def.resources
            .iter()
            .find(|(_, r)| r.name == resource_name)
            .map(|(&id, _)| id)
    }
}

// ── Spec model conversion (parsing delegated to lwm2m-registry) ──────────────

fn convert_object(object: lwm2m_registry::Object) -> (u32, String, ObjectDef) {
    let resources = object
        .resources
        .into_iter()
        .map(|r| {
            (
                u32::from(r.id),
                ResourceDef {
                    name: to_snake_case(&r.name),
                    resource_type: convert_resource_type(r.resource_type),
                    multiple_instances: r.has_multiple_instances,
                },
            )
        })
        .collect();
    (
        u32::from(object.object_id),
        object.object_version.to_string(),
        ObjectDef {
            name: to_snake_case(&object.name),
            urn: object.object_urn,
            resources,
        },
    )
}

fn convert_resource_type(resource_type: lwm2m_registry::ResourceType) -> ResourceType {
    use lwm2m_registry::ResourceType as Rt;
    match resource_type {
        Rt::Integer => ResourceType::Integer,
        Rt::Float => ResourceType::Float,
        Rt::String => ResourceType::String,
        Rt::Boolean => ResourceType::Boolean,
        Rt::Time => ResourceType::Time,
        Rt::Opaque => ResourceType::Opaque,
        Rt::Corelink => ResourceType::CoreLink,
        Rt::UnsignedInteger => ResourceType::UnsignedInteger,
        // No local equivalent; fall back to Integer like unknown types before
        Rt::ObjectLink | Rt::Other => ResourceType::Integer,
    }
}

/// Normalise an IPSO name to snake_case.
///
/// Handles two formats found in IPSO XML files:
/// - CamelCase object names: `IrrigationControl`  → `irrigation_control`
/// - Title-case resource names: `Available Power Sources` → `available_power_sources`
/// - Acronyms: `UTC Offset` → `utc_offset`, `SMNC` → `smnc`
/// - Acronym followed by word: `MeasureRFLink` → `measure_rf_link`
///
/// Insert `_` before uppercase X when:
///   (a) previous char was lowercase, OR
///   (b) previous char was uppercase AND next char is lowercase (end of acronym run)
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    let chars: Vec<char> = s.chars().collect();
    let mut prev_lower = false;
    let mut prev_upper = false;
    for (i, &ch) in chars.iter().enumerate() {
        if ch == ' ' || ch == '-' {
            if !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
            prev_lower = false;
            prev_upper = false;
        } else if ch.is_uppercase() {
            let next_lower = chars.get(i + 1).is_some_and(|c| c.is_lowercase());
            if prev_lower || (prev_upper && next_lower) {
                out.push('_');
            }
            for c in ch.to_lowercase() {
                out.push(c);
            }
            prev_lower = false;
            prev_upper = true;
        } else {
            out.push(ch);
            prev_lower = ch.is_lowercase();
            prev_upper = false;
        }
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use lwm2m_registry::{Object, Operations, Resource, Version};

    fn object(
        name: &str,
        id: u16,
        urn: &str,
        version: Version,
        resources: Vec<Resource>,
    ) -> Object {
        Object {
            name: name.to_owned(),
            object_id: id,
            object_urn: urn.to_owned(),
            object_version: version,
            lwm2m_version: Version::new(1, 1),
            has_multiple_instances: false,
            is_mandatory: false,
            resources,
        }
    }

    #[test]
    fn convert_irrigation_control() {
        let (id, version, obj) = convert_object(object(
            "IrrigationControl",
            28152,
            "urn:oma:lwm2m:x:28152:0.2",
            Version::new(0, 2),
            vec![Resource::new(
                1,
                "error".to_owned(),
                Operations::Read,
                false,
                true,
                lwm2m_registry::ResourceType::Integer,
            )],
        ));
        assert_eq!(id, 28152);
        assert_eq!(version, "0.2");
        assert_eq!(obj.name, "irrigation_control");
        assert_eq!(obj.urn, "urn:oma:lwm2m:x:28152:0.2");
        let res = obj.resources.get(&1).expect("resource 1");
        assert_eq!(res.name, "error");
        assert_eq!(res.resource_type, ResourceType::Integer);
        assert!(!res.multiple_instances);
    }

    #[test]
    fn convert_execute_resource_unspecified_type() {
        // Execute resources have an empty <Type>, parsed by lwm2m-registry
        // as `Other`; conversion falls back to Integer.
        let (id, _, obj) = convert_object(object(
            "SG Common",
            28183,
            "urn:oma:lwm2m:x:28183:0.1",
            Version::new(0, 1),
            vec![Resource::new(
                33,
                "measure_rf_link".to_owned(),
                Operations::Execute,
                false,
                true,
                lwm2m_registry::ResourceType::Other,
            )],
        ));
        assert_eq!(id, 28183);
        assert_eq!(obj.name, "sg_common");
        let res = obj.resources.get(&33).expect("resource 33");
        assert_eq!(res.name, "measure_rf_link");
        assert_eq!(res.resource_type, ResourceType::Integer);
    }

    #[test]
    fn snake_case_conversion() {
        // CamelCase object names
        assert_eq!(to_snake_case("IrrigationControl"), "irrigation_control");
        assert_eq!(to_snake_case("Device"), "device");
        assert_eq!(
            to_snake_case("ConnectivityMonitoring"),
            "connectivity_monitoring"
        );
        assert_eq!(to_snake_case("SgCommon"), "sg_common");
        assert_eq!(to_snake_case("MasterChannel"), "master_channel");
        // Title-case resource names (spaces → underscores)
        assert_eq!(to_snake_case("Model Number"), "model_number");
        assert_eq!(to_snake_case("Serial Number"), "serial_number");
        assert_eq!(
            to_snake_case("Available Power Sources"),
            "available_power_sources"
        );
        assert_eq!(
            to_snake_case("Supported Binding and Modes"),
            "supported_binding_and_modes"
        );
        // Acronyms: no underscore between consecutive uppercase letters
        assert_eq!(to_snake_case("UTC Offset"), "utc_offset");
        assert_eq!(to_snake_case("SMNC"), "smnc");
        assert_eq!(to_snake_case("Cell ID"), "cell_id");
        assert_eq!(to_snake_case("LAC"), "lac");
        // Acronym run followed by a new word: last uppercase of run gets underscore
        assert_eq!(to_snake_case("MeasureRFLink"), "measure_rf_link");
        assert_eq!(to_snake_case("SGCommon"), "sg_common");
    }
}
