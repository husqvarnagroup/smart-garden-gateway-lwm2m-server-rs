use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, RwLock},
};
use tracing::{info, warn};

/// A shared, hot-reloadable IPSO model. Inner `Arc<IpsoModel>` is swapped
/// atomically on SIGHUP; the write lock is held only during the swap.
pub type SharedIpso = Arc<RwLock<Arc<IpsoModel>>>;

/// Construct a freshly loaded [`SharedIpso`] from the given directories.
pub fn load_shared(dirs: &[impl AsRef<Path>]) -> SharedIpso {
    Arc::new(RwLock::new(Arc::new(IpsoModel::load_dirs(dirs))))
}

// ── Public types ──────────────────────────────────────────────────────────────

/// An LwM2M object version (`<ObjectVersion>` in a spec file or the `ver`
/// attribute of a registration link).
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    major: u16,
    minor: u16,
}

impl Version {
    pub fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

/// Version assumed by the LwM2M specification when the version is omitted.
impl Default for Version {
    fn default() -> Self {
        Version::new(1, 0)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("Invalid version string: {0}")]
pub struct ParseVersionError(String);

impl FromStr for Version {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseVersionError(s.to_owned());
        let numbers = s.trim().split('.').collect::<Vec<_>>();
        match numbers.as_slice() {
            [major] => Ok(Version {
                major: major.parse().map_err(|_| err())?,
                minor: 0,
            }),
            [major, minor] => Ok(Version {
                major: major.parse().map_err(|_| err())?,
                minor: minor.parse().map_err(|_| err())?,
            }),
            _ => Err(err()),
        }
    }
}

/// Operations allowed on a resource (`<Operations>` in a spec file).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Operations {
    Read,
    Write,
    ReadWrite,
    Execute,
    /// No operations allowed (e.g. Security object resources).
    None,
}

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
    ObjectLink,
    /// Unspecified type, e.g. on executable resources.
    Other,
}

#[derive(Debug, Clone)]
pub struct ResourceDef {
    pub name: String,
    pub resource_type: ResourceType,
    pub multiple_instances: bool,
    pub operations: Operations,
    pub mandatory: bool,
}

#[derive(Debug, Clone)]
pub struct ObjectDef {
    pub name: String,
    pub urn: String,
    pub resources: HashMap<u32, ResourceDef>,
}

/// In-memory IPSO object model loaded from XML files at startup.
///
/// Keyed by `(object_id, version)` where `version` is the parsed
/// `<ObjectVersion>` element value, or 1.0 when absent as defined by the
/// LwM2M specification.
#[derive(Debug, Clone, Default)]
pub struct IpsoModel {
    // object_id → version → ObjectDef; the BTreeMap keeps versions ordered so
    // the newest-version fallback is deterministic.
    objects: HashMap<u32, BTreeMap<Version, ObjectDef>>,
}

impl IpsoModel {
    /// Load all `*.xml` files (any case) from each directory in `dirs`,
    /// including subdirectories.
    /// Logs warnings for unreadable directories or unparsable files.
    pub fn load_dirs(dirs: &[impl AsRef<Path>]) -> Self {
        let mut model = IpsoModel::default();
        for dir in dirs {
            let dir = dir.as_ref();
            let mut files = Vec::new();
            collect_xml_files(dir, &mut files);
            let mut files_loaded = 0usize;
            for path in files {
                match parse_xml_file(&path) {
                    Some((id, version, def)) => {
                        model.objects.entry(id).or_default().insert(version, def);
                        files_loaded += 1;
                    }
                    None => warn!(path = %path.display(), "Failed to parse IPSO XML"),
                }
            }
            if files_loaded == 0 {
                warn!(dir = %dir.display(), "No IPSO object definitions found in directory");
            }
        }
        info!(objects = model.object_count(), "IPSO model loaded");
        model
    }

    /// Look up an object definition by id and optional version string.
    ///
    /// Resolution order:
    /// 1. Exact version match (when `version` parses; e.g. `"1"` matches `1.0`)
    /// 2. Newest available version of this object id
    pub fn get_versioned(&self, object_id: u32, version: Option<&str>) -> Option<&ObjectDef> {
        let versions = self.objects.get(&object_id)?;
        if let Some(ver) = version.and_then(|s| Version::from_str(s).ok()) {
            if let Some(def) = versions.get(&ver) {
                return Some(def);
            }
        }
        versions.values().next_back()
    }

    /// Look up without a specific version (falls back to the newest version).
    pub fn get(&self, object_id: u32) -> Option<&ObjectDef> {
        self.get_versioned(object_id, None)
    }

    /// Find the numeric object ID for a given snake_case object name.
    /// Scans all versions; returns the lowest matching object ID.
    pub fn object_id_by_name(&self, name: &str) -> Option<u32> {
        self.objects
            .iter()
            .filter(|(_, versions)| versions.values().any(|def| def.name == name))
            .map(|(&id, _)| id)
            .min()
    }

    /// Find the numeric resource ID for a resource name within a given object,
    /// using the version resolution order of `get_versioned`.
    /// Returns the lowest matching resource ID.
    pub fn resource_id_by_name(
        &self,
        object_id: u32,
        resource_name: &str,
        version: Option<&str>,
    ) -> Option<u32> {
        let def = self.get_versioned(object_id, version)?;
        def.resources
            .iter()
            .filter(|(_, r)| r.name == resource_name)
            .map(|(&id, _)| id)
            .min()
    }

    pub fn object_count(&self) -> usize {
        self.objects.values().map(|v| v.len()).sum()
    }
}

/// Recursively collect `*.xml` files (case-insensitive extension) under `dir`.
fn collect_xml_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(dir = %dir.display(), "Cannot read IPSO directory: {e}");
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_xml_files(&path, out);
        } else if path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("xml"))
        {
            out.push(path);
        }
    }
}

// ── XML parsing ───────────────────────────────────────────────────────────────

fn parse_xml_file(path: &Path) -> Option<(u32, Version, ObjectDef)> {
    let content = std::fs::read_to_string(path).ok()?;
    parse_xml_str(&content)
}

fn parse_xml_str(xml: &str) -> Option<(u32, Version, ObjectDef)> {
    use quick_xml::{events::Event, Reader};

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut object_id: Option<u32> = None;
    let mut object_name: Option<String> = None;
    let mut object_urn: Option<String> = None;
    let mut object_version: Option<String> = None;
    let mut resources: HashMap<u32, ResourceDef> = HashMap::new();

    let mut cur_res_id: Option<u32> = None;
    let mut cur_res_name: Option<String> = None;
    let mut cur_res_type: Option<ResourceType> = None;
    let mut cur_res_multiple = false;
    let mut cur_res_operations = Operations::None;
    let mut cur_res_mandatory = false;

    let mut current_tag = String::new();
    let mut in_resources = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let tag = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_owned();

                if tag == "Item" {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"ID" {
                            if let Ok(s) = std::str::from_utf8(&attr.value) {
                                cur_res_id = s.parse().ok();
                            }
                        }
                    }
                    cur_res_name = None;
                    cur_res_type = None;
                    cur_res_multiple = false;
                    cur_res_operations = Operations::None;
                    cur_res_mandatory = false;
                } else if tag == "Resources" {
                    in_resources = true;
                }

                current_tag = tag;
            }

            Ok(Event::End(e)) => {
                let name_bytes = e.name();
                let tag = std::str::from_utf8(name_bytes.as_ref()).unwrap_or("");
                if tag == "Item" {
                    if let (Some(id), Some(name)) = (cur_res_id, cur_res_name.take()) {
                        resources.insert(
                            id,
                            ResourceDef {
                                name: to_snake_case(&name),
                                resource_type: cur_res_type.take().unwrap_or(ResourceType::Other),
                                multiple_instances: cur_res_multiple,
                                operations: cur_res_operations,
                                mandatory: cur_res_mandatory,
                            },
                        );
                    } else {
                        cur_res_type = None;
                    }
                    cur_res_id = None;
                } else if tag == "Resources" {
                    in_resources = false;
                }
                current_tag.clear();
            }

            Ok(Event::Text(e)) => {
                let Ok(text) = e.unescape() else { continue };
                let text = text.as_ref();

                match current_tag.as_str() {
                    "ObjectID" if !in_resources => {
                        object_id = text.parse().ok();
                    }
                    "Name" if !in_resources && object_name.is_none() => {
                        object_name = Some(text.to_owned());
                    }
                    "ObjectURN" => {
                        object_urn = Some(text.to_owned());
                    }
                    "ObjectVersion" if !in_resources => {
                        object_version = Some(text.to_owned());
                    }
                    "Name" if in_resources => {
                        cur_res_name = Some(text.to_owned());
                    }
                    "Type" => {
                        cur_res_type = Some(parse_resource_type(text));
                    }
                    "Operations" if in_resources => {
                        cur_res_operations = parse_operations(text);
                    }
                    "Mandatory" if in_resources => {
                        cur_res_mandatory = text == "Mandatory";
                    }
                    "MultipleInstances" if in_resources => {
                        cur_res_multiple = text == "Multiple";
                    }
                    _ => {}
                }
            }

            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
    }

    let id = object_id?;
    let name = to_snake_case(&object_name?);
    let urn = object_urn.unwrap_or_default();
    let version = match object_version {
        Some(s) => s.parse().unwrap_or_else(|_| {
            warn!(version = %s, "Unparsable object version in spec file, assuming 1.0");
            Version::default()
        }),
        None => Version::default(),
    };

    Some((
        id,
        version,
        ObjectDef {
            name,
            urn,
            resources,
        },
    ))
}

fn parse_operations(s: &str) -> Operations {
    match s {
        "R" => Operations::Read,
        "W" => Operations::Write,
        "RW" => Operations::ReadWrite,
        "E" => Operations::Execute,
        _ => Operations::None,
    }
}

fn parse_resource_type(s: &str) -> ResourceType {
    match s {
        "Integer" => ResourceType::Integer,
        "Float" => ResourceType::Float,
        "String" => ResourceType::String,
        "Boolean" => ResourceType::Boolean,
        "Time" => ResourceType::Time,
        "Opaque" => ResourceType::Opaque,
        "Objlnk" => ResourceType::ObjectLink,
        "Corelnk" | "CoreLink" => ResourceType::CoreLink,
        "Unsigned Integer" | "UnsignedInteger" => ResourceType::UnsignedInteger,
        _ => ResourceType::Other,
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

    const IRRIGATION_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<LWM2M xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.openmobilealliance.org/tech/profiles/LWM2M-v1_1.xsd">
    <Object ObjectType="MODefinition">
        <Name>IrrigationControl</Name>
        <Description1><![CDATA[Smart Irrigation Control]]></Description1>
        <ObjectID>28152</ObjectID>
        <ObjectURN>urn:oma:lwm2m:x:28152:0.2</ObjectURN>
        <LWM2MVersion>1.1</LWM2MVersion>
        <ObjectVersion>0.2</ObjectVersion>
        <MultipleInstances>Single</MultipleInstances>
        <Mandatory>Optional</Mandatory>
        <Resources>
            <Item ID="1">
                <Name>error</Name>
                <Operations>R</Operations>
                <MultipleInstances>Single</MultipleInstances>
                <Mandatory>Mandatory</Mandatory>
                <Type>Integer</Type>
                <RangeEnumeration></RangeEnumeration>
                <Units></Units>
                <Description><![CDATA[Device error]]></Description>
            </Item>
            <Item ID="2">
                <Name>schedule</Name>
                <Operations>RW</Operations>
                <MultipleInstances>Single</MultipleInstances>
                <Mandatory>Optional</Mandatory>
                <Type>Objlnk</Type>
            </Item>
        </Resources>
        <Description2><![CDATA[]]></Description2>
    </Object>
</LWM2M>"#;

    #[test]
    fn parse_irrigation_control() {
        let (id, version, obj) = parse_xml_str(IRRIGATION_XML).expect("parse");
        assert_eq!(id, 28152);
        assert_eq!(version, Version::new(0, 2));
        assert_eq!(obj.name, "irrigation_control");
        assert_eq!(obj.urn, "urn:oma:lwm2m:x:28152:0.2");
        let res = obj.resources.get(&1).expect("resource 1");
        assert_eq!(res.name, "error");
        assert_eq!(res.resource_type, ResourceType::Integer);
        assert!(!res.multiple_instances);
        assert_eq!(res.operations, Operations::Read);
        assert!(res.mandatory);
    }

    #[test]
    fn parse_object_link_resource() {
        let (_, _, obj) = parse_xml_str(IRRIGATION_XML).expect("parse");
        let res = obj.resources.get(&2).expect("resource 2");
        assert_eq!(res.resource_type, ResourceType::ObjectLink);
        assert_eq!(res.operations, Operations::ReadWrite);
        assert!(!res.mandatory);
    }

    const SG_COMMON_FRAGMENT: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<LWM2M xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.openmobilealliance.org/tech/profiles/LWM2M-v1_1.xsd">
    <Object ObjectType="MODefinition">
        <Name>SG Common</Name>
        <ObjectID>28183</ObjectID>
        <ObjectURN>urn:oma:lwm2m:x:28183:0.1</ObjectURN>
        <ObjectVersion>0.1</ObjectVersion>
        <MultipleInstances>Single</MultipleInstances>
        <Mandatory>Optional</Mandatory>
        <Resources>
            <Item ID="33">
                <Name>measure_rf_link</Name>
                <Operations>E</Operations>
                <MultipleInstances>Single</MultipleInstances>
                <Mandatory>Mandatory</Mandatory>
                <Type></Type>
            </Item>
        </Resources>
    </Object>
</LWM2M>"#;

    #[test]
    fn parse_execute_resource_empty_type() {
        let (id, _, obj) = parse_xml_str(SG_COMMON_FRAGMENT).expect("parse");
        assert_eq!(id, 28183);
        assert_eq!(obj.name, "sg_common");
        let res = obj.resources.get(&33).expect("resource 33");
        assert_eq!(res.name, "measure_rf_link");
        assert_eq!(res.resource_type, ResourceType::Other);
        assert_eq!(res.operations, Operations::Execute);
    }

    #[test]
    fn parse_absent_object_version_defaults_to_1_0() {
        let xml = SG_COMMON_FRAGMENT.replace("<ObjectVersion>0.1</ObjectVersion>", "");
        let (_, version, _) = parse_xml_str(&xml).expect("parse");
        assert_eq!(version, Version::default());
        assert_eq!(version, Version::new(1, 0));
    }

    #[test]
    fn parse_unparsable_object_version_defaults_to_1_0() {
        let xml = SG_COMMON_FRAGMENT.replace(
            "<ObjectVersion>0.1</ObjectVersion>",
            "<ObjectVersion>not-a-version</ObjectVersion>",
        );
        let (_, version, _) = parse_xml_str(&xml).expect("parse");
        assert_eq!(version, Version::new(1, 0));
    }

    #[test]
    fn version_from_str() {
        assert_eq!(Version::from_str("2"), Ok(Version::new(2, 0)));
        assert_eq!(Version::from_str("1.2"), Ok(Version::new(1, 2)));
        assert_eq!(Version::from_str(" 1.1 "), Ok(Version::new(1, 1)));
        assert!(Version::from_str("1.2.3").is_err());
        assert!(Version::from_str("a.b").is_err());
        assert!(Version::from_str("").is_err());
        assert!(Version::from_str("1.x").is_err());
    }

    #[test]
    fn version_ordering_and_display() {
        assert!(Version::new(1, 2) > Version::new(1, 1));
        assert!(Version::new(2, 0) > Version::new(1, 9));
        assert!(Version::new(0, 10) > Version::new(0, 9));
        assert_eq!(Version::new(1, 2).to_string(), "1.2");
    }

    fn model_with_versions(versions: &[&str]) -> IpsoModel {
        let mut model = IpsoModel::default();
        for ver in versions {
            let xml = IRRIGATION_XML
                .replace(
                    "<ObjectVersion>0.2</ObjectVersion>",
                    &format!("<ObjectVersion>{ver}</ObjectVersion>"),
                )
                .replace(
                    "urn:oma:lwm2m:x:28152:0.2",
                    &format!("urn:oma:lwm2m:x:28152:{ver}"),
                );
            let (id, version, def) = parse_xml_str(&xml).expect("parse");
            model.objects.entry(id).or_default().insert(version, def);
        }
        model
    }

    #[test]
    fn get_versioned_exact_match() {
        let model = model_with_versions(&["0.2", "0.10"]);
        let def = model.get_versioned(28152, Some("0.2")).expect("object");
        assert_eq!(def.urn, "urn:oma:lwm2m:x:28152:0.2");
    }

    #[test]
    fn get_versioned_falls_back_to_newest() {
        let model = model_with_versions(&["0.2", "0.10"]);
        // Unknown version and no version both resolve to the newest (0.10 > 0.2).
        for ver in [Some("0.5"), Some("garbage"), None] {
            let def = model.get_versioned(28152, ver).expect("object");
            assert_eq!(def.urn, "urn:oma:lwm2m:x:28152:0.10");
        }
    }

    #[test]
    fn get_versioned_matches_major_only_version_string() {
        let model = model_with_versions(&["1.0", "2.1"]);
        // "1" from a registration link matches the 1.0 definition.
        let def = model.get_versioned(28152, Some("1")).expect("object");
        assert_eq!(def.urn, "urn:oma:lwm2m:x:28152:1.0");
    }

    #[test]
    fn name_lookups() {
        let model = model_with_versions(&["0.2"]);
        assert_eq!(model.object_id_by_name("irrigation_control"), Some(28152));
        assert_eq!(model.object_id_by_name("unknown"), None);
        assert_eq!(model.resource_id_by_name(28152, "error", None), Some(1));
        assert_eq!(model.resource_id_by_name(28152, "unknown", None), None);
    }

    #[test]
    fn load_dirs_recursive_and_case_insensitive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sub = dir.path().join("nested");
        std::fs::create_dir(&sub).expect("create subdir");
        std::fs::write(sub.join("irrigation.XML"), IRRIGATION_XML).expect("write");
        std::fs::write(dir.path().join("notes.txt"), "not xml").expect("write");

        let model = IpsoModel::load_dirs(&[dir.path()]);
        assert_eq!(model.object_count(), 1);
        assert!(model.get(28152).is_some());
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
