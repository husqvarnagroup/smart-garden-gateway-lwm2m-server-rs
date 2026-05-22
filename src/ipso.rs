use std::{collections::HashMap, path::Path};
use tracing::{info, warn};

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
/// element value from the XML (e.g. `"1.1"`), or `""` when absent.
#[derive(Debug, Clone, Default)]
pub struct IpsoModel {
    // object_id → version_string → ObjectDef
    objects: HashMap<u32, HashMap<String, ObjectDef>>,
}

impl IpsoModel {
    /// Load all `*.xml` files from each directory in `dirs`.
    /// Logs warnings for unreadable directories or unparseable files.
    pub fn load_dirs(dirs: &[impl AsRef<Path>]) -> Self {
        let mut model = IpsoModel::default();
        for dir in dirs {
            let dir = dir.as_ref();
            let entries = match std::fs::read_dir(dir) {
                Ok(e) => e,
                Err(e) => {
                    warn!(dir = %dir.display(), "Cannot read IPSO directory: {e}");
                    continue;
                }
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("xml") {
                    continue;
                }
                match parse_xml_file(&path) {
                    Some((id, version, def)) => {
                        model.objects.entry(id).or_default().insert(version, def);
                    }
                    None => warn!(path = %path.display(), "Failed to parse IPSO XML"),
                }
            }
        }
        let count: usize = model.objects.values().map(|v| v.len()).sum();
        info!(objects = count, "IPSO model loaded");
        model
    }

    /// Look up an object definition by id and optional version string.
    ///
    /// Resolution order:
    /// 1. Exact version match (when `version` is `Some`)
    /// 2. Unversioned entry (`version = ""`)
    /// 3. Any entry for this object id
    pub fn get_versioned(&self, object_id: u32, version: Option<&str>) -> Option<&ObjectDef> {
        let versions = self.objects.get(&object_id)?;
        if let Some(ver) = version {
            if let Some(def) = versions.get(ver) {
                return Some(def);
            }
        }
        versions.get("").or_else(|| versions.values().next())
    }

    /// Look up without a specific version (falls back through unversioned → any).
    pub fn get(&self, object_id: u32) -> Option<&ObjectDef> {
        self.get_versioned(object_id, None)
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
    pub fn resource_id_by_name(&self, object_id: u32, resource_name: &str, version: Option<&str>) -> Option<u32> {
        let def = self.get_versioned(object_id, version)?;
        def.resources
            .iter()
            .find(|(_, r)| r.name == resource_name)
            .map(|(&id, _)| id)
    }

    pub fn object_count(&self) -> usize {
        self.objects.values().map(|v| v.len()).sum()
    }
}

// ── XML parsing ───────────────────────────────────────────────────────────────

fn parse_xml_file(path: &Path) -> Option<(u32, String, ObjectDef)> {
    let content = std::fs::read_to_string(path).ok()?;
    parse_xml_str(&content)
}

fn parse_xml_str(xml: &str) -> Option<(u32, String, ObjectDef)> {
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
                                resource_type: cur_res_type.take().unwrap_or(ResourceType::Integer),
                                multiple_instances: cur_res_multiple,
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
    let version = object_version.unwrap_or_default();

    Some((id, version, ObjectDef { name, urn, resources }))
}

fn parse_resource_type(s: &str) -> ResourceType {
    match s {
        "Integer" => ResourceType::Integer,
        "Float" => ResourceType::Float,
        "String" => ResourceType::String,
        "Boolean" => ResourceType::Boolean,
        "Time" => ResourceType::Time,
        "Opaque" => ResourceType::Opaque,
        "Corelnk" | "CoreLink" => ResourceType::CoreLink,
        "Unsigned Integer" | "UnsignedInteger" => ResourceType::UnsignedInteger,
        _ => ResourceType::Integer,
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
        </Resources>
        <Description2><![CDATA[]]></Description2>
    </Object>
</LWM2M>"#;

    #[test]
    fn parse_irrigation_control() {
        let (id, version, obj) = parse_xml_str(IRRIGATION_XML).expect("parse");
        assert_eq!(id, 28152);
        assert_eq!(version, "0.2");
        assert_eq!(obj.name, "irrigation_control");
        assert_eq!(obj.urn, "urn:oma:lwm2m:x:28152:0.2");
        let res = obj.resources.get(&1).expect("resource 1");
        assert_eq!(res.name, "error");
        assert_eq!(res.resource_type, ResourceType::Integer);
        assert!(!res.multiple_instances);
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
    }

    #[test]
    fn snake_case_conversion() {
        // CamelCase object names
        assert_eq!(to_snake_case("IrrigationControl"), "irrigation_control");
        assert_eq!(to_snake_case("Device"), "device");
        assert_eq!(to_snake_case("ConnectivityMonitoring"), "connectivity_monitoring");
        assert_eq!(to_snake_case("SgCommon"), "sg_common");
        assert_eq!(to_snake_case("MasterChannel"), "master_channel");
        // Title-case resource names (spaces → underscores)
        assert_eq!(to_snake_case("Model Number"), "model_number");
        assert_eq!(to_snake_case("Serial Number"), "serial_number");
        assert_eq!(to_snake_case("Available Power Sources"), "available_power_sources");
        assert_eq!(to_snake_case("Supported Binding and Modes"), "supported_binding_and_modes");
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
