use std::{net::SocketAddr, path::PathBuf};

use crate::error::{Error, Result};

/// How the service authenticates to the MQTT broker.
pub enum MqttAuth {
    /// Mutual TLS with client certificate — required for AWS IoT Core.
    Tls {
        cert_path: PathBuf,
        key_path: PathBuf,
        ca_path: PathBuf,
    },
    /// Plain TCP with username / password (e.g. Mosquitto, HiveMQ).
    UsernamePassword { username: String, password: String },
    /// Plain TCP, no credentials (local testing / open broker).
    Anonymous,
}

/// Runtime configuration. All fields can be set via environment variables.
pub struct Config {
    /// UDP bind address for the CoAP/LWM2M server (default: [::]:20017).
    pub coap_bind_addr: SocketAddr,

    /// MQTT broker hostname or IP.
    pub mqtt_host: String,
    /// MQTT port. Defaults to 8883 for TLS, 1883 for plain TCP.
    pub mqtt_port: u16,
    /// MQTT client ID (should match the AWS IoT Thing name when using TLS).
    pub mqtt_client_id: String,

    /// Authentication mode, derived from which env vars are present.
    pub mqtt_auth: MqttAuth,

    /// MQTT topic prefix. Commands arrive on `{prefix}/cmd/{endpoint}`,
    /// responses are published to `{prefix}/resp/{endpoint}/{correlation_id}`.
    pub mqtt_topic_prefix: String,

    /// How long (seconds) to keep a device registration alive after the
    /// declared lifetime expires before removing it from the registry.
    pub registration_grace_secs: u32,

    /// Network interface to bind the CoAP socket to (SO_BINDTODEVICE).
    /// Set to "ppp0" when the radio is on a PPP link to ensure packets leave
    /// via the correct interface with the right source address.
    pub coap_interface: Option<String>,

    /// This server's CoAP URI advertised to devices during bootstrap
    /// (e.g. "coap://[fc00::6:100:0:0]"). Written to device Object 1 in the
    /// bootstrap write phase so devices know where to register afterwards.
    pub server_uri: Option<String>,

    /// Raw network key loaded from --lb-key-file (hex field "network_key").
    /// Written to LWM2M Security Object /0/1/5 during bootstrap write phase.
    pub network_key: Vec<u8>,
}

impl Config {
    /// Derive configuration from environment variables and CLI arguments.
    ///
    /// Auth mode selection (first match wins):
    ///   TLS_CERT_PATH set          → mutual TLS (also requires TLS_KEY_PATH, TLS_CA_PATH)
    ///   MQTT_USERNAME set          → plain TCP with username/password
    ///   neither                    → plain TCP, anonymous
    pub fn from_env() -> Result<Self> {
        let mqtt_auth = if let Ok(cert) = std::env::var("TLS_CERT_PATH") {
            MqttAuth::Tls {
                cert_path: PathBuf::from(cert),
                key_path: env_require("TLS_KEY_PATH").map(PathBuf::from)?,
                ca_path: env_require("TLS_CA_PATH").map(PathBuf::from)?,
            }
        } else if let Ok(username) = std::env::var("MQTT_USERNAME") {
            MqttAuth::UsernamePassword {
                username,
                password: env_default("MQTT_PASSWORD", ""),
            }
        } else {
            MqttAuth::Anonymous
        };

        let default_port = match &mqtt_auth {
            MqttAuth::Tls { .. } => "8883",
            _ => "1883",
        };

        let network_key = load_network_key()?;

        Ok(Config {
            coap_bind_addr: env_parse("COAP_BIND_ADDR", "[::]:20017")?,
            mqtt_host: env_require("MQTT_HOST")?,
            mqtt_port: env_parse("MQTT_PORT", default_port)?,
            mqtt_client_id: env_require("MQTT_CLIENT_ID")?,
            mqtt_auth,
            mqtt_topic_prefix: env_default("MQTT_TOPIC_PREFIX", "lwm2m"),
            registration_grace_secs: env_parse("REGISTRATION_GRACE_SECS", "30")?,
            coap_interface: std::env::var("COAP_INTERFACE").ok(),
            server_uri: std::env::var("SERVER_URI").ok(),
            network_key,
        })
    }
}

/// Parse `--lb-key-file <path>` from argv, read the JSON file, decode the
/// hex value under "network_key".
fn load_network_key() -> Result<Vec<u8>> {
    let args: Vec<String> = std::env::args().collect();
    let path = args
        .windows(2)
        .find(|w| w[0] == "--lb-key-file")
        .map(|w| w[1].clone())
        .ok_or_else(|| Error::Config("--lb-key-file <path> is required".into()))?;

    let content = std::fs::read_to_string(&path)
        .map_err(|e| Error::Config(format!("cannot read {path}: {e}")))?;

    let json: serde_json::Value = serde_json::from_str(&content)?;

    let hex_str = json["network_key"]
        .as_str()
        .ok_or_else(|| Error::Config("\"network_key\" missing or not a string in key file".into()))?;

    decode_hex(hex_str)
        .map_err(|e| Error::Config(format!("invalid hex in network_key: {e}")))
}

fn decode_hex(s: &str) -> std::result::Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd length".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn env_require(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::Config(format!("missing required env var {key}")))
}

fn env_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let raw = std::env::var(key).unwrap_or_else(|_| default.to_owned());
    raw.parse()
        .map_err(|e| Error::Config(format!("{key}={raw:?}: {e}")))
}
