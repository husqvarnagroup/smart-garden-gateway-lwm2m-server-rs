use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;

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

#[derive(Parser)]
#[command(about = "LWM2M/CoAP gateway with bootstrap support")]
struct Cli {
    /// Network interface for CoAP traffic (e.g. ppp0)
    interface: String,

    /// Bind the CoAP socket to the interface via SO_BINDTODEVICE
    #[arg(long)]
    bind_to_device: bool,

    /// CoAP URI of this server advertised to devices during bootstrap
    #[arg(long, default_value = "coap://[fc00::6:100:0:0]")]
    server_uri: String,

    /// UDP port to listen on
    #[arg(long, default_value_t = 20017)]
    port: u16,

    /// JSON file containing the network key (hex field "network_key")
    #[arg(long, default_value = "/var/lib/lemonbeatd/Network_management/Network_key.json")]
    lb_key_file: PathBuf,
}

/// Runtime configuration.
pub struct Config {
    /// UDP bind address derived from --port (always [::]:port).
    pub coap_bind_addr: SocketAddr,

    /// Interface to bind the socket to via SO_BINDTODEVICE; set when --bind-to-device is given.
    pub coap_interface: Option<String>,

    /// Server CoAP URI written to devices during bootstrap (from --server-uri).
    pub server_uri: String,

    /// Raw network key bytes loaded from --lb-key-file.
    pub network_key: Vec<u8>,

    // ── MQTT (configured via environment variables) ──────────────────────────
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_client_id: String,
    pub mqtt_auth: MqttAuth,
    pub mqtt_topic_prefix: String,
}

impl Config {
    pub fn from_args() -> Result<Self> {
        let cli = Cli::parse();

        let coap_bind_addr =
            SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, cli.port));
        let coap_interface = cli.bind_to_device.then_some(cli.interface);
        let network_key = load_network_key(&cli.lb_key_file)?;

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

        let default_mqtt_port = match &mqtt_auth {
            MqttAuth::Tls { .. } => "8883",
            _ => "1883",
        };

        Ok(Config {
            coap_bind_addr,
            coap_interface,
            server_uri: cli.server_uri,
            network_key,
            mqtt_host: env_require("MQTT_HOST")?,
            mqtt_port: env_parse("MQTT_PORT", default_mqtt_port)?,
            mqtt_client_id: env_require("MQTT_CLIENT_ID")?,
            mqtt_auth,
            mqtt_topic_prefix: env_default("MQTT_TOPIC_PREFIX", "lwm2m"),
        })
    }
}

fn load_network_key(path: &PathBuf) -> Result<Vec<u8>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| Error::Config(format!("cannot read {}: {e}", path.display())))?;
    let json: serde_json::Value = serde_json::from_str(&content)?;
    let hex_str = json["network_key"]
        .as_str()
        .ok_or_else(|| Error::Config("\"network_key\" missing or not a string in key file".into()))?;
    decode_hex(hex_str).map_err(|e| Error::Config(format!("invalid hex in network_key: {e}")))
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
