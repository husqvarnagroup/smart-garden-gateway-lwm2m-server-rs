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
    /// UDP bind address for the CoAP/LWM2M server (default: [::]:5683).
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
}

impl Config {
    /// Derive configuration from environment variables.
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

        Ok(Config {
            coap_bind_addr: env_parse("COAP_BIND_ADDR", "[::]:5683")?,
            mqtt_host: env_require("MQTT_HOST")?,
            mqtt_port: env_parse("MQTT_PORT", default_port)?,
            mqtt_client_id: env_require("MQTT_CLIENT_ID")?,
            mqtt_auth,
            mqtt_topic_prefix: env_default("MQTT_TOPIC_PREFIX", "lwm2m"),
            registration_grace_secs: env_parse("REGISTRATION_GRACE_SECS", "30")?,
        })
    }
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
