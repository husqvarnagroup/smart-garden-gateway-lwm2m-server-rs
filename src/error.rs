use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("CoAP encode/decode: {0}")]
    Coap(String),

    #[error("MQTT: {0}")]
    Mqtt(String),

    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config: {0}")]
    Config(String),

    #[error("device not found: {0}")]
    DeviceNotFound(String),

    #[error("bootstrap: {0}")]
    Bootstrap(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<rumqttc::ClientError> for Error {
    fn from(e: rumqttc::ClientError) -> Self {
        Error::Mqtt(e.to_string())
    }
}

impl From<rumqttc::ConnectionError> for Error {
    fn from(e: rumqttc::ConnectionError) -> Self {
        Error::Mqtt(e.to_string())
    }
}
