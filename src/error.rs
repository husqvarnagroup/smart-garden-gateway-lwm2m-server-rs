use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("CoAP encode/decode: {0}")]
    Coap(String),

    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config: {0}")]
    Config(String),

    #[error("bootstrap: {0}")]
    Bootstrap(String),
}

pub type Result<T> = std::result::Result<T, Error>;
