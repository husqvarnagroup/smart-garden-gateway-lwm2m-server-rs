use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use crate::error::{Error, Result};

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
    #[arg(
        long,
        default_value = "/var/lib/lemonbeatd/Network_management/Network_key.json"
    )]
    lb_key_file: PathBuf,

    /// Directories containing IPSO object definition XML files
    #[arg(long, num_args = 0.., value_name = "DIR")]
    ipso_directories: Vec<PathBuf>,
}

pub struct Config {
    /// UDP bind address derived from --port (always [::]:port).
    pub coap_bind_addr: SocketAddr,

    /// Interface to bind the socket to via SO_BINDTODEVICE; set when --bind-to-device is given.
    pub coap_interface: Option<String>,

    /// Server CoAP URI written to devices during bootstrap (from --server-uri).
    pub server_uri: String,

    /// Raw network key bytes loaded from --lb-key-file.
    pub network_key: Vec<u8>,

    /// Directories to scan for IPSO object definition XML files (from --ipso-directories).
    pub ipso_directories: Vec<PathBuf>,
}

impl Config {
    pub fn from_args() -> Result<Self> {
        let cli = Cli::parse();

        let coap_bind_addr = SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, cli.port));
        let coap_interface = cli.bind_to_device.then_some(cli.interface);
        let network_key = load_network_key(&cli.lb_key_file)?;

        Ok(Config {
            coap_bind_addr,
            coap_interface,
            server_uri: cli.server_uri,
            network_key,
            ipso_directories: cli.ipso_directories,
        })
    }
}

fn load_network_key(path: &PathBuf) -> Result<Vec<u8>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| Error::Config(format!("cannot read {}: {e}", path.display())))?;
    let json: serde_json::Value = serde_json::from_str(&content)?;
    let hex_str = json["network_key"].as_str().ok_or_else(|| {
        Error::Config("\"network_key\" missing or not a string in key file".into())
    })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // ── decode_hex ───────────────────────────────────────────────────────────

    #[test]
    fn decode_hex_valid_lowercase() {
        assert_eq!(
            decode_hex("0123456789abcdef").unwrap(),
            vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]
        );
    }

    #[test]
    fn decode_hex_valid_uppercase() {
        assert_eq!(
            decode_hex("AABBCCDD").unwrap(),
            vec![0xaa, 0xbb, 0xcc, 0xdd]
        );
    }

    #[test]
    fn decode_hex_empty_string() {
        assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn decode_hex_odd_length_fails() {
        let err = decode_hex("abc").unwrap_err();
        assert!(err.contains("odd length"), "got: {err}");
    }

    #[test]
    fn decode_hex_invalid_chars_fails() {
        let err = decode_hex("zz00").unwrap_err();
        assert!(!err.is_empty());
    }

    #[test]
    fn decode_hex_16_byte_key() {
        let hex = "00112233445566778899aabbccddeeff";
        let bytes = decode_hex(hex).unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0], 0x00);
        assert_eq!(bytes[15], 0xff);
    }

    // ── load_network_key ─────────────────────────────────────────────────────

    #[test]
    fn load_network_key_valid_file() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"{{"network_key": "0123456789abcdef0123456789abcdef"}}"#
        )
        .unwrap();
        let key = load_network_key(&f.path().to_path_buf()).unwrap();
        assert_eq!(key.len(), 16);
        assert_eq!(key[0], 0x01);
        assert_eq!(key[15], 0xef);
    }

    #[test]
    fn load_network_key_missing_file() {
        let result = load_network_key(&PathBuf::from("/nonexistent/path/key.json"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot read"), "got: {err}");
    }

    #[test]
    fn load_network_key_invalid_json() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "not json at all").unwrap();
        let result = load_network_key(&f.path().to_path_buf());
        assert!(result.is_err());
    }

    #[test]
    fn load_network_key_missing_field() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"other_field": "value"}}"#).unwrap();
        let result = load_network_key(&f.path().to_path_buf());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("network_key"), "got: {err}");
    }

    #[test]
    fn load_network_key_field_not_string() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"network_key": 12345}}"#).unwrap();
        let result = load_network_key(&f.path().to_path_buf());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("network_key"), "got: {err}");
    }

    #[test]
    fn load_network_key_invalid_hex() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"network_key": "not_hex_data!!"}}"#).unwrap();
        let result = load_network_key(&f.path().to_path_buf());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("hex"), "got: {err}");
    }

    #[test]
    fn load_network_key_odd_length_hex() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"network_key": "abc"}}"#).unwrap();
        let result = load_network_key(&f.path().to_path_buf());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("odd length"), "got: {err}");
    }

    #[test]
    fn load_network_key_empty_hex_string() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, r#"{{"network_key": ""}}"#).unwrap();
        let key = load_network_key(&f.path().to_path_buf()).unwrap();
        assert!(key.is_empty());
    }
}
