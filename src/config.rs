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
    pub ipso_directories: Vec<std::path::PathBuf>,
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
    if !s.len().is_multiple_of(2) {
        return Err("odd length".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}
