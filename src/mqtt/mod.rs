pub mod publisher;
pub mod subscriber;

use std::{fs, sync::Arc, time::Duration};

use rumqttc::{AsyncClient, EventLoop, MqttOptions, TlsConfiguration, Transport};
use rustls::{ClientConfig, RootCertStore};
use tracing::info;

use crate::{
    config::{Config, MqttAuth},
    error::{Error, Result},
};

pub fn build_client(cfg: &Config) -> Result<(AsyncClient, EventLoop)> {
    let mut mqtt_opts = MqttOptions::new(
        cfg.mqtt_client_id.clone(),
        cfg.mqtt_host.clone(),
        cfg.mqtt_port,
    );
    mqtt_opts.set_keep_alive(Duration::from_secs(30));

    match &cfg.mqtt_auth {
        MqttAuth::Tls { cert_path, key_path, ca_path } => {
            let root_store = load_root_store(ca_path)?;
            let cert_chain = load_certs(cert_path)?;
            let key = load_key(key_path)?;
            let tls_config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(cert_chain, key)
                .map_err(|e| Error::Config(format!("TLS client config: {e}")))?;
            mqtt_opts.set_transport(Transport::Tls(TlsConfiguration::Rustls(Arc::new(tls_config))));
            info!(host = %cfg.mqtt_host, port = cfg.mqtt_port, auth = "tls", "MQTT client created");
        }
        MqttAuth::UsernamePassword { username, password } => {
            mqtt_opts.set_credentials(username, password);
            info!(host = %cfg.mqtt_host, port = cfg.mqtt_port, auth = "password", "MQTT client created");
        }
        MqttAuth::Anonymous => {
            info!(host = %cfg.mqtt_host, port = cfg.mqtt_port, auth = "anonymous", "MQTT client created");
        }
    }

    let (client, event_loop) = AsyncClient::new(mqtt_opts, 128);
    Ok((client, event_loop))
}

fn load_root_store(path: &std::path::Path) -> Result<RootCertStore> {
    let pem = fs::read(path)
        .map_err(|e| Error::Config(format!("read CA {}: {e}", path.display())))?;
    let mut store = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
        store
            .add(cert.map_err(|e| Error::Config(format!("parse CA cert: {e}")))?)
            .map_err(|e| Error::Config(format!("add CA cert: {e}")))?;
    }
    Ok(store)
}

fn load_certs(path: &std::path::Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let pem = fs::read(path)
        .map_err(|e| Error::Config(format!("read cert {}: {e}", path.display())))?;
    rustls_pemfile::certs(&mut pem.as_slice())
        .map(|r| r.map_err(|e| Error::Config(format!("parse cert: {e}"))))
        .collect()
}

fn load_key(path: &std::path::Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let pem = fs::read(path)
        .map_err(|e| Error::Config(format!("read key {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .map_err(|e| Error::Config(format!("parse key: {e}")))?
        .ok_or_else(|| Error::Config("no private key found in key file".into()))
}
