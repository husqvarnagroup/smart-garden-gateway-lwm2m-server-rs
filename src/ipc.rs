use std::path::PathBuf;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{bootstrap::BootstrapRegistry, error::Result};

pub const DEFAULT_SOCKET_PATH: &str = "/tmp/lwm2mserver-command.ipc";

pub async fn run(
    path: PathBuf,
    bootstrap_registry: BootstrapRegistry,
    cancel: CancellationToken,
) -> Result<()> {
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    info!(path = %path.display(), "IPC command socket listening");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = std::fs::remove_file(&path);
                info!("IPC server shutting down");
                return Ok(());
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        tokio::spawn(handle_client(stream, bootstrap_registry.clone()));
                    }
                    Err(e) => warn!("IPC accept error: {e}"),
                }
            }
        }
    }
}

async fn handle_client(stream: tokio::net::UnixStream, bootstrap_registry: BootstrapRegistry) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let response = dispatch(&line, &bootstrap_registry).await;
        if writer.write_all(response.as_bytes()).await.is_err() {
            break;
        }
    }
}

async fn dispatch(line: &str, bootstrap_registry: &BootstrapRegistry) -> String {
    let requests: Vec<serde_json::Value> = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            warn!("IPC: invalid JSON: {e}");
            return "[{\"success\":false}]\n".into();
        }
    };

    let mut responses = Vec::with_capacity(requests.len());
    for req in &requests {
        responses.push(handle_request(req, bootstrap_registry).await);
    }

    let mut out = serde_json::to_string(&responses).unwrap_or_else(|_| "[{}]".into());
    out.push('\n');
    out
}

async fn handle_request(
    req: &serde_json::Value,
    bootstrap_registry: &BootstrapRegistry,
) -> serde_json::Value {
    let op = req["op"].as_str().unwrap_or("");
    let path = req["entity"]["path"].as_str().unwrap_or("");

    match op {
        "read" => match path {
            "devices" => {
                info!("IPC: read devices → [] (no registered devices)");
                serde_json::json!({"payload": {}, "success": true})
            }
            _ => {
                warn!(op, path, "IPC: unhandled read path");
                serde_json::json!({"success": false})
            }
        },

        "execute"
            if path.starts_with("includable_device/") && path.ends_with("/include") =>
        {
            // path = "includable_device/<id>/include"
            let middle = &path["includable_device/".len()..path.len() - "/include".len()];
            match middle.parse::<u32>() {
                Ok(id) => match bootstrap_registry.approve_inclusion(id).await {
                    Some((endpoint, _)) => {
                        info!(%endpoint, id, "IPC: inclusion approved");
                        serde_json::json!({"success": true})
                    }
                    None => {
                        warn!(id, "IPC: execute include — device not found or already approved");
                        serde_json::json!({"success": false})
                    }
                },
                Err(_) => {
                    warn!(path, "IPC: execute include — invalid id in path");
                    serde_json::json!({"success": false})
                }
            }
        }

        _ => {
            warn!(op, path, "IPC: unhandled request");
            serde_json::json!({"success": true})
        }
    }
}
