use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::Result;

const SOCKET_PATH: &str = "/tmp/lwm2mserver-command.ipc";

pub async fn run(cancel: CancellationToken) -> Result<()> {
    let _ = std::fs::remove_file(SOCKET_PATH);
    let listener = UnixListener::bind(SOCKET_PATH)?;
    info!(path = SOCKET_PATH, "IPC command socket listening");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = std::fs::remove_file(SOCKET_PATH);
                info!("IPC server shutting down");
                return Ok(());
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => { tokio::spawn(handle_client(stream)); }
                    Err(e) => warn!("IPC accept error: {e}"),
                }
            }
        }
    }
}

async fn handle_client(stream: tokio::net::UnixStream) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let response = dispatch(&line);
        if writer.write_all(response.as_bytes()).await.is_err() {
            break;
        }
    }
}

fn dispatch(line: &str) -> String {
    let requests: Vec<serde_json::Value> = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            warn!("IPC: invalid JSON: {e}");
            return "[{\"success\":false}]\n".into();
        }
    };

    let responses: Vec<serde_json::Value> = requests
        .iter()
        .map(|req| handle_request(req))
        .collect();

    let mut out = serde_json::to_string(&responses).unwrap_or_else(|_| "[{}]".into());
    out.push('\n');
    out
}

fn handle_request(req: &serde_json::Value) -> serde_json::Value {
    let op = req["op"].as_str().unwrap_or("");
    let path = req["entity"]["path"].as_str().unwrap_or("");

    match (op, path) {
        ("read", "devices") => {
            info!("IPC: read devices → [] (no registered devices)");
            serde_json::json!({"payload": {}, "success": true})
        }
        _ => {
            warn!(op, path, "IPC: unhandled request");
            serde_json::json!({"success": true})
        }
    }
}
