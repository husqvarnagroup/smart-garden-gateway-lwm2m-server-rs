use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    bootstrap::BootstrapRegistry,
    coap::server::DispatchRequest,
    error::Result,
    ipso::IpsoModel,
    model::{LwM2mCommand, PendingOperation, ResourcePath, ResourceValue},
    registry::DeviceRegistry,
};

pub const DEFAULT_SOCKET_PATH: &str = "/tmp/lwm2mserver-command.ipc";

static NEXT_OP_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Clone)]
struct IpcCtx {
    bootstrap_registry: BootstrapRegistry,
    registry: DeviceRegistry,
    ipso: Arc<IpsoModel>,
    dispatch_tx: mpsc::Sender<DispatchRequest>,
}

pub async fn run(
    path: PathBuf,
    bootstrap_registry: BootstrapRegistry,
    registry: DeviceRegistry,
    ipso: Arc<IpsoModel>,
    dispatch_tx: mpsc::Sender<DispatchRequest>,
    cancel: CancellationToken,
) -> Result<()> {
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    info!(path = %path.display(), "IPC command socket listening");

    let ctx = IpcCtx { bootstrap_registry, registry, ipso, dispatch_tx };

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
                        tokio::spawn(handle_client(stream, ctx.clone()));
                    }
                    Err(e) => warn!("IPC accept error: {e}"),
                }
            }
        }
    }
}

async fn handle_client(stream: tokio::net::UnixStream, ctx: IpcCtx) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let response = dispatch(&line, &ctx).await;
        if writer.write_all(response.as_bytes()).await.is_err() {
            break;
        }
    }
}

async fn dispatch(line: &str, ctx: &IpcCtx) -> String {
    let requests: Vec<serde_json::Value> = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            warn!("IPC: invalid JSON: {e}");
            return "[{\"success\":false}]\n".into();
        }
    };

    let mut responses = Vec::with_capacity(requests.len());
    for req in &requests {
        responses.push(handle_request(req, ctx).await);
    }

    let mut out = serde_json::to_string(&responses).unwrap_or_else(|_| "[{}]".into());
    out.push('\n');
    out
}

async fn handle_request(req: &serde_json::Value, ctx: &IpcCtx) -> serde_json::Value {
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
                Ok(id) => match ctx.bootstrap_registry.approve_inclusion(id).await {
                    Some(endpoint) => {
                        info!(%endpoint, id, "IPC: inclusion approved");
                        serde_json::json!({"success": true})
                    }
                    None => {
                        warn!(id, "IPC: execute include — device not found");
                        serde_json::json!({"success": false})
                    }
                },
                Err(_) => {
                    warn!(path, "IPC: execute include — invalid id in path");
                    serde_json::json!({"success": false})
                }
            }
        }

        "execute" if !path.is_empty() => {
            let device = req["entity"]["device"].as_str().unwrap_or("");
            handle_execute_path(path, device, ctx).await
        }

        _ => {
            warn!(op, path, "IPC: unhandled request");
            tracing::debug!(payload = %req, "IPC: unhandled request payload");
            serde_json::json!({"success": true})
        }
    }
}

async fn handle_execute_path(path: &str, device: &str, ctx: &IpcCtx) -> serde_json::Value {
    // path = "object_name/instance_id/resource_name"
    let parts: Vec<&str> = path.splitn(3, '/').collect();
    let (obj_name, inst_str, res_name) = match parts.as_slice() {
        [a, b, c] => (*a, *b, *c),
        _ => {
            warn!(path, "IPC execute: path must be object/instance/resource");
            return serde_json::json!({"success": false});
        }
    };

    let inst_id: u16 = match inst_str.parse() {
        Ok(v) => v,
        Err(_) => {
            warn!(path, "IPC execute: invalid instance id");
            return serde_json::json!({"success": false});
        }
    };

    let Some(obj_id) = ctx.ipso.object_id_by_name(obj_name) else {
        warn!(path, obj_name, "IPC execute: unknown object name");
        return serde_json::json!({"success": false});
    };

    let Some((addr, dev_id, versions)) = ctx.registry.addr_and_id_by_endpoint(device).await else {
        warn!(device, "IPC execute: device not found");
        return serde_json::json!({"success": false});
    };

    let ver = versions.get(&obj_id).map(String::as_str);
    let Some(res_id) = ctx.ipso.resource_id_by_name(obj_id, res_name, ver) else {
        warn!(path, res_name, "IPC execute: unknown resource name");
        return serde_json::json!({"success": false});
    };

    let resource_path = ResourcePath {
        object_id: obj_id as u16,
        instance_id: inst_id,
        resource_id: res_id as u16,
    };

    let (response_tx, response_rx) = oneshot::channel();
    let op = PendingOperation {
        id: NEXT_OP_ID.fetch_add(1, Ordering::Relaxed),
        command: LwM2mCommand::Execute { path: resource_path, args: None },
        response_tx,
        created_at: Instant::now(),
        attempts: 0,
    };

    if ctx.dispatch_tx.send(DispatchRequest { addr, ops: vec![op] }).await.is_err() {
        warn!(device, path, "IPC execute: dispatch channel closed");
        return serde_json::json!({"success": false});
    }

    match tokio::time::timeout(Duration::from_secs(30), response_rx).await {
        Ok(Ok(Ok(ResourceValue::CoapResponse { class, detail }))) => {
            let code = (class as u16) * 32 + detail as u16;
            let name = coap_status_name(class, detail);
            info!(device, path, "IPC execute: {name}");
            serde_json::json!({
                "metadata": {
                    "lwm2m_client_id": dev_id,
                    "lwm2m_uri": [obj_id, inst_id, res_id],
                    "lwm2m_response": name,
                    "lwm2m_response_code": code,
                },
                "success": true
            })
        }
        Ok(Ok(Err(e))) => {
            warn!(device, path, "IPC execute: CoAP error {e:?}");
            serde_json::json!({"success": false})
        }
        Ok(Err(_)) => {
            warn!(device, path, "IPC execute: response channel dropped");
            serde_json::json!({"success": false})
        }
        Err(_) => {
            warn!(device, path, "IPC execute: timeout");
            serde_json::json!({"success": false})
        }
        Ok(Ok(Ok(_))) => serde_json::json!({"success": false}),
    }
}

fn coap_status_name(class: u8, detail: u8) -> &'static str {
    match (class, detail) {
        (2, 1) => "CREATED",
        (2, 2) => "DELETED",
        (2, 3) => "VALID",
        (2, 4) => "CHANGED",
        (2, 5) => "CONTENT",
        (4, 0) => "BAD_REQUEST",
        (4, 1) => "UNAUTHORIZED",
        (4, 2) => "BAD_OPTION",
        (4, 3) => "FORBIDDEN",
        (4, 4) => "NOT_FOUND",
        (4, 5) => "METHOD_NOT_ALLOWED",
        (4, 6) => "NOT_ACCEPTABLE",
        (4, 12) => "PRECONDITION_FAILED",
        (4, 13) => "REQUEST_ENTITY_TOO_LARGE",
        (4, 15) => "UNSUPPORTED_CONTENT_FORMAT",
        (5, 0) => "INTERNAL_SERVER_ERROR",
        (5, 1) => "NOT_IMPLEMENTED",
        (5, 2) => "BAD_GATEWAY",
        (5, 3) => "SERVICE_UNAVAILABLE",
        (5, 4) => "GATEWAY_TIMEOUT",
        (5, 5) => "PROXYING_NOT_SUPPORTED",
        _ => "UNKNOWN",
    }
}
