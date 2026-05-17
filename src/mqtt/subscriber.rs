use rumqttc::{AsyncClient, Event, EventLoop, Incoming, QoS};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    coap::server::DispatchRequest,
    error::Result,
    model::{
        LwM2mCommand, LwM2mError, LwM2mResult, MqttCommand, MqttCommandPayload, MqttResponse,
        MqttResponsePayload, ResourcePath,
    },
    registry::DeviceRegistry,
};

pub async fn run(
    client: AsyncClient,
    mut event_loop: EventLoop,
    registry: DeviceRegistry,
    coap_dispatch_tx: mpsc::Sender<DispatchRequest>,
    mqtt_out_tx: mpsc::Sender<MqttResponse>,
    topic_prefix: String,
    cancel: CancellationToken,
) -> Result<()> {
    let cmd_topic = format!("{topic_prefix}/cmd/+");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("MQTT subscriber shutting down");
                let _ = client.disconnect().await;
                return Ok(());
            }
            event = event_loop.poll() => {
                match event {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        info!("MQTT connected, subscribing to {cmd_topic}");
                        if let Err(e) = client.subscribe(&cmd_topic, QoS::AtLeastOnce).await {
                            error!("MQTT subscribe failed: {e}");
                        }
                    }
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        let payload_str = String::from_utf8_lossy(&publish.payload).into_owned();
                        debug!(topic = %publish.topic, "MQTT command received");

                        match parse_command(&payload_str) {
                            Ok(cmd) => {
                                handle_command(
                                    cmd,
                                    &registry,
                                    &coap_dispatch_tx,
                                    mqtt_out_tx.clone(),
                                )
                                .await;
                            }
                            Err(e) => {
                                warn!(topic = %publish.topic, "failed to parse command: {e}");
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!("MQTT connection error: {e}, will reconnect");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }
}

fn parse_command(json: &str) -> Result<MqttCommand> {
    let raw: MqttCommandPayload = serde_json::from_str(json)?;

    let command = match raw.operation.to_lowercase().as_str() {
        "read" => LwM2mCommand::Read { path: raw.path.clone() },
        "write" => {
            let value = encode_write_value(&raw.value)?;
            LwM2mCommand::Write {
                path: raw.path.clone(),
                value,
                content_format: raw.content_format.unwrap_or(0),
            }
        }
        "execute" => {
            let args = raw
                .value
                .as_ref()
                .and_then(|v| v.as_str())
                .map(|s| s.as_bytes().to_vec());
            LwM2mCommand::Execute { path: raw.path.clone(), args }
        }
        other => {
            return Err(crate::error::Error::Coap(format!("unknown operation: {other}")));
        }
    };

    Ok(MqttCommand {
        correlation_id: raw.correlation_id,
        endpoint: raw.endpoint,
        command,
        response_topic: raw.response_topic,
    })
}

fn encode_write_value(value: &Option<serde_json::Value>) -> Result<Vec<u8>> {
    match value {
        None => Ok(Vec::new()),
        Some(serde_json::Value::String(s)) => Ok(s.as_bytes().to_vec()),
        Some(serde_json::Value::Number(n)) => Ok(n.to_string().into_bytes()),
        Some(serde_json::Value::Bool(b)) => Ok(if *b { b"1".to_vec() } else { b"0".to_vec() }),
        Some(other) => Ok(other.to_string().into_bytes()),
    }
}

async fn handle_command(
    cmd: MqttCommand,
    registry: &DeviceRegistry,
    coap_dispatch_tx: &mpsc::Sender<DispatchRequest>,
    mqtt_out_tx: mpsc::Sender<MqttResponse>,
) {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel::<LwM2mResult>();

    let endpoint = cmd.endpoint.clone();
    let correlation_id = cmd.correlation_id.clone();
    let response_topic = cmd.response_topic.clone();
    let path = match &cmd.command {
        LwM2mCommand::Read { path }
        | LwM2mCommand::Write { path, .. }
        | LwM2mCommand::Execute { path, .. } => path.clone(),
    };

    match registry.enqueue_op(&endpoint, cmd.command, response_tx).await {
        Err(e) => {
            warn!(endpoint, "enqueue failed: {e}");
            // Publish error response immediately.
            let resp = make_response(
                correlation_id,
                endpoint,
                path,
                response_topic,
                Err(LwM2mError::NotFound),
            );
            let _ = mqtt_out_tx.send(resp).await;
            return;
        }
        Ok(_op_id) => {}
    }

    // Spawn a task that waits for the CoAP result and publishes it.
    let out_tx = mqtt_out_tx.clone();
    let endpoint_for_spawn = endpoint.clone();
    tokio::spawn(async move {
        let result = match response_rx.await {
            Ok(r) => r,
            Err(_) => Err(LwM2mError::Timeout),
        };
        let resp = make_response(correlation_id, endpoint_for_spawn, path, response_topic, result);
        let _ = out_tx.send(resp).await;
    });

    // If the device is currently registered and reachable, trigger immediate dispatch.
    if let Some(addr) = registry.addr_for_endpoint(&endpoint).await {
        let ops = registry.drain_pending(addr).await;
        if !ops.is_empty() {
            let _ = coap_dispatch_tx.send(DispatchRequest { addr, ops }).await;
        }
    }
}

fn make_response(
    correlation_id: String,
    endpoint: String,
    path: ResourcePath,
    response_topic: String,
    result: LwM2mResult,
) -> MqttResponse {
    let (value, error) = match result {
        Ok(v) => (Some(v), None),
        Err(e) => (None, Some(e)),
    };
    MqttResponse {
        response_topic,
        payload: MqttResponsePayload {
            correlation_id,
            endpoint,
            path,
            value,
            error,
        },
    }
}

