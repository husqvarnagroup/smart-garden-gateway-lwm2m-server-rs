use rumqttc::{AsyncClient, QoS};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{error::Result, model::MqttResponse};

pub async fn run(
    client: AsyncClient,
    mut out_rx: mpsc::Receiver<MqttResponse>,
    cancel: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("MQTT publisher shutting down");
                return Ok(());
            }
            Some(resp) = out_rx.recv() => {
                match serde_json::to_vec(&resp.payload) {
                    Ok(json) => {
                        if let Err(e) = client
                            .publish(&resp.response_topic, QoS::AtLeastOnce, false, json)
                            .await
                        {
                            error!(topic = %resp.response_topic, "publish failed: {e}");
                        }
                    }
                    Err(e) => {
                        warn!("failed to serialize response: {e}");
                    }
                }
            }
        }
    }
}
