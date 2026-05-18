mod common;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn ipc_read_devices_returns_empty_payload() {
    let gw = common::TestGateway::start().await;

    let stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);

    writer
        .write_all(b"[{\"op\":\"read\",\"entity\":{\"service\":\"lwm2mserver\",\"path\":\"devices\"}}]\n")
        .await
        .unwrap();

    let mut lines = BufReader::new(reader).lines();
    let line = tokio::time::timeout(Duration::from_secs(1), lines.next_line())
        .await
        .expect("timeout")
        .expect("io error")
        .expect("connection closed");

    let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(resp[0]["success"], true);
    assert_eq!(resp[0]["payload"], serde_json::json!({}));

    gw.stop().await;
}

#[tokio::test]
async fn ipc_invalid_json_returns_error() {
    let gw = common::TestGateway::start().await;

    let stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);

    writer.write_all(b"not valid json\n").await.unwrap();

    let mut lines = BufReader::new(reader).lines();
    let line = tokio::time::timeout(Duration::from_secs(1), lines.next_line())
        .await
        .expect("timeout")
        .expect("io error")
        .expect("connection closed");

    let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(resp[0]["success"], false);

    gw.stop().await;
}
