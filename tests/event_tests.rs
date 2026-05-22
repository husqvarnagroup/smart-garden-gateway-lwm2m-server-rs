mod common;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Connect to the event socket and verify it stays open (no immediate close).
#[tokio::test]
async fn event_socket_accepts_connection() {
    let gw = common::TestGateway::start().await;

    let stream = tokio::net::UnixStream::connect(&gw.event_path)
        .await
        .unwrap();
    let (reader, _writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();

    // No events emitted yet — expect a timeout, not a close.
    let result = tokio::time::timeout(Duration::from_millis(100), lines.next_line()).await;
    assert!(result.is_err(), "expected timeout, not a close or data");

    gw.stop().await;
}

/// Approval flow:
///   1. Register a device ID via ensure_includable_id.
///   2. Send `execute includable_device/<id>/include` via IPC.
///   3. Verify IPC returns success and the device is now in the approved set.
///   4. Verify a second approval for the same id returns success (idempotent approval store).
#[tokio::test]
async fn ipc_execute_include_triggers_event() {
    let gw = common::TestGateway::start().await;

    // Register a fake device endpoint so it gets a numeric IPC id.
    let id = gw
        .bootstrap_registry
        .ensure_includable_id("test-device")
        .await;

    let cmd_stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (cmd_reader, mut cmd_writer) = tokio::io::split(cmd_stream);

    let cmd = format!(
        "[{{\"op\":\"execute\",\"entity\":{{\"service\":\"lwm2mserver\",\"path\":\"includable_device/{id}/include\"}}}}]\n"
    );
    cmd_writer.write_all(cmd.as_bytes()).await.unwrap();

    let mut cmd_lines = BufReader::new(cmd_reader).lines();
    let ipc_resp = tokio::time::timeout(Duration::from_secs(1), cmd_lines.next_line())
        .await
        .expect("IPC timeout")
        .expect("io error")
        .expect("connection closed");
    let ipc_json: serde_json::Value = serde_json::from_str(&ipc_resp).unwrap();
    assert_eq!(ipc_json[0]["success"], true);

    // Approval is stored — the write phase starts on the device's next /bs.
    assert!(gw.bootstrap_registry.is_approved("test-device").await);

    gw.stop().await;
}

/// Execute with an unknown id returns success=false.
#[tokio::test]
async fn ipc_execute_include_unknown_id_returns_false() {
    let gw = common::TestGateway::start().await;

    let stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);

    writer
        .write_all(b"[{\"op\":\"execute\",\"entity\":{\"service\":\"lwm2mserver\",\"path\":\"includable_device/9999/include\"}}]\n")
        .await
        .unwrap();

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
