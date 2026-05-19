mod common;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Connect to the event socket and verify it stays open (no immediate close).
#[tokio::test]
async fn event_socket_accepts_connection() {
    let gw = common::TestGateway::start().await;

    let stream = tokio::net::UnixStream::connect(&gw.event_path).await.unwrap();
    let (reader, _writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();

    // No events emitted yet — expect a timeout, not a close.
    let result = tokio::time::timeout(Duration::from_millis(100), lines.next_line()).await;
    assert!(result.is_err(), "expected timeout, not a close or data");

    gw.stop().await;
}

/// Full inclusion flow:
///   1. Manually register a device as includable in the bootstrap registry.
///   2. Connect to the event socket — the pending event should already have been sent
///      but we missed it (broadcast), so we issue the execute command and check the
///      inclusion_started event that fires immediately after approval.
///   3. Send `execute includable_device/<id>/include` via the IPC command socket.
///   4. Verify the event socket receives `inclusion_started = true`.
#[tokio::test]
async fn ipc_execute_include_triggers_event() {
    let gw = common::TestGateway::start().await;

    // Pre-register a fake device as includable so we can test the approval path
    // without running the full CoAP bootstrap sequence.
    let fake_addr: std::net::SocketAddr = "[::1]:9999".parse().unwrap();
    let (id, approval_rx) = gw
        .bootstrap_registry
        .register_includable("test-device".to_string(), fake_addr)
        .await
        .expect("register_includable");

    // Simulate the approval-waiting task that handle_ack normally spawns.
    // The real task would also run the bootstrap write phase, but here we only
    // need the event that fires immediately upon approval.
    let es = gw.event_sender.clone();
    tokio::spawn(async move {
        if approval_rx.await.is_ok() {
            es.send_includable(id, "test-device", true, false);
        }
    });

    // Connect to the event socket BEFORE issuing the command so we catch the event.
    let ev_stream = tokio::net::UnixStream::connect(&gw.event_path).await.unwrap();
    let (ev_reader, _) = tokio::io::split(ev_stream);
    let mut ev_lines = BufReader::new(ev_reader).lines();

    // Send the execute command over IPC.
    let cmd_stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (cmd_reader, mut cmd_writer) = tokio::io::split(cmd_stream);

    let cmd = format!(
        "[{{\"op\":\"execute\",\"entity\":{{\"service\":\"lwm2mserver\",\"path\":\"includable_device/{id}/include\"}}}}]\n"
    );
    cmd_writer.write_all(cmd.as_bytes()).await.unwrap();

    // Verify IPC returns success.
    let mut cmd_lines = BufReader::new(cmd_reader).lines();
    let ipc_resp = tokio::time::timeout(Duration::from_secs(1), cmd_lines.next_line())
        .await
        .expect("IPC timeout")
        .expect("io error")
        .expect("connection closed");
    let ipc_json: serde_json::Value = serde_json::from_str(&ipc_resp).unwrap();
    assert_eq!(ipc_json[0]["success"], true);

    // The approval task emits inclusion_started=true on the event socket.
    // The write phase itself won't complete (no real device), but the event fires first.
    let ev_line = tokio::time::timeout(Duration::from_secs(1), ev_lines.next_line())
        .await
        .expect("event timeout")
        .expect("io error")
        .expect("connection closed");

    let ev: serde_json::Value = serde_json::from_str(&ev_line).unwrap();
    assert_eq!(ev[0]["op"], "update");
    assert_eq!(
        ev[0]["entity"]["path"],
        format!("includable_device/{id}")
    );
    assert_eq!(ev[0]["payload"]["inclusion_started"]["vb"], true);
    assert_eq!(ev[0]["payload"]["inclusion_completed"]["vb"], false);
    assert_eq!(ev[0]["payload"]["identifier"]["vs"], "test-device");

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
