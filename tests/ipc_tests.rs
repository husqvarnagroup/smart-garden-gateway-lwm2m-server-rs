mod common;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Helper: send an IPC request and return the parsed JSON response.
async fn ipc_request(gw: &common::TestGateway, request: &str) -> serde_json::Value {
    let stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);

    let mut msg = request.to_owned();
    if !msg.ends_with('\n') {
        msg.push('\n');
    }
    writer.write_all(msg.as_bytes()).await.unwrap();

    let mut lines = BufReader::new(reader).lines();
    let line = tokio::time::timeout(Duration::from_secs(2), lines.next_line())
        .await
        .expect("timeout")
        .expect("io error")
        .expect("connection closed");

    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn ipc_read_devices_returns_empty_payload() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(
        &gw,
        r#"[{"op":"read","entity":{"service":"lwm2mserver","path":"devices"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], true);
    assert_eq!(resp[0]["payload"], serde_json::json!({}));
    gw.stop().await;
}

#[tokio::test]
async fn ipc_invalid_json_returns_error() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, "not valid json").await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

#[tokio::test]
async fn ipc_read_devices_returns_included_device() {
    let gw = common::TestGateway::start().await;

    // Include a device and register it via CoAP so it appears in the device list.
    gw.bootstrap_registry.mark_included("DEVICE001").await;

    let resp = ipc_request(&gw, r#"[{"op":"read","entity":{"path":"devices"}}]"#).await;
    assert_eq!(resp[0]["success"], true);
    // Device should appear in payload with connection_status since it's in the included list.
    assert!(resp[0]["payload"]["DEVICE001"].is_object());

    gw.stop().await;
}

#[tokio::test]
async fn ipc_read_unknown_path_returns_failure() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, r#"[{"op":"read","entity":{"path":"nonexistent"}}]"#).await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

#[tokio::test]
async fn ipc_execute_include_invalid_id_returns_failure() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(
        &gw,
        r#"[{"op":"execute","entity":{"path":"includable_device/abc/include"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

#[tokio::test]
async fn ipc_execute_include_unknown_id_returns_failure() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(
        &gw,
        r#"[{"op":"execute","entity":{"path":"includable_device/9999/include"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

#[tokio::test]
async fn ipc_execute_exclude_not_connected_device() {
    let gw = common::TestGateway::start().await;

    // Include a device but don't register it (not connected).
    gw.bootstrap_registry.mark_included("DEVICE_X").await;

    let resp = ipc_request(
        &gw,
        r#"[{"op":"execute","entity":{"path":"device/0/factory_reset","device":"DEVICE_X"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], true);
    // Should indicate device was not connected.
    assert_eq!(resp[0]["metadata"]["exclusion"], "device_not_connected");

    // Device should be removed from the included list.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !gw.bootstrap_registry.is_included("DEVICE_X").await,
        "device should no longer be included after exclusion"
    );

    gw.stop().await;
}

#[tokio::test]
async fn ipc_unhandled_op_returns_success() {
    let gw = common::TestGateway::start().await;
    // Unknown operations return success: true (pass-through behavior).
    let resp = ipc_request(&gw, r#"[{"op":"subscribe","entity":{"path":"something"}}]"#).await;
    assert_eq!(resp[0]["success"], true);
    gw.stop().await;
}

#[tokio::test]
async fn ipc_multiple_requests_in_batch() {
    let gw = common::TestGateway::start().await;

    // Send a batch of two requests in one line.
    let resp = ipc_request(
        &gw,
        r#"[{"op":"read","entity":{"path":"devices"}},{"op":"read","entity":{"path":"nonexistent"}}]"#,
    )
    .await;
    // First request: read devices → success.
    assert_eq!(resp[0]["success"], true);
    // Second request: unknown path → failure.
    assert_eq!(resp[1]["success"], false);

    gw.stop().await;
}

#[tokio::test]
async fn ipc_multiple_requests_on_same_connection() {
    let gw = common::TestGateway::start().await;

    let stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();

    // First request.
    writer
        .write_all(b"[{\"op\":\"read\",\"entity\":{\"path\":\"devices\"}}]\n")
        .await
        .unwrap();
    let line1 = tokio::time::timeout(Duration::from_secs(1), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let resp1: serde_json::Value = serde_json::from_str(&line1).unwrap();
    assert_eq!(resp1[0]["success"], true);

    // Second request on the same connection.
    writer
        .write_all(b"[{\"op\":\"read\",\"entity\":{\"path\":\"devices\"}}]\n")
        .await
        .unwrap();
    let line2 = tokio::time::timeout(Duration::from_secs(1), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let resp2: serde_json::Value = serde_json::from_str(&line2).unwrap();
    assert_eq!(resp2[0]["success"], true);

    gw.stop().await;
}

#[tokio::test]
async fn ipc_execute_on_unknown_device_returns_failure() {
    let gw = common::TestGateway::start().await;

    // Try to execute a resource on a device that doesn't exist in the registry.
    let resp = ipc_request(
        &gw,
        r#"[{"op":"execute","entity":{"path":"some_object/0/some_resource","device":"UNKNOWN"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);

    gw.stop().await;
}

#[tokio::test]
async fn ipc_write_on_unknown_device_returns_failure() {
    let gw = common::TestGateway::start().await;

    let resp = ipc_request(
        &gw,
        r#"[{"op":"write","entity":{"path":"some_object/0/some_resource","device":"UNKNOWN"},"payload":{"vs":"hello"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);

    gw.stop().await;
}

#[tokio::test]
async fn ipc_write_fota_missing_payload_returns_failure() {
    let gw = common::TestGateway::start().await;

    // Include and register a device so it's "connected" for the FOTA path.
    gw.bootstrap_registry.mark_included("FOTA_DEV").await;

    // Register via CoAP.
    let sock = tokio::net::UdpSocket::bind("[::1]:0").await.unwrap();
    let mut pkt = coap_lite::Packet::new();
    pkt.header.set_type(coap_lite::MessageType::Confirmable);
    pkt.header.code = coap_lite::MessageClass::Request(coap_lite::RequestType::Post);
    pkt.header.message_id = 0x0010;
    pkt.set_token(vec![0x10]);
    pkt.add_option(coap_lite::CoapOption::UriPath, b"rd".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"ep=FOTA_DEV".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"lt=3600".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"b=U".to_vec());
    sock.send_to(&pkt.to_bytes().unwrap(), gw.coap_addr)
        .await
        .unwrap();
    let mut buf = vec![0u8; 256];
    tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();

    // Try FOTA write without the required "vo" field.
    let resp = ipc_request(
        &gw,
        r#"[{"op":"write","entity":{"path":"firmware_update/0/package","device":"FOTA_DEV"},"payload":{}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);

    gw.stop().await;
}

#[tokio::test]
async fn ipc_write_fota_invalid_base64_returns_failure() {
    let gw = common::TestGateway::start().await;

    gw.bootstrap_registry.mark_included("FOTA_DEV2").await;

    // Register via CoAP.
    let sock = tokio::net::UdpSocket::bind("[::1]:0").await.unwrap();
    let mut pkt = coap_lite::Packet::new();
    pkt.header.set_type(coap_lite::MessageType::Confirmable);
    pkt.header.code = coap_lite::MessageClass::Request(coap_lite::RequestType::Post);
    pkt.header.message_id = 0x0011;
    pkt.set_token(vec![0x11]);
    pkt.add_option(coap_lite::CoapOption::UriPath, b"rd".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"ep=FOTA_DEV2".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"lt=3600".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"b=U".to_vec());
    sock.send_to(&pkt.to_bytes().unwrap(), gw.coap_addr)
        .await
        .unwrap();
    let mut buf = vec![0u8; 256];
    tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();

    // Try FOTA write with invalid base64.
    let resp = ipc_request(
        &gw,
        r#"[{"op":"write","entity":{"path":"firmware_update/0/package","device":"FOTA_DEV2"},"payload":{"vo":"not!valid!base64!!!"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);

    gw.stop().await;
}

#[tokio::test]
async fn ipc_write_fota_device_not_connected_returns_failure() {
    let gw = common::TestGateway::start().await;

    // Device is included but never registered (not connected).
    gw.bootstrap_registry.mark_included("FOTA_OFFLINE").await;

    let resp = ipc_request(
        &gw,
        r#"[{"op":"write","entity":{"path":"firmware_update/0/package","device":"FOTA_OFFLINE"},"payload":{"vo":"AQID"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);

    gw.stop().await;
}

#[tokio::test]
async fn ipc_empty_batch_returns_empty_array() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, "[]").await;
    assert_eq!(resp, serde_json::json!([]));
    gw.stop().await;
}

// ── Unexpected behaviour & error handling ────────────────────────────────────

/// A JSON object (not an array) should be treated as invalid input.
#[tokio::test]
async fn ipc_json_object_instead_of_array_returns_error() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, r#"{"op":"read","entity":{"path":"devices"}}"#).await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// A JSON null literal is not a valid request.
#[tokio::test]
async fn ipc_null_literal_returns_error() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, "null").await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// A request with a missing "op" field should be handled gracefully.
#[tokio::test]
async fn ipc_missing_op_field_returns_success() {
    let gw = common::TestGateway::start().await;
    // No "op" key at all — falls into the catch-all which returns success.
    let resp = ipc_request(&gw, r#"[{"entity":{"path":"devices"}}]"#).await;
    assert_eq!(resp[0]["success"], true);
    gw.stop().await;
}

/// A request with a missing "entity" field should be handled gracefully.
#[tokio::test]
async fn ipc_missing_entity_returns_success() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, r#"[{"op":"read"}]"#).await;
    // "read" with empty path falls into the unhandled-read branch → failure.
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// An empty string line should be treated as invalid JSON.
#[tokio::test]
async fn ipc_empty_string_returns_error() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, "").await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// Very large input should not crash the server.
#[tokio::test]
async fn ipc_oversized_input_returns_error() {
    let gw = common::TestGateway::start().await;
    // Send 1 MB of garbage — should fail to parse as JSON.
    let junk = "x".repeat(1_000_000);
    let resp = ipc_request(&gw, &junk).await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// Execute with a path that has too few segments (missing resource) fails.
#[tokio::test]
async fn ipc_execute_path_too_few_segments_returns_failure() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(
        &gw,
        r#"[{"op":"execute","entity":{"path":"object_only","device":"DEV"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// Execute with a path that has a non-numeric instance ID fails.
#[tokio::test]
async fn ipc_execute_path_invalid_instance_id_returns_failure() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(
        &gw,
        r#"[{"op":"execute","entity":{"path":"device/abc/reboot","device":"DEV"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// Write with no recognised value key in payload fails.
#[tokio::test]
async fn ipc_write_unrecognised_payload_format_returns_failure() {
    let gw = common::TestGateway::start().await;

    // Register a device so it's in the registry.
    gw.bootstrap_registry.mark_included("WRDEV").await;
    let sock = tokio::net::UdpSocket::bind("[::1]:0").await.unwrap();
    let mut pkt = coap_lite::Packet::new();
    pkt.header.set_type(coap_lite::MessageType::Confirmable);
    pkt.header.code = coap_lite::MessageClass::Request(coap_lite::RequestType::Post);
    pkt.header.message_id = 0x0020;
    pkt.set_token(vec![0x20]);
    pkt.add_option(coap_lite::CoapOption::UriPath, b"rd".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"ep=WRDEV".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"lt=3600".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"b=U".to_vec());
    sock.send_to(&pkt.to_bytes().unwrap(), gw.coap_addr)
        .await
        .unwrap();
    let mut buf = vec![0u8; 256];
    tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();

    // Write with payload that has no vs/vi/vf/vb/vt/vo key — unrecognised.
    let resp = ipc_request(
        &gw,
        r#"[{"op":"write","entity":{"path":"device/0/manufacturer","device":"WRDEV"},"payload":{"unknown_key":"value"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);

    gw.stop().await;
}

/// Sending binary/non-UTF8 data on the IPC socket should not crash the server;
/// the connection is simply dropped and the server stays healthy.
#[tokio::test]
async fn ipc_binary_data_does_not_crash_server() {
    let gw = common::TestGateway::start().await;

    // Send raw binary (invalid UTF-8) on the socket.
    let stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (_, mut writer) = tokio::io::split(stream);
    writer
        .write_all(&[0xFF, 0xFE, 0xFD, 0x00, 0x0A])
        .await
        .unwrap();
    drop(writer);

    // Give it a moment, then verify the server is still accepting connections.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = ipc_request(&gw, r#"[{"op":"read","entity":{"path":"devices"}}]"#).await;
    assert_eq!(resp[0]["success"], true);

    gw.stop().await;
}

/// Abruptly dropping the connection mid-request should not crash the server.
#[tokio::test]
async fn ipc_abrupt_disconnect_does_not_crash_server() {
    let gw = common::TestGateway::start().await;

    // Open and immediately close without sending a newline.
    let stream = tokio::net::UnixStream::connect(&gw.ipc_path).await.unwrap();
    let (_, mut writer) = tokio::io::split(stream);
    writer.write_all(b"[{\"op\":\"read").await.unwrap();
    drop(writer); // close mid-JSON

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Server should still work.
    let resp = ipc_request(&gw, r#"[{"op":"read","entity":{"path":"devices"}}]"#).await;
    assert_eq!(resp[0]["success"], true);

    gw.stop().await;
}

/// Multiple concurrent connections should all be handled independently.
#[tokio::test]
async fn ipc_concurrent_connections_handled() {
    let gw = common::TestGateway::start().await;

    let mut handles = vec![];
    for _ in 0..5 {
        let path = gw.ipc_path.clone();
        handles.push(tokio::spawn(async move {
            let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
            let (reader, mut writer) = tokio::io::split(stream);
            writer
                .write_all(b"[{\"op\":\"read\",\"entity\":{\"path\":\"devices\"}}]\n")
                .await
                .unwrap();
            let mut lines = BufReader::new(reader).lines();
            let line = tokio::time::timeout(Duration::from_secs(2), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(resp[0]["success"], true);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    gw.stop().await;
}

/// The "execute" path for inclusion with an empty path segment should fail gracefully.
#[tokio::test]
async fn ipc_execute_include_empty_id_returns_failure() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(
        &gw,
        r#"[{"op":"execute","entity":{"path":"includable_device//include"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], false);
    gw.stop().await;
}

/// Write with an empty path should be rejected.
#[tokio::test]
async fn ipc_write_empty_path_returns_success_passthrough() {
    let gw = common::TestGateway::start().await;
    // "write" with empty path doesn't match any handler and falls through
    // to the catch-all which returns success: true.
    let resp = ipc_request(
        &gw,
        r#"[{"op":"write","entity":{"path":""},"payload":{"vs":"x"}}]"#,
    )
    .await;
    assert_eq!(resp[0]["success"], true);
    gw.stop().await;
}

/// Execute with an empty path should also fall through.
#[tokio::test]
async fn ipc_execute_empty_path_returns_success_passthrough() {
    let gw = common::TestGateway::start().await;
    let resp = ipc_request(&gw, r#"[{"op":"execute","entity":{"path":""}}]"#).await;
    assert_eq!(resp[0]["success"], true);
    gw.stop().await;
}
