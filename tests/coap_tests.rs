mod common;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UdpSocket;

use coap_lite::{CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status};

fn registration_packet() -> Vec<u8> {
    let mut pkt = Packet::new();
    pkt.header.set_type(MessageType::Confirmable);
    pkt.header.code = MessageClass::Request(Method::Post);
    pkt.header.message_id = 0x0001;
    pkt.set_token(vec![0x01]);
    pkt.add_option(coap_lite::CoapOption::UriPath, b"rd".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"ep=test-device".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"lt=3600".to_vec());
    pkt.add_option(coap_lite::CoapOption::UriQuery, b"b=U".to_vec());
    pkt.to_bytes().unwrap()
}

#[tokio::test]
async fn coap_registration_returns_created() {
    let gw = common::TestGateway::start().await;

    let sock = UdpSocket::bind("[::1]:0").await.unwrap();
    sock.send_to(&registration_packet(), gw.coap_addr).await.unwrap();

    let mut buf = vec![0u8; 256];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("timeout")
        .unwrap();

    let resp = Packet::from_bytes(&buf[..len]).unwrap();
    assert_eq!(resp.header.code, MessageClass::Response(Status::Created));

    let location: Vec<String> = resp
        .get_option(coap_lite::CoapOption::LocationPath)
        .map(|list| {
            list.iter()
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect()
        })
        .unwrap_or_default();

    assert_eq!(location.first().map(String::as_str), Some("rd"));
    assert!(location.get(1).is_some(), "expected numeric registration id");

    gw.stop().await;
}

#[tokio::test]
async fn coap_registration_then_ipc_read_devices() {
    let gw = common::TestGateway::start().await;

    // Register a device via CoAP.
    let sock = UdpSocket::bind("[::1]:0").await.unwrap();
    sock.send_to(&registration_packet(), gw.coap_addr).await.unwrap();
    let mut buf = vec![0u8; 256];
    tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("registration timeout")
        .unwrap();

    // Query IPC for devices. Currently returns empty payload — documents the gap
    // between CoAP registration and IPC-exposed state.
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
    // IPC not yet wired to the device registry; payload stays empty until that is implemented.
    assert_eq!(resp[0]["payload"], serde_json::json!({}));

    gw.stop().await;
}

/// A device-initiated DELETE (e.g. before a firmware-update reboot) must not exclude the device.
/// The device should remain in the included set and no "delete" event should be emitted.
#[tokio::test]
async fn coap_deregister_included_device_preserves_inclusion() {
    let gw = common::TestGateway::start().await;
    let mut events = gw.event_sender.subscribe();

    // Device is already included before it registers (the normal post-bootstrap state).
    gw.bootstrap_registry.mark_included("test-device").await;
    assert!(gw.bootstrap_registry.is_included("test-device").await);

    // Register the device.
    let sock = UdpSocket::bind("[::1]:0").await.unwrap();
    sock.send_to(&registration_packet(), gw.coap_addr).await.unwrap();

    let mut buf = vec![0u8; 256];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("registration timeout")
        .unwrap();

    let reg_resp = Packet::from_bytes(&buf[..len]).unwrap();
    assert_eq!(reg_resp.header.code, MessageClass::Response(Status::Created));

    // Extract the numeric registration ID from the Location-Path response.
    let reg_id = reg_resp
        .get_option(CoapOption::LocationPath)
        .and_then(|list| list.iter().nth(1))
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .expect("missing registration id in Location-Path");

    // Device sends DELETE /rd/<id> — as it would before a firmware-update reboot.
    let mut del = Packet::new();
    del.header.set_type(MessageType::Confirmable);
    del.header.code = MessageClass::Request(Method::Delete);
    del.header.message_id = 0x0002;
    del.set_token(vec![0x02]);
    del.add_option(CoapOption::UriPath, b"rd".to_vec());
    del.add_option(CoapOption::UriPath, reg_id.into_bytes());
    sock.send_to(&del.to_bytes().unwrap(), gw.coap_addr).await.unwrap();

    let (len, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("delete response timeout")
        .unwrap();
    let del_resp = Packet::from_bytes(&buf[..len]).unwrap();
    assert_eq!(del_resp.header.code, MessageClass::Response(Status::Deleted));

    // Give the server a moment to process.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Inclusion state must be preserved.
    assert!(
        gw.bootstrap_registry.is_included("test-device").await,
        "device must remain included after self-deregistration"
    );

    // No "delete" op event must have been emitted.
    let mut delete_event_sent = false;
    while let Ok(msg) = events.try_recv() {
        let json: serde_json::Value = serde_json::from_str(&msg).unwrap_or_default();
        if json.get(0).and_then(|e| e.get("op")).map(|op| op == "delete").unwrap_or(false) {
            delete_event_sent = true;
        }
    }
    assert!(!delete_event_sent, "delete event must not be emitted for a device-initiated deregistration");

    gw.stop().await;
}
