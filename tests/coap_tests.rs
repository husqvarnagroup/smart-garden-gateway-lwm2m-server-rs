mod common;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UdpSocket;

use coap_lite::{MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status};

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
