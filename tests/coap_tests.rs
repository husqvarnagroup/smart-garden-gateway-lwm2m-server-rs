mod common;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UdpSocket;

use coap_lite::{
    CoapOption, MessageClass, MessageType, Packet, RequestType as Method, ResponseType as Status,
};

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
    sock.send_to(&registration_packet(), gw.coap_addr)
        .await
        .unwrap();

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
    assert!(
        location.get(1).is_some(),
        "expected numeric registration id"
    );

    gw.stop().await;
}

#[tokio::test]
async fn coap_registration_then_ipc_read_devices() {
    let gw = common::TestGateway::start().await;

    // Register a device via CoAP.
    let sock = UdpSocket::bind("[::1]:0").await.unwrap();
    sock.send_to(&registration_packet(), gw.coap_addr)
        .await
        .unwrap();
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
        .write_all(
            b"[{\"op\":\"read\",\"entity\":{\"service\":\"lwm2mserver\",\"path\":\"devices\"}}]\n",
        )
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
    sock.send_to(&registration_packet(), gw.coap_addr)
        .await
        .unwrap();

    let mut buf = vec![0u8; 256];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("registration timeout")
        .unwrap();

    let reg_resp = Packet::from_bytes(&buf[..len]).unwrap();
    assert_eq!(
        reg_resp.header.code,
        MessageClass::Response(Status::Created)
    );

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
    sock.send_to(&del.to_bytes().unwrap(), gw.coap_addr)
        .await
        .unwrap();

    let (len, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("delete response timeout")
        .unwrap();
    let del_resp = Packet::from_bytes(&buf[..len]).unwrap();
    assert_eq!(
        del_resp.header.code,
        MessageClass::Response(Status::Deleted)
    );

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
        if json
            .get(0)
            .and_then(|e| e.get("op"))
            .map(|op| op == "delete")
            .unwrap_or(false)
        {
            delete_event_sent = true;
        }
    }
    assert!(
        !delete_event_sent,
        "delete event must not be emitted for a device-initiated deregistration"
    );

    gw.stop().await;
}

// ── Property-based registration test ─────────────────────────────────────────

use coap_lite::link_format::LinkFormatParser;
use proptest::prelude::{ProptestConfig, Strategy};
use proptest::{prop_assert, prop_assert_eq};

/// Generates a valid LwM2M link-format payload such as
/// `</>;rt="oma.lwm2m";ct=112,</3>;ver=1.0,</3/0>`.
fn link_format_payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    use proptest::collection::vec as pvec;
    use proptest::prelude::any;

    // (object_id, has_version, version_major, version_minor, instance_ids)
    let object = (
        1u16..=3999u16,
        any::<bool>(),
        0u8..=9u8,
        0u8..=9u8,
        pvec(0u16..=0xfffeu16, 1..=5usize),
    );

    pvec(object, 1..=5usize).prop_map(|objects| {
        let mut parts: Vec<String> = vec![r#"</>;rt="oma.lwm2m";ct=112"#.to_string()];
        for (obj_id, has_version, major, minor, instances) in objects {
            let version = if has_version {
                format!(";ver={major}.{minor}")
            } else {
                String::new()
            };
            parts.push(format!("</{obj_id}>{version}"));
            for inst in instances {
                parts.push(format!("</{obj_id}/{inst}>"));
            }
        }
        parts.join(",").into_bytes()
    })
}

fn is_valid_link_format(payload: &[u8]) -> bool {
    let s = String::from_utf8_lossy(payload);
    LinkFormatParser::new(&s).all(|r| r.is_ok())
}

proptest::proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]
    #[test]
    fn coap_registration_accepts_arbitrary_valid_params(
        endpoint in "[a-zA-Z0-9_-]{3,20}",
        lifetime in 60u32..172800u32,
        payload in link_format_payload_strategy(),
    ) {
        prop_assert!(
            is_valid_link_format(&payload),
            "generated payload is not valid link-format: {:?}",
            String::from_utf8_lossy(&payload)
        );

        // proptest is sync; drive tokio manually per case.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let gw = common::TestGateway::start().await;
            // Included device (normal post-bootstrap state): keeps the server
            // from interleaving a factory-reset request with the response.
            gw.bootstrap_registry.mark_included(&endpoint).await;

            let mut pkt = Packet::new();
            pkt.header.set_type(MessageType::Confirmable);
            pkt.header.code = MessageClass::Request(Method::Post);
            pkt.header.message_id = 0x0001;
            pkt.set_token(vec![0x01]);
            pkt.add_option(CoapOption::UriPath, b"rd".to_vec());
            pkt.set_content_format(coap_lite::ContentFormat::ApplicationLinkFormat);
            pkt.add_option(CoapOption::UriQuery, b"lwm2m=1.1".to_vec());
            pkt.add_option(CoapOption::UriQuery, format!("ep={endpoint}").into_bytes());
            pkt.add_option(CoapOption::UriQuery, format!("lt={lifetime}").into_bytes());
            pkt.payload = payload.clone();

            let sock = UdpSocket::bind("[::1]:0").await.unwrap();
            sock.send_to(&pkt.to_bytes().unwrap(), gw.coap_addr)
                .await
                .unwrap();

            let mut buf = vec![0u8; 256];
            let (len, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
                .await
                .expect("registration timeout")
                .unwrap();

            let resp = Packet::from_bytes(&buf[..len]).unwrap();
            prop_assert_eq!(resp.header.get_type(), MessageType::Acknowledgement);
            prop_assert_eq!(resp.header.code, MessageClass::Response(Status::Created));
            prop_assert!(resp.payload.is_empty());

            let location: Vec<String> = resp
                .get_option(CoapOption::LocationPath)
                .map(|list| {
                    list.iter()
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .collect()
                })
                .unwrap_or_default();
            prop_assert_eq!(location.len(), 2);
            prop_assert_eq!(location[0].as_str(), "rd");
            prop_assert!(
                location[1].parse::<u64>().is_ok(),
                "registration id must be numeric, got {:?}",
                location[1]
            );

            gw.stop().await;
            Ok(())
        })?;
    }
}

// ── End-to-end retransmission and block-transfer tests ───────────────────────

fn registration_packet_for(ep: &str, mid: u16) -> Vec<u8> {
    let mut pkt = Packet::new();
    pkt.header.set_type(MessageType::Confirmable);
    pkt.header.code = MessageClass::Request(Method::Post);
    pkt.header.message_id = mid;
    pkt.set_token(vec![0x01]);
    pkt.add_option(CoapOption::UriPath, b"rd".to_vec());
    pkt.add_option(CoapOption::UriQuery, format!("ep={ep}").into_bytes());
    pkt.add_option(CoapOption::UriQuery, b"lt=3600".to_vec());
    pkt.to_bytes().unwrap()
}

async fn recv_packet(sock: &UdpSocket) -> Vec<u8> {
    let mut buf = vec![0u8; 512];
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await
        .expect("response timeout")
        .unwrap();
    buf[..len].to_vec()
}

/// RFC 7252 §4.5: a retransmitted CON (same peer, same Message ID) must be
/// answered with the cached response instead of being processed again.
#[tokio::test]
async fn coap_duplicate_mid_replays_cached_response() {
    let gw = common::TestGateway::start().await;
    gw.bootstrap_registry.mark_included("test-device").await;
    let sock = UdpSocket::bind("[::1]:0").await.unwrap();
    let req = registration_packet();

    sock.send_to(&req, gw.coap_addr).await.unwrap();
    let first = recv_packet(&sock).await;
    let resp = Packet::from_bytes(&first).unwrap();
    assert_eq!(resp.header.code, MessageClass::Response(Status::Created));

    // Retransmit the identical datagram.
    sock.send_to(&req, gw.coap_addr).await.unwrap();
    let second = recv_packet(&sock).await;

    assert_eq!(
        first, second,
        "retransmission must be answered with the identical cached response"
    );

    gw.stop().await;
}

#[tokio::test]
async fn coap_two_devices_get_distinct_registration_ids() {
    let gw = common::TestGateway::start().await;

    let mut ids = Vec::new();
    for ep in ["dev-a", "dev-b"] {
        gw.bootstrap_registry.mark_included(ep).await;
        let sock = UdpSocket::bind("[::1]:0").await.unwrap();
        sock.send_to(&registration_packet_for(ep, 0x0001), gw.coap_addr)
            .await
            .unwrap();
        let resp = Packet::from_bytes(&recv_packet(&sock).await).unwrap();
        assert_eq!(resp.header.code, MessageClass::Response(Status::Created));

        let id = resp
            .get_option(CoapOption::LocationPath)
            .and_then(|list| list.iter().nth(1))
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .expect("missing registration id");
        ids.push(id);
    }

    assert_ne!(ids[0], ids[1], "devices must get distinct registration ids");

    gw.stop().await;
}

/// RFC 7959: a registration payload uploaded in two Block1 blocks gets
/// 2.31 Continue for the first block and 2.01 Created with a Block1 echo for
/// the final one, and the server sees the reassembled body.
#[tokio::test]
async fn coap_block1_registration_is_reassembled() {
    use coap_lite::block_handler::BlockValue;

    let gw = common::TestGateway::start().await;
    gw.bootstrap_registry.mark_included("block-device").await;
    let sock = UdpSocket::bind("[::1]:0").await.unwrap();

    let payload: &[u8] =
        br#"</>;rt="oma.lwm2m";ct=112,</1/1>,</3>;ver=1.0,</3/0>,</5>;ver=1.0,</5/0>"#;
    let block_size = 64;
    assert!(payload.len() > block_size && payload.len() <= 2 * block_size);

    let block_packet = |num: usize, more: bool, chunk: &[u8], mid: u16| {
        let mut pkt = Packet::new();
        pkt.header.set_type(MessageType::Confirmable);
        pkt.header.code = MessageClass::Request(Method::Post);
        pkt.header.message_id = mid;
        pkt.set_token(vec![0x03]);
        pkt.add_option(CoapOption::UriPath, b"rd".to_vec());
        pkt.add_option(CoapOption::UriQuery, b"ep=block-device".to_vec());
        pkt.add_option(CoapOption::UriQuery, b"lt=3600".to_vec());
        pkt.add_option_as(
            CoapOption::Block1,
            BlockValue::new(num, more, block_size).unwrap(),
        );
        pkt.payload = chunk.to_vec();
        pkt.to_bytes().unwrap()
    };

    // Block 0 (intermediate) → 2.31 Continue.
    sock.send_to(
        &block_packet(0, true, &payload[..block_size], 0x0010),
        gw.coap_addr,
    )
    .await
    .unwrap();
    let resp0 = Packet::from_bytes(&recv_packet(&sock).await).unwrap();
    assert_eq!(resp0.header.code, MessageClass::Response(Status::Continue));

    // Block 1 (final) → 2.01 Created with the Block1 echo option.
    sock.send_to(
        &block_packet(1, false, &payload[block_size..], 0x0011),
        gw.coap_addr,
    )
    .await
    .unwrap();
    let resp1 = Packet::from_bytes(&recv_packet(&sock).await).unwrap();
    assert_eq!(resp1.header.code, MessageClass::Response(Status::Created));

    let ack = resp1
        .get_first_option_as::<BlockValue>(CoapOption::Block1)
        .expect("final response must echo the Block1 option")
        .expect("Block1 echo must be parseable");
    assert_eq!(usize::from(ack.num), 1);
    assert!(!ack.more);

    // The registration handler must have seen the reassembled link-format
    // body: all objects from both blocks are registered.
    let snapshot = gw.registry.snapshot().await;
    let device = snapshot
        .iter()
        .find(|s| s.endpoint == "block-device")
        .expect("device must be registered");
    assert!(device.objects.contains(&"1/1".to_string()));
    assert!(device.objects.contains(&"3/0".to_string()));
    assert!(device.objects.contains(&"5/0".to_string()));

    gw.stop().await;
}
