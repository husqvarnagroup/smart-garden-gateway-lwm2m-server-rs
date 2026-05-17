# lwm2m-gateway

Rust service running on an embedded Linux gateway. It acts as an LWM2M server for IoT devices communicating over a proprietary radio (PPP/IPv6) and bridges commands from AWS IoT Core (or any MQTT broker) to LWM2M Read/Write/Execute operations.

```
AWS IoT Core  ──MQTT/TLS──►  lwm2m-gateway  ──CoAP/UDP/IPv6──►  IoT devices
                              (on gateway)         (ppp0)
```

---

## Target hardware

| Property | Value |
|---|---|
| Device | GARDENA smart Gateway |
| CPU | ARMv5TE (`armv5tejl`) |
| OS | OpenEmbedded Linux, systemd, BusyBox |
| Radio interface | `ppp0` — IPv6-only PPP over `/dev/ttyS1` at 500000 baud |
| Gateway IPv6 | `fc00::6:100:0:0/64` (ULA) + `fe80::…` link-local on ppp0 |

---

## Development setup (one-time)

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — must be running for cross-compilation
- Rust toolchain via [rustup](https://rustup.rs)

### Install cross and the target

```bash
cargo install cross
rustup target add armv5te-unknown-linux-gnueabi
```

`cross` uses Docker to provide the ARM sysroot and C toolchain. No separate ARM GCC installation is needed on the host.

---

## Build

### Debug build (host, for fast iteration / clippy)

```bash
cargo build
cargo clippy
```

### Release build for the gateway

```bash
cross build --release --target armv5te-unknown-linux-gnueabi
```

Output: `target/armv5te-unknown-linux-gnueabi/release/lwm2m-gateway`

---

## Deploy to gateway

```bash
scp -O target/armv5te-unknown-linux-gnueabi/release/lwm2m-gateway \
    root@192.168.1.61:/usr/local/bin/
```

The `-O` flag forces legacy SCP protocol — required because the gateway's BusyBox SSH does not include an SFTP server.

### Run on the gateway

```bash
ssh root@192.168.1.61

# TLS mode (AWS IoT Core)
export MQTT_HOST=xxxx.iot.us-east-1.amazonaws.com
export MQTT_CLIENT_ID=my-gateway
export TLS_CERT_PATH=/etc/lwm2m/device.crt
export TLS_KEY_PATH=/etc/lwm2m/device.key
export TLS_CA_PATH=/etc/lwm2m/AmazonRootCA1.pem
lwm2m-gateway

# Username/password mode (Mosquitto etc.)
export MQTT_HOST=192.168.1.10
export MQTT_CLIENT_ID=my-gateway
export MQTT_USERNAME=gateway
export MQTT_PASSWORD=secret
lwm2m-gateway

# Anonymous mode (local broker, no auth)
export MQTT_HOST=192.168.1.10
export MQTT_CLIENT_ID=my-gateway
lwm2m-gateway
```

Log level is controlled via `RUST_LOG`, e.g. `RUST_LOG=lwm2m_gateway=debug`.

---

## Configuration reference

All configuration is via environment variables.

| Variable | Required | Default | Description |
|---|---|---|---|
| `MQTT_HOST` | yes | — | MQTT broker hostname or IP |
| `MQTT_CLIENT_ID` | yes | — | MQTT client ID |
| `MQTT_PORT` | no | `8883` (TLS) / `1883` (plain) | MQTT port |
| `MQTT_TOPIC_PREFIX` | no | `lwm2m` | Topic prefix (see below) |
| `TLS_CERT_PATH` | TLS mode | — | PEM device certificate |
| `TLS_KEY_PATH` | TLS mode | — | PEM private key |
| `TLS_CA_PATH` | TLS mode | — | PEM CA bundle |
| `MQTT_USERNAME` | password mode | — | MQTT username |
| `MQTT_PASSWORD` | password mode | `""` | MQTT password |
| `COAP_BIND_ADDR` | no | `[::]:20017` | UDP bind address for CoAP |
| `REGISTRATION_GRACE_SECS` | no | `30` | Extra seconds before an expired registration is purged |
| `RUST_LOG` | no | `lwm2m_gateway=info` | Log filter |

**Auth mode is selected automatically** by which variables are present:
1. `TLS_CERT_PATH` set → mutual TLS
2. `MQTT_USERNAME` set → username/password over plain TCP
3. Neither → anonymous plain TCP

---

## MQTT topic convention

| Direction | Topic pattern | Description |
|---|---|---|
| Inbound (broker → gateway) | `lwm2m/cmd/{endpoint}` | Command to execute on a device |
| Outbound (gateway → broker) | `lwm2m/resp/{endpoint}/{correlation_id}` | Result of the command |

### Command payload (JSON)

```json
{
  "correlationId": "abc-123",
  "endpoint": "sensor-01",
  "operation": "read",
  "path": { "objectId": 3, "instanceId": 0, "resourceId": 0 },
  "responseTopic": "lwm2m/resp/sensor-01/abc-123"
}
```

`operation` is `"read"`, `"write"`, or `"execute"`. Write requires a `"value"` field; execute accepts an optional `"value"` as arguments.

### Response payload (JSON)

```json
{
  "correlationId": "abc-123",
  "endpoint": "sensor-01",
  "path": { "objectId": 3, "instanceId": 0, "resourceId": 0 },
  "value": { "text": "lwm2m-gateway" }
}
```

On failure, `"error"` is present instead of `"value"`.

---

## Project layout

```
src/
├── main.rs           Entry point. Spawns five long-running tasks under a shared
│                     CancellationToken; any task failure brings down the process
│                     (suitable for systemd restart).
│
├── config.rs         Config::from_env() — all runtime parameters, including the
│                     MqttAuth enum that selects TLS / password / anonymous mode.
│
├── model.rs          Shared domain types:
│                       Device            — registered device state
│                       PendingOperation  — queued or in-flight CoAP op
│                       LwM2mCommand      — Read / Write / Execute
│                       MqttCommand       — parsed inbound MQTT payload
│                       MqttResponse      — outbound MQTT payload
│
├── registry.rs       DeviceRegistry (Arc<RwLock<…>>). Owns all device state.
│                     Key operations:
│                       register()        — called on CoAP POST /rd
│                       touch()           — updates last-contact on any inbound packet
│                       enqueue_op()      — queues an op from the MQTT subscriber
│                       drain_pending()   — returns queued ops on device heartbeat
│                       place_in_flight() — moves a sent op to the awaiting-ACK map
│                       complete_in_flight() — removes op when CoAP ACK arrives
│                       expire_stale()    — purges devices past lifetime + grace period
│
├── error.rs          Unified Error enum (thiserror).
│
├── coap/
│   ├── mod.rs        UDP socket bind helper; CoAP content-format constants.
│   ├── server.rs     Inbound CoAP task. recv_from loop on [::]:20017.
│   │                 Handles:
│   │                   POST /rd          — device registration → 2.01 Created
│   │                   POST /rd/<id>     — heartbeat → 2.04 Changed + drain ops
│   │                   ACK              — CoAP response → fires op oneshot
│   │                   RST              — fires op oneshot with CoapError
│   └── client.rs     Outbound CoAP task. Receives DispatchRequest from channel,
│                     builds CON requests, sends, retransmits up to 4× with
│                     exponential backoff (2 s initial, RFC 7252 defaults).
│
└── mqtt/
    ├── mod.rs        build_client() — constructs rumqttc AsyncClient + EventLoop
    │                 with the appropriate transport (rustls TLS or plain TCP).
    ├── subscriber.rs Drives the rumqttc EventLoop. Parses JSON command payloads,
    │                 calls registry.enqueue_op(), spawns a future per op that
    │                 waits on the oneshot and publishes the result.
    └── publisher.rs  Receives MqttResponse from channel, serializes to JSON,
                      publishes to the per-request response topic.

housekeeping.rs       60-second interval task. Expires stale device registrations
                      and times out in-flight ops that have not received an ACK,
                      firing their oneshots with LwM2mError::Timeout so no MQTT
                      response is silently dropped.
```

### Task topology

```
                      ┌─────────────────────────────────────────┐
UDP :20017 ───────────►│ coap_server_task                        │
                      │   recv_from loop                        │
                      │   registration / heartbeat / ACK        │──► coap_dispatch_tx
                      └──────────────────┬──────────────────────┘
                                         │ mqtt_out_tx (ACK results)
                      ┌──────────────────▼──────────────────────┐
                      │ coap_dispatch_task                       │
                      │   send CON requests, retransmit loop    │◄── coap_dispatch_tx
                      └─────────────────────────────────────────┘

                      ┌─────────────────────────────────────────┐
AWS IoT / broker ────►│ mqtt_inbound_task (EventLoop poll)      │
                      │   parse JSON → enqueue_op               │──► coap_dispatch_tx
                      │   spawn per-op response future          │
                      └─────────────────────────────────────────┘

                      ┌─────────────────────────────────────────┐
                      │ mqtt_outbound_task                       │◄── mqtt_out_tx
                      │   serialize JSON → publish              │
                      └─────────────────────────────────────────┘

                      ┌─────────────────────────────────────────┐
                      │ housekeeping_task (60 s interval)        │
                      │   expire registrations, timeout ops     │
                      └─────────────────────────────────────────┘
```
