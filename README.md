# lwm2m-gateway

Rust service running on an embedded Linux gateway. It acts as an LWM2M server for IoT devices communicating over a proprietary radio (PPP/IPv6) and exposes a Unix domain socket for local command/event integration.

```
local process  ──IPC (Unix socket)──►  lwm2m-gateway  ──CoAP/UDP/IPv6──►  IoT devices
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

---

## Usage

```bash
lwm2m-gateway ppp0 --bind-to-device \
    --server-uri "coap://[fc00::6:100:0:0]" \
    --port 20017 \
    --lb-key-file /var/lib/lemonbeatd/Network_management/Network_key.json
```

Log level is controlled via `RUST_LOG`, e.g. `RUST_LOG=lwm2m_gateway=debug`.

### Arguments

| Argument | Required | Default | Description |
|---|---|---|---|
| `<interface>` | yes | — | Network interface for CoAP traffic (e.g. `ppp0`) |
| `--bind-to-device` | no | off | Bind CoAP socket to the interface via `SO_BINDTODEVICE` |
| `--server-uri` | no | `coap://[fc00::6:100:0:0]` | This server's CoAP URI written to devices during bootstrap |
| `--port` | no | `20017` | UDP port to listen on |
| `--lb-key-file` | no | `/var/lib/lemonbeatd/…/Network_key.json` | JSON file containing `"network_key"` as a hex string |
| `RUST_LOG` | no | `lwm2m_gateway=info` | Log filter (env var) |

---

## IPC command socket

The gateway listens on `/tmp/lwm2mserver-command.ipc` (Unix domain socket). Messages are newline-terminated JSON arrays.

### List registered devices

Request:
```json
[{"op":"read","entity":{"service":"lwm2mserver","path":"devices"}}]
```

Response:
```json
[{"payload":{},"success":true}]
```

---

## Bootstrap protocol

Devices must be provisioned with the server address and network key before they can register. This is done via a proprietary two-phase bootstrap over the `/bs` path.

### IPv6 Traffic Class

The radio module uses the IPv6 Traffic Class byte to control MAC-layer encryption:

| TC | Meaning |
|---|---|
| `0x0c` | No MAC encryption — used before the network key is exchanged |
| `0x1c` | MAC encryption active — used after bootstrap completes |

All bootstrap packets (server → device) are sent with TC=0x0c. All post-bootstrap traffic (registration responses, data push ACKs) uses TC=0x1c.

### Phase 1 — read device public key

1. Device sends `CON POST /bs?ep=<name>` (TC=0x0c). The server does **not** ACK this immediately.
2. After a 3-second delay (to let the device open its response socket), the server sends `CON GET /0/0` with TC=0x0c.
3. Device responds with its X.509 DER certificate. The server extracts the P-256 public key and caches it under the device endpoint name.
4. Because the initial `POST /bs` was never ACKed, the device retransmits it, triggering Phase 2.

### Phase 2 — write network credentials

On receiving the second `POST /bs` (after the cert is cached):

1. Server sends `2.04 Changed` ACK (TC=0x0c).
2. Server executes the write sequence — all packets are CON with TC=0x0c, each waits for device ACK before proceeding (RFC 7252 retransmit: 2 s initial, up to 4 retries, exponential backoff):

| Step | Operation | Object | Content |
|---|---|---|---|
| 1 | `DELETE /1` | Server Object | Clear existing server instances |
| 2 | `PUT /1/1` | Server Object | Short Server ID=1, Lifetime=86400, Binding="U" (SenML+CBOR) |
| 3 | `DELETE /0` | Security Object | Clear existing security instances |
| 4 | `PUT /0/1` | Security Object | Server URI, server public key, encrypted network key (SenML+CBOR) |
| 5 | `POST /bs` | — | Bootstrap finish signal |

After the device ACKs `POST /bs` it switches to encrypted traffic (TC=0x1c).

### Credentials

- **Server public key**: an ephemeral P-256 keypair is generated at process startup. The compressed 33-byte public key is written to `/0/1/4`.
- **Network key**: a 16-byte key read from the JSON file passed via `--lb-key-file`. The file must contain a `"network_key"` field as a lowercase hex string:

```json
{ "network_key": "be3960fa8ccd1e306e579096dcecc0d6" }
```

The key is ECDH-encrypted for the device (ECIES: AES-128-GCM using the shared secret derived from the server's ephemeral private key and the device's P-256 public key from its X.509 certificate) and written to `/0/1/5`.

---

## Project layout

```
src/
├── main.rs           Entry point. Spawns four long-running tasks under a shared
│                     CancellationToken; any task failure brings down the process
│                     (suitable for systemd restart).
│
├── config.rs         Config::from_args() — clap-based CLI argument parsing.
│
├── model.rs          Shared domain types:
│                       Device            — registered device state
│                       PendingOperation  — queued or in-flight CoAP op
│                       LwM2mCommand      — Read / Write / Execute
│
├── registry.rs       DeviceRegistry (Arc<RwLock<…>>). Owns all device state.
│                     Key operations:
│                       register()           — called on CoAP POST /rd
│                       touch()              — updates last-contact on any packet
│                       drain_pending()      — returns queued ops on device heartbeat
│                       place_in_flight()    — moves a sent op to the awaiting-ACK map
│                       complete_in_flight() — removes op when CoAP ACK arrives
│                       expire_stale()       — purges devices past lifetime + grace
│
├── bootstrap.rs      BootstrapRegistry — drives the two-phase bootstrap described
│                     above. Generates the ephemeral P-256 keypair at startup,
│                     caches device X.509 certificates across /bs retransmits,
│                     performs ECDH key encapsulation for the network key, and
│                     tracks per-token oneshot channels for the write sequence.
│
├── error.rs          Unified Error enum (thiserror).
│
├── ipc.rs            Unix socket server on /tmp/lwm2mserver-command.ipc.
│                     Handles newline-framed JSON array requests from local
│                     processes; currently supports "read devices".
│
├── coap/
│   ├── mod.rs        UDP socket bind helper (socket2, SO_BINDTODEVICE).
│   ├── server.rs     Inbound CoAP task. recv_from loop on [::]:20017.
│   │                 Handles:
│   │                   POST /bs          — bootstrap phase 1 or phase 2 write sequence
│   │                   POST /rd          — device registration → 2.01 Created (TC=0x1c)
│   │                   POST /rd/<id>     — heartbeat → 2.04 Changed + drain ops (TC=0x1c)
│   │                   POST /dp          — data push → 2.04 Changed (TC=0x1c) + log SenML
│   │                   ACK              — bootstrap write-step or op oneshot completion
│   │                   RST              — op oneshot fires with CoapError
│   └── client.rs     Outbound CoAP task. Receives DispatchRequest from channel,
│                     builds CON requests, retransmits up to 4× with exponential
│                     backoff (2 s initial, RFC 7252 defaults).
│
└── housekeeping.rs   60-second interval task. Expires stale device registrations,
                      times out in-flight ops, and expires stale bootstrap sessions.
```

### Task topology

```
                      ┌──────────────────────────────────────┐
UDP :20017 ───────────►│ coap_server_task                     │
                      │   recv_from loop                     │──► coap_dispatch_tx
                      │   registration / heartbeat / ACK     │
                      └──────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
                      │ coap_dispatch_task                    │◄── coap_dispatch_tx
                      │   send CON requests, retransmit loop │
                      └──────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
/tmp/…-command.ipc ──►│ ipc_task                             │
                      │   read devices / future commands     │
                      └──────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
                      │ housekeeping_task (60 s interval)     │
                      │   expire registrations, timeout ops  │
                      └──────────────────────────────────────┘
```
