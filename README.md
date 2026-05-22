# lwm2mserver-rs

Rust service running on an embedded Linux gateway. It acts as an LWM2M server for IoT devices communicating over a proprietary radio (PPP/IPv6) and exposes Unix domain sockets for local command/event integration.

```
local process  ──IPC (Unix socket)──►  lwm2mserver-rs  ──CoAP/UDP/IPv6──►  IoT devices
               ◄──event (Unix socket)─  (on gateway)         (ppp0)
```

---

## Table of contents

- [Target hardware](#target-hardware)
- [Development setup](#development-setup-one-time)
- [Build](#build)
- [Deploy to gateway](#deploy-to-gateway)
- [Usage](#usage)
- [IPC sockets](#ipc-sockets)
  - [Command socket](#command-socket----tmplwm2mserver-commandipc)
    - [List registered devices](#list-registered-devices)
    - [Approve device inclusion](#approve-device-inclusion)
    - [Execute a device resource](#execute-a-device-resource)
    - [Write a device resource](#write-a-device-resource)
  - [Event socket](#event-socket----tmplwm2mserver-eventipc)
    - [Includable device event](#includable-device-event)
    - [Device data event](#device-data-event)
    - [Connection status event](#connection-status-event)
    - [Device deleted event](#device-deleted-event)
- [Bootstrap / device inclusion protocol](#bootstrap--device-inclusion-protocol)
- [Connection status tracking](#connection-status-tracking)
- [IPSO object definitions](#ipso-object-definitions)
- [Persistence](#persistence)
- [Project layout](#project-layout)

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

Output: `target/armv5te-unknown-linux-gnueabi/release/lwm2mserver-rs`

---

## Deploy to gateway

```bash
scp -O target/armv5te-unknown-linux-gnueabi/release/lwm2mserver-rs \
    root@192.168.1.61:/usr/local/bin/
```

The `-O` flag forces legacy SCP protocol — required because the gateway's BusyBox SSH does not include an SFTP server.

---

## Usage

```bash
lwm2mserver-rs ppp0 --bind-to-device \
    --server-uri "coap://[fc00::6:100:0:0]" \
    --port 20017 \
    --lb-key-file /var/lib/lemonbeatd/Network_management/Network_key.json \
    --ipso-directories /usr/share/lwm2m/objects /etc/lwm2m/objects
```

Log level is controlled via `RUST_LOG`, e.g. `RUST_LOG=lwm2mserver_rs=debug`.

### Arguments

| Argument | Required | Default | Description |
|---|---|---|---|
| `<interface>` | yes | — | Network interface for CoAP traffic (e.g. `ppp0`) |
| `--bind-to-device` | no | off | Bind CoAP socket to the interface via `SO_BINDTODEVICE` |
| `--server-uri` | no | `coap://[fc00::6:100:0:0]` | This server's CoAP URI written to devices during bootstrap |
| `--port` | no | `20017` | UDP port to listen on |
| `--lb-key-file` | no | `/var/lib/lemonbeatd/…/Network_key.json` | JSON file containing `"network_key"` as a hex string |
| `--ipso-directories` | no | _(none)_ | One or more directories containing IPSO object definition XML files (space-separated). All `*.xml` files in each directory are loaded at startup and used to translate `/dp` payloads into named, typed events. |
| `RUST_LOG` | no | `lwm2mserver_rs=info` | Log filter (env var) |

---

## IPC sockets

The gateway exposes two Unix domain sockets for integration with local processes. All messages are newline-terminated JSON arrays.

### Command socket — `/tmp/lwm2mserver-command.ipc`

Receives commands from local processes. Supported operations:

#### List registered devices

Returns accumulated IPSO state for each included device.

```json
[{"op":"read","entity":{"service":"lwm2mserver","path":"devices"}}]
```

Response:
```json
[{"payload":{"<sgtin>":{"device":{"_urn":"…","0":{…}}}},"success":true}]
```

#### Approve device inclusion

After a device sends its first `/bs` request its certificate is read and an `includable_device` event is emitted (see Event socket below). To allow the device to join the network, send:

```json
[{"op":"execute","entity":{"service":"lwm2mserver","path":"includable_device/<id>/include"}}]
```

where `<id>` is the numeric identifier from the event. The approval is stored; the write phase starts automatically on the device's next `/bs` request.

Response:
```json
[{"success":true}]
```

Returns `{"success":false}` if `<id>` is not known.

#### Execute a device resource

Sends a CoAP Execute to the named resource on a registered device. `path` uses the IPSO object and resource names from the loaded XML definitions, separated by `/`.

```json
[{"op":"execute","entity":{"device":"<sgtin>","path":"<object>/<instance>/<resource>"},"payload":{}}]
```

Example — trigger an RF link measurement:

```json
[{"op":"execute","entity":{"device":"3034F8319C00754000000097","path":"sg_common/0/measure_rf_link"},"payload":{}}]
```

Response on success:
```json
[{"metadata":{"lwm2m_client_id":1,"lwm2m_uri":[12345,0,1],"lwm2m_response":"CHANGED","lwm2m_response_code":68},"success":true}]
```

To pass an argument, include `"as"` in the payload: `{"payload":{"as":["value"]}}`.

#### Write a device resource

Sends a CoAP Write (PUT) to the named resource. The value is type-tagged using the standard value keys.

```json
[{"op":"write","entity":{"device":"<sgtin>","path":"<object>/<instance>/<resource>"},"payload":{"vi":42}}]
```

Supported payload keys: `vs` (string), `vi` (integer), `vf` (float), `vb` (boolean), `vt` (time/integer), `vo` (opaque/base64).

---

### Event socket — `/tmp/lwm2mserver-event.ipc`

Emits events to all connected listeners as newline-terminated JSON arrays. Connect and read lines; the socket stays open and streams events as they occur.

#### Includable device event

Emitted on **every** `/bs` request from a device that has not yet been fully included. The event repeats as long as the device keeps sending `/bs` (approximately every 5 seconds while in bootstrap mode).

```json
[{
  "op": "update",
  "entity": {"service": "lwm2mserver", "path": "includable_device/<id>"},
  "payload": {
    "identifier":          {"vs": "<sgtin>",   "ts": <unix_ts>},
    "inclusion_started":   {"vb": false,        "ts": <unix_ts>},
    "inclusion_completed": {"vb": false,        "ts": <unix_ts>}
  },
  "metadata": {"source": "lwm2mserver", "sequence": <n>}
}]
```

Once the user has approved and the write phase has started, `inclusion_started` becomes `true`. When the write phase finishes successfully, a final event with both `inclusion_started` and `inclusion_completed` set to `true` is emitted.

#### Device data event

Emitted when a registered device pushes a `/dp` (data push) payload. Object and resource names come from the IPSO XML definitions loaded via `--ipso-directories`. Each resource value uses a type-tagged key (`vi` integer, `vf` float, `vs` string, `vb` boolean, `vt` time, `vo` opaque/base64; arrays use `ai`, `af`, `as`, `ab`, `ao`).

```json
[{
  "op": "update",
  "entity": {"device": "<sgtin>", "path": ""},
  "payload": {
    "device": {
      "_urn": "urn:oma:lwm2m:oma:3",
      "0": {
        "manufacturer":     {"vs": "ACME",  "ts": 1234567890},
        "firmware_version": {"vs": "1.2.3", "ts": 1234567890},
        "error_code":       {"ai": [0],     "ts": 1234567890}
      }
    }
  },
  "metadata": {"source": "lwm2mserver", "sequence": <n>}
}]
```

#### Connection status event

Emitted whenever a device transitions between online and offline. An initial online event fires when a device first registers or contacts the server after being offline. An offline event fires when a CoAP request to the device fails (send error or CON timeout after retransmission).

```json
[{
  "op": "update",
  "entity": {"device": "<sgtin>", "path": "connection_status"},
  "payload": {
    "_urn": "urn:oma:lwm2m:x:28171",
    "0": {"online": {"vb": true, "ts": <unix_ts>}}
  },
  "metadata": {"source": "lwm2mserver", "sequence": <n>}
}]
```

#### Device deleted event

Emitted when a device sends a CoAP `DELETE /rd/<id>` (factory reset / self-deregistration). The device is removed from the registry and its persisted state is deleted.

```json
[{
  "op": "delete",
  "entity": {"device": "<sgtin>", "path": ""},
  "metadata": {"source": "lwm2mserver", "sequence": <n>}
}]
```

---

## Bootstrap / device inclusion protocol

Devices enter bootstrap mode to receive provisioning credentials (server URI, network key). The flow requires explicit user approval for each inclusion.

### IPv6 Traffic Class

The radio module uses the IPv6 Traffic Class byte to control MAC-layer encryption:

| TC | Meaning |
|---|---|
| `0x0c` | No MAC encryption — used before the network key is exchanged |
| `0x1c` | MAC encryption active — used after bootstrap completes |

All bootstrap packets (server → device) are sent with TC=0x0c. All post-bootstrap traffic (registration responses, data push ACKs) uses TC=0x1c.

### Step 1 — certificate read

1. Device sends `CON POST /bs?ep=<name>` repeatedly (~every 5 s). The server does **not** ACK.
2. On the first `/bs` the server waits 3 seconds (to let the device open its receive socket), then sends `CON GET /0/0` with TC=0x0c to read the device's X.509 certificate.
3. The device ACKs with its certificate payload. The server extracts and caches the P-256 public key permanently (it never changes for a given device, so it is not re-read on subsequent bootstrap attempts).
4. Every `/bs` — including those received while waiting for the cert and those received after — emits an `includable_device` event on the event socket.

### Step 2 — user approval

The local application receives `includable_device` events and presents the device to the user. When the user approves, the application sends the `execute includable_device/<id>/include` IPC command. The gateway stores the approval; no immediate action is taken.

### Step 3 — credential write

On the next `/bs` received after approval has been stored:

1. Server sends `2.04 Changed` ACK (TC=0x0c) — this is the **only** `/bs` that is ever ACKed.
2. The approval is consumed (a second factory reset requires re-approval).
3. Server executes the write sequence — all packets are CON with TC=0x0c, each waits for device ACK before proceeding (RFC 7252: 2 s initial timeout, up to 4 retries, exponential backoff):

| Step | Operation | Path | Content |
|---|---|---|---|
| 1 | `DELETE` | `/1` | Clear existing Server Object instances |
| 2 | `PUT` | `/1/1` | Short Server ID=1, Lifetime=86400, Binding="U" (SenML+CBOR) |
| 3 | `DELETE` | `/0` | Clear existing Security Object instances |
| 4 | `PUT` | `/0/1` | Server URI, server public key, encrypted network key (SenML+CBOR) |
| 5 | `POST` | `/bs` | Bootstrap finish signal |

After the device ACKs `POST /bs` it switches to encrypted traffic (TC=0x1c).

### Credentials

- **Server public key**: an ephemeral P-256 keypair is generated at process startup. The compressed 33-byte public key is written to `/0/1/4`.
- **Network key**: a 16-byte key read from the JSON file passed via `--lb-key-file`. The file must contain a `"network_key"` field as a lowercase hex string:

```json
{ "network_key": "be3960fa8ccd1e306e579096dcecc0d6" }
```

The key is ECDH-encrypted per device: AES-128-ECB using the first 16 bytes of the ECDH shared secret (server ephemeral private key × device P-256 public key from its X.509 certificate). Plaintext structure: 14 random bytes ‖ 16-byte key ‖ 2-byte CRC-16/XMODEM.

---

## Connection status tracking

The server maintains an online/offline status for each registered device and emits `connection_status` events on transitions.

**Going offline**: a device is marked offline when a CON request to it exhausts all retransmits (RFC 7252 default: 3 retransmits, 2 s initial timeout with exponential backoff) or when a UDP send fails at the socket level.

**Coming online**: any inbound CoAP packet from a device (registration, heartbeat, data push, ACK) marks it online.

**Proactive connectivity pings**: the housekeeping task sends a CoAP Execute to `sg_common/0/measure_rf_link` (instance 0) as a low-cost ping to detect status changes proactively:

- **Offline device**: pinged every 15 minutes for the first 6 hours after going offline.
- **Online device**: pinged once per day when no other communication has occurred.

Any real CoAP interaction (inbound or outbound) resets these timers so pings are suppressed while the device is actively communicating. At startup, all restored devices are in the offline state and are pinged on the first housekeeping tick (60 s after start) to establish current status.

---

## IPSO object definitions

When `--ipso-directories` is specified, the gateway loads all `*.xml` files matching the OMA LWM2M object definition schema. These are used to:

- Resolve numeric object/resource IDs to human-readable names in `/dp` event payloads
- Determine correct value types (integer, float, string, boolean, time, opaque)
- Detect multi-instance resources (encoded as JSON arrays `ai`/`af`/`as`/…)
- Select the correct `_urn` based on the object version declared by the device at registration
- Resolve resource names in IPC execute/write commands to numeric LWM2M IDs

The `<ObjectVersion>` element in each XML file is used as the version key. If a device registered with `ver=1.1` for object 3, the definition from the file containing `<ObjectVersion>1.1</ObjectVersion>` is used. Files without an `<ObjectVersion>` element are used as the unversioned fallback.

---

## Persistence

The server persists state to `/var/lib/lwm2mserver/` across restarts:

| File | Contents |
|---|---|
| `wakaama.json` | Registered device snapshots (endpoint, address, lifetime, object versions). Written atomically on every registration and heartbeat. Expired entries are skipped on load. |
| `included_devices.json` | List of included device endpoints. Written on inclusion and exclusion. |
| `devices/<sgtin>.json` | Accumulated IPSO state per device from `/dp` payloads. Written on every data push. Deleted on device factory reset. |

All writes are atomic (write to `.tmp`, then `rename`). On startup, restored devices are placed in the offline state regardless of their persisted status; the connectivity ping mechanism determines their actual online state within the first minute.

---

## Project layout

```
src/
├── main.rs           Entry point. Spawns long-running tasks under a shared
│                     CancellationToken; any task failure brings down the process
│                     (suitable for systemd restart).
│
├── config.rs         Config::from_args() — clap-based CLI argument parsing.
│
├── model.rs          Shared domain types:
│                       Device            — registered device state (address,
│                                           lifetime, objects, online status,
│                                           ping scheduling fields)
│                       PendingOperation  — queued or in-flight CoAP op
│                       LwM2mCommand      — Read / Write / Execute
│
├── registry.rs       DeviceRegistry (Arc<RwLock<…>>). Owns all device state.
│                     Key operations:
│                       register()              — called on CoAP POST /rd
│                       touch()                 — updates last-contact, clears offline_since
│                       set_device_offline()    — marks device offline, sets offline_since
│                       take_ping_candidates()  — returns devices due for a ping
│                       drain_pending()         — returns queued ops on heartbeat
│                       place_in_flight()       — moves a sent op to in-flight map
│                       complete_in_flight()    — removes op when CoAP ACK arrives
│                       expire_stale()          — purges devices past lifetime + grace
│                       merge_device_state_by_addr() — deep-merges /dp IPSO state
│
├── bootstrap.rs      BootstrapRegistry — drives device inclusion:
│                       • Generates ephemeral P-256 keypair at startup
│                       • Caches device X.509 certs permanently (cert never changes)
│                       • Tracks pending GET /0/0 exchanges by token
│                       • Stores user approval (approve_inclusion / is_approved /
│                         consume_approval) — approval consumed on next /bs
│                       • Assigns stable numeric IDs to endpoints for IPC references
│                       • Performs ECDH key encapsulation for the network key
│
├── error.rs          Unified Error enum (thiserror).
│
├── ipso.rs           IpsoModel — loads IPSO object definition XML files at startup.
│                     Keyed by (object_id, version). Provides get_versioned() for
│                     version-aware lookup with unversioned fallback.
│
├── persistence.rs    PersistenceStore — atomic JSON persistence to
│                     /var/lib/lwm2mserver/:
│                       wakaama.json         — device registry snapshots
│                       included_devices.json — included endpoint list
│                       devices/<sgtin>.json — per-device IPSO state
│
├── ipc.rs            Command socket server on /tmp/lwm2mserver-command.ipc.
│                     Handles newline-framed JSON requests:
│                       read devices                           — list device states
│                       execute includable_device/<id>/include — approve inclusion
│                       execute <object>/<inst>/<resource>     — CoAP Execute
│                       write   <object>/<inst>/<resource>     — CoAP Write
│                     Resource names are resolved via IpsoModel; operations are
│                     dispatched via coap_dispatch_tx and awaited (30 s timeout).
│
├── event.rs          Event socket server on /tmp/lwm2mserver-event.ipc.
│                     Broadcasts events to all connected listeners via a tokio
│                     broadcast channel. Events: includable_device, device_data,
│                     connection_status, device_deleted.
│
├── coap/
│   ├── mod.rs        UDP socket bind helper (socket2, SO_BINDTODEVICE).
│   │                 sgtin_from_ep() — strips URN prefix from ep parameter.
│   ├── server.rs     Inbound CoAP task. recv_from loop on [::]:20017.
│   │                 Every packet calls registry.touch() first — this is the
│   │                 sole path for marking a device online.
│   │                 Handles:
│   │                   POST /bs     — emit includable event; if approved: ACK +
│   │                                  write phase; else: GET /0/0 if cert not cached
│   │                   POST /rd     — device registration → 2.01 Created (TC=0x1c)
│   │                   POST /rd/<id>— heartbeat → 2.04 Changed + drain ops (TC=0x1c)
│   │                   POST /dp     — SenML+CBOR → IPSO-named event (TC=0x1c)
│   │                   DELETE /rd/<id> — device self-deregistration (factory reset)
│   │                   ACK          — bootstrap write-step or op oneshot completion
│   │                   RST          — op oneshot fires with CoapError
│   └── client.rs     Outbound CoAP task. Receives DispatchRequest from channel,
│                     builds CON requests, retransmits up to 3× with exponential
│                     backoff (2 s initial, RFC 7252 defaults). On send failure or
│                     final timeout: calls registry.set_device_offline() and emits
│                     a connection_status event.
│
├── housekeeping.rs   60-second interval task:
│                       • Expire stale device registrations (lifetime + 30 s grace)
│                       • Timeout in-flight ops (> 60 s)
│                       • Expire stale bootstrap sessions (> 30 s)
│                       • Dispatch connectivity pings (sg_common/0/measure_rf_link
│                         Execute): offline devices every 15 min for 6 h; online
│                         devices once per day when idle. Runs on first tick to
│                         probe all restored devices at startup.
│
├── console_fmt.rs    Custom tracing formatter: prefixes structured fields before
│                     the message — `TIMESTAMP LEVEL target: [k=v …] message`.
│
└── syslog_layer.rs   RFC 5424 syslog tracing layer. Active when JOURNAL_STREAM is
                      set (systemd). Emits structured data in the bnw@55029 SD-ID;
                      suppresses the plain fmt layer to avoid duplicate output.
```

### Task topology

```
                      ┌──────────────────────────────────────┐
UDP :20017 ───────────►│ coap_server_task                     │
                      │   recv_from loop                     │──► coap_dispatch_tx
                      │   /bs / /rd / /dp / DELETE / ACK/RST │──► event_sender (broadcast)
                      └──────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
                      │ coap_dispatch_task                    │◄── coap_dispatch_tx
                      │   send CON requests, retransmit loop │──► event_sender (connection_status)
                      └──────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
/tmp/…-command.ipc ──►│ ipc_task                             │
                      │   read / execute / write             │──► coap_dispatch_tx
                      └──────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
/tmp/…-event.ipc  ◄───│ event_task                           │◄── event_sender (broadcast)
                      │   fan-out to connected listeners     │
                      └──────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
                      │ housekeeping_task (60 s interval)     │
                      │   expire registrations, timeout ops, │──► coap_dispatch_tx
                      │   connectivity pings                  │
                      └──────────────────────────────────────┘
```
