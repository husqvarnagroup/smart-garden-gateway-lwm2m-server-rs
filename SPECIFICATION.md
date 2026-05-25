# LWM2M Server — Specification

This document describes the behaviour of the Rust `lwm2mserver-rs` implementation.

---

## Overview

The server is a gateway between a proprietary Lemonbeat/Gardena RF mesh network and an internal event bus. It runs on a gateway device, listens for LWM2M devices over IPv6/UDP, handles their full lifecycle (inclusion, registration, operations), and exposes a Unix-socket-based event bus so that other local services can subscribe to device events and send commands.

```
  [RF Devices]  --UDP/CoAP-->  [lwm2mserver-rs]  --Unix socket--> [Event Bus]
```

The server is written in Rust using the Tokio async runtime. CoAP is handled directly using the `coap-lite` crate. There is no wrapping of an external C library.

---

## Architecture

The server runs five concurrent tasks orchestrated with a `CancellationToken`:

| Task | Module | Role |
|---|---|---|
| **CoAP server** | `lwm2m::server` | Receives all inbound UDP packets; handles `/bs`, `/rd`, `/dp`, DELETE, and ACK/RST routing |
| **CoAP dispatch** | `lwm2m::client` | Sends outbound operations (Read/Write/Execute) to devices; manages retransmission and block-wise transfer |
| **Housekeeping** | `housekeeping` | Runs every 60 s: expires stale registrations, times out in-flight operations, expires stale bootstrap sessions, sends connectivity pings |
| **IPC command** | `ipc::command` | Listens on `lwm2mserver-command.ipc`; handles JSON command requests from other services |
| **IPC event** | `ipc::event` | Listens on `lwm2mserver-event.ipc`; relays events to all connected subscribers |

All state is protected by `tokio::sync::RwLock` or `tokio::sync::Mutex`. Operations from the IPC command task are injected into the CoAP dispatch task via an `mpsc::channel`.

---

## Core Concepts

### 1. CoAP Transport

The server opens a single shared `UdpSocket` bound to `[::]:20017` (configurable via `--port`). The socket can be bound to a named network interface via `SO_BINDTODEVICE` (`--bind-to-device` flag).

Before each transmission the IPv6 Traffic Class (TCLASS) is set to indicate whether MAC-layer encryption is active:

- `TC=0x0C` — plain (bootstrap phase; no network-layer encryption yet)
- `TC=0x1C` — encrypted (post-bootstrap; radio module applies encryption)

On non-Linux platforms this call is silently skipped.

### 2. IPSO Object Registry (`IpsoModel`)

IPSO objects are identified by numeric IDs in LWM2M but exposed with human-readable snake_case names on the event bus. The `IpsoModel` loads object definitions from XML files in directories supplied via `--ipso-directories`.

- Object names are converted from CamelCase or title-case to `snake_case` (e.g. `IrrigationControl` → `irrigation_control`).
- Resource names and URNs are taken directly from the XML `<Name>` and `<ObjectURN>` elements.
- The model supports versioned objects: multiple XML files for the same object ID are stored under different version keys. Lookup resolves: exact version → unversioned fallback → any version.
- The model is hot-reloadable: sending `SIGHUP` to the process swaps the inner `Arc<IpsoModel>` atomically.

### 3. Device Naming

Devices register with a URN endpoint name of the form `urn:dev:sg:<SGTIN>` (or `sgtin:<SGTIN>`). The bare SGTIN (after the last `:`) is used as the device identifier throughout the event bus and in all persisted files. Conversion is handled by `sgtin_from_ep()`.

### 4. Protocol / Event Bus Message Format

IPC messages use a JSON array envelope. Each element has four fields:

| Field | Type | Description |
|---|---|---|
| `op` | string | `"update"`, `"delete"` (events); `"read"`, `"write"`, `"execute"` (commands) |
| `entity` | object | `{"device": "<sgtin>", "path": "..."}` or `{"service": "lwm2mserver", "path": "..."}` |
| `payload` | object | Dict of typed values (`vs`, `vi`, `vf`, `vb`, `vo`, `vt` keys for string/int/float/bool/opaque/time) |
| `metadata` | object | Source service name, monotone sequence number, LWM2M response codes |

Messages are serialised as JSON lines (newline-delimited) on the sockets.

### 5. Event Bus (Unix Sockets)

Two Unix domain sockets are created at startup:

- **`/tmp/lwm2mserver-event.ipc`** — pub/sub: clients connect and receive all events published by the server (device data, status changes, inclusion events). Read-only from client perspective. Uses `tokio::sync::broadcast`; lagging subscribers drop messages with a warning.
- **`/tmp/lwm2mserver-command.ipc`** — req/rep: clients send a JSON-line array of request objects and receive a JSON-line array of response objects. Used to issue Write/Execute operations on devices and to query device state.

On shutdown the socket files are removed.

### 6. Device Inclusion (Bootstrap)

Devices are onboarded via the LWM2M Bootstrap interface. The flow:

1. Device sends `POST /bs?ep=<URN>` (bootstrap request). The server extracts the SGTIN, assigns it a stable numeric ID, and emits an `includable_device/<id>` event on the event socket.
2. If the device's certificate has not yet been retrieved, the server sends `GET /0/0` (LWM2M Security Object) as a Confirmable request after a 3-second delay to allow the device's receive socket to open. The response payload (SenML+CBOR) is parsed for resource `/0/0/3` (Public Key or Identity) which contains the device's DER-encoded X.509 certificate.
3. The certificate is validated: issuer must match one of the two hardcoded CAs (smartsystem CA or GARDENA Device CA G1), KeyUsage must include `digitalSignature`, ExtendedKeyUsage must include `clientAuth`, the signature must verify against the matching CA's P-256 public key, and the SubjectAltName must contain a `sgtin:` URI whose SGTIN matches the device's endpoint name (case-insensitive).
4. Once authenticated, the device appears as `includable_device/<n>` on the event socket and its certificate payload is cached permanently for this endpoint. An external service must send `EXECUTE includable_device/<n>/include` on the command socket to authorise inclusion.
5. On the device's next `POST /bs`, the server ACKs and starts the write phase: `CON DELETE /1` → `CON PUT /1/1` → `CON DELETE /0` → `CON PUT /0/1` → `CON POST /bs`. Each step uses RFC 7252 retransmission (up to 4 attempts, 2 s initial backoff, exponential). All write-phase packets use TC=0x0C.
6. Bootstrap finish ACK → inclusion complete → `included_devices.json` updated → `connection_status` and `includable_device` completion events emitted.

Bootstrap sessions expire after 30 seconds if the device never responds to `GET /0/0`.

#### Bootstrap Cryptography

During step 5 above, the Security Object instance `/0/1` is written with:
- `/0/1/0` — server CoAP URI
- `/0/1/1` — `false` (not a bootstrap server)
- `/0/1/2` — security mode `3` (NoSec; radio module provides encryption)
- `/0/1/4` — server's compressed P-256 ephemeral public key (33 bytes)
- `/0/1/5` — network key encrypted for the device

Key encryption procedure:
```
shared_x        = ECDH(server_ephemeral_private, device_P256_public).x_coordinate
aes_key         = shared_x[:16]                       # first 16 bytes of X coord
random_prefix   = urandom(14)
plaintext       = random_prefix(14) ‖ network_key(16) ‖ CRC16-XMODEM(first 30 bytes)(2)
ciphertext      = AES-128-ECB(aes_key, plaintext)     # 32 bytes (two blocks)
```

The server generates one ephemeral P-256 keypair at startup, shared across all bootstrap sessions.

### 7. Registration Lifecycle

After bootstrap the device performs an LWM2M Registration (`POST /rd`). The CoAP server handles:

- **`POST /rd`** (new registration) — parses `ep`, `lt`, `lwm2m`, `b` query parameters and the link-format body (`</3/0>;ver=1.1,...`). Registers or updates the device in `DeviceRegistry`. Responds with `2.01 Created` and `Location-Path: rd/<id>`. If the device is not in the included set, triggers `Execute /3/0/5` (factory reset) and removes the device from the registry after 10 seconds.
- **`POST /rd/<id>`** (registration update) — resets the expiry timer; applies new lifetime if `lt=` was included; drains any pending operations and sends them to the device.
- **`DELETE`** (deregistration) — device going offline temporarily. The device remains included and its state is preserved for the next reconnect. No exclusion occurs.

Housekeeping expires registrations that have exceeded their lifetime plus a 30-second grace period.

### 8. Device Data Push (`/dp`)

After registration, devices push state using `POST /dp` with a SenML+CBOR body. The server:

1. Decodes the CBOR array, resolving `bn` + relative `n` into full LWM2M URIs (`/<obj>/<inst>/<res>[/<res_inst>]`).
2. Looks up each object ID in `IpsoModel` (using the device's per-object version from registration) to get names and resource types.
3. Encodes values into the typed payload format (`vi`, `vf`, `vs`, `vb`, `vo`, `vt`); multi-instance resources use array variants (`ai`, `af`, `as`, `ab`, `ao`).
4. Deep-merges the decoded state into the device's in-memory state object.
5. Publishes the merged partial state as an `update` event to all event socket subscribers.
6. Persists the full merged state to `devices/<sgtin>.json` asynchronously.

### 9. Operations (Write / Execute)

Commands arrive on the command socket as JSON arrays. Supported operations:

| `op` | `entity.path` | Action |
|---|---|---|
| `read` | `devices` | Returns the in-memory state + online status for all included devices |
| `execute` | `includable_device/<id>/include` | Approves inclusion for device with given id |
| `execute` | `device/0/factory_reset` | Triggers exclusion: sends factory reset to device, removes from registry and included list |
| `execute` | any other `obj/inst/res` path | Looks up numeric IDs via IpsoModel, dispatches `Execute` to device |
| `write` | `firmware_update/0/package` | FOTA upload (see below) |
| `write` | any other `obj/inst/res` path | Looks up numeric IDs via IpsoModel, dispatches `Write` to device |

Path resolution: the IPC path uses snake_case names (`object_name/instance_id/resource_name`). These are translated to numeric IDs via `IpsoModel`, using the device's registered object version for that object.

Operations are dispatched via an `mpsc` channel to the CoAP dispatch task and awaited via a `oneshot` channel. Timeout is 30 seconds.

### 10. Firmware Update (FOTA)

The `write firmware_update/0/package` command handles firmware upload:

- A single process-wide mutex (`fota_lock`) prevents concurrent uploads.
- A special flush byte `[0x00]` clears the device's prior upload state.
- Payloads ≤ 512 bytes are sent as a single Write.
- Larger payloads use CoAP Block1 (RFC 7959) transfer, starting at SZX=5 (512-byte blocks). The device may negotiate a smaller SZX via `2.31 Continue` responses; the server honors this.
- For large payloads the command socket responds after the first `2.31 Continue` ACK (confirming the device accepted the upload); the actual completion or failure is reported asynchronously via a `firmware_update/0/package` event.

### 11. Device State Storage

Each included device has a JSON file at `/var/lib/lwm2mserver/devices/<sgtin>.json` that caches the last-known merged IPSO state. Written atomically (write to `.tmp`, then rename).

### 12. Global State Persistence

Two files are maintained in `/var/lib/lwm2mserver/`:

- **`included_devices.json`** — JSON array of SGTIN strings of all currently included devices. Written atomically on every change.
- **`wakaama.json`** — JSON object with `file_version: 2` and `clients` array. Each client entry stores endpoint name, LWM2M version, binding mode, content type, registration lifetime, `end_of_life` Unix timestamp, IPSO object IDs with versions, and the UDP session (address, port). Loaded at startup to restore registrations across restarts. Expired entries (past `end_of_life`) are silently dropped on load.

On startup a consistency check ensures every entry in `wakaama.json` appears in `included_devices.json`. Orphaned registry entries are dropped and their device state files are deleted.

### 13. Connectivity Monitoring

The housekeeping task (60-second tick) manages connectivity pings:

- **Online devices** are pinged (`Execute /1/1/8` — Registration Update Trigger) when there has been no contact for 24 hours, paced to at most one ping per 24-hour period.
- **Offline devices** are pinged every 15 minutes for up to 6 hours after going offline.

A device transitions online when the CoAP server receives any packet from it (`registry.touch()`). If the outbound CoAP dispatch task exhausts retransmits for an operation, the device is marked offline and a `connection_status` event is emitted.

### 14. Systemd Integration

The server uses `sd_notify` to signal systemd:

- `READY=1` — sent after all sockets are bound and state is restored.
- `STOPPING=1` — sent when `SIGTERM` or `SIGINT` is received.
- `WATCHDOG=1` — sent at half the configured `WatchdogSec` interval to keep the watchdog alive.

---

## Module Inventory

| File | Responsibility |
|---|---|
| `main.rs` | Startup: logging, config, state restore, task spawning, signal handling |
| `config.rs` | CLI argument parsing (`clap`), `Config` struct, network key loading |
| `model.rs` | Core data types: `Device`, `ResourcePath`, `LwM2mCommand`, `LwM2mError`, `PendingOperation`, well-known path constants |
| `registry.rs` | `DeviceRegistry` — thread-safe in-memory store of registered devices; handles register/update/expiry/state/online tracking |
| `housekeeping.rs` | Periodic background task: registration expiry, operation timeouts, bootstrap expiry, connectivity pings |
| `persistence.rs` | `PersistenceStore` — reads and writes `wakaama.json`, `included_devices.json`, `devices/<sgtin>.json` |
| `ipc/command.rs` | Unix socket command server: accepts JSON request arrays, returns JSON response arrays |
| `ipc/event.rs` | Unix socket event pub/sub: `EventSender` broadcasts newline-framed JSON to all subscribers |
| `lwm2m/mod.rs` | Shared CoAP utilities: socket bind, traffic class, content-format encoding, `BlockAckMap` |
| `lwm2m/server.rs` | Inbound CoAP handler: `/bs`, `/rd`, `/rd/<id>`, `/dp`, DELETE, ACK, RST routing; bootstrap write phase |
| `lwm2m/client.rs` | Outbound CoAP dispatcher: single sends with retransmission, block-wise writes |
| `lwm2m/bootstrap.rs` | Bootstrap registry (sessions, approval, cert cache, included set); certificate validation; network-key encryption; SenML+CBOR encoding for bootstrap payloads |
| `lwm2m/ipso.rs` | `IpsoModel` — loads and queries IPSO object definitions from XML; snake_case name conversion |
| `logging/` | Structured logging with `tracing`: journal/syslog layer + console formatter |
| `error.rs` | `Error` enum and `Result` type alias |

---

## Key CLI Arguments

| Argument | Default | Description |
|---|---|---|
| `<interface>` | (required) | Network interface name for CoAP traffic (e.g. `ppp0`) |
| `--bind-to-device` | false | Bind CoAP socket to the interface via `SO_BINDTODEVICE` |
| `--server-uri` | `coap://[fc00::6:100:0:0]` | CoAP URI of this server, written to devices during bootstrap |
| `--port` | `20017` | UDP port to listen on |
| `--lb-key-file` | `/var/lib/lemonbeatd/Network_management/Network_key.json` | JSON file containing the network key (`hex` field `"network_key"`) |
| `--ipso-directories` | (none) | Directories containing IPSO object definition XML files |

Log verbosity is controlled by the `RUST_LOG` environment variable (default: `lwm2mserver_rs=info`). When running under systemd with `JOURNAL_STREAM` set, logs are written to the journal in structured format; otherwise to stderr.

---

## Connectivity Ping Timing Constants

| Constant | Value | Description |
|---|---|---|
| `ONLINE_PING_INTERVAL` | 9000 s (2.5 h) | How long without contact before pinging an online device |
| `OFFLINE_PING_INTERVAL` | 15 min | How often to ping an offline device |
| `OFFLINE_PING_MAX_DURATION` | 6 h | Stop pinging after device has been offline this long |
| `GRACE_SECS` | 30 s | Grace period added to registration lifetime before expiry |
| `IN_FLIGHT_TIMEOUT_SECS` | 60 s | Maximum time an in-flight operation may wait for a response |
| `BOOTSTRAP_TIMEOUT_SECS` | 30 s | Maximum time to wait for a device to respond to `GET /0/0` |
| `BLOCK_THRESHOLD` | 512 B | Minimum payload length that triggers block-wise transfer |

---

## Deviations from Python Code

This section records where the Rust implementation intentionally or necessarily differs from the original Python `lwm2mserver`.

### No Wakaama Dependency

The Rust implementation does not use the Wakaama C library. The LWM2M protocol state machine is implemented directly: CoAP packets are parsed and emitted with the `coap-lite` crate, retransmission is handled with explicit Tokio timers, and the registration/bootstrap state is managed in Rust data structures. This eliminates the Cython binding layer, the `wakaama.step()` loop, and the callback-based API.

### No LWM2M 1.0 / TLV Support

Python supported both LWM2M 1.0 (TLV) and 1.1 (SenML+CBOR). The Rust server uses only SenML+CBOR. Devices still register with whatever LWM2M version they advertise, but the server does not negotiate or enforce a format; it sends SenML+CBOR in bootstrap writes and expects SenML+CBOR in `/dp` payloads.

### Hardcoded State Directory

The state directory is hardcoded to `/var/lib/lwm2mserver` in the Rust binary. Python accepted a configurable path via a CLI flag.

### Hardcoded Socket Paths

Event and command socket paths are hardcoded to `/tmp/lwm2mserver-event.ipc` and `/tmp/lwm2mserver-command.ipc`. Python derived them from the `--eventbus` directory argument.

### No Wake-on-Radio (WoR)

The WoR subsystem (detecting WoR-capable devices, sending wakeup frames via `lemonbeatd`, tracking awake windows) is not implemented. All operations are dispatched immediately.

### No Lemonbeat Dongle Address Mapping

Python's `LemonbeatDongleConnection` remapped link-local addresses (`fe80::`) to unique-local (`fc00::`) for routing through the gateway dongle. Rust uses addresses as-is; address rewriting is not performed.

### No Timezone Handler

Python's `TimezoneHandler` integrated with D-Bus to read the system timezone and write `device/0/current_time` and `device/0/utc_offset` to included devices. This feature is not present in Rust.

### No Bootstrap Unicast Control Flag

Python had a flag to allow or disallow unicast bootstrap requests. Rust accepts bootstrap requests from any address without restriction.

### Different Connectivity Timeouts

| Parameter | Python | Rust |
|---|---|---|
| Online → offline threshold | 9000 s (2.5 h) | 9000 s (2.5 h) (`ONLINE_PING_INTERVAL`) |
| Offline ping period | 900 s (15 min) | 900 s (15 min) (`OFFLINE_PING_INTERVAL`) |
| Offline ping window | unlimited | 6 h (`OFFLINE_PING_MAX_DURATION`) |

### No READ Operation Forwarded to Device

Python's `OperationHandler` forwarded `READ` commands from the event bus to the device via Wakaama, and published the result as an `OVERWRITE` event. Rust's command socket does not support device-targeted reads; `read devices` returns the accumulated in-memory state from `/dp` pushes.

### IPC Request Batching

The Rust command socket accepts a JSON array of request objects and returns a JSON array of responses (one-to-one). Python's event bus processed one request per connection.

### Exclusion Metadata

When a device is excluded (`execute device/0/factory_reset`), the Rust server returns a `metadata.exclusion` tag in the response: `"device_not_connected"` if the device was not registered, `"device_timeout"` if it did not respond, or `"device_failure"` if the factory reset CoAP call failed. Python returned a simpler success/failure envelope.

### No `ExpiringSet` for Includable Devices

Python's `ExpiringSet` automatically expired includable device entries after a configurable duration. Rust expires bootstrap sessions (GET /0/0 in-flight requests) after 30 seconds, but the `includable_id` mapping and the approval state are kept until the write phase completes or explicitly removed.

### Systemd Integration

The Rust implementation adds full `sd_notify` support (`READY`, `STOPPING`, `WATCHDOG`). Python had no systemd integration.
