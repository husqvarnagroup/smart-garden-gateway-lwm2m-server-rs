# LWM2M Server — Reverse-Engineered Specification

This document describes the behaviour of the original Python `lwm2mserver` package as derived from its source code.

---

## Overview

The server is a gateway between a proprietary Lemonbeat/Gardena RF mesh network and an internal event bus. It runs on a gateway device, listens for LWM2M devices over IPv6/UDP, handles their full lifecycle (inclusion, registration, operations), and exposes a Unix-socket-based event bus so that other local services can subscribe to device events and send commands.

```
  [RF Devices]  --UDP/CoAP-->  [Wakaama C lib]  <--> [Python Server]  --Unix socket--> [Event Bus]
```

---

## Core Concepts

### 1. LWM2M / Wakaama

The server wraps **Wakaama**, an Eclipse LWM2M C library exposed to Python via Cython bindings (`wakaama._wakaama`). Wakaama implements the LWM2M protocol state machine (registration, bootstrap, CoAP retransmissions). The server runs a tight `wakaama.step()` loop and feeds incoming UDP packets to `wakaama.handle_packet()`.

Both LWM2M 1.0 (TLV encoding) and 1.1 (SenML-CBOR, preferred) are supported. CBOR is enforced on registration regardless of what the device advertises.

### 2. IPSO Object Registry (`BnwRegistry`)

IPSO objects are identified by numeric IDs in LWM2M but exposed with human-readable names on the event bus. The registry (`BnwRegistry`) maps between them, e.g. `firmware_update/0/package` ↔ `(28155, 0, 0)`. Object definitions are loaded from JSON resource files and can be reloaded at runtime via SIGHUP.

Paths on the bus use the form `<object-name>/<instance-id>/<resource-name>/<resource-instance-id>` where trailing segments are optional.

### 3. Device Naming

Devices register with a URN endpoint name of the form `urn:dev:sg:<SGTIN>`. The SGTIN portion (after the prefix) is used as the device identifier throughout the event bus. Conversion helpers: `client_name_to_entity_device` / `entity_device_to_client_name`.

### 4. Protocol / Event Bus Message Format

All IPC messages use a JSON envelope format from the `bnw.protocol` package (vendored as `_protocol_flattened.py` when not installed). A message has four fields:

| Field | Type | Description |
|---|---|---|
| `operation` | `ProtocolOperation` | `READ`, `WRITE`, `EXECUTE`, `UPDATE`, `OVERWRITE`, `DELETE` |
| `entity` | `ProtocolEntity` | Identifies target: `{"device": "<sgtin>", "path": "..."}` or `{"service": "lwm2mserver", "path": "..."}` |
| `payload` | `ProtocolPayload` | Dict of typed values (`vs`, `vi`, `vf`, `vb`, `vo`, `vt` keys for string/int/float/bool/opaque/time) |
| `metadata` | `ProtocolMetadata` | Source service name, monotone sequence number, LWM2M response codes |

Messages are serialised as JSON lines (newline-delimited) on the sockets.

### 5. Event Bus (Unix Sockets)

Two Unix domain sockets are created at startup under the path given by `--eventbus`:

- **`lwm2mserver-event.ipc`** — pub/sub: clients connect and receive all events published by the server (device data, status changes). Read-only from client perspective.
- **`lwm2mserver-command.ipc`** — req/rep: clients send a JSON-line request and receive a JSON-line response. Used to issue READ/WRITE/EXECUTE operations on devices.

The server drops events to slow subscribers rather than blocking (high-water-mark check). A clean shutdown writes an empty frame to subscribers as EOF.

### 6. Device Inclusion (Bootstrap)

Devices are "included" (onboarded) via the LWM2M Bootstrap interface. The flow is driven by a state machine in `DeviceProcessManager.bootstrap_manager`:

1. Device sends bootstrap request (typically to `ff02::1` multicast; unicast can be allowed via flag).
2. Server authenticates the device by reading its X.509 certificate from the LWM2M Security object (object 0, instance 0, resource `public_key_or_identity`).
3. Certificate is validated against one of the configured CA certificates (two hardcoded defaults for the Gardena/Smartsystem PKI). The certificate's SGTIN field must match the endpoint name.
4. Once authenticated, the device appears as an `includable_device/<n>` service entity on the event bus. An external service must send `EXECUTE lwm2mserver/includable_device/<n>/include` to authorise inclusion.
5. Server then bootstraps the device: deletes old security/server objects, writes a new Security object (containing a network key encrypted to the device's public key via ECIES) and a Server object with the server URL and registration lifetime.
6. Device sends Bootstrap Finish ACK → bootstrap complete → device registers.

Includable devices expire after 30 s (configurable via `LWM2MSERVER_INCLUDABLE_DEVICE_EXPIRY_DURATION_DEFAULT`) if not acted upon.

### 7. Registration Lifecycle

After bootstrap the device performs an LWM2M Registration. The `monitoring_callback` handles three events:

- `CLIENT_REGISTERED` — device added to included set, `entity_device_to_client_id_mapping` updated, clients state persisted. If the device is not in the included set, a factory reset is triggered and the client is disconnected after 10 s.
- `CLIENT_UPDATED` — registration lifetime renewed.
- `CLIENT_DEREGISTERED` — device removed from mapping, wakeup tracking reset, clients state persisted.

### 8. Operations (Read / Write / Execute)

Incoming commands from the event bus are handled by `OperationHandler`, which delegates to `DeviceOperationHandler` (for device entities) or `ServiceOperationHandler` (for service entities).

Flow for a device operation:
1. Translate named path → numeric IPSO URI via registry.
2. Optionally wake the device (see Wake-on-Radio below).
3. Issue the corresponding Wakaama call (`wakaama.read/write/execute`).
4. Await result via an `asyncio.Future` set by a one-shot callback registered with Wakaama.
5. Translate result back to a `ProtocolPayload` and return a `EventBusCommandResponse`.

For `READ`, successful results are also published as `OVERWRITE` status events so other subscribers get the latest known value.

Custom handlers override the default Wakaama path for specific object/resource combinations:
- `device/0/factory_reset` (EXECUTE) → triggers device exclusion.
- `firmware_update/0/package` (WRITE) → FOTA upload with extended wakeup timeout.
- `connection_status/0/online` / `connection_status/0/check` (READ/EXECUTE) → handled locally without contacting device.

### 9. Device State Storage

Each included device has a JSON file at `devices/<sgtin>.json` that caches the last-known state. Every published UPDATE/OVERWRITE/DELETE message is applied to this in-memory dict and persisted atomically (write to `.swp` file, then rename). The state is keyed as `{object_name: {instance_id: {resource_name: value_dict}}}`.

### 10. Global State Persistence

Two files are maintained in the state storage directory:

- **`included_devices.json`** — JSON array of SGTIN strings of all currently included devices. Written atomically on every change.
- **`wakaama.json`** — JSON object with `file_version` (2) and `clients` array. Each client entry stores endpoint name, LWM2M version, binding, media type, registration lifetime, IPSO objects with versions, and the UDP session (address, traffic class). Loaded at startup to restore registrations across restarts.

### 11. Connectivity Monitoring

`DeviceProcessManager.connectivity_manager` maintains an online/offline state machine per device. It transitions:
- **initial → online** on first message.
- **online → offline** if no message received within `LWM2MSERVER_DEVICE_CONNECTIVITY_ONLINE_TIMEOUT` (default 9000 s).
- **offline → online** on next message.

When the connectivity state changes, a `connection_status/0/online` UPDATE event is published. A periodic "check" operation (`EXECUTE /1/1/8` — Registration Update Trigger) is run against devices that have been quiet for `LWM2MSERVER_DEVICE_CONNECTIVITY_INITIAL_TIMEOUT / 2`.

### 12. Wake-on-Radio (WoR)

Battery-powered devices sleep between LWM2M interactions. Before issuing a Wakaama operation, the server checks whether the device supports Wake-on-Radio (object ID 28184). If so, and if the device is not already tracked as awake, a wakeup frame is sent over the Lemonbeat radio module.

The wakeup channel is read from the device's last-known state (`wake_on_radio/0/wakeup_channel`). Up to 3 attempts are made (configurable). The device is tracked as awake for the duration specified in the wakeup frame. A threshold of 0.3 s prevents wakeup when the awake window is about to expire.

### 13. Lemonbeat / Radio Module

The RF network uses proprietary Lemonbeat protocol. Two specialisations exist:

- **`LemonbeatDongleConnection`** — maps link-local source addresses (`fe80::<EUI64>`) to unique-local (`fc00::6:<MAC>`) to route replies through the gateway dongle. Also applies traffic-class-based encryption tag.
- **`RadioModuleControl`** — sends wakeup frames to the radio module daemon (`lemonbeatd`) via a socket in `LEMONBEATD_RUNTIME_DIRECTORY`.

Traffic class values: `0x1C` = encrypted, `0x0C` = unencrypted.

### 14. Cryptography (Device Authentication)

During bootstrap the server reads the device's X.509 DER certificate from the Security object. The certificate is validated against a list of `DeviceCertificateAuthority` objects (EC key, P-256 for old CA, P-256 SHA-384 for new CA). The device is authenticated if the certificate is valid and its SGTIN field matches the endpoint name.

When writing the Security object back to the device, the network key is encrypted using ECIES: a fresh ephemeral EC key pair is generated, ECDH is performed against the device's public key, AES-GCM is used for the encryption, and the result is stored in the `public_key_or_identity` field.

### 15. Timezone Handling

A `TimezoneHandler` integrates with D-Bus (`dbus_next`) to read the system timezone and propagate it to included devices by writing the `device/0/current_time` and `device/0/utc_offset` resources.

---

## Module Inventory

| File | Responsibility |
|---|---|
| `lwm2mserver.py` | Main server class, all callback handlers, `OperationHandler`, `ServiceOperationHandler`, `DeviceOperationHandler`, `_IncludableDevice`, `TimezoneHandler` |
| `clients_state.py` | `WakaamaClientsState` — serialize/deserialize `wakaama.json` |
| `connection.py` | `UdpConnection`, `AsyncUdpConnection`, `LemonbeatDongleConnection`, `UdpSession` |
| `crypto.py` | `DeviceCertificateAuthority`, `DeviceCertificate`, `InclusionSession` (ECIES key wrapping) |
| `event.py` | `Bus` — Unix socket pub/sub + req/rep server |
| `ipso.py` | `BnwRegistry`, `BnwDataConverter`, `NamePath`, `IdPath`, `DeviceResource` |
| `lemonbeat.py` | `LemonbeatNetworkConfiguration` — reads `Network_key.json` |
| `processes.py` | `DeviceProcessManager`, state machines for bootstrap/registration/connectivity (Cython extension) |
| `protocol.py` / `_protocol_flattened.py` | Protocol envelope, message, payload, entity types |
| `radio_module.py` | `RadioModuleControl` — wakeup frame sender |
| `state.py` | `DeviceStateStorage` — per-device last-known state, JSON persistence |
| `utils.py` | `ExpiringSet`, `Crc16Xmodem` |
| `wake_on_radio.py` | `DeviceWakeupManager` — WoR state and wakeup orchestration |

---

## Key Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LWM2MSERVER_INCLUDABLE_DEVICE_EXPIRY_DURATION_DEFAULT` | 30 | Seconds before an unapproved includable device entry expires |
| `LWM2MSERVER_BOOTSTRAP_INACTIVITY_TIMEOUT` | 10.0 | Bootstrap state machine inactivity timeout (s) |
| `LWM2MSERVER_BOOTSTRAP_READ_SECURITY_MAX_DELAY` | 3.0 | Random delay before reading device certificate (s) |
| `LWM2MSERVER_LWM2M_REGISTRATION_LIFETIME` | 86400 | Registration lifetime written to device during bootstrap (s) |
| `LWM2MSERVER_DEVICE_CONNECTIVITY_ONLINE_TIMEOUT` | 9000 | Time without a message before a device is considered offline (s) |
| `LWM2MSERVER_DEVICE_CONNECTIVITY_OFFLINE_TIMEOUT` | 900 | Offline connectivity check period (s) |
| `LWM2MSERVER_DEVICE_CONNECTIVITY_INITIAL_TIMEOUT` | 60 | Initial online check timeout (s) |
| `LWM2MSERVER_WAKAAMA_DEFAULT_STEP_TIMEOUT` | 60 | Maximum Wakaama step interval (s) |
| `LWM2MSERVER_EVENTBUS_REQUEST_TIMEOUT` | 30.0 | Timeout for a single eventbus request (s) |
| `LWM2MSERVER_WAKEUP_TIMEOUT_MS` | 4000 | WoR awake duration for normal operations (ms) |
| `LWM2MSERVER_WAKEUP_FOTA_TIMEOUT_MS` | 1800000 | WoR awake duration for FOTA uploads (ms) |
| `LWM2MSERVER_WAKEUP_TOTAL_ATTEMPTS` | 3 | Total WoR send attempts |
| `LWM2MSERVER_WAKEUP_SLEEP_BEFORE_RETRY_ATTEMPT_MS` | 2000 | Delay between WoR retry attempts (ms) |
| `LEMONBEATD_RUNTIME_DIRECTORY` | — | Path to lemonbeatd runtime sockets (set by systemd) |
