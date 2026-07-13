# 02 — Device-targeted READ operation

Covers PORTING_TODO.md "Missing parts" §2 and task-list item **READ-1**.

## Goal

`read` commands on the command socket that target a *device* resource must be
forwarded to the device as a CoAP GET (after wakeup, see plan 01), the reply
decoded, returned as the command response, **and** published as an
`overwrite` event so consumers' cached state is corrected (replace, not
merge).

## Python reference

- `lwm2mserver/lwm2mserver.py::DeviceOperationHandler.read` (~line 1495):
  - Path resolution: entity must contain `device` and `path`; path is
    name-based (`object[/instance[/resource]]`), resolved via the IPSO
    registry; 1–3 segments allowed. >3 segments →
    "Read of a single instance of a multiple instance resource is not yet
    supported".
  - Wakes the device (generic 4 s timeout), performs the wakaama READ,
    response status `CONTENT (2.05)` → success.
  - Response: `success`, `payload` (decoded values, name-resolved),
    `metadata` = `{lwm2m_client_id, lwm2m_uri, lwm2m_response,
    lwm2m_response_code}`.
- `DeviceOperationHandler.handle_operation` (~line 1705): after a successful
  read it publishes (as a background task) an
  `EventBusProtocolMessage(OVERWRITE, EventBusDeviceEntity(device, path),
  response.payload)` on the event socket.
- `lwm2mserver/state.py::DeviceStateStorage._overwrite_state` (~line 202):
  OVERWRITE applies the full diff **including removals**, whereas UPDATE
  (used by `/dp`) filters removals out (patch semantics). Both then persist.

## Current Rust state

- `src/ipc/command.rs::handle_request` supports `read devices` only; any
  other read path logs "unhandled read path" and returns `success:false`.
- `LwM2mCommand::Read { path }` and the CoAP GET encoding already exist in
  `src/lwm2m/client.rs::build_request` — but nothing constructs a Read op,
  and `ResourcePath` (`src/model.rs`) is fixed 3-level
  (object/instance/resource).
- ACK payloads: `src/lwm2m/server.rs::handle_ack` completes the op with
  `coap_response_to_result`, which only carries class/detail — **the response
  payload bytes are dropped**, so a GET's content never reaches the caller.
- SenML+CBOR decoding to the event JSON shape exists in
  `src/lwm2m/server.rs::build_device_payload` (used for `/dp`).
- State handling: `DeviceRegistry::merge_device_state_by_addr` +
  `persistence::save_device_state` implement *merge* only.
- The event socket (`src/ipc/event.rs`) has no `overwrite` op; consumers
  distinguish `update` vs `overwrite` (see Python `_protocol_flattened.py`).

## Design

1. **Carry response payloads to op completers.**
   Extend `ResourceValue` (`src/model.rs`) with a variant that carries
   content, e.g.
   `CoapContent { class: u8, detail: u8, content_format: u16, payload: Vec<u8> }`,
   and produce it in `handle_ack`/`coap_response_to_result` when the ACK has
   a payload. Existing consumers (write/execute) keep matching on
   `CoapResponse`.

2. **Support 1–3 segment paths.** Either make
   `LwM2mCommand::Read` take its own path type
   (`ReadPath { object_id, instance_id: Option<u16>, resource_id: Option<u32> }`)
   or a `Vec<u16>`-style URI. `client.rs::build_request` emits only the
   present segments as Uri-Path options.

3. **IPC handler** (`src/ipc/command.rs`):
   - New arm: `"read"` with a non-`devices` path and an
     `entity.device`. Resolve names→ids via `IpsoModel`
     (reuse/generalize `resolve_resource` to allow missing instance/resource
     segments). Unknown device → error message like Python
     (`Device '<name>' not connected`).
   - Dispatch `LwM2mCommand::Read` through the existing
     `dispatch_and_await` machinery (this automatically gains wakeup once
     plan 01 lands).
   - On `CoapContent` with class 2: decode the payload with
     `build_device_payload` (move it from `server.rs` to a shared module,
     e.g. `src/lwm2m/senml.rs`, so both `/dp` and read use it), scope the
     decoded JSON to the requested path, and reply
     `{"success": true, "payload": …, "metadata": {"lwm2m_client_id": …,
     "lwm2m_uri": [...], "lwm2m_response": "CONTENT",
     "lwm2m_response_code": "2.05"}}`.
   - Non-2.05 → `success:false` with the error message convention used
     elsewhere in `command.rs`.

4. **Overwrite event + state semantics.**
   - `EventSender::send_device_overwrite(endpoint, path, payload)` emitting
     `[{"op":"overwrite","entity":{"device":…,"path":…},"payload":…}]`
     (mirror the exact envelope of the existing `update` events — check a
     captured Python event before freezing the format).
   - Registry: add `overwrite_device_state_by_addr(addr, path, new_state)`
     that **replaces** the subtree at `path` (removals included) instead of
     deep-merging; persist via the existing `save_device_state` flow.
   - Publish the event and apply the overwrite after answering the command
     (spawned task, like Python's background publish).

5. **Wakeup** — nothing extra: reads flow through `dispatch_op` (plan 01).

## Testing

- Unit: SenML+CBOR decode of a GET response fixture → expected JSON; path
  scoping (read of `device/0` returns only that instance).
- Unit: overwrite state semantics — a resource present in the cache but
  absent from the fresh read disappears (this is the key difference from
  merge; port assertions from Python `tests/test_state.py`).
- Integration (`tests/ipc_tests.rs` + a fake device socket): send
  `[{"op":"read","entity":{"service":…,"device":"<sgtin>","path":"device/0/serial_number"}}]`,
  answer the CoAP GET with 2.05 + CBOR, assert command response payload,
  and assert an `overwrite` event appears on the event socket.
- Error paths: device unknown, GET timeout (→ `success:false`), 4.04 from
  device.

## Risks / open questions

- **Multi-instance resources**: Python refuses 4-segment paths — keep that
  restriction and the same error text.
- Response content format: devices reply SenML+CBOR (112); if a device
  replies TLV or plain text the decode fails — return `success:false` with a
  clear message (Python would fail similarly in payload conversion).
- The `overwrite` event envelope must match what consumers already accept
  from Python — capture one from a real/Python system before implementing
  (`tests/manual` in sg-bnw-lwm2m-server may contain examples).
- Depends on: plan 01 only for wakeup (functionally independent), plan 06
  for nothing.
