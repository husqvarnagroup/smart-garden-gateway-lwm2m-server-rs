# 07 — Includable-device expiry

Covers PORTING_TODO.md "Missing parts" §7 and task-list item **INC-1**.

## Goal

`includable_device/<id>` entries announced on the event socket must expire
after a configurable idle duration and emit a **delete** event, so UIs drop
stale "device ready for inclusion" entries when the device stops sending
`/bs` requests.

## Python reference

`lwm2mserver/lwm2mserver.py::ServiceOperationHandler` (~lines 1820–2150):

- `_IncludableDevice` tracks `first_seen`/`last_seen`; every incoming `/bs`
  for a known includable calls `seen()` (refreshes `last_seen`) and
  republishes the `update` event.
- Expiry duration: constructor arg, default from env
  `LWM2MSERVER_INCLUDABLE_DEVICE_EXPIRY_DURATION_DEFAULT` = **30 s**.
- Background task `_expire_includable_devices_loop(duration/2)` sweeps every
  half-duration and calls `delete_expired_includable_devices()`, which for
  each entry with `last_seen + duration < now`:
  - removes it from the map, and
  - publishes op **`delete`** with entity
    `{service: "lwm2mserver", path: "includable_device/<instance_id>"}` and
    the same IPSO-shaped payload as the update event
    (`identifier`, `protocol`, `inclusion_started`, `inclusion_completed`,
    `inclusion_error`, `_urn: "urn:oma:lwm2m:x:28170:0.1"`).
- Related but separate: the runtime *approval* set is an
  `ExpiringSet(10000*60)` (`utils.py`) — approvals lapse after ~7 days;
  entries in the `--allow` list never lapse. (Rust currently keeps approvals
  until consumed.) Decide whether to port this too — recommended: yes, it's
  two lines once expiry exists.

Note Python does **not** delete entries during an ongoing/successful
inclusion — `seen()` keeps being called by the bootstrap flow, and completed
inclusions remove the includable through the normal flow.

## Current Rust state

- `src/lwm2m/bootstrap.rs::BootstrapRegistry`:
  `ensure_includable_id(endpoint) -> u32` allocates/returns a stable id
  (map `endpoint → id`); `remove_includable_id` deletes it after the write
  phase or failure. **No timestamps, no expiry.**
- `src/lwm2m/server.rs::handle_bootstrap` emits
  `event_sender.send_includable(id, endpoint, started, completed)` on every
  `/bs` — the update side already behaves like Python.
- `src/ipc/event.rs::EventSender::send_includable` exists;
  there is **no delete variant** for includables (`send_device_deleted`
  exists but uses the device entity).
- `src/housekeeping.rs` runs a 60 s sweep — natural place for the expiry
  check, but 60 s > 30 s default expiry; see design.

## Design

1. **Track `last_seen`** in `BootstrapRegistry`: change the includable map
   to `endpoint → IncludableEntry { id: u32, last_seen: Instant }`;
   `ensure_includable_id` refreshes `last_seen` (it is called on every
   `/bs`).
2. **Expiry sweep**: add
   `BootstrapRegistry::take_expired_includables(max_age: Duration) -> Vec<(u32, String)>`
   removing and returning expired entries. Don't expire endpoints that are
   currently mid-write-phase (approval consumed / write pending) — guard on
   the existing pending/approval state.
3. **Sweeper task**: Python sweeps every `duration/2`. Housekeeping's fixed
   60 s tick is too coarse for a 30 s default. Options:
   - (a) give housekeeping a second, faster interval (`duration/2`,
     min 1 s) for this check only — keeps all sweeps in one task; or
   - (b) spawn a tiny dedicated task in `main.rs`.
   Prefer (a): housekeeping already owns registry sweeps; add an interval.
4. **Delete event**: `EventSender::send_includable_deleted(id, endpoint)`
   emitting the same envelope as `send_includable` but with
   `"op": "delete"` (keep payload shape identical to the update event —
   compare with a captured Python delete event before freezing).
5. **Config** (depends on plan 06): duration from
   `LWM2MSERVER_INCLUDABLE_DEVICE_EXPIRY_DURATION_DEFAULT` (default 30 s).
6. **Approval expiry (optional, recommended)**: store approval timestamps in
   `BootstrapRegistry::approved`; `is_approved` treats entries older than
   `10000*60` s as absent. `--allow`-listed endpoints (plan 06) bypass.

## Testing

- Unit (bootstrap.rs): `ensure_includable_id` refreshes `last_seen`;
  `take_expired_includables` returns only stale entries; mid-inclusion
  endpoints survive.
- Integration (extend `tests/event_tests.rs`): trigger `/bs` → includable
  update event; advance time (`start_paused` + housekeeping with short
  interval) → expect the `delete` event with the same id; a second `/bs`
  after expiry allocates a **new** id (verify Python behavior: counter
  increments — ids are not reused).
- Regression: normal successful inclusion must NOT emit a spurious
  includable delete (the id is removed via `remove_includable_id` — decide
  whether Python emits delete there too; check
  `_handle_bootstrap_end`/inclusion-complete path and mirror it).

## Risks / open questions

- Event contract: confirm consumers key includables by instance id and
  handle `delete` (they do for Python — that's the point of the feature).
- Id reuse semantics after expiry (Python: fresh counter value mod 65536) —
  Rust `ensure_includable_id` currently returns a stable id per endpoint;
  after expiry it must allocate a new one to match.
- Small change overall (~100 LOC); independent of other plans except the
  config knob (06).
