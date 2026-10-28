# 03 — Timezone handler

Covers PORTING_TODO.md "Missing parts" §3 and task-list item **TZ-1**.

> **Correction to PORTING_TODO.md**: the Python handler does *not* write
> `device/0/current_time` + `device/0/utc_offset`. It writes a single
> resource: `device/0/timezone` with the **POSIX TZ string** (`vs`).
> Update PORTING_TODO.md/SPECIFICATION.md when porting.

## Goal

Keep the `timezone` resource of the Device object (`/3/0/15`, IPSO name
`device/0/timezone`) on all included devices in sync with the gateway's
timezone — on startup, whenever the system timezone changes, and whenever a
device (re)appears.

## Python reference

`lwm2mserver/lwm2mserver.py::TimezoneHandler` (~line 3007):

- **TZ string source**: `/etc/localtime` is a TZif file; the POSIX TZ string
  is its footer — read the file, split on `\n`, take the second-to-last
  entry (`get_posix_tz_string`). Errors → warn, TZ string `None` (then
  nothing is written).
- **Triggers**:
  1. `activate()` at startup: fetch TZ, enqueue update of *all* devices.
  2. D-Bus: subscribe to `PropertiesChanged` on bus name
     `org.freedesktop.timedate1`, object `/org/freedesktop/timedate1`,
     property `Timezone` (system bus). On change: re-fetch TZ string from
     `/etc/localtime` (the D-Bus value itself is the IANA name, not the
     POSIX string) and update all devices.
  3. Device online transition (connectivity hook).
  4. Resource update containing the `device` object in the decoded payload
     (i.e. after a `/dp` push that includes the device object).
- **Per-device update** (`_update_device_timezone_if_needed`):
  - Skip when the device object isn't in cached state yet (mid-inclusion).
  - Skip when cached `device/0/timezone` `vs` already equals the current TZ
    string.
  - Otherwise issue a normal device WRITE
    (`device/0/timezone`, payload `{"vs": tz}`) through the operation
    handler — i.e. it flows through wakeup and produces normal logs/events.
  - Failures are logged (info) and not retried; the next trigger retries.
- Activity for logging: `time-sync`.

## Current Rust state

- No D-Bus usage anywhere; no `zbus`/`dbus` dependency.
- Device writes from inside the server exist as a pattern: build a
  `PendingOperation` + send `DispatchRequest` (see factory reset in
  `src/lwm2m/server.rs` and pings in `src/housekeeping.rs`).
- Online transitions are detected in `server.rs::handle_packet` via
  `registry.touch(addr)` (emits connection-status event) — a natural hook
  point.
- `/dp` handling (`server.rs::handle_dp`) knows which objects a push
  contained — hook point for trigger 4.
- Device state lookup: `DeviceRegistry::device_state_by_endpoint`.

## Design

New task `src/timezone.rs`, spawned from `main.rs` alongside housekeeping:

1. **TZ string derivation** — pure function
   `posix_tz_string(path: &Path) -> Option<String>` reading the TZif footer
   exactly like Python (last-but-one `\n`-separated chunk). Unit-testable
   with fixture files (copy a couple of zoneinfo files into
   `tests/fixtures/`; e.g. `Europe/Zurich` → `CET-1CEST,M3.5.0,M10.5.0/3`).

2. **Change detection** — two options:
   - **(a) D-Bus parity (preferred)**: add `zbus` and subscribe to
     `PropertiesChanged` for `org.freedesktop.timedate1`.
     *MSRV check required*: the project pins Rust 1.75 (Yocto); zbus 4/5
     need newer toolchains — zbus `3.x` supports older Rust and would need
     to be pinned like the other held-back deps in `Cargo.toml`. Evaluate
     binary-size impact (`opt-level = "z"` target); zbus pulls a sizeable
     tree.
   - **(b) File-watch fallback**: poll `/etc/localtime` (mtime/symlink
     target) from the housekeeping tick (60 s). No new dependency, slightly
     delayed reaction. Acceptable deviation if zbus is too heavy — document
     in SPECIFICATION.md if chosen.
   Decision gate: try (a) behind MSRV; fall back to (b).

3. **Trigger wiring**:
   - Startup: after registry restore, compute TZ and enqueue updates for all
     included+registered devices.
   - Online transition: `server.rs::handle_packet` already knows when
     `touch()` reports a transition — send the endpoint through an
     `mpsc::Sender<TzTrigger>` owned by the timezone task (avoids doing
     writes inline in the packet path).
   - `/dp` containing the `device` object: same channel, from `handle_dp`.
   - TZ change: the task updates its cached string and iterates all devices.

4. **Per-device update** in the task:
   - `registry.device_state_by_endpoint(ep)` → skip if no `device` key; skip
     if `state["device"]["0"]["timezone"]["vs"] == current`.
   - Resolve `device/0/timezone` via `IpsoModel` (same code path as IPC
     writes) and dispatch a `LwM2mCommand::Write` with SenML+CBOR payload
     `{"vs": tz}` — reuse the write-encoding helper used by
     `ipc::command::handle_write_path` rather than duplicating it.
   - Log with `activity = "time-sync"` (add to the CLAUDE.md activity list?
     — Python uses `TIME_SYNC`; check the syslog consumer accepts it).

5. **Config**: none beyond the optional dependency; timezone file path
   hardcoded `/etc/localtime` (parameterize only for tests).

## Testing

- Unit: TZif footer parsing (valid file, file without footer, missing file).
- Unit: skip logic (no device object; TZ already correct → no dispatch).
- Integration: TestGateway + fake device: mark device online with a stale
  `timezone` in state → expect a CoAP PUT to `/3/0/15` with the TZ string;
  correct TZ in state → expect no PUT. Port scenarios from Python
  `tests/test_timezone.py`.
- D-Bus part: keep isolated behind a trait so tests can inject TZ changes
  without a bus.

## Risks / open questions

- **zbus vs MSRV/binary size** is the main decision (see 2a/2b).
- Writing to a sleeping WoR device requires plan 01 first, otherwise
  timezone writes to battery devices will just time out (Python order was
  the same: wakeup existed before TZ handler).
- `device/0/timezone` must exist in the shipped IPSO definitions — verify
  the resource name/id mapping in the IPSO XMLs loaded on the gateway.
