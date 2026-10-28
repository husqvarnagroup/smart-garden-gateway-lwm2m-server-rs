# 09 — Debug / service-plane introspection endpoints

Covers PORTING_TODO.md "Missing parts" §9 and task-list item **DBG-1**.

## Goal

Field-troubleshooting endpoints on the command socket, available only when
the server runs with `-d/--debug`: read bootstrap / registration /
connectivity status per device, and manually drive the bootstrap state
machine (authenticate / allow).

## Python reference

`lwm2mserver/lwm2mserver.py::ServiceOperationHandler.handle_operation`
(~lines 1959–2042), all gated on `self._debug`:

| Op | Path | Behavior |
|---|---|---|
| `execute` | `bootstrap-requests/authenticate` | payload `{device: {vs: <sgtin>}}`; advances the bootstrap SM as if the cert GET succeeded; error text includes current SM state |
| `execute` | `bootstrap-requests/allow` | approve inclusion by name (bypasses UI) |
| `read` | `bootstrap-status` | `{ <device>: {…} }` from `_bootstrap_status` (populated by bootstrap hooks: request/authenticated/allow/start/end + timestamps) |
| `read` | `registration-status` | `{ <device>: {…} }` from registration/update/deregistration hooks (update/dereg hooks are only installed in debug mode) |
| `read` | `connectivity-status` | per device: `state` (`vs`), `last_online`/`last_offline`/`last_check`/`next_timeout` (`vt` timestamps) |
| `read` | `state-graph/<dev>`, `bootstrap-graph/<dev>`, `connectivity-graph/<dev>` | DOT graphs of the python-statemachine instances |

Also debug-only: `_publish_bootstrap_request` events on service path
`bootstrap-requests` (used by tests).

## Current Rust state

- `src/ipc/command.rs::handle_request` — easy to extend; unhandled reads
  currently return `success:false`.
- No `--debug` flag (plan 06).
- The information mostly exists but is not aggregated:
  - bootstrap: `BootstrapRegistry` (pending sessions, cert cache, approvals,
    includable ids) — no per-device status timestamps.
  - registration: `DeviceRegistry` (`registered_at`, `last_registered_at`,
    `lifetime`, `objects`, `lwm2m_version`, `binding_mode`).
  - connectivity: `Device::{online, offline_since, last_contact,
    last_ping_attempt}`.
- There are **no state machines** in the Rust port → the three `*-graph`
  endpoints have no meaningful equivalent.

## Design

1. **Scope decision**: implement the three status reads + two executes;
   declare the graph endpoints N/A (no state machines) in
   SPECIFICATION.md — PORTING_TODO already marks graphs "optional".
2. **`--debug` gate** (plan 06): thread `debug: bool` into `IpcCtx`. Non-debug
   servers answer these paths with the generic unhandled response
   (`success:false`), identical to today.
3. **`read connectivity-status`** — straightforward projection of
   `DeviceRegistry`: add
   `DeviceRegistry::connectivity_report() -> serde_json::Value` returning
   per endpoint `{state: {vs}, last_online: {vt}, last_offline: {vt},
   last_check: {vt}, next_timeout: {vt}}`. Map: `state` from
   `online: Option<bool>` (`ONLINE`/`OFFLINE`/`UNKNOWN` — Python uses SM
   state names; choose stable uppercase strings and document),
   `last_check` = `last_ping_attempt`, `next_timeout` = when housekeeping
   would next ping (derivable from the ping-interval constants). Timestamps:
   registry stores `Instant`s — convert to Unix by anchoring
   (`SystemTime::now() - instant.elapsed()`).
4. **`read bootstrap-status`** — add a small ring/status map to
   `BootstrapRegistry`: per endpoint record timestamps of
   `request_received`, `authenticated` (cert validated), `allowed`
   (approval), `write_started`, `write_finished(success)`. Update at the
   existing call sites in `server.rs::handle_bootstrap` /
   `bootstrap_write_phase`. Keep it debug-gated (don't collect when not in
   debug mode) to avoid unbounded growth; cap entries (e.g. last 32
   devices).
5. **`read registration-status`** — per endpoint: `registered_at`,
   `last_update`, `lifetime`, `addr`, `lwm2m_version`, `objects` count —
   directly from `DeviceRegistry::snapshot()` (already exists for
   persistence) plus last-update timestamp.
6. **`execute bootstrap-requests/allow`** — by device name: look up the
   includable id for the endpoint (`BootstrapRegistry`), approve like the
   existing `includable_device/<id>/include` path. Error text mirrors
   Python's `"Can't allow device. State: …"` loosely (no SM states — use
   the bootstrap-status phase).
7. **`execute bootstrap-requests/authenticate`** — Rust's flow
   authenticates automatically when the cert GET succeeds; the manual
   version should mark the endpoint's cert as trusted without the GET
   (insert a fake/flagged entry into the cert cache is *wrong* — instead
   add an `authenticated_override: HashSet<String>` that
   `handle_bootstrap`/validation consults in debug mode). Only worth it if
   the debug workflow is actually used for devices whose cert cannot be
   read; otherwise return a clear "not supported" error and document.
   **Ask the team which debug workflows they actually use.**
8. **Payload conventions**: all values wrapped in the usual typed keys
   (`vs`/`vt`/`vi`) with `ts` — match the shapes in the Python code above.

## Testing

- Integration tests in `tests/ipc_tests.rs`:
  - non-debug: `read bootstrap-status` → `success:false`;
  - debug: after a scripted bootstrap + registration, all three reads return
    the expected per-device fields;
  - `execute bootstrap-requests/allow` approves and next `/bs` starts the
    write phase (compare with existing inclusion test flow).
- Keep the status collection out of hot paths (behind `if debug` checks) —
  assert no behavioral change in non-debug integration tests.

## Effort / dependencies

- Depends on plan 06 (`--debug`).
- ~250 LOC. Pure additive; low risk. Graphs intentionally dropped.
