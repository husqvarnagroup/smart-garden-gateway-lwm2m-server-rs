# 01 — Wake-on-Radio (WoR) subsystem

Covers PORTING_TODO.md "Missing parts" §1 and task-list items **WoR-1** (radio
module client) and **WoR-2** (wakeup manager).

## Goal

Battery devices with Wake-on-Radio sleep between radio windows. Before *any*
server-initiated CoAP operation (read, write, execute, FOTA, connectivity
ping) the server must ask lemonbeatd to send a wakeup frame, and track how
long the device stays awake so redundant wakeups are skipped.

## Python reference

- `lwm2mserver/wake_on_radio.py` — `DeviceWakeupManager`
- `lwm2mserver/radio_module.py` — `RadioModuleControl` (socket protocol)
- Call sites: `DeviceOperationHandler.read/write/execute` and the FOTA upload
  path in `lwm2mserver/lwm2mserver.py` (each wraps the operation in
  `wakeup_device(client_id, timeout_ms)` and maps radio-module errors to
  `DeviceUnreachableException`).

### Radio-module socket protocol (WoR-1)

- Unix stream socket at `<lemonbeatd-runtime-dir>/radiomodule_api`.
- A **new connection per wakeup** (survives lemonbeatd restarts without
  tracking socket state) — keep this behavior.
- Request framing: `[api_version=0x01][command][payload_len][payload]`.
  For `WAKEUP_DEVICE` (command `0x03`) the payload is 12 bytes:
  `duration_ms: u32 LE` + `mac: [u8; 6]` + `channel: u8` + `ack_req: u8 = 1`.
- Response: 3 header bytes `[api_version][result_code][length]`, then
  `length` payload bytes (drained and ignored for WAKEUP_DEVICE).
  `api_version != 1` → protocol error; short payload → length error.
- Result codes: `0x00 OKAY`, `0x06 WAKEUP_FAILED`, `0x05 LOCK_TIMEOUT`, …,
  `0xFF INTERNAL_ERROR` (full table in `radio_module.py::Result`; port the
  names for log messages).
- Retry loop: `wakeup_total_attempts` (default 3, env
  `LWM2MSERVER_WAKEUP_TOTAL_ATTEMPTS`) attempts; on non-OKAY sleep
  `wakeup_delay_retry_attempt_timeout` ms (default 2000, env
  `LWM2MSERVER_WAKEUP_SLEEP_BEFORE_RETRY_ATTEMPT_MS`) between attempts; all
  failed → error (device unreachable). Socket file missing
  (`FileNotFoundError`) → log error and return *without* retrying.
- No runtime directory configured → wakeup disabled, warn per attempt.

### Wakeup manager semantics (WoR-2)

- A device is WoR-capable iff it registered IPSO object **28184**
  (`client.has_object_versioned(28184)`); non-WoR devices skip everything.
- Wakeup channel comes from cached device state:
  `wake_on_radio/0/wakeup_channel` → `vi` value.
- Device MAC = **last 6 bytes of its IPv6 address**
  (`IPv6Address(session_addr).packed[-6:]`, see
  `wakaama/src/modules/connection.pyx::get_device_mac_address`).
- Awake-window tracking: map `sgtin → awake_until: Instant`.
  - `_is_device_awake`: remaining time must exceed the decision threshold
    **0.3 s** (`WAKEUP_DESICION_TRESHOLD_S`, race-condition guard, SG-23153).
  - `_needs_wakeup_update`: `duration_ms == 0` (put to sleep) → always send;
    untracked device → send; otherwise only send if the *new* wake deadline
    is later than the tracked one (**only extend, never shorten** — a FOTA
    wakeup of 30 min must not be cut short by a 4 s generic wakeup).
  - After a successful send, update tracking (`duration 0` removes the entry);
    prune expired entries opportunistically.
- Durations: generic ops 4000 ms (env `LWM2MSERVER_WAKEUP_TIMEOUT_MS`), FOTA
  1 800 000 ms (env `LWM2MSERVER_WAKEUP_FOTA_TIMEOUT_MS`).

## Current Rust state

- `src/lwm2m/client.rs` dispatches queued ops immediately; nothing wakes the
  device. Ops arrive via `DispatchRequest` from IPC (`src/ipc/command.rs`),
  housekeeping pings (`src/housekeeping.rs`), and the server's
  factory-reset path (`src/lwm2m/server.rs`).
- Device state (incl. `wake_on_radio` object) is already cached in
  `DeviceRegistry` (`model::Device::state`) and the registered object list in
  `Device::objects` (strings like `"28184/0"`), so both WoR detection and
  channel lookup are answerable from the registry.
- The device's IPv6 address is in `Device::addr` → MAC derivable.
- No config for the lemonbeatd runtime directory (see plan 06).

## Design

New module `src/lwm2m/wakeup.rs` (or `src/wakeup/` with two files):

1. **`RadioModuleClient`** (WoR-1)
   - `async fn wakeup_device(&self, mac: [u8; 6], channel: u8, duration_ms: u32) -> Result<(), WakeupError>`
   - Uses `tokio::net::UnixStream::connect(runtime_dir.join("radiomodule_api"))`
     per call; builds/parses the binary frame; maps result codes to a
     `WakeupResult` enum with the Python names for logging.
   - Retry loop + inter-attempt sleep as above; add an overall per-attempt
     I/O timeout (e.g. 2 s — Python relies on the socket, be defensive).
   - Constructor takes `Option<PathBuf>`; `None` → permanently disabled
     (warn on first use, `debug` afterwards to avoid log spam).

2. **`DeviceWakeupManager`** (WoR-2)
   - Shared handle (`Arc<Mutex<HashMap<String, Instant>>>` inside a cheap
     `Clone` struct) + `RadioModuleClient` + `DeviceRegistry`.
   - `async fn wakeup(&self, addr: SocketAddr, duration_ms: u32) -> Result<(), WakeupError>`:
     look up device by addr; return `Ok` early when: unknown device, not
     WoR-capable (no object `28184` in `Device::objects`), or already awake
     past the 0.3 s threshold with a deadline ≥ the requested one.
     Channel from `state["wake_on_radio"]["0"]["wakeup_channel"]["vi"]`;
     missing channel → log warn, skip (Python asserts; be tolerant).
   - Keep the "extend, never shorten" rule and `duration 0 = sleep` special
     case even though nothing calls sleep yet (FOTA teardown may later).

3. **Hook into dispatch** — single choke point in
   `src/lwm2m/client.rs::dispatch_op` (all outbound ops flow through it,
   including pings and FOTA):
   - Before building/sending, call
     `wakeup_manager.wakeup(addr, duration).await`.
   - Duration: FOTA (`LwM2mCommand::Write` to `FOTA_PATH`) →
     `wakeup_fota_timeout_ms()`; everything else →
     `wakeup_generic_timeout_ms()`.
   - On `WakeupError` → complete the op with `Err(LwM2mError::Timeout)`
     (device unreachable) and mark the device offline, mirroring Python's
     `DeviceUnreachableException`.
   - For multi-block FOTA the wakeup happens once up front (Python does the
     same: one wakeup with the long FOTA duration before `wakaama.write`).

4. **Plumbing**: construct the manager in `main.rs` (and `tests/common`),
   pass into `lwm2m::client::run`. New config (see plan 06):
   `--lemonbeatd-runtime-directory` CLI arg with `LEMONBEATD_RUNTIME_DIRECTORY`
   env fallback; wakeup attempt/delay/timeout env overrides.

## Logging (per CLAUDE.md rules)

- INFO on success: `Device wake-up succeeded on channel <ch> for <ms> ms`
  (device field = sgtin, activity = `state`).
- INFO per failed attempt with result-code name; ERROR when the socket file
  is missing; WARN when wakeup is disabled (no runtime dir).
- DEBUG for remaining-awake-time decisions.

## Testing

- Unit-test the frame encoding/decoding against byte fixtures taken from
  `radio_module.py` (e.g. duration 10000, mac 6×`0xAA`, channel 5 →
  `01 03 0c 10 27 00 00 aa aa aa aa aa aa 05 01`).
- Spawn a fake `radiomodule_api` Unix server in tests: assert retry count,
  retry delay behavior (use `start_paused`), FileNotFound short-circuit,
  api-version mismatch error.
- Wakeup-manager unit tests: non-WoR device skips, threshold logic
  (remaining 0.2 s → re-wake; 0.5 s → skip), extend-only rule, duration-0
  removal. Port scenarios from Python `tests/test_device_wakeup.py`.
- Integration: extend `tests/common::TestGateway` with an optional fake
  radiomodule socket; verify a `write` to a WoR device hits the fake before
  the CoAP PUT arrives.

## Risks / open questions

- Ordering: ops for one device are spawned concurrently in `client.rs`; two
  concurrent ops may race the awake check. The tracking map's mutex makes the
  check-and-send atomic enough (worst case one redundant wakeup) — same as
  Python.
- `run_tests.sh`/component tests must run without lemonbeatd — disabled mode
  covers that.
- Estimated size: ~400 LOC + tests. No new crate dependencies.
