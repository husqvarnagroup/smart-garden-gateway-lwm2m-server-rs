# Porting Gaps: sg-bnw-lwm2m-server (Python) → lwm2mserver-rs

Status of the Rust port compared to the Python original, as of 2026-07-09.
Complements the "Deviations from Python Code" section in `SPECIFICATION.md`:
that section records *accepted* differences; this document lists what is
actually **missing** and turns it into a task list for future work.

## Already ported (no action needed)

- CoAP transport (UDP/IPv6, traffic class 0x0C/0x1C, `SO_BINDTODEVICE`)
- Bootstrap flow incl. certificate validation, SGTIN check, network-key
  encryption (ECDH + AES-128-ECB + CRC16), write phase with retransmission
- Registration lifecycle (`/rd`, updates, deregistration, expiry, factory
  reset of non-included devices)
- `/dp` data push: SenML+CBOR decode, IPSO name resolution, state merge,
  event publication, per-device state files
- IPSO model with versioned objects, snake_case naming, SIGHUP hot reload
- IPC command/event sockets with the JSON envelope protocol
- Write/Execute operations, exclusion, FOTA upload with Block1 and SZX
  negotiation
- Persistence (`wakaama.json` v2, `included_devices.json`,
  `devices/<sgtin>.json`, startup consistency check)
- Connectivity monitoring (online/offline pings via housekeeping)
- Structured logging, systemd `sd_notify` (Rust-only addition)

## Missing parts

### 1. Wake-on-Radio (WoR) subsystem — biggest functional gap

Python: `wake_on_radio.py` (`DeviceWakeupManager`) + `radio_module.py`
(`RadioModuleControl`).

- Detects WoR-capable devices via IPSO object **28184** and reads
  `wake_on_radio/0/wakeup_channel` from device state.
- Before *every* device-targeted operation (read, write, execute, FOTA),
  sends a `WAKEUP_DEVICE` command over the lemonbeatd Unix socket
  (`<lemonbeatd-runtime-dir>/radiomodule_api`, binary framing:
  version, command, length, payload) with retries
  (`wakeup_total_attempts`, delay between attempts).
- Tracks per-device awake windows (`duration_ms`, 0 = put to sleep) with a
  0.3 s decision threshold; only extends, never shortens, awake time
  (important for FOTA).
- Needs the device MAC address from the UDP session.

Without this, battery/WoR devices will sleep through server-initiated
operations. Rust currently dispatches immediately.

### 2. Device-targeted READ operation

Python `DeviceOperationHandler.read()` forwards `read` commands from the
command socket to the device (with wakeup) and additionally publishes the
fresh values as an **`overwrite`** event on the event socket. Rust only
supports `read devices` (cached state); the `overwrite` op does not exist
in the Rust protocol at all (state.py distinguishes merge vs overwrite).

### 3. Timezone handler

Python `TimezoneHandler`: listens on D-Bus (`org.freedesktop.timedate1`)
for timezone changes, derives the POSIX TZ string from `/etc/localtime`,
and writes `device/0/current_time` + `device/0/utc_offset` to included
devices when the timezone changes or a device comes online.

### 4. Lemonbeat dongle compatibility mode (`-ldc`)

Python `LemonbeatDongleConnection`: rewrites link-local (`fe80::`) source
addresses to unique-local (`fc00::`) during bootstrap and controls the
traffic-class encryption flag, so a gateway dongle can route traffic.
Rust uses addresses as-is. Only needed for dongle-based dev/test setups —
decide whether to port or drop permanently.

### 5. Multicast-only bootstrap enforcement

Python by default only accepts bootstrap requests addressed to a multicast
destination (detected via `IPV6_RECVPKTINFO`); `--unicast-bootstrap` opts
out. Rust accepts bootstrap from any address unconditionally. Security-
relevant: decide and either implement or document as accepted.

### 6. Configurability gaps (hardcoded in Rust)

| Python option | Rust today |
|---|---|
| `--state-storage <dir>` | hardcoded `/var/lib/lwm2mserver` |
| `path` positional (eventbus socket dir) | hardcoded `/tmp/lwm2mserver-*.ipc` |
| `--device-ca-file <pem>` (repeatable) | two CAs hardcoded in `bootstrap.rs` |
| `--allow-all` / `--allow <sgtin>` (auto-approve inclusion) | not available |
| `--no-load-state` | not available |
| env-var overrides for timeouts (registration lifetime, bootstrap timeouts, FOTA delay, wakeup attempts, online/offline/initial timeouts, …) | constants in code |

### 7. Includable-device expiry

Python's `ExpiringSet` expires `includable_device/<n>` entries after a
configurable duration and publishes a delete event so UIs drop stale
entries. Rust keeps approval state and the id mapping until the write
phase completes or it is explicitly removed.

### 8. Long-running-operation progress/result events

Python FOTA publishes `operation/<id>` **progress** (percent, per-block)
and **result** events under the service entity, plus an operation lock
with timeout. Rust reports only the final outcome via a
`firmware_update/0/package` event. Consumers relying on progress
reporting will not work.

### 9. Debug / service-plane introspection endpoints

Python (debug mode) command-socket paths: `read bootstrap-status`,
`registration-status`, `connectivity-status`, `state-graph/<dev>`,
`bootstrap-graph/<dev>`, `connectivity-graph/<dev>`; `execute
bootstrap-requests/authenticate|allow`. Useful for field troubleshooting;
none exist in Rust.

### 10. LWM2M 1.0 / TLV content format

Python supported LWM2M 1.0 (TLV) and 1.1 (SenML+CBOR); Rust is
SenML+CBOR-only. Only needed if devices with old firmware must still be
onboarded — confirm fleet status before investing.

### 11. Test-coverage parity

Python has ~20 test modules (inclusion, exclusion, FOTA, connection
status, device wakeup, timezone, crypto, service plane, e2e SenML-CBOR,
Zephyr-based integration tests). Rust has three integration test files
(`coap_tests`, `ipc_tests`, `event_tests`). Behavioural coverage for
bootstrap edge cases (restart mid-bootstrap, resent requests), FOTA
failure paths, and connectivity transitions is much thinner.

## High-level task list

Ordered roughly by functional impact; each item is intended to be one
work package.

- [ ] **WoR-1: Radio module client** — implement the lemonbeatd
      `radiomodule_api` Unix-socket protocol (WAKEUP_DEVICE, result codes,
      retries); add `--lemonbeatd-runtime-directory` CLI arg / env var.
- [ ] **WoR-2: Wakeup manager** — awake-window tracking per device,
      object-28184 detection, `wakeup_channel` lookup from device state;
      hook into the CoAP dispatch task so every outbound operation (incl.
      FOTA and connectivity pings) wakes the device first; MAC address
      must be carried in the session/registry.
- [ ] **READ-1: Device-targeted read** — support `read
      <object>/<inst>/<res>` on the command socket (dispatch CoAP GET,
      decode SenML+CBOR reply) and publish the result as an `overwrite`
      event; add overwrite semantics (replace, not merge) to state
      handling.
- [ ] **TZ-1: Timezone handler** — D-Bus `timedate1` listener (e.g. via
      `zbus`), POSIX TZ string derivation, write `current_time` /
      `utc_offset` on change and on device-online.
- [ ] **BS-1: Multicast-only bootstrap** — receive destination address via
      `IPV6_RECVPKTINFO`, reject unicast bootstrap unless
      `--unicast-bootstrap` is set (or document the deviation as final).
- [ ] **CFG-1: Configuration parity** — CLI/env options for state
      directory, socket paths, device CA files, `--allow-all`/`--allow`,
      `--no-load-state`, and timeout overrides.
- [ ] **INC-1: Includable-device expiry** — expire includable entries
      after a configurable duration and emit delete events.
- [ ] **FOTA-1: Progress/result events** — per-block progress events and
      an operation-id-based result event compatible with existing
      consumers (verify what consumers actually use before building).
- [ ] **DBG-1: Introspection endpoints** — debug-gated command-socket
      reads for bootstrap/registration/connectivity status (state graphs
      optional).
- [ ] **LDC-1 (decide): Dongle compatibility mode** — port fe80→fc00
      address translation or formally drop it.
- [ ] **TLV-1 (decide): LWM2M 1.0/TLV support** — confirm whether any
      fleet devices still require TLV; port or close as won't-fix.
- [ ] **TEST-1: Test parity** — port the Python test scenarios
      (inclusion/exclusion flows, FOTA failure paths, connectivity
      transitions, bootstrap restart/resend) to Rust integration tests.
- [ ] **DOC-1: Spec upkeep** — update `SPECIFICATION.md` as each gap is
      closed (several are currently listed there as deviations).
