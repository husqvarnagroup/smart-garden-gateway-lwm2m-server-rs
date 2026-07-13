# Porting plans

Detailed implementation plans for the gaps listed in
[`../PORTING_TODO.md`](../PORTING_TODO.md) ("Missing parts" §1–§10), based on
the Python reference implementation in `~/projects/sg-bnw-lwm2m-server`.
Each file is one work package with Python references (file/line), current
Rust state, design, testing strategy, and open questions.

| Plan | PORTING_TODO | Task IDs | Depends on |
|---|---|---|---|
| [01 — Wake-on-Radio subsystem](01-wake-on-radio.md) | §1 | WoR-1, WoR-2 | 06 (config knobs) |
| [02 — Device-targeted READ](02-device-read.md) | §2 | READ-1 | 01 (wakeup, soft) |
| [03 — Timezone handler](03-timezone-handler.md) | §3 | TZ-1 | 01 (soft), zbus/MSRV decision |
| [04 — Lemonbeat dongle mode](04-lemonbeat-dongle-mode.md) | §4 | LDC-1 | **decision needed**; 05 (recvmsg groundwork) |
| [05 — Multicast-only bootstrap](05-multicast-only-bootstrap.md) | §5 | BS-1 | 06 (flag) |
| [06 — Configuration parity](06-configuration-parity.md) | §6 | CFG-1 | — |
| [07 — Includable-device expiry](07-includable-device-expiry.md) | §7 | INC-1 | 06 (env knob) |
| [08 — FOTA progress/result events](08-fota-progress-result-events.md) | §8 | FOTA-1 | 06 (`--debug`), consumer-format capture |
| [09 — Debug introspection endpoints](09-debug-introspection-endpoints.md) | §9 | DBG-1 | 06 (`--debug`) |
| [10 — Test-coverage parity](10-test-coverage-parity.md) | §10 | TEST-1 | fake-device harness first |

**DOC-1** (spec upkeep) is cross-cutting: every plan lists what to record in
`SPECIFICATION.md` when it lands; no separate plan file.

## Suggested order

1. **06** (config) — small, unblocks 01/05/07/08/09.
2. **10** (fake-device harness + tests for already-ported behavior) — in
   parallel; de-risks everything else.
3. **01** (WoR) — biggest functional gap, per PORTING_TODO.
4. **05** (multicast bootstrap) — security-relevant; builds the `recvmsg`
   groundwork that 04 would need.
5. **02** (device read), **07** (includable expiry), **08** (FOTA events),
   **09** (debug endpoints) — independent of each other.
6. **03** (timezone) after the zbus-vs-filewatch decision.
7. **04** (dongle mode) only if the team decides to keep it.

## Findings made while writing these plans

- PORTING_TODO §3 misstates the timezone resources: Python writes
  `device/0/timezone` (POSIX TZ string), not
  `current_time`/`utc_offset` — see plan 03.
- Python FOTA *progress* events are debug-mode-only; only the *result*
  event is unconditional — see plan 08.
- The device MAC needed for WoR wakeups is simply the last 6 bytes of the
  device's IPv6 address — no session plumbing needed in Rust (plan 01).
- Bootstrap-timeout values differ between the ports (Python 10 s inactivity
  vs Rust 30 s) — adopting env defaults will surface this (plan 06).
