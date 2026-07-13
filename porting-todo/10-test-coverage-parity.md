# 10 — Test-coverage parity

Covers PORTING_TODO.md "Missing parts" §10 and task-list item **TEST-1**.

## Goal

Port the *behavioral scenarios* of the Python test suite (~20 modules) to
Rust integration tests — not a 1:1 translation: many Python tests cover
wakaama/Cython plumbing that has no Rust counterpart. Focus on bootstrap
edge cases, FOTA failure paths, and connectivity transitions, which
PORTING_TODO singles out as thin.

## Inventory: Python modules → Rust mapping

| Python test module | What it covers | Rust status / target |
|---|---|---|
| `test_inclusion.py` | full inclusion flows: authenticate → allow → write phase; resent `/bs` mid-flow; restart mid-bootstrap | partial (happy path via component tests); port edge cases → `tests/bootstrap_tests.rs` (new) |
| `test_certificate_authorities.py` | CA selection, multiple CAs, invalid certs | `bootstrap.rs` unit tests exist for validation; add multi-CA cases with plan 06's `--device-ca-file` |
| `test_crypto.py` | ECDH + AES key encryption + CRC16 | ported (`bootstrap.rs` round-trip unit tests) — verify vector parity once |
| `test_connection.py` | UDP session handling, multicast/pktinfo, truncation | new `recvmsg` path from plan 05 needs equivalents: pktinfo parse, unicast-bootstrap rejection |
| `test_connection_status.py` | online/offline transitions, ping pacing, offline retry window | thin in Rust: only implicit via other tests → `tests/connectivity_tests.rs` (new) with `start_paused` time control |
| `test_device_wakeup.py` | WoR manager decision logic + radio-module protocol | plan 01 brings its own unit/integration tests — port assertions from here |
| `test_fota.py` | multi-block upload, SZX renegotiation, device abort, timeout mid-transfer, concurrent-upload lock | Rust has happy-path block tests; port failure paths → extend `tests/coap_tests.rs` or new `tests/fota_tests.rs` |
| `test_e2e_senml_cbor.py` | `/dp` decode fixtures end-to-end incl. exotic value types | partial (`server.rs` unit tests); port remaining fixtures verbatim (CBOR hex strings) |
| `test_state.py`, `test_clients_state.py` | merge vs overwrite semantics, urn handling, persistence atomicity | merge covered implicitly; overwrite comes with plan 02 — port `dictdiffer`-based cases as JSON fixtures |
| `test_ipso.py` | IPSO XML parsing, versioning, name mapping | ported (ipso.rs unit tests) — diff the case lists once |
| `test_operation_handler.py` | command-socket read/write/execute incl. error responses | partial (`ipc_tests.rs` is minimal); grow with plans 02/08/09 |
| `test_lwm2mserver_service_plane.py` | includable devices, expiry, debug endpoints, bootstrap-requests events | comes with plans 07/09 |
| `test_timezone.py` | TZif parsing, skip/write decisions | comes with plan 03 |
| `test_lwm2mserver.py`, `test_server.py`, `test_processes.py`, `test_utils.py` | wakaama wrapper, process/SM plumbing, ExpiringSet, CRC | mostly N/A (implementation-specific); CRC + expiry semantics land via plans 06/07 unit tests |
| `test_zephyr.py` + `tests/manual`, `tests/tfw` | integration against real Zephyr device firmware | out of scope for cargo test; see "device simulator" below |
| `wakaama/tests/integration/*` (registration, bootstrap, device mgmt, reporting interfaces) | protocol-level interface tests | the interesting cases (lifetime handling, content formats, block1 registration) → `tests/coap_tests.rs` |

## Plan of attack

1. **Build a reusable fake device** (`tests/common/fake_device.rs`):
   a scripted UDP endpoint that can bootstrap, register, answer
   GET/PUT/POST with configurable status/payload/delay/silence, and assert
   what it received. Most missing scenarios need exactly this; today's
   tests hand-roll packets inline. Include helpers for SenML+CBOR payload
   construction (reuse `ciborium` in dev-deps).
2. **Priority order** (aligned with PORTING_TODO's call-outs):
   1. Bootstrap edge cases (`tests/bootstrap_tests.rs`):
      resent `/bs` while GET /0/0 in flight; `/bs` from an already-included
      device (Python: triggers exclusion — verify Rust parity!); RST during
      write phase; server restart mid-bootstrap (persistence of included
      set); cert-validation failure then retry.
   2. FOTA failure paths: abort status mid-transfer, silent device after
      block N (timeout + offline event), SZX renegotiation (exists at unit
      level — add e2e), concurrent-upload rejection.
   3. Connectivity transitions (`start_paused`): registration → online
      event; lifetime expiry → offline + deregistration; ping scheduling for
      online (9000 s) and offline (900 s) devices; 6 h offline give-up;
      device answering ping → back online.
   4. `/dp` SenML fixture parity (copy CBOR fixtures from
      `tests/auxiliary` / `test_e2e_senml_cbor.py`).
3. **Port fixtures, not test code**: lift byte-level fixtures (CBOR
   payloads, cert DER files — `tests/device-cert.der` etc.) directly from
   the Python repo into `tests/fixtures/` so both suites assert against the
   same bytes.
4. **Time control**: adopt `#[tokio::test(start_paused = true)]` for all
   interval-driven scenarios (housekeeping, expiry, retransmits) — the
   pattern already exists in `server.rs` retry tests. This keeps the suite
   fast (<1 s per scenario, no real sleeps).
5. **Coverage gate**: CI has a coverage gate; each ported module should keep
   or raise the line. Track parity in a checklist section at the bottom of
   this file as modules land.
6. **Zephyr/e2e**: keep out of `cargo test`. If the team wants hardware
   integration parity, wire the existing Python `tests/tfw` harness against
   the Rust binary instead (it talks sockets, not Python internals) — that
   is the cheapest path to running the full legacy suite against the port.
   Evaluate in a spike; document outcome.

## Sequencing with other plans

Feature plans (01–09) each carry their own tests; this plan owns:
- the fake-device harness (do **first** — plans 01/02/08 want it),
- the bootstrap/FOTA/connectivity scenario suites for *already-ported*
  functionality (independent, can start immediately),
- fixture porting.

## Checklist (tick as ported)

- [ ] fake device harness
- [ ] bootstrap edge cases
- [ ] FOTA failure paths
- [ ] connectivity transitions
- [ ] SenML+CBOR fixture parity
- [ ] state merge/overwrite fixtures
- [ ] multi-CA cases
- [ ] tfw-against-rust spike
