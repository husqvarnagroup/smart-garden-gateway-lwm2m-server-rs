# 04 — Lemonbeat dongle compatibility mode (`-ldc`)

Covers PORTING_TODO.md "Missing parts" §4 and task-list item **LDC-1
(decide)**.

## Goal (if ported)

Allow running the server against a Lemonbeat *gateway dongle* (dev/test
setups) instead of the built-in radio module. The dongle cannot route replies
to link-local sources, so the server must rewrite addresses and echo traffic
classes.

## Python reference

`lwm2mserver/connection.py::LemonbeatDongleConnection` (~line 389), enabled by
`-ldc` / `--lemonbeat-dongle-connection`:

1. **Source-address rewrite on receive**: bootstrap requests arrive with a
   link-local source `fe80::<EUI-64>`. If `from_addr.is_link_local()`,
   rewrite to unique-local `fc00::6:<MAC48>` by dropping the `ff:fe` filler
   from the EUI-64 (exploded-string slicing `[20:27] + [32:]`):
   `fe80::0011:22ff:fe33:4455` → `fc00::6:0011:2233:4455`.
   All session matching and replies then use the rewritten address.
2. **Scope-id stripping**: only `(addr, port)` are used for session identity
   (scope id changes and breaks session matching).
3. **Per-session traffic-class echo on send**: replies are sent with the
   traffic class the request arrived with (requires `IPV6_RECVTCLASS` on
   receive and `IPV6_TCLASS` cmsg on send), instead of the fixed
   `TC_PLAIN`/`TC_ENCRYPTED` scheme.

## Current Rust state

- `src/lwm2m/mod.rs::bind` creates the socket; `set_tclass` sets a fixed
  traffic class per send (`TC_PLAIN` 0x0c bootstrap, `TC_ENCRYPTED` 0x1c).
  `--no-encryption` (`disable_tclass`) already exists for dev setups.
- Receive path (`src/lwm2m/server.rs::run`) uses plain `recv_from`; no
  ancillary data, no address rewriting; `SocketAddr` (incl. scope id) is the
  peer key everywhere (`DeviceRegistry::by_addr`).

## Recommendation: decide first — likely **drop**

Arguments for dropping:
- Only needed for dongle-based dev/test rigs; production gateways use the
  integrated radio + lemonbeatd, which the Rust port already targets.
- The Rust port already has `--no-encryption` for permissive dev setups.
- Cost is non-trivial: it drags in `recvmsg`/`sendmsg` ancillary-data
  handling (shared prerequisite with plan 05) *plus* address rewriting
  through the whole registry/bootstrap path.

If the team still uses dongle rigs for device bring-up, port it — the test
benefit is real. **Action: ask the team; record the decision in
SPECIFICATION.md "Deviations" either way.**

## Design (if ported)

1. **Prerequisite**: switch the receive path to `recvmsg` with ancillary
   data (shared with plan 05 — implement once):
   - Set `IPV6_RECVTCLASS` (and `IPV6_RECVPKTINFO`, plan 05) at bind time.
   - Replace `socket.recv_from` with a small `recv_msg()` helper on the
     tokio socket: `try_io`/`AsyncFd` + `nix::sys::socket::recvmsg`
     (new dependency `nix` with `socket`/`uio` features — MSRV-check and pin
     like other deps) returning
     `(bytes, from: SocketAddr, to: Option<Ipv6Addr>, tclass: Option<u8>)`.
2. **`--ldc` flag** in `src/config.rs` (default off).
3. **Address rewrite** immediately after receive in `server.rs::run`, before
   any handler sees the packet:
   ```
   fn ldc_rewrite(addr: SocketAddrV6) -> SocketAddrV6 // fe80::EUI64 → fc00::6:MAC48, scope_id = 0
   ```
   Pure function over the 16 address bytes (bytes 8..16 hold the EUI-64;
   drop bytes 11..13 `ff fe`), unit-testable. Non-link-local addresses pass
   through with scope id zeroed when `--ldc` is on.
4. **Reply traffic class**: store the received tclass per peer (e.g. a small
   `HashMap<SocketAddr, u8>` beside the registry, or a field on
   `Device`/bootstrap session) and, when `--ldc` is on, have
   `send_bootstrap_packet`/`send_encrypted_packet` use the stored value via
   `set_tclass` instead of the fixed constants.
5. Outbound packets go to the rewritten `fc00::6:<mac>` address — this falls
   out automatically because the registry only ever sees rewritten
   addresses.

## Testing

- Unit: address rewrite vectors (the docstring example above; non-EUI-64
  link-locals; already-unique-local addresses untouched).
- Integration is hard without a dongle; keep to unit level + a loopback test
  that verifies scope-id stripping doesn't break session matching.

## Effort

Small once the `recvmsg` groundwork from plan 05 exists (~150 LOC + tests);
do plan 05 first regardless of the drop/port decision.
