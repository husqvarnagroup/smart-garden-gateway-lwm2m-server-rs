# 05 — Multicast-only bootstrap enforcement

Covers PORTING_TODO.md "Missing parts" §5 and task-list item **BS-1**.
Security-relevant.

## Goal

Only accept `POST /bs` bootstrap requests that were sent to a **multicast**
destination (the legitimate device flow sends to `ff02::1`); reject requests
addressed to our unicast address unless `--unicast-bootstrap` is given
(dev/test escape hatch). This raises the bar for an attacker who knows the
server's unicast address but can't (or won't) multicast on-link.

## Python reference

- `lwm2mserver/connection.py::UdpConnection` (~line 84): sets
  `IPV6_RECVPKTINFO` at socket creation; `_recvmsg` extracts the packet's
  destination address (`in6_pktinfo.ipi6_addr`) from ancillary data and
  stores it as `to_addr` in per-session `UdpSessionData`.
- Session workaround (~line 191): if a *multicast* packet matches an
  existing session whose stored `to_address` is unicast (stale session from
  before deregistration), the session's user data is overwritten with the
  multicast destination — otherwise re-bootstrap after deregistration would
  be wrongly rejected. Port this behavior.
- Enforcement in the bootstrap callback (`lwm2mserver.py` ~line 777):
  ```python
  if user_data.to_address and not user_data.to_address.is_multicast \
      and not allow_unicast_bootstrap:
      warn("Not allowing bootstrap on unicast address (…)")
      return BOOTSTRAP_NO_ACTION   # request silently ignored, no reply
  ```
  Note: enforcement happens for the *bootstrap flow only*; registration and
  data traffic are unicast as normal.
- CLI: `--unicast-bootstrap` (default: enforcement ON).
- If `IPV6_RECVPKTINFO` is unavailable: warn "multicast bootstrap handling
  will not work" (then `to_addr` is `None` and the check is skipped — fails
  open; keep that semantic or tighten deliberately).

## Current Rust state

- `src/lwm2m/mod.rs::bind` builds the socket with `socket2` but sets no
  `IPV6_RECVPKTINFO`.
- `src/lwm2m/server.rs::run` uses `socket.recv_from` — destination address
  is unavailable; `handle_bootstrap` accepts any source unconditionally.
- There is no per-peer session object; the bootstrap flow is keyed by
  endpoint/token in `BootstrapRegistry`.

## Design

1. **Receive destination addresses** (shared groundwork with plan 04):
   - In `bind()`: `sock.set_recv_tclass_v6?` no — needed option is
     `IPV6_RECVPKTINFO`. `socket2` ≥0.5 exposes
     `Socket::set_recv_ipv6_pktinfo`? — verify; if not available in the
     pinned version, set via `libc::setsockopt` directly (3 lines, Linux +
     macOS names differ: on macOS it's `IPV6_RECVPKTINFO` too with
     `#[cfg]`-guarded constants).
   - Replace the `recv_from` in `server.rs::run` with a `recv_msg` helper:
     wrap the tokio socket with `AsyncFd`-style `try_io` and
     `nix::sys::socket::recvmsg` (or hand-rolled `libc::recvmsg`) with a
     cmsg buffer; parse `IPV6_PKTINFO` → `to_addr: Option<Ipv6Addr>`.
     Return `(len, from, to_addr)`. Keep the plain `recv_from` fallback for
     non-Linux dev builds behind `#[cfg]` if the helper is Linux-only.
2. **Plumb `to_addr`** into `handle_packet` → `handle_bootstrap` (add a
   field to the packet-context/arguments; if the tower refactor from the
   `gardena/lw/client-tower` branch is merged first, extend
   `InboundMessage` with `to_addr: Option<Ipv6Addr>` instead).
3. **Enforce** at the top of `handle_bootstrap`:
   ```rust
   if let Some(dst) = to_addr {
       if !dst.is_multicast() && !cfg.unicast_bootstrap {
           warn!(%addr, device = %endpoint, activity = "inclusion",
                 "Rejecting bootstrap request sent to unicast address");
           return Ok(None); // no reply, like Python's BOOTSTRAP_NO_ACTION
       }
   }
   ```
   `to_addr == None` (option unsupported) → allow + one-time warning at
   startup, matching Python's fail-open.
4. **Config** (`src/config.rs`): `--unicast-bootstrap` bool flag, threaded to
   the server ctx (see plan 06 for the general config plumbing).
5. **Stale-session equivalent**: Rust has no session cache for bootstrap —
   the check is purely per-packet, so the Python session workaround is
   unnecessary. Confirm: every retransmitted `/bs` carries its own pktinfo,
   so per-packet checking is actually *more* correct. Document this
   simplification in SPECIFICATION.md.

## Testing

- Unit: pure predicate given `(to_addr, unicast_bootstrap_flag)`.
- Integration: hard to send real multicast in CI; instead:
  - bind TestGateway on `[::1]`, send `/bs` → destination `::1` is unicast →
    expect *no* response and a rejection log; with `--unicast-bootstrap` →
    normal bootstrap flow (existing tests keep passing by enabling the flag
    in `tests/common`).
  - Multicast-positive path: send to `ff02::1` on loopback
    (`IPV6_MULTICAST_LOOP`) — attempt it; if flaky in CI, cover the
    predicate + pktinfo parsing separately and rely on the unicast-negative
    test.
- Verify component tests / `run_tests.sh` set `--unicast-bootstrap` where
  they bootstrap over unicast.

## Risks / open questions

- **Breaking change for dev setups**: enabling enforcement by default will
  break any current workflow that bootstraps via unicast — release note +
  default flag in the systemd unit/Yocto recipe must be coordinated.
- `recvmsg` on the tokio `UdpSocket` needs care (readiness loop,
  `WouldBlock` retry); prototype early. Alternative: `socket2`'s
  `recv_from_with_flags` does **not** deliver cmsgs — a real
  `recvmsg` path is unavoidable.
- Decision recorded in PORTING_TODO §5: implement (recommended — it is the
  Python default and security-relevant) rather than document-as-deviation.
