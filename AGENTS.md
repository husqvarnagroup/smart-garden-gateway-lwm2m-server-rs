# AGENTS.md

## General

### Code

* Run "cargo fmt" before each commit
* Ensure that "cargo clippy" and "cargo test" passes before each commit
* Always add a body to the commit message with a short explanation of the change
* Always add co-authored information with used model to commit

## Essential commands

```sh
cargo build
cargo test                    # all unit + integration tests
cargo test --doc              # doc tests
cargo clippy --all-targets --all-features -- -D warnings   # CI-enforced; warnings are errors
cargo fmt --all               # formatter
cargo machete                 # check for unused dependencies (runs in PR CI)
cargo llvm-cov --all-features --workspace --codecov --output-path codecov.json  # coverage
```

Order when verifying before a commit: `fmt → clippy → test`.

Pre-commit hooks run `fmt` and `clippy` automatically on staged files (`.pre-commit-config.yaml`).

### Logging

* Log message rules
  * Start with capital letter
  * INFO, WARN and ERROR are read by a troubleshooter. Avoid too technical terms that require knowledge of the code.
  * DEBUG are only read by developers, can be technical / reference concepts from code (variable names, etc.)
* Include following structured fields if available / applicable
  * device: the sgtin of the device
  * activity: inclusion, exclusion, fota, state, value-report, registration, control, connection-status, or time-sync (matches `BnwActivity` in the Python `sg-bnw-lwm2m-server`)

## Project structure

Single crate — library plus server binary (`src/main.rs`), **no workspace**.

| Path | Purpose |
|---|---|
| `src/main.rs`, `src/config.rs` | Startup, CLI parsing (`clap`), task spawning, signal handling |
| `src/lwm2m/` | CoAP/LwM2M: inbound server (`/bs`, `/rd`, `/dp`), outbound dispatch (retransmission, block-wise), bootstrap, IPSO model |
| `src/ipc/` | Unix-socket JSON command server and event pub/sub |
| `src/registry.rs`, `src/housekeeping.rs`, `src/persistence.rs` | Device registry, periodic expiry/timeout/ping task, JSON state files |
| `src/logging/` | `tracing` layers: journal/syslog + console formatter |
| `tests/` | Integration tests: `coap_tests.rs`, `ipc_tests.rs`, `event_tests.rs` |

`SPECIFICATION.md` describes the current architecture, protocol behaviour, and full module inventory — consult it before changing server behaviour.

## Technical Requirements

Largely implemented — `SPECIFICATION.md` is authoritative for current behaviour.

- LwM2M 1.1 spec
- CoAP transport on UDP
- no security (NoSec) on transport (physical network layer is handling it)
- CoAP blockwise transfer (starting at SZX=5 → 512-byte blocks; devices may negotiate smaller)
- separate ACK with timeout 22s (`LWM2M_COAP_SEPARATE_TIMEOUT` in the Python/wakaama build)
- CBOR and SenML-CBOR (application/senml+cbor)
- LinkFormat parsing
- LwM2M server and bootstrap server
  - bootstrap (r/w/d)
    - Bootstrap request 
    - Write request 
    - Delete request 
    - Bootstrap finished request 
    - Bootstrap Device Initiated 
  - registration
    - Register
    - Register update
    - De-register
  - maintenance (r/w/x)
  - send/notify
  - no Observe
- client registry
- retransmission/retries (exponential backoff)
- deduplication (caching)
- CoAP max message size: 1024
- LwM2M object (IPSO) registry

## CI

- Builds and tests on Rust "1.75", "1.94", "stable", "nightly".
- Clippy, code formatting (rustfmt + pre-commit), typos, gitlint (on PRs)
- `cargo machete` in the dependencies workflow

## Specs

Follow these specs:

* Terminology for Constrained-Node Networks: RFC 7228
* LwM2M 1.1
* The Constrained Application Protocol (CoAP): RFC 7252
* Block-Wise Transfers in the Constrained Application Protocol (CoAP): RFC 7959
* CBOR: RFC 8949
* Sensor Measurement Lists (SenML): RFC 8428

## Agent definitions

Additional agent instruction files at the project root:

- `AGENT-DOC.md` — documentation agent guidance
- `AGENT-TEST.md` — test agent guidance
- `AGENT-REVIEW.md` — code review agent guidance (idiomatic Rust, simplicity, security, correctness)

