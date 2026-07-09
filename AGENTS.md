# AGENTS.md

## Related repositories

These repositories are not always in the checkout. Do not assume that you can read them locally.

| Path | Purpose |
| --- | --- |
| `../sg-bnw-lwm2m-server` | Original project (Python, C, Cython). This project rewrites it. |
| `../wakaama-pyx-analysis-workspace` | Spec of Wakaama, made from an analysis of `sg-bnw-lwm2m-server`. Wakaama is part of the rewrite. |
| `../sg-bnw-lwm2m-component-tests` | Black-box tests. They run against the original and this project. The results must match. |

### Original project

Match the behaviour of `../sg-bnw-lwm2m-server`, not its code. Do not translate the Python/C/Cython code line by line. Write idiomatic Rust.

If the original deviates from a spec (CoAP, LwM2M, and others), match the original. For exceptions, see [Specs](#specs).

| Path in `../sg-bnw-lwm2m-server/lwm2mserver/` | Purpose |
| --- | --- |
| `.` | Main application code (Python) |
| `wakaama/src/wakaama-c/` | Wakaama LwM2M library (C) |
| `wakaama/src/` (without `wakaama-c/`) | Cython wrapper code (`.pyx`, `.pxd`) |

## Commands

```sh
cargo build
cargo test                    # unit, integration, and doc tests
cargo test --doc              # doc tests only
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo machete                 # unused dependencies
cargo llvm-cov --all-features --workspace --codecov --output-path codecov.json  # coverage
```

Before each commit, run `fmt`, then `clippy`, then `test`. All must pass.

If pre-commit is installed, it runs `fmt` and `clippy` on the full crate when you commit Rust files (`.pre-commit-config.yaml`).

## Project structure

Single crate: library plus server binary. No workspace. `SPECIFICATION.md` has the full module inventory, the architecture, and the protocol behaviour. Read it before you change server behaviour.

## Specs

If two sources disagree, follow the source that is higher in this list:

1. "Deviations from Python Code" in `SPECIFICATION.md` (intentional deviations from the original)
2. The [original project](#original-project)
3. The [Wakaama analysis](#related-repositories)
4. The rest of `SPECIFICATION.md` and the specs below

Specs:

- LwM2M 1.1
- RFC 7228: Terminology for Constrained-Node Networks
- RFC 7252: CoAP
- RFC 7959: CoAP Block-Wise Transfers
- RFC 8949: CBOR
- RFC 8428: SenML

## Technical requirements

Most of these are implemented.

- CoAP on UDP. No transport security (NoSec). The physical network layer gives security.
- Block-wise transfer, starting at SZX=5 (512-byte blocks). Devices can negotiate smaller blocks.
- Separate ACK with a timeout of 22 s (`LWM2M_COAP_SEPARATE_TIMEOUT` in the original)
- CoAP max message size: 1024 bytes
- Retransmission with exponential backoff
- Deduplication (caching)
- CBOR, SenML-CBOR (`application/senml+cbor`), and LinkFormat
- LwM2M server and bootstrap server:
  - Bootstrap (r/w/d): Bootstrap Request, Write, Delete, Bootstrap Finish, Client Initiated Bootstrap
  - Registration: Register, Update, De-register
  - Device Management (r/w/x)
  - Send/Notify. No Observe.
- Client registry
- LwM2M object (IPSO) registry

## Code

- Code must build with Rust 1.75 (MSRV, see `Cargo.toml`). CI also builds with `1.94`, `stable`, and `nightly`.
- CI runs clippy, rustfmt, pre-commit, `typos`, `gitlint` (on PRs), and `cargo machete` (on PRs).

### Logging

- Start each message with a capital letter.
- INFO, WARN, and ERROR messages are for a troubleshooter. Do not use terms that need knowledge of the code.
- DEBUG messages are for developers. They can use code terms, for example variable names.
- If applicable, add these structured fields:
  - `device`: the SGTIN of the device
  - `activity`: `inclusion`, `exclusion`, `fota`, `state`, `value-report`, `registration`, `control`, `connection-status`, or `time-sync` (same as `BnwActivity` in the original)

### Tests

- Write unit tests in `src/` for small, isolated functionality. Write integration tests in `tests/` for larger scenarios.
- Keep tests short and specific.
- Use the Arrange-Act-Assert pattern, unless a different structure is clearer.
- Use one assertion per test. If closely related assertions make the test clearer, you can use more than one.

### Documentation

- Write doc comments only where they add value. Do not document self-explanatory code.
- Doc tests are encouraged.
- Keep docs short and specific.

## Review

- Always do reviews with a separate agent in a new session.
- Review commits, a commit range, or a diff. Look at the surrounding code, not only the changed lines.
- Report findings only. Do not change code and do not make commits.
- Before you report a defect, make sure that it is real: read the surrounding code and run the tests. Do not report speculation as fact.
- Write short findings, ordered by severity. Give each a file/line reference and a short reason.
- Put each finding in one class: defect (must fix), risk (recommended fix), or suggestion (nice to have).
- New behaviour without test coverage is a finding.

### Review focus

- Correctness:
  - Logic errors and off-by-one errors
  - Wrong error handling, including unhandled `None`/`Err` paths
  - Race conditions in async/tokio code
  - Protocol violations against the [Specs](#specs)
- Security:
  - Unvalidated input from the network (CoAP payloads, LinkFormat, CBOR/SenML) or the IPC socket
  - Integer overflow or truncation
  - Panics reachable from remote input (`unwrap`, `expect`, indexing on untrusted data)
  - Resource exhaustion from unbounded buffers or missing size limits
  - Path handling in persistence
- Idiomatic Rust:
  - Iterators, pattern matching, `?` propagation, and borrowing instead of cloning
  - Correct use of `Option`/`Result` combinators
  - No needless `unsafe`
  - No clippy warnings
- Simplicity:
  - Small functions and clear names
  - No needless abstraction or duplication
  - Comments only where the code cannot speak for itself
  - Log messages that follow the [logging rules](#logging)
