# 06 — Configuration parity (CLI + env overrides)

Covers PORTING_TODO.md "Missing parts" §6 and task-list item **CFG-1**.
Several other plans (01, 03, 05, 07) hang new options off this one — do it
early.

## Goal

Expose the operational knobs the Python server has, so deployments and tests
don't need code changes: storage/socket paths, CA files, auto-approval,
state-loading, and timeout overrides.

## Python reference (defaults in parentheses)

CLI (`lwm2mserver.py` argparse, ~line 3315):

| Python option | Meaning | Rust today (`src/config.rs`) |
|---|---|---|
| `interface` positional (`lowpan0`) | network interface | `interface` positional ✓ (no default) |
| `path` positional (`/tmp`) | eventbus socket **directory** | hardcoded `/tmp/lwm2mserver-{command,event}.ipc` |
| `--allow-all` (off) | auto-approve every bootstrap | missing |
| `-a/--allow <sgtin>` (repeatable) | pre-approve specific devices; Python wraps non-empty allow-list in a plain set, empty list + not allow-all → `ExpiringSet(10000*60)` for runtime approvals | missing |
| `--unicast-bootstrap` (off) | see plan 05 | missing |
| `-v/--verbose`, `--log-level <mod>=<LVL>` | log tuning | `RUST_LOG`-style env filter exists; map or document |
| `-d/--debug` (off) | debug mode (gates plan 08 progress events + plan 09 endpoints) | missing |
| `--ipso-directories` | ✓ ported | ✓ |
| `--bind-to-device` | ✓ ported | ✓ |
| `--server-uri` | ✓ ported | ✓ |
| `-p/--port` | ✓ ported | ✓ (default 20017 vs Python auto) |
| `-ldc` | see plan 04 | missing (decision pending) |
| `--lb-key-file` | ✓ ported | ✓ |
| `--no-load-state` (off) | skip loading persisted device state | missing |
| `--state-storage <dir>` (cwd in Python; `/var/lib/lwm2mserver` in deployment) | persistence dir | hardcoded `/var/lib/lwm2mserver` in `main.rs` |
| `--device-ca-file <pem>` (repeatable) | device CA certs | two CAs hardcoded in `src/lwm2m/bootstrap.rs` |
| `--lemonbeatd-runtime-directory` | radiomodule socket dir (plan 01) | missing |

Env overrides (`lwm2mserver.py` ~lines 125–228):

| Variable | Default | Used by |
|---|---|---|
| `LWM2MSERVER_INCLUDABLE_DEVICE_EXPIRY_DURATION_DEFAULT` | 30 s | plan 07 |
| `LWM2MSERVER_BOOTSTRAP_INACTIVITY_TIMEOUT` | 10.0 s | bootstrap.rs `expire_stale` (Rust: `BOOTSTRAP_TIMEOUT_SECS = 30` in housekeeping — **note the differing value**) |
| `LWM2MSERVER_BOOTSTRAP_READ_SECURITY_MAX_DELAY` | 3.0 s | server.rs bootstrap GET /0/0 delay (Rust: 0.5–3 s random) |
| `LWM2MSERVER_BOOTSTRAP_RETRY_RANDOM_WAIT` | 1.0 s | bootstrap retransmit jitter |
| `LWM2MSERVER_FOTA_UPLOAD_DELAY` | 0.1 s | inter-block FOTA delay (plan 08; Rust has none) |
| `LWM2MSERVER_LWM2M_REGISTRATION_LIFETIME` | 86400 s | default `lt` (server.rs `unwrap_or(86400)`) |
| `LWM2MSERVER_DEVICE_CONNECTIVITY_ONLINE_TIMEOUT` | 9000 s | housekeeping `ONLINE_PING_INTERVAL` |
| `LWM2MSERVER_DEVICE_CONNECTIVITY_OFFLINE_TIMEOUT` | 900 s | housekeeping `OFFLINE_PING_INTERVAL` |
| `LWM2MSERVER_DEVICE_CONNECTIVITY_INITIAL_TIMEOUT` | 60 s | initial connectivity check (no direct Rust equivalent — housekeeping tick) |
| `LWM2MSERVER_WAKAAMA_DEFAULT_STEP_TIMEOUT` | 60 s | wakaama-specific; N/A in Rust — document as dropped |
| `LWM2MSERVER_WAKEUP_TIMEOUT_MS` | 4000 | plan 01 |
| `LWM2MSERVER_WAKEUP_TOTAL_ATTEMPTS` | 3 | plan 01 |
| `LWM2MSERVER_WAKEUP_SLEEP_BEFORE_RETRY_ATTEMPT_MS` | 2000 | plan 01 |
| `LWM2MSERVER_WAKEUP_FOTA_TIMEOUT_MS` | 1 800 000 | plan 01 |
| `LEMONBEATD_RUNTIME_DIRECTORY` | unset | plan 01 (CLI arg takes precedence) |

## Design

1. **Extend `Cli` in `src/config.rs`** with the missing flags. Use clap's
   `env = "…"` attribute for options that have env counterparts (clap `env`
   feature — add to Cargo.toml features of the already-pinned clap 4.5).
   Keep Python's names/abbreviations verbatim so wrapper scripts keep
   working (`--allow-all`, `-a/--allow`, `--no-load-state`,
   `--state-storage`, `--device-ca-file`, `--unicast-bootstrap`, `-d`,
   `--lemonbeatd-runtime-directory`, positional `path`).
2. **Group timeouts** into a `Timeouts` struct on `Config`, each field read
   from its `LWM2MSERVER_*` env var with the Python default. One helper:
   `fn env_or<T: FromStr>(name: &str, default: T) -> T` (warn on parse
   failure, keep default). Replace the hardcoded constants at their use
   sites (`housekeeping.rs`, `server.rs`, `bootstrap.rs`, later
   `wakeup.rs`) by values passed in at construction — avoid global state;
   thread through the existing constructor/`run()` signatures.
3. **CA files** (`--device-ca-file`, repeatable): `bootstrap.rs` currently
   embeds two PEM CAs. Change `validate_device_certificate` to take a
   `&[CaCert]` loaded at startup; default (no flag) = the two embedded CAs
   (deployment parity: the Yocto unit passes explicit files — check before
   changing the default).
4. **State storage / no-load-state**: `PersistenceStore::new` already takes
   a dir — plumb `--state-storage`; `--no-load-state` short-circuits
   `load_registry`/`load_all_device_states`/`load_included` in `main.rs`
   (still *saving* — Python semantics: skip loading only).
5. **Socket paths**: positional `path` = directory; command socket
   `<path>/lwm2mserver-command.ipc`, event `<path>/lwm2mserver-event.ipc`
   (verify exact Python filenames in `event.py`/`processes.py` before
   freezing — the Rust defaults were copied from the deployed system).
6. **`--allow-all` / `--allow`**: in `BootstrapRegistry`, short-circuit
   `is_approved(endpoint)` when allow-all, or when the endpoint is in the
   allow list. Approvals via `--allow` should not be consumed
   (`consume_approval` must not remove them). Runtime approvals keep the
   existing includable/approve flow (expiry: plan 07).
7. **`--debug`**: store on `Config`; consumed by plans 08 and 09.

## Testing

- `Config::from_args`-level unit tests via `Cli::parse_from([...])` — clap
  makes this easy; cover env fallbacks with temp env vars (serialize tests
  that touch env, or use a `figment`-free manual injection point).
- Integration: TestGateway gains a builder to set state dir + socket dir
  (removes today's hardcoded temp-path assumptions) and `--allow-all` to
  simplify inclusion tests.

## Risks / open questions

- Value drift: where the Rust constant deviates from the Python default
  (bootstrap inactivity 30 vs 10 s, GET /0/0 delay), adopting the env
  default silently changes behavior — list each in the PR and update
  SPECIFICATION.md.
- clap `env` feature adds a little binary size; acceptable, but verify the
  Yocto build.
- Keep `Config` the single source: no `std::env::var` calls sprinkled in
  modules (unlike Python).
