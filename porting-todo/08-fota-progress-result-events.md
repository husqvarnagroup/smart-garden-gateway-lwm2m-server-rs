# 08 — Long-running-operation progress/result events (FOTA)

Covers PORTING_TODO.md "Missing parts" §8 and task-list item **FOTA-1**.

## Goal

FOTA uploads must publish consumer-visible **result** events under a
service-entity operation path (and, in debug mode, per-block **progress**
events), with the operation id announced in the command response metadata —
so consumers can follow a long upload without holding the command socket
open.

## Python reference

`lwm2mserver/lwm2mserver.py::OperationHandler` (~lines 2650–2995):

- **Operation id**: random 10 chars `[a-z0-9]`, created per upload via
  `new_long_running_operation()`.
- **Command response metadata** (returned right after the *first* block is
  acknowledged): `operation_id`, `operation_progress_path =
  "operation/<id>/progress"`, `operation_result_path =
  "operation/<id>/result"`, plus the lwm2m metadata
  (`lwm2m_client_id`, `lwm2m_uri`, `lwm2m_response`, `lwm2m_response_code`).
  `success` = first block ACKed with `CONTINUE (2.31)`.
- **Progress events** — `_publish_operation_progress`: **only when the
  server runs with `--debug`** (`publish_progress` returns early
  otherwise!). Envelope: op `update`, entity
  `{service: "lwm2mserver", path: "operation/<id>/progress"}`, payload
  `{started: bool, progress: 0–100, finished: bool[, success: bool]}`
  (progress = `round(100 * written_blocks / expected_blocks)`), metadata =
  lwm2m metadata of the triggering ACK. Emitted per 2.31 Continue, and a
  final one with `success` + `finished: true`.
- **Result event** — always (not debug-gated):
  op `update`, entity `{service, path: "operation/<id>/result"}`,
  `success` flag, lwm2m metadata. Exactly once per operation.
- **Operation lock**: named lock `"fota"`-style via `operation_lock(name,
  release_manually=True, timeout=…)`; each Continue "retains" the lock, the
  final result releases it; a watchdog releases it if not retained within
  the timeout (device died mid-transfer). Second upload while locked →
  error response `"A firmware upload is already in progress"`.
- **Inter-block delay**: `LWM2MSERVER_FOTA_UPLOAD_DELAY` (default 0.1 s)
  sleep after each Continue, to leave airtime for other devices.

## Current Rust state

- `src/ipc/command.rs` FOTA path (~line 550): dispatches the block write,
  waits for the first Continue via `first_ack_tx` (30 s), then answers the
  command with `{"metadata": {"lwm2m_client_id": …, "operation_id": …}}` and
  spawns a task that waits (≤600 s) for the final result and calls
  `EventSender::send_fota_result(endpoint, op_id: u32, success)` — a
  **device**-entity event (`firmware_update/0/package` shape), not the
  Python `operation/<id>/result` service event.
- Per-block progress is invisible: `src/lwm2m/client.rs::dispatch_block_write`
  loops internally and only signals first-ack + final result.
- Operation ids are `u32` (`NEXT_OP_ID`), not the 10-char string.
- A FOTA lock exists (`lock_guard` in `command.rs` — verify its scope covers
  the whole transfer and rejects concurrent uploads; Python's retain/timeout
  semantics are richer).
- No inter-block delay.

## Design

**First: verify what consumers actually use** (PORTING_TODO explicitly says
so). Inspect the consumer of the event socket on the gateway (smart-gateway
service). Outcomes:
- (A) consumers only use the final result → port the result event + metadata
  paths, skip progress (debug-only anyway) or add it cheaply;
- (B) consumers parse progress → port both.
Plan below assumes (B)-lite: result event always, progress behind `--debug`
(exact Python parity).

1. **Progress hook out of the block-write loop**
   (`src/lwm2m/client.rs::dispatch_block_write`, or `BlockWriteService` once
   the tower branch lands): extend `PendingOperation` (or the FOTA-specific
   op) with an optional
   `progress_tx: Option<mpsc::UnboundedSender<BlockProgress>>` where
   `BlockProgress { written: usize, expected: usize }` — sent on every 2.31
   Continue. `expected` = `ceil(payload / block_size)` recomputed on SZX
   renegotiation.
2. **Operation id**: keep the internal `u32` op id, add a public 10-char
   alphanumeric id generated in the FOTA handler (`rand_core::OsRng` — no
   new dependency) used in event paths + metadata. Include **both** in the
   command response metadata (`operation_id` string like Python;
   `lwm2m_client_id` stays).
3. **Event emission** (`src/ipc/command.rs` FOTA path):
   - Response metadata gains `operation_id`, `operation_progress_path`,
     `operation_result_path`.
   - The spawned waiter task consumes `progress_rx`: in debug mode emit
     `update` events on `operation/<id>/progress` with
     `{started, progress, finished[, success]}` (values as Python; payload
     encoded via the existing event JSON conventions —
     `{"vb": …}`/`{"vi": …}` wrapping? **check**: Python sends these via
     `ProtocolPayload.from_py_dict`, so they are typed values — capture a
     real event to fix the wire shape).
   - On final result emit `operation/<id>/result` with `success` — via a new
     `EventSender::send_operation_result(op_id_str, success, metadata)`.
   - Keep `send_fota_result` (device-entity event) only if a consumer uses
     it; otherwise replace and note in SPECIFICATION.md.
4. **Lock semantics**: strengthen the existing FOTA lock to Python's
   retain/release model: the waiter task owns the guard, refreshes a
   deadline on every progress message, and force-releases (aborting the
   upload is unnecessary — just release the lock) when no block progress
   within the retain timeout. Concurrent upload attempt → immediate
   `success:false` with the Python error text.
5. **Inter-block delay**: in the block-write loop, after each Continue,
   `sleep(fota_upload_delay)` (config plan 06, default 0.1 s, env
   `LWM2MSERVER_FOTA_UPLOAD_DELAY`). This throttles airtime like Python.
6. **`--debug` flag** comes from plan 06.

## Testing

- Unit (client.rs block loop): progress messages per Continue with correct
  counts, incl. SZX renegotiation changing `expected`.
- Integration (fake device in `tests/`): 3-block upload →
  - command response metadata has `operation_id` + paths;
  - debug mode: ≥3 progress events ending `finished:true, success:true`,
    then exactly one result event;
  - non-debug: result event only;
  - failure mid-transfer (device answers 5.00 at block 2): progress with
    `success:false` + result `success:false`;
  - second upload during the first → rejected with the "already in
    progress" error.
- Lock watchdog: device stops ACKing → lock released after retain timeout,
  next upload proceeds.

## Risks / open questions

- **Wire format of payload values** in progress events (typed `{vb/vi}`
  wrappers vs bare JSON) — must be captured from Python before coding.
- Event-socket consumers may key on the current Rust `send_fota_result`
  shape (if the Rust port is already deployed anywhere) — coordinate.
- Depends on plan 06 (`--debug`, delay env); interacts with plan 01 (FOTA
  wakeup duration) but is independent code-wise.
