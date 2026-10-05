# Design: `quitsh server` — change-tracking watcher

Date: 2026-09-07 Status: Accepted (design), not yet implemented

## Problem

`quitsh` has target-level change detection (`pkg/dag`), but it is CI-shaped: a
caller passes a list of changed paths via `dag.WithInputChanges(paths)`, which
`SolveInputChanges` matches against each component's `inputs` regexes and
propagates through the target DAG. The path list comes from git
(`pkg/ci/pipeline/changes.go`).

That model does not serve local development. There is no notion of "what did I
already build successfully", so every invocation re-runs every target in the
selected stage, and no command in this repository even passes
`WithInputChanges`.

## Goal

A long-running local server that answers, per target, _"do this target's inputs
differ from the state at its last successful build?"_, so that
`quitsh build`/`test`/`lint` can skip targets that are already up to date.

Non-goals for v1: output-set tracking, a streaming watch mode, seeding state
from git history, any remote or multi-user operation.

## Overview

```
                  scan loop (periodic + on demand)
                        |
                        v
  repo files ---> Snapshot (path -> stamp) ---> digest per input set
                                                     |
  .component.yaml -> targets -> input sets ----------+
                                                     v
                                       last_successful_build per target
                                                     |
   quitsh client  <---- gRPC (unix socket) ----------+
        |
        +-- seeds node.Inputs.Changed, propagates forward in pkg/dag,
            marks unchanged nodes ExecStatusSkipped, runs, then reports
```

The server is a **pure per-target dirty oracle**. It knows nothing about target
dependencies. All graph semantics — dependency propagation, selection,
scheduling — stay in `pkg/dag` on the client, where they already live.

The server is an **optional accelerator**. Any failure (not running, dial
timeout, version mismatch, unreadable file) degrades to "assume dirty", i.e.
today's behaviour of running everything. The cache may never cause a skip it is
not sure about.

## Change-tracking model

### State

| State                   | Written by              | Content                                                        |
| ----------------------- | ----------------------- | -------------------------------------------------------------- |
| Current digests         | scan loop               | per input set: current `digest` + change-point history         |
| `last_successful_build` | `ReportResult(SUCCESS)` | per target: `map[input.ID]Digest`, `ScanID`, `At`              |
| `last_run`              | `ReportResult`          | per target: `SUCCESS`/`FAILED`, `ScanID`, `At` — informational |

`dirty` is **never stored**. It is derived per query:

> A target is dirty if it has no `last_successful_build`, or if any of its input
> sets' current digest differs from the digest recorded in that
> `last_successful_build`.

Because scans write only current digests and reports write only
`last_successful_build`, the two can never race to overwrite a status flag.

### Digests

After each scan every file is assigned to the input sets whose include/exclude
regexes match it — the same `input.Config` matching `pkg/dag` uses, behind
`recache`. An input set's digest is computed over its sorted `(relpath, stamp)`
pairs, where `stamp` is either `mtime+size` (default) or a content checksum,
selected by `Args.HashMode`.

Digests, not change counters: an edit followed by a revert returns the digest to
its previous value and the target reads clean again, with no rebuild.

### Change points and `scan_id`

Each `GetStatus` response carries the `scan_id` of the snapshot it was computed
from. `ReportResult` echoes it back, and the server records the digests **as of
that scan** — the state the build actually consumed — not the current ones.

Scenario this exists for:

```
scan 5   input set S digest = D1
query    A dirty. response scan_id = 5
         build of A starts, compiling the tree as of D1
scan 7   a file is saved -> S digest = D2   (change point: S -> D2 at scan 7)
         build of A finishes OK
report   {target: A, status: SUCCESS, scan_id: 5}
         => last_successful_build[A] = { S: digestAt(S, 5) } = { S: D1 }
next query
         current D2 != recorded D1  ->  A is dirty
```

A stays dirty, correctly: the successful build built D1, and D1 is no longer on
disk. Recording the _current_ state instead would silently swallow that edit
until the file was touched again.

To answer `digestAt(S, scan 5)` the server keeps **change points** per input set
— `S: [(3,D0), (5,D1), (7,D2)]` — and `digestAt(S, N)` is the last entry with
`scan_id <= N`. Input sets change rarely, so this is a handful of entries each.
Fixed-N generations were rejected: at a 2 s scan interval, N=16 covers 32 s and
a five-minute build would find its `scan_id` evicted. History is pruned by the
oldest `scan_id` still outstanding — pinned when `GetStatus` hands one out,
released on report, with a TTL for clients that crash.

If a reported `scan_id` has been pruned, the server records no
`last_successful_build` for that target and names it in `not_recorded`; the
client ignores this and the target stays dirty.

`FAILED` writes only `last_run`; no `last_successful_build` is touched, so the
target stays dirty. This gives `quitsh server status` an honest reason: _never
built_ / _last build failed at 14:32_ / _inputs changed since last build_.

Only two other writers exist, and both only delete: component re-discovery drops
entries for target IDs that no longer exist, and `quitsh server reset [target…]`
clears them. Nothing ever fabricates a `last_successful_build`.

### Detection: periodic rescan, not fsnotify

The server rescans the tree periodically (`Args.ScanInterval`) with `fastwalk`
(already a dependency), applying excludes and re-hashing only files whose
mtime/size moved. Queries are served concurrently from the current snapshot.

fsnotify was rejected: it has no recursive watch, so every directory must be
registered individually, which hits `fs.inotify.max_user_watches` on mono-repos,
and asynchronous event delivery introduces a settling problem (query answered
before the watcher processed your save). A rescan cannot desync.

### Freshness

`GetStatusRequest.max_age_ms` states how fresh the answer must be. `0` means
"newer than this request": the server triggers a scan, or joins one already in
flight, and answers when it completes. Skip decisions use `0`, because a stale
answer means a silently skipped build. `quitsh server status --stale-ok` passes
a larger value for an instant answer. The periodic loop exists to keep the mtime
cache warm so the fresh path is nearly free.

## gRPC API (`quitsh.watcher.v1`)

```proto
service Watcher {
  rpc GetStatus    (GetStatusRequest)    returns (GetStatusResponse);
  rpc ReportResult (ReportResultRequest) returns (ReportResultResponse);
  rpc Rescan       (RescanRequest)       returns (RescanResponse);
  rpc Reset        (ResetRequest)        returns (ResetResponse);
  rpc Info         (InfoRequest)         returns (InfoResponse);
  rpc Shutdown     (ShutdownRequest)     returns (ShutdownResponse);
}

message GetStatusRequest {
  repeated string target_ids = 1;   // empty = all known targets
  int64  max_age_ms          = 2;   // 0 = must be newer than this request
}

message GetStatusResponse {
  uuid  scan_id             = 1;
  int64  scanned_at_unix_ms  = 2;
  map<target_id, TargetStatus> statuses = 3;
}

message TargetStatus {
  string target_id             = 1;
  bool   dirty                 = 2;   // own inputs only; no dependency propagation
  bool   has_successful_build  = 3;
  repeated string dirty_inputs = 4;   // input set ids that differ
  LastRun last_run             = 5;   // informational
}

enum Status { STATUS_UNSPECIFIED = 0; STATUS_SUCCESS = 1; STATUS_FAILED = 2; }

message LastRun { Status status = 1; int64 scan_id = 2; int64 at_unix_ms = 3; }

message TargetResult { string target_id = 1; Status status = 2; }

message ReportResultRequest  { int64 scan_id = 1; repeated map<string, TargetResult> results = 2; }
message ReportResultResponse { repeated string not_recorded = 1; }
```

`dirty` is deliberately own-inputs-only. There is no `propagate` flag and no
`dirty_by_dependency` field: dependency propagation is the client's job.

`Info` returns server version, repo root, last scan time and duration, and
target/file counts. It backs `quitsh server status`, and the client calls it
once per process right after dialling; an incompatible version logs a warning
and degrades to "everything dirty". It is not called per `GetStatus`.

Transport: `Args.Address` in gRPC target syntax, defaulting to
`<root>/.quitsh/watcher.sock`. Socket mode `0600`. `tcp://127.0.0.1:<port>` is
opt-in for devcontainer setups;

Generated code is checked in so `go build` works without `buf`/`protoc`
installed; a `justfile` recipe regenerates it.

## Package layout

- **`pkg/watcher`** — domain core, no gRPC and no cobra. `Args`, `Hasher`
  (`mtime+size` / checksum), `Snapshot`, `Digest`, `ScanID`,
  `LastSuccessfulBuild`, `LastRun`, and `Tracker`: the state machine owning
  digests, change points, `last_successful_build`, and the dirty computation.
  Pure and synchronous; unit-testable with no I/O timing.
- **`pkg/watcher/scan`** — `fastwalk` walk + excludes → `Snapshot`, re-hashing
  only files whose mtime/size moved. Knows nothing about targets.
- **`pkg/watcher/proto`** — generated `quitsh.watcher.v1` code.
- **`pkg/watcher/server`** — owns the `Tracker`, runs the scan loop, serves
  gRPC, handles socket lifecycle and persistence. Sole writer of tracker state;
  one mutex plus a join point for the in-flight scan.
- **`pkg/watcher/client`** — dials the socket, `GetStatus`/`ReportResult`. Never
  fails hard: an unreachable server yields "all dirty".
- **`pkg/cli/cmd/watcher`** — `quitsh server serve | status | stop | reset`.
- **`pkg/dag`** — seeding from watcher status, `ExecStatusSkipped`, reporting.

The server needs each target's resolved input sets (including the default
component-wide input when `inputs` is omitted, and `self::x` resolution). That
resolution is factored out of `pkg/dag` into a helper both sides call, so the
logic exists once, use `constructNodes` and make it public shoudl already serve
most needs.

## Integration with `pkg/dag`

`DefineExecutionOrder` gains one alternative seeding source next to the existing
one. Today:

```go
resolveInputs := o.inputPathChanges != nil
...
g.SolveInputChanges(allInputs, allComps, &regexCache, o.inputPathChanges)
```

With `dag.WithWatcher(args)` the graph is instead seeded by
`g.SolveWatcherChanges(status)`, which sets `n.Inputs.Changed` from the server's
per-target answer and then reuses the **identical** forward `Propagate` over
`Forward` edges. `WithInputChanges` and `SolveInputChanges` are untouched; the
two seeding sources are mutually exclusive and feed one propagation
implementation. This is what lets the server stay graph-free.

Target _selection_ is not narrowed. `quitsh build` already selects every
build-stage target, so dependents are in the subgraph already; narrowing the
selection would be wrong, because `recomputeSubgraph` expands a selection
backward (dependencies) and never forward (dependents).

Instead, after propagation, nodes where `!Inputs.IsChanged()` are marked
`ExecStatusSkipped` — a new value in `pkg/dag/status.go`. The executors
(`run.go`, `run-concurrent.go`) skip a skipped node's runners, and
`PropagateExecStatus`/`Status()` treat skipped as success so that dirty
dependents still run. This is required: the existing `Execution.Cancel` path
leaves runners at `ExecStatusNotRun`, which makes `Status()` non-success and
cascades cancellation to everything downstream.

Reporting lives at the end of `dag.Execute`, which sends one batched
`ReportResult` carrying the `scan_id` from the `GetStatus` that seeded the graph
and each executed target's terminal status. Skipped targets report nothing.
Putting it in `Execute` rather than in each command means downstream
repositories' custom commands get it automatically.

## Configuration and CLI wiring

```go
package watcher

type Args struct {
    Enabled      bool          `yaml:"enabled"`
    Address      string        `yaml:"address"`      // gRPC target
    StateFile    string        `yaml:"stateFile"`    // default <root>/.quitsh/watcher-state.json
    ScanInterval time.Duration `yaml:"scanInterval"` // default 2s
    MaxAge       time.Duration `yaml:"maxAge"`       // query freshness, default 0 = always fresh
    HashMode     HashMode      `yaml:"hashMode"`     // mtime-size (default) | checksum
    Excludes     []string      `yaml:"excludes"`     // default: .git, result, node_modules, target
    Timeout      time.Duration `yaml:"timeout"`      // client dial+call budget, default 2s
}
```

Users embed it in their own config (`Watcher watcher.Args`) and wire it with
`cli.WithWatcher(func(c config.IConfig) *watcher.Args { … })`, mirroring
`WithToolchainDispatcherNix`'s accessor form — necessary because the config is
unmarshalled by `rootCmd` after `cli.New`.

`exec-target`/`exec-stage` gain `--skip-unchanged`, which enables the watcher
for that invocation.

## Persistence and lifecycle

State for all digests input sets address written atomicall to `Args.StateFile`
on shutdown and every 30 s. And the state should serialized in on startup
quickly.

On startup the server performs a **full scan** and compares against the loaded
state, so anything changed while it was down is correctly dirty.

Socket lifecycle: on start the server dials its own address. Connection refused
means a stale socket file — unlink and bind. A successful dial means another
server owns this repo — exit with an error. `SIGINT`/`SIGTERM` flushes state and
unlinks the socket. `quitsh server stop` calls `Shutdown`.

Component discovery: `cl.FindComponents` at startup; `.component.yaml` files are
always tracked regardless of input patterns, and when one changes the server
re-discovers components, rebuilds input matchers, drops entries for targets that
disappeared, and keeps the rest keyed by target ID.

## Failure modes

Every failure degrades toward running more, never toward skipping:

| Failure                             | Behaviour                                        |
| ----------------------------------- | ------------------------------------------------ |
| Server not running / dial timeout   | Log at debug, all targets dirty                  |
| Version mismatch (`Info`)           | Log at warn, all targets dirty                   |
| Pruned `scan_id` on report          | No `last_successful_build` recorded, stays dirty |
| File unreadable or deleted mid-scan | Treated as changed, logged at debug              |
| Scan in progress at query time      | Query joins it and waits                         |

Symlinks are not followed; the link itself is stamped.

## Testing

Following the repository's `test_small` / `test_all` build tags:

- `pkg/watcher`: table tests for `Tracker` — dirty → report → clean → edit →
  dirty; digest stability across scans; both hash modes; `digestAt` over change
  points; the edit-during-build case staying dirty; pruned `scan_id` handling.
- `pkg/watcher/scan`: walk and excludes against the `test/repo` fixture.
- `pkg/dag`: `SolveWatcherChanges` seeding plus forward propagation, and
  `ExecStatusSkipped` propagation semantics (skipped counts as success for
  dependents, does not cascade) — driven without a server.
- Integration (`test/`): a real server on a unix socket in a temp directory —
  query, report, restart-and-reload, and server-absent degradation.

## Future work

- Output-set tracking, so a target is also dirty when its declared outputs are
  modified or removed.
- Streaming `Watch` RPC and a `quitsh watch` mode that re-runs on save. The gRPC
  choice was made partly to keep this cheap.
- Seeding `last_successful_build` from `git HEAD~1..HEAD` at server start.
- Possible removal of `WithInputChanges`/`SolveInputChanges` once the watcher
  covers the CI use case too.
