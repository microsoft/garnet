# Full-replication benchmark handoff for CEServer08

## Purpose, revisions, and decisions

Use branch `tedhar-replica-overlapped-writes-full` in an isolated checkout.
Its direct parent is `1a51ad05d75d36ab1ed02c5a16af6877d2568063`
(`tedhar-replica-overlapped-writes` at packaging time). Record the fetched full
commit with `git rev-parse HEAD`; do not identify builds only by Garnet's version
string. Only the full branch is pushed by this packaging task. No PR is created.
The coordinating session handles copying this document to CEServer08 via SMB;
this child has not connected to or run benchmarks on CEServer08.

Baseline already contains the independent sequential/concurrent replica write
policy, bounded sink, component V5 handshake, configuration, tests, and docs.
`repl-disk-based-sync-write-buffers-concurrently` defaults to **false** and is
independent of buffer count. Baseline also inadvertently contains the scenario
selector scaffolding: transfer-only is its default and full fails explicitly.
The user clarified that the selector belongs to this full follow-up. Baseline
has deliberately not been amended or reverted.

This full-only commit implements `FullOptions` and `FullReplication`: the runner
launches separate, disposable real primary/replica server processes, can select
each server build independently, and uses production checkpoint negotiation,
shipment, recovery, and AOF catch-up. It changes send/receive defaults to full;
`generate` remains transfer-only and explicit `--scenario transfer-only` retains
the original component harness. It adds selected full/phase tests and an external
server build dependency to the cluster test project.

**Production code is changed**, even when timing is off: the replication manager
checks an environment variable once at startup; the shipment driver receives an
optional recorder and has null checks at shipment/file boundaries; recovery and
INFO have optional hooks. With `GARNET_REPLICA_BENCH_PHASE_TIMING=1`, a bounded
last-successful recorder captures whole-shipment/recovery timestamp pairs and
actual padded bytes at file boundaries. It does not instrument chunks.
Timing is disabled by default; `--phase-timing false` explicitly sets the owned
server environment value to `0`. Enabled INFO REPLICATION fields are
`benchmark_checkpoint_transfer_ms`, `benchmark_checkpoint_bytes`, and
`benchmark_recovery_ms`.

The user chose to retain opt-in timing despite the uncertainty below.
**Production baseline versus full with timing OFF has not been measured.**
That is the first comparison to run here; instrumentation OFF/ON within a full
build cannot answer it. No unrelated performance tuning preceded the new
write-policy/full work.

If full is abandoned because of production-time impact, remove **both** this
full implementation/instrumentation and baseline's scenario distinction/
scaffolding, while retaining the independent write-policy and component changes.
Do not merely disable timing or revert the full commit and leave the selector.

## Fetch and build on the receiving machine

Adapt every path to this machine. The examples use short isolated checkout
paths `D:\gf` (full) and `D:\gb` (baseline); they are not claims about CEServer08's
disks or existing checkout. Start from an existing Garnet repository, choose
unused checkout paths, and do not modify a shared/main checkout. Do not copy
baseline/full binaries into one output directory.

```powershell
# From an existing Garnet repo. D:\gf and D:\gb must not already exist.
git fetch origin refs/heads/tedhar-replica-overlapped-writes-full:refs/remotes/origin/tedhar-replica-overlapped-writes-full
git worktree add --detach D:\gf origin/tedhar-replica-overlapped-writes-full
git worktree add --detach D:\gb 1a51ad05d75d36ab1ed02c5a16af6877d2568063

Set-Location D:\gf
git rev-parse HEAD
git rev-parse HEAD^
dotnet --info
dotnet build main\GarnetServer\GarnetServer.csproj -c Release -f net10.0
dotnet build benchmark\Replica-transfer.benchmark\Replica-transfer.benchmark.csproj -c Release -f net10.0
dotnet test test\cluster\Garnet.test.cluster\Garnet.test.cluster.csproj -c Release -f net10.0 --filter "FullyQualifiedName~ReplicaCheckpointWriteTests|FullyQualifiedName~ReplicaTransferBenchmarkTests" -l "console;verbosity=normal"

Set-Location D:\gb
git rev-parse HEAD
dotnet build main\GarnetServer\GarnetServer.csproj -c Release -f net10.0
dotnet test test\cluster\Garnet.test.cluster\Garnet.test.cluster.csproj -c Release -f net10.0 --filter "FullyQualifiedName~ReplicaCheckpointWriteTests|FullyQualifiedName~ReplicaTransferBenchmarkTests" -l "console;verbosity=normal"
```

Use the .NET 10 SDK, Release, no debugger. Check every exit code and compiler
output before proceeding; do not trust a stale `--no-build` assembly. The full
cluster test project builds its external GarnetServer dependency. The selected
full fixture includes real-server **4-MiB** smoke cases with two replica buffers,
128-KiB chunks, depth 2, sequential/concurrent writes, enabled phases, fresh
checkpoints, matching fingerprints, and exhaustive value verification.

For two hosts, prepare isolated baseline/full server builds on each host.
Run the **full runner from D:\gf for every condition**, including the baseline
server condition. Never run baseline's unimplemented full dispatcher. Keep
version-owned outputs and dependencies separate on both machines.

## Safety, storage, and timing meaning

Use an isolated trusted private test network only. No auth/TLS is provided by
the control channel or the disposable cluster servers. Do not target production
endpoints, wildcard/public addresses, or existing servers. Bind uses IP literals,
not DNS; same-host loopback is the simplest starting point. For two hosts,
resolve their actual private IPs and restrict firewall access to the peer.
Allow the chosen control and server/cluster connectivity only on that network;
investigate blocked connectivity rather than disabling the firewall.

Choose **existing parent directories** on the desired primary and replica
storage. Example `E:\bp`, `F:\br`, and CSV parent `D:\bc` below must already
exist. These drive names are placeholders; do not silently fall back to another
volume. The runner creates fresh GUID-owned `full-send-*` and `full-receive-*`
subdirectories each run and retains log/AOF data, checkpoints, `launch.json`,
`stdout.log`, and `stderr.log`, including partial failed runs. CSV uses
`FileMode.CreateNew`: choose a new name every time. No arbitrary cleanup is
performed. Never recursively delete shared folders to free space.

Use RandomAccess initially (the full default). `--device-type` accepts
RandomAccess, Native, or FileStream on each runner. Windows Native has a long
checkpoint-path rejection; use short paths if testing it. Do not mix backends
within a comparison unless backend choice is the deliberate experiment.

The fixed server settings include real cluster, AOF, storage tier, diskless sync
false, 64-MiB log memory, 1-MiB pages, 64-MiB segments, and a 16-MiB index.
Population alternates reproducible seeded strings/hashes. Logical size excludes
keys, headers, objects, serialization, AOF and checkpoint overhead. Budget
generously: at least tens of GiB of free space per paired 2-GiB experiment across
both destinations, plus all retained prior runs; monitor actual growth rather
than treating logical payload as a capacity estimate.

The 600-second deadline covers startup, control connection, population, SAVE,
sync and exhaustive validation, not just the measured interval. Start the
sender promptly after the receiver (waiting for connection consumes its
deadline). Increase both timeouts equally if needed; maximum is 3600 seconds.
Record timeout changes. Receiver `--runs` must equal distinct effective
chunk/depth pairs times sender `--repeats`; each run launches fresh servers.
Use idle, distinct ports; defaults below are control 7010, replica 7011, primary
7012. Do not stop other workloads to claim occupied ports.

Owned child readiness comes from that child's stdout banner, followed by PING.
The replica gets READONLY. Timing starts before `CLUSTER REPLICATE ... SYNC`
and ends at visibility of a post-attach marker on the replica. Population, SAVE,
and exhaustive validation are excluded. Marker visibility is **not fsync or
crash durability**. A verified row confirms completed recovery, every logical
value, and checkpoint files in the new replica directory.

Every run regenerates version-owned data/checkpoints; never share or preinstall
checkpoint files outside the actual production transfer protocol. Server
fingerprints cover managed/native binaries and dependency/runtime manifests.
Explicit unsupported settings fail; unavailable implicit defaults or older
phase fields are reported as unavailable, not guessed.

Phase clocks are same-process monotonic. Shipment, recovery, barrier and total
are non-additive; do not subtract full/component timings to infer phases.

## Runnable two-console commands

Run console 1 first, then console 2. This example runs one same-host full-OFF
2-GiB condition. Variables are local to each console: set them in **both**.
For two hosts, `$Bind` on each console is that host's private IP, and `$Replica`
on sender is receiver's private IP; retain the corresponding server build paths
local to each host. Keep source/destination storage placement unchanged.

Console 1, receiver:

```powershell
$Runner = 'D:\gf\benchmark\Replica-transfer.benchmark\bin\Release\net10.0\Replica-transfer.benchmark.dll'
$Server = 'D:\gf\main\GarnetServer\bin\Release\net10.0\GarnetServer.dll'
$ReplicaOutput = 'F:\br' # Existing desired-storage parent.
$Bind = '127.0.0.1'
$Timing = 'false'
dotnet $Runner receive --scenario full --server $Server --output $ReplicaOutput --bind $Bind --port 7010 --server-port 7011 --runs 1 --write-buffers 2 --write-buffers-concurrently true --phase-timing $Timing --device-type RandomAccess --timeout-seconds 600
if ($LASTEXITCODE -ne 0) { throw 'Receiver failed; retain and inspect run logs.' }
```

Console 2, sender:

```powershell
$Runner = 'D:\gf\benchmark\Replica-transfer.benchmark\bin\Release\net10.0\Replica-transfer.benchmark.dll'
$Server = 'D:\gf\main\GarnetServer\bin\Release\net10.0\GarnetServer.dll'
$PrimaryOutput = 'E:\bp' # Existing desired-storage parent.
$CsvParent = 'D:\bc' # Existing parent; each run creates a new CSV.
$Bind = '127.0.0.1'
$Replica = '127.0.0.1'
$Timing = 'false'
$Condition = 'full-off-pair1'
$Csv = Join-Path $CsvParent "$Condition-$([guid]::NewGuid().ToString('N')).csv"
dotnet $Runner send --scenario full --server $Server --output $PrimaryOutput --bind $Bind --host $Replica --port 7010 --server-port 7012 --data-size 2g --chunks 16m --depths 4 --repeats 1 --csv $Csv --phase-timing $Timing --device-type RandomAccess --timeout-seconds 600
if ($LASTEXITCODE -ne 0) { throw "Sender failed; inspect $Csv and retained logs." }
Get-Content $Csv
```

**Baseline condition:** change `$Server` to
`D:\gb\main\GarnetServer\bin\Release\net10.0\GarnetServer.dll` in **BOTH** consoles,
keep `$Runner` at the full build, keep timing false, and change `$Condition`
to a fresh baseline label. Baseline supports the explicit two-buffer concurrent
policy, but has no phase recorder; unavailable phases are expected.

**Full ON condition:** restore `$Server` to full in both consoles, set `$Timing`
to `'true'` in both, and change `$Condition` to a new full-on label.
The literal false/true values are passed explicitly through `--phase-timing`.

**Manual four-MiB smoke:** use the same commands with sender `--data-size 4m
--chunks 128k --depths 2`, receiver two buffers and one run. Keep timing false
for an OFF smoke or true on both nodes for a phase smoke. A new CSV and GUID
directories are required for each attempt. Require one `full,verified` row
(CSV fields are quoted), two server fingerprints and both successful exits.

## CEServer08 experiment order and interpretation

First perform at least **three rotated paired Release runs of baseline versus
full OFF**: baseline/full, full/baseline, baseline/full. This tests introduced
production overhead including disabled hooks. Both nodes must use baseline for
the baseline condition and full for the full condition.

Then, optionally, perform at least **three rotated paired full OFF/ON runs**:
off/on, on/off, off/on. Rotation means alternating execution order, **not**
randomization. Run each member as a separate invocation of the two-console
commands, changing both server paths/timing flags as required. Using one
receiver sweep does not implement rotation between conditions.

Recommended reference settings: 2-GiB logical data, 16-MiB chunks, depth 4,
two concurrent replica buffers, RandomAccess, timeout 600. Hold hardware,
source/destination placement, build configuration, device backend and workload
constant. Avoid unrelated disk/network activity; record cache policy, whether
any eviction is used, OS, CPU, SDK/runtime, filesystem, disks, network speed,
commit IDs, fingerprints, arguments, run order and each CSV/log directory.
Fresh files are not proof of cold caches; no cache eviction was used in the
previous controls.

For each pair calculate `(candidate_seconds / reference_seconds - 1) * 100`
using verified `sync_seconds`; report individual pairs, mean paired percent
change and a paired confidence interval, not just pooled means. A proposed
1% material-regression boundary is not established by a mean below 1%;
uncertainty must bound it. Collect more pairs if noisy, and report failures
rather than silently excluding incompatible/time-out conditions.

After those controls, sweep receiver buffer counts **1, 2, 4** and explicitly
set `--write-buffers-concurrently false` or `true` for each condition. Keep chunks
16m/depth4 initially. One buffer cannot give concurrent writes; use it as a
control, not evidence of parallelism. Two/four buffers with false overlap
receiving with sequential writes; true allows concurrent positional writes.
Neither buffer count nor full-mode default implies concurrency. Do not change
both policy and build/timing simultaneously when attributing overhead.
If sweeping chunks/depths later, match `--runs` to cases times repeats.

The earlier same-host component plateau around 414 MB/s was traced to shared
Azure host-storage bandwidth around 825 MB/s aggregate (also seen with two
read-only streams), not proof that parallel writes cannot help CEServer08.
Yesterday's component experiment automatically used parallel writes above one
buffer; reproduce it now with `--scenario transfer-only
--write-buffers-concurrently true`. That component replaces receiver parsing,
device creation, orchestration and replies and omits production recovery/AOF;
it cannot substitute for the production comparisons above.

## Prior evidence (not a performance promise)

Reported full OFF/ON controls on GCRAZCDL0014: Ubuntu Azure Standard_D16s_v3,
.NET SDK 10.0.403, OS-disk primary/temp-disk replica, RandomAccess, 2-GiB data,
16m/depth4/two concurrent buffers, fresh files, no cache eviction.
Columns below always mean OFF/ON even when order was reversed:

| Pair execution order | Full OFF seconds | Full ON seconds | ON shipment seconds | ON recovery seconds |
| --- | ---: | ---: | ---: | ---: |
| Off/on | 6.3139002 | 6.4473231 | 5.813030910 | 0.333126106 |
| On/off | 6.2884624 | 6.1521921 | 5.560364657 | 0.296146968 |
| Off/on | 6.0752737 | 6.2026190 | 5.550673895 | 0.347439704 |

All six exhaustive verifications passed with the same tested server fingerprint
`7836A72A78F1998440C4EA771A12FC5B079F21D3CDD09B9C98F34ECBF5074136`.
Mean paired change +0.68%; three-pair 95% Student-t interval -5.45% to +6.81%.
**A less-than-1% end-to-end regression bound was NOT established.** These compare
instrumentation within a full build, not baseline commit versus full build.
Isolated actual recorder calls plus caller timestamps cost 156-182 ns and
32 allocated bytes per shipment/recovery pair, excluding file accounting and
full-system effects.

Parent-reported correctness evidence: selected full Debug 77 passed; Release
76 passed/one DEBUG-only skipped; configuration eight passed in each;
real replica policy six passed in each. Independently built current/91a0ddd
mixed revisions verified both directions, phase OFF/ON (both Garnet 2.2.7,
**not a released-version compatibility matrix**). Pre-full baseline isolated
Release 60 passed/one DEBUG-only skipped and configuration eight passed.
Post-baseline full benchmark fixture Release 52 passed.
Packaging-child verification on Windows, 2026-11-06: the requested Release
write/benchmark selection above passed with a normal build: **76 passed,
one DEBUG-only skipped, zero failed** (77 total). Both four-MiB real-server smoke
cases passed. `dotnet format Garnet.slnx --verify-no-changes --no-restore --include`
with the nine changed C# paths passed; working/indexed `git diff --check` passed.
These are correctness/packaging checks, not new production performance runs.
