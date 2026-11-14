# Migration experiment

Compare fresh-key SET insertion with `MIGRATE SLOTS` throughput on two primaries.
All experiment-specific resources live here; shared runners and server profiles
remain in their existing locations.

| Resource | Purpose |
|----------|---------|
| `run.ps1` | Creates the cluster, loads each dataset, runs MigrateBench, validates results, and writes measurements |
| `cluster.json` | vm0 owns all 16,384 slots; vm1 is a slotless primary; no replicas |
| `benchmark.json` | Runs standalone Resp.benchmark on client vm2, targeting vm0:7000 |
| `preload.conf` | Native benchmark profile, defaulting to 256-byte values; the runner generates per-size/worker copies |
| `throughput.ps1` | Linux-client coordinator for the faster pipelining/buffer/worker matrix |
| `sample-nic.ps1` | Route-selected 100 ms directional NIC counters |
| `foreground.ps1` | Shared foreground pass orchestration and phase-aware evidence summaries |
| `validation.ps1` | Foreground validation policy and per-transfer cardinality evidence checks |

Both servers use the shared
[`garnet-cluster-diskless.conf`](../../02-platform/tools/config/garnet/garnet-cluster-diskless.conf).
This measures memory-only migration, not NVMe throughput. Its 1 GiB hash index
accommodates the small-value dataset.

## Scope and server compatibility

These are opt-in experimental workflows, not the default ClusterBench workload.
`run.ps1` targets the older server contract with `MigrateBatchSize` and
`FastMigrate`. For servers using native `MigrationDeleteBatchSize` and
`UseScanAheadMigration`, use `throughput.ps1` with an explicit positive
`-DeleteBatchSize` and `-ScanAhead` value. Its default parameters still select
the older contract. Neither runner adds these capabilities to the deployed server.
The benchmark-only scan-ahead toggle and wire counters described below require
separately instrumented builds.

## Faster pipelining comparison

Run `throughput.ps1` on the Linux client VM, with shared SSH access to the two
primaries. Copy this script, `sample-nic.ps1`, and the shared
`garnet-cluster-diskless.conf` into the same remote directory. Deploy the same
pipeline-capable Garnet build to both primaries and matching MigrateBench to
the client first. MigrateBench must emit `MIGRATION_UTC <start:o> <end:o>` around
its timed `SLOTS` operation; this client-only instrumentation aligns NIC samples
without relying on coarse console timestamps.

```powershell
./throughput.ps1 -SourceAddress 10.5.128.4 -TargetAddress 10.5.128.5 `
    -ServerDirectory /home/guser/pipeline-server-dfb6ad8ed `
    -MigrateBenchPath /home/guser/pipeline-client-dfb6ad8ed/MigrateBench `
    -ServerRevision dfb6ad8ed
```

The default 54-cell matrix tests 8/256/1024-byte values, 128/256/512 KiB send
buffers, 1/8/64 workers, and pipeline depths 1 (off) and 8 (on).
`FastMigrate=false` in both modes. The collection limit remains the unmodified
server default, 262,143 keys; this workflow does not require the older
experimental `MigrateBatchSize` option.

For focused runs, `-ValueLengths 1024,4096` also supports 4-KiB values.
`-CollectionBatchLimit` records and verifies a separately deployed server's
collection limit; it does not change that limit in the server. Non-default
limits require a matching `MigrationCollectionBatchLimit:<keys>` startup log
entry on both primaries. Keep the byte send-buffer size and pipeline depth
separate from the number of collected keys.

Network-saturation runs can select 16 workers, 16-KiB values, a 1-MiB send
buffer and `-LogPageBytes 16777216` for the default 16-MiB log pages.
Matched comparisons also support 32 workers, 2-KiB values and pipeline depth 2.
`-ScanAhead $false` or `$true` requires a benchmark-instrumented server honoring
`GARNET_BENCHMARK_SCAN_AHEAD` and logging `MigrationScanAhead:Disabled` or
`MigrationScanAhead:Enabled` on both primaries. This is not a production server
configuration option. Both modes use the same binary and batching/pipeline logic;
disabled mode collects the next batch only after transmitting and deleting the
current batch. Omit the parameter to retain the deployed server's normal behavior.
`-MinimumPasses` defaults to 2; `-LoadThreads 16` keeps insertion parallelism fixed
when comparing different migration worker counts (the default follows workers).
For newer servers with native `UseScanAheadMigration` and
`MigrationDeleteBatchSize` settings, use `-DeleteBatchSize 256 -ScanAhead $false`.
This omits the removed `FastMigrate` configuration and verifies both native
settings in startup logs instead of using the benchmark environment toggle.
Zero (the default) selects the older server contract, not a zero-sized delete
batch. Use `key-counts.csv` to audit actual `DBSIZE` results after loading and
after each transfer.
`-MeasureProtocolBytes` requires an instrumented server that emits
`MIGRATION_WIRE_STATS records=<count> bytes=<bytes> messages=<count>` once per
completed worker. The coordinator retains source-log slices and rejects missing
workers or mismatched key counts. `UsefulGBps` counts key plus value bytes;
`ProtocolGBps` counts actual serialized forward migration messages, including
record layout and RESP framing but excluding reverse acknowledgements and
TCP/IP overhead. NIC counters remain a separate measurement.

Each dataset has 2,097,152 eight-byte keys. The log page size is 512 KiB rather
than the older experiment's 16 MiB, giving even the small-value dataset at least
128 pages for 64-worker partitioning without loading 67 million keys.
Log memory is 16 GiB; storage and AOF devices remain null. These results are
not directly comparable with the older dataset/page-size matrix.

The coordinator starts one cluster per buffer/worker/pipeline configuration,
reuses it across payload sizes, and runs SET preload directly with matching
workers, pipeline 4,096, and runtime zero (no subsequent GET benchmark).
It warms up with a forward/return pair, then measures at least two forward
transfers and at least five cumulative migration seconds. Returns are untimed
for reporting. Each transfer independently checks source zero, destination
cardinality, and sample lengths; MigrateBench verifies slot ownership.
Only the verified-empty source is truncated before reuse, avoiding historical
tombstone accumulation. Duplicate MigrateBench counts are disabled.

The workflow also runs `iperf3` from source to destination with 1/8/64 streams,
separately from migration. `comparison.csv` reports migration keys/s, useful
payload GB/s, measured source TX/RX and destination RX/TX, and comparison with
the advertised 6.75 GB/s VM ceiling and measured network baseline.
NIC means use duration-weighted 100 ms samples wholly inside the precise
forward-transfer windows; setup, counts, return transfers and gaps are excluded.
These are repeated transfers in one session, not independent statistical trials.

For scan/send overlap experiments, scale key count with workers rather than
only increasing value size. The native collection limit is measured in keys.
Use 2,097,152 keys with one worker, 8,388,608 with eight workers, and 67,108,864
with 64 workers, giving approximately eight/four/four batches per worker.
Run each worker count separately with `-KeyCount`, `-LogMemoryGiB 128`, and
`-IndexMemoryGiB 4`, on both the baseline and scan-ahead revisions. Keep those
memory settings identical across the matched runs; these larger-memory results
must not be compared directly with the default matrix.
The largest dataset occupies about 66 GiB of log records at 1,024-byte values.
Budget separately for server memory and client request generation on the client
VM. The coordinator checks effective log/index budgets and rejects eviction
before counting a loaded or transferred dataset. Redis verification commands
have a bounded 180-second timeout to accommodate full-log scans.

**Destructive:** use dedicated idle source/destination port 7000 instances.
The coordinator flushes their data and stops only the server/sampler processes
it starts. Existing unrelated services must be stopped explicitly beforehand.
Results are written locally on the client and remote logs are retrieved on exit.

## Controlled foreground helpers

`Resp.benchmark --migration-workload-directory <directory>` enables the separate
controlled workload implemented by `ClusterMigrationWorkload`. It requires online
cluster SERedis traffic, `--skipload`, GET/SET percentages, `--itp 1`, numeric
eight-byte keys, 2,048-byte values, and `--migration-target <second-primary>`.
TLS and replica traffic are not supported. Each worker awaits one request at a
time; these results are not interchangeable with pipelined LightClient results.

`foreground.ps1` and `validation.ps1` are helpers for a coordinator that supplies
workload control and transfer operations. Neither `run.ps1` nor `throughput.ps1`
launches this foreground workload automatically. The helpers' bandwidth summaries
assume the fixed key/value layout above.

### Measurement windows

The concurrent-workload harness runs one baseline window before all measured
forward migrations and one recovery window immediately after the final measured
forward migration, before its return transfer. Each window defaults to 30 seconds.
Intermediate passes retain their preconditioning, exact migration-overlap
accounting, request draining, and source/target DBSIZE checks, but do not repeat
baseline or recovery windows.

Each updated case records `Mode=OncePerCase` in `measurement-windows.json` and
retains `foreground-baseline.json` and `foreground-recovery.json` alongside the
per-pass migration replies. `Get-ForegroundSummary` requires both complete windows
and aggregates each exactly once. Historical cases without this metadata retain
their original per-pass phase interpretation. Keep the phase policy with the
results when comparing cases.

## Legacy runner prerequisites

Use the existing Azure VMSS tooling to provision vm0, vm1, and vm2 with Azure/SSH
access. Run with PowerShell 7 and a local .NET 10 SDK. The runner publishes Release
Linux MigrateBench and uploads it and the current cluster helper.
For repeated sweeps, `-MigrateBenchPath` can select an already deployed executable
on vm2, skipping publication and upload. Ensure that executable uses the intended
revision and corrected timing.

Deploy a compatible Garnet build to both primaries first: it must support the
`MigrateBatchSize` and `MigrationSendBufferSize` settings and migration with vector sets disabled. The runner
does not rebuild or deploy the server. These server changes are separate from
the experiment resources. MigrateBench timing must use `Stopwatch.Frequency`.

**Destructive:** each value size recreates the declared vm0/vm1 instances on port
7000, replacing their data. Windows sleep is inhibited during the run and restored
on exit.

## Run

Run commands from the `Azure.benchmark` root:

```powershell
.\experiments\migration\run.ps1 -rg <resource-group> -VmssName <vmss-name>
```

By default, the runner loads all three datasets using 16 SET workers, then migrates
slots `0..8191` with one migration worker, `FastMigrate=false`, a 256 KiB send buffer, and a collection
batch limit of 262,143 keys. It measures three forward transfers, returning the
range to vm0 between measurements. Return transfers are not reported as forward
measurements. `-TimeoutSec` bounds each MigrateBench process (default 600 seconds).

For matched workers and full-slot transfers into an empty destination:

```powershell
foreach ($batch in @(4096, 262143)) {
    foreach ($workers in @(1, 8, 64)) {
        .\experiments\migration\run.ps1 -rg <resource-group> -VmssName <vmss-name> `
            -StartSlot 0 -EndSlot 16383 -Repetitions 1 -FastMigrate $false `
            -LoadThreads $workers -ParallelMigrateTaskCount $workers -MigrateBatchSize $batch
    }
}
```

To add the 128/256/512 KiB send-buffer dimension (54 full-slot measurements):

```powershell
foreach ($buffer in @(131072, 262144, 524288)) {
    foreach ($batch in @(4096, 262143)) {
        foreach ($workers in @(1, 8, 64)) {
            .\experiments\migration\run.ps1 -rg <resource-group> -VmssName <vmss-name> `
                -StartSlot 0 -EndSlot 16383 -Repetitions 1 -FastMigrate $false `
                -LoadThreads $workers -ParallelMigrateTaskCount $workers `
                -MigrateBatchSize $batch -MigrationSendBufferSize $buffer
        }
    }
}
```

`-ValueLengths 256` selects one dataset. The runner generates effective cluster,
server, benchmark and preload configurations beside each run's results; it does
not modify these source declarations or the shared diskless profile.

Use `FastMigrate=false` for insertion-complete acknowledgements. The fast receive
path acknowledges queued imports before insertion finishes, so sender completion
does not establish destination completeness. Independently checked counts must
agree for a measurement to be accepted.

## Datasets and batching

`-MigrationSendBufferSize` explicitly selects 131,072, 262,144 (experiment default),
or 524,288 bytes independently of the 16 MiB log pages and collection-batch limit.
The runner verifies the effective size from the migration manager's startup log.
Experiment configurations enable Information-level logging so that startup
buffer-size evidence is retained.
The previous experiment used page-sized 16 MiB send buffers; 262,143 was its
collection limit in keys, not its send-buffer size in bytes.

`LSet: true` means **single-key SET preload**, not the Redis list command `LSET`.
Keys are eight bytes and each fresh database occupies at least 2 GiB of log records,
which the runner verifies from the main-log tail.

| Value bytes | Keys | Value payload |
|------------:|-----:|--------------:|
| 8 | 67,108,864 | 512 MiB |
| 256 | 8,388,608 | 2 GiB |
| 1,024 | 2,097,152 | 2 GiB |

A one-second GET phase follows preload and finishes before migration starts.
SET pipelines contain 4,096 commands. `-MigrateBatchSize` controls collection
batches, not network-buffer size: larger batches can span multiple sends, and
each batch flushes its final buffer. The shared profile uses a 16 MiB page/send
buffer when no explicit override is supplied to the server. Smaller send buffers
split a 4,096-key collection batch across multiple sends for larger values. Tail batches and
finite page-aligned worker partitions can contain fewer keys than the batch limit.

To run only the declared cluster and default 256-byte preload:

```powershell
.\03-workload\cluster.ps1 -Action create -Definition .\experiments\migration\cluster.json -rg <resource-group> -VmssName <vmss-name>
.\03-workload\benchmark.ps1 -Definition .\experiments\migration\benchmark.json -rg <resource-group> -VmssName <vmss-name> -Verbose
```

## Validation and measurements

The runner checks actual load workers, deployed migration worker/batch settings,
observed collection batch sizes, slot ownership, source/destination key counts,
key conservation, and sample value lengths. Full-slot transfers must move every
loaded key. Failures terminate the workflow.

Controlled foreground experiments fully verify acknowledged write values only
when the dataset payload (`key count * value bytes`) is at most 1 GiB. Larger
datasets use exact source/target `DBSIZE` conservation after every forward and
return migration, without exhaustive value verification. Each case records its
validation mode in `validation-policy.json`; older cases retain their original
verification mode.

Generated configurations, preload/migration/batch logs, and `measurements.csv`
are saved under the existing ignored `03-workload\results\migrate-slots-*`
directory. The underlying benchmark driver also retains its own results.

| CSV metric | Definition |
|------------|------------|
| `Seconds` | MigrateBench wall time from issuing MIGRATE until `CLUSTER MTASKS` reaches zero |
| `MovedKeys` | Destination count increase, verified against the source decrease |
| `ValueGBps` | `MovedKeys * ValueBytes / Seconds / 1e9` |
| `KeyValueGBps` | `MovedKeys * (KeyBytes + ValueBytes) / Seconds / 1e9` |
| `LoadedLogBytes` | Source log tail minus its initial address, before migration |
| `LoadSeconds` | Initial SET execution time, excluding request generation and subsequent GETs |
| `LoadOpsPerSecond` | Loaded keys divided by `LoadSeconds` |
| `LoadValueGBps` | `LoadOpsPerSecond * ValueBytes / 1e9` |
| `MigrationOpsPerSecond` | `MovedKeys / Seconds` |
| `MigrationToLoadRatio` | Migration keys/s divided by initial SET inserts/s |

Rows also record actual SET workers, SET pipeline size, migration workers,
FastMigrate, configured/observed migration batch size, and send-buffer size.
GB/s uses decimal GB and measures useful data, not wire traffic. Protocol framing,
migration metadata, AOF traffic and record overhead are excluded. Equal insertion
and migration throughput is an optimization target, not a guarantee: migration
also scans, serializes, coordinates ownership, and deletes source records.
