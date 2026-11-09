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

Both servers use the shared
[`garnet-cluster-diskless.conf`](../../02-platform/tools/config/garnet/garnet-cluster-diskless.conf).
This measures memory-only migration, not NVMe throughput. Its 1 GiB hash index
accommodates the small-value dataset.

## Prerequisites

Use the existing Azure VMSS tooling to provision vm0, vm1, and vm2 with Azure/SSH
access. Run with PowerShell 7 and a local .NET 10 SDK. The runner publishes Release
Linux MigrateBench and uploads it and the current cluster helper.

Deploy a compatible Garnet build to both primaries first: it must support the
`MigrateBatchSize` setting and migration with vector sets disabled. The runner
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
slots `0..8191` with one migration worker, `FastMigrate=false`, and a collection
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

`-ValueLengths 256` selects one dataset. The runner generates effective cluster,
server, benchmark and preload configurations beside each run's results; it does
not modify these source declarations or the shared diskless profile.

Use `FastMigrate=false` for insertion-complete acknowledgements. The fast receive
path acknowledges queued imports before insertion finishes, so sender completion
does not establish destination completeness. Independently checked counts must
agree for a measurement to be accepted.

## Datasets and batching

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
buffer, sufficient for 4,096 records at every tested payload. Tail batches and
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
