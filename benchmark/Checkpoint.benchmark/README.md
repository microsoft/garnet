# Checkpoint-file transfer benchmark

This signed, standalone executable sweeps checkpoint chunk sizes and bounded
pipeline depths using the real Garnet
`FileDataSource → FileTransmitSource → GarnetClientSession.ExecuteClusterSnapshotData`
sender and `FileDataSink.WriteChunk → OK` receiver path. It uses two processes:
one sender and one dedicated benchmark receiver, on the same machine or on two
machines. No Garnet server is started, reset, or modified. Checkpoint selection,
recovery, AOF replay, cluster administration, and TLS are deliberately excluded.
The receiver is a small TCP harness using Garnet's RESP reader/writer helpers,
not the full Garnet server's network/session dispatch.

## Safety and prerequisites

- Build with the .NET 10 SDK. Run Release builds without a debugger.
- **Never point the sender at a production Garnet server port.** It requires a
  benchmark-specific handshake before sending any checkpoint commands.
- There is **no authentication or encryption**. The receiver defaults to
  `127.0.0.1:7010`. For two machines, bind an explicit private interface on an
  isolated, trusted test network; restrict the firewall to the sender. Do not
  bind a public interface, production interface, or `0.0.0.0`.
- The source is an **existing, offline, immutable file**, opened read-only.
  Nothing generates or truncates source data. Do not use files still being
  changed by a running server. A source file is kept open throughout the sweep,
  and every received file is checked against its original SHA-256 hash.
  This is not a live-checkpoint locking mechanism: keeping the input offline
  and immutable is the caller's responsibility.
- Tsavorite devices take a **segment prefix**, not a normal file name. To
  transfer `E:\checkpoint-input\sample.0`, pass
  `--source-prefix E:\checkpoint-input\sample` (omit `.0`).
  This benchmark uses `Initialize(-1)`: a single unsegmented device, physical
  file `<prefix>.0`, starting at offset zero. It does not support multi-segment
  files, offsets, metadata, or recovery. An existing `<prefix>.1` is rejected.
- Source length and effective chunk size must both be multiples of **both
  devices' sector sizes**. Use files and chunks aligned to 4096 bytes for the
  usual 512-byte/4-KiB devices. A final short chunk is supported if sector aligned.
  Unaligned files fail instead of padding or claiming extra transferred bytes.
- `--output` must already exist on the destination storage. Each run creates a
  new `checkpoint-bench-<token>\payload.0`, reserved with `FileMode.CreateNew`.
  Existing run paths, source files, and CSV files are never overwritten. Received
  files, including partial files from failed runs, are retained. **There is no
  automated cleanup or arbitrary-path deletion.** Remove only your benchmark
  run directories yourself when finished.
- Budget destination capacity: the default sweep retains **48 copies** of the
  source (4 chunks × 4 depths × 3 repeats). For a 512-MiB source, that is 24 GiB.
  A receiver serves one connection at a time and exits after `--runs` verified
  transfers. Set `--runs` to the number of distinct effective chunk/depth pairs
  times repeats; sender deduplicates pairs that normalize to the same values.

## Build

From the repository root:

```powershell
dotnet build benchmark\Checkpoint.benchmark\Checkpoint.benchmark.csproj -c Release -f net10.0
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll --help
```

You can run the same built executable on both machines, or build the same revision
on each. Record the revision, OS, CPU, source/destination device, filesystem,
network link speed, and all arguments alongside the CSV.

## Prepare a disposable source (optional)

Prefer a copy of an offline checkpoint file on the source device you want to
measure. For synthetic testing, this PowerShell example explicitly creates a
**new** 512-MiB random file. It fails if either preparation directory already
exists or `sample.0` exists; it never reuses or replaces an existing file.
Choose your actual storage paths instead of these examples. Preparation is
outside the benchmark and is not timed.

```powershell
New-Item -ItemType Directory -Path E:\checkpoint-input -ErrorAction Stop
New-Item -ItemType Directory -Path F:\checkpoint-output -ErrorAction Stop
$file = [System.IO.FileStream]::new('E:\checkpoint-input\sample.0',
    [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write,
    [System.IO.FileShare]::None)
try {
    $block = [byte[]]::new(1MB)
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        for ($i = 0; $i -lt 512; $i++) {
            $rng.GetBytes($block)
            $file.Write($block, 0, $block.Length)
        }
    } finally { $rng.Dispose() }
} finally { $file.Dispose() }
```

## Run locally

Start the receiver in terminal 1, then start the sender in terminal 2. The
120-second timeout allows time to start the sender and hash the source; increase
it for very large files or slow storage.

```powershell
# Terminal 1: dedicated destination path, 48 successful runs, no ACK delay.
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll receive --output F:\checkpoint-output --runs 48 --timeout-seconds 120

# Terminal 2: default sweep, with a NEW results file.
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll send --source-prefix E:\checkpoint-input\sample --csv .\checkpoint-results.csv --timeout-seconds 120
```

For a quick functional check, use `receive --runs 1` and
`send --chunks 128k --depths 1 --repeats 1`. With a 260-KiB source aligned to
4096 bytes, that exercises two full 128-KiB chunks, one 4-KiB chunk, and EOF.

## Run on two machines

For example, if the receiver's **private test interface** is `192.168.50.20`,
and both machines have built the same revision:

```powershell
# Receiver machine (private trusted network only).
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll receive --bind 192.168.50.20 --port 7010 --output F:\checkpoint-output --runs 48 --timeout-seconds 120

# Sender machine.
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll send --host 192.168.50.20 --port 7010 --source-prefix E:\checkpoint-input\sample --chunks 128k,512k,1m,4m --depths 1,2,4,8 --repeats 3 --csv .\checkpoint-private-network.csv --timeout-seconds 120
```

`--host` and `--bind` take IP literals (IPv4 or IPv6), not DNS names. Unix users
can run the same DLL with their platform's path separators and existing storage
paths.

## Options

| Mode | Option | Default | Meaning |
| --- | --- | --- | --- |
| send | `--source-prefix` | required | Existing offline `<prefix>.0` file, no `.0` suffix in the argument |
| send | `--host` | `127.0.0.1` | Benchmark receiver IP |
| send | `--chunks` | `128k,512k,1m,4m` | Comma-separated chunk sizes |
| send | `--depths` | `1,2,4,8` | Comma-separated bounded FIFO depths |
| send | `--repeats` | `3` | Repeats per distinct effective pair |
| send | `--csv` | none | New CSV path; refuses an existing file |
| receive | `--output` | required | Existing destination directory |
| receive | `--bind` | `127.0.0.1` | Listener IP |
| receive | `--runs` | `1` | Number of verified transfers before exit |
| receive | `--max-chunk` | `16m` | Bounded receive-frame allocation limit |
| receive | `--ack-delay-ms` | `0` | Fixed additional ACK availability delay after each completed data write and EOF |
| both | `--port` | `7010` | Dedicated benchmark port |
| both | `--timeout-seconds` | `30` | Connect, command, I/O, verification, and receiver accept/read timeout (1–3600 seconds) |
| both | `--device` | `RandomAccess` | Real Tsavorite local device: `RandomAccess`, `Native`, or `FileStream` |
| both | `--buffered` | `false` | Request OS-buffered device I/O; backend/platform support varies |

Chunk/depth validation reuses `GarnetServerOptions`: memory sizes use binary
units, chunks normalize down to powers of two, and pipeline depth is bounded
(currently 1–64, with depth × effective chunk at most 512 MiB). Output and CSV
show **effective bytes**, not the requested spelling. The default receiver
accepts chunks up to 16 MiB; increase `--max-chunk` explicitly for larger sweeps.
The sender uses the production checkpoint send-buffer sizing rule:
`max(1 MiB, 2 × effective chunk)`.

`RandomAccess` is the portable default and uses actual Tsavorite device I/O.
To match Windows native checkpoint I/O, set `--device Native` on both ends.
On Linux, the native source device does not implement a read-only open, so the
sender refuses it: use `RandomAccess` for the source, optionally `Native` on the
receiver. `Native` requires its supported OS/native dependencies. Backends can
fall back from direct I/O to buffered I/O depending on the filesystem/platform;
do not assume `--buffered false` proves cache bypass. Record the actual backend
and buffering choices on **both machines**.

## Delayed-ACK experiment

To observe the benefit of depth under latency without a second machine:

```powershell
# Terminal 1: eight runs, 10 ms additional delay, same retained-file rules.
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll receive --output F:\checkpoint-output --runs 8 --ack-delay-ms 10 --timeout-seconds 120

# Terminal 2: one chunk size, four depths, two repeats.
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll send --source-prefix E:\checkpoint-input\sample --chunks 128k --depths 1,2,4,8 --repeats 2 --csv .\checkpoint-delayed-ack.csv --timeout-seconds 120
```

An independent FIFO response writer releases each ACK no earlier than
`completed-write-time + delay`. The receiver **continues reading and writing
subsequent chunks while earlier ACKs are delayed**; it does not sleep in the
write loop or add a serial service-time delay per chunk. Its response queue is
bounded above the maximum supported pipeline depth. Scheduler granularity can
add jitter, especially at sub-millisecond delays. This models delayed ACK
availability, not bandwidth limits, packet loss, TCP retransmission, or every
property of a real WAN. Validate final settings on the real network.
Requested delay is a minimum target, not a measured RTT: the implementation
rounds the remaining wait up to whole milliseconds, and host timer scheduling
can make a requested 1-ms wait substantially longer. See [RESULTS.md](RESULTS.md)
for one host's short, warm-cache loopback validation and this caveat in practice.

## What the results mean

The timed interval starts immediately before `FileTransmitSource.TransmitAsync`
and ends when it returns after the final empty EOF marker's `OK`. It includes
source reads, framing/copying/batching, real TCP transfer, sector-aligned
`FileDataSink` writes, ACK waits, and sink completion/device close. Connection
setup, file reservation, source SHA-256, and destination SHA-256 are outside
the interval. Hash reads can warm filesystem caches; source prehash and repeats
make these **steady-state**, not guaranteed cold-cache measurements.

For every run, the receiver validates token, file type, contiguous addresses,
exact chunk sizes, declared total bytes, and one EOF at exactly the final
address. It sends `OK` only after `WriteChunk` has completed, and the EOF `OK`
only after sink completion. A separate untimed `VERIFY` then checks physical
length and SHA-256. Only a verified run prints throughput or writes a CSV row.
Both processes exit nonzero on disconnect, timeout, protocol/I/O error,
cancellation, or hash mismatch; a failed run has **no success-shaped rate**.
CSV rows already written for earlier successful runs remain valid, but a
nonzero sender exit means the sweep was incomplete. Ctrl+C cancels the run;
network waits and storage waits are bounded by the selected timeout.

`MB/s = completed payload bytes / elapsed seconds / 1,000,000` (decimal MB,
not MiB). Reported bytes exclude RESP framing and count completed receiver
writes, not just bytes submitted to a socket. Completed device I/O is not a
claim of crash/power-loss durability; this does not add an extra filesystem
fsync or verify recovery.

Compare medians across repeats for each pair; inspect variability and the
first run separately. Choose the smallest chunk/depth near the throughput
plateau, rather than the largest possible values: extra outstanding work and
larger buffers cost memory and can hurt other workloads. Tiny files primarily
measure startup/EOF overhead, not sustained throughput. Use sufficiently large
representative data, avoid competing disk/network work, and account for loopback
CPU/memory limits, shared-device read/write contention, storage caches, and
destination capacity. This benchmark selects candidates for the file-transfer
stage, not a guarantee of optimal end-to-end replica recovery settings.

## Validation checklist

- Single run, depth 1; single run, depth 8; short sector-aligned final chunk.
- Multi-pair/repeat sweep with CSV; check row count, bytes, chunks, and hashes.
- Delayed-ACK sweep: confirm depth > 1 can overlap ACK delay.
- Try an unaligned source, existing CSV, occupied port, receiver disconnect,
  and receiver timeout: expect nonzero exit, retained partial output, and no
  throughput row for the failed run.
- Use separate processes/devices and then two private-network machines for
  realistic performance measurements; do not compare Debug to Release.
