# Verified local sample — not deployment or disk-throughput results

## Context

Verified on 2026-11-02, Windows 11 Enterprise 10.0.26300, .NET 10 Release,
HEAD `93a3119ab` plus the uncommitted checkpoint-pipeline/benchmark changes.
Host: Intel Xeon Silver 4214R, 12 cores/24 logical processors reported for one
socket, approximately 128 GiB physical memory.

Two separate processes used `127.0.0.1` and dynamically selected dedicated
ports. Both used Tsavorite `RandomAccess` devices with `--buffered false`.
Source and destination were **on the same D: NTFS fixed volume**, under
`benchmark\Checkpoint.benchmark\validation\source.0` and
`benchmark\Checkpoint.benchmark\validation\output\checkpoint-bench-<token>\payload.0`.
These disposable validation files and helper scripts were removed afterward.
The physical drive model/cache state was not characterized.

Source: **67,112,960 random nonzero-data bytes** (64 MiB + 4096 bytes), deliberately
offline and immutable. Every case ended with a 4096-byte final chunk, with
513, 65, or 17 completed chunks for 128 KiB, 1 MiB, or 4 MiB respectively.
Each chunk/depth pair ran three times at each requested ACK delay: **54 verified
transfers total**.

**These are short, warm-cache loopback samples**, not physical disk bandwidth,
WAN throughput, deployment recommendations, or end-to-end replication timings.
SHA-256 reads before the timed transfer can warm filesystem caches, depending
on backend/platform behavior. Repeated reads and destination verification also
affect caches. Source/destination sharing a volume, loopback CPU/memory costs,
scheduling, and host activity confound comparisons. No cold-cache, separate-disk,
two-machine, TLS, recovery, or cluster workload measurements were made.

## Commands

From the repository root:

```powershell
dotnet build benchmark\Checkpoint.benchmark\Checkpoint.benchmark.csproj -c Release -f net10.0
dotnet format Garnet.slnx --no-restore --include benchmark\Checkpoint.benchmark\Program.cs benchmark\Checkpoint.benchmark\Sender.cs benchmark\Checkpoint.benchmark\Receiver.cs benchmark\Checkpoint.benchmark\Protocol.cs --verbosity quiet
dotnet build benchmark\Checkpoint.benchmark\Checkpoint.benchmark.csproj -c Release -f net10.0 --no-restore

# For each DELAY = 0, then 1, in separate terminals.
# Replace PORT, SOURCE_PREFIX, OUTPUT and NEW_CSV with your dedicated values.
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll receive --output OUTPUT --runs 27 --port PORT --ack-delay-ms DELAY --timeout-seconds 60
dotnet benchmark\Checkpoint.benchmark\bin\Release\net10.0\Checkpoint.benchmark.dll send --source-prefix SOURCE_PREFIX --chunks 128k,1m,4m --depths 1,3,8 --repeats 3 --port PORT --timeout-seconds 60 --csv NEW_CSV
```

Final Release build: zero warnings/errors. Repository formatting verification
with `--verify-no-changes` and the same four `--include` paths passed.

## Median samples

Median of three repeats, decimal MB/s. Timing covers the real transmitter's
first read through completed sink writes and final EOF `OK`; connect/setup and
both SHA-256 passes are excluded. Do not infer a generally optimal pair from
these samples.

| Chunk | Depth | No added delay: seconds | MB/s | Requested 1-ms delay: seconds | MB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 128 KiB | 1 | 0.494412 | 135.743 | 7.703407 | 8.712 |
| 128 KiB | 3 | 0.223993 | 299.621 | 2.465736 | 27.218 |
| 128 KiB | 8 | 0.283589 | 236.655 | 0.901653 | 74.433 |
| 1 MiB | 1 | 0.278460 | 241.015 | 1.128829 | 59.454 |
| 1 MiB | 3 | 0.157605 | 425.831 | 0.346699 | 193.577 |
| 1 MiB | 8 | 0.147079 | 456.305 | 0.179634 | 373.609 |
| 4 MiB | 1 | 0.249094 | 269.428 | 0.551673 | 121.654 |
| 4 MiB | 3 | 0.139491 | 481.126 | 0.193604 | 346.651 |
| 4 MiB | 8 | 0.140950 | 476.148 | 0.171824 | 390.592 |

### Requested versus observed timer delay

**1 ms was requested, not a measured 1-ms network RTT.** A separate 100-sample
PowerShell-hosted `Task.Delay(1)` stopwatch probe on this machine measured
6.048-ms minimum, **15.418-ms median**, and 16.745-ms maximum. This probe includes
host invocation overhead and is not direct per-ACK instrumentation. The transfer
times likewise demonstrate that Windows timer scheduling made the minimum
1-ms delay considerably longer. No system timer-resolution setting was changed.

The response writer schedules ACK availability relative to each completed
write and rounds any positive remaining wait up to whole milliseconds; it
does **not** sleep in the receiver's chunk read/write loop. ACKs overlap:
with the same 513 chunks and requested delay, 128-KiB depth 8 finished in
0.902 seconds versus 7.703 seconds at depth 1 (about 8.5× faster).
This is consistent with concurrent ACK waits, not a serialized per-chunk sleep
service bottleneck. It is a controlled functionality demonstration, not a
promise of an 8.5× deployment improvement.

## Correctness and bounded failures

- All 54 normal runs: both processes exited 0; 27 CSV rows per sweep; receiver
  confirmed exact bytes, contiguous order, expected chunk count, correct EOF,
  physical file length, and matching SHA-256. Rates were emitted only after
  this untimed verification.
- Sender rejected chunk `0`, depth `0`/`65`, excessive `512m × depth 2`,
  missing input, and 4097-byte unaligned input: exit 1, no success CSV rows.
- Existing CSV: exit 1; original contents unchanged.
- A dedicated test peer accepted handshake/BEGIN but withheld data ACKs:
  depth-8 sender with `--timeout-seconds 1` exited 1 with an acknowledgement
  `TimeoutException`, no success rows. A peer disconnect during transfer also
  produced exit 1 and no success rows.
- Receiver disconnect and one-second accept timeout: exit 1.
- Deliberately injected out-of-order address, premature EOF, and hash mismatch
  after completed writes: receiver exited 1 with the corresponding validation
  error and no successful verification report.
- All helpers exited or were stopped by their specific tracked process IDs;
  no benchmark listener or validation helper was left running.

No production-layer changes were needed for these checks. The receiver is a
dedicated RESP/TCP harness, not full Garnet session dispatch. Consult README.md
for safe real-storage/private-network measurements and retained-output rules.
