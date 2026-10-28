# LightEpoch drain-stranding reproduction

Tests the release-ordering race that can leave a drain callback pending after all
threads have suspended. A stranded flush callback can leave client requests unsent.
The harness exercises the epoch directly; it does not need a server or socket.

```powershell
# Actual checked-out production epoch
dotnet run -f net10.0 -c Release --project playground\LightEpochLitmus

# Volatile-only release, without checking out another commit
dotnet run -f net10.0 -c Release --project playground\LightEpochLitmus -- --buggy

# The race without LightEpoch: broken release, then the two fences
dotnet run -f net10.0 -c Release --project playground\LightEpochLitmus -- --toy
```

Only five options are supported:

| Option | Meaning |
|---|---|
| `--buggy` | Test a production-derived copy whose release-slot clear uses `Volatile.Write` |
| `--toy` | Run the reduced race (see [Toy](#toy)); cannot be combined with `--buggy` |
| `--seconds N` | Stop after N seconds; default 180 (with `--toy`, per release) |
| `--rounds N` | Stop after N rounds, `_` separators allowed (`10_000_000`); default 20,000,000 per release with `--toy` |
| `--help` (`-h`) | Show usage |

`--seconds` and `--rounds` cannot be combined. The detector control always runs for
five seconds. Use `--rounds` to compare variants: each then runs the same number of
rounds, so `missed` is directly comparable. The drain split and `elapsed` still vary
with timing.

The default always tests the actual `Tsavorite.core.LightEpoch` from the checked-out
commit. Garnet.client compiles the same source under its own namespace. Before the
Exchange fix, default and `--buggy` both exercise a volatile-only release; after the
fix, default exercises Exchange and `--buggy` still exercises the unfenced release.
Rebuild after changing commits; do not use `--no-build`.

## What it checks

Two pinned threads enter the epoch. One suspends after a swept jitter delay. The
other queues a callback with `BumpCurrentEpoch` and suspends. Once both have returned,
the callback must have run. A third rendezvous prevents the next round's `Resume`
from rescuing a pending callback before it is counted.

With `Volatile.Write`, the slot clear can remain buffered while the suspender reads
`drainCount == 0`. The bumper can increment `drainCount`, still see the occupied slot,
and return without executing the callback. With no further epoch activity, it stays
stranded. Exchange fences the clear before subsequent counter reads.

The harness counts strands, then deliberately rescues them in the next round so it
can finish and report rather than hang forever. Read-only disturber threads keep
epoch-table cache lines shared to increase contention; up to six are selected
automatically from processors not used by the two participants.

## Detector validation and results

A five-second **control always runs first**. Its generated epoch copy reads
`drainCount` before clearing the slot in `Suspend`. A bounded 64-iteration
`Thread.SpinWait` widens this calibrated window without scheduler yielding.
Strands in this control are intentional and validate the detector; they are not
failures in production code.

The subsequent stress run does not force reordering. `--buggy` changes only the
release-slot clear in its generated copy. Both generated variants track production
source, and the build fails if their required edits no longer match.

| Exit | Meaning |
|---|---|
| 0 | No strands observed, and callbacks ran on both participant threads |
| 1 | At least one action was stranded in the stress run |
| 2 | Inconclusive: control failed to detect a strand, or callbacks ran on only one participant |
| 3 | Unsupported OS, fewer than four logical processors, or detected emulation |
| 64 | Invalid arguments |

`drainedBySuspender` and `drainedByBumper` identify the executing thread. Both must
be nonzero for a clean result to be meaningful, but they cannot prove every race
was sampled. Reproduction is probabilistic: a native x64 pre-fix run found four
strands in 37.7 million rounds over 180 seconds, while a separate 60-second run
found none. Use `--seconds` for longer runs. A finite clean run is not proof.

Run on native Windows or Linux hardware. Emulation does not reliably reproduce the
target's memory ordering, and the harness rejects detected emulation. Core pinning
must succeed; CPU affinity restrictions can prevent running the selected layout.

## Toy

`--toy` reduces the race to two memory operations per thread, without LightEpoch,
in `ToyLitmus`. Its drivers are two pinned threads:

- `SuspenderDriver` clears its slot, then reads `drainCount` (`Release`, then the
  `Suspend` check), using one of `Foo_Broken` (`Volatile.Write`),
  `Foo_Fixed_Exchange` (`Interlocked.Exchange`), or `Foo_Fixed_Barrier`
  (`Volatile.Write` + `Thread.MemoryBarrier`).
- `BumperDriver` runs `Bar`: it marks the drain pending, increments `drainCount`,
  then reads the slot (`BumpCurrentEpoch`, then the `SuspendDrain` scan). It also
  resets the state each round.

Either side that sees the other's store calls `TryDrain`, which claims the pending
drain with a compare-and-swap before counting it, as LightEpoch claims a drain-list
entry. Both sides may see each other's store, so the claim keeps a drain from
running twice. A correct release therefore executes exactly one drain per round, and
`missed = rounds - executed`.

Each release runs in turn on the same cores, for 20,000,000 rounds by default. On
native x64 the broken release misses hundreds of drains, and both fences miss none. With
`--toy`, exit 0 means the broken release reproduced and both fences stayed clean,
1 means a fence missed a drain, and 2 means the broken release did not reproduce.
The toy demonstrates the hardware reordering; the default run shows that
LightEpoch itself is exposed to it.
