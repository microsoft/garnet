---
name: adaptive-spin-wait
description: Selects Garnet spin-wait policies based on the slow-path cost. Use when adding or changing spin loops, busy waits, retry polling, SpinWait, Thread.SpinWait, fixed spin counts, or spin-to-block/backoff transitions.
---

# Adaptive Spin Waits

Use this skill whenever code aggressively spins while waiting for another thread or asynchronous
operation to make progress.

## Choose the Policy

There is no universal spin policy. Choose based on the expected hold time, condition cost, and
the cost of entering the slow path. Use `ConfiguredSpinWait` so that choice is explicit at the call
site instead of encoding policy in a raw loop.

### CPU-Local Spin

Use `NextSpinWillYield` when the slow path is cheap and should begin as soon as CPU-local spinning
would yield:

```csharp
var spinner = ConfiguredSpinWait.CpuLocal();
while (true)
{
    if (ConditionSatisfied())
        return;

    if (!spinner.TryWait())
        break;
}

WaitSlowPath();
```

This policy performs only CPU-local spins. It does not use `SpinWait`'s allocation-free
`Thread.Yield`, `Sleep(0)`, or `Sleep(1)` progression.

### Bounded Progressive Wait

Use a measured fixed number of `SpinOnce` calls when the slow path is comparatively expensive, such
as allocating an asynchronous timer or arming and blocking on a waiter:

```csharp
var spinner = ConfiguredSpinWait.BoundedProgressive(
    maxIterations,
    sleep1Threshold: -1);
while (true)
{
    if (ConditionSatisfied())
        return;

    if (!spinner.TryWait())
        break;
}

WaitSlowPath();
```

`SpinOnce` starts with CPU-local spinning and then progressively uses scheduler yielding. Passing
`sleep1Threshold: -1` permits `Thread.Yield` and `Sleep(0)` but prevents `Sleep(1)`. Use the default
overload when occasional millisecond sleeps are acceptable.

### Pure Busy Spin

Use bounded `Thread.SpinWait` only for calibrated ultra-short waits where scheduler yielding would
be more expensive:

```csharp
for (var i = 0; i < maxIterations && !ConditionSatisfied(); i++)
    Thread.SpinWait(pauseIterations);
```

### Time-Budgeted Spin

Retain a duration-based loop when the configured or public behavior is explicitly expressed as time.
Do not replace a time budget with an iteration count or `NextSpinWillYield`.

## Requirements

1. Use `ConfiguredSpinWait.CpuLocal()` or `ConfiguredSpinWait.BoundedProgressive(...)` for these
   policies; do not reproduce their mechanics inline.
2. Document why the selected policy matches the slow path.
3. Keep every aggressive phase bounded unless indefinite progressive yielding is the intended slow path.
4. Check completion and cancellation at the required cadence.
5. Await a `ValueTask` directly when the slow path can complete synchronously. Do not call
   `AsTask()` unless an API specifically requires `Task`.
6. Account for condition cost. A collection scan can dominate the wait instruction itself.
7. Measure before replacing a bounded progressive wait with the CPU-local policy; entering an
   asynchronous slow path earlier can increase both latency and allocations.

## Exponential Backoff

Exponential backoff is an asynchronous slow path, not a spin policy. `Task.Delay` can allocate when
the operation suspends. A bounded progressive wait can avoid that cost for short transitions, while
the capped backoff still prevents long-held operations from burning a core. Cap and jitter the delay,
honor cancellation, and measure polling CPU, allocations, and release-to-observation latency.

## Validation

Exercise at least:

- Immediate completion.
- Completion during the CPU-local spin.
- Entry into and completion from the slow path.
- Cancellation or timeout, when supported.
- Multiple concurrent waiters.

For performance-sensitive changes, compare CPU, allocations, slow-path entry count, wait latency
percentiles, and predicate invocation count across short and long hold times. Treat calibrated fixed
counts as application-specific values and preserve the benchmark evidence supporting them.
