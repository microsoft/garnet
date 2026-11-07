// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Garnet.common;

namespace Garnet.cluster
{
    internal sealed class ReplicaBenchmarkTiming
    {
        private sealed record Shipment(long Ticks, long Bytes);
        private Shipment shipment;
        private long recoveryTicks = -1;

        internal static ReplicaBenchmarkTiming Create()
            => Environment.GetEnvironmentVariable("GARNET_REPLICA_BENCH_PHASE_TIMING") == "1" ? new() : null;

        internal void RecordShipment(long started, long bytes)
            => Volatile.Write(ref shipment, new Shipment(Stopwatch.GetTimestamp() - started, bytes));

        internal void RecordRecovery(long started)
            => Volatile.Write(ref recoveryTicks, Stopwatch.GetTimestamp() - started);

        internal MetricsItem[] GetMetrics()
        {
            var current = Volatile.Read(ref shipment);
            var recovery = Volatile.Read(ref recoveryTicks);
            return
            [
                new("benchmark_checkpoint_transfer_ms", current == null ? "unavailable" : Milliseconds(current.Ticks)),
                new("benchmark_checkpoint_bytes", current == null ? "unavailable" : current.Bytes.ToString(CultureInfo.InvariantCulture)),
                new("benchmark_recovery_ms", recovery < 0 ? "unavailable" : Milliseconds(recovery))
            ];
        }

        private static string Milliseconds(long ticks)
            => (ticks * 1000.0 / Stopwatch.Frequency).ToString("R", CultureInfo.InvariantCulture);
    }
}