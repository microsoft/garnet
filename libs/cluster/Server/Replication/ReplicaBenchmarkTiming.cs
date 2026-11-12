// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Garnet.common;

namespace Garnet.cluster
{
    /// <summary>
    /// Optional manager-lifetime recorder of the last successful disk-based shipment and recovery.
    /// Samples whole operations using each process's monotonic clock; phases need not sum to sync time.
    /// No chunk callbacks, history buffer or cross-process clock synchronization are introduced.
    /// </summary>
    internal sealed class ReplicaBenchmarkTiming
    {
        /// <summary>Immutable publication keeps shipment duration and padded-byte count from the same sample.</summary>
        /// <param name="Ticks">Elapsed Stopwatch ticks for one successful whole shipment.</param>
        /// <param name="Bytes">Actual transmitted file bytes, including device-sector padding.</param>
        private sealed record Shipment(long Ticks, long Bytes);
        private Shipment shipment;
        private long recoveryTicks = -1;

        /// <summary>
        /// Allocates only for the explicit startup opt-in, captured once per replication manager.
        /// Disabled managers retain the null fast path; no environment variable activates timing.
        /// </summary>
        /// <param name="enabled">Startup setting; changing options later does not replace a manager's recorder.</param>
        internal static ReplicaBenchmarkTiming Create(bool enabled)
            => enabled ? new() : null;

        /// <summary>Publishes a successful shipment after all files and their EOF completion have finished.</summary>
        /// <param name="started">Stopwatch timestamp taken before iterating this shipment's readers.</param>
        /// <param name="bytes">Sum of actual transmitted offsets across completed files, including padding.</param>
        internal void RecordShipment(long started, long bytes)
            => Volatile.Write(ref shipment, new Shipment(Stopwatch.GetTimestamp() - started, bytes));

        /// <summary>Publishes a successful disk-based recovery without replacing the last sample on failure.</summary>
        /// <param name="started">Stopwatch timestamp at entry to the replica's synchronous recovery operation.</param>
        internal void RecordRecovery(long started)
            => Volatile.Write(ref recoveryTicks, Stopwatch.GetTimestamp() - started);

        /// <summary>
        /// Produces INFO fields from thread-safe last-successful snapshots.
        /// Shipment and recovery are independent samples; unavailable means no successful publication yet.
        /// This formatting/allocation occurs when INFO is requested, not in the transfer chunk loop.
        /// </summary>
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

        /// <summary>Converts a local elapsed tick count to invariant milliseconds for INFO consumers.</summary>
        /// <param name="ticks">Elapsed Stopwatch ticks, never a timestamp from another process.</param>
        private static string Milliseconds(long ticks)
            => (ticks * 1000.0 / Stopwatch.Frequency).ToString("R", CultureInfo.InvariantCulture);
    }
}