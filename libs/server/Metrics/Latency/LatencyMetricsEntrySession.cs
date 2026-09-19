// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using HdrHistogram;

namespace Garnet.server
{
    struct LatencyMetricsEntrySession
    {
        static readonly long HISTOGRAM_LOWER_BOUND = 1;
        static readonly long HISTOGRAM_UPPER_BOUND = TimeStamp.Seconds(100);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool IsValidRange(long value)
            => value < HISTOGRAM_UPPER_BOUND && value >= HISTOGRAM_LOWER_BOUND;

        public long startTimestamp;

        /// <summary>
        /// Double-buffered histograms, allocated on the first recorded value. A session typically uses
        /// only some of the latency types, and the buffers are large enough that allocating them at
        /// connection time makes the cost scale with connection count rather than with load.
        /// </summary>
        /// <remarks>
        /// Nulled by the monitor thread when the session quiesces, on paths that do not take the lock
        /// the record path runs under. Any new reader must therefore load this field into a local
        /// exactly once and work from that local; a null check followed by a separate dereference can
        /// fault. See <see cref="ReclaimIfQuiesced"/>.
        /// </remarks>
        public LongHistogram[] latency;

        /// <summary>
        /// Consecutive monitor windows in which this type recorded nothing. Read and written only by the
        /// monitor thread, so it adds nothing to the record path.
        /// </summary>
        int emptyWindows;

        readonly int significantDigits;

        public LatencyMetricsEntrySession(int significantDigits)
        {
            this.significantDigits = significantDigits;
            latency = null;
            emptyWindows = 0;
            startTimestamp = 0;
        }

        /// <summary>
        /// Releases the histograms of a type that has recorded nothing for <paramref name="threshold"/>
        /// consecutive monitor windows, so a connection that was active and then went quiet stops paying
        /// for them. Returns true if this call released them.
        /// </summary>
        /// <param name="threshold">Consecutive empty windows required before releasing.</param>
        /// <remarks>
        /// The reference is dropped rather than handed back to <c>ArrayPool&lt;long&gt;.Shared</c>, and that
        /// is load-bearing. The record path is deliberately unsynchronised, so a caller can already be
        /// holding the array when this runs. Returning the arrays would let another session rent them and
        /// receive that write, or would null <c>_counts</c> under a live writer. Dropping leaves the racing
        /// writer holding the only reference to the graph, so its write is unobservable and the collector
        /// reclaims it afterwards -- costing at most one sample from a window that recorded none.
        /// <para>
        /// That holds only while every reader of <see cref="latency"/> loads it into a local exactly once;
        /// a reader that re-read the field after null-checking it could observe the null published here and
        /// fault instead.
        /// </para>
        /// <para>
        /// Both buffers are tested because one is being written while the other is merged, and a session
        /// that resumed part way through the window has values in only one of them.
        /// </para>
        /// </remarks>
        public bool ReclaimIfQuiesced(int threshold)
        {
            var histograms = latency;
            if (histograms == null)
                return false;

            if (histograms[0].TotalCount > 0 || histograms[1].TotalCount > 0)
            {
                emptyWindows = 0;
                return false;
            }

            if (++emptyWindows < threshold)
                return false;

            emptyWindows = 0;
            latency = null;
            return true;
        }

        /// <summary>
        /// Allocates this type's histograms and returns them, so a caller reads <see cref="latency"/>
        /// exactly once on both the hit and the miss path.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        LongHistogram[] Allocate()
            => latency =
            [
                new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, significantDigits),
                new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, significantDigits)
            ];

        /// <summary>
        /// Releases this type's histograms when the owning session is disposed.
        /// </summary>
        /// <remarks>
        /// Drops the reference rather than returning the arrays to <c>ArrayPool&lt;long&gt;.Shared</c>, for
        /// the reason given on <see cref="ReclaimIfQuiesced"/>. Dispose narrows the window on the record
        /// path but does not close it -- it signals the async waiter without waiting for a pending
        /// operation to finish recording -- so a writer can still hold the array here. Pooling it would let
        /// another session rent it and receive that write.
        /// </remarks>
        public void Release() => latency = null;

        public void Start()
        {
            startTimestamp = Stopwatch.GetTimestamp();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(int ver)
        {
            if (startTimestamp == 0) return;

            long elapsed = Stopwatch.GetTimestamp() - startTimestamp;
            startTimestamp = 0;

            // Single-capture, not a convenience: the monitor thread can null this field concurrently,
            // so a null check followed by a separate dereference would fault. See ReclaimIfQuiesced.
            var histograms = latency ?? Allocate();
            histograms[ver].RecordValue(IsValidRange(elapsed) ? elapsed : HISTOGRAM_UPPER_BOUND);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(int ver, long elapsed)
        {
            if (elapsed == 0) return;

            // Single-capture, not a convenience: the monitor thread can null this field concurrently,
            // so a null check followed by a separate dereference would fault. See ReclaimIfQuiesced.
            var histograms = latency ?? Allocate();
            histograms[ver].RecordValue(IsValidRange(elapsed) ? elapsed : HISTOGRAM_UPPER_BOUND);
        }
    }
}