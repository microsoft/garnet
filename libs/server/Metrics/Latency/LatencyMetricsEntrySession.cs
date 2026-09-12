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
        /// is load-bearing. <see cref="RecordValue(int)"/> is deliberately unsynchronised: it tests
        /// <see cref="latency"/> for null and then dereferences it, so a caller can already hold the array
        /// when this runs. Returning the arrays would let another session rent them and receive that write,
        /// or would null <c>_counts</c> under a live writer. Dropping leaves the racing writer holding the
        /// only reference to the graph, so its write is unobservable and the collector reclaims it
        /// afterwards -- costing at most one sample from a window that recorded none.
        /// <para>
        /// Both buffers are tested because one is being written while the other is merged, and a session
        /// that resumed part way through the window has values in only one of them.
        /// </para>
        /// </remarks>
        public bool ReclaimIfQuiesced(int threshold)
        {
            if (latency == null)
                return false;

            if (latency[0].TotalCount > 0 || latency[1].TotalCount > 0)
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

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Allocate()
            => latency =
            [
                new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, significantDigits),
                new(HISTOGRAM_LOWER_BOUND, HISTOGRAM_UPPER_BOUND, significantDigits)
            ];

        public void Return()
        {
            if (latency == null)
                return;

            latency[0].Return();
            latency[1].Return();
        }

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

            if (latency == null) Allocate();
            latency[ver].RecordValue(IsValidRange(elapsed) ? elapsed : HISTOGRAM_UPPER_BOUND);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordValue(int ver, long elapsed)
        {
            if (elapsed == 0) return;

            if (latency == null) Allocate();
            latency[ver].RecordValue(IsValidRange(elapsed) ? elapsed : HISTOGRAM_UPPER_BOUND);
        }
    }
}