// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>Snapshot/ReadOnly flush ordering for <see cref="AllocatorBase{TStoreFunctions, TAllocator}"/>.</summary>
    public abstract unsafe partial class AllocatorBase<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>Runtime-only ordering state installed while a Snapshot checkpoint flushes live pages.</summary>
        private protected SnapshotFlushCoordination snapshotFlushCoordination;

        /// <summary>Bounded Snapshot completion window for this allocator. Object allocators use their object-log
        /// flush-buffer count. A non-object NullDevice still needs a window to keep its live pages resident until
        /// Snapshot writes complete; real-device non-object allocators require no coordination.</summary>
        internal int SnapshotFlushCoordinationWindowSize
        {
            get
            {
                var windowSize = SnapshotFlushWindowSize;
                return windowSize > 0 ? windowSize : IsNullDevice ? 1 : 0;
            }
        }

        /// <summary>Serializes coordination installation/removal with NullDevice HeadAddress publication.</summary>
        readonly object snapshotFlushSync = new();

        /// <summary>
        /// Exact first Snapshot address. This is also the initial HeadAddress limit when the first Snapshot page begins
        /// mid-page; later watermark advances convert the limit to page boundaries.
        /// </summary>
        long snapshotStartAddress;

        /// <summary>
        /// Publish permissive Snapshot coordination during PREPARE. It tracks subsequent ReadOnly flushes but does not
        /// block them until WAIT_FLUSH arms the page watermark.
        /// </summary>
        internal void PrepareSnapshotFlushCoordination(SnapshotFlushCoordination coordination)
        {
            lock (snapshotFlushSync)
            {
                Volatile.Write(ref snapshotStartAddress, IsNullDevice ? HeadAddress : FlushedUntilAddress);
                Volatile.Write(ref snapshotFlushCoordination, coordination);
            }
        }

        /// <summary>
        /// Capture the ReadOnly range cutoff, drain that cohort through <see cref="FlushedUntilAddress"/>, then return
        /// the stable Snapshot start address and enter page-by-page Snapshot flushing.
        /// </summary>
        /// <param name="coordination">Coordination published during PREPARE.</param>
        /// <returns>The stable first logical address that Snapshot must persist.</returns>
        internal long InstallSnapshotFlushCoordination(SnapshotFlushCoordination coordination)
        {
            lock (snapshotFlushSync)
            {
                Volatile.Write(ref snapshotStartAddress, IsNullDevice ? HeadAddress : FlushedUntilAddress);
                coordination.BeginCutoffCapture(GetPage(Volatile.Read(ref snapshotStartAddress)));
                // Pair CapturingCutoff publication with the ReadOnly worker's interlocked LastIssued publication.
                // Either the cutoff includes the range, or the worker classifies itself as post-cutoff.
                Interlocked.MemoryBarrier();
                coordination.PublishReadOnlyFlushCutoff(GetLastIssuedReadOnlyFlushAddress());
            }

            // Do not hold snapshotFlushSync while waiting: a captured ReadOnly worker must be free to issue the IO
            // whose FlushedUntilAddress publication completes this drain.
            WaitForReadOnlyFlushCutoff(coordination.ReadOnlyFlushCutoffAddress);

            lock (snapshotFlushSync)
            {
                if (!ReferenceEquals(coordination, snapshotFlushCoordination))
                    throw new TsavoriteException("Snapshot flush coordination was removed during installation");
                var stableSnapshotStartAddress = IsNullDevice ? HeadAddress : FlushedUntilAddress;
                Volatile.Write(ref snapshotStartAddress, stableSnapshotStartAddress);
                coordination.AdvanceReadOnlyFlushPageLimit(GetPage(stableSnapshotStartAddress));
                coordination.BeginFlushing();
                return stableSnapshotStartAddress;
            }
        }

        /// <summary>
        /// Remove <paramref name="coordination"/> if it is still installed. A claim that sampled an older coordination
        /// rechecks under <see cref="snapshotFlushSync"/> and retries against the current state.
        /// </summary>
        internal void ClearSnapshotFlushCoordination(SnapshotFlushCoordination coordination)
        {
            lock (snapshotFlushSync)
            {
                if (ReferenceEquals(snapshotFlushCoordination, coordination))
                {
                    coordination.Close();
                    Volatile.Write(ref snapshotFlushCoordination, null);
                }
            }
        }

        /// <summary>
        /// Classify one contiguous ReadOnly range against the Snapshot cutoff. Returns coordination only for a
        /// post-cutoff range that must obey the page-completion limit.
        /// </summary>
        /// <param name="untilAddress">Exclusive end of the already-published ReadOnly range.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected SnapshotFlushCoordination GetSnapshotFlushCoordinationForReadOnlyRange(long untilAddress)
        {
            var coordination = Volatile.Read(ref snapshotFlushCoordination);
            if (coordination is null)
                return null;

            var disposition = coordination.GetReadOnlyRangeDisposition(untilAddress);
            return disposition switch
            {
                ReadOnlyRangeDisposition.Uncoordinated => null,
                ReadOnlyRangeDisposition.CoordinatePages => coordination,
                _ => GetSnapshotFlushCoordinationForReadOnlyRangeSlow(coordination, untilAddress)
            };
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        SnapshotFlushCoordination GetSnapshotFlushCoordinationForReadOnlyRangeSlow(SnapshotFlushCoordination coordination, long untilAddress)
        {
            // The ReadOnly worker enters as an epoch callback. Waiting while protected could prevent the Snapshot
            // transition's drain from completing, so release that hold only during the brief cutoff handshake.
            var resumeEpoch = epoch.TrySuspend();
            try
            {
                return coordination.WaitForReadOnlyRangeDisposition(untilAddress)
                    ? coordination
                    : null;
            }
            finally
            {
                if (resumeEpoch)
                    epoch.Resume();
            }
        }

        /// <summary>
        /// Wait until Snapshot has completed far enough for ReadOnly to flush <paramref name="page"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected void WaitForSnapshotPage(SnapshotFlushCoordination coordination, long page)
        {
            if (coordination is null || coordination.ReadOnlyMayFlushFast(page))
                return;
            WaitForSnapshotPageSlow(coordination, page);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void WaitForSnapshotPageSlow(SnapshotFlushCoordination coordination, long page)
        {
            var resumeEpoch = epoch.TrySuspend();
            try
            {
                coordination.WaitUntilReadOnlyMayFlush(page);
            }
            finally
            {
                if (resumeEpoch)
                    epoch.Resume();
            }
        }

        /// <summary>
        /// Cap HeadAddress below the Snapshot page still protected by the completion watermark. This is redundant
        /// when a real main-log device already caps HeadAddress through FlushedUntilAddress, but is required for a
        /// NullDevice main log, whose FlushedUntilAddress may already be ahead of the Snapshot start. The caller holds
        /// <see cref="snapshotFlushSync"/> through the HeadAddress publication so installation cannot race this decision.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected long CapHeadAddressForSnapshot(long desiredHeadAddress)
        {
            var coordination = Volatile.Read(ref snapshotFlushCoordination);
            if (coordination is null || !coordination.RestrictsHeadAddress)
                return desiredHeadAddress;

            var startAddress = Volatile.Read(ref snapshotStartAddress);
            var watermarkPage = coordination.ReadOnlyFlushPageLimit;
            var headLimit = watermarkPage <= GetPage(startAddress)
                ? startAddress
                : GetFirstValidLogicalAddressOnPage(watermarkPage);
            return Math.Min(desiredHeadAddress, headLimit);
        }

        /// <summary>The endpoint issued by the allocator's ReadOnly flush worker.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected virtual long GetLastIssuedReadOnlyFlushAddress() => FlushedUntilAddress;

        void WaitForReadOnlyFlushCutoff(long untilAddress)
        {
            while (FlushedUntilAddress < untilAddress)
            {
                var error = errorList.GetEarliestError();
                if (error.FromAddress < untilAddress)
                    throw new TsavoriteException($"ReadOnly flush [{error.FromAddress}, {error.UntilAddress}) failed with error code {error.ErrorCode} before Snapshot installation");
                _ = flushEvent.Wait(TimeSpan.FromMilliseconds(1));
            }
        }
    }

    internal enum SnapshotFlushState : byte
    {
        /// <summary>Published during PREPARE; ReadOnly ranges proceed without restriction.</summary>
        Open,
        /// <summary>WAIT_FLUSH is sampling the last ReadOnly range endpoint.</summary>
        CapturingCutoff,
        /// <summary>The cutoff is stable and Snapshot is waiting for its FlushedUntilAddress publication.</summary>
        DrainingCutoff,
        /// <summary>The Snapshot start is stable and post-cutoff ReadOnly ranges obey the page limit.</summary>
        Flushing,
        /// <summary>All issued Snapshot writes have drained and coordination no longer restricts ReadOnly or Head.</summary>
        Closed
    }

    internal enum ReadOnlyRangeDisposition : byte
    {
        /// <summary>The range is outside page coordination.</summary>
        Uncoordinated,
        /// <summary>The cutoff or stable Snapshot start is not yet available.</summary>
        WaitForState,
        /// <summary>The range is post-cutoff and must obey the page-completion limit.</summary>
        CoordinatePages
    }

    /// <summary>Coordinates Snapshot and ReadOnly page flushes with a monotonic lifecycle and one contiguous
    /// Snapshot-completion watermark.</summary>
    internal sealed class SnapshotFlushCoordination : IDisposable
    {
        /// <summary>Serializes lifecycle, watermark, cutoff, and waiter transitions.</summary>
        readonly object sync = new();

        /// <summary>
        /// Exclusive upper bound for ReadOnly page flushing: page <c>P</c> may flush only when
        /// <c>P &lt; readOnlyFlushPageLimit</c>.
        /// </summary>
        long readOnlyFlushPageLimit;

        /// <summary>Fixed-size ring of out-of-order Snapshot page completions. A slot contains the completed page
        /// whose page number maps to that slot, or <see cref="long.MinValue"/> when it has never been used.</summary>
        readonly long[] completedSnapshotPages;
        readonly int[] snapshotPageStates;

        const int PageInFlight = 0;
        const int PageCompleted = 1;
        const int PageFailed = 2;

        int numInFlightSnapshotPages;

        /// <summary>
        /// First Snapshot flush failure. This field releases coordination waiters but is not thrown by them; the same
        /// exception separately faults the checkpoint's <see cref="FlushCompletionTracker"/> task and is surfaced when
        /// the caller awaits checkpoint completion.
        /// </summary>
        Exception failure;

        /// <summary>Monotonic coordination lifecycle; each checkpoint owns a fresh instance.</summary>
        int state = (int)SnapshotFlushState.Open;

        /// <summary>Number of ReadOnly callers currently waiting on <see cref="sync"/>.</summary>
        int numWaitingReadOnlyFlushes;

        /// <summary>Number of Snapshot issuer callers waiting for bounded completion-window capacity or final completion.</summary>
        int numWaitingSnapshotFlushes;

        /// <summary>Exclusive endpoint of the ReadOnly ranges that Snapshot drains before capturing its stable start.</summary>
        long readOnlyFlushCutoffAddress;

        /// <summary>Create coordination with a fixed number of concurrently issued Snapshot pages.</summary>
        internal SnapshotFlushCoordination(int completionWindowSize = 4)
        {
            if (completionWindowSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(completionWindowSize));

            completedSnapshotPages = new long[completionWindowSize];
            Array.Fill(completedSnapshotPages, long.MinValue);
            snapshotPageStates = new int[completionWindowSize];
        }

        /// <summary>
        /// ReadOnly pages must be strictly below this exclusive limit. During page-by-page Snapshot flushing it is one
        /// page beyond the most recently completed Snapshot page, so ReadOnly waits only for a Snapshot write on the same
        /// page. After the final write it equals the exclusive end page.
        /// </summary>
        internal long ReadOnlyFlushPageLimit
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref readOnlyFlushPageLimit);
        }

        internal SnapshotFlushState State
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (SnapshotFlushState)Volatile.Read(ref state);
        }

        internal bool IsClosed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => State == SnapshotFlushState.Closed;
        }

        internal bool RestrictsHeadAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var current = State;
                return current is SnapshotFlushState.CapturingCutoff
                    or SnapshotFlushState.DrainingCutoff
                    or SnapshotFlushState.Flushing;
            }
        }

        internal long ReadOnlyFlushCutoffAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref readOnlyFlushCutoffAddress);
        }

        /// <summary>Enter the short WAIT_FLUSH interval in which the ReadOnly cutoff is sampled.</summary>
        internal void BeginCutoffCapture(long firstSnapshotPage)
        {
            lock (sync)
            {
                if (State != SnapshotFlushState.Open)
                    throw new TsavoriteException($"Cannot capture Snapshot cutoff from state {State}");
                Volatile.Write(ref readOnlyFlushPageLimit, firstSnapshotPage);
                Volatile.Write(ref state, (int)SnapshotFlushState.CapturingCutoff);
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>Publish the ReadOnly range cutoff and begin draining that cohort through FlushedUntilAddress.</summary>
        internal void PublishReadOnlyFlushCutoff(long untilAddress)
        {
            lock (sync)
            {
                if (State != SnapshotFlushState.CapturingCutoff)
                    throw new TsavoriteException($"Cannot publish Snapshot cutoff from state {State}");
                Volatile.Write(ref readOnlyFlushCutoffAddress, untilAddress);
                Volatile.Write(ref state, (int)SnapshotFlushState.DrainingCutoff);
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>Publish the stable Snapshot start and begin page-completion coordination.</summary>
        internal void BeginFlushing()
        {
            lock (sync)
            {
                ThrowIfFailed();
                if (State != SnapshotFlushState.DrainingCutoff)
                    throw new TsavoriteException($"Cannot begin Snapshot flushing from state {State}");
                Volatile.Write(ref state, (int)SnapshotFlushState.Flushing);
                Monitor.PulseAll(sync);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlyRangeDisposition GetReadOnlyRangeDisposition(long untilAddress)
        {
            var current = State;
            if (current is SnapshotFlushState.Open or SnapshotFlushState.Closed)
                return ReadOnlyRangeDisposition.Uncoordinated;
            if (current == SnapshotFlushState.CapturingCutoff)
                return ReadOnlyRangeDisposition.WaitForState;

            if (untilAddress <= Volatile.Read(ref readOnlyFlushCutoffAddress))
                return ReadOnlyRangeDisposition.Uncoordinated;
            return current == SnapshotFlushState.Flushing
                ? ReadOnlyRangeDisposition.CoordinatePages
                : ReadOnlyRangeDisposition.WaitForState;
        }

        /// <summary>Wait through cutoff capture/drain and return whether this post-cutoff range needs page gating.</summary>
        internal bool WaitForReadOnlyRangeDisposition(long untilAddress)
        {
            lock (sync)
            {
                numWaitingReadOnlyFlushes++;
                try
                {
                    Interlocked.MemoryBarrier();
                    ReadOnlyRangeDisposition disposition;
                    while ((disposition = GetReadOnlyRangeDisposition(untilAddress)) == ReadOnlyRangeDisposition.WaitForState)
                        Monitor.Wait(sync);
                    return disposition == ReadOnlyRangeDisposition.CoordinatePages;
                }
                finally
                {
                    numWaitingReadOnlyFlushes--;
                }
            }
        }

        /// <summary>
        /// Advance the provisional limit after the cutoff ReadOnly cohort drains and the stable Snapshot start is known.
        /// This does not represent a completed Snapshot write; pages below the stable start are already main-log durable.
        /// </summary>
        internal void AdvanceReadOnlyFlushPageLimit(long firstSnapshotPage)
        {
            lock (sync)
            {
                if (failure is not null || firstSnapshotPage <= Volatile.Read(ref readOnlyFlushPageLimit))
                    return;
                Volatile.Write(ref readOnlyFlushPageLimit, firstSnapshotPage);
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>
        /// Publish successful completion of Snapshot page <paramref name="page"/> as an exclusive page limit. ReadOnly
        /// may immediately flush that completed page; only the page Snapshot is currently writing (or a later page)
        /// remains blocked. The watermark is monotonic because callbacks may finish bookkeeping out of order.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CompletePage(long page)
        {
            if (!FinishPage(page, PageCompleted))
                return;

            if (Volatile.Read(ref failure) is not null)
                return;

            var current = Volatile.Read(ref readOnlyFlushPageLimit);
            if (page != current)
                return;

            var initial = current;
            while (Volatile.Read(ref completedSnapshotPages[(int)(current % completedSnapshotPages.Length)]) == current
                && Volatile.Read(ref snapshotPageStates[(int)(current % snapshotPageStates.Length)]) == PageCompleted)
            {
                var observed = Interlocked.CompareExchange(ref readOnlyFlushPageLimit, current + 1, current);
                if (observed == current)
                {
                    current++;
                    continue;
                }
                current = observed;
            }

            // Snapshot-only operation with available window capacity has no waiters and performs no monitor acquisition.
            if (current > initial
                && (Volatile.Read(ref numWaitingReadOnlyFlushes) > 0 || Volatile.Read(ref numWaitingSnapshotFlushes) > 0))
                PulseProgressWaiters();
        }

        /// <summary>Record failure of one reserved Snapshot page and release its completion-window slot.</summary>
        internal void FailPage(long page, Exception exception)
        {
            RecordFailure(exception);
            _ = FinishPage(page, PageFailed);
        }

        bool FinishPage(long page, int completedState)
        {
            var slot = (int)(page % completedSnapshotPages.Length);
            if (Volatile.Read(ref completedSnapshotPages[slot]) != page)
                return false;
            if (Interlocked.CompareExchange(ref snapshotPageStates[slot], completedState, PageInFlight) != PageInFlight)
                return false;

            if (Interlocked.Decrement(ref numInFlightSnapshotPages) == 0)
                PulseProgressWaiters();
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void PulseProgressWaiters()
        {
            lock (sync)
                Monitor.PulseAll(sync);
        }

        /// <summary>Wait until <paramref name="page"/> fits in the fixed-size in-flight completion window.</summary>
        internal void WaitForWindowCapacity(long page)
        {
            ThrowIfFailed();
            var completedThrough = Volatile.Read(ref readOnlyFlushPageLimit);
            if (page - completedThrough < completedSnapshotPages.Length)
                return;

            lock (sync)
            {
                numWaitingSnapshotFlushes++;
                try
                {
                    Interlocked.MemoryBarrier();
                    while (State != SnapshotFlushState.Closed && failure is null
                        && page - Volatile.Read(ref readOnlyFlushPageLimit) >= completedSnapshotPages.Length)
                        Monitor.Wait(sync);
                    ThrowIfFailed();
                    if (State == SnapshotFlushState.Closed)
                        throw new TsavoriteException("Snapshot completion window closed before the page could be issued");
                }
                finally
                {
                    numWaitingSnapshotFlushes--;
                }
            }
        }

        /// <summary>Reserve a previously admitted page in the in-flight completion window.</summary>
        internal void ReservePage(long page)
        {
            var slot = (int)(page % completedSnapshotPages.Length);
            Volatile.Write(ref snapshotPageStates[slot], PageInFlight);
            Volatile.Write(ref completedSnapshotPages[slot], page);
            _ = Interlocked.Increment(ref numInFlightSnapshotPages);
        }

        /// <summary>Test/helper convenience that admits and reserves one page.</summary>
        internal void WaitToIssuePage(long page)
        {
            WaitForWindowCapacity(page);
            ReservePage(page);
        }

        /// <summary>Wait until every Snapshot page below <paramref name="exclusiveEndPage"/> has completed contiguously.</summary>
        internal void WaitForAllPages(long exclusiveEndPage)
        {
            if (Volatile.Read(ref readOnlyFlushPageLimit) >= exclusiveEndPage)
                return;

            lock (sync)
            {
                numWaitingSnapshotFlushes++;
                try
                {
                    Interlocked.MemoryBarrier();
                    while (failure is null && Volatile.Read(ref readOnlyFlushPageLimit) < exclusiveEndPage)
                    {
                        if (State == SnapshotFlushState.Closed)
                            throw new TsavoriteException("Snapshot completion window closed before all issued pages completed");
                        Monitor.Wait(sync);
                    }
                    ThrowIfFailed();
                }
                finally
                {
                    numWaitingSnapshotFlushes--;
                }
            }
        }

        /// <summary>Wait until every page already issued into the bounded window has completed or failed.</summary>
        internal void WaitForInFlightPages()
        {
            if (Volatile.Read(ref numInFlightSnapshotPages) == 0)
                return;

            lock (sync)
            {
                numWaitingSnapshotFlushes++;
                try
                {
                    Interlocked.MemoryBarrier();
                    while (Volatile.Read(ref numInFlightSnapshotPages) > 0)
                        Monitor.Wait(sync);
                }
                finally
                {
                    numWaitingSnapshotFlushes--;
                }
            }
        }

        internal void ThrowIfFailed()
        {
            var exception = Volatile.Read(ref failure);
            if (exception is not null)
                ExceptionDispatchInfo.Capture(exception).Throw();
        }
        /// <summary>
        /// Publish the terminal exclusive page and release the final Snapshot page for ReadOnly flushing.
        /// </summary>
        internal void CloseSuccessfully(long exclusiveEndPage)
        {
            lock (sync)
            {
                ThrowIfFailed();
                if (Volatile.Read(ref numInFlightSnapshotPages) != 0)
                    throw new TsavoriteException("Cannot close Snapshot coordination while page writes remain in flight");
                Volatile.Write(ref readOnlyFlushPageLimit, exclusiveEndPage);
                Volatile.Write(ref state, (int)SnapshotFlushState.Closed);
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>
        /// Record the first Snapshot failure and wake coordination waiters to recheck their conditions. Snapshot
        /// issuance observes the failure immediately; ReadOnly remains gated until in-flight writes drain and state closes.
        /// </summary>
        internal void RecordFailure(Exception exception)
        {
            lock (sync)
            {
                failure ??= exception;
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>Close coordination after all issued Snapshot page writes have drained.</summary>
        internal void Close()
        {
            lock (sync)
            {
                Volatile.Write(ref state, (int)SnapshotFlushState.Closed);
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>
        /// Block until ReadOnly page <paramref name="page"/> is strictly below the exclusive Snapshot completion limit,
        /// or until coordination closes after success or failure.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitUntilReadOnlyMayFlush(long page)
        {
            if (State != SnapshotFlushState.Flushing)
                return;

            // Snapshot normally stays well ahead of ReadOnly. The watermark is monotonic, so once this acquire read
            // observes page below it, no later transition can make the page unsafe. Avoid the monitor's interlocked
            // acquisition and sync-object cache-line traffic on this common path.
            if (page < Volatile.Read(ref readOnlyFlushPageLimit))
                return;

            WaitUntilReadOnlyMayFlushSlow(page);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void WaitUntilReadOnlyMayFlushSlow(long page)
        {
            lock (sync)
            {
                // Register before rechecking the predicate. A concurrent lock-free CompletePage then either sees this
                // waiter and pulses after we release sync in Monitor.Wait, or advances first and this recheck avoids waiting.
                numWaitingReadOnlyFlushes++;
                try
                {
                    // Pair waiter registration with CompletePage's interlocked watermark advance. Either CompletePage
                    // observes this waiter and pulses, or this recheck observes the advanced watermark and does not wait.
                    Interlocked.MemoryBarrier();
                    while (State == SnapshotFlushState.Flushing && page >= Volatile.Read(ref readOnlyFlushPageLimit))
                        Monitor.Wait(sync);
                }
                finally
                {
                    numWaitingReadOnlyFlushes--;
                }
            }
        }

        /// <summary>Lock-free page permission check used after cutoff classification.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ReadOnlyMayFlushFast(long page)
            => State != SnapshotFlushState.Flushing
            || page < Volatile.Read(ref readOnlyFlushPageLimit);

        /// <summary>Release all coordination waiters during checkpoint cleanup.</summary>
        public void Dispose()
            => Close();
    }
}