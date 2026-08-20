// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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

        /// <summary>Serializes coordination installation/removal with ReadOnly page-flush claims.</summary>
        readonly object snapshotFlushSync = new();

        /// <summary>
        /// Number of ReadOnly page flushes that have claimed permission and have not yet published their completion
        /// through <see cref="FlushedUntilAddress"/>.
        /// </summary>
        int activeReadOnlyPageFlushes;

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
        /// Arm Snapshot ordering, wait until previously issued and subsequently claimed ReadOnly flushes have published
        /// their <see cref="FlushedUntilAddress"/> advancement, then return the stable Snapshot start address.
        /// New ReadOnly claims at or above the provisional Snapshot page block while installation drains.
        /// </summary>
        /// <param name="coordination">Coordination published during PREPARE.</param>
        /// <returns>The stable first logical address that Snapshot must persist.</returns>
        internal long InstallSnapshotFlushCoordination(SnapshotFlushCoordination coordination)
        {
            lock (snapshotFlushSync)
            {
                Volatile.Write(ref snapshotStartAddress, IsNullDevice ? HeadAddress : FlushedUntilAddress);
                coordination.Arm(GetPage(Volatile.Read(ref snapshotStartAddress)));
                // Pair Arm's publication with the ReadOnly worker's interlocked LastIssued publication. Either this
                // read observes that endpoint, or the worker observes armed and enters the claim path.
                Interlocked.MemoryBarrier();
                // A ReadOnly worker publishes this endpoint before sampling the armed state. Therefore a worker that
                // remains unclaimed is covered here; one that samples armed takes an active claim below.
                coordination.SetPreCoordinationReadOnlyFlushEnd(GetLastIssuedReadOnlyFlushAddress());
                while (activeReadOnlyPageFlushes > 0)
                    Monitor.Wait(snapshotFlushSync);

                WaitForPreCoordinationReadOnlyFlushes(coordination.PreCoordinationReadOnlyFlushEndAddress);
                var stableSnapshotStartAddress = IsNullDevice ? HeadAddress : FlushedUntilAddress;
                Volatile.Write(ref snapshotStartAddress, stableSnapshotStartAddress);
                coordination.AdvanceInitialPage(GetPage(stableSnapshotStartAddress));
                coordination.CompleteInstallation();
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
                    Volatile.Write(ref snapshotFlushCoordination, null);
                Monitor.PulseAll(snapshotFlushSync);
            }
        }

        /// <summary>
        /// If no Snapshot checkpoint exists, return immediately without changing epoch state, taking a lock, or updating
        /// a counter. Otherwise wait until Snapshot has completed far enough for ReadOnly to flush <paramref name="page"/>,
        /// then claim the page under the installation lock.
        /// </summary>
        /// <returns>Whether a coordination claim was acquired and must later be released.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected bool AcquireReadOnlyPageFlush(SnapshotFlushCoordination coordination, long page)
        {
            if (coordination.IsTerminal)
                return false;

            // Once snapshotStartFlushedLogicalAddress is stable, no installation drain remains. Far-behind pages take
            // this lock-free path; only an actual same-page conflict proceeds to the epoch-suspend/wait path below.
            if (coordination.InstallationCompleted && coordination.ReadOnlyMayFlushFast(page))
                return false;

            return AcquireReadOnlyPageFlushSlow(coordination, page);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        bool AcquireReadOnlyPageFlushSlow(SnapshotFlushCoordination coordination, long page)
        {
            // OnPagesMarkedReadOnly enters as an epoch callback. Waiting while protected could prevent the Snapshot
            // transition's drain from completing, so temporarily release that hold only on the uncommon coordination path.
            var resumeEpoch = epoch.TrySuspend();
            try
            {
                while (true)
                {
                    coordination.WaitUntilReadOnlyMayFlush(page);
                    if (coordination.InstallationCompleted)
                        return false;

                    lock (snapshotFlushSync)
                    {
                        if (!ReferenceEquals(coordination, snapshotFlushCoordination))
                        {
                            coordination = Volatile.Read(ref snapshotFlushCoordination);
                            if (coordination is null)
                                return false;
                            continue;
                        }

                        if (coordination.IsTerminal)
                            return false;

                        // The gate may have armed after WaitUntilReadOnlyMayFlush took its permissive fast path.
                        if (!coordination.ReadOnlyMayFlush(page))
                            continue;

                        activeReadOnlyPageFlushes++;
                        return true;
                    }
                }
            }
            finally
            {
                if (resumeEpoch)
                    epoch.Resume();
            }
        }

        /// <summary>
        /// Return the armed coordination to sample once for this contiguous ReadOnly range. If PREPARE/WAIT_FLUSH
        /// publishes or arms coordination afterward, the range's endpoint was already published through
        /// LastIssuedFlushedUntilAddress and is drained as pre-coordination work.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected SnapshotFlushCoordination GetActiveSnapshotFlushCoordination()
        {
            var coordination = Volatile.Read(ref snapshotFlushCoordination);
            return coordination is not null && coordination.IsArmed && !coordination.IsTerminal ? coordination : null;
        }

        /// <summary>
        /// Release one ReadOnly page-flush claim after its callback has published all address advancement. Installation
        /// waits for this count to reach zero before capturing the stable Snapshot start.
        /// </summary>
        private protected void ReleaseReadOnlyPageFlush()
        {
            lock (snapshotFlushSync)
            {
                Debug.Assert(activeReadOnlyPageFlushes > 0);
                if (--activeReadOnlyPageFlushes == 0)
                    Monitor.PulseAll(snapshotFlushSync);
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
            var watermarkPage = coordination.LastCompletedSnapshotPage;
            var headLimit = watermarkPage <= GetPage(startAddress)
                ? startAddress
                : GetFirstValidLogicalAddressOnPage(watermarkPage);
            return Math.Min(desiredHeadAddress, headLimit);
        }

        /// <summary>The endpoint issued by the allocator's ReadOnly flush worker.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected virtual long GetLastIssuedReadOnlyFlushAddress() => FlushedUntilAddress;

        void WaitForPreCoordinationReadOnlyFlushes(long untilAddress)
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

    /// <summary>Coordinates Snapshot and ReadOnly page flushes with one contiguous Snapshot-completion watermark.</summary>
    internal sealed class SnapshotFlushCoordination : IDisposable
    {
        /// <summary>Serializes watermark, terminal-state, and waiter transitions.</summary>
        readonly object sync = new();

        /// <summary>
        /// Exclusive upper bound for ReadOnly page flushing: page <c>P</c> may flush only when
        /// <c>P &lt; lastCompletedSnapshotPage</c>.
        /// </summary>
        long lastCompletedSnapshotPage;

        /// <summary>
        /// First Snapshot flush failure. This field releases coordination waiters but is not thrown by them; the same
        /// exception separately faults the checkpoint's <see cref="FlushCompletionTracker"/> task and is surfaced when
        /// the caller awaits checkpoint completion.
        /// </summary>
        Exception failure;

        /// <summary>Whether the terminal exclusive-end watermark was published or this coordination was disposed.</summary>
        bool completed;

        /// <summary>Whether WAIT_FLUSH has armed the watermark and ReadOnly pages may need to wait.</summary>
        bool armed;

        /// <summary>Whether WAIT_FLUSH finished draining ReadOnly work and captured the stable Snapshot start address.</summary>
        bool installationCompleted;

        /// <summary>Number of ReadOnly callers currently waiting on <see cref="sync"/>.</summary>
        int waitingReadOnlyFlushes;

        /// <summary>Endpoint of ReadOnly IO issued before coordination publication.</summary>
        long preCoordinationReadOnlyFlushEndAddress;

        /// <summary>
        /// Create coordination that initially blocks ReadOnly flushing at and above
        /// <paramref name="provisionalFirstSnapshotPage"/>.
        /// </summary>
        internal SnapshotFlushCoordination(long provisionalFirstSnapshotPage)
            => lastCompletedSnapshotPage = provisionalFirstSnapshotPage;

        /// <summary>
        /// ReadOnly pages must be strictly below this exclusive limit. During page-by-page Snapshot flushing it is one
        /// page beyond the most recently completed Snapshot page, so ReadOnly waits only for a Snapshot write on the same
        /// page. After the final write it equals the exclusive end page.
        /// </summary>
        internal long LastCompletedSnapshotPage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref lastCompletedSnapshotPage);
        }

        internal bool IsArmed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref armed);
        }

        internal bool IsTerminal
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref completed) || Volatile.Read(ref failure) is not null;
        }

        internal bool InstallationCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref installationCompleted);
        }

        internal bool RestrictsHeadAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => IsArmed && !IsTerminal;
        }

        internal long PreCoordinationReadOnlyFlushEndAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref preCoordinationReadOnlyFlushEndAddress);
        }

        internal void SetPreCoordinationReadOnlyFlushEnd(long untilAddress)
        {
            var current = Volatile.Read(ref preCoordinationReadOnlyFlushEndAddress);
            while (untilAddress > current)
            {
                var observed = Interlocked.CompareExchange(ref preCoordinationReadOnlyFlushEndAddress, untilAddress, current);
                if (observed == current)
                    return;
                current = observed;
            }
        }

        /// <summary>Arm the restrictive page watermark at WAIT_FLUSH.</summary>
        internal void Arm(long firstSnapshotPage)
        {
            lock (sync)
            {
                Volatile.Write(ref lastCompletedSnapshotPage, firstSnapshotPage);
                Volatile.Write(ref armed, true);
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>Publish that snapshotStartFlushedLogicalAddress is stable and ReadOnly claims are no longer required.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CompleteInstallation() => Volatile.Write(ref installationCompleted, true);

        /// <summary>
        /// Advance the provisional limit after pre-existing ReadOnly claims drain and the stable Snapshot start is known.
        /// This does not represent a completed Snapshot write; pages below the stable start are already main-log durable.
        /// </summary>
        internal void AdvanceInitialPage(long firstSnapshotPage)
        {
            lock (sync)
            {
                if (failure is not null || firstSnapshotPage <= Volatile.Read(ref lastCompletedSnapshotPage))
                    return;
                Volatile.Write(ref lastCompletedSnapshotPage, firstSnapshotPage);
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
            if (Volatile.Read(ref failure) is not null)
                return;

            var exclusiveCompletedPage = page + 1;
            var current = Volatile.Read(ref lastCompletedSnapshotPage);
            while (exclusiveCompletedPage > current)
            {
                var observed = Interlocked.CompareExchange(ref lastCompletedSnapshotPage, exclusiveCompletedPage, current);
                if (observed == current)
                    break;
                current = observed;
            }

            // Snapshot-only operation has no waiters and performs no monitor acquisition or sync-object write.
            if (exclusiveCompletedPage > current && Volatile.Read(ref waitingReadOnlyFlushes) > 0)
                PulseReadOnlyWaiters();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void PulseReadOnlyWaiters()
        {
            lock (sync)
                Monitor.PulseAll(sync);
        }
        /// <summary>
        /// Publish the terminal exclusive page and release the final Snapshot page for ReadOnly flushing.
        /// </summary>
        internal void Complete(long exclusiveEndPage)
        {
            lock (sync)
            {
                Volatile.Write(ref lastCompletedSnapshotPage, exclusiveEndPage);
                completed = true;
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>
        /// Record the first Snapshot failure and release coordination waiters. The checkpoint task is faulted separately
        /// and surfaces this exception from the caller's checkpoint-completion await.
        /// </summary>
        internal void Fail(Exception exception)
        {
            lock (sync)
            {
                failure ??= exception;
                Monitor.PulseAll(sync);
            }
        }

        /// <summary>
        /// Block until ReadOnly page <paramref name="page"/> is strictly below the exclusive Snapshot completion limit, or
        /// until Snapshot completes or fails.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitUntilReadOnlyMayFlush(long page)
        {
            if (!Volatile.Read(ref armed))
                return;

            // Snapshot normally stays well ahead of ReadOnly. The watermark is monotonic, so once this acquire read
            // observes page below it, no later transition can make the page unsafe. Avoid the monitor's interlocked
            // acquisition and sync-object cache-line traffic on this common path.
            if (page < Volatile.Read(ref lastCompletedSnapshotPage))
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
                waitingReadOnlyFlushes++;
                try
                {
                    // Pair waiter registration with CompletePage's interlocked watermark advance. Either CompletePage
                    // observes this waiter and pulses, or this recheck observes the advanced watermark and does not wait.
                    Interlocked.MemoryBarrier();
                    while (armed && !completed && failure is null && page >= Volatile.Read(ref lastCompletedSnapshotPage))
                        Monitor.Wait(sync);
                }
                finally
                {
                    waitingReadOnlyFlushes--;
                }
            }
        }

        /// <summary>Recheck page permission while the allocator installation lock is held.</summary>
        internal bool ReadOnlyMayFlush(long page)
        {
            lock (sync)
                return !armed || completed || failure is not null || page < lastCompletedSnapshotPage;
        }

        /// <summary>Lock-free permission check used after installation has completed.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ReadOnlyMayFlushFast(long page)
            => !Volatile.Read(ref armed)
            || IsTerminal
            || page < Volatile.Read(ref lastCompletedSnapshotPage);

        /// <summary>Release all coordination waiters during checkpoint cleanup.</summary>
        public void Dispose()
        {
            lock (sync)
            {
                completed = true;
                Monitor.PulseAll(sync);
            }
        }
    }
}