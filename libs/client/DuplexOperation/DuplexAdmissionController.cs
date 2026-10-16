// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.client
{
    /// <summary>
    /// A paired reservation for one duplex operation. The inline address, dense request id, and optional
    /// completion ticket are issued by one compare-and-swap of the allocator cursor and therefore retain the
    /// same relative ordering. The <see cref="requestId"/> counts every scheduled operation (including
    /// fire-and-forget ones that carry no completion ticket) and indexes the dense request-side schedule table.
    /// </summary>
    internal readonly struct DuplexOperationReservation
    {
        internal readonly long requestAddress;
        internal readonly ushort completionTicket;
        internal readonly ushort requestId;

        internal DuplexOperationReservation(long requestAddress, ushort completionTicket, ushort requestId)
        {
            this.requestAddress = requestAddress;
            this.completionTicket = completionTicket;
            this.requestId = requestId;
        }
    }

    /// <summary>
    /// Coordinates admission and backpressure for paired request/completion operations. Request capacity is
    /// released by network flushes, while completion capacity is released independently by received replies.
    /// <para>
    /// A producer reserves its (inline address, completion ticket, request id) with a compare-and-swap that
    /// commits only when every limit holds, so a request id exists only once its producer may write it. The
    /// producer holds the epoch from that reservation until it publishes the schedule entry, and drains only
    /// after publishing. The flusher snapshots the tail and flushes <c>[readOnlyRequestId, tail)</c> from an
    /// epoch-bump callback, which cannot run until every producer that reserved an id in that range has
    /// published it and released or refreshed the epoch.
    /// </para>
    /// </summary>
    internal sealed class DuplexAdmissionController : IDisposable
    {
        // Bounded progressive backoff budget for inline-buffer backpressure before parking on a flush. Kept small
        // because the backpressure window is short (a flush completion) and the slow path is a cheap event wait;
        // SpinOnce escalates to scheduler yields within this budget so a spinning producer never starves the
        // thread that drives the flush.
        const int BackpressureSpinBudget = 16;

        readonly int inlineBufferCapacity;
        readonly int completionCapacity;
        readonly int scheduleCapacity;
        readonly LightEpoch epoch;
        readonly Action<int, int> onPagesMarkedReadOnly;

        LightPageAndOffset tail;
        long flushedInlineAddress;
        int readOnlyRequestId;
        long readOnlyInlineAddress;
        long activeFlushUntilAddress;
        int completionReservations;
        int scheduleReservations;
        int ongoingAggressiveShiftReadOnly;
        int activeFlushCount;
        int activePerOperationFlushResults;
        int disposed;

        // Each CompletionEvent.Set() allocates a SemaphoreSlim and takes its monitor lock, so releasers signal
        // only when a waiter has armed the matching flag. A waiter captures the event generation, arms the flag
        // with a full fence, and then re-checks its condition before waiting; a releaser publishes freed capacity
        // with a full fence and then consumes the flag. By Dekker ordering either the waiter observes the freed
        // capacity or the releaser observes the armed flag and releases the captured (or a later) generation.
        CompletionEvent requestFreed;
        CompletionEvent completionFreed;
        CompletionEvent idleStateChanged;
        int requestFreedWaiters;
        int completionFreedWaiters;
        int idleStateWaiters;

        internal int CompletionTail => tail.TaskId;

        internal int ActivePerOperationFlushResultCount => Volatile.Read(ref activePerOperationFlushResults);

        internal bool IsIdle
            => GetTailAddress() == Volatile.Read(ref flushedInlineAddress) &&
               Volatile.Read(ref completionReservations) == 0 &&
               Volatile.Read(ref scheduleReservations) == 0 &&
               Volatile.Read(ref activeFlushCount) == 0 &&
               Volatile.Read(ref activePerOperationFlushResults) == 0 &&
               Volatile.Read(ref ongoingAggressiveShiftReadOnly) == 0;

        internal DuplexAdmissionController(
            int inlineBufferCapacity,
            int completionCapacity,
            int scheduleCapacity,
            LightEpoch epoch,
            Action<int, int> onPagesMarkedReadOnly)
        {
            Debug.Assert(System.Numerics.BitOperations.IsPow2(inlineBufferCapacity));
            this.inlineBufferCapacity = inlineBufferCapacity;
            this.completionCapacity = completionCapacity;
            this.scheduleCapacity = scheduleCapacity;
            this.epoch = epoch;
            this.onPagesMarkedReadOnly = onPagesMarkedReadOnly;

            requestFreed.Initialize();
            completionFreed.Initialize();
            idleStateChanged.Initialize();
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            requestFreed.Dispose();
            completionFreed.Dispose();
            idleStateChanged.Dispose();
        }

        internal async ValueTask WaitForIdleAsync(CancellationToken token)
        {
            while (!IsIdle)
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);

                DrainRequests();
                var stateChanged = idleStateChanged;
                ArmWaiter(ref idleStateWaiters);
                if (IsIdle)
                    return;

                await stateChanged.WaitAsync(token).ConfigureAwait(false);
            }
        }

        /// <summary>Free-running inline address one past the last reserved inline byte.</summary>
        internal long GetTailAddress()
        {
            LightPageAndOffset local = default;
            local.Value = Volatile.Read(ref tail.Value);
            return local.InlineAddress;
        }

        /// <summary>
        /// Attempts to reserve the ordered inline address, request id and optional completion ticket for one
        /// operation. On backpressure, returns false and identifies the capacity event that the caller should await.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryScheduleOperation(
            int requestSize,
            bool expectsCompletion,
            out DuplexOperationReservation reservation,
            out CompletionEvent waitEvent)
        {
            // Inline-buffer backpressure (the schedule claim is already held) clears on a flush completion. Kick
            // the flusher once, then spin that window with a bounded progressive policy: it begins CPU-local but
            // escalates to scheduler yields so a spinning producer can never starve the thread driving the flush.
            // The spin body does no CompletionEvent.Set() work: each Set() allocates a SemaphoreSlim and takes
            // its internal monitor lock, so re-driving the flush on every iteration convoys every producer on
            // that lock and starves the flusher. On exhaustion, release the claims and let the caller park on
            // the next flush, which is signalled by CompleteFlushPart advancing the flushed inline address.
            var spinner = ConfiguredSpinWait.BoundedProgressive(BackpressureSpinBudget);
            var completionReserved = false;
            var scheduleReserved = false;
            var flushKicked = false;
            while (true)
            {
                Debug.Assert(epoch.ThisInstanceProtected());

                if (expectsCompletion && !completionReserved)
                {
                    if (!TryReserveCompletion())
                    {
                        waitEvent = completionFreed;
                        ArmWaiter(ref completionFreedWaiters);
                        if (!TryReserveCompletion())
                        {
                            reservation = default;
                            return false;
                        }
                    }
                    completionReserved = true;
                }

                if (!scheduleReserved)
                {
                    if (!TryReserveSchedule())
                    {
                        waitEvent = requestFreed;
                        ArmWaiter(ref requestFreedWaiters);
                        if (!TryReserveSchedule())
                        {
                            if (completionReserved)
                                ReleaseCompletions(1);
                            reservation = default;
                            return false;
                        }
                    }
                    scheduleReserved = true;
                }

                if (TryReserveRequest(requestSize, expectsCompletion, out reservation))
                {
                    waitEvent = default;
                    return true;
                }

                if (!flushKicked)
                {
                    // Kick the flusher once so the unflushed prefix is handed to a flush. Repeating DrainRequests
                    // every iteration convoys all producers on CompletionEvent.Set's SemaphoreSlim monitor lock,
                    // so it is done exactly once per backpressure episode. This producer holds no request id here,
                    // so a synchronously fired flush callback cannot wait on it.
                    DrainRequests();
                    flushKicked = true;
                }

                // Drain the epoch every iteration. The flush runs from an epoch-bump callback that cannot fire
                // until every other holder drains or suspends, so a spinning holder must refresh its epoch for
                // the flushed inline address to advance. ProtectAndDrain performs no Set() work unless a flush
                // actually completes, so it does not reintroduce the convoy.
                epoch.ProtectAndDrain();

                if (spinner.TryWait())
                    continue;

                waitEvent = requestFreed;
                ArmWaiter(ref requestFreedWaiters);
                if (TryReserveRequest(requestSize, expectsCompletion, out reservation))
                    return true;

                if (completionReserved)
                    ReleaseCompletionsQuiet(1);
                if (scheduleReserved)
                    ReleaseScheduleReservationsQuiet(1);
                reservation = default;
                return false;
            }
        }

        /// <summary>
        /// Advances the cursor by one request id, one completion ticket when <paramref name="expectsCompletion"/>,
        /// and <paramref name="size"/> inline bytes, committing only if the inline bytes fit. A record that would
        /// run past the end of the inline buffer starts at the beginning of the next lap instead; the skipped gap
        /// is never sent (the flusher locates every record by its own address) and is counted against the
        /// inline capacity until it is flushed past. Returns false, changing nothing, on inline backpressure.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryReserveRequest(int size, bool expectsCompletion, out DuplexOperationReservation reservation)
        {
            while (true)
            {
                LightPageAndOffset local = default;
                local.Value = Volatile.Read(ref tail.Value);
                var address = local.InlineAddress;

                if (size > 0)
                {
                    var offset = address & (inlineBufferCapacity - 1);
                    if (offset + size > inlineBufferCapacity)
                        address += inlineBufferCapacity - offset;

                    var used = LightPageAndOffset.InlineDistance(Volatile.Read(ref flushedInlineAddress), address + size);
                    if (used > inlineBufferCapacity)
                    {
                        // The flushed address can only trail the tail, so a distance beyond the capacity is either
                        // genuine backpressure or a tail that moved after it was read (a later flush then makes
                        // the distance wrap). Only report backpressure against an unchanged tail.
                        if (Volatile.Read(ref tail.Value) != local.Value)
                            continue;
                        reservation = default;
                        return false;
                    }
                }

                var taskId = local.TaskId;
                var requestId = local.RequestCount;
                var next = LightPageAndOffset.Pack(address + size, expectsCompletion ? taskId + 1 : taskId, requestId + 1);
                if (Interlocked.CompareExchange(ref tail.Value, next, local.Value) == local.Value)
                {
                    reservation = new DuplexOperationReservation(address & LightPageAndOffset.kInlineAddressMask, (ushort)taskId, (ushort)requestId);
                    return true;
                }
            }
        }

        bool TryReserveCompletion()
        {
            if (Interlocked.Increment(ref completionReservations) <= completionCapacity)
                return true;

            _ = Interlocked.Decrement(ref completionReservations);
            return false;
        }

        void ReleaseCompletions(int count)
        {
            _ = Interlocked.Add(ref completionReservations, -count);
            SignalIfArmed(ref completionFreedWaiters, ref completionFreed);
        }

        bool TryReserveSchedule()
        {
            if (Interlocked.Increment(ref scheduleReservations) <= scheduleCapacity)
                return true;

            _ = Interlocked.Decrement(ref scheduleReservations);
            return false;
        }

        internal void ReleaseScheduleReservations(int count)
        {
            if (count <= 0)
                return;

            _ = Interlocked.Add(ref scheduleReservations, -count);
            SignalIfArmed(ref requestFreedWaiters, ref requestFreed);
            SignalIfArmed(ref idleStateWaiters, ref idleStateChanged);
        }

        // Give back reservations taken but not used by an attempt that bailed on byte-ring backpressure, without
        // signalling. The attempt is about to park on requestFreed; the flush that frees inline bytes (always
        // possible when bailing, since backpressure means unflushed data exists) sets requestFreed and wakes it.
        // Broadcasting here instead would make every flush wakeup fan out into N re-bails, each re-broadcasting,
        // convoying all producers on CompletionEvent.Set's SemaphoreSlim monitor lock.
        void ReleaseScheduleReservationsQuiet(int count)
            => _ = Interlocked.Add(ref scheduleReservations, -count);

        void ReleaseCompletionsQuiet(int count)
            => _ = Interlocked.Add(ref completionReservations, -count);

        internal void AdvanceCompletion(int consumedCount)
        {
            if (consumedCount <= 0)
                return;

            ReleaseCompletions(consumedCount);
            SignalIfArmed(ref idleStateWaiters, ref idleStateChanged);
        }

        /// <summary>
        /// Arms <paramref name="waiters"/> with a full fence. Call after capturing the event generation and before
        /// re-checking the wait condition.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void ArmWaiter(ref int waiters)
            => _ = Interlocked.Exchange(ref waiters, 1);

        /// <summary>
        /// Signals <paramref name="ev"/> only if a waiter armed <paramref name="waiters"/>. Call after the freed
        /// capacity has been published with a full fence.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void SignalIfArmed(ref int waiters, ref CompletionEvent ev)
        {
            if (Volatile.Read(ref waiters) != 0 && Interlocked.Exchange(ref waiters, 0) != 0)
                ev.Set();
        }

        /// <summary>
        /// Opens a flush of the request-id range handed out by the most recent read-only bump. The inline address
        /// snapshotted with that range's end becomes the flushed inline address once every part of the range
        /// completes.
        /// </summary>
        internal void BeginFlushRange()
        {
            Debug.Assert(Volatile.Read(ref activeFlushCount) == 0);
            activeFlushUntilAddress = readOnlyInlineAddress;
            // Keep a scanner-owned sentinel so synchronous send callbacks cannot retire the range mid-scan.
            Volatile.Write(ref activeFlushCount, 1);
        }

        internal void RegisterFlushPart()
        {
            var count = Interlocked.Increment(ref activeFlushCount);
            Debug.Assert(count > 1);
        }

        internal void CompleteFlushPart()
        {
            try
            {
                var count = Interlocked.Decrement(ref activeFlushCount);
                Debug.Assert(count >= 0);
                if (count == 0)
                {
                    // Flushes are serialized (BeginFlushRange asserts no overlap), so the flushed inline address
                    // only moves forward. The interlocked store is the full fence that orders it before the
                    // waiter-flag reads below.
                    _ = Interlocked.Exchange(ref flushedInlineAddress, activeFlushUntilAddress);
                    SignalIfArmed(ref requestFreedWaiters, ref requestFreed);
                    SignalIfArmed(ref idleStateWaiters, ref idleStateChanged);
                    AggressiveShiftReadOnlyRunner(true);
                }
            }
            catch when (Volatile.Read(ref disposed) != 0)
            {
            }
        }

        internal void RegisterPerOperationFlushResult()
            => Interlocked.Increment(ref activePerOperationFlushResults);

        internal void CompletePerOperationFlushResult()
        {
            var active = Interlocked.Decrement(ref activePerOperationFlushResults);
            Debug.Assert(active >= 0);
            SignalIfArmed(ref idleStateWaiters, ref idleStateChanged);
        }

        /// <summary>
        /// Advances published requests toward the read-only frontier. The resulting epoch callback drains
        /// those requests through the owning ring's transport.
        /// </summary>
        internal void DrainRequests()
        {
            if (ongoingAggressiveShiftReadOnly == 0 &&
                Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0)
                AggressiveShiftReadOnlyRunner(false);
        }

        void AggressiveShiftReadOnlyRunner(bool recurse)
        {
            do
            {
                if (ToShift())
                {
                    if (recurse)
                    {
                        _ = Task.Run(EpochProtectAggressiveShiftReadOnlyRunner);
                        return;
                    }

                    if (AggressiveFlushShiftReadOnlyBump())
                        return;
                }
                // Release the flusher role with a full StoreLoad fence in one atomic operation. Without the
                // fence a concurrent producer that advanced the tail but observed the flag still held would
                // bail, while this thread could re-read a stale tail, see no work, and exit, stranding that
                // request unflushed. The fence guarantees the re-check below observes any request reserved
                // before release. The matching producer-side fence is the interlocked tail compare-and-swap.
                Interlocked.Exchange(ref ongoingAggressiveShiftReadOnly, 0);
                SignalIfArmed(ref idleStateWaiters, ref idleStateChanged);
            } while (ToShift() && ongoingAggressiveShiftReadOnly == 0 &&
                     Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0);

            bool ToShift()
            {
                // The 15-bit request-id space dwarfs the maximum outstanding requests, so the masked difference
                // is the unambiguous count of reserved-but-unflushed requests.
                LightPageAndOffset snapshot = default;
                snapshot.Value = Volatile.Read(ref tail.Value);
                return ((snapshot.RequestCount - readOnlyRequestId) & (int)LightPageAndOffset.kRequestMask) != 0;
            }

            bool AggressiveFlushShiftReadOnlyBump()
            {
                // Snapshot the tail once: its request id ends the flush range and its inline address is where
                // that range's inline bytes end. Every id in the range may still be unpublished, but each owning
                // producer holds the epoch until it publishes, so the bump callback cannot flush the range
                // before all of it is published.
                LightPageAndOffset snapshot = default;
                snapshot.Value = Volatile.Read(ref tail.Value);
                var oldRequestId = readOnlyRequestId;
                var newRequestId = snapshot.RequestCount;
                if (newRequestId == oldRequestId)
                    return false;

                readOnlyRequestId = newRequestId;
                readOnlyInlineAddress = snapshot.InlineAddress;
                epoch.BumpCurrentEpoch(() => onPagesMarkedReadOnly(oldRequestId, newRequestId));
                return true;
            }

            void EpochProtectAggressiveShiftReadOnlyRunner()
            {
                try
                {
                    epoch.Resume();
                    AggressiveShiftReadOnlyRunner(false);
                }
                finally
                {
                    epoch.Suspend();
                }
            }
        }
    }
}