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
        // Free-running inline address where an inline record starts; map it to a buffer offset with
        // GetInlineBufferOffset. Unused for out-of-line records, which reserve no inline bytes.
        internal readonly long inlineDataBufferAddress;
        internal readonly ushort completionTicket;
        internal readonly ushort requestId;

        internal DuplexOperationReservation(long inlineDataBufferAddress, ushort completionTicket, ushort requestId)
        {
            this.inlineDataBufferAddress = inlineDataBufferAddress;
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
    /// after publishing. The flusher snapshots the tail and flushes <c>[readOnlyUntilRequestId, tail)</c> from an
    /// epoch-bump callback, which cannot run until every producer that reserved an id in that range has
    /// published it and released or refreshed the epoch.
    /// </para>
    /// </summary>
    internal sealed class DuplexAdmissionController : IDisposable, IThreadPoolWorkItem
    {
        // Bounded progressive backoff budget for inline-buffer backpressure before parking on a flush. Kept small
        // because the backpressure window is short (a flush completion) and the slow path is a cheap event wait;
        // SpinOnce escalates to scheduler yields within this budget so a spinning producer never starves the
        // thread that drives the flush.
        const int BackpressureSpinBudget = 16;

        readonly int inlineDataBufferCapacity;
        readonly int activeCompletionCapacity;
        readonly int requestCapacity;
        readonly LightEpoch epoch;
        readonly Action<int, int> onPagesMarkedReadOnly;
        readonly Action flushReadOnlyRequests;
        readonly Action<Exception> onFlushError;

        LightPageAndOffset tail;
        long flushedInlineAddress;
        int readOnlyUntilRequestId;
        int readOnlyFromRequestId;
        long readOnlyInlineAddress;
        long activeFlushUntilAddress;
        // Completions reserved until their reply is consumed; bounded by completionCapacity.
        int activeCompletions;
        // Request ids reserved until the flush scan passes them; bounded by requestCapacity.
        int activeRequests;
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
               Volatile.Read(ref activeCompletions) == 0 &&
               Volatile.Read(ref activeRequests) == 0 &&
               Volatile.Read(ref activeFlushCount) == 0 &&
               Volatile.Read(ref activePerOperationFlushResults) == 0 &&
               Volatile.Read(ref ongoingAggressiveShiftReadOnly) == 0;

        internal DuplexAdmissionController(
            int inlineDataBufferCapacity,
            int completionCapacity,
            int requestCapacity,
            LightEpoch epoch,
            Action<int, int> onPagesMarkedReadOnly,
            Action<Exception> onFlushError)
        {
            Debug.Assert(System.Numerics.BitOperations.IsPow2(inlineDataBufferCapacity));
            this.inlineDataBufferCapacity = inlineDataBufferCapacity;
            this.activeCompletionCapacity = completionCapacity;
            this.requestCapacity = requestCapacity;
            this.epoch = epoch;
            this.onPagesMarkedReadOnly = onPagesMarkedReadOnly;
            flushReadOnlyRequests = FlushReadOnlyRequests;
            this.onFlushError = onFlushError;

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

        /// <summary>
        /// Continuation queued by <see cref="AggressiveShiftReadOnlyRunner"/> when a flush completes with more requests
        /// to shift. It inherits the flusher role, so it re-enters the runner without the role compare-and-swap.
        /// </summary>
        void IThreadPoolWorkItem.Execute()
        {
            if (Volatile.Read(ref disposed) != 0)
                return;
            var protectedEpoch = false;
            try
            {
                epoch.Resume();
                protectedEpoch = true;
                AggressiveShiftReadOnlyRunner(false);
            }
            catch (ObjectDisposedException) when (Volatile.Read(ref disposed) != 0)
            {
            }
            catch (Exception ex)
            {
                onFlushError(ex);
            }
            finally
            {
                if (protectedEpoch)
                    epoch.Suspend();
            }
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
        internal bool TryScheduleOperation(int requestSize, bool expectsCompletion, out DuplexOperationReservation reservation, out CompletionEvent waitEvent)
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
            var requestReserved = false;
            var flushKicked = false;
            while (true)
            {
                Debug.Assert(epoch.ThisInstanceProtected());

                if (expectsCompletion && !completionReserved)
                {
                    if (!TryReserveActiveCompletion())
                    {
                        waitEvent = completionFreed;
                        ArmWaiter(ref completionFreedWaiters);
                        if (!TryReserveActiveCompletion())
                        {
                            reservation = default;
                            return false;
                        }
                    }
                    completionReserved = true;
                }

                if (!requestReserved)
                {
                    if (!TryReserveActiveRequest())
                    {
                        waitEvent = requestFreed;
                        ArmWaiter(ref requestFreedWaiters);
                        if (!TryReserveActiveRequest())
                        {
                            if (completionReserved)
                                ReleaseActiveCompletions(1);
                            reservation = default;
                            return false;
                        }
                    }
                    requestReserved = true;
                }

                if (TryReserveEntry(requestSize, expectsCompletion, out reservation))
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
                if (TryReserveEntry(requestSize, expectsCompletion, out reservation))
                    return true;

                if (completionReserved)
                    _ = Interlocked.Decrement(ref activeCompletions);
                if (requestReserved)
                    _ = Interlocked.Decrement(ref activeRequests);
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
        bool TryReserveEntry(int size, bool expectsCompletion, out DuplexOperationReservation reservation)
        {
            while (true)
            {
                LightPageAndOffset local = default;
                local.Value = Volatile.Read(ref tail.Value);
                var address = local.InlineAddress;

                if (size > 0)
                {
                    var offset = address & (inlineDataBufferCapacity - 1);
                    if (offset + size > inlineDataBufferCapacity)
                        address += inlineDataBufferCapacity - offset;

                    var used = LightPageAndOffset.InlineDistance(Volatile.Read(ref flushedInlineAddress), address + size);
                    if (used > inlineDataBufferCapacity)
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

        bool TryReserveActiveCompletion()
        {
            if (Interlocked.Increment(ref activeCompletions) <= activeCompletionCapacity)
                return true;

            _ = Interlocked.Decrement(ref activeCompletions);
            return false;
        }

        bool TryReserveActiveRequest()
        {
            if (Interlocked.Increment(ref activeRequests) <= requestCapacity)
                return true;

            _ = Interlocked.Decrement(ref activeRequests);
            return false;
        }

        void ReleaseActiveCompletions(int count)
        {
            _ = Interlocked.Add(ref activeCompletions, -count);
            SignalIfArmed(ref completionFreedWaiters, ref completionFreed);
        }

        internal void ReleaseActiveRequests(int count)
        {
            if (count <= 0)
                return;

            _ = Interlocked.Add(ref activeRequests, -count);
            SignalIfArmed(ref requestFreedWaiters, ref requestFreed);
            SignalIfArmed(ref idleStateWaiters, ref idleStateChanged);
        }

        internal void AdvanceCompletion(int consumedCount)
        {
            if (consumedCount <= 0)
                return;

            ReleaseActiveCompletions(consumedCount);
            SignalIfArmed(ref idleStateWaiters, ref idleStateChanged);
        }

        /// <summary>
        /// Arms <paramref name="waiters"/> with a full fence. Call after capturing the event generation and before
        /// re-checking the wait condition.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void ArmWaiter(ref int waiters) => _ = Interlocked.Exchange(ref waiters, 1);

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
                        // The flusher role admits only one queued continuation. The controller itself
                        // is reusable work, without a Task, delegate, or producer execution context.
                        ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: true);
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
                return ((snapshot.RequestCount - readOnlyUntilRequestId) & (int)LightPageAndOffset.kRequestMask) != 0;
            }

            bool AggressiveFlushShiftReadOnlyBump()
            {
                // Snapshot the tail once: its request id ends the flush range and its inline address is where
                // that range's inline bytes end. Every id in the range may still be unpublished, but each owning
                // producer holds the epoch until it publishes, so the bump callback cannot flush the range
                // before all of it is published.
                LightPageAndOffset snapshot = default;
                snapshot.Value = Volatile.Read(ref tail.Value);
                var oldRequestId = readOnlyUntilRequestId;
                var newRequestId = snapshot.RequestCount;
                if (newRequestId == oldRequestId)
                    return false;

                readOnlyFromRequestId = oldRequestId;
                readOnlyUntilRequestId = newRequestId;
                readOnlyInlineAddress = snapshot.InlineAddress;
                // The flusher role remains held until this range completes, so the callback's
                // snapshot cannot be overwritten while it is waiting for the epoch barrier.
                epoch.BumpCurrentEpoch(flushReadOnlyRequests);
                return true;
            }

        }

        void FlushReadOnlyRequests()
            => onPagesMarkedReadOnly(readOnlyFromRequestId, readOnlyUntilRequestId);
    }
}