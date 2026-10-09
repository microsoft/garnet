// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.client
{
    /// <summary>
    /// A paired reservation for one duplex operation. The request address and optional completion ticket
    /// are issued by one atomic allocator advance and therefore retain the same relative ordering.
    /// </summary>
    internal readonly struct DuplexOperationReservation
    {
        internal readonly long requestAddress;
        internal readonly int completionTicket;

        internal DuplexOperationReservation(long requestAddress, int completionTicket)
        {
            this.requestAddress = requestAddress;
            this.completionTicket = completionTicket;
        }
    }

    /// <summary>
    /// Coordinates admission and backpressure for paired request/completion operations. Request capacity is
    /// released by network flushes, while completion capacity is released independently by received replies.
    /// </summary>
    internal sealed class DuplexAdmissionController : IDisposable
    {
        const long PageWrapDistance = 1L << (PageOffset.kPageBits - 1);

        readonly PageShape shape;
        readonly int completionCapacity;
        readonly long wrapDistance;
        readonly LightEpoch epoch;
        readonly Action<int, long> setPageLastOffset;
        readonly Action<long, long> onPagesMarkedReadOnly;

        PageOffset tailPageOffset;
        long flushedUntilAddress;
        long readOnlyAddress;
        long completionUntil;
        long activeFlushUntilAddress;
        int completionReservations;
        int ongoingAggressiveShiftReadOnly;
        int activeFlushCount;
        int activePerOperationFlushResults;
        int disposed;

        CompletionEvent requestFreed;
        CompletionEvent completionFreed;
        CompletionEvent idleStateChanged;

        internal int CompletionTail => tailPageOffset.TaskId;

        internal int ActivePerOperationFlushResultCount => Volatile.Read(ref activePerOperationFlushResults);

        internal bool IsIdle
            => GetTailAddress() == Volatile.Read(ref flushedUntilAddress) &&
               Volatile.Read(ref completionReservations) == 0 &&
               Volatile.Read(ref activeFlushCount) == 0 &&
               Volatile.Read(ref activePerOperationFlushResults) == 0 &&
               Volatile.Read(ref ongoingAggressiveShiftReadOnly) == 0;

        internal DuplexAdmissionController(
            PageShape shape,
            int completionCapacity,
            LightEpoch epoch,
            Action<int, long> setPageLastOffset,
            Action<long, long> onPagesMarkedReadOnly)
        {
            this.shape = shape;
            this.completionCapacity = completionCapacity;
            this.epoch = epoch;
            this.setPageLastOffset = setPageLastOffset;
            this.onPagesMarkedReadOnly = onPagesMarkedReadOnly;
            wrapDistance = PageWrapDistance << shape.PageSizeBits;

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
                if (IsIdle)
                    return;

                await stateChanged.WaitAsync(token).ConfigureAwait(false);
            }
        }

        internal long GetTailAddress()
        {
            var local = tailPageOffset;
            if (local.Offset >= shape.PageSizeBytes)
            {
                local.Page = (local.Page + 1) & (int)PageOffset.kPageMask;
                local.Offset = 0;
            }
            return (((long)local.Page) << shape.PageSizeBits) | (uint)local.Offset;
        }

        /// <summary>
        /// Attempts to reserve the ordered request address and optional completion ticket for one operation.
        /// On backpressure, returns false and identifies the capacity event that the caller should await.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryScheduleOperation(
            int requestSize,
            bool expectsCompletion,
            out DuplexOperationReservation reservation,
            out CompletionEvent waitEvent)
        {
            const int flushSpinCount = 10;
            var spins = 0;
            var completionReserved = false;
            while (true)
            {
                Debug.Assert(epoch.ThisInstanceProtected());

                if (expectsCompletion && !completionReserved)
                {
                    waitEvent = completionFreed;
                    if (!TryReserveCompletion())
                    {
                        reservation = default;
                        return false;
                    }
                    completionReserved = true;
                }

                waitEvent = requestFreed;
                var requestAddress = TryReserveRequest(requestSize, out var completionTicket, expectsCompletion);
                if (requestAddress >= 0)
                {
                    reservation = new DuplexOperationReservation(requestAddress, completionTicket);
                    return true;
                }

                if (requestAddress == -1)
                {
                    if (spins++ < flushSpinCount)
                    {
                        Thread.Yield();
                        continue;
                    }

                    if (completionReserved)
                        ReleaseCompletions(1);
                    reservation = default;
                    return false;
                }

                epoch.ProtectAndDrain();
                Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long TryReserveRequest(int size, out int completionTicket, bool expectsCompletion)
        {
            PageOffset localTailPageOffset = default;
            localTailPageOffset.PageAndOffset = tailPageOffset.PageAndOffset;

            if (localTailPageOffset.Offset > shape.PageSizeBytes)
            {
                completionTicket = 0;
                if (NeedToWait(localTailPageOffset.Page + 1))
                    return -1;
                return -2;
            }

            localTailPageOffset.PageAndOffset = expectsCompletion
                ? Interlocked.Add(ref tailPageOffset.PageAndOffset, size + (1L << PageOffset.kTaskOffset))
                : Interlocked.Add(ref tailPageOffset.PageAndOffset, size);

            completionTicket = localTailPageOffset.PrevTaskId;
            var page = localTailPageOffset.Page;
            var offset = localTailPageOffset.Offset - size;

            if (localTailPageOffset.Offset > shape.PageSizeBytes)
            {
                var pageIndex = (localTailPageOffset.Page + 1) & (int)PageOffset.kPageMask;
                if (offset > shape.PageSizeBytes)
                {
                    if (NeedToWait(pageIndex))
                        return -1;
                    return -2;
                }

                if (offset < shape.PageSizeBytes)
                    setPageLastOffset(page, offset);

                DrainRequests();

                if (NeedToWait(pageIndex))
                {
                    localTailPageOffset.TaskId = completionTicket;
                    localTailPageOffset.Offset = shape.PageSizeBytes;
                    Interlocked.Exchange(ref tailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);
                    return -1;
                }

                localTailPageOffset.Page = pageIndex;
                localTailPageOffset.Offset = size;
                tailPageOffset = localTailPageOffset;
                page = pageIndex;
                offset = 0;
            }

            return (((long)page) << shape.PageSizeBits) | (uint)offset;

            bool NeedToWait(int nextPage)
            {
                var limit = (shape.PageCount + (int)shape.GetUnwrappedPageIndex(Volatile.Read(ref flushedUntilAddress))) & (int)PageOffset.kPageMask;
                return nextPage >= limit && nextPage - limit < PageWrapDistance;
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
            completionFreed.Set();
        }

        internal void AdvanceCompletion(int consumedCount)
        {
            if (consumedCount <= 0)
                return;

            Volatile.Write(ref completionUntil, Volatile.Read(ref completionUntil) + consumedCount);
            ReleaseCompletions(consumedCount);
            idleStateChanged.Set();
        }

        internal void BeginFlushRange(long untilAddress)
        {
            Debug.Assert(Volatile.Read(ref activeFlushCount) == 0);
            activeFlushUntilAddress = untilAddress;
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
                    _ = Utility.MonotonicUpdate(ref flushedUntilAddress, activeFlushUntilAddress, wrapDistance, out _);
                    requestFreed.Set();
                    idleStateChanged.Set();
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
            idleStateChanged.Set();
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
                ongoingAggressiveShiftReadOnly = 0;
                idleStateChanged.Set();
            } while (ToShift() && ongoingAggressiveShiftReadOnly == 0 &&
                     Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0);

            bool ToShift()
            {
                var tailAddress = GetTailAddress();
                return tailAddress > readOnlyAddress || readOnlyAddress - tailAddress > wrapDistance;
            }

            bool AggressiveFlushShiftReadOnlyBump()
            {
                var newReadOnlyAddress = GetTailAddress();
                if (!Utility.MonotonicUpdate(ref readOnlyAddress, newReadOnlyAddress, wrapDistance, out var oldReadOnlyAddress))
                    return false;

                epoch.BumpCurrentEpoch(() => onPagesMarkedReadOnly(oldReadOnlyAddress, newReadOnlyAddress));
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