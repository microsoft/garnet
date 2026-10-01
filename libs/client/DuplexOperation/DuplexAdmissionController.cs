// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

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

        readonly int pageSizeBytes;
        readonly int pageSizeBits;
        readonly int pageCount;
        readonly int completionCapacity;
        readonly long wrapDistance;
        readonly LightEpoch epoch;
        readonly Action<int, long> setPageLastOffset;
        readonly Action<long, long> onPagesMarkedReadOnly;

        PageOffset tailPageOffset;
        long flushedUntilAddress;
        long readOnlyAddress;
        long completionUntil;
        int completionReservations;
        int ongoingAggressiveShiftReadOnly;
        int disposed;

        CompletionEvent requestFreed;
        CompletionEvent completionFreed;

        internal int CompletionTail => tailPageOffset.TaskId;

        internal DuplexAdmissionController(
            int pageSizeBytes,
            int pageSizeBits,
            int pageCount,
            int completionCapacity,
            LightEpoch epoch,
            Action<int, long> setPageLastOffset,
            Action<long, long> onPagesMarkedReadOnly)
        {
            this.pageSizeBytes = pageSizeBytes;
            this.pageSizeBits = pageSizeBits;
            this.pageCount = pageCount;
            this.completionCapacity = completionCapacity;
            this.epoch = epoch;
            this.setPageLastOffset = setPageLastOffset;
            this.onPagesMarkedReadOnly = onPagesMarkedReadOnly;
            wrapDistance = PageWrapDistance << pageSizeBits;

            requestFreed.Initialize();
            completionFreed.Initialize();
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            requestFreed.Dispose();
            completionFreed.Dispose();
        }

        internal long GetTailAddress()
        {
            var local = tailPageOffset;
            if (local.Offset >= pageSizeBytes)
            {
                local.Page = (local.Page + 1) & (int)PageOffset.kPageMask;
                local.Offset = 0;
            }
            return (((long)local.Page) << pageSizeBits) | (uint)local.Offset;
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

            if (localTailPageOffset.Offset > pageSizeBytes)
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

            if (localTailPageOffset.Offset > pageSizeBytes)
            {
                var pageIndex = (localTailPageOffset.Page + 1) & (int)PageOffset.kPageMask;
                if (offset > pageSizeBytes)
                {
                    if (NeedToWait(pageIndex))
                        return -1;
                    return -2;
                }

                if (offset < pageSizeBytes)
                    setPageLastOffset(page, offset);

                DrainRequests();

                if (NeedToWait(pageIndex))
                {
                    localTailPageOffset.TaskId = completionTicket;
                    localTailPageOffset.Offset = pageSizeBytes;
                    Interlocked.Exchange(ref tailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);
                    return -1;
                }

                localTailPageOffset.Page = pageIndex;
                localTailPageOffset.Offset = size;
                tailPageOffset = localTailPageOffset;
                page++;
                offset = 0;
            }

            return (((long)page) << pageSizeBits) | (uint)offset;

            bool NeedToWait(int nextPage)
            {
                var limit = (pageCount + (int)(flushedUntilAddress >> pageSizeBits)) & (int)PageOffset.kPageMask;
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
        }

        internal void CompleteFlush(CountWrapper count)
        {
            try
            {
                if (Interlocked.Decrement(ref count.count) == 0)
                {
                    var endAddress = count.untilAddress;
                    _ = Utility.MonotonicUpdate(ref flushedUntilAddress, endAddress, wrapDistance, out _);
                    requestFreed.Set();
                    AggressiveShiftReadOnlyRunner(true);
                }
            }
            catch when (Volatile.Read(ref disposed) != 0)
            {
            }
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