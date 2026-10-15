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
    /// A paired reservation for one duplex operation. The request address, dense request id, and optional
    /// completion ticket are issued by one atomic allocator advance and therefore retain the same relative
    /// ordering. The <see cref="requestId"/> counts every scheduled operation (including fire-and-forget ones
    /// that carry no completion ticket) and indexes the dense request-side schedule table.
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
    /// </summary>
    internal sealed class DuplexAdmissionController : IDisposable
    {
        const long PageWrapDistance = 1L << (LightPageAndOffset.kPageBits - 1);

        // Bounded progressive backoff budget for byte-ring backpressure before parking on a flush. Kept small
        // because the backpressure window is short (a straddle normalization or a flush completion) and the
        // slow path is a cheap event wait; SpinOnce escalates to scheduler yields within this budget so a
        // spinning producer never starves the thread that drives the flush.
        const int BackpressureSpinBudget = 16;

        readonly PageShape shape;
        readonly int completionCapacity;
        readonly int scheduleCapacity;
        readonly LightEpoch epoch;
        readonly Action<int, int> onPagesMarkedReadOnly;
        readonly IScheduleReadIndex schedule;

        LightPageAndOffset tailPageOffset;
        long flushedUntilAddress;
        int readOnlyRequestId;
        long completionUntil;
        long activeFlushUntilAddress;
        int completionReservations;
        int scheduleReservations;
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
               Volatile.Read(ref scheduleReservations) == 0 &&
               Volatile.Read(ref activeFlushCount) == 0 &&
               Volatile.Read(ref activePerOperationFlushResults) == 0 &&
               Volatile.Read(ref ongoingAggressiveShiftReadOnly) == 0;

        internal DuplexAdmissionController(
            PageShape shape,
            int completionCapacity,
            int scheduleCapacity,
            LightEpoch epoch,
            Action<int, int> onPagesMarkedReadOnly,
            IScheduleReadIndex schedule)
        {
            this.shape = shape;
            this.completionCapacity = completionCapacity;
            this.scheduleCapacity = scheduleCapacity;
            this.epoch = epoch;
            this.onPagesMarkedReadOnly = onPagesMarkedReadOnly;
            this.schedule = schedule;

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
            LightPageAndOffset local = default;
            local.PageAndOffset = tailPageOffset.PageAndOffset;
            return GetTailAddressFrom(local);
        }

        long GetTailAddressFrom(LightPageAndOffset local)
        {
            if (local.Offset >= shape.PageSizeBytes)
            {
                local.Page = (local.Page + 1) & (int)LightPageAndOffset.kPageMask;
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
            // Byte-ring backpressure (the schedule claim is already held) clears within a few straddle
            // normalizations or flush completions. Kick the flusher once, then spin that window with a bounded
            // progressive policy: it begins CPU-local but escalates to scheduler yields so a spinning producer
            // can never starve the thread driving the flush. The spin body does no CompletionEvent.Set() work:
            // each Set() allocates a SemaphoreSlim and takes its internal monitor lock, so re-driving the flush
            // on every iteration convoys every producer on that lock and starves the flusher (the original
            // livelock). On exhaustion, release the schedule claim and let the caller park on the next flush,
            // which is signalled by CompleteFlushPart advancing flushedUntilAddress.
            var spinner = ConfiguredSpinWait.BoundedProgressive(BackpressureSpinBudget);
            var completionReserved = false;
            var scheduleReserved = false;
            var flushKicked = false;
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

                if (!scheduleReserved)
                {
                    waitEvent = requestFreed;
                    if (!TryReserveSchedule())
                    {
                        if (completionReserved)
                            ReleaseCompletions(1);
                        reservation = default;
                        return false;
                    }
                    scheduleReserved = true;
                }

                waitEvent = requestFreed;
                var requestAddress = TryReserveRequest(requestSize, out var completionTicket, out var requestId, expectsCompletion);
                if (requestAddress >= 0)
                {
                    reservation = new DuplexOperationReservation(requestAddress, completionTicket, requestId);
                    return true;
                }

                if (!flushKicked)
                {
                    // Kick the flusher once so a read-only bump is initiated for the published prefix. Repeating
                    // DrainRequests every iteration is what convoyed all producers on CompletionEvent.Set's
                    // SemaphoreSlim monitor lock, so it is done exactly once per backpressure episode.
                    DrainRequests();
                    flushKicked = true;
                }

                // Drain the epoch every iteration. Producers hold the epoch while spinning here; the flush
                // advances flushedUntilAddress through epoch.BumpCurrentEpoch(onPagesMarkedReadOnly), whose
                // epoch transition cannot complete until every other holder drains or suspends. Without this
                // drain the bump stalls, the flushed watermark never advances, the straddled tail never
                // unparks, and the spin never terminates. ProtectAndDrain performs no Set() work unless a
                // flush actually completes, so it does not reintroduce the convoy.
                epoch.ProtectAndDrain();

                if (spinner.TryWait())
                    continue;

                if (completionReserved)
                    ReleaseCompletionsQuiet(1);
                if (scheduleReserved)
                    ReleaseScheduleReservationsQuiet(1);
                reservation = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long TryReserveRequest(int size, out ushort completionTicket, out ushort requestId, bool expectsCompletion)
        {
            LightPageAndOffset localTailPageOffset = default;
            localTailPageOffset.PageAndOffset = tailPageOffset.PageAndOffset;

            if (localTailPageOffset.Offset > shape.PageSizeBytes)
            {
                completionTicket = 0;
                requestId = 0;
                if (NeedToWait(localTailPageOffset.Page + 1))
                    return -1;
                return -2;
            }

            localTailPageOffset.PageAndOffset = expectsCompletion
                ? Interlocked.Add(ref tailPageOffset.PageAndOffset, size + LightPageAndOffset.kTaskIncrement + LightPageAndOffset.kRequestIncrement)
                : Interlocked.Add(ref tailPageOffset.PageAndOffset, size + LightPageAndOffset.kRequestIncrement);

            completionTicket = (ushort)localTailPageOffset.PrevTaskId;
            var prevRequestCount = (ushort)localTailPageOffset.PrevRequestCount;
            requestId = prevRequestCount;
            var page = localTailPageOffset.Page;
            var offset = localTailPageOffset.Offset - size;

            if (localTailPageOffset.Offset > shape.PageSizeBytes)
            {
                var pageIndex = (localTailPageOffset.Page + 1) & (int)LightPageAndOffset.kPageMask;
                if (offset > shape.PageSizeBytes)
                {
                    if (NeedToWait(pageIndex))
                        return -1;
                    return -2;
                }

                DrainRequests();

                if (NeedToWait(pageIndex))
                {
                    localTailPageOffset.TaskId = completionTicket;
                    localTailPageOffset.RequestCount = prevRequestCount;
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
                var limit = (shape.PageCount + (int)shape.GetUnwrappedPageIndex(Volatile.Read(ref flushedUntilAddress))) & (int)LightPageAndOffset.kPageMask;
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
            requestFreed.Set();
            idleStateChanged.Set();
        }

        // Give back reservations taken but not used by an attempt that bailed on byte-ring backpressure, without
        // signalling. The attempt is about to park on requestFreed; the flush that frees ring bytes (always
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

            Volatile.Write(ref completionUntil, Volatile.Read(ref completionUntil) + consumedCount);
            ReleaseCompletions(consumedCount);
            idleStateChanged.Set();
        }

        /// <summary>
        /// Opens a flush of a request-id range whose last record occupies <paramref name="lastConsumedBytes"/>
        /// ring bytes at <paramref name="lastAddress"/>. Request ids are assigned in ring byte order, so the end
        /// of the last record is the end of the whole range; it becomes the flushed watermark once every part of
        /// the range completes.
        /// </summary>
        internal void BeginFlushRange(long lastAddress, int lastConsumedBytes)
        {
            Debug.Assert(Volatile.Read(ref activeFlushCount) == 0);
            activeFlushUntilAddress = ByteEndAddress(lastAddress, lastConsumedBytes);
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
                    // Flushes are serialized (BeginFlushRange asserts no overlap), so the flushed watermark only
                    // moves forward; a plain store avoids the wrapped-address half-space compare that a full ring
                    // can hit exactly (see AggressiveFlushShiftReadOnlyBump).
                    Volatile.Write(ref flushedUntilAddress, activeFlushUntilAddress);
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
                // Release the flusher role with a full StoreLoad fence in one atomic operation. Without the
                // fence a concurrent producer that advanced the tail but observed the flag still held would
                // bail, while this thread could re-read a stale tail, see no work, and exit, stranding that
                // request unflushed. The fence guarantees the re-check below observes any request published
                // before release. The matching producer-side fence is the interlocked tail publication in the
                // allocate path, so both halves of the handshake are ordered.
                Interlocked.Exchange(ref ongoingAggressiveShiftReadOnly, 0);
                idleStateChanged.Set();
            } while (ToFlush() && ongoingAggressiveShiftReadOnly == 0 &&
                     Interlocked.CompareExchange(ref ongoingAggressiveShiftReadOnly, 1, 0) == 0);

            bool ToShift()
            {
                // Detect pending flush work in the request-id space, not the byte space. The 3-bit page field
                // gives only an 8-page wrapped byte address, whose half-space wrap distance a full ring
                // (pageCount pages) can reach exactly, making a wrapped-byte "is the tail ahead" compare
                // ambiguous and able to falsely report no work (wedging the frontier with producers parked
                // forever). The 15-bit request-id space dwarfs the maximum outstanding requests, so the masked
                // difference is the unambiguous count of reserved-but-unflushed requests.
                LightPageAndOffset snapshot = default;
                snapshot.PageAndOffset = Volatile.Read(ref tailPageOffset.PageAndOffset);
                return ((snapshot.RequestCount - readOnlyRequestId) & (int)LightPageAndOffset.kRequestMask) != 0;
            }

            // The tail is ahead AND the next request to flush has actually been published. The publication
            // probe is what makes the re-check safe without spinning: when a producer reserved a request id
            // (advancing the tail) but has not yet published it, ToShift is true yet there is nothing to flush.
            // Re-reading the published sentinel here, after the StoreLoad fence above, either observes that
            // producer's release publish (so this thread flushes it) or does not (so the producer's own
            // post-publish DrainRequests will), closing the same handshake the fence protects for the tail.
            bool ToFlush() => ToShift() && schedule.IsSchedulePublished(readOnlyRequestId);

            bool AggressiveFlushShiftReadOnlyBump()
            {
                // Snapshot the packed tail once; the frontier advances in the request-id space only.
                LightPageAndOffset snapshot = default;
                snapshot.PageAndOffset = Volatile.Read(ref tailPageOffset.PageAndOffset);
                var tailRequestId = snapshot.RequestCount;
                var oldRequestId = readOnlyRequestId;

                // Gate the advance on publication: a request id is reserved (and bumps the tail) before its
                // payload is published, and publication can lag across producers or across the straddle path's
                // own internal DrainRequests. Advancing the frontier to the raw tail would let the flusher walk
                // past a reserved-but-unpublished request, skip it, and then reuse its slot — orphaning the
                // request (leaking its out-of-line buffer and never sending it). The gate stops the frontier at
                // the first unpublished request. The flush derives the range's ring byte end from its slots.
                var gatedRequestId = GateFrontierOnPublication(oldRequestId, tailRequestId);
                if (gatedRequestId == oldRequestId)
                    return false; // Next request not yet published; its producer's publish will re-drive this.

                readOnlyRequestId = gatedRequestId;
                epoch.BumpCurrentEpoch(() => onPagesMarkedReadOnly(oldRequestId, gatedRequestId));
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

        /// <summary>
        /// Walks the half-open request-id range <c>[fromRequestId, untilRequestId)</c> in order and returns the
        /// first request id whose owning producer has not yet published its schedule entry (equal to
        /// <paramref name="untilRequestId"/> when all are published). The frontier must not advance past an
        /// unpublished request: the flusher cannot send it, would skip its slot, and the slot would later be
        /// reused by a wrapped request id — orphaning the request (leaking any out-of-line buffer and never
        /// transmitting it).
        /// </summary>
        int GateFrontierOnPublication(int fromRequestId, int untilRequestId)
        {
            var count = (untilRequestId - fromRequestId) & (int)LightPageAndOffset.kRequestMask;
            var published = 0;
            while (published < count &&
                   schedule.IsSchedulePublished((fromRequestId + published) & (int)LightPageAndOffset.kRequestMask))
                published++;

            return (fromRequestId + published) & (int)LightPageAndOffset.kRequestMask;
        }

        /// <summary>
        /// Ring byte address immediately after a record that occupies <paramref name="consumedBytes"/> starting
        /// at <paramref name="address"/>, normalized so a record that fills a page rolls to the next logical
        /// page (matching the tail's page-advance and wrapping at the logical page count).
        /// </summary>
        long ByteEndAddress(long address, int consumedBytes)
        {
            var raw = address + consumedBytes;
            var page = (raw >> shape.PageSizeBits) & (int)LightPageAndOffset.kPageMask;
            var offset = raw & shape.PageSizeMask;
            return (page << shape.PageSizeBits) | offset;
        }
    }
}