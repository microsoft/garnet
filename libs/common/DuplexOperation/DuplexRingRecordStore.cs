// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;

namespace Garnet.client
{
    /// <summary>
    /// Kind of a scheduled request: its payload bytes live either inline in a ring page or out-of-line in a
    /// separately rented buffer. Stored in the dense request-id-indexed schedule entry.
    /// </summary>
    internal enum RequestKind : byte
    {
        /// <summary>
        /// The request bytes live out-of-line in a rented buffer referenced by the schedule entry.
        /// </summary>
        OutOfLine = 0x00,

        /// <summary>
        /// The request bytes are packed contiguously in a ring page at the scheduled address.
        /// </summary>
        Inline = 0x01,
    }

    internal static class DuplexRingRecordFormat
    {
        internal const int HeaderSize = sizeof(long);
        internal const int CompletionSlotSize = 64;

        /// <summary>Ring bytes an out-of-line request advances the tail by, purely for ordering/backpressure.</summary>
        internal const int OutOfLineThrottleBytes = HeaderSize;
    }

    /// <summary>
    /// Immutable geometry of a power-of-two page ring: the page size and count, plus the
    /// address ↔ (page, offset) arithmetic that keys off them. Holds no position/cursor state.
    /// </summary>
    internal readonly struct PageShape
    {
        /// <summary>Size of each page, in bytes.</summary>
        internal int PageSizeBytes { get; }

        /// <summary>Log2 of <see cref="PageSizeBytes"/>.</summary>
        internal int PageSizeBits { get; }

        /// <summary>Mask isolating the in-page byte offset (<see cref="PageSizeBytes"/> - 1).</summary>
        internal int PageSizeMask { get; }

        /// <summary>Number of physical pages in the ring.</summary>
        internal int PageCount { get; }

        internal PageShape(int pageSizeBytes, int pageCount)
        {
            PageSizeBytes = pageSizeBytes;
            PageSizeBits = System.Numerics.BitOperations.Log2((uint)pageSizeBytes);
            PageSizeMask = pageSizeBytes - 1;
            PageCount = pageCount;
        }

        /// <summary>Byte offset of <paramref name="address"/> within its page.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetOffsetInPage(long address) => address & PageSizeMask;

        /// <summary>Page index of <paramref name="address"/> (not wrapped to the physical ring page count).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetUnwrappedPageIndex(long address) => address >> PageSizeBits;

        /// <summary>Physical ring slot: the logical page wrapped to <see cref="PageCount"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetPhysicalPageIndex(long address) => (int)(GetUnwrappedPageIndex(address) & (PageCount - 1));

        /// <summary>Composes a logical address from a page index and an in-page byte offset.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long PackAddress(long page, long offset) => (page << PageSizeBits) | (uint)offset;
    }

    /// <summary>
    /// Narrow, non-generic view of the request-id-indexed schedule that the admission controller needs to
    /// drive its frontier: whether a request id has been published. Keeps the (non-generic) controller
    /// decoupled from the schedule's generic request/completion/flush context types while still owning the
    /// frontier logic itself.
    /// </summary>
    internal interface IScheduleReadIndex
    {
        /// <summary>Whether the producer owning <paramref name="requestId"/> has published its schedule entry.</summary>
        bool IsSchedulePublished(int requestId);
    }

    /// <summary>
    /// Physical request and completion storage for a duplex operation ring.
    /// </summary>
    internal sealed unsafe class DuplexRingRecordStore<TRequestContext, TCompletionContext, TFlushContext>
        : IScheduleReadIndex
        where TRequestContext : struct, IDisposable
        where TFlushContext : class
    {
        unsafe struct RingPage
        {
            internal readonly byte[] value;
            internal readonly long pointer;

            internal RingPage(int pageSize)
            {
                value = GC.AllocateArray<byte>(pageSize, true);
                pointer = (long)Unsafe.AsPointer(ref value[0]);
            }
        }

        // Keep adjacent producer-published completions on separate cache lines.
        [StructLayout(LayoutKind.Sequential, Size = DuplexRingRecordFormat.CompletionSlotSize)]
        struct CompletionSlot
        {
            internal long published;
            internal TCompletionContext completion;
        }

        // Dense request-id-indexed schedule entry. Published by the reserving producer and claimed exactly
        // once by the flusher, mirroring the <see cref="CompletionSlot"/> publish/claim protocol. The entry
        // records where the request bytes live (<see cref="address"/>) plus the request context, so the flusher
        // can walk requests in dense request-id order instead of by ring address.
        internal struct ScheduleSlot
        {
            internal long published;
            internal long address;
            internal int payloadLength;
            internal RequestKind kind;
            internal TRequestContext request;
        }

        // Upper bound on schedulable requests; mirrors the dense request-id field width enforced client-side.
        const int MaxSchedulableRequests = 1 << 15;

        readonly RingBoundedBuffer<RingPage> bufferPages;
        readonly ScheduleSlot[] scheduleArray;
        readonly TFlushContext[] flushResults;
        readonly int scheduleMask;
        readonly CompletionSlot[] completions;
        readonly int completionMask;
        int closed;
        int allFlushContextsAllocated;

        internal PageShape Shape { get; }
        internal int CompletionCapacity { get; }
        internal int ScheduleCapacity { get; }

        /// <summary>Maximum number of requests that may be outstanding (awaiting local send) at once.</summary>
        internal int MaxOutstandingRequests { get; }

        /// <summary>Largest payload that fits contiguously in one ring page and can be written inline.</summary>
        internal int MaxInlinePayloadSize => Shape.PageSizeBytes;

        /// <summary>Count of flush-result contexts allocated so far (one may be cached per schedule slot).</summary>
        internal int AllocatedFlushContextCount
        {
            get
            {
                if (Volatile.Read(ref allFlushContextsAllocated) != 0)
                    return flushResults.Length;

                var count = 0;
                for (var i = 0; i < flushResults.Length; i++)
                {
                    if (Volatile.Read(ref flushResults[i]) != null)
                        count++;
                }

                if (count == flushResults.Length)
                    Volatile.Write(ref allFlushContextsAllocated, 1);

                return count;
            }
        }

        internal DuplexRingRecordStore(
            int ringPageSizeBytes,
            int ringPageCount,
            int maxOutstandingRequests,
            int completionCapacity)
        {
            Shape = new PageShape(ringPageSizeBytes, ringPageCount);
            if (maxOutstandingRequests == 0)
                maxOutstandingRequests = Math.Clamp(
                    checked(Shape.PageCount * Shape.PageSizeBytes / DuplexRingRecordFormat.HeaderSize),
                    Shape.PageCount,
                    MaxSchedulableRequests);
            if (maxOutstandingRequests is < 1 or > MaxSchedulableRequests)
                throw new ArgumentOutOfRangeException(nameof(maxOutstandingRequests), maxOutstandingRequests,
                    $"Maximum outstanding requests must be between 1 and {MaxSchedulableRequests}.");
            MaxOutstandingRequests = maxOutstandingRequests;

            // Hold the ring pages in a single-page ring whose slots are the ring's pages, so page lookups
            // reuse the buffer's wrap-around indexer instead of a hand-written modulo.
            bufferPages = new RingBoundedBuffer<RingPage>(pageSize: ringPageCount, pageCount: 1);
            for (var i = 0; i < ringPageCount; i++)
                bufferPages[i] = new RingPage(ringPageSizeBytes);

            CompletionCapacity = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, completionCapacity));
            completionMask = CompletionCapacity - 1;
            completions = new CompletionSlot[CompletionCapacity];

            ScheduleCapacity = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)maxOutstandingRequests);
            scheduleMask = ScheduleCapacity - 1;
            scheduleArray = new ScheduleSlot[ScheduleCapacity];
            flushResults = new TFlushContext[ScheduleCapacity];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetPhysicalAddress(long logicalAddress)
        {
            var offset = (int)Shape.GetOffsetInPage(logicalAddress);
            return bufferPages[Shape.GetUnwrappedPageIndex(logicalAddress)].pointer + offset;
        }

        /// <summary>
        /// Ring bytes a command of <paramref name="payloadLength"/> advances the tail by, and whether it is
        /// written inline. An inline record packs its whole payload contiguously into one page (byte-granular,
        /// no header). An out-of-line record keeps its payload in a separately rented buffer and advances the
        /// tail by a fixed nominal amount purely so the byte-driven read-only machinery keeps flushing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetRecordSize(int payloadLength, out bool isInline)
        {
            isInline = (uint)payloadLength <= (uint)MaxInlinePayloadSize;
            return isInline ? payloadLength : DuplexRingRecordFormat.OutOfLineThrottleBytes;
        }

        internal byte[] GetPageBuffer(long page) => bufferPages[page].value;

        /// <summary>Pointer to the inline payload destination for a reserved ring <paramref name="address"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetInlineWritePointer(long address) => (byte*)GetPhysicalAddress(address);

        /// <summary>Flush-result context cached for the schedule slot of <paramref name="requestId"/>, if any.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TFlushContext GetFlushResult(int requestId) => Volatile.Read(ref flushResults[requestId & scheduleMask]);

        /// <summary>Caches a reusable flush-result context for the schedule slot of <paramref name="requestId"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFlushResult(int requestId, TFlushContext flushContext)
            => Volatile.Write(ref flushResults[requestId & scheduleMask], flushContext);

        /// <summary>
        /// Publishes a dense request-id-indexed schedule entry locating the request bytes and carrying its
        /// request context. Mirrors <see cref="RegisterCompletion"/>: the producer writes the payload fields
        /// first and publishes the <see cref="ScheduleSlot.published"/> sentinel last, so a claimer that
        /// observes the sentinel also observes a fully initialized entry (and, for inline records, the payload
        /// bytes the producer already wrote into the page).
        /// </summary>
        internal void RegisterSchedule(int requestId, long address, RequestKind kind, int payloadLength, TRequestContext request)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
            var slot = requestId & scheduleMask;
            scheduleArray[slot].address = address;
            scheduleArray[slot].kind = kind;
            scheduleArray[slot].payloadLength = payloadLength;
            scheduleArray[slot].request = request;
            Volatile.Write(ref scheduleArray[slot].published, (long)requestId + 1);

            // Only out-of-line records own a buffer that teardown must reclaim. A concurrent teardown sets
            // closed then walks the schedule, so this publish races that walk. The walk pairs its closed-store
            // with a full fence before reading published; we pair our published-store with a full fence before
            // reading closed, so (Dekker) at least one side observes the other's write and reclaims the buffer
            // exactly once. The barrier is confined to the out-of-line path, leaving the inline publish
            // hot path with just the release write.
            if (kind == RequestKind.OutOfLine)
            {
                Interlocked.MemoryBarrier();
                if (Volatile.Read(ref closed) != 0 &&
                    TryClaimSchedule(requestId, out var reclaimed) && reclaimed.kind == RequestKind.OutOfLine)
                    reclaimed.request.Dispose();
            }
        }

        /// <summary>
        /// Claims a schedule entry exactly once, mirroring <see cref="TryClaimCompletion"/>: the published
        /// sentinel is atomically flipped negative so concurrent claimers cannot both observe the same request.
        /// On success the entry is returned by value and the stored request context is cleared so the store no
        /// longer references it (ownership transfers to the caller).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryClaimSchedule(int requestId, out ScheduleSlot schedule)
        {
            var slot = requestId & scheduleMask;
            var expected = (long)requestId + 1;
            if (Volatile.Read(ref scheduleArray[slot].published) == expected &&
                Interlocked.CompareExchange(ref scheduleArray[slot].published, -expected, expected) == expected)
            {
                schedule = scheduleArray[slot];
                scheduleArray[slot].request = default;
                return true;
            }

            schedule = default;
            return false;
        }

        /// <summary>
        /// Non-claiming probe: true if the schedule entry for <paramref name="requestId"/> is published (and
        /// not yet claimed). Used by the read-only frontier to avoid advancing past a reserved-but-unpublished
        /// request, which the flusher could not send and would otherwise orphan.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsSchedulePublished(int requestId)
        {
            var slot = requestId & scheduleMask;
            return Volatile.Read(ref scheduleArray[slot].published) == (long)requestId + 1;
        }

        /// <summary>
        /// Ring byte footprint of the schedule entry for <paramref name="requestId"/>: the ring
        /// <paramref name="address"/> the record occupies, returning the number of ring bytes it consumed (its
        /// inline payload length, or the out-of-line throttle stride). The caller must own the flush of this
        /// entry, which the read-only frontier only hands out after observing it published. Claiming leaves the
        /// footprint fields intact, and the slot is not reused until the flush releases its reservation, so the
        /// read is valid whether or not the entry has since been claimed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetScheduleFootprint(int requestId, out long address)
        {
            var slot = requestId & scheduleMask;
            // Acquire read orders the field reads after the producer's release publish.
            var published = Volatile.Read(ref scheduleArray[slot].published);
            Debug.Assert(Math.Abs(published) == (long)requestId + 1);
            address = scheduleArray[slot].address;
            return scheduleArray[slot].kind == RequestKind.Inline
                ? scheduleArray[slot].payloadLength
                : DuplexRingRecordFormat.OutOfLineThrottleBytes;
        }

        internal void RegisterCompletion(int ticket, TCompletionContext completion)
        {
            var slot = ticket & completionMask;
            completions[slot].completion = completion;
            Volatile.Write(ref completions[slot].published, (long)ticket + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryClaimCompletion(int ticket, out TCompletionContext completion)
        {
            var slot = ticket & completionMask;
            var expected = (long)ticket + 1;
            if (Volatile.Read(ref completions[slot].published) == expected &&
                Interlocked.CompareExchange(ref completions[slot].published, -expected, expected) == expected)
            {
                completion = completions[slot].completion;
                return true;
            }

            completion = default;
            return false;
        }

        internal bool TryReadCompletion(int ticket, out TCompletionContext completion)
        {
            var slot = ticket & completionMask;
            if (Volatile.Read(ref completions[slot].published) == (long)ticket + 1)
            {
                completion = completions[slot].completion;
                return true;
            }

            completion = default;
            return false;
        }

        internal void CloseAndReclaimRequests()
        {
            if (Interlocked.Exchange(ref closed, 1) != 0)
                return;

            // Reclaim any out-of-line payload buffers still owned by unclaimed schedule entries.
            for (var slot = 0; slot < scheduleArray.Length; slot++)
            {
                var published = Volatile.Read(ref scheduleArray[slot].published);
                if (published <= 0)
                    continue;

                var requestId = (int)(published - 1);
                if (TryClaimSchedule(requestId, out var schedule) && schedule.kind == RequestKind.OutOfLine)
                    schedule.request.Dispose();
            }
        }
    }
}