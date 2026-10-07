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
    /// Payload framing of a request-lane descriptor, carried in the most-significant byte (bits [56..63])
    /// of the 8-byte descriptor word. The low 56 bits carry the per-kind metadata.
    /// </summary>
    internal enum RequestKind : byte
    {
        /// <summary>
        /// The descriptor references request bytes held in the request side-table.
        /// </summary>
        OutOfLine = 0x00,

        /// <summary>
        /// The descriptor is followed by request bytes in the same page.
        /// </summary>
        Inline = 0x01,

        /// <summary>
        /// Empty or claimed descriptor.
        /// </summary>
        Uninitialized = 0xFF,
    }

    internal static class DuplexRingRecordFormat
    {
        internal const int HeaderSize = sizeof(long);
        internal const int TagShift = 56;
        internal const int CompletionSlotSize = 64;
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
    /// Physical request and completion storage for a duplex operation ring.
    /// </summary>
    internal sealed unsafe class DuplexRingRecordStore<TRequestContext, TCompletionContext, TFlushContext>
        where TRequestContext : struct, IDisposable
        where TFlushContext : class
    {
        unsafe struct RingPage
        {
            internal readonly byte[] value;
            internal readonly long pointer;
            internal long lastOffset;

            internal RingPage(int pageSize)
            {
                value = GC.AllocateArray<byte>(pageSize, true);
                // Address zero is a valid descriptor, so empty storage uses the all-ones sentinel.
                value.AsSpan().Fill(0xFF);
                pointer = (long)Unsafe.AsPointer(ref value[0]);
                lastOffset = 0;
            }
        }

        // Keep adjacent producer-published completions on separate cache lines.
        [StructLayout(LayoutKind.Sequential, Size = DuplexRingRecordFormat.CompletionSlotSize)]
        struct CompletionSlot
        {
            internal long published;
            internal TCompletionContext completion;
        }

        struct RequestSlot
        {
            internal TRequestContext request;
            internal TFlushContext flushContext;
        }

        const long PayloadMetaMask = (1L << DuplexRingRecordFormat.TagShift) - 1;
        const long TakenBit = 1L << 63;
        const int RecordAlignment = 8;

        readonly RingBoundedBuffer<RingPage> bufferPages;
        readonly RequestSlot[] requestSlots;
        readonly CompletionSlot[] completions;
        readonly int completionMask;
        int closed;
        int allFlushContextsAllocated;

        internal PageShape Shape { get; }
        internal int CompletionCapacity { get; }
        internal int MaxInlinePayloadSize => Shape.PageSizeBytes - DuplexRingRecordFormat.HeaderSize;
        internal int AllocatedFlushContextCount
        {
            get
            {
                if (Volatile.Read(ref allFlushContextsAllocated) != 0)
                    return requestSlots.Length;

                var count = 0;
                for (var i = 0; i < requestSlots.Length; i++)
                {
                    if (Volatile.Read(ref requestSlots[i].flushContext) != null)
                        count++;
                }

                if (count == requestSlots.Length)
                    Volatile.Write(ref allFlushContextsAllocated, 1);

                return count;
            }
        }

        internal DuplexRingRecordStore(
            int ringPageSizeBytes,
            int ringPageCount,
            int completionCapacity)
        {
            Shape = new PageShape(ringPageSizeBytes, ringPageCount);

            var ringSlotCount = ringPageCount * ringPageSizeBytes / DuplexRingRecordFormat.HeaderSize;
            requestSlots = new RequestSlot[ringSlotCount];

            // Hold the per-page records in a single-page ring whose slots are the ring's pages, so page lookups
            // reuse the buffer's wrap-around indexer instead of a hand-written modulo.
            bufferPages = new RingBoundedBuffer<RingPage>(pageSize: ringPageCount, pageCount: 1);
            for (var i = 0; i < ringPageCount; i++)
                bufferPages[i] = new RingPage(ringPageSizeBytes);

            CompletionCapacity = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, completionCapacity));
            completionMask = CompletionCapacity - 1;
            completions = new CompletionSlot[CompletionCapacity];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int AlignedInlineRecordSize(int payloadLength)
            => (DuplexRingRecordFormat.HeaderSize + payloadLength + (RecordAlignment - 1)) & ~(RecordAlignment - 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long EncodeDescriptor(RequestKind kind, long meta)
            => ((long)(byte)kind << DuplexRingRecordFormat.TagShift) | (meta & PayloadMetaMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long DecodeMeta(long word) => word & PayloadMetaMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ComputeSlot(long address)
        {
            var pageIndex = Shape.GetPhysicalPageIndex(address);
            var offset = (int)Shape.GetOffsetInPage(address);
            return ((pageIndex * Shape.PageSizeBytes) + offset) / DuplexRingRecordFormat.HeaderSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetPhysicalAddress(long logicalAddress)
        {
            var offset = (int)Shape.GetOffsetInPage(logicalAddress);
            return bufferPages[Shape.GetUnwrappedPageIndex(logicalAddress)].pointer + offset;
        }

        /// <summary>
        /// Ring bytes a command of <paramref name="payloadLength"/> reserves, and whether it is written inline.
        /// An inline record packs its whole payload into one page after an 8-byte header; otherwise the record
        /// is just the 8-byte out-of-line descriptor pointing at a separately rented payload buffer.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetRecordSize(int payloadLength, out bool isInline)
        {
            isInline = (uint)payloadLength <= (uint)MaxInlinePayloadSize;
            return isInline ? AlignedInlineRecordSize(payloadLength) : DuplexRingRecordFormat.HeaderSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetPageLastOffset(int page, long offset)
        {
            Debug.Assert(bufferPages[page].lastOffset == 0);
            bufferPages[page].lastOffset = offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ConsumePageEndOffset(long page, long endOffset)
        {
            ref var ringPage = ref bufferPages[page];
            if (ringPage.lastOffset <= 0 || endOffset <= ringPage.lastOffset)
                return endOffset;

            var realEndOffset = ringPage.lastOffset;
            ringPage.lastOffset = 0;
            return realEndOffset;
        }

        internal byte[] GetPageBuffer(long page) => bufferPages[page].value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryClaimRequest(long address, out RequestKind kind, out int recordSize, out int payloadLength, out long key, out TRequestContext request, out TFlushContext flushContext)
        {
            request = default;
            flushContext = default;
            payloadLength = 0;
            var recPtr = (long*)GetPhysicalAddress(address);
            var word = Volatile.Read(ref *recPtr);
            var tag = (byte)((ulong)word >> DuplexRingRecordFormat.TagShift);

            if (tag == (byte)RequestKind.Uninitialized)
            {
                kind = RequestKind.Uninitialized;
                recordSize = DuplexRingRecordFormat.HeaderSize;
                key = default;
                return false;
            }

            key = DecodeMeta(word);
            if ((tag & (byte)RequestKind.Inline) != 0)
            {
                kind = RequestKind.Inline;
                payloadLength = (int)key;
                recordSize = AlignedInlineRecordSize(payloadLength);
            }
            else
            {
                kind = RequestKind.OutOfLine;
                recordSize = DuplexRingRecordFormat.HeaderSize;
            }

            // Preserve metadata while claiming so a losing walker can still recover the record stride.
            if ((word & TakenBit) != 0 ||
                Interlocked.CompareExchange(ref *recPtr, word | TakenBit, word) != word)
                return false;

            var slot = ComputeSlot(address);
            request = requestSlots[slot].request;
            requestSlots[slot].request = default;
            flushContext = requestSlots[slot].flushContext;
            return true;
        }

        internal void RegisterRequest(long address, TRequestContext request)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
            var slot = ComputeSlot(address);
            requestSlots[slot].request = request;
            var ptr = (long*)GetPhysicalAddress(address);
            *ptr = EncodeDescriptor(RequestKind.OutOfLine, address);

            if (Volatile.Read(ref closed) != 0 &&
                TryClaimRequest(address, out var kind, out _, out _, out _, out var reclaimed, out _) &&
                kind == RequestKind.OutOfLine)
                reclaimed.Dispose();
        }

        internal byte* RegisterInlineRecord(long address, int payloadLength, TRequestContext request)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
            requestSlots[ComputeSlot(address)].request = request;
            var basePtr = GetPhysicalAddress(address);
            Volatile.Write(ref *(long*)basePtr, EncodeDescriptor(RequestKind.Inline, payloadLength));
            Interlocked.MemoryBarrier();
            return (byte*)(basePtr + DuplexRingRecordFormat.HeaderSize);
        }

        internal void SetFlushContext(long address, TFlushContext flushContext)
            => Volatile.Write(ref requestSlots[ComputeSlot(address)].flushContext, flushContext);

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

            // Live records are contiguous from offset zero. Stale bytes after the live tail may decode as
            // invalid framing, so stop when a recovered stride leaves the page.
            for (var page = 0; page < Shape.PageCount; page++)
            {
                for (var offset = 0; offset < Shape.PageSizeBytes;)
                {
                    var address = Shape.PackAddress(page, offset);
                    if (TryClaimRequest(address, out var kind, out var recordSize, out _, out _, out var request, out _) &&
                        kind == RequestKind.OutOfLine)
                        request.Dispose();

                    if (recordSize <= 0 || offset + recordSize > Shape.PageSizeBytes)
                        break;
                    offset += recordSize;
                }
            }
        }
    }
}