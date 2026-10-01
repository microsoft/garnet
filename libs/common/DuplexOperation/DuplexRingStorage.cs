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

    /// <summary>
    /// Physical request and completion storage for a duplex operation ring.
    /// </summary>
    internal sealed unsafe class DuplexRingStorage<TRequestContext, TCompletionContext>
        where TRequestContext : struct, IDisposable
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
        [StructLayout(LayoutKind.Sequential, Size = 64)]
        struct CompletionSlot
        {
            internal long published;
            internal TCompletionContext completion;
        }

        internal const int RecordHeaderSize = sizeof(long);

        internal const int TagShift = 56;
        const long PayloadMetaMask = (1L << TagShift) - 1;
        const long TakenBit = 1L << 63;
        const int RecordAlignment = 8;

        readonly RingBoundedBuffer<RingPage> bufferPages;
        readonly TRequestContext[] requests;
        readonly CompletionSlot[] completions;
        readonly int completionMask;
        int closed;

        internal int PageCount { get; }
        internal int PageSizeBytes { get; }
        internal int PageSizeBits { get; }
        internal int PageSizeMask { get; }
        internal int CompletionCapacity { get; }
        internal int MaxInlinePayloadSize => PageSizeBytes - RecordHeaderSize;

        internal DuplexRingStorage(
            int ringPageSizeBytes,
            int ringPageCount,
            int completionCapacity)
        {
            PageCount = ringPageCount;
            PageSizeBytes = ringPageSizeBytes;
            PageSizeBits = System.Numerics.BitOperations.Log2((uint)ringPageSizeBytes);
            PageSizeMask = ringPageSizeBytes - 1;

            var ringSlotCount = ringPageCount * ringPageSizeBytes / RecordHeaderSize;
            requests = new TRequestContext[ringSlotCount];

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
            => (RecordHeaderSize + payloadLength + (RecordAlignment - 1)) & ~(RecordAlignment - 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long EncodeDescriptor(RequestKind kind, long meta)
            => ((long)(byte)kind << TagShift) | (meta & PayloadMetaMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long DecodeMeta(long word) => word & PayloadMetaMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ComputeSlot(long address)
        {
            var pageIndex = (int)((address >> PageSizeBits) & (PageCount - 1));
            var offset = (int)(address & PageSizeMask);
            return ((pageIndex * PageSizeBytes) + offset) / RecordHeaderSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetPhysicalAddress(long logicalAddress)
        {
            var offset = (int)(logicalAddress & ((1L << PageSizeBits) - 1));
            return bufferPages[logicalAddress >> PageSizeBits].pointer + offset;
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
            return isInline ? AlignedInlineRecordSize(payloadLength) : RecordHeaderSize;
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
        internal bool TryClaimRequest(long address, out RequestKind kind, out int recordSize, out int payloadLength, out long key, out TRequestContext request)
        {
            request = default;
            payloadLength = 0;
            var recPtr = (long*)GetPhysicalAddress(address);
            var word = Volatile.Read(ref *recPtr);
            var tag = (byte)((ulong)word >> TagShift);

            if (tag == (byte)RequestKind.Uninitialized)
            {
                kind = RequestKind.Uninitialized;
                recordSize = RecordHeaderSize;
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
                recordSize = RecordHeaderSize;
            }

            // Preserve metadata while claiming so a losing walker can still recover the record stride.
            if ((word & TakenBit) != 0 ||
                Interlocked.CompareExchange(ref *recPtr, word | TakenBit, word) != word)
                return false;

            if (kind == RequestKind.OutOfLine)
            {
                var slot = ComputeSlot(address);
                request = requests[slot];
                requests[slot] = default;
            }
            return true;
        }

        internal void RegisterRequest(long address, TRequestContext request)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
            var slot = ComputeSlot(address);
            requests[slot] = request;
            var ptr = (long*)GetPhysicalAddress(address);
            *ptr = EncodeDescriptor(RequestKind.OutOfLine, address);

            if (Volatile.Read(ref closed) != 0 &&
                TryClaimRequest(address, out var kind, out _, out _, out _, out var reclaimed) &&
                kind == RequestKind.OutOfLine)
                reclaimed.Dispose();
        }

        internal byte* RegisterInlineRecord(long address, int payloadLength)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
            var basePtr = GetPhysicalAddress(address);
            Volatile.Write(ref *(long*)basePtr, EncodeDescriptor(RequestKind.Inline, payloadLength));
            Interlocked.MemoryBarrier();
            return (byte*)(basePtr + RecordHeaderSize);
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

            // Live records are contiguous from offset zero. Stale bytes after the live tail may decode as
            // invalid framing, so stop when a recovered stride leaves the page.
            for (var page = 0; page < PageCount; page++)
            {
                for (var offset = 0; offset < PageSizeBytes;)
                {
                    var address = ((long)page << PageSizeBits) | (uint)offset;
                    if (TryClaimRequest(address, out var kind, out var recordSize, out _, out _, out var request) &&
                        kind == RequestKind.OutOfLine)
                        request.Dispose();

                    if (recordSize <= 0 || offset + recordSize > PageSizeBytes)
                        break;
                    offset += recordSize;
                }
            }
        }
    }
}