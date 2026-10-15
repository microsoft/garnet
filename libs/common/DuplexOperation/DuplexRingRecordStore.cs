// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;

namespace Garnet.client
{
    /// <summary>
    /// Kind of a scheduled request: its payload bytes live either inline in the inline buffer or out-of-line in a
    /// separately rented buffer. Stored in the dense request-id-indexed schedule entry.
    /// </summary>
    internal enum RequestKind : byte
    {
        /// <summary>
        /// The request bytes live out-of-line in a rented buffer referenced by the schedule entry.
        /// </summary>
        OutOfLine = 0x00,

        /// <summary>
        /// The request bytes are packed contiguously in the inline buffer at the scheduled address.
        /// </summary>
        Inline = 0x01,
    }

    internal static class DuplexRingRecordFormat
    {
        internal const int HeaderSize = sizeof(long);
        internal const int CompletionSlotSize = 64;

        /// <summary>Largest supported inline-buffer capacity, in bytes.</summary>
        internal const int MaxInlineBufferCapacity = 1 << 30;
    }

    /// <summary>
    /// Physical request and completion storage for a duplex operation ring.
    /// </summary>
    internal sealed unsafe class DuplexRingRecordStore<TRequestContext, TCompletionContext, TFlushContext>
        where TRequestContext : struct, IDisposable
        where TFlushContext : class
    {
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

        readonly RingBoundedBuffer<byte> inlineDataBuffer;
        readonly ScheduleSlot[] scheduleArray;
        readonly TFlushContext[] flushResults;
        readonly int scheduleMask;
        readonly CompletionSlot[] completions;
        readonly int completionMask;
        int closed;
        int allFlushContextsAllocated;

        /// <summary>Size of the circular inline payload buffer, in bytes (a power of two).</summary>
        internal int InlineBufferCapacity { get; }
        internal int CompletionCapacity { get; }
        internal int ScheduleCapacity { get; }

        /// <summary>Maximum number of requests that may be outstanding (awaiting local send) at once.</summary>
        internal int MaxOutstandingRequests { get; }

        /// <summary>
        /// Largest payload written inline. Bounded by half the inline buffer so a record that does not fit
        /// before the buffer end can always be placed at the start of the next lap once earlier records flush.
        /// </summary>
        internal int MaxInlinePayloadSize { get; }

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
            if ((long)ringPageSizeBytes * ringPageCount > DuplexRingRecordFormat.MaxInlineBufferCapacity)
                throw new ArgumentOutOfRangeException(nameof(ringPageCount), ringPageCount,
                    $"Inline buffer capacity must not exceed {DuplexRingRecordFormat.MaxInlineBufferCapacity} bytes.");

            // Pinned because producers write inline payloads through raw pointers obtained under the epoch.
            inlineDataBuffer = new RingBoundedBuffer<byte>(ringPageSizeBytes, ringPageCount, pinned: true);
            InlineBufferCapacity = inlineDataBuffer.Capacity;
            MaxInlinePayloadSize = Math.Min(ringPageSizeBytes, InlineBufferCapacity / 2);
            if (maxOutstandingRequests == 0)
                maxOutstandingRequests = Math.Clamp(
                    InlineBufferCapacity / DuplexRingRecordFormat.HeaderSize,
                    ringPageCount,
                    MaxSchedulableRequests);
            if (maxOutstandingRequests is < 1 or > MaxSchedulableRequests)
                throw new ArgumentOutOfRangeException(nameof(maxOutstandingRequests), maxOutstandingRequests,
                    $"Maximum outstanding requests must be between 1 and {MaxSchedulableRequests}.");
            MaxOutstandingRequests = maxOutstandingRequests;

            CompletionCapacity = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)Math.Max(1, completionCapacity));
            completionMask = CompletionCapacity - 1;
            completions = new CompletionSlot[CompletionCapacity];

            ScheduleCapacity = (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)maxOutstandingRequests);
            scheduleMask = ScheduleCapacity - 1;
            scheduleArray = new ScheduleSlot[ScheduleCapacity];
            flushResults = new TFlushContext[ScheduleCapacity];
        }

        /// <summary>
        /// Inline-buffer bytes a command of <paramref name="payloadLength"/> reserves, and whether it is written
        /// inline. An inline record packs its whole payload contiguously into the inline buffer (byte-granular,
        /// no header). An out-of-line record keeps its payload in a separately rented buffer and reserves no
        /// inline bytes; it is throttled by the schedule capacity alone.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetRecordSize(int payloadLength, out bool isInline)
        {
            isInline = (uint)payloadLength <= (uint)MaxInlinePayloadSize;
            return isInline ? payloadLength : 0;
        }

        /// <summary>The pinned inline payload buffer.</summary>
        internal byte[] InlineBuffer => inlineDataBuffer.Array;

        /// <summary>Offset in <see cref="InlineBuffer"/> of the free-running inline <paramref name="address"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetInlineBufferOffset(long address) => inlineDataBuffer.GetOffset(address);

        /// <summary>Pointer to the inline payload destination for a reserved inline <paramref name="address"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetInlineWritePointer(long address) => (byte*)Unsafe.AsPointer(ref inlineDataBuffer[address]);

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