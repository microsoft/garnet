// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Garnet.client
{
    /// <summary>
    /// Free-running allocator cursor for the duplex request lane. A single 64-bit word packs three logical
    /// fields, high to low: the inline-buffer address (<see cref="InlineAddress"/>), the completion
    /// <see cref="TaskId"/> ticket, and the <see cref="RequestCount"/> ticket that counts every scheduled
    /// request (including fire-and-forget operations that carry no completion ticket).
    /// <para>
    /// The reserve path advances the word with a compare-and-swap that checks every capacity limit before it
    /// commits, so a producer either receives a mutually consistent (inline address, completion ticket,
    /// request id) tuple or changes nothing. Each field is updated through its own mask, so no field ever
    /// carries into another and each wraps independently at its own width. The request id alone drives
    /// scheduling and flushing; the inline address only locates inline payload bytes and throttles reuse of
    /// the inline buffer.
    /// </para>
    /// </summary>
    struct PageAndOffset
    {
        internal const int kRequestBits = DuplexRingRecordFormat.RequestIdBits;
        const int kRequestOffset = 0;
        internal const long kRequestMask = (1L << kRequestBits) - 1;

        internal const int kTaskBits = DuplexRingRecordFormat.CompletionTicketBits;
        const int kTaskOffset = kRequestOffset + kRequestBits;
        internal const long kTaskMask = (1L << kTaskBits) - 1;

        /// <summary>Largest request capacity the wrapping request id can distinguish from an empty range.</summary>
        internal const int MaxOutstandingRequests = DuplexRingRecordFormat.MaxOutstandingRequests;

        /// <summary>Largest completion capacity the wrapping completion ticket can distinguish from an empty range.</summary>
        internal const int MaxOutstandingCompletions = DuplexRingRecordFormat.MaxOutstandingCompletions;

        internal const int kInlineAddressBits = 64 - kRequestBits - kTaskBits;
        const int kInlineAddressOffset = kTaskOffset + kTaskBits;
        internal const long kInlineAddressMask = (1L << kInlineAddressBits) - 1;

        /// <summary>Packed cursor word.</summary>
        public long Value;

        /// <summary>Free-running inline-buffer address; wraps at 2^<see cref="kInlineAddressBits"/>.</summary>
        public readonly long InlineAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (Value >>> kInlineAddressOffset) & kInlineAddressMask;
        }

        /// <summary>Next completion ticket to issue; wraps at 2^<see cref="kTaskBits"/>.</summary>
        public readonly int TaskId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)((Value >> kTaskOffset) & kTaskMask);
        }

        /// <summary>Next request id to issue; wraps at 2^<see cref="kRequestBits"/>.</summary>
        public readonly int RequestCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)((Value >> kRequestOffset) & kRequestMask);
        }

        /// <summary>Packs the three fields, masking each to its own width.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long Pack(long inlineAddress, int taskId, int requestCount)
            => ((inlineAddress & kInlineAddressMask) << kInlineAddressOffset) |
               ((taskId & kTaskMask) << kTaskOffset) |
               ((requestCount & kRequestMask) << kRequestOffset);

        /// <summary>Forward distance from <paramref name="from"/> to <paramref name="to"/> in the wrapping inline-address space.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long InlineDistance(long from, long to) => (to - from) & kInlineAddressMask;
    }
}