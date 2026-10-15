// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.client
{
    /// <summary>
    /// Free-running allocator cursor for the duplex request lane. A single 64-bit word packs three logical
    /// fields, low to high: the inline-buffer address (<see cref="Offset"/> within <see cref="Page"/>), the
    /// completion <see cref="TaskId"/> ticket, and a <see cref="RequestCount"/> that counts every scheduled
    /// request (including fire-and-forget operations that carry no completion ticket). One
    /// <see cref="System.Threading.Interlocked.Add(ref long, long)"/> on the reserve path hands each producer a
    /// mutually consistent (address, ticket, request id) tuple. The request id alone drives scheduling and
    /// flushing; the address only locates inline payload bytes and throttles ring-page reuse.
    ///
    /// Field order (low to high) is chosen for carry safety. A carry can only ever propagate upward, so the
    /// top field is the only one whose overflow is harmless (it wraps off bit 63 and is discarded). The
    /// address is split into an oversized <see cref="Offset"/> and an explicitly assigned <see cref="Page"/>
    /// so concurrent adds past a page end pile up in the offset's spare bits instead of carrying into the
    /// page: only the producer that crosses the boundary claims the next page, after checking that it has
    /// been flushed. Per field:
    /// <list type="bullet">
    /// <item><description><see cref="Offset"/> never carries into <see cref="Page"/>: the reserve path detects
    /// a page overflow and explicitly resets the offset before it can reach 2^<see cref="kOffsetBits"/>.</description></item>
    /// <item><description><see cref="Page"/> never carries into <see cref="TaskId"/>: the increment only ever
    /// adds to the offset/task/request fields, and the page index is only mutated by explicit assignment on the
    /// slow path, bounded by <see cref="kPageMask"/>.</description></item>
    /// <item><description><see cref="TaskId"/> may wrap every 2^<see cref="kTaskBits"/> completions. Because it
    /// sits directly below <see cref="RequestCount"/>, that wrap carries into <see cref="RequestCount"/> only,
    /// never into the smaller fields, so the consumed completion ticket is never corrupted.</description></item>
    /// <item><description><see cref="RequestCount"/> occupies the top field, so its wrap is discarded off
    /// bit 63 and never corrupts any consumed field.</description></item>
    /// </list>
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    struct LightPageAndOffset
    {
        internal const int kOffsetBits = 31;
        const int kOffsetOffset = 0;
        internal const long kOffsetMask = (1L << kOffsetBits) - 1;
        const long kOffsetMaskInLong = kOffsetMask << kOffsetOffset;

        internal const int kPageBits = 3;
        const int kPageOffset = 31;
        internal const long kPageMask = (1L << kPageBits) - 1;
        const long kPageMaskInLong = kPageMask << kPageOffset;

        internal const int kTaskBits = 15;
        internal const int kTaskOffset = 34;
        internal const long kTaskMask = (1L << kTaskBits) - 1;
        const long kTaskMaskInLong = kTaskMask << kTaskOffset;

        internal const int kRequestBits = 15;
        internal const int kRequestOffset = 49;
        internal const long kRequestMask = (1L << kRequestBits) - 1;
        const long kRequestMaskInLong = kRequestMask << kRequestOffset;

        /// <summary>Atomic add constant that advances the completion ticket by one.</summary>
        internal const long kTaskIncrement = 1L << kTaskOffset;

        /// <summary>Atomic add constant that advances the scheduled-request counter by one.</summary>
        internal const long kRequestIncrement = 1L << kRequestOffset;

        [FieldOffset(0)]
        public long PageAndOffset;

        public int Page
        {
            get => (int)((PageAndOffset >> kPageOffset) & kPageMask);
            set => PageAndOffset = (((long)value) << kPageOffset) | (PageAndOffset & ~kPageMaskInLong);
        }

        public int Offset
        {
            get => (int)((PageAndOffset >> kOffsetOffset) & kOffsetMask);
            set => PageAndOffset = (((long)value) << kOffsetOffset) | (PageAndOffset & ~kOffsetMaskInLong);
        }

        public int TaskId
        {
            get => (int)((PageAndOffset >> kTaskOffset) & kTaskMask);
            set => PageAndOffset = (((long)value) << kTaskOffset) | (PageAndOffset & ~kTaskMaskInLong);
        }

        public int RequestCount
        {
            get => (int)((PageAndOffset >> kRequestOffset) & kRequestMask);
            set => PageAndOffset = (((long)value) << kRequestOffset) | (PageAndOffset & ~kRequestMaskInLong);
        }

        public int PrevTaskId => (int)(((PageAndOffset >> kTaskOffset) - 1) & kTaskMask);

        public int PrevRequestCount => (int)(((PageAndOffset >> kRequestOffset) - 1) & kRequestMask);
    }
}