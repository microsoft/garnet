// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Garnet.client
{
    /// <summary>
    /// Controls whether request-flush completion contexts are retained for reuse or allocated per operation.
    /// </summary>
    public enum FlushResultAllocationMode
    {
        /// <summary>
        /// Lazily allocate one completion context per physical request slot and retain it for reuse.
        /// </summary>
        Buffered,

        /// <summary>
        /// Allocate a completion context for each flushed operation and release it after send completion.
        /// </summary>
        PerOperation
    }

    /// <summary>
    /// Capacity and buffer settings for a <see cref="LightNetworkWriter"/>.
    /// </summary>
    /// <remarks>
    /// Creates a complete set of network writer capacity and buffer options.
    /// </remarks>
    /// <param name="networkBufferSizeBytes">Fixed send-buffer size, initial receive-buffer size, and maximum send chunk size.</param>
    /// <param name="requestPageSizeBytes">
    /// Size of each request-ring page, rounded down to a power of two. Must be at least
    /// <see cref="DuplexRingRecordFormat.HeaderSize"/>.
    /// </param>
    /// <param name="requestPageCount">Number of circular request-ring pages.</param>
    /// <param name="maxOutstandingRequests">Maximum number of minimum-allocation requests awaiting local send completion.</param>
    /// <param name="maxOutstandingCompletions">Maximum number of response completions awaiting replies.</param>
    /// <param name="maxConcurrentNetworkSends">Maximum number of concurrent transport sends.</param>
    /// <param name="maxOutOfLineRentedBytes">Maximum pooled out-of-line request bytes rented until local send completion. Zero disables throttling.</param>
    /// <param name="flushResultAllocationMode">Controls whether request-flush completion contexts are retained per request slot or allocated per operation.</param>
    public readonly struct LightNetworkWriterOptions(
        int networkBufferSizeBytes,
        int requestPageSizeBytes,
        int requestPageCount,
        int maxOutstandingRequests,
        int maxOutstandingCompletions,
        int maxConcurrentNetworkSends,
        long maxOutOfLineRentedBytes,
        FlushResultAllocationMode flushResultAllocationMode)
    {
        static int RequestSlotSizeBytes
            => Align(Unsafe.SizeOf<LightRequestContext>() + IntPtr.Size);

        internal static int FlushContextSizeBytes
            => Align(
                (2 * IntPtr.Size) +
                Unsafe.SizeOf<LightRequestContext>() +
                IntPtr.Size +
                sizeof(int) +
                sizeof(bool));

        /// <summary>
        /// Default settings for a general-purpose <see cref="GarnetLightClient"/>.
        /// </summary>
        /// <remarks>
        /// The default uses two 4-KiB request pages and 16 slots in both the request and completion lanes.
        /// Pooled out-of-line requests are limited to 64 MiB awaiting local send completion.
        /// <para>
        /// On a 64-bit process, the fixed request/completion ring footprint is approximately 9.5 KiB:
        /// </para>
        /// <list type="bullet">
        /// <item><description>
        /// Request pages: 2 pages * 4,096 bytes = 8 KiB of pinned payload storage.
        /// </description></item>
        /// <item><description>
        /// Request side table: 16 slots, matching the completion lane, with a 512-byte request-allocation quantum;
        /// each <c>LightRequestContext</c> plus flush-context reference is approximately 32 bytes,
        /// for approximately 0.5 KiB.
        /// </description></item>
        /// <item><description>
        /// Completion lane: 16 slots * 64 bytes per cache-line-padded completion slot = 1 KiB.
        /// </description></item>
        /// </list>
        /// The total excludes array headers, 8 KiB network buffers, and pooled out-of-line request buffers.
        /// It also excludes reusable flush contexts from the fixed footprint because they are allocated lazily.
        /// After every physical request slot has been used, their approximate worst-case footprint is
        /// 16 slots * 56 bytes = 896 bytes, keeping the fully warmed request/completion ring near 10.4 KiB.
        /// </remarks>
        public static LightNetworkWriterOptions Default => new(
            networkBufferSizeBytes: 1 << 13,
            requestPageSizeBytes: 1 << 12,
            requestPageCount: 2,
            maxOutstandingRequests: 1 << 4,
            maxOutstandingCompletions: 1 << 4,
            maxConcurrentNetworkSends: 8,
            maxOutOfLineRentedBytes: 64L << 20,
            flushResultAllocationMode: FlushResultAllocationMode.Buffered);

        /// <summary>
        /// Size of the fixed network send buffer and initial receive buffer.
        /// Also determines the maximum request chunk sent in one transport operation.
        /// </summary>
        public int NetworkBufferSizeBytes { get; } = networkBufferSizeBytes;

        /// <summary>
        /// Size of each request-ring page. This controls the maximum inline request size and,
        /// together with <see cref="RequestPageCount"/>, the request capacity awaiting local send completion.
        /// </summary>
        public int RequestPageSizeBytes { get; } = GetRequestPageSizeBytes(requestPageSizeBytes);

        /// <summary>
        /// Number of circular pages in the request ring.
        /// </summary>
        public int RequestPageCount { get; } = GetRequestPageCount(requestPageCount);

        /// <summary>
        /// Maximum number of minimum-allocation requests awaiting local send completion.
        /// Larger inline records consume multiple units of this capacity.
        /// </summary>
        public int MaxOutstandingRequests { get; } = ValidateMaxOutstandingRequests(
            requestPageSizeBytes,
            requestPageCount,
            maxOutstandingRequests);

        /// <summary>
        /// Minimum number of request-ring bytes reserved by every inline or out-of-line operation.
        /// </summary>
        public int RequestAllocationQuantumBytes
            => RequestPageSizeBytes / (MaxOutstandingRequests / RequestPageCount);

        /// <summary>
        /// Maximum number of response-expecting requests whose replies have not yet been consumed.
        /// This capacity is independent of request pages, which are released on local send completion.
        /// </summary>
        public int MaxOutstandingCompletions { get; } = maxOutstandingCompletions;

        /// <summary>
        /// Maximum number of transport sends that may be in progress concurrently.
        /// </summary>
        public int MaxConcurrentNetworkSends { get; } = maxConcurrentNetworkSends;

        /// <summary>
        /// Maximum pooled bytes reserved by out-of-line requests that have not completed their local send.
        /// Zero disables memory admission throttling. Inline requests allocate no pooled payload and do not
        /// consume this capacity.
        /// </summary>
        public long MaxOutOfLineRentedBytes { get; } = maxOutOfLineRentedBytes;

        /// <summary>
        /// Controls whether request-flush completion contexts are retained per request slot or allocated per operation.
        /// </summary>
        public FlushResultAllocationMode FlushResultAllocationMode { get; } = ValidateFlushResultAllocationMode(flushResultAllocationMode);

        /// <summary>
        /// Estimates the fixed request-page, request-side-table, and completion-lane memory in bytes.
        /// </summary>
        /// <remarks>
        /// Excludes array headers, network buffers, pooled out-of-line payloads, and lazily allocated
        /// flush contexts.
        /// </remarks>
        public long MinMemoryFootprint()
        {
            checked
            {
                var requestPageBytes = (long)RequestPageSizeBytes * RequestPageCount;
                var requestSideTableBytes = (long)MaxOutstandingRequests * RequestSlotSizeBytes;
                var completionLaneBytes = (long)MaxOutstandingCompletions * DuplexRingRecordFormat.CompletionSlotSize;
                return requestPageBytes + requestSideTableBytes + completionLaneBytes;
            }
        }

        /// <summary>
        /// Estimates the fully warmed request/completion ring memory in bytes.
        /// </summary>
        /// <remarks>
        /// Adds one flush context for every physical request slot to <see cref="MinMemoryFootprint"/>. In buffered
        /// mode this is the fully warmed retained footprint; in per-operation mode it is a conservative bound
        /// on transient contexts. Excludes array headers, network buffers, and pooled out-of-line payloads.
        /// </remarks>
        public long MaxMemoryFootprint()
        {
            checked
            {
                return MinMemoryFootprint() + ((long)MaxOutstandingRequests * FlushContextSizeBytes);
            }
        }

        static int Align(int size)
            => (size + (IntPtr.Size - 1)) & ~(IntPtr.Size - 1);

        static FlushResultAllocationMode ValidateFlushResultAllocationMode(FlushResultAllocationMode mode)
            => Enum.IsDefined(mode) ? mode : throw new ArgumentOutOfRangeException(nameof(flushResultAllocationMode), mode, null);

        static int ValidateMaxOutstandingRequests(int requestPageSizeBytes, int requestPageCount, int maxOutstandingRequests)
        {
            var pageSize = GetRequestPageSizeBytes(requestPageSizeBytes);
            var pageCount = GetRequestPageCount(requestPageCount);
            if (maxOutstandingRequests <= 0 || maxOutstandingRequests % pageCount != 0)
                throw new ArgumentOutOfRangeException(nameof(maxOutstandingRequests), maxOutstandingRequests,
                    "Maximum outstanding requests must be positive and evenly divisible across request pages.");

            var slotsPerPage = maxOutstandingRequests / pageCount;
            if (slotsPerPage > pageSize / DuplexRingRecordFormat.HeaderSize ||
                pageSize % slotsPerPage != 0 ||
                (pageSize / slotsPerPage) % DuplexRingRecordFormat.HeaderSize != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxOutstandingRequests), maxOutstandingRequests,
                    "Maximum outstanding requests must produce an allocation quantum that evenly divides each page and is aligned to the request header.");
            }

            return maxOutstandingRequests;
        }

        static int GetRequestPageCount(int requestPageCount)
        {
            if (requestPageCount <= 0 || requestPageCount > PageOffset.kPageMask)
                throw new ArgumentOutOfRangeException(nameof(requestPageCount));
            return requestPageCount;
        }

        static int GetRequestPageSizeBytes(int requestPageSizeBytes)
        {
            if (requestPageSizeBytes < DuplexRingRecordFormat.HeaderSize)
                throw new ArgumentOutOfRangeException(nameof(requestPageSizeBytes), requestPageSizeBytes,
                    $"Request page size must be at least {DuplexRingRecordFormat.HeaderSize} bytes.");

            return (int)Utility.PreviousPowerOf2(requestPageSizeBytes);
        }
    }
}