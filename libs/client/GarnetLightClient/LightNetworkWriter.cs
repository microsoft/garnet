// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.client
{
    /// <summary>
    /// Capacity and buffer settings for a <see cref="LightNetworkWriter"/>.
    /// </summary>
    /// <remarks>
    /// Creates a complete set of network writer capacity and buffer options.
    /// </remarks>
    /// <param name="networkBufferSizeBytes">Fixed send-buffer size, initial receive-buffer size, and maximum send chunk size.</param>
    /// <param name="requestPageSizeBytes">Size of each request-ring page, rounded down to a power of two.</param>
    /// <param name="requestPageCount">Number of circular request-ring pages.</param>
    /// <param name="maxOutstandingCompletions">Maximum number of response completions awaiting replies.</param>
    /// <param name="maxConcurrentNetworkSends">Maximum number of concurrent transport sends.</param>
    /// <param name="maxOutOfLineRentedBytes">Maximum pooled out-of-line request bytes rented until local send completion. Zero disables throttling.</param>
    public readonly struct LightNetworkWriterOptions(
        int networkBufferSizeBytes,
        int requestPageSizeBytes,
        int requestPageCount,
        int maxOutstandingCompletions,
        int maxConcurrentNetworkSends,
        long maxOutOfLineRentedBytes = 0)
    {
        static int RequestSlotSizeBytes
            => Align(Unsafe.SizeOf<LightRequestContext>() + IntPtr.Size);

        static int FlushContextSizeBytes
            => Align(
                (2 * IntPtr.Size) +
                Unsafe.SizeOf<LightRequestContext>() +
                (2 * IntPtr.Size) +
                sizeof(int));

        /// <summary>
        /// Default settings for a general-purpose <see cref="GarnetLightClient"/>.
        /// </summary>
        /// <remarks>
        /// The default uses two 1-KiB request pages. For inline requests, two pages outperform larger
        /// page counts, and 1 KiB is the only tested page-size increase over 512 bytes that improves
        /// per-request cost; larger pages regress. For out-of-line requests, total ring capacity rather
        /// than page geometry determines the amortized per-request cost. Pooled out-of-line requests are
        /// limited to 64 MiB awaiting local send completion.
        /// <para>
        /// On a 64-bit process, the fixed request/completion ring footprint is approximately 14 KiB:
        /// </para>
        /// <list type="bullet">
        /// <item><description>
        /// Request pages: 2 pages * 1,024 bytes = 2 KiB of pinned payload storage.
        /// </description></item>
        /// <item><description>
        /// Request side table: (2 * 1,024 bytes) / 8-byte record alignment = 256 slots;
        /// each <c>LightRequestContext</c> plus flush-context reference is approximately 32 bytes,
        /// for approximately 8 KiB.
        /// </description></item>
        /// <item><description>
        /// Completion lane: 64 slots * 64 bytes per cache-line-padded completion slot = 4 KiB.
        /// </description></item>
        /// </list>
        /// The total excludes array headers, 8 KiB network buffers, and pooled out-of-line request buffers.
        /// It also excludes reusable flush contexts from the fixed footprint because they are allocated lazily.
        /// After every physical request slot has been used, their approximate worst-case footprint is
        /// 256 slots * 64 bytes = 16 KiB, keeping the fully warmed request/completion ring near 30 KiB.
        /// </remarks>
        public static LightNetworkWriterOptions Default => new(
            networkBufferSizeBytes: 1 << 13,
            requestPageSizeBytes: 1 << 10,
            requestPageCount: 2,
            maxOutstandingCompletions: 1 << 6,
            maxConcurrentNetworkSends: 8,
            maxOutOfLineRentedBytes: 64L << 20);

        /// <summary>
        /// Size of the fixed network send buffer and initial receive buffer.
        /// Also determines the maximum request chunk sent in one transport operation.
        /// </summary>
        public int NetworkBufferSizeBytes { get; } = networkBufferSizeBytes;

        /// <summary>
        /// Size of each request-ring page. This controls the maximum inline request size and,
        /// together with <see cref="RequestPageCount"/>, the request capacity awaiting local send completion.
        /// </summary>
        public int RequestPageSizeBytes { get; } = (int)Utility.PreviousPowerOf2(requestPageSizeBytes);

        /// <summary>
        /// Number of circular pages in the request ring.
        /// </summary>
        public int RequestPageCount { get; } = requestPageCount;

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
                var requestSlotCount = requestPageBytes / DuplexRingRecordFormat.HeaderSize;
                var requestSideTableBytes = requestSlotCount * RequestSlotSizeBytes;
                var completionLaneBytes = (long)MaxOutstandingCompletions * DuplexRingRecordFormat.CompletionSlotSize;
                return requestPageBytes + requestSideTableBytes + completionLaneBytes;
            }
        }

        /// <summary>
        /// Estimates the fully warmed request/completion ring memory in bytes.
        /// </summary>
        /// <remarks>
        /// Adds one lazily allocated reusable flush context for every physical request slot to
        /// <see cref="MinMemoryFootprint"/>. Excludes array headers, network buffers, and pooled
        /// out-of-line payloads.
        /// </remarks>
        public long MaxMemoryFootprint()
        {
            checked
            {
                var requestPageBytes = (long)RequestPageSizeBytes * RequestPageCount;
                var requestSlotCount = requestPageBytes / DuplexRingRecordFormat.HeaderSize;
                return MinMemoryFootprint() + (requestSlotCount * FlushContextSizeBytes);
            }
        }

        static int Align(int size)
            => (size + (IntPtr.Size - 1)) & ~(IntPtr.Size - 1);
    }

    /// <summary>
    /// Concurrent network writer for inline and out-of-line payloads.
    /// <para>
    /// This is a thin, network-owning shell over a
    /// <see cref="DuplexOperationChannel{TRequest, TCompletion, TTransport}"/>
    /// specialized to <see cref="LightRequestContext"/> requests and <see cref="TcsWrapper"/> completions. It owns
    /// the socket, the <see cref="GarnetLightClientTcpNetworkHandler"/> and the send buffer pool, and forwards
    /// all ring bookkeeping (allocation, request enqueue, flush, and the completion lane) to the ring.
    /// </para>
    /// <para>
    /// The request lane is freed on flush (send); the completion lane is freed on reply. Because the ring's
    /// single combined allocator advances the request address and the completion ticket together, a producer
    /// receives an aligned (address, ticket) pair from one call and the reply reader can match replies to
    /// completions in the same monotonic order.
    /// </para>
    /// </summary>
    internal sealed class LightNetworkWriter : IDisposable
    {
        readonly struct RingTransport : ITransportContext
        {
            readonly ClientTcpNetworkSender tcpSender;
            readonly GarnetLightClientTcpNetworkHandler networkHandler;
            readonly bool useTls;

            internal RingTransport(
                ClientTcpNetworkSender tcpSender,
                GarnetLightClientTcpNetworkHandler networkHandler,
                bool useTls)
            {
                this.tcpSender = tcpSender;
                this.networkHandler = networkHandler;
                this.useTls = useTls;
            }

            public void Send(byte[] buffer, int offset, int length, object context)
            {
                if (useTls)
                    networkHandler.SendResponse(buffer, offset, length, context);
                else
                    tcpSender.SendResponse(buffer, offset, length, context);
            }

            public void OnFlushError(Exception exception)
                => networkHandler.Dispose();
        }

        readonly DuplexOperationChannel<LightRequestContext, TcsWrapper, RingTransport> channel;
        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;
        readonly WaiterQueue<MemoryThrottle, int> memoryThrottle;
        readonly GarnetLightClientTcpNetworkHandler networkHandler;
        readonly ILogger logger;

        /// <summary>
        /// Shared epoch protecting the ring's page allocator and flush machinery.
        /// </summary>
        public LightEpoch epoch => channel.epoch;

        /// <summary>Number of completion tickets issued so far (task-space).</summary>
        public int CompletionTail => channel.CompletionTail;

        /// <summary>
        /// Constructor
        /// </summary>
        public LightNetworkWriter(
            GarnetLightClient serverHook,
            Socket socket,
            LightNetworkWriterOptions options,
            SslClientAuthenticationOptions sslOptions,
            out GarnetLightClientTcpNetworkHandler networkHandler,
            LightEpoch epoch,
            PoolOwnerType ownerType,
            ILogger logger = null)
        {
            this.logger = logger;
            this.networkBufferSettings = new NetworkBufferSettings(options.NetworkBufferSizeBytes, options.NetworkBufferSizeBytes);
            this.networkPool = networkBufferSettings.CreateBufferPool(ownerType: ownerType, logger: logger);
            var outOfLineRentedBytesThrottle = new MemoryThrottle(options.MaxOutOfLineRentedBytes);
            this.memoryThrottle = new WaiterQueue<MemoryThrottle, int>(outOfLineRentedBytesThrottle);

            // The flush-completion callback is a static routine on the flush result and recovers its ring via
            // the result's sink, so the handler needs no ring instance at construction. That removes the
            // ring<->handler cycle: build the handler first, then construct the fully-wired ring.
            var handler = new GarnetLightClientTcpNetworkHandler(
                serverHook,
                DuplexOperationAsyncFlushResult<LightRequestContext>.CompleteChunk,
                socket,
                networkBufferSettings,
                networkPool,
                sslOptions != null,
                serverHook,
                networkSendThrottleMax: options.MaxConcurrentNetworkSends,
                logger: logger);
            this.networkHandler = networkHandler = handler;
            var useTls = sslOptions != null;
            var tcpSender = useTls ? null : (ClientTcpNetworkSender)handler.GetNetworkSender();

            this.channel = new DuplexOperationChannel<LightRequestContext, TcsWrapper, RingTransport>(
                options.RequestPageSizeBytes,
                options.RequestPageCount,
                options.MaxOutstandingCompletions,
                networkBufferSettings.sendBufferSize,
                new RingTransport(tcpSender, handler, useTls),
                epoch,
                logger);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            channel.Dispose();
            networkHandler.Dispose();
            networkPool?.Dispose();
            memoryThrottle.Dispose();
        }

        /// <summary>
        /// Rent request buffer for out-of-line operation
        /// </summary>
        /// <param name="length"></param>
        /// <returns></returns>
        internal LightRequestContext RentRequestBuffer(int length)
            => LightRequestContext.RentRequestBuffer(networkPool, length);

        /// <summary>
        /// Gets the actual pooled allocation size that an out-of-line request will reserve.
        /// </summary>
        internal int GetRequestBufferAllocationSize(int length)
            => LightRequestContext.GetRequestBufferAllocationSize(networkPool, length);

        internal ValueTask<bool> RentMemoryThrottle(int allocationSize, CancellationToken token)
            => memoryThrottle.AdmitAsync(allocationSize, token);

        internal bool AdmitOutOfLineRental(int allocationSize, CancellationToken token)
            => memoryThrottle.Admit(allocationSize, token);

        internal void ReleaseOutOfLineRental(int allocationSize)
            => memoryThrottle.Release(allocationSize);

        internal LightRequestContext RentAdmittedOutOfLineBuffer(int length, int reservedBytes)
            => LightRequestContext.RentRequestBuffer(networkPool, length, memoryThrottle, reservedBytes);

        /// <summary>
        /// Attempts to reserve the paired request address and optional completion ticket for one send operation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryScheduleSend(
            int requestSize,
            bool expectsResponse,
            out DuplexOperationReservation reservation,
            out CompletionEvent waitEvent)
            => channel.TryScheduleOperation(requestSize, expectsResponse, out reservation, out waitEvent);

        /// <summary>
        /// Ring bytes a command of <paramref name="payloadLength"/> reserves, and whether it is written inline
        /// (its whole payload packed into one page) or out-of-line (an 8-byte descriptor plus a rented buffer).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRecordSize(int payloadLength, out bool isInline)
            => channel.GetRecordSize(payloadLength, out isInline);

        /// <summary>
        /// Reserve an inline record and return a pointer to write its payload directly into page memory,
        /// skipping the pooled buffer. The caller must hold the epoch and fill exactly
        /// <paramref name="payloadLength"/> bytes.
        /// </summary>
        public unsafe byte* RegisterInlineRecord(long address, int payloadLength)
            => channel.RegisterInlineRecord(address, payloadLength);

        /// <summary>Register (store and publish) a request record at the descriptor address.</summary>
        public void RegisterOfflineRecord(long address, LightRequestContext payload)
            => channel.RegisterOfflineRecord(address, payload);

        /// <summary>Register (store and publish) a completion for the given ticket.</summary>
        public void RegisterCompletion(int ticket, TcsWrapper completion)
            => channel.RegisterCompletion(ticket, completion);

        /// <summary>Nudge the read-only shift so enqueued descriptors are flushed promptly.</summary>
        public void DrainRequests()
            => channel.DrainRequests();

        /// <summary>Reader-side: try to read a published completion for the given ticket.</summary>
        public bool TryReadCompletion(int ticket, out TcsWrapper completion)
            => channel.TryReadCompletion(ticket, out completion);

        /// <summary>Atomically claim a published completion for single delivery (teardown/fault path).</summary>
        public bool TryClaimCompletionTicket(int ticket, out TcsWrapper completion)
            => channel.TryClaimCompletionTicket(ticket, out completion);

        /// <summary>Reader-side: advance the reply watermark, freeing completion slots.</summary>
        public void AdvanceReplied(int consumedCount)
            => channel.AdvanceCompletion(consumedCount);
    }
}