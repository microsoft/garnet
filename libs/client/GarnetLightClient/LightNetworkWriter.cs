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
        readonly MemoryThrottle outOfLineRentedBytesThrottle;
        readonly WaiterQueue<MemoryThrottle, int> memoryThrottle;
        readonly GarnetLightClientTcpNetworkHandler networkHandler;
        readonly ILogger logger;
        readonly long minMemoryFootprintBytes;
        readonly long maxMemoryFootprintBytes;

        int closed;
        int disposed;

        /// <summary>
        /// Shared epoch protecting the ring's page allocator and flush machinery.
        /// </summary>
        public LightEpoch epoch => channel.epoch;

        internal bool Closed => Volatile.Read(ref closed) != 0;

        internal long ActiveMemoryUsageBytes
            => SaturatingAdd(
                SaturatingAdd(
                    minMemoryFootprintBytes,
                    (long)channel.AllocatedFlushContextCount * LightNetworkWriterOptions.FlushContextSizeBytes),
                outOfLineRentedBytesThrottle.InUseBytes);

        internal long MaxMemoryUsageBytes
            => outOfLineRentedBytesThrottle.CapacityBytes == 0
                ? long.MaxValue
                : SaturatingAdd(maxMemoryFootprintBytes, outOfLineRentedBytesThrottle.CapacityBytes);

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
            this.outOfLineRentedBytesThrottle = new MemoryThrottle(options.MaxOutOfLineRentedBytes);
            this.memoryThrottle = new WaiterQueue<MemoryThrottle, int>(this.outOfLineRentedBytesThrottle);
            this.minMemoryFootprintBytes = options.MinMemoryFootprint();
            this.maxMemoryFootprintBytes = options.MaxMemoryFootprint();

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
                options.MaxOutstandingRequests,
                options.MaxOutstandingCompletions,
                networkBufferSettings.sendBufferSize,
                new RingTransport(tcpSender, handler, useTls),
                epoch,
                options.FlushResultAllocationMode,
                logger);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            Close();
            networkHandler.Dispose();
            networkPool?.Dispose();
        }

        internal void Close()
        {
            if (Interlocked.Exchange(ref closed, 1) != 0)
                return;

            channel.Dispose();
            memoryThrottle.Dispose();
        }

        static long SaturatingAdd(long left, long right)
            => left > long.MaxValue - right ? long.MaxValue : left + right;

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

        internal ValueTask WaitForIdleAsync(CancellationToken token)
            => channel.WaitForIdleAsync(token);

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