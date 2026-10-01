// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;

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

        readonly DuplexOperationChannel<LightRequestContext, TcsWrapper, RingTransport> ring;
        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;
        readonly GarnetLightClientTcpNetworkHandler networkHandler;
        readonly ILogger logger;

        /// <summary>
        /// Shared epoch protecting the ring's page allocator and flush machinery.
        /// </summary>
        public LightEpoch epoch => ring.epoch;

        /// <summary>Number of completion tickets issued so far (task-space).</summary>
        public int CompletionTail => ring.CompletionTail;

        /// <summary>
        /// Constructor
        /// </summary>
        public LightNetworkWriter(GarnetLightClient serverHook, Socket socket, int messageBufferSize, SslClientAuthenticationOptions sslOptions, out GarnetLightClientTcpNetworkHandler networkHandler, int sendPageSize, int pageBufferCount, int completionCapacity, int networkSendThrottleMax, LightEpoch epoch, PoolOwnerType ownerType, ILogger logger = null)
        {
            this.logger = logger;
            this.networkBufferSettings = new NetworkBufferSettings(messageBufferSize, messageBufferSize);
            this.networkPool = networkBufferSettings.CreateBufferPool(ownerType: ownerType, logger: logger);

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
                networkSendThrottleMax: networkSendThrottleMax,
                logger: logger);
            this.networkHandler = networkHandler = handler;
            var useTls = sslOptions != null;
            var tcpSender = useTls ? null : (ClientTcpNetworkSender)handler.GetNetworkSender();

            this.ring = new DuplexOperationChannel<LightRequestContext, TcsWrapper, RingTransport>(
                sendPageSize,
                pageBufferCount,
                completionCapacity,
                networkBufferSettings.sendBufferSize,
                new RingTransport(tcpSender, handler, useTls),
                epoch,
                logger);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            ring.Dispose();
            networkHandler.Dispose();
            networkPool?.Dispose();
        }

        internal LightRequestContext RentPayloadBuffer(int length)
        {
            var allocationSize = length;
            if (length <= networkPool.MaxAllocationSize)
            {
                var minimumSize = Math.Max(length, networkPool.MinAllocationSize);
                var roundedSize = System.Numerics.BitOperations.RoundUpToPowerOf2((uint)minimumSize);
                if (roundedSize <= (uint)networkPool.MaxAllocationSize)
                    allocationSize = (int)roundedSize;
            }

            var entry = networkPool.Get(allocationSize, PoolEntryBufferType.OutOfLinePayload);
            ObjectDisposedException.ThrowIf(entry is null, this);
            return new LightRequestContext(entry, length);
        }

        /// <summary>
        /// Attempts to reserve the paired request address and optional completion ticket for one send operation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryScheduleSend(
            int requestSize,
            bool expectsResponse,
            out DuplexOperationReservation reservation,
            out CompletionEvent waitEvent)
            => ring.TryScheduleOperation(requestSize, expectsResponse, out reservation, out waitEvent);

        /// <summary>
        /// Ring bytes a command of <paramref name="payloadLength"/> reserves, and whether it is written inline
        /// (its whole payload packed into one page) or out-of-line (an 8-byte descriptor plus a rented buffer).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetRecordSize(int payloadLength, out bool isInline) => ring.GetRecordSize(payloadLength, out isInline);

        /// <summary>
        /// Reserve an inline record and return a pointer to write its payload directly into page memory,
        /// skipping the pooled buffer. The caller must hold the epoch and fill exactly
        /// <paramref name="payloadLength"/> bytes.
        /// </summary>
        public unsafe byte* RegisterInlineRecord(long address, int payloadLength)
            => ring.RegisterInlineRecord(address, payloadLength);

        /// <summary>Register (store and publish) a request record at the descriptor address.</summary>
        public void RegisterOfflineRecord(long address, LightRequestContext payload)
            => ring.RegisterOfflineRecord(address, payload);

        /// <summary>Register (store and publish) a completion for the given ticket.</summary>
        public void RegisterCompletion(int ticket, TcsWrapper completion)
            => ring.RegisterCompletion(ticket, completion);

        /// <summary>Nudge the read-only shift so enqueued descriptors are flushed promptly.</summary>
        public void DrainRequests()
            => ring.DrainRequests();

        /// <summary>Reader-side: try to read a published completion for the given ticket.</summary>
        public bool TryReadCompletion(int ticket, out TcsWrapper completion)
            => ring.TryReadCompletion(ticket, out completion);

        /// <summary>Atomically claim a published completion for single delivery (teardown/fault path).</summary>
        public bool TryClaimCompletionTicket(int ticket, out TcsWrapper completion)
            => ring.TryClaimCompletionTicket(ticket, out completion);

        /// <summary>Reader-side: advance the reply watermark, freeing completion slots.</summary>
        public void AdvanceReplied(int consumedCount)
            => ring.AdvanceCompletion(consumedCount);
    }
}