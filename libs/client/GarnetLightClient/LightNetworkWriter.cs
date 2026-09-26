// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    /// <summary>
    /// Concurrent network writer for out-of-line (chunked) payloads only.
    /// <para>
    /// This is a thin, network-owning shell over a <see cref="DuplexBackpressureRing{TRequest, TCompletion}"/>
    /// specialized to <see cref="LightPayload"/> requests and <see cref="TcsWrapper"/> completions. It owns
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
        const int PayloadDescriptorSize = sizeof(long);

        readonly DuplexBackpressureRing<LightPayload, TcsWrapper> ring;
        readonly NetworkBufferSettings networkBufferSettings;
        readonly LimitedFixedBufferPool networkPool;
        readonly GarnetLightClientTcpNetworkHandler networkHandler;
        readonly ILogger logger;

        bool disposed;

        /// <summary>
        /// Shared epoch protecting the ring's page allocator and flush machinery.
        /// </summary>
        public LightEpoch epoch => ring.epoch;

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
                LightPayloadAsyncFlushResult<LightPayload>.CompleteChunk,
                socket,
                networkBufferSettings,
                networkPool,
                sslOptions != null,
                serverHook,
                networkSendThrottleMax: networkSendThrottleMax,
                logger: logger);
            this.networkHandler = networkHandler = handler;
            var networkSender = handler.GetNetworkSender();

            this.ring = new DuplexBackpressureRing<LightPayload, TcsWrapper>(
                sendPageSize,
                pageBufferCount,
                completionCapacity,
                networkBufferSettings.sendBufferSize,
                networkSender.SendResponse,
                _ => handler.Dispose(),
                epoch,
                logger);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Volatile.Write(ref disposed, true);
            ring.Dispose();
            networkHandler.Dispose();
            networkPool?.Dispose();
        }

        internal LightPayload RentPayloadBuffer(int length)
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
            return new(entry, length);
        }

        /// <summary>
        /// Claim a request address and (for response-expecting claims) a completion ticket.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int taskId, long address) TryAllocate(int size, bool expectsResponse, out CompletionEvent waitEvent)
            => ring.TryAllocate(size, expectsResponse, out waitEvent);

        /// <summary>Register (store and publish) a request payload at the descriptor address.</summary>
        public void RegisterRequest(long address, LightPayload payload) => ring.RegisterRequest(address, payload);

        /// <summary>Register (store and publish) a completion for the given ticket.</summary>
        public void RegisterCompletion(int ticket, TcsWrapper completion) => ring.RegisterCompletion(ticket, completion);

        /// <summary>Nudge the read-only shift so enqueued descriptors are flushed promptly.</summary>
        public void DoAggressiveShiftReadOnly() => ring.DoAggressiveShiftReadOnly();

        /// <summary>Number of completion tickets issued so far (task-space).</summary>
        public int CompletionTail => ring.CompletionTail;

        /// <summary>Reader-side: try to read a published completion for the given ticket.</summary>
        public bool TryReadCompletion(int ticket, out TcsWrapper completion) => ring.TryReadCompletion(ticket, out completion);

        /// <summary>Reader-side: advance the reply watermark, freeing completion slots.</summary>
        public void AdvanceReplied(int consumedCount) => ring.AdvanceReplied(consumedCount);
    }
}
