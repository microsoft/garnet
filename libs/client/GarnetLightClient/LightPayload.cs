// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
using Garnet.common;

namespace Garnet.client
{
    /// <summary>
    /// A request-lane payload for the <see cref="DuplexBackpressureRing{TRequest, TCompletion}"/>
    /// (and its <see cref="LightNetworkWriter"/> realization). It is a pure request descriptor: it
    /// carries only the rented buffer and its length. The response completion no longer travels with
    /// the payload — in the duplex ring the completion lives in a separate, reply-gated lane keyed by
    /// the completion ticket that the combined allocator hands out alongside the request address.
    /// The request lane is freed on flush (send), independently of when the reply arrives.
    /// </summary>
    struct LightPayload : IPayload
    {
        /// <summary>
        /// Rented buffer holding the serialized command bytes.
        /// </summary>
        internal PoolEntry Entry;

        int length;

        /// <inheritdoc />
        public byte[] Buffer => Entry.entry;

        /// <inheritdoc />
        public int Length => length;

        internal LightPayload(PoolEntry entry, int length)
        {
            this.Entry = entry;
            this.length = length;
        }

        /// <inheritdoc />
        public void Dispose() => Entry?.Dispose();
    }

    /// <summary>
    /// Sink that finalizes a completed request-lane flush. Carried by
    /// <see cref="LightPayloadAsyncFlushResult{TRequest}"/> so the (static) network flush completion
    /// callback can route back to the owning ring instance without the network handler holding a ring
    /// reference at construction time.
    /// </summary>
    internal interface IFlushCompletionSink
    {
        /// <summary>Finalize the flush of a payload chunk group, advancing the flushed watermark.</summary>
        void CompleteFlush(CountWrapper count);
    }

    /// <summary>
    /// Out-of-line payload async flush result for the request lane of
    /// <see cref="DuplexBackpressureRing{TRequest, TCompletion}"/>.
    /// </summary>
    sealed class LightPayloadAsyncFlushResult<TRequest> where TRequest : struct, IPayload
    {
        public CountWrapper count;
        public TRequest payload;
        public int remainingChunks;
        public IFlushCompletionSink sink;

        /// <summary>
        /// Network flush completion callback: finalize one dispatched chunk of the flush result carried as
        /// <paramref name="context"/>. When the final chunk completes, disposes the request buffer and routes
        /// to the owning ring through its <see cref="IFlushCompletionSink"/>. Static so the network handler
        /// can be wired with a plain method group and holds no ring reference at construction time.
        /// </summary>
        /// <param name="context">The <see cref="LightPayloadAsyncFlushResult{TRequest}"/> handed to the send.</param>
        public static void CompleteChunk(object context)
        {
            switch (context)
            {
                case LightPayloadAsyncFlushResult<TRequest> result:
                    if (Interlocked.Decrement(ref result.remainingChunks) == 0)
                    {
                        // The request buffer is done; any completion lives in the completion lane until its reply.
                        result.payload.Dispose();
                        result.sink.CompleteFlush(result.count);
                    }
                    break;

                default:
                    Debug.Fail($"Unexpected network flush context type {context?.GetType().FullName ?? "null"}.");
                    break;
            }
        }
    }
}
