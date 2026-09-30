// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.client
{
    /// <summary>
    /// A request-lane payload for the <see cref="DuplexOperationRing{TRequest, TCompletion, TTransport}"/>
    /// (and its <see cref="LightNetworkWriter"/> realization). It is a pure request descriptor: it
    /// carries only the rented buffer and its length. The response completion no longer travels with
    /// the payload — in the duplex ring the completion lives in a separate, reply-gated lane keyed by
    /// the completion ticket that the combined allocator hands out alongside the request address.
    /// The request lane is freed on flush (send), independently of when the reply arrives.
    /// </summary>
    struct LightRequest : IRequest
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

        internal LightRequest(PoolEntry entry, int length)
        {
            this.Entry = entry;
            this.length = length;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Entry == null)
                return;

            Entry.Dispose();
        }
    }
}