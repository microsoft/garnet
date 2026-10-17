// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Abstract base class for server session provider
    /// </summary>
    public abstract class ServerSessionBase : IMessageConsumer
    {
        /// <summary>
        /// Bytes read
        /// </summary>
        protected int bytesRead;

        /// <summary>
        /// NetworkSender instance
        /// </summary>
        protected readonly INetworkSender networkSender;

        /// <summary>
        ///  Create instance of session backed by given networkSender
        /// </summary>
        /// <param name="networkSender"></param>
        public ServerSessionBase(INetworkSender networkSender)
        {
            this.networkSender = networkSender;
            bytesRead = 0;
        }

        /// <inheritdoc />
        public abstract unsafe int TryConsumeMessages(byte* req_buf, int bytesRead);

        /// <summary>
        /// Consume the message incoming on the wire, allowing the session to suspend.
        /// </summary>
        /// <remarks>
        /// Declared here rather than left to <see cref="IMessageConsumer"/>'s default implementation so that
        /// the interface slot resolves to this hierarchy. A default interface member is bound at the type that
        /// declares the interface, so an override added further down would otherwise never be dispatched to.
        /// </remarks>
        /// <param name="req_buf">Pointer to the first unconsumed byte.</param>
        /// <param name="bytesRead">Number of unconsumed bytes.</param>
        /// <returns>Number of bytes consumed.</returns>
        public virtual unsafe ValueTask<int> TryConsumeMessagesAsync(byte* req_buf, int bytesRead)
            => new(TryConsumeMessages(req_buf, bytesRead));

        /// <summary>
        /// Publish an update to a key to all the subscribers of the key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public abstract unsafe void Publish(PinnedSpanByte key, PinnedSpanByte value);

        /// <summary>
        /// Publish an update to a key to all the (pattern) subscribers of the key
        /// </summary>
        /// <param name="pattern"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public abstract unsafe void PatternPublish(PinnedSpanByte pattern, PinnedSpanByte key, PinnedSpanByte value);

        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose() => networkSender?.Dispose();
    }
}