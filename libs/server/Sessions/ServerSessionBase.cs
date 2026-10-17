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
        /// The sender this session writes to, for a publisher to wait on when this session's connection is full
        /// </summary>
        internal INetworkSender PublishSender => networkSender;

        /// <summary>
        /// Publish an update to a key to this subscriber, unless its connection is full, in which case nothing
        /// is written and the publisher waits for room.
        /// </summary>
        /// <remarks>
        /// The default publishes as <see cref="Publish"/> does, which is to wait for room inside the call.
        /// </remarks>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns>False if the connection is full and nothing was written</returns>
        internal virtual unsafe bool TryPublish(PinnedSpanByte key, PinnedSpanByte value)
        {
            Publish(key, value);
            return true;
        }

        /// <summary>
        /// Publish an update to a key to this (pattern) subscriber, unless its connection is full, in which case
        /// nothing is written and the publisher waits for room.
        /// </summary>
        /// <remarks>
        /// The default publishes as <see cref="PatternPublish"/> does, which is to wait for room inside the call.
        /// </remarks>
        /// <param name="pattern"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns>False if the connection is full and nothing was written</returns>
        internal virtual unsafe bool TryPatternPublish(PinnedSpanByte pattern, PinnedSpanByte key, PinnedSpanByte value)
        {
            PatternPublish(pattern, key, value);
            return true;
        }

        /// <summary>
        /// Publish an update to a key to this subscriber without waiting for its connection to have room, sending
        /// past its limit on outstanding sends if it has none.
        /// </summary>
        /// <remarks>
        /// For a publisher that may neither wait nor hold the message back. The default publishes as
        /// <see cref="Publish"/> does.
        /// </remarks>
        /// <param name="key"></param>
        /// <param name="value"></param>
        internal virtual unsafe void PublishWithoutWaiting(PinnedSpanByte key, PinnedSpanByte value)
            => Publish(key, value);

        /// <summary>
        /// Publish an update to a key to this (pattern) subscriber without waiting for its connection to have room,
        /// sending past its limit on outstanding sends if it has none.
        /// </summary>
        /// <remarks>
        /// For a publisher that may neither wait nor hold the message back. The default publishes as
        /// <see cref="PatternPublish"/> does.
        /// </remarks>
        /// <param name="pattern"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        internal virtual unsafe void PatternPublishWithoutWaiting(PinnedSpanByte pattern, PinnedSpanByte key, PinnedSpanByte value)
            => PatternPublish(pattern, key, value);

        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose() => networkSender?.Dispose();
    }
}