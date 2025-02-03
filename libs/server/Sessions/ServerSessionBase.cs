// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.networking;

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
        /// Publish an update to a key to all the subscribers of the key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public abstract unsafe void Publish(ArgSlice key, ArgSlice value);

        /// <summary>
        /// Publish an update to a key to all the (pattern) subscribers of the key
        /// </summary>
        /// <param name="pattern"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public abstract unsafe void PatternPublish(ArgSlice pattern, ArgSlice key, ArgSlice value);

        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose() => networkSender?.Dispose();
    }
}