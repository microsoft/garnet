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
        /// <param name="keyPtr"></param>
        /// <param name="keyLength"></param>
        /// <param name="valPtr"></param>
        /// <param name="valLength"></param>
        /// <param name="inputPtr"></param>
        /// <param name="sid"></param>
        public abstract unsafe void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid);

        /// <summary>
        /// Publish an update to a key to all the (prefix) subscribers of the key
        /// </summary>
        /// <param name="prefixPtr"></param>
        /// <param name="prefixLength"></param>
        /// <param name="keyPtr"></param>
        /// <param name="keyLength"></param>
        /// <param name="valPtr"></param>
        /// <param name="valLength"></param>
        /// <param name="inputPtr"></param>
        /// <param name="sid"></param>
        public abstract unsafe void PrefixPublish(byte* prefixPtr, int prefixLength, ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid);

        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose() => networkSender?.Dispose();
    }
}