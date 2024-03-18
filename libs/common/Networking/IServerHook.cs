// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.networking
{
    /// <summary>
    /// Hook for server
    /// </summary>
    public interface IServerHook
    {
        /// <summary>
        /// Try creating a message consumer
        /// </summary>
        /// <param name="bytesReceived"></param>
        /// <param name="networkSender"></param>
        /// <param name="session"></param>
        /// <returns></returns>
        bool TryCreateMessageConsumer(Span<byte> bytesReceived, INetworkSender networkSender, out IMessageConsumer session);

        /// <summary>
        /// Dispose message consumer
        /// </summary>
        /// <param name="session"></param>
        void DisposeMessageConsumer(INetworkHandler session);

        /// <summary>
        /// Check whether server is disposed
        /// </summary>
        bool Disposed { get; }
    }
}