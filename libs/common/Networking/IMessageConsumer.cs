// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.networking
{
    /// <summary>
    /// Interface for consumers of messages (from networks), such as sessions
    /// </summary>
    public interface IMessageConsumer : IDisposable
    {
        /// <summary>
        /// Consume the message incoming on the wire
        /// </summary>
        /// <param name="reqBuffer"></param>
        /// <param name="bytesRead"></param>
        /// <returns></returns>
        unsafe int TryConsumeMessages(byte* reqBuffer, int bytesRead);

        /// <summary>
        /// Lower bound on the bytes needed, counted from <paramref name="buffer"/>, to hold the message being received, when the unparsed
        /// bytes say so. A full receive buffer grows toward this size in steps larger than doubling. Zero when unknown.
        /// </summary>
        /// <param name="buffer">Unparsed bytes at the head of a full receive buffer</param>
        /// <param name="bytes">Number of bytes at <paramref name="buffer"/></param>
        unsafe long GetRequiredReceiveBufferSize(byte* buffer, int bytes) => 0;
    }
}