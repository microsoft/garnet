// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;

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
        /// Consume the message incoming on the wire, allowing the consumer to suspend.
        /// </summary>
        /// <param name="reqBuffer">Pointer to the first unconsumed byte</param>
        /// <param name="bytesRead">Number of unconsumed bytes</param>
        /// <returns>Number of bytes consumed</returns>
        unsafe ValueTask<int> TryConsumeMessagesAsync(byte* reqBuffer, int bytesRead)
            => new(TryConsumeMessages(reqBuffer, bytesRead));
    }
}