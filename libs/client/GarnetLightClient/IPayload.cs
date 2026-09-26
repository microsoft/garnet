// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.client
{
    /// <summary>
    /// A request-lane payload stored in a <see cref="DuplexBackpressureRing{TRequest, TCompletion}"/>.
    /// The ring transfers ownership of the underlying buffer to the sender (flusher), which disposes
    /// it once the bytes have been handed to the network.
    /// </summary>
    internal interface IPayload : IDisposable
    {
        /// <summary>
        /// Backing buffer holding the serialized request bytes.
        /// </summary>
        byte[] Buffer { get; }

        /// <summary>
        /// Number of valid bytes in <see cref="Buffer"/>.
        /// </summary>
        int Length { get; }
    }
}
