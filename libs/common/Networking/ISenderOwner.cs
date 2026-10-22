// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Owns the connection a <see cref="GarnetTcpNetworkSender"/> sends on, and ends it together with the session
    /// the connection serves.
    /// </summary>
    internal interface ISenderOwner
    {
        /// <summary>
        /// Called once per stall, on a timer thread, so it must not block. It must end the connection.
        /// </summary>
        /// <param name="timeout">The send timeout the connection exceeded.</param>
        void OnSendStalled(TimeSpan timeout);

        /// <summary>
        /// Ends the connection on request.
        /// </summary>
        /// <returns>True if the caller is the first to request it.</returns>
        bool TryClose();
    }
}