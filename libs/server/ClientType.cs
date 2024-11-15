// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Type option for CLIENT|LIST and CLIENT|KILL commands.
    /// </summary>
    public enum ClientType : byte
    {
        // IMPORTANT: Any changes to the values of this enum should be reflected in its parser (SessionParseStateExtensions.TryGetClientType)

        /// <summary>
        /// Default invalid case.
        /// </summary>
        Invalid = 0,

        /// <summary>
        /// Normal client connections, including MONITOR parked connections.
        /// </summary>
        NORMAL,
        /// <summary>
        /// Connection from a MASTER cluster node to current node.
        /// </summary>
        MASTER,
        /// <summary>
        /// Connection from a REPLICA cluster node to current node.
        /// </summary>
        REPLICA,
        /// <summary>
        /// Connection which is dedicated to listening for PUBLISH data (Resp2 only).
        /// </summary>
        PUBSUB,
        /// <summary>
        /// Equivalent to <see cref="REPLICA"/>.
        /// 
        /// Separate value as SLAVE is not permitted on new commands, but is still supported
        /// for older commands.
        /// </summary>
        SLAVE,
    }
}