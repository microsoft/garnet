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

    public static class ClientTypeUtils
    {
        /// <summary>
        /// Parse client type from span
        /// </summary>
        /// <param name="input">ReadOnlySpan input to parse</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryParseClientType(ReadOnlySpan<byte> input, out ClientType value)
        {
            value = ClientType.Invalid;

            if (input.EqualsUpperCaseSpanIgnoringCase("NORMAL"u8))
                value = ClientType.NORMAL;
            else if (input.EqualsUpperCaseSpanIgnoringCase("MASTER"u8))
                value = ClientType.MASTER;
            else if (input.EqualsUpperCaseSpanIgnoringCase("REPLICA"u8))
                value = ClientType.REPLICA;
            else if (input.EqualsUpperCaseSpanIgnoringCase("PUBSUB"u8))
                value = ClientType.PUBSUB;
            else if (input.EqualsUpperCaseSpanIgnoringCase("SLAVE"u8))
                value = ClientType.SLAVE;

            return value != ClientType.Invalid;
        }
    }
}