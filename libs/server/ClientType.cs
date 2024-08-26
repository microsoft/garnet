// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

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

    public static class ClientTypeExtensions
    {
        /// <summary>
        /// Validate that the given <see cref="ClientType"/> is legal, and _could_ have come from the given <see cref="ArgSlice"/>.
        /// 
        /// TODO: Long term we can kill this and use <see cref="IUtf8SpanParsable{ClientType}"/> instead of <see cref="Enum.TryParse{TEnum}(string?, bool, out TEnum)"/>
        /// and avoid extra validation.  See: https://github.com/dotnet/runtime/issues/81500 .
        /// </summary>
        public static bool IsValid(this ClientType type, ref ArgSlice fromSlice)
        {
            return type != ClientType.Invalid && Enum.IsDefined(type) && !fromSlice.ReadOnlySpan.ContainsAnyInRange((byte)'0', (byte)'9');
        }
    }
}
