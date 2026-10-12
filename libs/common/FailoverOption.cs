// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;

namespace Garnet.common
{
    /// <summary>
    /// Failover option flags
    /// </summary>
    public enum FailoverOption : byte
    {
        // IMPORTANT: Any changes to the values of this enum should be reflected in its parser (SessionParseStateExtensions.TryGetFailoverOption)

        /// <summary>
        /// Internal use only
        /// </summary>
        DEFAULT,
        /// <summary>
        /// Internal use only
        /// </summary>
        INVALID,

        /// <summary>
        /// Failover endpoint input marker
        /// </summary>
        TO,
        /// <summary>
        /// Force failover flag
        /// </summary>
        FORCE,
        /// <summary>
        /// Issue abort of ongoing failover
        /// </summary>
        ABORT,
        /// <summary>
        /// Timeout marker
        /// </summary>
        TIMEOUT,
        /// <summary>
        /// Issue takeover without consensus to replica
        /// </summary>
        TAKEOVER
    }

    /// <summary>
    /// Utils for info command
    /// </summary>
    public static class FailoverUtils
    {
        static readonly byte[][] failoverOptions = [.. Enum.GetValues<FailoverOption>().Select(x => Encoding.ASCII.GetBytes(x.ToString()))];

        /// <summary>
        /// Return the raw option-name bytes for a failover option. The client argument writer frames each
        /// argument as a RESP bulk string exactly once, so the bytes returned here must be unframed.
        /// </summary>
        /// <param name="failoverOption"></param>
        /// <returns></returns>
        public static byte[] GetFailoverOptionBytes(FailoverOption failoverOption)
            => failoverOptions[(int)failoverOption];
    }
}