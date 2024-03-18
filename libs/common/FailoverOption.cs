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
        static readonly byte[][] infoSections =
            Enum.GetValues(typeof(FailoverOption)).
            Cast<FailoverOption>().
            Select(x => Encoding.ASCII.GetBytes($"${x.ToString().Length}\r\n{x}\r\n")).ToArray();

        /// <summary>
        /// Return resp formatted failover option
        /// </summary>
        /// <param name="failoverOption"></param>
        /// <returns></returns>
        public static byte[] GetRespFormattedFailoverOption(FailoverOption failoverOption)
            => infoSections[(int)failoverOption];
    }
}