// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.cluster
{
    /// <summary>
    /// Failover status flags
    /// </summary>
    internal enum FailoverStatus : byte
    {
        NO_FAILOVER,
        BEGIN_FAILOVER,
        ISSUING_PAUSE_WRITES,
        WAITING_FOR_SYNC,
        FAILOVER_IN_PROGRESS,
        TAKING_OVER_AS_PRIMARY
    }

    internal static class FailoverUtils
    {
        /// <summary>
        /// Convert failover to string message for info command.
        /// </summary>
        /// <param name="status">Failover status type.</param>
        /// <returns>String message for provided status, otherwise exception is thrown.</returns>
        /// <exception cref="Exception"></exception>
        public static string GetFailoverStatus(FailoverStatus? status)
        {
            return status switch
            {
                FailoverStatus.NO_FAILOVER => "no-failover",
                FailoverStatus.BEGIN_FAILOVER => "begin-failover",
                FailoverStatus.ISSUING_PAUSE_WRITES => "issuing-pause-writes",
                FailoverStatus.WAITING_FOR_SYNC => "waiting-for-sync",
                FailoverStatus.FAILOVER_IN_PROGRESS => "failover-in-progress",
                FailoverStatus.TAKING_OVER_AS_PRIMARY => "taking-over-as-primary",
                _ => throw new Exception("invalid failover status"),
            };
        }
    }
}