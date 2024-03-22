// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.client
{
    public sealed partial class GarnetClient
    {
        /// <summary>
        /// CLUSTER resp formatted
        /// </summary>
        public static readonly Memory<byte> CLUSTER = "$7\r\nCLUSTER\r\n"u8.ToArray();
        static readonly Memory<byte> FAILOVER = "FAILOVER"u8.ToArray();

        /// <summary>
        /// Issue cluster failover command to replica node
        /// </summary>
        /// <param name="failoverOption"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> Failover(FailoverOption failoverOption = default, CancellationToken cancellationToken = default)
        {
            var args = failoverOption == default ?
                new Memory<byte>[] { FAILOVER } :
                new Memory<byte>[] { FAILOVER, FailoverUtils.GetRespFormattedFailoverOption(failoverOption) };
            return await ExecuteForStringResultWithCancellationAsync(CLUSTER, args, token: cancellationToken) == "OK";
        }
    }
}