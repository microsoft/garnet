// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.client
{
    public sealed partial class GarnetClient
    {
        static readonly Memory<byte> SAVE = "$4\r\nSAVE\r\n"u8.ToArray();
        static readonly Memory<byte> INFO = "$4\r\nINFO\r\n"u8.ToArray();
        static readonly Memory<byte> REPLICAOF = "$9\r\nREPLICAOF\r\n"u8.ToArray();

        /// <summary>
        /// Take a checkpoint of the Garnet instance.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> Save(CancellationToken cancellationToken = default) =>
            await ExecuteForStringResultWithCancellationAsync(SAVE, default(string), token: cancellationToken) == "OK";

        /// <summary>
        /// Take a checkpoint of the Garnet instance.
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void Save(Action<long, string> callback, long context = 0)
            => ExecuteForStringResult(callback, context, SAVE, default(string));

        /// <summary>
        /// Return information and statistics of the server from the corresponding section
        /// </summary>
        /// <param name="infoSection"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<string> Info(InfoMetricsType infoSection = default, CancellationToken cancellationToken = default) =>
            await ExecuteForStringResultWithCancellationAsync(INFO, InfoCommandUtils.GetRespFormattedInfoSection(infoSection), token: cancellationToken);

        /// <summary>
        /// Make the receiving node a replica of the node at the given endpoint
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<string> ReplicaOf(string address, int port, CancellationToken cancellationToken = default)
        {
            List<Memory<byte>> args = new List<Memory<byte>>()
            {
                Encoding.ASCII.GetBytes(address),
                Encoding.ASCII.GetBytes(port.ToString())
            };
            return await ExecuteForStringResultWithCancellationAsync(REPLICAOF, args, cancellationToken);
        }
    }
}