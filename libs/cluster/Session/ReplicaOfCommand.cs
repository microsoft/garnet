// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        private bool TryREPLICAOF(int count, byte* ptr)
        {
            if (!RespReadUtils.ReadStringWithLengthHeader(out var address, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadStringWithLengthHeader(out var portStr, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            //Turn of replication and make replica into a primary but do not delete data
            if (address.ToUpper().Equals("NO") && portStr.ToUpper().Equals("ONE"))
            {
                clusterProvider.clusterManager?.TryResetReplica();
                clusterProvider.replicationManager.TryUpdateForFailover();
                UnsafeWaitForConfigTransition();
            }
            else
            {
                int port = -1;
                try
                {
                    port = int.Parse(portStr);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning("TryREPLICAOF {msg}", ex.Message);
                    while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"-ERR REPLICAOF {ex.Message}"), ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                //TODO: Delete data and make this node a replica of the node listening at endpoint
                if (clusterProvider.serverOptions.EnableCluster)
                {
                    var primaryId = clusterProvider.clusterManager.CurrentConfig.GetWorkerNodeIdFromAddress(address, port);
                    if (primaryId == null)
                    {
                        while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"-ERR I don't know about node {address}:{port}.\r\n"), ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    else
                    {
                        var resp = clusterProvider.replicationManager.BeginReplicate(this, primaryId, background: false, force: true);
                        while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                }
                else
                {
                    while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"-ERR REPLICAOF available only when cluster enabled.\r\n"), ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}