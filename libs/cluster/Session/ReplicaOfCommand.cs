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
            if (address.Equals("NO", StringComparison.OrdinalIgnoreCase) &&
                portStr.Equals("ONE", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    if (!clusterProvider.replicationManager.StartRecovery())
                    {
                        logger?.LogError(nameof(TryREPLICAOF) + ": {logMessage}", Encoding.ASCII.GetString(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK));
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    clusterProvider.clusterManager.TryResetReplica();
                    clusterProvider.replicationManager.TryUpdateForFailover();
                    UnsafeBumpAndWaitForEpochTransition();
                }
                finally
                {
                    clusterProvider.replicationManager.SuspendRecovery();
                }
            }
            else
            {
                if (!int.TryParse(portStr, out var port))
                {
                    logger?.LogWarning(nameof(TryREPLICAOF) + " failed to parse port {port}", portStr);
                    while (!RespWriteUtils.WriteError($"ERR REPLICAOF failed to parse port '{portStr}'", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                var primaryId = clusterProvider.clusterManager.CurrentConfig.GetWorkerNodeIdFromAddress(address, port);
                if (primaryId == null)
                {
                    while (!RespWriteUtils.WriteError($"ERR I don't know about node {address}:{port}.", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                else
                {
                    if (!clusterProvider.replicationManager.TryBeginReplicate(this, primaryId, background: false, force: true, out var errorMessage))
                    {
                        while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    return true;
                }
            }

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}