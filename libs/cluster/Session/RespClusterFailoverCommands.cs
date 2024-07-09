// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        /// <summary>
        /// Implements CLUSTER FAILOVER command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterFailover(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 1 or 2 arguments
            if (count is < 0 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            var failoverOption = FailoverOption.DEFAULT;
            TimeSpan failoverTimeout = default;
            if (count > 0)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var failoverOptionStr, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Try to parse failover option
                if (!Enum.TryParse(failoverOptionStr, ignoreCase: true, out failoverOption))
                {
                    // On failure set the invalid flag, write error and continue parsing to drain rest of parameters if any
                    while (!RespWriteUtils.WriteError($"ERR Failover option ({failoverOptionStr}) not supported", ref dcurr, dend))
                        SendAndReset();
                    failoverOption = FailoverOption.INVALID;
                }

                if (count > 1)
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out var failoverTimeoutSeconds, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    failoverTimeout = TimeSpan.FromSeconds(failoverTimeoutSeconds);
                }
            }
            readHead = (int)(ptr - recvBufferPtr);

            // If option provided is invalid return early
            if (failoverOption == FailoverOption.INVALID)
                return true;

            if (clusterProvider.serverOptions.EnableAOF)
            {
                if (failoverOption == FailoverOption.ABORT)
                {
                    clusterProvider.failoverManager.TryAbortReplicaFailover();
                }
                else
                {
                    var current = clusterProvider.clusterManager.CurrentConfig;
                    // Make local node configuration indicates that this a replica with a configured primary
                    if (current.IsReplica && current.LocalNodePrimaryId != null)
                    {
                        if (!clusterProvider.failoverManager.TryStartReplicaFailover(failoverOption, failoverTimeout))
                        {
                            while (!RespWriteUtils.WriteError($"ERR failed to start failover for primary({current.GetLocalNodePrimaryAddress()})", ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError($"ERR Node is not configured as a {NodeRole.REPLICA}", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                }
            }
            else
            {
                // Return error if AOF is not enabled
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Finally return +OK if operation completed without any errors            
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER failstopwrites (only for internode use)
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterFailStopWrites(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeId, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (!string.IsNullOrEmpty(nodeId))
            {// Make this node a primary after receiving a request from a replica that is trying to takeover
                clusterProvider.clusterManager.TryStopWrites(nodeId);
            }
            else
            {// Reset this node back to its original state
                clusterProvider.clusterManager.TryResetReplica();
            }
            UnsafeBumpAndWaitForEpochTransition();
            while (!RespWriteUtils.WriteInteger(clusterProvider.replicationManager.ReplicationOffset, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Implements CLUSTER failreplicationoffset (only for internode use)
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterFailReplicationOffset(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expects exactly 1 argument
            if (count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadLongWithLengthHeader(out var primaryReplicationOffset, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var rOffset = clusterProvider.replicationManager.WaitForReplicationOffset(primaryReplicationOffset).GetAwaiter().GetResult();
            while (!RespWriteUtils.WriteInteger(rOffset, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}