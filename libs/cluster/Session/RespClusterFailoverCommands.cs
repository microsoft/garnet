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
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterFailover(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 1 or 2 arguments
            if (parseState.Count is < 0 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            var failoverOption = FailoverOption.DEFAULT;
            TimeSpan failoverTimeout = default;
            if (parseState.Count > 0)
            {
                // Try to parse failover option
                var sbFailoverOption = parseState.GetArgSliceByRef(0).ReadOnlySpan;
                if (!common.FailoverUtils.TryParseFailoverOption(sbFailoverOption, out failoverOption))
                {
                    var failoverOptionStr = parseState.GetString(0);

                    // On failure set the invalid flag, write error and continue parsing to drain rest of parameters if any
                    while (!RespWriteUtils.WriteError($"ERR Failover option ({failoverOptionStr}) not supported",
                               ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                if (parseState.Count > 1)
                {
                    if (!parseState.TryGetInt(1, out var failoverTimeoutSeconds))
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    failoverTimeout = TimeSpan.FromSeconds(failoverTimeoutSeconds);
                }
            }

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
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterFailStopWrites(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);

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
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterFailReplicationOffset(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expects exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            if (!parseState.TryGetLong(0, out var primaryReplicationOffset))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var rOffset = clusterProvider.replicationManager.WaitForReplicationOffset(primaryReplicationOffset).GetAwaiter().GetResult();
            while (!RespWriteUtils.WriteInteger(rOffset, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}