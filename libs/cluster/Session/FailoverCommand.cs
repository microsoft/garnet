// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        private bool TryFAILOVER()
        {
            var replicaAddress = string.Empty;
            var replicaPort = 0;
            var timeout = -1;
            var abort = false;
            var force = false;

            var currTokenIdx = 0;
            while (currTokenIdx < parseState.Count)
            {
                if (!parseState.TryGetEnum(currTokenIdx++, true, out FailoverOption failoverOption))
                    failoverOption = FailoverOption.INVALID;

                if (failoverOption == FailoverOption.INVALID)
                    continue;

                switch (failoverOption)
                {
                    case FailoverOption.TO:
                        // 1. Address
                        replicaAddress = parseState.GetString(currTokenIdx++);

                        // 2. Port
                        if (!parseState.TryGetInt(currTokenIdx++, out replicaPort))
                        {
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }
                        break;
                    case FailoverOption.TIMEOUT:
                        if (!parseState.TryGetInt(currTokenIdx++, out timeout))
                        {
                            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }
                        break;
                    case FailoverOption.ABORT:
                        abort = true;
                        break;
                    case FailoverOption.FORCE:
                        force = true;
                        break;
                    default:
                        throw new Exception($"Failover option {failoverOption} not supported");
                }
            }

            if (clusterProvider.clusterManager.CurrentConfig.LocalNodeRole != NodeRole.PRIMARY)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Validate failing over node config
            if (replicaPort != -1 && replicaAddress != string.Empty)
            {
                var replicaNodeId = clusterProvider.clusterManager.CurrentConfig.GetWorkerNodeIdFromAddress(replicaAddress, replicaPort);
                if (replicaNodeId == null)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNKNOWN_ENDPOINT, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                var worker = clusterProvider.clusterManager.CurrentConfig.GetWorkerFromNodeId(replicaNodeId);
                if (worker.Role != NodeRole.REPLICA)
                {
                    while (!RespWriteUtils.WriteError($"ERR Node @{replicaAddress}:{replicaPort} is not a replica.", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (worker.ReplicaOfNodeId != clusterProvider.clusterManager.CurrentConfig.LocalNodeId)
                {
                    while (!RespWriteUtils.WriteError($"ERR Node @{replicaAddress}:{replicaPort} is not my replica.", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            // Try abort ongoing failover
            if (abort)
            {
                clusterProvider.clusterManager.TrySetLocalNodeRole(NodeRole.PRIMARY);
                clusterProvider.failoverManager.TryAbortReplicaFailover();
            }
            else
            {
                var timeoutTimeSpan = timeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(timeout);
                _ = clusterProvider.failoverManager.TryStartPrimaryFailover(replicaAddress, replicaPort, force ? FailoverOption.FORCE : FailoverOption.DEFAULT, timeoutTimeSpan);
            }

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }
    }
}