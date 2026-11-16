// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        private bool IsFailoverTimeoutToken(int idx)
            => parseState.TryGetFailoverOption(idx, out var option) && option == FailoverOption.TIMEOUT;

        /// <summary>
        /// Implements CLUSTER FAILOVER command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterFailover(out bool invalidParameters)
        {
            invalidParameters = false;

            // CLUSTER FAILOVER [FORCE | TAKEOVER | ABORT] [TIMEOUT seconds], also accepting the legacy <option> <seconds> form
            if (parseState.Count > 3)
            {
                invalidParameters = true;
                return true;
            }

            var failoverOption = FailoverOption.DEFAULT;
            TimeSpan failoverTimeout = default;
            var currTokenIdx = 0;

            if (currTokenIdx < parseState.Count && !IsFailoverTimeoutToken(currTokenIdx))
            {
                if (!parseState.TryGetFailoverOption(currTokenIdx, out failoverOption) ||
                    failoverOption == FailoverOption.DEFAULT || failoverOption == FailoverOption.INVALID)
                {
                    var failoverOptionStr = parseState.GetString(currTokenIdx);
                    while (!RespWriteUtils.TryWriteError($"ERR Failover option ({failoverOptionStr}) not supported", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
                currTokenIdx++;
            }

            if (currTokenIdx < parseState.Count)
            {
                var hasTimeoutToken = IsFailoverTimeoutToken(currTokenIdx);
                if (hasTimeoutToken)
                    currTokenIdx++;

                // The bare legacy form only accepts the timeout as the final argument after an option
                if (currTokenIdx >= parseState.Count || (!hasTimeoutToken && currTokenIdx != parseState.Count - 1))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (!parseState.TryGetInt(currTokenIdx, out var failoverTimeoutSeconds))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (failoverTimeoutSeconds < 0)
                {
                    while (!RespWriteUtils.TryWriteError("ERR timeout is negative"u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                failoverTimeout = TimeSpan.FromSeconds(failoverTimeoutSeconds);
                currTokenIdx++;
            }

            if (currTokenIdx < parseState.Count)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();
                return true;
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
                            (string address, int port) primaryEndpoint = current.GetWorkerClientAddressFromNodeId(current.LocalNodePrimaryId);
                            while (!RespWriteUtils.TryWriteError($"ERR failed to start failover for primary({primaryEndpoint})", ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }
                    }
                    else
                    {
                        while (!RespWriteUtils.TryWriteError($"ERR Node is not configured as a {NodeRole.REPLICA}", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                }
            }
            else
            {
                // Return error if AOF is not enabled
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Finally return +OK if operation completed without any errors            
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER failstopwrites (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClient.ExecuteClusterFailStopWrites"/>
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
            {// Demote the primary before draining its maintenance and returning the final offset.
                clusterProvider.clusterManager.TryStopWrites(nodeId);
                ReleaseCurrentEpoch();
                try
                {
                    AsyncUtils.BlockingWait(clusterProvider.storeWrapper.SuspendPrimaryOnlyTasksAsync());
                }
                finally
                {
                    AcquireCurrentEpoch();
                }
            }
            else
            {// Reset this node back to its original state
                clusterProvider.clusterManager.TryResetReplica();
                clusterProvider.storeWrapper.StartPrimaryTasks();
            }

            // Cannot avoid blocking here we're on the network thread
            AsyncUtils.BlockingWait(UnsafeBumpAndWaitForEpochTransitionAsync());
            while (!RespWriteUtils.TryWriteAsciiBulkString(clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress.ToString(), ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Implements CLUSTER failreplicationoffset (only for internode use)
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        /// <seealso cref="T:Garnet.client.GarnetClient.ExecuteClusterFailReplicationOffset"/>
        private bool NetworkClusterFailReplicationOffset(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expects exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var primaryReplicationOffset = AofAddress.FromByteArray(parseState.GetArgSliceByRef(0).ToArray());

            // Cannot avoid blocking here we're on the network thread
            var rOffset = AsyncUtils.BlockingWait(clusterProvider.replicationManager.WaitForReplicationOffsetAsync(primaryReplicationOffset));
            while (!RespWriteUtils.TryWriteAsciiBulkString(rOffset.ToString(), ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}