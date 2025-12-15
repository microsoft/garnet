// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        public string RemoteNodeId { get; private set; }

        /// <summary>
        /// Implements CLUSTER BUMPEPOCH command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterBumpEpoch(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            // Process BUMPEPOCH
            if (clusterProvider.clusterManager.TryBumpClusterEpoch())
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_CONFIG_UPDATE, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// Implements CLUSTER FORGET command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterForget(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 1 or 2 arguments
            if (parseState.Count is < 1 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            // Parse Node-Id
            var nodeId = parseState.GetString(0);

            var expirySeconds = 60;
            if (parseState.Count == 2)
            {
                // [Optional] Parse expiry in seconds 
                if (!parseState.TryGetInt(1, out expirySeconds))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            logger?.LogTrace("CLUSTER FORGET {nodeid} {seconds}", nodeId, expirySeconds);
            if (!clusterProvider.clusterManager.TryRemoveWorker(nodeId, expirySeconds, out var errorMessage))
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // Terminate any outstanding migration tasks
                _ = clusterProvider.migrationManager.TryRemoveMigrationTask(nodeId);
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER INFO command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterInfo(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var clusterInfo = clusterProvider.clusterManager.GetInfo();
            while (!RespWriteUtils.TryWriteAsciiBulkString(clusterInfo, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER HELP command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterHelp(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var clusterCommands = ClusterCommandInfo.GetClusterCommands();
            while (!RespWriteUtils.TryWriteArrayLength(clusterCommands.Count, ref dcurr, dend))
                SendAndReset();
            foreach (var command in clusterCommands)
            {
                while (!RespWriteUtils.TryWriteSimpleString(command, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER MEET command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMeet(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 2 arguments
            if (parseState.Count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var ipAddress = parseState.GetString(0);
            if (!parseState.TryGetInt(1, out var port))
            {
                while (!RespWriteUtils.TryWriteError(
                           Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrInvalidPort,
                           parseState.GetString(1))), ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            logger?.LogTrace("CLUSTER MEET");
            clusterProvider.clusterManager.RunMeetTask(ipAddress, port);
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER MYID command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMyId(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            while (!RespWriteUtils.TryWriteAsciiBulkString(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER MYPARENTID command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMyParentId(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var current = clusterProvider.clusterManager.CurrentConfig;
            var parentId = current.LocalNodeRole == NodeRole.PRIMARY ? current.LocalNodeId : current.LocalNodePrimaryId;
            while (!RespWriteUtils.TryWriteAsciiBulkString(parentId, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER ENDPOINT command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterEndpoint(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var nodeId = parseState.GetString(0);

            var current = clusterProvider.clusterManager.CurrentConfig;
            var endpoint = current.GetEndpointFromNodeId(nodeId);
            while (!RespWriteUtils.TryWriteAsciiBulkString(endpoint.ToString(), ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Implements CLUSTER NODES command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterNodes(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var nodes = clusterProvider.clusterManager.CurrentConfig.GetClusterInfo(clusterProvider);
            while (!RespWriteUtils.TryWriteAsciiBulkString(nodes, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SET-CONFIG-EPOCH command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSetConfigEpoch(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 arguments
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            if (!parseState.TryGetLong(0, out var configEpoch))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (clusterProvider.clusterManager.CurrentConfig.NumWorkers > 1)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!clusterProvider.clusterManager.TrySetLocalConfigEpoch(configEpoch, out var errorMessage))
                {
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SHARDS command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterShards(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var shardsInfo = clusterProvider.clusterManager.CurrentConfig.GetShardsInfo(clusterProvider.clusterManager.clusterConnectionStore);
            while (!RespWriteUtils.TryWriteAsciiDirect(shardsInfo, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER GOSSIP command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterGossip(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 1 or 2 arguments
            if (parseState.Count is < 1 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            var gossipWithMeet = false;

            var currTokenIdx = 0;
            if (parseState.Count > 1)
            {
                var withMeetSpan = parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                Debug.Assert(withMeetSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHMEET));
                if (withMeetSpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHMEET))
                    gossipWithMeet = true;
            }

            var gossipMessage = parseState.GetArgSliceByRef(currTokenIdx).ToArray();

            clusterProvider.clusterManager.gossipStats.UpdateGossipBytesRecv(gossipMessage.Length);
            var current = clusterProvider.clusterManager.CurrentConfig;

            // Try merge if not just a ping message
            if (gossipMessage.Length > 0)
            {
                var other = ClusterConfig.FromByteArray(gossipMessage);
                // Accept gossip message if it is a gossipWithMeet or node from node that is already known and trusted
                // GossipWithMeet messages are only send through a call to CLUSTER MEET at the remote node
                if (gossipWithMeet || current.IsKnown(other.LocalNodeId))
                {
                    _ = clusterProvider.clusterManager.TryMerge(other);

                    // Remember that this connection is being used for another cluster node to talk to us
                    Debug.Assert(RemoteNodeId is null || RemoteNodeId == other.LocalNodeId, "Node Id shouldn't change once set for a connection");
                    RemoteNodeId = other.LocalNodeId;
                }
                else
                    logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
            }

            // Respond if configuration has changed or gossipWithMeet option is specified
            if (lastSentConfig != current || gossipWithMeet)
            {
                var configByteArray = current.ToByteArray();
                clusterProvider.clusterManager.gossipStats.UpdateGossipBytesSend(configByteArray.Length);
                while (!RespWriteUtils.TryWriteBulkString(configByteArray, ref dcurr, dend))
                    SendAndReset();
                lastSentConfig = current;
            }
            else
            {
                while (!RespWriteUtils.TryWriteBulkString([], ref dcurr, dend))
                    SendAndReset();
            }

            // After each GOSSIP, ensure cluster connections for replication are in a good state
            if (Server is GarnetServerBase garnetServer)
            {
                clusterProvider.replicationManager.EnsureReplication(this, garnetServer.ActiveClusterSessions());
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER RESET command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterReset(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 0, 1 or 2 arguments
            if (parseState.Count > 2)
            {
                invalidParameters = true;
                return true;
            }

            var soft = true;
            var expirySeconds = 60;

            if (parseState.Count > 0)
            {
                var option = parseState.GetArgSliceByRef(0).ReadOnlySpan;
                soft = option.EqualsUpperCaseSpanIgnoringCase("SOFT"u8);
            }

            if (parseState.Count > 1)
            {
                if (!parseState.TryGetInt(1, out expirySeconds))
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            var resp = clusterProvider.clusterManager.TryReset(soft, expirySeconds);
            if (!soft) clusterProvider.FlushDB(true);

            while (!RespWriteUtils.TryWriteDirect(resp, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implement CLUSTER PUBLISH command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterPublish(out bool invalidParameters)
        {
            invalidParameters = false;

            //  CLUSTER PUBLISH|SPUBLISH channel message
            // Expecting exactly 2 arguments
            if (parseState.Count != 2)
            {
                invalidParameters = true;
                return true;
            }

            if (clusterProvider.storeWrapper.subscribeBroker == null)
            {
                while (!RespWriteUtils.TryWriteError("ERR PUBLISH is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            clusterProvider.storeWrapper.subscribeBroker.Publish(parseState.GetArgSliceByRef(0), parseState.GetArgSliceByRef(1));
            return true;
        }
    }
}