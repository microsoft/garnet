// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
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
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CONFIG_UPDATE, ref dcurr, dend))
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
                expirySeconds = parseState.GetInt(1);
            }

            logger?.LogTrace("CLUSTER FORGET {nodeid} {seconds}", nodeId, expirySeconds);
            if (!clusterProvider.clusterManager.TryRemoveWorker(nodeId, expirySeconds, out var errorMessage))
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // Terminate any outstanding migration tasks
                _ = clusterProvider.migrationManager.TryRemoveMigrationTask(nodeId);
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
            while (!RespWriteUtils.WriteAsciiBulkString(clusterInfo, ref dcurr, dend))
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
            while (!RespWriteUtils.WriteArrayLength(clusterCommands.Count, ref dcurr, dend))
                SendAndReset();
            foreach (var command in clusterCommands)
            {
                while (!RespWriteUtils.WriteSimpleString(command, ref dcurr, dend))
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
            var port = parseState.GetInt(1);

            logger?.LogTrace("CLUSTER MEET {ipaddressStr} {port}", ipAddress, port);
            clusterProvider.clusterManager.RunMeetTask(ipAddress, port);
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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

            while (!RespWriteUtils.WriteAsciiBulkString(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, ref dcurr, dend))
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
            while (!RespWriteUtils.WriteAsciiBulkString(parentId, ref dcurr, dend))
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
            var (host, port) = current.GetEndpointFromNodeId(nodeId);
            while (!RespWriteUtils.WriteAsciiBulkString($"{host}:{port}", ref dcurr, dend))
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

            var nodes = clusterProvider.clusterManager.CurrentConfig.GetClusterInfo();
            while (!RespWriteUtils.WriteAsciiBulkString(nodes, ref dcurr, dend))
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

            var configEpoch = parseState.GetInt(0);

            if (clusterProvider.clusterManager.CurrentConfig.NumWorkers > 2)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!clusterProvider.clusterManager.TrySetLocalConfigEpoch(configEpoch, out var errorMessage))
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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

            var shardsInfo = clusterProvider.clusterManager.CurrentConfig.GetShardsInfo();
            while (!RespWriteUtils.WriteAsciiDirect(shardsInfo, ref dcurr, dend))
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

            var gossipMessage = parseState.GetArgSliceByRef(currTokenIdx).SpanByte.ToByteArray();

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
                }
                else
                    logger?.LogWarning("Received gossip from unknown node: {node-id}", other.LocalNodeId);
            }

            // Respond if configuration has changed or gossipWithMeet option is specified
            if (lastSentConfig != current || gossipWithMeet)
            {
                var configByteArray = current.ToByteArray();
                clusterProvider.clusterManager.gossipStats.UpdateGossipBytesSend(configByteArray.Length);
                while (!RespWriteUtils.WriteBulkString(configByteArray, ref dcurr, dend))
                    SendAndReset();
                lastSentConfig = current;
            }
            else
            {
                while (!RespWriteUtils.WriteBulkString(Array.Empty<byte>(), ref dcurr, dend))
                    SendAndReset();
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
                expirySeconds = parseState.GetInt(1);
            }

            var resp = clusterProvider.clusterManager.TryReset(soft, expirySeconds);
            if (!soft) clusterProvider.FlushDB(true);

            while (!RespWriteUtils.WriteDirect(resp, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}