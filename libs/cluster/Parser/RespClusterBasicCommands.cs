// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        private bool NetworkClusterBumpEpoch(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;
            // Check admin permissions for command
            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            // Process BUMPEPOCH
            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterForget(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;
            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting 1 or 2 arguments
            if (count is < 1 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;

            // Parse Node-Id
            if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var expirySeconds = 60;
            if (count == 2)
            {
                // [Optional] Parse expiry in seconds 
                if (!RespReadUtils.ReadIntWithLengthHeader(out expirySeconds, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }
            readHead = (int)(ptr - recvBufferPtr);

            logger?.LogTrace("CLUSTER FORGET {nodeid} {seconds}", nodeid, expirySeconds);
            if (!clusterProvider.clusterManager.TryRemoveWorker(nodeid, expirySeconds, out var errorMessage))
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

        /// <summary>
        /// Implements CLUSTER INFO command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterInfo(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            var clusterInfo = clusterProvider.clusterManager.GetInfo();
            while (!RespWriteUtils.WriteAsciiBulkString(clusterInfo, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER HELP command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterHelp(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMeet(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;
            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 2 arguments
            if (count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var ipaddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadIntWithLengthHeader(out var port, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var ipaddressStr = Encoding.ASCII.GetString(ipaddress);
            logger?.LogTrace("CLUSTER MEET {ipaddressStr} {port}", ipaddressStr, port);
            clusterProvider.clusterManager.RunMeetTask(ipaddressStr, port);
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER MYID command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMyid(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            while (!RespWriteUtils.WriteAsciiBulkString(clusterProvider.clusterManager.CurrentConfig.LocalNodeId, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER MYPARENTID command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterMyParentId(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);

            var current = clusterProvider.clusterManager.CurrentConfig;
            var parentId = current.LocalNodeRole == NodeRole.PRIMARY ? current.LocalNodeId : current.LocalNodePrimaryId;
            while (!RespWriteUtils.WriteAsciiBulkString(parentId, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER ENDPOINT command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterEndpoint(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 arguments
            if (count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);
            var current = clusterProvider.clusterManager.CurrentConfig;
            var (host, port) = current.GetEndpointFromNodeId(nodeid);
            while (!RespWriteUtils.WriteAsciiBulkString($"{host}:{port}", ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Implements CLUSTER NODES command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterNodes(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            var nodes = clusterProvider.clusterManager.CurrentConfig.GetClusterInfo();
            while (!RespWriteUtils.WriteAsciiBulkString(nodes, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SET-CONFIG-EPOCH command
        /// </summary>
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSetConfigEpoch(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 1 arguments
            if (count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var configEpoch, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterShards(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            var shardsInfo = clusterProvider.clusterManager.CurrentConfig.GetShardsInfo();
            while (!RespWriteUtils.WriteAsciiDirect(shardsInfo, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER GOSSIP command
        /// </summary>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterGossip(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 1 or 2 arguments
            if (count is < 1 or > 2)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            var gossipWithMeet = false;
            if (count > 1)
            {
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var withMeet, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                Debug.Assert(withMeet.SequenceEqual(CmdStrings.WITHMEET.ToArray()));
                if (withMeet.SequenceEqual(CmdStrings.WITHMEET.ToArray()))
                    gossipWithMeet = true;
            }

            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var gossipMessage, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterReset(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting 0, 1 or 2 arguments
            if (count > 2)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            var soft = true;
            var expirySeconds = 60;

            if (count > 0)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var option, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                soft = option.Equals("SOFT", StringComparison.OrdinalIgnoreCase);
            }

            if (count > 1)
            {
                if (!RespReadUtils.ReadIntWithLengthHeader(out expirySeconds, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }
            readHead = (int)(ptr - recvBufferPtr);

            var resp = clusterProvider.clusterManager.TryReset(soft, expirySeconds);
            if (!soft) clusterProvider.FlushDB(true);

            while (!RespWriteUtils.WriteDirect(resp, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}