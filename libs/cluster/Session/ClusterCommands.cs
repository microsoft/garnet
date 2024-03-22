// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Server session for RESP protocol - cluster commands are in this file
    /// </summary>
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        ClusterConfig lastSentConfig;

        int migrateSetCount = 0;
        byte migrateState = 0;

        private int CountKeysInSessionStore(int slot)
        {
            ClusterKeyIterationFunctions.MainStoreCountKeys iterFuncs = new(slot);
            basicGarnetApi.IterateMainStore(ref iterFuncs);
            return iterFuncs.keyCount;
        }

        private int CountKeysInObjectStore(int slot)
        {
            if (!clusterProvider.serverOptions.DisableObjects)
            {
                ClusterKeyIterationFunctions.ObjectStoreCountKeys iterFuncs = new(slot);
                basicGarnetApi.IterateObjectStore(ref iterFuncs);
                return iterFuncs.keyCount;
            }
            return 0;
        }

        private int CountKeysInSlot(int slot) => CountKeysInSessionStore(slot) + CountKeysInObjectStore(slot);

        private List<byte[]> GetKeysInSlot(int slot, int keyCount)
        {
            List<byte[]> keys = new();
            ClusterKeyIterationFunctions.MainStoreGetKeysInSlot mainIterFuncs = new(keys, slot, keyCount);
            basicGarnetApi.IterateMainStore(ref mainIterFuncs);

            if (!clusterProvider.serverOptions.DisableObjects)
            {
                ClusterKeyIterationFunctions.ObjectStoreGetKeysInSlot objectIterFuncs = new(keys, slot);
                basicGarnetApi.IterateObjectStore(ref objectIterFuncs);
            }
            return keys;
        }

        private bool ParseSlots(int count, ref byte* ptr, out HashSet<int> slots, out ReadOnlySpan<byte> resp, bool range)
        {
            slots = new();
            resp = CmdStrings.RESP_OK;
            bool mRef = false;
            bool outOfRange = false;
            bool invalidRange = false;
            int slotStart;
            int slotEnd;

            while (count > 0)
            {
                if (range)
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out slotStart, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    if (!RespReadUtils.ReadIntWithLengthHeader(out slotEnd, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    count -= 2;
                }
                else
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out slotStart, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    count--;
                    slotEnd = slotStart;
                }

                if (mRef || outOfRange || invalidRange)
                    continue;

                if (slotStart > slotEnd)
                {
                    invalidRange = true;
                    continue;
                }

                if (ClusterConfig.OutOfRange(slotStart) || ClusterConfig.OutOfRange(slotEnd))
                {
                    resp = CmdStrings.RESP_SLOT_OUT_OFF_RANGE;
                    outOfRange = true;
                }

                for (int slot = slotStart; slot <= slotEnd; slot++)
                {
                    if (mRef)
                        continue;
                    if (!slots.Add(slot))
                    {
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slot} specified multiple times\r\n"));
                        mRef = true;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Release epoch, wait for config transition and re-acquire the epoch
        /// </summary>
        public void UnsafeWaitForConfigTransition()
        {
            ReleaseCurrentEpoch();
            clusterProvider.WaitForConfigTransition();
            AcquireCurrentEpoch();
        }

        private bool ProcessClusterCommands(ReadOnlySpan<byte> bufSpan, int count)
        {
            if (clusterProvider.clusterManager == null)
            {
                if (!DrainCommands(bufSpan, count - 1))
                    return false;
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRCLUSTER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            bool errorFlag = false;
            string errorCmd = string.Empty;
            if (count > 1)
            {
                var param = GetCommand(bufSpan, out bool success1);
                if (!success1) return false;

                if (ProcessClusterBasicCommands(bufSpan, param, count, out errorFlag, out errorCmd))
                    goto checkErrorFlags;
                else if (ProcessFailoverCommands(bufSpan, param, count, out errorFlag, out errorCmd))
                    goto checkErrorFlags;
                else if (ProcessSlotManageCommands(bufSpan, param, count, out errorFlag, out errorCmd))
                    goto checkErrorFlags;
                else if (ProcessClusterMigrationCommands(bufSpan, param, count, out errorFlag, out errorCmd))
                    goto checkErrorFlags;
                else if (ProcessClusterReplicationCommands(bufSpan, param, count, out errorFlag, out errorCmd))
                    goto checkErrorFlags;
                else
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    string paramStr = Encoding.ASCII.GetString(param.ToArray());
                    while (!RespWriteUtils.WriteResponse(new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR Unknown subcommand or wrong number of arguments for '" + paramStr + "'. Try CLUSTER HELP.\r\n")), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                errorFlag = true;
                errorCmd = "CLUSTER";
            }

        checkErrorFlags:
            if (errorFlag && !string.IsNullOrWhiteSpace(errorCmd))
            {
                var errorMsg = string.Format(CmdStrings.ErrMissingParam, errorCmd);
                var bresp_ERRMISSINGPARAM = Encoding.ASCII.GetBytes(errorMsg);
                bresp_ERRMISSINGPARAM.CopyTo(new Span<byte>(dcurr, bresp_ERRMISSINGPARAM.Length));
                dcurr += bresp_ERRMISSINGPARAM.Length;
            }
            sessionMetrics?.incr_total_cluster_commands_processed();
            return true;
        }

        private bool ProcessClusterBasicCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
        {
            errorFlag = false;
            errorCmd = string.Empty;
            if (param.SequenceEqual(CmdStrings.BUMPEPOCH) || param.SequenceEqual(CmdStrings.bumpepoch))
            {
                bool success;
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out success))
                {
                    return success;
                }

                if (count > 2)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    success = clusterProvider.clusterManager.TryBumpClusterEpoch();
                    readHead = (int)(ptr - recvBufferPtr);

                    if (success)
                    {
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        var resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Updating the config epoch\r\n"));
                        while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }
            else if (param.SequenceEqual(CmdStrings.FORGET) || param.SequenceEqual(CmdStrings.forget))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count < 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    int expirySeconds = 60;
                    if (count == 4)
                    {
                        if (!RespReadUtils.ReadIntWithLengthHeader(out expirySeconds, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                    }
                    readHead = (int)(ptr - recvBufferPtr);

                    logger?.LogTrace("CLUSTER FORGET {nodeid} {seconds}", nodeid, expirySeconds);
                    var resp = clusterProvider.clusterManager.TryRemoveWorker(nodeid, expirySeconds);

                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.INFO) || param.SequenceEqual(CmdStrings.info))
            {
                if (count > 2)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    readHead = (int)(ptr - recvBufferPtr);
                    var clusterInfo = clusterProvider.clusterManager.GetInfo();
                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(clusterInfo), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.HELP) || param.SequenceEqual(CmdStrings.help))
            {
                var ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                List<string> clusterCommands = ClusterCommandInfo.GetClusterCommands();
                while (!RespWriteUtils.WriteArrayLength(clusterCommands.Count, ref dcurr, dend))
                    SendAndReset();
                foreach (String command in clusterCommands)
                {
                    while (!RespWriteUtils.WriteSimpleString(Encoding.ASCII.GetBytes(command), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.MEET) || param.SequenceEqual(CmdStrings.meet))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count != 4)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    Debug.WriteLine($"{Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, Math.Min(bytesRead, 128))).Replace("\n", "|").Replace("\r", "")}");
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var ipaddress, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (!RespReadUtils.ReadIntWithLengthHeader(out int port, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    var ipaddressStr = Encoding.ASCII.GetString(ipaddress);
                    logger?.LogTrace("CLUSTER MEET {ipaddressStr} {port}", ipaddressStr, port);
                    clusterProvider.clusterManager.RunMeetTask(ipaddressStr, port);
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.MYID) || param.SequenceEqual(CmdStrings.myid))
            {
                var ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(clusterProvider.clusterManager.CurrentConfig.GetLocalNodeId()), ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.MYPARENTID) || param.SequenceEqual(CmdStrings.myparentid))
            {
                var ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);

                var current = clusterProvider.clusterManager.CurrentConfig;
                var parentId = current.GetLocalNodeRole() == NodeRole.PRIMARY ? current.GetLocalNodeId() : current.GetLocalNodePrimaryId();
                while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(parentId), ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.ENDPOINT) || param.SequenceEqual(CmdStrings.endpoint))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);
                var current = clusterProvider.clusterManager.CurrentConfig;
                var (host, port) = current.GetEndpointFromNodeId(nodeid);
                while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes($"{host}:{port}"), ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.NODES) || param.SequenceEqual(CmdStrings.nodes))
            {
                if (count > 2)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    readHead = (int)(ptr - recvBufferPtr);
                    string nodes = clusterProvider.clusterManager.CurrentConfig.GetClusterInfo();
                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(nodes), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.set_config_epoch) || param.SequenceEqual(CmdStrings.SET_CONFIG_EPOCH))
            {
                if (count != 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    Debug.WriteLine($"{Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, Math.Min(bytesRead, 128))).Replace("\n", "|").Replace("\r", "")}");
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadIntWithLengthHeader(out int configEpoch, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);
                    if (clusterProvider.clusterManager.CurrentConfig.NumWorkers > 2)
                    {
                        ReadOnlySpan<byte> resp = "-ERR The user can assign a config epoch only when the node does not know any other node.\r\n"u8;
                        while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        var resp = clusterProvider.clusterManager.TrySetLocalConfigEpoch(configEpoch);
                        while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }
            else if (param.SequenceEqual(CmdStrings.SHARDS) || param.SequenceEqual(CmdStrings.shards))
            {
                var ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                var shardsInfo = clusterProvider.clusterManager.CurrentConfig.GetShardsInfo();
                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes(shardsInfo), ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.GOSSIP))
            {
                if (count < 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    bool gossipWithMeet = false;
                    if (count > 3)
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
                        if (gossipWithMeet || current.IsKnown(other.GetLocalNodeId()))
                        {
                            var updated = clusterProvider.clusterManager.TryMerge(other);
                            // logger?.LogTrace("GOSSIP RECV {nodeid} {host}:{port} {update}", other.GetLocalNodeId(), other.GetLocalNodeIp(), other.GetLocalNodePort(), updated);
                        }
                        else
                            logger?.LogWarning("Received gossip from unknown node: {node-id}", other.GetLocalNodeId());
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
            }
            else if (param.SequenceEqual(CmdStrings.RESET) || param.SequenceEqual(CmdStrings.reset))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                var ptr = recvBufferPtr + readHead;
                bool soft = true;
                int expirySeconds = 60;
                if (count > 2)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var option, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    if (option.ToUpper().Equals("HARD"))
                        soft = false;
                }

                if (count > 3)
                {
                    if (!RespReadUtils.ReadIntWithLengthHeader(out expirySeconds, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }

                readHead = (int)(ptr - recvBufferPtr);

                clusterProvider.clusterManager.TryReset(soft, expirySeconds);
                if (!soft) clusterProvider.FlushDB(true);

                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else { return false; }
            return true;
        }

        public bool ProcessFailoverCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
        {
            errorFlag = false;
            errorCmd = string.Empty;
            if (param.SequenceEqual(CmdStrings.FAILOVER) || param.SequenceEqual(CmdStrings.failover))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count < 2)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    FailoverOption failoverOption = FailoverOption.DEFAULT;
                    TimeSpan failoverTimeout = default;
                    if (count > 2)
                    {
                        if (!RespReadUtils.ReadStringWithLengthHeader(out var failoverOptionStr, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        try
                        {
                            failoverOption = (FailoverOption)Enum.Parse(typeof(FailoverOption), failoverOptionStr.ToUpper());
                        }
                        catch
                        {
                            while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"-ERR Failover option ({failoverOptionStr}) not supported\r\n"), ref dcurr, dend))
                                SendAndReset();
                            failoverOption = FailoverOption.INVALID;
                        }
                    }

                    if (count > 3)
                    {
                        if (!RespReadUtils.ReadIntWithLengthHeader(out var failoverTimeoutSeconds, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        failoverTimeout = TimeSpan.FromSeconds(failoverTimeoutSeconds);
                    }
                    readHead = (int)(ptr - recvBufferPtr);

                    var resp = CmdStrings.RESP_OK;
                    if (clusterProvider.serverOptions.EnableAOF)
                    {
                        if (failoverOption == FailoverOption.ABORT)
                            clusterProvider.failoverManager.TryAbortReplicaFailover();
                        else
                        {
                            var current = clusterProvider.clusterManager.CurrentConfig;
                            var nodeRole = current.GetLocalNodeRole();
                            if (nodeRole == NodeRole.REPLICA)
                            {
                                if (!clusterProvider.failoverManager.TryStartReplicaFailover(failoverOption, failoverTimeout))
                                    resp = Encoding.ASCII.GetBytes($"-ERR failed to start failover for primary({current.GetLocalNodePrimaryAddress()})");
                            }
                            else
                                resp = Encoding.ASCII.GetBytes($"-ERR Node is not a {NodeRole.REPLICA} ~{nodeRole}~");
                        }
                    }
                    else
                    {
                        resp = "-ERR Replica AOF is switched off. Replication unavailable. Please restart replica with --aof option.\r\n"u8;
                    }
                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.failauthreq))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var nodeIdBytes, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var requestEpochBytes, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var claimedSlots, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                var resp = clusterProvider.clusterManager.AuthorizeFailover(
                    Encoding.ASCII.GetString(nodeIdBytes),
                    BitConverter.ToInt64(requestEpochBytes),
                    claimedSlots) ? CmdStrings.RESP_RETURN_VAL_1 : CmdStrings.RESP_RETURN_VAL_0;
                while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.failstopwrites))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var nodeIdBytes, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);
                clusterProvider.clusterManager.TryStopWrites(Encoding.ASCII.GetString(nodeIdBytes));
                UnsafeWaitForConfigTransition();
                while (!RespWriteUtils.WriteInteger(clusterProvider.replicationManager.ReplicationOffset, ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.failreplicationoffset))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadLongWithLengthHeader(out var primaryReplicationOffset, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                var rOffset = clusterProvider.replicationManager.WaitForReplicationOffset(primaryReplicationOffset).GetAwaiter().GetResult();
                while (!RespWriteUtils.WriteInteger(rOffset, ref dcurr, dend))
                    SendAndReset();
            }
            else { return false; }
            return true;
        }

        public bool ProcessSlotManageCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
        {
            errorFlag = false;
            errorCmd = string.Empty;
            if (param.SequenceEqual(CmdStrings.ADDSLOTS) || param.SequenceEqual(CmdStrings.addslots))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count < 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    Debug.WriteLine($"{Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, Math.Min(bytesRead, 128))).Replace("\n", "|").Replace("\r", "")}");
                    var ptr = recvBufferPtr + readHead;
                    if (!ParseSlots(count - 2, ref ptr, out var slots, out var resp, range: false))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    if (resp.SequenceEqual(CmdStrings.RESP_OK))
                    {
                        clusterProvider.clusterManager.TryAddSlots(slots.ToList(), out var slotIndex);
                        if (slotIndex != -1)
                            resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slotIndex} is already busy\r\n"));
                    }

                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.ADDSLOTSRANGE) || param.SequenceEqual(CmdStrings.addslotsrange))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count < 4 || (count & 0x1) == 0x1)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    Debug.WriteLine($"{Encoding.UTF8.GetString(new Span<byte>(recvBufferPtr, Math.Min(bytesRead, 128))).Replace("\n", "|").Replace("\r", "")}");
                    var ptr = recvBufferPtr + readHead;
                    if (!ParseSlots(count - 2, ref ptr, out var slots, out var resp, range: true))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    if (resp.SequenceEqual(CmdStrings.RESP_OK))
                    {
                        clusterProvider.clusterManager.TryAddSlots(slots.ToList(), out var slotIndex);
                        if (slotIndex != -1)
                            resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slotIndex} is already busy\r\n"));
                    }

                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.BANLIST) || param.SequenceEqual(CmdStrings.banlist))
            {
                var ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                var banlist = clusterProvider.clusterManager.GetBanList();

                while (!RespWriteUtils.WriteArrayLength(banlist.Count, ref dcurr, dend))
                    SendAndReset();
                foreach (var replica in banlist)
                {
                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(replica), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.COUNTKEYSINSLOT) || param.SequenceEqual(CmdStrings.countkeysinslot))
            {
                var current = clusterProvider.clusterManager.CurrentConfig;
                if (count != 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    if (ClusterConfig.OutOfRange(slot))
                    {
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (!current.IsLocal((ushort)slot))
                    {
                        Redirect((ushort)slot, current);
                    }
                    else
                    {
                        try
                        {
                            int keyCount = CountKeysInSlot(slot);
                            while (!RespWriteUtils.WriteInteger(keyCount, ref dcurr, dend))
                                SendAndReset();
                        }
                        catch (Exception ex)
                        {
                            logger?.LogError(ex, "Critical error in count keys");
                            int keyCount = -1;
                            while (!RespWriteUtils.WriteInteger(keyCount, ref dcurr, dend))
                                SendAndReset();
                        }
                    }
                }
            }
            else if (param.SequenceEqual(CmdStrings.DELSLOTS) || param.SequenceEqual(CmdStrings.delslots))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count < 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!ParseSlots(count - 2, ref ptr, out var slots, out var resp, range: false))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);
                    if (resp.SequenceEqual(CmdStrings.RESP_OK))
                    {
                        clusterProvider.clusterManager.TryRemoveSlots(slots.ToList(), out var slotIndex);
                        if (slotIndex != -1)
                            resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slotIndex} is already not assigned\r\n"));
                    }

                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.DELSLOTSRANGE) || param.SequenceEqual(CmdStrings.delslotsrange))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                //CLUSTER ADDSLOTSRANGE [start-slot end-slot] // 2 + [2] even number of arguments
                if (count < 4 || (count & 0x1) == 0x1)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!ParseSlots(count - 2, ref ptr, out var slots, out var resp, range: true))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);
                    if (resp.SequenceEqual(CmdStrings.RESP_OK))
                    {
                        clusterProvider.clusterManager.TryRemoveSlots(slots.ToList(), out var slotIndex);
                        if (slotIndex != -1)
                            resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slotIndex} is already not assigned\r\n"));
                    }

                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.DELKEYSINSLOT) || param.SequenceEqual(CmdStrings.delkeysinslot))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count != 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    var slots = new HashSet<int>() { slot };
                    clusterProvider.clusterManager.DeleteKeysInSlotsFromMainStore(basicGarnetApi, slots);
                    if (!clusterProvider.serverOptions.DisableObjects)
                        clusterProvider.clusterManager.DeleteKeysInSlotsFromObjectStore(basicGarnetApi, slots);

                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.DELKEYSINSLOTRANGE) || param.SequenceEqual(CmdStrings.delkeysinslotrange))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count != 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    //Parse slot ranges
                    if (!ParseSlots(count - 2, ref ptr, out var slots, out var resp, range: true))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    if (resp.SequenceEqual(CmdStrings.RESP_OK))
                    {
                        clusterProvider.clusterManager.DeleteKeysInSlotsFromMainStore(basicGarnetApi, slots);
                        if (!clusterProvider.serverOptions.DisableObjects)
                            clusterProvider.clusterManager.DeleteKeysInSlotsFromObjectStore(basicGarnetApi, slots);
                    }

                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.GETKEYSINSLOT) || param.SequenceEqual(CmdStrings.getkeysinslot))
            {
                var current = clusterProvider.clusterManager.CurrentConfig;
                if (count < 4)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (!RespReadUtils.ReadIntWithLengthHeader(out int keyCount, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    readHead = (int)(ptr - recvBufferPtr);

                    if (ClusterConfig.OutOfRange(slot))
                    {
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (!current.IsLocal((ushort)slot))
                    {
                        Redirect((ushort)slot, current);
                    }
                    else
                    {
                        var keys = GetKeysInSlot(slot, keyCount);
                        int keyCountRet = Math.Min(keys.Count, keyCount);
                        while (!RespWriteUtils.WriteArrayLength(keyCountRet, ref dcurr, dend))
                            SendAndReset();
                        for (int i = 0; i < keyCountRet; i++)
                            while (!RespWriteUtils.WriteBulkString(keys[i], ref dcurr, dend))
                                SendAndReset();
                    }
                }
            }
            else if (param.SequenceEqual(CmdStrings.KEYSLOT) || param.SequenceEqual(CmdStrings.keyslot))
            {
                if (count < 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    byte* keyPtr = null;
                    int ksize = 0;
                    if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    int slot = NumUtils.HashSlot(keyPtr, ksize);
                    while (!RespWriteUtils.WriteInteger(slot, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.SETSLOT) || param.SequenceEqual(CmdStrings.setslot))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count < 4)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    //CLUSTER SETSLOT <slot> IMPORTING <node-id>
                    //CLUSTER SETSLOT <slot> MIGRATING <node-id>
                    //CLUSTER SETSLOT <slot> NODE <node-id>
                    //CLUSTER SETSLOT <slot> STABLE
                    var ptr = recvBufferPtr + readHead;
                    //<slot>
                    if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    //subcommand
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var subcommand, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    SlotState slotState = SlotState.STABLE;
                    try { slotState = (SlotState)Enum.Parse(typeof(SlotState), subcommand); }
                    catch { }

                    string nodeid = null;
                    if (count > 4)
                    {
                        if (!RespReadUtils.ReadStringWithLengthHeader(out nodeid, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                    }
                    readHead = (int)(ptr - recvBufferPtr);

                    ReadOnlySpan<byte> resp = CmdStrings.RESP_OK;
                    if (!ClusterConfig.OutOfRange(slot))
                    {
                        switch (slotState)
                        {
                            case SlotState.STABLE:
                                clusterProvider.clusterManager.ResetSlotState(slot, out resp);
                                break;
                            case SlotState.IMPORTING:
                                clusterProvider.clusterManager.PrepareSlotForImport(slot, nodeid, out resp);
                                break;
                            case SlotState.MIGRATING:
                                clusterProvider.clusterManager.PrepareSlotForMigration(slot, nodeid, out resp);
                                break;
                            case SlotState.NODE:
                                clusterProvider.clusterManager.PrepareSlotForOwnershipChange(slot, nodeid, out resp);
                                break;
                            default:
                                resp = Encoding.ASCII.GetBytes($"-ERR Slot state {subcommand} not supported.\r\n");
                                while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                                    SendAndReset();
                                break;
                        }

                        if (resp.SequenceEqual(CmdStrings.RESP_OK)) UnsafeWaitForConfigTransition();

                        while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        resp = CmdStrings.RESP_SLOT_OUT_OFF_RANGE;
                        while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }
            else if (param.SequenceEqual(CmdStrings.SETSLOTSRANGE) || param.SequenceEqual(CmdStrings.setslotsrange))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                if (count < 5)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    // CLUSTER SETSLOTRANGE IMPORTING <source-node-id> <slot-start> <slot-end> [slot-start slot-end]
                    // CLUSTER SETSLOTRANGE MIGRATING <destination-node-id> <slot-start> <slot-end> [slot-start slot-end]
                    // CLUSTER SETSLOTRANGE NODE <node-id> <slot-start> <slot-end> [slot-start slot-end]
                    // CLUSTER SETSLOTRANGE STABLE <slot-start> <slot-end> [slot-start slot-end]

                    SlotState slotState;
                    string nodeid = default;
                    int _count = count - 3;
                    var ptr = recvBufferPtr + readHead;
                    // Extract subcommand
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var subcommand, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    // Parse slot state
                    try { slotState = (SlotState)Enum.Parse(typeof(SlotState), subcommand); }
                    catch (Exception ex)
                    {
                        // Log error for invalid slot state option
                        logger?.LogError(ex, "");
                        if (!DrainCommands(bufSpan, count - 3))
                            return false;
                        errorFlag = true;
                        errorCmd = Encoding.ASCII.GetString(param.ToArray());
                        return true;
                    }

                    //Extract nodeid for operations other than stable
                    if (slotState != SlotState.STABLE)
                    {
                        if (!RespReadUtils.ReadStringWithLengthHeader(out nodeid, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        _count = count - 4;
                    }

                    //Parse slot ranges
                    if (!ParseSlots(_count, ref ptr, out var slots, out var resp, range: true))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    //Execute
                    if (resp.SequenceEqual(CmdStrings.RESP_OK))
                    {
                        switch (slotState)
                        {
                            case SlotState.STABLE:
                                clusterProvider.clusterManager.ResetSlotsState(slots, out resp);
                                break;
                            case SlotState.IMPORTING:
                                clusterProvider.clusterManager.PrepareSlotsForImport(slots, nodeid, out resp);
                                break;
                            case SlotState.MIGRATING:
                                clusterProvider.clusterManager.PrepareSlotsForMigration(slots, nodeid, out resp);
                                break;
                            case SlotState.NODE:
                                clusterProvider.clusterManager.PrepareSlotsForOwnershipChange(slots, nodeid, out resp);
                                break;
                            default:
                                resp = Encoding.ASCII.GetBytes($"-ERR Slot state {subcommand} not supported.\r\n");
                                while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                                    SendAndReset();
                                break;
                        }
                    }

                    if (resp.SequenceEqual(CmdStrings.RESP_OK)) UnsafeWaitForConfigTransition();
                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.SLOTS) || param.SequenceEqual(CmdStrings.slots))
            {
                var ptr = recvBufferPtr + readHead;
                readHead = (int)(ptr - recvBufferPtr);
                var slotsInfo = clusterProvider.clusterManager.CurrentConfig.GetSlotsInfo();
                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes(slotsInfo), ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.SLOTSTATE) || param.SequenceEqual(CmdStrings.slotstate))
            {
                //CLUSTER SLOTSTATE <slot>
                if (count < 3)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    //<slot>
                    if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    var current = clusterProvider.clusterManager.CurrentConfig;
                    var nodeId = current.GetNodeIdFromSlot((ushort)slot);
                    var state = current.GetState((ushort)slot);
                    var stateStr = state switch
                    {
                        SlotState.STABLE => "=",
                        SlotState.IMPORTING => "<",
                        SlotState.MIGRATING => ">",
                        SlotState.OFFLINE => "-",
                        SlotState.FAIL => "-",
                        _ => throw new Exception($"Invalid SlotState filetype {state}"),
                    };
                    while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"+{slot} {stateStr} {nodeId}\r\n"), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else { return false; }
            return true;
        }

        public bool ProcessClusterMigrationCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
        {
            errorFlag = false;
            errorCmd = string.Empty;

            if (param.SequenceEqual(CmdStrings.MIGRATE))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                //CLUSTER MIGRATE <node-id> <slot> <number-of-keys-in-slot> <serialized-data>
                if (count != 5)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var sourceNodeId, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (!RespReadUtils.ReadStringWithLengthHeader(out var _replace, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (!RespReadUtils.ReadStringWithLengthHeader(out var storeType, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    bool replaceOption = _replace.Equals("T");

                    // Check if payload size has been received
                    if (ptr + 4 > recvBufferPtr + bytesRead)
                        return false;

                    int headerLength = *(int*)ptr;
                    ptr += 4;
                    // Check if payload has been received
                    if (ptr + headerLength > recvBufferPtr + bytesRead)
                        return false;

                    if (storeType.Equals("SSTORE"))
                    {
                        int keyCount = *(int*)ptr;
                        ptr += 4;
                        int i = 0;

                        while (i < keyCount)
                        {

                            byte* keyPtr = null, valPtr = null;
                            byte keyMetaDataSize = 0, valMetaDataSize = 0;
                            if (!RespReadUtils.ReadSerializedSpanByte(ref keyPtr, ref keyMetaDataSize, ref valPtr, ref valMetaDataSize, ref ptr, recvBufferPtr + bytesRead))
                                return false;

                            ref SpanByte key = ref SpanByte.Reinterpret(keyPtr);
                            if (keyMetaDataSize > 0) key.ExtraMetadata = *(long*)(keyPtr + 4);
                            ref SpanByte value = ref SpanByte.Reinterpret(valPtr);
                            if (valMetaDataSize > 0) value.ExtraMetadata = *(long*)(valPtr + 4);

                            //An error has occurred
                            if (migrateState > 0)
                                continue;

                            var slot = NumUtils.HashSlot(key.ToPointer(), key.LengthWithoutMetadata);
                            if (!clusterProvider.clusterManager.IsImporting(slot))//Slot is not in importing state
                            {
                                migrateState = 1;
                                continue;
                            }

                            if (i < migrateSetCount)
                                continue;

                            migrateSetCount++;

                            // Set if key replace flag is set or key does not exist
                            if (replaceOption || !CheckIfKeyExists(new ArgSlice(key.ToPointer(), key.Length)))
                                _ = basicGarnetApi.SET(ref key, ref value);
                            i++;
                        }
                    }
                    else if (storeType.Equals("OSTORE"))
                    {
                        int keyCount = *(int*)ptr;
                        ptr += 4;
                        int i = 0;
                        while (i < keyCount)
                        {
                            if (!RespReadUtils.ReadSerializedData(out var key, out var data, out var expiration, ref ptr, recvBufferPtr + bytesRead))
                                return false;

                            //An error has occurred
                            if (migrateState > 0)
                                continue;

                            var slot = NumUtils.HashSlot(key);
                            if (!clusterProvider.clusterManager.IsImporting(slot))//Slot is not in importing state
                            {
                                migrateState = 1;
                                continue;
                            }

                            if (i < migrateSetCount)
                                continue;

                            migrateSetCount++;

                            var value = clusterProvider.storeWrapper.DeserializeGarnetObject(data);
                            value.Expiration = expiration;

                            // Set if key replace flag is set or key does not exist
                            if (replaceOption || !CheckIfKeyExists(key))
                                _ = basicGarnetApi.SET(key, value);

                            i++;
                        }
                    }
                    else
                    {
                        throw new Exception("CLUSTER MIGRATE STORE TYPE ERROR!");
                    }

                    var resp = CmdStrings.RESP_OK;
                    if (migrateState == 1)
                        resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Node not in IMPORTING state.\r\n"));

                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();

                    migrateSetCount = 0;
                    migrateState = 0;
                    readHead = (int)(ptr - recvBufferPtr);
                }
            }
            else if (param.SequenceEqual(CmdStrings.MTASKS))
            {
                if (count != 2)
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    errorFlag = true;
                    errorCmd = Encoding.ASCII.GetString(param.ToArray());
                }
                else
                {
                    int mtasks = clusterProvider.migrationManager.GetMigrationTaskCount();
                    while (!RespWriteUtils.WriteInteger(mtasks, ref dcurr, dend))
                        SendAndReset();
                    var ptr = recvBufferPtr + readHead;
                    readHead = (int)(ptr - recvBufferPtr);
                }
            }
            else { return false; }
            return true;
        }

        private bool ProcessClusterReplicationCommands(ReadOnlySpan<byte> bufSpan, ReadOnlySpan<byte> param, int count, out bool errorFlag, out string errorCmd)
        {
            errorFlag = false;
            errorCmd = string.Empty;
            if (param.SequenceEqual(CmdStrings.REPLICAS) || param.SequenceEqual(CmdStrings.replicas))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);
                List<string> replicas = clusterProvider.clusterManager.ListReplicas(nodeid);

                while (!RespWriteUtils.WriteArrayLength(replicas.Count, ref dcurr, dend))
                    SendAndReset();
                foreach (var replica in replicas)
                {
                    while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(replica), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.REPLICATE) || param.SequenceEqual(CmdStrings.replicate))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                var ptr = recvBufferPtr + readHead;
                bool background = false;
                if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (count == 4)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var backgroundFlag, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    backgroundFlag = backgroundFlag.ToUpper();
                    if (backgroundFlag.Equals("SYNC"))
                        background = false;
                    else if (backgroundFlag.Equals("ASYNC"))
                        background = true;
                    else
                    {
                        while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"-ERR Invalid CLUSTER REPLICATE FLAG ({backgroundFlag}) not valid\r\n"), ref dcurr, dend))
                            SendAndReset();
                        readHead = (int)(ptr - recvBufferPtr);
                        return true;
                    }
                }
                readHead = (int)(ptr - recvBufferPtr);

                if (!clusterProvider.serverOptions.EnableAOF)
                {
                    while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes("-ERR Replica AOF is switched off. Replication unavailable. Please restart replica with --aof option.\r\n"), ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    var resp = clusterProvider.replicationManager.BeginReplicate(this, nodeid, background, false);
                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.aofsync))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadStringWithLengthHeader(out var nodeid, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadLongWithLengthHeader(out long nextAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                if (clusterProvider.serverOptions.EnableAOF)
                {
                    clusterProvider.replicationManager.TryAddReplicationTask(nodeid, nextAddress, out var aofSyncTaskInfo);
                    var resp = clusterProvider.replicationManager.TryConnectToReplica(nodeid, nextAddress, aofSyncTaskInfo);
                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes("-ERR Primary AOF is switched off. Replication unavailable. Please restart replica with --aof option..\r\n"), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.appendlog))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeId, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadLongWithLengthHeader(out long previousAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadLongWithLengthHeader(out long currentAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.ReadLongWithLengthHeader(out long nextAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                byte* record = null;
                int recordLength = 0;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref record, ref recordLength, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                var currentConfig = clusterProvider.clusterManager.CurrentConfig;
                var localRole = currentConfig.GetLocalNodeRole();
                var primaryId = currentConfig.GetLocalNodePrimaryId();
                if (localRole != NodeRole.REPLICA)
                {
                    // TODO: handle this
                    //while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes("-ERR aofsync node not a replica\r\n"), ref dcurr, dend))
                    //    SendAndReset();
                }
                else if (!primaryId.Equals(nodeId))
                {
                    // TODO: handle this
                    //while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"-ERR aofsync node replicating {primaryId}\r\n"), ref dcurr, dend))
                    //    SendAndReset();
                }
                else
                {
                    clusterProvider.replicationManager.ProcessPrimaryStream(record, recordLength, previousAddress, currentAddress, nextAddress);
                    //while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    //    SendAndReset();
                }
            }
            else if (param.SequenceEqual(CmdStrings.initiate_replica_sync))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadStringWithLengthHeader(out string nodeId, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadStringWithLengthHeader(out string primary_replid, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var cEntryByteArray, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadLongWithLengthHeader(out long replicaAofBeginAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadLongWithLengthHeader(out long replicaAofTailAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                CheckpointEntry remoteEntry = CheckpointEntry.FromByteArray(cEntryByteArray);
                var resp = clusterProvider.replicationManager.BeginReplicaSyncSession(nodeId, primary_replid, remoteEntry, replicaAofBeginAddress, replicaAofTailAddress);
                while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.send_ckpt_metadata))
            {
                var ptr = recvBufferPtr + readHead;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var fileTokenBytes, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadIntWithLengthHeader(out var fileTypeInt, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var checkpointMetadata, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                Guid fileToken = new Guid(fileTokenBytes);
                CheckpointFileType fileType = (CheckpointFileType)fileTypeInt;
                clusterProvider.replicationManager.ProcessCheckpointMetadata(fileToken, fileType, checkpointMetadata);
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.send_ckpt_file_segment))
            {
                var ptr = recvBufferPtr + readHead;
                Span<byte> data = default;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var fileTokenBytes, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadIntWithLengthHeader(out var ckptFileTypeInt, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadLongWithLengthHeader(out var startAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadSpanByteWithLengthHeader(ref data, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadIntWithLengthHeader(out var segmentId, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                readHead = (int)(ptr - recvBufferPtr);
                var fileToken = new Guid(fileTokenBytes);
                CheckpointFileType ckptFileType = (CheckpointFileType)ckptFileTypeInt;

                // Commenting due to high verbosity
                // logger?.LogTrace("send_ckpt_file_segment {fileToken} {ckptFileType} {startAddress} {dataLength}", fileToken, ckptFileType, startAddress, data.Length);
                clusterProvider.replicationManager.recvCheckpointHandler.ProcessFileSegments(segmentId, fileToken, ckptFileType, startAddress, data);
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else if (param.SequenceEqual(CmdStrings.begin_replica_recover))
            {
                var ptr = recvBufferPtr + readHead;

                if (!RespReadUtils.ReadBoolWithLengthHeader(out var recoverMainStoreFromToken, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadBoolWithLengthHeader(out var recoverObjectStoreFromToken, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadBoolWithLengthHeader(out var replayAOF, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadStringWithLengthHeader(out var primary_replid, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var cEntryByteArray, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadLongWithLengthHeader(out var beginAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                if (!RespReadUtils.ReadLongWithLengthHeader(out var tailAddress, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                readHead = (int)(ptr - recvBufferPtr);

                CheckpointEntry entry = CheckpointEntry.FromByteArray(cEntryByteArray);
                long replicationOffset = clusterProvider.replicationManager.BeginReplicaRecover(
                    recoverMainStoreFromToken,
                    recoverObjectStoreFromToken,
                    replayAOF,
                    primary_replid,
                    entry,
                    beginAddress,
                    tailAddress);
                while (!RespWriteUtils.WriteInteger(replicationOffset, ref dcurr, dend))
                    SendAndReset();
            }
            else
                return false;

            return true;
        }
    }
}