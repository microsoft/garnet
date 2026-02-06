// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Server session for RESP protocol - cluster commands are in this file
    /// </summary>
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        ClusterConfig lastSentConfig;

        private int CountKeysInSlot(int slot)
        {
            ClusterKeyIterationFunctions.CountKeys iterFuncs = new(slot);
            var cursor = 0L;
            _ = basicGarnetApi.IterateStore(ref iterFuncs, ref cursor);
            return iterFuncs.KeyCount;
        }

        private List<byte[]> GetKeysInSlot(int slot, int keyCount)
        {
            List<byte[]> keys = [];
            ClusterKeyIterationFunctions.GetKeysInSlot iterFuncs = new(keys, slot, keyCount);
            var cursor = 0L;
            _ = basicGarnetApi.IterateStore(ref iterFuncs, ref cursor);
            return keys;
        }

        /// <summary>
        /// Try to parse slots
        /// </summary>
        /// <param name="startIdx"></param>
        /// <param name="slots"></param>
        /// <param name="errorMessage">
        /// The ASCII encoded error message if there one of the following conditions is true
        /// <list type="bullet">
        ///   <item>If the same slot is specified multiple times.</item>
        ///   <item>If the slot is out of range.</item>
        /// </list>
        /// otherwise <see langword="default" />
        /// </param>
        /// <param name="range"></param>
        /// <returns>A boolean indicating that there was error in parsing of the arguments.</returns>
        /// <remarks>
        /// The error handling is little special for this method because we need to drain all arguments even in the case of error.
        /// <para/>
        /// The <paramref name="errorMessage"/> will only have a generic error message set in the event of duplicate or out of range slot. 
        /// The method will still return <see langword="true" /> in case of such error.
        /// </remarks>
        private bool TryParseSlots(int startIdx, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, bool range)
        {
            slots = [];
            errorMessage = default;

            var currTokenIdx = startIdx;
            while (currTokenIdx < parseState.Count)
            {
                int slotStart;
                int slotEnd;
                if (range)
                {
                    if (!parseState.TryGetInt(currTokenIdx++, out slotStart) ||
                        !parseState.TryGetInt(currTokenIdx++, out slotEnd))
                    {
                        errorMessage = CmdStrings.RESP_ERR_INVALID_SLOT;
                        return false;
                    }
                }
                else
                {
                    if (!parseState.TryGetInt(currTokenIdx++, out slotStart))
                    {
                        errorMessage = CmdStrings.RESP_ERR_INVALID_SLOT;
                        return false;
                    }

                    slotEnd = slotStart;
                }

                if (slotStart > slotEnd)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Invalid range {slotStart} > {slotEnd}!");
                    return false;
                }

                if (ClusterConfig.OutOfRange(slotStart) || ClusterConfig.OutOfRange(slotEnd))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE;
                    return false;
                }

                for (var slot = slotStart; slot <= slotEnd; slot++)
                {
                    if (!slots.Add(slot))
                    {
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot {slot} specified multiple times");
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Handle cluster subcommands.
        /// </summary>
        /// <param name="command">Subcommand to execute.</param>
        /// <param name="invalidParameters">True if number of parameters is invalid</param>
        /// <returns>True if command is fully processed, false if more processing is needed.</returns>
        private void ProcessClusterCommands(RespCommand command, out bool invalidParameters)
        {
            _ = command switch
            {
                RespCommand.CLUSTER_ADDSLOTS => NetworkClusterAddSlots(out invalidParameters),
                RespCommand.CLUSTER_ADDSLOTSRANGE => NetworkClusterAddSlotsRange(out invalidParameters),
                RespCommand.CLUSTER_ADVANCE_TIME => NetworkClusterAdvanceTime(out invalidParameters),
                RespCommand.CLUSTER_APPENDLOG => NetworkClusterAppendLog(out invalidParameters),
                RespCommand.CLUSTER_ATTACH_SYNC => NetworkClusterAttachSync(out invalidParameters),
                RespCommand.CLUSTER_BANLIST => NetworkClusterBanList(out invalidParameters),
                RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER => NetworkClusterBeginReplicaRecover(out invalidParameters),
                RespCommand.CLUSTER_BUMPEPOCH => NetworkClusterBumpEpoch(out invalidParameters),
                RespCommand.CLUSTER_COUNTKEYSINSLOT => NetworkClusterCountKeysInSlot(out invalidParameters),
                RespCommand.CLUSTER_DELKEYSINSLOT => NetworkClusterDelKeysInSlot(out invalidParameters),
                RespCommand.CLUSTER_DELKEYSINSLOTRANGE => NetworkClusterDelKeysInSlotRange(out invalidParameters),
                RespCommand.CLUSTER_DELSLOTS => NetworkClusterDelSlots(out invalidParameters),
                RespCommand.CLUSTER_DELSLOTSRANGE => NetworkClusterDelSlotsRange(out invalidParameters),
                RespCommand.CLUSTER_ENDPOINT => NetworkClusterEndpoint(out invalidParameters),
                RespCommand.CLUSTER_FAILOVER => NetworkClusterFailover(out invalidParameters),
                RespCommand.CLUSTER_FAILREPLICATIONOFFSET => NetworkClusterFailReplicationOffset(out invalidParameters),
                RespCommand.CLUSTER_FAILSTOPWRITES => NetworkClusterFailStopWrites(out invalidParameters),
                RespCommand.CLUSTER_FLUSHALL => NetworkClusterFlushAll(out invalidParameters),
                RespCommand.CLUSTER_FORGET => NetworkClusterForget(out invalidParameters),
                RespCommand.CLUSTER_GOSSIP => NetworkClusterGossip(out invalidParameters),
                RespCommand.CLUSTER_GETKEYSINSLOT => NetworkClusterGetKeysInSlot(out invalidParameters),
                RespCommand.CLUSTER_HELP => NetworkClusterHelp(out invalidParameters),
                RespCommand.CLUSTER_INFO => NetworkClusterInfo(out invalidParameters),
                RespCommand.CLUSTER_INITIATE_REPLICA_SYNC => NetworkClusterInitiateReplicaSync(out invalidParameters),
                RespCommand.CLUSTER_KEYSLOT => NetworkClusterKeySlot(out invalidParameters),
                RespCommand.CLUSTER_MEET => NetworkClusterMeet(out invalidParameters),
                RespCommand.CLUSTER_MIGRATE => NetworkClusterMigrate(out invalidParameters),
                RespCommand.CLUSTER_MTASKS => NetworkClusterMTasks(out invalidParameters),
                RespCommand.CLUSTER_MYID => NetworkClusterMyId(out invalidParameters),
                RespCommand.CLUSTER_MYPARENTID => NetworkClusterMyParentId(out invalidParameters),
                RespCommand.CLUSTER_NODES => NetworkClusterNodes(out invalidParameters),
                RespCommand.CLUSTER_PUBLISH or RespCommand.CLUSTER_SPUBLISH => NetworkClusterPublish(out invalidParameters),
                RespCommand.CLUSTER_REPLICAS => NetworkClusterReplicas(out invalidParameters),
                RespCommand.CLUSTER_REPLICATE => NetworkClusterReplicate(out invalidParameters),
                RespCommand.CLUSTER_RESET => NetworkClusterReset(out invalidParameters),
                RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT => NetworkClusterSendCheckpointFileSegment(out invalidParameters),
                RespCommand.CLUSTER_SEND_CKPT_METADATA => NetworkClusterSendCheckpointMetadata(out invalidParameters),
                RespCommand.CLUSTER_SETCONFIGEPOCH => NetworkClusterSetConfigEpoch(out invalidParameters),
                RespCommand.CLUSTER_SETSLOT => NetworkClusterSetSlot(out invalidParameters),
                RespCommand.CLUSTER_SETSLOTSRANGE => NetworkClusterSetSlotsRange(out invalidParameters),
                RespCommand.CLUSTER_SHARDS => NetworkClusterShards(out invalidParameters),
                RespCommand.CLUSTER_SLOTS => NetworkClusterSlots(out invalidParameters),
                RespCommand.CLUSTER_SLOTSTATE => NetworkClusterSlotState(out invalidParameters),
                RespCommand.CLUSTER_SYNC => NetworkClusterSync(out invalidParameters),
                _ => throw new Exception($"Unexpected cluster subcommand: {command}")
            };
            this.sessionMetrics?.incr_total_cluster_commands_processed();
        }
    }
}