// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common;
using Garnet.server;

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
            _ = basicGarnetApi.IterateMainStore(ref iterFuncs);
            return iterFuncs.keyCount;
        }

        private int CountKeysInObjectStore(int slot)
        {
            if (!clusterProvider.serverOptions.DisableObjects)
            {
                ClusterKeyIterationFunctions.ObjectStoreCountKeys iterFuncs = new(slot);
                _ = basicGarnetApi.IterateObjectStore(ref iterFuncs);
                return iterFuncs.keyCount;
            }
            return 0;
        }

        private int CountKeysInSlot(int slot) => CountKeysInSessionStore(slot) + CountKeysInObjectStore(slot);

        private List<byte[]> GetKeysInSlot(int slot, int keyCount)
        {
            List<byte[]> keys = [];
            ClusterKeyIterationFunctions.MainStoreGetKeysInSlot mainIterFuncs = new(keys, slot, keyCount);
            _ = basicGarnetApi.IterateMainStore(ref mainIterFuncs);

            if (!clusterProvider.serverOptions.DisableObjects)
            {
                ClusterKeyIterationFunctions.ObjectStoreGetKeysInSlot objectIterFuncs = new(keys, slot);
                _ = basicGarnetApi.IterateObjectStore(ref objectIterFuncs);
            }
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
        /// <param name="count">Number of parameters in teh command buffer</param>
        /// <returns>True if command is fully processed, false if more processing is needed.</returns>
        private bool ProcessClusterCommands(RespCommand command, int count)
        {
            bool result;
            bool invalidParameters;

            result =
                command switch
                {
                    RespCommand.CLUSTER_ADDSLOTS => NetworkClusterAddSlots(out invalidParameters),
                    RespCommand.CLUSTER_ADDSLOTSRANGE => NetworkClusterAddSlotsRange(out invalidParameters),
                    RespCommand.CLUSTER_AOFSYNC => NetworkClusterAOFSync(count, out invalidParameters),
                    RespCommand.CLUSTER_APPENDLOG => NetworkClusterAppendLog(count, out invalidParameters),
                    RespCommand.CLUSTER_BANLIST => NetworkClusterBanList(out invalidParameters),
                    RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER => NetworkClusterBeginReplicaRecover(count, out invalidParameters),
                    RespCommand.CLUSTER_BUMPEPOCH => NetworkClusterBumpEpoch(count, out invalidParameters),
                    RespCommand.CLUSTER_COUNTKEYSINSLOT => NetworkClusterCountKeysInSlot(out invalidParameters),
                    RespCommand.CLUSTER_DELKEYSINSLOT => NetworkClusterDelKeysInSlot(out invalidParameters),
                    RespCommand.CLUSTER_DELKEYSINSLOTRANGE => NetworkClusterDelKeysInSlotRange(out invalidParameters),
                    RespCommand.CLUSTER_DELSLOTS => NetworkClusterDelSlots(out invalidParameters),
                    RespCommand.CLUSTER_DELSLOTSRANGE => NetworkClusterDelSlotsRange(out invalidParameters),
                    RespCommand.CLUSTER_ENDPOINT => NetworkClusterEndpoint(count, out invalidParameters),
                    RespCommand.CLUSTER_FAILOVER => NetworkClusterFailover(count, out invalidParameters),
                    RespCommand.CLUSTER_FAILREPLICATIONOFFSET => NetworkClusterFailReplicationOffset(count, out invalidParameters),
                    RespCommand.CLUSTER_FAILSTOPWRITES => NetworkClusterFailStopWrites(count, out invalidParameters),
                    RespCommand.CLUSTER_FORGET => NetworkClusterForget(count, out invalidParameters),
                    RespCommand.CLUSTER_GOSSIP => NetworkClusterGossip(count, out invalidParameters),
                    RespCommand.CLUSTER_GETKEYSINSLOT => NetworkClusterGetKeysInSlot(out invalidParameters),
                    RespCommand.CLUSTER_HELP => NetworkClusterHelp(count, out invalidParameters),
                    RespCommand.CLUSTER_INFO => NetworkClusterInfo(count, out invalidParameters),
                    RespCommand.CLUSTER_INITIATE_REPLICA_SYNC => NetworkClusterInitiateReplicaSync(count, out invalidParameters),
                    RespCommand.CLUSTER_KEYSLOT => NetworkClusterKeySlot(out invalidParameters),
                    RespCommand.CLUSTER_MEET => NetworkClusterMeet(count, out invalidParameters),
                    RespCommand.CLUSTER_MIGRATE => NetworkClusterMigrate(count, out invalidParameters),
                    RespCommand.CLUSTER_MTASKS => NetworkClusterMTasks(count, out invalidParameters),
                    RespCommand.CLUSTER_MYID => NetworkClusterMyId(count, out invalidParameters),
                    RespCommand.CLUSTER_MYPARENTID => NetworkClusterMyParentId(count, out invalidParameters),
                    RespCommand.CLUSTER_NODES => NetworkClusterNodes(count, out invalidParameters),
                    RespCommand.CLUSTER_REPLICAS => NetworkClusterReplicas(count, out invalidParameters),
                    RespCommand.CLUSTER_REPLICATE => NetworkClusterReplicate(count, out invalidParameters),
                    RespCommand.CLUSTER_RESET => NetworkClusterReset(count, out invalidParameters),
                    RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT => NetworkClusterSendCheckpointFileSegment(count, out invalidParameters),
                    RespCommand.CLUSTER_SEND_CKPT_METADATA => NetworkClusterSendCheckpointMetadata(count, out invalidParameters),
                    RespCommand.CLUSTER_SETCONFIGEPOCH => NetworkClusterSetConfigEpoch(count, out invalidParameters),
                    RespCommand.CLUSTER_SETSLOT => NetworkClusterSetSlot(out invalidParameters),
                    RespCommand.CLUSTER_SETSLOTSRANGE => NetworkClusterSetSlotsRange(out invalidParameters),
                    RespCommand.CLUSTER_SHARDS => NetworkClusterShards(count, out invalidParameters),
                    RespCommand.CLUSTER_SLOTS => NetworkClusterSlots(out invalidParameters),
                    RespCommand.CLUSTER_SLOTSTATE => NetworkClusterSlotState(out invalidParameters),
                    _ => throw new Exception($"Unexpected cluster subcommand: {command}")
                };

            if (invalidParameters)
            {
                // Have to lookup the RESP name now that we're in the failure case
                string subCommand;
                if (RespCommandsInfo.TryGetRespCommandInfo(command, out var info))
                {
                    subCommand = info.Name.ToLowerInvariant();
                }
                else
                {
                    subCommand = "unknown";
                }

                var errorMsg = string.Format(CmdStrings.GenericErrWrongNumArgs, subCommand);
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }

            return result;
        }
    }
}