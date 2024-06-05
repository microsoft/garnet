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
        /// <param name="errorMessage">
        /// The ASCII encoded error message if there one of the following conditions is true
        /// <list type="bullet">
        ///   <item>If the same slot is specified multiple times.</item>
        ///   <item>If the slot is out of range.</item>
        /// </list>
        /// otherwise <see langword="default" />
        /// </param>
        /// <returns>A boolean indicating that there was error in parsing of the arguments.</returns>
        /// <remarks>
        /// The error handling is little special for this method because we need to drain all arguments even in the case of error.
        /// <para/>
        /// The <paramref name="errorMessage"/> will only have a generic error message set in the event of duplicate or out of range slot. 
        /// The method will still return <see langword="true" /> in case of such error.
        /// </remarks>
        private bool TryParseSlots(int count, ref byte* ptr, out HashSet<int> slots, out ReadOnlySpan<byte> errorMessage, bool range)
        {
            slots = [];
            errorMessage = default;
            var duplicate = false;
            var outOfRange = false;
            var invalidRange = false;
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

                if (duplicate || outOfRange || invalidRange)
                    continue;

                if (slotStart > slotEnd)
                {
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Invalid range {slotStart} > {slotEnd}!");
                    invalidRange = true;
                    continue;
                }

                if (ClusterConfig.OutOfRange(slotStart) || ClusterConfig.OutOfRange(slotEnd))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE;
                    outOfRange = true;
                }

                for (var slot = slotStart; slot <= slotEnd && !duplicate; slot++)
                {
                    if (!slots.Add(slot))
                    {
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot {slot} specified multiple times");
                        duplicate = true;
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

        /// <summary>
        /// Handle cluster subcommands.
        /// </summary>
        /// <param name="command">Subcommand to execute.</param>
        /// <param name="bufSpan">Remaining parameters in the command buffer.</param>
        /// <param name="count">Number of parameters in teh command buffer</param>
        /// <returns>True if command is fully processed, false if more processesing is needed.</returns>
        private bool ProcessClusterCommands(RespCommand command, ReadOnlySpan<byte> bufSpan, int count)
        {
            bool result = true;
            bool invalidParameters = false;

            switch (command)
            {
                case RespCommand.CLUSTER_ADDSLOTS:
                    result = NetworkClusterAddSlots(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_ADDSLOTSRANGE:
                    result = NetworkClusterAddSlotsRange(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_AOFSYNC:
                    result = NetworkClusterAOFSync(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_APPENDLOG:
                    result = NetworkClusterAppendLog(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_BANLIST:
                    result = NetworkClusterBanList(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_BEGIN_REPLICA_RECOVER:
                    result = NetworkClusterBeginReplicaRecover(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_BUMPEPOCH:
                    result = NetworkClusterBumpEpoch(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_COUNTKEYSINSLOT:
                    result = NetworkClusterCountKeysInSlot(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_DELKEYSINSLOT:
                    result = NetworkClusterDelKeysInSlot(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_DELKEYSINSLOTRANGE:
                    result = NetworkClusterDelKeysInSlotRange(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_DELSLOTS:
                    result = NetworkClusterDelSlots(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_DELSLOTSRANGE:
                    result = NetworkClusterDelSlotsRange(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_ENDPOINT:
                    result = NetworkClusterEndpoint(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_FAILOVER:
                    result = NetworkClusterFailover(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_FAILREPLICATIONOFFSET:
                    result = NetworkClusterFailReplicationOffset(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_FAILSTOPWRITES:
                    result = NetworkClusterFailStopWrites(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_FORGET:
                    result = NetworkClusterForget(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_GOSSIP:
                    result = NetworkClusterGossip(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_GETKEYSINSLOT:
                    result = NetworkClusterGetKeysInSlot(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_HELP:
                    result = NetworkClusterHelp(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_INFO:
                    result = NetworkClusterInfo(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_INITIATE_REPLICA_SYNC:
                    result = NetworkClusterInitiateReplicaSync(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_KEYSLOT:
                    result = NetworkClusterKeySlot(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_MEET:
                    result = NetworkClusterMeet(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_MIGRATE:
                    result = NetworkClusterMigrate(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_MTASKS:
                    result = NetworkClusterMTasks(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_MYID:
                    result = NetworkClusterMyid(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_MYPARENTID:
                    result = NetworkClusterMyParentId(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_NODES:
                    result = NetworkClusterNodes(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_REPLICAS:
                    result = NetworkClusterReplicas(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_REPLICATE:
                    result = NetworkClusterReplicate(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_RESET:
                    result = NetworkClusterReset(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SEND_CKPT_FILE_SEGMENT:
                    result = NetworkClusterSendCheckpointFileSegment(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SEND_CKPT_METADATA:
                    result = NetworkClusterSendCheckpointMetadata(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SETCONFIGEPOCH:
                    result = NetworkClusterSetConfigEpoch(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SETSLOT:
                    result = NetworkClusterSetSlot(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SETSLOTSRANGE:
                    result = NetworkClusterSetSlotsRange(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SHARDS:
                    result = NetworkClusterShards(count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SLOTS:
                    result = NetworkClusterSlots(bufSpan, count, out invalidParameters);
                    break;

                case RespCommand.CLUSTER_SLOTSTATE:
                    result = NetworkClusterSlotState(bufSpan, count, out invalidParameters);
                    break;
            }

            if (invalidParameters)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;

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