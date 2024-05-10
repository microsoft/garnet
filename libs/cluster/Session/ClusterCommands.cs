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

        private bool ProcessClusterCommands(ReadOnlySpan<byte> bufSpan, int count)
        {
            var parseSuccess = true;
            if (count > 0)
            {
                var invalidParameters = false;
                if (!ParseClusterSubcommand(bufSpan, out var subcmd)) return false;
                switch (subcmd)
                {
                    case ClusterSubcommand.BUMPEPOCH:
                        parseSuccess = NetworkClusterBumpEpoch(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.FORGET:
                        parseSuccess = NetworkClusterForget(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.INFO:
                        parseSuccess = NetworkClusterInfo(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.HELP:
                        parseSuccess = NetworkClusterHelp(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.MEET:
                        parseSuccess = NetworkClusterMeet(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.MYID:
                        parseSuccess = NetworkClusterMyid(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.MYPARENTID:
                        parseSuccess = NetworkClusterMyParentId(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.ENDPOINT:
                        parseSuccess = NetworkClusterEndpoint(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.NODES:
                        parseSuccess = NetworkClusterNodes(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SETCONFIGEPOCH:
                        parseSuccess = NetworkClusterSetConfigEpoch(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SHARDS:
                        parseSuccess = NetworkClusterShards(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.GOSSIP:
                        parseSuccess = NetworkClusterGossip(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.RESET:
                        parseSuccess = NetworkClusterReset(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.FAILOVER:
                        parseSuccess = NetworkClusterFailover(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.FAILAUTHREQ:
                        parseSuccess = NetworkClusterFailAuthReq(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.FAILSTOPWRITES:
                        parseSuccess = NetworkClusterFailStopWrites(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.FAILREPLICATIONOFFSET:
                        parseSuccess = NetworkClusterFailReplicationOffset(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.ADDSLOTS:
                        parseSuccess = NetworkClusterAddSlots(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.ADDSLOTSRANGE:
                        parseSuccess = NetworkClusterAddSlotsRange(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.BANLIST:
                        parseSuccess = NetworkClusterBanList(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.COUNTKEYSINSLOT:
                        parseSuccess = NetworkClusterCountKeysInSlot(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.DELSLOTS:
                        parseSuccess = NetworkClusterDelSlots(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.DELSLOTSRANGE:
                        parseSuccess = NetworkClusterDelSlotsRange(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.DELKEYSINSLOT:
                        parseSuccess = NetworkClusterDelKeysInSlot(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.DELKEYSINSLOTRANGE:
                        parseSuccess = NetworkClusterDelKeysInSlotRange(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.GETKEYSINSLOT:
                        parseSuccess = NetworkClusterGetKeysInSlot(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.KEYSLOT:
                        parseSuccess = NetworkClusterKeySlot(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SETSLOT:
                        parseSuccess = NetworkClusterSetSlot(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SETSLOTSRANGE:
                        parseSuccess = NetworkClusterSetSlotsRange(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SLOTS:
                        parseSuccess = NetworkClusterSlots(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SLOTSTATE:
                        parseSuccess = NetworkClusterSlotState(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.MIGRATE:
                        parseSuccess = NetworkClusterMigrate(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.MTASKS:
                        parseSuccess = NetworkClusterMTasks(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.REPLICAS:
                        parseSuccess = NetworkClusterReplicas(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.REPLICATE:
                        parseSuccess = NetworkClusterReplicate(bufSpan, count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.AOFSYNC:
                        parseSuccess = NetworkClusterAOFSync(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.APPENDLOG:
                        parseSuccess = NetworkClusterAppendLog(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.INITIATE_REPLICA_SYNC:
                        parseSuccess = NetworkClusterInitiateReplicaSync(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SEND_CKPT_METADATA:
                        parseSuccess = NetworkClusterSendCheckpointMetadata(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.SEND_CKPT_FILE_SEGMENT:
                        parseSuccess = NetworkClusterSendCheckpointFileSegment(count - 1, out invalidParameters);
                        break;
                    case ClusterSubcommand.BEGIN_REPLICA_RECOVER:
                        parseSuccess = NetworkClusterBeginReplicaRecover(count - 1, out invalidParameters);
                        break;
                    default:
                        if (!DrainCommands(bufSpan, count - 1))
                            return false;
                        while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for '{subcmd}'. Try CLUSTER HELP.", ref dcurr, dend))
                            SendAndReset();
                        break;

                }

                if (invalidParameters)
                {
                    if (!DrainCommands(bufSpan, count - 1))
                        return false;
                    var errorMsg = string.Format(CmdStrings.GenericErrMissingParam, subcmd.ToString());
                    while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return parseSuccess;
        }
    }
}