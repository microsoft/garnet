// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        /// <summary>
        /// Implements CLUSTER ADDSLOTS command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAddSlots(out bool invalidParameters)
        {
            invalidParameters = false;
            
            // Expecting at least 1 slot or at most maximum number of slots
            if (parseState.Count < 1 || parseState.Count >= ClusterConfig.MAX_HASH_SLOT_VALUE)
            {
                invalidParameters = true;
                return false;
            }

            // Try to parse slot ranges.
            var slotsParsed = TryParseSlots(0, out var slots, out var errorMessage, range: false);

            // The slot parsing may give errorMessage even if the methods TryParseSlots true.
            if (!slotsParsed)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Try to to add slots
            if (!clusterProvider.clusterManager.TryAddSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is already busy", ref dcurr, dend))
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
        /// Implements CLUSTER ADDSLOTSRANGE command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAddSlotsRange(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting even number of arguments
            if (parseState.Count == 0 || (parseState.Count & 0x1) != 0)
            {
                invalidParameters = true;
                return false;
            }

            // Try to parse slot ranges.
            var slotsParsed = TryParseSlots(0, out var slots, out var errorMessage, range: true);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (!slotsParsed)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            
            // Try to to add slots
            if (!clusterProvider.clusterManager.TryAddSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is already busy", ref dcurr, dend))
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
        /// Implements CLUSTER BANLIST command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterBanList(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var banlist = clusterProvider.clusterManager.GetBanList();

            while (!RespWriteUtils.WriteArrayLength(banlist.Count, ref dcurr, dend))
                SendAndReset();
            foreach (var replica in banlist)
            {
                while (!RespWriteUtils.WriteAsciiBulkString(replica, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER COUNTKEYSINSLOT command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterCountKeysInSlot(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var current = clusterProvider.clusterManager.CurrentConfig;
            if (!parseState.TryGetInt(0, out var slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (ClusterConfig.OutOfRange(slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!current.IsLocal((ushort)slot))
            {
                Redirect((ushort)slot, current);
            }
            else
            {
                try
                {
                    var keyCount = CountKeysInSlot(slot);
                    while (!RespWriteUtils.WriteInteger(keyCount, ref dcurr, dend))
                        SendAndReset();
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Critical error in count keys");
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_N1, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER DELSLOTS command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelSlots(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting at least 1 slot or at most maximum number of slots
            if (parseState.Count < 1 || parseState.Count >= ClusterConfig.MAX_HASH_SLOT_VALUE)
            {
                invalidParameters = true;
                return false;
            }

            //Try to parse slot ranges.
            var slotsParsed = TryParseSlots(0, out var slots, out var errorMessage, range: false);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (!slotsParsed)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            //Try remove the slots
            if (!clusterProvider.clusterManager.TryRemoveSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is not assigned", ref dcurr, dend))
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
        /// Implements CLUSTER DELSLOTSRANGE command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelSlotsRange(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting even number of arguments
            if (parseState.Count == 0 || (parseState.Count & 0x1) != 0)
            {
                invalidParameters = true;
                return false;
            }

            //Try to parse slot ranges.
            var slotsParsed = TryParseSlots(0, out var slots, out var errorMessage, range: true);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (!slotsParsed)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            //Try remove the slots
            if (!clusterProvider.clusterManager.TryRemoveSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.WriteError($"ERR Slot {slotIndex} is not assigned", ref dcurr, dend))
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
        /// Implements CLUSTER DELKEYSINSLOT command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelKeysInSlot(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            if (!parseState.TryGetInt(0, out var slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var slots = new HashSet<int> { slot };
            ClusterManager.DeleteKeysInSlotsFromMainStore(basicGarnetApi, slots);
            if (!clusterProvider.serverOptions.DisableObjects)
                ClusterManager.DeleteKeysInSlotsFromObjectStore(basicGarnetApi, slots);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER DELKEYSINSLOTRANGE command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelKeysInSlotRange(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting even number of arguments
            if (parseState.Count == 0 || (parseState.Count & 0x1) != 0)
            {
                invalidParameters = true;
                return false;
            }

            //Try to parse slot ranges.
            var slotsParsed = TryParseSlots(0, out var slots, out var errorMessage, range: true);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (!slotsParsed)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            ClusterManager.DeleteKeysInSlotsFromMainStore(basicGarnetApi, slots);
            if (!clusterProvider.serverOptions.DisableObjects)
                ClusterManager.DeleteKeysInSlotsFromObjectStore(basicGarnetApi, slots);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER GETKEYSINSLOT command
        /// </summary>
        /// <returns></returns>
        private bool NetworkClusterGetKeysInSlot(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (parseState.Count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var current = clusterProvider.clusterManager.CurrentConfig;
            if (!parseState.TryGetInt(0, out var slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!parseState.TryGetInt(1, out var keyCount))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (ClusterConfig.OutOfRange(slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!current.IsLocal((ushort)slot))
            {
                Redirect((ushort)slot, current);
            }
            else
            {
                var keys = GetKeysInSlot(slot, keyCount);
                var keyCountRet = Math.Min(keys.Count, keyCount);
                while (!RespWriteUtils.WriteArrayLength(keyCountRet, ref dcurr, dend))
                    SendAndReset();
                for (var i = 0; i < keyCountRet; i++)
                    while (!RespWriteUtils.WriteBulkString(keys[i], ref dcurr, dend))
                        SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER KEYSLOT
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterKeySlot(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyPtr = sbKey.ToPointer();
            var keySize = sbKey.Length;

            int slot = HashSlotUtils.HashSlot(keyPtr, keySize);
            while (!RespWriteUtils.WriteInteger(slot, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SETSLOT command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSetSlot(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting 2 or 3 arguments
            if (parseState.Count is < 2 or > 3)
            {
                invalidParameters = true;
                return true;
            }

            if (!parseState.TryGetInt(0, out var slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var subcommand = parseState.GetString(1);

            if (!Enum.TryParse(subcommand, ignoreCase: true, out SlotState slotState))
                slotState = SlotState.INVALID;

            string nodeId = null;
            if (parseState.Count > 2)
            {
                nodeId = parseState.GetString(2);
            }

            if (!ClusterConfig.OutOfRange(slot))
            {
                // Try to set slot state
                bool setSlotsSucceeded;
                ReadOnlySpan<byte> errorMessage = default;
                switch (slotState)
                {
                    case SlotState.STABLE:
                        setSlotsSucceeded = true;
                        clusterProvider.clusterManager.ResetSlotState(slot);
                        break;
                    case SlotState.IMPORTING:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForImport(slot, nodeId, out errorMessage);
                        break;
                    case SlotState.MIGRATING:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForMigration(slot, nodeId, out errorMessage);
                        break;
                    case SlotState.NODE:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForOwnershipChange(slot, nodeId, out errorMessage);
                        break;
                    default:
                        setSlotsSucceeded = false;
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot state {subcommand} not supported.");
                        break;
                }

                if (setSlotsSucceeded)
                {
                    UnsafeBumpAndWaitForEpochTransition();

                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SETSLOTSRANGE command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSetSlotsRange(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting at least 3 (STABLE + range) arguments.
            if (parseState.Count < 3)
            {
                invalidParameters = true;
                return true;
            }

            // CLUSTER SETSLOTRANGE IMPORTING <source-node-id> <slot-start> <slot-end> [slot-start slot-end]
            // CLUSTER SETSLOTRANGE MIGRATING <destination-node-id> <slot-start> <slot-end> [slot-start slot-end]
            // CLUSTER SETSLOTRANGE NODE <node-id> <slot-start> <slot-end> [slot-start slot-end]
            // CLUSTER SETSLOTRANGE STABLE <slot-start> <slot-end> [slot-start slot-end]
            string nodeId = default;

            // Extract subcommand
            var subcommand = parseState.GetString(0);
            
            // Try parse slot state
            if (!Enum.TryParse(subcommand, out SlotState slotState))
            {
                // Log error for invalid slot state option
                logger?.LogError("The given input '{input}' is not a valid slot state option.", subcommand);
                slotState = SlotState.INVALID;
            }

            // Extract nodeid for operations other than stable
            if (slotState != SlotState.STABLE && slotState != SlotState.INVALID)
            {
                nodeId = parseState.GetString(1);
            }

            // Try to parse slot ranges. The parsing may give errorMessage even if the TryParseSlots returns true.
            var slotsParsed = TryParseSlots(2, out var slots, out var errorMessage, range: true);
            if (!slotsParsed)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Try to set slot states
            bool setSlotsSucceeded;
            switch (slotState)
            {
                case SlotState.STABLE:
                    setSlotsSucceeded = true;
                    clusterProvider.clusterManager.ResetSlotsState(slots);
                    break;
                case SlotState.IMPORTING:
                    setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForImport(slots, nodeId, out errorMessage);
                    break;
                case SlotState.MIGRATING:
                    setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForMigration(slots, nodeId, out errorMessage);
                    break;
                case SlotState.NODE:
                    setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForOwnershipChange(slots, nodeId, out errorMessage);
                    break;
                default:
                    setSlotsSucceeded = false;
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Slot state {subcommand} not supported.");
                    break;
            }

            if (setSlotsSucceeded)
            {
                UnsafeBumpAndWaitForEpochTransition();

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SLOTS command
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSlots(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 argument
            if (parseState.Count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var slotsInfo = clusterProvider.clusterManager.CurrentConfig.GetSlotsInfo();
            while (!RespWriteUtils.WriteAsciiDirect(slotsInfo, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SLOTSTATE
        /// </summary>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSlotState(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 0 arguments
            if (parseState.Count != 1)
            {
                invalidParameters = true;
                return true;
            }

            if (!parseState.TryGetInt(0, out var slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var current = clusterProvider.clusterManager.CurrentConfig;
            var nodeId = current.GetOwnerIdFromSlot((ushort)slot);
            var state = current.GetState((ushort)slot);
            var stateStr = state switch
            {
                SlotState.STABLE => "=",
                SlotState.IMPORTING => "<",
                SlotState.MIGRATING => ">",
                SlotState.OFFLINE => "x",
                SlotState.FAIL => "*",
                _ => throw new Exception($"Invalid SlotState filetype {state}"),
            };
            while (!RespWriteUtils.WriteAsciiDirect($"+{slot} {stateStr} {nodeId}\r\n", ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}