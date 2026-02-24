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
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Try to to add slots
            if (!clusterProvider.clusterManager.TryAddSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Slot {slotIndex} is already busy", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Try to to add slots
            if (!clusterProvider.clusterManager.TryAddSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Slot {slotIndex} is already busy", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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

            while (!RespWriteUtils.TryWriteArrayLength(banlist.Count, ref dcurr, dend))
                SendAndReset();
            foreach (var replica in banlist)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(replica, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (ClusterConfig.OutOfRange(slot))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
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
                    while (!RespWriteUtils.TryWriteInt32(keyCount, ref dcurr, dend))
                        SendAndReset();
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "Critical error in count keys");
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_N1, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            //Try remove the slots
            if (!clusterProvider.clusterManager.TryRemoveSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Slot {slotIndex} is not assigned", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            //Try remove the slots
            if (!clusterProvider.clusterManager.TryRemoveSlots(slots, out var slotIndex) && slotIndex != -1)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Slot {slotIndex} is not assigned", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var slots = new HashSet<int> { slot };
            ClusterManager.DeleteKeysInSlots(basicGarnetApi, slots);

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            ClusterManager.DeleteKeysInSlots(basicGarnetApi, slots);

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
            var preferredType = clusterProvider.serverOptions.ClusterPreferredEndpointType;

            if (!parseState.TryGetInt(0, out var slot))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!parseState.TryGetInt(1, out var keyCount))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (ClusterConfig.OutOfRange(slot))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteArrayLength(keyCountRet, ref dcurr, dend))
                    SendAndReset();
                for (var i = 0; i < keyCountRet; i++)
                    while (!RespWriteUtils.TryWriteBulkString(keys[i], ref dcurr, dend))
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

            var sbKey = parseState.GetArgSliceByRef(0);
            var keyPtr = sbKey.ToPointer();
            var keySize = sbKey.Length;

            int slot = HashSlotUtils.HashSlot(keyPtr, keySize);
            while (!RespWriteUtils.TryWriteInt32(slot, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!parseState.TryGetSlotState(1, out var slotState) || slotState == SlotState.INVALID ||
                slotState == SlotState.OFFLINE)
            {
                var slotStateStr = parseState.GetString(1);
                while (!RespWriteUtils.TryWriteError($"ERR Slot state {slotStateStr} not supported.", ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            string nodeId = null;
            if (parseState.Count > 2)
            {
                nodeId = parseState.GetString(2);
            }

            // Check that node id is only provided for options other than STABLE
            if ((slotState == SlotState.STABLE && nodeId is not null) || (slotState != SlotState.STABLE && nodeId is null))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();

                return true;
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
                        clusterProvider.clusterManager.TryResetSlotState(slot);
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
                        throw new InvalidOperationException($"Unexpected {nameof(SlotState)}: {slotState}");
                }

                if (setSlotsSucceeded)
                {
                    UnsafeBumpAndWaitForEpochTransition();

                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
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

            // Try parse slot state
            if (!parseState.TryGetSlotState(0, out var slotState))
            {
                // Log error for invalid slot state option
                var subcommand = parseState.GetString(0);
                logger?.LogError("The given input '{input}' is not a valid slot state option.", subcommand);
                slotState = SlotState.INVALID;
            }

            if (slotState == SlotState.INVALID)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_STATE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Extract nodeid for operations other than stable
            if (slotState is not SlotState.STABLE and not SlotState.INVALID)
            {
                nodeId = parseState.GetString(1);
            }

            // Try to parse slot ranges. The parsing may give errorMessage even if the TryParseSlots returns true.
            var slotsParsed = TryParseSlots(slotState == SlotState.STABLE ? 1 : 2, out var slots, out var errorMessage, range: true);
            if (!slotsParsed)
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Try to set slot states
            bool setSlotsSucceeded;
            switch (slotState)
            {
                case SlotState.STABLE:
                    setSlotsSucceeded = true;
                    clusterProvider.clusterManager.TryResetSlotState(slots);
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
                    var subcommand = parseState.GetString(0);
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Slot state {subcommand} not supported.");
                    break;
            }

            if (setSlotsSucceeded)
            {
                UnsafeBumpAndWaitForEpochTransition();

                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
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
            var preferredType = clusterProvider.serverOptions.ClusterPreferredEndpointType;
            var slotsInfo = clusterProvider.clusterManager.CurrentConfig.GetSlotsInfo(preferredType);
            while (!RespWriteUtils.TryWriteAsciiDirect(slotsInfo, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_INVALID_SLOT, ref dcurr, dend))
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
            while (!RespWriteUtils.TryWriteAsciiDirect($"+{slot} {stateStr} {nodeId}\r\n", ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}