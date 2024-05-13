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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAddSlots(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting at least 1 slot or at most maximum number of slots
            if (count < 1 || count >= ClusterConfig.MAX_HASH_SLOT_VALUE)
            {
                invalidParameters = true;
                return false;
            }

            var ptr = recvBufferPtr + readHead;
            // Try to parse slot ranges.
            var slotsParsed = TryParseSlots(count, ref ptr, out var slots, out var errorMessage, range: false);
            readHead = (int)(ptr - recvBufferPtr);

            // The slot parsing may give errorMessage even if the methods TryParseSlots true.
            if (slotsParsed && errorMessage != default)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            else if (!slotsParsed) return false;

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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterAddSlotsRange(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting even number of arguments
            if (count == 0 || (count & 0x1) != 0)
            {
                invalidParameters = true;
                return false;
            }

            var ptr = recvBufferPtr + readHead;
            // Try to parse slot ranges.
            var slotsParsed = TryParseSlots(count, ref ptr, out var slots, out var errorMessage, range: true);
            readHead = (int)(ptr - recvBufferPtr);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (slotsParsed && errorMessage != default)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            else if (!slotsParsed) return false;

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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterBanList(int count, out bool invalidParameters)
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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterCountKeysInSlot(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 1 argument
            if (count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var current = clusterProvider.clusterManager.CurrentConfig;
            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var slot, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (ClusterConfig.OutOfRange(slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelSlots(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting at least 1 slot or at most maximum number of slots
            if (count < 1 || count >= ClusterConfig.MAX_HASH_SLOT_VALUE)
            {
                invalidParameters = true;
                return false;
            }

            var ptr = recvBufferPtr + readHead;
            //Try to parse slot ranges.
            var slotsParsed = TryParseSlots(count, ref ptr, out var slots, out var errorMessage, range: false);
            readHead = (int)(ptr - recvBufferPtr);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (slotsParsed && errorMessage != default)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            else if (!slotsParsed) return false;

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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelSlotsRange(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting even number of arguments
            if (count == 0 || (count & 0x1) != 0)
            {
                invalidParameters = true;
                return false;
            }

            var ptr = recvBufferPtr + readHead;
            //Try to parse slot ranges.
            var slotsParsed = TryParseSlots(count, ref ptr, out var slots, out var errorMessage, range: true);
            readHead = (int)(ptr - recvBufferPtr);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (slotsParsed && errorMessage != default)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            else if (!slotsParsed) return false;

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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelKeysInSlot(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 1 argument
            if (count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var slot, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            var slots = new HashSet<int>() { slot };
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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterDelKeysInSlotRange(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting even number of arguments
            if (count == 0 || (count & 0x1) != 0)
            {
                invalidParameters = true;
                return false;
            }

            var ptr = recvBufferPtr + readHead;
            //Try to parse slot ranges.
            var slotsParsed = TryParseSlots(count, ref ptr, out var slots, out var errorMessage, range: true);
            readHead = (int)(ptr - recvBufferPtr);

            //The slot parsing may give errorMessage even if the TryParseSlots returns true.
            if (slotsParsed && errorMessage != default)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            else if (!slotsParsed) return false;

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
        private bool NetworkClusterGetKeysInSlot(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 1 argument
            if (count != 2)
            {
                invalidParameters = true;
                return true;
            }

            var current = clusterProvider.clusterManager.CurrentConfig;
            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadIntWithLengthHeader(out int slot, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadIntWithLengthHeader(out int keyCount, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (ClusterConfig.OutOfRange(slot))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE, ref dcurr, dend))
                    SendAndReset();
            }
            else if (!current.IsLocal((ushort)slot))
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
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterKeySlot(int count, out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting exactly 1 argument
            if (count != 1)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            byte* keyPtr = null;
            var ksize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            int slot = NumUtils.HashSlot(keyPtr, ksize);
            while (!RespWriteUtils.WriteInteger(slot, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SETSLOT command
        /// </summary>
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSetSlot(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;
            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting 2 or 3 arguments
            if (count is < 2 or > 3)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var slot, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadStringWithLengthHeader(out var subcommand, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!Enum.TryParse(subcommand, ignoreCase: true, out SlotState slotState))
                slotState = SlotState.INVALID;

            string nodeid = null;
            if (count > 2)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out nodeid, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }
            readHead = (int)(ptr - recvBufferPtr);

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
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForImport(slot, nodeid, out errorMessage);
                        break;
                    case SlotState.MIGRATING:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForMigration(slot, nodeid, out errorMessage);
                        break;
                    case SlotState.NODE:
                        setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotForOwnershipChange(slot, nodeid, out errorMessage);
                        break;
                    default:
                        setSlotsSucceeded = false;
                        errorMessage = Encoding.ASCII.GetBytes($"ERR Slot state {subcommand} not supported.");
                        break;
                }

                if (setSlotsSucceeded)
                {
                    UnsafeWaitForConfigTransition();

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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSetSlotsRange(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting at least 3 (STABLE + range) arguments.
            if (count < 3)
            {
                invalidParameters = true;
                return true;
            }

            // CLUSTER SETSLOTRANGE IMPORTING <source-node-id> <slot-start> <slot-end> [slot-start slot-end]
            // CLUSTER SETSLOTRANGE MIGRATING <destination-node-id> <slot-start> <slot-end> [slot-start slot-end]
            // CLUSTER SETSLOTRANGE NODE <node-id> <slot-start> <slot-end> [slot-start slot-end]
            // CLUSTER SETSLOTRANGE STABLE <slot-start> <slot-end> [slot-start slot-end]
            string nodeid = default;
            var _count = count - 1;
            var ptr = recvBufferPtr + readHead;
            // Extract subcommand
            if (!RespReadUtils.ReadStringWithLengthHeader(out var subcommand, ref ptr, recvBufferPtr + bytesRead))
                return false;

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
                if (!RespReadUtils.ReadStringWithLengthHeader(out nodeid, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                _count = count - 2;
            }

            // Try to parse slot ranges. The parsing may give errorMessage even if the TryParseSlots returns true.
            var slotsParsed = TryParseSlots(_count, ref ptr, out var slots, out var errorMessage, range: true);
            if (slotsParsed && errorMessage != default)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            else if (!slotsParsed) return false;
            readHead = (int)(ptr - recvBufferPtr);

            // Try to set slot states
            bool setSlotsSucceeded;
            switch (slotState)
            {
                case SlotState.STABLE:
                    setSlotsSucceeded = true;
                    clusterProvider.clusterManager.ResetSlotsState(slots);
                    break;
                case SlotState.IMPORTING:
                    setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForImport(slots, nodeid, out errorMessage);
                    break;
                case SlotState.MIGRATING:
                    setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForMigration(slots, nodeid, out errorMessage);
                    break;
                case SlotState.NODE:
                    setSlotsSucceeded = clusterProvider.clusterManager.TryPrepareSlotsForOwnershipChange(slots, nodeid, out errorMessage);
                    break;
                default:
                    setSlotsSucceeded = false;
                    errorMessage = Encoding.ASCII.GetBytes($"ERR Slot state {subcommand} not supported.");
                    break;
            }

            if (setSlotsSucceeded)
            {
                UnsafeWaitForConfigTransition();

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
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSlots(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

            if (!CheckACLAdminPermissions(bufSpan, count, out var success))
            {
                return success;
            }

            // Expecting exactly 0 argument
            if (count != 0)
            {
                invalidParameters = true;
                return true;
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            var slotsInfo = clusterProvider.clusterManager.CurrentConfig.GetSlotsInfo();
            while (!RespWriteUtils.WriteAsciiDirect(slotsInfo, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Implements CLUSTER SLOTSTATE
        /// </summary>
        /// <param name="bufSpan"></param>
        /// <param name="count"></param>
        /// <param name="invalidParameters"></param>
        /// <returns></returns>
        private bool NetworkClusterSlotState(ReadOnlySpan<byte> bufSpan, int count, out bool invalidParameters)
        {
            invalidParameters = false;

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

            var ptr = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadIntWithLengthHeader(out var slot, ref ptr, recvBufferPtr + bytesRead))
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
            while (!RespWriteUtils.WriteAsciiDirect($"+{slot} {stateStr} {nodeId}\r\n", ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}