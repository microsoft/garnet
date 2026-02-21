// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        public static bool Expired(ref SpanByte value) => value.MetadataSize == 8 && value.ExtraMetadata < DateTimeOffset.UtcNow.Ticks;

        public static bool Expired(ref IGarnetObject value) => value.Expiration != 0 && value.Expiration < DateTimeOffset.UtcNow.Ticks;

        internal enum MigrateCmdParseState : byte
        {
            SUCCESS,
            CLUSTERDOWN,
            UNKNOWNTARGET,
            MULTISLOTREF,
            SLOTNOTLOCAL,
            CROSSSLOT,
            TARGETNODENOTMASTER,
            INCOMPLETESLOTSRANGE,
            SLOTOUTOFRANGE,
            NOTMIGRATING,
            MULTI_TRANSFER_OPTION,
            FAILEDTOADDKEY,
            HOSTNAME_RESOLUTION_FAILED
        }

        private bool HandleCommandParsingErrors(MigrateCmdParseState mpState, string targetAddress, int targetPort, int slotMultiRef)
        {
            if (mpState is MigrateCmdParseState.SUCCESS)
                return true;

            var errorMessage = mpState switch
            {
                MigrateCmdParseState.CLUSTERDOWN => CmdStrings.RESP_ERR_GENERIC_CLUSTER,
                MigrateCmdParseState.UNKNOWNTARGET => CmdStrings.RESP_ERR_GENERIC_UNKNOWN_ENDPOINT,
                MigrateCmdParseState.MULTISLOTREF => Encoding.ASCII.GetBytes($"ERR Slot {slotMultiRef} specified multiple times."),
                MigrateCmdParseState.SLOTNOTLOCAL => Encoding.ASCII.GetBytes($"ERR slot {slotMultiRef} not owned by current node."),
                MigrateCmdParseState.CROSSSLOT => CmdStrings.RESP_ERR_CROSSSLOT,
                MigrateCmdParseState.TARGETNODENOTMASTER => Encoding.ASCII.GetBytes($"ERR Cannot initiate migration, target node ({targetAddress}:{targetPort}) is not a primary."),
                MigrateCmdParseState.INCOMPLETESLOTSRANGE => CmdStrings.RESP_ERR_GENERIC_INCOMPLETESLOTSRANGE,
                MigrateCmdParseState.SLOTOUTOFRANGE => Encoding.ASCII.GetBytes($"ERR Slot {slotMultiRef} out of range."),
                MigrateCmdParseState.NOTMIGRATING => CmdStrings.RESP_ERR_GENERIC_SLOTNOTMIGRATING,
                MigrateCmdParseState.FAILEDTOADDKEY => CmdStrings.RESP_ERR_GENERIC_FAILEDTOADDKEY,
                MigrateCmdParseState.HOSTNAME_RESOLUTION_FAILED => CmdStrings.RESP_ERR_GENERIC_HOSTNAME_RESOLUTION_FAILED,
                _ => CmdStrings.RESP_ERR_GENERIC_PARSING,
            };
            while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                SendAndReset();
            return false;
        }

        private bool TryMIGRATE(out bool invalidParameters)
        {
            invalidParameters = false;

            // Expecting at least 5 arguments
            if (parseState.Count < 5)
            {
                invalidParameters = true;
                return true;
            }

            // Migrate command format
            // migrate host port <KEY | ""> destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [[KEYS keys] | [SLOTSRANGE start-slot end-slot [start-slot end-slot]]]]
            #region parseMigrationArguments

            // Address
            var targetAddress = parseState.GetString(0);

            // Key
            var keySlice = parseState.GetArgSliceByRef(2);

            // Port, Destination DB, Timeout
            if (!parseState.TryGetInt(1, out var targetPort) ||
                !parseState.TryGetInt(3, out var dbId) ||
                !parseState.TryGetInt(4, out var timeout))

            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var copyOption = false;
            var replaceOption = false;
            string username = null;
            string passwd = null;
            Sketch sketch = null;
            HashSet<int> slots = null;

            ClusterConfig current = null;
            string sourceNodeId = null;
            string targetNodeId = null;
            var pstate = MigrateCmdParseState.CLUSTERDOWN;
            var slotParseError = -1;
            var transferOption = TransferOption.NONE;
            string effectiveAddress = targetAddress;
            if (clusterProvider.serverOptions.EnableCluster)
            {
                pstate = MigrateCmdParseState.SUCCESS;
                current = clusterProvider.clusterManager.CurrentConfig;
                sourceNodeId = current.LocalNodeId;

                // First try to find worker by the provided address (could be IP or hostname)
                targetNodeId = current.GetWorkerNodeIdFromAddressOrHostname(targetAddress, targetPort);

                // If not found directly, check if targetAddress is a hostname and try to resolve it
                if (targetNodeId == null && !IPAddress.TryParse(targetAddress, out _))
                {
                    // targetAddress is not a valid IP, try to resolve it as hostname
                    try
                    {
                        var hostEntry = Dns.GetHostEntry(targetAddress);
                        if (hostEntry.AddressList.Length > 0)
                        {
                            // Try each resolved IP address to find one that matches cluster config
                            foreach (var resolvedIp in hostEntry.AddressList)
                            {
                                effectiveAddress = resolvedIp.ToString();
                                targetNodeId = current.GetWorkerNodeIdFromAddressOrHostname(effectiveAddress, targetPort);
                                if (targetNodeId != null)
                                    break; // Found a match in cluster config
                            }
                        }
                    }
                    catch (System.Net.Sockets.SocketException ex)
                    {
                        // Hostname resolution failed (DNS failure, network issue, etc.)
                        logger?.LogWarning(ex, "Failed to resolve hostname {hostname}", targetAddress);
                        pstate = MigrateCmdParseState.HOSTNAME_RESOLUTION_FAILED;
                    }
                    catch (ArgumentException ex)
                    {
                        // Hostname is invalid (e.g., too long or malformed)
                        logger?.LogWarning(ex, "Failed to resolve hostname {hostname} due to invalid format", targetAddress);
                        pstate = MigrateCmdParseState.HOSTNAME_RESOLUTION_FAILED;
                    }
                    catch (ArgumentOutOfRangeException ex)
                    {
                        // Hostname is invalid (e.g., contains out-of-range characters or length)
                        logger?.LogWarning(ex, "Failed to resolve hostname {hostname} due to out-of-range value", targetAddress);
                        pstate = MigrateCmdParseState.HOSTNAME_RESOLUTION_FAILED;
                    }
                }

                if (targetNodeId == null && pstate == MigrateCmdParseState.SUCCESS)
                    pstate = MigrateCmdParseState.UNKNOWNTARGET;
            }

            // Add single key if specified
            if (keySlice.Length > 0)
            {
                transferOption = TransferOption.KEYS;
                sketch = new();
                sketch.HashAndStore(ref keySlice);
            }

            var currTokenIdx = 5;
            while (currTokenIdx < parseState.Count)
            {
                var option = parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (option.EqualsUpperCaseSpanIgnoringCase("COPY"u8))
                    copyOption = true;
                else if (option.EqualsUpperCaseSpanIgnoringCase("REPLACE"u8))
                    replaceOption = true;
                else if (option.EqualsUpperCaseSpanIgnoringCase("AUTH"u8))
                {
                    passwd = parseState.GetString(currTokenIdx++);
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("AUTH2"u8))
                {
                    username = parseState.GetString(currTokenIdx++);
                    passwd = parseState.GetString(currTokenIdx++);
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("KEYS"u8))
                {
                    slots = [];
                    if (transferOption == TransferOption.SLOTS)
                        pstate = MigrateCmdParseState.MULTI_TRANSFER_OPTION;

                    transferOption = TransferOption.KEYS;
                    sketch ??= new();
                    while (currTokenIdx < parseState.Count)
                    {
                        var currKeySlice = parseState.GetArgSliceByRef(currTokenIdx++);
                        var sbKey = currKeySlice.SpanByte;

                        // Skip if previous error encountered
                        if (pstate != MigrateCmdParseState.SUCCESS) continue;

                        // Check if all keys are local R/W because we migrate keys and need to be able to delete them
                        var slot = HashSlotUtils.HashSlot(sbKey.ToPointer(), sbKey.Length);
                        if (!current.IsLocal(slot, readWriteSession: false))
                        {
                            pstate = MigrateCmdParseState.SLOTNOTLOCAL;
                            continue;
                        }

                        // Check if keys refer to multiple slots
                        if (!slots.Contains(slot) && slots.Count > 0)
                        {
                            pstate = MigrateCmdParseState.CROSSSLOT;
                            continue;
                        }

                        // Check if slot is not set as MIGRATING
                        if (!current.IsMigratingSlot(slot))
                        {
                            pstate = MigrateCmdParseState.NOTMIGRATING;
                            continue;
                        }

                        // Add key to sketch
                        sketch.HashAndStore(ref currKeySlice);
                        _ = slots.Add(slot);
                    }
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("SLOTS"u8))
                {
                    if (transferOption == TransferOption.KEYS)
                        pstate = MigrateCmdParseState.MULTI_TRANSFER_OPTION;
                    transferOption = TransferOption.SLOTS;
                    slots = [];
                    while (currTokenIdx < parseState.Count)
                    {
                        if (!parseState.TryGetInt(currTokenIdx++, out var slot))
                        {
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }

                        // Skip if previous error encountered
                        if (pstate != MigrateCmdParseState.SUCCESS) continue;

                        // Check if slot is in valid range
                        if (ClusterConfig.OutOfRange(slot))
                        {
                            pstate = MigrateCmdParseState.SLOTOUTOFRANGE;
                            slotParseError = slot;
                            continue;
                        }

                        // Check if slot is local and can be migrated
                        if (!current.IsLocal((ushort)slot, readWriteSession: false))
                        {
                            pstate = MigrateCmdParseState.SLOTNOTLOCAL;
                            slotParseError = slot;
                            continue;
                        }

                        // Add slot range and check for duplicates or overlap
                        if (!slots.Add(slot))
                        {
                            pstate = MigrateCmdParseState.MULTISLOTREF;
                            slotParseError = slot;
                            continue;
                        }
                    }
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase("SLOTSRANGE"u8))
                {
                    if (transferOption == TransferOption.KEYS)
                        pstate = MigrateCmdParseState.MULTI_TRANSFER_OPTION;
                    transferOption = TransferOption.SLOTS;
                    slots = [];
                    if (parseState.Count - currTokenIdx == 0 || ((parseState.Count - currTokenIdx) & 0x1) > 0)
                    {
                        pstate = MigrateCmdParseState.INCOMPLETESLOTSRANGE;
                        break;
                    }

                    while (currTokenIdx < parseState.Count)
                    {
                        if (!parseState.TryGetInt(currTokenIdx++, out var slotStart)
                            || !parseState.TryGetInt(currTokenIdx++, out var slotEnd))
                        {
                            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                                SendAndReset();
                            return true;
                        }

                        // Skip if previous error encountered
                        if (pstate != MigrateCmdParseState.SUCCESS) continue;

                        for (var slot = slotStart; slot <= slotEnd; slot++)
                        {
                            // Check if slot is in valid range
                            if (ClusterConfig.OutOfRange(slot))
                            {
                                pstate = MigrateCmdParseState.SLOTOUTOFRANGE;
                                slotParseError = slot;
                                continue;
                            }

                            // Check if slot is not owned by current node or cluster mode is not enabled
                            if (!current.IsLocal((ushort)slot, readWriteSession: false))
                            {
                                pstate = MigrateCmdParseState.SLOTNOTLOCAL;
                                slotParseError = slot;
                                continue;
                            }

                            // Add slot range and check for duplicates or overlap
                            if (!slots.Add(slot))
                            {
                                pstate = MigrateCmdParseState.MULTISLOTREF;
                                slotParseError = slot;
                                continue;
                            }
                        }
                    }
                }
            }

            #endregion

            #region checkParseErrors
            if (clusterProvider.clusterManager != null && targetNodeId != null && current.GetNodeRoleFromNodeId(targetNodeId) != NodeRole.PRIMARY)
                pstate = MigrateCmdParseState.TARGETNODENOTMASTER;

            if (!HandleCommandParsingErrors(pstate, targetAddress, targetPort, slotParseError))
                return true;

            #endregion

            logger?.LogDebug("MIGRATE COPY:{copyOption} REPLACE:{replaceOption} OpType:{opType}", copyOption, replaceOption, (sketch != null ? "KEYS" : "SLOTS"));

            #region scheduleMigration
            if (!clusterProvider.migrationManager.TryAddMigrationTask(
                this,
                sourceNodeId,
                effectiveAddress,
                targetPort,
                targetNodeId,
                username,
                passwd,
                copyOption,
                replaceOption,
                timeout,
                slots,
                sketch,
                transferOption,
                out var mSession))
            {
                // Migration task could not be added due to possible conflicting migration tasks
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_IOERR, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                //Start migration task
                if (!mSession.TryStartMigrationTask(out var errorMessage))
                {
                    while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
            #endregion
        }
    }
}