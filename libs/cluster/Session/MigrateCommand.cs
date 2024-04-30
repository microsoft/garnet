// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{

    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        public static bool Expired(ref SpanByte value) => value.MetadataSize > 0 && value.ExtraMetadata < DateTimeOffset.UtcNow.Ticks;

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
                MigrateCmdParseState.CROSSSLOT => CmdStrings.RESP_ERR_CROSSLOT,
                MigrateCmdParseState.TARGETNODENOTMASTER => Encoding.ASCII.GetBytes($"ERR Cannot initiate migration, target node ({targetAddress}:{targetPort}) is not a primary."),
                MigrateCmdParseState.INCOMPLETESLOTSRANGE => CmdStrings.RESP_ERR_GENERIC_INCOMPLETESLOTSRANGE,
                MigrateCmdParseState.SLOTOUTOFRANGE => Encoding.ASCII.GetBytes($"ERR Slot {slotMultiRef} out of range."),
                MigrateCmdParseState.NOTMIGRATING => CmdStrings.RESP_ERR_GENERIC_SLOTNOTMIGRATING,
                _ => CmdStrings.RESP_ERR_GENERIC_PARSING,
            };
            while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                SendAndReset();
            return false;
        }

        private bool TryMIGRATE(int count, byte* ptr)
        {
            // Migrate command format
            // migrate host port <KEY | ""> destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [[KEYS keys] | [SLOTSRANGE start-slot end-slot [start-slot end-slot]]]]
            #region parseMigrationArguments
            //1. Address
            if (!RespReadUtils.ReadStringWithLengthHeader(out var targetAddress, ref ptr, recvBufferPtr + bytesRead))
                return false;

            //2. Port
            if (!RespReadUtils.ReadIntWithLengthHeader(out var targetPort, ref ptr, recvBufferPtr + bytesRead))
                return false;

            //3. Key
            byte* singleKeyPtr = null;
            var sksize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref singleKeyPtr, ref sksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            //4. Destination DB
            if (!RespReadUtils.ReadIntWithLengthHeader(out var dbid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            //5. Timeout
            if (!RespReadUtils.ReadIntWithLengthHeader(out var timeout, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var args = count - 5;
            var copyOption = false;
            var replaceOption = false;
            string username = null;
            string passwd = null;
            List<(long, long)> keysWithSize = null;
            HashSet<int> slots = [];

            ClusterConfig current = null;
            string sourceNodeId = null;
            string targetNodeId = null;
            var pstate = MigrateCmdParseState.CLUSTERDOWN;
            var slotParseError = -1;
            if (clusterProvider.serverOptions.EnableCluster)
            {
                pstate = MigrateCmdParseState.SUCCESS;
                current = clusterProvider.clusterManager.CurrentConfig;
                sourceNodeId = current.LocalNodeId;
                targetNodeId = current.GetWorkerNodeIdFromAddress(targetAddress, targetPort);
                if (targetNodeId == null) pstate = MigrateCmdParseState.UNKNOWNTARGET;
            }

            // Add single key if specified
            if (sksize > 0)
            {
                keysWithSize = [];
                keysWithSize.Add(new(((IntPtr)singleKeyPtr).ToInt64(), sksize));
            }

            while (args > 0)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out var option, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                args--;

                if (option.Equals("COPY", StringComparison.OrdinalIgnoreCase))
                    copyOption = true;
                else if (option.Equals("REPLACE", StringComparison.OrdinalIgnoreCase))
                    replaceOption = true;
                else if (option.Equals("AUTH", StringComparison.OrdinalIgnoreCase))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out passwd, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    args--;
                }
                else if (option.Equals("AUTH2", StringComparison.OrdinalIgnoreCase))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out username, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    if (!RespReadUtils.ReadStringWithLengthHeader(out passwd, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    args -= 2;
                }
                else if (option.Equals("KEYS", StringComparison.OrdinalIgnoreCase))
                {
                    keysWithSize ??= [];
                    while (args > 0)
                    {
                        byte* keyPtr = null;
                        var ksize = 0;

                        if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        args--;

                        // Skip if previous error encountered
                        if (pstate != MigrateCmdParseState.SUCCESS) continue;

                        // Check if all keys are local R/W because we migrate keys and need to be able to delete them
                        var slot = NumUtils.HashSlot(keyPtr, ksize);
                        if (!current.IsLocal(slot, readCommand: false))
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

                        // Add pointer of current parsed key
                        keysWithSize.Add(new(((IntPtr)keyPtr).ToInt64(), ksize));
                    }
                }
                else if (option.Equals("SLOTS", StringComparison.OrdinalIgnoreCase))
                {
                    while (args > 0)
                    {
                        if (!RespReadUtils.ReadIntWithLengthHeader(out var slot, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        args--;

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
                        if (!current.IsLocal((ushort)slot, readCommand: false))
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
                else if (option.Equals("SLOTSRANGE", StringComparison.OrdinalIgnoreCase))
                {
                    if (args == 0 || (args & 0x1) > 0)
                    {
                        pstate = MigrateCmdParseState.INCOMPLETESLOTSRANGE;
                        while (args > 0)
                        {
                            if (!RespReadUtils.ReadIntWithLengthHeader(out var slotStart, ref ptr, recvBufferPtr + bytesRead))
                                return false;
                            args--;
                        }
                    }
                    else
                    {
                        while (args > 0)
                        {
                            if (!RespReadUtils.ReadIntWithLengthHeader(out var slotStart, ref ptr, recvBufferPtr + bytesRead))
                                return false;

                            if (!RespReadUtils.ReadIntWithLengthHeader(out var slotEnd, ref ptr, recvBufferPtr + bytesRead))
                                return false;
                            args -= 2;

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
                                if (!current.IsLocal((ushort)slot, readCommand: false))
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
            }
            readHead = (int)(ptr - recvBufferPtr);
            #endregion

            #region checkParseErrors
            if (clusterProvider.clusterManager != null && current.GetNodeRoleFromNodeId(targetNodeId) != NodeRole.PRIMARY)
                pstate = MigrateCmdParseState.TARGETNODENOTMASTER;

            if (!HandleCommandParsingErrors(pstate, targetAddress, targetPort, slotParseError))
                return true;

            // Check if session is authorized to perform migration.
            if (!CheckACLAdminPermissions())
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOAUTH, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            #endregion

            logger?.LogDebug("MIGRATE COPY:{copyOption} REPLACE:{replaceOption} OpType:{opType}", copyOption, replaceOption, (keysWithSize != null ? "KEYS" : "SLOTS"));

            #region scheduleMigration
            if (!clusterProvider.migrationManager.TryAddMigrationTask(
                sourceNodeId,
                targetAddress,
                targetPort,
                targetNodeId,
                username,
                passwd,
                copyOption,
                replaceOption,
                timeout,
                slots,
                keysWithSize,
                out var mSession))
            {
                // Migration task could not be added due to possible conflicting migration tasks
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_IOERR, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                //Start migration task
                if (!mSession.TryStartMigrationTask(out var errorMessage))
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
            #endregion
        }
    }
}