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
            SLOTOUTOFRANGE
        }

        private bool HandleCommandParsingErrors(MigrateCmdParseState mpState, string targetAddress, int targetPort, int slotMultiRef)
        {
            ReadOnlySpan<byte> resp;
            switch (mpState)
            {
                case MigrateCmdParseState.SUCCESS:
                    return true;
                case MigrateCmdParseState.CLUSTERDOWN:
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Cluster nodes not initialized correctly.\r\n"));
                    break;
                case MigrateCmdParseState.UNKNOWNTARGET:
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR I don't know about node ({targetAddress}:{targetPort}).\r\n"));
                    break;
                case MigrateCmdParseState.MULTISLOTREF:
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slotMultiRef} specified multiple times\r\n"));
                    break;
                case MigrateCmdParseState.SLOTNOTLOCAL:
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR slot {slotMultiRef} not owned by current node.\r\n"));
                    break;
                case MigrateCmdParseState.CROSSSLOT:
                    resp = Encoding.ASCII.GetBytes($"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
                    break;
                case MigrateCmdParseState.TARGETNODENOTMASTER:
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Cannot initiate migration, target node ({targetAddress}:{targetPort}) is not a primary.\r\n"));
                    break;
                case MigrateCmdParseState.INCOMPLETESLOTSRANGE:
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR incomplete slotrange\r\n."));
                    break;
                case MigrateCmdParseState.SLOTOUTOFRANGE:
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Slot {slotMultiRef} out of range\r\n"));
                    break;
                default:
                    resp = new(Encoding.ASCII.GetBytes($"-ERR Parsing error.\r\n"));
                    break;
            }
            while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
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
            int sksize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref singleKeyPtr, ref sksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            //4. Destination DB
            if (!RespReadUtils.ReadIntWithLengthHeader(out var dbid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            //5. Timeout
            if (!RespReadUtils.ReadIntWithLengthHeader(out var timeout, ref ptr, recvBufferPtr + bytesRead))
                return false;

            int args = count - 6;
            bool copyOption = false;
            bool replaceOption = false;
            string username = null;
            string passwd = null;
            List<(long, long)> keysWithSize = null;
            HashSet<int> slots = [];

            ClusterConfig current = null;
            string sourceNodeId = null;
            string targetNodeId = null;
            MigrateCmdParseState pstate = MigrateCmdParseState.CLUSTERDOWN;
            int slotParseError = -1;
            if (clusterProvider.serverOptions.EnableCluster)
            {
                pstate = MigrateCmdParseState.SUCCESS;
                current = clusterProvider.clusterManager.CurrentConfig;
                sourceNodeId = current.GetLocalNodeId();
                targetNodeId = current.GetWorkerNodeIdFromAddress(targetAddress, targetPort);
                if (targetNodeId == null) pstate = MigrateCmdParseState.UNKNOWNTARGET;
            }

            //Add single key if specified
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

                if (option.ToUpper().Equals("COPY"))
                    copyOption = true;
                else if (option.ToUpper().Equals("REPLACE"))
                    replaceOption = true;
                else if (option.ToUpper().Equals("AUTH"))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out passwd, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    args--;
                }
                else if (option.ToUpper().Equals("AUTH2"))
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out username, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    if (!RespReadUtils.ReadStringWithLengthHeader(out passwd, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    args -= 2;
                }
                else if (option.ToUpper().Equals("KEYS"))
                {
                    keysWithSize ??= [];
                    while (args > 0)
                    {
                        byte* keyPtr = null;
                        int ksize = 0;

                        if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        args--;

                        //Skip if previous error encountered
                        if (pstate != MigrateCmdParseState.SUCCESS) continue;

                        //Check if all keys are local R/W because we migrate keys and need to be able to delete them
                        var slot = NumUtils.HashSlot(keyPtr, ksize);
                        if (!current.IsLocal((ushort)slot, readCommand: false))
                        {
                            pstate = MigrateCmdParseState.SLOTNOTLOCAL;
                            continue;
                        }

                        //Check if keys refer to multiple slots
                        if (!slots.Contains(slot) && slots.Count > 0)
                        {
                            pstate = MigrateCmdParseState.CROSSSLOT;
                            continue;
                        }

                        //Add pointer of current parsed key
                        keysWithSize.Add(new(((IntPtr)keyPtr).ToInt64(), ksize));
                    }
                }
                else if (option.ToUpper().Equals("SLOTS"))
                {
                    while (args > 0)
                    {
                        if (!RespReadUtils.ReadIntWithLengthHeader(out var slot, ref ptr, recvBufferPtr + bytesRead))
                            return false;
                        args--;

                        //Skip if previous error encountered
                        if (pstate != MigrateCmdParseState.SUCCESS) continue;

                        //Check if slot is in valid range
                        if (ClusterConfig.OutOfRange(slot))
                        {
                            pstate = MigrateCmdParseState.SLOTOUTOFRANGE;
                            slotParseError = slot;
                            continue;
                        }

                        //Check if slot is local and can be migrated
                        if (!current.IsLocal((ushort)slot, readCommand: false))
                        {
                            pstate = MigrateCmdParseState.SLOTNOTLOCAL;
                            slotParseError = slot;
                            continue;
                        }

                        //add slot range and check for duplicates or overlap
                        if (!slots.Add(slot))
                        {
                            pstate = MigrateCmdParseState.MULTISLOTREF;
                            slotParseError = slot;
                            continue;
                        }
                    }
                }
                else if (option.ToUpper().Equals("SLOTSRANGE"))
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

                            //Skip if previous error encountered
                            if (pstate != MigrateCmdParseState.SUCCESS) continue;

                            for (int slot = slotStart; slot <= slotEnd; slot++)
                            {
                                //Check if slot is in valid range
                                if (ClusterConfig.OutOfRange(slot))
                                {
                                    pstate = MigrateCmdParseState.SLOTOUTOFRANGE;
                                    slotParseError = slot;
                                    continue;
                                }

                                //Check if slot is not owned by current node or cluster mode is not enabled
                                if (!current.IsLocal((ushort)slot, readCommand: false))
                                {
                                    pstate = MigrateCmdParseState.SLOTNOTLOCAL;
                                    slotParseError = slot;
                                    continue;
                                }

                                //add slot range and check for duplicates or overlap
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
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_NOAUTH, ref dcurr, dend))
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
                //Migration task could not be added due to possible conflicting migration tasks
                var resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-IOERR Migrate keys failed.\r\n"));
                while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                //Start migration task
                mSession.StartMigrationTask(out var resp);
                while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
            #endregion
        }
    }
}