﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            var type = input.header.type;

            switch (type)
            {
                case GarnetObjectType.Expire:
                case GarnetObjectType.Persist:
                    return false;
                default:
                    if ((byte)type < CustomCommandManager.StartOffset)
                        return GarnetObject.NeedToCreate(*(RespInputHeader*)input.ToPointer());
                    else
                    {
                        var customObjectCommand = GetCustomObjectCommand(ref input, type);
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.spanByteAndMemory.Memory, 0);
                        var ret = customObjectCommand.NeedInitialUpdate(key, input.payload.ReadOnlySpan, ref outp);
                        output.spanByteAndMemory.Memory = outp.Memory;
                        output.spanByteAndMemory.Length = outp.Length;
                        return ret;
                    }
            }
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var type = input.header.type;
            if ((byte)type < CustomCommandManager.StartOffset)
            {
                value = GarnetObject.Create(type);
                value.Operate(ref input, ref output.spanByteAndMemory, out _, out _);
                return true;
            }
            else
            {
                Debug.Assert(type != GarnetObjectType.Expire && type != GarnetObjectType.Persist, "Expire and Persist commands should have been handled already by NeedInitialUpdate.");

                var customObjectCommand = GetCustomObjectCommand(ref input, type);
                var objectId = (byte)((byte)type - CustomCommandManager.StartOffset);
                value = functionsState.customObjectCommands[objectId].factory.Create((byte)type);

                (IMemoryOwner<byte> Memory, int Length) outp = (output.spanByteAndMemory.Memory, 0);
                var result = customObjectCommand.InitialUpdater(key, input.payload.ReadOnlySpan, value, ref outp, ref rmwInfo);
                output.spanByteAndMemory.Memory = outp.Memory;
                output.spanByteAndMemory.Length = outp.Length;
                return result;
            }
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                var header = (RespInputHeader*)input.ToPointer();
                header->SetExpiredFlag();
                WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateKeyValueSize(key, value));
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (InPlaceUpdaterWorker(ref key, ref input, ref value, ref output, ref rmwInfo, out long sizeChange))
            {
                if (!rmwInfo.RecordInfo.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null) WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeChange);
                return true;
            }
            return false;
        }

        bool InPlaceUpdaterWorker(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, out long sizeChange)
        {
            sizeChange = 0;

            // Expired data
            if (value.Expiration > 0 && input.header.CheckExpiry(value.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            switch (input.header.type)
            {
                case GarnetObjectType.Expire:
                    var optionType = (ExpireOption)(*input.payload.ptr);
                    var expiryExists = (value.Expiration > 0);
                    var expiration = *(long*)(input.payload.ptr + 1);
                    return EvaluateObjectExpireInPlace(optionType, expiryExists, expiration, ref value, ref output);
                case GarnetObjectType.Persist:
                    if (value.Expiration > 0)
                    {
                        value.Expiration = 0;
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.spanByteAndMemory);
                    }
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.spanByteAndMemory);
                    return true;
                default:
                    if ((byte)input.header.type < CustomCommandManager.StartOffset)
                    {
                        var operateSuccessful = value.Operate(ref input, ref output.spanByteAndMemory, out sizeChange,
                        out var removeKey);
                        if (removeKey)
                        {
                            rmwInfo.Action = RMWAction.ExpireAndStop;
                            return false;
                        }

                        return operateSuccessful;
                    }
                    else
                    {
                        if (IncorrectObjectType(ref input, value, ref output.spanByteAndMemory))
                            return true;

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.spanByteAndMemory.Memory, 0);
                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var result = customObjectCommand.Updater(key, input.payload.ReadOnlySpan, value, ref outp, ref rmwInfo);
                        output.spanByteAndMemory.Memory = outp.Memory;
                        output.spanByteAndMemory.Length = outp.Length;
                        return result;
                        //return customObjectCommand.InPlaceUpdateWorker(key, ref input, value, ref output.spanByteAndMemory, ref rmwInfo);
                    }
            }
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate(ref byte[] key, ref ObjectInput input, ref IGarnetObject oldValue, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            => true;

        /// <inheritdoc />
        public bool CopyUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject oldValue, ref IGarnetObject newValue, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var header = (RespInputHeader*)input.ToPointer();

            // Expired data
            if (oldValue.Expiration > 0 && header->CheckExpiry(oldValue.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }
            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater(ref byte[] key, ref ObjectInput input, ref IGarnetObject oldValue, ref IGarnetObject value, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that 
            // the record was CASed into the hash chain before it gets modified
            oldValue.CopyUpdate(ref oldValue, ref value, rmwInfo.RecordInfo.IsInNewVersion);

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            switch (input.header.type)
            {
                case GarnetObjectType.Expire:
                    var expireOption = (ExpireOption)(*input.payload.ptr);
                    var expiryExists = (value.Expiration > 0);
                    var expiration = *(long*)(input.payload.ptr + 1);
                    EvaluateObjectExpireInPlace(expireOption, expiryExists, expiration, ref value, ref output);
                    break;
                case GarnetObjectType.Persist:
                    if (value.Expiration > 0)
                    {
                        value.Expiration = 0;
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.spanByteAndMemory);
                    }
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.spanByteAndMemory);
                    break;
                default:
                    if ((byte)input.header.type < CustomCommandManager.StartOffset)
                    {
                        value.Operate(ref input, ref output.spanByteAndMemory, out _, out var removeKey);
                        if (removeKey)
                        {
                            rmwInfo.Action = RMWAction.ExpireAndStop;
                            return false;
                        }
                        break;
                    }
                    else
                    {
                        // TODO: Update to invoke CopyUpdater of custom object command without creating a new object
                        // using Clone. Currently, expire and persist commands are performed on the new copy of the object.
                        if (IncorrectObjectType(ref input, value, ref output.spanByteAndMemory))
                            return true;

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.spanByteAndMemory.Memory, 0);
                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var result = customObjectCommand.Updater(key, input.payload.ReadOnlySpan, value, ref outp, ref rmwInfo);
                        output.spanByteAndMemory.Memory = outp.Memory;
                        output.spanByteAndMemory.Length = outp.Length;
                        return result;
                    }
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateKeyValueSize(key, value));

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}