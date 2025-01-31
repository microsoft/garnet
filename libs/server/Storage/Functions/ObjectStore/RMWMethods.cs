﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(SpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            var type = input.header.type;

            switch (type)
            {
                case GarnetObjectType.Expire:
                case GarnetObjectType.PExpire:
                case GarnetObjectType.Persist:
                    return false;
                default:
                    if ((byte)type < CustomCommandManager.TypeIdStartOffset)
                        return GarnetObject.NeedToCreate(input.header);
                    else
                    {
                        var customObjectCommand = GetCustomObjectCommand(ref input, type);
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.spanByteAndMemory.Memory, 0);
                        var ret = customObjectCommand.NeedInitialUpdate(key, ref input, ref outp);
                        output.spanByteAndMemory.Memory = outp.Memory;
                        output.spanByteAndMemory.Length = outp.Length;
                        return ret;
                    }
            }
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref LogRecord<IGarnetObject> logRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(!logRecord.Info.HasETag && !logRecord.Info.HasExpiration, "Should not have Expiration or ETag on InitialUpdater log records");

            var type = input.header.type;
            IGarnetObject value;
            if ((byte)type < CustomCommandManager.TypeIdStartOffset)
            {
                value = GarnetObject.Create(type);
                _ = value.Operate(ref input, ref output.spanByteAndMemory, out _, out _);
                _ = logRecord.TrySetValueObject(value);
                return true;
            }

            Debug.Assert(type is not GarnetObjectType.Expire and not GarnetObjectType.PExpire and not GarnetObjectType.Persist, "Expire and Persist commands should have been handled already by NeedInitialUpdate.");

            var customObjectCommand = GetCustomObjectCommand(ref input, type);
            var objectId = (byte)((byte)type - CustomCommandManager.TypeIdStartOffset);
            value = functionsState.customObjectCommands[objectId].factory.Create((byte)type);

            (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.spanByteAndMemory.Memory, 0);
            var result = customObjectCommand.InitialUpdater(logRecord.Key, ref input, value, ref memoryAndLength, ref rmwInfo);
            _ = logRecord.TrySetValueObject(value);
            output.spanByteAndMemory.Memory = memoryAndLength.Memory;
            output.spanByteAndMemory.Length = memoryAndLength.Length;
            return result;
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref LogRecord<IGarnetObject> dstLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(dstLogRecord.ValueObject.Size);
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref LogRecord<IGarnetObject> logRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            if (InPlaceUpdaterWorker(ref logRecord, ref input, ref output, ref rmwInfo, out long sizeChange))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null) WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeChange);
                return true;
            }
            return false;
        }

        bool InPlaceUpdaterWorker(ref LogRecord<IGarnetObject> logRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, out long sizeChange)
        {
            sizeChange = 0;

            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            switch (input.header.type)
            {
                case GarnetObjectType.Expire:
                case GarnetObjectType.PExpire:
                    var expiryValue = input.parseState.GetLong(0);

                    var optionType = (ExpireOption)input.arg1;
                    var expireAt = input.arg2 == 1;

                    long expiryTicks;
                    if (expireAt)
                    {
                        expiryTicks = input.header.type == GarnetObjectType.PExpire
                            ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryValue)
                            : ConvertUtils.UnixTimestampInSecondsToTicks(expiryValue);
                    }
                    else
                    {
                        var tsExpiry = input.header.type == GarnetObjectType.PExpire
                            ? TimeSpan.FromMilliseconds(expiryValue)
                            : TimeSpan.FromSeconds(expiryValue);
                        expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    }

                    return EvaluateObjectExpireInPlace(ref logRecord, optionType, expiryTicks, ref output);
                case GarnetObjectType.Persist:
                    if (logRecord.Info.HasExpiration)
                    {
                        logRecord.RemoveExpiration();
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.spanByteAndMemory);
                    }
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.spanByteAndMemory);
                    return true;
                default:
                    if ((byte)input.header.type < CustomCommandManager.TypeIdStartOffset)
                    {
                        var operateSuccessful = logRecord.ValueObject.Operate(ref input, ref output.spanByteAndMemory, out sizeChange,
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
                        if (IncorrectObjectType(ref input, logRecord.ValueObject, ref output.spanByteAndMemory))
                            return true;

                        (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.spanByteAndMemory.Memory, 0);
                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var result = customObjectCommand.Updater(logRecord.Key, ref input, logRecord.ValueObject, ref memoryAndLength, ref rmwInfo);
                        output.spanByteAndMemory.Memory = memoryAndLength.Memory;
                        output.spanByteAndMemory.Length = memoryAndLength.Length;
                        return result;
                    }
            }
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
            => true;

        /// <inheritdoc />
        public bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<IGarnetObject> dstLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
        {
            // Expired data
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }
            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<IGarnetObject> dstLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
        {
            // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that 
            // the record was CASed into the hash chain before it gets modified
            var oldValueSize = srcLogRecord.ValueObject.Size;
            var value = srcLogRecord.ValueObject.CopyUpdate(srcLogRecord.Info.IsInNewVersion, ref rmwInfo);

            // Do not set dstLogRecord.Expiration until we know it is a command for which we allocated length in the LogRecord for it.
            dstLogRecord.TrySetValueObject(value);

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            switch (input.header.type)
            {
                case GarnetObjectType.Expire:
                case GarnetObjectType.PExpire:
                    var expiryValue = input.parseState.GetLong(0);

                    var optionType = (ExpireOption)input.arg1;
                    var expireAt = input.arg2 == 1;

                    long expiryTicks;
                    if (expireAt)
                    {
                        expiryTicks = input.header.type == GarnetObjectType.PExpire
                            ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryValue)
                            : ConvertUtils.UnixTimestampInSecondsToTicks(expiryValue);
                    }
                    else
                    {
                        var tsExpiry = input.header.type == GarnetObjectType.PExpire
                            ? TimeSpan.FromMilliseconds(expiryValue)
                            : TimeSpan.FromSeconds(expiryValue);
                        expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    }

                    // Expire will have allocated space for the expiration, so copy it over and do the "in-place" logic to replace it in the new record
                    if (srcLogRecord.Info.HasExpiration)
                        dstLogRecord.TrySetExpiration(srcLogRecord.Expiration);
                    EvaluateObjectExpireInPlace(ref dstLogRecord, optionType, expiryTicks, ref output);
                    break;

                case GarnetObjectType.Persist:
                    if (srcLogRecord.Info.HasExpiration)
                    {
                        // dstLogRecord probably doesn't have expiration since we didn't set it above, but Remove it to be safe
                        dstLogRecord.RemoveExpiration();
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.spanByteAndMemory);
                    }
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.spanByteAndMemory);
                    break;
                default:
                    if ((byte)input.header.type < CustomCommandManager.TypeIdStartOffset)
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
                        var result = customObjectCommand.Updater(srcLogRecord.Key, ref input, value, ref outp, ref rmwInfo);
                        output.spanByteAndMemory.Memory = outp.Memory;
                        output.spanByteAndMemory.Length = outp.Length;
                        return result;
                    }
            }

            // If oldValue has been set to null, subtract it's size from the tracked heap size
            var sizeAdjustment = rmwInfo.ClearSourceValueObject ? value.Size - oldValueSize : value.Size;
            functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeAdjustment);

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(srcLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}