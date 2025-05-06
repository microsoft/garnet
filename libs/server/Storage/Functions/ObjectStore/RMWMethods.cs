// Copyright (c) Microsoft Corporation.
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
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            var type = input.header.type;

            switch (type)
            {
                case GarnetObjectType.Expire:
                case GarnetObjectType.PExpire:
                case GarnetObjectType.Persist:
                    return false;
                default:
                    if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
                        return GarnetObject.NeedToCreate(input.header);
                    else
                    {
                        var customObjectCommand = GetCustomObjectCommand(ref input, type);
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.SpanByteAndMemory.Memory, 0);
                        var ret = customObjectCommand.NeedInitialUpdate(key, ref input, ref outp);
                        output.SpanByteAndMemory.Memory = outp.Memory;
                        output.SpanByteAndMemory.Length = outp.Length;
                        return ret;
                    }
            }
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(!logRecord.Info.HasETag && !logRecord.Info.HasExpiration, "Should not have Expiration or ETag on InitialUpdater log records");

            var type = input.header.type;
            IGarnetObject value;
            if ((byte)type < CustomCommandManager.CustomTypeIdStartOffset)
            {
                value = GarnetObject.Create(type);
                _ = value.Operate(ref input, ref output, out _);
                _ = logRecord.TrySetValueObject(value, ref sizeInfo);
                return true;
            }

            Debug.Assert(type is not GarnetObjectType.Expire and not GarnetObjectType.PExpire and not GarnetObjectType.Persist, "Expire and Persist commands should have returned false from NeedInitialUpdate.");

            var customObjectCommand = GetCustomObjectCommand(ref input, type);
            value = functionsState.GetCustomObjectFactory((byte)type).Create((byte)type);

            (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.SpanByteAndMemory.Memory, 0);
            var result = customObjectCommand.InitialUpdater(logRecord.Key, ref input, value, ref memoryAndLength, ref rmwInfo);
            _ = logRecord.TrySetValueObject(value, ref sizeInfo);
            output.SpanByteAndMemory.Memory = memoryAndLength.Memory;
            output.SpanByteAndMemory.Length = memoryAndLength.Length;
            if (result)
                sizeInfo.AssertOptionals(logRecord.Info);
            return result;
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }

            functionsState.objectStoreSizeTracker?.AddTrackedSize(dstLogRecord.ValueObject.MemorySize);
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
        {
            if (InPlaceUpdaterWorker(ref logRecord, ref sizeInfo, ref input, ref output, ref rmwInfo, out long sizeChange))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null) 
                    WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeChange);
                return true;
            }
            return false;
        }

        bool InPlaceUpdaterWorker(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo, out long sizeChange)
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

                    if (!EvaluateObjectExpireInPlace(ref logRecord, optionType, expiryTicks, ref output))
                        return false;
                    return true;    // The options may or may not produce a result that matches up with what sizeInfo has, so return rather than drop down to AssertOptionals
                case GarnetObjectType.Persist:
                    if (logRecord.Info.HasExpiration)
                    {
                        logRecord.RemoveExpiration();
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                    }
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
                    return true;
                default:
                    if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                    {
                        var operateSuccessful = ((IGarnetObject)logRecord.ValueObject).Operate(ref input, ref output, out sizeChange);
                        if (output.HasWrongType)
                            return true;
                        if (output.HasRemoveKey)
                        {
                            rmwInfo.Action = RMWAction.ExpireAndStop;
                            return false;
                        }

                        sizeInfo.AssertOptionals(logRecord.Info);
                        return operateSuccessful;
                    }
                    else
                    {
                        if (IncorrectObjectType(ref input, ((IGarnetObject)logRecord.ValueObject), ref output.SpanByteAndMemory))
                        {
                            output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                            return true;
                        }

                        (IMemoryOwner<byte> Memory, int Length) memoryAndLength = (output.SpanByteAndMemory.Memory, 0);
                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var result = customObjectCommand.Updater(logRecord.Key, ref input, ((IGarnetObject)logRecord.ValueObject), ref memoryAndLength, ref rmwInfo);
                        output.SpanByteAndMemory.Memory = memoryAndLength.Memory;
                        output.SpanByteAndMemory.Length = memoryAndLength.Length;
                        if (!result)
                            return false;
                        break;
                    }
            }
            sizeInfo.AssertOptionals(logRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
            => true;

        /// <inheritdoc />
        public bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
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
        public bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // We're performing the object update here (and not in CopyUpdater) so that we are guaranteed that 
            // the record was CASed into the hash chain before it gets modified
            var oldValueSize = srcLogRecord.ValueObject.MemorySize;
            var value = ((IGarnetObject)srcLogRecord.ValueObject).CopyUpdate(srcLogRecord.Info.IsInNewVersion, ref rmwInfo);

            // First copy the new Value and optionals to the new record. This will also ensure space for expiration if it's present.
            // Do not set actually set dstLogRecord.Expiration until we know it is a command for which we allocated length in the LogRecord for it.
            if (!dstLogRecord.TrySetValueObject(value, ref sizeInfo))
                return false;

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
                    if (!EvaluateObjectExpireInPlace(ref dstLogRecord, optionType, expiryTicks, ref output))
                        return false;
                    break;

                case GarnetObjectType.Persist:
                    if (!dstLogRecord.TryCopyFrom(ref srcLogRecord, ref sizeInfo))
                        return false;
                    if (srcLogRecord.Info.HasExpiration)
                    {
                        dstLogRecord.RemoveExpiration();
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                    }
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
                    break;
                default:
                    if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                    {
                        value.Operate(ref input, ref output, out _);
                        if (output.HasWrongType)
                            return true;
                        if (output.HasRemoveKey)
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
                        if (IncorrectObjectType(ref input, value, ref output.SpanByteAndMemory))
                        {
                            output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                            return true;
                        }

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.SpanByteAndMemory.Memory, 0);
                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var result = customObjectCommand.Updater(srcLogRecord.Key, ref input, value, ref outp, ref rmwInfo);
                        output.SpanByteAndMemory.Memory = outp.Memory;
                        output.SpanByteAndMemory.Length = outp.Length;
                        return result;
                    }
            }
            sizeInfo.AssertOptionals(dstLogRecord.Info);

            // If oldValue has been set to null, subtract its size from the tracked heap size
            var sizeAdjustment = rmwInfo.ClearSourceValueObject ? value.MemorySize - oldValueSize : value.MemorySize;
            functionsState.objectStoreSizeTracker?.AddTrackedSize(sizeAdjustment);

            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(srcLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}