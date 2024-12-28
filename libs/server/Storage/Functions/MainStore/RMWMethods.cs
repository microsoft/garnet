// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public readonly bool NeedInitialUpdate(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETEXXX:
                case RespCommand.PERSIST:
                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                case RespCommand.EXPIREAT:
                case RespCommand.PEXPIREAT:
                case RespCommand.GETDEL:
                case RespCommand.GETEX:
                    return false;
                default:
                    if ((ushort)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState
                            .customCommands[(ushort)input.header.cmd - CustomCommandManager.StartOffset].functions
                            .NeedInitialUpdate(key.AsReadOnlySpan(), ref input, ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    return true;
            }
        }

        /// <inheritdoc />
        public readonly bool InitialUpdater(ref LogRecord dstLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            ref var logRecord = ref dstLogRecord.StringLogRecord;
            var value = logRecord.Value;

            // Because this is InitialUpdater, the destination length should be set correctly, but test and log failures to be safe.
            switch (input.header.cmd)
            {
                case RespCommand.PFADD:
                    logRecord.RemoveExpiration();

                    if (!logRecord.TrySetValueLength(HyperLogLog.DefaultHLL.SparseInitialLength(ref input)))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "PFADD");
                        return false;
                    }

                    HyperLogLog.DefaultHLL.Init(ref input, value.ToPointer(), value.Length);
                    *output.SpanByte.ToPointer() = 1;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy + 4 (skip len size)
                    var sbSrcHLL = input.parseState.GetArgSliceByRef(0).SpanByte;

                    logRecord.RemoveExpiration();

                    if (!logRecord.TrySetValueLength(sbSrcHLL.Length))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "PFMERGE");
                        return false;
                    }

                    Buffer.MemoryCopy(sbSrcHLL.ToPointer(), value.ToPointer(), value.Length, value.Length);
                    break;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                    if (!logRecord.TrySetValue(SpanByte.FromPinnedSpan(newInputValue)))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }

                    // Set or remove expiration
                    if (input.arg1 == 0)
                        logRecord.RemoveETag();
                    else if (!logRecord.TrySetExpiration(input.arg1))
                    {
                        functionsState.logger?.LogError("Could not set expiration in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }
                    break;

                case RespCommand.SETKEEPTTL:
                    // Copy input to value; do not change expiration
                    _ = logRecord.TrySetValue(input.parseState.GetArgSliceByRef(0).SpanByte);
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETEXXX:
                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                case RespCommand.EXPIREAT:
                case RespCommand.PEXPIREAT:
                case RespCommand.PERSIST:
                case RespCommand.GETDEL:
                case RespCommand.GETEX:
                    throw new Exception();

                case RespCommand.SETBIT:
                    // Set the bit; remove expiration
                    logRecord.RemoveExpiration();
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!logRecord.TrySetValueLength(BitmapManager.Length(bOffset)))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETBIT");
                        return false;
                    }

                    // Always return 0 at initial updater because previous value was 0
                    _ = BitmapManager.UpdateBitmap(value.ToPointer(), bOffset, bSetVal);
                    CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    break;

                case RespCommand.BITFIELD:
                    logRecord.RemoveExpiration();

                    var bitFieldArgs = GetBitFieldArguments(ref input);

                    if (!logRecord.TrySetValueLength(BitmapManager.LengthFromType(bitFieldArgs)))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "BitField");
                        return false;
                    }

                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, value.ToPointer(), value.Length);
                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    newValue.CopyTo(value.AsSpan().Slice(offset));

                    if (!CopyValueLengthToOutput(value, ref output))
                        return false;
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(0);

                    // Copy value to be appended to the newly allocated value buffer
                    appendValue.ReadOnlySpan.CopyTo(value.AsSpan());

                    if (!CopyValueLengthToOutput(value, ref output))
                        return false;
                    break;
                case RespCommand.INCR:
                    logRecord.RemoveExpiration();

                    // This is InitialUpdater so set the value to 1 and the length to the # of digits in "1"
                    if (!logRecord.TrySetValueLength(1))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCR");
                        return false;
                    }
                    _ = TryCopyUpdateNumber(1L, ref value, ref output);
                    break;
                case RespCommand.INCRBY:
                    {
                        logRecord.RemoveExpiration();
                        var fNeg = false;
                        var incrBy = input.arg1;

                        var ndigits = NumUtils.NumDigitsInLong(incrBy, ref fNeg);
                        if (!logRecord.TrySetValueLength(ndigits + (fNeg ? 1 : 0)))
                        {
                            functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCRBY");
                            return false;
                        }

                        _ = TryCopyUpdateNumber(incrBy, ref value, ref output);
                    }
                    break;
                case RespCommand.DECR:
                    logRecord.RemoveExpiration();

                    // This is InitialUpdater so set the value to -1 and the length to the # of digits in "-1"
                    if (!logRecord.TrySetValueLength(2))
                    {
                        Debug.Assert(value.Length >= 2, "Length overflow in DECR");
                        return false;
                    }
                    _ = TryCopyUpdateNumber(-1, ref value, ref output);
                    break;
                case RespCommand.DECRBY:
                    {
                        logRecord.RemoveExpiration();
                        var fNeg = false;
                        var decrBy = -input.arg1;

                        var ndigits = NumUtils.NumDigitsInLong(decrBy, ref fNeg);
                        if (!logRecord.TrySetValueLength(ndigits + (fNeg ? 1 : 0)))
                        {
                            functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "DECRBY");
                            return false;
                        }

                        _ = TryCopyUpdateNumber(decrBy, ref value, ref output);
                    }
                    break;
                case RespCommand.INCRBYFLOAT:
                    logRecord.RemoveExpiration();
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    if (!TryCopyUpdateNumber(incrByFloat, ref value, ref output))
                        return false;
                    break;
                default:
                    logRecord.RemoveExpiration();

                    if ((ushort)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(ushort)input.header.cmd - CustomCommandManager.StartOffset].functions;
                        if (!logRecord.TrySetValueLength(functions.GetInitialLength(ref input)))
                        {
                            functionsState.logger?.LogError("Length overflow in 'default' > StartOffset: {methodName}.{caseName}", "InitialUpdater", "default");
                            return false;
                        }
                        if (input.arg1 > 0 && !logRecord.TrySetExpiration(input.arg1))
                        {
                            functionsState.logger?.LogError("Could not set expiration in 'default' > StartOffset: {methodName}.{caseName}", "InitialUpdater", "default");
                            return false;
                        }

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        if (!functions.InitialUpdater(logRecord.Key.AsReadOnlySpan(), ref input, value.AsSpan(), ref outp, ref rmwInfo))
                            return false;
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        break;
                    }

                    // Copy input to value
                    if (!logRecord.TrySetValue(input.parseState.GetArgSliceByRef(0).SpanByte))
                    {
                        functionsState.logger?.LogError("Failed to set value in {methodName}.{caseName}", "InitialUpdater", "default");
                        return false;
                    }

                    // Copy value to output
                    CopyTo(logRecord.Value, ref output, functionsState.memoryPool);
                    break;
            }

            // Success if we made it here
            return true;
        }

        /// <inheritdoc />
        public readonly void PostInitialUpdater(ref LogRecord dstLogRecord, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            ref var logRecord = ref dstLogRecord.StringLogRecord;
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }
        }

        /// <inheritdoc />
        public readonly bool InPlaceUpdater(ref LogRecord dstLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            ref var logRecord = ref dstLogRecord.StringLogRecord;
            if (InPlaceUpdaterWorker(ref logRecord, ref input, ref output, ref rmwInfo, ref recordInfo))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                return true;
            }
            return false;
        }

        private readonly bool InPlaceUpdaterWorker(ref StringLogRecord logRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            // First byte of input payload identifies command
            switch (input.header.cmd)
            {
                case RespCommand.SETEXNX:
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.Value, ref output);
                    }
                    return true;

                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    var setValue = input.parseState.GetArgSliceByRef(0);

                    // Need CU if no space for new value
                    if (!logRecord.HasEnoughSpace(setValue.Length, logRecord.Info.HasETag, withExpiration: input.arg1 != 0))
                        return false;

                    // If the SetGet flag is set, copy the current value to output for the GET part of the command.
                    if (input.header.CheckSetGetFlag())
                        CopyRespTo(logRecord.Value, ref output);

                    // Copy input to value
                    _ = logRecord.TrySetValue(setValue.SpanByte);
                    if (input.arg1 != 0)
                        _ = logRecord.TrySetExpiration(input.arg1);
                    return true;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    setValue = input.parseState.GetArgSliceByRef(0);

                    // Need CU if no space for new value. We're not changing whether the Expiration is there or not.
                    if (!logRecord.HasEnoughSpace(setValue.Length, logRecord.Info.HasETag, logRecord.Info.HasExpiration))
                        return false;

                    // If the SetGet flag is set, copy the current value to output for the GET part of the command.
                    if (input.header.CheckSetGetFlag())
                        CopyRespTo(logRecord.Value, ref output);

                    // Copy input to value
                    _ = logRecord.TrySetValue(setValue.SpanByte);
                    return true;

                case RespCommand.PEXPIRE:
                case RespCommand.EXPIRE:
                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    return EvaluateExpireInPlace(ref logRecord, expireOption, expiryTicks, ref output);

                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    return EvaluateExpireInPlace(ref logRecord, expireOption, expiryTicks, ref output);

                case RespCommand.PERSIST:
                    logRecord.RemoveExpiration();
                    return true;

                case RespCommand.INCR:
                    return TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: 1);

                case RespCommand.DECR:
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -1);

                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    var incrBy = input.arg1;
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: incrBy);

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -decrBy);

                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, incrByFloat);

                case RespCommand.SETBIT:
                    var v = value.ToPointer();
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(value.Length, bOffset)) return false;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    var oldValSet = BitmapManager.UpdateBitmap(v, bOffset, bSetVal);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    return true;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    v = value.ToPointer();
                    if (!BitmapManager.IsLargeEnoughForType(bitFieldArgs, value.Length)) return false;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, v, value.Length);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    return true;

                case RespCommand.PFADD:
                    v = value.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(v, value.Length))
                    {
                        *output.SpanByte.ToPointer() = (byte)0xFF;
                        return true;
                    }

                    var updated = false;
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    var result = HyperLogLog.DefaultHLL.Update(ref input, v, value.Length, ref updated);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    if (result)
                        *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    return result;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLL = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer();
                    var dstHLL = value.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(dstHLL, value.Length))
                    {
                        //InvalidType                                                
                        *(long*)output.SpanByte.ToPointer() = -1;
                        return true;
                    }
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                    return HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, value.Length);
                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    if (newValue.Length + offset > value.LengthWithoutMetadata)
                        return false;

                    newValue.CopyTo(value.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output);
                    return true;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref value, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(ref value, ref output);

                    if (input.arg1 > 0)
                    {
                        byte* pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));

                        var newExpiry = input.arg1;
                        return EvaluateExpireInPlace(ExpireOption.None, expiryExists: value.MetadataSize > 0, newExpiry, ref value, ref _output);
                    }

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan
                            .EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);

                        if (persist) // Persist the key
                        {
                            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                            value.AsSpan().CopyTo(value.AsSpan());
                            value.ShrinkSerializedLength(value.Length - value.MetadataSize);
                            value.UnmarkExtraMetadata();
                            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                            return true;
                        }
                    }

                    return true;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendSize = input.parseState.GetArgSliceByRef(0).Length;

                    if (appendSize == 0)
                    {
                        CopyValueLengthToOutput(ref value, ref output);
                        return true;
                    }

                    return false;

                default:
                    var cmd = (ushort)input.header.cmd;
                    if (cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[cmd - CustomCommandManager.StartOffset].functions;
                        var expiration = input.arg1;
                        if (expiration == -1)
                        {
                            // there is existing metadata, but we want to clear it.
                            // we remove metadata, shift the value, shrink length
                            if (value.ExtraMetadata > 0)
                            {
                                var oldValue = value.AsReadOnlySpan();
                                rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                                value.UnmarkExtraMetadata();
                                oldValue.CopyTo(value.AsSpan());
                                value.ShrinkSerializedLength(oldValue.Length);
                                rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                            }
                        }
                        else if (expiration > 0)
                        {
                            // there is no existing metadata, but we want to add it. we cannot do in place update.
                            if (value.ExtraMetadata == 0) return false;
                            // set expiration to the specific value
                            value.ExtraMetadata = expiration;
                        }

                        var valueLength = value.LengthWithoutMetadata;
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functions.InPlaceUpdater(key.AsReadOnlySpan(), ref input, value.AsSpan(), ref valueLength, ref outp, ref rmwInfo);
                        Debug.Assert(valueLength <= value.LengthWithoutMetadata);

                        // Adjust value length if user shrinks it
                        if (valueLength < value.LengthWithoutMetadata)
                        {
                            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                            value.ShrinkSerializedLength(valueLength + value.MetadataSize);
                            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                        }

                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }
        }

        /// <inheritdoc />
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByte oldValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndResume ensures that we set as new value, since it does not exist
                    if (oldValue.MetadataSize > 0 && input.header.CheckExpiry(oldValue.ExtraMetadata))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndResume;
                        return false;
                    }
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(oldValue, ref output);
                    }
                    return false;
                case RespCommand.SETEXXX:
                    // Expired data, return false immediately so we do not set, since it does not exist
                    // ExpireAndStop ensures that caller sees a NOTFOUND status
                    if (oldValue.MetadataSize > 0 && input.header.CheckExpiry(oldValue.ExtraMetadata))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                        return false;
                    }
                    return true;
                default:
                    if ((ushort)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState.customCommands[(ushort)input.header.cmd - CustomCommandManager.StartOffset].functions
                            .NeedCopyUpdate(key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(), ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;

                    }
                    return true;
            }
        }

        /// <inheritdoc />
        public readonly bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogLogRecord, ref RawStringInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
        {
            ref var dstLogRecord = ref dstLogLogRecord.StringLogRecord;

            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            switch (input.header.cmd)
            {
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output);
                    }

                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    var metadataSize = input.arg1 == 0 ? 0 : sizeof(long);

                    Debug.Assert(newInputValue.Length + metadataSize == newValue.Length);

                    newValue.ExtraMetadata = input.arg1;
                    newInputValue.CopyTo(newValue.AsSpan());
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    Debug.Assert(oldValue.MetadataSize + setValue.Length == newValue.Length);

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output);
                    }

                    // Copy input to value, retain metadata of oldValue
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    setValue.CopyTo(newValue.AsSpan());
                    break;

                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                    var expiryExists = oldValue.MetadataSize > 0;

                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    if (!EvaluateExpireCopyUpdate(ref logRecord, expireOption, expiryTicks, newValue, ref output))
                        return false;
                    break;

                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    expiryExists = oldValue.MetadataSize > 0;

                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    if (!EvaluateExpireCopyUpdate(ref logRecord, expireOption, expiryTicks, newValue, ref output))
                        return false;
                    break;

                case RespCommand.PERSIST:
                    oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                    if (oldValue.MetadataSize != 0)
                    {
                        newValue.AsSpan().CopyTo(newValue.AsSpan());
                        newValue.ShrinkSerializedLength(newValue.Length - newValue.MetadataSize);
                        newValue.UnmarkExtraMetadata();
                        output.SpanByte.AsSpan()[0] = 1;
                    }
                    break;

                case RespCommand.INCR:
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref output, input: 1))
                        return false;
                    break;

                case RespCommand.DECR:
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref output, input: -1))
                        return false;
                    break;

                case RespCommand.INCRBY:
                    var incrBy = input.arg1;
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref output, input: incrBy))
                        return false;
                    break;

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref output, input: -decrBy))
                        return false;
                    break;

                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        // Move to tail of the log
                        oldValue.CopyTo(ref newValue);
                        break;
                    }
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: incrByFloat);
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValue.ToPointer(), newValue.Length, oldValue.Length);
                    var oldValSet = BitmapManager.UpdateBitmap(newValue.ToPointer(), bOffset, bSetVal);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValue.ToPointer(), newValue.Length, oldValue.Length);
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, newValue.ToPointer(), newValue.Length);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.PFADD:
                    var updated = false;
                    var newValPtr = newValue.ToPointer();
                    var oldValPtr = oldValue.ToPointer();

                    if (newValue.Length != oldValue.Length)
                        updated = HyperLogLog.DefaultHLL.CopyUpdate(ref input, oldValPtr, newValPtr, newValue.Length);
                    else
                    {
                        Buffer.MemoryCopy(oldValPtr, newValPtr, newValue.Length, oldValue.Length);
                        HyperLogLog.DefaultHLL.Update(ref input, newValPtr, newValue.Length, ref updated);
                    }
                    *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    break;

                case RespCommand.PFMERGE:
                    //srcA offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLLPtr = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer(); // HLL merging from
                    var oldDstHLLPtr = oldValue.ToPointer(); // original HLL merging to (too small to hold its data plus srcA)
                    var newDstHLLPtr = newValue.ToPointer(); // new HLL merging to (large enough to hold srcA and srcB

                    HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValue.Length);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    oldValue.CopyTo(ref newValue);

                    newInputValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    newInputValue.CopyTo(newValue.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(ref newValue, ref output);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(oldValue, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(oldValue, ref output);

                    if (input.arg1 > 0)
                    {
                        Debug.Assert(newValue.Length == oldValue.Length + sizeof(long));
                        byte* pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));
                        var newExpiry = input.arg1;
                        if (!EvaluateExpireCopyUpdate(ref logRecord, ExpireOption.None, newExpiry, newValue, ref _output))
                            return false;
                    }

                    oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan
                            .EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);

                        if (persist) // Persist the key
                        {
                            newValue.AsSpan().CopyTo(newValue.AsSpan());
                            newValue.ShrinkSerializedLength(newValue.Length - newValue.MetadataSize);
                            newValue.UnmarkExtraMetadata();
                        }
                    }
                    break;

                case RespCommand.APPEND:
                    // Copy any existing value with metadata to thew new value
                    oldValue.CopyTo(ref newValue);

                    var appendValue = input.parseState.GetArgSliceByRef(0);

                    // Append the new value with the client input at the end of the old data
                    appendValue.ReadOnlySpan.CopyTo(newValue.AsSpan().Slice(oldValue.LengthWithoutMetadata));

                    CopyValueLengthToOutput(ref newValue, ref output);
                    break;

                default:
                    if ((ushort)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(ushort)input.header.cmd - CustomCommandManager.StartOffset].functions;
                        var expiration = input.arg1;
                        if (expiration == 0)
                        {
                            // We want to retain the old metadata
                            newValue.ExtraMetadata = oldValue.ExtraMetadata;
                        }
                        else if (expiration > 0)
                        {
                            // We want to add the given expiration
                            newValue.ExtraMetadata = expiration;
                        }

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);

                        var ret = functions
                            .CopyUpdater(key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(), newValue.AsSpan(), ref outp, ref rmwInfo);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            rmwInfo.SetUsedValueLength(ref recordInfo, ref newValue, newValue.TotalSize);
            return true;
        }

        /// <inheritdoc />
        public readonly bool PostCopyUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}