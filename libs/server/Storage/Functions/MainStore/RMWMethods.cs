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
        public readonly bool InitialUpdater(ref LogRecord logRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var value = logRecord.ValueSpan;

            // Because this is InitialUpdater, the destination length should be set correctly, but test and log failures to be safe.
            switch (input.header.cmd)
            {
                case RespCommand.PFADD:
                    logRecord.RemoveExpiration();

                    if (!logRecord.TrySetValueSpanLength(HyperLogLog.DefaultHLL.SparseInitialLength(ref input)))
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

                    if (!logRecord.TrySetValueSpanLength(sbSrcHLL.Length))
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

                    if (!logRecord.TrySetValueSpan(SpanByte.FromPinnedSpan(newInputValue)))
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
                    _ = logRecord.TrySetValueSpan(input.parseState.GetArgSliceByRef(0).SpanByte);
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
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!logRecord.TrySetValueSpanLength(BitmapManager.Length(bOffset)))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETBIT");
                        return false;
                    }

                    logRecord.RemoveExpiration();

                    // Always return 0 at initial updater because previous value was 0
                    _ = BitmapManager.UpdateBitmap(value.ToPointer(), bOffset, bSetVal);
                    CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    break;

                case RespCommand.BITFIELD:
                    logRecord.RemoveExpiration();

                    var bitFieldArgs = GetBitFieldArguments(ref input);

                    if (!logRecord.TrySetValueSpanLength(BitmapManager.LengthFromType(bitFieldArgs)))
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
                    if (!logRecord.TrySetValueSpanLength(1))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCR");
                        return false;
                    }
                    _ = TryCopyUpdateNumber(1L, ref value, ref output);
                    break;
                case RespCommand.INCRBY:
                    logRecord.RemoveExpiration();
                    var fNeg = false;
                    var incrBy = input.arg1;

                    var ndigits = NumUtils.NumDigitsInLong(incrBy, ref fNeg);
                    if (!logRecord.TrySetValueSpanLength(ndigits + (fNeg ? 1 : 0)))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCRBY");
                        return false;
                    }

                    _ = TryCopyUpdateNumber(incrBy, ref value, ref output);
                    break;
                case RespCommand.DECR:
                    logRecord.RemoveExpiration();

                    // This is InitialUpdater so set the value to -1 and the length to the # of digits in "-1"
                    if (!logRecord.TrySetValueSpanLength(2))
                    {
                        Debug.Assert(value.Length >= 2, "Length overflow in DECR");
                        return false;
                    }
                    _ = TryCopyUpdateNumber(-1, ref value, ref output);
                    break;
                case RespCommand.DECRBY:
                    logRecord.RemoveExpiration();
                    fNeg = false;
                    var decrBy = -input.arg1;

                    ndigits = NumUtils.NumDigitsInLong(decrBy, ref fNeg);
                    if (!logRecord.TrySetValueSpanLength(ndigits + (fNeg ? 1 : 0)))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "DECRBY");
                        return false;
                    }

                    _ = TryCopyUpdateNumber(decrBy, ref value, ref output);
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
                        if (!logRecord.TrySetValueSpanLength(functions.GetInitialLength(ref input)))
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
                    if (!logRecord.TrySetValueSpan(input.parseState.GetArgSliceByRef(0).SpanByte))
                    {
                        functionsState.logger?.LogError("Failed to set value in {methodName}.{caseName}", "InitialUpdater", "default");
                        return false;
                    }

                    // Copy value to output
                    CopyTo(logRecord.ValueSpan, ref output, functionsState.memoryPool);
                    break;
            }

            // Success if we made it here
            return true;
        }

        /// <inheritdoc />
        public readonly void PostInitialUpdater(ref LogRecord logRecord, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }
        }

        /// <inheritdoc />
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
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

        private readonly bool InPlaceUpdaterWorker(ref LogRecord logRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }
            var valueRef = logRecord.ValueSpanRef;  // reduce redundant offset calculation

            // First byte of input payload identifies command
            switch (input.header.cmd)
            {
                case RespCommand.SETEXNX:
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.ValueSpan, ref output);
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
                        CopyRespTo(logRecord.ValueSpan, ref output);

                    // Copy input to value
                    _ = logRecord.TrySetValueSpan(setValue.SpanByte);
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
                        CopyRespTo(logRecord.ValueSpan, ref output);

                    // Copy input to value
                    _ = logRecord.TrySetValueSpan(setValue.SpanByte);
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
                    return TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: -1);

                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    var incrBy = input.arg1;
                    return TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: incrBy);

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    return TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: -decrBy);

                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    return TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, incrByFloat);

                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(valueRef.Length, bOffset) && !logRecord.TrySetValueSpanLength(BitmapManager.Length(bOffset)))
                        return false;

                    logRecord.RemoveExpiration();

                    var valuePtr = valueRef.ToPointer();
                    var oldValSet = BitmapManager.UpdateBitmap(valuePtr, bOffset, bSetVal);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    return true;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    valuePtr = valueRef.ToPointer();
                    if (!BitmapManager.IsLargeEnoughForType(bitFieldArgs, valueRef.Length) && !logRecord.TrySetValueSpanLength(BitmapManager.LengthFromType(bitFieldArgs)))
                        return false;

                    logRecord.RemoveExpiration();

                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, valuePtr, valueRef.Length);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    return true;

                case RespCommand.PFADD:
                    valuePtr = valueRef.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(valuePtr, valueRef.Length))
                    {
                        *output.SpanByte.ToPointer() = (byte)0xFF;
                        return true;
                    }

                    var updated = false;
                    logRecord.RemoveExpiration();
                    var result = HyperLogLog.DefaultHLL.Update(ref input, valuePtr, valueRef.Length, ref updated);

                    if (result)
                        *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    return result;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLL = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer();
                    var dstHLL = valueRef.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(dstHLL, valueRef.Length))
                    {
                        //InvalidType                                                
                        *(long*)output.SpanByte.ToPointer() = -1;
                        return true;
                    }
                    logRecord.RemoveExpiration();
                    return HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, valueRef.Length);

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    if (newValue.Length + offset > valueRef.Length)
                        return false;

                    newValue.CopyTo(valueRef.AsSpan().Slice(offset));

                    return CopyValueLengthToOutput(valueRef, ref output);

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(valueRef, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(valueRef, ref output);

                    if (input.arg1 > 0)
                    {
                        var pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));

                        var newExpiry = input.arg1;
                        return EvaluateExpireInPlace(ref logRecord, ExpireOption.None, newExpiry, ref _output);
                    }

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan
                            .EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);
                        if (persist)
                            logRecord.RemoveExpiration();
                    }

                    return true;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update. TODO: TrySetValueLength to do an IPU if possible
                    var appendSize = input.parseState.GetArgSliceByRef(0).Length;

                    if (appendSize == 0)
                        return CopyValueLengthToOutput(valueRef, ref output);

                    return false;

                default:
                    var cmd = (ushort)input.header.cmd;
                    if (cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[cmd - CustomCommandManager.StartOffset].functions;
                        var expiration = input.arg1;
                        if (expiration == -1)
                        {
                            // There is existing expiration and we want to clear it.
                            logRecord.RemoveExpiration();
                        }
                        else if (expiration > 0)
                        {
                            // There is no existing metadata, but we want to add it. Try to do in place update.
                            if (!logRecord.TrySetExpiration(expiration))
                                return false;
                        }

                        var valueLength = valueRef.Length;
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functions.InPlaceUpdater(logRecord.Key.AsReadOnlySpan(), ref input, valueRef.AsSpan(), ref valueLength, ref outp, ref rmwInfo);
                        Debug.Assert(valueLength <= valueRef.Length);

                        // Adjust value length if user shrinks it
                        if (valueLength < valueRef.Length)
                            _ = logRecord.TrySetValueSpanLength(valueLength);

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
                            .NeedCopyUpdate(srcLogRecord.Key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(), ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;

                    }
                    return true;
            }
        }

        /// <inheritdoc />
        public readonly bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
        {
            // Expired data
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            var oldValue = srcLogRecord.ValueSpan;
            ref var newValueRef = ref dstLogRecord.ValueSpanRef;

            switch (input.header.cmd)
            {
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(oldValue, ref output);
                    }

                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    Debug.Assert(newInputValue.Length == newValueRef.Length);

                    if (input.arg1 != 0)
                        _ = dstLogRecord.TrySetExpiration(input.arg1);
                    newInputValue.CopyTo(newValueRef.AsSpan());
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    Debug.Assert(setValue.Length == newValueRef.Length);

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(oldValue, ref output);
                    }

                    // Copy input to value, retain metadata of oldValue
                    if (srcLogRecord.Info.HasExpiration && !dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                        return false;
                    setValue.CopyTo(newValueRef.AsSpan());
                    break;

                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    if (!EvaluateExpireCopyUpdate(ref dstLogRecord, expireOption, expiryTicks, newValueRef, ref output))
                        return false;
                    break;

                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    if (!EvaluateExpireCopyUpdate(ref dstLogRecord, expireOption, expiryTicks, newValueRef, ref output))
                        return false;
                    break;

                case RespCommand.PERSIST:
                    oldValue.AsReadOnlySpan().CopyTo(newValueRef.AsSpan());
                    if (srcLogRecord.Info.HasExpiration)
                    {
                        if (!dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                        {
                            functionsState.logger?.LogError("Can't set expiration in {methodName}.{caseName}", "CopyUpdater", "PERSIST");
                            return false;
                        }
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
                        oldValue.CopyTo(ref newValueRef);
                        break;
                    }
                    TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref output, input: incrByFloat);
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');
                    var newValuePtr = newValueRef.ToPointer();
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValuePtr, newValueRef.Length, oldValue.Length);
                    var oldValSet = BitmapManager.UpdateBitmap(newValuePtr, bOffset, bSetVal);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    newValuePtr = newValueRef.ToPointer();
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValuePtr, newValueRef.Length, oldValue.Length);
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, newValuePtr, newValueRef.Length);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.PFADD:
                    var updated = false;
                    newValuePtr = newValueRef.ToPointer();
                    var oldValuePtr = oldValue.ToPointer();

                    if (newValueRef.Length != oldValue.Length)
                        updated = HyperLogLog.DefaultHLL.CopyUpdate(ref input, oldValuePtr, newValuePtr, newValueRef.Length);
                    else
                    {
                        Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValueRef.Length, oldValue.Length);
                        HyperLogLog.DefaultHLL.Update(ref input, newValuePtr, newValueRef.Length, ref updated);
                    }
                    *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    break;

                case RespCommand.PFMERGE:
                    //srcA offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLLPtr = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer(); // HLL merging from
                    var oldDstHLLPtr = oldValue.ToPointer(); // original HLL merging to (too small to hold its data plus srcA)
                    var newDstHLLPtr = newValueRef.ToPointer(); // new HLL merging to (large enough to hold srcA and srcB

                    HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValueRef.Length);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    oldValue.CopyTo(ref newValueRef);

                    newInputValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    newInputValue.CopyTo(newValueRef.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(newValueRef, ref output);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(oldValue, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(oldValue, ref output);

                    Debug.Assert(newValueRef.Length == oldValue.Length);
                    if (input.arg1 > 0)
                    {
                        byte* pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));
                        var newExpiry = input.arg1;
                        if (!EvaluateExpireCopyUpdate(ref dstLogRecord, ExpireOption.None, newExpiry, newValueRef, ref _output))
                            return false;
                    }

                    oldValue.AsReadOnlySpan().CopyTo(newValueRef.AsSpan());

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan
                            .EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);
                        if (persist) // Persist the key
                            dstLogRecord.RemoveExpiration();
                    }
                    break;

                case RespCommand.APPEND:
                    // Copy any existing value with metadata to thew new value
                    oldValue.CopyTo(ref newValueRef);

                    var appendValue = input.parseState.GetArgSliceByRef(0);

                    // Append the new value with the client input at the end of the old data
                    appendValue.ReadOnlySpan.CopyTo(newValueRef.AsSpan().Slice(oldValue.LengthWithoutMetadata));

                    CopyValueLengthToOutput(newValueRef, ref output);
                    break;

                default:
                    if ((ushort)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(ushort)input.header.cmd - CustomCommandManager.StartOffset].functions;
                        var expiration = input.arg1;
                        if (expiration == 0)
                        {
                            // We want to retain the old expiration, if any
                            if (srcLogRecord.Info.HasExpiration && !dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                                return false;
                        }
                        else if (expiration > 0)
                        {
                            // We want to add the given expiration
                            if (!dstLogRecord.TrySetExpiration(expiration))
                                return false;
                        }

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);

                        var ret = functions.CopyUpdater(dstLogRecord.Key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(), newValueRef.AsSpan(), ref outp, ref rmwInfo);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }
            return true;
        }

        /// <inheritdoc />
        public readonly bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : IReadOnlyLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}