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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public readonly bool NeedInitialUpdate(SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
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
        public readonly bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(!logRecord.Info.HasETag && !logRecord.Info.HasExpiration, "Should not have Expiration or ETag on InitialUpdater log records");

            // Because this is InitialUpdater, the destination length should be set correctly, but test and log failures to be safe.
            switch (input.header.cmd)
            {
                case RespCommand.PFADD:
                    sizeInfo.AssertValueDataLength(HyperLogLog.DefaultHLL.SparseInitialLength(ref input));
                    if (!logRecord.TrySetValueSpanLength(ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "PFADD");
                        return false;
                    }

                    var value = logRecord.ValueSpan;
                    HyperLogLog.DefaultHLL.Init(ref input, value.ToPointer(), value.Length);
                    *output.SpanByte.ToPointer() = 1;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy + 4 (skip len size)
                    var sbSrcHLL = input.parseState.GetArgSliceByRef(0).SpanByte;

                    if (!logRecord.TrySetValueSpanLength(sbSrcHLL.Length, ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "PFMERGE");
                        return false;
                    }

                    value = logRecord.ValueSpan;
                    Buffer.MemoryCopy(sbSrcHLL.ToPointer(), value.ToPointer(), value.Length, value.Length);
                    break;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                    if (!logRecord.TrySetValueSpan(SpanByte.FromPinnedSpan(newInputValue), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }

                    // Set or remove expiration
                    if (input.arg1 != 0 && !logRecord.TrySetExpiration(input.arg1))
                    {
                        functionsState.logger?.LogError("Could not set expiration in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }
                    break;

                case RespCommand.SETKEEPTTL:
                    // Copy input to value; do not change expiration
                    _ = logRecord.TrySetValueSpan(input.parseState.GetArgSliceByRef(0).SpanByte, ref sizeInfo);
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

                    if (!logRecord.TrySetValueSpanLength(BitmapManager.Length(bOffset), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETBIT");
                        return false;
                    }

                    // Always return 0 at initial updater because previous value was 0
                    value = logRecord.ValueSpan;
                    _ = BitmapManager.UpdateBitmap(value.ToPointer(), bOffset, bSetVal);
                    functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);

                    if (!logRecord.TrySetValueSpanLength(BitmapManager.LengthFromType(bitFieldArgs), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "BitField");
                        return false;
                    }

                    value = logRecord.ValueSpan;
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, value.ToPointer(), value.Length);
                    if (!overflow)
                        functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    value = logRecord.ValueSpan;
                    newValue.CopyTo(value.AsSpan().Slice(offset));

                    if (!CopyValueLengthToOutput(value, ref output))
                        return false;
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(0);

                    // Copy value to be appended to the newly allocated value buffer
                    value = logRecord.ValueSpan;
                    Debug.Assert(value.Length <= 128, "TODO whhhhat");
                    appendValue.ReadOnlySpan.CopyTo(value.AsSpan());

                    if (!CopyValueLengthToOutput(value, ref output))
                        return false;
                    break;
                case RespCommand.INCR:
                    // This is InitialUpdater so set the value to 1 and the length to the # of digits in "1"
                    if (!logRecord.TrySetValueSpanLength(1, ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCR");
                        return false;
                    }

                    value = logRecord.ValueSpan;
                    _ = TryCopyUpdateNumber(1L, value, ref output);
                    break;
                case RespCommand.INCRBY:
                    var fNeg = false;
                    var incrBy = input.arg1;

                    var ndigits = NumUtils.NumDigitsInLong(incrBy, ref fNeg);
                    if (!logRecord.TrySetValueSpanLength(ndigits + (fNeg ? 1 : 0), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCRBY");
                        return false;
                    }

                    value = logRecord.ValueSpan;
                    _ = TryCopyUpdateNumber(incrBy, value, ref output);
                    break;
                case RespCommand.DECR:
                    // This is InitialUpdater so set the value to -1 and the length to the # of digits in "-1"
                    if (!logRecord.TrySetValueSpanLength(2, ref sizeInfo))
                    {
                        Debug.Assert(logRecord.ValueSpan.Length >= 2, "Length overflow in DECR");
                        return false;
                    }
                    value = logRecord.ValueSpan;
                    _ = TryCopyUpdateNumber(-1, value, ref output);
                    break;
                case RespCommand.DECRBY:
                    fNeg = false;
                    var decrBy = -input.arg1;

                    ndigits = NumUtils.NumDigitsInLong(decrBy, ref fNeg);
                    if (!logRecord.TrySetValueSpanLength(ndigits + (fNeg ? 1 : 0), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "DECRBY");
                        return false;
                    }

                    value = logRecord.ValueSpan;
                    _ = TryCopyUpdateNumber(decrBy, value, ref output);
                    break;
                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }

                    value = logRecord.ValueSpan;
                    if (!TryCopyUpdateNumber(incrByFloat, value, ref output))
                        return false;
                    break;
                default:
                    if ((ushort)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(ushort)input.header.cmd - CustomCommandManager.StartOffset].functions;
                        if (!logRecord.TrySetValueSpanLength(functions.GetInitialLength(ref input), ref sizeInfo))
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
                        value = logRecord.ValueSpan;
                        if (!functions.InitialUpdater(logRecord.Key.AsReadOnlySpan(), ref input, value.AsSpan(), ref outp, ref rmwInfo))
                            return false;
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        break;
                    }

                    // Copy input to value
                    if (!logRecord.TrySetValueSpan(input.parseState.GetArgSliceByRef(0).SpanByte, ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Failed to set value in {methodName}.{caseName}", "InitialUpdater", "default");
                        return false;
                    }

                    // Copy value to output
                    CopyTo(logRecord.ValueSpan, ref output, functionsState.memoryPool);
                    break;
            }

            // Success if we made it here
            sizeInfo.AssertOptionals(logRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public readonly void PostInitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }
        }

        /// <inheritdoc />
        public readonly bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            if (InPlaceUpdaterWorker(ref logRecord, ref sizeInfo, ref input, ref output, ref rmwInfo))
            {
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogRMW(logRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                return true;
            }
            return false;
        }

        private readonly bool InPlaceUpdaterWorker(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
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
                        CopyRespTo(logRecord.ValueSpan, ref output);
                    }
                    break;

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
                    _ = logRecord.TrySetValueSpan(setValue.SpanByte, ref sizeInfo);
                    if (input.arg1 != 0)
                        _ = logRecord.TrySetExpiration(input.arg1);
                    break;

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
                    _ = logRecord.TrySetValueSpan(setValue.SpanByte, ref sizeInfo);
                    break;

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
                    _ = logRecord.RemoveExpiration();
                    break;

                case RespCommand.INCR:
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref sizeInfo, ref output, ref rmwInfo, input: 1))
                        return false;
                    break;
                case RespCommand.DECR:
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref sizeInfo, ref output, ref rmwInfo, input: -1))
                        return false;
                    break;
                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    var incrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref sizeInfo, ref output, ref rmwInfo, input: incrBy))
                        return false;
                    break;
                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref sizeInfo, ref output, ref rmwInfo, input: -decrBy))
                        return false;
                    break;
                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        break;
                    }
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref sizeInfo, ref output, ref rmwInfo, incrByFloat))
                        return false;
                    break;
                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(logRecord.ValueSpan.Length, bOffset) 
                            && !logRecord.TrySetValueSpanLength(BitmapManager.Length(bOffset), ref sizeInfo))
                        return false;

                    _ = logRecord.RemoveExpiration();

                    var valuePtr = logRecord.ValueSpan.ToPointer();
                    var oldValSet = BitmapManager.UpdateBitmap(valuePtr, bOffset, bSetVal);
                    if (oldValSet == 0)
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    if (!BitmapManager.IsLargeEnoughForType(bitFieldArgs, logRecord.ValueSpan.Length) 
                            && !logRecord.TrySetValueSpanLength(BitmapManager.LengthFromType(bitFieldArgs), ref sizeInfo))
                        return false;

                    _ = logRecord.RemoveExpiration();

                    valuePtr = logRecord.ValueSpan.ToPointer();
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, valuePtr, logRecord.ValueSpan.Length);

                    if (!overflow)
                        functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.PFADD:
                    valuePtr = logRecord.ValueSpan.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(valuePtr, logRecord.ValueSpan.Length))
                    {
                        *output.SpanByte.ToPointer() = (byte)0xFF;
                        break;
                    }

                    var updated = false;
                    _ = logRecord.RemoveExpiration();
                    var result = HyperLogLog.DefaultHLL.Update(ref input, valuePtr, logRecord.ValueSpan.Length, ref updated);

                    if (result)
                        *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    if (!result)
                        return false;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLL = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer();
                    var dstHLL = logRecord.ValueSpan.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(dstHLL, logRecord.ValueSpan.Length))
                    {
                        //InvalidType                                                
                        *(long*)output.SpanByte.ToPointer() = -1;
                        break;
                    }
                    _ = logRecord.RemoveExpiration();
                    if (!HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, logRecord.ValueSpan.Length))
                        return false;
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    if (newValue.Length + offset > logRecord.ValueSpan.Length)
                        return false;

                    newValue.CopyTo(logRecord.ValueSpan.AsSpan().Slice(offset));
                    if (!CopyValueLengthToOutput(logRecord.ValueSpan, ref output))
                        return false;
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(logRecord.ValueSpan, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(logRecord.ValueSpan, ref output);

                    if (input.arg1 > 0)
                    {
                        var pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));

                        var newExpiry = input.arg1;
                        if (!EvaluateExpireInPlace(ref logRecord, ExpireOption.None, newExpiry, ref _output))
                            break;
                    }

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan
                            .EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);
                        if (persist)
                            _ = logRecord.RemoveExpiration();
                    }
                    break;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    var appendLength = appendValue.Length;
                    if (appendLength > 0)
                    { 
                        // Try to grow in place.
                        var originalLength = logRecord.ValueSpan.Length;
                        if (!logRecord.TrySetValueSpanLength(originalLength + appendLength, ref sizeInfo))
                            return false;

                        // Append the new value with the client input at the end of the old data
                        appendValue.ReadOnlySpan.CopyTo(logRecord.ValueSpan.AsSpan().Slice(originalLength));
                    }
                    if (!CopyValueLengthToOutput(logRecord.ValueSpan, ref output))
                        return false;
                    break;

                default:
                    var cmd = (ushort)input.header.cmd;
                    if (cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[cmd - CustomCommandManager.StartOffset].functions;
                        var expiration = input.arg1;
                        if (expiration == -1)
                        {
                            // There is existing expiration and we want to clear it.
                            _ = logRecord.RemoveExpiration();
                        }
                        else if (expiration > 0)
                        {
                            // There is no existing metadata, but we want to add it. Try to do in place update.
                            if (!logRecord.TrySetExpiration(expiration))
                                return false;
                        }

                        var valueLength = logRecord.ValueSpan.Length;
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functions.InPlaceUpdater(logRecord.Key.AsReadOnlySpan(), ref input, logRecord.ValueSpan.AsSpan(), ref valueLength, ref outp, ref rmwInfo);
                        Debug.Assert(valueLength <= logRecord.ValueSpan.Length);

                        // Adjust value length if user shrinks it
                        if (valueLength < logRecord.ValueSpan.Length)
                            _ = logRecord.TrySetValueSpanLength(valueLength, ref sizeInfo);

                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        if (!ret)
                            return false;
                        break;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            sizeInfo.AssertOptionals(logRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndResume ensures that we set as new value, since it does not exist
                    if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndResume;
                        return false;
                    }
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(srcLogRecord.ValueSpan, ref output);
                    }
                    return false;
                case RespCommand.SETEXXX:
                    // Expired data, return false immediately so we do not set, since it does not exist
                    // ExpireAndStop ensures that caller sees a NOTFOUND status
                    if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
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
                            .NeedCopyUpdate(srcLogRecord.Key.AsReadOnlySpan(), ref input, srcLogRecord.ValueSpan.AsReadOnlySpan(), ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;

                    }
                    return true;
            }
        }

        /// <inheritdoc />
        public readonly bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            // Expired data
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            var oldValue = srcLogRecord.ValueSpan;  // reduce redundant length calcs
            // Do not pre-get newValue = dstLogRecord.ValueSpan here, because it may change, e.g. moving between inline and overflow

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
                    Debug.Assert(newInputValue.Length == dstLogRecord.ValueSpan.Length);

                    if (input.arg1 != 0)
                        _ = dstLogRecord.TrySetExpiration(input.arg1);
                    // TODO ETag?
                    newInputValue.CopyTo(dstLogRecord.ValueSpan.AsSpan());
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    Debug.Assert(setValue.Length == dstLogRecord.ValueSpan.Length);

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(oldValue, ref output);
                    }

                    // Copy input to value, retain metadata of oldValue
                    if (srcLogRecord.Info.HasExpiration && !dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                        return false;
                    // TODO ETag?
                    setValue.CopyTo(dstLogRecord.ValueSpan.AsSpan());
                    break;

                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    if (!EvaluateExpireCopyUpdate(ref dstLogRecord, ref sizeInfo, expireOption, expiryTicks, dstLogRecord.ValueSpan, ref output))
                        return false;
                    break;

                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    if (!EvaluateExpireCopyUpdate(ref dstLogRecord, ref sizeInfo, expireOption, expiryTicks, dstLogRecord.ValueSpan, ref output))
                        return false;
                    break;

                case RespCommand.PERSIST:
                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;
                    if (srcLogRecord.Info.HasExpiration)
                        output.SpanByte.AsSpan()[0] = 1;
                    break;

                case RespCommand.INCR:
                    // TODO ETag on all of these?
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref output, input: 1))
                        return false;
                    break;

                case RespCommand.DECR:
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref output, input: -1))
                        return false;
                    break;

                case RespCommand.INCRBY:
                    var incrBy = input.arg1;
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref output, input: incrBy))
                        return false;
                    break;

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref output, input: -decrBy))
                        return false;
                    break;

                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        // Move to tail of the log
                        oldValue.CopyTo(dstLogRecord.ValueSpan);
                        break;
                    }
                    _ = TryCopyUpdateNumber(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref output, input: incrByFloat);
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

                    var newValue = dstLogRecord.ValueSpan;
                    var newValuePtr = newValue.ToPointer();
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValuePtr, newValue.Length, oldValue.Length);
                    var oldValSet = BitmapManager.UpdateBitmap(newValuePtr, bOffset, bSetVal);
                    if (oldValSet == 0)
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);

                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

                    newValue = dstLogRecord.ValueSpan;
                    newValuePtr = newValue.ToPointer();
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValuePtr, newValue.Length, oldValue.Length);
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, newValuePtr, newValue.Length);

                    if (!overflow)
                        functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.PFADD:
                    var updated = false;
                    newValue = dstLogRecord.ValueSpan;
                    newValuePtr = newValue.ToPointer();
                    var oldValuePtr = oldValue.ToPointer();

                    // TODO ETag?
                    if (srcLogRecord.Info.HasExpiration && !dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                        return false;

                    if (newValue.Length != oldValue.Length)
                        updated = HyperLogLog.DefaultHLL.CopyUpdate(ref input, oldValuePtr, newValuePtr, newValue.Length);
                    else
                    {
                        Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                        _ = HyperLogLog.DefaultHLL.Update(ref input, newValuePtr, newValue.Length, ref updated);
                    }

                    *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    break;

                case RespCommand.PFMERGE:
                    // TODO ETag?
                    if (srcLogRecord.Info.HasExpiration && !dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                        return false;

                    //srcA offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLLPtr = input.parseState.GetArgSliceByRef(0).SpanByte.ToPointer(); // HLL merging from
                    var oldDstHLLPtr = oldValue.ToPointer(); // original HLL merging to (too small to hold its data plus srcA)
                    newValue = dstLogRecord.ValueSpan;
                    var newDstHLLPtr = newValue.ToPointer(); // new HLL merging to (large enough to hold srcA and srcB

                    HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValue.Length);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);

                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

                    newValue = dstLogRecord.ValueSpan;
                    newInputValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    newInputValue.CopyTo(newValue.AsSpan().Slice(offset));

                    _ = CopyValueLengthToOutput(newValue, ref output);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(oldValue, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(oldValue, ref output);

                    newValue = dstLogRecord.ValueSpan;
                    Debug.Assert(newValue.Length == oldValue.Length);
                    if (input.arg1 > 0)
                    {
                        var pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));
                        var newExpiry = input.arg1;
                        if (!EvaluateExpireCopyUpdate(ref dstLogRecord, ref sizeInfo, ExpireOption.None, newExpiry, newValue, ref _output))
                            return false;
                    }

                    oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan
                            .EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);
                        if (persist) // Persist the key
                            _ = dstLogRecord.RemoveExpiration();
                    }
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

                    // Append the new value with the client input at the end of the old data
                    newValue = dstLogRecord.ValueSpan;
                    appendValue.ReadOnlySpan.CopyTo(newValue.AsSpan().Slice(oldValue.Length));

                    _ = CopyValueLengthToOutput(newValue, ref output);
                    break;

                default:
                    if ((ushort)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(ushort)input.header.cmd - CustomCommandManager.StartOffset].functions;
                        var expiration = input.arg1;
                        if (expiration == 0)
                        {
                            // We want to retain the old expiration, if any. TODO: ETag update?
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

                        var ret = functions.CopyUpdater(dstLogRecord.Key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(), dstLogRecord.ValueSpan.AsSpan(), ref outp, ref rmwInfo);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public readonly bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}