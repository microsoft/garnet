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
                case RespCommand.PERSIST:
                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                case RespCommand.EXPIREAT:
                case RespCommand.PEXPIREAT:
                case RespCommand.GETDEL:
                case RespCommand.GETEX:
                    return false;
                case RespCommand.SETEXXX:
                    // when called withetag all output needs to be placed on the buffer
                    if (input.header.CheckWithETagFlag())
                    {
                        // XX when unsuccesful will write back NIL
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    }
                    return false;
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                // add etag on first insertion, already tracked by header.CheckWithEtagFlag()
                case RespCommand.SET:
                case RespCommand.SETEXNX:
                case RespCommand.SETKEEPTTL:
                    return true;
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
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
            RespCommand cmd = input.header.cmd;
            switch (cmd)
            {
                case RespCommand.PFADD:
                    RecordSizeInfo.AssertValueDataLength(HyperLogLog.DefaultHLL.SparseInitialLength(ref input), ref sizeInfo);
                    if (!logRecord.TrySetValueLength(ref sizeInfo))
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

                    if (!logRecord.TrySetValueLength(sbSrcHLL.Length, ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "PFMERGE");
                        return false;
                    }

                    value = logRecord.ValueSpan;
                    Buffer.MemoryCopy(sbSrcHLL.ToPointer(), value.ToPointer(), value.Length, value.Length);
                    break;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).SpanByte;
                    if (!logRecord.TrySetValueSpan(newInputValue, ref sizeInfo))
                        return false;
                    if (logRecord.Info.HasExpiration)
                        _ = logRecord.TrySetExpiration(input.arg1);

                    // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                    if (logRecord.Info.HasETag)
                        _ = logRecord.TrySetETag(input.parseState.GetLong(1) + 1);
                    ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref logRecord);

                    // write back array of the format [etag, nil]
                    var nilResponse = CmdStrings.RESP_ERRNOTFOUND;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    WriteValAndEtagToDst(
                        4 + 1 + NumUtils.CountDigits(functionsState.etagState.ETag) + 2 + nilResponse.Length,
                        nilResponse,
                        functionsState.etagState.ETag,
                        ref output,
                        functionsState.memoryPool,
                        writeDirect: true
                    );

                    break;
                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    newInputValue = input.parseState.GetArgSliceByRef(0).SpanByte;
                    if (!logRecord.TrySetValueSpan(newInputValue, ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }

                    // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                    if (sizeInfo.FieldInfo.HasETag && !logRecord.TrySetETag(LogRecord.NoETag + 1))
                    {
                        functionsState.logger?.LogError("Could not set etag in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }
                    ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref logRecord);
                    // Copy initial etag to output only for SET + WITHETAG and not SET NX or XX. TODO: Is this condition satisfied here?
                    functionsState.CopyRespNumber(LogRecord.NoETag + 1, ref output);

                    // Set or remove expiration
                    if (sizeInfo.FieldInfo.HasExpiration && !logRecord.TrySetExpiration(input.arg1))
                    {
                        functionsState.logger?.LogError("Could not set expiration in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }

                    break;
                case RespCommand.SETKEEPTTL:
                    // Copy input to value; do not change expiration
                    _ = logRecord.TrySetValueSpan(input.parseState.GetArgSliceByRef(0).SpanByte, ref sizeInfo);

                    // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                    if (sizeInfo.FieldInfo.HasETag && !logRecord.TrySetETag(LogRecord.NoETag + 1))
                    {
                        functionsState.logger?.LogError("Could not set etag in {methodName}.{caseName}", "InitialUpdater", "SETKEEPTTL");
                        return false;
                    }
                    ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref logRecord);
                    // Copy initial etag to output
                    functionsState.CopyRespNumber(LogRecord.NoETag + 1, ref output);
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
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!logRecord.TrySetValueLength(BitmapManager.Length(bOffset), ref sizeInfo))
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

                    if (!logRecord.TrySetValueLength(BitmapManager.LengthFromType(bitFieldArgs), ref sizeInfo))
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

                case RespCommand.BITFIELD_RO:
                    var bitFieldArgs_RO = GetBitFieldArguments(ref input);
                    if (!logRecord.TrySetValueLength(BitmapManager.LengthFromType(bitFieldArgs_RO), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "BitField");
                        return false;
                    }
                    value = logRecord.ValueSpan;
                    var bitfieldReturnValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO, value.ToPointer(), value.Length);
                    functionsState.CopyRespNumber(bitfieldReturnValue_RO, ref output);
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
                    appendValue.ReadOnlySpan.CopyTo(value.AsSpan());

                    if (!CopyValueLengthToOutput(value, ref output))
                        return false;
                    break;
                case RespCommand.INCR:
                    // This is InitialUpdater so set the value to 1 and the length to the # of digits in "1"
                    if (!logRecord.TrySetValueLength(1, ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCR");
                        return false;
                    }

                    value = logRecord.ValueSpan;
                    _ = TryCopyUpdateNumber(1L, value, ref output);
                    break;
                case RespCommand.INCRBY:
                    var incrBy = input.arg1;

                    var ndigits = NumUtils.CountDigits(incrBy, out var isNegative);
                    if (!logRecord.TrySetValueLength(ndigits + (isNegative ? 1 : 0), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCRBY");
                        return false;
                    }

                    _ = TryCopyUpdateNumber(incrBy, logRecord.ValueSpan, ref output);
                    break;
                case RespCommand.DECR:
                    // This is InitialUpdater so set the value to -1 and the length to the # of digits in "-1"
                    if (!logRecord.TrySetValueLength(2, ref sizeInfo))
                    {
                        Debug.Assert(logRecord.ValueSpan.Length >= 2, "Length overflow in DECR");
                        return false;
                    }
                    value = logRecord.ValueSpan;
                    _ = TryCopyUpdateNumber(-1, value, ref output);
                    break;
                case RespCommand.DECRBY:
                    var decrBy = -input.arg1;

                    ndigits = NumUtils.CountDigits(decrBy, out isNegative);
                    if (!logRecord.TrySetValueLength(ndigits + (isNegative ? 1 : 0), ref sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "DECRBY");
                        return false;
                    }

                    _ = TryCopyUpdateNumber(decrBy, logRecord.ValueSpan, ref output);
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
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd);
                        if (!logRecord.TrySetValueLength(functions.GetInitialLength(ref input), ref sizeInfo))
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
            // reset etag state set at need initial update
            if (input.header.cmd is (RespCommand.SET or RespCommand.SETEXNX or RespCommand.SETKEEPTTL or RespCommand.SETIFMATCH or RespCommand.SETIFGREATER))
                ETagState.ResetState(ref functionsState.etagState);

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

        // NOTE: In the below control flow if you decide to add a new command or modify a command such that it will now do an early return with TRUE,
        // you must make sure you must reset etagState in FunctionState
        private readonly bool InPlaceUpdaterWorker(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                logRecord.RemoveETag();
                return false;
            }

            RespCommand cmd = input.header.cmd;
            bool hadRecordPreMutation = logRecord.Info.HasETag;
            bool shouldUpdateEtag = hadRecordPreMutation;
            if (shouldUpdateEtag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref logRecord);

            switch (cmd)
            {
                case RespCommand.SETEXNX:
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.ValueSpan, ref output);
                    }
                    else if (input.header.CheckWithETagFlag())
                    {
                        // when called withetag all output needs to be placed on the buffer
                        // EXX when unsuccesful will write back NIL
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    }

                    // reset etag state after done using
                    ETagState.ResetState(ref functionsState.etagState);
                    // Nothing is set because being in this block means NX was already violated
                    return true;
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    long etagFromClient = input.parseState.GetLong(1);
                    // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
                    int comparisonResult = etagFromClient.CompareTo(functionsState.etagState.ETag);
                    int expectedResult = cmd is RespCommand.SETIFMATCH ? 0 : 1;

                    if (comparisonResult != expectedResult)
                    {
                        if (input.header.CheckSetGetFlag())
                            CopyRespWithEtagData(logRecord.ValueSpan, ref output, shouldUpdateEtag, functionsState.memoryPool);
                        else
                        {
                            // write back array of the format [etag, nil]
                            var nilResponse = CmdStrings.RESP_ERRNOTFOUND;
                            // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                            WriteValAndEtagToDst(
                                4 + 1 + NumUtils.CountDigits(functionsState.etagState.ETag) + 2 + nilResponse.Length,
                                nilResponse,
                                functionsState.etagState.ETag,
                                ref output,
                                functionsState.memoryPool,
                                writeDirect: true
                            );
                        }
                        // reset etag state after done using
                        ETagState.ResetState(ref functionsState.etagState);
                        return true;
                    }

                    // If we're here we know we have a valid ETag for update. Get the value to update. We'll ned to return false for CopyUpdate if no space for new value.
                    var inputValue = input.parseState.GetArgSliceByRef(0).SpanByte;
                    if (!logRecord.TrySetValueSpan(inputValue, ref sizeInfo))
                        return false;
                    long newEtag = cmd is RespCommand.SETIFMATCH ? (functionsState.etagState.ETag + 1) : (etagFromClient + 1);
                    if (!logRecord.TrySetETag(newEtag))
                        return false;
                    if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                        return false;

                    // Write Etag and Val back to Client as an array of the format [etag, nil]
                    var nilResp = CmdStrings.RESP_ERRNOTFOUND;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.CountDigits(newEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, nilResp, newEtag, ref output, functionsState.memoryPool, writeDirect: true);
                    // reset etag state after done using
                    ETagState.ResetState(ref functionsState.etagState);
                    shouldUpdateEtag = false;   // since we already updated the ETag
                    break;
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.ValueSpan, ref output);
                    }

                    // If the user calls withetag then we need to either update an existing etag and set the value or set the value with an etag and increment it.
                    bool inputHeaderHasEtag = input.header.CheckWithETagFlag();

                    var setValue = input.parseState.GetArgSliceByRef(0);
                    if (!logRecord.TrySetValueSpan(setValue.SpanByte, ref sizeInfo))
                        return false;

                    if (inputHeaderHasEtag != shouldUpdateEtag)
                        shouldUpdateEtag = inputHeaderHasEtag;
                    if (inputHeaderHasEtag)
                    {
                        var newETag = functionsState.etagState.ETag + 1;
                        logRecord.TrySetETag(newETag);
                        functionsState.CopyRespNumber(newETag, ref output);
                    }
                    else
                        logRecord.RemoveETag();
                    shouldUpdateEtag = false;   // since we already updated the ETag

                    if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                        return false;
                    break;
                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    // If the user calls withetag then we need to either update an existing etag and set the value
                    // or set the value with an initial etag and increment it. If withEtag is called we return the etag back to the user
                    inputHeaderHasEtag = input.header.CheckWithETagFlag();

                    // If the SetGet flag is set, copy the current value to output for the GET part of the command.
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithETagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");

                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.ValueSpan, ref output);
                    }

                    setValue = input.parseState.GetArgSliceByRef(0);
                    if (!logRecord.TrySetValueSpan(setValue.SpanByte, ref sizeInfo))
                        return false;

                    if (inputHeaderHasEtag != shouldUpdateEtag)
                        shouldUpdateEtag = inputHeaderHasEtag;
                    if (inputHeaderHasEtag)
                    {
                        var newETag = functionsState.etagState.ETag + 1;
                        logRecord.TrySetETag(newETag);
                        functionsState.CopyRespNumber(newETag, ref output);
                    }
                    else
                        logRecord.RemoveETag();
                    shouldUpdateEtag = false;   // since we already updated the ETag
                    break;

                case RespCommand.PEXPIRE:
                case RespCommand.EXPIRE:
                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    // reset etag state that may have been initialized earlier, but don't update etag because only the metadata was updated
                    ETagState.ResetState(ref functionsState.etagState);
                    shouldUpdateEtag = false;

                    if (!EvaluateExpireInPlace(ref logRecord, expireOption, expiryTicks, ref output))
                        return false;
                    break;
                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    // reset etag state that may have been initialized earlier, but don't update etag because only the metadata was updated
                    ETagState.ResetState(ref functionsState.etagState);
                    shouldUpdateEtag = false;

                    if (!EvaluateExpireInPlace(ref logRecord, expireOption, expiryTicks, ref output))
                        return false;
                    break;

                case RespCommand.PERSIST:
                    if (logRecord.Info.HasExpiration)
                    {
                        _ = logRecord.RemoveExpiration();
                        output.SpanByte.AsSpan()[0] = 1;
                    }

                    // reset etag state that may have been initialized earlier, but don't update etag because only the metadata was updated
                    ETagState.ResetState(ref functionsState.etagState);
                    shouldUpdateEtag = false;
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

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        shouldUpdateEtag = false;
                        break;
                    }
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref sizeInfo, ref output, ref rmwInfo, incrByFloat))
                        return false;
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(logRecord.ValueSpan.Length, bOffset)
                            && !logRecord.TrySetValueLength(BitmapManager.Length(bOffset), ref sizeInfo))
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
                            && !logRecord.TrySetValueLength(BitmapManager.LengthFromType(bitFieldArgs), ref sizeInfo))
                        return false;

                    _ = logRecord.RemoveExpiration();

                    valuePtr = logRecord.ValueSpan.ToPointer();
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, valuePtr, logRecord.ValueSpan.Length);

                    if (overflow)
                    {
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        shouldUpdateEtag = false;
                        return true;
                    }

                    functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    break;

                case RespCommand.BITFIELD_RO:
                    var bitFieldArgs_RO = GetBitFieldArguments(ref input);

                    if (!BitmapManager.IsLargeEnoughForType(bitFieldArgs_RO, logRecord.ValueSpan.Length)
                            && !logRecord.TrySetValueLength(BitmapManager.LengthFromType(bitFieldArgs_RO), ref sizeInfo))
                        return false;

                    _ = logRecord.RemoveExpiration();

                    valuePtr = logRecord.ValueSpan.ToPointer();
                    var bitfieldReturnValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO, valuePtr, logRecord.ValueSpan.Length);
                    functionsState.CopyRespNumber(bitfieldReturnValue_RO, ref output);
                    break;

                case RespCommand.PFADD:
                    valuePtr = logRecord.ValueSpan.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(valuePtr, logRecord.ValueSpan.Length))
                    {
                        *output.SpanByte.ToPointer() = (byte)0xFF;  // Flags invalid HLL

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        return true;
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
                        *output.SpanByte.ToPointer() = (byte)0xFF;  // Flags invalid HLL

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        return true;
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

                    // If both EX and PERSIST were specified, EX wins
                    if (input.arg1 > 0)
                    {
                        var pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));

                        var newExpiry = input.arg1;
                        if (!EvaluateExpireInPlace(ref logRecord, ExpireOption.None, newExpiry, ref _output))
                            return false;
                    }
                    else if (!sizeInfo.FieldInfo.HasExpiration)
                    {
                        // GetRMWModifiedFieldLength saw PERSIST
                        _ = logRecord.RemoveExpiration();
                    }

                    // reset etag state that may have been initialized earlier, but don't update etag
                    ETagState.ResetState(ref functionsState.etagState);
                    shouldUpdateEtag = false;
                    break;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    var appendLength = appendValue.Length;
                    if (appendLength > 0)
                    {
                        // Try to grow in place.
                        var originalLength = logRecord.ValueSpan.Length;
                        if (!logRecord.TrySetValueLength(originalLength + appendLength, ref sizeInfo))
                            return false;

                        // Append the new value with the client input at the end of the old data
                        appendValue.ReadOnlySpan.CopyTo(logRecord.ValueSpan.AsSpan().Slice(originalLength));
                        if (!CopyValueLengthToOutput(logRecord.ValueSpan, ref output))
                            return false;
                        break;
                    }

                    // reset etag state that may have been initialized earlier, but don't update etag
                    ETagState.ResetState(ref functionsState.etagState);
                    return CopyValueLengthToOutput(logRecord.ValueSpan, ref output);

                default:
                    if (cmd > RespCommandExtensions.LastValidCommand)
                    {
                        if (shouldUpdateEtag)
                        {
                            functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                            // reset etag state that may have been initialized earlier but don't update ETag
                            ETagState.ResetState(ref functionsState.etagState);
                            return true;
                        }

                        var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
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
                            _ = logRecord.TrySetValueLength(valueLength, ref sizeInfo);

                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        if (!ret)
                            return false;
                        break;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            // increment the Etag transparently if in place update happened
            if (shouldUpdateEtag)
            {
                logRecord.TrySetETag(this.functionsState.etagState.ETag + 1);
                ETagState.ResetState(ref functionsState.etagState);
            }
            else if (hadRecordPreMutation)
            {
                // reset etag state that may have been initialized earlier
                ETagState.ResetState(ref functionsState.etagState);
            }

            sizeInfo.AssertOptionals(logRecord.Info);
            return true;
        }

        // NOTE: In the below control flow if you decide to add a new command or modify a command such that it will now do an early return with FALSE, you must make sure you must reset etagState in FunctionState
        /// <inheritdoc />
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    long etagToCheckWith = input.parseState.GetLong(1);

                    // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
                    int comparisonResult = etagToCheckWith.CompareTo(functionsState.etagState.ETag);
                    int expectedResult = input.header.cmd is RespCommand.SETIFMATCH ? 0 : 1;

                    if (comparisonResult == expectedResult)
                        return true;

                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespWithEtagData(srcLogRecord.ValueSpan, ref output, srcLogRecord.Info.HasETag, functionsState.memoryPool);
                    }
                    else
                    {
                        // write back array of the format [etag, nil]
                        var nilResponse = CmdStrings.RESP_ERRNOTFOUND;
                        // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                        WriteValAndEtagToDst(
                            4 + 1 + NumUtils.CountDigits(functionsState.etagState.ETag) + 2 + nilResponse.Length,
                            nilResponse,
                            functionsState.etagState.ETag,
                            ref output,
                            functionsState.memoryPool,
                            writeDirect: true
                        );
                    }

                    ETagState.ResetState(ref functionsState.etagState);
                    return false;

                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndResume ensures that we set as new value, since it does not exist
                    if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndResume;

                        // reset etag state that may have been initialized earlier
                        ETagState.ResetState(ref functionsState.etagState);
                        return false;
                    }

                    // since this case is only hit when this an update, the NX is violated and so we can return early from it without setting the value

                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(srcLogRecord.ValueSpan, ref output);
                    }
                    else if (input.header.CheckWithETagFlag())
                    {
                        // EXX when unsuccesful will write back NIL
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    }

                    // reset etag state that may have been initialized earlier
                    ETagState.ResetState(ref functionsState.etagState);
                    return false;
                case RespCommand.SETEXXX:
                    // Expired data, return false immediately so we do not set, since it does not exist
                    // ExpireAndStop ensures that caller sees a NOTFOUND status
                    if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                        // reset etag state that may have been initialized earlier
                        ETagState.ResetState(ref functionsState.etagState);
                        return false;
                    }
                    return true;
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        if (srcLogRecord.Info.HasETag)
                        {
                            functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                            // reset etag state that may have been initialized earlier
                            ETagState.ResetState(ref functionsState.etagState);
                            return false;
                        }
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);

                        var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
                            .NeedCopyUpdate(srcLogRecord.Key.AsReadOnlySpan(), ref input, srcLogRecord.ValueSpan.AsReadOnlySpan(), ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    return true;
            }
        }

        // NOTE: Before doing any return from this method, please make sure you are calling reset on etagState in functionsState.
        /// <inheritdoc />
        public readonly bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            // Expired data
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                _ = dstLogRecord.RemoveETag();
                rmwInfo.Action = RMWAction.ExpireAndResume;
                // reset etag state that may have been initialized earlier
                ETagState.ResetState(ref functionsState.etagState);
                return false;
            }

            var oldValue = srcLogRecord.ValueSpan;  // reduce redundant length calcs
            // Do not pre-get newValue = dstLogRecord.ValueSpan here, because it may change, e.g. moving between inline and overflow

            RespCommand cmd = input.header.cmd;

            bool recordHadEtagPreMutation = srcLogRecord.Info.HasETag;
            bool shouldUpdateEtag = recordHadEtagPreMutation;
            if (shouldUpdateEtag)
            {
                // during checkpointing we might skip the inplace calls and go directly to copy update so we need to initialize here if needed
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref srcLogRecord);
            }

            switch (cmd)
            {
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    // By now the comparison for etag against existing etag has already been done in NeedCopyUpdate
                    shouldUpdateEtag = true;
                    long etagFromClient = input.parseState.GetLong(1);

                    var inputValue = input.parseState.GetArgSliceByRef(0).SpanByte;
                    if (!dstLogRecord.TrySetValueSpan(inputValue, ref sizeInfo))
                        return false;

                    // change the current etag to the the etag sent from client since rest remains same
                    if (cmd == RespCommand.SETIFGREATER)
                        functionsState.etagState.ETag = etagFromClient;
                    long newEtag = functionsState.etagState.ETag + 1;
                    if (!dstLogRecord.TrySetETag(newEtag))
                        return false;

                    if (!(input.arg1 == 0 ? dstLogRecord.RemoveExpiration() : dstLogRecord.TrySetExpiration(input.arg1)))
                        return false;

                    // Write Etag and Val back to Client as an array of the format [etag, nil]
                    var nilResp = CmdStrings.RESP_ERRNOTFOUND;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.CountDigits(newEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, nilResp, newEtag, ref output, functionsState.memoryPool, writeDirect: true);
                    shouldUpdateEtag = false;   // since we already updated the ETag
                    break;
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    bool inputHeaderHasEtag = input.header.CheckWithETagFlag();

                    if (inputHeaderHasEtag != shouldUpdateEtag)
                        shouldUpdateEtag = inputHeaderHasEtag;

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithETagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(oldValue, ref output);
                    }

                    var newInputValue = input.parseState.GetArgSliceByRef(0).SpanByte;
                    Debug.Assert(newInputValue.Length == dstLogRecord.ValueSpan.Length);

                    // Copy input to value, along with optionals from source record including Expiration.
                    if (!dstLogRecord.TrySetValueSpan(newInputValue, ref sizeInfo) || !dstLogRecord.TryCopyRecordOptionals(ref srcLogRecord, ref sizeInfo))
                        return false;

                    if (inputHeaderHasEtag != shouldUpdateEtag)
                        shouldUpdateEtag = inputHeaderHasEtag;
                    if (inputHeaderHasEtag)
                    {
                        var newETag = functionsState.etagState.ETag + 1;
                        dstLogRecord.TrySetETag(newETag);
                        functionsState.CopyRespNumber(newETag, ref output);
                    }
                    else
                        dstLogRecord.RemoveETag();
                    shouldUpdateEtag = false;   // since we already updated the ETag

                    // Update expiration if it was supplied.
                    if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                        return false;
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    // If the user calls withetag then we need to either update an existing etag and set the value
                    // or set the value with an initial etag and increment it. If withEtag is called we return the etag back to the user
                    inputHeaderHasEtag = input.header.CheckWithETagFlag();

                    // If the SetGet flag is set, copy the current value to output for the GET part of the command.
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithETagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");

                        // Copy value to output for the GET part of the command.
                        CopyRespTo(srcLogRecord.ValueSpan, ref output);
                    }

                    inputValue = input.parseState.GetArgSliceByRef(0).SpanByte;
                    if (!dstLogRecord.TrySetValueSpan(inputValue, ref sizeInfo))
                        return false;

                    if (inputHeaderHasEtag != shouldUpdateEtag)
                        shouldUpdateEtag = inputHeaderHasEtag;
                    if (inputHeaderHasEtag)
                    {
                        var newETag = functionsState.etagState.ETag + 1;
                        dstLogRecord.TrySetETag(newETag);
                        functionsState.CopyRespNumber(newETag, ref output);
                    }
                    else
                        dstLogRecord.RemoveETag();
                    shouldUpdateEtag = false;   // since we already updated the ETag

                    break;

                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                    shouldUpdateEtag = false;
                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    // First copy the old Value and non-Expiration optionals to the new record. This will also ensure space for expiration.
                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

                    if (!EvaluateExpireCopyUpdate(ref dstLogRecord, ref sizeInfo, expireOption, expiryTicks, dstLogRecord.ValueSpan, ref output))
                        return false;
                    break;

                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    shouldUpdateEtag = false;
                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    // First copy the old Value and non-Expiration optionals to the new record. This will also ensure space for expiration.
                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

                    if (!EvaluateExpireCopyUpdate(ref dstLogRecord, ref sizeInfo, expireOption, expiryTicks, dstLogRecord.ValueSpan, ref output))
                        return false;
                    break;

                case RespCommand.PERSIST:
                    shouldUpdateEtag = false;
                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;
                    if (srcLogRecord.Info.HasExpiration)
                    {
                        dstLogRecord.RemoveExpiration();
                        output.SpanByte.AsSpan()[0] = 1;
                    }
                    break;

                case RespCommand.INCR:
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
                    var bOffset = input.arg1;
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
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, newValuePtr, newValue.Length);

                    if (overflow)
                    {
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        shouldUpdateEtag = false;
                        return true;
                    }

                    functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    break;

                case RespCommand.BITFIELD_RO:
                    var bitFieldArgs_RO = GetBitFieldArguments(ref input);

                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

                    newValue = dstLogRecord.ValueSpan;
                    var bitfieldReturnValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO, newValue.ToPointer(), newValue.Length);

                    functionsState.CopyRespNumber(bitfieldReturnValue_RO, ref output);
                    break;

                case RespCommand.PFADD:
                    var updated = false;
                    newValue = dstLogRecord.ValueSpan;
                    newValuePtr = newValue.ToPointer();
                    var oldValuePtr = oldValue.ToPointer();

                    if (!dstLogRecord.TryCopyRecordOptionals(ref srcLogRecord, ref sizeInfo))
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
                    if (!dstLogRecord.TryCopyRecordOptionals(ref srcLogRecord, ref sizeInfo))
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
                    newInputValue = input.parseState.GetArgSliceByRef(1).SpanByte;
                    newInputValue.CopyTo(newValue.AsSpan().Slice(offset));

                    _ = CopyValueLengthToOutput(newValue, ref output);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(oldValue, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;

                    // reset etag state that may have been initialized earlier
                    ETagState.ResetState(ref functionsState.etagState);
                    return false;

                case RespCommand.GETEX:
                    shouldUpdateEtag = false;
                    CopyRespTo(oldValue, ref output);

                    if (!dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo))
                        return false;

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

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);
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
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        if (srcLogRecord.Info.HasETag)
                        {
                            functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                            // reset etag state that may have been initialized earlier
                            ETagState.ResetState(ref functionsState.etagState);
                            return true;
                        }

                        var functions = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd);
                        var expiration = input.arg1;
                        if (expiration > 0)
                        {
                            // We want to update to the given expiration
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


            if (shouldUpdateEtag)
            {
                dstLogRecord.TrySetETag(functionsState.etagState.ETag + 1);
                ETagState.ResetState(ref functionsState.etagState);
            }
            else if (recordHadEtagPreMutation)
            {
                // reset etag state that may have been initialized earlier
                ETagState.ResetState(ref functionsState.etagState);
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