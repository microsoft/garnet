// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public readonly bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETKEEPTTLXX:
                case RespCommand.PERSIST:
                case RespCommand.EXPIRE:
                case RespCommand.GETDEL:
                case RespCommand.DELIFEXPIM:
                case RespCommand.GETEX:
                case RespCommand.DELIFGREATER:
                    return false;
                case RespCommand.SETEXXX:
                    // when called withetag all output needs to be placed on the buffer
                    if (input.header.CheckWithETagFlag())
                    {
                        // XX when unsuccesful will write back NIL
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output);
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
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
                                .NeedInitialUpdate(key, ref input, ref writer);
                            return ret;
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }

                    return true;
            }
        }

        /// <inheritdoc />
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(!logRecord.Info.HasETag && !logRecord.Info.HasExpiration, "Should not have Expiration or ETag on InitialUpdater log records");

            // Because this is InitialUpdater, the destination length should be set correctly, but test and log failures to be safe.
            RespCommand cmd = input.header.cmd;
            switch (cmd)
            {
                case RespCommand.PFADD:
                    RecordSizeInfo.AssertValueDataLength(HyperLogLog.DefaultHLL.SparseInitialLength(ref input), in sizeInfo);
                    if (!logRecord.TrySetValueLength(in sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "PFADD");
                        return false;
                    }

                    var value = logRecord.ValueSpan;
                    if (logRecord.IsPinnedValue)
                        HyperLogLog.DefaultHLL.Init(ref input, logRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            HyperLogLog.DefaultHLL.Init(ref input, valuePtr, value.Length);

                    *output.SpanByte.ToPointer() = 1;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy + 4 (skip len size)
                    var sbSrcHLL = input.parseState.GetArgSliceByRef(0);

                    if (!logRecord.TrySetValueLength(sbSrcHLL.Length, in sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "PFMERGE");
                        return false;
                    }

                    value = logRecord.ValueSpan;

                    if (logRecord.IsPinnedValue)
                        Buffer.MemoryCopy(sbSrcHLL.ToPointer(), logRecord.PinnedValuePointer, value.Length, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            Buffer.MemoryCopy(sbSrcHLL.ToPointer(), valuePtr, value.Length, value.Length);

                    break;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!logRecord.TrySetValueSpan(newInputValue, in sizeInfo))
                        return false;
                    if (sizeInfo.FieldInfo.HasExpiration)
                        _ = logRecord.TrySetExpiration(input.arg1);

                    // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                    Debug.Assert(sizeInfo.FieldInfo.HasETag, "Expected sizeInfo.FieldInfo.HasETag to be true");
                    _ = logRecord.TrySetETag(input.parseState.GetLong(1) + (cmd == RespCommand.SETIFMATCH ? 1 : 0));
                    ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);

                    // write back array of the format [etag, nil]
                    var nilResponse = functionsState.nilResp;
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
                    newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!logRecord.TrySetValueSpan(newInputValue, in sizeInfo))
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
                    ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);
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
                    _ = logRecord.TrySetValueSpan(input.parseState.GetArgSliceByRef(0).ReadOnlySpan, in sizeInfo);

                    // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                    if (sizeInfo.FieldInfo.HasETag && !logRecord.TrySetETag(LogRecord.NoETag + 1))
                    {
                        functionsState.logger?.LogError("Could not set etag in {methodName}.{caseName}", "InitialUpdater", "SETKEEPTTL");
                        return false;
                    }
                    ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);
                    // Copy initial etag to output
                    functionsState.CopyRespNumber(LogRecord.NoETag + 1, ref output);
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETEXXX:
                case RespCommand.EXPIRE:
                case RespCommand.PERSIST:
                case RespCommand.GETDEL:
                case RespCommand.GETEX:
                    throw new Exception();

                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!logRecord.TrySetValueLength(BitmapManager.Length(bOffset), in sizeInfo, zeroInit: true))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETBIT");
                        return false;
                    }

                    // Always return 0 at initial updater because previous value was 0
                    value = logRecord.ValueSpan;

                    if (logRecord.IsPinnedValue)
                        _ = BitmapManager.UpdateBitmap(logRecord.PinnedValuePointer, bOffset, bSetVal);
                    else
                        fixed (byte* valuePtr = value)
                            _ = BitmapManager.UpdateBitmap(valuePtr, bOffset, bSetVal);

                    functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);

                    if (!logRecord.TrySetValueLength(BitmapManager.LengthFromType(bitFieldArgs), in sizeInfo, zeroInit: true))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "BitField");
                        return false;
                    }

                    // Ensure new-record space is zero-init'd before we do any bit operations (e.g. it may have been revivified, which for efficiency does not clear old data)
                    value = logRecord.ValueSpan;
                    value.Clear();

                    long bitfieldReturnValue;
                    bool overflow;
                    if (logRecord.IsPinnedValue)
                        (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, logRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, valuePtr, value.Length);

                    if (!overflow)
                        functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    // If the offset is greater than 0, we need to zero-fill the gap (e.g. new record might have been revivified).
                    value = logRecord.ValueSpan;
                    if (offset > 0)
                        value.Slice(0, offset).Clear();
                    newValue.CopyTo(value.Slice(offset));

                    if (!CopyValueLengthToOutput(value, ref output))
                        return false;
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    // Copy value to be appended to the newly allocated value buffer
                    value = logRecord.ValueSpan;
                    appendValue.ReadOnlySpan.CopyTo(value);

                    if (!CopyValueLengthToOutput(value, ref output))
                        return false;
                    break;
                case RespCommand.INCR:
                    // This is InitialUpdater so set the value to 1 and the length to the # of digits in "1"
                    if (!logRecord.TrySetValueLength(1, in sizeInfo))
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
                    if (!logRecord.TrySetValueLength(ndigits + (isNegative ? 1 : 0), in sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCRBY");
                        return false;
                    }

                    _ = TryCopyUpdateNumber(incrBy, logRecord.ValueSpan, ref output);
                    break;
                case RespCommand.DECR:
                    // This is InitialUpdater so set the value to -1 and the length to the # of digits in "-1"
                    if (!logRecord.TrySetValueLength(2, in sizeInfo))
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
                    if (!logRecord.TrySetValueLength(ndigits + (isNegative ? 1 : 0), in sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "DECRBY");
                        return false;
                    }

                    _ = TryCopyUpdateNumber(decrBy, logRecord.ValueSpan, ref output);
                    break;
                case RespCommand.INCRBYFLOAT:
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    if (!TryCopyUpdateNumber(incrByFloat, logRecord.ValueSpan, ref output))
                        return false;
                    break;
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd);
                        if (!logRecord.TrySetValueLength(functions.GetInitialLength(ref input), in sizeInfo, zeroInit: true))   // ZeroInit to be safe
                        {
                            functionsState.logger?.LogError("Length overflow in 'default' > StartOffset: {methodName}.{caseName}", "InitialUpdater", "default");
                            return false;
                        }
                        if (input.arg1 > 0 && !logRecord.TrySetExpiration(input.arg1))
                        {
                            functionsState.logger?.LogError("Could not set expiration in 'default' > StartOffset: {methodName}.{caseName}", "InitialUpdater", "default");
                            return false;
                        }

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            functions.InitialUpdater(logRecord.Key, ref input, logRecord.ValueSpan, ref writer, ref rmwInfo);
                            Debug.Assert(sizeInfo.FieldInfo.ValueSize == logRecord.ValueSpan.Length, $"Inconsistency in initial updater value length: expected {sizeInfo.FieldInfo.ValueSize}, actual {logRecord.ValueSpan.Length}");
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                        break;
                    }

                    // Copy input to value
                    if (!logRecord.TrySetValueSpan(input.parseState.GetArgSliceByRef(0).ReadOnlySpan, in sizeInfo))
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
        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
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
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            if (logRecord.Info.ValueIsObject)
            {
                rmwInfo.Action = RMWAction.WrongType;
                return false;
            }

            if (InPlaceUpdaterWorker(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo))
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
        private readonly bool InPlaceUpdaterWorker(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            RespCommand cmd = input.header.cmd;
            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = cmd is RespCommand.DELIFEXPIM ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                logRecord.RemoveETag();
                return false;
            }

            bool hadETagPreMutation = logRecord.Info.HasETag;
            bool shouldUpdateEtag = hadETagPreMutation;
            if (shouldUpdateEtag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in logRecord);
            bool shouldCheckExpiration = true;

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
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output);
                    }

                    // reset etag state after done using
                    ETagState.ResetState(ref functionsState.etagState);
                    // Nothing is set because being in this block means NX was already violated
                    return true;
                case RespCommand.DELIFGREATER:
                    long etagFromClient = input.parseState.GetLong(0);
                    rmwInfo.Action = etagFromClient > functionsState.etagState.ETag ? RMWAction.ExpireAndStop : RMWAction.CancelOperation;
                    ETagState.ResetState(ref functionsState.etagState);
                    return false;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    etagFromClient = input.parseState.GetLong(1);
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
                            var nilResponse = functionsState.nilResp;
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
                    var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!logRecord.TrySetValueSpan(inputValue, in sizeInfo))
                        return false;
                    long newEtag = cmd is RespCommand.SETIFMATCH ? (functionsState.etagState.ETag + 1) : etagFromClient;
                    if (!logRecord.TrySetETag(newEtag))
                        return false;

                    // Need to check for input.arg1 != 0 because GetRMWModifiedFieldInfo shares its logic with CopyUpdater and thus may set sizeInfo.FieldInfo.Expiration true
                    // due to srcRecordInfo having expiration set; here, that srcRecordInfo is us, so we should do nothing if input.arg1 == 0.
                    if (sizeInfo.FieldInfo.HasExpiration && input.arg1 != 0 && !logRecord.TrySetExpiration(input.arg1))
                        return false;

                    // Write Etag and Val back to Client as an array of the format [etag, nil]
                    var nilResp = functionsState.nilResp;
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

                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!logRecord.TrySetValueSpan(setValue, in sizeInfo))
                        return false;

                    // If shouldUpdateEtag != inputHeaderHasEtag, then if inputHeaderHasEtag is true there is one that nextUpdate will remove (so we don't want to
                    // update it), else there isn't one and nextUpdate will add it.
                    shouldUpdateEtag = inputHeaderHasEtag;

                    // Update expiration
                    if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                        return false;

                    // If withEtag is called we return the etag back in the response
                    if (inputHeaderHasEtag)
                    {
                        var newETag = functionsState.etagState.ETag + 1;
                        if (!logRecord.TrySetETag(newETag))
                            return false;
                        functionsState.CopyRespNumber(newETag, ref output);
                        // reset etag state after done using
                        ETagState.ResetState(ref functionsState.etagState);
                    }
                    else
                    {
                        if (!logRecord.RemoveETag())
                            return false;
                    }

                    shouldUpdateEtag = false;   // since we already updated the ETag
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

                    setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!logRecord.TrySetValueSpan(setValue, in sizeInfo))
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

                case RespCommand.EXPIRE:
                    var expirationWithOption = new ExpirationWithOption(input.arg1);

                    // reset etag state that may have been initialized earlier, but don't update etag because only the expiration was updated
                    ETagState.ResetState(ref functionsState.etagState);
                    return EvaluateExpireInPlace(ref logRecord, expirationWithOption.ExpireOption, expirationWithOption.ExpirationTimeInTicks, ref output);

                case RespCommand.PERSIST:
                    if (logRecord.Info.HasExpiration)
                    {
                        _ = logRecord.RemoveExpiration();
                        output.SpanByte.Span[0] = 1;
                    }

                    // reset etag state that may have been initialized earlier, but don't update etag because only the metadata was updated
                    ETagState.ResetState(ref functionsState.etagState);
                    shouldUpdateEtag = false;
                    break;

                case RespCommand.INCR:
                    if (!TryInPlaceUpdateNumber(ref logRecord, in sizeInfo, ref output, ref rmwInfo, input: 1))
                        return false;
                    break;
                case RespCommand.DECR:
                    if (!TryInPlaceUpdateNumber(ref logRecord, in sizeInfo, ref output, ref rmwInfo, input: -1))
                        return false;
                    break;
                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    var incrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref logRecord, in sizeInfo, ref output, ref rmwInfo, input: incrBy))
                        return false;
                    break;
                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref logRecord, in sizeInfo, ref output, ref rmwInfo, input: -decrBy))
                        return false;
                    break;
                case RespCommand.INCRBYFLOAT:
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    if (!TryInPlaceUpdateNumber(ref logRecord, in sizeInfo, ref output, ref rmwInfo, incrByFloat))
                        return false;
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(logRecord.ValueSpan.Length, bOffset)
                            && !logRecord.TrySetValueLength(BitmapManager.Length(bOffset), in sizeInfo, zeroInit: true))
                        return false;

                    _ = logRecord.RemoveExpiration();

                    byte oldValSet;
                    if (logRecord.IsPinnedValue)
                        oldValSet = BitmapManager.UpdateBitmap(logRecord.PinnedValuePointer, bOffset, bSetVal);
                    else
                        fixed (byte* valuePtr = logRecord.ValueSpan)
                            oldValSet = BitmapManager.UpdateBitmap(valuePtr, bOffset, bSetVal);

                    if (oldValSet == 0)
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    if (!BitmapManager.IsLargeEnoughForType(bitFieldArgs, logRecord.ValueSpan.Length)
                            && !logRecord.TrySetValueLength(BitmapManager.LengthFromType(bitFieldArgs), in sizeInfo, zeroInit: true))
                        return false;

                    _ = logRecord.RemoveExpiration();

                    long bitfieldReturnValue;
                    bool overflow;
                    if (logRecord.IsPinnedValue)
                        (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, logRecord.PinnedValuePointer, logRecord.ValueSpan.Length);
                    else
                        fixed (byte* valuePtr = logRecord.ValueSpan)
                            (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, valuePtr, logRecord.ValueSpan.Length);

                    if (overflow)
                    {
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output);

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        shouldUpdateEtag = false;
                        return true;
                    }

                    functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    break;

                case RespCommand.PFADD:
                    bool result = false, parseOk = false;
                    var updated = false;
                    var valueLen = logRecord.ValueSpan.Length;
                    if (logRecord.IsPinnedValue)
                    {
                        parseOk = result = HyperLogLog.DefaultHLL.IsValidHYLL(logRecord.PinnedValuePointer, valueLen);
                        if (result)
                        {
                            _ = logRecord.RemoveExpiration();
                            result = HyperLogLog.DefaultHLL.Update(ref input, logRecord.PinnedValuePointer, valueLen, ref updated);
                        }
                    }
                    else
                    {
                        fixed (byte* valuePtr = logRecord.ValueSpan)
                        {
                            parseOk = result = HyperLogLog.DefaultHLL.IsValidHYLL(valuePtr, valueLen);
                            if (result)
                            {
                                _ = logRecord.RemoveExpiration();
                                result = HyperLogLog.DefaultHLL.Update(ref input, valuePtr, valueLen, ref updated);
                            }
                        }
                    }

                    if (!parseOk)
                    {
                        *output.SpanByte.ToPointer() = (byte)0xFF;  // Flags invalid HLL

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        return true;
                    }

                    if (result)
                        *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    if (!result)
                        return false;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLL = input.parseState.GetArgSliceByRef(0).ToPointer();

                    result = parseOk = false;
                    valueLen = logRecord.ValueSpan.Length;
                    if (logRecord.IsPinnedValue)
                    {
                        var dstHLL = logRecord.PinnedValuePointer;
                        parseOk = result = HyperLogLog.DefaultHLL.IsValidHYLL(dstHLL, valueLen);
                        if (result)
                        {
                            _ = logRecord.RemoveExpiration();
                            result = HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, valueLen);
                        }
                    }
                    else
                    {
                        fixed (byte* dstHLL = logRecord.ValueSpan)
                        {
                            parseOk = result = HyperLogLog.DefaultHLL.IsValidHYLL(dstHLL, valueLen);
                            if (result)
                            {
                                _ = logRecord.RemoveExpiration();
                                result = HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, valueLen);
                            }
                        }
                    }

                    if (!parseOk)
                    {
                        //InvalidType                                                
                        *output.SpanByte.ToPointer() = (byte)0xFF;  // Flags invalid HLL

                        // reset etag state that may have been initialized earlier, but don't update etag
                        ETagState.ResetState(ref functionsState.etagState);
                        return true;
                    }
                    if (!result)
                        return false;
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    if (newValue.Length + offset > logRecord.ValueSpan.Length
                            && !logRecord.TrySetValueLength(newValue.Length + offset, in sizeInfo))
                        return false;

                    newValue.CopyTo(logRecord.ValueSpan.Slice(offset));
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
                        var pbOutput = stackalloc byte[OutputHeader.Size];
                        var _output = new SpanByteAndMemory(PinnedSpanByte.FromPinnedPointer(pbOutput, OutputHeader.Size));

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
                    return true;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    var appendLength = appendValue.Length;
                    if (appendLength > 0)
                    {
                        // Try to grow in place.
                        var originalLength = logRecord.ValueSpan.Length;
                        if (!logRecord.TrySetValueLength(originalLength + appendLength, in sizeInfo))
                            return false;

                        // Append the new value with the client input at the end of the old data
                        appendValue.
                        // Append the new value with the client input at the end of the old data
                        ReadOnlySpan.CopyTo(logRecord.ValueSpan.Slice(originalLength));
                        if (!CopyValueLengthToOutput(logRecord.ValueSpan, ref output))
                            return false;
                        break;
                    }

                    // reset etag state that may have been initialized earlier, but don't update etag
                    ETagState.ResetState(ref functionsState.etagState);
                    return CopyValueLengthToOutput(logRecord.ValueSpan, ref output);
                case RespCommand.DELIFEXPIM:
                    // this is the case where it isn't expired
                    shouldUpdateEtag = false;
                    break;
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
                        var expirationInTicks = input.arg1;
                        if (expirationInTicks == -1)
                        {
                            // There is existing expiration and we want to clear it.
                            _ = logRecord.RemoveExpiration();
                        }
                        else if (expirationInTicks > 0)
                        {
                            // There is no existing metadata, but we want to add it. Try to do in place update.
                            if (!logRecord.TrySetExpiration(expirationInTicks))
                                return false;
                        }
                        shouldCheckExpiration = false;

                        var value = logRecord.ValueSpan;
                        var valueLength = value.Length;
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            var ret = functions.InPlaceUpdater(logRecord.Key, ref input, value, ref valueLength, ref writer, ref rmwInfo);

                            // Adjust value length if user shrinks it
                            if (valueLength < logRecord.ValueSpan.Length)
                                _ = logRecord.TrySetValueLength(valueLength, in sizeInfo);
                            return ret;
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            // increment the Etag transparently if in place update happened
            if (shouldUpdateEtag)
            {
                logRecord.TrySetETag(this.functionsState.etagState.ETag + 1);
                ETagState.ResetState(ref functionsState.etagState);
            }
            else if (hadETagPreMutation)
            {
                // reset etag state that may have been initialized earlier
                ETagState.ResetState(ref functionsState.etagState);
            }

            sizeInfo.AssertOptionals(logRecord.Info, checkExpiration: shouldCheckExpiration);
            return true;
        }

        // NOTE: In the below control flow if you decide to add a new command or modify a command such that it will now do an early return with FALSE, you must make sure you must reset etagState in FunctionState
        /// <inheritdoc />
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            switch (input.header.cmd)
            {
                case RespCommand.DELIFEXPIM:
                    if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                    }

                    return false;
                case RespCommand.DELIFGREATER:
                    if (srcLogRecord.Info.HasETag)
                        ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);
                    long etagFromClient = input.parseState.GetLong(0);
                    if (etagFromClient > functionsState.etagState.ETag)
                        rmwInfo.Action = RMWAction.ExpireAndStop;

                    ETagState.ResetState(ref functionsState.etagState);
                    // We always return false because we would rather not create a new record in hybrid log if we don't need to delete the object.
                    // Setting no Action and returning false for non-delete case will shortcircuit the InternalRMW code to not run CU, and return SUCCESS.
                    // If we want to delete the object setting the Action to ExpireAndStop will add the tombstone in hybrid log for us.
                    return false;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    if (srcLogRecord.Info.HasETag)
                        ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);

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
                        var nilResponse = functionsState.nilResp;
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
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output);
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

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
                                .NeedCopyUpdate(srcLogRecord.Key, ref input, srcLogRecord.ValueSpan, ref writer);
                            return ret;
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
                    return true;
            }
        }

        // NOTE: Before doing any return from this method, please make sure you are calling reset on etagState in functionsState.
        /// <inheritdoc />
        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
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
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);
            }

            switch (cmd)
            {
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    // By now the comparison for etag against existing etag has already been done in NeedCopyUpdate
                    shouldUpdateEtag = true;

                    var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!dstLogRecord.TrySetValueSpan(inputValue, in sizeInfo))
                        return false;

                    // change the current etag to the the etag sent from client since rest remains same
                    functionsState.etagState.ETag = input.parseState.GetLong(1);
                    if (!dstLogRecord.TrySetETag(functionsState.etagState.ETag + (cmd == RespCommand.SETIFMATCH ? 1 : 0)))
                        return false;

                    if (sizeInfo.FieldInfo.HasExpiration && !dstLogRecord.TrySetExpiration(input.arg1 != 0 ? input.arg1 : srcLogRecord.Expiration))
                        return false;

                    // Write Etag and Val back to Client as an array of the format [etag, nil]
                    long eTagForResponse = cmd == RespCommand.SETIFMATCH ? functionsState.etagState.ETag + 1 : functionsState.etagState.ETag;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.CountDigits(eTagForResponse);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + functionsState.nilResp.Length, functionsState.nilResp, eTagForResponse, ref output, functionsState.memoryPool, writeDirect: true);
                    shouldUpdateEtag = false;   // since we already updated the ETag
                    break;
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    bool inputHeaderHasEtag = input.header.CheckWithETagFlag();

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithETagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(srcLogRecord.ValueSpan, ref output);
                    }

                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    Debug.Assert(newInputValue.Length == dstLogRecord.ValueSpan.Length);

                    // Copy input to value, along with optionals from source record including Expiration.
                    if (!dstLogRecord.TrySetValueSpan(newInputValue, in sizeInfo) || !dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                        return false;

                    // Update expiration if it was supplied.
                    if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                        return false;

                    // If shouldUpdateEtag != inputHeaderHasEtag, then if inputHeaderHasEtag is true there is one that nextUpdate will remove (so we don't want to
                    // update it), else there isn't one and nextUpdate will add it.
                    shouldUpdateEtag = inputHeaderHasEtag;

                    if (inputHeaderHasEtag)
                    {
                        var newETag = functionsState.etagState.ETag + 1;
                        if (!dstLogRecord.TrySetETag(newETag))
                            return false;
                        functionsState.CopyRespNumber(newETag, ref output);
                        ETagState.ResetState(ref functionsState.etagState);
                    }
                    else
                    {
                        if (!dstLogRecord.RemoveETag())
                            return false;
                    }
                    shouldUpdateEtag = false;   // since we already updated the ETag

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

                    inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!dstLogRecord.TrySetValueSpan(inputValue, in sizeInfo))
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
                    shouldUpdateEtag = false;
                    var expirationWithOption = new ExpirationWithOption(input.arg1);

                    // First copy the old Value and non-Expiration optionals to the new record. This will also ensure space for expiration.
                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;

                    if (!EvaluateExpireCopyUpdate(ref dstLogRecord, in sizeInfo, expirationWithOption.ExpireOption, expirationWithOption.ExpirationTimeInTicks, dstLogRecord.ValueSpan, ref output))
                        return false;
                    break;
                case RespCommand.PERSIST:
                    shouldUpdateEtag = false;
                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;
                    if (srcLogRecord.Info.HasExpiration)
                    {
                        dstLogRecord.RemoveExpiration();
                        output.SpanByte.Span[0] = 1;
                    }
                    break;

                case RespCommand.INCR:
                    if (!TryCopyUpdateNumber(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref output, input: 1))
                        return false;
                    break;

                case RespCommand.DECR:
                    if (!TryCopyUpdateNumber(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref output, input: -1))
                        return false;
                    break;

                case RespCommand.INCRBY:
                    var incrBy = input.arg1;
                    if (!TryCopyUpdateNumber(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref output, input: incrBy))
                        return false;
                    break;

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryCopyUpdateNumber(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref output, input: -decrBy))
                        return false;
                    break;

                case RespCommand.INCRBYFLOAT:
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    _ = TryCopyUpdateNumber(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref output, input: incrByFloat);
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;

                    // Some duplicate code to avoid "fixed" when possible
                    var newValue = dstLogRecord.ValueSpan;
                    byte* oldValuePtr;
                    byte oldValSet;
                    if (srcLogRecord.IsPinnedValue)
                    {
                        oldValuePtr = srcLogRecord.PinnedValuePointer;
                        if (dstLogRecord.IsPinnedValue)
                        {
                            var newValuePtr = dstLogRecord.PinnedValuePointer;
                            Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                            oldValSet = BitmapManager.UpdateBitmap(newValuePtr, bOffset, bSetVal);
                        }
                        else
                        {
                            fixed (byte* newValuePtr = dstLogRecord.ValueSpan)
                            {
                                Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                                oldValSet = BitmapManager.UpdateBitmap(newValuePtr, bOffset, bSetVal);
                            }
                        }
                    }
                    else
                    {
                        fixed (byte* oldPtr = srcLogRecord.ValueSpan)
                        {
                            oldValuePtr = oldPtr;
                            if (dstLogRecord.IsPinnedValue)
                            {
                                var newValuePtr = dstLogRecord.PinnedValuePointer;
                                Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                                oldValSet = BitmapManager.UpdateBitmap(newValuePtr, bOffset, bSetVal);
                            }
                            else
                            {
                                fixed (byte* newValuePtr = dstLogRecord.ValueSpan)
                                {
                                    Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                                    oldValSet = BitmapManager.UpdateBitmap(newValuePtr, bOffset, bSetVal);
                                }
                            }
                        }
                    }

                    if (oldValSet == 0)
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;

                    newValue = dstLogRecord.ValueSpan;
                    oldValue = srcLogRecord.ValueSpan;
                    if (newValue.Length > oldValue.Length)
                    {
                        // Zero-init the rest of the new value before we do any bit operations (e.g. it may have been revivified, which for efficiency does not clear old data)
                        newValue.Slice(oldValue.Length).Clear();
                    }

                    long bitfieldReturnValue;
                    bool overflow;
                    if (dstLogRecord.IsPinnedValue)
                        (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, dstLogRecord.PinnedValuePointer, newValue.Length);
                    else
                        fixed (byte* newValuePtr = newValue)
                            (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, newValuePtr, newValue.Length);

                    if (!overflow)
                        functionsState.CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output);
                    break;

                case RespCommand.PFADD:
                    var updated = false;
                    newValue = dstLogRecord.ValueSpan;

                    if (!dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                        return false;

                    // Some duplicate code to avoid "fixed" when possible
                    newValue = dstLogRecord.ValueSpan;
                    if (srcLogRecord.IsPinnedValue)
                    {
                        oldValuePtr = srcLogRecord.PinnedValuePointer;
                        if (dstLogRecord.IsPinnedValue)
                        {
                            var newValuePtr = dstLogRecord.PinnedValuePointer;
                            if (newValue.Length != oldValue.Length)
                                updated = HyperLogLog.DefaultHLL.CopyUpdate(ref input, oldValuePtr, newValuePtr, newValue.Length);
                            else
                            {
                                Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                                _ = HyperLogLog.DefaultHLL.Update(ref input, newValuePtr, newValue.Length, ref updated);
                            }
                        }
                        else
                        {
                            fixed (byte* newValuePtr = dstLogRecord.ValueSpan)
                            {
                                if (newValue.Length != oldValue.Length)
                                    updated = HyperLogLog.DefaultHLL.CopyUpdate(ref input, oldValuePtr, newValuePtr, newValue.Length);
                                else
                                {
                                    Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                                    _ = HyperLogLog.DefaultHLL.Update(ref input, newValuePtr, newValue.Length, ref updated);
                                }
                            }
                        }
                    }
                    else
                    {
                        fixed (byte* oldPtr = srcLogRecord.ValueSpan)
                        {
                            oldValuePtr = oldPtr;
                            if (dstLogRecord.IsPinnedValue)
                            {
                                var newValuePtr = dstLogRecord.PinnedValuePointer;
                                if (newValue.Length != oldValue.Length)
                                    updated = HyperLogLog.DefaultHLL.CopyUpdate(ref input, oldValuePtr, newValuePtr, newValue.Length);
                                else
                                {
                                    Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                                    _ = HyperLogLog.DefaultHLL.Update(ref input, newValuePtr, newValue.Length, ref updated);
                                }
                            }
                            else
                            {
                                fixed (byte* newValuePtr = dstLogRecord.ValueSpan)
                                {
                                    if (newValue.Length != oldValue.Length)
                                        updated = HyperLogLog.DefaultHLL.CopyUpdate(ref input, oldValuePtr, newValuePtr, newValue.Length);
                                    else
                                    {
                                        Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValue.Length, oldValue.Length);
                                        _ = HyperLogLog.DefaultHLL.Update(ref input, newValuePtr, newValue.Length, ref updated);
                                    }
                                }
                            }
                        }
                    }

                    *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    break;

                case RespCommand.PFMERGE:
                    if (!dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                        return false;

                    // Explanation of variables:
                    //srcA offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLLPtr = input.parseState.GetArgSliceByRef(0).ToPointer(); // HLL merging from
                    // byte* oldDstHLLPtr = oldValue.ToPointer(); // original HLL merging to (too small to hold its data plus srcA)
                    // byte* newDstHLLPtr = newValue.ToPointer(); // new HLL merging to (large enough to hold srcA and srcB

                    // Some duplicate code to avoid "fixed" when possible
                    newValue = dstLogRecord.ValueSpan;
                    if (srcLogRecord.IsPinnedValue)
                    {
                        var oldDstHLLPtr = srcLogRecord.PinnedValuePointer;
                        if (dstLogRecord.IsPinnedValue)
                        {
                            var newDstHLLPtr = dstLogRecord.PinnedValuePointer;
                            HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValue.Length);
                        }
                        else
                        {
                            fixed (byte* newDstHLLPtr = dstLogRecord.ValueSpan)
                                HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValue.Length);
                        }
                    }
                    else
                    {
                        fixed (byte* oldDstHLLPtr = srcLogRecord.ValueSpan)
                        {
                            if (dstLogRecord.IsPinnedValue)
                            {
                                var newDstHLLPtr = dstLogRecord.PinnedValuePointer;
                                HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValue.Length);
                            }
                            else
                            {
                                fixed (byte* newDstHLLPtr = dstLogRecord.ValueSpan)
                                    HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValue.Length);
                            }
                        }
                    }

                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);

                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;

                    newValue = dstLogRecord.ValueSpan;
                    if (oldValue.Length < offset)
                    {
                        // The offset is greater than the old value length so we need to zero-fill the gap (e.g. new record might have been revivified, which does not clear out all bytes).
                        newValue.Slice(oldValue.Length, offset - oldValue.Length).Clear();
                    }

                    input.parseState.GetArgSliceByRef(1).ReadOnlySpan.CopyTo(newValue.Slice(offset));

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

                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;

                    newValue = dstLogRecord.ValueSpan;
                    Debug.Assert(newValue.Length == oldValue.Length);
                    if (input.arg1 > 0)
                    {
                        var pbOutput = stackalloc byte[OutputHeader.Size];
                        var _output = new SpanByteAndMemory(PinnedSpanByte.FromPinnedPointer(pbOutput, OutputHeader.Size));
                        var newExpiry = input.arg1;
                        if (!EvaluateExpireCopyUpdate(ref dstLogRecord, in sizeInfo, ExpireOption.None, newExpiry, newValue, ref _output))
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
                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;

                    // Append the new value with the client input at the end of the old data
                    newValue = dstLogRecord.ValueSpan;
                    appendValue.ReadOnlySpan.CopyTo(newValue.Slice(oldValue.Length));

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
                        var expirationInTicks = input.arg1;
                        if (expirationInTicks > 0)
                        {
                            // We want to update to the given expiration
                            if (!dstLogRecord.TrySetExpiration(expirationInTicks))
                                return false;
                        }

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            return functions.CopyUpdater(srcLogRecord.Key, ref input, oldValue, dstLogRecord.ValueSpan, ref writer, ref rmwInfo);
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
                    throw new GarnetException("Unsupported operation on input");
            }


            if (shouldUpdateEtag)
            {
                if (cmd is not RespCommand.SETIFGREATER)
                    functionsState.etagState.ETag++;
                dstLogRecord.TrySetETag(functionsState.etagState.ETag);
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
        public readonly bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(dstLogRecord.Key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}