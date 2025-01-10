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
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
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
                case RespCommand.SETIFMATCH:
                case RespCommand.SETEXXX:
                    // when called withetag all output needs to be placed on the buffer
                    if (input.header.CheckWithEtagFlag())
                    {
                        // XX when unsuccesful will write back NIL
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    }
                    return false;
                case RespCommand.SET:
                case RespCommand.SETEXNX:
                case RespCommand.SETKEEPTTL:
                    if (input.header.CheckWithEtagFlag())
                    {
                        this.functionsState.etagState.etagOffsetForVarlen = Constants.EtagSize;
                    }
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
        public bool InitialUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);

            RespCommand cmd = input.header.cmd;
            switch (cmd)
            {
                case RespCommand.PFADD:
                    var v = value.ToPointer();
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(HyperLogLog.DefaultHLL.SparseInitialLength(ref input));
                    HyperLogLog.DefaultHLL.Init(ref input, v, value.Length);
                    *output.SpanByte.ToPointer() = 1;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy + 4 (skip len size)
                    var sbSrcHLL = input.parseState.GetArgSliceByRef(0).SpanByte;
                    var length = sbSrcHLL.Length;
                    var srcHLL = sbSrcHLL.ToPointer();
                    var dstHLL = value.ToPointer();

                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(length);
                    Buffer.MemoryCopy(srcHLL, dstHLL, value.Length, value.Length);
                    break;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    int spaceForEtag = this.functionsState.etagState.etagOffsetForVarlen;
                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    var metadataSize = input.arg1 == 0 ? 0 : sizeof(long);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(newInputValue.Length + metadataSize + spaceForEtag);
                    value.ExtraMetadata = input.arg1;
                    newInputValue.CopyTo(value.AsSpan(spaceForEtag));

                    if (spaceForEtag != 0)
                    {
                        recordInfo.SetHasETag();
                        // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                        value.SetEtagInPayload(Constants.BaseEtag + 1);
                        EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);
                        // Copy initial etag to output only for SET + WITHETAG and not SET NX or XX 
                        CopyRespNumber(Constants.BaseEtag + 1, ref output);
                    }

                    break;
                case RespCommand.SETKEEPTTL:
                    spaceForEtag = this.functionsState.etagState.etagOffsetForVarlen;
                    // Copy input to value, retain metadata in value
                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    value.ShrinkSerializedLength(value.MetadataSize + setValue.Length + spaceForEtag);
                    setValue.CopyTo(value.AsSpan(spaceForEtag));

                    if (spaceForEtag != 0)
                    {
                        recordInfo.SetHasETag();
                        value.SetEtagInPayload(Constants.BaseEtag + 1);
                        EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);
                        // Copy initial etag to output
                        CopyRespNumber(Constants.BaseEtag + 1, ref output);
                    }

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

                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(BitmapManager.Length(bOffset));

                    // Always return 0 at initial updater because previous value was 0
                    BitmapManager.UpdateBitmap(value.ToPointer(), bOffset, bSetVal);
                    CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    break;

                case RespCommand.BITFIELD:
                    value.UnmarkExtraMetadata();
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    value.ShrinkSerializedLength(BitmapManager.LengthFromType(bitFieldArgs));
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

                    CopyValueLengthToOutput(ref value, ref output, 0);
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(0);

                    // Copy value to be appended to the newly allocated value buffer
                    appendValue.ReadOnlySpan.CopyTo(value.AsSpan());

                    CopyValueLengthToOutput(ref value, ref output, 0);
                    break;
                case RespCommand.INCR:
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(1); // # of digits in "1"
                    CopyUpdateNumber(1, ref value, ref output);
                    break;
                case RespCommand.INCRBY:
                    value.UnmarkExtraMetadata();
                    var fNeg = false;
                    var incrBy = input.arg1;
                    var ndigits = NumUtils.NumDigitsInLong(incrBy, ref fNeg);
                    value.ShrinkSerializedLength(ndigits + (fNeg ? 1 : 0));
                    CopyUpdateNumber(incrBy, ref value, ref output);
                    break;
                case RespCommand.DECR:
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(2); // # of digits in "-1"
                    CopyUpdateNumber(-1, ref value, ref output);
                    break;
                case RespCommand.DECRBY:
                    value.UnmarkExtraMetadata();
                    fNeg = false;
                    var decrBy = -input.arg1;
                    ndigits = NumUtils.NumDigitsInLong(decrBy, ref fNeg);
                    value.ShrinkSerializedLength(ndigits + (fNeg ? 1 : 0));
                    CopyUpdateNumber(decrBy, ref value, ref output);
                    break;
                case RespCommand.INCRBYFLOAT:
                    value.UnmarkExtraMetadata();
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    CopyUpdateNumber(incrByFloat, ref value, ref output);
                    break;
                default:
                    value.UnmarkExtraMetadata();
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd);
                        // compute metadata size for result
                        var expiration = input.arg1;
                        metadataSize = expiration switch
                        {
                            -1 => 0,
                            0 => 0,
                            _ => 8,
                        };

                        value.ShrinkSerializedLength(metadataSize + functions.GetInitialLength(ref input));
                        if (expiration > 0)
                            value.ExtraMetadata = expiration;

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        functions.InitialUpdater(key.AsReadOnlySpan(), ref input, value.AsSpan(), ref outp, ref rmwInfo);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        break;
                    }

                    // Copy input to value
                    var inputValue = input.parseState.GetArgSliceByRef(0);
                    value.ShrinkSerializedLength(inputValue.Length);
                    value.ExtraMetadata = input.arg1;
                    inputValue.ReadOnlySpan.CopyTo(value.AsSpan());

                    // Copy value to output
                    CopyTo(ref value, ref output, functionsState.memoryPool);
                    break;
            }

            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
            return true;
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // reset etag state set at need initial update
            if (input.header.cmd is (RespCommand.SET or RespCommand.SETEXNX or RespCommand.SETKEEPTTL))
                EtagState.ResetState(ref functionsState.etagState);

            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            }
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (InPlaceUpdaterWorker(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
            {
                rmwInfo.UsedValueLength = value.TotalSize;
                if (!rmwInfo.RecordInfo.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                return true;
            }
            return false;
        }

        private bool InPlaceUpdaterWorker(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            // Expired data
            if (value.MetadataSize > 0 && input.header.CheckExpiry(value.ExtraMetadata))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                recordInfo.ClearHasETag();
                return false;
            }

            RespCommand cmd = input.header.cmd;
            bool hadRecordPreMutation = recordInfo.ETag;
            bool shouldUpdateEtag = hadRecordPreMutation;
            if (shouldUpdateEtag)
            {
                EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);
            }

            switch (cmd)
            {
                case RespCommand.SETEXNX:
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output);
                    }
                    else if (input.header.CheckWithEtagFlag())
                    {
                        // when called withetag all output needs to be placed on the buffer
                        // EXX when unsuccesful will write back NIL
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    }

                    // reset etag state after done using
                    EtagState.ResetState(ref functionsState.etagState);
                    // Nothing is set because being in this block means NX was already violated
                    return true;
                case RespCommand.SETIFMATCH:
                    long etagFromClient = input.parseState.GetLong(1);
                    if (functionsState.etagState.etag != etagFromClient)
                    {
                        CopyRespWithEtagData(ref value, ref output, shouldUpdateEtag, functionsState.etagState.etagSkippedStart, functionsState.memoryPool);
                        // reset etag state after done using
                        EtagState.ResetState(ref functionsState.etagState);
                        return true;
                    }

                    // Need Copy update if no space for new value
                    var inputValue = input.parseState.GetArgSliceByRef(0);

                    // retain metadata unless metadata sent
                    int metadataSize = input.arg1 != 0 ? sizeof(long) : value.MetadataSize;

                    if (value.Length < inputValue.length + Constants.EtagSize + metadataSize)
                        return false;

                    if (input.arg1 != 0)
                    {
                        value.ExtraMetadata = input.arg1;
                    }

                    recordInfo.SetHasETag();

                    // Increment the ETag
                    long newEtag = functionsState.etagState.etag + 1;

                    value.ShrinkSerializedLength(metadataSize + inputValue.Length + Constants.EtagSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);

                    value.SetEtagInPayload(newEtag);

                    inputValue.ReadOnlySpan.CopyTo(value.AsSpan(Constants.EtagSize));

                    // write back array of the format [etag, nil]
                    var nilResp = CmdStrings.RESP_ERRNOTFOUND;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.NumDigitsInLong(newEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, ref nilResp, newEtag, ref output, functionsState.memoryPool, writeDirect: true);
                    // reset etag state after done using
                    EtagState.ResetState(ref functionsState.etagState);
                    // early return since we already updated the ETag
                    return true;
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    // If the user calls withetag then we need to either update an existing etag and set the value or set the value with an etag and increment it.
                    bool inputHeaderHasEtag = input.header.CheckWithEtagFlag();

                    int nextUpdateEtagOffset = functionsState.etagState.etagSkippedStart;

                    // only when both are not false && false or true and true, do we need to readjust
                    if (inputHeaderHasEtag != shouldUpdateEtag)
                    {
                        // in the common path the above condition is skipped
                        if (inputHeaderHasEtag)
                        {
                            // nextUpdate will add etag but currently there is no etag
                            nextUpdateEtagOffset = Constants.EtagSize;
                            shouldUpdateEtag = true;
                            // if something is going to go past this into copy we need to provide offset management for its varlen during allocation
                            this.functionsState.etagState.etagOffsetForVarlen = Constants.EtagSize;
                        }
                        else
                        {
                            shouldUpdateEtag = false;
                            // nextUpdate will remove etag but currently there is an etag
                            nextUpdateEtagOffset = 0;
                            this.functionsState.etagState.etagOffsetForVarlen = 0;
                        }
                    }

                    ArgSlice setValue = input.parseState.GetArgSliceByRef(0);

                    // Need CU if no space for new value
                    metadataSize = input.arg1 == 0 ? 0 : sizeof(long);
                    if (setValue.Length + metadataSize > value.Length - nextUpdateEtagOffset)
                        return false;

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithEtagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(setValue.Length + metadataSize + nextUpdateEtagOffset);

                    // Copy input to value
                    value.ExtraMetadata = input.arg1;
                    setValue.ReadOnlySpan.CopyTo(value.AsSpan(nextUpdateEtagOffset));
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    // If withEtag is called we return the etag back in the response
                    if (inputHeaderHasEtag)
                    {
                        recordInfo.SetHasETag();
                        value.SetEtagInPayload(functionsState.etagState.etag + 1);
                        // withetag flag means we need to write etag back to the output buffer
                        CopyRespNumber(functionsState.etagState.etag + 1, ref output);
                        // reset etag state after done using
                        EtagState.ResetState(ref functionsState.etagState);
                        // early return since we already updated etag
                        return true;
                    }
                    else
                    {
                        recordInfo.ClearHasETag();
                    }

                    break;
                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    // If the user calls withetag then we need to either update an existing etag and set the value
                    // or set the value with an initial etag and increment it.
                    // If withEtag is called we return the etag back to the user
                    inputHeaderHasEtag = input.header.CheckWithEtagFlag();

                    nextUpdateEtagOffset = functionsState.etagState.etagSkippedStart;

                    // only when both are not false && false or true and true, do we need to readjust
                    if (inputHeaderHasEtag != shouldUpdateEtag)
                    {
                        // in the common path the above condition is skipped
                        if (inputHeaderHasEtag)
                        {
                            // nextUpdate will add etag but currently there is no etag
                            nextUpdateEtagOffset = Constants.EtagSize;
                            shouldUpdateEtag = true;
                            // if something is going to go past this into copy we need to provide offset management for its varlen during allocation
                            this.functionsState.etagState.etagOffsetForVarlen = Constants.EtagSize;
                        }
                        else
                        {
                            shouldUpdateEtag = false;
                            // nextUpdate will remove etag but currentyly there is an etag
                            nextUpdateEtagOffset = 0;
                            this.functionsState.etagState.etagOffsetForVarlen = 0;
                        }
                    }

                    setValue = input.parseState.GetArgSliceByRef(0);
                    // Need CU if no space for new value
                    if (setValue.Length + value.MetadataSize > value.Length - nextUpdateEtagOffset)
                        return false;

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithEtagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(setValue.Length + value.MetadataSize + functionsState.etagState.etagSkippedStart);

                    // Copy input to value
                    setValue.ReadOnlySpan.CopyTo(value.AsSpan(functionsState.etagState.etagSkippedStart));
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    if (inputHeaderHasEtag)
                    {
                        recordInfo.SetHasETag();
                        value.SetEtagInPayload(functionsState.etagState.etag + 1);
                        // withetag flag means we need to write etag back to the output buffer
                        CopyRespNumber(functionsState.etagState.etag + 1, ref output);
                        // reset etag state after done using
                        EtagState.ResetState(ref functionsState.etagState);
                        // early return since we already updated etag
                        return true;
                    }
                    else
                    {
                        recordInfo.ClearHasETag();
                    }

                    break;

                case RespCommand.PEXPIRE:
                case RespCommand.EXPIRE:
                    var expiryExists = value.MetadataSize > 0;

                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);

                    if (!EvaluateExpireInPlace(expireOption, expiryExists, expiryTicks, ref value, ref output))
                        return false;

                    // doesn't update etag, since it's only the metadata that was updated
                    return true; ;
                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    expiryExists = value.MetadataSize > 0;

                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);

                    if (!EvaluateExpireInPlace(expireOption, expiryExists, expiryTicks, ref value, ref output))
                        return false;

                    // doesn't update etag, since it's only the metadata that was updated
                    return true;

                case RespCommand.PERSIST:
                    if (value.MetadataSize != 0)
                    {
                        rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                        value.AsSpan().CopyTo(value.AsSpanWithMetadata());
                        value.ShrinkSerializedLength(value.Length - value.MetadataSize);
                        value.UnmarkExtraMetadata();
                        rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                        output.SpanByte.AsSpan()[0] = 1;
                    }
                    // does not update etag
                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);
                    return true;

                case RespCommand.INCR:
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: 1, functionsState.etagState.etagSkippedStart))
                        return false;
                    break;

                case RespCommand.DECR:
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -1, functionsState.etagState.etagSkippedStart))
                    {

                        return false;
                    }
                    break;

                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    var incrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: incrBy, functionsState.etagState.etagSkippedStart))
                        return false;
                    break;

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -decrBy, functionsState.etagState.etagSkippedStart))
                        return false;
                    break;

                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        return true;
                    }
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, incrByFloat, functionsState.etagState.etagSkippedStart))
                        return false;
                    break;

                case RespCommand.SETBIT:
                    var v = value.ToPointer() + functionsState.etagState.etagSkippedStart;
                    var bOffset = input.parseState.GetLong(0);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(functionsState.etagState.etagAccountedLength, bOffset)) return false;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    var oldValSet = BitmapManager.UpdateBitmap(v, bOffset, bSetVal);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    v = value.ToPointer() + functionsState.etagState.etagSkippedStart;

                    if (!BitmapManager.IsLargeEnoughForType(bitFieldArgs, value.Length - functionsState.etagState.etagSkippedStart))
                        return false;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, v, value.Length - functionsState.etagState.etagSkippedStart);

                    if (overflow)
                    {
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        // etag not updated
                        return true;
                    }

                    CopyRespNumber(bitfieldReturnValue, ref output);
                    break;

                case RespCommand.PFADD:
                    v = value.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(v, value.Length))
                    {
                        *output.SpanByte.ToPointer() = (byte)0xFF;
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
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
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
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

                    if (newValue.Length + offset > value.LengthWithoutMetadata - functionsState.etagState.etagSkippedStart)
                        return false;

                    newValue.CopyTo(value.AsSpan(functionsState.etagState.etagSkippedStart).Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref value, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(ref value, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);

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
                            value.AsSpan().CopyTo(value.AsSpanWithMetadata());
                            value.ShrinkSerializedLength(value.Length - value.MetadataSize);
                            value.UnmarkExtraMetadata();
                            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                            // reset etag state that may have been initialized earlier
                            EtagState.ResetState(ref functionsState.etagState);
                            return true;
                        }
                    }

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);
                    return true;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendSize = input.parseState.GetArgSliceByRef(0).Length;

                    if (appendSize == 0)
                    {
                        CopyValueLengthToOutput(ref value, ref output, functionsState.etagState.etagSkippedStart);
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        return true;
                    }

                    return false;
                default:
                    if (cmd > RespCommandExtensions.LastValidCommand)
                    {
                        if (shouldUpdateEtag)
                        {
                            CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                            // reset etag state that may have been initialized earlier
                            EtagState.ResetState(ref functionsState.etagState);
                            return true;
                        }

                        var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
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

            // increment the Etag transparently if in place update happened
            if (shouldUpdateEtag)
            {
                value.SetEtagInPayload(this.functionsState.etagState.etag + 1);
            }

            if (hadRecordPreMutation)
            {
                // reset etag state that may have been initialized earlier
                EtagState.ResetState(ref functionsState.etagState);
            }

            return true;
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETIFMATCH:

                    long etagToCheckWith = input.parseState.GetLong(1);

                    if (functionsState.etagState.etag == etagToCheckWith)
                    {
                        return true;
                    }
                    else
                    {
                        CopyRespWithEtagData(ref oldValue, ref output, hasEtagInVal: rmwInfo.RecordInfo.ETag, functionsState.etagState.etagSkippedStart, functionsState.memoryPool);
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        return false;
                    }

                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndResume ensures that we set as new value, since it does not exist
                    if (oldValue.MetadataSize > 0 && input.header.CheckExpiry(oldValue.ExtraMetadata))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndResume;
                        rmwInfo.RecordInfo.ClearHasETag();
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        return false;
                    }

                    // since this case is only hit when this an update, the NX is violated and so we can return early from it without setting the value

                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    }
                    else if (input.header.CheckWithEtagFlag())
                    {
                        // EXX when unsuccesful will write back NIL
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    }

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);
                    return false;
                case RespCommand.SETEXXX:
                    // Expired data, return false immediately so we do not set, since it does not exist
                    // ExpireAndStop ensures that caller sees a NOTFOUND status
                    if (oldValue.MetadataSize > 0 && input.header.CheckExpiry(oldValue.ExtraMetadata))
                    {
                        rmwInfo.RecordInfo.ClearHasETag();
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        return false;
                    }
                    return true;
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        if (rmwInfo.RecordInfo.ETag)
                        {
                            CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                            // reset etag state that may have been initialized earlier
                            EtagState.ResetState(ref functionsState.etagState);
                            return false;
                        }
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
                            .NeedCopyUpdate(key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(functionsState.etagState.etagSkippedStart), ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    return true;
            }
        }

        /// <inheritdoc />
        public bool CopyUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            // Expired data
            if (oldValue.MetadataSize > 0 && input.header.CheckExpiry(oldValue.ExtraMetadata))
            {
                recordInfo.ClearHasETag();
                rmwInfo.Action = RMWAction.ExpireAndResume;
                // reset etag state that may have been initialized earlier
                EtagState.ResetState(ref functionsState.etagState);
                return false;
            }

            rmwInfo.ClearExtraValueLength(ref recordInfo, ref newValue, newValue.TotalSize);

            RespCommand cmd = input.header.cmd;

            bool recordHadEtagPreMutation = recordInfo.ETag;
            bool shouldUpdateEtag = recordHadEtagPreMutation;
            if (shouldUpdateEtag)
            {
                // during checkpointing we might skip the inplace calls and go directly to copy update so we need to initialize here if needed
                EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref oldValue);
            }

            switch (cmd)
            {
                case RespCommand.SETIFMATCH:
                    shouldUpdateEtag = true;
                    // Copy input to value
                    Span<byte> dest = newValue.AsSpan(Constants.EtagSize);
                    ReadOnlySpan<byte> src = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                    // retain metadata unless metadata sent
                    int metadataSize = input.arg1 != 0 ? sizeof(long) : oldValue.MetadataSize;

                    Debug.Assert(src.Length + Constants.EtagSize + metadataSize == newValue.Length);

                    src.CopyTo(dest);

                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    if (input.arg1 != 0)
                    {
                        newValue.ExtraMetadata = input.arg1;
                    }

                    long newEtag = functionsState.etagState.etag + 1;

                    recordInfo.SetHasETag();

                    // Write Etag and Val back to Client
                    // write back array of the format [etag, nil]
                    var nilResp = CmdStrings.RESP_ERRNOTFOUND;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.NumDigitsInLong(newEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, ref nilResp, newEtag, ref output, functionsState.memoryPool, writeDirect: true);
                    break;
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    var nextUpdateEtagOffset = functionsState.etagState.etagSkippedStart;
                    var nextUpdateEtagAccountedLength = functionsState.etagState.etagAccountedLength;
                    bool inputWithEtag = input.header.CheckWithEtagFlag();

                    // only when both are not false && false or true and true, do we need to readjust
                    if (inputWithEtag != shouldUpdateEtag)
                    {
                        // in the common path the above condition is skipped
                        if (inputWithEtag)
                        {
                            // nextUpdate will add etag but currently there is no etag
                            nextUpdateEtagOffset = Constants.EtagSize;
                            shouldUpdateEtag = true;
                            recordInfo.SetHasETag();
                        }
                        else
                        {
                            // nextUpdate will remove etag but currentyly there is an etag
                            nextUpdateEtagOffset = 0;
                            shouldUpdateEtag = false;
                            recordInfo.ClearHasETag();
                        }
                    }

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithEtagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    }

                    // Copy input to value
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    metadataSize = input.arg1 == 0 ? 0 : sizeof(long);

                    // new value when allocated should have 8 bytes more if the previous record had etag and the cmd was not SETEXXX
                    Debug.Assert(newInputValue.Length + metadataSize + nextUpdateEtagOffset == newValue.Length);

                    newValue.ExtraMetadata = input.arg1;
                    newInputValue.CopyTo(newValue.AsSpan(nextUpdateEtagOffset));

                    if (inputWithEtag)
                    {
                        CopyRespNumber(functionsState.etagState.etag + 1, ref output);
                    }

                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    nextUpdateEtagOffset = functionsState.etagState.etagSkippedStart;
                    nextUpdateEtagAccountedLength = functionsState.etagState.etagAccountedLength;
                    inputWithEtag = input.header.CheckWithEtagFlag();

                    // only when both are not false && false or true and true, do we need to readjust
                    if (inputWithEtag != shouldUpdateEtag)
                    {
                        // in the common path the above condition is skipped
                        if (inputWithEtag)
                        {
                            // nextUpdate will add etag but currently there is no etag
                            nextUpdateEtagOffset = Constants.EtagSize;
                            shouldUpdateEtag = true;
                            recordInfo.SetHasETag();
                        }
                        else
                        {
                            shouldUpdateEtag = false;
                            // nextUpdate will remove etag but currentyly there is an etag
                            nextUpdateEtagOffset = 0;
                            recordInfo.ClearHasETag();
                        }
                    }

                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                    Debug.Assert(oldValue.MetadataSize + setValue.Length + nextUpdateEtagOffset == newValue.Length);

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        Debug.Assert(!input.header.CheckWithEtagFlag(), "SET GET CANNNOT BE CALLED WITH WITHETAG");
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    }

                    // Copy input to value, retain metadata of oldValue
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    setValue.CopyTo(newValue.AsSpan(nextUpdateEtagOffset));

                    if (inputWithEtag)
                    {
                        CopyRespNumber(functionsState.etagState.etag + 1, ref output);
                    }

                    break;

                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                    shouldUpdateEtag = false;

                    var expiryExists = oldValue.MetadataSize > 0;

                    var expiryValue = input.parseState.GetLong(0);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;
                    var expireOption = (ExpireOption)input.arg1;

                    EvaluateExpireCopyUpdate(expireOption, expiryExists, expiryTicks, ref oldValue, ref newValue, ref output);
                    break;

                case RespCommand.PEXPIREAT:
                case RespCommand.EXPIREAT:
                    expiryExists = oldValue.MetadataSize > 0;
                    shouldUpdateEtag = false;

                    var expiryTimestamp = input.parseState.GetLong(0);
                    expiryTicks = input.header.cmd == RespCommand.PEXPIREAT
                        ? ConvertUtils.UnixTimestampInMillisecondsToTicks(expiryTimestamp)
                        : ConvertUtils.UnixTimestampInSecondsToTicks(expiryTimestamp);
                    expireOption = (ExpireOption)input.arg1;

                    EvaluateExpireCopyUpdate(expireOption, expiryExists, expiryTicks, ref oldValue, ref newValue, ref output);
                    break;

                case RespCommand.PERSIST:
                    shouldUpdateEtag = false;
                    oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                    if (oldValue.MetadataSize != 0)
                    {
                        newValue.AsSpan().CopyTo(newValue.AsSpanWithMetadata());
                        newValue.ShrinkSerializedLength(newValue.Length - newValue.MetadataSize);
                        newValue.UnmarkExtraMetadata();
                        output.SpanByte.AsSpan()[0] = 1;
                    }
                    break;

                case RespCommand.INCR:
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: 1, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.DECR:
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: -1, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.INCRBY:
                    var incrBy = input.arg1;
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: incrBy, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: -decrBy, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.INCRBYFLOAT:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetDouble(0, out var incrByFloat))
                    {
                        // Move to tail of the log
                        oldValue.CopyTo(ref newValue);
                        break;
                    }
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: incrByFloat, functionsState.etagState.etagSkippedStart);
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
                    Buffer.MemoryCopy(oldValue.ToPointer() + functionsState.etagState.etagSkippedStart, newValue.ToPointer() + functionsState.etagState.etagSkippedStart, newValue.Length - functionsState.etagState.etagSkippedStart, oldValue.Length - functionsState.etagState.etagSkippedStart);
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, newValue.ToPointer() + functionsState.etagState.etagSkippedStart, newValue.Length - functionsState.etagState.etagSkippedStart);

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
                    newInputValue.CopyTo(newValue.AsSpan(functionsState.etagState.etagSkippedStart).Slice(offset));

                    CopyValueLengthToOutput(ref newValue, ref output, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref oldValue, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    rmwInfo.Action = RMWAction.ExpireAndStop;

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);
                    return false;

                case RespCommand.GETEX:
                    shouldUpdateEtag = false;
                    CopyRespTo(ref oldValue, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);

                    if (input.arg1 > 0)
                    {
                        Debug.Assert(newValue.Length == oldValue.Length + sizeof(long));
                        byte* pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));
                        var newExpiry = input.arg1;
                        EvaluateExpireCopyUpdate(ExpireOption.None, expiryExists: oldValue.MetadataSize > 0, newExpiry, ref oldValue, ref newValue, ref _output);
                    }

                    oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());

                    if (input.parseState.Count > 0)
                    {
                        var persist = input.parseState.GetArgSliceByRef(0).ReadOnlySpan
                            .EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST);

                        if (persist) // Persist the key
                        {
                            newValue.AsSpan().CopyTo(newValue.AsSpanWithMetadata());
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

                    CopyValueLengthToOutput(ref newValue, ref output, functionsState.etagState.etagSkippedStart);
                    break;

                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        if (recordInfo.ETag)
                        {
                            CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                            // reset etag state that may have been initialized earlier
                            EtagState.ResetState(ref functionsState.etagState);
                            return true;
                        }

                        var functions = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd);
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
                            .CopyUpdater(key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(functionsState.etagState.etagSkippedStart), newValue.AsSpan(functionsState.etagState.etagSkippedStart), ref outp, ref rmwInfo);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            rmwInfo.SetUsedValueLength(ref recordInfo, ref newValue, newValue.TotalSize);

            if (shouldUpdateEtag)
            {
                newValue.SetEtagInPayload(functionsState.etagState.etag + 1);
                EtagState.ResetState(ref functionsState.etagState);
            }
            else if (recordHadEtagPreMutation)
                EtagState.ResetState(ref functionsState.etagState);

            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}