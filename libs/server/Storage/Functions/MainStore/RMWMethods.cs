// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        enum IPUResult : byte
        {
            Failed = 0,
            Succeeded,
            NotUpdated,
        }

        /// <inheritdoc />
        public bool NeedInitialUpdate(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
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
                    if (input.header.CheckWithEtagFlag())
                    {
                        // XX when unsuccesful will write back NIL
                        CopyDefaultResp(functionsState.nilResp, ref output);
                    }
                    return false;
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    // add etag on first insertion
                    this.functionsState.etagState.etagOffsetForVarlen = EtagConstants.EtagSize;
                    return true;
                case RespCommand.SET:
                case RespCommand.SETEXNX:
                case RespCommand.SETKEEPTTL:
                    if (input.header.CheckWithEtagFlag())
                    {
                        this.functionsState.etagState.etagOffsetForVarlen = EtagConstants.EtagSize;
                    }
                    return true;
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
                                .NeedInitialUpdate(key.AsReadOnlySpan(), ref input, ref writer);
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
        public bool InitialUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
            value.UnmarkExtraMetadata();

            RespCommand cmd = input.header.cmd;
            switch (cmd)
            {
                case RespCommand.PFADD:
                    var v = value.ToPointer();
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
                    value.ShrinkSerializedLength(length);
                    Buffer.MemoryCopy(srcHLL, dstHLL, value.Length, value.Length);
                    break;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    int spaceForEtag = this.functionsState.etagState.etagOffsetForVarlen;
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    var metadataSize = input.arg1 == 0 ? 0 : sizeof(long);
                    value.ShrinkSerializedLength(newInputValue.Length + metadataSize + spaceForEtag);
                    value.ExtraMetadata = input.arg1;
                    newInputValue.CopyTo(value.AsSpan(spaceForEtag));
                    long clientSentEtag = input.parseState.GetLong(1);
                    if (cmd == RespCommand.SETIFMATCH)
                        clientSentEtag++;

                    recordInfo.SetHasETag();
                    // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                    value.SetEtagInPayload(clientSentEtag);
                    EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);

                    // write back array of the format [etag, nil]
                    var nilResponse = functionsState.nilResp;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    WriteValAndEtagToDst(
                        4 + 1 + NumUtils.CountDigits(functionsState.etagState.etag) + 2 + nilResponse.Length,
                        ref nilResponse,
                        functionsState.etagState.etag,
                        ref output,
                        functionsState.memoryPool,
                        writeDirect: true
                    );

                    break;
                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    spaceForEtag = this.functionsState.etagState.etagOffsetForVarlen;
                    // Copy input to value
                    newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    metadataSize = input.arg1 == 0 ? 0 : sizeof(long);
                    value.ShrinkSerializedLength(newInputValue.Length + metadataSize + spaceForEtag);
                    value.ExtraMetadata = input.arg1;
                    newInputValue.CopyTo(value.AsSpan(spaceForEtag));

                    if (spaceForEtag != 0)
                    {
                        recordInfo.SetHasETag();
                        // the increment on initial etag is for satisfying the variant that any key with no etag is the same as a zero'd etag
                        value.SetEtagInPayload(EtagConstants.NoETag + 1);
                        EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);
                        // Copy initial etag to output only for SET + WITHETAG and not SET NX or XX
                        CopyRespNumber(EtagConstants.NoETag + 1, ref output);
                    }

                    break;
                case RespCommand.SETKEEPTTL:
                    spaceForEtag = this.functionsState.etagState.etagOffsetForVarlen;
                    // Copy input to value
                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    value.ShrinkSerializedLength(value.MetadataSize + setValue.Length + spaceForEtag);
                    setValue.CopyTo(value.AsSpan(spaceForEtag));

                    if (spaceForEtag != 0)
                    {
                        recordInfo.SetHasETag();
                        value.SetEtagInPayload(EtagConstants.NoETag + 1);
                        EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref value);
                        // Copy initial etag to output
                        CopyRespNumber(EtagConstants.NoETag + 1, ref output);
                    }

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
                    value.ShrinkSerializedLength(BitmapManager.Length(bOffset));
                    BitmapManager.UpdateBitmap(value.ToPointer(), bOffset, bSetVal);
                    // Always return 0 at initial updater because previous value was 0
                    CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    value.ShrinkSerializedLength(BitmapManager.LengthFromType(bitFieldArgs));
                    // Ensure new-record space is zero-init'd before we do any bit operations (e.g. it may have been revivified, which for efficiency does not clear old data)
                    value.AsSpan().Clear();
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, value.ToPointer(), value.Length);
                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(functionsState.nilResp, ref output);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;
                    if (offset > 0)
                    {
                        // If the offset is greater than 0, we need to zero-fill the gap (e.g. new record might have been revivified).
                        value.AsSpan().Slice(0, offset).Clear();
                    }
                    newValue.CopyTo(value.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output, 0);
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    value.ShrinkSerializedLength(appendValue.Length);
                    appendValue.ReadOnlySpan.CopyTo(value.AsSpan());
                    CopyValueLengthToOutput(ref value, ref output, 0);
                    break;
                case RespCommand.INCR:
                    value.ShrinkSerializedLength(1); // # of digits in "1"
                    CopyUpdateNumber(1, ref value, ref output);
                    break;
                case RespCommand.INCRBY:
                    var incrBy = input.arg1;
                    var ndigits = NumUtils.CountDigits(incrBy, out var isNegative);
                    value.ShrinkSerializedLength(ndigits + (isNegative ? 1 : 0));
                    CopyUpdateNumber(incrBy, ref value, ref output);
                    break;
                case RespCommand.DECR:
                    value.ShrinkSerializedLength(2); // # of digits in "-1"
                    CopyUpdateNumber(-1, ref value, ref output);
                    break;
                case RespCommand.DECRBY:
                    isNegative = false;
                    var decrBy = -input.arg1;
                    ndigits = NumUtils.CountDigits(decrBy, out isNegative);
                    value.ShrinkSerializedLength(ndigits + (isNegative ? 1 : 0));
                    CopyUpdateNumber(decrBy, ref value, ref output);
                    break;
                case RespCommand.INCRBYFLOAT:
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    CopyUpdateNumber(incrByFloat, ref value, ref output);
                    break;

                case RespCommand.VADD:
                    {
                        if (input.arg1 is VectorManager.VADDAppendLogArg or VectorManager.MigrateElementKeyLogArg or VectorManager.MigrateIndexKeyLogArg)
                        {
                            // Synthetic op, do nothing
                            break;
                        }

                        var dims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(0).Span);
                        var reduceDims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(1).Span);
                        // ValueType is here, skipping during index creation
                        // Values is here, skipping during index creation
                        // Element is here, skipping during index creation
                        var quantizer = MemoryMarshal.Read<VectorQuantType>(input.parseState.GetArgSliceByRef(5).Span);
                        var buildExplorationFactor = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(6).Span);
                        // Attributes is here, skipping during index creation
                        var numLinks = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(8).Span);
                        var distanceMetric = MemoryMarshal.Read<VectorDistanceMetricType>(input.parseState.GetArgSliceByRef(9).Span);

                        // Pre-allocated by caller because DiskANN needs to be able to call into Garnet as part of create_index
                        // and thus we can't call into it from session functions
                        var context = MemoryMarshal.Read<ulong>(input.parseState.GetArgSliceByRef(10).Span);
                        var index = MemoryMarshal.Read<nint>(input.parseState.GetArgSliceByRef(11).Span);

                        recordInfo.VectorSet = true;

                        functionsState.vectorManager.CreateIndex(dims, reduceDims, quantizer, buildExplorationFactor, numLinks, distanceMetric, context, index, ref value);
                    }
                    break;
                case RespCommand.VREM:
                    Debug.Assert(input.arg1 == VectorManager.VREMAppendLogArg, "Should only see VREM writes as part of replication");
                    break;
                default:
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

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            functions.InitialUpdater(key.AsReadOnlySpan(), ref input, value.AsSpan(), ref writer, ref rmwInfo);
                        }
                        finally
                        {
                            writer.Dispose();
                        }
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
            if (input.header.cmd is (RespCommand.SET or RespCommand.SETEXNX or RespCommand.SETKEEPTTL or RespCommand.SETIFMATCH or RespCommand.SETIFGREATER))
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
            var ipuResult = InPlaceUpdaterWorker(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            switch (ipuResult)
            {
                case IPUResult.Failed:
                    return false;
                case IPUResult.Succeeded:
                    rmwInfo.UsedValueLength = value.TotalSize;
                    if (!rmwInfo.RecordInfo.Modified)
                        functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                    if (functionsState.appendOnlyFile != null)
                        WriteLogRMW(ref key, ref input, rmwInfo.Version, rmwInfo.SessionID);
                    return true;
                case IPUResult.NotUpdated:
                default:
                    return true;
            }
        }

        // NOTE: In the below control flow if you decide to add a new command or modify a command such that it will now do an early return with TRUE, you must make sure you must reset etagState in FunctionState
        private IPUResult InPlaceUpdaterWorker(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            RespCommand cmd = input.header.cmd;
            // Expired data
            if (value.MetadataSize == 8 && input.header.CheckExpiry(value.ExtraMetadata))
            {
                rmwInfo.Action = cmd is RespCommand.DELIFEXPIM ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                recordInfo.ClearHasETag();
                return IPUResult.Failed;
            }

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
                        CopyDefaultResp(functionsState.nilResp, ref output);
                    }

                    // reset etag state after done using
                    EtagState.ResetState(ref functionsState.etagState);
                    // Nothing is set because being in this block means NX was already violated
                    return IPUResult.NotUpdated;

                case RespCommand.DELIFGREATER:
                    long etagFromClient = input.parseState.GetLong(0);
                    rmwInfo.Action = etagFromClient > functionsState.etagState.etag ? RMWAction.ExpireAndStop : RMWAction.CancelOperation;
                    EtagState.ResetState(ref functionsState.etagState);
                    return IPUResult.Failed;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    etagFromClient = input.parseState.GetLong(1);
                    // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
                    int comparisonResult = etagFromClient.CompareTo(functionsState.etagState.etag);
                    int expectedResult = cmd is RespCommand.SETIFMATCH ? 0 : 1;

                    if (comparisonResult != expectedResult)
                    {
                        if (input.header.CheckSetGetFlag())
                        {
                            CopyRespWithEtagData(ref value, ref output, shouldUpdateEtag, functionsState.etagState.etagSkippedStart, functionsState.memoryPool);
                        }
                        else
                        {
                            // write back array of the format [etag, nil]
                            var nilResponse = functionsState.nilResp;
                            // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                            WriteValAndEtagToDst(
                                4 + 1 + NumUtils.CountDigits(functionsState.etagState.etag) + 2 + nilResponse.Length,
                                ref nilResponse,
                                functionsState.etagState.etag,
                                ref output,
                                functionsState.memoryPool,
                                writeDirect: true
                            );
                        }
                        // reset etag state after done using
                        EtagState.ResetState(ref functionsState.etagState);
                        return IPUResult.NotUpdated;
                    }

                    // Need Copy update if no space for new value
                    var inputValue = input.parseState.GetArgSliceByRef(0);

                    // retain metadata unless metadata sent
                    int metadataSize = input.arg1 != 0 ? sizeof(long) : value.MetadataSize;

                    if (value.Length < inputValue.length + EtagConstants.EtagSize + metadataSize)
                        return IPUResult.Failed;

                    recordInfo.SetHasETag();

                    long newEtag = cmd is RespCommand.SETIFMATCH ? (functionsState.etagState.etag + 1) : etagFromClient;

                    long oldExtraMetadata = value.ExtraMetadata;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(metadataSize + inputValue.Length + EtagConstants.EtagSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    if (input.arg1 != 0)
                    {
                        value.ExtraMetadata = input.arg1;
                    }
                    else if (oldExtraMetadata != 0)
                    {
                        value.ExtraMetadata = oldExtraMetadata;
                    }

                    value.SetEtagInPayload(newEtag);

                    inputValue.ReadOnlySpan.CopyTo(value.AsSpan(EtagConstants.EtagSize));

                    // write back array of the format [etag, nil]
                    var nilResp = functionsState.nilResp;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.CountDigits(newEtag);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, ref nilResp, newEtag, ref output, functionsState.memoryPool, writeDirect: true);
                    // reset etag state after done using
                    EtagState.ResetState(ref functionsState.etagState);
                    // early return since we already updated the ETag
                    return IPUResult.Succeeded;

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
                            nextUpdateEtagOffset = EtagConstants.EtagSize;
                            shouldUpdateEtag = true;
                            // if something is going to go past this into copy we need to provide offset management for its varlen during allocation
                            this.functionsState.etagState.etagOffsetForVarlen = EtagConstants.EtagSize;
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
                        return IPUResult.Failed;

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
                        return IPUResult.Succeeded;
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
                            nextUpdateEtagOffset = EtagConstants.EtagSize;
                            shouldUpdateEtag = true;
                            // if something is going to go past this into copy we need to provide offset management for its varlen during allocation
                            this.functionsState.etagState.etagOffsetForVarlen = EtagConstants.EtagSize;
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
                        return IPUResult.Failed;

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
                        return IPUResult.Succeeded;
                    }
                    else
                    {
                        recordInfo.ClearHasETag();
                    }

                    break;

                case RespCommand.EXPIRE:
                    var expiryExists = value.MetadataSize == 8;

                    var expirationWithOption = new ExpirationWithOption(input.arg1);

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);

                    return EvaluateExpireInPlace(expirationWithOption.ExpireOption, expiryExists, expirationWithOption.ExpirationTimeInTicks, ref value, ref output);

                case RespCommand.PERSIST:
                    if (value.MetadataSize == 8)
                    {
                        rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                        value.AsSpan().CopyTo(value.AsSpanWithMetadata());
                        value.ShrinkSerializedLength(value.Length - value.MetadataSize);
                        value.UnmarkExtraMetadata();
                        rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                        output.SpanByte.AsSpan()[0] = 1;
                        EtagState.ResetState(ref functionsState.etagState);
                        return IPUResult.Succeeded;
                    }
                    else
                    {
                        EtagState.ResetState(ref functionsState.etagState);
                        return IPUResult.NotUpdated;
                    }

                case RespCommand.INCR:
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: 1, functionsState.etagState.etagSkippedStart))
                        return IPUResult.Failed;
                    break;

                case RespCommand.DECR:
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -1, functionsState.etagState.etagSkippedStart))
                    {

                        return IPUResult.Failed;
                    }
                    break;

                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    var incrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: incrBy, functionsState.etagState.etagSkippedStart))
                        return IPUResult.Failed;
                    break;

                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -decrBy, functionsState.etagState.etagSkippedStart))
                        return IPUResult.Failed;
                    break;

                case RespCommand.INCRBYFLOAT:
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, incrByFloat, functionsState.etagState.etagSkippedStart))
                        return IPUResult.Failed;
                    break;

                case RespCommand.SETBIT:
                    var v = value.ToPointer() + functionsState.etagState.etagSkippedStart;
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(functionsState.etagState.etagAccountedLength, bOffset)) return IPUResult.Failed;

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
                        return IPUResult.Failed;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, v, value.Length - functionsState.etagState.etagSkippedStart);

                    if (overflow)
                    {
                        CopyDefaultResp(functionsState.nilResp, ref output);
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        return IPUResult.Succeeded;
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
                        return IPUResult.NotUpdated;
                    }

                    var updated = false;
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    var result = HyperLogLog.DefaultHLL.Update(ref input, v, value.Length, ref updated);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    if (result)
                        *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    return result ? IPUResult.Succeeded : IPUResult.Failed;

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
                        return IPUResult.NotUpdated;
                    }
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                    return HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, value.Length) ? IPUResult.Succeeded : IPUResult.Failed;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    if (newValue.Length + offset > value.LengthWithoutMetadata - functionsState.etagState.etagSkippedStart)
                        return IPUResult.Failed;

                    newValue.CopyTo(value.AsSpan(functionsState.etagState.etagSkippedStart).Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref value, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return IPUResult.Failed;

                case RespCommand.GETEX:
                    CopyRespTo(ref value, ref output, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);

                    if (input.arg1 > 0)
                    {
                        byte* pbOutput = stackalloc byte[ObjectOutputHeader.Size];
                        var _output = new SpanByteAndMemory(SpanByte.FromPinnedPointer(pbOutput, ObjectOutputHeader.Size));

                        var newExpiry = input.arg1;
                        return EvaluateExpireInPlace(ExpireOption.None, expiryExists: value.MetadataSize == 8, newExpiry, ref value, ref _output);
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
                            return IPUResult.Succeeded;
                        }
                    }

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);
                    return IPUResult.NotUpdated;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendSize = input.parseState.GetArgSliceByRef(0).Length;

                    if (appendSize == 0)
                    {
                        CopyValueLengthToOutput(ref value, ref output, functionsState.etagState.etagSkippedStart);
                        // reset etag state that may have been initialized earlier
                        EtagState.ResetState(ref functionsState.etagState);
                        return IPUResult.NotUpdated;
                    }

                    return IPUResult.Failed;
                case RespCommand.DELIFEXPIM:
                    // this is the case where it isn't expired
                    shouldUpdateEtag = false;
                    break;
                case RespCommand.VADD:
                    // Adding to an existing VectorSet is modeled as a read operations
                    //
                    // However, we do synthesize some (pointless) writes to implement replication
                    // and a "make me delete=able"-update during drop.
                    //
                    // Another "not quite write" is the recreate an index write operation
                    // that occurs if we're adding to an index that was restored from disk 
                    // or a primary node.

                    // Handle "make me delete-able"
                    if (input.arg1 == VectorManager.DeleteAfterDropArg)
                    {
                        value.AsSpan().Clear();
                    }
                    else if (input.arg1 == VectorManager.RecreateIndexArg)
                    {
                        var newIndexPtr = MemoryMarshal.Read<nint>(input.parseState.GetArgSliceByRef(10).Span);

                        functionsState.vectorManager.RecreateIndex(newIndexPtr, ref value);
                    }

                    // Ignore everything else
                    return IPUResult.Succeeded;
                case RespCommand.VREM:
                    // Removing from a VectorSet is modeled as a read operations
                    //
                    // However, we do synthesize some (pointless) writes to implement replication
                    // in a similar manner to VADD.

                    Debug.Assert(input.arg1 == VectorManager.VREMAppendLogArg, "VREM in place update should only happen for replication");                    // Ignore everything else
                    return IPUResult.Succeeded;
                default:
                    if (cmd > RespCommandExtensions.LastValidCommand)
                    {
                        if (shouldUpdateEtag)
                        {
                            CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                            // reset etag state that may have been initialized earlier
                            EtagState.ResetState(ref functionsState.etagState);
                            return IPUResult.Succeeded;
                        }

                        var functions = functionsState.GetCustomCommandFunctions((ushort)cmd);
                        var expirationInTicks = input.arg1;
                        if (expirationInTicks == -1)
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
                        else if (expirationInTicks > 0)
                        {
                            // there is no existing metadata, but we want to add it. we cannot do in place update.
                            if (value.ExtraMetadata == 0) return IPUResult.Failed;
                            // set expiration to the specific value
                            value.ExtraMetadata = expirationInTicks;
                        }

                        var valueLength = value.LengthWithoutMetadata;

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            var ret = functions.InPlaceUpdater(key.AsReadOnlySpan(), ref input, value.AsSpan(), ref valueLength, ref writer, ref rmwInfo);
                            Debug.Assert(valueLength <= value.LengthWithoutMetadata);

                            // Adjust value length if user shrinks it
                            if (valueLength < value.LengthWithoutMetadata)
                            {
                                rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                                value.ShrinkSerializedLength(valueLength + value.MetadataSize);
                                rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                            }

                            return ret ? IPUResult.Succeeded : IPUResult.Failed;
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
                value.SetEtagInPayload(this.functionsState.etagState.etag + 1);
            }

            if (hadRecordPreMutation)
            {
                // reset etag state that may have been initialized earlier
                EtagState.ResetState(ref functionsState.etagState);
            }

            return IPUResult.Succeeded;
        }

        // NOTE: In the below control flow if you decide to add a new command or modify a command such that it will now do an early return with FALSE, you must make sure you must reset etagState in FunctionState
        /// <inheritdoc />
        public bool NeedCopyUpdate(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            switch (input.header.cmd)
            {
                case RespCommand.DELIFEXPIM:
                    if (oldValue.MetadataSize == 8 && input.header.CheckExpiry(oldValue.ExtraMetadata))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                    }

                    return false;
                case RespCommand.DELIFGREATER:
                    if (rmwInfo.RecordInfo.ETag)
                        EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref oldValue);

                    long etagFromClient = input.parseState.GetLong(0);
                    if (etagFromClient > functionsState.etagState.etag)
                    {
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                    }

                    EtagState.ResetState(ref functionsState.etagState);
                    // We always return false because we would rather not create a new record in hybrid log if we don't need to delete the object.
                    // Setting no Action and returning false for non-delete case will shortcircuit the InternalRMW code to not run CU, and return SUCCESS.
                    // If we want to delete the object setting the Action to ExpireAndStop will add the tombstone in hybrid log for us.
                    return false;

                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    if (rmwInfo.RecordInfo.ETag)
                        EtagState.SetValsForRecordWithEtag(ref functionsState.etagState, ref oldValue);

                    long etagToCheckWith = input.parseState.GetLong(1);

                    // in IFMATCH we check for equality, in IFGREATER we are checking for sent etag being strictly greater
                    int comparisonResult = etagToCheckWith.CompareTo(functionsState.etagState.etag);
                    int expectedResult = input.header.cmd is RespCommand.SETIFMATCH ? 0 : 1;

                    if (comparisonResult == expectedResult)
                    {
                        return true;
                    }

                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespWithEtagData(ref oldValue, ref output, hasEtagInVal: rmwInfo.RecordInfo.ETag, functionsState.etagState.etagSkippedStart, functionsState.memoryPool);
                    }
                    else
                    {
                        // write back array of the format [etag, nil]
                        var nilResponse = functionsState.nilResp;
                        // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                        WriteValAndEtagToDst(
                            4 + 1 + NumUtils.CountDigits(functionsState.etagState.etag) + 2 + nilResponse.Length,
                            ref nilResponse,
                            functionsState.etagState.etag,
                            ref output,
                            functionsState.memoryPool,
                            writeDirect: true
                        );
                    }

                    EtagState.ResetState(ref functionsState.etagState);
                    return false;
                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndResume ensures that we set as new value, since it does not exist
                    if (oldValue.MetadataSize == 8 && input.header.CheckExpiry(oldValue.ExtraMetadata))
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
                        CopyDefaultResp(functionsState.nilResp, ref output);
                    }

                    // reset etag state that may have been initialized earlier
                    EtagState.ResetState(ref functionsState.etagState);
                    return false;
                case RespCommand.SETEXXX:
                    // Expired data, return false immediately so we do not set, since it does not exist
                    // ExpireAndStop ensures that caller sees a NOTFOUND status
                    if (oldValue.MetadataSize == 8 && input.header.CheckExpiry(oldValue.ExtraMetadata))
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

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
                                .NeedCopyUpdate(key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(functionsState.etagState.etagSkippedStart), ref writer);
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
        public bool CopyUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            // Expired data
            if (oldValue.MetadataSize == 8 && input.header.CheckExpiry(oldValue.ExtraMetadata))
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
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                    // By now the comparison for etag against existing etag has already been done in NeedCopyUpdate
                    shouldUpdateEtag = true;
                    // Copy input to value
                    ReadOnlySpan<byte> src = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                    // retain metadata unless metadata sent
                    int metadataSize = input.arg1 != 0 ? sizeof(long) : oldValue.MetadataSize;

                    Debug.Assert(src.Length + EtagConstants.EtagSize + metadataSize == newValue.Length);

                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    if (input.arg1 != 0)
                    {
                        newValue.ExtraMetadata = input.arg1;
                    }

                    Span<byte> dest = newValue.AsSpan(EtagConstants.EtagSize);
                    src.CopyTo(dest);

                    long etagFromClient = input.parseState.GetLong(1);

                    functionsState.etagState.etag = etagFromClient;

                    long etagForResponse = cmd == RespCommand.SETIFMATCH ? functionsState.etagState.etag + 1 : functionsState.etagState.etag;

                    recordInfo.SetHasETag();

                    // Write Etag and Val back to Client
                    // write back array of the format [etag, nil]
                    var nilResp = functionsState.nilResp;
                    // *2\r\n: + <numDigitsInEtag> + \r\n + <nilResp.Length>
                    var numDigitsInEtag = NumUtils.CountDigits(etagForResponse);
                    WriteValAndEtagToDst(4 + 1 + numDigitsInEtag + 2 + nilResp.Length, ref nilResp, etagForResponse, ref output, functionsState.memoryPool, writeDirect: true);
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
                            nextUpdateEtagOffset = EtagConstants.EtagSize;
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
                            nextUpdateEtagOffset = EtagConstants.EtagSize;
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
                    shouldUpdateEtag = false;

                    var expiryExists = oldValue.MetadataSize == 8;

                    var expirationWithOption = new ExpirationWithOption(input.arg1);

                    EvaluateExpireCopyUpdate(expirationWithOption.ExpireOption, expiryExists, expirationWithOption.ExpirationTimeInTicks, ref oldValue, ref newValue, ref output);
                    break;

                case RespCommand.PERSIST:
                    shouldUpdateEtag = false;
                    oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                    if (oldValue.MetadataSize == 8)
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
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: incrByFloat, functionsState.etagState.etagSkippedStart);
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
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
                    var oldValuePtr = oldValue.ToPointer() + functionsState.etagState.etagSkippedStart;
                    var newValuePtr = newValue.ToPointer() + functionsState.etagState.etagSkippedStart;
                    var oldValueLength = oldValue.Length - functionsState.etagState.etagSkippedStart;
                    var newValueLength = newValue.Length - functionsState.etagState.etagSkippedStart;
                    Buffer.MemoryCopy(oldValuePtr, newValuePtr, newValueLength, oldValueLength);
                    if (newValueLength > oldValueLength)
                    {
                        // Zero-init the rest of the new value before we do any bit operations (e.g. it may have been revivified, which for efficiency does not clear old data)
                        new Span<byte>(newValuePtr + oldValueLength, newValueLength - oldValueLength).Clear();
                    }
                    var (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, newValuePtr, newValueLength);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(functionsState.nilResp, ref output);
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
                    var oldValueDataSpan = oldValue.AsSpan(functionsState.etagState.etagSkippedStart);
                    var newValueDataSpan = newValue.AsSpan(functionsState.etagState.etagSkippedStart);
                    if (oldValueDataSpan.Length < offset)
                    {
                        // If the offset is greater than the old value, we need to zero-fill the gap (e.g. new record might have been revivified).
                        var zeroFillLength = offset - oldValueDataSpan.Length;
                        newValueDataSpan.Slice(oldValueDataSpan.Length, zeroFillLength).Clear();
                    }
                    newInputValue.CopyTo(newValueDataSpan.Slice(offset));

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
                        EvaluateExpireCopyUpdate(ExpireOption.None, expiryExists: oldValue.MetadataSize == 8, newExpiry, ref oldValue, ref newValue, ref _output);
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

                case RespCommand.VADD:
                    // Handle "make me delete-able"
                    if (input.arg1 == VectorManager.DeleteAfterDropArg)
                    {
                        newValue.AsSpan().Clear();
                    }
                    else if (input.arg1 == VectorManager.RecreateIndexArg)
                    {
                        var newIndexPtr = MemoryMarshal.Read<nint>(input.parseState.GetArgSliceByRef(10).Span);

                        oldValue.CopyTo(ref newValue);

                        functionsState.vectorManager.RecreateIndex(newIndexPtr, ref newValue);
                    }

                    break;

                case RespCommand.VREM:
                    Debug.Assert(input.arg1 == VectorManager.VREMAppendLogArg, "Unexpected CopyUpdater call on VREM key");
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
                        var expirationInTicks = input.arg1;
                        if (expirationInTicks == 0)
                        {
                            // We want to retain the old metadata
                            newValue.ExtraMetadata = oldValue.ExtraMetadata;
                        }
                        else if (expirationInTicks > 0)
                        {
                            // We want to add the given expiration
                            newValue.ExtraMetadata = expirationInTicks;
                        }

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                        try
                        {
                            var ret = functions
                                .CopyUpdater(key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(functionsState.etagState.etagSkippedStart), newValue.AsSpan(functionsState.etagState.etagSkippedStart), ref writer, ref rmwInfo);
                            return ret;
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            rmwInfo.SetUsedValueLength(ref recordInfo, ref newValue, newValue.TotalSize);

            if (shouldUpdateEtag)
            {
                if (cmd is not RespCommand.SETIFGREATER)
                    functionsState.etagState.etag++;

                newValue.SetEtagInPayload(functionsState.etagState.etag);
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