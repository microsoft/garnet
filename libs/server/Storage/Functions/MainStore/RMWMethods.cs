// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        /// <inheritdoc />
        public readonly bool NeedInitialUpdate<TKey>(TKey key, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETKEEPTTLXX:
                case RespCommand.GETDEL:
                case RespCommand.GETEX:
                case RespCommand.DELIFGREATER:
                    return false;
                case RespCommand.SETEXXX:
                    return false;
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                case RespCommand.SETWITHETAG:
                // add etag on first insertion, already tracked by header.CheckWithEtagFlag()
                case RespCommand.SET:
                case RespCommand.SETEXNX:
                case RespCommand.SETKEEPTTL:
                    return true;
                case RespCommand.RICREATE:
                    return true;
                case RespCommand.RIPROMOTE:
                    return false; // Key must already exist; don't create new
                case RespCommand.RIRESTORE:
                    return false; // Key must already exist
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                        try
                        {
                            // For custom functions, deliberately hiding the complexity of key types
                            var ret = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd)
                                .NeedInitialUpdate(key.KeyBytes, ref input, ref writer);
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
        public readonly bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
        {
            Debug.Assert(!logRecord.Info.HasETag && !logRecord.Info.HasExpiration, "Should not have Expiration or ETag on InitialUpdater log records");

            // Because this is InitialUpdater, the destination length should be set correctly, but test and log failures to be safe.
            var cmd = input.header.cmd;
            switch (cmd)
            {
                case RespCommand.PFADD:
                    RecordSizeInfo.AssertValueDataLength(HyperLogLog.DefaultHLL.SparseInitialLength(ref input), in sizeInfo);
                    if (!logRecord.TrySetContentLengths(in sizeInfo))
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

                    *output.SpanByteAndMemory.SpanByte.ToPointer() = 1;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy + 4 (skip len size)
                    var sbSrcHLL = input.parseState.GetArgSliceByRef(0);

                    if (!logRecord.TrySetContentLengths(sbSrcHLL.Length, in sizeInfo))
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
                    return HandleSetIfMatchInitialUpdate(cmd, ref logRecord, in sizeInfo, ref input, ref output);
                case RespCommand.SETWITHETAG:
                    return HandleSetWithEtagInitialUpdate(ref logRecord, in sizeInfo, ref input, ref output);
                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!logRecord.TrySetValueSpanAndPrepareOptionals(newInputValue, in sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }

                    // Set or remove expiration
                    if (sizeInfo.FieldInfo.HasExpiration && !logRecord.TrySetExpiration(input.arg1))
                    {
                        functionsState.logger?.LogError("Could not set expiration in {methodName}.{caseName}", "InitialUpdater", "SETEXNX");
                        return false;
                    }

                    break;
                case RespCommand.SETKEEPTTL:
                    // Copy input to value; do not change expiration
                    _ = logRecord.TrySetValueSpanAndPrepareOptionals(input.parseState.GetArgSliceByRef(0).ReadOnlySpan, in sizeInfo);
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETEXXX:
                case RespCommand.GETDEL:
                case RespCommand.GETEX:
                    throw new Exception();

                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!logRecord.TrySetContentLengths(BitmapManager.Length(bOffset), in sizeInfo, zeroInit: true))
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

                    functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
                    break;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);

                    if (!logRecord.TrySetContentLengths(BitmapManager.LengthFromType(bitFieldArgs), in sizeInfo, zeroInit: true))
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
                        functionsState.CopyRespNumber(bitfieldReturnValue, ref output.SpanByteAndMemory);
                    else
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output.SpanByteAndMemory);
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    // If the offset is greater than 0, we need to zero-fill the gap (e.g. new record might have been revivified).
                    value = logRecord.ValueSpan;
                    if (offset > 0)
                        value.Slice(0, offset).Clear();
                    newValue.CopyTo(value.Slice(offset));

                    if (!TryCopyValueLengthToOutput(value, ref output))
                        return false;
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    // Copy value to be appended to the newly allocated value buffer
                    value = logRecord.ValueSpan;
                    appendValue.ReadOnlySpan.CopyTo(value);

                    if (!TryCopyValueLengthToOutput(value, ref output))
                        return false;
                    break;
                case RespCommand.INCR:
                    // This is InitialUpdater so set the value to 1 and the length to the # of digits in "1"
                    if (!logRecord.TrySetContentLengths(1, in sizeInfo))
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
                    if (!logRecord.TrySetContentLengths(ndigits + (isNegative ? 1 : 0), in sizeInfo))
                    {
                        functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "INCRBY");
                        return false;
                    }

                    _ = TryCopyUpdateNumber(incrBy, logRecord.ValueSpan, ref output);
                    break;
                case RespCommand.DECR:
                    // This is InitialUpdater so set the value to -1 and the length to the # of digits in "-1"
                    if (!logRecord.TrySetContentLengths(2, in sizeInfo))
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
                    if (!logRecord.TrySetContentLengths(ndigits + (isNegative ? 1 : 0), in sizeInfo))
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
                case RespCommand.RICREATE:
                    {
                        // The stub bytes (including TreeHandle) are passed as parseState arg 0.
                        // On AOF replay, HandleRangeIndexCreateReplay intercepts this and replaces
                        // the stale TreeHandle with a fresh one before the RMW reaches here.
                        var stubSpan = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                        if (!logRecord.TrySetContentLengths(RangeIndexManager.IndexSizeBytes, in sizeInfo))
                        {
                            functionsState.logger?.LogError("Length overflow in {methodName}.{caseName}", "InitialUpdater", "RICREATE");
                            return false;
                        }

                        stubSpan.CopyTo(logRecord.ValueSpan);

                        var dataHeader = logRecord.RecordDataHeader;
                        dataHeader.RecordType = RangeIndexManager.RangeIndexRecordType;
                    }
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

                        functionsState.vectorManager.CreateIndex(dims, reduceDims, quantizer, buildExplorationFactor, numLinks, distanceMetric, context, index, logRecord.ValueSpan);
                    }
                    break;
                case RespCommand.VREM:
                    Debug.Assert(input.arg1 == VectorManager.VREMAppendLogArg, "Should only see VREM writes as part of replication");
                    break;
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd);
                        if (!logRecord.TrySetContentLengths(functions.GetInitialLength(ref input), in sizeInfo, zeroInit: true))   // ZeroInit to be safe
                        {
                            functionsState.logger?.LogError("Length overflow in 'default' > StartOffset: {methodName}.{caseName}", "InitialUpdater", "default");
                            return false;
                        }
                        if (input.arg1 > 0 && !logRecord.TrySetExpiration(input.arg1))
                        {
                            functionsState.logger?.LogError("Could not set expiration in 'default' > StartOffset: {methodName}.{caseName}", "InitialUpdater", "default");
                            return false;
                        }

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
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
                    if (!logRecord.TrySetValueSpanAndPrepareOptionals(input.parseState.GetArgSliceByRef(0).ReadOnlySpan, in sizeInfo))
                    {
                        functionsState.logger?.LogError("Failed to set value in {methodName}.{caseName}", "InitialUpdater", "default");
                        return false;
                    }

                    // Copy value to output
                    CopyTo(logRecord.ValueSpan, ref output, functionsState.memoryPool);
                    break;
            }

            // Success if we made it here
            sizeInfo.AssertOptionalsIfSet(logRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public readonly void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                input.header.SetExpiredFlag();
                rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            }
        }

        /// <inheritdoc />
        public readonly bool InPlaceUpdater(ref LogRecord logRecord, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
        {
            if (logRecord.Info.ValueIsObject)
            {
                rmwInfo.Action = RMWAction.WrongType;
                return false;
            }

            // RangeIndex type safety – normal string records have RecordType 0; skip all checks in that common case.
            if (logRecord.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                // Reject non-RI commands on RI keys
                if (!input.header.cmd.IsLegalOnRangeIndex())
                {
                    rmwInfo.Action = RMWAction.WrongType;
                    return false;
                }
            }
            else if (input.header.cmd.IsRangeIndexCommand())
            {
                // Reject RI-specific commands on non-RI keys
                rmwInfo.Action = RMWAction.WrongType;
                return false;
            }

            var ipuResult = InPlaceUpdaterWorker(ref logRecord, ref input, ref output, ref rmwInfo);
            switch (ipuResult)
            {
                case IPUResult.Failed:
                    return false;
                case IPUResult.Succeeded:
                    if (!logRecord.Info.Modified)
                        functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                    if (functionsState.appendOnlyFile != null)
                        rmwInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
                    return true;
                case IPUResult.NotUpdated:
                default:
                    return true;
            }
        }

        private readonly IPUResult InPlaceUpdaterWorker(ref LogRecord logRecord, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
        {
            var cmd = input.header.cmd;
            // Expired data
            if (logRecord.Info.HasExpiration && input.header.CheckExpiry(logRecord.Expiration))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                _ = logRecord.RemoveETag();
                return IPUResult.Failed;
            }

            var shouldCheckExpiration = true;

            RecordSizeInfo sizeInfo2 = new();

            switch (cmd)
            {
                case RespCommand.DELIFGREATER:
                case RespCommand.SETIFMATCH:
                case RespCommand.SETIFGREATER:
                case RespCommand.SETWITHETAG:
                    return HandleEtagInPlaceUpdateWorker(cmd, ref logRecord, ref input, ref output, ref rmwInfo);
                case RespCommand.SETEXNX:
                    // Note: SETEXNX may or may not actually have an expiration.
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.ValueSpan, ref output);
                    }

                    // Nothing is set because being in this block means NX was already violated
                    return IPUResult.NotUpdated;
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    // Note: SETEXXX may or may not actually have an expiration.
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.ValueSpan, ref output);
                    }

                    // If we are not adding an ETag or Expiration we don't need to grow the record size for any reason other than value growth,
                    // so we can optimize this to just set the length and span. Note: SETEXXX may or may not actually have an expiration.
                    // TODO: Convert this and similar to use LogRecord.CanGrowPinnedValue.
                    if (logRecord.Info.ValueIsInline && (input.arg1 == 0 || logRecord.Info.HasExpiration))
                    {
                        var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                        if (!logRecord.TrySetPinnedValueSpan(input.parseState.GetArgSliceByRef(0), valueAddress, ref valueLength))
                            return IPUResult.Failed;
                    }
                    else
                    {
                        // Create local sizeInfo
                        sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(in logRecord, ref input) };
                        functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);
                        if (!logRecord.TrySetValueSpanAndPrepareOptionals(input.parseState.GetArgSliceByRef(0), in sizeInfo2))
                            return IPUResult.Failed;
                    }

                    // Update expiration
                    if (input.arg1 != 0)
                    {
                        if (!logRecord.TrySetExpiration(input.arg1))
                            Debug.Fail("Should have succeeded in setting Expiration as we should have ensured there was space there already");
                    }
                    else if (logRecord.Info.HasExpiration)
                        _ = logRecord.RemoveExpiration();

                    break;
                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    // If the SetGet flag is set, copy the current value to output for the GET part of the command.
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(logRecord.ValueSpan, ref output);
                    }

                    var setValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;

                    if (logRecord.Info.ValueIsInline)
                    {
                        // We won't change ETag or Expiration. Precheck adequate length before making any changes.
                        if (!logRecord.CanGrowPinnedValue(setValue.Length, newETagLen: logRecord.ETagLen, newExpirationLen: logRecord.ExpirationLen, out var valueAddress, out var valueLength))
                            return IPUResult.Failed;
                        if (!logRecord.TrySetPinnedValueLength(setValue.Length, valueAddress, ref valueLength))
                        {
                            Debug.Fail("Should have succeeded in growing the value as we have ensured there was space there already");
                            return IPUResult.Failed;
                        }
                    }
                    else
                    {
                        // Create local sizeInfo
                        sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(logRecord, ref input) };
                        functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);
                        if (!logRecord.TrySetValueSpanAndPrepareOptionals(setValue, in sizeInfo2))
                            return IPUResult.Failed;
                    }

                    setValue.CopyTo(logRecord.ValueSpan);
                    break;

                case RespCommand.INCR:
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: 1))
                        return IPUResult.Failed;
                    break;
                case RespCommand.DECR:
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: -1))
                        return IPUResult.Failed;
                    break;
                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    var incrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: incrBy))
                        return IPUResult.Failed;
                    break;
                case RespCommand.DECRBY:
                    var decrBy = input.arg1;
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, input: -decrBy))
                        return IPUResult.Failed;
                    break;
                case RespCommand.INCRBYFLOAT:
                    var incrByFloat = BitConverter.Int64BitsToDouble(input.arg1);
                    if (!TryInPlaceUpdateNumber(ref logRecord, ref output, ref rmwInfo, incrByFloat))
                        return IPUResult.Failed;
                    break;

                case RespCommand.SETBIT:
                    var bOffset = input.arg1;
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(1).ReadOnlySpan[0] - '0');

                    if (!BitmapManager.IsLargeEnough(logRecord.ValueSpan.Length, bOffset))
                    {
                        var newLength = BitmapManager.Length(bOffset);
                        if (logRecord.Info.ValueIsInline)
                        {
                            // We are removing expiration and not changing ETag presence. Precheck adequate length before making any changes.
                            if (!logRecord.CanGrowPinnedValue(newLength, newETagLen: logRecord.ETagLen, newExpirationLen: 0, out var valueAddress, out var valueLength))
                                return IPUResult.Failed;
                            // Remove Expiration first to free up the space for value growth.
                            _ = logRecord.RemoveExpiration();
                            if (!logRecord.TrySetPinnedValueLength(newLength, valueAddress, ref valueLength, zeroInit: true))
                            {
                                Debug.Fail("Should have succeeded in growing the value as we have ensured there was space there already");
                                return IPUResult.Failed;
                            }
                        }
                        else
                        {
                            // Create local sizeInfo
                            sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(logRecord, ref input) };
                            functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);
                            if (!logRecord.TrySetContentLengths(newLength, in sizeInfo2, zeroInit: true))
                                return IPUResult.Failed;
                            _ = logRecord.RemoveExpiration();
                        }
                    }

                    byte oldValSet;
                    if (logRecord.IsPinnedValue)
                        oldValSet = BitmapManager.UpdateBitmap(logRecord.PinnedValuePointer, bOffset, bSetVal);
                    else
                        fixed (byte* valuePtr = logRecord.ValueSpan)
                            oldValSet = BitmapManager.UpdateBitmap(valuePtr, bOffset, bSetVal);

                    functionsState.CopyDefaultResp(
                        oldValSet == 0 ? CmdStrings.RESP_RETURN_VAL_0 : CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                    break;
                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    if (!BitmapManager.IsLargeEnoughForType(bitFieldArgs, logRecord.ValueSpan.Length))
                    {
                        var newLength = BitmapManager.LengthFromType(bitFieldArgs);
                        if (logRecord.Info.ValueIsInline)
                        {
                            // We are removing expiration and not changing ETag presence. Precheck adequate length before making any changes.
                            if (!logRecord.CanGrowPinnedValue(newLength, newETagLen: logRecord.ETagLen, newExpirationLen: 0, out var valueAddress, out var valueLength))
                                return IPUResult.Failed;
                            // Remove Expiration first to free up the space for value growth.
                            _ = logRecord.RemoveExpiration();
                            if (!logRecord.TrySetPinnedValueLength(newLength, valueAddress, ref valueLength, zeroInit: true))
                            {
                                Debug.Fail("Should have succeeded in growing the value as we have ensured there was space there already");
                                return IPUResult.Failed;
                            }
                        }
                        else
                        {
                            // Create local sizeInfo
                            sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(logRecord, ref input) };
                            functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);
                            if (!logRecord.TrySetContentLengths(newLength, in sizeInfo2, zeroInit: true))
                                return IPUResult.Failed;
                            _ = logRecord.RemoveExpiration();
                        }
                    }

                    long bitfieldReturnValue;
                    bool overflow;
                    if (logRecord.IsPinnedValue)
                        (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, logRecord.PinnedValuePointer, logRecord.ValueSpan.Length);
                    else
                        fixed (byte* valuePtr = logRecord.ValueSpan)
                            (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, valuePtr, logRecord.ValueSpan.Length);

                    if (overflow)
                    {
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output.SpanByteAndMemory);
                        return IPUResult.Succeeded;
                    }

                    functionsState.CopyRespNumber(bitfieldReturnValue, ref output.SpanByteAndMemory);
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
                        *output.SpanByteAndMemory.SpanByte.ToPointer() = (byte)0xFF;  // Flags invalid HLL
                        return IPUResult.NotUpdated;
                    }

                    if (result)
                        *output.SpanByteAndMemory.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    if (!result)
                        return IPUResult.Failed;
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
                        *output.SpanByteAndMemory.SpanByte.ToPointer() = (byte)0xFF;  // Flags invalid HLL
                        return IPUResult.NotUpdated;
                    }
                    if (!result)
                        return IPUResult.Failed;
                    break;

                case RespCommand.SETRANGE:
                    var offset = input.parseState.GetInt(0);
                    var newValue = input.parseState.GetArgSliceByRef(1).ReadOnlySpan;

                    var totalLength = newValue.Length + offset;
                    if (totalLength > logRecord.ValueSpan.Length)
                    {
                        // Try to grow in place. We are not changing the presence of ETag or Expiration
                        if (logRecord.Info.ValueIsInline)
                        {
                            var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                            if (!logRecord.TrySetPinnedValueLength(totalLength, valueAddress, ref valueLength))
                                return IPUResult.Failed;
                        }
                        else
                        {
                            // Create local sizeInfo
                            sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(in logRecord, ref input) };
                            functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);
                            if (!logRecord.TrySetContentLengths(totalLength, in sizeInfo2))
                                return IPUResult.Failed;
                        }
                    }

                    newValue.CopyTo(logRecord.ValueSpan.Slice(offset));
                    if (!TryCopyValueLengthToOutput(logRecord.ValueSpan, ref output))
                        return IPUResult.Failed;
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(logRecord.ValueSpan, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return IPUResult.Failed;

                case RespCommand.GETEX:
                    CopyRespTo(logRecord.ValueSpan, ref output);

                    var ipuResult = IPUResult.NotUpdated;

                    // If both EX and PERSIST were specified, EX wins
                    if (input.arg1 > 0)
                    {
                        var _output = StringOutput.FromPinnedSpan(stackalloc byte[sizeof(int)]);

                        var newExpiry = input.arg1;
                        ipuResult = EvaluateExpireInPlace(ref logRecord, ExpireOption.None, newExpiry, ref _output);
                        if (ipuResult == IPUResult.Failed)
                            return IPUResult.Failed;
                    }
                    else if (input.parseState.Count > 0)
                    {
                        // PERSIST means to remove the Expiration; if there is no expiration, the following is a no-op.
                        if (input.parseState.GetArgSliceByRef(0).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST) && logRecord.Info.HasExpiration)
                        {
                            _ = logRecord.RemoveExpiration();
                            ipuResult = IPUResult.Succeeded;
                        }
                    }

                    // reset etag state that may have been initialized earlier, but don't update etag
                    return ipuResult;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendValue = input.parseState.GetArgSliceByRef(0);
                    var appendLength = appendValue.Length;
                    if (appendLength > 0)
                    {
                        // Try to grow in place.
                        var originalLength = logRecord.ValueSpan.Length;
                        totalLength = originalLength + appendLength;

                        // Try to grow in place. We are not changing the presence of ETag or Expiration
                        if (logRecord.Info.ValueIsInline)
                        {
                            var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                            if (!logRecord.TrySetPinnedValueLength(totalLength, valueAddress, ref valueLength))
                                return IPUResult.Failed;
                        }
                        else
                        {
                            // Create local sizeInfo
                            sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(in logRecord, ref input) };
                            functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);
                            if (!logRecord.TrySetContentLengths(totalLength, in sizeInfo2))
                                return IPUResult.Failed;
                        }

                        // Append the new value with the client input at the end of the old data
                        appendValue.ReadOnlySpan.CopyTo(logRecord.ValueSpan.Slice(originalLength));
                        if (!TryCopyValueLengthToOutput(logRecord.ValueSpan, ref output))
                            return IPUResult.Failed;
                        break;
                    }

                    return TryCopyValueLengthToOutput(logRecord.ValueSpan, ref output) ? IPUResult.Succeeded : IPUResult.Failed;
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
                        logRecord.ValueSpan.Clear();
                    }
                    else if (input.arg1 == VectorManager.RecreateIndexArg)
                    {
                        var newIndexPtr = MemoryMarshal.Read<nint>(input.parseState.GetArgSliceByRef(11).Span);

                        functionsState.vectorManager.RecreateIndex(newIndexPtr, logRecord.ValueSpan);
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
                                return IPUResult.Failed;
                        }
                        shouldCheckExpiration = false;

                        var value = logRecord.ValueSpan;
                        var newLength = value.Length;
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                        try
                        {
                            var ret = functions.InPlaceUpdater(logRecord.Key, ref input, value, ref newLength, ref writer, ref rmwInfo);

                            // Adjust value length if user shrank it. Because we are shrinking this will always succeed.
                            // This assumes newLength has not been changed if !ret.
                            if (newLength < logRecord.ValueSpan.Length)
                            {
                                if (logRecord.Info.ValueIsInline)
                                {
                                    var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                                    _ = logRecord.TrySetPinnedValueLength(newLength, valueAddress, ref valueLength);
                                }
                                else
                                {
                                    // Create local sizeInfo
                                    sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetRMWModifiedFieldInfo(in logRecord, ref input) };
                                    functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);
                                    _ = logRecord.TrySetContentLengths(newLength, in sizeInfo2);
                                }
                            }
                            return ret ? IPUResult.Succeeded : IPUResult.Failed;
                        }
                        finally
                        {
                            writer.Dispose();
                        }
                    }
                    throw new GarnetException("Unsupported operation on input");
                case RespCommand.RICREATE:
                    // Index already exists at this key — reject with error
                    return IPUResult.NotUpdated;
                case RespCommand.RIPROMOTE:
                    // Record is in mutable region — no-op. Not logged to AOF (internal maintenance).
                    Debug.Assert(!RangeIndexManager.ReadIndex(logRecord.ValueSpan).IsFlushed,
                        "Mutable record should never have Flushed flag set");
                    return IPUResult.NotUpdated;
                case RespCommand.RIRESTORE:
                    // Set the TreeHandle from the restored BfTree pointer. Not logged to AOF (transient pointer).
                    RangeIndexManager.RecreateIndex((nint)input.arg1, logRecord.ValueSpan);
                    return IPUResult.NotUpdated;
            }

            sizeInfo2.AssertOptionalsIfSet(logRecord.Info, checkExpiration: shouldCheckExpiration);
            return IPUResult.Succeeded;
        }

        /// <inheritdoc />
        public readonly bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            switch (input.header.cmd)
            {
                case RespCommand.DELIFGREATER:
                case RespCommand.SETIFGREATER:
                case RespCommand.SETIFMATCH:
                case RespCommand.SETWITHETAG:
                    return HandleEtagNeedCopyUpdate(input.header.cmd, in srcLogRecord, ref input, ref output, ref rmwInfo);
                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndResume ensures that we set as new value, since it does not exist
                    if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndResume;
                        return false;
                    }

                    // since this case is only hit when this an update, the NX is violated and so we can return early from it without setting the value

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
                case RespCommand.RICREATE:
                    // Index already exists — never copy-update, reject in caller
                    return false;
                case RespCommand.RIPROMOTE:
                    // Always copy to tail to promote from read-only region
                    return true;
                case RespCommand.RIRESTORE:
                    // Copy to tail if needed, then IPU will set TreeHandle
                    return true;
                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
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

        /// <inheritdoc />
        public readonly bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // Expired data
            if (srcLogRecord.Info.HasExpiration && input.header.CheckExpiry(srcLogRecord.Expiration))
            {
                _ = dstLogRecord.RemoveETag();
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            var oldValue = srcLogRecord.ValueSpan;  // reduce redundant length calcs
            // Do not pre-get newValue = dstLogRecord.ValueSpan here, because it may change, e.g. moving between inline and overflow

            RespCommand cmd = input.header.cmd;

            switch (cmd)
            {
                case RespCommand.SETIFMATCH:
                case RespCommand.SETIFGREATER:
                case RespCommand.SETWITHETAG:
                    return HandleEtagCopyUpdateWorker(cmd, in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output);
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(srcLogRecord.ValueSpan, ref output);
                    }

                    var newInputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    Debug.Assert(newInputValue.Length == dstLogRecord.ValueSpan.Length);

                    // Copy input to value, along with optionals from source record including Expiration.
                    if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(newInputValue, in sizeInfo) || !dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                        return false;

                    // Update expiration if it was supplied.
                    if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                        return false;

                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    // If the SetGet flag is set, copy the current value to output for the GET part of the command.
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(srcLogRecord.ValueSpan, ref output);
                    }

                    var inputValue = input.parseState.GetArgSliceByRef(0).ReadOnlySpan;
                    if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(inputValue, in sizeInfo))
                        return false;

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

                    functionsState.CopyDefaultResp(
                        oldValSet == 0 ? CmdStrings.RESP_RETURN_VAL_0 : CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
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
                        functionsState.CopyRespNumber(bitfieldReturnValue, ref output.SpanByteAndMemory);
                    else
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output.SpanByteAndMemory);
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

                    *output.SpanByteAndMemory.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    break;

                case RespCommand.PFMERGE:
                    if (!dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                        return false;

                    // Explanation of variables:
                    //srcA offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)
                    var srcHLLPtr = input.parseState.GetArgSliceByRef(0).ToPointer(); // HLL merging from
                    // byte* oldDstHLLPtr = oldValue.ToPointer(); // original HLL merging to (too small to hold its data plus srcA)
                    // byte* newDstHLLPtr = newValue.ToPointer(); // new HLL merging to (large enough to hold srcA and srcB

                    // Zeroinit any extra space in the new value (e.g. revivified record does not clear it out, for efficiency).
                    newValue = dstLogRecord.ValueSpan;
                    if (oldValue.Length < newValue.Length)
                        newValue.Slice(oldValue.Length).Clear();

                    // Some duplicate code to avoid "fixed" when possible
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

                    _ = TryCopyValueLengthToOutput(newValue, ref output);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(oldValue, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.GETEX:
                    CopyRespTo(oldValue, ref output);

                    if (!dstLogRecord.TryCopyFrom(in srcLogRecord, in sizeInfo))
                        return false;

                    newValue = dstLogRecord.ValueSpan;
                    Debug.Assert(newValue.Length == oldValue.Length);
                    if (input.arg1 > 0)
                    {
                        var _output = StringOutput.FromPinnedSpan(stackalloc byte[sizeof(int)]);
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

                    _ = TryCopyValueLengthToOutput(newValue, ref output);
                    break;

                case RespCommand.VADD:
                    // Handle "make me delete-able"
                    if (input.arg1 == VectorManager.DeleteAfterDropArg)
                    {
                        dstLogRecord.ValueSpan.Clear();
                    }
                    else if (input.arg1 == VectorManager.RecreateIndexArg)
                    {
                        var newIndexPtr = MemoryMarshal.Read<nint>(input.parseState.GetArgSliceByRef(11).Span);

                        oldValue.CopyTo(dstLogRecord.ValueSpan);

                        functionsState.vectorManager.RecreateIndex(newIndexPtr, dstLogRecord.ValueSpan);
                    }

                    break;

                case RespCommand.VREM:
                    Debug.Assert(input.arg1 == VectorManager.VREMAppendLogArg, "Unexpected CopyUpdater call on VREM key");
                    break;

                default:
                    if (input.header.cmd > RespCommandExtensions.LastValidCommand)
                    {
                        var functions = functionsState.GetCustomCommandFunctions((ushort)input.header.cmd);
                        var expirationInTicks = input.arg1;
                        if (expirationInTicks > 0)
                        {
                            // We want to update to the given expiration
                            if (!dstLogRecord.TrySetExpiration(expirationInTicks))
                                return false;
                        }

                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
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
                case RespCommand.RICREATE:
                    // NeedCopyUpdate returns false for RICREATE, so CopyUpdater should never be reached.
                    throw new GarnetException("CopyUpdater should not be called for RICREATE");
                case RespCommand.RIPROMOTE:
                    {
                        // Copy stub bytes from source to destination, clearing the flushed flag.
                        var srcValue = srcLogRecord.ValueSpan;
                        if (!dstLogRecord.TrySetContentLengths(RangeIndexManager.IndexSizeBytes, in sizeInfo))
                            return false;
                        srcValue.CopyTo(dstLogRecord.ValueSpan);
                        RangeIndexManager.ClearFlushedFlag(dstLogRecord.ValueSpan);

                        // Preserve the RecordType
                        var dataHeader = dstLogRecord.RecordDataHeader;
                        dataHeader.RecordType = RangeIndexManager.RangeIndexRecordType;

                        // NOTE: Source TreeHandle is cleared in PostCopyUpdater (after CAS success)
                        // to avoid orphaning the tree if the CAS fails.
                    }
                    break;
                case RespCommand.RIRESTORE:
                    {
                        // Copy stub bytes and set TreeHandle from restored BfTree.
                        var srcValue = srcLogRecord.ValueSpan;
                        if (!dstLogRecord.TrySetContentLengths(RangeIndexManager.IndexSizeBytes, in sizeInfo))
                            return false;
                        srcValue.CopyTo(dstLogRecord.ValueSpan);
                        RangeIndexManager.RecreateIndex((nint)input.arg1, dstLogRecord.ValueSpan);

                        var dataHeader = dstLogRecord.RecordDataHeader;
                        dataHeader.RecordType = RangeIndexManager.RangeIndexRecordType;
                    }
                    break;
            }

            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public readonly bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref RMWInfo rmwInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);

            var cmd = input.header.cmd;

            // RIPROMOTE/RIRESTORE are internal store-maintenance ops — skip AOF.
            if (cmd != RespCommand.RIPROMOTE && cmd != RespCommand.RIRESTORE)
            {
                if (functionsState.appendOnlyFile != null)
                    rmwInfo.UserData |= NeedAofLog;
            }

            // RIPROMOTE: pass ownership of the BfTree from src to dst.
            //  • If src.TreeHandle != 0 (live transfer): dst inherited the handle via byte-copy in
            //    CopyUpdater; clear src.TreeHandle so a later eviction of src does not free the tree.
            //  • If src.TreeHandle == 0 (cold case — src was post-eviction or post-recovery): pre-stage
            //    data.bftree from <srcAddr:x16>.flush.bftree and register a pending entry so the next
            //    checkpoint captures dst's content. Cleanly handles steady-state cold-restore, recovery
            //    Scenario D (below-FUA-at-checkpoint stub recovered), and any other path that promotes
            //    a flushed stub with TreeHandle == 0.
            //  • In BOTH branches: set src.IsTransferred so a later OnEvict on the stale source does
            //    not remove the liveIndexes entry (live: owned by dst's tree; cold: owned by the new
            //    pending entry), and a later OnFlush on the stale source does not snapshot a stale view.
            if (cmd == RespCommand.RIPROMOTE)
            {
                var srcSpan = srcLogRecord.ValueSpan;
                var srcHandle = RangeIndexManager.ReadIndex(srcSpan).TreeHandle;
                if (srcHandle != nint.Zero)
                {
                    RangeIndexManager.ClearTreeHandle(srcSpan);
                }
                else
                {
                    // rmwInfo.SourceAddress is the source logical address (preserved through
                    // CopyUpdater; rmwInfo.Address has been reassigned to the destination).
                    if (functionsState.storeWrapper?.rangeIndexManager is { } rim
                        && rmwInfo.SourceAddress != Tsavorite.core.LogAddress.kInvalidAddress)
                    {
                        rim.PreStageAndRegisterPending(dstLogRecord.Key, rmwInfo.SourceAddress);
                    }
                }

                RangeIndexManager.SetTransferredFlag(srcSpan);
            }

            return true;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostRMWOperation<TKey, TEpochAccessor>(TKey key, ref StringInput input, ref RMWInfo rmwInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((rmwInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogRMW(key.KeyBytes, ref input, rmwInfo.Version, rmwInfo.SessionID, epochAccessor);
        }
    }
}