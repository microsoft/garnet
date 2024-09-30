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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var cmd = input.AsSpan()[0];
            switch ((RespCommand)cmd)
            {
                case RespCommand.SETIFMATCH:
                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETEXXX:
                case RespCommand.PERSIST:
                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                case RespCommand.GETDEL:
                    return false;
                default:
                    if (cmd >= CustomCommandManager.StartOffset)
                    {
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState.customCommands[cmd - CustomCommandManager.StartOffset].functions.NeedInitialUpdate(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    return true;
            }
        }

        /// <inheritdoc />
        public bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var inputPtr = input.ToPointer();
            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);

            switch ((RespCommand)(*inputPtr))
            {
                case RespCommand.PFADD:
                    byte* i = inputPtr + RespInputHeader.Size;
                    byte* v = value.ToPointer();

                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(HyperLogLog.DefaultHLL.SparseInitialLength(i));
                    HyperLogLog.DefaultHLL.Init(i, v, value.Length);
                    *output.SpanByte.ToPointer() = (byte)1;
                    break;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy + 4 (skip len size)                    
                    i = input.ToPointer() + RespInputHeader.Size;
                    byte* srcHLL = sizeof(int) + i;
                    byte* dstHLL = value.ToPointer();

                    int length = *(int*)i;
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(length);
                    Buffer.MemoryCopy(srcHLL, dstHLL, value.Length, value.Length);
                    break;

                case RespCommand.SET:
                case RespCommand.SETEXNX:
                    // Copy input to value
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(input.Length - RespInputHeader.Size);
                    value.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(value.AsSpan());
                    break;

                case RespCommand.SETKEEPTTL:
                    // Copy input to value, retain metadata in value
                    value.ShrinkSerializedLength(value.MetadataSize + input.Length - RespInputHeader.Size);
                    input.AsReadOnlySpan().Slice(RespInputHeader.Size).CopyTo(value.AsSpan());
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETEXXX:
                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                case RespCommand.PERSIST:
                case RespCommand.GETDEL:
                    throw new Exception();

                case RespCommand.SETBIT:
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(BitmapManager.Length(input.ToPointer() + RespInputHeader.Size));

                    // Always return 0 at initial updater because previous value was 0
                    BitmapManager.UpdateBitmap(inputPtr + RespInputHeader.Size, value.ToPointer());
                    CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    break;

                case RespCommand.BITFIELD:
                    long bitfieldReturnValue;
                    bool overflow;

                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(BitmapManager.LengthFromType(input.ToPointer() + RespInputHeader.Size));
                    (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(inputPtr + RespInputHeader.Size, value.ToPointer(), value.Length);
                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.SETRANGE:
                    var offset = *(int*)(inputPtr + RespInputHeader.Size);
                    var newValueSize = *(int*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var newValuePtr = new Span<byte>((byte*)*(long*)(inputPtr + RespInputHeader.Size + sizeof(int) * 2), newValueSize);
                    newValuePtr.CopyTo(value.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output, 0);
                    break;

                case RespCommand.APPEND:
                    // Copy value to be appended to the newly allocated value buffer
                    var appendSize = *(int*)(inputPtr + RespInputHeader.Size);
                    var appendPtr = *(long*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var appendSpan = new Span<byte>((byte*)appendPtr, appendSize);
                    appendSpan.CopyTo(value.AsSpan());
                    CopyValueLengthToOutput(ref value, ref output, 0);
                    break;
                case RespCommand.INCRBY:
                    value.UnmarkExtraMetadata();
                    // Check if input contains a valid number
                    length = input.LengthWithoutMetadata - RespInputHeader.Size;
                    if (!IsValidNumber(length, inputPtr + RespInputHeader.Size, output.SpanByte.AsSpan(), out var incrBy))
                        return false;
                    // If incrby is being made for initial update then it was not made with etag so the offset is sent as 0
                    CopyUpdateNumber(incrBy, ref value, ref output, 0);
                    break;
                case RespCommand.DECRBY:
                    value.UnmarkExtraMetadata();
                    // Check if input contains a valid number
                    length = input.LengthWithoutMetadata - RespInputHeader.Size;
                    if (!IsValidNumber(length, inputPtr + RespInputHeader.Size, output.SpanByte.AsSpan(), out var decrBy))
                        return false;
                    // If incrby is being made for initial update then it was not made with etag so the offset is sent as 0
                    CopyUpdateNumber(-decrBy, ref value, ref output, 0);
                    break;
                case RespCommand.SETWITHETAG:
                    recordInfo.SetHasETag();

                    // Copy input to value
                    value.ShrinkSerializedLength(input.Length - RespInputHeader.Size + sizeof(long));
                    value.ExtraMetadata = input.ExtraMetadata;

                    // initial etag set to 0, this is a counter based etag that is incremented on change
                    *(long*)value.ToPointer() = 0;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(value.AsSpan(sizeof(long)));

                    // Copy initial etag to output
                    CopyRespNumber(0, ref output);
                    break;
                default:
                    value.UnmarkExtraMetadata();
                    recordInfo.ClearHasETag();
                    if (*inputPtr >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[*inputPtr - CustomCommandManager.StartOffset].functions;
                        // compute metadata size for result
                        long expiration = input.ExtraMetadata;
                        int metadataSize = expiration switch
                        {
                            -1 => 0,
                            0 => 0,
                            _ => 8,
                        };

                        value.ShrinkSerializedLength(metadataSize + functions.GetInitialLength(input.AsReadOnlySpan().Slice(RespInputHeader.Size)));
                        if (expiration > 0)
                            value.ExtraMetadata = expiration;

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        functions.InitialUpdater(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], value.AsSpan(), ref outp, ref rmwInfo);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        break;
                    }

                    // Copy input to value
                    value.ShrinkSerializedLength(input.Length - RespInputHeader.Size);
                    value.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(value.AsSpan());

                    // Copy value to output
                    CopyTo(ref value, ref output, functionsState.memoryPool);
                    break;
            }

            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
            return true;
        }

        /// <inheritdoc />
        public void PostInitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                ((RespInputHeader*)input.ToPointer())->SetExpiredFlag();
                WriteLogRMW(ref key, ref input, ref value, rmwInfo.Version, rmwInfo.SessionID);
            }
        }

        /// <inheritdoc />
        public bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (InPlaceUpdaterWorker(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
            {
                rmwInfo.UsedValueLength = value.TotalSize;
                if (!rmwInfo.RecordInfo.Modified)
                    functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogRMW(ref key, ref input, ref value, rmwInfo.Version, rmwInfo.SessionID);
                return true;
            }
            return false;
        }

        private bool InPlaceUpdaterWorker(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var inputPtr = input.ToPointer();

            // Expired data
            if (value.MetadataSize > 0 && ((RespInputHeader*)inputPtr)->CheckExpiry(value.ExtraMetadata))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            // First byte of input payload identifies command
            var cmd = (RespCommand)(*inputPtr);

            int etagIgnoredOffset = 0;
            int etagIgnoredEnd = -1;
            long oldEtag = -1;
            if (recordInfo.ETag)
            {
                etagIgnoredOffset = sizeof(long);
                etagIgnoredEnd = value.LengthWithoutMetadata;
                oldEtag = *(long*)value.ToPointer();
            }

            switch (cmd)
            {
                case RespCommand.SETEXNX:
                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    }
                    return true;

                case RespCommand.SETIFMATCH:
                    // Cancelling the operation and returning false is used to indicate no RMW because of ETAGMISMATCH
                    // In this case no etag will match the "nil" etag on a record without an etag
                    if (!recordInfo.ETag)
                    {
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }

                    long prevEtag = *(long*)value.ToPointer();
                    long etagFromClient = *(long*)(inputPtr + RespInputHeader.Size);
                    if (prevEtag != etagFromClient)
                    {
                        // Cancelling the operation and returning false is used to indicate no RMW because of ETAGMISMATCH
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }

                    // Need CU if no space for new value
                    if (input.Length - RespInputHeader.Size > value.Length) return false;

                    // Increment the ETag
                    *(long*)(input.ToPointer() + RespInputHeader.Size) += 1;

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(input.Length - RespInputHeader.Size);

                    // Copy input to value
                    value.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(value.AsSpan());
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    CopyRespToWithInput(ref input, ref value, ref output, false, 0, -1, true);
                    // early return since we already updated the ETag
                    return true;

                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    var nextUpdateEtagOffset = etagIgnoredOffset;
                    var nextUpdateEtagIgnoredEnd = etagIgnoredEnd;
                    if (!((RespInputHeader*)inputPtr)->CheckRetainEtagFlag())
                    {
                        // if the user did not explictly asked for retaining the etag we need to ignore the etag even if it existed on the previous record
                        nextUpdateEtagOffset = 0;
                        nextUpdateEtagIgnoredEnd = -1;
                        recordInfo.ClearHasETag();
                    }

                    // Need CU if no space for new value
                    if (input.Length - RespInputHeader.Size > value.Length - etagIgnoredOffset) return false;

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(input.Length - RespInputHeader.Size + nextUpdateEtagOffset);

                    // Copy input to value
                    value.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(value.AsSpan(nextUpdateEtagOffset));
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    // respect etag retention only if input header tells you to explicitly
                    if (!((RespInputHeader*)inputPtr)->CheckRetainEtagFlag())
                    {
                        // if the user did not explictly asked for retaining the etag we need to ignore the etag even if it existed on the previous record
                        etagIgnoredOffset = 0;
                        etagIgnoredEnd = -1;
                        recordInfo.ClearHasETag();
                    }

                    // Need CU if no space for new value
                    if (value.MetadataSize + input.Length - RespInputHeader.Size > value.Length - etagIgnoredOffset) return false;

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.MetadataSize + input.Length - RespInputHeader.Size + etagIgnoredOffset);

                    // Copy input to value
                    input.AsReadOnlySpan().Slice(RespInputHeader.Size).CopyTo(value.AsSpan(etagIgnoredOffset));
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                    return true;

                case RespCommand.PEXPIRE:
                case RespCommand.EXPIRE:
                    // doesn't update etag    
                    ExpireOption optionType = (ExpireOption)(*(inputPtr + RespInputHeader.Size));
                    bool expiryExists = (value.MetadataSize > 0);
                    return EvaluateExpireInPlace(optionType, expiryExists, ref input, ref value, ref output);

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
                    return true;

                case RespCommand.INCR:
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: 1, etagIgnoredOffset))
                        return false;
                    break;
                case RespCommand.DECR:
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -1, etagIgnoredOffset))
                        return false;
                    break;

                case RespCommand.INCRBY:
                    var length = input.LengthWithoutMetadata - RespInputHeader.Size;
                    // Check if input contains a valid number
                    if (!IsValidNumber(length, inputPtr + RespInputHeader.Size, output.SpanByte.AsSpan(), out var incrBy))
                        return true;
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: incrBy, etagIgnoredOffset))
                        return false;
                    break;

                case RespCommand.DECRBY:
                    length = input.LengthWithoutMetadata - RespInputHeader.Size;
                    // Check if input contains a valid number
                    if (!IsValidNumber(length, inputPtr + RespInputHeader.Size, output.SpanByte.AsSpan(), out var decrBy))
                        return true;
                    if (!TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -decrBy, etagIgnoredOffset))
                        return false;
                    break;

                case RespCommand.SETBIT:
                    byte* i = inputPtr + RespInputHeader.Size;
                    byte* v = value.ToPointer() + etagIgnoredOffset;

                    // the "- etagIgnoredOffset" accounts for subtracting the space for the etag in the payload if it exists in the Value 
                    if (!BitmapManager.IsLargeEnough(i, value.Length - etagIgnoredOffset)) return false;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    byte oldValSet = BitmapManager.UpdateBitmap(i, v);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;
                case RespCommand.BITFIELD:
                    i = inputPtr + RespInputHeader.Size;
                    v = value.ToPointer() + etagIgnoredOffset;

                    // the "- etagIgnoredOffset" accounts for subtracting the space for the etag in the payload if it exists in the Value 
                    if (!BitmapManager.IsLargeEnoughForType(i, value.Length - etagIgnoredOffset)) return false;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    long bitfieldReturnValue;
                    bool overflow;
                    (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(i, v, value.Length);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);

                    break;

                case RespCommand.PFADD:
                    i = inputPtr + RespInputHeader.Size;
                    v = value.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(v, value.Length))
                    {
                        *output.SpanByte.ToPointer() = (byte)0xFF;
                        return true;
                    }

                    bool updated = false;
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    var result = HyperLogLog.DefaultHLL.Update(i, v, value.Length, ref updated);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    if (result)
                        *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;

                    // doesnt update etag because this doesnt work with etag data
                    return result;

                case RespCommand.PFMERGE:
                    //srcHLL offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)                    
                    byte* srcHLL = inputPtr + RespInputHeader.Size + sizeof(int);
                    byte* dstHLL = value.ToPointer();

                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(dstHLL, value.Length))
                    {
                        //InvalidType                                                
                        *(long*)output.SpanByte.ToPointer() = -1;
                        return true;
                    }
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                    // doesnt update etag because this doesnt work with etag data
                    return HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, value.Length);

                case RespCommand.SETRANGE:
                    var offset = *(int*)(inputPtr + RespInputHeader.Size);
                    var newValueSize = *(int*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var newValuePtr = new Span<byte>((byte*)*(long*)(inputPtr + RespInputHeader.Size + sizeof(int) * 2), newValueSize);

                    if (newValueSize + offset > value.LengthWithoutMetadata - etagIgnoredOffset)
                        return false;

                    newValuePtr.CopyTo(value.AsSpan(etagIgnoredOffset).Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output, etagIgnoredOffset);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref value, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.SETWITHETAG:
                    if (input.Length - RespInputHeader.Size + sizeof(long) > value.Length)
                        return false;

                    // retain the older etag (and increment it to account for this update) if requested and if it also exists otherwise set etag to initial etag of 0
                    long etagVal = ((RespInputHeader*)inputPtr)->CheckRetainEtagFlag() && recordInfo.ETag ? (oldEtag + 1) : 0;

                    recordInfo.SetHasETag();

                    // Copy input to value
                    value.ShrinkSerializedLength(input.Length - RespInputHeader.Size + sizeof(long));
                    value.ExtraMetadata = input.ExtraMetadata;

                    *(long*)value.ToPointer() = etagVal;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(value.AsSpan(sizeof(long)));

                    // Copy initial etag to output
                    CopyRespNumber(etagVal, ref output);
                    // early return since initial etag setting does not need to be incremented
                    return true;

                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendSize = *(int*)(inputPtr + RespInputHeader.Size);

                    if (appendSize == 0)
                    {
                        CopyValueLengthToOutput(ref value, ref output, etagIgnoredOffset);
                        return true;
                    }

                    return false;

                default:
                    if (*inputPtr >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[*inputPtr - CustomCommandManager.StartOffset].functions;
                        var expiration = input.ExtraMetadata;
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

                        int valueLength = value.LengthWithoutMetadata - etagIgnoredOffset;
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functions.InPlaceUpdater(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], value.AsSpan(etagIgnoredOffset), ref valueLength, ref outp, ref rmwInfo);
                        Debug.Assert(valueLength <= value.LengthWithoutMetadata);

                        // Adjust value length if user shrinks it
                        if (valueLength < value.LengthWithoutMetadata - etagIgnoredOffset)
                        {
                            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                            value.ShrinkSerializedLength(valueLength + value.MetadataSize);
                            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                        }

                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        if (!ret)
                            return false;

                        break;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            // increment the Etag transparently if in place update happened
            if (recordInfo.ETag && rmwInfo.Action == RMWAction.Default)
            {
                *(long*)value.ToPointer() = oldEtag + 1;
            }
            return true;
        }

        /// <inheritdoc />
        public bool NeedCopyUpdate(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var inputPtr = input.ToPointer();

            int etagIgnoredOffset = 0;
            int etagIgnoredEnd = -1;
            if (rmwInfo.RecordInfo.ETag)
            {
                etagIgnoredOffset = sizeof(long);
                etagIgnoredEnd = oldValue.LengthWithoutMetadata;
            }

            switch ((RespCommand)(*inputPtr))
            {
                case RespCommand.SETIFMATCH:
                    if (!rmwInfo.RecordInfo.ETag)
                        return false;

                    long existingEtag = *(long*)oldValue.ToPointer();
                    long etagToCheckWith = *(long*)(input.ToPointer() + RespInputHeader.Size);
                    if (existingEtag != etagToCheckWith)
                    {
                        // cancellation and return false indicates ETag mismatch
                        rmwInfo.Action = RMWAction.CancelOperation;
                        return false;
                    }
                    return true;
                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndStop ensures that caller sees a NOTFOUND status
                    if (oldValue.MetadataSize > 0 && ((RespInputHeader*)inputPtr)->CheckExpiry(oldValue.ExtraMetadata))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                        return false;
                    }
                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    }
                    return false;
                default:
                    if (*inputPtr >= CustomCommandManager.StartOffset)
                    {
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState.customCommands[*inputPtr - CustomCommandManager.StartOffset].functions.NeedCopyUpdate(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], oldValue.AsReadOnlySpan(etagIgnoredOffset), ref outp);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;

                    }
                    return true;
            }
        }

        /// <inheritdoc />
        public bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var inputPtr = input.ToPointer();

            // Expired data
            if (oldValue.MetadataSize > 0 && ((RespInputHeader*)inputPtr)->CheckExpiry(oldValue.ExtraMetadata))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            rmwInfo.ClearExtraValueLength(ref recordInfo, ref newValue, newValue.TotalSize);

            var cmd = (RespCommand)(*inputPtr);

            bool shouldUpdateEtag = true;
            int etagIgnoredOffset = 0;
            int etagIgnoredEnd = -1;
            long oldEtag = -1;
            if (recordInfo.ETag)
            {
                etagIgnoredOffset = sizeof(long);
                etagIgnoredEnd = oldValue.LengthWithoutMetadata;
                oldEtag = *(long*)oldValue.ToPointer();
            }

            switch (cmd)
            {
                case RespCommand.SETWITHETAG:
                    Debug.Assert(input.Length - RespInputHeader.Size + sizeof(long) == newValue.Length);

                    // etag setting will be done here so does not need to be incremented outside switch
                    shouldUpdateEtag = false;
                    // retain the older etag (and increment it to account for this update) if requested and if it also exists otherwise set etag to initial etag of 0
                    long etagVal = ((RespInputHeader*)inputPtr)->CheckRetainEtagFlag() && recordInfo.ETag ? (oldEtag + 1) : 0;
                    recordInfo.SetHasETag();
                    // Copy input to value
                    newValue.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(newValue.AsSpan(sizeof(long)));
                    // set the etag
                    *(long*)newValue.ToPointer() = etagVal;
                    // Copy initial etag to output
                    CopyRespNumber(etagVal, ref output);
                    break;

                case RespCommand.SETIFMATCH:
                    Debug.Assert(recordInfo.ETag, "We should never be able to CU for ETag command on non-etag data. Inplace update should have returned mismatch.");

                    // this update is so the early call to send the resp command works, outside of the switch
                    // we are doing a double op of setting the etag to normalize etag update for other operations
                    *(long*)(input.ToPointer() + RespInputHeader.Size) += 1;

                    // Copy input to value
                    newValue.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(newValue.AsSpan());

                    // Write Etag and Val back to Client
                    CopyRespToWithInput(ref input, ref newValue, ref output, false, 0, -1, true);
                    break;

                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    var nextUpdateEtagOffset = etagIgnoredOffset;
                    var nextUpdateEtagIgnoredEnd = etagIgnoredEnd;
                    if (!((RespInputHeader*)inputPtr)->CheckRetainEtagFlag())
                    {
                        // if the user did not explictly asked for retaining the etag we need to ignore the etag if it existed on the previous record
                        nextUpdateEtagOffset = 0;
                        nextUpdateEtagIgnoredEnd = -1;
                        recordInfo.ClearHasETag();
                    }
                
                    // new value when allocated should have 8 bytes more if the previous record had etag and the cmd was not SETEXXX
                    Debug.Assert(input.Length - RespInputHeader.Size == newValue.Length - etagIgnoredOffset);

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    }

                    // Copy input to value
                    newValue.ExtraMetadata = input.ExtraMetadata;

                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(newValue.AsSpan(nextUpdateEtagOffset));
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    nextUpdateEtagOffset = etagIgnoredOffset;
                    nextUpdateEtagIgnoredEnd = etagIgnoredEnd;
                    if (!((RespInputHeader*)inputPtr)->CheckRetainEtagFlag())
                    {
                        // if the user did not explictly asked for retaining the etag we need to ignore the etag if it existed on the previous record
                        nextUpdateEtagOffset = 0;
                        nextUpdateEtagIgnoredEnd = -1;
                    }

                    Debug.Assert(oldValue.MetadataSize + input.Length - RespInputHeader.Size == newValue.Length);

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    }

                    // Copy input to value, retain metadata of oldValue
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    input.AsReadOnlySpan().Slice(RespInputHeader.Size).CopyTo(newValue.AsSpan(nextUpdateEtagOffset));
                    break;

                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                    Debug.Assert(newValue.Length == oldValue.Length + input.MetadataSize);
                    shouldUpdateEtag = false;
                    ExpireOption optionType = (ExpireOption)(*(inputPtr + RespInputHeader.Size));
                    bool expiryExists = oldValue.MetadataSize > 0;
                    EvaluateExpireCopyUpdate(optionType, expiryExists, ref input, ref oldValue, ref newValue, ref output);
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
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: 1, etagIgnoredOffset);
                    break;

                case RespCommand.DECR:
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: -1, etagIgnoredOffset);
                    break;

                case RespCommand.INCRBY:
                    var length = input.LengthWithoutMetadata - RespInputHeader.Size;
                    // Check if input contains a valid number
                    if (!IsValidNumber(length, input.ToPointer() + RespInputHeader.Size, output.SpanByte.AsSpan(), out var incrBy))
                    {
                        // Move to tail of the log
                        oldValue.CopyTo(ref newValue);
                        break;
                    }
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: incrBy, etagIgnoredOffset);
                    break;

                case RespCommand.DECRBY:
                    length = input.LengthWithoutMetadata - RespInputHeader.Size;
                    // Check if input contains a valid number
                    if (!IsValidNumber(length, input.ToPointer() + RespInputHeader.Size, output.SpanByte.AsSpan(), out var decrBy))
                    {
                        // Move to tail of the log
                        oldValue.CopyTo(ref newValue);
                        break;
                    }
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: -decrBy, etagIgnoredOffset);
                    break;

                case RespCommand.SETBIT:
                    Buffer.MemoryCopy(oldValue.ToPointer() + etagIgnoredOffset, newValue.ToPointer() + etagIgnoredOffset, newValue.Length - etagIgnoredOffset, oldValue.Length - etagIgnoredOffset);
                    byte oldValSet = BitmapManager.UpdateBitmap(inputPtr + RespInputHeader.Size, newValue.ToPointer());
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;

                case RespCommand.BITFIELD:
                    Buffer.MemoryCopy(oldValue.ToPointer() + etagIgnoredOffset, newValue.ToPointer() + etagIgnoredOffset, newValue.Length - etagIgnoredOffset, oldValue.Length - etagIgnoredOffset);
                    long bitfieldReturnValue;
                    bool overflow;
                    (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(inputPtr + RespInputHeader.Size, newValue.ToPointer() + etagIgnoredOffset, newValue.Length - etagIgnoredOffset);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.PFADD:
                    //  HYPERLOG doesnt work with non hyperlog key values
                    bool updated = false;
                    byte* newValPtr = newValue.ToPointer();
                    byte* oldValPtr = oldValue.ToPointer();

                    if (newValue.Length != oldValue.Length)
                        updated = HyperLogLog.DefaultHLL.CopyUpdate(inputPtr + RespInputHeader.Size, oldValPtr, newValPtr, newValue.Length);
                    else
                    {
                        Buffer.MemoryCopy(oldValPtr, newValPtr, newValue.Length - etagIgnoredOffset, oldValue.Length);
                        HyperLogLog.DefaultHLL.Update(inputPtr + RespInputHeader.Size, newValPtr, newValue.Length, ref updated);
                    }
                    *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    break;

                case RespCommand.PFMERGE:
                    //  HYPERLOG doesnt work with non hyperlog key values
                    //srcA offset: [hll allocated size = 4 byte] + [hll data structure] //memcpy +4 (skip len size)                    
                    byte* srcHLLPtr = inputPtr + RespInputHeader.Size + sizeof(int); // HLL merging from
                    byte* oldDstHLLPtr = oldValue.ToPointer(); // original HLL merging to (too small to hold its data plus srcA)
                    byte* newDstHLLPtr = newValue.ToPointer(); // new HLL merging to (large enough to hold srcA and srcB

                    HyperLogLog.DefaultHLL.CopyUpdateMerge(srcHLLPtr, oldDstHLLPtr, newDstHLLPtr, oldValue.Length, newValue.Length);
                    break;

                case RespCommand.SETRANGE:
                    var offset = *(int*)(inputPtr + RespInputHeader.Size);
                    oldValue.CopyTo(ref newValue);

                    var newValueSize = *(int*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var newValuePtr = new Span<byte>((byte*)*(long*)(inputPtr + RespInputHeader.Size + sizeof(int) * 2), newValueSize);
                    newValuePtr.CopyTo(newValue.AsSpan(etagIgnoredOffset).Slice(offset));

                    CopyValueLengthToOutput(ref newValue, ref output, etagIgnoredOffset);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref oldValue, ref output, etagIgnoredOffset, etagIgnoredEnd);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.APPEND:
                    // Copy any existing value with metadata to thew new value
                    oldValue.CopyTo(ref newValue);

                    var appendSize = *(int*)(inputPtr + RespInputHeader.Size);
                    var appendPtr = *(long*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var appendSpan = new Span<byte>((byte*)appendPtr, appendSize);

                    // Append the new value with the client input at the end of the old data
                    // the oldValue.LengthWithoutMetadata already contains the etag offset here
                    appendSpan.CopyTo(newValue.AsSpan().Slice(oldValue.LengthWithoutMetadata));

                    CopyValueLengthToOutput(ref newValue, ref output, etagIgnoredOffset);
                    break;

                default:
                    if (*inputPtr >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[*inputPtr - CustomCommandManager.StartOffset].functions;
                        var expiration = input.ExtraMetadata;
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

                        var ret = functionsState.customCommands[*inputPtr - CustomCommandManager.StartOffset].functions.CopyUpdater(key.AsReadOnlySpan(), input.AsReadOnlySpan().Slice(RespInputHeader.Size),
                            oldValue.AsReadOnlySpan(etagIgnoredOffset), newValue.AsSpan(etagIgnoredOffset), ref outp, ref rmwInfo);
                        output.Memory = outp.Memory;
                        output.Length = outp.Length;
                        return ret;
                    }
                    throw new GarnetException("Unsupported operation on input");
            }

            rmwInfo.SetUsedValueLength(ref recordInfo, ref newValue, newValue.TotalSize);

            // increment the Etag transparently if in place update happened
            if (recordInfo.ETag && shouldUpdateEtag)
            {
                *(long*)newValue.ToPointer() = oldEtag + 1;
            }

            return true;
        }

        /// <inheritdoc />
        public bool PostCopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, ref oldValue, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}