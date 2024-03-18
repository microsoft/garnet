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
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainStoreFunctions : IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var cmd = input.AsSpan()[0];
            switch ((RespCommand)cmd)
            {
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

                    CopyValueLengthToOutput(ref value, ref output);
                    break;

                case RespCommand.APPEND:
                    // Copy value to be appended to the newly allocated value buffer
                    var appendSize = *(int*)(inputPtr + RespInputHeader.Size);
                    var appendPtr = *(long*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var appendSpan = new Span<byte>((byte*)appendPtr, appendSize);
                    appendSpan.CopyTo(value.AsSpan());

                    CopyValueLengthToOutput(ref value, ref output);
                    break;

                case RespCommand.DECRBY:
                    value.UnmarkExtraMetadata();
                    CopyUpdateNumber(-NumUtils.BytesToLong(input.LengthWithoutMetadata - RespInputHeader.Size, inputPtr + RespInputHeader.Size), ref value, ref output);
                    break;

                default:
                    value.UnmarkExtraMetadata();

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
            switch ((RespCommand)(*inputPtr))
            {
                case RespCommand.SETEXNX:
                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output);
                    }
                    return true;

                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    // Need CU if no space for new value
                    if (input.Length - RespInputHeader.Size > value.Length) return false;

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(input.Length - RespInputHeader.Size);

                    // Copy input to value
                    value.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(value.AsSpan());
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    return true;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    // Need CU if no space for new value
                    if (value.MetadataSize + input.Length - RespInputHeader.Size > value.Length) return false;

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(value.MetadataSize + input.Length - RespInputHeader.Size);

                    // Copy input to value
                    input.AsReadOnlySpan().Slice(RespInputHeader.Size).CopyTo(value.AsSpan());
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                    return true;

                case RespCommand.PEXPIRE:
                case RespCommand.EXPIRE:
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
                    return true;

                case RespCommand.INCR:
                    long val = NumUtils.BytesToLong(value.AsSpan());
                    val++;
                    return InPlaceUpdateNumber(val, ref value, ref output, ref rmwInfo, ref recordInfo);

                case RespCommand.DECR:
                    val = NumUtils.BytesToLong(value.AsSpan());
                    val--;
                    return InPlaceUpdateNumber(val, ref value, ref output, ref rmwInfo, ref recordInfo);

                case RespCommand.INCRBY:
                    val = NumUtils.BytesToLong(value.LengthWithoutMetadata, value.ToPointer());
                    val += NumUtils.BytesToLong(input.LengthWithoutMetadata - RespInputHeader.Size, inputPtr + RespInputHeader.Size);
                    return InPlaceUpdateNumber(val, ref value, ref output, ref rmwInfo, ref recordInfo);

                case RespCommand.DECRBY:
                    val = NumUtils.BytesToLong(value.LengthWithoutMetadata, value.ToPointer());
                    val -= NumUtils.BytesToLong(input.LengthWithoutMetadata - RespInputHeader.Size, inputPtr + RespInputHeader.Size);
                    return InPlaceUpdateNumber(val, ref value, ref output, ref rmwInfo, ref recordInfo);

                case RespCommand.SETBIT:
                    byte* i = inputPtr + RespInputHeader.Size;
                    byte* v = value.ToPointer();

                    if (!BitmapManager.IsLargeEnough(i, value.Length)) return false;

                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(value.Length + value.MetadataSize);
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    byte oldValSet = BitmapManager.UpdateBitmap(i, v);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    return true;
                case RespCommand.BITFIELD:
                    i = inputPtr + RespInputHeader.Size;
                    v = value.ToPointer();
                    if (!BitmapManager.IsLargeEnoughForType(i, value.Length)) return false;

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
                    return true;

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
                    return HyperLogLog.DefaultHLL.TryMerge(srcHLL, dstHLL, value.Length);
                case RespCommand.SETRANGE:
                    var offset = *(int*)(inputPtr + RespInputHeader.Size);
                    var newValueSize = *(int*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var newValuePtr = new Span<byte>((byte*)*(long*)(inputPtr + RespInputHeader.Size + sizeof(int) * 2), newValueSize);

                    if (newValueSize + offset > value.LengthWithoutMetadata)
                        return false;

                    newValuePtr.CopyTo(value.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output);
                    return true;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref value, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;


                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendSize = *(int*)(inputPtr + RespInputHeader.Size);

                    if (appendSize == 0)
                    {
                        CopyValueLengthToOutput(ref value, ref output);
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

                        int valueLength = value.LengthWithoutMetadata;
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functions.InPlaceUpdater(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], value.AsSpan(), ref valueLength, ref outp, ref rmwInfo);
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
        public bool NeedCopyUpdate(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var inputPtr = input.ToPointer();
            switch ((RespCommand)(*inputPtr))
            {
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
                        CopyRespTo(ref oldValue, ref output);
                    }
                    return false;
                default:
                    if (*inputPtr >= CustomCommandManager.StartOffset)
                    {
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState.customCommands[*inputPtr - CustomCommandManager.StartOffset].functions.NeedCopyUpdate(key.AsReadOnlySpan(), input.AsReadOnlySpan()[RespInputHeader.Size..], oldValue.AsReadOnlySpan(), ref outp);
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

            switch ((RespCommand)(*inputPtr))
            {
                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    Debug.Assert(input.Length - RespInputHeader.Size == newValue.Length);

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output);
                    }

                    // Copy input to value
                    newValue.ExtraMetadata = input.ExtraMetadata;
                    input.AsReadOnlySpan()[RespInputHeader.Size..].CopyTo(newValue.AsSpan());
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    Debug.Assert(oldValue.MetadataSize + input.Length - RespInputHeader.Size == newValue.Length);

                    // Check if SetGet flag is set
                    if (((RespInputHeader*)inputPtr)->CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output);
                    }

                    // Copy input to value, retain metadata of oldValue
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    input.AsReadOnlySpan().Slice(RespInputHeader.Size).CopyTo(newValue.AsSpan());
                    break;

                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                    Debug.Assert(newValue.Length == oldValue.Length + input.MetadataSize);
                    ExpireOption optionType = (ExpireOption)(*(inputPtr + RespInputHeader.Size));
                    bool expiryExists = oldValue.MetadataSize > 0;
                    EvaluateExpireCopyUpdate(optionType, expiryExists, ref input, ref oldValue, ref newValue, ref output);
                    break;

                case RespCommand.PERSIST:
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
                    // Copy over metadata
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    long curr = NumUtils.BytesToLong(oldValue.AsSpan());
                    CopyUpdateNumber(curr + 1, ref newValue, ref output);
                    break;

                case RespCommand.INCRBY:
                    // Copy over metadata
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    curr = NumUtils.BytesToLong(oldValue.AsSpan());
                    CopyUpdateNumber(curr + NumUtils.BytesToLong(input.LengthWithoutMetadata - RespInputHeader.Size, inputPtr + RespInputHeader.Size), ref newValue, ref output);
                    break;

                case RespCommand.DECR:
                    // Copy over metadata
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    curr = NumUtils.BytesToLong(oldValue.AsSpan());
                    CopyUpdateNumber(curr - 1, ref newValue, ref output);
                    break;

                case RespCommand.DECRBY:
                    // Copy over metadata
                    newValue.ExtraMetadata = oldValue.ExtraMetadata;
                    curr = NumUtils.BytesToLong(oldValue.AsSpan());
                    CopyUpdateNumber(curr - NumUtils.BytesToLong(input.LengthWithoutMetadata - RespInputHeader.Size, inputPtr + RespInputHeader.Size), ref newValue, ref output);
                    break;

                case RespCommand.SETBIT:
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValue.ToPointer(), newValue.Length, oldValue.Length);
                    byte oldValSet = BitmapManager.UpdateBitmap(inputPtr + RespInputHeader.Size, newValue.ToPointer());
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output);
                    break;

                case RespCommand.BITFIELD:
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValue.ToPointer(), newValue.Length, oldValue.Length);
                    long bitfieldReturnValue;
                    bool overflow;
                    (bitfieldReturnValue, overflow) = BitmapManager.BitFieldExecute(inputPtr + RespInputHeader.Size, newValue.ToPointer(), newValue.Length);

                    if (!overflow)
                        CopyRespNumber(bitfieldReturnValue, ref output);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref output);
                    break;

                case RespCommand.PFADD:
                    bool updated = false;
                    byte* newValPtr = newValue.ToPointer();
                    byte* oldValPtr = oldValue.ToPointer();

                    if (newValue.Length != oldValue.Length)
                        updated = HyperLogLog.DefaultHLL.CopyUpdate(inputPtr + RespInputHeader.Size, oldValPtr, newValPtr, newValue.Length);
                    else
                    {
                        Buffer.MemoryCopy(oldValPtr, newValPtr, newValue.Length, oldValue.Length);
                        HyperLogLog.DefaultHLL.Update(inputPtr + RespInputHeader.Size, newValPtr, newValue.Length, ref updated);
                    }
                    *output.SpanByte.ToPointer() = updated ? (byte)1 : (byte)0;
                    break;

                case RespCommand.PFMERGE:
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
                    newValuePtr.CopyTo(newValue.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(ref newValue, ref output);
                    break;

                case RespCommand.GETDEL:
                    // Copy value to output for the GET part of the command.
                    // Then, set ExpireAndStop action to delete the record.
                    CopyRespTo(ref oldValue, ref output);
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;

                case RespCommand.APPEND:
                    // Copy any existing value with metadata to thew new value
                    oldValue.CopyTo(ref newValue);

                    var appendSize = *(int*)(inputPtr + RespInputHeader.Size);
                    var appendPtr = *(long*)(inputPtr + RespInputHeader.Size + sizeof(int));
                    var appendSpan = new Span<byte>((byte*)appendPtr, appendSize);

                    // Append the new value with the client input at the end of the old data
                    appendSpan.CopyTo(newValue.AsSpan().Slice(oldValue.LengthWithoutMetadata));

                    CopyValueLengthToOutput(ref newValue, ref output);
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
                            oldValue.AsReadOnlySpan(), newValue.AsSpan(), ref outp, ref rmwInfo);
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
        public void PostCopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, ref oldValue, rmwInfo.Version, rmwInfo.SessionID);
        }
    }
}