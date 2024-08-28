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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool NeedInitialUpdate(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
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
                        var ret = functionsState.customCommands[cmd - CustomCommandManager.StartOffset].functions.NeedInitialUpdate(key.AsReadOnlySpan(), ref input, ref outp);
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
            var inputPtr = input.ToPointer();

            switch (input.header.cmd)
            {
                case RespCommand.PFADD:
                    var count = input.parseState.GetInt(0);
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
                    var newInputValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan;
                    var metadataSize = input.arg1 == 0 ? 0 : sizeof(long);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(newInputValue.Length + metadataSize);
                    value.ExtraMetadata = input.arg1;
                    newInputValue.CopyTo(value.AsSpan());
                    break;

                case RespCommand.SETKEEPTTL:
                    // Copy input to value, retain metadata in value
                    var setValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan;
                    value.ShrinkSerializedLength(value.MetadataSize + setValue.Length);
                    setValue.CopyTo(value.AsSpan());
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETEXXX:
                case RespCommand.EXPIRE:
                case RespCommand.PEXPIRE:
                case RespCommand.PERSIST:
                case RespCommand.GETDEL:
                    throw new Exception();

                case RespCommand.SETBIT:
                    var currTokenIdx = input.parseStateStartIdx;
                    var bOffset = input.parseState.GetLong(currTokenIdx++);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(currTokenIdx).ReadOnlySpan[0] - '0');

                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(BitmapManager.Length(bOffset));

                    // Always return 0 at initial updater because previous value was 0
                    BitmapManager.UpdateBitmap(value.ToPointer(), bOffset, bSetVal);
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
                    var offset = input.parseState.GetInt(input.parseStateStartIdx);
                    var newValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx + 1).ReadOnlySpan;
                    newValue.CopyTo(value.AsSpan().Slice(offset));

                    CopyValueLengthToOutput(ref value, ref output);
                    break;

                case RespCommand.APPEND:
                    var appendValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx);

                    // Copy value to be appended to the newly allocated value buffer
                    appendValue.ReadOnlySpan.CopyTo(value.AsSpan());

                    CopyValueLengthToOutput(ref value, ref output);
                    break;
                case RespCommand.INCRBY:
                    value.UnmarkExtraMetadata();
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out var incrBy))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    CopyUpdateNumber(incrBy, ref value, ref output);
                    break;
                case RespCommand.DECRBY:
                    value.UnmarkExtraMetadata();
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out var decrBy))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    CopyUpdateNumber(-decrBy, ref value, ref output);
                    break;
                default:
                    value.UnmarkExtraMetadata();

                    if ((byte)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(byte)input.header.cmd - CustomCommandManager.StartOffset].functions;
                        // compute metadata size for result
                        long expiration = input.arg1;
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
                    var inputValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx);
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
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                ((RespInputHeader*)input.ToPointer())->SetExpiredFlag();
                WriteLogRMW(ref key, ref input, ref value, rmwInfo.Version, rmwInfo.SessionID);
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
                    WriteLogRMW(ref key, ref input, ref value, rmwInfo.Version, rmwInfo.SessionID);
                return true;
            }
            return false;
        }

        private bool InPlaceUpdaterWorker(ref SpanByte key, ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            var inputPtr = input.ToPointer();

            // Expired data
            if (value.MetadataSize > 0 && input.header.CheckExpiry(value.ExtraMetadata))
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
                        CopyRespTo(ref value, ref output);
                    }
                    return true;

                case RespCommand.SET:
                case RespCommand.SETEXXX:
                    var setValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx);

                    // Need CU if no space for new value
                    var metadataSize = input.arg1 == 0 ? 0 : sizeof(long);
                    if (setValue.Length + metadataSize > value.Length) return false;

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.UnmarkExtraMetadata();
                    value.ShrinkSerializedLength(setValue.Length + metadataSize);

                    // Copy input to value
                    value.ExtraMetadata = input.arg1;
                    setValue.ReadOnlySpan.CopyTo(value.AsSpan());
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

                    return true;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    setValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx);
                    // Need CU if no space for new value
                    if (setValue.Length + value.MetadataSize > value.Length) return false;

                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref value, ref output);
                    }

                    // Adjust value length
                    rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
                    value.ShrinkSerializedLength(setValue.Length + value.MetadataSize);

                    // Copy input to value
                    setValue.ReadOnlySpan.CopyTo(value.AsSpan());
                    rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
                    return true;

                case RespCommand.PEXPIRE:
                case RespCommand.EXPIRE:
                    var expiryExists = value.MetadataSize > 0;
                    
                    var expiryValue = input.parseState.GetInt(input.parseStateStartIdx);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;

                    var optionType = ExpireOption.None;
                    if (input.parseState.Count - input.parseStateStartIdx > 1)
                    {
                        optionType = input.parseState.GetEnum<ExpireOption>(input.parseStateStartIdx + 1, true);
                    }

                    return EvaluateExpireInPlace(optionType, expiryExists, expiryTicks, ref value, ref output);

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
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: 1);

                case RespCommand.DECR:
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -1);

                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out var incrBy))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: incrBy);

                case RespCommand.DECRBY:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out var decrBy))
                    {
                        output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                        return true;
                    }
                    return TryInPlaceUpdateNumber(ref value, ref output, ref rmwInfo, ref recordInfo, input: -decrBy);

                case RespCommand.SETBIT:
                    var v = value.ToPointer();
                    var currTokenIdx = input.parseStateStartIdx;
                    var bOffset = input.parseState.GetLong(currTokenIdx++);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(currTokenIdx).ReadOnlySpan[0] - '0');

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
                    var i = inputPtr + RespInputHeader.Size;
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
                    var offset = input.parseState.GetInt(input.parseStateStartIdx);
                    var newValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx + 1).ReadOnlySpan;

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


                case RespCommand.APPEND:
                    // If nothing to append, can avoid copy update.
                    var appendSize = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).Length;

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

                        int valueLength = value.LengthWithoutMetadata;
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
        public bool NeedCopyUpdate(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            switch (input.header.cmd)
            {
                case RespCommand.SETEXNX:
                    // Expired data, return false immediately
                    // ExpireAndStop ensures that caller sees a NOTFOUND status
                    if (oldValue.MetadataSize > 0 && input.header.CheckExpiry(oldValue.ExtraMetadata))
                    {
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                        return false;
                    }
                    // Check if SetGet flag is set
                    if (input.header.CheckSetGetFlag())
                    {
                        // Copy value to output for the GET part of the command.
                        CopyRespTo(ref oldValue, ref output);
                    }
                    return false;
                default:
                    if ((byte)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                        var ret = functionsState.customCommands[(byte)input.header.cmd - CustomCommandManager.StartOffset].functions
                            .NeedCopyUpdate(key.AsReadOnlySpan(), ref input, oldValue.AsReadOnlySpan(), ref outp);
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
            var inputPtr = input.ToPointer();

            // Expired data
            if (oldValue.MetadataSize > 0 && input.header.CheckExpiry(oldValue.ExtraMetadata))
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            rmwInfo.ClearExtraValueLength(ref recordInfo, ref newValue, newValue.TotalSize);

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
                    var newInputValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan;
                    var metadataSize = input.arg1 == 0 ? 0 : sizeof(long);

                    Debug.Assert(newInputValue.Length + metadataSize == newValue.Length);

                    newValue.ExtraMetadata = input.arg1;
                    newInputValue.CopyTo(newValue.AsSpan());
                    break;

                case RespCommand.SETKEEPTTLXX:
                case RespCommand.SETKEEPTTL:
                    var setValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx).ReadOnlySpan;
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

                    var expiryValue = input.parseState.GetInt(input.parseStateStartIdx);
                    var tsExpiry = input.header.cmd == RespCommand.EXPIRE
                        ? TimeSpan.FromSeconds(expiryValue)
                        : TimeSpan.FromMilliseconds(expiryValue);
                    var expiryTicks = DateTimeOffset.UtcNow.Ticks + tsExpiry.Ticks;

                    var optionType = ExpireOption.None;
                    if (input.parseState.Count - input.parseStateStartIdx > 1)
                    {
                        optionType = input.parseState.GetEnum<ExpireOption>(input.parseStateStartIdx + 1, true);
                    }

                    EvaluateExpireCopyUpdate(optionType, expiryExists, expiryTicks, ref oldValue, ref newValue, ref output);
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
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: 1);
                    break;

                case RespCommand.DECR:
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: -1);
                    break;

                case RespCommand.INCRBY:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out var incrBy))
                    {
                        // Move to tail of the log
                        oldValue.CopyTo(ref newValue);
                        break;
                    }
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: incrBy);
                    break;

                case RespCommand.DECRBY:
                    // Check if input contains a valid number
                    if (!input.parseState.TryGetLong(input.parseStateStartIdx, out var decrBy))
                    {
                        // Move to tail of the log
                        oldValue.CopyTo(ref newValue);
                        break;
                    }
                    TryCopyUpdateNumber(ref oldValue, ref newValue, ref output, input: -decrBy);
                    break;

                case RespCommand.SETBIT:
                    var currTokenIdx = input.parseStateStartIdx;
                    var bOffset = input.parseState.GetLong(currTokenIdx++);
                    var bSetVal = (byte)(input.parseState.GetArgSliceByRef(currTokenIdx).ReadOnlySpan[0] - '0');
                    Buffer.MemoryCopy(oldValue.ToPointer(), newValue.ToPointer(), newValue.Length, oldValue.Length);
                    var oldValSet = BitmapManager.UpdateBitmap(newValue.ToPointer(), bOffset, bSetVal);
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
                    var offset = input.parseState.GetInt(input.parseStateStartIdx);
                    oldValue.CopyTo(ref newValue);

                    newInputValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx + 1).ReadOnlySpan;
                    newInputValue.CopyTo(newValue.AsSpan().Slice(offset));

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

                    var appendValue = input.parseState.GetArgSliceByRef(input.parseStateStartIdx);

                    // Append the new value with the client input at the end of the old data
                    appendValue.ReadOnlySpan.CopyTo(newValue.AsSpan().Slice(oldValue.LengthWithoutMetadata));

                    CopyValueLengthToOutput(ref newValue, ref output);
                    break;

                default:
                    if ((byte)input.header.cmd >= CustomCommandManager.StartOffset)
                    {
                        var functions = functionsState.customCommands[(byte)input.header.cmd - CustomCommandManager.StartOffset].functions;
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

                        var ret = functionsState.customCommands[(byte)input.header.cmd - CustomCommandManager.StartOffset].functions
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
        public bool PostCopyUpdater(ref SpanByte key, ref RawStringInput input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(rmwInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogRMW(ref key, ref input, ref oldValue, rmwInfo.Version, rmwInfo.SessionID);
            return true;
        }
    }
}