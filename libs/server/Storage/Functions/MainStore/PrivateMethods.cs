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
        static void CopyTo(ref SpanByte src, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            int srcLength = src.LengthWithoutMetadata;

            if (dst.IsSpanByte)
            {
                if (dst.Length >= srcLength)
                {
                    dst.Length = srcLength;
                    src.AsReadOnlySpan().CopyTo(dst.SpanByte.AsSpan());
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = memoryPool.Rent(srcLength);
            dst.Length = srcLength;
            src.AsReadOnlySpan().CopyTo(dst.Memory.Memory.Span);
        }

        void CopyRespTo(ref SpanByte src, ref SpanByteAndMemory dst, int start = 0, int end = -1)
        {
            int srcLength = end == -1 ? src.LengthWithoutMetadata : ((start < end) ? (end - start) : 0);
            if (srcLength == 0)
            {
                CopyDefaultResp(CmdStrings.RESP_EMPTY, ref dst);
                return;
            }

            var numLength = NumUtils.CountDigits(srcLength);
            int totalSize = 1 + numLength + 2 + srcLength + 2; // $5\r\nvalue\r\n

            if (dst.IsSpanByte)
            {
                if (dst.Length >= totalSize)
                {
                    dst.Length = totalSize;

                    byte* tmp = dst.SpanByte.ToPointer();
                    *tmp++ = (byte)'$';
                    NumUtils.WriteInt32(srcLength, numLength, ref tmp);
                    *tmp++ = (byte)'\r';
                    *tmp++ = (byte)'\n';
                    src.AsReadOnlySpan().Slice(start, srcLength).CopyTo(new Span<byte>(tmp, srcLength));
                    tmp += srcLength;
                    *tmp++ = (byte)'\r';
                    *tmp++ = (byte)'\n';
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = functionsState.memoryPool.Rent(totalSize);
            dst.Length = totalSize;
            fixed (byte* ptr = dst.Memory.Memory.Span)
            {
                byte* tmp = ptr;
                *tmp++ = (byte)'$';
                NumUtils.WriteInt32(srcLength, numLength, ref tmp);
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';
                src.AsReadOnlySpan().Slice(start, srcLength).CopyTo(new Span<byte>(tmp, srcLength));
                tmp += srcLength;
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';
            }
        }

        void CopyRespToWithInput(ref RawStringInput input, ref SpanByte value, ref SpanByteAndMemory dst, bool isFromPending)
        {
            switch (input.header.cmd)
            {
                case RespCommand.ASYNC:
                    // If the GET is expected to complete continuations asynchronously, we should not write anything
                    // to the network buffer in case the operation does go pending (latter is indicated by isFromPending)
                    // This is accomplished by calling ConvertToHeap on the destination SpanByteAndMemory
                    if (isFromPending)
                        dst.ConvertToHeap();
                    CopyRespTo(ref value, ref dst, functionsState.etagState.etagSkippedStart, functionsState.etagState.etagAccountedLength);
                    break;

                case RespCommand.MIGRATE:
                    if (value.Length <= dst.Length)
                    {
                        value.CopyTo(ref dst.SpanByte);
                        dst.Length = value.Length;
                        return;
                    }

                    dst.ConvertToHeap();
                    dst.Length = value.TotalSize;

                    if (dst.Memory == default) // Allocate new heap buffer
                        dst.Memory = functionsState.memoryPool.Rent(dst.Length);
                    else if (dst.Memory.Memory.Span.Length < value.TotalSize)
                    // Allocate new heap buffer only if existing one is smaller
                    // otherwise it is safe to re-use existing buffer
                    {
                        dst.Memory.Dispose();
                        dst.Memory = functionsState.memoryPool.Rent(dst.Length);
                    }
                    value.CopyTo(dst.Memory.Memory.Span);
                    break;

                case RespCommand.VADD:
                case RespCommand.VSIM:
                case RespCommand.VEMB:
                case RespCommand.VGETATTR:
                case RespCommand.VINFO:
                case RespCommand.VREM:
                case RespCommand.VDIM:
                case RespCommand.GET:
                    // Get value without RESP header; exclude expiration
                    if (value.LengthWithoutMetadata <= dst.Length)
                    {
                        dst.Length = value.LengthWithoutMetadata - functionsState.etagState.etagSkippedStart;
                        value.AsReadOnlySpan(functionsState.etagState.etagSkippedStart).CopyTo(dst.SpanByte.AsSpan());
                        return;
                    }

                    dst.ConvertToHeap();
                    dst.Length = value.LengthWithoutMetadata - functionsState.etagState.etagSkippedStart;
                    dst.Memory = functionsState.memoryPool.Rent(value.LengthWithoutMetadata);
                    value.AsReadOnlySpan(functionsState.etagState.etagSkippedStart).CopyTo(dst.Memory.Memory.Span);
                    break;

                case RespCommand.GETBIT:
                    var offset = input.arg1;
                    var oldValSet = BitmapManager.GetBit(offset, value.ToPointer() + functionsState.etagState.etagSkippedStart, value.Length - functionsState.etagState.etagSkippedStart);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref dst);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref dst);
                    break;

                case RespCommand.BITCOUNT:
                    var bcStartOffset = 0;
                    var bcEndOffset = -1;
                    byte bcOffsetType = 0x0;

                    if (input.parseState.Count > 1)
                    {
                        bcStartOffset = input.parseState.GetInt(0);
                        bcEndOffset = input.parseState.GetInt(1);

                        if (input.parseState.Count > 2)
                        {
                            var spanOffsetType = input.parseState.GetArgSliceByRef(2).ReadOnlySpan;
                            bcOffsetType = spanOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8) ? (byte)0x1 : (byte)0x0;
                        }
                    }

                    var count = BitmapManager.BitCountDriver(bcStartOffset, bcEndOffset, bcOffsetType, value.ToPointer() + functionsState.etagState.etagSkippedStart, value.Length - functionsState.etagState.etagSkippedStart);
                    CopyRespNumber(count, ref dst);
                    break;

                case RespCommand.BITPOS:
                    var bpSetVal = (byte)(input.parseState.GetArgSliceByRef(0).ReadOnlySpan[0] - '0');
                    var bpStartOffset = 0;
                    var bpEndOffset = -1;
                    byte bpOffsetType = 0x0;
                    if (input.parseState.Count > 1)
                    {
                        bpStartOffset = input.parseState.GetInt(1);
                        if (input.parseState.Count > 2)
                        {
                            bpEndOffset = input.parseState.GetInt(2);
                            if (input.parseState.Count > 3)
                            {
                                var sbOffsetType = input.parseState.GetArgSliceByRef(3).ReadOnlySpan;
                                bpOffsetType = sbOffsetType.EqualsUpperCaseSpanIgnoringCase("BIT"u8)
                                    ? (byte)0x1
                                    : (byte)0x0;
                            }
                        }
                    }

                    var pos = BitmapManager.BitPosDriver(
                        input: value.ToPointer() + functionsState.etagState.etagSkippedStart,
                        inputLen: value.Length - functionsState.etagState.etagSkippedStart,
                        startOffset: bpStartOffset,
                        endOffset: bpEndOffset,
                        searchFor: bpSetVal,
                        offsetType: bpOffsetType
                        );
                    *(long*)dst.SpanByte.ToPointer() = pos;
                    CopyRespNumber(pos, ref dst);
                    break;

                case RespCommand.BITOP:
                    var bitmap = (IntPtr)value.ToPointer() + functionsState.etagState.etagSkippedStart;
                    var output = dst.SpanByte.ToPointer();

                    *(long*)output = bitmap.ToInt64();
                    *(int*)(output + 8) = value.Length;

                    return;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    var (retValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs,
                                                value.ToPointer() + functionsState.etagState.etagSkippedStart,
                                                value.Length - functionsState.etagState.etagSkippedStart);
                    if (!overflow)
                        CopyRespNumber(retValue, ref dst);
                    else
                        CopyDefaultResp(functionsState.nilResp, ref dst);
                    return;

                case RespCommand.BITFIELD_RO:
                    var bitFieldArgs_RO = GetBitFieldArguments(ref input);
                    var retValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO,
                                                value.ToPointer() + functionsState.etagState.etagSkippedStart,
                                                value.Length - functionsState.etagState.etagSkippedStart);
                    CopyRespNumber(retValue_RO, ref dst);
                    return;

                case RespCommand.PFCOUNT:
                case RespCommand.PFMERGE:
                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(value.ToPointer(), value.Length))
                    {
                        *(long*)dst.SpanByte.ToPointer() = -1;
                        return;
                    }

                    if (value.Length <= dst.Length)
                    {
                        Buffer.MemoryCopy(value.ToPointer(), dst.SpanByte.ToPointer(), value.Length, value.Length);
                        dst.SpanByte.Length = value.Length;
                        return;
                    }

                    throw new GarnetException($"Not enough space in {input.header.cmd} buffer");

                case RespCommand.TTL:
                    var ttlValue = ConvertUtils.SecondsFromDiffUtcNowTicks(value.MetadataSize == 8 ? value.ExtraMetadata : -1);
                    CopyRespNumber(ttlValue, ref dst);
                    return;

                case RespCommand.PTTL:
                    var pttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(value.MetadataSize == 8 ? value.ExtraMetadata : -1);
                    CopyRespNumber(pttlValue, ref dst);
                    return;

                case RespCommand.GETRANGE:
                    var len = value.LengthWithoutMetadata - functionsState.etagState.etagSkippedStart;
                    var start = input.parseState.GetInt(0);
                    var end = input.parseState.GetInt(1);

                    (start, end) = NormalizeRange(start, end, len);
                    CopyRespTo(ref value, ref dst, start + functionsState.etagState.etagSkippedStart, end + functionsState.etagState.etagSkippedStart);
                    return;
                case RespCommand.EXPIRETIME:
                    var expireTime = ConvertUtils.UnixTimeInSecondsFromTicks(value.MetadataSize == 8 ? value.ExtraMetadata : -1);
                    CopyRespNumber(expireTime, ref dst);
                    return;

                case RespCommand.PEXPIRETIME:
                    var pexpireTime = ConvertUtils.UnixTimeInMillisecondsFromTicks(value.MetadataSize == 8 ? value.ExtraMetadata : -1);
                    CopyRespNumber(pexpireTime, ref dst);
                    return;

                default:
                    throw new GarnetException("Unsupported operation on input");
            }
        }

        IPUResult EvaluateExpireInPlace(ExpireOption optionType, bool expiryExists, long newExpiry, ref SpanByte value, ref SpanByteAndMemory output)
        {
            ObjectOutputHeader* o = (ObjectOutputHeader*)output.SpanByte.ToPointer();
            if (expiryExists)
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                        o->result1 = 0;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        value.ExtraMetadata = newExpiry;
                        o->result1 = 1;
                        break;
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                        var replace = newExpiry > value.ExtraMetadata;
                        if (replace) value.ExtraMetadata = newExpiry;
                        if (replace)
                            o->result1 = 1;
                        else
                            o->result1 = 0;
                        break;
                    case ExpireOption.LT:
                    case ExpireOption.XXLT:
                        replace = newExpiry < value.ExtraMetadata;
                        if (replace) value.ExtraMetadata = newExpiry;
                        if (replace)
                            o->result1 = 1;
                        else
                            o->result1 = 0;
                        break;
                    default:
                        throw new GarnetException($"EvaluateExpireInPlace exception expiryExists:{expiryExists}, optionType{optionType}");
                }
                return o->result1 == 1 ? IPUResult.Succeeded : IPUResult.NotUpdated;
            }
            else
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                    case ExpireOption.LT:  // If expiry doesn't exist, LT should treat the current expiration as infinite
                        return IPUResult.Failed;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                    case ExpireOption.XXLT:
                        o->result1 = 0;
                        return IPUResult.NotUpdated;
                    default:
                        throw new GarnetException($"EvaluateExpireInPlace exception expiryExists:{expiryExists}, optionType{optionType}");
                }
            }
        }

        void EvaluateExpireCopyUpdate(ExpireOption optionType, bool expiryExists, long newExpiry, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output)
        {
            ObjectOutputHeader* o = (ObjectOutputHeader*)output.SpanByte.ToPointer();
            if (expiryExists)
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        newValue.ExtraMetadata = newExpiry;
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        o->result1 = 1;
                        break;
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        bool replace = newExpiry < oldValue.ExtraMetadata;
                        newValue.ExtraMetadata = replace ? oldValue.ExtraMetadata : newExpiry;
                        if (replace)
                            o->result1 = 0;
                        else
                            o->result1 = 1;
                        break;
                    case ExpireOption.LT:
                    case ExpireOption.XXLT:
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        replace = newExpiry > oldValue.ExtraMetadata;
                        newValue.ExtraMetadata = replace ? oldValue.ExtraMetadata : newExpiry;
                        if (replace)
                            o->result1 = 0;
                        else
                            o->result1 = 1;
                        break;
                }
            }
            else
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                    case ExpireOption.LT:   // If expiry doesn't exist, LT should treat the current expiration as infinite
                        newValue.ExtraMetadata = newExpiry;
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        o->result1 = 1;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                    case ExpireOption.XXLT:
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        o->result1 = 0;
                        break;
                }
            }
        }

        static (int, int) NormalizeRange(int start, int end, int len)
        {
            if (start >= 0 && start <= len)//start in [0,len]
            {
                if (end < 0 && (len + end) > 0)
                    return (start, len + end + 1);
                else if (end >= start)
                    return (start, end < len ? end + 1 : len);
            }
            else if (start < 0)
            {
                if (start > end) return (0, 0);
                start %= len;
                start = start >= 0 ? start : len + start;
                end = end > len ? len : (end % len);
                if (end < 0 && (len + end) > 0)
                    return (start, len + end + 1);
                else if (end >= start)
                    return (start, end < len ? (start == end ? end + 1 : end) : len);
            }
            return (0, 0);
        }

        internal static bool CheckExpiry(ref SpanByte src) => src.ExtraMetadata < DateTimeOffset.UtcNow.Ticks;

        static bool InPlaceUpdateNumber(long val, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo, int valueOffset)
        {
            var ndigits = NumUtils.CountDigits(val, out var isNegative);
            ndigits += isNegative ? 1 : 0;

            if (ndigits > value.LengthWithoutMetadata - valueOffset)
                return false;

            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
            value.ShrinkSerializedLength(ndigits + value.MetadataSize + valueOffset);
            _ = NumUtils.WriteInt64(val, value.AsSpan(valueOffset));
            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

            Debug.Assert(output.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            value.AsReadOnlySpan(valueOffset).CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = value.LengthWithoutMetadata - valueOffset;
            return true;
        }

        static bool InPlaceUpdateNumber(double val, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo, int valueOffset)
        {
            var ndigits = NumUtils.CountCharsInDouble(val, out var _, out var _, out var _);

            if (ndigits > value.LengthWithoutMetadata - valueOffset)
                return false;

            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
            value.ShrinkSerializedLength(ndigits + value.MetadataSize + valueOffset);
            _ = NumUtils.WriteDouble(val, value.AsSpan(valueOffset));
            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);

            Debug.Assert(output.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            value.AsReadOnlySpan(valueOffset).CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = value.LengthWithoutMetadata - valueOffset;
            return true;
        }

        static bool TryInPlaceUpdateNumber(ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo, long input, int valueOffset)
        {
            // Check if value contains a valid number
            int valLen = value.LengthWithoutMetadata - valueOffset;
            byte* valPtr = value.ToPointer() + valueOffset;
            if (!IsValidNumber(valLen, valPtr, output.SpanByte.AsSpan(), out var val))
                return true;

            try
            {
                checked { val += input; }
            }
            catch
            {
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return true;
            }

            return InPlaceUpdateNumber(val, ref value, ref output, ref rmwInfo, ref recordInfo, valueOffset);
        }

        static bool TryInPlaceUpdateNumber(ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo, double input, int valueOffset)
        {
            // Check if value contains a valid number
            int valLen = value.LengthWithoutMetadata - valueOffset;
            byte* valPtr = value.ToPointer() + valueOffset;
            if (!IsValidDouble(valLen, valPtr, output.SpanByte.AsSpan(), out var val))
                return true;

            val += input;

            if (!double.IsFinite(val))
            {
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return true;
            }

            return InPlaceUpdateNumber(val, ref value, ref output, ref rmwInfo, ref recordInfo, valueOffset);
        }

        static void CopyUpdateNumber(long next, ref SpanByte newValue, ref SpanByteAndMemory output, int etagIgnoredOffset)
        {
            NumUtils.WriteInt64(next, newValue.AsSpan(etagIgnoredOffset));
            newValue.AsReadOnlySpan(etagIgnoredOffset).CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = newValue.LengthWithoutMetadata - etagIgnoredOffset;
        }

        static void CopyUpdateNumber(double next, ref SpanByte newValue, ref SpanByteAndMemory output, int etagIgnoredOffset)
        {
            NumUtils.WriteDouble(next, newValue.AsSpan(etagIgnoredOffset));
            newValue.AsReadOnlySpan(etagIgnoredOffset).CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = newValue.LengthWithoutMetadata - etagIgnoredOffset;
        }

        static void CopyUpdateNumber(double next, ref SpanByte newValue, ref SpanByteAndMemory output)
        {
            NumUtils.WriteDouble(next, newValue.AsSpan());
            newValue.AsReadOnlySpan().CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = newValue.LengthWithoutMetadata;
        }

        /// <summary>
        /// Copy update from old value to new value while also validating whether oldValue is a numerical value.
        /// </summary>
        /// <param name="oldValue">Old value copying from</param>
        /// <param name="newValue">New value copying to</param>
        /// <param name="output">Output value</param>
        /// <param name="input">Parsed input value</param>
        static void TryCopyUpdateNumber(ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, long input, int etagIgnoredOffset)
        {
            newValue.ExtraMetadata = oldValue.ExtraMetadata;

            // Check if value contains a valid number
            if (!IsValidNumber(oldValue.LengthWithoutMetadata - etagIgnoredOffset, oldValue.ToPointer() + etagIgnoredOffset, output.SpanByte.AsSpan(), out var val))
            {
                // Move to tail of the log even when oldValue is alphanumeric
                // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                oldValue.CopyTo(ref newValue);
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return;
            }

            // Check operation overflow
            try
            {
                checked { val += input; }
            }
            catch
            {
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return;
            }

            // Move to tail of the log and update
            CopyUpdateNumber(val, ref newValue, ref output, etagIgnoredOffset);
        }

        /// <summary>
        /// Copy update from old value to new value while also validating whether oldValue is a numerical value.
        /// </summary>
        /// <param name="oldValue">Old value copying from</param>
        /// <param name="newValue">New value copying to</param>
        /// <param name="output">Output value</param>
        /// <param name="input">Parsed input value</param>
        /// <param name="etagIgnoredOffset">Number of bytes to skip for ignoring etag in value payload</param>
        static void TryCopyUpdateNumber(ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, double input, int etagIgnoredOffset)
        {
            newValue.ExtraMetadata = oldValue.ExtraMetadata;

            // Check if value contains a valid number
            if (!IsValidDouble(oldValue.LengthWithoutMetadata - etagIgnoredOffset, oldValue.ToPointer() + etagIgnoredOffset, output.SpanByte.AsSpan(), out var val))
            {
                // Move to tail of the log even when oldValue is alphanumeric
                // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                oldValue.CopyTo(ref newValue);
                return;
            }

            val += input;
            if (!double.IsFinite(val))
            {
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return;
            }

            // Move to tail of the log and update
            CopyUpdateNumber(val, ref newValue, ref output, etagIgnoredOffset);
        }

        /// <summary>
        /// Parse ASCII byte array into long and validate that only contains ASCII decimal characters
        /// </summary>
        /// <param name="length">Length of byte array</param>
        /// <param name="source">Pointer to byte array</param>
        /// <param name="output">Output error flag</param>
        /// <param name="val">Parsed long value</param>
        /// <returns>True if input contained only ASCII decimal characters, otherwise false</returns>
        static bool IsValidNumber(int length, byte* source, Span<byte> output, out long val)
        {
            // Check for valid number
            if (!NumUtils.TryReadInt64(length, source, out val))
            {
                // Signal value is not a valid number
                output[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }
            return true;
        }

        static bool IsValidDouble(int length, byte* source, Span<byte> output, out double val)
        {
            // Check for valid number
            if (!NumUtils.TryParseWithInfinity(new ReadOnlySpan<byte>(source, length), out val))
            {
                // Signal value is not a valid number
                output[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }

            if (!double.IsFinite(val))
            {
                // Signal value is not a Nan/Infinity
                output[0] = (byte)OperationError.NAN_OR_INFINITY;
                return false;
            }

            return true;
        }

        void CopyDefaultResp(ReadOnlySpan<byte> resp, ref SpanByteAndMemory dst)
        {
            if (resp.Length < dst.SpanByte.Length)
            {
                resp.CopyTo(dst.SpanByte.AsSpan());
                dst.SpanByte.Length = resp.Length;
                return;
            }

            dst.ConvertToHeap();
            dst.Length = resp.Length;
            dst.Memory = functionsState.memoryPool.Rent(resp.Length);
            resp.CopyTo(dst.Memory.Memory.Span);
        }

        void CopyRespNumber(long number, ref SpanByteAndMemory dst)
        {
            byte* curr = dst.SpanByte.ToPointer();
            byte* end = curr + dst.SpanByte.Length;
            if (RespWriteUtils.TryWriteInt64(number, ref curr, end, out int integerLen, out int totalLen))
            {
                dst.SpanByte.Length = (int)(curr - dst.SpanByte.ToPointer());
                return;
            }

            //handle resp buffer overflow here
            dst.ConvertToHeap();
            dst.Length = totalLen;
            dst.Memory = functionsState.memoryPool.Rent(totalLen);
            fixed (byte* ptr = dst.Memory.Memory.Span)
            {
                byte* cc = ptr;
                *cc++ = (byte)':';
                NumUtils.WriteInt64(number, integerLen, ref cc);
                *cc++ = (byte)'\r';
                *cc++ = (byte)'\n';
            }
        }

        /// <summary>
        /// Copy length of value to output (as ASCII bytes)
        /// </summary>
        static void CopyValueLengthToOutput(ref SpanByte value, ref SpanByteAndMemory output, int eTagIgnoredOffset)
        {
            int numDigits = NumUtils.CountDigits(value.LengthWithoutMetadata - eTagIgnoredOffset);

            Debug.Assert(output.IsSpanByte, "This code assumes it is called in a non-pending context or in a pending context where dst.SpanByte's pointer remains valid");
            var outputPtr = output.SpanByte.ToPointer();
            NumUtils.WriteInt32(value.LengthWithoutMetadata - eTagIgnoredOffset, numDigits, ref outputPtr);
            output.SpanByte.Length = numDigits;
        }

        static void CopyRespWithEtagData(ref SpanByte value, ref SpanByteAndMemory dst, bool hasEtagInVal, int etagSkippedStart, MemoryPool<byte> memoryPool)
        {
            int valueLength = value.LengthWithoutMetadata;
            // always writing an array of size 2 => *2\r\n
            int desiredLength = 4;
            ReadOnlySpan<byte> etagTruncatedVal;
            // get etag to write, default etag 0 for when no etag
            long etag = hasEtagInVal ? value.GetEtagInPayload() : EtagConstants.NoETag;
            // remove the length of the ETAG
            var etagAccountedValueLength = valueLength - etagSkippedStart;
            if (hasEtagInVal)
            {
                etagAccountedValueLength = valueLength - EtagConstants.EtagSize;
            }

            // here we know the value span has first bytes set to etag so we hardcode skipping past the bytes for the etag below
            etagTruncatedVal = value.AsReadOnlySpan(etagSkippedStart);
            // *2\r\n :(etag digits)\r\n $(val Len digits)\r\n (value len)\r\n
            desiredLength += 1 + NumUtils.CountDigits(etag) + 2 + 1 + NumUtils.CountDigits(etagAccountedValueLength) + 2 + etagAccountedValueLength + 2;

            WriteValAndEtagToDst(desiredLength, ref etagTruncatedVal, etag, ref dst, memoryPool);
        }

        static void WriteValAndEtagToDst(int desiredLength, ref ReadOnlySpan<byte> value, long etag, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool, bool writeDirect = false)
        {
            if (desiredLength <= dst.Length)
            {
                dst.Length = desiredLength;
                byte* curr = dst.SpanByte.ToPointer();
                byte* end = curr + dst.SpanByte.Length;
                RespWriteUtils.WriteEtagValArray(etag, ref value, ref curr, end, writeDirect);
                return;
            }

            dst.ConvertToHeap();
            dst.Length = desiredLength;
            dst.Memory = memoryPool.Rent(desiredLength);
            fixed (byte* ptr = dst.Memory.Memory.Span)
            {
                byte* curr = ptr;
                byte* end = ptr + desiredLength;
                RespWriteUtils.WriteEtagValArray(etag, ref value, ref curr, end, writeDirect);
            }
        }

        /// <summary>
        /// Logging upsert from
        /// a. ConcurrentWriter
        /// b. PostSingleWriter
        /// </summary>
        void WriteLogUpsert(ref SpanByte key, ref RawStringInput input, ref SpanByte value, long version, int sessionId)
        {
            if (functionsState.StoredProcMode) return;

            if (input.header.cmd == RespCommand.VADD && input.arg1 is not (VectorManager.VADDAppendLogArg or VectorManager.MigrateElementKeyLogArg or VectorManager.MigrateIndexKeyLogArg))
            {
                return;
            }

            // We need this check because when we ingest records from the primary
            // if the input is zero then input overlaps with value so any update to RespInputHeader->flags
            // will incorrectly modify the total length of value.
            if (input.SerializedLength > 0)
                input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = version, sessionID = sessionId },
                ref key, ref value, ref input, out _);
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ref SpanByte key, ref RawStringInput input, long version, int sessionId)
        {
            if (functionsState.StoredProcMode) return;

            if (input.header.cmd == RespCommand.VADD && input.arg1 is not (VectorManager.VADDAppendLogArg or VectorManager.MigrateElementKeyLogArg or VectorManager.MigrateIndexKeyLogArg))
            {
                return;
            }

            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.StoreRMW, storeVersion = version, sessionID = sessionId },
                ref key, ref input, out _);
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. ConcurrentDeleter
        ///  b. PostSingleDeleter
        /// </summary>
        void WriteLogDelete(ref SpanByte key, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            SpanByte def = default;
            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.StoreDelete, storeVersion = version, sessionID = sessionID }, ref key, ref def, out _);
        }

        BitFieldCmdArgs GetBitFieldArguments(ref RawStringInput input)
        {
            var currTokenIdx = 0;

            // Get secondary command. Legal commands: GET, SET & INCRBY.
            var cmd = RespCommand.NONE;
            var sbCmd = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;
            if (sbCmd.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GET))
                cmd = RespCommand.GET;
            else if (sbCmd.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SET))
                cmd = RespCommand.SET;
            else if (sbCmd.EqualsUpperCaseSpanIgnoringCase(CmdStrings.INCRBY))
                cmd = RespCommand.INCRBY;

            var bitfieldEncodingParsed = input.parseState.TryGetBitfieldEncoding(
                                                currTokenIdx++, out var bitCount, out var isSigned);
            Debug.Assert(bitfieldEncodingParsed);
            var sign = isSigned ? (byte)BitFieldSign.SIGNED : (byte)BitFieldSign.UNSIGNED;

            // Calculate number offset from bitCount if offsetArg starts with #
            var offsetParsed = input.parseState.TryGetBitfieldOffset(currTokenIdx++, out var offset, out var multiplyOffset);
            Debug.Assert(offsetParsed);
            if (multiplyOffset)
                offset *= bitCount;

            long value = default;
            if (cmd == RespCommand.SET || cmd == RespCommand.INCRBY)
            {
                value = input.parseState.GetLong(currTokenIdx++);
            }

            var overflowType = (byte)BitFieldOverflow.WRAP;
            if (currTokenIdx < input.parseState.Count)
            {
                var overflowTypeParsed = input.parseState.TryGetBitFieldOverflow(currTokenIdx, out var overflowTypeValue);
                Debug.Assert(overflowTypeParsed);
                overflowType = (byte)overflowTypeValue;
            }

            // Number of bits in signed number
            // At most 64 bits can fit into encoding info
            var typeInfo = (byte)(sign | bitCount);

            return new BitFieldCmdArgs(cmd, typeInfo, offset, value, overflowType);
        }
    }
}