// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainStoreFunctions : IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long>
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

            var numLength = NumUtils.NumDigits(srcLength);
            int totalSize = 1 + numLength + 2 + srcLength + 2; // $5\r\nvalue\r\n

            if (dst.IsSpanByte)
            {
                if (dst.Length >= totalSize)
                {
                    dst.Length = totalSize;

                    byte* tmp = dst.SpanByte.ToPointer();
                    *tmp++ = (byte)'$';
                    NumUtils.IntToBytes(srcLength, numLength, ref tmp);
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
                NumUtils.IntToBytes(srcLength, numLength, ref tmp);
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';
                src.AsReadOnlySpan().Slice(start, srcLength).CopyTo(new Span<byte>(tmp, srcLength));
                tmp += srcLength;
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';
            }
        }

        void CopyRespToWithInput(ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
        {
            var inputPtr = input.ToPointer();
            switch ((RespCommand)(*inputPtr))
            {
                case RespCommand.MIGRATE:
                    long expiration = value.ExtraMetadata;
                    if (value.Length <= dst.Length)
                    {
                        value.CopyTo(ref dst.SpanByte);
                        dst.Length = value.Length;
                        return;
                    }

                    dst.ConvertToHeap();
                    dst.Length = value.Length;
                    dst.Memory = functionsState.memoryPool.Rent(value.Length);
                    value.AsReadOnlySpanWithMetadata().CopyTo(dst.Memory.Memory.Span);
                    break;

                case RespCommand.GET:
                    // Get value without RESP header; exclude expiration
                    if (value.LengthWithoutMetadata <= dst.Length)
                    {
                        dst.Length = value.LengthWithoutMetadata;
                        value.AsReadOnlySpan().CopyTo(dst.SpanByte.AsSpan());
                        return;
                    }

                    dst.ConvertToHeap();
                    dst.Length = value.LengthWithoutMetadata;
                    dst.Memory = functionsState.memoryPool.Rent(value.LengthWithoutMetadata);
                    value.AsReadOnlySpan().CopyTo(dst.Memory.Memory.Span);
                    break;

                case RespCommand.GETBIT:
                    byte oldValSet = BitmapManager.GetBit(inputPtr + RespInputHeader.Size, value.ToPointer(), value.Length);
                    if (oldValSet == 0)
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref dst);
                    else
                        CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref dst);
                    break;

                case RespCommand.BITCOUNT:
                    long count = BitmapManager.BitCountDriver(inputPtr + RespInputHeader.Size, value.ToPointer(), value.Length);
                    CopyRespNumber(count, ref dst);
                    break;

                case RespCommand.BITPOS:
                    long pos = BitmapManager.BitPosDriver(inputPtr + RespInputHeader.Size, value.ToPointer(), value.Length);
                    *(long*)dst.SpanByte.ToPointer() = pos;
                    CopyRespNumber(pos, ref dst);
                    break;

                case RespCommand.BITOP:
                    IntPtr bitmap = (IntPtr)value.ToPointer();
                    byte* output = dst.SpanByte.ToPointer();

                    *(long*)output = (long)bitmap.ToInt64();
                    *(int*)(output + 8) = value.Length;

                    return;

                case RespCommand.BITFIELD:
                    long retValue = 0;
                    bool overflow;
                    (retValue, overflow) = BitmapManager.BitFieldExecute(inputPtr + RespInputHeader.Size, value.ToPointer(), value.Length);
                    if (!overflow)
                        CopyRespNumber(retValue, ref dst);
                    else
                        CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref dst);
                    return;

                case RespCommand.PFCOUNT:
                    if (!HyperLogLog.DefaultHLL.IsValidHYLL(value.ToPointer(), value.Length))
                    {
                        *(long*)dst.SpanByte.ToPointer() = -1;
                        return;
                    }

                    long E = 13;
                    E = HyperLogLog.DefaultHLL.Count(value.ToPointer());
                    *(long*)dst.SpanByte.ToPointer() = E;
                    return;

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
                    throw new GarnetException("Not enough space in PFMERGE buffer");

                case RespCommand.TTL:
                    long ttlValue = ConvertUtils.SecondsFromDiffUtcNowTicks(value.MetadataSize > 0 ? value.ExtraMetadata : -1);
                    CopyRespNumber(ttlValue, ref dst);
                    return;

                case RespCommand.PTTL:
                    long pttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(value.MetadataSize > 0 ? value.ExtraMetadata : -1);
                    CopyRespNumber(pttlValue, ref dst);
                    return;

                case RespCommand.GETRANGE:
                    int len = value.LengthWithoutMetadata;
                    int start = *(int*)(inputPtr + RespInputHeader.Size);
                    int end = *(int*)(inputPtr + RespInputHeader.Size + 4);

                    (start, end) = NormalizeRange(start, end, len);
                    CopyRespTo(ref value, ref dst, start, end);
                    return;
                default:
                    throw new GarnetException("Unsupported operation on input");
            }
        }

        bool EvaluateExpireInPlace(ExpireOption optionType, bool expiryExists, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output)
        {
            ObjectOutputHeader* o = (ObjectOutputHeader*)output.SpanByte.ToPointer();
            if (expiryExists)
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                        o->countDone = 0;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        value.ExtraMetadata = input.ExtraMetadata;
                        o->countDone = 1;
                        break;
                    case ExpireOption.GT:
                        bool replace = input.ExtraMetadata < value.ExtraMetadata;
                        value.ExtraMetadata = replace ? value.ExtraMetadata : input.ExtraMetadata;
                        if (replace)
                            o->countDone = 0;
                        else
                            o->countDone = 1;
                        break;
                    case ExpireOption.LT:
                        replace = input.ExtraMetadata > value.ExtraMetadata;
                        value.ExtraMetadata = replace ? value.ExtraMetadata : input.ExtraMetadata;
                        if (replace)
                            o->countDone = 0;
                        else
                            o->countDone = 1;
                        break;
                    default:
                        throw new GarnetException($"EvaluateExpireInPlace exception expiryExists:{expiryExists}, optionType{optionType}");
                }
                return true;
            }
            else
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                        return false;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.LT:
                        o->countDone = 0;
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireInPlace exception expiryExists:{expiryExists}, optionType{optionType}");
                }
            }
        }

        void EvaluateExpireCopyUpdate(ExpireOption optionType, bool expiryExists, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output)
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
                        newValue.ExtraMetadata = input.ExtraMetadata;
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        o->countDone = 1;
                        break;
                    case ExpireOption.GT:
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        bool replace = input.ExtraMetadata < oldValue.ExtraMetadata;
                        newValue.ExtraMetadata = replace ? oldValue.ExtraMetadata : input.ExtraMetadata;
                        if (replace)
                            o->countDone = 0;
                        else
                            o->countDone = 1;
                        break;
                    case ExpireOption.LT:
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        replace = input.ExtraMetadata > oldValue.ExtraMetadata;
                        newValue.ExtraMetadata = replace ? oldValue.ExtraMetadata : input.ExtraMetadata;
                        if (replace)
                            o->countDone = 0;
                        else
                            o->countDone = 1;
                        break;
                }
            }
            else
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                        newValue.ExtraMetadata = input.ExtraMetadata;
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        o->countDone = 1;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.LT:
                        oldValue.AsReadOnlySpan().CopyTo(newValue.AsSpan());
                        o->countDone = 0;
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

        static bool CheckExpiry(ref SpanByte src) => src.ExtraMetadata < DateTimeOffset.UtcNow.Ticks;

        static bool InPlaceUpdateNumber(long val, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            bool fNeg = false;
            int ndigits = NumUtils.NumDigitsInLong(val, ref fNeg);
            ndigits += fNeg ? 1 : 0;

            if (ndigits > value.Length)
                return false;

            rmwInfo.ClearExtraValueLength(ref recordInfo, ref value, value.TotalSize);
            value.ShrinkSerializedLength(ndigits + value.MetadataSize);
            NumUtils.LongToSpanByte(val, value.AsSpan());
            rmwInfo.SetUsedValueLength(ref recordInfo, ref value, value.TotalSize);
            value.AsReadOnlySpan().CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = value.LengthWithoutMetadata;
            return true;
        }

        static void CopyUpdateNumber(long next, ref SpanByte newValue, ref SpanByteAndMemory output)
        {
            NumUtils.LongToSpanByte(next, newValue.AsSpan());
            newValue.AsReadOnlySpan().CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = newValue.LengthWithoutMetadata;
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
            if (RespWriteUtils.WriteInteger(number, ref curr, end, out int integerLen, out int totalLen))
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
                NumUtils.LongToBytes(number, integerLen, ref cc);
                *cc++ = (byte)'\r';
                *cc++ = (byte)'\n';
            }
        }

        /// <summary>
        /// Copy length of value to output (as ASCII bytes)
        /// </summary>
        void CopyValueLengthToOutput(ref SpanByte value, ref SpanByteAndMemory output)
        {
            int numDigits = NumUtils.NumDigits(value.LengthWithoutMetadata);
            var outputPtr = output.SpanByte.ToPointer();
            NumUtils.IntToBytes(value.LengthWithoutMetadata, numDigits, ref outputPtr);
            output.SpanByte.Length = numDigits;
        }

        /// <summary>
        /// Logging upsert from
        /// a. ConcurrentWriter
        /// b. PostSingleWriter
        /// </summary>
        void WriteLogUpsert(ref SpanByte key, ref SpanByte input, ref SpanByte value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;

            //We need this check because when we ingest records from the primary
            //if the input is zero then input overlaps with value so any update to RespInputHeader->flags
            //will incorrectly modify the total length of value.
            if (input.Length > 0)
                ((RespInputHeader*)input.ToPointer())->flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.StoreUpsert, version = version, sessionID = sessionID }, ref key, ref input, ref value, out _);
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ref SpanByte key, ref SpanByte input, ref SpanByte value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;

            ((RespInputHeader*)input.ToPointer())->flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.StoreRMW, version = version, sessionID = sessionID }, ref key, ref input, out _);
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
            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.StoreDelete, version = version, sessionID = sessionID }, ref key, ref def, out _);
        }
    }
}