// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        static void CopyTo(SpanByte src, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
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

        void CopyRespTo(SpanByte src, ref SpanByteAndMemory dst, int start = 0, int end = -1)
        {
            int srcLength = end == -1 ? src.LengthWithoutMetadata : ((start < end) ? (end - start) : 0);
            if (srcLength == 0)
            {
                functionsState.CopyDefaultResp(CmdStrings.RESP_EMPTY, ref dst);
                return;
            }

            var numLength = NumUtils.NumDigits(srcLength);
            var totalSize = 1 + numLength + 2 + srcLength + 2; // $5\r\nvalue\r\n

            if (dst.IsSpanByte)
            {
                if (dst.Length >= totalSize)
                {
                    dst.Length = totalSize;

                    var tmp = dst.SpanByte.ToPointer();
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
                var tmp = ptr;
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

        void CopyRespToWithInput(ref RawStringInput input, SpanByte value, ref SpanByteAndMemory dst, bool isFromPending)
        {
            switch (input.header.cmd)
            {
                case RespCommand.ASYNC:
                    // If the GET is expected to complete continuations asynchronously, we should not write anything
                    // to the network buffer in case the operation does go pending (latter is indicated by isFromPending)
                    // This is accomplished by calling ConvertToHeap on the destination SpanByteAndMemory
                    if (isFromPending)
                        dst.ConvertToHeap();
                    CopyRespTo(value, ref dst);
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
                    var offset = input.parseState.GetLong(0);
                    var oldValSet = BitmapManager.GetBit(offset, value.ToPointer(), value.Length);
                    if (oldValSet == 0)
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref dst);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref dst);
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

                    var count = BitmapManager.BitCountDriver(bcStartOffset, bcEndOffset, bcOffsetType, value.ToPointer(), value.Length);
                    functionsState.CopyRespNumber(count, ref dst);
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

                    var pos = BitmapManager.BitPosDriver(bpSetVal, bpStartOffset, bpEndOffset, bpOffsetType,
                        value.ToPointer(), value.Length);
                    *(long*)dst.SpanByte.ToPointer() = pos;
                    functionsState.CopyRespNumber(pos, ref dst);
                    break;

                case RespCommand.BITOP:
                    var bitmap = (IntPtr)value.ToPointer();
                    var output = dst.SpanByte.ToPointer();

                    *(long*)output = bitmap.ToInt64();
                    *(int*)(output + 8) = value.Length;

                    return;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);
                    var (retValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, value.ToPointer(), value.Length);
                    if (!overflow)
                        functionsState.CopyRespNumber(retValue, ref dst);
                    else
                        functionsState.CopyDefaultResp(CmdStrings.RESP_ERRNOTFOUND, ref dst);
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
                    var ttlValue = ConvertUtils.SecondsFromDiffUtcNowTicks(value.MetadataSize > 0 ? value.ExtraMetadata : -1);
                    functionsState.CopyRespNumber(ttlValue, ref dst);
                    return;

                case RespCommand.PTTL:
                    var pttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(value.MetadataSize > 0 ? value.ExtraMetadata : -1);
                    functionsState.CopyRespNumber(pttlValue, ref dst);
                    return;

                case RespCommand.GETRANGE:
                    var len = value.LengthWithoutMetadata;
                    var start = input.parseState.GetInt(0);
                    var end = input.parseState.GetInt(1);

                    (start, end) = NormalizeRange(start, end, len);
                    CopyRespTo(value, ref dst, start, end);
                    return;

                case RespCommand.EXPIRETIME:
                    var expireTime = ConvertUtils.UnixTimeInSecondsFromTicks(value.MetadataSize > 0 ? value.ExtraMetadata : -1);
                    functionsState.CopyRespNumber(expireTime, ref dst);
                    return;

                case RespCommand.PEXPIRETIME:
                    var pexpireTime = ConvertUtils.UnixTimeInMillisecondsFromTicks(value.MetadataSize > 0 ? value.ExtraMetadata : -1);
                    functionsState.CopyRespNumber(pexpireTime, ref dst);
                    return;

                default:
                    throw new GarnetException("Unsupported operation on input");
            }
        }

        bool EvaluateExpireInPlace(ref LogRecord<SpanByte> logRecord, ExpireOption optionType, long newExpiry, ref SpanByteAndMemory output)
        {
            var o = (ObjectOutputHeader*)output.SpanByte.ToPointer();
            o->result1 = 0;
            if (logRecord.Info.HasExpiration)
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                        return true;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        _ = logRecord.TrySetExpiration(newExpiry);
                        o->result1 = 1;
                        return true;
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                        if (newExpiry > logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            o->result1 = 1;
                        }
                        return true;
                    case ExpireOption.LT:
                    case ExpireOption.XXLT:
                        if (newExpiry < logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            o->result1 = 1;
                        }
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireInPlace exception expiryExists: True, optionType {optionType}");
                }
            }
            else
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                    case ExpireOption.LT:  // If expiry doesn't exist, LT should treat the current expiration as infinite, so the new value must be less
                        var ok = logRecord.TrySetExpiration(newExpiry);
                        o->result1 = 1;
                        return ok;
                    case ExpireOption.XX:
                    case ExpireOption.GT:  // If expiry doesn't exist, GT should treat the current expiration as infinite, so the new value cannot be greater
                    case ExpireOption.XXGT:
                    case ExpireOption.XXLT:
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireInPlace exception expiryExists: False, optionType {optionType}");
                }
            }
        }

        bool EvaluateExpireCopyUpdate(ref LogRecord<SpanByte> logRecord, ExpireOption optionType, long newExpiry, SpanByte newValue, ref SpanByteAndMemory output)
        {
            var expiryExists = logRecord.Info.HasExpiration;
            var o = (ObjectOutputHeader*)output.SpanByte.ToPointer();
            o->result1 = 0;

            if (!logRecord.TrySetValueSpan(newValue))
            {
                functionsState.logger?.LogError("Failed to set value in {methodName}", "EvaluateExpireCopyUpdate");
                return false;
            }
            if (expiryExists)
            {
                // Expiration already exists so there is no need to check for space (i.e. failure of TrySetExpiration)
                switch (optionType)
                {
                    case ExpireOption.NX:
                        return true;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        _ = logRecord.TrySetExpiration(newExpiry);
                        o->result1 = 1;
                        return true;
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                        if (newExpiry > logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            o->result1 = 1;
                        }
                        return true;
                    case ExpireOption.LT:
                    case ExpireOption.XXLT:
                        if (newExpiry < logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            o->result1 = 1;
                        }
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireCopyUpdate exception expiryExists:{expiryExists}, optionType{optionType}");
                }
            }
            else
            {
                // No expiration yet. Because this is CopyUpdate we should already have verified the space, but check anyway
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                    case ExpireOption.LT:   // If expiry doesn't exist, LT should treat the current expiration as infinite
                        if (!logRecord.TrySetExpiration(newExpiry))
                        {
                            functionsState.logger?.LogError("Failed to add expiration in {methodName}.{caseName}", "EvaluateExpireCopyUpdate", "LT");
                            return false;
                        }
                        o->result1 = 1;
                        return true;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                    case ExpireOption.XXLT:
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireCopyUpdate exception expiryExists:{expiryExists}, optionType{optionType}");
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

        internal static bool CheckExpiry<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
            => srcLogRecord.Info.HasExpiration && srcLogRecord.Expiration < DateTimeOffset.UtcNow.Ticks;

        static bool InPlaceUpdateNumber(ref LogRecord<SpanByte> logRecord, long val, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var fNeg = false;
            var ndigits = NumUtils.NumDigitsInLong(val, ref fNeg);
            ndigits += fNeg ? 1 : 0;

            if (!logRecord.TrySetValueSpanLength(ndigits))
                return false;

            ref var valueRef = ref logRecord.ValueSpanRef;  // To eliminate redundant length calculations getting to Value
            _ = NumUtils.LongToSpanByte(val, valueRef.AsSpan());

            Debug.Assert(output.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            valueRef.AsReadOnlySpan().CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = valueRef.Length;
            return true;
        }

        static bool InPlaceUpdateNumber(ref LogRecord<SpanByte> logRecord, double val, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var ndigits = NumUtils.NumOfCharInDouble(val, out var _, out var _, out var _);

            if (!logRecord.TrySetValueSpanLength(ndigits))
                return false;

            ref var valueRef = ref logRecord.ValueSpanRef;  // To reduce redundant length calculations getting to Value
            _ = NumUtils.DoubleToSpanByte(val, valueRef.AsSpan());

            Debug.Assert(output.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            valueRef.AsReadOnlySpan().CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = valueRef.LengthWithoutMetadata;
            return true;
        }

        static bool TryInPlaceUpdateNumber(ref LogRecord<SpanByte> logRecord, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, long input)
        {
            // Check if value contains a valid number
            var value = logRecord.ValueSpan;  // To reduce redundant length calculations getting to Value
            if (!IsValidNumber(value.Length, value.ToPointer(), output.SpanByte.AsSpan(), out var val))
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

            return InPlaceUpdateNumber(ref logRecord, val, ref output, ref rmwInfo);
        }

        static bool TryInPlaceUpdateNumber(ref LogRecord<SpanByte> logRecord, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, double input)
        {
            var value = logRecord.ValueSpan;  // To reduce redundant length calculations getting to Value

            // Check if value contains a valid number
            if (!IsValidDouble(value.LengthWithoutMetadata, value.ToPointer(), output.SpanByte.AsSpan(), out var val))
                return true;

            val += input;

            if (!double.IsFinite(val))
            {
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return true;
            }

            return InPlaceUpdateNumber(ref logRecord, val, ref output, ref rmwInfo);
        }

        static bool TryCopyUpdateNumber(long next, ref SpanByte newValue, ref SpanByteAndMemory output)
        {
            if (NumUtils.LongToSpanByte(next, newValue.AsSpan()) == 0)
                return false;
            newValue.AsReadOnlySpan().CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = newValue.Length;
            return true;
        }

        static bool TryCopyUpdateNumber(double next, ref SpanByte newValue, ref SpanByteAndMemory output)
        {
            if (NumUtils.DoubleToSpanByte(next, newValue.AsSpan()) == 0)
                return false;
            newValue.AsReadOnlySpan().CopyTo(output.SpanByte.AsSpan());
            output.SpanByte.Length = newValue.Length;
            return true;
        }

        /// <summary>
        /// Copy update from old 'long' value to new value while also validating whether oldValue is a numerical value.
        /// </summary>
        /// <param name="srcLogRecord">The source log record, either in-memory or from disk</param>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="output">Output value</param>
        /// <param name="input">Parsed input value</param>
        static bool TryCopyUpdateNumber<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref SpanByteAndMemory output, long input)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            if (!dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                return false;

            var srcValue = srcLogRecord.ValueSpan;  // To reduce redundant length calculations getting to ValueSpan

            // Check if value contains a valid number
            if (!IsValidNumber(srcValue.LengthWithoutMetadata, srcValue.ToPointer(), output.SpanByte.AsSpan(), out var val))
            {
                // Move to tail of the log even when oldValue is alphanumeric
                // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                return dstLogRecord.TrySetValueSpan(srcLogRecord.ValueSpan);
            }

            // Check operation overflow
            try
            {
                checked { val += input; }
            }
            catch
            {
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }

            // Move to tail of the log and update
            return TryCopyUpdateNumber(val, ref dstLogRecord.ValueSpanRef, ref output);
        }

        /// <summary>
        /// Copy update from old 'double' value to new value while also validating whether oldValue is a numerical value.
        /// </summary>
        /// <param name="srcLogRecord">The source log record, either in-memory or from disk</param>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="output">Output value</param>
        /// <param name="input">Parsed input value</param>
        static bool TryCopyUpdateNumber<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref SpanByteAndMemory output, double input)
            where TSourceLogRecord : ISourceLogRecord<SpanByte>
        {
            if (!dstLogRecord.TrySetExpiration(srcLogRecord.Expiration))
                return false;

            var srcValue = srcLogRecord.ValueSpan;  // To reduce redundant length calculations getting to ValueSpan

            // Check if value contains a valid number
            if (!IsValidDouble(srcValue.LengthWithoutMetadata, srcValue.ToPointer(), output.SpanByte.AsSpan(), out var val))
            {
                // Move to tail of the log even when oldValue is alphanumeric
                // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                return dstLogRecord.TrySetValueSpan(srcLogRecord.ValueSpan);
            }

            val += input;
            if (!double.IsFinite(val))
            {
                output.SpanByte.AsSpan()[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }

            // Move to tail of the log and update
            return TryCopyUpdateNumber(val, ref dstLogRecord.ValueSpanRef, ref output);
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
            val = 0;
            try
            {
                // Check for valid number
                if (!NumUtils.TryBytesToLong(length, source, out val))
                {
                    // Signal value is not a valid number
                    output[0] = (byte)OperationError.INVALID_TYPE;
                    return false;
                }
            }
            catch
            {
                // Signal value is not a valid number
                output[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }
            return true;
        }

        static bool IsValidDouble(int length, byte* source, Span<byte> output, out double val)
        {
            val = 0;
            try
            {
                // Check for valid number
                if (!NumUtils.TryBytesToDouble(length, source, out val) || !double.IsFinite(val))
                {
                    // Signal value is not a valid number
                    output[0] = (byte)OperationError.INVALID_TYPE;
                    return false;
                }
            }
            catch
            {
                // Signal value is not a valid number
                output[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }
            return true;
        }

        /// <summary>
        /// Copy length of value to output (as ASCII bytes)
        /// </summary>
        static bool CopyValueLengthToOutput(SpanByte value, ref SpanByteAndMemory output)
        {
            Debug.Assert(output.IsSpanByte, "This code assumes it is called in a non-pending context or in a pending context where dst.SpanByte's pointer remains valid");

            var numDigits = NumUtils.NumDigits(value.Length);
            if (numDigits > output.SpanByte.Length)
            {
                Debug.Fail("Output length overflow in CopyValueLengthToOutput");
                return false;
            }

            var outputPtr = output.SpanByte.ToPointer();
            NumUtils.IntToBytes(value.Length, numDigits, ref outputPtr);
            output.SpanByte.Length = numDigits;
            return true;
        }

        /// <summary>
        /// Logging upsert from
        /// a. ConcurrentWriter
        /// b. PostSingleWriter
        /// </summary>
        void WriteLogUpsert(SpanByte key, ref RawStringInput input, ref SpanByte value, long version, int sessionId)
        {
            if (functionsState.StoredProcMode) return;

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
        void WriteLogRMW(SpanByte key, ref RawStringInput input, long version, int sessionId)
        {
            if (functionsState.StoredProcMode) return;
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
        void WriteLogDelete(SpanByte key, long version, int sessionID)
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
            if (sbCmd.EqualsUpperCaseSpanIgnoringCase("GET"u8))
                cmd = RespCommand.GET;
            else if (sbCmd.EqualsUpperCaseSpanIgnoringCase("SET"u8))
                cmd = RespCommand.SET;
            else if (sbCmd.EqualsUpperCaseSpanIgnoringCase("INCRBY"u8))
                cmd = RespCommand.INCRBY;

            var encodingArg = input.parseState.GetString(currTokenIdx++);
            var offsetArg = input.parseState.GetString(currTokenIdx++);

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

            var sign = encodingArg[0] == 'i' ? (byte)BitFieldSign.SIGNED : (byte)BitFieldSign.UNSIGNED;
            // Number of bits in signed number
            var bitCount = (byte)int.Parse(encodingArg.AsSpan(1));
            // At most 64 bits can fit into encoding info
            var typeInfo = (byte)(sign | bitCount);

            // Calculate number offset from bitCount if offsetArg starts with #
            var offset = offsetArg[0] == '#' ? long.Parse(offsetArg.AsSpan(1)) * bitCount : long.Parse(offsetArg);

            return new BitFieldCmdArgs(cmd, typeInfo, offset, value, overflowType);
        }
    }
}