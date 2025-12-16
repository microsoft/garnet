// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, SpanByteAndMemory, long>
    {
        static void CopyTo(ReadOnlySpan<byte> src, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            int srcLength = src.Length;

            if (dst.IsSpanByte)
            {
                if (dst.Length >= srcLength)
                {
                    dst.Length = srcLength;
                    src.CopyTo(dst.SpanByte.Span);
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = memoryPool.Rent(srcLength);
            dst.Length = srcLength;
            src.CopyTo(dst.MemorySpan);
        }

        void CopyRespTo(ReadOnlySpan<byte> src, ref SpanByteAndMemory dst, int start = 0, int end = -1)
        {
            int srcLength = end == -1 ? src.Length : ((start < end) ? (end - start) : 0);
            if (srcLength == 0)
            {
                functionsState.CopyDefaultResp(CmdStrings.RESP_EMPTY, ref dst);
                return;
            }

            var numLength = NumUtils.CountDigits(srcLength);
            var totalSize = 1 + numLength + 2 + srcLength + 2; // $5\r\nvalue\r\n

            if (dst.IsSpanByte)
            {
                if (dst.Length >= totalSize)
                {
                    dst.Length = totalSize;

                    var tmp = dst.SpanByte.ToPointer();
                    *tmp++ = (byte)'$';
                    NumUtils.WriteInt32(srcLength, numLength, ref tmp);
                    *tmp++ = (byte)'\r';
                    *tmp++ = (byte)'\n';
                    src.Slice(start, srcLength).CopyTo(new Span<byte>(tmp, srcLength));
                    tmp += srcLength;
                    *tmp++ = (byte)'\r';
                    *tmp++ = (byte)'\n';
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = functionsState.memoryPool.Rent(totalSize);
            dst.Length = totalSize;
            fixed (byte* ptr = dst.MemorySpan)
            {
                var tmp = ptr;
                *tmp++ = (byte)'$';
                NumUtils.WriteInt32(srcLength, numLength, ref tmp);
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';
                src.Slice(start, srcLength).CopyTo(new Span<byte>(tmp, srcLength));
                tmp += srcLength;
                *tmp++ = (byte)'\r';
                *tmp++ = (byte)'\n';
            }
        }

        void CopyRespToWithInput<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref SpanByteAndMemory output, bool isFromPending)
            where TSourceLogRecord : ISourceLogRecord
        {
            var value = srcLogRecord.ValueSpan;

            switch (input.header.cmd)
            {
                case RespCommand.ASYNC:
                    // If the GET is expected to complete continuations asynchronously, we should not write anything
                    // to the network buffer in case the operation does go pending (latter is indicated by isFromPending)
                    // This is accomplished by calling ConvertToHeap on the destination SpanByteAndMemory
                    if (isFromPending)
                        output.ConvertToHeap();
                    CopyRespTo(value, ref output);
                    break;

                case RespCommand.GET:
                    // Get value without RESP header; exclude expiration
                    if (value.Length <= output.Length)
                    {
                        output.Length = value.Length;
                        value.CopyTo(output.SpanByte.Span);
                        return;
                    }

                    output.ConvertToHeap();
                    output.Length = value.Length;
                    output.Memory = functionsState.memoryPool.Rent(value.Length);
                    value.CopyTo(output.MemorySpan);
                    break;

                case RespCommand.GETBIT:
                    var offset = input.arg1;
                    byte oldValSet;

                    if (srcLogRecord.IsPinnedValue)
                        oldValSet = BitmapManager.GetBit(offset, srcLogRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            oldValSet = BitmapManager.GetBit(offset, valuePtr, value.Length);

                    functionsState.CopyDefaultResp(
                        oldValSet == 0 ? CmdStrings.RESP_RETURN_VAL_0 : CmdStrings.RESP_RETURN_VAL_1, ref output);
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

                    long count;
                    if (srcLogRecord.IsPinnedValue)
                        count = BitmapManager.BitCountDriver(bcStartOffset, bcEndOffset, bcOffsetType, srcLogRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            count = BitmapManager.BitCountDriver(bcStartOffset, bcEndOffset, bcOffsetType, valuePtr, value.Length);

                    functionsState.CopyRespNumber(count, ref output);
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

                    long pos;
                    if (srcLogRecord.IsPinnedValue)
                        pos = BitmapManager.BitPosDriver(input: srcLogRecord.PinnedValuePointer, inputLen: value.Length, startOffset: bpStartOffset,
                                endOffset: bpEndOffset, searchFor: bpSetVal, offsetType: bpOffsetType);
                    else
                        fixed (byte* valuePtr = value)
                            pos = BitmapManager.BitPosDriver(input: valuePtr, inputLen: value.Length, startOffset: bpStartOffset,
                                endOffset: bpEndOffset, searchFor: bpSetVal, offsetType: bpOffsetType);

                    *(long*)output.SpanByte.ToPointer() = pos;
                    functionsState.CopyRespNumber(pos, ref output);
                    break;

                case RespCommand.BITOP:
                    var outPtr = output.SpanByte.ToPointer();

                    if (srcLogRecord.IsPinnedValue)
                        *(long*)outPtr = ((IntPtr)srcLogRecord.PinnedValuePointer).ToInt64();
                    else
                        fixed (byte* valuePtr = value)
                            *(long*)outPtr = ((IntPtr)valuePtr).ToInt64();

                    *(int*)(outPtr + sizeof(long)) = value.Length;
                    return;

                case RespCommand.BITFIELD:
                    var bitFieldArgs = GetBitFieldArguments(ref input);

                    long retValue;
                    bool overflow;
                    if (srcLogRecord.IsPinnedValue)
                        (retValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, srcLogRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            (retValue, overflow) = BitmapManager.BitFieldExecute(bitFieldArgs, valuePtr, value.Length);

                    if (!overflow)
                        functionsState.CopyRespNumber(retValue, ref output);
                    else
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output);
                    return;

                case RespCommand.BITFIELD_RO:
                    var bitFieldArgs_RO = GetBitFieldArguments(ref input);

                    long retValue_RO;
                    if (srcLogRecord.IsPinnedValue)
                        retValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO, srcLogRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            retValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO, valuePtr, value.Length);

                    functionsState.CopyRespNumber(retValue_RO, ref output);
                    return;

                case RespCommand.PFCOUNT:
                case RespCommand.PFMERGE:
                    bool isValid;
                    if (srcLogRecord.IsPinnedValue)
                    {
                        isValid = HyperLogLog.DefaultHLL.IsValidHYLL(srcLogRecord.PinnedValuePointer, value.Length);
                        if (isValid)
                            Buffer.MemoryCopy(srcLogRecord.PinnedValuePointer, output.SpanByte.ToPointer(), value.Length, value.Length);
                    }
                    else
                    {
                        fixed (byte* valuePtr = value)
                        {
                            isValid = HyperLogLog.DefaultHLL.IsValidHYLL(valuePtr, value.Length);
                            if (isValid)
                                Buffer.MemoryCopy(valuePtr, output.SpanByte.ToPointer(), value.Length, value.Length);
                        }
                    }

                    if (!isValid)
                    {
                        *(long*)output.SpanByte.ToPointer() = -1;
                        return;
                    }

                    if (value.Length <= output.Length)
                    {
                        output.SpanByte.Length = value.Length;
                        return;
                    }

                    throw new GarnetException($"Not enough space in {input.header.cmd} buffer");

                case RespCommand.TTL:
                    var ttlValue = ConvertUtils.SecondsFromDiffUtcNowTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                    functionsState.CopyRespNumber(ttlValue, ref output);
                    return;

                case RespCommand.PTTL:
                    var pttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                    functionsState.CopyRespNumber(pttlValue, ref output);
                    return;

                case RespCommand.GETRANGE:
                    var len = value.Length;
                    var start = input.parseState.GetInt(0);
                    var end = input.parseState.GetInt(1);

                    (start, end) = NormalizeRange(start, end, len);
                    CopyRespTo(value, ref output, start, end);
                    return;
                case RespCommand.EXPIRETIME:
                    var expireTime = ConvertUtils.UnixTimeInSecondsFromTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                    functionsState.CopyRespNumber(expireTime, ref output);
                    return;

                case RespCommand.PEXPIRETIME:
                    var pexpireTime = ConvertUtils.UnixTimeInMillisecondsFromTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                    functionsState.CopyRespNumber(pexpireTime, ref output);
                    return;

                default:
                    throw new GarnetException("Unsupported operation on input");
            }
        }

        IPUResult EvaluateExpireInPlace(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, ref SpanByteAndMemory output)
        {
            var o = (OutputHeader*)output.SpanByte.ToPointer();
            o->result1 = 0;

            if (!EvaluateExpire(ref logRecord, optionType, newExpiry, logRecord.Info.HasExpiration, logErrorOnFail: false, functionsState.logger, out var expirationChanged))
                return IPUResult.Failed;

            if (!expirationChanged)
                return IPUResult.NotUpdated;
            o->result1 = 1;
            return IPUResult.Succeeded;
        }

        bool EvaluateExpireCopyUpdate(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ExpireOption optionType, long newExpiry, ReadOnlySpan<byte> newValue, ref SpanByteAndMemory output)
        {
            var hasExpiration = logRecord.Info.HasExpiration;

            var o = (OutputHeader*)output.SpanByte.ToPointer();
            o->result1 = 0;

            // TODO ETag?
            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo))
            {
                functionsState.logger?.LogError("Failed to set value in {methodName}", nameof(EvaluateExpireCopyUpdate));
                return false;
            }

            var isSuccessful = EvaluateExpire(ref logRecord, optionType, newExpiry, hasExpiration, logErrorOnFail: true,
                functionsState.logger, out var expirationChanged);

            o->result1 = expirationChanged ? 1 : 0;
            return isSuccessful;
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

        static bool InPlaceUpdateNumber(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, long val, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var ndigits = NumUtils.CountDigits(val, out var isNegative);
            ndigits += isNegative ? 1 : 0;

            if (!logRecord.TrySetContentLengths(ndigits, in sizeInfo))
                return false;

            var value = logRecord.ValueSpan;    // To eliminate redundant length calculations getting to Value
            _ = NumUtils.WriteInt64(val, value);

            Debug.Assert(output.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            value.CopyTo(output.SpanByte.Span);
            output.SpanByte.Length = value.Length;
            return true;
        }

        static bool InPlaceUpdateNumber(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, double val, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            var ndigits = NumUtils.CountCharsInDouble(val, out var _, out var _, out var _);

            if (!logRecord.TrySetContentLengths(ndigits, in sizeInfo))
                return false;

            var value = logRecord.ValueSpan;    // To reduce redundant length calculations getting to Value
            _ = NumUtils.WriteDouble(val, value);

            Debug.Assert(output.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            value.CopyTo(output.SpanByte.Span);
            output.SpanByte.Length = value.Length;
            return true;
        }

        static bool TryInPlaceUpdateNumber(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, long input)
        {
            // Check if value contains a valid number
            var value = logRecord.ValueSpan;  // To reduce redundant length calculations getting to Value

            long val;
            if (logRecord.IsPinnedValue)
            {
                if (!IsValidNumber(value.Length, logRecord.PinnedValuePointer, output.SpanByte.Span, out val))
                    return true;
            }
            else
            {
                fixed (byte* valuePtr = value)
                {
                    if (!IsValidNumber(value.Length, valuePtr, output.SpanByte.Span, out val))
                        return true;
                }
            }

            try
            {
                checked { val += input; }
            }
            catch
            {
                output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                return true;
            }

            return InPlaceUpdateNumber(ref logRecord, in sizeInfo, val, ref output, ref rmwInfo);
        }

        static bool TryInPlaceUpdateNumber(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, double input)
        {
            var value = logRecord.ValueSpan;  // To reduce redundant length calculations getting to Value

            double val;
            if (logRecord.IsPinnedValue)
            {
                if (!IsValidDouble(value.Length, logRecord.PinnedValuePointer, output.SpanByte.Span, out val))
                    return true;
            }
            else
            {
                fixed (byte* valuePtr = value)
                {
                    if (!IsValidDouble(value.Length, valuePtr, output.SpanByte.Span, out val))
                        return true;
                }
            }


            val += input;

            if (!double.IsFinite(val))
            {
                output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                return true;
            }

            return InPlaceUpdateNumber(ref logRecord, in sizeInfo, val, ref output, ref rmwInfo);
        }

        static bool TryCopyUpdateNumber(long next, Span<byte> newValue, ref SpanByteAndMemory output)
        {
            if (NumUtils.WriteInt64(next, newValue) == 0)
                return false;
            newValue.CopyTo(output.SpanByte.Span);
            output.SpanByte.Length = newValue.Length;
            return true;
        }

        static bool TryCopyUpdateNumber(double next, Span<byte> newValue, ref SpanByteAndMemory output)
        {
            if (NumUtils.WriteDouble(next, newValue) == 0)
                return false;
            newValue.CopyTo(output.SpanByte.Span);
            output.SpanByte.Length = newValue.Length;
            return true;
        }

        /// <summary>
        /// Copy update from old 'long' value to new value while also validating whether oldValue is a numerical value.
        /// </summary>
        /// <param name="srcLogRecord">The source log record, either in-memory or from disk</param>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="sizeInfo">Size info for record fields</param>
        /// <param name="output">Output value</param>
        /// <param name="input">Parsed input value</param>
        static bool TryCopyUpdateNumber<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref SpanByteAndMemory output, long input)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                return false;

            var srcValue = srcLogRecord.ValueSpan;  // To reduce redundant length calculations getting to ValueSpan

            long val;
            if (srcLogRecord.IsPinnedValue)
            {
                if (!IsValidNumber(srcValue.Length, srcLogRecord.PinnedValuePointer, output.SpanByte.Span, out val))
                {
                    // Move to tail of the log even when oldValue is alphanumeric
                    // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                    output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                    return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
                }
            }
            else
            {
                fixed (byte* valuePtr = srcValue)
                {
                    if (!IsValidNumber(srcValue.Length, valuePtr, output.SpanByte.Span, out val))
                    {
                        // Move to tail of the log even when oldValue is alphanumeric
                        // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                        output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                        return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
                    }
                }
            }

            // Check operation overflow
            try
            {
                checked { val += input; }
            }
            catch
            {
                output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }

            // Move to tail of the log and update
            return TryCopyUpdateNumber(val, dstLogRecord.ValueSpan, ref output);
        }

        /// <summary>
        /// Copy update from old 'double' value to new value while also validating whether oldValue is a numerical value.
        /// </summary>
        /// <param name="srcLogRecord">The source log record, either in-memory or from disk</param>
        /// <param name="dstLogRecord">The destination log record</param>
        /// <param name="sizeInfo">Size information for record fields</param>
        /// <param name="output">Output value</param>
        /// <param name="input">Parsed input value</param>
        static bool TryCopyUpdateNumber<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref SpanByteAndMemory output, double input)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                return false;

            var srcValue = srcLogRecord.ValueSpan;  // To reduce redundant length calculations getting to ValueSpan

            double val;
            if (srcLogRecord.IsPinnedValue)
            {
                if (!IsValidDouble(srcValue.Length, srcLogRecord.PinnedValuePointer, output.SpanByte.Span, out val))
                {
                    // Move to tail of the log even when oldValue is alphanumeric
                    // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                    output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                    return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
                }
            }
            else
            {
                fixed (byte* valuePtr = srcValue)
                {
                    if (!IsValidDouble(srcValue.Length, valuePtr, output.SpanByte.Span, out val))
                    {
                        // Move to tail of the log even when oldValue is alphanumeric
                        // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                        output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                        return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
                    }
                }
            }

            val += input;
            if (!double.IsFinite(val))
            {
                output.SpanByte.Span[0] = (byte)OperationError.INVALID_TYPE;
                return false;
            }

            // Move to tail of the log and update
            return TryCopyUpdateNumber(val, dstLogRecord.ValueSpan, ref output);
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

        /// <summary>
        /// Copy length of value to output (as ASCII bytes)
        /// </summary>
        static bool TryCopyValueLengthToOutput(ReadOnlySpan<byte> value, ref SpanByteAndMemory output)
        {
            Debug.Assert(output.IsSpanByte, "This code assumes it is called in a non-pending context or in a pending context where dst.SpanByte's pointer remains valid");

            var numDigits = NumUtils.CountDigits(value.Length);
            if (numDigits > output.SpanByte.Length)
            {
                Debug.Fail("Output length overflow in TryCopyValueLengthToOutput");
                return false;
            }

            var outputPtr = output.SpanByte.ToPointer();
            NumUtils.WriteInt32(value.Length, numDigits, ref outputPtr);
            output.SpanByte.Length = numDigits;
            return true;
        }

        void CopyRespWithEtagData(ReadOnlySpan<byte> value, ref SpanByteAndMemory dst, bool hasETag, MemoryPool<byte> memoryPool)
        {
            int valueLength = value.Length;
            // always writing an array of size 2 => *2\r\n
            int desiredLength = 4;

            // get etag to write, default etag 0 for when no etag
            long etag = hasETag ? functionsState.etagState.ETag : LogRecord.NoETag;

            // here we know the value span has first bytes set to etag so we hardcode skipping past the bytes for the etag below
            // *2\r\n :(etag digits)\r\n $(val Len digits)\r\n (value len)\r\n
            desiredLength += 1 + NumUtils.CountDigits(etag) + 2 + 1 + NumUtils.CountDigits(valueLength) + 2 + valueLength + 2;

            WriteValAndEtagToDst(desiredLength, value, etag, ref dst, memoryPool);
        }

        static void WriteValAndEtagToDst(int desiredLength, ReadOnlySpan<byte> value, long etag, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool, bool writeDirect = false)
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
            fixed (byte* ptr = dst.MemorySpan)
            {
                byte* curr = ptr;
                byte* end = ptr + desiredLength;
                RespWriteUtils.WriteEtagValArray(etag, ref value, ref curr, end, writeDirect);
            }
        }

        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, long version, int sessionId)
        {
            if (functionsState.StoredProcMode) return;

            // We need this check because when we ingest records from the primary
            // if the input is zero then input overlaps with value so any update to RespInputHeader->flags
            // will incorrectly modify the total length of value.
            if (input.SerializedLength > 0)
                input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = version, sessionID = sessionId },
                key, value, ref input, out _);
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ReadOnlySpan<byte> key, ref StringInput input, long version, int sessionId)
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.StoreRMW, storeVersion = version, sessionID = sessionId },
                key, ref input, out _);
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. InPlaceDeleter
        ///  b. PostInitialDeleter
        /// </summary>
        void WriteLogDelete(ReadOnlySpan<byte> key, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;
            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.StoreDelete, storeVersion = version, sessionID = sessionID }, key, item2: default, out _);
        }

        BitFieldCmdArgs GetBitFieldArguments(ref StringInput input)
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