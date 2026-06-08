// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
        static void CopyTo(ReadOnlySpan<byte> src, ref StringOutput dst, MemoryPool<byte> memoryPool)
        {
            int srcLength = src.Length;

            if (dst.SpanByteAndMemory.IsSpanByte)
            {
                if (dst.SpanByteAndMemory.Length >= srcLength)
                {
                    dst.SpanByteAndMemory.Length = srcLength;
                    src.CopyTo(dst.SpanByteAndMemory.SpanByte.Span);
                    return;
                }
                dst.SpanByteAndMemory.ConvertToHeap();
            }

            dst.SpanByteAndMemory.Memory = memoryPool.Rent(srcLength);
            dst.SpanByteAndMemory.Length = srcLength;
            src.CopyTo(dst.SpanByteAndMemory.MemorySpan);
        }

        void CopyRespTo(ReadOnlySpan<byte> src, ref StringOutput dst, int start = 0, int end = -1)
        {
            int srcLength = end == -1 ? src.Length : ((start < end) ? (end - start) : 0);
            if (srcLength == 0)
            {
                functionsState.CopyDefaultResp(CmdStrings.RESP_EMPTY, ref dst.SpanByteAndMemory);
                return;
            }

            var numLength = NumUtils.CountDigits(srcLength);
            var totalSize = 1 + numLength + 2 + srcLength + 2; // $5\r\nvalue\r\n

            if (dst.SpanByteAndMemory.IsSpanByte)
            {
                if (dst.SpanByteAndMemory.Length >= totalSize)
                {
                    dst.SpanByteAndMemory.Length = totalSize;

                    var tmp = dst.SpanByteAndMemory.SpanByte.ToPointer();
                    *tmp++ = (byte)'$';
                    NumUtils.WriteInt32(srcLength, numLength, ref tmp);
                    *tmp++ = (byte)'\r';
                    *tmp++ = (byte)'\n';
                    src.Slice(start, srcLength).CopyTo(new Span<byte>(tmp, srcLength));
                    tmp += srcLength;
                    *tmp++ = (byte)'\r';
                    *tmp = (byte)'\n';
                    return;
                }
                dst.SpanByteAndMemory.ConvertToHeap();
            }

            dst.SpanByteAndMemory.Memory = functionsState.memoryPool.Rent(totalSize);
            dst.SpanByteAndMemory.Length = totalSize;
            fixed (byte* ptr = dst.SpanByteAndMemory.MemorySpan)
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

        void CopyRespToWithInput<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output, bool isFromPending)
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
                        output.SpanByteAndMemory.ConvertToHeap();
                    CopyRespTo(value, ref output);
                    break;

                case RespCommand.VADD:
                case RespCommand.VSIM:
                case RespCommand.VEMB:
                case RespCommand.VGETATTR:
                case RespCommand.VINFO:
                case RespCommand.VREM:
                case RespCommand.VDIM:
                case RespCommand.GET:
                case RespCommand.RIGET:
                case RespCommand.RISET:
                case RespCommand.RIDEL:
                case RespCommand.RISCAN:
                case RespCommand.RIRANGE:
                case RespCommand.RIEXISTS:
                case RespCommand.RICONFIG:
                case RespCommand.RIMETRICS:
                    // Get value without RESP header; exclude expiration
                    if (value.Length <= output.SpanByteAndMemory.Length)
                    {
                        output.SpanByteAndMemory.Length = value.Length;
                        value.CopyTo(output.SpanByteAndMemory.SpanByte.Span);
                        return;
                    }

                    output.SpanByteAndMemory.ConvertToHeap();
                    output.SpanByteAndMemory.Length = value.Length;
                    output.SpanByteAndMemory.Memory = functionsState.memoryPool.Rent(value.Length);
                    value.CopyTo(output.SpanByteAndMemory.MemorySpan);
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
                        oldValSet == 0 ? CmdStrings.RESP_RETURN_VAL_0 : CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                    break;

                case RespCommand.BITCOUNT:
                    long bcStartOffset = 0;
                    long bcEndOffset = -1;
                    byte bcOffsetType = 0x0;

                    if (input.parseState.Count > 0)
                    {
                        bcStartOffset = input.parseState.GetLong(0);
                    }

                    if (input.parseState.Count > 1)
                    {
                        bcEndOffset = input.parseState.GetLong(1);
                    }

                    if (input.parseState.Count > 2)
                    {
                        bcOffsetType = (byte)(input.arg1 & 0x1);
                    }

                    long count;
                    if (srcLogRecord.IsPinnedValue)
                        count = BitmapManager.BitCountDriver(bcStartOffset, bcEndOffset, bcOffsetType, srcLogRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            count = BitmapManager.BitCountDriver(bcStartOffset, bcEndOffset, bcOffsetType, valuePtr, value.Length);

                    functionsState.CopyRespNumber(count, ref output.SpanByteAndMemory);
                    break;

                case RespCommand.BITPOS:
                    var bpSetVal = (byte)(input.parseState.GetArgSliceByRef(0).ReadOnlySpan[0] - '0');
                    long bpStartOffset = 0;
                    long bpEndOffset = -1;
                    byte bpOffsetType = 0x0;
                    if (input.parseState.Count > 1)
                    {
                        bpStartOffset = input.parseState.GetLong(1);
                        if (input.parseState.Count > 2)
                        {
                            bpEndOffset = input.parseState.GetLong(2);
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

                    *(long*)output.SpanByteAndMemory.SpanByte.ToPointer() = pos;
                    functionsState.CopyRespNumber(pos, ref output.SpanByteAndMemory);
                    break;

                case RespCommand.BITOP:
                    // Expose the value as a SpanByteAndMemory: inline values point directly at log memory
                    // (stable under the unsafe context); overflow values come back as a no-copy borrowed
                    // Memory<byte> that the caller pins for the duration of BITOP execution. For values
                    // sourced from a DiskLogRecord (pending completion of a disk read), the inline
                    // SectorAlignedMemory recordBuffer would be returned to the pool when the
                    // DiskLogRecord is disposed; the getter handles that by copying inline values into a
                    // pooled IMemoryOwner before returning, so the SpanByteAndMemory returned here is
                    // always safe to use beyond this callback.
                    output.SpanByteAndMemory = srcLogRecord.ValueSpanByteAndMemory;
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
                        functionsState.CopyRespNumber(retValue, ref output.SpanByteAndMemory);
                    else
                        functionsState.CopyDefaultResp(functionsState.nilResp, ref output.SpanByteAndMemory);
                    return;

                case RespCommand.BITFIELD_RO:
                    var bitFieldArgs_RO = GetBitFieldArguments(ref input);

                    long retValue_RO;
                    if (srcLogRecord.IsPinnedValue)
                        retValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO, srcLogRecord.PinnedValuePointer, value.Length);
                    else
                        fixed (byte* valuePtr = value)
                            retValue_RO = BitmapManager.BitFieldExecute_RO(bitFieldArgs_RO, valuePtr, value.Length);

                    functionsState.CopyRespNumber(retValue_RO, ref output.SpanByteAndMemory);
                    return;

                case RespCommand.PFCOUNT:
                case RespCommand.PFMERGE:
                    // Caller (HyperLogLogOps) provides a sector-aligned destination buffer sized to
                    // HyperLogLog.DefaultHLL.DenseBytes. Validate signature AND size before the copy
                    // so a corrupted/oversized value cannot overflow the destination.
                    var pfDstPtr = output.SpanByteAndMemory.SpanByte.ToPointer();
                    var pfDstCapacity = output.SpanByteAndMemory.SpanByte.Length;

                    bool isValid;
                    if (srcLogRecord.IsPinnedValue)
                    {
                        isValid = HyperLogLog.DefaultHLL.IsValidHYLL(srcLogRecord.PinnedValuePointer, value.Length);
                    }
                    else
                    {
                        fixed (byte* valuePtr = value)
                            isValid = HyperLogLog.DefaultHLL.IsValidHYLL(valuePtr, value.Length);
                    }

                    // Surface invalid OR oversized as the -1 sentinel; the caller already checks for it.
                    if (!isValid || value.Length > pfDstCapacity)
                    {
                        *(long*)pfDstPtr = -1;
                        return;
                    }

                    // Pass the actual destination capacity to MemoryCopy so the bounds check is meaningful.
                    if (srcLogRecord.IsPinnedValue)
                    {
                        Buffer.MemoryCopy(srcLogRecord.PinnedValuePointer, pfDstPtr, pfDstCapacity, value.Length);
                    }
                    else
                    {
                        fixed (byte* valuePtr = value)
                            Buffer.MemoryCopy(valuePtr, pfDstPtr, pfDstCapacity, value.Length);
                    }

                    output.SpanByteAndMemory.SpanByte.Length = value.Length;
                    return;

                case RespCommand.TTL:
                    var ttlValue = ConvertUtils.SecondsFromDiffUtcNowTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                    functionsState.CopyRespNumber(ttlValue, ref output.SpanByteAndMemory);
                    return;

                case RespCommand.PTTL:
                    var pttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                    functionsState.CopyRespNumber(pttlValue, ref output.SpanByteAndMemory);
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
                    functionsState.CopyRespNumber(expireTime, ref output.SpanByteAndMemory);
                    return;

                case RespCommand.PEXPIRETIME:
                    var pexpireTime = ConvertUtils.UnixTimeInMillisecondsFromTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                    functionsState.CopyRespNumber(pexpireTime, ref output.SpanByteAndMemory);
                    return;

                default:
                    throw new GarnetException("Unsupported operation on input");
            }
        }

        IPUResult EvaluateExpireInPlace(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, ref StringOutput output)
        {
            ref var result = ref output.AsRef<int>();
            result = 0;

            if (!EvaluateExpire(ref logRecord, optionType, newExpiry, logRecord.Info.HasExpiration, logErrorOnFail: false, functionsState.logger, out var expirationChanged))
                return IPUResult.Failed;

            if (!expirationChanged)
                return IPUResult.NotUpdated;

            result = 1;
            return IPUResult.Succeeded;
        }

        bool EvaluateExpireCopyUpdate(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ExpireOption optionType, long newExpiry, ReadOnlySpan<byte> newValue, ref StringOutput output)
        {
            var hasExpiration = logRecord.Info.HasExpiration;

            ref var result = ref output.AsRef<int>();
            result = 0;

            // TODO ETag?
            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo))
            {
                functionsState.logger?.LogError("Failed to set value in {methodName}", nameof(EvaluateExpireCopyUpdate));
                return false;
            }

            var isSuccessful = EvaluateExpire(ref logRecord, optionType, newExpiry, hasExpiration, logErrorOnFail: true,
                functionsState.logger, out var expirationChanged);

            result = expirationChanged ? 1 : 0;
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

        static bool TryInPlaceUpdateNumber(ref LogRecord logRecord, ref StringOutput output, ref RMWInfo rmwInfo, long input)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");

            // Check if the current value in the logRecord contains a valid number and if so, add the input to it.
            try
            {
                if (logRecord.IsPinnedValue)
                {
                    // Using the pinned pointer directly is faster than pinning 'value'.
                    var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                    if (!IsValidNumber(valueLength, (byte*)valueAddress, ref output, out var val))
                        return true;

                    checked { val += input; }
                    var ndigits = NumUtils.CountDigits(val, out var isNegative);

                    // Set the logRecord's length to the full length of the new value, including negative sign, and get the updated valueLength
                    if (!logRecord.TrySetPinnedValueLength(ndigits + (isNegative ? 1 : 0), valueAddress, ref valueLength))
                        return false;

                    // Call the pinned form of the number-writer. Don't include space for the negative sign; that's added in the callee.
                    {
                        var valuePtr = (byte*)valueAddress; // Use ONLY here; it is updated by WriteInt64, so do not use as the pointer for the subsequent copy
                        NumUtils.WriteInt64(val, ndigits, ref valuePtr);
                    }

                    new ReadOnlySpan<byte>((byte*)valueAddress, valueLength).CopyTo(output.SpanByteAndMemory.SpanByte.Span);
                    output.SpanByteAndMemory.SpanByte.Length = valueLength;
                }
                else
                {
                    // The value is not inline, so LogRecord will probably change it to inline because the update is to a very short (# chars in number) length.
                    var value = logRecord.ValueSpan;

                    // TODO: Create sizeInfo
                    RecordSizeInfo sizeInfo = default;

                    fixed (byte* valuePtr = value)
                    {
                        if (!IsValidNumber(value.Length, valuePtr, ref output, out var val))
                            return true;
                        checked { val += input; }
                        var ndigits = NumUtils.CountDigits(val, out var isNegative);

                        // Set the logRecord's length to the full length of the new value, including negative sign.
                        if (!logRecord.TrySetContentLengths(ndigits + (isNegative ? 1 : 0), in sizeInfo))
                            return false;
                        value = logRecord.ValueSpan;    // Re-get since length (and possibly inline-ness) has changed

                        // This call will pin the destination.
                        _ = NumUtils.WriteInt64(val, value);

                        value.CopyTo(output.SpanByteAndMemory.SpanByte.Span);
                        output.SpanByteAndMemory.SpanByte.Length = value.Length;
                    }
                }
            }
            catch
            {
                output.OutputFlags |= StringOutputFlags.InvalidTypeError;
            }
            return true;
        }

        static bool TryInPlaceUpdateNumber(ref LogRecord logRecord, ref StringOutput output, ref RMWInfo rmwInfo, double input)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");

            // Check if the current value in the logRecord contains a valid double and if so, add the input to it.
            if (logRecord.IsPinnedValue)
            {
                // Using the pinned pointer directly is faster than pinning 'value'.
                var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                if (!IsValidDouble(valueLength, (byte*)valueAddress, ref output, out var val))
                    return true;

                val += input;
                if (!double.IsFinite(val))
                {
                    output.OutputFlags |= StringOutputFlags.InvalidTypeError;
                    return true;
                }
                var ndigits = NumUtils.CountCharsInDouble(val, out var _, out var _, out var _);

                if (!logRecord.TrySetPinnedValueLength(ndigits, valueAddress, ref valueLength))
                    return false;

                // Call the pinned form of the number-writer. ndigits includes space for the negative sign if present.
                var ptr = (byte*)valueAddress;
                NumUtils.WriteDouble(val, ndigits, ref ptr);

                new ReadOnlySpan<byte>((byte*)valueAddress, valueLength).CopyTo(output.SpanByteAndMemory.SpanByte.Span);
                output.SpanByteAndMemory.SpanByte.Length = valueLength;
            }
            else
            {
                // The value is not inline, so LogRecord will probably change it to inline because the update is to a very short (# chars in number) length.

                // TODO: Create sizeInfo
                RecordSizeInfo sizeInfo = default;

                var value = logRecord.ValueSpan;
                fixed (byte* valuePtr = value)
                {
                    if (!IsValidDouble(value.Length, valuePtr, ref output, out var val))
                        return true;

                    val += input;
                    if (!double.IsFinite(val))
                    {
                        output.OutputFlags |= StringOutputFlags.InvalidTypeError;
                        return true;
                    }
                    var ndigits = NumUtils.CountCharsInDouble(val, out var _, out var _, out var _);

                    // Set the logRecord's length to the length of the new value
                    if (!logRecord.TrySetContentLengths(ndigits, in sizeInfo))
                        return false;
                    value = logRecord.ValueSpan;
                    _ = NumUtils.WriteDouble(val, value);

                    value.CopyTo(output.SpanByteAndMemory.SpanByte.Span);
                    output.SpanByteAndMemory.SpanByte.Length = value.Length;
                }
            }
            return true;
        }

        static bool TryCopyUpdateNumber(long next, Span<byte> newValue, ref StringOutput output)
        {
            if (NumUtils.WriteInt64(next, newValue) == 0)
                return false;
            newValue.CopyTo(output.SpanByteAndMemory.SpanByte.Span);
            output.SpanByteAndMemory.SpanByte.Length = newValue.Length;
            return true;
        }

        static bool TryCopyUpdateNumber(double next, Span<byte> newValue, ref StringOutput output)
        {
            if (NumUtils.WriteDouble(next, newValue) == 0)
                return false;
            newValue.CopyTo(output.SpanByteAndMemory.SpanByte.Span);
            output.SpanByteAndMemory.SpanByte.Length = newValue.Length;
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
        static bool TryCopyUpdateNumber<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringOutput output, long input)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                return false;

            var srcValue = srcLogRecord.ValueSpan;  // To reduce redundant length calculations getting to ValueSpan

            long val;
            if (srcLogRecord.IsPinnedValue)
            {
                if (!IsValidNumber(srcValue.Length, srcLogRecord.PinnedValuePointer, ref output, out val))
                {
                    // Move to tail of the log even when oldValue is alphanumeric
                    // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                    output.OutputFlags |= StringOutputFlags.InvalidTypeError;
                    return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
                }
            }
            else
            {
                fixed (byte* valuePtr = srcValue)
                {
                    if (!IsValidNumber(srcValue.Length, valuePtr, ref output, out val))
                    {
                        // Move to tail of the log even when oldValue is alphanumeric
                        // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                        output.OutputFlags |= StringOutputFlags.InvalidTypeError;
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
                output.OutputFlags |= StringOutputFlags.InvalidTypeError;
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
        static bool TryCopyUpdateNumber<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringOutput output, double input)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!dstLogRecord.TryCopyOptionals(in srcLogRecord, in sizeInfo))
                return false;

            var srcValue = srcLogRecord.ValueSpan;  // To reduce redundant length calculations getting to ValueSpan

            double val;
            if (srcLogRecord.IsPinnedValue)
            {
                if (!IsValidDouble(srcValue.Length, srcLogRecord.PinnedValuePointer, ref output, out val))
                {
                    // Move to tail of the log even when oldValue is alphanumeric
                    // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                    output.OutputFlags |= StringOutputFlags.InvalidTypeError;
                    return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
                }
            }
            else
            {
                fixed (byte* valuePtr = srcValue)
                {
                    if (!IsValidDouble(srcValue.Length, valuePtr, ref output, out val))
                    {
                        // Move to tail of the log even when oldValue is alphanumeric
                        // We have already paid the cost of bringing from disk so we are treating as a regular access and bring it into memory
                        output.OutputFlags |= StringOutputFlags.InvalidTypeError;
                        return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
                    }
                }
            }

            val += input;
            if (!double.IsFinite(val))
            {
                output.OutputFlags |= StringOutputFlags.InvalidTypeError;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static bool IsValidNumber(int length, byte* source, ref StringOutput output, out long val)
        {
            // Check for valid number
            if (NumUtils.TryReadInt64(length, source, out val))
                return true;

            // Signal value is not a valid number
            output.OutputFlags |= StringOutputFlags.InvalidTypeError;
            return false;
        }

        static bool IsValidDouble(int length, byte* source, ref StringOutput output, out double val)
        {
            // Check for valid number
            if (!NumUtils.TryParseWithInfinity(new ReadOnlySpan<byte>(source, length), out val))
            {
                // Signal value is not a valid number
                output.OutputFlags |= StringOutputFlags.InvalidTypeError;
                return false;
            }

            if (!double.IsFinite(val))
            {
                // Signal value is not a Nan/Infinity
                output.OutputFlags |= StringOutputFlags.NaNOrInfinityError;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copy length of value to output (as ASCII bytes)
        /// </summary>
        static bool TryCopyValueLengthToOutput(ReadOnlySpan<byte> value, ref StringOutput output)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in a non-pending context or in a pending context where dst.SpanByte's pointer remains valid");

            var numDigits = NumUtils.CountDigits(value.Length);
            if (numDigits > output.SpanByteAndMemory.SpanByte.Length)
            {
                Debug.Fail("Output length overflow in TryCopyValueLengthToOutput");
                return false;
            }

            var outputPtr = output.SpanByteAndMemory.SpanByte.ToPointer();
            NumUtils.WriteInt32(value.Length, numDigits, ref outputPtr);
            output.SpanByteAndMemory.SpanByte.Length = numDigits;
            return true;
        }

        void CopyRespWithEtagData(ReadOnlySpan<byte> value, ref StringOutput dst, bool hasETag, long etag, MemoryPool<byte> memoryPool)
        {
            int valueLength = value.Length;
            // always writing an array of size 2 => *2\r\n
            int desiredLength = 4;

            // use provided etag, default etag 0 for when no etag
            long etagToWrite = hasETag ? etag : LogRecord.NoETag;

            // Account for the two RESP array elements written separately below:
            // *2\r\n :(etag digits)\r\n $(value length digits)\r\n (value bytes)\r\n
            desiredLength += 1 + NumUtils.CountDigits(etagToWrite) + 2 + 1 + NumUtils.CountDigits(valueLength) + 2 + valueLength + 2;

            WriteValAndEtagToDst(desiredLength, value, etagToWrite, ref dst, memoryPool);
        }

        static void WriteValAndEtagToDst(int desiredLength, ReadOnlySpan<byte> value, long etag, ref StringOutput dst, MemoryPool<byte> memoryPool, bool writeDirect = false)
        {
            if (desiredLength <= dst.SpanByteAndMemory.Length)
            {
                dst.SpanByteAndMemory.Length = desiredLength;
                byte* curr = dst.SpanByteAndMemory.SpanByte.ToPointer();
                byte* end = curr + dst.SpanByteAndMemory.SpanByte.Length;
                RespWriteUtils.WriteEtagValArray(etag, ref value, ref curr, end, writeDirect);
                return;
            }

            dst.SpanByteAndMemory.ConvertToHeap();
            dst.SpanByteAndMemory.Length = desiredLength;
            dst.SpanByteAndMemory.Memory = memoryPool.Rent(desiredLength);
            fixed (byte* ptr = dst.SpanByteAndMemory.MemorySpan)
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
        void WriteLogUpsert<TEpochAccessor>(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, long version, int sessionId, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
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

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreUpsert,
                version,
                sessionId,
                key,
                value,
                ref input,
                epochAccessor,
                out _);
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW<TEpochAccessor>(ReadOnlySpan<byte> key, ref StringInput input, long version, int sessionId, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode) return;

            if (input.header.cmd == RespCommand.VADD && input.arg1 is not (VectorManager.VADDAppendLogArg or VectorManager.MigrateElementKeyLogArg or VectorManager.MigrateIndexKeyLogArg))
            {
                return;
            }

            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreRMW,
                version,
                sessionId,
                key,
                ref input,
                epochAccessor,
                out _);
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. InPlaceDeleter
        ///  b. PostInitialDeleter
        /// </summary>
        void WriteLogDelete<TEpochAccessor>(ReadOnlySpan<byte> key, long version, int sessionID, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode) return;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.StoreDelete,
                version,
                sessionID,
                key,
                value: default,
                epochAccessor,
                out _);
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
            if (!BitmapManager.TryValidateBitfieldOffset(offset, (byte)bitCount, multiplyOffset, out offset, out _))
                throw new GarnetException("ERR bit offset is not an integer or out of range");

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