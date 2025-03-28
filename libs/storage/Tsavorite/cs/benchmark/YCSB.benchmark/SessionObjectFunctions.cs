// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public sealed class SessionObjectFunctions : SpanByteFunctions<Empty>
    {
        /// <inheritdoc />
        public override bool ConcurrentReader(ref LogRecord logRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            => SingleReader(ref logRecord, ref input, ref output, ref readInfo);

        /// <inheritdoc />
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            if (!srcLogRecord.Info.ValueIsObject)
                srcLogRecord.ValueSpan.CopyTo(output.SpanByte.Span);
            else                                    // Slice the output because it is a larger buffer
                output.SpanByte.Span.Slice(0, sizeof(long)).AsRef<long>() = ((ObjectValue)srcLogRecord.ValueObject).value;
            return true;
        }


        // Only the ReadOnlySpan<byte> form of ConcurrentWriter value is used here.

        /// <inheritdoc/>
        public override bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            if (!logRecord.ValueIsObject)           // If !ValueIsObject, the destination data length, either inline or out-of-line, should already be sufficient
                srcValue.CopyTo(logRecord.ValueSpan);
            else                                    // Slice the input because it comes from a larger buffer
                ((ObjectValue)logRecord.ValueObject).value = srcValue.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            // This does not try to set ETag or Expiration
            if (dstLogRecord.Info.ValueIsInline && srcValue.Length <= dstLogRecord.ValueSpan.Length)
                srcValue.CopyTo(dstLogRecord.ValueSpan);
            else if (!dstLogRecord.ValueIsObject)   // process overflow
                return dstLogRecord.TrySetValueSpan(srcValue, ref sizeInfo);
            else                                    // Slice the input because it comes from a larger buffer
                ((ObjectValue)dstLogRecord.ValueObject).value = srcValue.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            // This does not try to set ETag or Expiration. It is called only during Setup.
            return dstLogRecord.TrySetValueObject(srcValue, ref sizeInfo);
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo) => throw new TsavoriteException("InitialUpdater not implemented for YCSB");

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            => InPlaceUpdater(ref dstLogRecord, ref sizeInfo, ref input, ref output, ref rmwInfo);

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            if (!logRecord.ValueIsObject)           // If !ValueIsObject, the destination data length, either inline or out-of-line, should already be sufficient
                input.CopyTo(logRecord.ValueSpan);
            else                                    // Slice the input because it comes from a larger buffer
                ((ObjectValue)logRecord.ValueObject).value = input.ReadOnlySpan.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }
    }
}