// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public sealed class SessionObjectFunctions : SpanByteFunctions<Empty>
    {
        /// <inheritdoc />
        public override bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            if (!srcLogRecord.Info.ValueIsObject)
                srcLogRecord.ValueSpan.CopyTo(output.SpanByte.Span);
            else                                    // Slice the output because it is a larger buffer
                output.SpanByte.AsSpan(0, sizeof(long)).AsRef<long>() = ((ObjectValue)srcLogRecord.ValueObject).value;
            return true;
        }

        // Note: Currently, only the ReadOnlySpan<byte> form of InPlaceWriter value is used here.

        /// <inheritdoc/>
        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            if (!logRecord.Info.ValueIsObject)      // If !ValueIsObject, the destination data length, either inline or out-of-line, should already be sufficient
                srcValue.CopyTo(logRecord.ValueSpan);
            else                                    // Slice the input because it comes from a larger buffer
                ((ObjectValue)logRecord.ValueObject).value = srcValue.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            if (dstLogRecord.Info.ValueIsInline && srcValue.Length <= dstLogRecord.ValueSpan.Length)
                srcValue.CopyTo(dstLogRecord.ValueSpan);
            else if (!dstLogRecord.Info.ValueIsObject)  // process overflow
                return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo);
            else                                        // Slice the input because it comes from a larger buffer
                ((ObjectValue)dstLogRecord.ValueObject).value = srcValue.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration. It is called only during Setup.
            return dstLogRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo) => throw new TsavoriteException("InitialUpdater not implemented for YCSB");

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            => InPlaceUpdater(ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            if (!logRecord.Info.ValueIsObject)      // If !ValueIsObject, the destination data length, either inline or out-of-line, should already be sufficient
                input.CopyTo(logRecord.ValueSpan);
            else                                    // Slice the input because it comes from a larger buffer
                ((ObjectValue)logRecord.ValueObject).value = input.ReadOnlySpan.Slice(0, FixedLengthValue.Size).AsRef<FixedLengthValue>().value;
            return true;
        }
    }
}