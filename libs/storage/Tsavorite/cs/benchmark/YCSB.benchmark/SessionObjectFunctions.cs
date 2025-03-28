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
        {
            logRecord.ValueSpan.CopyTo(output.SpanByte.Span);
            return true;
        }

        /// <inheritdoc />
        public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            srcLogRecord.ValueSpan.CopyTo(output.SpanByte.Span);
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
    }
}