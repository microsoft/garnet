// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public sealed class SessionSpanByteFunctions : SpanByteFunctions<Empty>
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


        // Only the ReadOnlySpan<byte> form of Upsert value is used here.

        /// <inheritdoc/>
        public override bool ConcurrentWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            // This does not try to set ETag or Expiration
            srcValue.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }
    }
}