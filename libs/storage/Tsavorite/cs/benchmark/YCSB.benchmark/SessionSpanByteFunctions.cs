// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public sealed class SessionSpanByteFunctions : SpanByteFunctions<Empty>
    {
        /// <inheritdoc />
        public override bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            srcLogRecord.ValueSpan.CopyTo(output.SpanByte.Span);
            return true;
        }

        // Note: Currently, only the ReadOnlySpan<byte> form of Upsert value is used here.

        /// <inheritdoc/>
        public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            srcValue.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo) => throw new TsavoriteException("InitialUpdater not implemented for YCSB");

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            input.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            input.CopyTo(logRecord.ValueSpan);
            return true;
        }
    }
}