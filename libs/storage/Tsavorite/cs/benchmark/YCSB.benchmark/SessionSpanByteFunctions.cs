// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Tsavorite.benchmark
{
    public sealed class SessionSpanByteFunctions : SpanByteFunctions<Empty>
    {
        /// <inheritdoc />
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            // Copy only the first cache line for more interpretable results
            srcLogRecord.ValueSpan.Slice(0, 32).CopyTo(output.SpanByte.Span);
            return true;
        }

        // Note: InitialWriter and InPlaceWriter are inherited from SpanByteFunctions<Empty>,
        // which extracts the value from input.ReadOnlySpan.

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            => throw new TsavoriteException("InitialUpdater not implemented for YCSB");

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            input.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            input.CopyTo(logRecord.ValueSpan);
            return true;
        }
    }
}