// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Session functions for KV.benchmark.
    /// - <see cref="Reader"/> copies the first <see cref="kReaderCopyBytes"/> bytes
    ///   (one cache line) of the value into the output buffer — matches YCSB's
    ///   "first cache line" measurement choice so reported throughput isolates engine
    ///   overhead from memcpy bandwidth.
    /// - <see cref="InitialUpdater"/> is implemented (not throwing) so a concurrent
    ///   RMW that lands on a key during the brief delete-reinsert gap succeeds.
    /// </summary>
    public sealed class KvSessionFunctions : SpanByteFunctions<Empty>
    {
        internal const int kReaderCopyBytes = 64;

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            var src = srcLogRecord.ValueSpan;
            var n = src.Length < kReaderCopyBytes ? src.Length : kReaderCopyBytes;
            src.Slice(0, n).CopyTo(output.SpanByte.Span);
            return true;
        }

        public override bool InPlaceWriter(ref LogRecord logRecord, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            srcValue.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }

        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            input.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            input.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord logRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            input.CopyTo(logRecord.ValueSpan);
            return true;
        }
    }
}
