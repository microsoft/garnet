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
        // Hard-coded constant (matches YCSB's `Slice(0, 32)`) so JIT can const-fold.
        // Was previously read from KV_READER_COPY_BYTES env var, but reading a static
        // readonly field breaks the constant-fold and produces measurably slower codegen.
        internal const int kReaderCopyBytes = 32;

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            // Byte-identical to YCSB's SessionSpanByteFunctions.Reader: copy a constant
            // 32 bytes (no length check — caller guarantees value >= 32 via --value-size validation).
            srcLogRecord.ValueSpan.Slice(0, 32).CopyTo(output.SpanByte.Span);
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
