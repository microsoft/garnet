// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Session functions for KV.benchmark.
    /// - <see cref="Reader"/> copies the first <see cref="kReaderCopyBytes"/> bytes
    ///   (one cache line) of the value into the output buffer — isolates engine
    ///   overhead from memcpy bandwidth for interpretable read throughput.
    /// - <see cref="InitialUpdater"/> is implemented (not throwing) so a concurrent
    ///   RMW that lands on a key during the brief delete-reinsert gap succeeds.
    /// </summary>
    public sealed class KvSessionFunctions : SpanByteFunctions<Empty>
    {
        // Hard-coded constant so JIT can const-fold the Slice/CopyTo into a single
        // 32-byte SSE memcpy. Reading this from a static-readonly field defeats the
        // const-fold and produces measurably slower codegen.
        internal const int kReaderCopyBytes = 32;

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            // Constant 32-byte copy — value length must be >= 32 (--value-size validation enforces this).
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
