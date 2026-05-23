// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    // Optional post-load read-back: confirms every key the load phase wrote is
    // actually retrievable and that its first cache line matches the per-thread
    // pattern. Slow (single-threaded), opt-in via --validate; used only as a
    // sanity check after experiments, not on the measurement path.
    public sealed partial class KvBenchmark
    {
        /// <summary>
        /// Reads back every key the load phase wrote and verifies the first
        /// <see cref="KvSessionFunctions.kReaderCopyBytes"/> bytes match the
        /// per-thread baked pattern. Returns (mismatches, readMisses).
        /// </summary>
        public unsafe (long mismatches, long readMisses) Validate()
        {
            using var session = store.NewSession<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions>(functions);
            var bContext = session.BasicContext;

            byte* inputPtr = stackalloc byte[Options.ValueSize];
            byte* outputPtr = stackalloc byte[KvSessionFunctions.kReaderCopyBytes];
            var inputSpan = new Span<byte>(inputPtr, Options.ValueSize);
            var outputSpan = new Span<byte>(outputPtr, KvSessionFunctions.kReaderCopyBytes);
            var pinnedInput = PinnedSpanByte.FromPinnedSpan(inputSpan);
            var output = SpanByteAndMemory.FromPinnedSpan(outputSpan);

            long mismatches = 0;
            long misses = 0;
            KvKey key = default;
            int nCmp = Math.Min(KvSessionFunctions.kReaderCopyBytes, Options.ValueSize);

            for (long k = 0; k < Options.Keys; k++)
            {
                key.Value = k;
                // Mirror the load-phase partition so we know which thread's pattern to expect.
                int writerThread = (int)((double)k / Options.Keys * Options.Threads);
                if (writerThread >= Options.Threads) writerThread = Options.Threads - 1;

                var st = bContext.Read(key, ref pinnedInput, ref output, Empty.Default);
                if (!st.Found) { ++misses; continue; }
                for (int i = 0; i < nCmp; i++)
                {
                    var expected = (byte)((writerThread * 31 + i) & 0xFF);
                    if (outputPtr[i] != expected) { ++mismatches; break; }
                }
            }
            return (mismatches, misses);
        }
    }
}
