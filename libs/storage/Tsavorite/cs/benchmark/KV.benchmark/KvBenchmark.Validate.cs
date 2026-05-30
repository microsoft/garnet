// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
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
        ///
        /// Records below HeadAddress trigger async disk I/O (Status.IsPending);
        /// we issue ops in batches and drain via CompletePendingWithOutputs.
        /// </summary>
        public unsafe (long mismatches, long readMisses) Validate()
        {
            using var session = store.NewSession<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions>(functions);
            var bContext = session.BasicContext;

            byte* inputPtr = stackalloc byte[Options.ValueSize];
            var inputSpan = new Span<byte>(inputPtr, Options.ValueSize);
            var pinnedInput = PinnedSpanByte.FromPinnedSpan(inputSpan);

            long mismatches = 0;
            long misses = 0;
            int nCmp = Math.Min(KvSessionFunctions.kReaderCopyBytes, Options.ValueSize);
            int loadThreads = Options.ResolvedLoadThreads;
            long keyCount = Options.Keys;

            // Per-op pinned output buffer (one per slot in the batch) so async pending
            // reads can write into a stable location while we issue more ops in the
            // same chunk. SpanByteAndMemory just records the destination pointer.
            const int kBatch = 256;
            var outputArr = new byte[kBatch * KvSessionFunctions.kReaderCopyBytes];
            var pinnedHandle = GCHandle.Alloc(outputArr, GCHandleType.Pinned);
            try
            {
                byte* outBase = (byte*)pinnedHandle.AddrOfPinnedObject();

                KvKey key = default;
                long k = 0;
                while (k < keyCount)
                {
                    long batchEnd = Math.Min(k + kBatch, keyCount);
                    int slot = 0;

                    for (long kk = k; kk < batchEnd; ++kk, ++slot)
                    {
                        int writerThread = (int)((double)kk / keyCount * loadThreads);
                        if (writerThread >= loadThreads) writerThread = loadThreads - 1;

                        key.Value = kk;
                        var outSpan = new Span<byte>(outBase + slot * KvSessionFunctions.kReaderCopyBytes, KvSessionFunctions.kReaderCopyBytes);
                        var output = SpanByteAndMemory.FromPinnedSpan(outSpan);
                        var st = bContext.Read(key, ref pinnedInput, ref output, Empty.Default);
                        if (st.IsPending)
                        {
                            // Will be checked in the drain pass via KeyBytes lookup.
                            continue;
                        }
                        if (!st.Found) { ++misses; continue; }
                        for (int i = 0; i < nCmp; i++)
                        {
                            var expected = (byte)((writerThread * 31 + i) & 0xFF);
                            if (outSpan[i] != expected) { ++mismatches; break; }
                        }
                    }

                    // Drain any async-completed reads and verify each one.
                    bContext.CompletePendingWithOutputs(out var completed, wait: true);
                    while (completed.Next())
                    {
                        ref var cr = ref completed.Current;
                        if (!cr.Status.Found) { ++misses; cr.Output.Dispose(); continue; }

                        // Reconstruct writer thread from the key value.
                        var kb = cr.Key.KeyBytes;
                        long completedKey = MemoryMarshal.Read<long>(kb);
                        int writerThread = (int)((double)completedKey / keyCount * loadThreads);
                        if (writerThread >= loadThreads) writerThread = loadThreads - 1;

                        var outSpan = cr.Output.SpanByte.Span;
                        for (int i = 0; i < nCmp; i++)
                        {
                            var expected = (byte)((writerThread * 31 + i) & 0xFF);
                            if (outSpan[i] != expected) { ++mismatches; break; }
                        }
                        cr.Output.Dispose();
                    }
                    completed.Dispose();

                    k = batchEnd;
                }
            }
            finally
            {
                pinnedHandle.Free();
            }

            return (mismatches, misses);
        }
    }
}