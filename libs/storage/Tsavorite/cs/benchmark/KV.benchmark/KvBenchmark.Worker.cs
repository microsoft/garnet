// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
#pragma warning disable IDE0065 // Misplaced using directive
    using KvStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;
    using KvAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>>;

    // Per-thread worker entrypoint + RUN-phase hot loop.
    // Kept in its own file so the hot path is easy to reason about and review:
    // any change here is liable to move ops/sec.
    public sealed partial class KvBenchmark
    {
        /// <summary>Global chunk counter for the RUN phase (Interlocked.Add per chunk).</summary>
        long globalChunkIdx;

        /// <summary>
        /// Per-thread worker. Owns its Tsavorite session, picks the load or run path,
        /// and writes its final counters into <paramref name="stats"/>.
        /// </summary>
        unsafe void WorkerProc(int threadIdx, bool isLoad, WorkerStats stats, CountdownEvent ready)
        {
            Pinning.PinWorker(threadIdx);

            using var session = store.NewSession<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions>(functions);
            var bContext = session.BasicContext;

            ref var mySlot = ref scoreboard[threadIdx + 1];
            ref var doneFlag = ref doneBox[0].Value;

            // Load partition (only meaningful when isLoad=true).
            long loadFrom = 0, loadTo = 0;
            if (isLoad)
            {
                loadFrom = (long)((double)Options.Keys * threadIdx / Options.Threads);
                loadTo = (long)((double)Options.Keys * (threadIdx + 1) / Options.Threads);
            }

            ready.Signal();
            gate.Wait();

            long localOps = 0;
            long reads = 0, writes = 0, deletes = 0;
            var allocBefore = GC.GetAllocatedBytesForCurrentThread();

            if (isLoad)
            {
                // Each thread Upserts its partition deterministically. Single small buffer suffices.
                byte* valuePtr = stackalloc byte[Options.ValueSize];
                for (int i = 0; i < Options.ValueSize; i++)
                    valuePtr[i] = (byte)((threadIdx * 31 + i) & 0xFF);
                var valueSpan = new Span<byte>(valuePtr, Options.ValueSize);

                KvKey key = default;
                for (long k = loadFrom; k < loadTo; k++)
                {
                    key.Value = k;
                    bContext.Upsert(key, valueSpan, Empty.Default);
                    ++localOps;
                    if ((localOps & (kChunkSize - 1)) == 0)
                    {
                        if ((localOps & 65535) == 0)
                            bContext.CompletePending(false);
                        Volatile.Write(ref mySlot.Value, localOps);
                    }
                }
                bContext.CompletePending(true);
                writes = localOps;
            }
            else
            {
                var deleteReinsert = Options.RumdHasDeletes();

                // Hot-path buffers: stackalloc'd HERE (caller of RunWorkload) and passed in by ref.
                // Two design points:
                //  1. Stackalloc here (not inside RunWorkload) keeps the hot-loop method body small
                //     enough for the JIT to inline BasicContext.Read into it (~3.7% measured).
                //  2. Each buffer is overallocated by 31 B and rounded up to a 32-byte boundary
                //     so Reader's `Slice(0, 32).CopyTo(...)` -> single 32-byte AVX2 vmovdqu has an
                //     aligned destination. (Source side stays unaligned: Tsavorite records have
                //     a 21-byte header before the value, which can't be fixed here. Measured
                //     ~+5% at memory-bound scale.)
                byte* valueRaw = stackalloc byte[Options.ValueSize + 31];
                byte* valuePtrD = (byte*)(((nuint)valueRaw + 31u) & ~(nuint)31u);
                byte* inputRaw = stackalloc byte[Options.ValueSize + 31];
                byte* inputPtrD = (byte*)(((nuint)inputRaw + 31u) & ~(nuint)31u);
                byte* outputRaw = stackalloc byte[KvSessionFunctions.kReaderCopyBytes + 31];
                byte* outputPtrD = (byte*)(((nuint)outputRaw + 31u) & ~(nuint)31u);
                for (int i = 0; i < Options.ValueSize; i++)
                    valuePtrD[i] = (byte)((threadIdx * 31 + i) & 0xFF);
                for (int i = 0; i < Options.ValueSize; i++)
                    inputPtrD[i] = (byte)((threadIdx * 17 + i + 1) & 0xFF);
                var valueSpanD = new Span<byte>(valuePtrD, Options.ValueSize);
                var inputSpanD = new Span<byte>(inputPtrD, Options.ValueSize);
                var outputSpanD = new Span<byte>(outputPtrD, KvSessionFunctions.kReaderCopyBytes);
                var pinnedInputD = PinnedSpanByte.FromPinnedSpan(inputSpanD);
                var outputD = SpanByteAndMemory.FromPinnedSpan(outputSpanD);

                (localOps, reads, writes, deletes) = RunWorkload(
                    bContext, ref mySlot, ref doneFlag,
                    Options.Keys, Options.UseZipf,
                    valueSpanD, ref pinnedInputD, ref outputD,
                    Options.ReadPct, Options.UpsertPctCumulative, Options.RmwPctCumulative,
                    deleteReinsert,
                    seed: (uint)(Options.Seed) + (uint)(threadIdx + 1));
            }

            // Final scoreboard tick so the post-Join sum captures the in-flight chunk.
            Volatile.Write(ref mySlot.Value, localOps);

            var allocAfter = GC.GetAllocatedBytesForCurrentThread();
            stats.LocalOps = localOps;
            stats.Reads = reads;
            stats.Writes = writes;
            stats.Deletes = deletes;
            stats.AllocBytesDelta = allocAfter - allocBefore;
            stats.FinalExitTicks = Stopwatch.GetTimestamp();
        }

        /// <summary>
        /// RUN-phase hot loop. Structure (kept identical for all rumd ratios and distributions):
        /// <list type="bullet">
        ///   <item>Per-chunk: <c>Interlocked.Add</c> on <see cref="globalChunkIdx"/>, then iterate kChunkSize ops.</item>
        ///   <item>Per-op: inline xorshift32 key generation (bitmask if keyCount is a power of two,
        ///         else Lemire's fast modulo; zipf path delegates to <see cref="ZipfGenerator"/>).</item>
        ///   <item>Per-op: SECOND independent xorshift32 coin toss for op selection — independence is
        ///         MANDATORY when distribution=zipf because zipf consumes its source RNG non-uniformly.</item>
        ///   <item>Per-op: compare coin toss against pre-computed 32-bit cutoffs (no per-op multiply or divide).</item>
        ///   <item>Per-op: chained <c>if (...) { ...; continue; }</c> branches; delete path optionally re-Upserts.</item>
        ///   <item>Per-chunk: <see cref="Volatile.Write{T}"/> the running total into the per-thread scoreboard slot.</item>
        ///   <item>Done flag checked at chunk boundary only (worst-case stop lag: one chunk ≈ 0.3 ms).</item>
        /// </list>
        /// Returns (localOps, reads, writes, deletes). Reinserts are counted under writes so they appear
        /// in both the live scoreboard sum and the final per-phase total.
        /// </summary>
        unsafe (long localOps, long reads, long writes, long deletes) RunWorkload(
            Tsavorite.core.BasicContext<KvKey, PinnedSpanByte, SpanByteAndMemory, Empty, KvSessionFunctions, KvStoreFunctions, KvAllocator> bContext,
            ref PaddedLong slot, ref bool doneFlag,
            long keyCount, bool useZipf,
            Span<byte> value, ref PinnedSpanByte pinnedInputSpan, ref SpanByteAndMemory _output,
            int readPercent, int upsertPercent, int rmwPercent,
            bool deleteReinsert,
            uint seed)
        {
            // RNG #1 — key generation. RNG #2 — op-select. Seeded distinctly so they're independent.
            uint xk = seed == 0 ? 1u : seed;
            uint yk = 362436069u, zk = 521288629u, wk = 88675123u;
            uint xr = (seed == 0 ? 1u : seed) ^ 0x9E3779B9u;
            if (xr == 0) xr = 0x9E3779B9u;
            uint yr = 362436069u, zr = 521288629u, wr = 88675123u;

            // Zipf needs a XoshiroRng struct; seeded from the keygen state.
            var rngStruct = new XoshiroRng(((ulong)xk << 32) | wk);
            var zipf = useZipf ? new ZipfGenerator(ZipfConstants) : default;

            // Hoist all per-thread invariants out of the hot loop.
            bool fast32 = keyCount > 0 && keyCount <= uint.MaxValue;
            uint keyCount32 = fast32 ? (uint)keyCount : 0u;
            bool keysPow2 = fast32 && (keyCount32 & (keyCount32 - 1)) == 0;
            uint keyMask32 = fast32 ? keyCount32 - 1 : 0u;

            // Pre-compute 32-bit op-select cutoffs so the coin toss is a single compare per op.
            // pct=100 -> cutoff = 2^32 (always > any uint), so the branch is always taken.
            ulong readCutoff = ((ulong)(uint)readPercent << 32) / 100;
            ulong upsertCutoff = ((ulong)(uint)upsertPercent << 32) / 100;
            ulong rmwCutoff = ((ulong)(uint)rmwPercent << 32) / 100;

            long reads_done = 0, writes_done = 0, deletes_done = 0;
            KvKey key = default;  // hoisted; avoids per-op 16-byte zero-init.

            while (!Volatile.Read(ref doneFlag))
            {
                long chunk_idx = Interlocked.Add(ref globalChunkIdx, kChunkSize) - kChunkSize;
                long chunk_end = chunk_idx + kChunkSize;
                for (long idx = chunk_idx; idx < chunk_end; ++idx)
                {
                    if (idx % 512 == 0)
                        bContext.CompletePending(false);

                    // ===== Key generation (one of three paths, hoisted bools select the right one) =====
                    if (useZipf)
                    {
                        long kk = zipf.Next(ref rngStruct);
                        if (kk >= keyCount) kk = keyCount - 1;
                        key.Value = kk;
                    }
                    else if (fast32)
                    {
                        uint tk = xk ^ (xk << 11);
                        xk = yk; yk = zk; zk = wk;
                        wk = (wk ^ (wk >> 19)) ^ (tk ^ (tk >> 8));
                        key.Value = keysPow2
                            ? (long)(wk & keyMask32)
                            : (long)(uint)(((ulong)wk * keyCount32) >> 32);  // Lemire fast-mod
                    }
                    else
                    {
                        // 64-bit keyCount: combine two xorshift32 advances.
                        uint t1 = xk ^ (xk << 11);
                        xk = yk; yk = zk; zk = wk;
                        ulong rhi = (wk = (wk ^ (wk >> 19)) ^ (t1 ^ (t1 >> 8)));
                        uint t2 = xk ^ (xk << 11);
                        xk = yk; yk = zk; zk = wk;
                        ulong rlo = (wk = (wk ^ (wk >> 19)) ^ (t2 ^ (t2 >> 8)));
                        key.Value = (long)(((rhi << 32) | rlo) % (ulong)keyCount);
                    }

                    // ===== Op selection (independent RNG; compare against precomputed cutoffs) =====
                    uint tr = xr ^ (xr << 11);
                    xr = yr; yr = zr; zr = wr;
                    wr = (wr ^ (wr >> 19)) ^ (tr ^ (tr >> 8));
                    ulong rcoin = wr;

                    if (rcoin < readCutoff)
                    {
                        bContext.Read(key, ref pinnedInputSpan, ref _output, Empty.Default);
                        ++reads_done;
                        continue;
                    }
                    if (rcoin < upsertCutoff)
                    {
                        bContext.Upsert(key, value, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    if (rcoin < rmwCutoff)
                    {
                        bContext.RMW(key, ref pinnedInputSpan, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    bContext.Delete(key, Empty.Default);
                    ++deletes_done;
                    if (deleteReinsert)
                    {
                        bContext.Upsert(key, value, Empty.Default);
                        ++writes_done;
                    }
                }

                Volatile.Write(ref slot.Value, reads_done + writes_done + deletes_done);
            }
            bContext.CompletePending(true);
            long localOps = reads_done + writes_done + deletes_done;
            return (localOps, reads_done, writes_done, deletes_done);
        }
    }
#pragma warning restore IDE0065
}
