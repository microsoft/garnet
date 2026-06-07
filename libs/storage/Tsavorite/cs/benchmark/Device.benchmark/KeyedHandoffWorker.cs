// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Device.benchmark
{
    /// <summary>
    /// Shared, read-only-at-run state for the keyed scaffolding: the key->address map (direct array
    /// or open-addressing hash table) and an optional shared read-buffer pool.
    /// </summary>
    sealed class SharedKeyed
    {
        public long[] keyToAddr;                       // direct array (default)
        public long[] htKey, htAddr;                   // open-addressing hash table (--hash-index)
        public int hashMask;
        public ConcurrentQueue<IntPtr> bufPool;        // shared read-buffer pool (--buf-pool)

        public long Lookup(long key)
        {
            if (htKey is null)
                return keyToAddr[key];
            // Tsavorite-style hash walk: mix, then probe the shared table.
            ulong h = (ulong)key * 0x9E3779B97F4A7C15UL;
            h ^= h >> 29;
            int i = (int)h & hashMask;
            while (htKey[i] != key + 1) i = (i + 1) & hashMask;
            return htAddr[i];
        }
    }

    /// <summary>
    /// Scaffolding that models Tsavorite's pending-read path WITHOUT the Tsavorite engine. Each layer
    /// is a toggle so we can see which one breaks device.bench's scalability:
    ///   handoff (dict + readyResponses) | --hash-index | --big-ctx (PendingContext-sized struct) |
    ///   --pool-ctx (pooled AsyncIOContext) | --buf-pool (shared SectorAlignedBufferPool) | epoch.
    /// </summary>
    sealed unsafe class KeyedHandoffWorker : BenchWorkerBase
    {
        // ~192-byte struct modelling PendingContext stored by value in ioPendingRequests.
        struct BigCtx { public void* buf; public fixed long pad[23]; }

        // Pooled per-op context modelling AsyncIOContext (a rented class).
        sealed class Ctx { public long id; public void* record; public long a, b, c, d, e; }

        sealed class Op
        {
            public long id;
            public void* buffer;     // own buffer (used when not --buf-pool)
            public void* output;     // optional copy-out destination
            public IntPtr readBuf;   // buffer actually handed to ReadAsync (own or pool-rented)
            public Ctx ctx;          // rented context (when --pool-ctx)
        }

        internal enum EpochMode { Off, PerOp, Hold }

        readonly int batchSize, sectorSize;
        readonly long numKeys;
        readonly bool copyOut, bigCtx, poolCtx, tlsBufPool;
        readonly EpochMode epochMode;
        readonly IDevice device;
        readonly SharedKeyed shared;
        readonly LightEpoch epoch;
        readonly ManualResetEventSlim startEvent, timeUpEvent, doneEvent;
        readonly uint seed;

        readonly ConcurrentQueue<Op> readyResponses = new();
        readonly Dictionary<long, Op> ioPending = new();
        readonly Dictionary<long, BigCtx> bigPending = new();
        readonly Stack<Op> pool = new();
        readonly Stack<Ctx> ctxPool = new();
        readonly Stack<IntPtr> myBufPool = new();   // per-worker buffer pool (--buf-pool-tls)
        long nextId;

        public KeyedHandoffWorker(int batchSize, int threadId, int sectorSize, long numKeys, bool copyOut,
            bool bigCtx, bool poolCtx, bool tlsBufPool, EpochMode epochMode, SharedKeyed shared, LightEpoch epoch, IDevice device,
            ManualResetEventSlim startEvent, ManualResetEventSlim timeUpEvent, ManualResetEventSlim doneEvent)
        {
            this.batchSize = batchSize;
            this.sectorSize = sectorSize;
            this.numKeys = numKeys;
            this.copyOut = copyOut;
            this.bigCtx = bigCtx;
            this.poolCtx = poolCtx;
            this.tlsBufPool = tlsBufPool;
            this.epochMode = epochMode;
            this.shared = shared;
            this.epoch = epoch;
            this.device = device;
            this.startEvent = startEvent;
            this.timeUpEvent = timeUpEvent;
            this.doneEvent = doneEvent;
            this.seed = (uint)(threadId + 1);
            for (int i = 0; i < batchSize; i++)
            {
                pool.Push(new Op
                {
                    buffer = NativeMemory.AlignedAlloc((nuint)sectorSize, (nuint)sectorSize),
                    output = copyOut ? NativeMemory.AlignedAlloc((nuint)sectorSize, (nuint)sectorSize) : null,
                });
                if (poolCtx) ctxPool.Push(new Ctx());
            }
        }

        IntPtr RentBuf(void* ownBuffer)
        {
            if (tlsBufPool)
                return myBufPool.TryPop(out var tp) ? tp : (IntPtr)NativeMemory.AlignedAlloc((nuint)sectorSize, (nuint)sectorSize);
            if (shared.bufPool is null)
                return (IntPtr)ownBuffer;
            return shared.bufPool.TryDequeue(out var p) ? p : (IntPtr)NativeMemory.AlignedAlloc((nuint)sectorSize, (nuint)sectorSize);
        }

        void Callback(uint errorCode, uint numBytes, object ctx)
        {
            var op = (Op)ctx;
            if (errorCode != 0)
            {
                localErrors++;
                if (errorCode < (uint)localErrorCounts.Length)
                    localErrorCounts[errorCode]++;
            }
            readyResponses.Enqueue(op);
        }

        void DrainReady()
        {
            device.TryComplete();
            while (readyResponses.TryDequeue(out var op))
            {
                ioPending.Remove(op.id);
                if (bigCtx) bigPending.Remove(op.id);
                if (poolCtx) { ctxPool.Push(op.ctx); op.ctx = null; }
                if (tlsBufPool) myBufPool.Push(op.readBuf);
                else if (shared.bufPool is not null) shared.bufPool.Enqueue(op.readBuf);
                if (copyOut) Buffer.MemoryCopy((void*)op.readBuf, op.output, sectorSize, sectorSize);
                localCompletedOk++;
                pool.Push(op);
            }
        }

        void IssueRead(Op op, long key)
        {
            long addr = shared.Lookup(key);
            op.id = nextId++;
            op.readBuf = RentBuf(op.buffer);
            ioPending[op.id] = op;
            if (bigCtx) bigPending[op.id] = new BigCtx { buf = (void*)op.readBuf };
            if (poolCtx)
            {
                var c = ctxPool.Count > 0 ? ctxPool.Pop() : new Ctx();
                c.id = op.id; c.record = (void*)op.readBuf; c.a = addr; c.b = op.id; c.c = c.d = c.e = addr;
                op.ctx = c;
            }
            while (device.Throttle()) Thread.Yield();
            device.ReadAsync((ulong)addr, op.readBuf, (uint)sectorSize, Callback, op);
        }

        public override void Run()
        {
            uint x = seed == 0 ? 1u : seed, y = 362436069u, z = 521288629u, w = 88675123u;
            long submitted = 0;
            bool hold = epochMode == EpochMode.Hold, perOp = epochMode == EpochMode.PerOp;
            try
            {
                startEvent.Wait();
                while (!timeUpEvent.IsSet)
                {
                    if (hold) { epoch.Resume(); epoch.ProtectAndDrain(); }
                    DrainReady();
                    for (int i = 0; i < batchSize; i++)
                    {
                        Op op;
                        while (!pool.TryPop(out op))
                        {
                            DrainReady();
                            if (hold) epoch.ProtectAndDrain();
                        }

                        uint t = x ^ (x << 11);
                        x = y; y = z; z = w;
                        w = (w ^ (w >> 19)) ^ (t ^ (t >> 8));
                        long key = (long)(((ulong)w * (ulong)numKeys) >> 32);

                        if (perOp)
                        {
                            epoch.Resume();
                            try { IssueRead(op, key); }
                            finally { epoch.Suspend(); }
                        }
                        else
                        {
                            IssueRead(op, key);
                        }
                        submitted++;
                    }
                    if (hold) epoch.Suspend();
                }
            }
            finally
            {
                if (hold) epoch.Resume();
                while (ioPending.Count > 0 || readyResponses.Count > 0)
                {
                    DrainReady();
                    if (hold) epoch.ProtectAndDrain();
                }
                if (hold) epoch.Suspend();
                _ = Interlocked.Add(ref Program.totalSubmitted, submitted);
                doneEvent.Set();
            }
        }
    }
}