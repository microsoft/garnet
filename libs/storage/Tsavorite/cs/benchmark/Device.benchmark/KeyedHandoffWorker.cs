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
    /// Scaffolding that models Tsavorite's pending-read handoff WITHOUT the Tsavorite engine
    /// (no hash chains, no log allocator). It keeps only the structures the
    /// Read()->CompletePending() pattern needs:
    ///   * a shared, read-only key->address dictionary (stands in for the hash index),
    ///   * a per-worker pending dictionary + readyResponses queue, where the device completion
    ///     thread enqueues finished ops and the worker thread drains them in CompletePending, and
    ///   * (optionally) the shared LightEpoch, either Resumed/Suspended per op (like BasicContext.Read)
    ///     or held across the whole chunk.
    /// Layers are toggled by flags so we can see which one breaks device.bench's scalability.
    /// </summary>
    sealed unsafe class KeyedHandoffWorker : BenchWorkerBase
    {
        sealed class Op
        {
            public long id;
            public void* buffer;     // device read destination (sector-aligned)
            public void* output;     // optional copy-out destination (models Reader copy)
        }

        // off = no epoch; perOp = Resume/Suspend each op (BasicContext.Read); hold = Resume once per chunk.
        internal enum EpochMode { Off, PerOp, Hold }

        readonly int batchSize, sectorSize;
        readonly long numKeys;
        readonly bool copyOut;
        readonly EpochMode epochMode;
        readonly IDevice device;
        readonly long[] keyToAddr;       // shared, read-only: key -> device byte offset
        readonly LightEpoch epoch;       // shared; null when epochMode == Off
        readonly ManualResetEventSlim startEvent, timeUpEvent, doneEvent;
        readonly uint seed;

        // Per-worker handoff state (mirrors Tsavorite's per-session ExecutionContext).
        readonly ConcurrentQueue<Op> readyResponses = new();
        readonly Dictionary<long, Op> ioPending = new();
        readonly Stack<Op> pool = new();
        long nextId;

        public KeyedHandoffWorker(int batchSize, int threadId, int sectorSize, long numKeys, bool copyOut,
            EpochMode epochMode, long[] keyToAddr, LightEpoch epoch, IDevice device,
            ManualResetEventSlim startEvent, ManualResetEventSlim timeUpEvent, ManualResetEventSlim doneEvent)
        {
            this.batchSize = batchSize;
            this.sectorSize = sectorSize;
            this.numKeys = numKeys;
            this.copyOut = copyOut;
            this.epochMode = epochMode;
            this.keyToAddr = keyToAddr;
            this.epoch = epoch;
            this.device = device;
            this.startEvent = startEvent;
            this.timeUpEvent = timeUpEvent;
            this.doneEvent = doneEvent;
            this.seed = (uint)(threadId + 1);
            for (int i = 0; i < batchSize; i++)
            {
                var op = new Op
                {
                    buffer = NativeMemory.AlignedAlloc((nuint)sectorSize, (nuint)sectorSize),
                    output = copyOut ? NativeMemory.AlignedAlloc((nuint)sectorSize, (nuint)sectorSize) : null,
                };
                pool.Push(op);
            }
        }

        // Device completion thread runs this: hand the finished op off to the worker thread.
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

        // Drain finished ops back to the pool (the Read continuation). Caller owns epoch protection.
        void DrainReady()
        {
            device.TryComplete();
            while (readyResponses.TryDequeue(out var op))
            {
                ioPending.Remove(op.id);
                if (copyOut)
                    Buffer.MemoryCopy(op.buffer, op.output, sectorSize, sectorSize);
                localCompletedOk++;
                pool.Push(op);
            }
        }

        // Issue the device read for a reserved op. Caller owns epoch protection.
        void IssueRead(Op op, long key)
        {
            op.id = nextId++;
            ioPending[op.id] = op;
            while (device.Throttle()) Thread.Yield();
            device.ReadAsync((ulong)keyToAddr[key], (IntPtr)op.buffer, (uint)sectorSize, Callback, op);
        }

        public override void Run()
        {
            uint x = seed == 0 ? 1u : seed, y = 362436069u, z = 521288629u, w = 88675123u;
            long submitted = 0;
            bool hold = epochMode == EpochMode.Hold;
            bool perOp = epochMode == EpochMode.PerOp;
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
                        long key = (long)(((ulong)w * (ulong)numKeys) >> 32);  // Lemire fast-mod into [0,numKeys)

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