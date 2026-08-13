// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Regression tests for concurrent VADD to a vector set whose records spill to the object log.
    /// Concurrent inserts historically deadlocked: a server VADD that recreates a disk-tiered index blocks
    /// on a pending disk read while holding the vector-set lock exclusively, and the quantization workers,
    /// which run on .NET thread-pool threads, spin-waited on that same lock and consumed every pool thread so
    /// the disk-read completion (also a pool work item) could never be scheduled. The fix makes the quantization
    /// workers acquire the lock non-blockingly and yield their pool thread (await) on contention instead of
    /// spin-waiting, so a disk-IO completion can always be scheduled and release the lock.
    /// </summary>
    [TestFixture]
    public class ConcurrentVaddDiskSpillTests : TestBase
    {
        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        static byte[] Vec(Random r, int dim)
        {
            var v = new float[dim];
            double n = 0;
            for (var i = 0; i < dim; i++) { v[i] = (float)(r.NextDouble() * 2 - 1); n += (double)v[i] * v[i]; }
            n = Math.Sqrt(n);
            for (var i = 0; i < dim; i++) v[i] = (float)(v[i] / n);
            return MemoryMarshal.Cast<float, byte>(v.AsSpan()).ToArray();
        }

        long done;

        /// <summary>
        /// Liveness smoke test: concurrent VADD to a single set whose ~8 KB DiskANN records spill to the object
        /// log (the 4 KB-page lowMemory helper) must keep making forward progress. The historical deadlock stalls
        /// all workers permanently; progress is otherwise continuous (disk-bound but always moving), so a sustained
        /// no-progress window flags the hang.
        /// </summary>
        [Test]
        public void ConcurrentVaddToSpilledSetMakesProgress()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, enableVectorSetPreview: true);
            server.Start();

            const int threads = 8, dim = 32;
            const int runSeconds = 60;
            const int stallLimitSeconds = 45;
            var cfg = TestUtils.GetConfig(allowAdmin: true);
            cfg.SyncTimeout = 60000;
            using var redis = ConnectionMultiplexer.Connect(cfg);

            var stop = new CancellationTokenSource();
            var deadline = DateTime.UtcNow.AddSeconds(runSeconds);
            var workers = new Task[threads];
            for (var t = 0; t < threads; t++)
            {
                var tid = t;
                workers[tid] = Task.Run(() =>
                {
                    var db = redis.GetDatabase(0);
                    var r = new Random(tid);
                    var id = new byte[4];
                    var k = 0;
                    while (!stop.IsCancellationRequested && DateTime.UtcNow < deadline)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(id, tid * 10_000_000 + k++);
                        db.Execute("VADD", ["hk", "FP32", Vec(r, dim), (byte[])id.Clone(), "BIN", "EF", "64", "M", "16", "XDISTANCE_METRIC", "COSINE"]);
                        Interlocked.Increment(ref done);
                    }
                });
            }

            long last = 0;
            var stalledSeconds = 0;
            while (true)
            {
                var allDone = true;
                foreach (var w in workers)
                    if (!w.IsCompleted) allDone = false;
                if (allDone) break;
                Thread.Sleep(2000);
                var cur = Interlocked.Read(ref done);
                stalledSeconds = cur == last ? stalledSeconds + 2 : 0;
                last = cur;
                if (stalledSeconds >= stallLimitSeconds)
                {
                    stop.Cancel();
                    Assert.Fail($"DEADLOCK: {threads} concurrent VADD workers made no progress for {stalledSeconds}s at {cur} inserts " +
                                "(a server VADD is blocked in VectorManager.ReadCallbackUnmanaged on a pending-read completion that is starved of a thread).");
                }
            }
            stop.Cancel();

            Task.WaitAll(workers);
            Assert.That(Interlocked.Read(ref done), Is.GreaterThan(1000),
                "Workers did not perform enough inserts to exercise the object-log spill path.");
        }
    }
}