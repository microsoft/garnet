// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
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
    /// which ran on .NET thread-pool threads and spin-wait on that same lock, consumed every pool thread so
    /// the disk-read completion (also a pool work item) could never be scheduled. The fix runs the
    /// quantization workers on dedicated threads so their spin can never starve the pool.
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

        /// <summary>
        /// Directly guards the fix: quantization work must never execute on a thread-pool thread, because it
        /// spin-waits on the per-set lock and would otherwise starve the pool of the disk-IO completion that
        /// releases that lock. Inserts enough BIN-quantized vectors to force a real quantization table build
        /// (which proves the workers actually ran), then asserts none of that work happened on the pool.
        /// </summary>
        [Test]
        public void QuantizationDoesNotRunOnThreadPoolThreads()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableVectorSetPreview: true);
            server.Start();

            var vectorManager = server.Provider.StoreWrapper.DefaultDatabase.VectorManager;
            var buildsAtStart = vectorManager.QuantizationRequestsProcessed;

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int vectors = 2000, dim = 32;
            var r = new Random(1);
            var id = new byte[4];
            for (var k = 0; k < vectors; k++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(id, k);
                db.Execute("VADD", ["hk", "FP32", Vec(r, dim), (byte[])id.Clone(), "BIN", "M", "16"]);
            }

            // Wait for a successful quantization table build so the assertion below is non-vacuous
            // (a build increments the counter only after a worker has dequeued and processed the request).
            var sw = Stopwatch.StartNew();
            while (vectorManager.QuantizationRequestsProcessed == buildsAtStart)
            {
                Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromSeconds(60)), "Quantization table build did not complete in time.");
                Thread.Sleep(200);
            }

            Assert.That(vectorManager.QuantizationRanOnThreadPoolThread, Is.False,
                "Quantization ran on a thread-pool thread; it must run on dedicated threads so its lock spin-wait " +
                "cannot starve the pool of the disk-IO completion that releases the vector-set lock (deadlock).");
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

        long updateDone;
        volatile string serverDied;

        /// <summary>
        /// Regression guard for the object-log flush overflow crash reported in
        /// https://gist.github.com/badrishc/1da9f5175490b3cbd74b93c89f03cb6e. With a tiny main log and a 16 KB page
        /// (<c>--memory 1m --page 16k --index 16m --storage-tier</c>), each ~8 KB DiskANN node record spills to the
        /// object log as an overflow value. Concurrent VADD from several connections turns pages (which shifts the
        /// read-only address and flushes them) while other inserts update neighbor nodes, eliding the prior
        /// read-only-but-not-yet-flushed record versions and freeing the overflow byte[] behind them. The page
        /// flush captured that overflow and then re-derived its on-disk length from the now-freed <c>objectIdMap</c>
        /// slot, dereferencing a null <c>OverflowByteArray</c> (<c>get_Length</c>) and taking down the flush
        /// thread — the server aborted after a few thousand inserts. The fix takes the length from the
        /// epoch-captured overflow and skips any record whose heap was concurrently freed, so the flush can never
        /// dereference a freed slot. This exact configuration must now run to completion with the server alive.
        /// </summary>
        [Test]
        public void ConcurrentVaddSpilledToObjectLogDoesNotCrashFlush()
        {
            // Match the gist's deterministic crash configuration: tiny memory + 16k page forces the ~8 KB vector
            // records into the object log (overflow); storage tier is enabled because MethodTestDir is the log dir.
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                memorySize: "1m", pageSize: "16k", indexSize: "16m", enableVectorSetPreview: true);
            server.Start();

            const int threads = 8, dim = 32;
            // Duration-bounded so the test can never hang: without the fix the flush thread crashes within the first
            // second (the gist aborts after ~2.8k inserts), so a short window still clears the crash region with wide
            // margin while keeping the run fast. The stall monitor separately fails a silent hang.
            const int runSeconds = 12;
            const int stallLimitMs = 8000;

            var cfg = TestUtils.GetConfig(allowAdmin: true);
            cfg.SyncTimeout = 60000;

            var stop = new CancellationTokenSource();
            var deadline = DateTime.UtcNow.AddSeconds(runSeconds);

            // Each worker plus the monitor get their OWN connection so the server processes the VADDs on independent
            // RespServerSessions truly concurrently. A single shared ConnectionMultiplexer multiplexes every command
            // onto one TCP connection, so the server would serialize them on one session and the concurrent
            // flush-vs-elide race that triggers the crash could never occur (the gist's repro.py uses 8 connections).
            var muxes = new ConnectionMultiplexer[threads + 1];
            for (var i = 0; i < muxes.Length; i++) muxes[i] = ConnectionMultiplexer.Connect(cfg);
            try
            {
                var workers = new Task[threads];
                for (var t = 0; t < threads; t++)
                {
                    var tid = t;
                    workers[tid] = Task.Run(() =>
                    {
                        var db = muxes[tid].GetDatabase(0);
                        var r = new Random(tid);
                        var id = new byte[4];
                        var k = 0;
                        while (!stop.IsCancellationRequested && DateTime.UtcNow < deadline)
                        {
                            // Growing IDs (like the gist's load) keep turning pages so the object log flushes continuously,
                            // while the neighbor updates each insert performs elide prior read-only record versions.
                            BinaryPrimitives.WriteInt32LittleEndian(id, tid * 10_000_000 + k++);
                            try
                            {
                                db.Execute("VADD", ["hk", "FP32", Vec(r, dim), (byte[])id.Clone(), "BIN", "EF", "64", "M", "16", "XDISTANCE_METRIC", "COSINE"]);
                                Interlocked.Increment(ref updateDone);
                            }
                            catch (RedisConnectionException e)
                            {
                                // The flush-thread NRE takes the server down; surfaced here as a dropped connection.
                                serverDied ??= $"VADD saw a dropped connection (server flush thread crashed): {e.Message}";
                                stop.Cancel();
                                return;
                            }
                            catch (RedisException)
                            {
                                // Transient server-side errors (e.g. timeouts under disk pressure) are not a crash; keep going.
                            }
                        }
                    });
                }

                // Liveness/stall monitor on an independent connection: a healthy server answers PING throughout. If the
                // flush thread died, the connection drops and PING throws; if the store deadlocks, progress stalls.
                var monitor = Task.Run(() =>
                {
                    var mdb = muxes[threads].GetDatabase(0);
                    long last = 0;
                    var stalledMs = 0;
                    while (!stop.IsCancellationRequested && DateTime.UtcNow < deadline)
                    {
                        try
                        {
                            _ = mdb.Execute("PING");
                        }
                        catch (RedisConnectionException e)
                        {
                            serverDied ??= $"PING saw a dropped connection (server flush thread crashed): {e.Message}";
                            stop.Cancel();
                            return;
                        }
                        catch (RedisException)
                        {
                            // Ignore transient timeouts.
                        }
                        var cur = Interlocked.Read(ref updateDone);
                        stalledMs = cur == last ? stalledMs + 500 : 0;
                        last = cur;
                        if (stalledMs >= stallLimitMs)
                        {
                            serverDied ??= $"Concurrent VADD made no progress for {stalledMs} ms at {cur} inserts " +
                                "(deadlock in the object-log read/flush path).";
                            stop.Cancel();
                            return;
                        }
                        Thread.Sleep(500);
                    }
                });

                Task.WaitAll(workers);
                stop.Cancel();
                monitor.Wait();
            }
            finally
            {
                foreach (var m in muxes) m?.Dispose();
            }

            Assert.That(serverDied, Is.Null, serverDied);
            Assert.That(Interlocked.Read(ref updateDone), Is.GreaterThan(500),
                "Workers did not perform enough inserts to exercise the object-log flush/elide race.");

            // Final integrity check on a fresh connection: a live, consistent server answers PING and serves a
            // similarity query over the spilled set without error.
            using var verify = ConnectionMultiplexer.Connect(cfg);
            var vdb = verify.GetDatabase(0);
            Assert.That((string)vdb.Execute("PING"), Is.EqualTo("PONG"), "Server did not respond to PING after the insert load.");
            var probe = new Random(12345);
            Assert.DoesNotThrow(() => vdb.Execute("VSIM", ["hk", "FP32", Vec(probe, dim), "COUNT", "10", "EF", "64"]),
                "VSIM over the spilled set failed after concurrent inserts, indicating flush/store corruption.");
        }
    }
}