// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Repros for BfTree lifecycle-vs-data-op concurrency bugs that are NOT migration-specific:
    /// a whole-key <c>DEL</c> drives the exact same <c>DisposeTreeUnderLock</c> path as
    /// <c>MigrateOperation.DeleteRangeIndex</c>, so concurrent <c>RI.SET</c> + whole-key <c>DEL</c>
    /// exercises the same drop/insert interaction discovered during cluster-migration testing.
    ///
    /// <para>Both tests are <see cref="ExplicitAttribute"/>: they currently crash or hang by
    /// design (they reproduce open bugs) and must not run in CI until the underlying issues are
    /// fixed. They are intended to become passing regression tests once the fixes land.</para>
    /// </summary>
    [TestFixture]
    public class RangeIndexConcurrentDeleteRepro : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableRangeIndexPreview: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Race repro: concurrent <c>RI.SET</c> while another client repeatedly does a whole-key
        /// <c>DEL</c> (which drops the native BfTree via a deferred epoch dispose) + <c>RI.CREATE</c>.
        /// The deferred <c>bftree_drop</c> can race a concurrent <c>InsertByPtr</c> that runs under
        /// the shared lock but OUTSIDE epoch protection — a native use-after-free
        /// (bf-tree panic: <c>mini_page_op.rs</c> / <c>circular_buffer/mod.rs</c> assertion).
        /// </summary>
        [Test]
        [Explicit("Repro for the BfTree drop-vs-insert use-after-free; crashes the process by design.")]
        public async Task ConcurrentSetAndWholeKeyDeleteRepro()
        {
            const string key = "myindex";
            const int writerCount = 4;
            var runFor = TimeSpan.FromSeconds(5);

            using var seed = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            seed.GetDatabase(0).Execute("RI.CREATE", key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8");

            using var cts = new CancellationTokenSource(runFor);

            // Writers: RI.SET in a loop with a tiny pause so the deleter's exclusive lock is not
            // starved. A "no such range index" error is expected right after a DEL — treat benign.
            var writers = new Task[writerCount];
            for (var w = 0; w < writerCount; w++)
            {
                var id = w;
                writers[w] = Task.Run(() =>
                {
                    using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var db = conn.GetDatabase(0);
                    var i = 0;
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            db.Execute("RI.SET", key, $"field_{id}_{i:D6}", $"value_{id}_{i:D6}");
                        }
                        catch (RedisServerException)
                        {
                            // index missing (deleter ran) or size error — benign for this repro
                        }
                        i++;
                        if ((i & 0x3F) == 0)
                            Thread.Sleep(1);
                    }
                });
            }

            // Deleter: continuously DEL the whole key (drops the tree) then recreate it.
            var deleter = Task.Run(() =>
            {
                using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = conn.GetDatabase(0);
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        db.KeyDelete(key);
                        db.Execute("RI.CREATE", key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                    }
                    catch (RedisServerException)
                    {
                        // RI.CREATE may race another recreate — benign
                    }
                    Thread.Sleep(2);
                }
            });

            await Task.WhenAll(writers).ConfigureAwait(false);
            await deleter.ConfigureAwait(false);
        }

        /// <summary>
        /// Livelock repro: many writers continuously hold the per-key SHARED RangeIndex lock
        /// (RI.SET) with no pause, while a deleter needs the per-key EXCLUSIVE lock (whole-key
        /// <c>DEL</c> → <c>DisposeTreeUnderLock</c>). The read-optimized lock's exclusive
        /// acquisition spins per-core CAS waiting for all shared holders to drain; under sustained
        /// shared traffic the exclusive acquisition is starved and the <c>DEL</c> never makes
        /// progress, hanging the operation. A livelock under normal client traffic is not an
        /// acceptable signal and should be guarded against.
        /// </summary>
        [Test]
        [Explicit("Repro for exclusive-lock starvation under sustained RI.SET traffic; hangs by design.")]
        public async Task ConcurrentSetStarvesWholeKeyDeleteRepro()
        {
            const string key = "myindex";
            const int writerCount = 8;
            var runFor = TimeSpan.FromSeconds(8);

            using var seed = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            seed.GetDatabase(0).Execute("RI.CREATE", key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8");

            using var cts = new CancellationTokenSource(runFor);

            // Writers: hammer RI.SET with zero pause to keep the shared lock continuously busy.
            var writers = new Task[writerCount];
            for (var w = 0; w < writerCount; w++)
            {
                var id = w;
                writers[w] = Task.Run(() =>
                {
                    using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                    var db = conn.GetDatabase(0);
                    var i = 0;
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            db.Execute("RI.SET", key, $"field_{id}_{i:D6}", $"value_{id}_{i:D6}");
                        }
                        catch (RedisServerException)
                        {
                            // index missing (deleter ran) or size error — benign for this repro
                        }
                        i++;
                    }
                });
            }

            // Deleter: whole-key DEL that should complete promptly but can be starved by the
            // continuous shared-lock traffic above.
            var deleter = Task.Run(() =>
            {
                using var conn = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
                var db = conn.GetDatabase(0);
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        db.KeyDelete(key);
                        db.Execute("RI.CREATE", key, "DISK", "CACHESIZE", "65536", "MINRECORD", "8");
                    }
                    catch (RedisServerException)
                    {
                        // RI.CREATE may race another recreate — benign
                    }
                }
            });

            await Task.WhenAll(writers).ConfigureAwait(false);
            await deleter.ConfigureAwait(false);
        }
    }
}