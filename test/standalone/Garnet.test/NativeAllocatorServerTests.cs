// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// End-to-end Garnet server tests across the native-allocator switch (off and on),
    /// wired through <c>--use-native-allocator</c> (<see cref="TestUtils.CreateGarnetServer"/>'s
    /// <c>nativeAllocator</c> parameter -> <c>GarnetServerOptions.UseNativeAllocator</c>).
    ///
    /// Coverage rationale per mode:
    ///  * off  — managed baseline (behavior-identical control).
    ///  * on   — routes hash index, log pages, and recovery frames to the direct-VM backend;
    ///           exercised by the save/recover round-trip (index build, log-page churn under the tracker,
    ///           recovery-frame reads) and the low-memory eviction case (native log-page free/reuse pool).
    ///
    /// The direct-VM surfaces call the OS virtual-memory APIs directly and need no shipped native library.
    /// The native surfaces are installed process-globally at server start from the options, so this fixture is
    /// NonParallelizable and each test creates/disposes its own server.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class NativeAllocatorServerTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            // Restore the process-global managed allocator so a native (on) case does not bleed native allocation
            // into a later fixture running in the same test process (mirrors the Tsavorite-side native fixtures,
            // e.g. NativeAllocatorStoreTests / NativeHashIndexTests, which reset to managed in teardown).
            _ = Tsavorite.core.NativeAllocatorInitializer.Initialize(false);
            // Native (on) mode leaves direct-VM index/log blocks to be freed by the NativePageBlockRegistry
            // finalizer; drain finalizers so those deferred frees do not bleed into a later test's process-global
            // native state (tracker / LightEpoch) in the same assembly.
            System.GC.Collect();
            System.GC.WaitForPendingFinalizers();
            System.GC.Collect();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        static readonly object[] Modes =
        [
            false,
            true,
        ];

        [Test]
        [TestCaseSource(nameof(Modes))]
        public void StringAndObjectRoundTrip(bool useNative)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, nativeAllocator: useNative);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            // Strings, including a large value (~256 KB) that flows through the sector-aligned IO/flush buffers.
            ClassicAssert.IsTrue(db.StringSet("s:small", "hello-native"));
            var big = new string('x', 256 * 1024);
            ClassicAssert.IsTrue(db.StringSet("s:big", big));
            ClassicAssert.AreEqual("hello-native", (string)db.StringGet("s:small"));
            ClassicAssert.AreEqual(big, (string)db.StringGet("s:big"));

            // Collections exercise the object store surfaces.
            db.ListRightPush("l:1", ["a", "b", "c", "d"]);
            CollectionAssert.AreEqual(new RedisValue[] { "a", "b", "c", "d" }, db.ListRange("l:1"));

            for (var i = 0; i < 100; i++)
                db.SortedSetAdd("z:1", $"m{i}", i);
            ClassicAssert.AreEqual(100, db.SortedSetLength("z:1"));
            ClassicAssert.AreEqual("m0", (string)db.SortedSetRangeByRank("z:1", 0, 0).Single());

            db.HashSet("h:1", [new HashEntry("f1", "v1"), new HashEntry("f2", "v2")]);
            ClassicAssert.AreEqual("v1", (string)db.HashGet("h:1", "f1"));
        }

        [Test]
        [TestCaseSource(nameof(Modes))]
        public void SaveRecoverRoundTrip(bool useNative)
        {
            var big = new string('y', 200 * 1024);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, nativeAllocator: useNative);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < 500; i++)
                    ClassicAssert.IsTrue(db.StringSet($"k{i}", $"v{i}"));
                ClassicAssert.IsTrue(db.StringSet("k:big", big));
                db.ListRightPush("k:list", ["a", "b", "c", "d"]);
                // Checkpoint: in Full mode the recovery path reads via direct-VM frames.
                db.Execute("SAVE");
            }

            // Restart + recover: the recovery path reads via direct-VM frames and rebuilds the index (Full).
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, nativeAllocator: useNative);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual("v0", (string)db.StringGet("k0"));
                ClassicAssert.AreEqual("v499", (string)db.StringGet("k499"));
                ClassicAssert.AreEqual(big, (string)db.StringGet("k:big"));
                CollectionAssert.AreEqual(new RedisValue[] { "a", "b", "c", "d" }, db.ListRange("k:list"));
            }
        }

        [Test]
        [TestCaseSource(nameof(Modes))]
        public void LowMemoryEvictionChurn(bool useNative)
        {
            // lowMemory + a small log => the LogSizeTracker drives continuous flush + eviction, exercising the
            // native log-page free/reuse (OverflowPool) path in Full mode across many pages.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, nativeAllocator: useNative);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            const int n = 5000;
            var payload = new string('z', 512);
            for (var i = 0; i < n; i++)
                ClassicAssert.IsTrue(db.StringSet($"key:{i}", $"{payload}:{i}"));

            // Spot-check spread across the (mostly on-disk after eviction) keyspace to force disk reads back
            // through the sector-aligned IO buffers.
            for (var i = 0; i < n; i += 250)
                ClassicAssert.AreEqual($"{payload}:{i}", (string)db.StringGet($"key:{i}"));
        }

        [Test]
        [TestCaseSource(nameof(Modes))]
        public void ObjectStoreEvictionChurn(bool useNative)
        {
            // Regression guard for the native OBJECT-log-page recycle path: heavy SET + EXPIRE (plus collection
            // commands) under lowMemory drives eviction of native object-log pages and their reuse via the recycle
            // pool. A recycled page whose stale inline bytes were not re-zeroed leaves a stale KeyIsOverflow record
            // that eviction misreads, crashing the server ("Get(): index 0 must be less than Count 0" in the
            // ObjectIdMap). Full mode is the case under test; off is a behavior-identical control.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, nativeAllocator: useNative);
            server.Start();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var rng = new System.Random(17);
            const int n = 8000;
            for (var i = 0; i < n; i++)
            {
                var key = $"k:{System.Guid.NewGuid():N}";
                ClassicAssert.IsTrue(db.StringSet(key, System.Guid.NewGuid().ToString("N")));
                var chance = rng.Next(100);
                if (chance < 30)
                    _ = db.KeyExpire(key, System.TimeSpan.FromHours(2));         // long TTL (survives)
                else if (chance < 60)
                    _ = db.KeyExpire(key, System.TimeSpan.FromMilliseconds(1));  // immediate expiration -> tombstone

                // Interleave collection commands so the object allocator's own records churn through eviction/
                // recycle alongside the expirable string records.
                if ((i & 0x3F) == 0)
                {
                    var ok = $"o:{i}";
                    db.HashSet(ok, [new HashEntry("f", new string('v', 48))]);
                    db.ListRightPush(ok + ":l", ["a", "b", "c"]);
                    _ = db.KeyExpire(ok, System.TimeSpan.FromMilliseconds(1));
                }
            }

            // The server must still be alive and correct after the eviction/recycle churn — a corrupt recycled page
            // would have crashed the session/process during the loop above.
            ClassicAssert.AreEqual("PONG", db.Execute("PING").ToString());
            ClassicAssert.IsTrue(db.StringSet("sentinel", "ok"));
            ClassicAssert.AreEqual("ok", (string)db.StringGet("sentinel"));
        }
    }
}