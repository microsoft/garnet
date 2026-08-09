// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// End-to-end Garnet server tests across the three native-allocator modes (off | buffer-pool | full),
    /// wired through <c>--native-allocator</c> (<see cref="TestUtils.CreateGarnetServer"/>'s
    /// <c>nativeAllocator</c> parameter -> <c>GarnetServerOptions.NativeAllocatorSurfaces</c>).
    ///
    /// Coverage rationale per mode:
    ///  * off        — managed baseline (behavior-identical control).
    ///  * BufferPool — routes the SectorAlignedBufferPool IO/flush buffers through mimalloc; exercised by large
    ///                 values (which flow through the aligned buffers) + checkpoint SAVE/COMMIT flushes.
    ///  * Full       — additionally routes hash index, log pages, and recovery frames to the direct-VM backend;
    ///                 exercised by the save/recover round-trip (index build, log-page churn under the tracker,
    ///                 recovery-frame reads) and the low-memory eviction case (native log-page free/reuse pool).
    ///
    /// A native mode requires the shipped mimalloc for the current RID; if unavailable the run fails fast at
    /// startup, so the fixture skips the native cases when mimalloc cannot load (keeps CI green on unshipped RIDs).
    /// The native surfaces are installed process-globally at server start from the options, so this fixture is
    /// NonParallelizable and each test creates/disposes its own server.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class NativeAllocatorServerTests
    {
        GarnetServer server;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        static void SkipIfUnavailable(NativeAllocatorSurfaces mode)
        {
            // The direct-VM surfaces (Full's LogPages/HashIndex/Frames) need no native library, but Full includes
            // BufferPool, so any non-off mode here requires mimalloc. Skip rather than fail on an unshipped RID.
            if (mode != NativeAllocatorSurfaces.None && !Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");
        }

        static readonly object[] Modes =
        [
            NativeAllocatorSurfaces.None,
            NativeAllocatorSurfaces.BufferPool,
            NativeAllocatorSurfaces.Full,
        ];

        [Test]
        [TestCaseSource(nameof(Modes))]
        public void StringAndObjectRoundTrip(NativeAllocatorSurfaces mode)
        {
            SkipIfUnavailable(mode);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, nativeAllocator: mode);
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
        public void SaveRecoverRoundTrip(NativeAllocatorSurfaces mode)
        {
            SkipIfUnavailable(mode);

            var big = new string('y', 200 * 1024);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, nativeAllocator: mode);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (var i = 0; i < 500; i++)
                    ClassicAssert.IsTrue(db.StringSet($"k{i}", $"v{i}"));
                ClassicAssert.IsTrue(db.StringSet("k:big", big));
                db.ListRightPush("k:list", ["a", "b", "c", "d"]);
                // Checkpoint: the flush routes through the (native, in BufferPool/Full) sector-aligned buffers.
                db.Execute("SAVE");
            }

            // Restart + recover: the recovery path reads via direct-VM frames and rebuilds the index (Full).
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, nativeAllocator: mode);
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
        public void LowMemoryEvictionChurn(NativeAllocatorSurfaces mode)
        {
            SkipIfUnavailable(mode);

            // lowMemory + a small log => the LogSizeTracker drives continuous flush + eviction, exercising the
            // native log-page free/reuse (OverflowPool) path in Full mode across many pages.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true, nativeAllocator: mode);
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
    }
}