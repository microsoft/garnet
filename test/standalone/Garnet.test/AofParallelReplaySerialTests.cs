// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using static Garnet.test.TestUtils;

namespace Garnet.test
{
    /// <summary>
    /// Layer 1 discriminator for the parallel AOF replay defect.
    ///
    /// The failing CI tests only fail in the single-physical-log multi-replay mode
    /// (AofPhysicalSublogCount == 1 &amp;&amp; AofReplayTaskCount &gt; 1), where N replay tasks scan the
    /// same shared page concurrently, coordinated by a single LeaderFollowerBarrier. The observed
    /// symptom is an entry applied the wrong number of times (twice for INCR, zero times for SET)
    /// while the replication offset still advances.
    ///
    /// These tests replay a real committed AOF through the exact same partition/apply logic
    /// (AofProcessor.CanReplay + AofProcessor.ProcessAofRecordInternal + virtual-sublog routing) but
    /// forced to run SERIALLY (single-threaded, one replay task at a time) via
    /// RecoverLogDriver.ForceSerialIntraPageReplay. This removes all concurrency and the barrier
    /// handoff from the picture.
    ///
    /// Interpretation:
    ///  - If a value comes back wrong here, the entry partition/apply logic itself is buggy
    ///    (deterministic logic bug, reproduces every run).
    ///  - If every value is exactly correct, the partition/apply logic is sound and the CI failures
    ///    are a genuine race in the concurrent barrier handoff (see Layer 2).
    ///
    /// Only non-transactional workloads are used: transaction replay blocks on
    /// ProcessSynchronizedOperation awaiting all participant tasks and would deadlock when serialized.
    /// </summary>
    [TestFixture]
    public class AofParallelReplaySerialTests : TestBase
    {
        GarnetServer server;

        // Matches TestReplayTaskCount used by the failing cluster tests.
        const int ReplayTaskCount = 3;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            server = null;
            RecoverLogDriver.ForceSerialIntraPageReplay = false;
            RecoverLogDriver.SerialIntraPageReplayInvocations = 0;
            DeleteDirectory(MethodTestDir, wait: true);
            OnTearDown();
        }

        /// <summary>
        /// Writes a plain (non-transactional) INCRBY workload spanning keys that route to all replay
        /// tasks, commits the AOF, then recovers it with intra-page replay forced serial. Asserts every
        /// key ends up with exactly its summed increment (i.e. each owned AOF entry applied exactly once).
        /// </summary>
        [Test]
        public void SerialMultiReplayAppliesEachIncrementExactlyOnce()
        {
            // Use enough distinct keys to cover all ReplayTaskCount routing buckets, each incremented
            // multiple times so exactly-once (not just once-total) is exercised, including per-key ordering.
            var expected = new Dictionary<string, long>();
            var keys = new List<string>();
            for (var i = 0; i < 24; i++)
                keys.Add($"key:{i}");

            // Phase 1: write the workload and commit the AOF.
            server = CreateGarnetServer(MethodTestDir, enableAOF: true, replayTaskCount: ReplayTaskCount);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);

                // Interleave increments across keys to produce many independent BasicHeader entries.
                for (var round = 0; round < 5; round++)
                {
                    for (var i = 0; i < keys.Count; i++)
                    {
                        var incr = (i + 1) + round; // varying, deterministic
                        db.StringIncrement(keys[i], incr);
                        expected.TryGetValue(keys[i], out var cur);
                        expected[keys[i]] = cur + incr;
                    }
                }

                db.Execute("COMMITAOF");
            }
            server.Dispose(false);

            // Phase 2: recover with intra-page replay forced serial (deterministic, no concurrency).
            RecoverLogDriver.ForceSerialIntraPageReplay = true;
            RecoverLogDriver.SerialIntraPageReplayInvocations = 0;
            server = CreateGarnetServer(MethodTestDir, enableAOF: true, tryRecover: true, replayTaskCount: ReplayTaskCount);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                foreach (var kvp in expected)
                {
                    var value = db.StringGet(kvp.Key);
                    ClassicAssert.IsTrue(value.HasValue, $"Key {kvp.Key} missing after serial recovery (entry applied zero times).");
                    ClassicAssert.AreEqual(kvp.Value, (long)value, $"Key {kvp.Key} has wrong value after serial recovery (entry applied the wrong number of times).");
                }
            }

            // Prove the serial replay branch was actually exercised (not the parallel path).
            ClassicAssert.Greater(RecoverLogDriver.SerialIntraPageReplayInvocations, 0, "Serial intra-page replay branch was not exercised.");
        }

        /// <summary>
        /// Same as above but with idempotent SET writes. A serial replay must reproduce the exact final
        /// value per key (the last write wins). A missed apply would surface as a missing/stale key.
        /// </summary>
        [Test]
        public void SerialMultiReplayAppliesEachSetExactlyOnce()
        {
            var expected = new Dictionary<string, string>();
            const int keyCount = 24;

            server = CreateGarnetServer(MethodTestDir, enableAOF: true, replayTaskCount: ReplayTaskCount);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                for (var round = 0; round < 3; round++)
                {
                    for (var i = 0; i < keyCount; i++)
                    {
                        var key = $"key:{i}";
                        var val = $"v-{i}-{round}";
                        db.StringSet(key, val);
                        expected[key] = val; // last write wins
                    }
                }

                db.Execute("COMMITAOF");
            }
            server.Dispose(false);

            RecoverLogDriver.ForceSerialIntraPageReplay = true;
            RecoverLogDriver.SerialIntraPageReplayInvocations = 0;
            server = CreateGarnetServer(MethodTestDir, enableAOF: true, tryRecover: true, replayTaskCount: ReplayTaskCount);
            server.Start();
            using (var redis = ConnectionMultiplexer.Connect(GetConfig(allowAdmin: true)))
            {
                var db = redis.GetDatabase(0);
                foreach (var kvp in expected)
                {
                    var value = db.StringGet(kvp.Key);
                    ClassicAssert.IsTrue(value.HasValue, $"Key {kvp.Key} missing after serial recovery (entry applied zero times).");
                    ClassicAssert.AreEqual(kvp.Value, value.ToString(), $"Key {kvp.Key} has wrong value after serial recovery.");
                }
            }

            ClassicAssert.Greater(RecoverLogDriver.SerialIntraPageReplayInvocations, 0, "Serial intra-page replay branch was not exercised.");
        }
    }
}
