// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Disk-based replica sync from a Snapshot checkpoint whose object log holds data while its snapshot file holds none.
    /// </summary>
    /// <remarks>
    /// During a Snapshot checkpoint, a <c>(v+1)</c> CopyUpdate of a <c>(v)</c> object record that no flush has written yet captures
    /// the <c>(v)</c> value into the checkpoint's object log, and the record's ReadOnly flush then writes it with a reference into that
    /// log. If ReadOnly flushes have written every page up to the checkpoint's tail when its Snapshot flush starts, the Snapshot
    /// flush writes nothing: the checkpoint's snapshot file is empty, but its object log is not. A replica that recovers the
    /// checkpoint reads the captures from that log, so the primary must send it.
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationSnapshotObjectLogTests : TestBase
    {
        ClusterTestContext context;

        const int PrimaryIndex = 0;
        const int ReplicaIndex = 1;

        /// <summary>The largest filler value written. Every filler is inline, and its record size is a fixed overhead plus its value
        /// length rounded up to 8 bytes.</summary>
        const int MaxFillerValueLength = 248;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context?.TearDown();

        [Test]
        [Category("REPLICATION")]
        public void ReplicaRecoversCapturesOfACheckpointWhoseSnapshotFileIsEmpty()
        {
            // 4 KB pages, so a page is quick to fill exactly, and enough of them that nothing is flushed unless the test flushes it.
            context.CreateInstances(2, disableObjects: false, lowMemory: true, memorySize: "256k", enableAOF: true);
            context.CreateConnection();
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(PrimaryIndex, [(0, 16383)], addslot: true, context.logger));
            context.clusterTestUtils.BumpEpoch(PrimaryIndex, logger: context.logger);

            var primary = context.clusterTestUtils.GetServer(PrimaryIndex);
            var store = context.nodes[PrimaryIndex].Provider.StoreWrapper.store;
            const string listKey = "list";
            _ = context.clusterTestUtils.Lpush(PrimaryIndex, listKey, [1, 2, 3], context.logger);

            // Hold the checkpoint after its version shift, before its Snapshot flush starts. It is the primary's first checkpoint, so it
            // is a full one, whose WAIT_INDEX_CHECKPOINT phase lies between the two. SAVE runs the checkpoint on its session and blocks
            // that connection until the checkpoint completes, so it gets a connection of its own.
            using var reached = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            context.nodes[PrimaryIndex].Provider.StoreWrapper.DefaultDatabase.StateMachineDriver.UnsafeRegisterCallback(
                new PauseCheckpointOnce(Phase.WAIT_INDEX_CHECKPOINT, reached, release));
            var saveConfig = TestUtils.GetConfig([context.clusterTestUtils.GetEndPoint(PrimaryIndex)], allowAdmin: true);
            saveConfig.SyncTimeout = 60_000;
            using var saveConnection = ConnectionMultiplexer.Connect(saveConfig);
            Task<RedisResult> save = null;
            try
            {
                save = Task.Run(() => saveConnection.GetServer(context.clusterTestUtils.GetEndPoint(PrimaryIndex)).Execute("SAVE"));
                ClassicAssert.IsTrue(reached.Wait(TimeSpan.FromSeconds(30)), "The checkpoint never reached its version shift");

                // A (v+1) LPUSH copy-updates the (v) list, which no flush has written, so the checkpoint captures the (v) list.
                ClassicAssert.AreEqual(5, (int)primary.Execute(0, "LPUSH", [listKey, 4, 5]));

                // End the main log exactly at a page boundary and flush it. The ReadOnly flush writes the superseded list with its
                // reference into the checkpoint's object log, and the Snapshot flush, which starts at the flushed end, has nothing to write.
                FillPageExactly(primary, () => store.Log.TailAddress, store.Log.PageSizeBytes);
                store.Log.Flush(wait: true);
                ClassicAssert.AreEqual(store.Log.TailAddress, store.Log.FlushedUntilAddress, "The ReadOnly flush must reach the tail");
            }
            finally
            {
                release.Set();
            }
            ClassicAssert.IsTrue(save.Wait(TimeSpan.FromSeconds(60)), "SAVE did not complete");
            ClassicAssert.AreEqual("OK", save.Result.ToString());

            store.GetLatestCheckpointTokens(out var hlogToken, out _, out _);
            var files = store.GetLogFileSize(hlogToken);
            ClassicAssert.LessOrEqual(files.snapshotFileEndAddress, PageHeader.Size, "The Snapshot flush must have written nothing");
            ClassicAssert.Greater(files.snapshotObjectFileEndAddress, 0, "The checkpoint's object log must hold the capture");

            // A replica attached now syncs from this checkpoint, and its recovery reads the capture from the checkpoint's object log.
            context.clusterTestUtils.Meet(ReplicaIndex, PrimaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitAll(context.logger);
            _ = context.clusterTestUtils.ClusterReplicate(ReplicaIndex, PrimaryIndex, async: false, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(ReplicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, context.logger);

            var expected = new Dictionary<string, List<int>> { [listKey] = context.clusterTestUtils.Lrange(PrimaryIndex, listKey, context.logger) };
            ClassicAssert.AreEqual(new List<int> { 5, 4, 1, 2, 3 }, expected[listKey]);
            context.ValidateNodeObjects(ref expected, ReplicaIndex);
        }

        /// <summary>Write string records until the main log's tail is exactly at a page boundary.</summary>
        /// <remarks>
        /// Records are 8-byte aligned, so a filler whose value length is a multiple of 8 takes exactly that many bytes more than the
        /// smallest filler, whose size is measured first. Each filler then either fills the page or leaves room for at least the
        /// smallest one. A page whose remainder is smaller than that cannot be filled, so a filler that does not fit starts the next.
        /// </remarks>
        static void FillPageExactly(IServer server, Func<long> tailAddress, long pageSize)
        {
            var fillerCount = 0;
            long Write(int valueLength)
            {
                ClassicAssert.Less(fillerCount, 1000, "The page was never filled exactly");
                var before = tailAddress();
                ClassicAssert.AreEqual("OK", (string)server.Execute(0, "SET", [$"filler{fillerCount++:D6}", new string('x', valueLength)]));
                return tailAddress() - before;
            }
            long Remaining() => (pageSize - (tailAddress() & (pageSize - 1))) & (pageSize - 1);

            // Measure the smallest filler where it fits on the current page, so its size excludes a page change.
            long minSize = 0;
            while (minSize == 0)
            {
                if (Remaining() >= 1024)
                    minSize = Write(8);
                else
                    _ = Write(MaxFillerValueLength);
            }

            for (var remaining = Remaining(); remaining != 0; remaining = Remaining())
            {
                int valueLength;
                if (remaining < minSize)
                    valueLength = MaxFillerValueLength;
                else if (remaining <= minSize + MaxFillerValueLength - 8)
                    valueLength = (int)(remaining - minSize + 8);
                else
                    valueLength = (int)Math.Min(MaxFillerValueLength, remaining - (2 * minSize) + 8);
                _ = Write(valueLength);
            }
        }

        /// <summary>Holds the first checkpoint on entry to <paramref name="phase"/> until the test releases it.</summary>
        sealed class PauseCheckpointOnce(Phase phase, ManualResetEventSlim reached, ManualResetEventSlim release) : IStateMachineCallback
        {
            /// <summary>Set once the checkpoint has been held, so later transitions and checkpoints pass through.</summary>
            int paused;

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != phase || Interlocked.Exchange(ref paused, 1) != 0)
                    return;
                reached.Set();
                if (!release.Wait(TimeSpan.FromSeconds(30)))
                    throw new TimeoutException("The test did not release the checkpoint");
            }
        }
    }
}