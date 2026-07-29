// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Vector Sets under <em>asynchronous</em> AOF replay.
    ///
    /// <para>
    /// With <c>asyncReplay</c> the replica applies the AOF stream on background replay tasks rather
    /// than inline, so VADD records are handed to <c>VectorManager</c> off the network path and,
    /// with more than one replay task, concurrently with each other. Vector Set replication had no
    /// coverage under this configuration at all — the whole
    /// <c>Garnet.test.cluster.replication.asyncreplay</c> project contains zero vector references.
    /// </para>
    /// <para>
    /// This matters beyond ordinary throughput concerns because a Vector Set VADD is not a simple
    /// value write: it mutates a native index behind the record. Async replay changes which thread
    /// performs that mutation and how it interleaves with a concurrent full sync, so it is a genuinely
    /// different execution of the same logical path.
    /// </para>
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetAsyncReplayTests : VectorSetReplicationTestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        protected override Dictionary<string, LogLevel> MonitorTests => new()
        {
            [nameof(VectorSetReplicatedUnderAsyncReplay)] = LogLevel.Trace,
            [nameof(VectorSetReplicatedUnderAsyncReplayAfterFullSync)] = LogLevel.Trace,
            [nameof(VectorSetReplicatedUnderParallelAsyncReplay)] = LogLevel.Trace,
        };

        private void SetupAsyncReplayCluster(int nodeCount, bool disklessSync, int replayTaskCount = 1, int vectorSetReplayTaskCount = 0)
        {
            context.CreateInstances(
                nodeCount,
                enableAOF: true,
                asyncReplay: true,
                enableDisklessSync: disklessSync,
                replicaDisklessSyncFullSyncAofThreshold: disklessSync ? "1k" : null,
                OnDemandCheckpoint: !disklessSync,
                replayTaskCount: replayTaskCount,
                vectorSetReplayTaskCount: vectorSetReplayTaskCount,
                timeout: timeout);
            context.CreateConnection();

            FormCluster(PrimaryIndex, [.. Enumerable.Range(1, nodeCount - 1)]);
        }

        /// <summary>
        /// The baseline that was missing entirely: VADDs replicated to an attached replica that is
        /// replaying asynchronously must still land, in full, with identical embeddings.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedUnderAsyncReplay()
        {
            const string Key = "{vsdisk}async";
            const int Elements = 300;

            SetupAsyncReplayCluster(2, disklessSync: false);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_26);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Async replay combined with a diskless full sync — the configuration that carries the index
        /// record. The replica must rebuild its own index from the streamed record and then correctly
        /// apply the asynchronously replayed VADDs that follow it.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedUnderAsyncReplayAfterFullSync()
        {
            const string Key = "{vsdisk}asyncfullsync";
            const int Elements = 250;
            const int AfterSyncElements = 100;

            SetupAsyncReplayCluster(2, disklessSync: true);

            // Populated before the replica exists, so the attach must carry the index record.
            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_27);

            var before = FullSyncCount();
            Attach(ReplicaIndex, PrimaryIndex);
            AssertTookFullSync(before);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            // Writes after the sync exercise async replay against an index the replica just rebuilt.
            PopulateVectorSet(PrimaryIndex, Key, AfterSyncElements, seed: 2026_07_29_28);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Several replay tasks and several dedicated Vector Set replay tasks, so VADDs for multiple
        /// keys are applied concurrently on the replica. Distinct keys must not interfere: each has to
        /// end up with exactly its own elements.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedUnderParallelAsyncReplay()
        {
            const string FirstKey = "{vsdisk}asyncpar1";
            const string SecondKey = "{vsdisk}asyncpar2";
            const string ThirdKey = "{vsdisk}asyncpar3";
            const int FirstElements = 200;
            const int SecondElements = 150;
            const int ThirdElements = 80;

            SetupAsyncReplayCluster(2, disklessSync: false, replayTaskCount: 4, vectorSetReplayTaskCount: 4);
            Attach(ReplicaIndex, PrimaryIndex);

            // Interleaved so the replay tasks see records for the three keys mixed together.
            for (var round = 0; round < 4; round++)
            {
                PopulateVectorSet(PrimaryIndex, FirstKey, FirstElements / 4, seed: 2026_07_29_29 + round);
                PopulateVectorSet(PrimaryIndex, SecondKey, SecondElements / 4, seed: 2026_07_29_33 + round);
                PopulateVectorSet(PrimaryIndex, ThirdKey, ThirdElements / 4, seed: 2026_07_29_37 + round);
            }

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, FirstKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, SecondKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, ThirdKey);

            ClassicAssert.AreEqual(FirstElements, VectorSetSize(ReplicaIndex, FirstKey));
            ClassicAssert.AreEqual(SecondElements, VectorSetSize(ReplicaIndex, SecondKey));
            ClassicAssert.AreEqual(ThirdElements, VectorSetSize(ReplicaIndex, ThirdKey));
        }
    }
}
