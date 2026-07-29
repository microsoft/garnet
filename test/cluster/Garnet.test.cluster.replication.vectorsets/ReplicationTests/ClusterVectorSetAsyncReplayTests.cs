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
    /// Vector Set coverage for asynchronous AOF replay. VADDs mutate native indexes on
    /// VectorManager replay tasks, so async replay is a distinct execution path.
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

        /// <summary>Baseline: asynchronously replayed VADDs must land with identical embeddings.</summary>
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
        /// Combines async replay with diskless full sync of the index record; the replica rebuilds
        /// the streamed index and applies later VADDs.
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

            // Populated before the replica exists, so attach carries the index record.
            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_27);

            var before = FullSyncCount();
            Attach(ReplicaIndex, PrimaryIndex);
            AssertTookFullSync(before);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            // Writes after sync exercise async replay against the rebuilt index.
            PopulateVectorSet(PrimaryIndex, Key, AfterSyncElements, seed: 2026_07_29_28);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Multiple replay tasks apply VADDs for different keys concurrently; each key must keep exactly its own elements.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedUnderParallelAsyncReplay()
        {
            const string FirstKey = "{vsdisk}asyncpar1";
            const string SecondKey = "{vsdisk}asyncpar2";
            const string ThirdKey = "{vsdisk}asyncpar3";
            const int Rounds = 4;
            const int FirstPerRound = 50;
            const int SecondPerRound = 37;
            const int ThirdPerRound = 20;

            SetupAsyncReplayCluster(2, disklessSync: false, replayTaskCount: 4, vectorSetReplayTaskCount: 4);
            Attach(ReplicaIndex, PrimaryIndex);

            // Interleaved so replay tasks see records for all three keys mixed together.
            for (var round = 0; round < Rounds; round++)
            {
                PopulateVectorSet(PrimaryIndex, FirstKey, FirstPerRound, seed: 2026_07_29_29 + round);
                PopulateVectorSet(PrimaryIndex, SecondKey, SecondPerRound, seed: 2026_07_29_33 + round);
                PopulateVectorSet(PrimaryIndex, ThirdKey, ThirdPerRound, seed: 2026_07_29_37 + round);
            }

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, FirstKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, SecondKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, ThirdKey);

            ClassicAssert.AreEqual(Rounds * FirstPerRound, VectorSetSize(ReplicaIndex, FirstKey));
            ClassicAssert.AreEqual(Rounds * SecondPerRound, VectorSetSize(ReplicaIndex, SecondKey));
            ClassicAssert.AreEqual(Rounds * ThirdPerRound, VectorSetSize(ReplicaIndex, ThirdKey));
        }
    }
}