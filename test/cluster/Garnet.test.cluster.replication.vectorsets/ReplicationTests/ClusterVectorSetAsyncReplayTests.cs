// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

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

        private void SetupAsyncReplayCluster(int nodeCount, bool disklessSync)
        {
            context.CreateInstances(
                nodeCount,
                enableAOF: true,
                asyncReplay: true,
                enableDisklessSync: disklessSync,
                replicaDisklessSyncFullSyncAofThreshold: disklessSync ? "1k" : null,
                OnDemandCheckpoint: !disklessSync,
                timeout: timeout);

            context.FormClusterAllNodes(nodeCount);
        }

        /// <summary>Baseline: asynchronously replayed VADDs must land with identical embeddings.</summary>
        [Test]
        public void VectorSetReplicatedUnderAsyncReplay()
        {
            const string Key = "{vsdisk}async";
            const int Elements = 300;

            SetupAsyncReplayCluster(2, disklessSync: false);
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_26);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Combines async replay with diskless full sync of the index record; the replica rebuilds
        /// the streamed index and applies later VADDs.
        /// </summary>
        [Test]
        public void VectorSetReplicatedUnderAsyncReplayAfterFullSync()
        {
            const string Key = "{vsdisk}asyncfullsync";
            const int Elements = 250;
            const int AfterSyncElements = 100;

            SetupAsyncReplayCluster(2, disklessSync: true);

            // Populated before the replica exists, so attach carries the index record.
            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_27);

            // The replica has its own replication id, so the attach takes a full sync.
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            // Writes after sync exercise async replay against the rebuilt index.
            PopulateVectorSet(PrimaryIndex, Key, AfterSyncElements, seed: 2026_07_29_28);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }
    }
}