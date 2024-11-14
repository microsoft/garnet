// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public unsafe class ClusterReplicationAsyncReplayTests
    {
        ClusterReplicationTests tests;

        [SetUp]
        public void Setup()
        {
            tests = new ClusterReplicationTests(UseTLS: false, asyncReplay: true);
            tests.Setup();
        }

        [TearDown]
        public void TearDown()
        {
            tests.TearDown();
            tests = null;
        }

        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplaySRTest([Values] bool disableObjects) => tests.ClusterSRTest(disableObjects);

        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayCheckpointRestartSecondary([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterSRNoCheckpointRestartSecondary(performRMW, disableObjects);

        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterSRPrimaryCheckpoint(performRMW, disableObjects);

        [Test, Order(4)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplaySRPrimaryCheckpointRetrieve([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory, [Values] bool manySegments)
            => tests.ClusterSRPrimaryCheckpointRetrieve(performRMW, disableObjects, lowMemory, manySegments);

        [Test, Order(5)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayCheckpointRetrieveDisableStorageTier([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterCheckpointRetrieveDisableStorageTier(performRMW, disableObjects);

        [Test, Order(6)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayReplicaAfterPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory)
            => tests.ClusterSRAddReplicaAfterPrimaryCheckpoint(performRMW, disableObjects, lowMemory);

        [Test, Order(7)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayPrimaryRestart([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterSRPrimaryRestart(performRMW, disableObjects);

        [Test, Order(8)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayRedirectWrites()
            => tests.ClusterSRRedirectWrites();

        [Test, Order(9)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayRedirectWrites([Values] bool performRMW)
            => tests.ClusterSRReplicaOfTest(performRMW);

        [Test, Order(10)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayReplicationSimpleFailover([Values] bool performRMW, [Values] bool checkpoint)
            => tests.ClusterReplicationSimpleFailover(performRMW, checkpoint);

        [Test, Order(11)]
        [Category("REPLICATION")]
        public void ClusterFailoverAttachReplicas([Values] bool performRMW, [Values] bool takePrimaryCheckpoint, [Values] bool takeNewPrimaryCheckpoint, [Values] bool enableIncrementalSnapshots)
            => tests.ClusterFailoverAttachReplicas(performRMW, takePrimaryCheckpoint, takeNewPrimaryCheckpoint, enableIncrementalSnapshots);

        [Test, Order(12)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayDivergentCheckpointTest([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterDivergentCheckpointTest(performRMW, disableObjects);

        [Test, Order(13)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayDivergentReplicasMMTest([Values] bool performRMW, [Values] bool disableObjects, [Values] bool ckptBeforeDivergence)
            => tests.ClusterDivergentReplicasMMTest(performRMW, disableObjects, ckptBeforeDivergence);

        [Test, Order(14)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayDivergentCheckpointMMTest([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterDivergentCheckpointMMTest(performRMW, disableObjects);

        [Test, Order(15)]
        [Category("REPLICATION")]
        public void ClusterAsyncReplayDivergentCheckpointMMFastCommitTest([Values] bool disableObjects, [Values] bool mainMemoryReplication)
            => tests.ClusterDivergentCheckpointMMFastCommitTest(disableObjects, mainMemoryReplication);
    }
}