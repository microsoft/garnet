// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public unsafe class ClusterTLSRT
    {
        ClusterReplicationTests tests;

        [SetUp]
        public void Setup()
        {
            tests = new ClusterReplicationTests(UseTLS: true);
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
        public void ClusterTLSR([Values] bool disableObjects)
            => tests.ClusterSRTest(disableObjects);

        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterTLSRCheckpointRestartSecondary([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterSRNoCheckpointRestartSecondary(performRMW, disableObjects);

        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterTLSRPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterSRPrimaryCheckpoint(performRMW, disableObjects);

        [Test, Order(4)]
        [Category("REPLICATION")]
        public void ClusterTLSRPrimaryCheckpointRetrieve([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory, [Values] bool manySegments)
            => tests.ClusterSRPrimaryCheckpointRetrieve(performRMW, disableObjects, lowMemory, manySegments);

        [Test, Order(5)]
        [Category("REPLICATION")]
        public void ClusterTLSCheckpointRetrieveDisableStorageTier([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterCheckpointRetrieveDisableStorageTier(performRMW, disableObjects);

        [Test, Order(6)]
        [Category("REPLICATION")]
        public void ClusterTLSRAddReplicaAfterPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory)
            => tests.ClusterSRAddReplicaAfterPrimaryCheckpoint(performRMW, disableObjects, lowMemory);

        [Test, Order(7)]
        [Category("REPLICATION")]
        public void ClusterTLSRPrimaryRestart([Values] bool performRMW, [Values] bool disableObjects)
            => tests.ClusterSRPrimaryRestart(performRMW, disableObjects);

        [Test, Order(8)]
        [Category("REPLICATION")]
        public void ClusterTLSRRedirectWrites()
            => tests.ClusterSRRedirectWrites();

        [Test, Order(9)]
        [Category("REPLICATION")]
        public void ClusterTLSRReplicaOfTest([Values] bool performRMW)
            => tests.ClusterSRReplicaOfTest(performRMW);
    }
}