// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterTLSRT
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
        public async Task ClusterTLSR([Values] bool disableObjects)
            => await tests.ClusterSRTest(disableObjects);

        [Test, Order(2)]
        [Category("REPLICATION")]
        public async Task ClusterTLSRCheckpointRestartSecondary([Values] bool performRMW, [Values] bool disableObjects)
            => await tests.ClusterSRNoCheckpointRestartSecondary(performRMW, disableObjects);

        [Test, Order(3)]
        [Category("REPLICATION")]
        public async Task ClusterTLSRPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects)
            => await tests.ClusterSRPrimaryCheckpoint(performRMW, disableObjects);

        [Test, Order(4)]
        [Category("REPLICATION")]
        public async Task ClusterTLSRPrimaryCheckpointRetrieve([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory, [Values] bool manySegments)
            => await tests.ClusterSRPrimaryCheckpointRetrieve(performRMW, disableObjects, lowMemory, manySegments);

        [Test, Order(5)]
        [Category("REPLICATION")]
        public async Task ClusterTLSCheckpointRetrieveDisableStorageTier([Values] bool performRMW, [Values] bool disableObjects)
            => await tests.ClusterCheckpointRetrieveDisableStorageTier(performRMW, disableObjects);

        [Test, Order(6)]
        [Category("REPLICATION")]
        public async Task ClusterTLSRAddReplicaAfterPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory)
            => await tests.ClusterSRAddReplicaAfterPrimaryCheckpoint(performRMW, disableObjects, lowMemory);

        [Test, Order(7)]
        [Category("REPLICATION")]
        public async Task ClusterTLSRPrimaryRestart([Values] bool performRMW, [Values] bool disableObjects)
            => await tests.ClusterSRPrimaryRestart(performRMW, disableObjects);

        [Test, Order(8)]
        [Category("REPLICATION")]
        public async Task ClusterTLSRRedirectWrites()
            => await tests.ClusterSRRedirectWrites();

        [Test, Order(9)]
        [Category("REPLICATION")]
        public async Task ClusterTLSRReplicaOfTest([Values] bool performRMW)
            => await tests.ClusterSRReplicaOfTest(performRMW);
    }
}