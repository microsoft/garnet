// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Failover coverage for populated Vector Sets. Promotion reverses replication direction,
    /// so aliased handles get observed from the other side.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetFailoverTests : VectorSetReplicationTestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        private void SetupCluster(int nodeCount)
        {
            context.CreateInstances(
                nodeCount,
                enableAOF: true,
                enableDisklessSync: false,
                OnDemandCheckpoint: true,
                timeout: timeout);

            context.FormClusterAllNodes(nodeCount);
        }

        /// <summary>After promotion, the new primary must keep every element and the demoted node must agree.</summary>
        [Test]
        public void VectorSetSurvivesFailover()
        {
            const string Key = "{vsdisk}failoverbasic";
            const int Elements = 250;

            SetupCluster(2);
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_21);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            context.FailoverTo(ReplicaIndex, PrimaryIndex);

            ClassicAssert.AreEqual(Elements, VectorSetSize(ReplicaIndex, Key), "the promoted node lost elements across the failover");

            context.clusterTestUtils.ReadOnly(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// After promotion, the new primary extends the inherited set and replicates those writes
        /// back to the demoted original writer.
        /// </summary>
        [Test]
        public void PromotedReplicaServesVectorSetToDemotedPrimary()
        {
            const string Key = "{vsdisk}failoverwrite";
            const int InitialElements = 200;
            const int AddedElements = 150;

            SetupCluster(2);
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            PopulateVectorSet(PrimaryIndex, Key, InitialElements, seed: 2026_07_29_22);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            context.FailoverTo(ReplicaIndex, PrimaryIndex);

            // ReplicaIndex is now primary; write through it.
            PopulateVectorSet(ReplicaIndex, Key, AddedElements, seed: 2026_07_29_23);
            context.clusterTestUtils.WaitForReplicaAofSync(ReplicaIndex, PrimaryIndex, logger: context.logger);

            ClassicAssert.AreEqual(InitialElements + AddedElements, VectorSetSize(ReplicaIndex, Key));

            context.clusterTestUtils.ReadOnly(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// Repeated failovers re-point the replication stream several times, surfacing state that only resets in one direction.
        /// </summary>
        [Test]
        public void VectorSetSurvivesRepeatedFailovers()
        {
            const string Key = "{vsdisk}failoverloop";
            const int InitialElements = 100;
            const int PerRoundElements = 40;
            const int Rounds = 3;

            SetupCluster(2);
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            PopulateVectorSet(PrimaryIndex, Key, InitialElements, seed: 2026_07_29_24);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            var primary = PrimaryIndex;
            var replica = ReplicaIndex;

            for (var round = 0; round < Rounds; round++)
            {
                context.FailoverTo(replica, primary);
                (primary, replica) = (replica, primary);

                PopulateVectorSet(primary, Key, PerRoundElements, seed: 2026_07_29_25 + round);
                context.clusterTestUtils.WaitForReplicaAofSync(primary, replica, logger: context.logger);

                context.clusterTestUtils.ReadOnly(replica);
                AssertFullyReplicated(primary, replica, Key);
            }

            ClassicAssert.AreEqual(InitialElements + (PerRoundElements * Rounds), VectorSetSize(primary, Key), "elements were lost across repeated failovers");
        }
    }
}