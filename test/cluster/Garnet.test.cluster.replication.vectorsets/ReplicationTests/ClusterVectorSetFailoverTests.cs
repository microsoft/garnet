// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
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
            context.CreateConnection();

            FormCluster(PrimaryIndex, [.. Enumerable.Range(1, nodeCount - 1)]);
        }

        private void FailoverTo(int replicaIndex, int oldPrimaryIndex)
        {
            context.ClusterFailoverSpinWait(replicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(replicaIndex, oldPrimaryIndex, logger: context.logger);

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(Node(replicaIndex), logger: context.logger).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(Node(oldPrimaryIndex), logger: context.logger).Value);
        }

        /// <summary>After promotion, the new primary must keep every element and the demoted node must agree.</summary>
        [Test]
        [Category("REPLICATION")]
        public void VectorSetSurvivesFailover()
        {
            const string Key = "{vsdisk}failoverbasic";
            const int Elements = 250;

            SetupCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_21);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            FailoverTo(ReplicaIndex, PrimaryIndex);

            ClassicAssert.AreEqual(Elements, VectorSetSize(ReplicaIndex, Key), "the promoted node lost elements across the failover");

            MakeReadable(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// After promotion, the new primary extends the inherited set and replicates those writes
        /// back to the demoted original writer.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        public void PromotedReplicaServesVectorSetToDemotedPrimary()
        {
            const string Key = "{vsdisk}failoverwrite";
            const int InitialElements = 200;
            const int AddedElements = 150;

            SetupCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, InitialElements, seed: 2026_07_29_22);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            FailoverTo(ReplicaIndex, PrimaryIndex);

            // ReplicaIndex is now primary; write through it.
            PopulateVectorSet(ReplicaIndex, Key, AddedElements, seed: 2026_07_29_23);
            context.clusterTestUtils.WaitForReplicaAofSync(ReplicaIndex, PrimaryIndex, logger: context.logger);

            ClassicAssert.AreEqual(InitialElements + AddedElements, VectorSetSize(ReplicaIndex, Key));

            MakeReadable(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// Repeated failovers re-point the replication stream several times, surfacing state that only resets in one direction.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        public void VectorSetSurvivesRepeatedFailovers()
        {
            const string Key = "{vsdisk}failoverloop";
            const int InitialElements = 100;
            const int PerRoundElements = 40;
            const int Rounds = 3;

            SetupCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, InitialElements, seed: 2026_07_29_24);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            var primary = PrimaryIndex;
            var replica = ReplicaIndex;

            for (var round = 0; round < Rounds; round++)
            {
                FailoverTo(replica, primary);
                (primary, replica) = (replica, primary);

                PopulateVectorSet(primary, Key, PerRoundElements, seed: 2026_07_29_25 + round);
                context.clusterTestUtils.WaitForReplicaAofSync(primary, replica, logger: context.logger);

                MakeReadable(replica);
                AssertFullyReplicated(primary, replica, Key);
            }

            ClassicAssert.AreEqual(
                InitialElements + (PerRoundElements * Rounds),
                VectorSetSize(primary, Key),
                "elements were lost across repeated failovers");
        }
    }
}