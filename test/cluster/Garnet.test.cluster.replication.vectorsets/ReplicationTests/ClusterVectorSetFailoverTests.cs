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
    /// Failover with populated Vector Sets.
    ///
    /// <para>
    /// The one pre-existing failover test for Vector Sets,
    /// <c>FailoverStopsVectorManagerReplicationTasksAsync</c>, asserts only that
    /// <c>VectorManager.AreReplicationTasksActive</c> flips on the right node. Nothing checked that the
    /// data survived the role swap, that the promoted node owns the index it is now the authority for,
    /// or that the demoted node re-syncs correctly.
    /// </para>
    /// <para>
    /// Failover matters here because it inverts the direction of every subsequent transfer. A replica
    /// that received an index record now becomes the source of truth for it, and the old primary — the
    /// node that originally allocated the native handle — becomes a receiver. Any handle that was
    /// aliased rather than rebuilt gets a second chance to be observed, now from the other side.
    /// </para>
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetFailoverTests : VectorSetReplicationTestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        protected override Dictionary<string, LogLevel> MonitorTests => new()
        {
            [nameof(VectorSetSurvivesFailover)] = LogLevel.Trace,
            [nameof(PromotedReplicaServesVectorSetToDemotedPrimary)] = LogLevel.Trace,
            [nameof(VectorSetSurvivesRepeatedFailovers)] = LogLevel.Trace,
        };

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

        /// <summary>
        /// The basic guarantee nobody was checking: after promotion the new primary still holds every
        /// element, and the demoted node agrees with it.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
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
        /// After promotion the new primary must be able to <em>extend</em> the Vector Set it inherited,
        /// and the demoted node — the process that originally allocated the native handle — must take
        /// those writes correctly.
        ///
        /// <para>
        /// This is the interesting direction. The old primary already has a live index for this key
        /// from when it was the writer, so anything that fails to reset that state cleanly will show up
        /// as a divergence between the two nodes here.
        /// </para>
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
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

            // ReplicaIndex is now the primary; write through it.
            PopulateVectorSet(ReplicaIndex, Key, AddedElements, seed: 2026_07_29_23);
            context.clusterTestUtils.WaitForReplicaAofSync(ReplicaIndex, PrimaryIndex, logger: context.logger);

            ClassicAssert.AreEqual(InitialElements + AddedElements, VectorSetSize(ReplicaIndex, Key));

            MakeReadable(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// Failing back and forth repeatedly. Each swap re-points the replication stream, so any state
        /// that is reset on one transition but not the other accumulates; running several rounds
        /// surfaces that where a single failover would not.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
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
