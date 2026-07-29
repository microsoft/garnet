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
    /// Disk-based full-sync coverage for populated Vector Sets. The replica recovers a shipped
    /// checkpoint, so OnDiskRead must zero IndexPtr and force lazy rebuild.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetDiskBasedSyncTests : VectorSetReplicationTestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        protected override Dictionary<string, LogLevel> MonitorTests => new()
        {
            [nameof(VectorSetReplicatedToReplicaByDiskBasedFullSync)] = LogLevel.Trace,
            [nameof(VectorSetsStayPartitionedAcrossDiskBasedFullSync)] = LogLevel.Trace,
            [nameof(VectorSetReplicatedToEveryReplicaByDiskBasedFullSync)] = LogLevel.Trace,
            [nameof(VectorSetReplicatedAfterDiskBasedReAttach)] = LogLevel.Trace,
        };

        /// <summary>
        /// Creates a checkpoint-sync cluster; OnDemandCheckpoint gives the primary a checkpoint to ship.
        /// </summary>
        private void SetupDiskBasedCluster(int nodeCount)
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

        /// <summary>
        /// Populates before the replica exists so the attach carries the index record in a checkpoint.
        /// The replica must rebuild its own index and keep every element.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedToReplicaByDiskBasedFullSync()
        {
            const string Key = "{vsdisk}diskbased";
            const int Elements = 400;

            SetupDiskBasedCluster(2);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_10);
            TakeCheckpoint(PrimaryIndex);

            var before = DiskBasedFullSyncCount();
            Attach(ReplicaIndex, PrimaryIndex, waitForRecovery: true);
            AssertTookDiskBasedFullSync(before);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Several differently sized sets travel in one checkpoint. Element checks catch any
        /// per-record recovery mix-up that aliases one set's index to another.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetsStayPartitionedAcrossDiskBasedFullSync()
        {
            const string SmallKey = "{vsdisk}dbsmall";
            const string MediumKey = "{vsdisk}dbmedium";
            const string LargeKey = "{vsdisk}dblarge";
            const int SmallElements = 10;
            const int MediumElements = 120;
            const int LargeElements = 350;

            SetupDiskBasedCluster(2);

            PopulateVectorSet(PrimaryIndex, LargeKey, LargeElements, seed: 2026_07_29_11);
            PopulateVectorSet(PrimaryIndex, SmallKey, SmallElements, seed: 2026_07_29_12);
            PopulateVectorSet(PrimaryIndex, MediumKey, MediumElements, seed: 2026_07_29_13);
            TakeCheckpoint(PrimaryIndex);

            var before = DiskBasedFullSyncCount();
            Attach(ReplicaIndex, PrimaryIndex, waitForRecovery: true);
            AssertTookDiskBasedFullSync(before);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, SmallKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, MediumKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, LargeKey);

            ClassicAssert.AreEqual(SmallElements, VectorSetSize(ReplicaIndex, SmallKey));
            ClassicAssert.AreEqual(MediumElements, VectorSetSize(ReplicaIndex, MediumKey));
            ClassicAssert.AreEqual(LargeElements, VectorSetSize(ReplicaIndex, LargeKey));
        }

        /// <summary>
        /// Several replicas each take a checkpoint from the same populated primary; no two nodes may share a handle.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedToEveryReplicaByDiskBasedFullSync()
        {
            const string Key = "{vsdisk}dbfanout";
            const int Elements = 200;
            const int ReplicaCount = 3;

            SetupDiskBasedCluster(1 + ReplicaCount);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_14);
            TakeCheckpoint(PrimaryIndex);

            var before = DiskBasedFullSyncCount();

            for (var replica = 1; replica <= ReplicaCount; replica++)
            {
                Attach(replica, PrimaryIndex, waitForRecovery: true);
            }

            AssertTookDiskBasedFullSync(before);

            for (var replica = 1; replica <= ReplicaCount; replica++)
            {
                MakeReadable(replica);
                AssertFullyReplicated(PrimaryIndex, replica, Key);
            }

            for (var a = 0; a <= ReplicaCount; a++)
            {
                for (var b = a + 1; b <= ReplicaCount; b++)
                {
                    AssertOwnsItsIndex(b, a, Key);
                }
            }
        }

        /// <summary>
        /// Detaches an already-synced replica and forces checkpoint re-sync. It must discard
        /// prior index state and rebuild from the checkpoint.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedAfterDiskBasedReAttach()
        {
            const string Key = "{vsdisk}dbreattach";
            const int Elements = 300;

            SetupDiskBasedCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_15);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            var before = DiskBasedFullSyncCount();

            ResetReplica(ReplicaIndex, PrimaryIndex);
            PushPrimaryAhead(PrimaryIndex);
            PopulateVectorSet(PrimaryIndex, Key, count: 100, seed: 2026_07_29_16);
            TakeCheckpoint(PrimaryIndex);
            Attach(ReplicaIndex, PrimaryIndex, waitForRecovery: true);

            AssertTookDiskBasedFullSync(before);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }
    }
}