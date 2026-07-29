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
    /// Vector Sets carried to a replica by a <em>disk-based</em> (checkpoint) full sync.
    ///
    /// <para>
    /// This is the counterpart transport to <see cref="ClusterVectorSetDisklessSyncTests"/> and it had
    /// no coverage at all. The pre-existing cluster Vector Set fixture attaches its replicas at setup
    /// and only then writes, so replicas full-sync against an <em>empty</em> store and no index record
    /// ever crosses the wire; everything after that arrives as replicated VADD payloads over AOF. The
    /// interesting case — a replica taking a checkpoint of an already-populated Vector Set — was never
    /// exercised.
    /// </para>
    /// <para>
    /// Unlike the diskless path, this one is expected to be safe by construction: the replica recovers
    /// the received checkpoint from disk, and <c>Recovery.cs</c> fires
    /// <c>GarnetRecordTriggers.OnDiskRead</c> per record during that pass, which zeroes
    /// <c>IndexPtr</c> and forces the lazy <c>Service.RecreateIndex</c> rebuild. That is a code-reading
    /// argument, not a tested one, and the gate is itself conditional
    /// (<c>CallOnDiskRead =&gt; rangeIndexManager != null || vectorManager != null</c>), so a
    /// regression there would otherwise be silent. These tests pin that behaviour down.
    /// </para>
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
        /// Stands up <paramref name="nodeCount"/> nodes with diskless sync explicitly <b>off</b>, so a
        /// replica attach transmits a checkpoint. <c>OnDemandCheckpoint</c> ensures the primary has one
        /// to send even though nothing has explicitly saved.
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
        /// The disk-based analogue of
        /// <see cref="ClusterVectorSetDisklessSyncTests.VectorSetReadableOnReplicaAfterDisklessFullSync"/>.
        ///
        /// The Vector Set is fully populated <em>before</em> the replica exists, so the attach has no
        /// choice but to carry the index record inside a checkpoint. The replica must end up with its
        /// own index and with every element that was written.
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
        /// Disk-based analogue of
        /// <see cref="ClusterVectorSetDisklessSyncTests.VectorSetsStayPartitionedAcrossDisklessFullSync"/>.
        ///
        /// Several Vector Sets of deliberately different sizes travel in one checkpoint. If recovery
        /// were to mis-handle the per-record context, one key could end up answering out of another's
        /// index; the element sweep catches that even when the cardinalities happen to line up.
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
        /// Several replicas each take their own checkpoint of the same populated primary. As in the
        /// diskless fan-out case, no two nodes may share a handle.
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
        /// The replica is attached and in sync first, so it builds its own index from VADD payloads,
        /// and only then is it detached and forced to re-sync from a checkpoint.
        ///
        /// <para>
        /// This is the disk-based shape of the production incident: the node already holds a working
        /// Vector Set, loses its link, and comes back through a full sync. Its own index must be
        /// discarded and rebuilt rather than left aliasing whatever the checkpoint carried.
        /// </para>
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