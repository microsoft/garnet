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
    /// Vector Sets carried to another node by a <em>diskless</em> full sync.
    ///
    /// <para>
    /// The diskless snapshot iterator streams raw Tsavorite log records verbatim
    /// (<c>ReplicationSnapshotIterator</c> -&gt; <c>DiskLogRecord.Serialize</c>). A Vector Set's index
    /// record persists the native DiskANN handle <c>IndexPtr</c> at offset 8 of its 56-byte value
    /// (<c>VectorManager.Index</c>), so the replica receives the <em>primary's</em> pointer.
    /// </para>
    /// <para>
    /// The only hook that zeroes that field is <c>GarnetRecordTriggers.OnDiskRead</c>, which by
    /// definition never fires for records streamed straight into memory. Since
    /// <c>VectorManager.NeedsRecreate</c> is exactly <c>indexPtr == 0</c>, a non-zero foreign pointer
    /// is indistinguishable from a healthy local one, the lazy <c>Service.RecreateIndex</c> rebuild is
    /// skipped, and the raw value reaches the P/Invoke unvalidated. In production this faults the
    /// replica with SIGSEGV inside <c>NativeDiskANNMethods.card</c>.
    /// </para>
    /// <para>
    /// The pre-existing cluster Vector Set fixture cannot catch this. It goes through
    /// <c>SimpleSetupClusterAsync</c>, which never passes <c>enableDisklessSync</c>, and it attaches
    /// replicas up front and then writes, so each replica builds its own native index from replicated
    /// VADD payloads. The defect needs an <em>already-populated</em> Vector Set to travel over a full
    /// sync.
    /// </para>
    /// <para>
    /// These are the executable form of the TLA+ model in <c>tla/VectorIndexLifetime.tla</c>. Every
    /// counterexample TLC produced shares one 6-step trace: <c>SyncReset</c> -&gt;
    /// <c>PrimaryCreate(k1)</c> -&gt; <c>SyncSnapshot</c> -&gt; <c>BeginRead(k1)</c> -&gt;
    /// <c>Deref</c>. The three preconditions it isolates are what each test sets up: (1) the receiving
    /// node takes a full sync, (2) the source's record for the key carries a live non-zero handle at
    /// snapshot time, and (3) the receiving node subsequently reads that key. The
    /// <c>MC_Vec_QuiesceOnly_Buggy</c> scenario still failed, which is why none of these tests need to
    /// race anything: a quiet, fully serialized read after the sync completes is enough.
    /// </para>
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetDisklessSyncTests : VectorSetReplicationTestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        protected override Dictionary<string, LogLevel> MonitorTests => new()
        {
            [nameof(VectorSetReadableOnReplicaAfterDisklessFullSync)] = LogLevel.Trace,
            [nameof(VectorSetsStayPartitionedAcrossDisklessFullSync)] = LogLevel.Trace,
            [nameof(VectorSetReadableOnReplicaWithoutFullSync)] = LogLevel.Trace,
            [nameof(VectorSetReplicatedToEveryReplicaByDisklessFullSync)] = LogLevel.Trace,
            [nameof(VectorSetSurvivesFailoverAfterDisklessFullSync)] = LogLevel.Trace,
            [nameof(VectorSetMigratedBetweenPrimariesWithDisklessSync)] = LogLevel.Trace,
        };

        /// <summary>
        /// Stands up <paramref name="nodeCount"/> nodes configured for diskless sync with the full-sync
        /// AOF threshold pinned low, gives node 0 every slot, and introduces the rest to it.
        /// </summary>
        private void SetupDisklessCluster(int nodeCount)
        {
            context.CreateInstances(
                nodeCount,
                enableAOF: true,
                enableDisklessSync: true,
                replicaDisklessSyncFullSyncAofThreshold: "1k",
                timeout: timeout);
            context.CreateConnection();

            FormCluster(PrimaryIndex, [.. Enumerable.Range(1, nodeCount - 1)]);
        }

        /// <summary>
        /// The model's <c>SyncReset</c> followed by <c>SyncSnapshot</c>: detach the replica, move the
        /// primary far enough ahead that the re-attach cannot be served incrementally, then re-attach.
        /// </summary>
        private void ForceDisklessFullSync(int replicaIndex = ReplicaIndex)
        {
            var before = FullSyncCount();

            ResetReplica(replicaIndex, PrimaryIndex);
            PushPrimaryAhead(PrimaryIndex);
            Attach(replicaIndex, PrimaryIndex);

            AssertTookFullSync(before);
        }

        /// <summary>
        /// Executable form of <c>MC_Vec_Buggy</c> / <c>MC_Vec_Segfault_Buggy</c>, which violate
        /// <c>HandleIsLocal</c> and <c>NoSegfault</c>.
        ///
        /// A Vector Set that already exists on the primary is carried to the replica by a diskless full
        /// sync, and the replica is then asked to read it. The replica receives the index record with
        /// the primary's <c>IndexPtr</c> intact, never rebuilds the index locally, and the first read
        /// that reaches <c>NativeDiskANNMethods.card</c> dereferences a handle it does not own.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReadableOnReplicaAfterDisklessFullSync()
        {
            const string Key = "{vsdisk}solo";
            const int Elements = 500;

            SetupDisklessCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            // Written while the replica is attached, so it arrives as VADD payloads and the replica
            // builds its own native index. This much already works today.
            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_00);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            ForceDisklessFullSync();

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Executable form of <c>MC_Vec_WrongIndex_Buggy</c>, which violates <c>NoWrongIndex</c>.
        ///
        /// With more than one Vector Set in play a streamed foreign handle can resolve to a different
        /// live index rather than to unmapped memory, in which case the read succeeds and quietly
        /// reports another set's contents. Two sets of deliberately different sizes make that
        /// substitution visible: each key must report exactly its own elements, and the element-level
        /// sweep catches the case where the counts happen to line up but the vectors do not.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetsStayPartitionedAcrossDisklessFullSync()
        {
            // Same hash slot, so both keys live on the single primary in this topology.
            const string SmallKey = "{vsdisk}small";
            const string LargeKey = "{vsdisk}large";
            const int SmallElements = 10;
            const int LargeElements = 400;

            SetupDisklessCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, LargeKey, LargeElements, seed: 2026_07_29_02);
            PopulateVectorSet(PrimaryIndex, SmallKey, SmallElements, seed: 2026_07_29_03);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            ClassicAssert.AreEqual(LargeElements, VectorSetSize(PrimaryIndex, LargeKey));
            ClassicAssert.AreEqual(SmallElements, VectorSetSize(PrimaryIndex, SmallKey));

            ForceDisklessFullSync();

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, SmallKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, LargeKey);

            // Explicit cross-check of the substitution the model describes: neither key may report the
            // other's cardinality.
            ClassicAssert.AreEqual(SmallElements, VectorSetSize(ReplicaIndex, SmallKey), "small set must not report elements belonging to another Vector Set");
            ClassicAssert.AreEqual(LargeElements, VectorSetSize(ReplicaIndex, LargeKey), "large set must report exactly its own elements");
        }

        /// <summary>
        /// Executable form of the <c>MC_Vec_NoFullSync</c> control, which TLC verified.
        ///
        /// Identical to <see cref="VectorSetReadableOnReplicaAfterDisklessFullSync"/> except that the
        /// replica never takes a full sync. Removing that single step is what makes the model safe, so
        /// this test must pass even while the others fail; if it ever fails the defect is not the one
        /// the model describes and the rest of this fixture is measuring the wrong thing.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReadableOnReplicaWithoutFullSync()
        {
            const string Key = "{vsdisk}control";
            const int Elements = 500;

            SetupDisklessCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_04);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);

            // The control's whole point: without a full sync the replica builds its own index from
            // replicated VADD payloads, so this holds today and pins down the full sync as the cause.
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Gap: diskless coverage beyond a single replica.
        ///
        /// Every replica attaching to a populated primary takes its own streaming full sync, so each
        /// one independently receives the same foreign <c>IndexPtr</c>. All of them must end up with
        /// their own index and the full element set — a fix that only sanitizes the first sync session,
        /// or that hangs the sanitization off shared per-primary state, would pass the two-node tests
        /// and fail here.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetReplicatedToEveryReplicaByDisklessFullSync()
        {
            const string Key = "{vsdisk}fanout";
            const int Elements = 200;
            const int ReplicaCount = 3;

            SetupDisklessCluster(1 + ReplicaCount);

            // Populated before any replica exists, so every attach must carry the index record itself.
            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_06);

            var before = FullSyncCount();

            for (var replica = 1; replica <= ReplicaCount; replica++)
            {
                Attach(replica, PrimaryIndex);
            }

            AssertTookFullSync(before);

            for (var replica = 1; replica <= ReplicaCount; replica++)
            {
                MakeReadable(replica);
                AssertFullyReplicated(PrimaryIndex, replica, Key);
            }

            // No two nodes may share a handle, not just replica-vs-primary.
            for (var a = 0; a <= ReplicaCount; a++)
            {
                for (var b = a + 1; b <= ReplicaCount; b++)
                {
                    AssertOwnsItsIndex(b, a, Key);
                }
            }
        }

        /// <summary>
        /// Gap: diskless full sync followed by a failover.
        ///
        /// A replica that took a full sync is promoted, which makes it the authority for data it never
        /// built an index for. The old primary then attaches to it and syncs back the other way, so the
        /// same record makes a second crossing in the opposite direction. Both nodes must still own
        /// their own index and hold every element.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetSurvivesFailoverAfterDisklessFullSync()
        {
            const string Key = "{vsdisk}failover";
            const int Elements = 200;

            SetupDisklessCluster(2);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_07);

            var before = FullSyncCount();
            Attach(ReplicaIndex, PrimaryIndex);
            AssertTookFullSync(before);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            // Promote the replica. Roles now swap: ReplicaIndex is the primary, PrimaryIndex the replica.
            context.ClusterFailoverSpinWait(ReplicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(ReplicaIndex, PrimaryIndex, logger: context.logger);

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(Node(ReplicaIndex), logger: context.logger).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(Node(PrimaryIndex), logger: context.logger).Value);

            MakeReadable(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);

            // The promoted node must still accept writes into the set it inherited, and they must reach
            // the demoted one.
            PopulateVectorSet(ReplicaIndex, Key, count: 50, seed: 2026_07_29_08);
            context.clusterTestUtils.WaitForReplicaAofSync(ReplicaIndex, PrimaryIndex, logger: context.logger);

            MakeReadable(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// Gap: slot migration while diskless sync is enabled.
        ///
        /// Migration is a third transport for an index record (<c>SetContextForMigration</c>) and it
        /// runs concurrently with replication. This moves a populated Vector Set between two primaries
        /// that each have a replica, then requires all four nodes to be consistent and, critically, for
        /// no two of them to share a handle.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetMigratedBetweenPrimariesWithDisklessSync()
        {
            const int Primary0 = 0;
            const int Primary1 = 1;
            const int Replica0 = 2;
            const int Replica1 = 3;
            const int Elements = 150;

            context.CreateInstances(
                4,
                enableAOF: true,
                enableDisklessSync: true,
                timeout: timeout);
            context.CreateConnection();

            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 2, replica_count: 1, logger: context.logger);

            var primary0 = Node(Primary0);
            var primary1 = Node(Primary1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0, logger: context.logger);
            var slots = context.clusterTestUtils.ClusterSlots(primary0, logger: context.logger);

            // Find a key that hashes into a slot Primary0 actually owns.
            string key;
            int hashSlot;
            var ix = 0;
            while (true)
            {
                key = $"{nameof(VectorSetMigratedBetweenPrimariesWithDisklessSync)}_{ix}";
                hashSlot = context.clusterTestUtils.HashSlot(key);

                if (slots.Any(x => x.nnInfo.Any(y => y.nodeid == primary0Id) && hashSlot >= x.startSlot && hashSlot <= x.endSlot))
                    break;

                ix++;
            }

            PopulateVectorSet(Primary0, key, Elements, seed: 2026_07_29_09);
            context.clusterTestUtils.WaitForReplicaAofSync(Primary0, Replica0, logger: context.logger);

            WaitUntilServes(Replica0, key);
            AssertFullyReplicated(Primary0, Replica0, key);

            // Captured before the migration: once the slot moves, Primary0 drops the record entirely.
            var handleBeforeMigration = ReadPersistedIndexPtr(Primary0, key);
            ClassicAssert.AreNotEqual(nint.Zero, handleBeforeMigration);

            context.clusterTestUtils.MigrateSlots(primary0, primary1, [hashSlot], logger: context.logger);
            context.clusterTestUtils.WaitForMigrationCleanup(Primary0, context.cts.Token, context.logger);
            context.clusterTestUtils.WaitForMigrationCleanup(Primary1, context.cts.Token, context.logger);

            context.clusterTestUtils.WaitForReplicaAofSync(Primary0, Replica0, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(Primary1, Replica1, logger: context.logger);

            ClassicAssert.IsFalse(context.clusterTestUtils.GetOwnedSlotsFromNode(primary0, context.logger).Contains(hashSlot));
            ClassicAssert.IsTrue(context.clusterTestUtils.GetOwnedSlotsFromNode(primary1, context.logger).Contains(hashSlot));

            WaitUntilServes(Replica1, key);
            AssertFullyReplicated(Primary1, Replica1, key);

            // The migrated-to primary must serve an index it built itself rather than the handle the
            // source primary was using, which is meaningless in this process once the slot has moved.
            var handleAfterMigration = ReadPersistedIndexPtr(Primary1, key);
            ClassicAssert.AreNotEqual(nint.Zero, handleAfterMigration, $"node {Primary1} should have built its own index for '{key}'");
            ClassicAssert.AreNotEqual(handleBeforeMigration, handleAfterMigration, $"node {Primary1} adopted node {Primary0}'s DiskANN handle for '{key}' across the migration");
        }
    }
}
