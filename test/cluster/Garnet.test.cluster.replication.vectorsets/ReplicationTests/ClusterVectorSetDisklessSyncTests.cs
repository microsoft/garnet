// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Diskless full-sync coverage for populated Vector Sets. The iterator streams IndexPtr,
    /// so the receiver must sanitize and rebuild before the TLA+ quiet-read counterexample.
    /// Executable form of the model in tla/VectorIndexLifetime.tla.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetDisklessSyncTests : VectorSetReplicationTestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        /// <summary>Creates a diskless-sync cluster with a low threshold so re-attach takes full sync.</summary>
        private void SetupDisklessCluster(int nodeCount)
        {
            context.CreateInstances(
                nodeCount,
                enableAOF: true,
                enableDisklessSync: true,
                replicaDisklessSyncFullSyncAofThreshold: "1k",
                timeout: timeout);

            context.FormClusterAllNodes(nodeCount);
        }

        /// <summary>
        /// Forces the model's reset/snapshot path: detach, move past incremental replay, then re-attach.
        /// Full sync is guaranteed by two independent triggers in ReplicaSyncSession.NeedToFullSync:
        /// the soft reset gives the node a fresh replication id, and the padding writes push the AOF
        /// gap past the 1k threshold configured in SetupDisklessCluster.
        /// </summary>
        private void ForceDisklessFullSync(int replicaIndex = ReplicaIndex)
        {
            context.clusterTestUtils.ResetReplica(replicaIndex, PrimaryIndex, context.logger);
            context.clusterTestUtils.PushPrimaryAhead(PrimaryIndex);
            context.clusterTestUtils.Attach(replicaIndex, PrimaryIndex, logger: context.logger);
        }

        /// <summary>
        /// A populated set travels by diskless full sync, then the replica reads it. This catches
        /// foreign IndexPtr dereferences that used to surface at NativeDiskANNMethods.card.
        /// </summary>
        [Test]
        public void VectorSetReadableOnReplicaAfterDisklessFullSync()
        {
            const string Key = "{vsdisk}solo";
            const int Elements = 500;

            SetupDisklessCluster(2);
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            // Attached writes arrive as VADD replay, so the replica builds its own index.
            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_00);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            ForceDisklessFullSync();

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Multiple sets can make a foreign handle point at the wrong live index instead of faulting.
        /// Different sizes plus element checks expose cross-set substitution.
        /// </summary>
        [Test]
        public void VectorSetsStayPartitionedAcrossDisklessFullSync()
        {
            // Same hash slot, so both keys live on the single primary in this topology.
            const string SmallKey = "{vsdisk}small";
            const string LargeKey = "{vsdisk}large";
            const int SmallElements = 10;
            const int LargeElements = 400;

            SetupDisklessCluster(2);
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            PopulateVectorSet(PrimaryIndex, LargeKey, LargeElements, seed: 2026_07_29_02);
            PopulateVectorSet(PrimaryIndex, SmallKey, SmallElements, seed: 2026_07_29_03);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            ClassicAssert.AreEqual(LargeElements, VectorSetSize(PrimaryIndex, LargeKey));
            ClassicAssert.AreEqual(SmallElements, VectorSetSize(PrimaryIndex, SmallKey));

            ForceDisklessFullSync();

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, SmallKey);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, LargeKey);

            // Neither key may report the other's cardinality.
            ClassicAssert.AreEqual(SmallElements, VectorSetSize(ReplicaIndex, SmallKey), "small set must not report elements belonging to another Vector Set");
            ClassicAssert.AreEqual(LargeElements, VectorSetSize(ReplicaIndex, LargeKey), "large set must report exactly its own elements");
        }

        /// <summary>
        /// Control: without full sync, replicated VADD payloads build the replica's local index,
        /// so this should pass even if full-sync cases fail.
        /// </summary>
        [Test]
        public void VectorSetReadableOnReplicaWithoutFullSync()
        {
            const string Key = "{vsdisk}control";
            const int Elements = 500;

            SetupDisklessCluster(2);
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_04);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);

            // Without full sync, VADD replay builds a local index and isolates full sync as the cause.
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Every replica takes its own diskless full sync from the same populated primary.
        /// Each must rebuild its own index, not share per-primary state.
        /// </summary>
        [Test]
        public void VectorSetReplicatedToEveryReplicaByDisklessFullSync()
        {
            const string Key = "{vsdisk}fanout";
            const int Elements = 200;
            const int ReplicaCount = 3;

            SetupDisklessCluster(1 + ReplicaCount);

            // Populated before any replica exists, so each attach carries the index record.
            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_06);

            // Each replica starts with its own replication id, so every attach takes a full sync.
            for (var replica = 1; replica <= ReplicaCount; replica++)
            {
                context.clusterTestUtils.Attach(replica, PrimaryIndex, logger: context.logger);
            }

            for (var replica = 1; replica <= ReplicaCount; replica++)
            {
                context.clusterTestUtils.ReadOnly(replica);
                AssertFullyReplicated(PrimaryIndex, replica, Key);
            }

            // No two nodes may share a handle.
            for (var a = 0; a <= ReplicaCount; a++)
            {
                for (var b = a + 1; b <= ReplicaCount; b++)
                {
                    AssertOwnsItsIndex(b, a, Key);
                }
            }
        }

        /// <summary>
        /// Promotes a replica after diskless full sync, then syncs back to the old primary.
        /// The record crosses both directions and both nodes must own their indexes.
        /// </summary>
        [Test]
        public void VectorSetSurvivesFailoverAfterDisklessFullSync()
        {
            const string Key = "{vsdisk}failover";
            const int Elements = 200;

            SetupDisklessCluster(2);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_07);

            // The replica has its own replication id, so the attach takes a full sync.
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            context.FailoverTo(ReplicaIndex, PrimaryIndex);

            context.clusterTestUtils.ReadOnly(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);

            // Inherited sets must still accept writes and replicate them back.
            PopulateVectorSet(ReplicaIndex, Key, count: 50, seed: 2026_07_29_08);
            context.clusterTestUtils.WaitForReplicaAofSync(ReplicaIndex, PrimaryIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(PrimaryIndex);
            AssertFullyReplicated(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// A brand-new Vector Set created on a node that received a set by diskless full sync must
        /// not be handed the streamed set's context. The stream-in path installs the index record
        /// but never reserves its context in the receiver's allocator, so the next allocation can
        /// collide with it. Promoting the receiver makes it writable so the fresh create runs there.
        /// </summary>
        [Test]
        public void FreshVectorSetDoesNotReuseStreamedContextAfterDisklessFullSync()
        {
            const string StreamedKey = "{vsctx}streamed";
            const string FreshKey = "{vsctx}fresh";
            const int Elements = 200;

            SetupDisklessCluster(2);

            PopulateVectorSet(PrimaryIndex, StreamedKey, Elements, seed: 2026_07_29_10);

            // The replica has its own replication id, so the attach takes a full sync that carries
            // the streamed set's index record (and its context) verbatim.
            context.clusterTestUtils.Attach(ReplicaIndex, PrimaryIndex, logger: context.logger);

            context.clusterTestUtils.ReadOnly(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, StreamedKey);

            // Promote the receiver so the fresh create allocates a context on the very node that
            // took the diskless full sync.
            context.FailoverTo(ReplicaIndex, PrimaryIndex);

            var streamedContext = ReadPersistedContext(ReplicaIndex, StreamedKey);

            PopulateVectorSet(ReplicaIndex, FreshKey, count: 50, seed: 2026_07_29_11);
            var freshContext = ReadPersistedContext(ReplicaIndex, FreshKey);

            ClassicAssert.AreNotEqual(streamedContext, freshContext, $"a fresh Vector Set '{FreshKey}' was handed the streamed set '{StreamedKey}' context ({streamedContext}); the diskless full-sync receiver never reserved the streamed context, so the allocator reissued it");
        }

        /// <summary>
        /// Migrates a populated Vector Set between primaries while diskless sync is enabled.
        /// All four nodes must agree and no two may share a handle.
        /// </summary>
        [Test]
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

            var primary0 = context.clusterTestUtils.GetEndPoint(Primary0);
            var primary1 = context.clusterTestUtils.GetEndPoint(Primary1);
            var primary0Id = context.clusterTestUtils.ClusterMyId(primary0, logger: context.logger);
            var slots = context.clusterTestUtils.ClusterSlots(primary0, logger: context.logger);

            // Find a key that hashes into a slot Primary0 owns.
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

            // Read before migration because Primary0 drops the record once the slot moves.
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

            // The migrated-to primary must build its own index, not reuse Primary0's handle.
            var handleAfterMigration = ReadPersistedIndexPtr(Primary1, key);
            ClassicAssert.AreNotEqual(nint.Zero, handleAfterMigration, $"node {Primary1} should have built its own index for '{key}'");
            ClassicAssert.AreNotEqual(handleBeforeMigration, handleAfterMigration, $"node {Primary1} adopted node {Primary0}'s DiskANN handle for '{key}' across the migration");
        }
    }
}