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
    /// Vector Sets across a process restart inside a cluster.
    ///
    /// <para>
    /// Restart is the fourth way a persisted index record can be read by a process that did not create
    /// it — here the "other process" is the node's own previous incarnation. The <c>IndexPtr</c> stored
    /// in the checkpoint belongs to an address space that no longer exists, so recovery must discard it
    /// and let <c>Service.RecreateIndex</c> rebuild lazily. <c>Recovery.cs</c> does that by firing
    /// <c>GarnetRecordTriggers.OnDiskRead</c> per record as it loads pages.
    /// </para>
    /// <para>
    /// The standalone suite covers <c>SAVE</c> + <c>tryRecover</c> well, but no cluster test did, and
    /// in a cluster the recovered node immediately re-enters a replication relationship — so a stale
    /// handle would not merely fault locally, it would propagate.
    /// </para>
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorSetRecoveryTests : VectorSetReplicationTestBase
    {
        private const int PrimaryIndex = 0;
        private const int ReplicaIndex = 1;

        protected override Dictionary<string, LogLevel> MonitorTests => new()
        {
            [nameof(ReplicaRebuildsVectorSetIndexAfterRestart)] = LogLevel.Trace,
            [nameof(PrimaryRebuildsVectorSetIndexAfterRestart)] = LogLevel.Trace,
            [nameof(VectorSetSurvivesRestartOfBothNodes)] = LogLevel.Trace,
        };

        private void SetupRecoverableCluster(int nodeCount)
        {
            context.CreateInstances(
                nodeCount,
                enableAOF: true,
                tryRecover: true,
                OnDemandCheckpoint: true,
                enableDisklessSync: false,
                timeout: timeout);
            context.CreateConnection();

            FormCluster(PrimaryIndex, [.. Enumerable.Range(1, nodeCount - 1)]);
        }

        private void Restart(int nodeIndex)
        {
            context.RestartNode(nodeIndex, ensureAofFlush: true);
            context.CreateConnection();
        }

        /// <summary>
        /// Forces a checkpoint so the restart has something to recover from rather than replaying the
        /// whole AOF.
        /// </summary>
        private void Checkpoint(int nodeIndex)
        {
            WaitForVectorReplay(nodeIndex);
            TakeCheckpoint(nodeIndex);
        }

        /// <summary>
        /// A handle recovered from a checkpoint belongs to a dead process. Whatever the node ends up
        /// dereferencing after recovery, it must not be the pointer it wrote before it went down.
        /// </summary>
        private void AssertIndexRebuiltAfterRestart(int nodeIndex, string key, nint beforeRestart)
        {
            var after = ReadPersistedIndexPtr(nodeIndex, key);

            ClassicAssert.AreNotEqual(
                beforeRestart,
                after,
                $"node {nodeIndex} recovered '{key}' still holding the DiskANN handle from its previous incarnation (0x{beforeRestart:x}); that address space is gone");
        }

        /// <summary>
        /// The replica is restarted while the primary stays up. It must rebuild its own index from the
        /// recovered checkpoint, keep every element, and end up agreeing with the primary again.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void ReplicaRebuildsVectorSetIndexAfterRestart()
        {
            const string Key = "{vsdisk}restartreplica";
            const int Elements = 250;

            SetupRecoverableCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_17);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            Checkpoint(PrimaryIndex);
            var replicaPtrBefore = ReadPersistedIndexPtr(ReplicaIndex, Key);

            Restart(ReplicaIndex);
            context.clusterTestUtils.WaitForReplicaRecovery(ReplicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);

            // Reading first is what drives the lazy rebuild, so the data assertions have to come before
            // the pointer is inspected.
            AssertVectorSetsMatch(PrimaryIndex, ReplicaIndex, Key);
            AssertIndexRebuiltAfterRestart(ReplicaIndex, Key, replicaPtrBefore);
            AssertOwnsItsIndex(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// The primary is restarted underneath a live replica. It must recover its Vector Sets, rebuild
        /// its own index, and continue to serve the replica correctly — including for writes issued
        /// after it comes back.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void PrimaryRebuildsVectorSetIndexAfterRestart()
        {
            const string Key = "{vsdisk}restartprimary";
            const int Elements = 250;

            SetupRecoverableCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_18);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            Checkpoint(PrimaryIndex);
            var primaryPtrBefore = ReadPersistedIndexPtr(PrimaryIndex, Key);

            Restart(PrimaryIndex);

            ClassicAssert.AreEqual(Elements, VectorSetSize(PrimaryIndex, Key), "the restarted primary lost elements of the recovered Vector Set");
            AssertIndexRebuiltAfterRestart(PrimaryIndex, Key, primaryPtrBefore);

            // The recovered primary must still be able to take writes and propagate them.
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);
            PopulateVectorSet(PrimaryIndex, Key, count: 50, seed: 2026_07_29_19);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);
        }

        /// <summary>
        /// Whole-cluster restart: both nodes recover independently from their own checkpoints, so both
        /// are reading records written by processes that no longer exist. Neither may come back holding
        /// its old handle, and they must still agree element for element.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void VectorSetSurvivesRestartOfBothNodes()
        {
            const string Key = "{vsdisk}restartboth";
            const int Elements = 200;

            SetupRecoverableCluster(2);
            Attach(ReplicaIndex, PrimaryIndex);

            PopulateVectorSet(PrimaryIndex, Key, Elements, seed: 2026_07_29_20);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertFullyReplicated(PrimaryIndex, ReplicaIndex, Key);

            Checkpoint(PrimaryIndex);
            Checkpoint(ReplicaIndex);

            var primaryPtrBefore = ReadPersistedIndexPtr(PrimaryIndex, Key);
            var replicaPtrBefore = ReadPersistedIndexPtr(ReplicaIndex, Key);

            Restart(PrimaryIndex);
            Restart(ReplicaIndex);

            context.clusterTestUtils.WaitForReplicaRecovery(ReplicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, ReplicaIndex, logger: context.logger);

            MakeReadable(ReplicaIndex);
            AssertVectorSetsMatch(PrimaryIndex, ReplicaIndex, Key);

            AssertIndexRebuiltAfterRestart(PrimaryIndex, Key, primaryPtrBefore);
            AssertIndexRebuiltAfterRestart(ReplicaIndex, Key, replicaPtrBefore);
            AssertOwnsItsIndex(ReplicaIndex, PrimaryIndex, Key);
        }
    }
}
