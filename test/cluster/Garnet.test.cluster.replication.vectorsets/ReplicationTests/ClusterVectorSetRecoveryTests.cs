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
    /// Restart recovery for clustered Vector Sets. A checkpointed IndexPtr belongs to the old
    /// process and must be discarded so lazy rebuild creates a local index before replication resumes.
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
        /// Forces a checkpoint so restart recovers persisted records instead of replaying the whole AOF.
        /// </summary>
        private void Checkpoint(int nodeIndex)
        {
            WaitForVectorReplay(nodeIndex);
            TakeCheckpoint(nodeIndex);
        }

        /// <summary>A recovered handle must differ from the dead process's handle.</summary>
        private void AssertIndexRebuiltAfterRestart(int nodeIndex, string key, nint beforeRestart)
        {
            var after = ReadPersistedIndexPtr(nodeIndex, key);

            ClassicAssert.AreNotEqual(
                beforeRestart,
                after,
                $"node {nodeIndex} recovered '{key}' still holding the DiskANN handle from its previous incarnation (0x{beforeRestart:x}); that address space is gone");
        }

        /// <summary>
        /// Restarts the replica while the primary stays up; it must rebuild, keep every element,
        /// and agree with the primary.
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

            // Reading first drives lazy rebuild, so inspect the pointer afterwards.
            AssertVectorSetsMatch(PrimaryIndex, ReplicaIndex, Key);
            AssertIndexRebuiltAfterRestart(ReplicaIndex, Key, replicaPtrBefore);
            AssertOwnsItsIndex(ReplicaIndex, PrimaryIndex, Key);
        }

        /// <summary>
        /// Restarts the primary under a live replica, then verifies recovery can still accept
        /// writes and seed a newly attached replica.
        /// </summary>
        [Test]
        [Category("REPLICATION")]
        [CancelAfter(180_000)]
        public void PrimaryRebuildsVectorSetIndexAfterRestart()
        {
            const string Key = "{vsdisk}restartprimary";
            const int Elements = 250;
            const int SpareIndex = 2;

            SetupRecoverableCluster(3);
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

            PopulateVectorSet(PrimaryIndex, Key, count: 50, seed: 2026_07_29_19);

            Attach(SpareIndex, PrimaryIndex);
            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, SpareIndex, logger: context.logger);

            MakeReadable(SpareIndex);
            AssertFullyReplicated(PrimaryIndex, SpareIndex, Key);
        }

        /// <summary>
        /// Restarts both nodes from checkpoints; both must discard old handles and still match element for element.
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