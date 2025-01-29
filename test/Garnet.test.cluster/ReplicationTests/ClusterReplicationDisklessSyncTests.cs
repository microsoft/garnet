// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// TODO: Testing scenarios
    /// 1. Empty replica attach sync
    ///     a. Primary empty
    ///     b. Primary non-empty
    /// 2. Replica same history and version different AOF
    /// 3. Replica same history and different version and AOF
    /// 4. Replica different history, version and AOF
    /// </summary>
    [NonParallelizable]
    public class ClusterReplicationDisklessSyncTests
    {
        ClusterTestContext context;
        readonly int keyCount = 256;

        protected bool useTLS = false;
        protected bool asyncReplay = false;

        public Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        void PopulatePrimary(int primaryIndex, bool disableObjects, bool performRMW)
        {
            context.kvPairs = context.kvPairs ?? ([]);
            context.kvPairsObj = context.kvPairsObj ?? ([]);
            var addCount = 5;
            var keyLength = 16;
            var kvpairCount = keyCount;
            context.kvPairs = [];
            context.kvPairsObj = [];
            // New insert
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: primaryIndex);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: 0, addCount);

            if (!disableObjects)
                context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex: 0);
        }

        void Validate(int primaryIndex, int replicaIndex, bool disableObjects)
        {
            // Validate replica data
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex: replicaIndex, primaryIndex: primaryIndex);
            if (disableObjects)
                context.ValidateNodeObjects(ref context.kvPairsObj, replicaIndex);
        }

        void ResetAndReAttach(int replicaIndex, int primaryIndex, bool soft)
        {
            // Soft reset replica
            _ = context.clusterTestUtils.ClusterReset(replicaIndex, soft: soft, expiry: 1, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(replicaIndex, logger: context.logger);
            // Re-introduce node after reset
            while (!context.clusterTestUtils.IsKnown(replicaIndex, primaryIndex, logger: context.logger))
            {
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
                context.clusterTestUtils.Meet(replicaIndex, primaryIndex, logger: context.logger);
            }
        }

        /// <summary>
        /// Attach empty replica after primary has been populated with some data
        /// </summary>
        /// <param name="disableObjects"></param>
        /// <param name="performRMW"></param>
        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterEmptyReplicaDisklessSync([Values] bool disableObjects, [Values] bool performRMW)
        {
            var nodes_count = 2;
            var primaryIndex = 0;
            var replicaIndex = 1;
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true);
            context.CreateConnection(useTLS: useTLS);

            // Setup primary and introduce it to future replica
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            // Ensure node is known
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            // Populate Primary
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Attach sync session
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Wait for replica to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);

            // Validate replica data
            Validate(primaryIndex, replicaIndex, disableObjects);
        }


        /// <summary>
        /// Re-attach replica on disconnect after it has received some amount of data.
        /// The replica should replay only the portion it missed without doing a full sync
        /// </summary>
        /// <param name="disableObjects"></param>
        /// <param name="performRMW"></param>
        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterAofReplayDisklessSync([Values] bool disableObjects, [Values] bool performRMW)
        {
            var nodes_count = 2;
            var primaryIndex = 0;
            var replicaIndex = 1;
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true);
            context.CreateConnection(useTLS: useTLS);

            // Setup primary and introduce it to future replica
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            // Ensure node is known
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            // Attach sync session
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Wait for replica to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);

            // Populate Primary
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Validate replica data
            Validate(primaryIndex, replicaIndex, disableObjects);

            // Reset and re-attach replica as primary
            ResetAndReAttach(replicaIndex, primaryIndex, soft: true);

            // Populate Primary (ahead of replica)
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Re-attach sync session
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Validate replica data
            Validate(primaryIndex, replicaIndex, disableObjects);
        }

        /// <summary>
        /// Attach one replica and populate it with data through primary.
        /// Disconnect replica and attach a new empty replica.
        /// Populate new replica with more data
        /// Re-attach disconnected replica.
        /// This should perform a full sync when the old replica is attach because primary will be in version v
        /// (because of syncing with new empty replica) and old replica will be in version v - 1.
        /// </summary>
        /// <param name="disableObjects"></param>
        /// <param name="performRMW"></param>
        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterDBVersionAlignmentDisklessSync([Values] bool disableObjects, [Values] bool performRMW)
        {
            var nodes_count = 3;
            var primaryIndex = 0;
            var replicaOneIndex = 1;
            var replicaTwoIndex = 2;
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true);
            context.CreateConnection(useTLS: useTLS);

            // Setup primary and introduce it to future replica
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaOneIndex, replicaOneIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaTwoIndex, replicaTwoIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaOneIndex, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaTwoIndex, logger: context.logger);

            // Ensure node everybody knowns everybody
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaOneIndex, primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaTwoIndex, primaryIndex, logger: context.logger);

            // Populate Primary
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Attach first replica
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaOneIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Validate first replica data
            Validate(primaryIndex, replicaOneIndex, disableObjects);

            // Validate db version
            var primaryVersion = context.clusterTestUtils.GetStoreCurrentVersion(primaryIndex, isMainStore: true, logger: context.logger);
            var replicaOneVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaOneIndex, isMainStore: true, logger: context.logger);
            ClassicAssert.AreEqual(2, primaryVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaOneVersion);

            // Reset and re-attach replica as primary
            ResetAndReAttach(replicaOneIndex, primaryIndex, soft: true);

            // Attach second replica
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaTwoIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Populate primary with more data
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Validate second replica data
            Validate(primaryIndex, replicaTwoIndex, disableObjects);

            // Validate db version
            primaryVersion = context.clusterTestUtils.GetStoreCurrentVersion(primaryIndex, isMainStore: true, logger: context.logger);
            replicaOneVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaOneIndex, isMainStore: true, logger: context.logger);
            var replicaTwoVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaTwoIndex, isMainStore: true, logger: context.logger);
            ClassicAssert.AreEqual(3, primaryVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaTwoVersion);
            ClassicAssert.AreEqual(2, replicaOneVersion);

            // Re-attach first replica
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaOneIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Validate second replica data
            Validate(primaryIndex, replicaOneIndex, disableObjects);

            primaryVersion = context.clusterTestUtils.GetStoreCurrentVersion(primaryIndex, isMainStore: true, logger: context.logger);
            replicaOneVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaOneIndex, isMainStore: true, logger: context.logger);
            replicaTwoVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaTwoIndex, isMainStore: true, logger: context.logger);
            ClassicAssert.AreEqual(4, primaryVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaOneVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaTwoVersion);
        }
    }
}