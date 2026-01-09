// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
#if DEBUG
using Garnet.common;
#endif
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

        int timeout = (int)TimeSpan.FromSeconds(15).TotalSeconds;
        int testTimeout = (int)TimeSpan.FromSeconds(120).TotalSeconds;

        public Dictionary<string, LogLevel> monitorTests = new(){
            { "ClusterDisklessSyncFailover", LogLevel.Trace }
        };

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests, testTimeoutSeconds: testTimeout);
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
            // New insert
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: primaryIndex);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: 0, addCount);

            if (!disableObjects)
                context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex: primaryIndex);
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

        void Failover(int replicaIndex, string option = null)
        {
            _ = context.clusterTestUtils.ClusterFailover(replicaIndex, option, logger: context.logger);
            var role = context.clusterTestUtils.RoleCommand(replicaIndex, logger: context.logger);
            while (!role.Value.Equals("master"))
            {
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
                role = context.clusterTestUtils.RoleCommand(replicaIndex, logger: context.logger);
            }

        }

        /// <summary>
        /// Attach empty replica after primary has been populated with some data
        /// </summary>
        /// <param name="disableObjects"></param>
        /// <param name="performRMW"></param>
        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterEmptyReplicaDisklessSync([Values] bool disableObjects, [Values] bool performRMW, [Values] bool prePopulate)
        {
            var nodes_count = 2;
            var primaryIndex = 0;
            var replicaIndex = 1;
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true, timeout: timeout);
            context.CreateConnection(useTLS: useTLS);

            // Setup primary and introduce it to future replica
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            // Ensure node is known
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            // Populate Primary
            if (prePopulate) PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Attach sync session
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Populate Primary
            if (!prePopulate) PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Wait for replica to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);

            // Validate replica data
            Validate(primaryIndex, replicaIndex, disableObjects);
        }

        /// <summary>
        /// Re-attach replica on disconnect after it has received some amount of data.
        /// The replica should replay only the portion it missed without doing a full sync,
        /// unless force full sync by setting the full sync AOF threshold to a very low value (1k).
        /// </summary>
        /// <param name="disableObjects"></param>
        /// <param name="performRMW"></param>
        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterAofReplayDisklessSync([Values] bool disableObjects, [Values] bool performRMW, [Values] bool forceFullSync)
        {
            var nodes_count = 2;
            var primaryIndex = 0;
            var replicaIndex = 1;
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true, timeout: timeout, replicaDisklessSyncFullSyncAofThreshold: forceFullSync ? "1k" : string.Empty);
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

            // Wait for replica to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);
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
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true, timeout: timeout);
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
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaTwoIndex, replicaOneIndex, logger: context.logger);

            // Populate Primary
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Attach first replica
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaOneIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Wait for replica to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaOneIndex, logger: context.logger);

            // Validate first replica data
            Validate(primaryIndex, replicaOneIndex, disableObjects);

            // Validate db version
            var primaryVersion = context.clusterTestUtils.GetStoreCurrentVersion(primaryIndex, logger: context.logger);
            var replicaOneVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaOneIndex, logger: context.logger);

            // Versions increase per scan and we have only a single store which takes a single scan regardless of 'disableObjects' setting
            var expectedVersion1 = 2;
            ClassicAssert.AreEqual(expectedVersion1, primaryVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaOneVersion);

            // Reset and re-attach replica as primary
            ResetAndReAttach(replicaOneIndex, primaryIndex, soft: true);

            // Attach second replica
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaTwoIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Populate primary with more data
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Wait for replica to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaTwoIndex, logger: context.logger);

            // Validate second replica data
            Validate(primaryIndex, replicaTwoIndex, disableObjects);

            // Validate db version
            primaryVersion = context.clusterTestUtils.GetStoreCurrentVersion(primaryIndex, logger: context.logger);
            replicaOneVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaOneIndex, logger: context.logger);
            var replicaTwoVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaTwoIndex, logger: context.logger);

            // With unified store we have only a single scan so a single increment regardless of 'disableObjects' setting
            var expectedVersion2 = 3;
            ClassicAssert.AreEqual(expectedVersion2, primaryVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaTwoVersion);
            ClassicAssert.AreEqual(expectedVersion1, replicaOneVersion);

            // Re-attach first replica
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaOneIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Validate second replica data
            Validate(primaryIndex, replicaOneIndex, disableObjects);

            primaryVersion = context.clusterTestUtils.GetStoreCurrentVersion(primaryIndex, logger: context.logger);
            replicaOneVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaOneIndex, logger: context.logger);
            replicaTwoVersion = context.clusterTestUtils.GetStoreCurrentVersion(replicaTwoIndex, logger: context.logger);

            // With unified store we have only a single scan so a single increment regardless of 'disableObjects' setting
            var expectedVersion3 = 4;
            ClassicAssert.AreEqual(expectedVersion3, primaryVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaOneVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaTwoVersion);
        }

        [Test, Order(4)]
        [Category("REPLICATION")]
        public void ClusterDisklessSyncParallelAttach([Values] bool disableObjects, [Values] bool performRMW)
        {
            var nodes_count = 4;
            var primaryIndex = 0;
            var replicaOneIndex = 1;
            var replicaTwoIndex = 2;
            var replicaThreeIndex = 3;
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true, timeout: timeout);
            context.CreateConnection(useTLS: useTLS);

            // Setup primary and introduce it to future replica
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaOneIndex, replicaOneIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaTwoIndex, replicaTwoIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaThreeIndex, replicaThreeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaOneIndex, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaTwoIndex, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaThreeIndex, logger: context.logger);

            // Ensure node everybody knowns everybody
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaOneIndex, primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaTwoIndex, primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaThreeIndex, primaryIndex, logger: context.logger);

            // Populate Primary
            for (var i = 0; i < 5; i++)
                PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Attach all replicas
            for (var replica = 1; replica < nodes_count; replica++)
                _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replica, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Validate all replicas
            for (var replica = 1; replica < nodes_count; replica++)
                Validate(primaryIndex, replica, disableObjects);
        }

        [Test, Order(5)]
        [Category("REPLICATION")]
        public void ClusterDisklessSyncFailover([Values] bool disableObjects, [Values] bool performRMW)
        {
            var nodes_count = 3;
            var primary = 0;
            var replicaOne = 1;
            var replicaTwo = 2;
            var populateIter = 1;

            int[] nOffsets = [primary, replicaOne, replicaTwo];

            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, enableDisklessSync: true, timeout: timeout);
            context.CreateConnection(useTLS: useTLS);

            // Setup primary and introduce it to future replica
            _ = context.clusterTestUtils.AddDelSlotsRange(nOffsets[primary], [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(nOffsets[primary], nOffsets[primary] + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(nOffsets[replicaOne], nOffsets[replicaOne] + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(nOffsets[replicaTwo], nOffsets[replicaTwo] + 1, logger: context.logger);
            context.clusterTestUtils.Meet(nOffsets[primary], nOffsets[replicaOne], logger: context.logger);
            context.clusterTestUtils.Meet(nOffsets[primary], nOffsets[replicaTwo], logger: context.logger);

            context.clusterTestUtils.WaitUntilNodeIsKnown(nOffsets[primary], nOffsets[replicaOne], logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(nOffsets[replicaOne], nOffsets[replicaTwo], logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(nOffsets[replicaTwo], nOffsets[primary], logger: context.logger);

            // Populate Primary
            for (var i = 0; i < populateIter; i++)
                PopulatePrimary(nOffsets[primary], disableObjects, performRMW);

            // Attach replicas
            for (var replica = 1; replica < nodes_count; replica++)
                _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replica, primaryNodeIndex: nOffsets[primary], logger: context.logger);

            // Wait for replica to catch up
            for (var replica = 1; replica < nodes_count; replica++)
                context.clusterTestUtils.WaitForReplicaAofSync(nOffsets[primary], nOffsets[replica], logger: context.logger);

            // Validate all replicas
            for (var replica = 1; replica < nodes_count; replica++)
                Validate(nOffsets[primary], nOffsets[replica], disableObjects);

            // Perform failover and promote replica one
            Failover(nOffsets[replicaOne]);
            (nOffsets[replicaOne], nOffsets[primary]) = (nOffsets[primary], nOffsets[replicaOne]);

            // Wait for replica to catch up
            for (var replica = 1; replica < nodes_count; replica++)
                context.clusterTestUtils.WaitForReplicaAofSync(nOffsets[primary], nOffsets[replica], logger: context.logger);

            // Populate Primary
            for (var i = 0; i < populateIter; i++)
                PopulatePrimary(nOffsets[primary], disableObjects, performRMW);

            // Wait for replica to catch up
            for (var replica = 1; replica < nodes_count; replica++)
                context.clusterTestUtils.WaitForReplicaAofSync(nOffsets[primary], nOffsets[replica], logger: context.logger);

            // Validate all replicas
            for (var replica = 1; replica < nodes_count; replica++)
                Validate(nOffsets[primary], nOffsets[replica], disableObjects);
        }

#if DEBUG
        [Test, Order(6)]
        [Category("REPLICATION")]
        public void ClusterDisklessSyncResetSyncManagerCts()
        {
            var nodes_count = 2;
            var primaryIndex = 0;
            var replicaOneIndex = 1;
            context.CreateInstances(nodes_count, enableAOF: true, useTLS: useTLS, enableDisklessSync: true, timeout: timeout);
            context.CreateConnection(useTLS: useTLS);

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaOneIndex, replicaOneIndex + 1, logger: context.logger);

            context.clusterTestUtils.Meet(primaryIndex, replicaOneIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaOneIndex, primaryIndex, logger: context.logger);

            try
            {
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Diskless_Sync_Reset_Cts);
                var _resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaOneIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
                ClassicAssert.AreEqual("Exception injection triggered Replication_Diskless_Sync_Reset_Cts", _resp);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Diskless_Sync_Reset_Cts);
            }

            var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaOneIndex, primaryNodeIndex: primaryIndex, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);
        }
#endif
    }
}