// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster.ReplicationTests
{
    /// <summary>
    /// These tests simulate scenarios where a replica gets stuck or is in replication attach and verify that
    /// CLUSTER RESET HARD can properly cancel ongoing operations and allow the replica to be reused.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    [NonParallelizable]
    public class ClusterResetDuringReplicationTests : AllureTestBase
    {
        ClusterTestContext context;

        readonly int createInstanceTimeout = (int)System.TimeSpan.FromSeconds(30).TotalSeconds;
        const int testTimeout = 120_000;

        readonly Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests, testTimeoutSeconds: testTimeout);
        }

        [TearDown]
        public void TearDown()
        {
            context?.TearDown();
        }

        /// <summary>
        /// Test CLUSTER RESET HARD functionality during diskless replication attach.
        /// This test simulates a scenario where a replica gets stuck while attaching to a primary
        /// and verifies that CLUSTER RESET HARD can properly cancel the operation and reset the node.
        /// </summary>
        [Test, Order(1), CancelAfter(testTimeout)]
        [Category("REPLICATION")]
        public async Task ClusterResetHardDuringDisklessReplicationAttach(CancellationToken cancellationToken)
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = 2;

            // Create instances with diskless sync enabled
            context.CreateInstances(nodes_count, enableAOF: true, enableDisklessSync: true, timeout: createInstanceTimeout);
            context.CreateConnection();

            // Setup primary
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            // Ensure nodes know each other
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            try
            {
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_InProgress_During_Diskless_Replica_Attach_Sync);

                // Initiate replication.
                var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, async: true, logger: context.logger);

                // Wait for the primary to reach the desired code path (ReplicationManager.TryBeginDisklessSync).
                await ExceptionInjectionHelper.WaitOnClearAsync(ExceptionInjectionType.Replication_InProgress_During_Diskless_Replica_Attach_Sync);

                // Verify that the replica is in a replicating state
                var replicationInfo = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.RECOVER_STATUS], logger: context.logger);
                ClassicAssert.AreEqual("ClusterReplicate", replicationInfo[0].Item2);

                // Issuing CLUSTER RESET HARD while replication is ongoing/stuck.
                var resetResp = context.clusterTestUtils.ClusterReset(replicaIndex, soft: false, expiry: 60, logger: context.logger);
                ClassicAssert.AreEqual("OK", resetResp);

                // Release waiting task at ReplicationManager.TryBeginDisklessSync
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_InProgress_During_Diskless_Replica_Attach_Sync);

                // Verify that the node is no longer in recovery state
                replicationInfo = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.RECOVER_STATUS], logger: context.logger);
                ClassicAssert.AreEqual("NoRecovery", replicationInfo[0].Item2);

                // Verify the node role is back to PRIMARY (default after reset)
                var role = context.clusterTestUtils.RoleCommand(replicaIndex, logger: context.logger);
                ClassicAssert.AreEqual("master", role.Value);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_InProgress_During_Diskless_Replica_Attach_Sync);
            }
        }

        /// <summary>
        /// Test CLUSTER RESET HARD functionality during diskbased replication attach.
        /// This test simulates a scenario where a replica gets stuck while attaching to a primary
        /// and verifies that CLUSTER RESET HARD can properly cancel the operation and reset the node.
        /// </summary>
        [Test, Order(2), CancelAfter(testTimeout)]
        [Category("REPLICATION")]
        public async Task ClusterResetHardDuringDiskBasedReplicationAttach(CancellationToken cancellationToken)
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = 2;

            // (diskless sync is false)
            context.CreateInstances(nodes_count, enableAOF: true, enableDisklessSync: false, timeout: createInstanceTimeout);
            context.CreateConnection();

            // Setup primary
            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            try
            {
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync);

                // Initiate replication.
                var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, async: true, logger: context.logger);

                // Wait for the primary to reach the desired code path (ReplicationManager.TryBeginDiskbasedSync).
                await ExceptionInjectionHelper.WaitOnClearAsync(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync);

                // Verify that the replica is in a replicating state
                var replicationInfo = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.RECOVER_STATUS], logger: context.logger);
                ClassicAssert.AreEqual("ClusterReplicate", replicationInfo[0].Item2);

                // Issuing CLUSTER RESET HARD while replication is ongoing/stuck.
                var resetResp = context.clusterTestUtils.ClusterReset(replicaIndex, soft: false, expiry: 60, logger: context.logger);
                ClassicAssert.AreEqual("OK", resetResp);

                // Release waiting task at ReplicaSyncSession.SendCheckpoint
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync);

                // Verify that the node is no longer in recovery state
                replicationInfo = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.RECOVER_STATUS], logger: context.logger);
                ClassicAssert.AreEqual("NoRecovery", replicationInfo[0].Item2);

                // Verify the node role is back to PRIMARY (default after reset)
                var role = context.clusterTestUtils.RoleCommand(replicaIndex, logger: context.logger);
                ClassicAssert.AreEqual("master", role.Value);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_InProgress_During_DiskBased_Replica_Attach_Sync);
            }
        }
    }
}
#endif