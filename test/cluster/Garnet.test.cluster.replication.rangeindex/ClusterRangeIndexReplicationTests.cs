// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    public class ClusterRangeIndexReplicationTests : TestBase
    {
        ClusterTestContext context;

        readonly int timeout = 60;
        readonly int testTimeout = (int)TimeSpan.FromSeconds(120).TotalSeconds;

        public Dictionary<string, LogLevel> monitorTests = new()
        {
            { "ClusterRangeIndexCheckpointSync", LogLevel.Trace },
            { "ClusterRangeIndexReplicationFailover", LogLevel.Trace },
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

        void PopulateRangeIndex(int nodeIndex, string indexName, int fieldCount)
        {
            var endpoint = (IPEndPoint)context.endpoints[nodeIndex];
            var result = context.clusterTestUtils.Execute(endpoint, "RI.CREATE",
                [indexName, "MEMORY", "CACHESIZE", "65536", "MINRECORD", "8"],
                logger: context.logger);
            ClassicAssert.AreEqual("OK", (string)result);

            for (var i = 0; i < fieldCount; i++)
            {
                result = context.clusterTestUtils.Execute(endpoint, "RI.SET",
                    [indexName, $"field{i:D4}", $"value{i:D4}"],
                    logger: context.logger);
                ClassicAssert.AreEqual("OK", (string)result);
            }
        }

        void ValidateRangeIndex(int nodeIndex, string indexName, int fieldCount)
        {
            var endpoint = (IPEndPoint)context.endpoints[nodeIndex];
            for (var i = 0; i < fieldCount; i++)
            {
                var result = context.clusterTestUtils.Execute(endpoint, "RI.GET",
                    [indexName, $"field{i:D4}"],
                    logger: context.logger);
                ClassicAssert.AreEqual($"value{i:D4}", (string)result,
                    $"Mismatch at field{i:D4} on node {nodeIndex}");
            }
        }

        /// <summary>
        /// Create RI keys on primary, take checkpoint, attach replica via checkpoint-based sync.
        /// Verify RI.GET returns correct values on replica.
        /// </summary>
        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterRangeIndexCheckpointSync()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodesCount = 2;

            context.CreateInstances(nodesCount, disableObjects: true, enableAOF: true,
                enableRangeIndexPreview: true);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Populate range index on primary
            PopulateRangeIndex(primaryIndex, "idx1", 10);

            // Wait for replica to sync via AOF
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Validate on replica
            ValidateRangeIndex(replicaIndex, "idx1", 10);
        }

        /// <summary>
        /// Multiple RI keys with MEMORY backend, checkpoint, replicate.
        /// Verify all keys and their fields are accessible on replica.
        /// </summary>
        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterRangeIndexCheckpointSyncMultipleKeys()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodesCount = 2;

            context.CreateInstances(nodesCount, disableObjects: true, enableAOF: true,
                enableRangeIndexPreview: true);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Populate multiple range indexes on primary
            PopulateRangeIndex(primaryIndex, "idx1", 10);
            PopulateRangeIndex(primaryIndex, "idx2", 20);
            PopulateRangeIndex(primaryIndex, "idx3", 5);

            // Take checkpoint
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

            // Wait for replica to sync
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Validate all indexes on replica
            ValidateRangeIndex(replicaIndex, "idx1", 10);
            ValidateRangeIndex(replicaIndex, "idx2", 20);
            ValidateRangeIndex(replicaIndex, "idx3", 5);
        }

        /// <summary>
        /// Low memory forces RI key eviction (flush.bftree files created).
        /// Take checkpoint, attach replica. Verify flush snapshot files are transferred
        /// and RI data is restored on replica.
        /// </summary>
        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterRangeIndexCheckpointSyncWithEviction()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodesCount = 2;

            context.CreateInstances(nodesCount, disableObjects: true, enableAOF: true,
                lowMemory: true, enableRangeIndexPreview: true);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Populate range indexes — low memory should trigger eviction
            PopulateRangeIndex(primaryIndex, "idx1", 10);
            PopulateRangeIndex(primaryIndex, "idx2", 10);

            // Take checkpoint
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

            // Wait for replica to sync
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Validate on replica — even evicted keys should be accessible
            ValidateRangeIndex(replicaIndex, "idx1", 10);
            ValidateRangeIndex(replicaIndex, "idx2", 10);
        }

        /// <summary>
        /// Populate RI → checkpoint → more RI.SET → attach replica.
        /// Verify both checkpointed data and AOF-replayed data arrive.
        /// </summary>
        [Test, Order(4)]
        [Category("REPLICATION")]
        public void ClusterRangeIndexAddReplicaAfterCheckpoint()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodesCount = 2;

            context.CreateInstances(nodesCount, tryRecover: true, disableObjects: true,
                enableAOF: true, enableRangeIndexPreview: true);
            context.CreateConnection();

            // Setup primary only — do not attach replica yet
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0,
                new List<(int, int)> { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.BumpEpoch(0, logger: context.logger);

            // Populate and checkpoint
            PopulateRangeIndex(primaryIndex, "idx1", 10);
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

            // Add more data after checkpoint (will be in AOF)
            var endpoint = (IPEndPoint)context.endpoints[primaryIndex];
            for (var i = 10; i < 20; i++)
            {
                context.clusterTestUtils.Execute(endpoint, "RI.SET",
                    ["idx1", $"field{i:D4}", $"value{i:D4}"],
                    logger: context.logger);
            }

            // Now attach replica
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            context.clusterTestUtils.Meet(1, 0, logger: context.logger);
            context.clusterTestUtils.WaitAll(context.logger);
            _ = context.clusterTestUtils.ClusterReplicate(1, primaryId, async: false, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(1, logger: context.logger);

            // Wait for sync
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Validate all 20 fields — 10 from checkpoint, 10 from AOF
            ValidateRangeIndex(replicaIndex, "idx1", 20);
        }

        /// <summary>
        /// Checkpoint sync → shutdown replica → more RI.SET on primary → restart replica.
        /// Verify replica catches up.
        /// </summary>
        [Test, Order(5)]
        [Category("REPLICATION")]
        public void ClusterRangeIndexReplicationRestartReplica()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodesCount = 2;

            context.CreateInstances(nodesCount, disableObjects: true, enableAOF: true,
                enableRangeIndexPreview: true);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Populate and sync
            PopulateRangeIndex(primaryIndex, "idx1", 10);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            ValidateRangeIndex(replicaIndex, "idx1", 10);

            // Take checkpoint so replica can recover
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            var replicaLastSaveTime = context.clusterTestUtils.LastSave(replicaIndex, logger: context.logger);
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(replicaIndex, replicaLastSaveTime, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Shutdown replica
            context.nodes[replicaIndex].Dispose(false);
            context.clusterTestUtils.WaitForAofSyncDriverDipose(primaryIndex);

            // Add more data while replica is down
            var endpoint = (IPEndPoint)context.endpoints[primaryIndex];
            for (var i = 10; i < 20; i++)
            {
                context.clusterTestUtils.Execute(endpoint, "RI.SET",
                    ["idx1", $"field{i:D4}", $"value{i:D4}"],
                    logger: context.logger);
            }

            // Restart replica
            context.nodes[replicaIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(replicaIndex),
                disableObjects: true,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                cleanClusterConfig: false,
                enableRangeIndexPreview: true);
            context.nodes[replicaIndex].Start();
            context.CreateConnection();

            // Wait for sync
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Validate all 20 fields
            ValidateRangeIndex(replicaIndex, "idx1", 20);
        }

        /// <summary>
        /// Populate RI on primary → replicate via checkpoint → failover replica to primary.
        /// Verify RI data accessible on new primary.
        /// </summary>
        [Test, Order(6)]
        [Category("REPLICATION")]
        public void ClusterRangeIndexReplicationFailover()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodesCount = 2;

            context.CreateInstances(nodesCount, disableObjects: true, enableAOF: true,
                enableRangeIndexPreview: true);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Populate and sync
            PopulateRangeIndex(primaryIndex, "idx1", 10);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            ValidateRangeIndex(replicaIndex, "idx1", 10);

            // Take checkpoint
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            var replicaLastSaveTime = context.clusterTestUtils.LastSave(replicaIndex, logger: context.logger);
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(replicaIndex, replicaLastSaveTime, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            // Failover
            _ = context.clusterTestUtils.ClusterFailover(replicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitForNoFailover(replicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitForFailoverCompleted(replicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaRecovery(primaryIndex, logger: context.logger);

            // Validate RI data on new primary (was replicaIndex)
            ValidateRangeIndex(replicaIndex, "idx1", 10);

            // Verify we can write to new primary
            var newPrimaryEndpoint = (IPEndPoint)context.endpoints[replicaIndex];
            var result = context.clusterTestUtils.Execute(newPrimaryEndpoint, "RI.SET",
                ["idx1", "field0010", "value0010"],
                logger: context.logger);
            ClassicAssert.AreEqual("OK", (string)result);
        }
    }
}
