// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

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


        [Test, Order(1)]
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

            // Soft reset replica
            _ = context.clusterTestUtils.ClusterReset(replicaIndex, soft: true, expiry: 1, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(replicaIndex, logger: context.logger);
            // Re-introduce node after reset
            while (!context.clusterTestUtils.IsKnown(replicaIndex, primaryIndex, logger: context.logger))
            {
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
                context.clusterTestUtils.Meet(replicaIndex, primaryIndex, logger: context.logger);
            }

            // Populate Primary (ahead of replica)
            PopulatePrimary(primaryIndex, disableObjects, performRMW);

            // Re-attach sync session
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Validate replica data
            Validate(primaryIndex, replicaIndex, disableObjects);
        }
    }
}