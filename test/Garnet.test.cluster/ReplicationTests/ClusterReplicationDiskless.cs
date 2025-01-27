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
    public class ClusterReplicationDiskless
    {
        ClusterTestContext context;
        readonly int keyCount = 256;

        protected bool useTLS = false;
        protected bool asyncReplay = false;

        public Dictionary<string, LogLevel> monitorTests = new()
        {
            {"ClusterReplicationSimpleFailover", LogLevel.Warning},
        };

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

        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterSimpleAttachSync([Values] bool disableObjects)
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

            // Populate Primary
            var keyLength = 16;
            var kvpairCount = keyCount;
            context.kvPairs = [];
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);

            // Ensure node is known
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            // Attach sync session
            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, logger: context.logger);

            // Wait for replica to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, logger: context.logger);

            // Validate replica data
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex: replicaIndex, primaryIndex: primaryIndex);
        }
    }
}