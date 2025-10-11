// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{

    public class ClusterReplicationShardedLog
    {
        ClusterTestContext context;

        public TextWriter LogTextWriter { get; set; }

        protected bool useTLS = false;
        protected bool asyncReplay = false;
        readonly int keyCount = 256;

        public Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            if (LogTextWriter != null) context.logTextWriter = LogTextWriter;
            context.Setup(monitorTests);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterSimpleReplicationShardedLogTest([Values] bool disableObjects)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, sublog: 2);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 16;
            var kvpairCount = keyCount;
            context.kvPairs = [];

            //Populate Primary
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
        }
    }
}