// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterCheckpointUpgradeRecover
    {
        ClusterTestContext context;

        readonly Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        //[Test, Order(1)]
        public void ClusterCheckpointAcrossModesTest([Values] bool clusterToStandalone)
        {
            // startup in cluster or standalone mode
            context.CreateInstances(1, enableCluster: clusterToStandalone);
            context.CreateConnection();

            // If cluster mode assign slots
            if (clusterToStandalone)
                ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));

            var keyLength = 32;
            var kvpairCount = 128;
            Dictionary<string, int> kvPairs = [];
            context.PopulatePrimary(ref kvPairs, keyLength, kvpairCount, 0);

            var primaryLastSaveTime = context.clusterTestUtils.LastSave(0, logger: context.logger);
            context.clusterTestUtils.Checkpoint(0, context.logger);
            context.clusterTestUtils.WaitCheckpoint(0, primaryLastSaveTime, logger: context.logger);

            context.nodes[0].Dispose(false);
            // Restart in standalone or cluster mode
            context.nodes[0] = context.CreateInstance(context.clusterTestUtils.GetEndPoint(0), enableCluster: !clusterToStandalone, tryRecover: true);
            context.nodes[0].Start();
            context.CreateConnection();

            // Assign slot if started initially in standalone
            if (!clusterToStandalone)
                ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));

            context.clusterTestUtils.PingAll(logger: context.logger);

            var db = context.clusterTestUtils.GetDatabase();
            foreach (var kv in kvPairs)
            {
                var key = kv.Key;
                var value = kv.Value;
                var dbValue = (int)db.StringGet(key);
                ClassicAssert.AreEqual(value, dbValue);
            }
        }
    }
}