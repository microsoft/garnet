// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Covers <c>--upgrade</c> on a cluster node. The standalone tests cover the conversion itself; these cover what is
    /// specific to cluster mode: the upgrade process must not start gossip or replication, must not disturb the node's
    /// cluster identity or slot assignment, and the node must come back as the same primary serving the same data.
    /// </summary>
    [TestFixture, NonParallelizable]
    public class ClusterObjectLogUpgradeTests : TestBase
    {
        const int NumKeys = 200;
        const int ExtraAofKeys = 40;

        ClusterTestContext context;

        readonly Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests, testTimeoutSeconds: 180);
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        // Small enough that the serialized object stays headerless in the current format, so its bytes are already v7-dense.
        static string SmallValueFor(int key) => $"v{key:D6}";

        static string KeyFor(int key) => $"hash:{key}";

        [Test, Category("CLUSTER")]
        public void ClusterUpgradeConvertsDownlevelStore([Values(false, true)] bool enableAof, [Values(false, true)] bool useFoldOver)
        {
            context.CreateInstances(1, enableCluster: true, lowMemory: true, enableAOF: enableAof, CommitFrequencyMs: enableAof ? -1 : 0);
            context.CreateConnection(enabledCluster: true);
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));

            var opts = context.nodeOptions[0];

            // Read at checkpoint time from the live options instance, so setting it on the running node still selects the type.
            opts.UseFoldOverCheckpoints = useFoldOver;

            // The fixture rewrites one store rooted at a single directory; cluster nodes put the log and the checkpoints in the same
            // per-port folder, so assert that rather than silently converting only part of the store.
            var storeRoot = new DirectoryInfo(opts.LogDir).FullName;
            ClassicAssert.AreEqual(storeRoot, new DirectoryInfo(opts.CheckpointDir).FullName);

            var db = context.clusterTestUtils.GetDatabase();
            for (var key = 0; key < NumKeys; key++)
                db.HashSet(KeyFor(key), [new HashEntry("f", SmallValueFor(key))]);

            var lastSave = context.clusterTestUtils.LastSave(0, logger: context.logger);
            context.clusterTestUtils.WaitUntilNextSecond(0, lastSave);
            context.clusterTestUtils.Checkpoint(0, context.logger);
            context.clusterTestUtils.WaitCheckpoint(0, lastSave, logger: context.logger);

            // These live only in the AOF, so recovering them proves the AOF replays against the converted store.
            if (enableAof)
            {
                for (var key = NumKeys; key < NumKeys + ExtraAofKeys; key++)
                    db.HashSet(KeyFor(key), [new HashEntry("f", SmallValueFor(key))]);
            }

            var nodeIdBeforeUpgrade = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            var ownedSlotsBeforeUpgrade = context.clusterTestUtils.GetOwnedSlotsFromNode(0, context.logger);

            context.ShutdownNode(0, ensureAofFlush: enableAof);

            var convertedRecords = V7CheckpointFixture.TransformCheckpointToV7(storeRoot);
            ClassicAssert.Greater(convertedRecords, 0, "expected the fixture to have out-of-line object records on the main log");

            RunUpgrade(opts);

            var stillDownlevel = V7CheckpointFixture.CountDownlevelRecordsOnMainLog(storeRoot);
            ClassicAssert.AreEqual(0, stillDownlevel, "records were left in the downlevel encoding after the upgrade");

            var storeDir = ObjectLogUpgradeSwap.GetStoreDirectory(storeRoot);
            ClassicAssert.IsNotEmpty(Directory.GetFiles(storeDir, GarnetServerOptions.ObjectLogFileName + "_pre_upgrade_*"), "the downlevel object log should have been retired");
            ClassicAssert.IsEmpty(Directory.GetFiles(storeDir, GarnetServerOptions.UpgradeObjectLogFileName + ".*"), "the converted object log should have been promoted, not left behind");
            ClassicAssert.IsFalse(ObjectLogUpgradeSwap.HasPendingSwap(storeRoot), "the rename marker should be gone");

            opts.Recover = true;
            context.RestartNode(0);
            context.CreateConnection(enabledCluster: true);
            context.clusterTestUtils.PingAll(context.logger);

            // An upgrade run must not touch cluster state: same node id, same slots, still a primary serving them.
            ClassicAssert.AreEqual(nodeIdBeforeUpgrade, context.clusterTestUtils.GetNodeIdFromNode(0, context.logger), "the upgrade run changed the node's cluster identity");
            CollectionAssert.AreEquivalent(ownedSlotsBeforeUpgrade, context.clusterTestUtils.GetOwnedSlotsFromNode(0, context.logger), "the upgrade run changed the node's slot assignment");

            db = context.clusterTestUtils.GetDatabase();
            var total = NumKeys + (enableAof ? ExtraAofKeys : 0);
            for (var key = 0; key < total; key++)
                ClassicAssert.AreEqual(SmallValueFor(key), (string)db.HashGet(KeyFor(key), "f"), $"{KeyFor(key)} did not survive the v7 upgrade");
        }

        /// <summary>
        /// Run the offline upgrade over the node's own options. <see cref="GarnetServerOptions.Upgrade"/> is read while the store is
        /// being constructed, so it has to be set before the server is created and cleared before the node is restarted to serve.
        /// </summary>

        void RunUpgrade(GarnetServerOptions opts)
        {
            opts.Recover = true;
            opts.CleanClusterConfig = false;
            opts.Upgrade = true;
            try
            {
                using var upgradeServer = new GarnetServer(opts, context.loggerFactory);
                ClassicAssert.IsTrue(upgradeServer.IsUpgradeRun);
                upgradeServer.RunUpgrade();
            }
            finally
            {
                opts.Upgrade = false;
            }
        }
    }
}