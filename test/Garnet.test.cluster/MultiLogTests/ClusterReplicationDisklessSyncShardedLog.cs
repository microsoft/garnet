// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using NUnit.Framework;

namespace Garnet.test.cluster.MultiLogTests
{
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationDisklessSyncShardedLog : ClusterReplicationDisklessSyncTests
    {
        const int TestSublogCount = 2;

        public Dictionary<string, bool> enabledTests = new()
        {
            {"ClusterEmptyReplicaDisklessSync", true},
            {"ClusterAofReplayDisklessSync", true},
            {"ClusterDBVersionAlignmentDisklessSync", true},
            {"ClusterDisklessSyncParallelAttach", true},
            {"ClusterDisklessSyncFailover", true},
            {"ClusterDisklessSyncResetSyncManagerCts", true},
        };

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var methods = typeof(ClusterReplicationShardedLog).GetMethods().Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);
            foreach (var method in methods)
                enabledTests.TryAdd(method.Name, true);
        }

        [SetUp]
        public override void Setup()
        {
            var testName = TestContext.CurrentContext.Test.MethodName;
            if (!enabledTests.TryGetValue(testName, out var isEnabled) || !isEnabled)
            {
                Assert.Ignore($"Skipping {testName} for {nameof(ClusterReplicationShardedLog)}");
            }
            asyncReplay = false;
            sublogCount = TestSublogCount;
            base.Setup();
        }

        [TearDown]
        public override void TearDown()
        {
            var testName = TestContext.CurrentContext.Test.MethodName;
            if (!enabledTests.TryGetValue(testName, out var isEnabled) || !isEnabled)
            {
                Assert.Ignore($"Skipping {testName} for {nameof(ClusterReplicationShardedLog)}");
            }
            base.TearDown();
        }
    }
}