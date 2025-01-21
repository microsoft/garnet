// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    internal class ClusterConfigTests
    {
        [Test, Order(1)]
        [Category("CLUSTER-CONFIG"), CancelAfter(1000)]
        public void ClusterConfigInitializesUnassignedWorkerTest()
        {
            ClusterConfig config = new ClusterConfig().InitializeLocalWorker(
                Generator.CreateHexId(),
                "127.0.0.1",
                7001,
                configEpoch: 0,
                NodeRole.PRIMARY,
                null,
                "");

            (string address, int port) = config.GetWorkerAddress(0);
            Assert.That(address == "unassigned");
            Assert.That(port == 0);
            Assert.That(NodeRole.UNASSIGNED == config.GetNodeRoleFromNodeId("asdasdqwe"));

            var configBytes = config.ToByteArray();
            var restoredConfig = ClusterConfig.FromByteArray(configBytes);

            (address, port) = restoredConfig.GetWorkerAddress(0);
            Assert.That(address == "unassigned");
            Assert.That(port == 0);
            Assert.That(NodeRole.UNASSIGNED == restoredConfig.GetNodeRoleFromNodeId("asdasdqwe"));
        }
    }
}