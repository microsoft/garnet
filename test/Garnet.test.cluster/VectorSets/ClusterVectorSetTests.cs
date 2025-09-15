// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Net;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterVectorSetTests
    {
        private const int DefaultShards = 2;

        private static readonly Dictionary<string, LogLevel> MonitorTests =
            new()
            {
                [nameof(BasicVADDReplicates)] = LogLevel.Error,
            };


        private ClusterTestContext context;

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            context.logTextWriter = TestContext.Progress;
            context.Setup(MonitorTests);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        [Test]
        public void BasicVADDReplicates()
        {
            const int PrimaryIndex = 0;
            const int SecondaryIndex = 1;

            context.CreateInstances(DefaultShards, useTLS: true, enableAOF: true);
            context.CreateConnection(useTLS: true);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1, logger: context.logger);

            var primary = (IPEndPoint)context.endpoints[PrimaryIndex];
            var secondary = (IPEndPoint)context.endpoints[SecondaryIndex];

            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(secondary).Value);

            var addRes = (int)context.clusterTestUtils.Execute(primary, "VADD", ["foo", "XB8", new byte[] { 1, 2, 3, 4 }, new byte[] { 0, 0, 0, 0 }, "XPREQ8"]);
            ClassicAssert.AreEqual(1, addRes);

            var simRes = (byte[][])context.clusterTestUtils.Execute(primary, "VSIM", ["foo", "XB8", new byte[] { 2, 3, 4, 5 }]);
            ClassicAssert.IsTrue(simRes.Length > 0);

            context.clusterTestUtils.WaitForReplicaAofSync(PrimaryIndex, SecondaryIndex);

            var readonlyOnReplica = (string)context.clusterTestUtils.Execute(secondary, "READONLY", []);
            ClassicAssert.AreEqual("OK", readonlyOnReplica);

            var simOnReplica = context.clusterTestUtils.Execute(secondary, "VSIM", ["foo", "XB8", new byte[] { 2, 3, 4, 5 }]);
            ClassicAssert.IsTrue(simOnReplica.Length > 0);
        }
    }
}
