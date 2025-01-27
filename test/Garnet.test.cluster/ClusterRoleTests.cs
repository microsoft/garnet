// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterRoleTests
    {
        ClusterTestContext context;
        readonly int defaultShards = 3;

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

        [Test]
        public void ClusterTestRoleCommand()
        {
            var node_count = 3;
            var replica_count = node_count - 1;
            context.CreateInstances(node_count, enableAOF: true);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(1, replica_count, logger: context.logger);

            var result = context.clusterTestUtils.GetServer(0).Execute("ROLE");
            ClassicAssert.True(result.Length == 3);
            ClassicAssert.AreEqual("master", result[0].ToString());
            ClassicAssert.True(int.TryParse(result[1].ToString(), out _));
            ClassicAssert.True(result[2].Length == 2);
            ClassicAssert.AreEqual("127.0.0.1", result[2][0][0].ToString());
            ClassicAssert.AreEqual("127.0.0.1", result[2][1][0].ToString());

            result = context.clusterTestUtils.GetServer(1).Execute("ROLE");
            ClassicAssert.True(result.Length == 5);
            ClassicAssert.AreEqual("slave", result[0].ToString());
            ClassicAssert.AreEqual("127.0.0.1", result[1].ToString());
            ClassicAssert.True(int.TryParse(result[2].ToString(), out _));
            ClassicAssert.AreEqual("connected", result[3].ToString());
            ClassicAssert.True(int.TryParse(result[4].ToString(), out _));
        }
    }
}