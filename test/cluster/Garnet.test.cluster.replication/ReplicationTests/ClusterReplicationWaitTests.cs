// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationWaitTests : TestBase
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            context?.TearDown();
        }

        void SetupPrimaryWithReplicas(int replicaCount)
        {
            context.CreateInstances(replicaCount + 1, enableAOF: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, replicaCount, logger: context.logger);
            for (var i = 1; i <= replicaCount; i++)
                context.clusterTestUtils.WaitForReplicaAofSync(0, i, context.logger);
        }

        RedisResultWithTime Execute(int nodeIndex, string cmd, params object[] args)
        {
            var sw = Stopwatch.StartNew();
            var result = context.clusterTestUtils.Execute(context.clusterTestUtils.GetEndPoint(nodeIndex), cmd, args, logger: context.logger);
            return new(result.ToString(), sw.ElapsedMilliseconds);
        }

        record struct RedisResultWithTime(string Result, long ElapsedMilliseconds);

        [Test]
        [Category("REPLICATION")]
        public void ClusterWaitAllReplicasTest()
        {
            SetupPrimaryWithReplicas(2);

            ClassicAssert.AreEqual("OK", Execute(0, "SET", "key", "value").Result);
            ClassicAssert.AreEqual("2", Execute(0, "WAIT", "2", "10000").Result);
            ClassicAssert.AreEqual("2", Execute(0, "WAIT", "0", "0").Result);
        }

        [Test]
        [Category("REPLICATION")]
        public async Task ClusterWaitTimeoutWithStoppedReplicaTest()
        {
            SetupPrimaryWithReplicas(2);

            context.ShutdownNode(2);
            await context.clusterTestUtils.WaitForConnectedReplicaCountAsync(0, 1, context.logger).ConfigureAwait(false);

            ClassicAssert.AreEqual("OK", Execute(0, "SET", "key", "value").Result);
            var wait = Execute(0, "WAIT", "2", "300");
            ClassicAssert.AreEqual("1", wait.Result);
            ClassicAssert.GreaterOrEqual(wait.ElapsedMilliseconds, 250);
        }

        [Test]
        [Category("REPLICATION")]
        public async Task ClusterWaitWithoutTimeoutTest()
        {
            SetupPrimaryWithReplicas(1);

            ClassicAssert.AreEqual("OK", Execute(0, "SET", "key", "value").Result);
            var wait = Task.Run(() => Execute(0, "WAIT", "1", "0"));
            ClassicAssert.IsTrue(await wait.WaitAsync(TimeSpan.FromSeconds(30)).ContinueWith(t => t.IsCompletedSuccessfully).ConfigureAwait(false));
            ClassicAssert.AreEqual("1", wait.Result.Result);
        }

        [Test]
        [Category("REPLICATION")]
        public void ClusterWaitOnReplicaTest()
        {
            SetupPrimaryWithReplicas(1);

            ClassicAssert.AreEqual("ERR WAIT cannot be used with replica instances.", Execute(1, "WAIT", "1", "0").Result);
        }

        [Test]
        [Category("REPLICATION")]
        public async Task ClusterWaitInTransactionTest()
        {
            SetupPrimaryWithReplicas(1);

            var client = context.clusterTestUtils.GetGarnetClientSession(0);
            ClassicAssert.AreEqual("OK", await client.ExecuteAsync("MULTI").ConfigureAwait(false));
            ClassicAssert.AreEqual("QUEUED", await client.ExecuteAsync("SET", "key", "value").ConfigureAwait(false));
            ClassicAssert.AreEqual("QUEUED", await client.ExecuteAsync("WAIT", "2", "0").ConfigureAwait(false));

            var exec = client.ExecuteForArrayAsync("EXEC");
            ClassicAssert.IsTrue(await exec.WaitAsync(TimeSpan.FromSeconds(30)).ContinueWith(t => t.IsCompletedSuccessfully).ConfigureAwait(false));
            ClassicAssert.AreEqual(2, exec.Result.Length);
            ClassicAssert.AreEqual("OK", exec.Result[0]);
            ClassicAssert.LessOrEqual(int.Parse(exec.Result[1]), 1);
        }
    }
}