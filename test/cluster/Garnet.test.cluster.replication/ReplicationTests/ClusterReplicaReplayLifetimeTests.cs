// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicaReplayLifetimeTests : TestBase
    {
        ClusterTestContext context;
        StringWriter logOutput;
        readonly Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            logOutput = new StringWriter();
            context = new ClusterTestContext { logTextWriter = TextWriter.Synchronized(logOutput) };
            context.Setup(monitorTests, testTimeoutSeconds: 120);
        }

        [TearDown]
        public void TearDown()
        {
            context?.TearDown();
            logOutput?.Dispose();
        }

        [Test, CancelAfter(120_000)]
        [Category("REPLICATION")]
        public void ParallelReplayAppliesCommitMetadataWithThreadLocalEpochProtection(CancellationToken cancellationToken)
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, asyncReplay: true, replayTaskCount: 2, AofMemorySize: "128k", AofPageSize: "32k", memorySize: "1m", pageSize: "16k", lowMemory: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            const int keyCount = 512;
            for (var i = 0; i < keyCount; i++)
            {
                var key = Encoding.ASCII.GetBytes($"parallel-replay-{i}");
                var value = Encoding.ASCII.GetBytes($"value-{i}".PadRight(200, 'v'));
                var response = context.clusterTestUtils.SetKey(0, key, value, out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, response);
            }

            while (!context.clusterTestUtils.GetReplicationOffset(0).Equals(context.clusterTestUtils.GetReplicationOffset(1)))
            {
                cancellationToken.ThrowIfCancellationRequested();
                string output;
                lock (context.logTextWriter)
                    output = logOutput.ToString();

                ClassicAssert.IsFalse(output.Contains("BumpCurrentEpoch must be called on a protected thread"), $"Parallel replay used another thread's epoch protection:\n{output}");
                Thread.Sleep(10);
            }

            for (var i = 0; i < keyCount; i++)
            {
                var key = Encoding.ASCII.GetBytes($"parallel-replay-{i}");
                var actual = context.clusterTestUtils.GetKey(1, key, out _, out _, out var response, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, response);
                ClassicAssert.AreEqual($"value-{i}".PadRight(200, 'v'), actual);
            }
        }
    }
}