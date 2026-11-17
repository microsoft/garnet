// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using System.Threading;
using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationAsyncReplay : ClusterReplicationBaseTests
    {
        [SetUp]
        public override void Setup()
        {
            asyncReplay = true;
            base.Setup();
        }

        [TearDown]
        public override void TearDown()
        {
            base.TearDown();
        }

        [Test, CancelAfter(120_000)]
        [Category("REPLICATION")]
        public void ParallelReplayAppliesCommitMetadataWithThreadLocalEpochProtection(
            [Values(2, 3)] int replayTaskCount, CancellationToken cancellationToken)
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, asyncReplay: asyncReplay,
                replayTaskCount: replayTaskCount, AofMemorySize: "128k", AofPageSize: "32k",
                memorySize: "1m", pageSize: "16k", lowMemory: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            const int keyCount = 512;
            for (var i = 0; i < keyCount; i++)
            {
                var key = Encoding.ASCII.GetBytes($"parallel-replay-{i}");
                var value = Encoding.ASCII.GetBytes($"value-{i}".PadRight(200, 'v'));
                var response = context.clusterTestUtils.SetKey(0, key, value, out _, out _, logger: context.logger);
                Assert.That(response, Is.EqualTo(ResponseState.OK));
            }

            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger, cancellationToken);

            for (var i = 0; i < keyCount; i++)
            {
                var key = Encoding.ASCII.GetBytes($"parallel-replay-{i}");
                var actual = context.clusterTestUtils.GetKey(1, key, out _, out _, out var response, logger: context.logger);
                Assert.That(response, Is.EqualTo(ResponseState.OK));
                Assert.That(actual, Is.EqualTo($"value-{i}".PadRight(200, 'v')));
            }
        }
    }
}