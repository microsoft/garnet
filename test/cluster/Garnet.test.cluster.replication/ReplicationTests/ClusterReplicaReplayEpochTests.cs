// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Background replica replay of AOF chunks consumed directly from the in-memory log buffer under epoch protection.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicaReplayEpochTests : TestBase
    {
        const int TestTimeoutMs = 180_000;

        ClusterTestContext context;

        readonly Dictionary<string, LogLevel> monitorTests = [];

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests, testTimeoutSeconds: TestTimeoutMs / 1000);
        }

        [TearDown]
        public void TearDown()
        {
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Pause_Before_Replay_Commit_Metadata);
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Hold_Before_Replay_Commit_Metadata);
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Hold_After_Replay_Commit_Metadata);
            context?.TearDown();
        }

        /// <summary>
        /// Replay parks on a commit-metadata record in the middle of an in-memory chunk while the primary keeps writing far
        /// more than the replica AOF buffer holds. Every record after the commit-metadata record must still be replayed.
        /// </summary>
        [Test, CancelAfter(TestTimeoutMs)]
        [Category("REPLICATION")]
        public async Task ReplayCommitMetadataMidChunkDoesNotLoseRecordsUnderEvictionAsync(CancellationToken cancellationToken)
        {
            ClusterTestUtils.IgnoreIfExceptionInjectionDisabled();

            const int primaryIndex = 0;
            const int replicaIndex = 1;
            const string aofMemorySize = "128k";
            const int valueLength = 200;
            const int keysWrittenWhileHeld = 8_000;

            context.CreateInstances(2, disableObjects: true, enableAOF: true, asyncReplay: true, AofMemorySize: aofMemorySize, AofPageSize: "32k", pageSize: "16k", memorySize: "1m", lowMemory: true, timeout: 60);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1, logger: context.logger);

            var expected = new Dictionary<string, string>();
            var nextKey = 0;
            void WriteKey()
            {
                var key = $"k{nextKey++}";
                var value = $"{key}:".PadRight(valueLength, 'v');
                var resp = context.clusterTestUtils.SetKey(primaryIndex, Encoding.ASCII.GetBytes(key), Encoding.ASCII.GetBytes(value), out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, resp);
                expected[key] = value;
            }

            for (var i = 0; i < 100; i++)
                WriteKey();
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger, cancellationToken);

            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Hold_Before_Replay_Commit_Metadata);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Hold_After_Replay_Commit_Metadata);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Pause_Before_Replay_Commit_Metadata);

            // Write until replay parks on a commit-metadata record that has more records after it in the same protected chunk
            while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Replication_Pause_Before_Replay_Commit_Metadata))
            {
                cancellationToken.ThrowIfCancellationRequested();
                WriteKey();
            }

            // Many times the replica AOF buffer, so the replica must evict the page holding the parked chunk to keep receiving
            for (var i = 0; i < keysWrittenWhileHeld; i++)
                WriteKey();

            await WaitForReplicaTailToStallAsync(replicaIndex, cancellationToken);

            // The commit shifts the read-only address and starts flushing the pages behind the parked chunk
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Hold_Before_Replay_Commit_Metadata);
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            // Later commit-metadata records in the same chunk refresh the replay epoch and let the replica recycle that buffer page
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Hold_After_Replay_Commit_Metadata);

            using var syncTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            syncTimeout.CancelAfter(TimeSpan.FromSeconds(60));
            try
            {
                context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger, syncTimeout.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                Assert.Fail($"Replica replay did not catch up: primary {context.clusterTestUtils.GetReplicationOffset(primaryIndex)}, replica {context.clusterTestUtils.GetReplicationOffset(replicaIndex)}");
            }

            var missing = 0;
            string firstMissing = null;
            foreach (var (key, value) in expected)
            {
                var actual = context.clusterTestUtils.GetKey(replicaIndex, Encoding.ASCII.GetBytes(key), out _, out _, out var responseState, logger: context.logger);
                if (responseState != ResponseState.OK || actual != value)
                {
                    missing++;
                    firstMissing ??= key;
                }
            }

            ClassicAssert.AreEqual(0, missing, $"{missing} of {expected.Count} keys were not replayed on the replica; first: {firstMissing}");
        }

        async Task WaitForReplicaTailToStallAsync(int replicaIndex, CancellationToken cancellationToken)
        {
            var tail = context.GetAofTailAddress(replicaIndex);
            var stableSince = DateTime.UtcNow;
            while (DateTime.UtcNow - stableSince < TimeSpan.FromSeconds(2))
            {
                await Task.Delay(100, cancellationToken);
                var current = context.GetAofTailAddress(replicaIndex);
                if (current != tail)
                {
                    tail = current;
                    stableSince = DateTime.UtcNow;
                }
            }
        }
    }
}
#endif