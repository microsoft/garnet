// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.server;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    internal class MigrationResponseRingTests : TestBase
    {
        ClusterTestContext context;
        ClusterProvider provider;
        MigrateSession session;
        MigrateSession.MigrateOperation operation;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
            context.CreateInstances(1, lowMemory: true, memorySize: "16m", pageSize: "16k");
            provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
        }

        [TearDown]
        public void TearDown()
        {
            operation?.Dispose();
            session?.Dispose();
            context.TearDown();
        }

        void CreateOperation(int depth, bool copy = false, int batchSize = 4, int deleteBatchSize = 256)
        {
            provider.serverOptions.MigrationPipelineDepth = depth;
            provider.serverOptions.MigrationDeleteBatchSize = deleteBatchSize;
            var nodeId = provider.clusterManager.CurrentConfig.LocalNodeId;
            session = new MigrateSession(null, provider, "127.0.0.1", ClusterTestContext.Port,
                nodeId, null, null, nodeId, copy, false, 10000, [0], null, TransferOption.KEYS);
            operation = new MigrateSession.MigrateOperation(session, batchSize: batchSize);
        }

        [TestCase(false, 256)]
        [TestCase(true, 256)]
        [TestCase(false, 300)]
        public void DeleteKeysOnlyRemovesTransmittedEntriesAcrossBatches(bool copy, int deleteBatchSize)
        {
            CreateOperation(3, copy, batchSize: 1024, deleteBatchSize: deleteBatchSize);
            for (var i = 0; i < 513; i++)
                Assert.That(operation.sketch.TryHashAndStore(Encoding.ASCII.GetBytes($"delete-key-{i}")), Is.True);

            var index = 0;
            foreach (var (_, key, _) in operation.sketch.argSliceVector)
            {
                Assert.That(operation.LocalSession.BasicGarnetApi.SET(key, PinnedSpanByte.FromPinnedSpan("value"u8)), Is.EqualTo(GarnetStatus.OK));
                operation.sketch.HashAndStore(key);
                operation.sketch.Keys[index] = (key, index % 3 != 0);
                index++;
            }

            operation.DeleteKeys();

            foreach (var (key, transmitted) in operation.sketch.Keys)
                Assert.That(operation.LocalSession.BasicGarnetApi.EXISTS(key), Is.EqualTo(copy || !transmitted ? GarnetStatus.OK : GarnetStatus.NOTFOUND));
        }

        static TaskCompletionSource<string>[] CreateResponses(int count)
            => Enumerable.Range(0, count).Select(_ => new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();

        [TestCase(1)]
        [TestCase(3)]
        [TestCase(64)]
        public async Task WindowUsesDepthCapturedAtConstruction(int depth)
        {
            CreateOperation(depth);
            provider.serverOptions.MigrationPipelineDepth = depth == 1 ? 3 : 1;
            var responses = CreateResponses(depth);
            Task<bool> send = null;
            for (var i = 0; i < responses.Length; i++)
            {
                send = operation.HandleMigrateTaskResponseAsync(responses[i].Task);
                if (i < responses.Length - 1)
                    Assert.That(await send, Is.True);
            }

            Assert.That(send.IsCompleted, Is.False);
            for (var i = 1; i < responses.Length; i++)
                responses[i].SetResult("OK");
            Assert.That(send.IsCompleted, Is.False);
            responses[0].SetResult("OK");
            Assert.That(await send.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
            Assert.That(await operation.HandleMigrateTaskResponseAsync(null, drain: true), Is.True);
        }

        [Test]
        public async Task ResponsesRemainOrderedAcrossWraparoundAndDrain()
        {
            CreateOperation(3);
            var responses = CreateResponses(5);
            Assert.That(await operation.HandleMigrateTaskResponseAsync(responses[0].Task), Is.True);
            Assert.That(await operation.HandleMigrateTaskResponseAsync(responses[1].Task), Is.True);
            var firstWindow = operation.HandleMigrateTaskResponseAsync(responses[2].Task);
            responses[1].SetResult("OK");
            Assert.That(firstWindow.IsCompleted, Is.False);
            responses[0].SetResult("OK");
            Assert.That(await firstWindow.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);

            Assert.That(await operation.HandleMigrateTaskResponseAsync(responses[3].Task), Is.True);
            var wrappedWindow = operation.HandleMigrateTaskResponseAsync(responses[4].Task);
            responses[4].SetResult("OK");
            Assert.That(wrappedWindow.IsCompleted, Is.False);
            responses[2].SetResult("OK");
            Assert.That(await wrappedWindow.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);

            var drain = operation.HandleMigrateTaskResponseAsync(null, drain: true);
            Assert.That(drain.IsCompleted, Is.False);
            responses[3].SetResult("OK");
            Assert.That(await drain.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task RejectedOrFaultedResponseFailsMigration(bool fault)
        {
            CreateOperation(3);
            var response = fault
                ? Task.FromException<string>(new InvalidOperationException("Rejected batch"))
                : Task.FromResult("ERR rejected batch");
            Assert.That(await operation.HandleMigrateTaskResponseAsync(response, drain: true), Is.False);
            Assert.That(session.Status, Is.EqualTo(MigrateState.FAIL));
        }

        [Test]
        public async Task DisposalRejectsLateResponsesWithPendingBatches()
        {
            CreateOperation(3);
            var responses = CreateResponses(2);
            foreach (var response in responses)
                Assert.That(await operation.HandleMigrateTaskResponseAsync(response.Task), Is.True);

            operation.Dispose();
            var lateResponse = Task.FromException<string>(new InvalidOperationException("Disconnected"));
            Assert.That(await operation.HandleMigrateTaskResponseAsync(lateResponse), Is.False);
            foreach (var response in responses)
                response.SetException(new InvalidOperationException("Disconnected"));
            Assert.That(session.Status, Is.EqualTo(MigrateState.FAIL));
        }

        [Test]
        public async Task SessionCancellationInterruptsPendingResponse()
        {
            CreateOperation(1);
            var response = CreateResponses(1)[0];
            var send = operation.HandleMigrateTaskResponseAsync(response.Task);
            Assert.That(send.IsCompleted, Is.False);

            session.Dispose();
            Assert.That(await send.WaitAsync(TimeSpan.FromSeconds(10)), Is.False);
            operation.Dispose();
            response.SetException(new InvalidOperationException("Disconnected"));
            Assert.That(session.Status, Is.EqualTo(MigrateState.FAIL));
        }
    }
}