// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
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

        [TestCase(-1, false)]
        [TestCase(0, false)]
        [TestCase(1, false)]
        [TestCase(-1, true)]
        [TestCase(0, true)]
        [TestCase(1, true)]
        public void InlineSerializationUsesBorrowedBuffer(int capacityDelta, bool evict)
        {
            CreateOperation(3);
            var api = operation.LocalSession.BasicGarnetApi;
            var key = PinnedSpanByte.FromPinnedSpan("inline-serialization"u8);
            var initialValue = GC.AllocateArray<byte>(512, pinned: true);
            initialValue.AsSpan().Fill((byte)'a');
            Assert.That(api.SET(key, PinnedSpanByte.FromPinnedSpan(initialValue)), Is.EqualTo(GarnetStatus.OK));
            Assert.That(api.SET(key, PinnedSpanByte.FromPinnedSpan("short-value"u8)), Is.EqualTo(GarnetStatus.OK));
            if (evict)
            {
                var log = provider.storeWrapper.store.Log;
                log.FlushAndEvict(wait: true);
                Assert.That(log.HeadAddress, Is.EqualTo(log.TailAddress));
            }

            var input = new UnifiedInput(RespCommand.MIGRATE);
            var output = new UnifiedOutput();
            try
            {
                Assert.That(api.Read_UnifiedStore(key, ref input, ref output), Is.EqualTo(GarnetStatus.OK));
                var length = output.Accumulator.InlineLength;
                var expected = output.SpanByteAndMemory.Memory.Memory.Span[..length].ToArray();
                var owner = output.SpanByteAndMemory.Memory;
                var capacity = sizeof(int) + length + capacityDelta;
                var destination = GC.AllocateArray<byte>(capacity + 2, pinned: true);
                destination.AsSpan().Fill(0xCC);
                output.SpanByteAndMemory.SpanByte = PinnedSpanByte.FromPinnedSpan(destination.AsSpan(1, capacity));

                Assert.That(api.Read_UnifiedStore(key, ref input, ref output), Is.EqualTo(GarnetStatus.OK));
                Assert.That(output.Accumulator.IsEmpty, Is.True);
                Assert.That(output.SpanByteAndMemory.Memory, Is.SameAs(owner), "Borrowing must retain the reusable heap owner.");
                Assert.That(output.SpanByteAndMemory.IsSpanByte, Is.EqualTo(capacityDelta >= 0));
                if (capacityDelta >= 0)
                {
                    Assert.That(BinaryPrimitives.ReadInt32LittleEndian(destination.AsSpan(1)), Is.EqualTo(length));
                    Assert.That(destination.AsSpan(1 + sizeof(int), length).ToArray(), Is.EqualTo(expected));
                }
                else
                {
                    Assert.That(destination, Is.All.EqualTo(0xCC));
                    Assert.That(output.SpanByteAndMemory.Memory.Memory.Span[..length].ToArray(), Is.EqualTo(expected));
                }
                Assert.That(destination[0], Is.EqualTo(0xCC));
                Assert.That(destination[^1], Is.EqualTo(0xCC));
            }
            finally
            {
                output.Dispose();
                GC.KeepAlive(initialValue);
            }
        }

        [Test]
        public void BorrowedSerializationFallsBackForOverflowThenReturnsInline()
        {
            CreateOperation(3);
            var api = operation.LocalSession.BasicGarnetApi;
            var key = PinnedSpanByte.FromPinnedSpan("serialization-transition"u8);
            var destination = GC.AllocateArray<byte>(1024, pinned: true);
            var overflow = GC.AllocateArray<byte>(32768, pinned: true);
            overflow.AsSpan().Fill((byte)'v');
            var input = new UnifiedInput(RespCommand.MIGRATE);
            var output = new UnifiedOutput();
            try
            {
                for (var i = 0; i < 3; i++)
                {
                    var value = i == 1 ? PinnedSpanByte.FromPinnedSpan(overflow) : PinnedSpanByte.FromPinnedSpan("inline"u8);
                    Assert.That(api.SET(key, value), Is.EqualTo(GarnetStatus.OK));
                    var owner = output.SpanByteAndMemory.Memory;
                    output.SpanByteAndMemory.SpanByte = PinnedSpanByte.FromPinnedSpan(destination);
                    Assert.That(api.Read_UnifiedStore(key, ref input, ref output), Is.EqualTo(GarnetStatus.OK));
                    Assert.That(output.SpanByteAndMemory.IsSpanByte, Is.EqualTo(i != 1));
                    Assert.That(output.Accumulator.IsEmpty, Is.EqualTo(i != 1));
                    if (i == 0)
                        Assert.That(output.SpanByteAndMemory.Memory, Is.Null, "The inline fast path needs no staging allocation.");
                    if (i == 2)
                        Assert.That(output.SpanByteAndMemory.Memory, Is.SameAs(owner));
                }
            }
            finally
            {
                output.Dispose();
                GC.KeepAlive(overflow);
                GC.KeepAlive(destination);
            }
        }

        [Test]
        public async Task DisposalDuringTransmitRetainsBorrowedBufferUntilWorkerExits()
        {
            CreateOperation(3);
            var client = operation.Client;
            operation.EnterWorker();
            try
            {
                Assert.That(await operation.InitializeAsync(), Is.True);
                client.SetClusterMigrateHeader(provider.clusterManager.CurrentConfig.LocalNodeId, false, false);
                operation.sketch.HashAndStore(PinnedSpanByte.FromPinnedSpan("disposal-key"u8));

                Assert.ThrowsAsync<OperationCanceledException>(() => operation.TransmitKeysAsync(_ =>
                {
                    operation.Dispose();
                    operation.Dispose();
                    Assert.That(client.Disposed, Is.False, "An active worker still owns the send buffer.");
                    throw new OperationCanceledException();
                }));

                Assert.That(client.Disposed, Is.False, "Returning from a batch must not end the worker's lifetime.");
                Assert.That(client.GetRecordSerializationBuffer().IsValid, Is.True);
            }
            finally
            {
                operation.ExitWorker();
            }

            Assert.That(client.Disposed, Is.True, "The exiting worker must release the client.");
        }

        [Test]
        public void DisposedOperationRejectsWorkerEntry()
        {
            CreateOperation(3);
            operation.Dispose();
            Assert.Throws<ObjectDisposedException>(() => operation.EnterWorker());
            operation.Dispose();
            Assert.That(operation.Client.Disposed, Is.True);
        }

        [Test]
        public async Task BorrowedRecordCommitPreservesFramingAndRejectsStaleDestinations()
        {
            CreateOperation(3);
            Assert.That(await operation.InitializeAsync(), Is.True);
            var client = operation.Client;
            client.SetClusterMigrateHeader(provider.clusterManager.CurrentConfig.LocalNodeId, false, false);
            unsafe
            {
                const int length = 16;
                var available = client.GetAvailableNetworkBufferSpan();
                var paddingLength = available.Length - (1 + sizeof(int) + length + 2) - (1 + sizeof(int));
                Assert.That(client.TryWriteRecordSpan(new byte[paddingLength], MigrationRecordSpanType.LogRecord, out var response), Is.True);
                Assert.That(response, Is.Null);
                var frame = client.GetAvailableNetworkBufferSpan();
                var destination = client.GetRecordSerializationBuffer();
                Assert.That(destination.Length, Is.EqualTo(sizeof(int) + length));
                Assert.That(client.GetAvailableNetworkBufferSpan().Length, Is.EqualTo(frame.Length));

                Assert.Throws<ArgumentOutOfRangeException>(() => client.CommitRecordSpan(destination, length + 1, MigrationRecordSpanType.LogRecord));
                *(int*)destination.ToPointer() = length - 1;
                Assert.Throws<InvalidOperationException>(() => client.CommitRecordSpan(destination, length, MigrationRecordSpanType.LogRecord));
                *(int*)destination.ToPointer() = length;
                new Span<byte>(destination.ToPointer() + sizeof(int), length).Fill(0xAB);
                client.CommitRecordSpan(destination, length, MigrationRecordSpanType.LogRecord);

                Assert.That(frame.ReadOnlySpan[0], Is.EqualTo((byte)MigrationRecordSpanType.LogRecord));
                Assert.That(BinaryPrimitives.ReadInt32LittleEndian(frame.ReadOnlySpan[1..]), Is.EqualTo(length));
                Assert.That(frame.ReadOnlySpan.Slice(1 + sizeof(int), length).ToArray(), Is.All.EqualTo(0xAB));
                Assert.That(client.GetAvailableNetworkBufferSpan().Length, Is.EqualTo(2), "The batch trailer must still fit.");
                Assert.That(client.GetRecordSerializationBuffer().IsValid, Is.False);
                Assert.Throws<InvalidOperationException>(() => client.CommitRecordSpan(destination, length, MigrationRecordSpanType.LogRecord));
            }
        }

        [Test]
        public async Task MissingRecordDoesNotCommitOrSendBorrowedSpace()
        {
            CreateOperation(3);
            Assert.That(await operation.InitializeAsync(), Is.True);
            var client = operation.Client;
            client.SetClusterMigrateHeader(provider.clusterManager.CurrentConfig.LocalNodeId, false, false);
            var available = client.GetAvailableNetworkBufferSpan().Length;
            var output = new UnifiedOutput();
            try
            {
                output.SpanByteAndMemory.SpanByte = client.GetRecordSerializationBuffer();
                var input = new UnifiedInput(RespCommand.MIGRATE);
                Assert.That(operation.LocalSession.BasicGarnetApi.Read_UnifiedStore(PinnedSpanByte.FromPinnedSpan("missing"u8), ref input, ref output),
                    Is.EqualTo(GarnetStatus.NOTFOUND));
                Assert.That(client.GetAvailableNetworkBufferSpan().Length, Is.EqualTo(available));
                Assert.That(client.SendAndResetIterationBuffer(), Is.Null);
            }
            finally
            {
                output.Dispose();
            }
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