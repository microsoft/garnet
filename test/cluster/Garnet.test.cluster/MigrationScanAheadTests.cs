// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.cluster;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;

#if DEBUG
using System.Linq;
using System.Threading;
using Tsavorite.core;
#endif

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    internal class MigrationScanAheadTests : TestBase
    {
        const int KeySendBatchSize = 1024;
        const int KeyCount = KeySendBatchSize + 7;

        ClusterTestContext context;
        ClusterProvider provider;
        MigrateSession session;
        MigrateSession.MigrateOperation operation;
        GarnetClientSession source;
        GarnetClientSession target;
        Task<bool> migration;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public async Task TearDown()
        {
            operation?.Dispose();
            session?.Dispose();
#if DEBUG
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
#endif
            try
            {
                if (migration != null)
                {
                    try
                    {
                        await migration.WaitAsync(TimeSpan.FromSeconds(10));
                    }
                    catch (OperationCanceledException)
                    {
                        // Cancellation joins the prefetched scan before releasing its storage session.
                    }
                }
            }
            finally
            {
                context.TearDown();
#if DEBUG
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead);
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Migration_Before_Batch_Response);
#endif
            }
        }

        static string Key(int index) => $"{{scan-ahead}}{index:D2}";

        async Task CreateOperationAsync(int depth, bool copy = false, int keyCount = KeyCount, bool prepare = true, bool useScanAheadMigration = true, int keySendBatchSize = KeySendBatchSize, int deleteBatchSize = 256)
        {
            context.CreateInstances(2, lowMemory: true, memorySize: "32m", pageSize: "16k",
                migrationSendBufferSize: "16k", migrationPipelineDepth: depth);
            context.CreateConnection();
            foreach (var options in context.nodeOptions)
            {
                options.ParallelMigrateTaskCount = 1;
                options.UseScanAheadMigration = useScanAheadMigration;
                options.MigrationDeleteBatchSize = deleteBatchSize;
                options.MigrateKeySendBatchSize = keySendBatchSize;
            }
            Assert.That(context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true), Is.EqualTo("OK"));
            context.clusterTestUtils.SetConfigEpoch(0, 1);
            context.clusterTestUtils.SetConfigEpoch(1, 2);
            context.clusterTestUtils.Meet(0, 1);
            context.clusterTestUtils.WaitUntilNodeIsKnown(0, 1);
            context.clusterTestUtils.WaitUntilNodeIsKnown(1, 0);

            source = context.clusterTestUtils.GetGarnetClientSession(0);
            target = context.clusterTestUtils.GetGarnetClientSession(1);
            for (var start = 0; start < keyCount; start += 512)
            {
                var writes = new Task<string>[Math.Min(512, keyCount - start)];
                for (var i = 0; i < writes.Length; i++)
                    writes[i] = source.ExecuteAsync("SET", Key(start + i), $"value-{start + i}");
                Assert.That(await Task.WhenAll(writes).WaitAsync(TimeSpan.FromSeconds(10)), Is.All.EqualTo("OK"));
            }

            provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
            if (!prepare)
                return;
            var sourceId = context.clusterTestUtils.GetLocalNodeId(0);
            var targetId = context.clusterTestUtils.GetLocalNodeId(1);
            var endpoint = context.clusterTestUtils.GetEndPoint(1);
            var slot = HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(Key(0)));
            session = new MigrateSession(null, provider, endpoint.Address.ToString(), endpoint.Port, targetId,
                null, null, sourceId, copy, false, 10000, [slot], null, TransferOption.SLOTS);
            Assert.That(await session.TrySetSlotRangesAsync(sourceId, MigrateState.IMPORT), Is.True);
            Assert.That(session.TryPrepareLocalForMigration(), Is.True);
            operation = new MigrateSession.MigrateOperation(session);
            Assert.That(await operation.InitializeAsync(), Is.True);
        }

        Task<bool> StartMigration()
        {
            return migration = RunWorkerAsync();

            async Task<bool> RunWorkerAsync()
            {
                operation.EnterWorker();
                try
                {
                    var log = provider.storeWrapper.store.Log;
                    return await operation.MigrateSlotsAsync(log.BeginAddress, log.TailAddress, log.TailAddress);
                }
                finally
                {
                    operation.ExitWorker();
                }
            }
        }

        async Task<string> TargetAsync(params string[] command)
        {
            Assert.That(await target.ExecuteAsync("ASKING"), Is.EqualTo("OK"));
            return await target.ExecuteAsync(command);
        }

        [Test]
        public async Task TransfersAcrossSmallBatches([Values(1, 3)] int depth, [Values] bool copy, [Values] bool useScanAheadMigration)
        {
            await CreateOperationAsync(depth, copy, useScanAheadMigration: useScanAheadMigration);
            Assert.That(await StartMigration().WaitAsync(TimeSpan.FromSeconds(20)), Is.True);
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo(copy ? KeyCount.ToString() : "0"));
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo(KeyCount.ToString()));
            for (var i = 0; i < KeyCount; i++)
                Assert.That(await TargetAsync("GET", Key(i)), Is.EqualTo($"value-{i}"));
        }

        [Test]
        public async Task TransfersMixedInlineAndChunkedRecords([Values(1, 3)] int depth, [Values] bool useScanAheadMigration, [Values] bool evict)
        {
            const int count = 48;
            await CreateOperationAsync(depth, keyCount: count, useScanAheadMigration: useScanAheadMigration);
            var expected = new string[count];
            for (var i = 0; i < count; i++)
            {
                var length = (i % 4) switch { 0 => 32, 1 => 1024, 2 => 4096, _ => 32768 };
                expected[i] = new string((char)('a' + i % 26), length);
                Assert.That(await source.ExecuteAsync("SET", Key(i), expected[i]), Is.EqualTo("OK"));
            }
            if (evict)
                provider.storeWrapper.store.Log.FlushAndEvict(wait: true);

            Assert.That(await StartMigration().WaitAsync(TimeSpan.FromSeconds(30)), Is.True);
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo("0"));
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo(count.ToString()));
            for (var i = 0; i < count; i++)
                Assert.That(await TargetAsync("GET", Key(i)), Is.EqualTo(expected[i]));
        }

        [TestCase(255, false, 256)]
        [TestCase(256, false, 256)]
        [TestCase(257, false, 256)]
        [TestCase(513, false, 256)]
        [TestCase(513, true, 512)]
        [TestCase(513, false, 1)]
        [TestCase(513, false, 64)]
        [TestCase(513, false, 512)]
        [TestCase(513, false, int.MaxValue)]
        public async Task TransfersAcrossDeletionBatchBoundaries(int keyCount, bool copy, int deleteBatchSize)
        {
            await CreateOperationAsync(3, copy, keyCount: keyCount, deleteBatchSize: deleteBatchSize);
            Assert.That(await StartMigration().WaitAsync(TimeSpan.FromSeconds(20)), Is.True);
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo(copy ? keyCount.ToString() : "0"));
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo(keyCount.ToString()));
            for (var i = 0; i < keyCount; i++)
                Assert.That(await TargetAsync("GET", Key(i)), Is.EqualTo($"value-{i}"));
        }

        [TestCase(256)]
        [TestCase(512)]
        public async Task DeleteSlotsSkipsNamespacedAndDeferredKeysAcrossBatches(int deleteBatchSize)
        {
            const int keyCount = 260;
            await CreateOperationAsync(3, keyCount: keyCount, deleteBatchSize: deleteBatchSize);
            var vectorKey = Encoding.ASCII.GetBytes(Key(127));
            var rangeKey = Encoding.ASCII.GetBytes(Key(258));
            operation.EncounteredVectorSet(vectorKey, [], null);
            operation.AddRangeIndexKey(rangeKey);
            for (var i = 0; i < keyCount; i++)
            {
                var key = Encoding.ASCII.GetBytes(Key(i));
                Assert.That(i == 1
                    ? operation.sketch.TryHashAndStore([1], key)
                    : operation.sketch.TryHashAndStore(key), Is.True);
            }

            operation.DeleteKeys();

            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo("3"));
            foreach (var (_, key, hasNamespace) in operation.sketch.argSliceVector)
            {
                var retained = hasNamespace || key.ReadOnlySpan.SequenceEqual(vectorKey) || key.ReadOnlySpan.SequenceEqual(rangeKey);
                Assert.That(operation.LocalSession.BasicGarnetApi.EXISTS(key), Is.EqualTo(retained ? GarnetStatus.OK : GarnetStatus.NOTFOUND));
            }
        }

        [Test]
        public async Task EmptyScanCompletesWithoutShipping([Values] bool useScanAheadMigration)
        {
            await CreateOperationAsync(3, keyCount: 0, useScanAheadMigration: useScanAheadMigration);
            Assert.That(await StartMigration().WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo("0"));
        }

#if DEBUG
        [TestCase(1024, false)]
        [TestCase(1024, true)]
        [TestCase(1025, false)]
        [TestCase(1025, true)]
        [TestCase(4096, false)]
        [TestCase(4096, true)]
        [CancelAfter(30000)]
        public async Task ConfiguredCollectionLimitAppliesToBothSketches(int collectionBatchSize, bool useScanAheadMigration, CancellationToken cancellationToken)
        {
            var keyCount = collectionBatchSize + 7;
            await CreateOperationAsync(3, keyCount: keyCount,
                useScanAheadMigration: useScanAheadMigration, keySendBatchSize: collectionBatchSize);
            for (var i = 0; i < collectionBatchSize; i++)
                Assert.That(operation.sketch.TryHashAndStore(Encoding.ASCII.GetBytes(Key(i))), Is.True);
            Assert.That(operation.sketch.TryHashAndStore(Encoding.ASCII.GetBytes(Key(collectionBatchSize))), Is.False);
            operation.sketch.Clear();

            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            _ = StartMigration();
            await WaitForPauseAsync(ExceptionInjectionType.Migration_Before_Batch_Response, cancellationToken);
            Assert.That(operation.sketch.argSliceVector.Count, Is.EqualTo(collectionBatchSize));
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            Assert.That(await migration.WaitAsync(cancellationToken), Is.True);
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo("0"));
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo(keyCount.ToString()));
            for (var i = 0; i < keyCount; i++)
                Assert.That(await TargetAsync("GET", Key(i)), Is.EqualTo($"value-{i}"));
        }

        [TestCase(1)]
        [TestCase(3)]
        [CancelAfter(30000)]
        public async Task DisabledScanAheadWaitsForShippingBeforeScanning(int depth, CancellationToken cancellationToken)
        {
            await CreateOperationAsync(depth, useScanAheadMigration: false);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            _ = StartMigration();
            await WaitForPauseAsync(ExceptionInjectionType.Migration_Before_Batch_Response, cancellationToken);

            Assert.That(ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead), Is.True);
            Assert.That(migration.IsCompleted, Is.False);
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo(KeyCount.ToString()));
            RecreateKey(KeySendBatchSize, "updated-before-scan");

            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            Assert.That(await migration.WaitAsync(cancellationToken), Is.True);
            Assert.That(ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead), Is.True,
                "Disabled scan-ahead must not launch a background scan");
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo("0"));
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo(KeyCount.ToString()));
            Assert.That(await TargetAsync("GET", Key(KeySendBatchSize)), Is.EqualTo("updated-before-scan"));
        }

        [Test]
        [CancelAfter(30000)]
        public async Task DisabledScanAheadRetainsUnacknowledgedKeysOnCancellationOrDisconnect([Values] bool cancel, CancellationToken cancellationToken)
        {
            await CreateOperationAsync(3, useScanAheadMigration: false);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            _ = StartMigration();
            await WaitForPauseAsync(ExceptionInjectionType.Migration_Before_Batch_Response, cancellationToken);
            if (cancel)
                session.Dispose();
            else
                operation.Dispose();
            Assert.That(await migration.WaitAsync(cancellationToken), Is.False);
            Assert.That(session.Status, Is.EqualTo(MigrateState.FAIL));
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo(KeyCount.ToString()));
        }

        static async Task WaitForPauseAsync(ExceptionInjectionType injection, CancellationToken token)
        {
            while (ExceptionInjectionHelper.IsEnabled(injection))
                await Task.Delay(10, token);
        }

        async Task PauseOverlappingStagesAsync(int depth, CancellationToken token)
        {
            await CreateOperationAsync(depth);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            _ = StartMigration();
            await Task.WhenAll(
                WaitForPauseAsync(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead, token),
                WaitForPauseAsync(ExceptionInjectionType.Migration_Before_Batch_Response, token));
            Assert.That(migration.IsCompleted, Is.False);
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo(KeyCount.ToString()), "Unacknowledged keys must not be deleted");
            Assert.That(operation.sketch.Status, Is.EqualTo(SketchStatus.TRANSMITTING));
            Assert.That(operation.sketch.argSliceVector.Count, Is.EqualTo(KeySendBatchSize));
        }

        unsafe void RecreateKey(int index, string value, bool hash = false)
        {
            using var mutations = new LocalServerSession(provider.storeWrapper);
            var keyBytes = Encoding.ASCII.GetBytes(Key(index));
            var valueBytes = Encoding.ASCII.GetBytes(value);
            fixed (byte* keyPtr = keyBytes, valuePtr = valueBytes, fieldPtr = "field"u8)
            {
                var key = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                var data = PinnedSpanByte.FromPinnedPointer(valuePtr, valueBytes.Length);
                // Model an already-admitted mutation; new network creates redirect while the slot is migrating.
                var status = hash
                    ? mutations.BasicGarnetApi.HashSet(key, PinnedSpanByte.FromPinnedPointer(fieldPtr, 5), data, out _)
                    : mutations.BasicGarnetApi.SET(key, data);
                Assert.That(status, Is.EqualTo(GarnetStatus.OK));
            }
        }

        unsafe bool KeyExists(int index)
        {
            using var reads = new LocalServerSession(provider.storeWrapper);
            var bytes = Encoding.ASCII.GetBytes(Key(index));
            fixed (byte* ptr = bytes)
                return reads.BasicGarnetApi.EXISTS(PinnedSpanByte.FromPinnedPointer(ptr, bytes.Length)) == GarnetStatus.OK;
        }

        unsafe bool CanAccessKey(int index, bool readOnly)
        {
            var bytes = Encoding.ASCII.GetBytes(Key(index));
            fixed (byte* ptr = bytes)
                return provider.migrationManager.CanAccessKey(PinnedSpanByte.FromPinnedPointer(ptr, bytes.Length),
                    HashSlotUtils.HashSlot(bytes), readOnly);
        }

        [Test]
        [CancelAfter(60000)]
        public async Task RegisteredMigrationProtectsOnlyActiveBatch(CancellationToken cancellationToken)
        {
            const int batchSize = (1 << 18) - 1;
            const int keyCount = batchSize + 7;
            await CreateOperationAsync(3, keyCount: keyCount, prepare: false, keySendBatchSize: batchSize);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            var slot = HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(Key(0)));
            context.clusterTestUtils.MigrateSlotsIndex(0, 1, [slot]);
            await Task.WhenAll(
                WaitForPauseAsync(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead, cancellationToken),
                WaitForPauseAsync(ExceptionInjectionType.Migration_Before_Batch_Response, cancellationToken));

            Assert.That(CanAccessKey(0, readOnly: false), Is.False);
            Assert.That(CanAccessKey(0, readOnly: true), Is.True);
            // Bloom-filter false positives may conservatively protect some keys in the pending batch.
            var pendingKey = Enumerable.Range(batchSize, 7).First(index => CanAccessKey(index, readOnly: false));
            Assert.That(await source.ExecuteAsync("SET", Key(pendingKey), "updated-after-prefetch")
                .WaitAsync(cancellationToken), Is.EqualTo("OK"));

            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            context.clusterTestUtils.WaitForMigrationCleanup(0, cancellationToken);
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo("0"));
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo(keyCount.ToString()));
            Assert.That(await target.ExecuteAsync("GET", Key(pendingKey)), Is.EqualTo("updated-after-prefetch"));
        }

        [TestCase(1)]
        [TestCase(3)]
        [CancelAfter(30000)]
        public async Task PrefetchedKeysReadCurrentValuesOnlyAfterProtection(int depth, CancellationToken cancellationToken)
        {
            await PauseOverlappingStagesAsync(depth, cancellationToken);
            var grownValue = new string('x', 128 * 1024);
            RecreateKey(KeySendBatchSize, grownValue);
            Assert.That(await source.ExecuteAsync("DEL", Key(KeySendBatchSize + 1)), Is.EqualTo("1"));
            Assert.That(await source.ExecuteAsync("DEL", Key(KeySendBatchSize + 2)), Is.EqualTo("1"));
            RecreateKey(KeySendBatchSize + 2, "recreated");
            Assert.That(await source.ExecuteAsync("DEL", Key(KeySendBatchSize + 3)), Is.EqualTo("1"));
            RecreateKey(KeySendBatchSize + 3, "hash-value", hash: true);
            Assert.That(await source.ExecuteAsync("PEXPIRE", Key(KeySendBatchSize + 4), "1"), Is.EqualTo("1"));
            Assert.That(() => KeyExists(KeySendBatchSize + 4), Is.False.After(5000).PollEvery(10));
            Assert.That(await source.ExecuteAsync("PEXPIRE", Key(KeySendBatchSize + 5), "300000"), Is.EqualTo("1"));
            Assert.That(await source.ExecuteAsync("PERSIST", Key(KeySendBatchSize + 5)), Is.EqualTo("1"));
            Assert.That(await source.ExecuteAsync("SET", Key(KeySendBatchSize + 6), "updated"), Is.EqualTo("OK"));

            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Slot_After_Scan_Ahead);
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Migration_Before_Batch_Response);
            Assert.That(await migration.WaitAsync(cancellationToken), Is.True);

            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo("0"));
            Assert.That(await target.ExecuteAsync("DBSIZE"), Is.EqualTo((KeyCount - 2).ToString()));
            for (var i = 0; i < KeySendBatchSize; i++)
                Assert.That(await TargetAsync("GET", Key(i)), Is.EqualTo($"value-{i}"));
            Assert.That(await TargetAsync("GET", Key(KeySendBatchSize)), Is.EqualTo(grownValue));
            Assert.That(await TargetAsync("GET", Key(KeySendBatchSize + 1)), Is.Null);
            Assert.That(await TargetAsync("GET", Key(KeySendBatchSize + 2)), Is.EqualTo("recreated"));
            Assert.That(await TargetAsync("HGET", Key(KeySendBatchSize + 3), "field"), Is.EqualTo("hash-value"));
            Assert.That(await TargetAsync("GET", Key(KeySendBatchSize + 4)), Is.Null);
            Assert.That(await TargetAsync("GET", Key(KeySendBatchSize + 5)), Is.EqualTo($"value-{KeySendBatchSize + 5}"));
            Assert.That(await TargetAsync("PTTL", Key(KeySendBatchSize + 5)), Is.EqualTo("-1"));
            Assert.That(await TargetAsync("GET", Key(KeySendBatchSize + 6)), Is.EqualTo("updated"));
        }

        [TestCase(false)]
        [TestCase(true)]
        [CancelAfter(30000)]
        public async Task CancellationOrDisconnectJoinsScanAheadAndRetainsUnacknowledgedKeys(bool cancel, CancellationToken cancellationToken)
        {
            await PauseOverlappingStagesAsync(3, cancellationToken);
            if (cancel)
                session.Dispose();
            else
                operation.Dispose();
            Assert.That(await migration.WaitAsync(cancellationToken), Is.False);
            Assert.That(session.Status, Is.EqualTo(MigrateState.FAIL));
            Assert.That(await source.ExecuteAsync("DBSIZE"), Is.EqualTo(KeyCount.ToString()));
        }
#endif
    }
}