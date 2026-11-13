// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.cluster;
using Garnet.common;
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
        MigrateSessionTaskStore taskStore;

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
            taskStore?.Dispose();
            operation?.Dispose();
            session?.Dispose();
            context.TearDown();
        }

        void CreateOperation(int depth, bool copy = false, int keySendBatchSize = 1024, int deleteBatchSize = 256, HashSet<int> slots = null)
        {
            provider.serverOptions.MigrationPipelineDepth = depth;
            provider.serverOptions.MigrationDeleteBatchSize = deleteBatchSize;
            provider.serverOptions.MigrateKeySendBatchSize = keySendBatchSize;
            var nodeId = provider.clusterManager.CurrentConfig.LocalNodeId;
            session = new MigrateSession(null, provider, "127.0.0.1", ClusterTestContext.Port,
                nodeId, null, null, nodeId, copy, false, 10000, slots ?? [0], null, TransferOption.KEYS);
            operation = new MigrateSession.MigrateOperation(session);
        }

        MigrateSession.MigrateOperation[] CreateSlotOperations()
        {
            provider.serverOptions.ParallelMigrateTaskCount = 2;
            var nodeId = provider.clusterManager.CurrentConfig.LocalNodeId;
            session = new MigrateSession(null, provider, "127.0.0.1", ClusterTestContext.Port,
                nodeId, null, null, nodeId, false, false, 10000, [0], null, TransferOption.SLOTS);
            var operations = GetOperations(session);
            for (var i = 0; i < operations.Length; i++)
            {
                operations[i].Dispose();
                provider.serverOptions.MigrateKeySendBatchSize = 1 << (10 + (i * 4));
                operations[i] = new MigrateSession.MigrateOperation(session);
            }
            return operations;
        }

        static MigrateSession.MigrateOperation[] GetOperations(MigrateSession migration)
            => (MigrateSession.MigrateOperation[])typeof(MigrateSession)
                .GetField("migrateOperation", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(migration);

        static CancellationToken GetCancellationToken(MigrateSession migration)
            => GetCancellationSource(migration).Token;

        static CancellationTokenSource GetCancellationSource(MigrateSession migration)
            => (CancellationTokenSource)typeof(MigrateSession)
                .GetField("migrationCancellationSource", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(migration);

        async Task<ClusterSession> CreateClusterSessionAsync(TcpClient connection, CancellationToken token, ClusterSession excluded = null)
        {
            await connection.ConnectAsync(IPAddress.Loopback, ClusterTestContext.Port, token);
            var stream = connection.GetStream();
            await stream.WriteAsync("*1\r\n$4\r\nPING\r\n"u8.ToArray(), token);
            await stream.ReadExactlyAsync(new byte[7], token);
            var server = (GarnetServerTcp)provider.storeWrapper.Servers[0];
            var caller = server.ActiveClusterSessions().OfType<ClusterSession>().Single(candidate => candidate != excluded);
            Assert.That(() => caller.LocalCurrentEpoch, Is.Zero.After(5_000, 10));
            return caller;
        }

        void CreateKeysMigration(ClusterSession caller)
        {
            var nodeId = provider.clusterManager.CurrentConfig.LocalNodeId;
            var sketch = new Sketch(bucketCount: 1, bfSize: 64);
            sketch.HashAndStore(PinnedSpanByte.FromPinnedSpan("cancelled-migration-key"u8));
            session = new MigrateSession(caller, provider, "127.0.0.1", ClusterTestContext.Port,
                nodeId, null, null, nodeId, false, false, 10000, [0], sketch, TransferOption.KEYS);
            operation = GetOperations(session)[0];
        }

        bool RegisterTask(out MigrateSession migration, SketchStatus status = SketchStatus.DELETING,
            HashSet<int> slots = null, string targetNodeId = null, TransferOption transferOption = TransferOption.KEYS)
        {
            var sketch = new Sketch(bucketCount: 1, bfSize: 64);
            sketch.HashAndStore(PinnedSpanByte.FromPinnedSpan("task-store-access"u8));
            sketch.SetStatus(status);
            var nodeId = provider.clusterManager.CurrentConfig.LocalNodeId;
            return taskStore.TryAddMigrateSession(null, provider, nodeId, "127.0.0.1", ClusterTestContext.Port,
                targetNodeId ?? nodeId, null, null, false, false, 10000, slots ?? [0], sketch, transferOption, out migration);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void SlotMembershipMatchesSnapshotAcrossAllSlots(bool dense)
        {
            HashSet<int> slots = dense
                ? new HashSet<int>(Enumerable.Range(0, ClusterConfig.MAX_HASH_SLOT_VALUE))
                : [0, 63, 64, 127, 128, ClusterConfig.MAX_HASH_SLOT_VALUE - 1];
            var expected = slots.ToArray();
            CreateOperation(1, slots: slots);
            slots.Clear();
            slots.Add(65);

            Assert.That(session.GetSlots, Is.EquivalentTo(expected));
            Assert.That(Enumerable.Range(0, ClusterConfig.MAX_HASH_SLOT_VALUE).Where(operation.Contains),
                Is.EquivalentTo(expected));
            Assert.That(operation.Contains(-1), Is.False);
            Assert.That(operation.Contains(ClusterConfig.MAX_HASH_SLOT_VALUE), Is.False);
        }

        [Test]
        public void SlotMembershipGatesProtectedKeysWithoutSlotAliasing()
        {
            CreateOperation(1, slots: [0, 64, ClusterConfig.MAX_HASH_SLOT_VALUE - 1]);
            var key = PinnedSpanByte.FromPinnedSpan("slot-membership"u8);
            GetOperations(session)[0].sketch.HashAndStore(key);
            GetOperations(session)[0].sketch.SetStatus(SketchStatus.DELETING);

            foreach (var slot in new[] { 0, 64, ClusterConfig.MAX_HASH_SLOT_VALUE - 1 })
            {
                Assert.That(session.CanAccessKey(key, slot, readOnly: false), Is.False);
                Assert.That(session.CanAccessKey(key, slot, readOnly: true), Is.False);
            }
            foreach (var slot in new[] { 1, 63, 65, 127, 128, ClusterConfig.MAX_HASH_SLOT_VALUE - 2 })
            {
                Assert.That(session.CanAccessKey(key, slot, readOnly: false), Is.True);
                Assert.That(session.CanAccessKey(key, slot, readOnly: true), Is.True);
            }
        }

        [TestCase(TransferOption.KEYS)]
        [TestCase(TransferOption.SLOTS)]
        public void TaskStoreRejectsEmptySlots(TransferOption transferOption)
        {
            taskStore = new MigrateSessionTaskStore();
            Assert.That(RegisterTask(out session, slots: [], transferOption: transferOption), Is.False);
            Assert.That(session, Is.Null);
            Assert.That(taskStore.GetNumSessions(), Is.Zero);
            Assert.That(RegisterTask(out session), Is.True);
            Assert.That(taskStore.GetNumSessions(), Is.EqualTo(1));
            Assert.That(taskStore.TryRemove(session), Is.True);
            Assert.That(taskStore.GetNumSessions(), Is.Zero);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void TaskStoreCountsSessionsRatherThanSlots(bool byTarget)
        {
            taskStore = new MigrateSessionTaskStore();
            Assert.That(taskStore.GetNumSessions(), Is.Zero);
            Assert.That(RegisterTask(out session, slots: [0, 1, 2]), Is.True);
            var first = session;
            Assert.That(taskStore.GetNumSessions(), Is.EqualTo(1));
            Assert.That(RegisterTask(out var second, slots: [3, 4]), Is.True);
            Assert.That(taskStore.GetNumSessions(), Is.EqualTo(2));
            Assert.That(RegisterTask(out var otherTarget, slots: [5, 6], targetNodeId: "other-target"), Is.True);
            Assert.That(taskStore.GetNumSessions(), Is.EqualTo(3));
            Assert.That(RegisterTask(out _, slots: [2, 7]), Is.False);
            Assert.That(taskStore.GetNumSessions(), Is.EqualTo(3));

            for (var i = 0; i < 2; i++)
            {
                Assert.That(byTarget ? taskStore.TryRemove(first.GetTargetNodeId) : taskStore.TryRemove(first), Is.True);
                Assert.That(taskStore.GetNumSessions(), Is.EqualTo(byTarget ? 1 : 2));
            }

            Assert.That(taskStore.TryRemove(first), Is.True);
            Assert.That(taskStore.TryRemove(second), Is.True);
            Assert.That(taskStore.GetNumSessions(), Is.EqualTo(1));
            Assert.That(taskStore.TryRemove(otherTarget), Is.True);
            Assert.That(taskStore.TryRemove(otherTarget), Is.True);
            Assert.That(taskStore.GetNumSessions(), Is.Zero);
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task TaskStoreAccessDoesNotWaitForRemoval(bool byTarget)
        {
            taskStore = new MigrateSessionTaskStore();
            Assert.That(RegisterTask(out session), Is.True);
            var captured = session;
            var key = PinnedSpanByte.FromPinnedSpan("task-store-access"u8);
            using var disposing = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            using var registration = GetCancellationToken(session).Register(() =>
            {
                disposing.Set();
                release.Wait();
            });
            var removal = Task.Factory.StartNew(() => byTarget
                ? taskStore.TryRemove(captured.GetTargetNodeId)
                : taskStore.TryRemove(captured), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            Task<bool> access = null;
            Task<int> count = null;
            try
            {
                Assert.That(disposing.Wait(TimeSpan.FromSeconds(5)), Is.True);
                access = Task.Run(() => taskStore.CanAccessKey(key, 0, readOnly: false));
                Assert.That(await access.WaitAsync(TimeSpan.FromSeconds(5)), Is.False,
                    "Admission must remain protected without waiting for the writer's disposal callback.");
                count = Task.Run(taskStore.GetNumSessions);
                Assert.That(await count.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo(1),
                    "Counting must not wait for the writer's disposal callback.");
            }
            finally
            {
                release.Set();
                Assert.That(await removal.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
                if (access != null)
                    await access.WaitAsync(TimeSpan.FromSeconds(10));
                if (count != null)
                    await count.WaitAsync(TimeSpan.FromSeconds(10));
            }
            Assert.That(taskStore.GetNumSessions(), Is.Zero);
            Assert.That(taskStore.CanAccessKey(key, 0, readOnly: false), Is.True);
            Assert.That(captured.CanAccessKey(key, 0, readOnly: false), Is.False,
                "A captured session must retain usable admission metadata after disposal.");
        }

        [Test]
        public void TaskStoreRetiredRemovalPreservesReplacement()
        {
            taskStore = new MigrateSessionTaskStore();
            Assert.That(RegisterTask(out session), Is.True);
            var retired = session;
            Assert.That(taskStore.TryRemove(retired.GetTargetNodeId), Is.True);
            Assert.That(RegisterTask(out session, SketchStatus.TRANSMITTING), Is.True);

            Assert.That(taskStore.TryRemove(retired), Is.True);
            var key = PinnedSpanByte.FromPinnedSpan("task-store-access"u8);
            Assert.That(taskStore.GetNumSessions(), Is.EqualTo(1));
            Assert.That(taskStore.CanAccessKey(key, 0, readOnly: false), Is.False);
            Assert.That(taskStore.CanAccessKey(key, 0, readOnly: true), Is.True);
        }

        [Test]
        public async Task TaskStoreConcurrentDisposeHasOneCleanupOwner()
        {
            taskStore = new MigrateSessionTaskStore();
            Assert.That(RegisterTask(out session), Is.True);
            var captured = session;
            var slots = (MigrateSession[])typeof(MigrateSessionTaskStore)
                .GetField("sessions", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(taskStore);
            var cleanupCount = 0;
            using var disposing = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            using var registration = GetCancellationToken(captured).Register(() =>
            {
                Interlocked.Increment(ref cleanupCount);
                disposing.Set();
                release.Wait();
            });
            var winner = Task.Factory.StartNew(taskStore.Dispose, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
            Task[] others = [];
            try
            {
                Assert.That(disposing.Wait(TimeSpan.FromSeconds(5)), Is.True);
                others = Enumerable.Range(0, 4).Select(_ => Task.Factory.StartNew(taskStore.Dispose,
                    CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();
                await Task.WhenAll(others).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.That(winner.IsCompleted, Is.False);
                Assert.That(taskStore.GetNumSessions(), Is.Zero);
                Assert.That(Volatile.Read(ref slots[0]), Is.SameAs(captured),
                    "Later disposers must return without running the winner's table cleanup.");
            }
            finally
            {
                release.Set();
                await winner.WaitAsync(TimeSpan.FromSeconds(10));
                await Task.WhenAll(others).WaitAsync(TimeSpan.FromSeconds(10));
            }
            Assert.That(cleanupCount, Is.EqualTo(1));
            Assert.That(Volatile.Read(ref slots[0]), Is.Null);
        }

        [Test]
        public async Task TaskStoreConcurrentLookupSurvivesReplacementAndShutdown()
        {
            taskStore = new MigrateSessionTaskStore();
            var key = PinnedSpanByte.FromPinnedSpan("task-store-access"u8);
            using var stop = new CancellationTokenSource();
            using var ready = new CountdownEvent(4);
            var readers = new Task[4];
            for (var i = 0; i < readers.Length; i++)
            {
                readers[i] = Task.Factory.StartNew(() =>
                {
                    ready.Signal();
                    while (!stop.IsCancellationRequested)
                    {
                        _ = taskStore.CanAccessKey(key, 0, readOnly: false);
                        _ = taskStore.CanAccessKey(key, 0, readOnly: true);
                        Assert.That(taskStore.GetNumSessions(), Is.InRange(0, 1));
                    }
                }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }
            try
            {
                Assert.That(ready.Wait(TimeSpan.FromSeconds(5)), Is.True);
                for (var i = 0; i < 32; i++)
                {
                    Assert.That(RegisterTask(out session), Is.True);
                    Assert.That(taskStore.CanAccessKey(key, 0, readOnly: false), Is.False);
                    Assert.That(i % 2 == 0 ? taskStore.TryRemove(session) : taskStore.TryRemove(session.GetTargetNodeId), Is.True);
                }
                Assert.That(RegisterTask(out session), Is.True);
                taskStore.Dispose();
                Assert.That(taskStore.GetNumSessions(), Is.Zero);
                Assert.That(taskStore.CanAccessKey(key, 0, readOnly: false), Is.True);
                Assert.That(RegisterTask(out session), Is.False);
                Assert.That(taskStore.GetNumSessions(), Is.Zero);
                Assert.That(() => GetOperations(session)[0].EnterWorker(), Throws.TypeOf<ObjectDisposedException>());
            }
            finally
            {
                stop.Cancel();
                await Task.WhenAll(readers).WaitAsync(TimeSpan.FromSeconds(10));
            }
        }

        [TestCase(1024, 16)]
        [TestCase(1025, 32)]
        [TestCase(4096, 64)]
        [TestCase(4097, 128)]
        [TestCase(8192, 128)]
        [TestCase(16384, 256)]
        [TestCase(32768, 512)]
        [TestCase(65536, 1024)]
        [TestCase(131072, 2048)]
        [TestCase(262143, 4096)]
        public void ConfiguredBatchSizeTargets64KeysPerBloomBucket(int keySendBatchSize, int bucketCount)
        {
            CreateOperation(1, keySendBatchSize: keySendBatchSize);
            ReadOnlySpan<byte> key = "migration-filter-key"u8;
            Assert.That(operation.sketch.TryHashAndStore(key), Is.True);
            var hash = HashUtils.MurmurHash2x64A(key);
            Assert.That(operation.sketch.Probe(hash, out _), Is.True);
            Assert.That(operation.sketch.Probe(hash ^ (uint)bucketCount, out _), Is.True);
            Assert.That(operation.sketch.Probe(hash ^ (uint)(bucketCount >> 1), out _), Is.False);
            for (var i = 0; i < 3; i++)
            {
                var shift = 16 + i * 16;
                Assert.That(operation.sketch.Probe(hash ^ (1UL << (shift + 14)), out _), Is.True);
                Assert.That(operation.sketch.Probe(hash ^ (1UL << (shift + 13)), out _), Is.False);
            }
            operation.sketch.Clear();
            Assert.That(operation.sketch.Probe(hash, out _), Is.False);
        }

        [Test]
        public void BucketedFilterRequiresEveryPosition([Values(1, 16, 65536)] int bucketCount, [Values(1, 2, 3, 4)] int hashCount)
        {
            var filter = new BucketedBloomFilter(bucketCount, bfSize: 64, hashCount: hashCount);
            var hash = BucketedFilterHash(bucketCount, hashCount);
            Assert.That(filter.Contains(hash), Is.False);
            filter.Clear();
            filter.Add(hash);
            Assert.That(filter.Contains(hash), Is.True);
            for (var i = 0; i < hashCount; i++)
                Assert.That(filter.Contains(BucketedFilterHash(bucketCount, hashCount, i)), Is.False);
            if (bucketCount > 1)
                Assert.That(filter.Contains(hash ^ 1), Is.False);
            if (bucketCount < BucketedBloomFilter.MaxBucketCount)
                Assert.That(filter.Contains(hash ^ (uint)bucketCount), Is.True);

            filter.Clear();
            Assert.That(filter.Contains(hash), Is.False);
            filter.Add(hash);
            Assert.That(filter.Contains(hash), Is.True);
        }

        [TestCase(0u)]
        [TestCase(7u)]
        [TestCase(uint.MaxValue)]
        public void BucketedFilterKeyAndHashOverloadsShareMembership(uint seed)
        {
            var filter = new BucketedBloomFilter(batchSize: 1024);
            ReadOnlySpan<byte> key = "migration-filter-key"u8;
            var hash = HashUtils.MurmurHash2x64A(key, seed);
            Assert.That(filter.Contains(key, seed), Is.False);

            filter.Add(key, seed);
            Assert.That(filter.Contains(key, seed), Is.True);
            Assert.That(filter.Contains(hash), Is.True);
            Assert.That(filter.Contains(key, seed ^ 1), Is.False);

            filter.Clear();
            filter.Add(hash);
            Assert.That(filter.Contains(key, seed), Is.True);
        }

        [TestCase(0)]
        [TestCase(-1)]
        [TestCase(4194305)]
        [TestCase(int.MaxValue)]
        public void BucketedFilterRejectsUnsupportedBatchSize(int batchSize)
        {
            Assert.That(() => new BucketedBloomFilter(batchSize),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        }

        static ulong BucketedFilterHash(int bucketCount, int hashCount, int changedHash = -1)
        {
            var hashBits = 48 / hashCount;
            var hash = (ulong)(bucketCount - 1);
            for (var i = 0; i < hashCount; i++)
                hash |= (ulong)(i == changedHash ? i + 1 : 63 - i) << (16 + i * hashBits);
            return hash;
        }

        [TestCase(0, 64, 2)]
        [TestCase(-64, 64, 2)]
        [TestCase(3, 64, 2)]
        [TestCase(131072, 64, 2)]
        [TestCase(1, 0, 2)]
        [TestCase(1, -64, 2)]
        [TestCase(1, 3, 2)]
        [TestCase(1, 131072, 2)]
        [TestCase(1, 64, 0)]
        [TestCase(1, 64, 49)]
        [TestCase(65536, 65536, 4)]
        public void BucketedFilterRejectsInvalidConfiguration(int bucketCount, int bfSize, int hashCount)
        {
            Assert.That(() => new BucketedBloomFilter(bucketCount, bfSize, hashCount),
                Throws.InstanceOf<ArgumentException>());
        }

        [TestCase(1, 1, 48)]
        [TestCase(4, 4, 3)]
        public void BucketedFilterSupportsSubByteBitmaps(int bucketCount, int bfSize, int hashCount)
        {
            var filter = new BucketedBloomFilter(bucketCount, bfSize, hashCount);
            filter.Add(ulong.MaxValue);
            Assert.That(filter.Contains(ulong.MaxValue), Is.True);
            filter.Clear();
            Assert.That(filter.Contains(ulong.MaxValue), Is.False);
        }

        [Test]
        public void BucketedFilterRetainsAllInsertedHashes()
        {
            var filter = new BucketedBloomFilter(bucketCount: 1 << 16, bfSize: 16, hashCount: 3);
            var hashes = new ulong[4096];
            var random = new Random(42);
            Span<byte> bytes = stackalloc byte[sizeof(ulong)];
            for (var i = 0; i < hashes.Length; i++)
            {
                random.NextBytes(bytes);
                hashes[i] = i switch { 0 => 0, 1 => ulong.MaxValue, _ => BinaryPrimitives.ReadUInt64LittleEndian(bytes) };
                filter.Add(hashes[i]);
            }
            Assert.That(hashes.All(filter.Contains), Is.True);
            filter.Clear();
            Assert.That(hashes.All(hash => !filter.Contains(hash)), Is.True);
        }

        [Test]
        public void BucketedFilterSupportsMaximumDimensions()
        {
            var filter = new BucketedBloomFilter(bucketCount: 1 << 16, bfSize: 1 << 16, hashCount: 3);
            filter.Add(0);
            filter.Add(ulong.MaxValue);
            Assert.That(filter.Contains(0), Is.True);
            Assert.That(filter.Contains(ulong.MaxValue), Is.True);
            Assert.That(filter.Contains(0xffff000000000001), Is.False);
            filter.Clear();
            Assert.That(filter.Contains(0), Is.False);
            Assert.That(filter.Contains(ulong.MaxValue), Is.False);
        }

        [TestCase(0, 1, 2)]
        [TestCase(1, 1, 2)]
        [TestCase(4, 1, 2)]
        [TestCase(0, 65536, 3)]
        [TestCase(1, 65536, 3)]
        [TestCase(4, 65536, 3)]
        public void SketchMembershipRequiresEveryPosition(int namespaceLength, int bucketCount, int hashCount)
        {
            var sketch = new Sketch(bucketCount: bucketCount, bfSize: 64, hashCount: hashCount);
            ReadOnlySpan<byte> key = "migration-filter-key"u8;
            ulong hash;
            if (namespaceLength == 0)
            {
                Assert.That(sketch.TryHashAndStore(key), Is.True);
                hash = HashUtils.MurmurHash2x64A(key);
            }
            else
            {
                var ns = new byte[namespaceLength];
                ns[0] = 7;
                Assert.That(sketch.TryHashAndStore(ns, key), Is.True);
                hash = HashUtils.MurmurHash2x64A(key, seed: (uint)VectorManager.ExtractContextFromNamespaces(ns));
            }
            AssertSketchMembership(sketch, hash, hashCount);
        }

        [TestCase(1, 2)]
        [TestCase(65536, 3)]
        public void ExplicitKeysSketchRequiresEveryPosition(int bucketCount, int hashCount)
        {
            var sketch = new Sketch(bucketCount: bucketCount, bfSize: 64, hashCount: hashCount);
            var key = PinnedSpanByte.FromPinnedSpan("migration-filter-key"u8);
            sketch.HashAndStore(key);
            AssertSketchMembership(sketch, HashUtils.MurmurHash2x64A(key.ReadOnlySpan), hashCount);
        }

        [Test]
        public void CollectedBatchRebuildsProtection([Values(0, 1, 4)] int namespaceLength, [Values] bool reuseCollection, [Values] bool bucketed)
        {
            var bucketCount = bucketed ? 1 << 16 : 1;
            var hashCount = bucketed ? 3 : 2;
            var sketch = new Sketch(bucketCount: bucketCount, bfSize: 64, hashCount: hashCount);
            Assert.That(sketch.TryHashAndStore("previous-filter-key"u8), Is.True);
            var previousHash = HashUtils.MurmurHash2x64A("previous-filter-key"u8);
            sketch.SetStatus(SketchStatus.MIGRATED);
            var previous = sketch.argSliceVector;
            var batch = reuseCollection ? previous : new ArgSliceVector();
            if (reuseCollection)
                batch.Clear();

            ReadOnlySpan<byte> key = "migration-filter-key"u8;
            ulong hash;
            if (namespaceLength == 0)
            {
                Assert.That(batch.TryAddItem(key), Is.True);
                hash = HashUtils.MurmurHash2x64A(key);
            }
            else
            {
                var ns = new byte[namespaceLength];
                ns[0] = 7;
                Assert.That(batch.TryAddItem(ns, key), Is.True);
                hash = HashUtils.MurmurHash2x64A(key, seed: (uint)VectorManager.ExtractContextFromNamespaces(ns));
            }

            Assert.That(sketch.ReplaceBatch(batch), Is.SameAs(previous));
            Assert.That(sketch.argSliceVector, Is.SameAs(batch));
            Assert.That(batch.Count, Is.EqualTo(1));
            if (!reuseCollection)
                Assert.That(previous.IsEmpty, Is.True);
            Assert.That(sketch.Probe(previousHash, out _), Is.False);
            AssertSketchMembership(sketch, hash, hashCount);
        }

        [TestCase(false)]
        [TestCase(true)]
        public void RetiredBatchAllowsAccessAndClearsMembershipBeforeReuse(bool reuseCollection)
        {
            var operations = CreateSlotOperations();
            var sketch = operations[0].sketch;
            var key = PinnedSpanByte.FromPinnedSpan("migration-access-key"u8);
            var hash = HashUtils.MurmurHash2x64A(key.ReadOnlySpan);
            Assert.That(sketch.TryHashAndStore(key.ReadOnlySpan), Is.True);
            sketch.SetStatus(SketchStatus.DELETING);
            Assert.That(session.CanAccessKey(key, 0, readOnly: false), Is.False);

            sketch.RetireBatch();

            Assert.That(sketch.argSliceVector.IsEmpty, Is.True);
            Assert.That(sketch.Probe(hash, out var status), Is.True, "Retirement retains the physical bitmap.");
            Assert.That(status, Is.EqualTo(SketchStatus.INITIALIZING));
            Assert.That(session.CanAccessKey(key, 0, readOnly: false), Is.True);
            Assert.That(session.CanAccessKey(key, 0, readOnly: true), Is.True);

            var batch = reuseCollection ? sketch.argSliceVector : new ArgSliceVector();
            Assert.That(batch.TryAddItem("next-filter-key"u8), Is.True);
            sketch.ReplaceBatch(batch);
            sketch.SetStatus(SketchStatus.DELETING);
            Assert.That(sketch.Probe(hash, out _), Is.False, "Rebuilding removes retired membership.");
            var nextHash = HashUtils.MurmurHash2x64A("next-filter-key"u8);
            Assert.That(sketch.Probe(nextHash, out status), Is.True);
            Assert.That(status, Is.EqualTo(SketchStatus.DELETING));
        }

        static void AssertSketchMembership(Sketch sketch, ulong hash, int hashCount = 2)
        {
            var hashBits = 48 / hashCount;
            var positions = Enumerable.Range(0, hashCount)
                .Select(i => (int)(hash >> (16 + i * hashBits)) & 63).ToArray();
            var missing = Enumerable.Range(0, 64).First(bit => !positions.Contains(bit));

            sketch.SetStatus(SketchStatus.DELETING);
            Assert.That(sketch.Probe(hash, out var status), Is.True);
            Assert.That(status, Is.EqualTo(SketchStatus.DELETING));
            for (var i = 0; i < hashCount; i++)
            {
                var shift = 16 + i * hashBits;
                var absent = (hash & ~(63UL << shift)) | ((ulong)missing << shift);
                Assert.That(sketch.Probe(absent, out status), Is.False);
                Assert.That(status, Is.EqualTo(SketchStatus.INITIALIZING));
            }

            sketch.Clear();
            Assert.That(sketch.Probe(hash, out status), Is.False);
            Assert.That(status, Is.EqualTo(SketchStatus.INITIALIZING));
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task KeysEpochBarrierReleasesAndRestoresCaller(bool cancel)
        {
            using var callerConnection = new TcpClient();
            using var readerConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var cancellation = new CancellationTokenSource();
            var caller = await CreateClusterSessionAsync(callerConnection, deadline.Token);
            var reader = await CreateClusterSessionAsync(readerConnection, deadline.Token, caller);
            Task<bool> barrier = null;
            try
            {
                caller.AcquireCurrentEpoch();
                reader.AcquireCurrentEpoch();
                barrier = caller.UnsafeBumpAndWaitForEpochTransitionAsync(cancellation.Token).AsTask();
                Assert.That(caller.LocalCurrentEpoch, Is.Zero);
                Assert.That(barrier.IsCompleted, Is.False);
                if (cancel)
                    cancellation.Cancel();
                else
                    reader.ReleaseCurrentEpoch();

                Assert.That(await barrier.WaitAsync(deadline.Token), Is.EqualTo(!cancel));
                Assert.That(caller.LocalCurrentEpoch, Is.EqualTo(provider.garnetEpoch.GetCurrentEpoch()));
            }
            finally
            {
                cancellation.Cancel();
                reader.ReleaseCurrentEpoch();
                try
                {
                    if (barrier != null)
                        await barrier.WaitAsync(TimeSpan.FromSeconds(5));
                }
                finally
                {
                    caller.ReleaseCurrentEpoch();
                }
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task KeysMigrationAbortsWhenBarrierCancelled(bool deleting)
        {
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var reader = await CreateClusterSessionAsync(connection, deadline.Token);
            var caller = new ClusterSession(provider, null, null, null, null, default, default, default, null);
            CreateKeysMigration(caller);
            var key = operation.sketch.Keys[0].Item1;
            Assert.That(operation.LocalSession.BasicGarnetApi.SET(key, PinnedSpanByte.FromPinnedSpan("value"u8)), Is.EqualTo(GarnetStatus.OK));
            operation.sketch.Keys[0] = (key, true);
            var cancellation = GetCancellationSource(session);
            Task<bool> migrate = null;
            try
            {
                caller.AcquireCurrentEpoch();
                reader.AcquireCurrentEpoch();
                migrate = deleting
                    ? (Task<bool>)typeof(MigrateSession).GetMethod("DeleteKeysAsync", BindingFlags.Instance | BindingFlags.NonPublic).Invoke(session, null)
                    : session.MigrateKeysAsync();
                Assert.That(() => operation.sketch.Status,
                    Is.EqualTo(deleting ? SketchStatus.DELETING : SketchStatus.TRANSMITTING).After(5_000, 10));
                Assert.That(() => provider.garnetEpoch.GetCurrentEpoch(),
                    Is.GreaterThan(reader.LocalCurrentEpoch).After(5_000, 10));
                Assert.That(migrate.IsCompleted, Is.False);
                cancellation.Cancel();

                Assert.That(await migrate.WaitAsync(deadline.Token), Is.False);
                Assert.That(reader.LocalCurrentEpoch, Is.Not.Zero, "Cancellation must not wait for the other reader to drain.");
                Assert.That(caller.LocalCurrentEpoch, Is.Not.Zero, "The command session must reacquire its epoch.");
                Assert.That(operation.LocalSession.BasicGarnetApi.EXISTS(key), Is.EqualTo(GarnetStatus.OK));
            }
            finally
            {
                cancellation.Cancel();
                reader.ReleaseCurrentEpoch();
                try
                {
                    if (migrate != null)
                        await migrate.WaitAsync(TimeSpan.FromSeconds(5));
                }
                finally
                {
                    caller.ReleaseCurrentEpoch();
                }
            }
        }

        [Test]
        public void KeysDeletionHonorsCancellationWhenBarrierIsAlreadyQuiesced()
        {
            var caller = new ClusterSession(provider, null, null, null, null, default, default, default, null);
            CreateKeysMigration(caller);
            var key = operation.sketch.Keys[0].Item1;
            Assert.That(operation.LocalSession.BasicGarnetApi.SET(key, PinnedSpanByte.FromPinnedSpan("value"u8)), Is.EqualTo(GarnetStatus.OK));
            operation.sketch.Keys[0] = (key, true);
            GetCancellationSource(session).Cancel();
            try
            {
                Assert.ThrowsAsync<OperationCanceledException>(() => (Task<bool>)typeof(MigrateSession)
                    .GetMethod("DeleteKeysAsync", BindingFlags.Instance | BindingFlags.NonPublic).Invoke(session, null));
                Assert.That(operation.LocalSession.BasicGarnetApi.EXISTS(key), Is.EqualTo(GarnetStatus.OK));
                Assert.That(caller.LocalCurrentEpoch, Is.Not.Zero);
            }
            finally
            {
                caller.ReleaseCurrentEpoch();
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task KeysMigrationReportsCancellationOrDisposalAsFailure(bool dispose)
        {
            CreateOperation(1);
            if (dispose)
                session.Dispose();
            else
                GetCancellationSource(session).Cancel();

            Assert.That(await session.MigrateKeysAsync().WaitAsync(TimeSpan.FromSeconds(5)), Is.False);
        }

#if DEBUG
        [TestCase(false)]
        [TestCase(true)]
        public async Task RangeIndexBatchAbortsWhenBarrierFails(bool deleting)
        {
            var operations = CreateSlotOperations();
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var reader = await CreateClusterSessionAsync(connection, deadline.Token);
            var cancellation = GetCancellationSource(session);
            const ExceptionInjectionType pause = ExceptionInjectionType.RangeIndex_Migration_After_Transmitting;
            Task<bool> migrate = null;
            try
            {
                if (deleting)
                    ExceptionInjectionHelper.EnableException(pause);
                else
                    reader.AcquireCurrentEpoch();

                // An empty batch isolates the two barriers without snapshot or network transfer work.
                migrate = (Task<bool>)typeof(MigrateSession)
                    .GetMethod("MigrateRangeIndexKeysAsync", BindingFlags.Instance | BindingFlags.NonPublic)
                    .Invoke(session, [operations[0], Array.Empty<byte[]>(), cancellation.Token]);
                if (deleting)
                {
                    Assert.That(() => ExceptionInjectionHelper.IsEnabled(pause), Is.False.After(5_000, 10));
                    reader.AcquireCurrentEpoch();
                    ExceptionInjectionHelper.EnableException(pause);
                }

                Assert.That(() => operations[0].sketch.Status,
                    Is.EqualTo(deleting ? SketchStatus.DELETING : SketchStatus.TRANSMITTING).After(5_000, 10));
                Assert.That(() => provider.garnetEpoch.GetCurrentEpoch(),
                    Is.GreaterThan(reader.LocalCurrentEpoch).After(5_000, 10));
                Assert.That(migrate.IsCompleted, Is.False);
                cancellation.Cancel();

                Assert.That(await migrate.WaitAsync(deadline.Token), Is.False);
                Assert.That(reader.LocalCurrentEpoch, Is.Not.Zero, "The reader has not drained.");
                Assert.That(operations[0].sketch.argSliceVector.IsEmpty, Is.True);
            }
            finally
            {
                ExceptionInjectionHelper.SuspendParking();
                ExceptionInjectionHelper.DisableException(pause);
                cancellation.Cancel();
                reader.ReleaseCurrentEpoch();
                try
                {
                    if (migrate != null)
                        await migrate.WaitAsync(deadline.Token);
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }
            }
        }

        [Test]
        public async Task RangeIndexBatchRejectsIncompleteProtection()
        {
            var operations = CreateSlotOperations();
            var keys = Enumerable.Range(0, 1124)
                .Select(index => Encoding.ASCII.GetBytes($"range-index-{index}"))
                .ToHashSet(ByteArrayComparer.Instance);
            using var injection = ExceptionInjectionHelper.EnabledScope(ExceptionInjectionType.RangeIndex_Migration_Before_Transmitting);
            var migrate = (Task<bool>)typeof(MigrateSession)
                .GetMethod("MigrateRangeIndexKeysAsync", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(session, [operations[0], keys, CancellationToken.None]);

            Assert.That(await migrate.WaitAsync(TimeSpan.FromSeconds(5)), Is.False);
            Assert.That(operations[0].sketch.argSliceVector.IsEmpty, Is.True);
            Assert.That(operations[0].sketch.Status, Is.EqualTo(SketchStatus.INITIALIZING));
        }
#endif

        [TestCase(SketchStatus.INITIALIZING, SketchStatus.DELETING, true, false)]
        [TestCase(SketchStatus.MIGRATED, SketchStatus.TRANSMITTING, false, false)]
        [TestCase(SketchStatus.TRANSMITTING, SketchStatus.DELETING, true, false)]
        [TestCase(SketchStatus.TRANSMITTING, SketchStatus.MIGRATED, true, true)]
        public void AccessChecksPreserveProtectionAcrossSketchSizes(SketchStatus first, SketchStatus second, bool readOnly, bool expected)
        {
            var operations = CreateSlotOperations();
            var key = PinnedSpanByte.FromPinnedSpan("migration-access-key"u8);
            operations[0].sketch.HashAndStore(key);
            operations[0].sketch.SetStatus(first);
            operations[1].sketch.HashAndStore(key);
            operations[1].sketch.SetStatus(second);

            Assert.That(session.CanAccessKey(key, 0, readOnly), Is.EqualTo(expected));
        }

        [TestCase(SketchStatus.TRANSMITTING)]
        [TestCase(SketchStatus.DELETING)]
        public void WriteDenialStopsBeforeLaterWorkers(SketchStatus state)
        {
            var operations = CreateSlotOperations();
            var key = PinnedSpanByte.FromPinnedSpan("migration-access-key"u8);
            operations[0].sketch.HashAndStore(key);
            operations[0].sketch.SetStatus(state);
            operations[1].sketch.HashAndStore(key);
            // An invalid trailing state makes an unnecessary probe observable.
            operations[1].sketch.SetStatus((SketchStatus)byte.MaxValue);

            Assert.That(session.CanAccessKey(key, 0, readOnly: false), Is.False);
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
            CreateOperation(3, copy, deleteBatchSize: deleteBatchSize);
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