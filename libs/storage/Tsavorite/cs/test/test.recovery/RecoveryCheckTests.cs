// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.test.recovery.sumstore;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    public enum DeviceMode
    {
        Local,
        Cloud
    }

    public class RecoveryCheckBase : TestBase
    {
        protected IDevice log;
        protected const int NumOps = 5000;
        protected AdId[] inputArray;

        protected void BaseSetup()
        {
            inputArray = new AdId[NumOps];
            for (int i = 0; i < NumOps; i++)
                inputArray[i].adId = i;

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: false);
            RecreateDirectory(MethodTestDir);
        }

        protected void BaseTearDown()
        {
            log?.Dispose();
            log = null;
            TestUtils.OnTearDown();
        }

        protected static void AssertEquivalentTailAddress(long tailAddress1, long tailAddress2, long pageSize, int iteration)
        {
            if (tailAddress1 != tailAddress2)
            {
                // We adjust TailAddress in recovery to start at PageHeader.Size offset within the page if it ended on a page boundary
                // in the RecoveryInfo, so test for that case here.
                Assert.That(tailAddress1 / pageSize == tailAddress2 / pageSize && tailAddress2 % pageSize == PageHeader.Size, Is.True,
                    $"iteration {iteration}: tailAddress1 != tailAddress2 even after adjusting for PageHeader.Size offset");
            }
        }

        public class MyFunctions : SimpleLongSimpleFunctions
        {
            public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref long input, ref long output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.IsTrue(status.Found, $"status = {status}");
                ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<long>(), output, $"output = {output}");
            }
        }

        public class MyFunctions2 : SimpleLongSimpleFunctions
        {
            public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref long input, ref long output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Verify(status, diskLogRecord.Key.AsRef<long>(), output);
            }

            internal static void Verify(Status status, long key, long output)
            {
                ClassicAssert.IsTrue(status.Found);
                if (key < 950)
                    ClassicAssert.AreEqual(key, output);
                else
                    ClassicAssert.AreEqual(key + 1, output);
            }
        }
    }
    [TestFixture]
    public class RecoveryCheck1Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask RecoveryCheck1(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] CompletionSyncMode completionSyncMode, [Values] ReadCacheMode readCacheMode, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            const long pageSize = 1L << 10;
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;

            // Local variables in an async function can be moved, so we must use an array for the key
            var keyArray = new byte[sizeof(long)];

            for (long key = 0; key < 1000; key++)
            {
                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();
                keyLong = key;

                _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
            }

            if (readCacheMode == ReadCacheMode.UseRC)
            {
                store1.Log.FlushAndEvict(true);

                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();

                for (long key = 0; key < 1000; key++)
                {
                    keyLong = key;
                    long output = default;

                    var status = bc1.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                    var wasPending = status.IsPending;
                    if (wasPending)
                    {
                        Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}");
                }
            }

            var task = store1.TakeFullCheckpointAsync(checkpointType);

            using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            if (completionSyncMode == CompletionSyncMode.Async)
            {
                var (status, token) = await task;
                _ = await store2.RecoverAsync(default, token);
            }
            else
            {
                var (status, token) = task.AsTask().GetAwaiter().GetResult();
                _ = store2.Recover(default, token);
            }

            ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress);
            ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress);
            AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: 0);

            using var s2 = store2.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();
                keyLong = key;

                long output = default;
                var status = bc2.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}");
            }
        }
    }
    [TestFixture]
    public class RecoveryCheck2Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        //[Repeat(3000)]
        public async ValueTask RecoveryCheck2(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] CompletionSyncMode completionSyncMode, [Values] ReadCacheMode readCacheMode, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            const long pageSize = 1L << 10;
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bc1 = s1.BasicContext;

            using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            // Local variables in an async function can be moved, so we must use an array for the key
            var keyArray = new byte[sizeof(long)];

            for (int iter = 0; iter < 5; iter++)
            {
                for (long key = 1000 * iter; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                }

                if (readCacheMode == ReadCacheMode.UseRC)
                {
                    store1.Log.FlushAndEvict(true);

                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();

                    for (long key = 1000 * iter; key < 1000 * iter + 1000; key++)
                    {
                        keyLong = key;
                        long output = default;
                        var status = bc1.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                        var wasPending = status.IsPending;
                        if (wasPending)
                        {
                            Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }
                        ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                        ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                    }
                }

                var task = store1.TakeHybridLogCheckpointAsync(checkpointType);

                if (completionSyncMode == CompletionSyncMode.Async)
                {
                    var (status, token) = await task;
                    _ = await store2.RecoverAsync(default, token);
                }
                else
                {
                    var (status, token) = task.AsTask().GetAwaiter().GetResult();
                    _ = store2.Recover(default, token);
                }

                ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress, $"iter {iter}");
                ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress, $"iter {iter}");
                AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: iter);

                using var s2 = store2.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    long output = default;
                    var status = bc2.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                    var wasPending = status.IsPending;
                    if (wasPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                }
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryCheck2Repeated([Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
        {
            Guid token = default;
            const long pageSize = 1L << 10;

            // Local variables in an async function can be moved, so we must use an array for the key
            var keyArray = new byte[sizeof(long)];

            for (int iter = 0; iter < 6; iter++)
            {
                using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MutableFraction = 1,
                    PageSize = pageSize,
                    LogMemorySize = 1L << 20,
                    CheckpointDir = MethodTestDir
                }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );

                if (iter > 0)
                    _ = store.Recover(default, token);

                using var s1 = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc1 = s1.BasicContext;

                for (long key = 1000 * iter; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                }

                var task = store.TakeHybridLogCheckpointAsync(checkpointType);
                bool success;
                (success, token) = task.AsTask().GetAwaiter().GetResult();
                ClassicAssert.IsTrue(success);

                using var s2 = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;

                for (long key = 0; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    long output = default;
                    var status = bc2.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                    var wasPending = status.IsPending;
                    if (wasPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                }
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryRollback([Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType)
        {
            const long pageSize = 1L << 10;
            using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 11,
                SegmentSize = 1L << 11,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bc1 = s1.BasicContext;

            for (long key = 0; key < 1000; key++)
                _ = bc1.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref key));

            var task = store.TakeHybridLogCheckpointAsync(checkpointType);
            (bool success, Guid token) = task.AsTask().GetAwaiter().GetResult();
            ClassicAssert.IsTrue(success);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}");
            }

            for (long key = 1000; key < 2000; key++)
                _ = bc1.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref key));

            // Reset store to empty state
            store.Reset();

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.NotFound, $"status = {status}, key = {key}, wasPending = {wasPending}");
            }

            // Rollback to previous checkpoint
            _ = store.Recover(default, token);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}");
            }

            for (long key = 1000; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.NotFound, $"status = {status}, key = {key}, wasPending = {wasPending}");
            }

            for (long key = 1000; key < 2000; key++)
                _ = bc1.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref key));

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}");
            }
        }
    }
    [TestFixture]
    public class RecoveryCheck3Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask RecoveryCheck3(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] CompletionSyncMode completionSyncMode, [Values] ReadCacheMode readCacheMode, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            const long pageSize = 1L << 10;
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bc1 = s1.BasicContext;

            using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            // Local variables in an async function can be moved, so we must use an array for the key
            var keyArray = new byte[sizeof(long)];

            for (int iter = 0; iter < 5; iter++)
            {
                for (long key = 1000 * iter; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                }

                if (readCacheMode == ReadCacheMode.UseRC)
                {
                    store1.Log.FlushAndEvict(true);

                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();

                    for (long key = 1000 * iter; key < 1000 * iter + 1000; key++)
                    {
                        keyLong = key;
                        long output = default;
                        var status = bc1.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                        var wasPending = status.IsPending;
                        if (wasPending)
                        {
                            Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }
                        ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                        ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                    }
                }

                var task = store1.TakeFullCheckpointAsync(checkpointType);

                if (completionSyncMode == CompletionSyncMode.Async)
                {
                    var (status, token) = await task;
                    _ = await store2.RecoverAsync(default, token);
                }
                else
                {
                    var (status, token) = task.AsTask().GetAwaiter().GetResult();
                    _ = store2.Recover(default, token);
                }

                ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress, $"iter {iter}");
                ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress, $"iter {iter}");
                AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: iter);

                using var s2 = store2.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    long output = default;
                    // Local variables in an async function can be moved, so we must copy the key
                    var status = bc2.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                    var wasPending = status.IsPending;
                    if (wasPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                }
            }
        }
    }
    [TestFixture]
    public class RecoveryCheck4Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask RecoveryCheck4(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] CompletionSyncMode completionSyncMode, [Values] ReadCacheMode readCacheMode, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            const long pageSize = 1L << 10;
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bc1 = s1.BasicContext;

            using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            // Local variables in an async function can be moved, so we must use an array for the key
            var keyArray = new byte[sizeof(long)];

            for (int iter = 0; iter < 5; iter++)
            {
                for (long key = 1000 * iter; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                }

                if (readCacheMode == ReadCacheMode.UseRC)
                {
                    store1.Log.FlushAndEvict(true);

                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();

                    for (long key = 1000 * iter; key < 1000 * iter + 1000; key++)
                    {
                        keyLong = key;
                        long output = default;
                        var status = bc1.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                        var wasPending = status.IsPending;
                        if (wasPending)
                        {
                            Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }
                        ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                        ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                    }
                }

                if (iter == 0)
                    _ = store1.TakeIndexCheckpointAsync().AsTask().GetAwaiter().GetResult();
                var task = store1.TakeHybridLogCheckpointAsync(checkpointType);

                if (completionSyncMode == CompletionSyncMode.Async)
                {
                    var (status, token) = await task;
                    _ = await store2.RecoverAsync(default, token);
                }
                else
                {
                    var (status, token) = task.AsTask().GetAwaiter().GetResult();
                    _ = store2.Recover(default, token);
                }

                ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress, $"iter {iter}");
                ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress, $"iter {iter}");
                AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: iter);

                using var s2 = store2.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * iter + 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    long output = default;
                    var status = bc2.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                    var wasPending = status.IsPending;
                    if (wasPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, key = {key}, wasPending = {wasPending}, iter = {iter}");
                }
            }
        }
    }
    [TestFixture]
    public class RecoveryCheck5Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryCheck5(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] bool isAsync, [Values] bool useReadCache, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            const long pageSize = 1L << 10;
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = useReadCache,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;

            // Local variables in an async function can be moved, so we must use an array for the key
            var keyArray = new byte[sizeof(long)];

            for (long key = 0; key < 1000; key++)
            {
                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();
                keyLong = key;

                _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
            }

            if (useReadCache)
            {
                store1.Log.FlushAndEvict(true);

                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();

                for (long key = 0; key < 1000; key++)
                {
                    keyLong = key;

                    long output = default;
                    var status = bc1.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                    var wasPending = status.IsPending;
                    if (wasPending)
                    {
                        Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, key = {key}, wasPending = {wasPending}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, key = {key}, wasPending = {wasPending}");
                }
            }

            var result = await store1.GrowIndexAsync();
            ClassicAssert.IsTrue(result);

            for (long key = 0; key < 1000; key++)
            {
                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();
                keyLong = key;

                long output = default;
                var status = bc1.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, key = {key}, wasPending = {wasPending}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, key = {key}, wasPending = {wasPending}");
            }

            var task = store1.TakeFullCheckpointAsync(checkpointType);

            using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = pageSize,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = useReadCache,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            if (isAsync)
            {
                var (status, token) = await task;
                _ = await store2.RecoverAsync(default, token);
            }
            else
            {
                var (status, token) = task.AsTask().GetAwaiter().GetResult();
                _ = store2.Recover(default, token);
            }

            ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress);
            ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress);
            AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: 0);

            using var s2 = store2.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;

            for (long key = 0; key < 1000; key++)
            {
                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();
                keyLong = key;

                long output = default;
                var status = bc2.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, key = {key}, wasPending = {wasPending}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, key = {key}, wasPending = {wasPending}");
            }
        }
    }
    [TestFixture]
    public class RecoveryCheckSnapshotTests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();
    }
    [TestFixture]
    public class RecoveryCheckStreamingSnapshotTests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        public class SnapshotIterator : IStreamingSnapshotIteratorFunctions
        {
            readonly TsavoriteKV<LongStoreFunctions, LongAllocator> store2;
            readonly long expectedCount;

            ClientSession<TestSpanByteKey, long, long, Empty, MyFunctions, LongStoreFunctions, LongAllocator> session2;
            BasicContext<TestSpanByteKey, long, long, Empty, MyFunctions, LongStoreFunctions, LongAllocator> bc2;

            public SnapshotIterator(TsavoriteKV<LongStoreFunctions, LongAllocator> store2, long expectedCount)
            {
                this.store2 = store2;
                this.expectedCount = expectedCount;
            }

            public bool OnStart(Guid checkpointToken, long currentVersion, long nextVersion)
            {
                store2.SetVersion(nextVersion);
                session2 = store2.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions());
                bc2 = session2.BasicContext;
                return true;
            }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords)
                where TSourceLogRecord : ISourceLogRecord
            {
                _ = bc2.Upsert(TestSpanByteKey.FromPinnedSpan(logRecord.Key), logRecord.ValueSpan);
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords)
                => Assert.Fail(exception.Message);

            public void OnStop(bool completed, long numberOfRecords)
            {
                Assert.That(numberOfRecords, Is.EqualTo(expectedCount));
                session2.Dispose();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask StreamingSnapshotBasicTest([Values] CompletionSyncMode completionSyncMode, [Values] ReadCacheMode readCacheMode,
                [Values] bool reInsert, [Values(1L << 13, 1L << 16)] long indexSize)
        {
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;

            // Local variables in an async function can be moved, so we must use an array for the key and value
            var keyArray = new byte[sizeof(long)];
            var valueArray = new byte[sizeof(long)];

            for (long key = 0; key < (reInsert ? 800 : 1000); key++)
            {
                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();
                var valueSpan = new Span<byte>(valueArray);
                ref var valueLong = ref valueSpan.AsRef<long>();
                keyLong = key;
                // If reInsert, we insert the wrong value during the first pass for the first 500 keys
                valueLong = reInsert && key < 500 ? key + 1 : key;

                _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), valueSpan);
            }

            if (reInsert)
            {
                store1.Log.FlushAndEvict(true);
                for (long key = 0; key < 500; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                }
                for (long key = 800; key < 1000; key++)
                {
                    var keySpan = new Span<byte>(keyArray);
                    ref var keyLong = ref keySpan.AsRef<long>();
                    keyLong = key;

                    _ = bc1.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                }
            }

            if (readCacheMode == ReadCacheMode.UseRC)
            {
                store1.Log.FlushAndEvict(true);

                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();

                for (long key = 0; key < 1000; key++)
                {
                    keyLong = key;
                    long output = default;
                    // Local variables in an async function can be moved, so we must copy the key
                    var status = bc1.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                    var wasPending = status.IsPending;
                    if (wasPending)
                    {
                        Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}");
                }
            }

            // First create the new store, we will insert into this store as part of the iterator functions on the old store
            using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                LogMemorySize = 1L << 20,
                ReadCacheEnabled = readCacheMode == ReadCacheMode.UseRC,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            // Take a streaming snapshot checkpoint of the old store
            var iterator = new SnapshotIterator(store2, 1000);
            var task = store1.TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: iterator);
            if (completionSyncMode == CompletionSyncMode.Async)
                _ = await task;
            else
                _ = task.AsTask().GetAwaiter().GetResult();

            // Verify that the new store has all the records
            using var s2 = store2.NewSession<TestSpanByteKey, long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                var keySpan = new Span<byte>(keyArray);
                ref var keyLong = ref keySpan.AsRef<long>();
                keyLong = key;

                long output = default;
                var status = bc2.Read(TestSpanByteKey.FromArray(keyArray), ref output);
                var wasPending = status.IsPending;
                if (wasPending)
                {
                    Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}, wasPending = {wasPending}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}, wasPending = {wasPending}");
            }
        }
    }
}