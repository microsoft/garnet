// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;
using Tsavorite.test.recovery.sumstore;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>;

    public enum DeviceMode
    {
        Local,
        Cloud
    }

    public class RecoveryCheckBase
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
            DeleteDirectory(MethodTestDir);
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;

            for (long key = 0; key < 1000; key++)
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

            if (readCacheMode == ReadCacheMode.UseRC)
            {
                store1.Log.FlushAndEvict(true);
                for (long key = 0; key < 1000; key++)
                {
                    long output = default;
                    var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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

            using var s2 = store2.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}");
                ClassicAssert.AreEqual(key, output, $"output = {output}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

                if (readCacheMode == ReadCacheMode.UseRC)
                {
                    store1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                        if (status.IsPending)
                        {
                            Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }
                        ClassicAssert.IsTrue(status.Found, $"status = {status}");
                        ClassicAssert.AreEqual(key, output, $"output = {output}");
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

                ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress, $"iter {i}");
                ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress, $"iter {i}");
                AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: i);

                using var s2 = store2.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}");
                }
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryCheck2Repeated(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType
            )
        {
            Guid token = default;
            const long pageSize = 1L << 10;

            for (int i = 0; i < 6; i++)
            {
                using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MutableFraction = 1,
                    PageSize = pageSize,
                    LogMemorySize = 1L << 20,
                    CheckpointDir = MethodTestDir
                }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );

                if (i > 0)
                    _ = store.Recover(default, token);

                using var s1 = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc1 = s1.BasicContext;

                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

                var task = store.TakeHybridLogCheckpointAsync(checkpointType);
                bool success;
                (success, token) = task.AsTask().GetAwaiter().GetResult();
                ClassicAssert.IsTrue(success);

                using var s2 = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;

                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bc1 = s1.BasicContext;

            for (long key = 0; key < 1000; key++)
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

            var task = store.TakeHybridLogCheckpointAsync(checkpointType);
            (bool success, Guid token) = task.AsTask().GetAwaiter().GetResult();
            ClassicAssert.IsTrue(success);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}");
                ClassicAssert.AreEqual(key, output, $"output = {output}");
            }

            for (long key = 1000; key < 2000; key++)
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

            // Reset store to empty state
            store.Reset();

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.NotFound, $"status = {status}");
            }

            // Rollback to previous checkpoint
            _ = store.Recover(default, token);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}");
                ClassicAssert.AreEqual(key, output, $"output = {output}");
            }

            for (long key = 1000; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.NotFound, $"status = {status}");
            }

            for (long key = 1000; key < 2000; key++)
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}");
                ClassicAssert.AreEqual(key, output, $"output = {output}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

                if (readCacheMode == ReadCacheMode.UseRC)
                {
                    store1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                        if (status.IsPending)
                        {
                            Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }
                        ClassicAssert.IsTrue(status.Found, $"status = {status}");
                        ClassicAssert.AreEqual(key, output, $"output = {output}");
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

                ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress, $"iter {i}");
                ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress, $"iter {i}");
                AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: i);

                using var s2 = store2.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

                if (readCacheMode == ReadCacheMode.UseRC)
                {
                    store1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                        if (status.IsPending)
                        {
                            Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }
                        ClassicAssert.IsTrue(status.Found, $"status = {status}");
                        ClassicAssert.AreEqual(key, output, $"output = {output}");
                    }
                }

                if (i == 0)
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

                ClassicAssert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress, $"iter {i}");
                ClassicAssert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress, $"iter {i}");
                AssertEquivalentTailAddress(store1.Log.TailAddress, store2.Log.TailAddress, pageSize, iteration: i);

                using var s2 = store2.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;
            for (long key = 0; key < 1000; key++)
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

            if (useReadCache)
            {
                store1.Log.FlushAndEvict(true);
                for (long key = 0; key < 1000; key++)
                {
                    long output = default;
                    var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}");
                }
            }

            var result = await store1.GrowIndexAsync();
            ClassicAssert.IsTrue(result);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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

            using var s2 = store2.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}, key = {key}");
                ClassicAssert.AreEqual(key, output, $"output = {output}, key = {key}");
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

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        [Ignore("TODO DeltaLog")]
        public async ValueTask IncrSnapshotRecoveryCheck([Values] DeviceMode deviceMode)
        {
            DeviceLogCommitCheckpointManager checkpointManager;
            if (deviceMode == DeviceMode.Local)
            {
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(MethodTestDir + "/checkpoints/"));  // PurgeAll deletes this directory
            }
            else
            {
                IgnoreIfNotRunningAzureTests();
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    TestUtils.AzureStorageNamedDeviceFactoryCreator,
                    new AzureCheckpointNamingScheme($"{AzureTestContainer}/{AzureTestDirectory}"));
            }

            await IncrSnapshotRecoveryCheck(checkpointManager);
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }

        private async ValueTask IncrSnapshotRecoveryCheck(ICheckpointManager checkpointManager)
        {
            using var store1 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                LogMemorySize = 1L << 20,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, MyFunctions2>(new MyFunctions2());
            var bc1 = s1.BasicContext;
            for (long key = 0; key < 1000; key++)
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));

            var task = store1.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot);
            var (success, token) = await task;

            for (long key = 950; key < 1000; key++)
            {
                var value = key + 1;
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
            }

            var version1 = store1.CurrentVersion;
            var _result1 = store1.TryInitiateHybridLogCheckpoint(out var _token1, CheckpointType.Snapshot, tryIncremental: true);
            await store1.CompleteCheckpointAsync();

            ClassicAssert.IsTrue(_result1);
            ClassicAssert.AreEqual(token, _token1);

            for (long key = 1000; key < 2000; key++)
            {
                var value = key + 1;
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
            }

            var version2 = store1.CurrentVersion;
            var _result2 = store1.TryInitiateHybridLogCheckpoint(out var _token2, CheckpointType.Snapshot, true);
            await store1.CompleteCheckpointAsync();

            ClassicAssert.IsTrue(_result2);
            ClassicAssert.AreEqual(token, _token2);

            // Test that we can recover to latest version
            using var store2 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                LogMemorySize = 1L << 14,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            _ = await store2.RecoverAsync(default, _token2);

            ClassicAssert.AreEqual(store2.Log.TailAddress, store1.Log.TailAddress);

            using var s2 = store2.NewSession<long, long, Empty, MyFunctions2>(new MyFunctions2());
            var bc2 = s2.BasicContext;

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                MyFunctions2.Verify(status, key, output);
            }

            // Test that we can recover to earlier version
            using var store3 = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1L << 10,
                LogMemorySize = 1L << 14,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            _ = await store3.RecoverAsync(recoverTo: version1);

            ClassicAssert.IsTrue(store3.EntryCount == 1000);
            using var s3 = store3.NewSession<long, long, Empty, MyFunctions2>(new MyFunctions2());
            var bc3 = s3.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc3.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc3.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                MyFunctions2.Verify(status, key, output);
            }
        }
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

            ClientSession<long, long, Empty, MyFunctions, LongStoreFunctions, LongAllocator> session2;
            BasicContext<long, long, Empty, MyFunctions, LongStoreFunctions, LongAllocator> bc2;

            public SnapshotIterator(TsavoriteKV<LongStoreFunctions, LongAllocator> store2, long expectedCount)
            {
                this.store2 = store2;
                this.expectedCount = expectedCount;
            }

            public bool OnStart(Guid checkpointToken, long currentVersion, long nextVersion)
            {
                store2.SetVersion(nextVersion);
                session2 = store2.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
                bc2 = session2.BasicContext;
                return true;
            }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords)
                where TSourceLogRecord : ISourceLogRecord
            {
                _ = bc2.Upsert(logRecord.Key, logRecord.ValueSpan);
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;

            for (long key = 0; key < (reInsert ? 800 : 1000); key++)
            {
                // If reInsert, we insert the wrong value during the first pass for the first 500 keys
                long value = reInsert && key < 500 ? key + 1 : key;
                _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
            }

            if (reInsert)
            {
                store1.Log.FlushAndEvict(true);
                for (long key = 0; key < 500; key++)
                    _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));
                for (long key = 800; key < 1000; key++)
                    _ = bc1.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));
            }

            if (readCacheMode == ReadCacheMode.UseRC)
            {
                store1.Log.FlushAndEvict(true);
                for (long key = 0; key < 1000; key++)
                {
                    long output = default;
                    var status = bc1.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                    if (status.IsPending)
                    {
                        Assert.That(bc1.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"status = {status}");
                    ClassicAssert.AreEqual(key, output, $"output = {output}");
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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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
            using var s2 = store2.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc2.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                if (status.IsPending)
                {
                    Assert.That(bc2.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"status = {status}");
                ClassicAssert.AreEqual(key, output, $"output = {output}");
            }
        }
    }
}