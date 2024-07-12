// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.devices;
using Tsavorite.test.recovery.sumstore;

namespace Tsavorite.test.recovery
{
    using LongStoreFunctions = StoreFunctions<long, long, LongKeyComparer, NoSerializer<long>, NoSerializer<long>, DefaultRecordDisposer<long, long>>;
    using LongAllocator = BlittableAllocator<long, long, StoreFunctions<long, long, LongKeyComparer, NoSerializer<long>, NoSerializer<long>, DefaultRecordDisposer<long, long>>>;

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
            {
                inputArray[i].adId = i;
            }

            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "hlog.log"), deleteOnClose: false);
            TestUtils.RecreateDirectory(TestUtils.MethodTestDir);
        }

        protected void BaseTearDown()
        {
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        public class MyFunctions : SimpleSimpleFunctions<long, long>
        {
            public override void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found, $"status = {status}");
                Assert.AreEqual(key, output, $"output = {output}");
            }
        }

        public class MyFunctions2 : SimpleSimpleFunctions<long, long>
        {
            public override void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Verify(status, key, output);
            }

            internal static void Verify(Status status, long key, long output)
            {
                Assert.IsTrue(status.Found);
                if (key < 950)
                    Assert.AreEqual(key, output);
                else
                    Assert.AreEqual(key + 1, output);
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

        public async ValueTask RecoveryCheck1([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(1 << 13, 1 << 16)] int indexSize)
        {
            using var store1 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;

            for (long key = 0; key < 1000; key++)
            {
                _ = bc1.Upsert(ref key, ref key);
            }

            if (useReadCache)
            {
                store1.Log.FlushAndEvict(true);
                for (long key = 0; key < 1000; key++)
                {
                    long output = default;
                    var status = bc1.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                _ = bc1.CompletePending(true);
            }

            var task = store1.TakeFullCheckpointAsync(checkpointType);

            using var store2 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = indexSize,
                LogDevice = log,
                MutableFraction = 1,
                PageSize = 1 << 10,
                MemorySize = 1 << 20,
                ReadCacheEnabled = useReadCache,
                CheckpointDir = TestUtils.MethodTestDir
            }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
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

            Assert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress);
            Assert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress);
            Assert.AreEqual(store1.Log.TailAddress, store2.Log.TailAddress);

            using var s2 = store2.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc2.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            _ = bc2.CompletePending(true);
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
        public async ValueTask RecoveryCheck2([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(1 << 13, 1 << 16)] int indexSize)
        {
            using var store1 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bc1 = s1.BasicContext;

            using var store2 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    _ = bc1.Upsert(ref key, ref key);
                }

                if (useReadCache)
                {
                    store1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = bc1.Read(ref key, ref output);
                        if (!status.IsPending)
                        {
                            Assert.IsTrue(status.Found, $"status = {status}");
                            Assert.AreEqual(key, output, $"output = {output}");
                        }
                    }
                    _ = bc1.CompletePending(true);
                }

                var task = store1.TakeHybridLogCheckpointAsync(checkpointType);

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

                Assert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress);
                Assert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress);
                Assert.AreEqual(store1.Log.TailAddress, store2.Log.TailAddress);

                using var s2 = store2.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                _ = bc2.CompletePending(true);
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryCheck2Repeated([Values] CheckpointType checkpointType)
        {
            Guid token = default;

            for (int i = 0; i < 6; i++)
            {
                using var store = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                    {
                        IndexSize = 1 << 13,
                        LogDevice = log,
                        MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                        CheckpointDir = TestUtils.MethodTestDir
                    }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );

                if (i > 0)
                    _ = store.Recover(default, token);

                using var s1 = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
                var bc1 = s1.BasicContext;

                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    _ = bc1.Upsert(ref key, ref key);
                }

                var task = store.TakeHybridLogCheckpointAsync(checkpointType);
                bool success;
                (success, token) = task.AsTask().GetAwaiter().GetResult();
                Assert.IsTrue(success);

                using var s2 = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
                var bc2 = s2.BasicContext;

                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                _ = bc2.CompletePending(true);
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void RecoveryRollback([Values] CheckpointType checkpointType)
        {
            using var store = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = 1 << 13,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 11, SegmentSize = 1 << 11,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bc1 = s1.BasicContext;

            for (long key = 0; key < 1000; key++)
            {
                _ = bc1.Upsert(ref key, ref key);
            }

            var task = store.TakeHybridLogCheckpointAsync(checkpointType);
            (bool success, Guid token) = task.AsTask().GetAwaiter().GetResult();
            Assert.IsTrue(success);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            _ = bc1.CompletePendingWithOutputs(out var completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.Found);
                Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
            }
            completedOutputs.Dispose();

            for (long key = 1000; key < 2000; key++)
            {
                _ = bc1.Upsert(ref key, ref key);
            }

            // Reset store to empty state
            store.Reset();

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.NotFound, $"status = {status}");
                }
            }
            _ = bc1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.NotFound);
            }
            completedOutputs.Dispose();

            // Rollback to previous checkpoint
            _ = store.Recover(default, token);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            _ = bc1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.Found);
                Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
            }
            completedOutputs.Dispose();

            for (long key = 1000; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.NotFound, $"status = {status}");
                }
            }
            _ = bc1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.NotFound);
            }
            completedOutputs.Dispose();

            for (long key = 1000; key < 2000; key++)
            {
                _ = bc1.Upsert(ref key, ref key);
            }

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
                else
                {
                    _ = bc1.CompletePendingWithOutputs(out completedOutputs, true);
                    while (completedOutputs.Next())
                    {
                        Assert.IsTrue(completedOutputs.Current.Status.Found);
                        Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
                    }
                    completedOutputs.Dispose();
                }
            }
            _ = bc1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.Found);
                Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
            }
            completedOutputs.Dispose();
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
        public async ValueTask RecoveryCheck3([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(1 << 13, 1 << 16)] int indexSize)
        {
            using var store1 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bc1 = s1.BasicContext;

            using var store2 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    _ = bc1.Upsert(ref key, ref key);
                }

                if (useReadCache)
                {
                    store1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = bc1.Read(ref key, ref output);
                        if (!status.IsPending)
                        {
                            Assert.IsTrue(status.Found, $"status = {status}");
                            Assert.AreEqual(key, output, $"output = {output}");
                        }
                    }
                    _ = bc1.CompletePending(true);
                }

                var task = store1.TakeFullCheckpointAsync(checkpointType);

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

                Assert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress);
                Assert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress);
                Assert.AreEqual(store1.Log.TailAddress, store2.Log.TailAddress);

                using var s2 = store2.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                _ = bc2.CompletePending(true);
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
        public async ValueTask RecoveryCheck4([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(1 << 13, 1 << 16)] int indexSize)
        {
            using var store1 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bc1 = s1.BasicContext;

            using var store2 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    _ = bc1.Upsert(ref key, ref key);
                }

                if (useReadCache)
                {
                    store1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = bc1.Read(ref key, ref output);
                        if (!status.IsPending)
                        {
                            Assert.IsTrue(status.Found, $"status = {status}");
                            Assert.AreEqual(key, output, $"output = {output}");
                        }
                    }
                    _ = bc1.CompletePending(true);
                }

                if (i == 0)
                    _ = store1.TakeIndexCheckpointAsync().AsTask().GetAwaiter().GetResult();

                var task = store1.TakeHybridLogCheckpointAsync(checkpointType);

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

                Assert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress);
                Assert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress);
                Assert.AreEqual(store1.Log.TailAddress, store2.Log.TailAddress);

                using var s2 = store2.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
                var bc2 = s2.BasicContext;
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = bc2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                _ = bc2.CompletePending(true);
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
        public async ValueTask RecoveryCheck5([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(1 << 13, 1 << 16)] int indexSize)
        {
            using var store1 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc1 = s1.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                _ = bc1.Upsert(ref key, ref key);
            }

            if (useReadCache)
            {
                store1.Log.FlushAndEvict(true);
                for (long key = 0; key < 1000; key++)
                {
                    long output = default;
                    var status = bc1.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                _ = bc1.CompletePending(true);
            }

            _ = store1.GrowIndex();

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            _ = bc1.CompletePending(true);

            var task = store1.TakeFullCheckpointAsync(checkpointType);

            using var store2 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = indexSize,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    ReadCacheEnabled = useReadCache,
                    CheckpointDir = TestUtils.MethodTestDir
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
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

            Assert.AreEqual(store1.Log.HeadAddress, store2.Log.HeadAddress);
            Assert.AreEqual(store1.Log.ReadOnlyAddress, store2.Log.ReadOnlyAddress);
            Assert.AreEqual(store1.Log.TailAddress, store2.Log.TailAddress);

            using var s2 = store2.NewSession<long, long, Empty, MyFunctions>(new MyFunctions());
            var bc2 = s2.BasicContext;

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc2.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            _ = bc2.CompletePending(true);
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
        public async ValueTask IncrSnapshotRecoveryCheck([Values] DeviceMode deviceMode)
        {
            ICheckpointManager checkpointManager;
            if (deviceMode == DeviceMode.Local)
            {
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir + "/checkpoints/"));  // PurgeAll deletes this directory
            }
            else
            {
                TestUtils.IgnoreIfNotRunningAzureTests();
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                    new AzureCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            }

            await IncrSnapshotRecoveryCheck(checkpointManager);
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }

        private async ValueTask IncrSnapshotRecoveryCheck(ICheckpointManager checkpointManager)
        {
            using var store1 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = 1 << 16,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 20,
                    CheckpointManager = checkpointManager
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var s1 = store1.NewSession<long, long, Empty, MyFunctions2>(new MyFunctions2());
            var bc1 = s1.BasicContext;
            for (long key = 0; key < 1000; key++)
                _ = bc1.Upsert(ref key, ref key);

            var task = store1.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot);
            var (success, token) = await task;

            for (long key = 950; key < 1000; key++)
                _ = bc1.Upsert(key, key + 1);

            var version1 = store1.CurrentVersion;
            var _result1 = store1.TryInitiateHybridLogCheckpoint(out var _token1, CheckpointType.Snapshot, true);
            await store1.CompleteCheckpointAsync();

            Assert.IsTrue(_result1);
            Assert.AreEqual(token, _token1);

            for (long key = 1000; key < 2000; key++)
                _ = bc1.Upsert(key, key + 1);

            var version2 = store1.CurrentVersion;
            var _result2 = store1.TryInitiateHybridLogCheckpoint(out var _token2, CheckpointType.Snapshot, true);
            await store1.CompleteCheckpointAsync();

            Assert.IsTrue(_result2);
            Assert.AreEqual(token, _token2);

            // Test that we can recover to latest version
            using var store2 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = 1 << 16,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 14,
                    CheckpointManager = checkpointManager
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            await store2.RecoverAsync(default, _token2);

            Assert.AreEqual(store2.Log.TailAddress, store1.Log.TailAddress);

            using var s2 = store2.NewSession<long, long, Empty, MyFunctions2>(new MyFunctions2());
            var bc2 = s2.BasicContext;

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = bc2.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    MyFunctions2.Verify(status, key, output);
                }
            }
            _ = bc2.CompletePending(true);

            // Test that we can recover to earlier version
            using var store3 = new TsavoriteKV<long, long, LongStoreFunctions, LongAllocator>(new()
                {
                    IndexSize = 1 << 16,
                    LogDevice = log,
                    MutableFraction = 1, PageSize = 1 << 10, MemorySize = 1 << 14,
                    CheckpointManager = checkpointManager
                }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            _ = await store3.RecoverAsync(recoverTo: version1);

            Assert.IsTrue(store3.EntryCount == 1000);
            using var s3 = store3.NewSession<long, long, Empty, MyFunctions2>(new MyFunctions2());
            var bc3 = s3.BasicContext;
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = bc3.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    MyFunctions2.Verify(status, key, output);
                }
            }
            _ = bc3.CompletePending(true);
        }
    }
}