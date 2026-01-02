// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.sumstore
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    using SpanByteStoreFunctions = StoreFunctions<AdId.Comparer, SpanByteRecordDisposer>;

    using StructAllocator = SpanByteAllocator<StoreFunctions<AdId.Comparer, SpanByteRecordDisposer>>;
    using StructStoreFunctions = StoreFunctions<AdId.Comparer, SpanByteRecordDisposer>;

    [TestFixture]
    internal class DeviceTypeRecoveryTests
    {
        internal const long NumUniqueKeys = 1L << 12;
        internal const long KeySpace = 1L << 20;
        internal const long NumOps = 1L << 17;
        internal const long CompletePendingInterval = 1L << 10;
        internal const long CheckpointInterval = 1L << 14;

        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private readonly List<Guid> logTokens = [];
        private readonly List<Guid> indexTokens = [];
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            // Only clean these in the initial Setup, as tests use the other Setup() overload to recover
            logTokens.Clear();
            indexTokens.Clear();
            DeleteDirectory(MethodTestDir, true);
        }

        private void Setup(TestDeviceType deviceType)
        {
            log = CreateTestDevice(deviceType, Path.Join(MethodTestDir, "Test.log"));
            store = new(new()
            {
                IndexSize = KeySpace,
                LogDevice = log,
                SegmentSize = 1L << 25, //MemorySize = 1L << 14, PageSize = 1L << 9,  // locks ups at session.RMW line in Populate() for Local Memory
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        private void TearDown(bool deleteDir)
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;

            // Do NOT clean up here unless specified, as tests use this TearDown() to prepare for recovery
            if (deleteDir)
                DeleteDirectory(MethodTestDir);
        }

        private void PrepareToRecover(TestDeviceType deviceType)
        {
            TearDown(deleteDir: false);
            Setup(deviceType);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestSeparateCheckpoint([Values] CompletionSyncMode syncMode, [Values] TestDeviceType deviceType)
        {
            Setup(deviceType);
            Populate(SeparateCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                if (i >= indexTokens.Count) break;
                PrepareToRecover(deviceType);
                await RecoverAndTest(i, syncMode == CompletionSyncMode.Async);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask RecoveryTestFullCheckpoint([Values] CompletionSyncMode syncMode, [Values] TestDeviceType deviceType)
        {
            Setup(deviceType);
            Populate(FullCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                PrepareToRecover(deviceType);
                await RecoverAndTest(i, syncMode == CompletionSyncMode.Async);
            }
        }

        private void FullCheckpointAction(int opNum)
        {
            if ((opNum + 1) % CheckpointInterval == 0)
            {
                Guid token;
                while (!store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot)) { }
                logTokens.Add(token);
                indexTokens.Add(token);
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
        }

        private void SeparateCheckpointAction(int opNum)
        {
            if ((opNum + 1) % CheckpointInterval != 0)
                return;

            var checkpointNum = (opNum + 1) / CheckpointInterval;
            Guid token;
            if (checkpointNum % 2 == 1)
            {
                while (!store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot)) { }
                logTokens.Add(token);
            }
            else
            {
                while (!store.TryInitiateIndexCheckpoint(out token)) { }
                indexTokens.Add(token);
            }
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
        }

        private void Populate(Action<int> checkpointAction)
        {
            // Prepare the dataset
            var inputArray = GC.AllocateArray<AdInput>((int)NumOps, pinned: true);
            for (int i = 0; i < NumOps; i++)
            {
                inputArray[i].adId.adId = i % NumUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            // Register thread with Tsavorite
            using var session = store.NewSession<AdInput, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Process the batch of input data
            for (int i = 0; i < NumOps; i++)
            {
                _ = bContext.RMW(SpanByte.FromPinnedVariable(ref inputArray[i].adId), ref inputArray[i], Empty.Default);

                checkpointAction(i);

                if (i % CompletePendingInterval == 0)
                    _ = bContext.CompletePending(false);
            }

            // Make sure operations are completed
            _ = bContext.CompletePending(true);
        }

        private async ValueTask RecoverAndTest(int tokenIndex, bool isAsync)
        {
            var logToken = logTokens[tokenIndex];
            var indexToken = indexTokens[tokenIndex];

            // Recover
            if (isAsync)
                _ = await store.RecoverAsync(indexToken, logToken);
            else
                _ = store.Recover(indexToken, logToken);

            // Create array for reading
            var inputArray = GC.AllocateArray<AdInput>((int)NumUniqueKeys, pinned: true);
            for (int i = 0; i < NumUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i;
                inputArray[i].numClicks.numClicks = 0;
            }

            // Register with thread
            using var session = store.NewSession<AdInput, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            AdInput input = default;
            Output output = default;

            // Issue read requests
            for (var i = 0; i < NumUniqueKeys; i++)
            {
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref inputArray[i].adId), ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found, $"At tokenIndex {tokenIndex}, keyIndex {i}, AdId {inputArray[i].adId.adId}");
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            _ = bContext.CompletePending(true);
        }
    }

    [TestFixture]
    internal class AllocatorTypeRecoveryTests
    {
        const int StackAllocMax = 12;
        const int RandSeed = 101;
        const long ExpectedValueBase = DeviceTypeRecoveryTests.NumUniqueKeys * (DeviceTypeRecoveryTests.NumOps / DeviceTypeRecoveryTests.NumUniqueKeys - 1);
        private static long ExpectedValue(int key) => ExpectedValueBase + key;

        private IDisposable storeDisp;
        private Guid logToken;
        private Guid indexToken;
        private IDevice log;
        private IDevice objlog;
        private bool smallSector;

        [SetUp]
        public void Setup()
        {
            smallSector = false;

            // Only clean these in the initial Setup, as tests use the other Setup() overload to recover
            logToken = Guid.Empty;
            indexToken = Guid.Empty;
            DeleteDirectory(MethodTestDir, true);
        }

        private TsavoriteKV<TStoreFunctions, TAllocator> Setup<TStoreFunctions, TAllocator>(AllocatorType allocatorType, Func<TStoreFunctions> storeFunctionsCreator, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorCreator)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var kvSettings = new KVSettings()
            {
                IndexSize = DeviceTypeRecoveryTests.KeySpace,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 25,
                ObjectLogSegmentSize = 1L << 27,
                CheckpointDir = MethodTestDir
            };

            log = new LocalMemoryDevice(kvSettings.SegmentSize * 4, 1L << 22, 2, sector_size: smallSector ? 64 : (uint)512, fileName: Path.Join(MethodTestDir, $"{allocatorType}.log"));
            objlog = allocatorType == AllocatorType.Object
                ? new LocalMemoryDevice(capacity: kvSettings.ObjectLogSegmentSize * 4, 1L << 22, 2, fileName: Path.Join(MethodTestDir, $"{allocatorType}.obj.log"))
                : null;

            kvSettings.LogDevice = log;
            kvSettings.ObjectLogDevice = objlog;

            var result = new TsavoriteKV<TStoreFunctions, TAllocator>(kvSettings, storeFunctionsCreator(), allocatorCreator);

            storeDisp = result;
            return result;
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        private void TearDown(bool deleteDir)
        {
            storeDisp?.Dispose();
            storeDisp = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            // Do NOT clean up here unless specified, as tests use this TearDown() to prepare for recovery
            if (deleteDir)
                DeleteDirectory(MethodTestDir);
        }

        private TsavoriteKV<TStoreFunctions, TAllocator> PrepareToRecover<TStoreFunctions, TAllocator>(AllocatorType allocatorType,
                Func<TStoreFunctions> storeFunctionsCreator, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorCreator)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            TearDown(deleteDir: false);
            return Setup(allocatorType, storeFunctionsCreator, allocatorCreator);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestByAllocatorType([Values] AllocatorType allocatorType, [Values] CompletionSyncMode syncMode)
        {
            await TestDriver(allocatorType, syncMode == CompletionSyncMode.Async);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestFailOnSectorSize([Values] AllocatorType allocatorType, [Values] CompletionSyncMode syncMode)
        {
            smallSector = true;
            await TestDriver(allocatorType, syncMode == CompletionSyncMode.Async);
        }

        private async ValueTask TestDriver(AllocatorType allocatorType, [Values] bool isAsync)
        {
            var task = allocatorType switch
            {
                AllocatorType.SpanByte => RunTest<SpanByteStoreFunctions, StructAllocator>(allocatorType,
                                                () => StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance),
                                                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions),
                                                Populate, Read, Recover, isAsync),
                AllocatorType.Object => RunTest<ClassStoreFunctions, ClassAllocator>(allocatorType,
                                                () => StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer.Instance),
                                                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions),
                                                Populate, Read, Recover, isAsync),
                _ => throw new ApplicationException("Unknown allocator type"),
            };
            ;
            await task;
        }

        private async ValueTask RunTest<TStoreFunctions, TAllocator>(AllocatorType allocatorType,
                Func<TStoreFunctions> storeFunctionsCreator, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorCreator,
                Action<TsavoriteKV<TStoreFunctions, TAllocator>> populateAction,
                Action<TsavoriteKV<TStoreFunctions, TAllocator>> readAction,
                Func<TsavoriteKV<TStoreFunctions, TAllocator>, bool, ValueTask> recoverFunc,
                bool isAsync)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var store = Setup(allocatorType, storeFunctionsCreator, allocatorCreator);
            populateAction(store);
            readAction(store);
            if (smallSector)
            {
                _ = Assert.ThrowsAsync<TsavoriteException>(async () => await Checkpoint(store, isAsync));
                Assert.Pass("Verified expected exception; the test cannot continue, so exiting early with success");
            }
            else
                await Checkpoint(store, isAsync);

            ClassicAssert.AreNotEqual(Guid.Empty, logToken);
            ClassicAssert.AreNotEqual(Guid.Empty, indexToken);
            readAction(store);

            store = PrepareToRecover(allocatorType, storeFunctionsCreator, allocatorCreator);
            await recoverFunc(store, isAsync);
            readAction(store);
        }

        static int GetRandomLength(Random r) => r.Next(StackAllocMax) + 1;  // +1 to remain in range 1..StackAllocMax

        private unsafe void Populate(TsavoriteKV<SpanByteStoreFunctions, StructAllocator> store)
        {
            using var session = store.NewSession<PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Random rng = new(RandSeed);

            // Single alloc outside the loop, to the max length we'll need.
            AdId key = new();
            Span<int> valueSpan = stackalloc int[StackAllocMax];

            for (int i = 0; i < DeviceTypeRecoveryTests.NumOps; i++)
            {
                // We must be consistent on length across iterations of each key value
                var key0 = i % (int)DeviceTypeRecoveryTests.NumUniqueKeys;
                if (key0 == 0)
                    rng = new(RandSeed);

                key.adId = key0;

                var len = GetRandomLength(rng);
                for (int j = 0; j < len; j++)
                    valueSpan[j] = i;

                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), VLVector.FromPinnedSpan(valueSpan), Empty.Default);
            }
            _ = bContext.CompletePending(true);
        }

        private unsafe void Populate(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < DeviceTypeRecoveryTests.NumOps; i++)
            {
                var key = new TestObjectKey { key = i % (int)DeviceTypeRecoveryTests.NumUniqueKeys };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), value);
            }
            _ = bContext.CompletePending(true);
        }

        private async ValueTask Checkpoint<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, bool isAsync)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            if (isAsync)
            {
                var (success, token) = await store.TakeFullCheckpointAsync(CheckpointType.Snapshot);
                ClassicAssert.IsTrue(success);
                logToken = token;
            }
            else
            {
                while (!store.TryInitiateFullCheckpoint(out logToken, CheckpointType.Snapshot)) { }
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            indexToken = logToken;
        }

        private async ValueTask RecoverAndReadTest(TsavoriteKV<SpanByteStoreFunctions, StructAllocator> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<SpanByteStoreFunctions, StructAllocator> store)
        {
            using var session = store.NewSession<PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Random rng = new(RandSeed);

            AdId key = new();

            for (var i = 0; i < DeviceTypeRecoveryTests.NumUniqueKeys; i++)
            {
                key.adId = i;

                int[] output = null;
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found);

                var len = GetRandomLength(rng);
                for (int j = 0; j < len; j++)
                    ClassicAssert.AreEqual(ExpectedValue(i), output[j], $"mismatched data at position {j}, len {len}");
            }
        }

        private async ValueTask RecoverAndReadTest(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            for (var i = 0; i < DeviceTypeRecoveryTests.NumUniqueKeys; i++)
            {
                var key = new TestObjectKey { key = i };
                var output = new TestObjectOutput();
                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found, $"keyIndex {i}");
                ClassicAssert.AreEqual(ExpectedValue(i), output.value.value);
            }
        }

        private async ValueTask Recover<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, bool isAsync = false)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            if (isAsync)
                _ = await store.RecoverAsync(indexToken, logToken);
            else
                _ = store.Recover(indexToken, logToken);
        }
    }
}