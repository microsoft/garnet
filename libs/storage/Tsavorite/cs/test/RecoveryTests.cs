// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.sumstore
{
    using LongAllocator = BlittableAllocator<long, long, StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>>;
    using LongStoreFunctions = StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>;
    using MyValueAllocator = GenericAllocator<MyValue, MyValue, StoreFunctions<MyValue, MyValue, MyValue.Comparer, DefaultRecordDisposer<MyValue, MyValue>>>;
    using MyValueStoreFunctions = StoreFunctions<MyValue, MyValue, MyValue.Comparer, DefaultRecordDisposer<MyValue, MyValue>>;

    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using StructAllocator = BlittableAllocator<AdId, NumClicks, StoreFunctions<AdId, NumClicks, AdId.Comparer, DefaultRecordDisposer<AdId, NumClicks>>>;
    using StructStoreFunctions = StoreFunctions<AdId, NumClicks, AdId.Comparer, DefaultRecordDisposer<AdId, NumClicks>>;

    [AllureNUnit]
    [TestFixture]
    internal class DeviceTypeRecoveryTests : AllureTestBase
    {
        internal const long NumUniqueKeys = 1L << 12;
        internal const long KeySpace = 1L << 20;
        internal const long NumOps = 1L << 17;
        internal const long CompletePendingInterval = 1L << 10;
        internal const long CheckpointInterval = 1L << 14;

        private TsavoriteKV<AdId, NumClicks, StructStoreFunctions, StructAllocator> store;
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
            }, StoreFunctions<AdId, NumClicks>.Create(new AdId.Comparer())
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
        public async ValueTask RecoveryTestSeparateCheckpoint([Values] bool isAsync, [Values] TestDeviceType deviceType)
        {
            Setup(deviceType);
            Populate(SeparateCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                if (i >= indexTokens.Count) break;
                PrepareToRecover(deviceType);
                await RecoverAndTestAsync(i, isAsync);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask RecoveryTestFullCheckpoint([Values] bool isAsync, [Values] TestDeviceType deviceType)
        {
            Setup(deviceType);
            Populate(FullCheckpointAction);

            for (var i = 0; i < logTokens.Count; i++)
            {
                PrepareToRecover(deviceType);
                await RecoverAndTestAsync(i, isAsync);
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
            var inputArray = new AdInput[NumOps];
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
                _ = bContext.RMW(ref inputArray[i].adId, ref inputArray[i], Empty.Default);

                checkpointAction(i);

                if (i % CompletePendingInterval == 0)
                    _ = bContext.CompletePending(false);
            }

            // Make sure operations are completed
            _ = bContext.CompletePending(true);
        }

        private async ValueTask RecoverAndTestAsync(int tokenIndex, bool isAsync)
        {
            var logToken = logTokens[tokenIndex];
            var indexToken = indexTokens[tokenIndex];

            // Recover
            if (isAsync)
                _ = await store.RecoverAsync(indexToken, logToken);
            else
                _ = store.Recover(indexToken, logToken);

            // Create array for reading
            var inputArray = new AdInput[NumUniqueKeys];
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
                var status = bContext.Read(ref inputArray[i].adId, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.Found, $"At tokenIndex {tokenIndex}, keyIndex {i}, AdId {inputArray[i].adId.adId}");
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            _ = bContext.CompletePending(true);
        }
    }

    [AllureNUnit]
    [TestFixture]
    public class AllocatorTypeRecoveryTests : AllureTestBase
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

        private TsavoriteKV<TData, TData, TStoreFunctions, TAllocator> Setup<TData, TStoreFunctions, TAllocator>(AllocatorType allocatorType, Func<TStoreFunctions> storeFunctionsCreator, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorCreator)
            where TStoreFunctions : IStoreFunctions<TData, TData>
            where TAllocator : IAllocator<TData, TData, TStoreFunctions>
        {
            log = new LocalMemoryDevice(1L << 26, 1L << 22, 2, sector_size: smallSector ? 64 : (uint)512, fileName: Path.Join(MethodTestDir, $"{typeof(TData).Name}.log"));
            objlog = allocatorType == AllocatorType.Generic
                ? new LocalMemoryDevice(1L << 26, 1L << 22, 2, fileName: Path.Join(MethodTestDir, $"{typeof(TData).Name}.obj.log"))
                : null;

            var result = new TsavoriteKV<TData, TData, TStoreFunctions, TAllocator>(new()
            {
                IndexSize = DeviceTypeRecoveryTests.KeySpace,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 25,
                CheckpointDir = MethodTestDir
            }, storeFunctionsCreator()
                , allocatorCreator
            );

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

        private TsavoriteKV<TData, TData, TStoreFunctions, TAllocator> PrepareToRecover<TData, TStoreFunctions, TAllocator>(AllocatorType allocatorType,
                Func<TStoreFunctions> storeFunctionsCreator, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorCreator)
            where TStoreFunctions : IStoreFunctions<TData, TData>
            where TAllocator : IAllocator<TData, TData, TStoreFunctions>
        {
            TearDown(deleteDir: false);
            return Setup<TData, TStoreFunctions, TAllocator>(allocatorType, storeFunctionsCreator, allocatorCreator);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestByAllocatorType([Values] AllocatorType allocatorType, [Values] bool isAsync)
        {
            await TestDriver(allocatorType, isAsync);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestFailOnSectorSize([Values] AllocatorType allocatorType, [Values] bool isAsync)
        {
            smallSector = true;
            await TestDriver(allocatorType, isAsync);
        }

        private async ValueTask TestDriver(AllocatorType allocatorType, [Values] bool isAsync)
        {
            var task = allocatorType switch
            {
                AllocatorType.FixedBlittable => RunTest<long, LongStoreFunctions, LongAllocator>(allocatorType,
                                                () => StoreFunctions<long, long>.Create(LongKeyComparer.Instance),
                                                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions),
                                                Populate, Read, Recover, isAsync),
                AllocatorType.SpanByte => RunTest<SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(allocatorType,
                                                StoreFunctions<SpanByte, SpanByte>.Create,
                                                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions),
                                                Populate, Read, Recover, isAsync),
                AllocatorType.Generic => RunTest<MyValue, MyValueStoreFunctions, MyValueAllocator>(allocatorType,
                                                () => StoreFunctions<MyValue, MyValue>.Create(new MyValue.Comparer(), () => new MyValueSerializer(), () => new MyValueSerializer(), DefaultRecordDisposer<MyValue, MyValue>.Instance),
                                                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions),
                                                Populate, Read, Recover, isAsync),
                _ => throw new ApplicationException("Unknown allocator type"),
            };
            ;
            await task;
        }

        private async ValueTask RunTest<TData, TStoreFunctions, TAllocator>(AllocatorType allocatorType,
                Func<TStoreFunctions> storeFunctionsCreator, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorCreator,
                Action<TsavoriteKV<TData, TData, TStoreFunctions, TAllocator>> populateAction,
                Action<TsavoriteKV<TData, TData, TStoreFunctions, TAllocator>> readAction,
                Func<TsavoriteKV<TData, TData, TStoreFunctions, TAllocator>, bool, ValueTask> recoverFunc,
                bool isAsync)
            where TStoreFunctions : IStoreFunctions<TData, TData>
            where TAllocator : IAllocator<TData, TData, TStoreFunctions>
        {
            var store = Setup<TData, TStoreFunctions, TAllocator>(allocatorType, storeFunctionsCreator, allocatorCreator);
            populateAction(store);
            readAction(store);
            if (smallSector)
            {
                Assert.ThrowsAsync<TsavoriteException>(async () => await Checkpoint(store, isAsync));
                Assert.Pass("Verified expected exception; the test cannot continue, so exiting early with success");
            }
            else
                await Checkpoint(store, isAsync);

            ClassicAssert.AreNotEqual(Guid.Empty, logToken);
            ClassicAssert.AreNotEqual(Guid.Empty, indexToken);
            readAction(store);

            store = PrepareToRecover<TData, TStoreFunctions, TAllocator>(allocatorType, storeFunctionsCreator, allocatorCreator);
            await recoverFunc(store, isAsync);
            readAction(store);
        }

        private void Populate(TsavoriteKV<long, long, LongStoreFunctions, LongAllocator> store)
        {
            using var session = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bContext = session.BasicContext;

            for (int i = 0; i < DeviceTypeRecoveryTests.NumOps; i++)
                _ = bContext.Upsert(i % DeviceTypeRecoveryTests.NumUniqueKeys, i);
            _ = bContext.CompletePending(true);
        }

        static int GetRandomLength(Random r) => r.Next(StackAllocMax) + 1;  // +1 to remain in range 1..StackAllocMax

        private unsafe void Populate(TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store)
        {
            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Random rng = new(RandSeed);

            // Single alloc outside the loop, to the max length we'll need.
            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[StackAllocMax];

            for (int i = 0; i < DeviceTypeRecoveryTests.NumOps; i++)
            {
                // We must be consistent on length across iterations of each key value
                var key0 = i % (int)DeviceTypeRecoveryTests.NumUniqueKeys;
                if (key0 == 0)
                    rng = new(RandSeed);

                keySpan[0] = key0;
                var keySpanByte = keySpan.AsSpanByte();

                var len = GetRandomLength(rng);
                for (int j = 0; j < len; j++)
                    valueSpan[j] = i;
                var valueSpanByte = valueSpan.Slice(0, len).AsSpanByte();

                _ = bContext.Upsert(ref keySpanByte, ref valueSpanByte, Empty.Default);
            }
            _ = bContext.CompletePending(true);
        }

        private unsafe void Populate(TsavoriteKV<MyValue, MyValue, MyValueStoreFunctions, MyValueAllocator> store)
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions2>(new MyFunctions2());
            var bContext = session.BasicContext;

            for (int i = 0; i < DeviceTypeRecoveryTests.NumOps; i++)
            {
                var key = new MyValue { value = i % (int)DeviceTypeRecoveryTests.NumUniqueKeys };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(key, value);
            }
            _ = bContext.CompletePending(true);
        }

        private async ValueTask Checkpoint<TData, TStoreFunctions, TAllocator>(TsavoriteKV<TData, TData, TStoreFunctions, TAllocator> store, bool isAsync)
            where TStoreFunctions : IStoreFunctions<TData, TData>
            where TAllocator : IAllocator<TData, TData, TStoreFunctions>
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

        private async ValueTask RecoverAndReadTest(TsavoriteKV<long, long, LongStoreFunctions, LongAllocator> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<long, long, LongStoreFunctions, LongAllocator> store)
        {
            using var session = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bContext = session.BasicContext;

            for (var i = 0; i < DeviceTypeRecoveryTests.NumUniqueKeys; i++)
            {
                var status = bContext.Read(i % DeviceTypeRecoveryTests.NumUniqueKeys, default, out long output);
                ClassicAssert.IsTrue(status.Found, $"keyIndex {i}");
                ClassicAssert.AreEqual(ExpectedValue(i), output);
            }
        }

        private async ValueTask RecoverAndReadTest(TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store)
        {
            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Random rng = new(RandSeed);

            Span<int> keySpan = stackalloc int[1];
            var keySpanByte = keySpan.AsSpanByte();

            for (var i = 0; i < DeviceTypeRecoveryTests.NumUniqueKeys; i++)
            {
                keySpan[0] = i;

                var len = GetRandomLength(rng);

                int[] output = null;
                var status = bContext.Read(ref keySpanByte, ref output, Empty.Default);

                ClassicAssert.IsTrue(status.Found);
                for (int j = 0; j < len; j++)
                    ClassicAssert.AreEqual(ExpectedValue(i), output[j], $"mismatched data at position {j}, len {len}");
            }
        }

        private async ValueTask RecoverAndReadTest(TsavoriteKV<MyValue, MyValue, MyValueStoreFunctions, MyValueAllocator> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<MyValue, MyValue, MyValueStoreFunctions, MyValueAllocator> store)
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions2>(new MyFunctions2());
            var bContext = session.BasicContext;

            for (var i = 0; i < DeviceTypeRecoveryTests.NumUniqueKeys; i++)
            {
                var key = new MyValue { value = i };
                var status = bContext.Read(key, default, out MyOutput output);
                ClassicAssert.IsTrue(status.Found, $"keyIndex {i}");
                ClassicAssert.AreEqual(ExpectedValue(i), output.value.value);
            }
        }

        private async ValueTask Recover<TData, TStoreFunctions, TAllocator>(TsavoriteKV<TData, TData, TStoreFunctions, TAllocator> store, bool isAsync = false)
            where TStoreFunctions : IStoreFunctions<TData, TData>
            where TAllocator : IAllocator<TData, TData, TStoreFunctions>
        {
            if (isAsync)
                _ = await store.RecoverAsync(indexToken, logToken);
            else
                _ = store.Recover(indexToken, logToken);
        }
    }
}