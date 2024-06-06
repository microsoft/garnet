﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.recovery.sumstore
{
    [TestFixture]
    internal class DeviceTypeRecoveryTests
    {
        internal const long numUniqueKeys = (1 << 12);
        internal const long keySpace = (1L << 14);
        internal const long numOps = (1L << 17);
        internal const long completePendingInterval = (1L << 10);
        internal const long checkpointInterval = (1L << 14);

        private TsavoriteKV<AdId, NumClicks> store;
        private readonly List<Guid> logTokens = new();
        private readonly List<Guid> indexTokens = new();
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            // Only clean these in the initial Setup, as tests use the other Setup() overload to recover
            logTokens.Clear();
            indexTokens.Clear();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, true);
        }

        private void Setup(TestUtils.DeviceType deviceType)
        {
            log = TestUtils.CreateTestDevice(deviceType, Path.Join(TestUtils.MethodTestDir, "Test.log"));
            store = new TsavoriteKV<AdId, NumClicks>(keySpace,
                new LogSettings { LogDevice = log, SegmentSizeBits = 25 }, //new LogSettings { LogDevice = log, MemorySizeBits = 14, PageSizeBits = 9 },  // locks ups at session.RMW line in Populate() for Local Memory
                new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir }
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
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private void PrepareToRecover(TestUtils.DeviceType deviceType)
        {
            TearDown(deleteDir: false);
            Setup(deviceType);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestSeparateCheckpoint([Values] bool isAsync, [Values] TestUtils.DeviceType deviceType)
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
        public async ValueTask RecoveryTestFullCheckpoint([Values] bool isAsync, [Values] TestUtils.DeviceType deviceType)
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
            if ((opNum + 1) % checkpointInterval == 0)
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
            if ((opNum + 1) % checkpointInterval != 0)
                return;

            var checkpointNum = (opNum + 1) / checkpointInterval;
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
            var inputArray = new AdInput[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            // Register thread with Tsavorite
            using var session = store.NewSession<AdInput, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Process the batch of input data
            for (int i = 0; i < numOps; i++)
            {
                bContext.RMW(ref inputArray[i].adId, ref inputArray[i], Empty.Default);

                checkpointAction(i);

                if (i % completePendingInterval == 0)
                    bContext.CompletePending(false);
            }

            // Make sure operations are completed
            bContext.CompletePending(true);
        }

        private async ValueTask RecoverAndTestAsync(int tokenIndex, bool isAsync)
        {
            var logToken = logTokens[tokenIndex];
            var indexToken = indexTokens[tokenIndex];

            // Recover
            if (isAsync)
                await store.RecoverAsync(indexToken, logToken);
            else
                store.Recover(indexToken, logToken);

            // Create array for reading
            var inputArray = new AdInput[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
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
            for (var i = 0; i < numUniqueKeys; i++)
            {
                var status = bContext.Read(ref inputArray[i].adId, ref input, ref output, Empty.Default);
                Assert.IsTrue(status.Found, $"At tokenIndex {tokenIndex}, keyIndex {i}, AdId {inputArray[i].adId.adId}");
                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            bContext.CompletePending(true);
        }
    }

    [TestFixture]
    public class AllocatorTypeRecoveryTests
    {
        const int StackAllocMax = 12;
        const int RandSeed = 101;
        const long expectedValueBase = DeviceTypeRecoveryTests.numUniqueKeys * (DeviceTypeRecoveryTests.numOps / DeviceTypeRecoveryTests.numUniqueKeys - 1);
        private static long ExpectedValue(int key) => expectedValueBase + key;

        private IDisposable storeDisp;
        private Guid logToken;
        private Guid indexToken;
        private IDevice log;
        private IDevice objlog;
        private bool smallSector;

        // 'object' to avoid generic args
        private object serializerSettingsObj;

        [SetUp]
        public void Setup()
        {
            smallSector = false;
            serializerSettingsObj = null;

            // Only clean these in the initial Setup, as tests use the other Setup() overload to recover
            logToken = Guid.Empty;
            indexToken = Guid.Empty;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, true);
        }

        private TsavoriteKV<TData, TData> Setup<TData>()
        {
            log = new LocalMemoryDevice(1L << 26, 1L << 22, 2, sector_size: smallSector ? 64 : (uint)512, fileName: Path.Join(TestUtils.MethodTestDir, $"{typeof(TData).Name}.log"));
            objlog = serializerSettingsObj is null
                ? null
                : new LocalMemoryDevice(1L << 26, 1L << 22, 2, fileName: Path.Join(TestUtils.MethodTestDir, $"{typeof(TData).Name}.obj.log"));

            var result = new TsavoriteKV<TData, TData>(DeviceTypeRecoveryTests.keySpace,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, SegmentSizeBits = 25 },
                new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir },
                serializerSettingsObj as SerializerSettings<TData, TData>
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
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private TsavoriteKV<TData, TData> PrepareToRecover<TData>()
        {
            TearDown(deleteDir: false);
            return Setup<TData>();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestByAllocatorType([Values] TestUtils.AllocatorType allocatorType, [Values] bool isAsync)
        {
            await TestDriver(allocatorType, isAsync);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryTestFailOnSectorSize([Values] TestUtils.AllocatorType allocatorType, [Values] bool isAsync)
        {
            smallSector = true;
            await TestDriver(allocatorType, isAsync);
        }

        private async ValueTask TestDriver(TestUtils.AllocatorType allocatorType, [Values] bool isAsync)
        {
            ValueTask task;
            switch (allocatorType)
            {
                case TestUtils.AllocatorType.FixedBlittable:
                    task = RunTest<long>(Populate, Read, Recover, isAsync);
                    break;
                case TestUtils.AllocatorType.SpanByte:
                    task = RunTest<SpanByte>(Populate, Read, Recover, isAsync);
                    break;
                case TestUtils.AllocatorType.Generic:
                    serializerSettingsObj = new MyValueSerializer();
                    task = RunTest<MyValue>(Populate, Read, Recover, isAsync);
                    break;
                default:
                    throw new ApplicationException("Unknown allocator type");
            };
            await task;
        }

        private async ValueTask RunTest<TData>(Action<TsavoriteKV<TData, TData>> populateAction, Action<TsavoriteKV<TData, TData>> readAction, Func<TsavoriteKV<TData, TData>, bool, ValueTask> recoverFunc, bool isAsync)
        {
            var store = Setup<TData>();
            populateAction(store);
            readAction(store);
            if (smallSector)
            {
                Assert.ThrowsAsync<TsavoriteException>(async () => await Checkpoint(store, isAsync));
                Assert.Pass("Verified expected exception; the test cannot continue, so exiting early with success");
            }
            else
                await Checkpoint(store, isAsync);

            Assert.AreNotEqual(Guid.Empty, logToken);
            Assert.AreNotEqual(Guid.Empty, indexToken);
            readAction(store);

            store = PrepareToRecover<TData>();
            await recoverFunc(store, isAsync);
            readAction(store);
        }

        private void Populate(TsavoriteKV<long, long> store)
        {
            using var session = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bContext = session.BasicContext;

            for (int i = 0; i < DeviceTypeRecoveryTests.numOps; i++)
                bContext.Upsert(i % DeviceTypeRecoveryTests.numUniqueKeys, i);
            bContext.CompletePending(true);
        }

        static int GetRandomLength(Random r) => r.Next(StackAllocMax) + 1;  // +1 to remain in range 1..StackAllocMax

        private unsafe void Populate(TsavoriteKV<SpanByte, SpanByte> store)
        {
            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Random rng = new(RandSeed);

            // Single alloc outside the loop, to the max length we'll need.
            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[StackAllocMax];

            for (int i = 0; i < DeviceTypeRecoveryTests.numOps; i++)
            {
                // We must be consistent on length across iterations of each key value
                var key0 = i % (int)DeviceTypeRecoveryTests.numUniqueKeys;
                if (key0 == 0)
                    rng = new(RandSeed);

                keySpan[0] = key0;
                var keySpanByte = keySpan.AsSpanByte();

                var len = GetRandomLength(rng);
                for (int j = 0; j < len; j++)
                    valueSpan[j] = i;
                var valueSpanByte = valueSpan.Slice(0, len).AsSpanByte();

                bContext.Upsert(ref keySpanByte, ref valueSpanByte, Empty.Default);
            }
            bContext.CompletePending(true);
        }

        private unsafe void Populate(TsavoriteKV<MyValue, MyValue> store)
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions2>(new MyFunctions2());
            var bContext = session.BasicContext;

            for (int i = 0; i < DeviceTypeRecoveryTests.numOps; i++)
            {
                var key = new MyValue { value = i % (int)DeviceTypeRecoveryTests.numUniqueKeys };
                var value = new MyValue { value = i };
                bContext.Upsert(key, value);
            }
            bContext.CompletePending(true);
        }

        private async ValueTask Checkpoint<TData>(TsavoriteKV<TData, TData> store, bool isAsync)
        {
            if (isAsync)
            {
                var (success, token) = await store.TakeFullCheckpointAsync(CheckpointType.Snapshot);
                Assert.IsTrue(success);
                logToken = token;
            }
            else
            {
                while (!store.TryInitiateFullCheckpoint(out logToken, CheckpointType.Snapshot)) { }
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }
            indexToken = logToken;
        }

        private async ValueTask RecoverAndReadTest(TsavoriteKV<long, long> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<long, long> store)
        {
            using var session = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bContext = session.BasicContext;

            for (var i = 0; i < DeviceTypeRecoveryTests.numUniqueKeys; i++)
            {
                var status = bContext.Read(i % DeviceTypeRecoveryTests.numUniqueKeys, default, out long output);
                Assert.IsTrue(status.Found, $"keyIndex {i}");
                Assert.AreEqual(ExpectedValue(i), output);
            }
        }

        private async ValueTask RecoverAndReadTest(TsavoriteKV<SpanByte, SpanByte> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<SpanByte, SpanByte> store)
        {
            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Random rng = new(RandSeed);

            Span<int> keySpan = stackalloc int[1];
            var keySpanByte = keySpan.AsSpanByte();

            for (var i = 0; i < DeviceTypeRecoveryTests.numUniqueKeys; i++)
            {
                keySpan[0] = i;

                var len = GetRandomLength(rng);

                int[] output = null;
                var status = bContext.Read(ref keySpanByte, ref output, Empty.Default);

                Assert.IsTrue(status.Found);
                for (int j = 0; j < len; j++)
                    Assert.AreEqual(ExpectedValue(i), output[j], $"mismatched data at position {j}, len {len}");
            }
        }

        private async ValueTask RecoverAndReadTest(TsavoriteKV<MyValue, MyValue> store, bool isAsync)
        {
            await Recover(store, isAsync);
            Read(store);
        }

        private static void Read(TsavoriteKV<MyValue, MyValue> store)
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions2>(new MyFunctions2());
            var bContext = session.BasicContext;

            for (var i = 0; i < DeviceTypeRecoveryTests.numUniqueKeys; i++)
            {
                var key = new MyValue { value = i };
                var status = bContext.Read(key, default, out MyOutput output);
                Assert.IsTrue(status.Found, $"keyIndex {i}");
                Assert.AreEqual(ExpectedValue(i), output.value.value);
            }
        }

        private async ValueTask Recover<TData>(TsavoriteKV<TData, TData> store, bool isAsync = false)
        {
            if (isAsync)
                await store.RecoverAsync(indexToken, logToken);
            else
                store.Recover(indexToken, logToken);
        }
    }
}