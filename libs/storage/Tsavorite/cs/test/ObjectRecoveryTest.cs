// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.test.recovery.sumstore;

namespace Tsavorite.test.recovery.objects
{
    using static Tsavorite.test.TestUtils;
    using ClassAllocator = ObjectAllocator<StoreFunctions<AdId.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<AdId.Comparer, DefaultRecordDisposer>;

    internal struct StructTuple<T1, T2>
    {
        public T1 Item1;
        public T2 Item2;
    }

    [AllureNUnit]
    [TestFixture]
    internal class ObjectRecoveryTests : AllureTestBase
    {
        const long NumUniqueKeys = 1L << 14;
        const long KeySpace = 1L << 14;
        const long NumOps = 1L << 19;
        const long CompletePendingInterval = 1L << 10;
        const long CheckpointInterval = 1L << 16;
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private Guid token;
        private IDevice log, objlog;

        [SetUp]
        public void Setup() => Setup(deleteDir: true);

        public void Setup(bool deleteDir)
        {
            if (deleteDir)
                TestUtils.RecreateDirectory(TestUtils.MethodTestDir);

            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectRecoveryTests.log"));
            objlog = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectRecoveryTests.obj.log"));

            store = new(new()
            {
                IndexSize = KeySpace,
                LogDevice = log,
                ObjectLogDevice = objlog,
                CheckpointDir = TestUtils.MethodTestDir
            }, StoreFunctions.Create(new AdId.Comparer(), () => new NumClicksObj.Serializer(), DefaultRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        public void TearDown(bool deleteDir)
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            if (deleteDir)
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private void PrepareToRecover()
        {
            TearDown(deleteDir: false);
            Setup(deleteDir: false);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask ObjectRecoveryTest1([Values] CompletionSyncMode syncMode)
        {
            Populate();
            PrepareToRecover();

            if (syncMode == CompletionSyncMode.Async)
                _ = await store.RecoverAsync(token, token);
            else
                _ = store.Recover(token, token);

            Verify(token, token);
        }

        public unsafe void Populate()
        {
            // Prepare the dataset
            var inputArray = GC.AllocateArray<StructTuple<AdId, Input>>((int)NumOps);
            for (int i = 0; i < NumOps; i++)
            {
                inputArray[i] = new()
                {
                    Item1 = new AdId { adId = i % NumUniqueKeys },
                    Item2 = new Input { numClicks = new NumClicksObj { numClicks = 1 } }
                };
            }

            // Register thread with Tsavorite
            var session = store.NewSession<Input, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Process the batch of input data
            bool first = true;
            for (int i = 0; i < NumOps; i++)
            {
                _ = bContext.RMW(SpanByte.FromPinnedVariable(ref inputArray[i].Item1), ref inputArray[i].Item2, Empty.Default);

                if ((i + 1) % CheckpointInterval == 0)
                {
                    if (first)
                        while (!store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot)) ;
                    else
                        while (!store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot)) ;

                    store.CompleteCheckpointAsync().GetAwaiter().GetResult();
                    first = false;
                }

                if (i % CompletePendingInterval == 0)
                    _ = bContext.CompletePending(false, false);
            }


            // Make sure operations are completed
            _ = bContext.CompletePending(true);
            session.Dispose();
        }

        public unsafe void Verify(Guid cprVersion, Guid indexVersion)
        {
            // Create array for reading
            var inputArray = GC.AllocateArray<StructTuple<AdId, Input>>((int)NumUniqueKeys);
            for (int i = 0; i < NumUniqueKeys; i++)
            {
                inputArray[i] = new()
                {
                    Item1 = new AdId { adId = i },
                    Item2 = new Input { numClicks = new NumClicksObj { numClicks = 0 } }
                };
            }

            var session = store.NewSession<Input, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            Input input = default;
            // Issue read requests
            for (var i = 0; i < NumUniqueKeys; i++)
            {
                Output output = new();
                _ = bContext.Read(SpanByte.FromPinnedVariable(ref inputArray[i].Item1), ref input, ref output, Empty.Default);
            }

            // Complete all pending requests
            _ = bContext.CompletePending(true);

            // Release
            session.Dispose();
        }
    }
}