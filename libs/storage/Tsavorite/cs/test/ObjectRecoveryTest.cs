// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objects
{
    internal struct StructTuple<T1, T2>
    {
        public T1 Item1;
        public T2 Item2;
    }

    [TestFixture]
    internal class ObjectRecoveryTests
    {
        const long numUniqueKeys = (1 << 14);
        const long keySpace = (1L << 14);
        const long numOps = (1L << 19);
        const long completePendingInterval = (1L << 10);
        const long checkpointInterval = (1L << 16);
        private TsavoriteKV<AdId, NumClicks> store;
        private Guid token;
        private IDevice log, objlog;

        [SetUp]
        public void Setup() => Setup(deleteDir: true);

        public void Setup(bool deleteDir)
        {
            if (deleteDir)
                TestUtils.RecreateDirectory(TestUtils.MethodTestDir);

            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectRecoveryTests.log"), false);
            objlog = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "ObjectRecoveryTests.obj.log"), false);

            store = new TsavoriteKV<AdId, NumClicks>
                (
                    keySpace,
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog },
                    new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir },
                    new SerializerSettings<AdId, NumClicks> { keySerializer = () => new AdIdSerializer(), valueSerializer = () => new NumClicksSerializer() }
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
        public async ValueTask ObjectRecoveryTest1([Values] bool isAsync)
        {
            Populate();
            PrepareToRecover();

            if (isAsync)
                await store.RecoverAsync(token, token);
            else
                store.Recover(token, token);

            Verify(token, token);
        }

        public unsafe void Populate()
        {
            // Prepare the dataset
            var inputArray = new StructTuple<AdId, Input>[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i] = new StructTuple<AdId, Input>
                {
                    Item1 = new AdId { adId = i % numUniqueKeys },
                    Item2 = new Input { numClicks = new NumClicks { numClicks = 1 } }
                };
            }

            // Register thread with Tsavorite
            var session = store.NewSession<Input, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            // Process the batch of input data
            bool first = true;
            for (int i = 0; i < numOps; i++)
            {
                bContext.RMW(ref inputArray[i].Item1, ref inputArray[i].Item2, Empty.Default);

                if ((i + 1) % checkpointInterval == 0)
                {
                    if (first)
                        while (!store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot)) ;
                    else
                        while (!store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot)) ;

                    store.CompleteCheckpointAsync().GetAwaiter().GetResult();

                    first = false;
                }

                if (i % completePendingInterval == 0)
                {
                    bContext.CompletePending(false, false);
                }
            }


            // Make sure operations are completed
            bContext.CompletePending(true);
            session.Dispose();
        }

        public unsafe void Verify(Guid cprVersion, Guid indexVersion)
        {
            // Create array for reading
            var inputArray = new StructTuple<AdId, Input>[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i] = new StructTuple<AdId, Input>
                {
                    Item1 = new AdId { adId = i },
                    Item2 = new Input { numClicks = new NumClicks { numClicks = 0 } }
                };
            }

            var session = store.NewSession<Input, Output, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            Input input = default;
            // Issue read requests
            for (var i = 0; i < numUniqueKeys; i++)
            {
                Output output = new();
                bContext.Read(ref inputArray[i].Item1, ref input, ref output, Empty.Default);
            }

            // Complete all pending requests
            bContext.CompletePending(true);

            // Release
            session.Dispose();
        }
    }
}