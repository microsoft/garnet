// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objectstore
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
        private string test_path;
        private Guid token;
        private IDevice log, objlog;

        [SetUp]
        public void Setup() => Setup(deleteDir: true);

        public void Setup(bool deleteDir)
        {
            test_path = TestUtils.MethodTestDir;
            if (deleteDir)
                TestUtils.RecreateDirectory(test_path);

            log = Devices.CreateLogDevice(test_path + "/ObjectRecoveryTests.log", false);
            objlog = Devices.CreateLogDevice(test_path + "/ObjectRecoveryTests.obj.log", false);

            store = new TsavoriteKV<AdId, NumClicks>
                (
                    keySpace,
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog },
                    new CheckpointSettings { CheckpointDir = test_path },
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
                TestUtils.DeleteDirectory(test_path);
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

            // Process the batch of input data
            bool first = true;
            for (int i = 0; i < numOps; i++)
            {
                session.RMW(ref inputArray[i].Item1, ref inputArray[i].Item2, Empty.Default, i);

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
                    session.CompletePending(false, false);
                }
            }


            // Make sure operations are completed
            session.CompletePending(true);
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

            var outputArray = new Output[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                outputArray[i] = new Output();
            }

            // Register with thread
            var session = store.NewSession<Input, Output, Empty, Functions>(new Functions());

            Input input = default;
            // Issue read requests
            for (var i = 0; i < numUniqueKeys; i++)
            {
                session.Read(ref inputArray[i].Item1, ref input, ref outputArray[i], Empty.Default, i);
            }

            // Complete all pending requests
            session.CompletePending(true);

            // Release
            session.Dispose();

            // Test outputs
            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(cprVersion,
                new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactory(),
                        new DefaultCheckpointNamingScheme(
                          new DirectoryInfo(test_path).FullName)), null);

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            foreach (var guid in checkpointInfo.continueTokens.Keys)
            {
                var cp = checkpointInfo.continueTokens[guid].Item2;
                for (long i = 0; i <= cp.UntilSerialNo; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }

            int threadCount = 1; // single threaded test
            int numCompleted = threadCount - checkpointInfo.continueTokens.Count;
            for (int t = 0; t < numCompleted; t++)
            {
                var sno = numOps;
                for (long i = 0; i < sno; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }

            // Assert if expected is same as found
            for (long i = 0; i < numUniqueKeys; i++)
            {
                Assert.AreEqual(expected[i], outputArray[i].value.numClicks, $"AdId {inputArray[i].Item1.adId}");
            }
        }
    }
}