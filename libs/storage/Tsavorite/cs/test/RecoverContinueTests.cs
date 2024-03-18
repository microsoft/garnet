
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.recovery.sumstore.cntinue
{
    [TestFixture]
    internal class RecoverContinueTests
    {
        private TsavoriteKV<AdId, NumClicks> store1;
        private TsavoriteKV<AdId, NumClicks> store2;
        private TsavoriteKV<AdId, NumClicks> store3;
        private IDevice log;
        private int numOps;
        private string checkpointDir;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/test.log", deleteOnClose: true);
            checkpointDir = TestUtils.MethodTestDir + "/checkpoints3";
            Directory.CreateDirectory(checkpointDir);

            store1 = new TsavoriteKV<AdId, NumClicks>
                (16,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            store2 = new TsavoriteKV<AdId, NumClicks>
                (16,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            store3 = new TsavoriteKV<AdId, NumClicks>
                (16,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            numOps = 5000;
        }

        [TearDown]
        public void TearDown()
        {
            store1?.Dispose();
            store2?.Dispose();
            store3?.Dispose();
            store1 = null;
            store2 = null;
            store3 = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.DeleteDirectory(checkpointDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask RecoverContinueTest([Values] bool isAsync)
        {
            long sno = 0;

            var firstsession = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions(), "first");
            IncrementAllValues(ref firstsession, ref sno);
            store1.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot);
            store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            firstsession.Dispose();

            // Check if values after checkpoint are correct
            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            CheckAllValues(ref session1, 1);
            session1.Dispose();

            // Recover and check if recovered values are correct
            if (isAsync)
                await store2.RecoverAsync();
            else
                store2.Recover();
            var session2 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            CheckAllValues(ref session2, 1);
            session2.Dispose();

            // Continue and increment values
            var continuesession = store2.ResumeSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions(), "first", out CommitPoint cp);
            long newSno = cp.UntilSerialNo;
            Assert.AreEqual(sno - 1, newSno);
            IncrementAllValues(ref continuesession, ref sno);
            store2.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot);
            store2.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            continuesession.Dispose();

            // Check if values after continue checkpoint are correct
            var session3 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            CheckAllValues(ref session3, 2);
            session3.Dispose();

            // Recover and check if recovered values are correct
            if (isAsync)
                await store3.RecoverAsync();
            else
                store3.Recover();

            var nextsession = store3.ResumeSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions(), "first", out cp);
            long newSno2 = cp.UntilSerialNo;
            Assert.AreEqual(sno - 1, newSno2);
            CheckAllValues(ref nextsession, 2);
            nextsession.Dispose();
        }

        private void CheckAllValues(
            ref ClientSession<AdId, NumClicks, AdInput, Output, Empty, AdSimpleFunctions> store,
            int value)
        {
            AdInput inputArg = default;
            Output outputArg = default;
            for (var key = 0; key < numOps; key++)
            {
                inputArg.adId.adId = key;
                var status = store.Read(ref inputArg.adId, ref inputArg, ref outputArg, Empty.Default, store.SerialNo);

                if (status.IsPending)
                    store.CompletePending(true);
                else
                {
                    Assert.AreEqual(value, outputArg.value.numClicks);
                }
            }

            store.CompletePending(true);
        }

        private void IncrementAllValues(
            ref ClientSession<AdId, NumClicks, AdInput, Output, Empty, AdSimpleFunctions> store,
            ref long sno)
        {
            AdInput inputArg = default;
            for (int key = 0; key < numOps; key++, sno++)
            {
                inputArg.adId.adId = key;
                inputArg.numClicks.numClicks = 1;
                store.RMW(ref inputArg.adId, ref inputArg, Empty.Default, sno);
            }
            store.CompletePending(true);
        }


    }

    public class AdSimpleFunctions : FunctionsBase<AdId, NumClicks, AdInput, Output, Empty>
    {
        public override void ReadCompletionCallback(ref AdId key, ref AdInput input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key.adId, output.value.numClicks);
        }

        // Read functions
        public override bool SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = input.numClicks;
            return true;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
            return true;
        }
    }
}