﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.devices;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.sumstore
{
    [TestFixture]
    class RecoveryTests
    {
        const int numOps = 5000;
        AdId[] inputArray;

        private byte[] commitCookie;
        string checkpointDir;
        ICheckpointManager checkpointManager;

        private TsavoriteKV<AdId, NumClicks> store1;
        private TsavoriteKV<AdId, NumClicks> store2;
        private IDevice log;


        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            checkpointManager = default;
            checkpointDir = default;
            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
                inputArray[i].adId = i;
        }

        [TearDown]
        public void TearDown()
        {
            store1?.Dispose();
            store1 = null;
            store2?.Dispose();
            store2 = null;
            log?.Dispose();
            log = null;

            checkpointManager?.Dispose();
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask PageBlobSimpleRecoveryTest([Values] CheckpointType checkpointType, [Values] CompletionSyncMode completionSyncMode, [Values] bool testCommitCookie)
        {
            IgnoreIfNotRunningAzureTests();
            checkpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(AzureEmulatedStorageString),
                new AzureCheckpointNamingScheme($"{AzureTestContainer}/{AzureTestDirectory}"));
            await SimpleRecoveryTest1_Worker(checkpointType, completionSyncMode, testCommitCookie);
            checkpointManager.PurgeAll();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask LocalDeviceSimpleRecoveryTest([Values] CheckpointType checkpointType, [Values] CompletionSyncMode completionSyncMode, [Values] bool testCommitCookie)
        {
            checkpointManager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactory(),
                new DefaultCheckpointNamingScheme(Path.Join(MethodTestDir, "chkpt")));
            await SimpleRecoveryTest1_Worker(checkpointType, completionSyncMode, testCommitCookie);
            checkpointManager.PurgeAll();
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleRecoveryTest1([Values] CheckpointType checkpointType, [Values] CompletionSyncMode completionSyncMode, [Values] bool testCommitCookie)
        {
            await SimpleRecoveryTest1_Worker(checkpointType, completionSyncMode, testCommitCookie);
        }

        private async ValueTask SimpleRecoveryTest1_Worker(CheckpointType checkpointType, CompletionSyncMode completionSyncMode, bool testCommitCookie)
        {
            if (testCommitCookie)
            {
                // Generate a new unique byte sequence for test
                commitCookie = Guid.NewGuid().ToByteArray();
            }

            if (checkpointManager is null)
                checkpointDir = Path.Join(MethodTestDir, "checkpoints");

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleRecoveryTest1.log"), deleteOnClose: true);

            store1 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager }
                );

            store2 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager }
                );

            NumClicks value;
            AdInput inputArg = default;
            Output output = default;

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext1 = session1.BasicContext;

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                bContext1.Upsert(ref inputArray[key], ref value, Empty.Default);
            }

            if (testCommitCookie)
                store1.CommitCookie = commitCookie;
            store1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            if (completionSyncMode == CompletionSyncMode.Sync)
                store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            else
                await store1.CompleteCheckpointAsync();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                store2.Recover(token);
            else
                await store2.RecoverAsync(token);

            if (testCommitCookie)
                Assert.IsTrue(store2.RecoveredCommitCookie.SequenceEqual(commitCookie));
            else
                Assert.Null(store2.RecoveredCommitCookie);

            var session2 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext2 = session2.BasicContext;
            Assert.AreEqual(1, session2.ID);    // This is the first session on the recovered store

            for (int key = 0; key < numOps; key++)
            {
                var status = bContext2.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default);

                if (status.IsPending)
                {
                    bContext2.CompletePendingWithOutputs(out var outputs, wait: true);
                    Assert.IsTrue(outputs.Next());
                    output = outputs.Current.Output;
                    Assert.IsFalse(outputs.Next());
                    outputs.Current.Dispose();
                }
                else
                    Assert.IsTrue(status.Found);
                Assert.AreEqual(key, output.value.numClicks);
            }
            session2.Dispose();
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleRecoveryTest2([Values] CheckpointType checkpointType, [Values] CompletionSyncMode completionSyncMode)
        {
            checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(), new DefaultCheckpointNamingScheme(Path.Join(MethodTestDir, "checkpoints4")), false);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleRecoveryTest2.log"), deleteOnClose: true);

            store1 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
                );

            store2 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
                );


            NumClicks value;
            AdInput inputArg = default;
            Output output = default;

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext1 = session1.BasicContext;

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                bContext1.Upsert(ref inputArray[key], ref value, Empty.Default);
            }
            store1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                store2.Recover(token);
            else
                await store2.RecoverAsync(token);

            var session2 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext2 = session1.BasicContext;

            for (int key = 0; key < numOps; key++)
            {
                var status = bContext2.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default);

                if (status.IsPending)
                    bContext2.CompletePending(true);
                else
                {
                    Assert.AreEqual(key, output.value.numClicks);
                }
            }
            session2.Dispose();
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask ShouldRecoverBeginAddress([Values] CompletionSyncMode completionSyncMode)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleRecoveryTest2.log"), deleteOnClose: true);
            checkpointDir = Path.Join(MethodTestDir, "checkpoints6");

            store1 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            store2 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            NumClicks value;

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext1 = session1.BasicContext;

            var address = 0L;
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                bContext1.Upsert(ref inputArray[key], ref value, Empty.Default);

                if (key == 2999)
                    address = store1.Log.TailAddress;
            }

            store1.Log.ShiftBeginAddress(address);

            store1.TryInitiateFullCheckpoint(out Guid token, CheckpointType.FoldOver);
            if (completionSyncMode == CompletionSyncMode.Sync)
                store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            else
                await store1.CompleteCheckpointAsync();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                store2.Recover(token);
            else
                await store2.RecoverAsync(token);

            Assert.AreEqual(address, store2.Log.BeginAddress);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleReadAndUpdateInfoTest([Values] CompletionSyncMode completionSyncMode)
        {
            checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(), new DefaultCheckpointNamingScheme(Path.Join(MethodTestDir, "checkpoints")), false);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleReadAndUpdateInfoTest.log"), deleteOnClose: true);

            store1 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
                );

            store2 = new TsavoriteKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
                );


            NumClicks value;
            AdInput inputArg = default;
            Output output = default;
            AdSimpleFunctions functions1 = new(1);
            AdSimpleFunctions functions2 = new(2);

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(functions1);
            var bContext1 = session1.BasicContext;

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                if ((key & 1) > 0)
                    bContext1.Upsert(ref inputArray[key], ref value, Empty.Default);
                else
                {
                    AdInput input = new() { adId = inputArray[key], numClicks = value };
                    bContext1.RMW(ref inputArray[key], ref input);
                }
            }
            store1.TryInitiateFullCheckpoint(out Guid token, CheckpointType.FoldOver);
            if (completionSyncMode == CompletionSyncMode.Sync)
                store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            else
                await store1.CompleteCheckpointAsync();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                store2.Recover(token);
            else
                await store2.RecoverAsync(token);

            var session2 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(functions2);
            var bContext2 = session2.BasicContext;

            // Just need one operation here to verify readInfo/upsertInfo in the functions
            var lastKey = inputArray.Length - 1;
            var status = bContext2.Read(ref inputArray[lastKey], ref inputArg, ref output, Empty.Default);
            Assert.IsFalse(status.IsPending, status.ToString());

            value.numClicks = lastKey;
            status = bContext2.Upsert(ref inputArray[lastKey], ref value, Empty.Default);
            Assert.IsFalse(status.IsPending, status.ToString());

            inputArg = new() { adId = inputArray[lastKey], numClicks = new NumClicks { numClicks = 0 } }; // CopyUpdater adds, so make this 0
            status = bContext2.RMW(ref inputArray[lastKey], ref inputArg);
            Assert.IsFalse(status.IsPending, status.ToString());

            // Now verify Pending
            store2.Log.FlushAndEvict(wait: true);

            output.value = new() { numClicks = lastKey };
            inputArg.numClicks = new() { numClicks = lastKey };
            status = bContext2.Read(ref inputArray[lastKey], ref inputArg, ref output, Empty.Default);
            Assert.IsTrue(status.IsPending, status.ToString());
            bContext2.CompletePending(wait: true);

            // Upsert does not go pending so is skipped here

            --lastKey;
            output.value = new() { numClicks = lastKey };
            inputArg.numClicks = new() { numClicks = lastKey };
            status = bContext2.RMW(ref inputArray[lastKey], ref inputArg);
            Assert.IsTrue(status.IsPending, status.ToString());
            bContext2.CompletePending(wait: true);

            session2.Dispose();
        }

    }

    public class AdSimpleFunctions : SessionFunctionsBase<AdId, NumClicks, AdInput, Output, Empty>
    {
        long expectedVersion;

        internal AdSimpleFunctions(long ver = -1) => expectedVersion = ver;

        public override void ReadCompletionCallback(ref AdId key, ref AdInput input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key.adId, output.value.numClicks);
        }

        // Read functions
        public override bool SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo)
        {
            if (expectedVersion >= 0)
                Assert.AreEqual(expectedVersion, readInfo.Version);
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            if (expectedVersion >= 0)
                Assert.AreEqual(expectedVersion, readInfo.Version);
            dst.value = value;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (expectedVersion >= 0)
                Assert.AreEqual(expectedVersion, rmwInfo.Version);
            value = input.numClicks;
            return true;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (expectedVersion >= 0)
                Assert.AreEqual(expectedVersion, rmwInfo.Version);
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref Output output, ref RMWInfo rmwInfo)
        {
            if (expectedVersion >= 0)
                Assert.AreEqual(expectedVersion, rmwInfo.Version);
            return true;
        }

        public override bool CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (expectedVersion >= 0)
                Assert.AreEqual(expectedVersion, rmwInfo.Version);
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
            return true;
        }
    }
}