// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.sumstore
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<AdId.Comparer, SpanByteRecordDisposer>>;
    using StructStoreFunctions = StoreFunctions<AdId.Comparer, SpanByteRecordDisposer>;

    public class CheckpointManagerWithCookie : DeviceLogCommitCheckpointManager
    {
        /// <summary>
        /// Cookie
        /// </summary>
        public readonly byte[] Cookie;

        public CheckpointManagerWithCookie(bool testCommitCookie, INamedDeviceFactoryCreator deviceFactoryCreator, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = true, int fastCommitThrottleFreq = 0, ILogger logger = null)
            : base(deviceFactoryCreator, checkpointNamingScheme, removeOutdated, fastCommitThrottleFreq, logger)
        {
            if (testCommitCookie)
            {
                // Generate a new unique byte sequence for test
                Cookie = Guid.NewGuid().ToByteArray();
            }
        }

        public override byte[] GetCookie() => Cookie;
    }

    [TestFixture]
    class RecoveryTests
    {
        const int NumOps = 5000;
        AdId[] inputArray;

        string checkpointDir;
        CheckpointManagerWithCookie checkpointManager;

        private TsavoriteKV<StructStoreFunctions, StructAllocator> store1;
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store2;
        private IDevice log;


        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            checkpointManager = default;
            checkpointDir = default;
            inputArray = new AdId[NumOps];
            for (int i = 0; i < NumOps; i++)
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
        public async ValueTask PageBlobSimpleRecoveryTest(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] CompletionSyncMode completionSyncMode, [Values] bool testCommitCookie)
        {
            IgnoreIfNotRunningAzureTests();
            checkpointManager = new CheckpointManagerWithCookie(
                testCommitCookie,
                TestUtils.AzureStorageNamedDeviceFactoryCreator,
                new AzureCheckpointNamingScheme($"{AzureTestContainer}/{AzureTestDirectory}"));
            await SimpleRecoveryTest1_Worker(checkpointType, completionSyncMode, testCommitCookie);
            checkpointManager.PurgeAll();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask LocalDeviceSimpleRecoveryTest(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] CompletionSyncMode completionSyncMode,
            [Values] bool testCommitCookie)
        {
            checkpointManager = new CheckpointManagerWithCookie(
                testCommitCookie,
                new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(Path.Join(MethodTestDir, "chkpt")));
            await SimpleRecoveryTest1_Worker(checkpointType, completionSyncMode, testCommitCookie);
            checkpointManager.PurgeAll();
        }

        private async ValueTask SimpleRecoveryTest1_Worker(CheckpointType checkpointType, CompletionSyncMode completionSyncMode, bool testCommitCookie)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleRecoveryTest1.log"), deleteOnClose: true);

            store1 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointDir = checkpointDir,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            store2 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointDir = checkpointDir,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            NumClicks value;
            AdInput inputArg = default;
            Output output = default;

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext1 = session1.BasicContext;

            for (int key = 0; key < NumOps; key++)
            {
                value.numClicks = key;
                _ = bContext1.Upsert(SpanByte.FromPinnedVariable(ref inputArray[key]), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }

            _ = store1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            if (completionSyncMode == CompletionSyncMode.Sync)
                store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            else
                await store1.CompleteCheckpointAsync();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                _ = store2.Recover(token);
            else
                _ = await store2.RecoverAsync(token);

            if (testCommitCookie)
                ClassicAssert.IsTrue(store2.RecoveredCommitCookie.SequenceEqual(checkpointManager.Cookie));
            else
                ClassicAssert.Null(store2.RecoveredCommitCookie);

            var session2 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext2 = session2.BasicContext;
            ClassicAssert.AreEqual(1, session2.ID);    // This is the first session on the recovered store

            for (int key = 0; key < NumOps; key++)
            {
                var status = bContext2.Read(SpanByte.FromPinnedVariable(ref inputArray[key]), ref inputArg, ref output, Empty.Default);

                if (status.IsPending)
                {
                    _ = bContext2.CompletePendingWithOutputs(out var outputs, wait: true);
                    ClassicAssert.IsTrue(outputs.Next());
                    output = outputs.Current.Output;
                    ClassicAssert.IsFalse(outputs.Next());
                    outputs.Current.Dispose();
                }
                else
                    ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(key, output.value.numClicks);
            }
            session2.Dispose();
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleRecoveryTest2(
            [Values(CheckpointType.Snapshot, CheckpointType.FoldOver)] CheckpointType checkpointType,
            [Values] CompletionSyncMode completionSyncMode)
        {
            checkpointManager = new CheckpointManagerWithCookie(false, new LocalStorageNamedDeviceFactoryCreator(), new DefaultCheckpointNamingScheme(Path.Join(MethodTestDir, "checkpoints4")), false);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleRecoveryTest2.log"), deleteOnClose: true);

            store1 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            store2 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            NumClicks value;
            AdInput inputArg = default;
            Output output = default;

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext1 = session1.BasicContext;

            for (int key = 0; key < NumOps; key++)
            {
                value.numClicks = key;
                _ = bContext1.Upsert(SpanByte.FromPinnedVariable(ref inputArray[key]), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }
            _ = store1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                _ = store2.Recover(token);
            else
                _ = await store2.RecoverAsync(token);

            var session2 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext2 = session1.BasicContext;

            for (int key = 0; key < NumOps; key++)
            {
                var status = bContext2.Read(SpanByte.FromPinnedVariable(ref inputArray[key]), ref inputArg, ref output, Empty.Default);

                if (status.IsPending)
                    _ = bContext2.CompletePending(true);
                else
                    ClassicAssert.AreEqual(key, output.value.numClicks);
            }
            session2.Dispose();
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask ShouldRecoverBeginAddress([Values] CompletionSyncMode completionSyncMode)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleRecoveryTest2.log"), deleteOnClose: true);
            checkpointDir = Path.Join(MethodTestDir, "checkpoints6");

            store1 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointDir = checkpointDir
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            store2 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointDir = checkpointDir
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            NumClicks value;

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
            var bContext1 = session1.BasicContext;

            var address = 0L;
            for (int key = 0; key < NumOps; key++)
            {
                value.numClicks = key;
                _ = bContext1.Upsert(SpanByte.FromPinnedVariable(ref inputArray[key]), SpanByte.FromPinnedVariable(ref value), Empty.Default);

                if (key == 2999)
                    address = store1.Log.TailAddress;
            }

            store1.Log.ShiftBeginAddress(address);

            _ = store1.TryInitiateFullCheckpoint(out Guid token, CheckpointType.FoldOver);
            if (completionSyncMode == CompletionSyncMode.Sync)
                store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            else
                await store1.CompleteCheckpointAsync();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                _ = store2.Recover(token);
            else
                _ = await store2.RecoverAsync(token);

            ClassicAssert.AreEqual(address, store2.Log.BeginAddress);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleReadAndUpdateInfoTest([Values] CompletionSyncMode completionSyncMode)
        {
            checkpointManager = new CheckpointManagerWithCookie(false, new LocalStorageNamedDeviceFactoryCreator(), new DefaultCheckpointNamingScheme(Path.Join(MethodTestDir, "checkpoints")), false);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SimpleReadAndUpdateInfoTest.log"), deleteOnClose: true);

            store1 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            store2 = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 29,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            NumClicks value;
            AdInput inputArg = default;
            Output output = default;
            AdSimpleFunctions functions1 = new(1);
            AdSimpleFunctions functions2 = new(2);

            var session1 = store1.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(functions1);
            var bContext1 = session1.BasicContext;

            for (int key = 0; key < NumOps; key++)
            {
                value.numClicks = key;
                if ((key & 1) > 0)
                    _ = bContext1.Upsert(SpanByte.FromPinnedVariable(ref inputArray[key]), SpanByte.FromPinnedVariable(ref value), Empty.Default);
                else
                {
                    AdInput input = new() { adId = inputArray[key], numClicks = value };
                    _ = bContext1.RMW(SpanByte.FromPinnedVariable(ref inputArray[key]), ref input);
                }
            }
            _ = store1.TryInitiateFullCheckpoint(out Guid token, CheckpointType.FoldOver);
            if (completionSyncMode == CompletionSyncMode.Sync)
                store1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            else
                await store1.CompleteCheckpointAsync();
            session1.Dispose();

            if (completionSyncMode == CompletionSyncMode.Sync)
                _ = store2.Recover(token);
            else
                _ = await store2.RecoverAsync(token);

            var session2 = store2.NewSession<AdInput, Output, Empty, AdSimpleFunctions>(functions2);
            var bContext2 = session2.BasicContext;

            // Just need one operation here to verify readInfo/upsertInfo in the functions
            var lastKey = inputArray.Length - 1;
            var status = bContext2.Read(SpanByte.FromPinnedVariable(ref inputArray[lastKey]), ref inputArg, ref output, Empty.Default);
            ClassicAssert.IsFalse(status.IsPending, status.ToString());

            value.numClicks = lastKey;
            status = bContext2.Upsert(SpanByte.FromPinnedVariable(ref inputArray[lastKey]), SpanByte.FromPinnedVariable(ref value), Empty.Default);
            ClassicAssert.IsFalse(status.IsPending, status.ToString());

            inputArg = new() { adId = inputArray[lastKey], numClicks = new NumClicks { numClicks = 0 } }; // CopyUpdater adds, so make this 0
            status = bContext2.RMW(SpanByte.FromPinnedVariable(ref inputArray[lastKey]), ref inputArg);
            ClassicAssert.IsFalse(status.IsPending, status.ToString());

            // Now verify Pending
            store2.Log.FlushAndEvict(wait: true);

            output.value = new() { numClicks = lastKey };
            inputArg.numClicks = new() { numClicks = lastKey };
            status = bContext2.Read(SpanByte.FromPinnedVariable(ref inputArray[lastKey]), ref inputArg, ref output, Empty.Default);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext2.CompletePending(wait: true);

            // Upsert does not go pending so is skipped here

            --lastKey;
            output.value = new() { numClicks = lastKey };
            inputArg.numClicks = new() { numClicks = lastKey };
            status = bContext2.RMW(SpanByte.FromPinnedVariable(ref inputArray[lastKey]), ref inputArg);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext2.CompletePending(wait: true);

            session2.Dispose();
        }
    }

    public class AdSimpleFunctions : SessionFunctionsBase<AdInput, Output, Empty>
    {
        long expectedVersion;

        internal AdSimpleFunctions(long ver = -1) => expectedVersion = ver;

        public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref AdInput input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(diskLogRecord.Key.AsRef<AdId>().adId, output.value.numClicks);
        }

        // Read functions
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref AdInput input, ref Output output, ref ReadInfo readInfo)
        {
            if (expectedVersion >= 0)
                ClassicAssert.AreEqual(expectedVersion, readInfo.Version);
            output.value = srcLogRecord.ValueSpan.AsRef<NumClicks>();
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
        {
            if (expectedVersion >= 0)
                ClassicAssert.AreEqual(expectedVersion, rmwInfo.Version);
            dstLogRecord.ValueSpan.AsRef<NumClicks>() = input.numClicks;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
        {
            if (expectedVersion >= 0)
                ClassicAssert.AreEqual(expectedVersion, rmwInfo.Version);
            _ = Interlocked.Add(ref logRecord.ValueSpan.AsRef<NumClicks>().numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
        {
            if (expectedVersion >= 0)
                ClassicAssert.AreEqual(expectedVersion, rmwInfo.Version);
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref AdInput input, ref Output output, ref RMWInfo rmwInfo)
        {
            if (expectedVersion >= 0)
                ClassicAssert.AreEqual(expectedVersion, rmwInfo.Version);
            dstLogRecord.ValueSpan.AsRef<NumClicks>().numClicks += srcLogRecord.ValueSpan.AsRef<NumClicks>().numClicks + input.numClicks.numClicks;
            return true;
        }

        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref AdInput input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = NumClicks.Size, ValueIsObject = false };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref AdInput input)
            => new() { KeySize = key.Length, ValueSize = NumClicks.Size, ValueIsObject = false };
    }
}